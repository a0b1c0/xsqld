//! Runs the requests of one `/v2/pipeline` call on one connection.
//!
//! Two modes:
//! - native: statements run as written (read pool, a request running on its own, a stream holding an open
//!   transaction);
//! - grouped: the connection is inside the write thread's group transaction. The client's `BEGIN` becomes
//!   `SAVEPOINT xsqld_client`, `COMMIT` becomes `RELEASE`, `ROLLBACK` becomes `ROLLBACK TO` + `RELEASE`, so one
//!   client transaction failing undoes only its own work. If SQLite itself rolls back the whole transaction
//!   (ON CONFLICT ROLLBACK, an interrupt, an I/O error) the run stops with [`Stop::GroupLost`] and the write thread
//!   redoes the group without this request.

use std::time::Instant;

use rusqlite::types::{ToSqlOutput, ValueRef};
use rusqlite::{Statement, ToSql};

use crate::conn::{CLIENT_SP, Conn};
use crate::proto::{self, BatchCond, BatchResult, Col, StmtResult, StreamRequest, StreamResponse, StreamResult, Value};
use crate::sql::{self, Kind};

/// Protocol problems found before anything runs: answered with HTTP 400 and a text body, like sqld
pub fn validate(requests: &[StreamRequest], max_statements: usize) -> Result<(), String> {
    let mut statements = 0usize;
    let mut closed = false;
    for r in requests {
        if closed {
            return Err("Stream for this baton was closed".into());
        }
        match r {
            StreamRequest::Close => closed = true,
            StreamRequest::Execute { stmt } => {
                statements += 1;
                check_sql(stmt)?;
            }
            StreamRequest::Batch { batch } => {
                statements += batch.steps.len();
                for (i, step) in batch.steps.iter().enumerate() {
                    check_sql(&step.stmt)?;
                    if let Some(c) = &step.condition {
                        check_cond(c, i)?;
                    }
                }
            }
        }
    }
    if statements > max_statements {
        return Err(format!(
            "Too many statements in one request: {statements} (limit {max_statements})"
        ));
    }
    Ok(())
}

fn check_sql(stmt: &proto::Stmt) -> Result<(), String> {
    match (&stmt.sql, stmt.sql_id) {
        (Some(_), None) => Ok(()),
        (None, Some(id)) => Err(format!("SQL text {id} not found")),
        (Some(_), Some(_)) => Err("Either `sql` or `sql_id` are required, but not both".into()),
        (None, None) => Err("Either `sql` or `sql_id` are required".into()),
    }
}

fn check_cond(c: &BatchCond, step: usize) -> Result<(), String> {
    match c {
        BatchCond::Ok { step: s } | BatchCond::Error { step: s } if (*s as usize) < step => Ok(()),
        BatchCond::Ok { .. } | BatchCond::Error { .. } => Err("Invalid reference to step in a batch condition".into()),
        BatchCond::Not { cond } => check_cond(cond, step),
        BatchCond::And { conds } | BatchCond::Or { conds } => conds.iter().try_for_each(|c| check_cond(c, step)),
        BatchCond::IsAutocommit => {
            Err("BatchCond of type `is_autocommit` is only supported in protocol version hrana3 and higher".into())
        }
    }
}

/// All SQL texts of the requests
pub fn statements(requests: &[StreamRequest]) -> impl Iterator<Item = &str> {
    requests.iter().flat_map(|r| -> Box<dyn Iterator<Item = &str>> {
        match r {
            StreamRequest::Close => Box::new(std::iter::empty()),
            StreamRequest::Execute { stmt } => Box::new(stmt.sql.as_deref().into_iter()),
            StreamRequest::Batch { batch } => Box::new(batch.steps.iter().filter_map(|s| s.stmt.sql.as_deref())),
        }
    })
}

/// The group transaction is gone (SQLite rolled it back)
#[derive(Debug)]
pub struct GroupLost;

pub struct Exec<'c> {
    conn: &'c Conn,
    grouped: bool,
    /// Grouped mode: the client's transaction (savepoint) is open
    client_tx: bool,
    /// The request list contained `close`
    pub closed: bool,
    /// Bytes of row data still allowed in this HTTP response
    budget: u64,
}

pub type Results = Vec<StreamResult>;

impl<'c> Exec<'c> {
    pub fn native(conn: &'c Conn, budget: u64) -> Self {
        Self {
            conn,
            grouped: false,
            client_tx: false,
            closed: false,
            budget,
        }
    }

    pub fn grouped(conn: &'c Conn, budget: u64) -> Self {
        Self {
            conn,
            grouped: true,
            client_tx: false,
            closed: false,
            budget,
        }
    }

    /// Grouped mode: the requests ended with the client's transaction still open
    pub fn client_tx_open(&self) -> bool {
        self.client_tx
    }

    pub fn run(&mut self, requests: &[StreamRequest]) -> Result<Results, GroupLost> {
        // connections are shared between streams; each request sees `last_insert_rowid` as on a fresh sqld stream
        self.conn.reset_last_insert_rowid();
        let mut out = Vec::with_capacity(requests.len());
        for r in requests {
            out.push(match r {
                StreamRequest::Close => {
                    self.close();
                    StreamResult::Ok {
                        response: StreamResponse::Close,
                    }
                }
                StreamRequest::Execute { stmt } => match self.stmt(stmt)? {
                    Ok(result) => StreamResult::Ok {
                        response: StreamResponse::Execute { result },
                    },
                    Err(error) => StreamResult::Error { error },
                },
                StreamRequest::Batch { batch } => match self.batch(&batch.steps)? {
                    Ok(result) => StreamResult::Ok {
                        response: StreamResponse::Batch { result },
                    },
                    Err(error) => StreamResult::Error { error },
                },
            });
        }
        Ok(out)
    }

    fn close(&mut self) {
        self.closed = true;
        if self.grouped {
            if self.client_tx {
                self.rollback_client();
            }
        } else {
            self.conn.rollback_all();
        }
    }

    fn rollback_client(&mut self) {
        let _ = self
            .conn
            .c
            .execute_batch(&format!("ROLLBACK TO {CLIENT_SP}; RELEASE {CLIENT_SP};"));
        self.client_tx = false;
    }

    fn batch(&mut self, steps: &[proto::BatchStep]) -> Result<Result<BatchResult, proto::Error>, GroupLost> {
        // like sqld, statements that cannot even be turned into a query fail the whole batch before any step runs
        for s in steps {
            if let Some(e) = pre_check(&s.stmt).or_else(|| self.syntax_error(&s.stmt)) {
                return Ok(Err(e));
            }
        }
        let mut ok = Vec::with_capacity(steps.len());
        let mut res = BatchResult::default();
        for step in steps {
            let enabled = step.condition.as_ref().is_none_or(|c| eval(c, &ok));
            if !enabled {
                ok.push(false);
                res.step_results.push(None);
                res.step_errors.push(None);
                continue;
            }
            match self.stmt(&step.stmt)? {
                Ok(r) => {
                    ok.push(true);
                    res.step_results.push(Some(r));
                    res.step_errors.push(None);
                }
                Err(e) => {
                    ok.push(false);
                    res.step_results.push(None);
                    res.step_errors.push(Some(e));
                }
            }
        }
        Ok(Ok(res))
    }

    /// Syntax errors only: other prepare errors may depend on earlier steps (a table the batch creates). The
    /// prepared statement stays in the cache for the real run.
    fn syntax_error(&self, stmt: &proto::Stmt) -> Option<proto::Error> {
        let text = stmt.sql.as_deref()?;
        let e = self.conn.c.prepare_cached(text).err()?;
        let e = sql::prepare_error(&e, text);
        (e.code == "SQL_PARSE_ERROR").then_some(e)
    }

    fn stmt(&mut self, stmt: &proto::Stmt) -> Result<Result<StmtResult, proto::Error>, GroupLost> {
        if let Some(e) = pre_check(stmt) {
            return Ok(Err(e));
        }
        let sql_text = stmt.sql.as_deref().unwrap_or_default();
        let kind = sql::classify(sql_text);
        let started = Instant::now();
        let r = self.run_stmt(stmt, sql_text, kind, started);
        if self.grouped && self.conn.is_autocommit() {
            return Err(GroupLost);
        }
        Ok(r)
    }

    fn run_stmt(
        &mut self,
        stmt: &proto::Stmt,
        sql_text: &str,
        kind: Kind,
        started: Instant,
    ) -> Result<StmtResult, proto::Error> {
        let mut st = self
            .conn
            .c
            .prepare_cached(sql_text)
            .map_err(|e| sql::prepare_error(&e, sql_text))?;
        bind(&mut st, stmt)?;
        if self.grouped {
            match kind {
                Kind::Begin(_) => {
                    if self.client_tx {
                        return Err(sql::sqlite_error(1, "cannot start a transaction within a transaction"));
                    }
                    drop(st);
                    self.conn
                        .c
                        .execute_batch(&format!("SAVEPOINT {CLIENT_SP}"))
                        .map_err(|e| sql::rusqlite_error(&e))?;
                    self.client_tx = true;
                    return Ok(empty_result(started));
                }
                Kind::Commit => {
                    if !self.client_tx {
                        return Err(sql::sqlite_error(1, "cannot commit - no transaction is active"));
                    }
                    drop(st);
                    self.conn
                        .c
                        .execute_batch(&format!("RELEASE {CLIENT_SP}"))
                        .map_err(|e| sql::rusqlite_error(&e))?;
                    self.client_tx = false;
                    return Ok(empty_result(started));
                }
                Kind::Rollback => {
                    if !self.client_tx {
                        return Err(sql::sqlite_error(1, "cannot rollback - no transaction is active"));
                    }
                    drop(st);
                    self.rollback_client();
                    return Ok(empty_result(started));
                }
                _ => {}
            }
        }
        let mut rows = Vec::new();
        {
            let mut q = st.raw_query();
            while let Some(row) = q.next().map_err(|e| sql::rusqlite_error(&e))? {
                let ncols = row.as_ref().column_count();
                let mut vals = Vec::with_capacity(ncols);
                for i in 0..ncols {
                    let v = row.get_ref(i).map_err(|e| sql::rusqlite_error(&e))?;
                    let size = value_size(&v);
                    if size > self.budget {
                        return Err(proto::Error::new("Response is too large", "RESPONSE_TOO_LARGE"));
                    }
                    self.budget -= size;
                    vals.push(to_value(v));
                }
                rows.push(vals);
            }
        }
        // read after stepping: a schema change since prepare makes SQLite recompile the statement on its first
        // step, and `SELECT *` then has the new columns
        let cols: Vec<Col> = st
            .columns()
            .map(|c| Col {
                name: Some(c.name().to_owned()),
                decltype: c.decl_type().map(str::to_owned),
            })
            .collect();
        let affected = if kind.is_iud() { self.conn.c.changes() } else { 0 };
        let rows_read = rows.len() as u64;
        Ok(StmtResult {
            cols,
            rows,
            affected_row_count: affected,
            last_insert_rowid: kind.is_insert().then(|| self.conn.c.last_insert_rowid()),
            replication_index: None,
            rows_read,
            rows_written: affected,
            query_duration_ms: started.elapsed().as_micros() as f64 / 1000.0,
        })
    }
}

fn empty_result(started: Instant) -> StmtResult {
    StmtResult {
        query_duration_ms: started.elapsed().as_micros() as f64 / 1000.0,
        ..Default::default()
    }
}

/// Problems sqld reports before running anything (`hrana/stmt.rs` `proto_stmt_to_query`)
fn pre_check(stmt: &proto::Stmt) -> Option<proto::Error> {
    let text = stmt.sql.as_deref().unwrap_or_default();
    if sql::classify(text) == Kind::Empty {
        return Some(proto::Error::new(
            "SQL string does not contain any statement",
            "SQL_NO_STATEMENT",
        ));
    }
    if sql::many_statements(text) {
        return Some(proto::Error::new(
            "SQL string contains more than one statement",
            "SQL_MANY_STATEMENTS",
        ));
    }
    if sql::classify(text) == Kind::Vacuum {
        // sqld's parser does not support it; here it could not run anyway (it needs an attached database)
        return Some(proto::Error::new(
            format!("SQL string could not be parsed: unsupported statement: {text}"),
            "SQL_PARSE_ERROR",
        ));
    }
    if !stmt.args.is_empty() && !stmt.named_args.is_empty() {
        return Some(proto::Error::new(
            "Specifying both positional and named arguments is not supported",
            "ARGS_BOTH_POSITIONAL_AND_NAMED",
        ));
    }
    None
}

fn eval(c: &BatchCond, ok: &[bool]) -> bool {
    match c {
        BatchCond::Ok { step } => ok.get(*step as usize).copied().unwrap_or(false),
        BatchCond::Error { step } => !ok.get(*step as usize).copied().unwrap_or(false),
        BatchCond::Not { cond } => !eval(cond, ok),
        BatchCond::And { conds } => conds.iter().all(|c| eval(c, ok)),
        BatchCond::Or { conds } => conds.iter().any(|c| eval(c, ok)),
        BatchCond::IsAutocommit => false,
    }
}

struct Arg<'a>(&'a Value);

impl ToSql for Arg<'_> {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        Ok(ToSqlOutput::Borrowed(match self.0 {
            Value::Null => ValueRef::Null,
            Value::Integer(i) => ValueRef::Integer(*i),
            Value::Float(f) => ValueRef::Real(*f),
            Value::Text(s) => ValueRef::Text(s.as_bytes()),
            Value::Blob(b) => ValueRef::Blob(b),
        }))
    }
}

/// Binds arguments with sqld's rules (`query.rs` `Params::bind`): more values than parameters is an error,
/// a parameter without a value is an error (SQLite alone would bind NULL)
fn bind(st: &mut Statement, stmt: &proto::Stmt) -> Result<(), proto::Error> {
    let invalid = |m: String| proto::Error::new(format!("Arguments do not match SQL parameters: {m}"), "ARGS_INVALID");
    let named = !stmt.named_args.is_empty();
    let provided = if named { stmt.named_args.len() } else { stmt.args.len() };
    let count = st.parameter_count();
    if provided > count {
        return Err(invalid(format!(
            "too many parameters, expected {count} found {provided}"
        )));
    }
    let positional = |pos: usize| {
        if named {
            None
        } else {
            stmt.args.get(pos.wrapping_sub(1))
        }
    };
    let by_name = |name: &str| {
        if !named {
            return None;
        }
        stmt.named_args.iter().find(|a| a.name == name).map(|a| &a.value)
    };
    for index in 1..=count {
        let name = st.parameter_name(index).map(str::to_owned);
        let value = match name.as_deref() {
            Some(n) if n.starts_with('?') => {
                let pos: usize = n[1..].parse().map_err(|_| {
                    invalid(format!(
                        "invalid parameter {n}: expected a numerical position after `?`"
                    ))
                })?;
                positional(pos)
            }
            Some(n) => by_name(n).or_else(|| by_name(&n[1..])),
            None => positional(index),
        };
        match value {
            Some(v) => st
                .raw_bind_parameter(index, Arg(v))
                .map_err(|e| sql::rusqlite_error(&e))?,
            None if st.is_explain() > 0 => return Ok(()),
            None => {
                let which = name.unwrap_or_else(|| index.to_string());
                return Err(invalid(format!("value for parameter {which} not found")));
            }
        }
    }
    Ok(())
}

fn to_value(v: ValueRef) -> Value {
    match v {
        ValueRef::Null => Value::Null,
        ValueRef::Integer(i) => Value::Integer(i),
        ValueRef::Real(f) => Value::Float(f),
        ValueRef::Text(t) => Value::Text(String::from_utf8_lossy(t).into_owned()),
        ValueRef::Blob(b) => Value::Blob(b.to_vec()),
    }
}

/// Rough JSON size of a value, for the response size limit
fn value_size(v: &ValueRef) -> u64 {
    match v {
        ValueRef::Null => 15,
        ValueRef::Integer(_) | ValueRef::Real(_) => 48,
        ValueRef::Text(t) => 30 + t.len() as u64,
        ValueRef::Blob(b) => 30 + (b.len() as u64).div_ceil(3) * 4,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;

    fn reqs(json: &str) -> Vec<StreamRequest> {
        serde_json::from_str::<proto::PipelineReq>(&format!(r#"{{"requests":{json}}}"#))
            .unwrap()
            .requests
    }

    fn setup() -> (tempfile::TempDir, Conn) {
        let dir = tempfile::tempdir().unwrap();
        let cfg = Config::for_tests(dir.path());
        let c = Conn::open_writer(&dir.path().join("data"), &cfg).unwrap();
        c.c.execute_batch(
            "CREATE TABLE p(id INTEGER PRIMARY KEY); CREATE TABLE t(id INTEGER PRIMARY KEY, a TEXT UNIQUE NOT NULL, \
             b BLOB, c REAL, p INTEGER REFERENCES p(id));",
        )
        .unwrap();
        (dir, c)
    }

    fn json(r: &Results) -> serde_json::Value {
        let mut v = serde_json::to_value(r).unwrap();
        strip_durations(&mut v);
        v
    }

    fn strip_durations(v: &mut serde_json::Value) {
        match v {
            serde_json::Value::Object(m) => {
                m.remove("query_duration_ms");
                m.values_mut().for_each(strip_durations);
            }
            serde_json::Value::Array(a) => a.iter_mut().for_each(strip_durations),
            _ => {}
        }
    }

    #[test]
    fn execute_values_and_errors() {
        let (_d, c) = setup();
        let r = Exec::native(&c, u64::MAX)
            .run(&reqs(
                r#"[{"type":"execute","stmt":{"sql":"INSERT INTO t(a,b,c) VALUES (?,?,?)","args":[{"type":"text","value":"x"},{"type":"blob","base64":"AAEC"},{"type":"float","value":1.5}]}},
                {"type":"execute","stmt":{"sql":"SELECT id, a, b, c, p, 1+1 AS two FROM t"}},
                {"type":"execute","stmt":{"sql":"INSERT INTO t(a) VALUES (?)","args":[{"type":"text","value":"x"}]}},
                {"type":"execute","stmt":{"sql":"INSERT INTO t(a,p) VALUES ('y', 9)"}},
                {"type":"execute","stmt":{"sql":"INSERT INTO t(b) VALUES (1)"}},
                {"type":"execute","stmt":{"sql":"SELECT * FROM nope"}},
                {"type":"execute","stmt":{"sql":"  "}},
                {"type":"execute","stmt":{"sql":"SELECT 1; SELECT 2"}},
                {"type":"execute","stmt":{"sql":"SELECT ?","args":[]}},
                {"type":"execute","stmt":{"sql":"SELECT 1","args":[{"type":"null"}]}},
                {"type":"close"}]"#,
            ))
            .unwrap();
        let v = json(&r);
        assert_eq!(
            v[0]["response"]["result"],
            serde_json::json!({"cols":[],"rows":[],"affected_row_count":1,"last_insert_rowid":"1","replication_index":null,"rows_read":0,"rows_written":1})
        );
        assert_eq!(
            v[1]["response"]["result"]["cols"],
            serde_json::json!([{"name":"id","decltype":"INTEGER"},{"name":"a","decltype":"TEXT"},{"name":"b","decltype":"BLOB"},{"name":"c","decltype":"REAL"},{"name":"p","decltype":"INTEGER"},{"name":"two","decltype":null}])
        );
        assert_eq!(
            v[1]["response"]["result"]["rows"],
            serde_json::json!([[{"type":"integer","value":"1"},{"type":"text","value":"x"},{"type":"blob","base64":"AAEC"},{"type":"float","value":1.5},{"type":"null"},{"type":"integer","value":"2"}]])
        );
        assert_eq!(v[1]["response"]["result"]["last_insert_rowid"], serde_json::Value::Null);
        let err = |i: usize| {
            (
                v[i]["error"]["message"].as_str().unwrap().to_owned(),
                v[i]["error"]["code"].as_str().unwrap().to_owned(),
            )
        };
        assert_eq!(
            err(2),
            (
                "SQLite error: UNIQUE constraint failed: t.a".into(),
                "SQLITE_CONSTRAINT".into()
            )
        );
        assert_eq!(
            err(3),
            (
                "SQLite error: FOREIGN KEY constraint failed".into(),
                "SQLITE_CONSTRAINT".into()
            )
        );
        assert_eq!(
            err(4),
            (
                "SQLite error: NOT NULL constraint failed: t.a".into(),
                "SQLITE_CONSTRAINT".into()
            )
        );
        assert_eq!(
            err(5),
            ("SQLite error: no such table: nope".into(), "SQLITE_UNKNOWN".into())
        );
        assert_eq!(
            err(6),
            (
                "SQL string does not contain any statement".into(),
                "SQL_NO_STATEMENT".into()
            )
        );
        assert_eq!(
            err(7),
            (
                "SQL string contains more than one statement".into(),
                "SQL_MANY_STATEMENTS".into()
            )
        );
        assert_eq!(
            err(8),
            (
                "Arguments do not match SQL parameters: value for parameter 1 not found".into(),
                "ARGS_INVALID".into()
            )
        );
        assert_eq!(
            err(9),
            (
                "Arguments do not match SQL parameters: too many parameters, expected 0 found 1".into(),
                "ARGS_INVALID".into()
            )
        );
        assert_eq!(v[10], serde_json::json!({"type":"ok","response":{"type":"close"}}));
    }

    const D1_BATCH: &str = r#"[{"type":"batch","batch":{"steps":[
        {"stmt":{"sql":"BEGIN IMMEDIATE"}},
        {"condition":{"type":"ok","step":0},"stmt":{"sql":"INSERT INTO t(a) VALUES (?)","args":[{"type":"text","value":"z"}]}},
        {"condition":{"type":"ok","step":1},"stmt":{"sql":"INSERT INTO t(a) VALUES (?)","args":[{"type":"text","value":"@V@"}]}},
        {"condition":{"type":"ok","step":2},"stmt":{"sql":"COMMIT"}},
        {"condition":{"type":"not","cond":{"type":"ok","step":3}},"stmt":{"sql":"ROLLBACK"}}]}}]"#;

    fn count(c: &Conn) -> i64 {
        c.c.query_row("SELECT count(*) FROM t", [], |r| r.get(0)).unwrap()
    }

    #[test]
    fn batch_rolls_back_in_both_modes() {
        for grouped in [false, true] {
            let (_d, c) = setup();
            c.c.execute("INSERT INTO t(a) VALUES ('x')", []).unwrap();
            if grouped {
                c.c.execute_batch("BEGIN IMMEDIATE").unwrap();
            }
            let mut ex = if grouped {
                Exec::grouped(&c, u64::MAX)
            } else {
                Exec::native(&c, u64::MAX)
            };
            let r = ex.run(&reqs(&D1_BATCH.replace("@V@", "x"))).unwrap();
            let v = json(&r);
            let res = &v[0]["response"]["result"];
            assert_eq!(
                res["step_errors"][2]["message"],
                "SQLite error: UNIQUE constraint failed: t.a"
            );
            assert_eq!(res["step_results"][3], serde_json::Value::Null);
            assert_eq!(res["step_errors"][3], serde_json::Value::Null);
            assert!(res["step_results"][4].is_object(), "ROLLBACK ran");
            assert!(res["step_results"][0].is_object() && res["step_results"][1].is_object());
            assert!(!ex.client_tx_open());
            if grouped {
                assert!(!c.is_autocommit(), "group transaction still open");
                c.c.execute_batch("COMMIT").unwrap();
            }
            assert_eq!(count(&c), 1, "grouped = {grouped}");

            let mut ex = if grouped {
                Exec::grouped(&c, u64::MAX)
            } else {
                Exec::native(&c, u64::MAX)
            };
            if grouped {
                c.c.execute_batch("BEGIN IMMEDIATE").unwrap();
            }
            let r = ex.run(&reqs(&D1_BATCH.replace("@V@", "w"))).unwrap();
            assert!(
                json(&r)[0]["response"]["result"]["step_errors"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .all(|e| e.is_null())
            );
            if grouped {
                c.c.execute_batch("COMMIT").unwrap();
            }
            assert_eq!(count(&c), 3);
        }
    }

    #[test]
    fn grouped_transaction_control_errors_match_sqlite() {
        let (_d, c) = setup();
        let native: Vec<_> = ["COMMIT", "ROLLBACK"]
            .iter()
            .map(|s| c.c.execute_batch(s).unwrap_err().to_string())
            .collect();
        c.c.execute_batch("BEGIN IMMEDIATE").unwrap();
        let mut ex = Exec::grouped(&c, u64::MAX);
        let r = json(
            &ex.run(&reqs(
                r#"[{"type":"execute","stmt":{"sql":"COMMIT"}},{"type":"execute","stmt":{"sql":"ROLLBACK"}},
                {"type":"execute","stmt":{"sql":"BEGIN"}},{"type":"execute","stmt":{"sql":"BEGIN"}}]"#,
            ))
            .unwrap(),
        );
        assert_eq!(r[0]["error"]["message"], format!("SQLite error: {}", native[0]));
        assert_eq!(r[1]["error"]["message"], format!("SQLite error: {}", native[1]));
        assert_eq!(r[2]["type"], "ok");
        assert_eq!(
            r[3]["error"]["message"],
            "SQLite error: cannot start a transaction within a transaction"
        );
        assert!(ex.client_tx_open());
    }

    #[test]
    fn grouped_detects_lost_group_transaction() {
        let (_d, c) = setup();
        c.c.execute_batch("CREATE TABLE r(a UNIQUE ON CONFLICT ROLLBACK); INSERT INTO r VALUES (1);")
            .unwrap();
        c.c.execute_batch("BEGIN IMMEDIATE").unwrap();
        let mut ex = Exec::grouped(&c, u64::MAX);
        let r = ex.run(&reqs(
            r#"[{"type":"execute","stmt":{"sql":"INSERT INTO r VALUES (1)"}}]"#,
        ));
        assert!(r.is_err());
        assert!(c.is_autocommit());
    }

    #[test]
    fn close_rolls_back_open_transaction() {
        let (_d, c) = setup();
        let mut ex = Exec::native(&c, u64::MAX);
        ex.run(&reqs(
            r#"[{"type":"execute","stmt":{"sql":"BEGIN"}},{"type":"execute","stmt":{"sql":"INSERT INTO t(a) VALUES ('q')"}},{"type":"close"}]"#,
        ))
        .unwrap();
        assert!(ex.closed && c.is_autocommit());
        assert_eq!(count(&c), 0);
    }

    #[test]
    fn conditions() {
        let ok = [true, false];
        let c = |j: &str| serde_json::from_str::<BatchCond>(j).unwrap();
        assert!(eval(&c(r#"{"type":"ok","step":0}"#), &ok));
        assert!(eval(&c(r#"{"type":"error","step":1}"#), &ok));
        assert!(eval(&c(r#"{"type":"not","cond":{"type":"ok","step":1}}"#), &ok));
        assert!(!eval(
            &c(r#"{"type":"and","conds":[{"type":"ok","step":0},{"type":"ok","step":1}]}"#),
            &ok
        ));
        assert!(eval(
            &c(r#"{"type":"or","conds":[{"type":"ok","step":0},{"type":"ok","step":1}]}"#),
            &ok
        ));
        assert!(check_cond(&c(r#"{"type":"ok","step":2}"#), 2).is_err());
        assert!(check_cond(&c(r#"{"type":"is_autocommit"}"#), 2).is_err());
    }

    #[test]
    fn validation() {
        assert!(
            validate(
                &reqs(r#"[{"type":"close"},{"type":"execute","stmt":{"sql":"SELECT 1"}}]"#),
                10
            )
            .is_err()
        );
        assert!(validate(&reqs(r#"[{"type":"execute","stmt":{"sql_id":1}}]"#), 10).is_err());
        assert!(
            validate(
                &reqs(r#"[{"type":"execute","stmt":{"sql":"SELECT 1"}},{"type":"execute","stmt":{"sql":"SELECT 1"}}]"#),
                1
            )
            .is_err()
        );
        assert!(
            validate(
                &reqs(r#"[{"type":"execute","stmt":{"sql":"SELECT 1"}},{"type":"close"}]"#),
                10
            )
            .is_ok()
        );
    }

    #[test]
    fn named_and_numbered_parameters() {
        let (_d, c) = setup();
        let r = json(
            &Exec::native(&c, u64::MAX)
                .run(&reqs(
                    r#"[{"type":"execute","stmt":{"sql":"SELECT ?2, ?1","args":[{"type":"integer","value":"1"},{"type":"integer","value":"2"}]}},
                    {"type":"execute","stmt":{"sql":"SELECT :a, $b","named_args":[{"name":"a","value":{"type":"text","value":"A"}},{"name":"b","value":{"type":"text","value":"B"}}]}}]"#,
                ))
                .unwrap(),
        );
        assert_eq!(r[0]["response"]["result"]["rows"][0][0]["value"], "2");
        assert_eq!(r[1]["response"]["result"]["rows"][0][0]["value"], "A");
        assert_eq!(r[1]["response"]["result"]["rows"][0][1]["value"], "B");
    }

    #[test]
    fn batch_with_syntax_error_runs_nothing() {
        let (_d, c) = setup();
        let r = json(
            &Exec::native(&c, u64::MAX)
                .run(&reqs(
                    r#"[{"type":"batch","batch":{"steps":[{"stmt":{"sql":"CREATE TABLE n(a)"}},{"stmt":{"sql":"INSERT INTO n VALUES (1)"}},{"stmt":{"sql":"INSERT INTO t VALUES ("}}]}}]"#,
                ))
                .unwrap(),
        );
        assert_eq!(r[0]["error"]["code"], "SQL_PARSE_ERROR");
        let tables: i64 =
            c.c.query_row("SELECT count(*) FROM sqlite_schema WHERE name = 'n'", [], |r| r.get(0))
                .unwrap();
        assert_eq!(tables, 0);
    }

    #[test]
    fn columns_follow_schema_changes_by_other_connections() {
        let (d, c) = setup();
        let select = r#"[{"type":"execute","stmt":{"sql":"SELECT * FROM p"}}]"#;
        let cols = |r: &Results| json(r)[0]["response"]["result"]["cols"].as_array().unwrap().len();
        assert_eq!(cols(&Exec::native(&c, u64::MAX).run(&reqs(select)).unwrap()), 1);
        rusqlite::Connection::open(d.path().join("data"))
            .unwrap()
            .execute_batch("ALTER TABLE p ADD COLUMN extra TEXT")
            .unwrap();
        assert_eq!(cols(&Exec::native(&c, u64::MAX).run(&reqs(select)).unwrap()), 2);
    }

    #[test]
    fn last_insert_rowid_starts_fresh_per_request() {
        let (_d, c) = setup();
        c.c.execute_batch("CREATE TABLE kv(k TEXT PRIMARY KEY) WITHOUT ROWID; INSERT INTO p VALUES (7);")
            .unwrap();
        let r = json(
            &Exec::native(&c, u64::MAX)
                .run(&reqs(
                    r#"[{"type":"execute","stmt":{"sql":"INSERT INTO kv VALUES ('a')"}},{"type":"execute","stmt":{"sql":"SELECT last_insert_rowid()"}}]"#,
                ))
                .unwrap(),
        );
        assert_eq!(r[0]["response"]["result"]["last_insert_rowid"], "0");
        assert_eq!(r[1]["response"]["result"]["rows"][0][0]["value"], "0");
    }

    #[test]
    fn response_budget() {
        let (_d, c) = setup();
        let r = json(
            &Exec::native(&c, 1000)
                .run(&reqs(r#"[{"type":"execute","stmt":{"sql":"SELECT zeroblob(5000)"}}]"#))
                .unwrap(),
        );
        assert_eq!(r[0]["error"]["code"], "RESPONSE_TOO_LARGE");
    }
}
