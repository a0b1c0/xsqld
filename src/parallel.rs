//! Split-range parallel aggregation (README §3.1): one aggregate over a large table runs on several read connections
//! at once, each on a range of rowids, all on one snapshot, and SQLite combines the partial results.
//!
//! Always on, and invisible to clients: a statement is split only when the result is certain to be the one a single
//! connection returns, byte for byte; in every other case it runs as it always did. The steps, cheapest first, each
//! of which can send the statement back to the usual path:
//!
//! 1. the text has the one shape [`crate::shape`] takes (no schema access; a few microseconds);
//! 2. the table has at least `--parallel-min-rows` rows: its rowids span that many (two lookups at the ends of the
//!    rowid b-tree), and an estimate from the b-tree finds that many ([`estimate_rows`], at most once every 10 s per
//!    table);
//! 3. SQLite confirms the rest: an ordinary rowid table; grouping and MIN / MAX columns whose values compare equal
//!    only when they are the same value; a plan that scans the whole table (so the ranges cannot make it worse); and
//!    compiled programs that read only that table and call only deterministic built-in functions;
//! 4. additional read connections are free right now (never waited for), within `--parallel-pool-percent` of the
//!    pool for all split statements together;
//! 5. all connections are on one snapshot (`sqlite3_snapshot_get` / `sqlite3_snapshot_open`).
//!
//! Then every range runs on its connection; any failure stops all of them and the statement runs again on one
//! connection, as it would have, so a client never sees a partial sum. The same happens when a SUM turns out not to
//! be an exact integer sum (a REAL value, or values large enough that some order of addition could overflow), or the
//! partial rows outgrow `--parallel-max-partial-rows`.
//!
//! The read gate (checkpoint starvation, [`crate::reader::ReadGate`]) sees one read: the ranges run inside the
//! request's own pass, so a RESTART waits for this statement exactly as long as for any read of the same length.

use std::ffi::CStr;
#[cfg(test)]
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Instant;

use rusqlite::{Connection, OptionalExtension, Statement, ffi};

use crate::conn::Conn;
use crate::exec;
use crate::metrics::{DbMetrics, Fallback};
use crate::proto::{self, Col};
use crate::reader::Readers;
use crate::shape::{self, Shape};

const MAIN: &CStr = c"main";

/// Points where tests step in (by database file, so that tests running at the same time do not meet)
#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Point {
    /// Before the connections are put on one snapshot; returning true makes each start its own read transaction
    /// instead (the protection removed)
    Snapshot,
    /// No snapshot to take (untouched WAL): after the first connection started reading, before the others do
    UntouchedFirst,
    /// All on the snapshot, before the ranges run
    Ranges,
}

#[cfg(test)]
pub type Hook = Arc<dyn Fn(Point) -> bool + Send + Sync>;

#[cfg(test)]
pub static HOOKS: std::sync::LazyLock<parking_lot::Mutex<std::collections::HashMap<std::path::PathBuf, Hook>>> =
    std::sync::LazyLock::new(Default::default);

#[cfg(test)]
fn hook(readers: &Readers, at: Point) -> bool {
    let h = HOOKS.lock().get(readers.path()).cloned();
    h.is_some_and(|h| h(at))
}

#[cfg(not(test))]
fn hook(_: &Readers, _: Point) -> bool {
    false
}

#[cfg(not(test))]
#[derive(Clone, Copy)]
enum Point {
    Snapshot,
    UntouchedFirst,
    Ranges,
}

/// What one read request lends the statements it runs: its database's read pool and its deadline
pub struct Ctx<'r> {
    pub readers: &'r Readers,
    pub deadline: Instant,
}

/// Columns and rows of a split statement
pub type Done = Result<(Vec<Col>, Vec<Vec<proto::Value>>), proto::Error>;

/// Partial rows of one range
type Rows = Vec<Vec<SqlValue>>;

/// Rows of one range, or why it stopped
type Part = Result<Rows, Stop>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Stop {
    /// More partial rows than allowed in all
    Rows,
    /// An error, the deadline, or another range stopping
    Error,
}

impl Ctx<'_> {
    fn metrics(&self) -> &DbMetrics {
        self.readers.metrics()
    }

    fn fallback(&self, why: Fallback) -> Option<Done> {
        self.metrics().parallel_fallback[why as usize].fetch_add(1, Ordering::Relaxed);
        None
    }

    /// A fallback the schema decides: remembered with the schema's version, so that the same statement costs only
    /// its parse next time (the checks behind it compile three statements more)
    fn lasting(&self, conn: &Conn, text: &str, why: Fallback) -> Option<Done> {
        if let Some(v) = schema_version(&conn.c) {
            self.readers.mark_unsplittable(text, why, Some(v));
        }
        self.fallback(why)
    }

    /// Runs `st` (compiled from `text` on `conn` and bound from `stmt`, not stepped yet) split into ranges when that
    /// gives the result one connection would give. None: run it as usual (nothing was returned to the client and
    /// `conn` is back outside any transaction; `budget` is unchanged).
    pub fn try_run(
        &self,
        conn: &Conn,
        st: &Statement,
        stmt: &proto::Stmt,
        text: &str,
        budget: &mut u64,
    ) -> Option<Done> {
        let shape = shape::parse(text)?;
        let m = self.metrics();
        m.parallel_considered.fetch_add(1, Ordering::Relaxed);
        // a statement found not splittable before is not looked at again; the schema decides `table` and `plan`, so
        // those are forgotten when it changes (an index dropped, a column retyped)
        match self.readers.unsplittable(text) {
            Some((why, None)) => return self.fallback(why),
            Some((why, Some(v))) if schema_version(&conn.c) == Some(v) => return self.fallback(why),
            _ => {}
        }
        // parameters: SQLite must number them as the shape did, or the built statements would bind other values
        let Ok(values) = exec::arg_values(st, stmt) else {
            return self.lasting(conn, text, Fallback::Plan);
        };
        let numbered_alike = st.parameter_count() == shape.param_count
            && shape.params.iter().all(|p| match p.text.as_str() {
                "?" => st.parameter_name(p.index).is_none(),
                t => st.parameter_name(p.index) == Some(t),
            });
        if !numbered_alike || values.iter().any(Option::is_none) {
            return self.lasting(conn, text, Fallback::Plan);
        }
        let table = shape::quote(&shape.table);
        // a cheap size estimate first (both ends of the rowid b-tree); the checks that cost more come after
        // (a column named rowid hides that name: only an estimate, check_table finds the name that is the rowid)
        let estimate = ["rowid", "_rowid_", "oid"]
            .into_iter()
            .map(|rid| rowid_bounds(&conn.c, &table, rid).map(|b| b.map(|b| (rid, b))))
            .find(Result::is_ok)
            .unwrap_or(Err(()));
        let (rid0, (lo, hi)) = match estimate {
            Err(()) => return self.lasting(conn, text, Fallback::Table),
            Ok(None) => return self.fallback(Fallback::Small),
            Ok(Some(b)) => b,
        };
        let min_rows = self.readers.cfg().parallel_min_rows as i128;
        // the span of rowids is an upper bound of the rows, the count when rowids are dense; when it is not below the
        // threshold, the rows are estimated (rowids such as timestamps span far more than the table holds)
        if hi as i128 - lo as i128 + 1 < min_rows {
            return self.fallback(Fallback::Small);
        }
        let rows = self
            .readers
            .table_rows(&shape.table, || estimate_rows(&conn.c, &table, rid0, lo, hi));
        if rows.is_none_or(|n| n < min_rows) {
            return self.fallback(Fallback::Small);
        }
        // the checks below run outside the snapshot, each on its own read: the schema they saw must be the one the
        // snapshot has (a table rebuilt meanwhile with another collation or type would be split as the old one)
        let Some(checked_schema) = schema_version(&conn.c) else {
            return self.fallback(Fallback::Error);
        };
        let Some((rid, decltypes)) = check_table(&conn.c, &shape) else {
            return self.lasting(conn, text, Fallback::Table);
        };
        let range_sql = shape.range_sql(rid);
        if !check_plans(&conn.c, st, &values, text, &range_sql, !shape.keys.is_empty()) {
            return self.lasting(conn, text, Fallback::Plan);
        }
        // the combining statement compiles against a table of grouping columns and partial values only: a reference to
        // any other column fails here, before any work
        let Ok(mem) = merge_db(self.deadline, self.readers.cfg(), &shape, &decltypes) else {
            return self.lasting(conn, text, Fallback::Plan);
        };
        let merge_sql = shape.merge_sql();
        if !program_ok(&mem, &merge_sql, &["sum", "min", "max"], 1) {
            return self.lasting(conn, text, Fallback::Plan);
        }

        let cpus = std::thread::available_parallelism().map_or(1, |n| n.get());
        let want = self.readers.cfg().parallel_max_ranges.min(cpus);
        let Some(mut lease) = self.readers.lease_extra(want.saturating_sub(1), self.deadline) else {
            return self.fallback(Fallback::Pool);
        };
        let n = lease.conns.len() + 1;
        let shared = if hook(self.readers, Point::Snapshot) {
            begin_apart(conn, &lease.conns)
        } else {
            begin_shared(conn, &lease.conns, || {
                hook(self.readers, Point::UntouchedFirst);
            })
        };
        if shared.is_err() {
            end_all(conn, &lease.conns);
            return self.fallback(Fallback::Snapshot);
        }
        if schema_version(&conn.c) != Some(checked_schema) {
            end_all(conn, &lease.conns);
            return self.fallback(Fallback::Snapshot);
        }
        // under the snapshot now: the ranges cover exactly the rows it holds. Each range binds the same values: the
        // statement text is the same on every connection, so are its parameter numbers.
        let bounds = rowid_bounds(&conn.c, &table, rid).ok().flatten();
        let args = conn
            .prepare(&range_sql)
            .and_then(|rs| exec::derived_args(&rs, st, &values, &[":xsqld_lo", ":xsqld_hi"]));
        let (Some((lo, hi)), Ok(args)) = (bounds, args) else {
            // emptied meanwhile, or failed: one connection answers that at once
            end_all(conn, &lease.conns);
            return self.fallback(Fallback::Error);
        };
        let Some(ranges) = cuts(&conn.c, &table, rid, lo, hi, n) else {
            end_all(conn, &lease.conns);
            return self.fallback(Fallback::Error);
        };
        hook(self.readers, Point::Ranges);
        let extra = std::mem::take(&mut lease.conns);
        let (extra, parts) = self.run_ranges(conn, extra, &args, &range_sql, &ranges, shape.width());
        // a stopped split leaves the deadlines in the past: the statement may still run on this connection, and the
        // read transactions must end without being stopped
        for c in std::iter::once(conn).chain(&extra) {
            c.set_deadline(Some(self.deadline));
        }
        end_all(conn, &extra);
        lease.conns = extra;
        drop(lease);
        // the two outcomes that depend on the data rather than the moment: the same statement is not split again
        // (until the server restarts or the list fills up), so that a report with too many groups pays for the
        // attempt once, not every time
        let parts = match parts {
            Ok(p) => p,
            Err(Stop::Rows) => {
                self.readers.mark_unsplittable(text, Fallback::Rows, None);
                return self.fallback(Fallback::Rows);
            }
            Err(Stop::Error) => return self.fallback(Fallback::Error),
        };
        if !exact(&shape, &parts) {
            self.readers.mark_unsplittable(text, Fallback::Inexact, None);
            return self.fallback(Fallback::Inexact);
        }
        let started = Instant::now();
        let before = *budget;
        let out = merge(&mem, &shape, &parts, &merge_sql, st, &values, budget);
        m.parallel_merge.record(started.elapsed());
        match out {
            Some(done) => {
                m.parallel_runs.fetch_add(1, Ordering::Relaxed);
                m.parallel_ranges.fetch_add(n as u64, Ordering::Relaxed);
                Some(done)
            }
            None => {
                *budget = before;
                self.fallback(Fallback::Error)
            }
        }
    }

    /// Runs the range statement on `conn` (range 0, this thread) and on each of `extra` (moved to a thread each, and
    /// handed back); all stop as soon as one fails
    fn run_ranges(
        &self,
        conn: &Conn,
        extra: Vec<Conn>,
        args: &[exec::Derived],
        range_sql: &str,
        ranges: &[(i64, i64)],
        width: usize,
    ) -> (Vec<Conn>, Result<Vec<Rows>, Stop>) {
        let stop = AtomicBool::new(false);
        let rows = AtomicUsize::new(0);
        let limit = self.readers.cfg().parallel_max_partial_rows;
        // both: the interrupt ends a running statement at once, the stopper also one that has not started yet
        let handles: Vec<_> = std::iter::once(conn)
            .chain(&extra)
            .map(|c| (c.c.get_interrupt_handle(), c.stopper()))
            .collect();
        let halt = |why: Stop| {
            if !stop.swap(true, Ordering::Relaxed) {
                for (h, s) in &handles {
                    s.stop();
                    h.interrupt();
                }
            }
            Err(why)
        };
        let run = |c: &Conn, (lo, hi): (i64, i64)| -> Part {
            if stop.load(Ordering::Relaxed) {
                return Err(Stop::Error);
            }
            let Ok(mut rs) = c.prepare(range_sql) else {
                return halt(Stop::Error);
            };
            if rs.column_count() != width || exec::bind_args(&mut rs, args, &[lo, hi]).is_err() {
                return halt(Stop::Error);
            }
            let mut out = Vec::new();
            let mut q = rs.raw_query();
            loop {
                match q.next() {
                    Ok(Some(row)) => {
                        if rows.fetch_add(1, Ordering::Relaxed) >= limit {
                            return halt(Stop::Rows);
                        }
                        let mut vals = Vec::with_capacity(width);
                        for i in 0..width {
                            match row.get_ref(i) {
                                Ok(v) => vals.push(owned(v)),
                                Err(_) => return halt(Stop::Error),
                            }
                        }
                        out.push(vals);
                    }
                    Ok(None) => return Ok(out),
                    Err(_) => return halt(Stop::Error),
                }
            }
        };
        let (extra, results): (Vec<Conn>, Vec<Part>) = std::thread::scope(|s| {
            let threads: Vec<_> = extra
                .into_iter()
                .zip(&ranges[1..])
                .map(|(c, &r)| {
                    let run = &run;
                    s.spawn(move || {
                        let part = run(&c, r);
                        (c, part)
                    })
                })
                .collect();
            let first = run(conn, ranges[0]);
            // a panic ends the process (main.rs), so every thread hands its connection back
            let done: Vec<(Conn, Part)> = threads.into_iter().filter_map(|t| t.join().ok()).collect();
            let (conns, mut parts): (Vec<Conn>, Vec<Part>) = done.into_iter().unzip();
            parts.insert(0, first);
            (conns, parts)
        });
        // the first failure is the reason; the others stopped because of it
        let mut parts = Vec::with_capacity(results.len());
        let mut failed = None;
        for r in results {
            match r {
                Ok(p) => parts.push(p),
                Err(Stop::Rows) => failed = Some(Stop::Rows),
                Err(e) => failed = failed.or(Some(e)),
            }
        }
        let out = match failed {
            Some(e) => Err(e),
            None if parts.len() == ranges.len() => Ok(parts),
            None => Err(Stop::Error),
        };
        (extra, out)
    }
}

fn schema_version(c: &Connection) -> Option<i64> {
    c.prepare_cached("PRAGMA schema_version")
        .ok()?
        .query_row([], |r| r.get(0))
        .ok()
}

/// Least and largest rowid of `table`, from the two ends of its b-tree: one MIN or MAX per SELECT, since with both in
/// one SQLite scans the table. Ok(None) for an empty table, Err if it has no rowid (a view, `WITHOUT ROWID`).
fn rowid_bounds(c: &Connection, table: &str, rid: &str) -> Result<Option<(i64, i64)>, ()> {
    let sql = format!("SELECT (SELECT MIN({rid}) FROM {table}), (SELECT MAX({rid}) FROM {table})");
    let mut q = c.prepare_cached(&sql).map_err(drop)?;
    let (a, b) = q
        .query_row([], |r| Ok((r.get::<_, Option<i64>>(0)?, r.get::<_, Option<i64>>(1)?)))
        .map_err(drop)?;
    Ok(a.zip(b))
}

/// Rows of `table` (rowids `lo..=hi`), estimated without reading them. The span is cut into 16 parts and each part is
/// looked at from both ends: its first and last rowid, and how far apart the 64 rows nearest each end are. A part with
/// fewer than 64 rows is counted exactly; one whose two ends show about the same density (within a factor of 2) is
/// taken to hold that density over the stretch its rows occupy; otherwise (a dense block and a row far from it, a
/// sparse stretch and a dense one) that stretch is halved and each half looked at the same way. At most 256 parts are
/// looked at (five lookups in the rowid b-tree and at most 128 cells read each), however large the table; past that the
/// remaining parts are taken at the mean of their two densities. Exact for dense rowids and for a few rows far from the
/// rest, close for evenly sparse ones (timestamps as ids); a table that is in fact small but has many clusters can be
/// overestimated and then split: the result is the same, only the work is more.
fn estimate_rows(c: &Connection, table: &str, rid: &str, lo: i64, hi: i64) -> Option<i128> {
    const PARTS: i128 = 16;
    const BUDGET: usize = 256;
    const NEAR: i64 = 64;
    let q = |sql: String| c.prepare_cached(&sql).ok();
    let mut first = q(format!("SELECT MIN({rid}) FROM {table} WHERE {rid} BETWEEN ?1 AND ?2"))?;
    let mut last = q(format!("SELECT MAX({rid}) FROM {table} WHERE {rid} BETWEEN ?1 AND ?2"))?;
    let mut up = q(format!(
        "SELECT {rid} FROM {table} WHERE {rid} BETWEEN ?1 AND ?2 ORDER BY {rid} LIMIT 1 OFFSET {}",
        NEAR - 1
    ))?;
    let mut down = q(format!(
        "SELECT {rid} FROM {table} WHERE {rid} BETWEEN ?1 AND ?2 ORDER BY {rid} DESC LIMIT 1 OFFSET {}",
        NEAR - 1
    ))?;
    let mut count = q(format!("SELECT COUNT(*) FROM {table} WHERE {rid} BETWEEN ?1 AND ?2"))?;
    let one = |st: &mut rusqlite::CachedStatement, a: i64, b: i64| -> Option<Option<i64>> {
        st.query_row([a, b], |r| r.get::<_, Option<i64>>(0))
            .optional()
            .ok()
            .map(Option::flatten)
    };
    let (lo, hi) = (lo as i128, hi as i128);
    let span = hi - lo + 1;
    let mut parts: Vec<(i64, i64)> = (0..PARTS)
        .map(|k| (lo + span * k / PARTS, lo + span * (k + 1) / PARTS - 1))
        .filter(|(a, b)| a <= b)
        .map(|(a, b)| (a as i64, b as i64))
        .collect();
    let mut looked = 0usize;
    let mut total = 0.0f64;
    let near = (NEAR - 1) as f64;
    while let Some((a, b)) = parts.pop() {
        looked += 1;
        let Some(r0) = one(&mut first, a, b)? else {
            continue;
        };
        let rl = one(&mut last, a, b)?.unwrap_or(r0);
        let (Some(u), Some(d)) = (one(&mut up, a, b)?, one(&mut down, a, b)?) else {
            total += count.query_row([a, b], |r| r.get::<_, i64>(0)).ok()? as f64;
            continue;
        };
        // rows per rowid near the start and near the end of the stretch the part's rows occupy
        let (d0, d1) = (near / (u as f64 - r0 as f64), near / (rl as f64 - d as f64));
        if (d0 > 2.0 * d1 || d1 > 2.0 * d0) && looked + parts.len() + 2 <= BUDGET {
            let mid = (r0 as i128 + (rl as i128 - r0 as i128) / 2) as i64;
            parts.push((r0, mid));
            parts.push((mid + 1, rl));
            continue;
        }
        total += (d0 + d1) / 2.0 * (rl as f64 - r0 as f64 + 1.0);
    }
    Some(total as i128)
}

/// Affinity of a declared type, by SQLite's rules (datatype3.html §3.1)
fn affinity(decltype: &str) -> &'static str {
    let t = decltype.to_ascii_uppercase();
    if t.contains("INT") {
        "INTEGER"
    } else if t.contains("CHAR") || t.contains("CLOB") || t.contains("TEXT") {
        "TEXT"
    } else if t.contains("BLOB") || t.is_empty() {
        "BLOB"
    } else if t.contains("REAL") || t.contains("FLOA") || t.contains("DOUB") {
        "REAL"
    } else {
        "NUMERIC"
    }
}

/// The table is an ordinary rowid table; returns a name of its rowid no column hides, and the declared type of each
/// grouping column. Grouping and MIN / MAX columns must hold values that compare equal only when they are the same
/// value: the BINARY collation, and an affinity that stores 1 and 1.0 alike (not BLOB, nor a STRICT table's ANY).
/// Otherwise which of two equal values a group shows depends on the order rows arrive in, which splitting changes.
/// Aliases must not be column names (SQLite would resolve some references to the other one).
fn check_table(c: &Connection, shape: &Shape) -> Option<(&'static str, Vec<String>)> {
    let kind: (String, i64) = c
        .prepare_cached("SELECT type, wr FROM pragma_table_list WHERE schema = 'main' AND name = ?1 COLLATE NOCASE")
        .ok()?
        .query_row([&shape.table], |r| Ok((r.get(0)?, r.get(1)?)))
        .ok()?;
    if kind != ("table".to_owned(), 0) {
        return None;
    }
    let mut cols: Vec<(String, String)> = Vec::new();
    let mut q = c
        .prepare_cached("SELECT name, type FROM pragma_table_xinfo(?1, 'main')")
        .ok()?;
    let mut rows = q.query([&shape.table]).ok()?;
    while let Some(r) = rows.next().ok()? {
        let name: String = r.get(0).ok()?;
        let ty: String = r.get(1).ok()?;
        cols.push((name.to_ascii_lowercase(), ty));
    }
    let rid = ["rowid", "_rowid_", "oid"]
        .into_iter()
        .find(|r| cols.iter().all(|(n, _)| n != r))?;
    if shape
        .aliases
        .iter()
        .any(|a| a == rid || cols.iter().any(|(n, _)| n == a))
    {
        return None;
    }
    let comparable = |name: &str| -> Option<String> {
        let (_, ty) = cols.iter().find(|(n, _)| n == name)?;
        let ok =
            affinity(ty) != "BLOB" && !ty.eq_ignore_ascii_case("ANY") && collation(c, &shape.table, name)? == "BINARY";
        ok.then(|| ty.clone())
    };
    let decltypes = shape.keys.iter().map(|k| comparable(k)).collect::<Option<Vec<_>>>()?;
    for a in &shape.aggs {
        if let Some(col) = &a.column {
            comparable(col)?;
        }
    }
    Some((rid, decltypes))
}

/// Collation of a column, from the schema (`sqlite3_table_column_metadata`), uppercased
fn collation(c: &Connection, table: &str, column: &str) -> Option<String> {
    let t = std::ffi::CString::new(table).ok()?;
    let col = std::ffi::CString::new(column).ok()?;
    let mut coll: *const std::ffi::c_char = std::ptr::null();
    // SAFETY: the handle belongs to the open connection `c`, used on this thread; the out pointers are valid locals,
    // the ones not wanted are null (allowed); the returned string lives as long as the schema, copied at once
    let rc = unsafe {
        ffi::sqlite3_table_column_metadata(
            c.handle(),
            MAIN.as_ptr(),
            t.as_ptr(),
            col.as_ptr(),
            std::ptr::null_mut(),
            &mut coll,
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            std::ptr::null_mut(),
        )
    };
    if rc != ffi::SQLITE_OK || coll.is_null() {
        return None;
    }
    // SAFETY: non-null, NUL-terminated, owned by the schema
    Some(unsafe { CStr::from_ptr(coll) }.to_str().ok()?.to_ascii_uppercase())
}

/// Lines of `EXPLAIN QUERY PLAN` for `sql`, bound like the statement it was built from
fn query_plan(
    c: &Connection,
    sql: &str,
    original: &Statement,
    values: &[Option<&proto::Value>],
    extra: &[&str],
) -> Option<Vec<String>> {
    let mut q = c.prepare(&format!("EXPLAIN QUERY PLAN {sql}")).ok()?;
    let args = exec::derived_args(&q, original, values, extra).ok()?;
    exec::bind_args(&mut q, &args, &vec![0; extra.len()]).ok()?;
    let mut rows = q.raw_query();
    let mut out = Vec::new();
    while let Some(r) = rows.next().ok()? {
        out.push(r.get::<_, String>(3).ok()?);
    }
    Some(out)
}

/// The original statement scans the whole table (`SCAN t`, perhaps with temporary b-trees for GROUP BY and ORDER BY)
/// and the range statement searches the rowid range of the same table instead, with the same grouping: splitting
/// keeps the work per row the same. A plan that uses an index is left alone (ranges of rowids would make it scan).
/// Then the compiled range statement: it opens one table, and calls only deterministic built-ins and the four
/// aggregates.
fn check_plans(
    c: &Connection,
    st: &Statement,
    values: &[Option<&proto::Value>],
    text: &str,
    range_sql: &str,
    grouped: bool,
) -> bool {
    let Some(orig) = query_plan(c, crate::sql::trimmed(text), st, values, &[]) else {
        return false;
    };
    let Some(range) = query_plan(c, range_sql, st, values, &[":xsqld_lo", ":xsqld_hi"]) else {
        return false;
    };
    const GROUP_TMP: &str = "USE TEMP B-TREE FOR GROUP BY";
    const ORDER_TMP: &str = "USE TEMP B-TREE FOR ORDER BY";
    let Some((scan, rest)) = orig.split_first() else {
        return false;
    };
    let Some(target) = scan.strip_prefix("SCAN ") else {
        return false;
    };
    if target.contains(' ') || !rest.iter().all(|l| l == GROUP_TMP || l == ORDER_TMP) {
        return false;
    }
    // a bare `SELECT count(*) FROM t` counts the b-tree's cells without reading rows (opcode Count): ranges would
    // be slower than that
    if has_op(c, crate::sql::trimmed(text), "Count") != Some(false) {
        return false;
    }
    let orig_groups = rest.iter().any(|l| l == GROUP_TMP);
    let want = format!("SEARCH {target} USING INTEGER PRIMARY KEY (");
    let range_ok = match range.as_slice() {
        [search] => search.starts_with(&want) && !orig_groups,
        [search, g] => search.starts_with(&want) && g == GROUP_TMP && orig_groups && grouped,
        _ => false,
    };
    if !range_ok {
        return false;
    }
    program_ok(c, range_sql, &["count", "sum", "min", "max"], 1)
}

/// Whether the compiled program of `sql` on `c` has the opcode `op`
fn has_op(c: &Connection, sql: &str, op: &str) -> Option<bool> {
    let mut q = c.prepare(&format!("EXPLAIN {sql}")).ok()?;
    let mut rows = q.raw_query();
    while let Some(r) = rows.next().ok()? {
        if r.get_ref(1).ok()?.as_str().ok()? == op {
            return Some(true);
        }
    }
    Some(false)
}

/// The compiled program of `sql` on `c` (its `EXPLAIN` listing): opens exactly `tables` tables for reading and nothing
/// virtual, calls only [`shape::SCALAR_FUNCTIONS`] and the aggregates `aggs`
fn program_ok(c: &Connection, sql: &str, aggs: &[&str], tables: usize) -> bool {
    let run = || -> Option<bool> {
        let mut q = c.prepare(&format!("EXPLAIN {sql}")).ok()?;
        let mut rows = q.raw_query();
        let mut opened = 0;
        while let Some(r) = rows.next().ok()? {
            let op: String = r.get(1).ok()?;
            let p4: Option<String> = r.get(5).ok()?;
            let fname = p4
                .as_deref()
                .and_then(|p| p.split('(').next())
                .unwrap_or_default()
                .to_ascii_lowercase();
            match op.as_str() {
                "OpenRead" => opened += 1,
                "OpenWrite" | "VOpen" | "VFilter" | "VColumn" | "VNext" | "Program" | "OpenDup" | "OpenAutoindex" => {
                    return Some(false);
                }
                "Function" | "PureFunc" if !shape::SCALAR_FUNCTIONS.contains(&fname.as_str()) => return Some(false),
                "AggStep" | "AggStep1" | "AggInverse" | "AggValue" | "AggFinal" if !aggs.contains(&fname.as_str()) => {
                    return Some(false);
                }
                _ => {}
            }
        }
        Some(opened == tables)
    };
    run().unwrap_or(false)
}

/// The in-memory database the partial rows are combined in, with the table they go to; interrupted at `deadline`
///
/// It has the limits of the database's connections (the combining statement runs the client's `HAVING`, a `LIKE`
/// pattern there must be refused as on one connection), and double-quoted text is a name only, never a string: on
/// the database, `"v"` names the column `v`; in the in-memory table there is no such column, and the statement must
/// fail to compile rather than compare with the text `'v'`.
fn merge_db(
    deadline: Instant,
    cfg: &crate::config::Config,
    shape: &Shape,
    decltypes: &[String],
) -> rusqlite::Result<Connection> {
    use rusqlite::config::DbConfig;
    let mut mem = Connection::open_in_memory()?;
    crate::conn::apply_limits(&mem, cfg)?;
    mem.set_db_config(DbConfig::SQLITE_DBCONFIG_DQS_DML, false)?;
    mem.set_db_config(DbConfig::SQLITE_DBCONFIG_DQS_DDL, false)?;
    mem.progress_handler(1000, Some(move || Instant::now() >= deadline))?;
    mem.execute_batch(&shape.parts_ddl(decltypes))?;
    Ok(mem)
}

/// Puts every connection on one snapshot. The first (`conn`, which runs the original statement otherwise) takes it;
/// the others open it while the first still holds its read transaction, so no checkpoint can copy frames past it and
/// the WAL cannot start over until all have it.
///
/// `sqlite3_snapshot_get` fails while no transaction has been written to the WAL since it was created on disk
/// (documented: right after the server started on a checkpointed database, before the first write). Then the first
/// connection reads the database file alone, and no checkpoint can change that file while it holds its read
/// transaction; another connection that also finds no snapshot to take sees the same untouched WAL, so the same data.
/// If one of them does get a snapshot, a write came in between: no common snapshot, run on one connection.
fn begin_shared(conn: &Conn, extra: &[Conn], after_first: impl Fn()) -> Result<(), ()> {
    fn get(c: &Conn) -> Result<(i32, *mut ffi::sqlite3_snapshot), ()> {
        c.c.execute_batch("BEGIN").map_err(drop)?;
        let mut snap: *mut ffi::sqlite3_snapshot = std::ptr::null_mut();
        // SAFETY: handle of an open connection, used on this thread; a snapshot it returns is freed by the caller
        let rc = unsafe { ffi::sqlite3_snapshot_get(c.c.handle(), MAIN.as_ptr(), &mut snap) };
        Ok((rc, snap))
    }
    let (rc, snap) = get(conn)?;
    match rc {
        ffi::SQLITE_OK => {
            let mut res = Ok(());
            for c in extra {
                let opened = c.c.execute_batch("BEGIN").is_ok()
                    // SAFETY: open connection, outside a read transaction (just BEGIN); `snap` is valid until freed
                    && unsafe { ffi::sqlite3_snapshot_open(c.c.handle(), MAIN.as_ptr(), snap) } == ffi::SQLITE_OK;
                if !opened {
                    res = Err(());
                    break;
                }
            }
            // SAFETY: from sqlite3_snapshot_get, freed once
            unsafe { ffi::sqlite3_snapshot_free(snap) };
            res
        }
        ffi::SQLITE_ERROR => {
            UNTOUCHED.fetch_add(1, Ordering::Relaxed);
            after_first();
            for c in extra {
                let (rc, snap) = get(c)?;
                if rc == ffi::SQLITE_OK {
                    // SAFETY: from sqlite3_snapshot_get, freed once
                    unsafe { ffi::sqlite3_snapshot_free(snap) };
                }
                if rc != ffi::SQLITE_ERROR {
                    return Err(());
                }
            }
            Ok(())
        }
        _ => Err(()),
    }
}

/// Times the connections went on one snapshot by the untouched-WAL rule (tests)
static UNTOUCHED: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Tests only, standing for the protection removed: each connection starts its own read transaction
#[cfg_attr(not(test), allow(dead_code))]
fn begin_apart(conn: &Conn, extra: &[Conn]) -> Result<(), ()> {
    for c in std::iter::once(conn).chain(extra) {
        c.c.execute_batch("BEGIN").map_err(drop)?;
        c.c.query_row("SELECT 1 FROM sqlite_schema LIMIT 1", [], |_| Ok(()))
            .ok();
        std::thread::sleep(std::time::Duration::from_micros(200));
    }
    Ok(())
}

/// Ends the read transactions of the split (no error if none is open)
fn end_all(conn: &Conn, extra: &[Conn]) {
    conn.rollback_all();
    for c in extra {
        c.rollback_all();
    }
}

/// A value of a partial row, kept exactly: text as its bytes (SQLite text need not be valid UTF-8, and two
/// different invalid texts must stay two groups)
#[derive(Debug, Clone, PartialEq)]
enum SqlValue {
    Null,
    Integer(i64),
    Real(f64),
    Text(Vec<u8>),
    Blob(Vec<u8>),
}

impl rusqlite::ToSql for SqlValue {
    fn to_sql(&self) -> rusqlite::Result<rusqlite::types::ToSqlOutput<'_>> {
        use rusqlite::types::ValueRef;
        Ok(rusqlite::types::ToSqlOutput::Borrowed(match self {
            SqlValue::Null => ValueRef::Null,
            SqlValue::Integer(i) => ValueRef::Integer(*i),
            SqlValue::Real(f) => ValueRef::Real(*f),
            SqlValue::Text(t) => ValueRef::Text(t),
            SqlValue::Blob(b) => ValueRef::Blob(b),
        }))
    }
}

fn owned(v: rusqlite::types::ValueRef) -> SqlValue {
    use rusqlite::types::ValueRef;
    match v {
        ValueRef::Null => SqlValue::Null,
        ValueRef::Integer(i) => SqlValue::Integer(i),
        ValueRef::Real(f) => SqlValue::Real(f),
        ValueRef::Text(t) => SqlValue::Text(t.to_vec()),
        ValueRef::Blob(b) => SqlValue::Blob(b.to_vec()),
    }
}

/// Up to `n` contiguous ranges covering `lo..=hi` that hold about as many rows each. Rowids are dense in most tables,
/// but a table that once had rows at far-away rowids (an explicit id, a deleted batch) has long stretches without
/// rows; cutting the span of rowid values evenly would then give one range nearly all the rows. So the table is
/// probed at 64 points first (each probe two lookups in the rowid b-tree): gaps longer than a probe step are found
/// and left out of the length that is cut evenly.
fn cuts(c: &Connection, table: &str, rid: &str, lo: i64, hi: i64, n: usize) -> Option<Vec<(i64, i64)>> {
    const PROBES: i128 = 64;
    let (lo, hi) = (lo as i128, hi as i128);
    let step = ((hi - lo) / PROBES).max(1);
    let mut below = c
        .prepare_cached(&format!("SELECT MAX({rid}) FROM {table} WHERE {rid} <= ?1"))
        .ok()?;
    let mut above = c
        .prepare_cached(&format!("SELECT MIN({rid}) FROM {table} WHERE {rid} > ?1"))
        .ok()?;
    // stretches of rowids with rows, separated by gaps longer than `step`
    let mut runs: Vec<(i128, i128)> = Vec::new();
    let (mut start, mut p) = (lo, lo);
    loop {
        p += step;
        if p >= hi {
            runs.push((start, hi));
            break;
        }
        let b: i64 = below.query_row([p as i64], |r| r.get(0)).ok()?;
        let a: Option<i64> = above.query_row([p as i64], |r| r.get(0)).ok()?;
        match a {
            None => {
                runs.push((start, b as i128));
                break;
            }
            Some(a) if a as i128 - b as i128 > step => {
                runs.push((start, b as i128));
                start = a as i128;
                p = a as i128;
            }
            Some(_) => {}
        }
    }
    let total: i128 = runs.iter().map(|(a, b)| b - a + 1).sum();
    // the rowid at which the k-th of n equal shares of the rows (by the length of the stretches) begins
    let at = |k: i128| -> i128 {
        let mut want = total * k / n as i128;
        for (a, b) in &runs {
            if want < b - a + 1 {
                return a + want;
            }
            want -= b - a + 1;
        }
        hi + 1
    };
    let mut bounds: Vec<i128> = (0..=n as i128)
        .map(|k| {
            if k == 0 {
                lo
            } else if k == n as i128 {
                hi + 1
            } else {
                at(k)
            }
        })
        .collect();
    bounds.dedup();
    Some(bounds.windows(2).map(|w| (w[0] as i64, (w[1] - 1) as i64)).collect())
}

/// Every SUM is an exact integer sum whatever the order of the additions: each partial sum is an integer (SQLite's
/// SUM returns a REAL once it meets a value that is not an integer) and, over all ranges, the number of values times
/// the largest magnitude among them stays within 64 bits, so that no partial sum, however the rows are ordered,
/// can overflow (SQLite reports `integer overflow` only when one does).
fn exact(shape: &Shape, parts: &[Vec<Vec<SqlValue>>]) -> bool {
    for sc in shape.sum_cols() {
        let mut count: i128 = 0;
        let mut largest: i128 = 0;
        for row in parts.iter().flatten() {
            match (&row[sc.sum], &row[sc.count], &row[sc.min], &row[sc.max]) {
                (SqlValue::Null, SqlValue::Integer(0), SqlValue::Null, SqlValue::Null) => {}
                (SqlValue::Integer(_), SqlValue::Integer(n), SqlValue::Integer(lo), SqlValue::Integer(hi)) => {
                    count += *n as i128;
                    largest = largest.max((*lo as i128).abs()).max((*hi as i128).abs());
                }
                _ => return false,
            }
        }
        if count.saturating_mul(largest) > i64::MAX as i128 {
            return false;
        }
    }
    true
}

/// Loads the partial rows into the in-memory table and runs the combining statement: Some(result) with the
/// original statement's column names and declared types, or None if anything failed (the statement then runs on one
/// connection). RESPONSE_TOO_LARGE is a result: the rows are the ones one connection returns, in the same order.
fn merge(
    mem: &Connection,
    shape: &Shape,
    parts: &[Vec<Vec<SqlValue>>],
    merge_sql: &str,
    st: &Statement,
    values: &[Option<&proto::Value>],
    budget: &mut u64,
) -> Option<Done> {
    let width = shape.width();
    mem.execute_batch("BEGIN").ok()?;
    {
        let marks = vec!["?"; width].join(", ");
        let mut ins = mem
            .prepare(&format!("INSERT INTO {} VALUES ({marks})", shape::quote(&shape.table)))
            .ok()?;
        for row in parts.iter().flatten() {
            ins.execute(rusqlite::params_from_iter(row.iter())).ok()?;
        }
    }
    mem.execute_batch("COMMIT").ok()?;
    let mut ms = mem.prepare(merge_sql).ok()?;
    let args = exec::derived_args(&ms, st, values, &[]).ok()?;
    exec::bind_args(&mut ms, &args, &[]).ok()?;
    let rows = match exec::collect_rows(&mut ms, budget) {
        Ok(rows) => rows,
        Err(e) if e.code == "RESPONSE_TOO_LARGE" => return Some(Err(e)),
        Err(_) => return None,
    };
    let cols = st
        .columns()
        .map(|c| Col {
            name: Some(c.name().to_owned()),
            decltype: c.decl_type().map(str::to_owned),
        })
        .collect();
    Some(Ok((cols, rows)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use crate::proto::{PipelineReq, StreamRequest};
    use crate::reader::{ReadGate, ReadOutcome};
    use std::sync::Arc;
    use std::time::Duration;

    pub struct Fx {
        _dir: tempfile::TempDir,
        pub readers: Arc<Readers>,
        pub w: Conn,
        pub metrics: Arc<DbMetrics>,
        pub path: std::path::PathBuf,
    }

    pub fn fixture(schema: &str, tweak: impl FnOnce(&mut Config)) -> Fx {
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = Config::for_tests(dir.path());
        cfg.parallel_min_rows = 1;
        tweak(&mut cfg);
        let cfg = Arc::new(cfg);
        let path = dir.path().join("data");
        let w = Conn::open_writer(&path, &cfg).unwrap();
        w.c.execute_batch(schema).unwrap();
        let metrics = Arc::new(DbMetrics::default());
        let gate = ReadGate::new(metrics.clone());
        Fx {
            readers: Arc::new(Readers::new(path.clone(), cfg, gate)),
            _dir: dir,
            w,
            metrics,
            path,
        }
    }

    fn reqs(json: &str) -> Vec<StreamRequest> {
        serde_json::from_str::<PipelineReq>(&format!(r#"{{"requests":{json}}}"#))
            .unwrap()
            .requests
    }

    fn strip(v: &mut serde_json::Value) {
        match v {
            serde_json::Value::Object(m) => {
                m.remove("query_duration_ms");
                m.values_mut().for_each(strip);
            }
            serde_json::Value::Array(a) => a.iter_mut().for_each(strip),
            _ => {}
        }
    }

    impl Fx {
        pub fn runs(&self) -> u64 {
            self.metrics.parallel_runs.load(Ordering::Relaxed)
        }

        pub fn fallbacks(&self, why: Fallback) -> u64 {
            self.metrics.parallel_fallback[why as usize].load(Ordering::Relaxed)
        }

        async fn run(&self, requests: &str) -> serde_json::Value {
            self.run_until(requests, Instant::now() + Duration::from_secs(30)).await
        }

        pub async fn run_until(&self, requests: &str, deadline: Instant) -> serde_json::Value {
            let ReadOutcome::Done { results, .. } = self.readers.run(reqs(requests), deadline).await.unwrap() else {
                panic!("not a read: {requests}")
            };
            let mut v = serde_json::to_value(&results).unwrap();
            strip(&mut v);
            v
        }

        /// The statement's result (or error) as the read pool returns it, and the same inside a client transaction,
        /// where it never runs split
        pub async fn both(&self, sql: &str, args: &str) -> (serde_json::Value, serde_json::Value) {
            let stmt =
                serde_json::json!({"sql": sql, "args": serde_json::from_str::<serde_json::Value>(args).unwrap()});
            let split = self.run(&format!(r#"[{{"type":"execute","stmt":{stmt}}}]"#)).await;
            let one = self
                .run(&format!(
                    r#"[{{"type":"batch","batch":{{"steps":[{{"stmt":{{"sql":"BEGIN"}}}},{{"stmt":{stmt}}},{{"stmt":{{"sql":"COMMIT"}}}}]}}}}]"#
                ))
                .await;
            let split = split[0].clone();
            let one = &one[0]["response"]["result"];
            let one = if one["step_errors"][1].is_null() {
                serde_json::json!({"type":"ok","response":{"type":"execute","result":one["step_results"][1]}})
            } else {
                serde_json::json!({"type":"error","error":one["step_errors"][1]})
            };
            (split, one)
        }

        /// Same result split as on one connection, and it did run split
        pub async fn same_split(&self, sql: &str, args: &str) {
            let before = self.runs();
            let (split, one) = self.both(sql, args).await;
            assert_eq!(split, one, "{sql}");
            assert_eq!(self.runs(), before + 1, "not split: {sql}");
        }

        /// Same result, and it ran on one connection for `why`
        pub async fn same_unsplit(&self, sql: &str, args: &str, why: Fallback) {
            let (before, fb) = (self.runs(), self.fallbacks(why));
            let (split, one) = self.both(sql, args).await;
            assert_eq!(split, one, "{sql}");
            assert_eq!(self.runs(), before, "split: {sql}");
            assert!(self.fallbacks(why) > fb, "not {why:?}: {sql}");
        }
    }

    struct Rng(u64);
    impl Rng {
        fn next(&mut self) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0
        }
        fn below(&mut self, n: u64) -> u64 {
            self.next() % n
        }
    }

    const SCHEMA: &str = "CREATE TABLE t(id INTEGER PRIMARY KEY, g INTEGER, s TEXT, v INTEGER, w INTEGER, \
                          n TEXT COLLATE NOCASE, r REAL, b, big INTEGER);";

    /// Rows with NULLs, mixed case, holes and negative rowids
    fn fill(w: &Conn, seed: u64, rows: u64) {
        let mut r = Rng(seed.wrapping_mul(0x9E3779B97F4A7C15) | 1);
        w.c.execute_batch("BEGIN").unwrap();
        let mut id: i64 = -(r.below(1000) as i64);
        for _ in 0..rows {
            // mostly dense, sometimes a hole, once in a while far away
            id += match r.below(100) {
                0 => 1 + r.below(100_000) as i64,
                1..=10 => 2,
                _ => 1,
            };
            let null = |r: &mut Rng, p: u64| r.below(100) < p;
            let g = (!null(&mut r, 5)).then(|| r.below(7) as i64 - 3);
            let s = (!null(&mut r, 5)).then(|| ["a", "b", "B", "ab", "", "é"][r.below(6) as usize]);
            let v = (!null(&mut r, 10)).then(|| r.below(2001) as i64 - 1000);
            let w2 = (!null(&mut r, 50)).then(|| r.below(5) as i64);
            let n = ["x", "X", "y"][r.below(3) as usize];
            let rv = r.below(1000) as f64 / 7.0;
            w.c.execute(
                "INSERT INTO t VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                rusqlite::params![
                    id,
                    g,
                    s,
                    v,
                    w2,
                    n,
                    rv,
                    r.below(3) as i64,
                    i64::MAX / 3 + r.below(10) as i64
                ],
            )
            .unwrap();
        }
        w.c.execute_batch("COMMIT").unwrap();
    }

    /// Statements of the shape that must not be split, or not this time: the result is still one connection's, and the
    /// reason is counted
    #[tokio::test(flavor = "multi_thread")]
    async fn statements_that_cannot_be_split_exactly_run_on_one_connection() {
        let fx = fixture(SCHEMA, |c| c.parallel_max_partial_rows = 1000);
        fill(&fx.w, 7, 3000);
        fx.w.c
            .execute_batch(
                "CREATE TABLE u(id INTEGER PRIMARY KEY, g INTEGER, v INTEGER) WITHOUT ROWID; \
                 CREATE VIEW vv AS SELECT * FROM t; CREATE VIEW vr AS SELECT rowid AS rowid, * FROM t; \
                 CREATE VIRTUAL TABLE ft USING fts5(a); INSERT INTO ft VALUES ('x'), ('y'); \
                 CREATE TABLE sh(rowid INTEGER, _rowid_ INTEGER, oid INTEGER, g INTEGER); \
                 INSERT INTO sh VALUES (1, 2, 3, 4), (5, 6, 7, 8); \
                 INSERT INTO u VALUES (1, 1, 1), (2, 1, 2); \
                 CREATE TABLE ov(id INTEGER PRIMARY KEY, g INTEGER, big INTEGER, alt INTEGER); \
                 INSERT INTO ov VALUES (1, 1, 4000000000000000000, 4000000000000000000), \
                 (1000, 1, 4000000000000000000, -4000000000000000000), (2000, 1, 4000000000000000000, 4000000000000000000), \
                 (3000, 1, 4000000000000000000, -4000000000000000000);",
            )
            .unwrap();
        use Fallback::*;
        let cases: &[(&str, Fallback)] = &[
            // a SUM over REAL values: its last digits depend on the order of additions
            ("SELECT g, SUM(r) FROM t GROUP BY g", Inexact),
            // sums near the 64-bit limit: one connection reports `integer overflow`, the split must too (here a
            // range overflows on its own; below the ranges do not, but some order of the additions could)
            ("SELECT SUM(big) FROM t", Error),
            // each range sums one value: no range overflows, but one connection does (4e18 + 4e18 + 4e18)
            ("SELECT g, SUM(big) FROM ov GROUP BY g", Inexact),
            // no order overflows here (+, −, +, −), but the bound cannot tell: run again, same result
            ("SELECT SUM(alt) FROM ov WHERE g = 1", Inexact),
            // NOCASE: 'x' and 'X' are one group, which of them it shows depends on row order
            ("SELECT n, COUNT(*) FROM t GROUP BY n", Table),
            ("SELECT MIN(n) FROM t", Table),
            // no declared type: 1 and 1.0 can both be stored and compare equal
            ("SELECT b, COUNT(*) FROM t GROUP BY b", Table),
            // an alias that is a column name
            ("SELECT COUNT(*) AS v FROM t WHERE g > 0", Table),
            ("SELECT COUNT(*) FROM vv WHERE v > 0", Table),
            ("SELECT COUNT(*) FROM vr WHERE v > 0", Table),
            ("SELECT COUNT(*) FROM ft WHERE a = 'x'", Table),
            ("SELECT COUNT(*) FROM u WHERE v > 0", Table),
            ("SELECT g, COUNT(*) FROM sh GROUP BY g", Table),
            // counting the b-tree's cells is faster than any split
            ("SELECT COUNT(*) FROM t", Plan),
            // the plan would change: the user's own rowid condition, an index
            ("SELECT COUNT(*) FROM t WHERE id > 100", Plan),
            ("SELECT COUNT(*) FROM t WHERE rowid BETWEEN 10 AND 2000", Plan),
            // not a deterministic function
            ("SELECT COUNT(*) FROM t WHERE abs(random()) >= 0", Plan),
            ("SELECT COUNT(*) FROM t WHERE v < unixepoch()", Plan),
            // more groups than --parallel-max-partial-rows (1000 here) over all ranges
            ("SELECT v, COUNT(*) FROM t GROUP BY v", Rows),
        ];
        for (sql, why) in cases {
            fx.same_unsplit(sql, "[]", *why).await;
        }
        // remembered: the same statement is not split again
        let before = fx.fallbacks(Rows);
        fx.same_unsplit("SELECT v, COUNT(*) FROM t GROUP BY v", "[]", Rows)
            .await;
        assert_eq!(fx.fallbacks(Rows), before + 1);
        // an index on the grouping column: the plan scans it in order, ranges of rowids would not
        fx.w.c.execute_batch("CREATE INDEX t_g ON t(g)").unwrap();
        fx.same_unsplit("SELECT g, COUNT(*) FROM t GROUP BY g", "[]", Plan)
            .await;
        // a table named by a column alias: _rowid_ stands in for a column named rowid
        fx.w.c
            .execute_batch(
                "CREATE TABLE rc(rowid TEXT, g INTEGER); \
                 WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 500) \
                 INSERT INTO rc SELECT 'r' || i, i % 3 FROM n;",
            )
            .unwrap();
        fx.same_split("SELECT g, COUNT(*), MAX(rowid) FROM rc GROUP BY g", "[]")
            .await;
    }

    /// A statement found not splittable for its table or plan is remembered until the schema changes
    #[tokio::test(flavor = "multi_thread")]
    async fn a_remembered_plan_is_forgotten_when_the_schema_changes() {
        let fx = fixture(SCHEMA, |_| {});
        fill(&fx.w, 9, 2000);
        let q = "SELECT g, COUNT(*) FROM t WHERE w IS NOT NULL GROUP BY g";
        fx.same_split(q, "[]").await;
        fx.w.c.execute_batch("CREATE INDEX t_g ON t(g)").unwrap();
        fx.same_unsplit(q, "[]", Fallback::Plan).await;
        fx.same_unsplit(q, "[]", Fallback::Plan).await;
        assert!(
            fx.readers
                .unsplittable(q)
                .is_some_and(|(why, v)| why == Fallback::Plan && v.is_some())
        );
        fx.w.c.execute_batch("DROP INDEX t_g").unwrap();
        fx.same_split(q, "[]").await;
    }

    /// Review of 0.1.3: what the combining statement could see that one connection does not, and a schema change
    /// between the checks and the snapshot
    #[tokio::test(flavor = "multi_thread")]
    async fn review_merge_sees_only_what_one_connection_sees() {
        let fx = fixture(SCHEMA, |_| {});
        fill(&fx.w, 13, 3000);
        let mut differ: Vec<String> = Vec::new();
        // 1. rowid and its other names in HAVING / ORDER BY: the in-memory table has rowids of its own
        for sql in [
            "SELECT g, COUNT(*) FROM t GROUP BY g ORDER BY rowid DESC",
            "SELECT g, COUNT(*) FROM t GROUP BY g ORDER BY oid",
            "SELECT g, COUNT(*) FROM t GROUP BY g ORDER BY _rowid_ DESC",
            "SELECT g, COUNT(*) FROM t GROUP BY g ORDER BY \"rowid\" DESC",
            "SELECT g, COUNT(*) FROM t GROUP BY g HAVING rowid > 100",
            // 2. a double-quoted name that is a column outside GROUP BY (a string in the in-memory database)
            "SELECT g, COUNT(*) FROM t GROUP BY g HAVING \"v\" > 998",
            "SELECT g, COUNT(*) FROM t GROUP BY g ORDER BY \"v\" DESC",
        ] {
            let (split, one) = fx.both(sql, "[]").await;
            if split != one {
                differ.push(sql.to_owned());
            }
        }
        // 3. the limits of the read connections apply to the combining statement too
        let pattern = format!("{}%", "a".repeat(2000));
        let args = format!(r#"[{{"type":"text","value":"{pattern}"}}]"#);
        let (split, one) = fx
            .both("SELECT s, COUNT(*) FROM t GROUP BY s HAVING s LIKE ?", &args)
            .await;
        assert_eq!(
            one["error"]["message"],
            "SQLite error: LIKE or GLOB pattern too complex"
        );
        if split != one {
            differ.push(format!("LIKE pattern of 2000 bytes: {}", split["type"]));
        }
        assert!(differ.is_empty(), "differ from one connection: {differ:#?}");
    }

    /// Review of 0.1.3: in the in-memory database a double-quoted name is a name only (never the string), and the
    /// limits are those of the database's connections
    #[test]
    fn the_combining_database_reads_double_quotes_as_names_and_has_the_limits() {
        let shape = shape::parse("SELECT g, COUNT(*) FROM t GROUP BY g").unwrap();
        let dir = tempfile::tempdir().unwrap();
        let cfg = Config::for_tests(dir.path());
        let mem = merge_db(
            Instant::now() + Duration::from_secs(5),
            &cfg,
            &shape,
            &["INTEGER".into()],
        )
        .unwrap();
        assert!(mem.prepare("SELECT \"g\" FROM t").is_ok());
        assert!(
            mem.prepare("SELECT 1 FROM t WHERE \"v\" > 1").is_err(),
            "\"v\" read as a string"
        );
        let long = format!("{}%", "a".repeat(cfg.max_like_pattern_bytes as usize + 1));
        let e = mem
            .query_row("SELECT 'a' LIKE ?1", [&long], |r| r.get::<_, bool>(0))
            .unwrap_err();
        assert!(e.to_string().contains("LIKE or GLOB pattern too complex"), "{e}");
    }

    /// Review of 0.1.3: the table rebuilt (another collation) between the checks and the snapshot
    #[tokio::test(flavor = "multi_thread")]
    async fn review_a_schema_change_before_the_snapshot_is_noticed() {
        let fx = fixture(
            "CREATE TABLE c(id INTEGER PRIMARY KEY, k TEXT, v INTEGER); \
             WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 4000) \
             INSERT INTO c SELECT i, CASE i % 4 WHEN 0 THEN 'a' WHEN 1 THEN 'A' WHEN 2 THEN 'b' ELSE 'B' END, i FROM n;",
            |_| {},
        );
        let (path, cfg) = (fx.path.clone(), fx.readers.cfg().clone());
        let done = Arc::new(AtomicBool::new(false));
        let d2 = done.clone();
        HOOKS.lock().insert(
            fx.path.clone(),
            Arc::new(move |p| {
                if p == Point::Snapshot && !d2.swap(true, Ordering::Relaxed) {
                    let w = Conn::open_writer(&path, &cfg).unwrap();
                    w.c.execute_batch(
                        "BEGIN; CREATE TABLE c2(id INTEGER PRIMARY KEY, k TEXT COLLATE NOCASE, v INTEGER); \
                         INSERT INTO c2 SELECT * FROM c; DROP TABLE c; ALTER TABLE c2 RENAME TO c; COMMIT;",
                    )
                    .unwrap();
                }
                false
            }),
        );
        let q = "SELECT k, COUNT(*), SUM(v) FROM c WHERE v > 0 GROUP BY k";
        let (split, _) = fx.both(q, "[]").await;
        HOOKS.lock().remove(&fx.path);
        let (_, one) = fx.both(q, "[]").await;
        assert_eq!(split, one, "{q}");
    }

    /// Not the shape at all: not even considered (the usual path, nothing counted but the normal read)
    #[tokio::test(flavor = "multi_thread")]
    async fn other_statements_are_not_considered() {
        let fx = fixture(SCHEMA, |_| {});
        fill(&fx.w, 3, 500);
        for sql in [
            "SELECT AVG(v) FROM t",
            "SELECT TOTAL(v) FROM t",
            "SELECT COUNT(DISTINCT g) FROM t",
            "SELECT group_concat(s) FROM t",
            "SELECT g, COUNT(*) OVER () FROM t",
            "WITH x AS (SELECT * FROM t) SELECT COUNT(*) FROM x",
            "SELECT COUNT(*) FROM t a JOIN t b ON a.id = b.id",
            "SELECT COUNT(*) FROM t, t AS b",
            "SELECT COUNT(*) FROM (SELECT * FROM t)",
            "SELECT COUNT(*) FROM t WHERE g IN (SELECT g FROM t)",
            "SELECT COUNT(*) FROM main.t WHERE v > 0",
            "SELECT COUNT(*) FROM t NOT INDEXED WHERE v > 0",
            "SELECT g, s, COUNT(*) FROM t GROUP BY g",
            "SELECT COUNT(*) + 1 FROM t WHERE v > 0",
            "SELECT g * 2, COUNT(*) FROM t GROUP BY g",
            "SELECT g, COUNT(*) FROM t GROUP BY g + 0",
            "SELECT g, COUNT(*) FROM t GROUP BY 1",
            "SELECT SUM(v * 1.5) FROM t",
            "SELECT g AS k, COUNT(*) FROM t WHERE k > 0 GROUP BY g",
            "SELECT COUNT(*) FROM t WHERE v > 0 UNION SELECT 1",
            "SELECT DISTINCT g FROM t",
            "SELECT g FROM t GROUP BY g",
            "SELECT g, COUNT(*) FROM t GROUP BY g ORDER BY avg(v)",
            "SELECT g, COUNT(*) FROM t GROUP BY g LIMIT (SELECT 1)",
            "SELECT MIN(v, 3), COUNT(*) FROM t",
            "SELECT SUM(v) FILTER (WHERE g > 0) FROM t",
            "SELECT COUNT(*) FROM t WHERE v = :xsqld_lo",
            // names in HAVING / ORDER BY other than grouping columns and aliases (review of 0.1.3)
            "SELECT g, COUNT(*) FROM t GROUP BY g HAVING MAX(s) > 'a' AND v > 0",
            "SELECT g, COUNT(*) FROM t GROUP BY g ORDER BY rowid DESC",
            "SELECT g, COUNT(*) FROM t GROUP BY g HAVING \"v\" > 998",
            "SELECT COUNT(*) FROM t WHERE v > CURRENT_TIMESTAMP",
        ] {
            let before = fx.metrics.parallel_considered.load(Ordering::Relaxed);
            let (split, one) = fx.both(sql, "[]").await;
            assert_eq!(split, one, "{sql}");
            assert_eq!(
                fx.metrics.parallel_considered.load(Ordering::Relaxed),
                before,
                "considered: {sql}"
            );
        }
    }

    /// Empty table, one row, a few rows with large gaps, negative rowids only
    #[tokio::test(flavor = "multi_thread")]
    async fn tiny_and_odd_tables() {
        let fx = fixture(SCHEMA, |_| {});
        let q = "SELECT g, COUNT(*), SUM(v), MIN(s) FROM t WHERE v IS NOT NULL OR v IS NULL GROUP BY g";
        fx.same_unsplit(q, "[]", Fallback::Small).await;
        fx.w.c
            .execute("INSERT INTO t(id, g, v, s) VALUES (5, 1, 2, 'a')", [])
            .unwrap();
        fx.same_split(q, "[]").await;
        fx.w.c
            .execute_batch(
                "INSERT INTO t(id, g, v, s) VALUES (-9223372036854775808, 2, 3, 'b'), (9223372036854775807, 1, 4, 'c'), \
                 (-7, NULL, NULL, NULL);",
            )
            .unwrap();
        fx.same_split(q, "[]").await;
        fx.same_split("SELECT COUNT(*), SUM(v) FROM t WHERE g IS NOT NULL", "[]")
            .await;
        fx.w.c.execute("DELETE FROM t WHERE id > 0", []).unwrap();
        fx.same_split(q, "[]").await;
    }

    /// Text that is not valid UTF-8 keeps its bytes: two different invalid texts stay two groups
    #[tokio::test(flavor = "multi_thread")]
    async fn group_keys_keep_their_bytes() {
        let fx = fixture(SCHEMA, |_| {});
        fx.w.c
            .execute_batch(
                "WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 400) \
                 INSERT INTO t(id, s, v, r) SELECT i, CAST(CASE i % 3 WHEN 0 THEN X'FF' WHEN 1 THEN X'FE' ELSE X'41' END AS TEXT), i, \
                 CASE i % 2 WHEN 0 THEN 0.0 ELSE -0.0 END FROM n;",
            )
            .unwrap();
        fx.same_split("SELECT s, COUNT(*), MAX(s) FROM t GROUP BY s", "[]")
            .await;
        fx.same_split("SELECT r, COUNT(*) FROM t GROUP BY r", "[]").await;
        fx.same_split("SELECT MIN(r), MAX(r) FROM t WHERE v > 0", "[]").await;
    }

    /// One range failing (an error only some rows raise): all stop, and one connection reports the error
    #[tokio::test(flavor = "multi_thread")]
    async fn a_failing_range_stops_all_and_one_connection_answers() {
        let fx = fixture(SCHEMA, |_| {});
        fill(&fx.w, 11, 4000);
        let id: i64 =
            fx.w.c
                .query_row("SELECT id FROM t ORDER BY id LIMIT 1 OFFSET 3500", [], |r| r.get(0))
                .unwrap();
        let sql = format!("SELECT SUM(CASE WHEN id = {id} THEN abs(-9223372036854775807 - 1) ELSE 1 END) FROM t");
        fx.same_unsplit(&sql, "[]", Fallback::Error).await;
        let (split, _) = fx.both(&sql, "[]").await;
        assert_eq!(split["error"]["message"], "SQLite error: integer overflow");
    }

    /// A range that fails at its first row stops the others at once: the statement ends long before the other
    /// ranges could have read their rows
    #[tokio::test(flavor = "multi_thread")]
    async fn the_other_ranges_stop_when_one_fails() {
        let fx = fixture(SCHEMA, |_| {});
        fx.w.c
            .execute_batch(
                "WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 400000) \
                 INSERT INTO t(id, g, s, v) SELECT i, i % 50, 'x' || (i % 7), i FROM n;",
            )
            .unwrap();
        // the same work without the error, split (no GROUP BY: each row is summed as it is read, so an error comes at
        // the row that raises it, not after sorting)
        let slow = "SELECT SUM(length(printf('%.100c', s))) FROM t WHERE g >= 0";
        let started = Instant::now();
        fx.same_split(slow, "[]").await;
        let whole = started.elapsed();
        // the first row of the first range raises an error; one connection meets it at once too
        let failing = "SELECT SUM(CASE WHEN id = 1 THEN abs(-9223372036854775807 - 1) ELSE length(printf('%.100c', s)) END) FROM t WHERE g >= 0";
        let stmt = serde_json::json!({ "sql": failing });
        let started = Instant::now();
        let out = fx.run(&format!(r#"[{{"type":"execute","stmt":{stmt}}}]"#)).await;
        let took = started.elapsed();
        assert_eq!(out[0]["error"]["message"], "SQLite error: integer overflow");
        assert_eq!(fx.fallbacks(Fallback::Error), 1);
        assert!(
            took * 4 < whole,
            "{took:?} with an error at once, {whole:?} for the whole work"
        );
        // often enough to meet a range that had not started when another failed
        for _ in 0..30 {
            let started = Instant::now();
            let out = fx.run(&format!(r#"[{{"type":"execute","stmt":{stmt}}}]"#)).await;
            assert_eq!(out[0]["error"]["message"], "SQLite error: integer overflow");
            assert!(started.elapsed() * 4 < whole, "{:?}", started.elapsed());
        }
        // and the statements after it on the same connections are not stopped
        fx.same_split(slow, "[]").await;
    }

    /// The deadline ends a split statement as it ends one on one connection
    #[tokio::test(flavor = "multi_thread")]
    async fn deadline_interrupts_every_range() {
        let fx = fixture(SCHEMA, |_| {});
        fx.w.c
            .execute_batch(
                "WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 300000) \
                 INSERT INTO t(id, g, s, v) SELECT i, i % 50, 'x' || (i % 7), i FROM n;",
            )
            .unwrap();
        let sql = r#"[{"type":"execute","stmt":{"sql":"SELECT s, g, COUNT(*), SUM(v) FROM t WHERE length(s) > 0 GROUP BY s, g"}}]"#;
        let started = Instant::now();
        let out = fx.run_until(sql, Instant::now() + Duration::from_millis(30)).await;
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "took {:?}",
            started.elapsed()
        );
        assert_eq!(out[0]["error"]["code"], "SQLITE_INTERRUPT", "{out}");
        assert_eq!(fx.runs(), 0);
        assert_eq!(fx.fallbacks(Fallback::Error), 1);
        // the connections are fine afterwards
        fx.same_split("SELECT s, COUNT(*) FROM t WHERE g > 3 GROUP BY s", "[]")
            .await;
    }

    /// No additional connection free right now: one connection, without waiting
    #[tokio::test(flavor = "multi_thread")]
    async fn a_busy_pool_means_one_connection_without_waiting() {
        let fx = fixture(SCHEMA, |c| {
            c.read_pool_size = 4;
            c.parallel_pool_percent = 50;
        });
        fill(&fx.w, 5, 2000);
        let q = "SELECT g, COUNT(*) FROM t WHERE v > 0 GROUP BY g";
        // the share of split statements taken: 2 of 4
        let held = fx
            .readers
            .lease_extra(2, Instant::now() + Duration::from_secs(5))
            .unwrap();
        assert_eq!(held.conns.len(), 2);
        let started = Instant::now();
        fx.same_unsplit(q, "[]", Fallback::Pool).await;
        assert!(started.elapsed() < Duration::from_secs(1));
        drop(held);
        // the pool itself full: 3 permits held elsewhere, the read takes the 4th
        let pool = fx.readers.lease_extra(3, Instant::now() + Duration::from_secs(5));
        assert_eq!(pool.as_ref().map(|l| l.conns.len()), Some(2), "capped at the share");
        fx.same_unsplit(q, "[]", Fallback::Pool).await;
        drop(pool);
        fx.same_split(q, "[]").await;
        // at most the share: 1 + 2 connections
        assert_eq!(fx.metrics.parallel_ranges.load(Ordering::Relaxed), 3);
    }

    /// Under writes that keep COUNT(*) and SUM(v) of `inv` constant, every split result shows those constants: the
    /// ranges read one snapshot. Without the shared snapshot (each connection starting its own read transaction) the
    /// same test sees mixtures.
    #[tokio::test(flavor = "multi_thread")]
    async fn ranges_read_one_snapshot_under_writes() {
        for apart in [false, true] {
            let fx = fixture(
                "CREATE TABLE inv(id INTEGER PRIMARY KEY, g INTEGER NOT NULL, v INTEGER NOT NULL);",
                |_| {},
            );
            fx.w.c
                .execute_batch(
                    "WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 20000) \
                     INSERT INTO inv SELECT i, i % 10, 0 FROM n;",
                )
                .unwrap();
            if apart {
                HOOKS.lock().insert(fx.path.clone(), Arc::new(|p| p == Point::Snapshot));
            }
            let stop = Arc::new(AtomicBool::new(false));
            let writer = {
                let (stop, path, cfg) = (stop.clone(), fx.path.clone(), fx.readers.cfg().clone());
                std::thread::spawn(move || {
                    let w = Conn::open_writer(&path, &cfg).unwrap();
                    let mut r = Rng(99);
                    let mut next = 1_000_000i64;
                    let mut commits = 0u64;
                    while !stop.load(Ordering::Relaxed) {
                        w.c.execute_batch("BEGIN IMMEDIATE").unwrap();
                        // move an amount between two rows far apart (different ranges; ids the replacing below does
                        // not reach), and replace the oldest row by a new one with its values
                        let k = 1 + r.below(100) as i64;
                        let (a, b) = (10_001 + r.below(5_000) as i64, 15_001 + r.below(5_000) as i64);
                        w.c.execute("UPDATE inv SET v = v + ?1 WHERE id = ?2", [k, a]).unwrap();
                        w.c.execute("UPDATE inv SET v = v - ?1 WHERE id = ?2", [k, b]).unwrap();
                        let (g, v): (i64, i64) =
                            w.c.query_row(
                                "DELETE FROM inv WHERE id = (SELECT MIN(id) FROM inv) RETURNING g, v",
                                [],
                                |r| Ok((r.get(0)?, r.get(1)?)),
                            )
                            .unwrap();
                        next += 1;
                        w.c.execute("INSERT INTO inv VALUES (?1, ?2, ?3)", [next, g, v])
                            .unwrap();
                        w.c.execute_batch("COMMIT").unwrap();
                        commits += 1;
                        if commits.is_multiple_of(50) {
                            let _ = w.c.query_row("PRAGMA wal_checkpoint(PASSIVE)", [], |_| Ok(()));
                        }
                    }
                })
            };
            let mut mixed = 0;
            let before = fx.runs();
            for _ in 0..150 {
                let out = fx
                    .run(r#"[{"type":"execute","stmt":{"sql":"SELECT COUNT(*), SUM(v), MIN(g) FROM inv WHERE g >= 0"}}]"#)
                    .await;
                let row = &out[0]["response"]["result"]["rows"][0];
                if row[0]["value"] != "20000" || row[1]["value"] != "0" {
                    mixed += 1;
                }
            }
            stop.store(true, Ordering::Relaxed);
            writer.join().unwrap();
            HOOKS.lock().remove(&fx.path);
            assert!(fx.runs() >= before + 140, "split {} of 150", fx.runs() - before);
            if apart {
                assert!(mixed > 0, "the test does not see mixed snapshots");
            } else {
                assert_eq!(mixed, 0, "{mixed} of 150 split results mixed snapshots");
            }
        }
    }

    /// Right after a start on a checkpointed database nothing has been written to the WAL: no snapshot to take. The
    /// connections still read one state; a write landing between the first and the others is noticed.
    #[tokio::test(flavor = "multi_thread")]
    async fn untouched_wal_is_one_state_unless_a_write_comes_between() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("data");
        let mut cfg = Config::for_tests(dir.path());
        cfg.parallel_min_rows = 1;
        let cfg = Arc::new(cfg);
        {
            let w = Conn::open_writer(&path, &cfg).unwrap();
            w.c.execute_batch(SCHEMA).unwrap();
            fill(&w, 21, 3000);
        }
        // closing the last connection checkpoints and removes the WAL; this writer opens it anew and writes nothing
        let w = Conn::open_writer(&path, &cfg).unwrap();
        let metrics = Arc::new(DbMetrics::default());
        let fx = Fx {
            readers: Arc::new(Readers::new(path.clone(), cfg.clone(), ReadGate::new(metrics.clone()))),
            _dir: dir,
            w,
            metrics,
            path: path.clone(),
        };
        let q = "SELECT g, COUNT(*), SUM(v) FROM t WHERE v > -500 GROUP BY g";
        let untouched = UNTOUCHED.load(Ordering::Relaxed);
        fx.same_split(q, "[]").await;
        assert!(
            UNTOUCHED.load(Ordering::Relaxed) > untouched,
            "the untouched-WAL rule was not used"
        );
        // a write between the first connection and the others
        let (p2, c2) = (path.clone(), cfg.clone());
        HOOKS.lock().insert(
            path.clone(),
            Arc::new(move |p| {
                if p == Point::UntouchedFirst {
                    let w = Conn::open_writer(&p2, &c2).unwrap();
                    w.c.execute("INSERT INTO t(g, v) VALUES (1, 1)", []).unwrap();
                }
                false
            }),
        );
        let fresh = tempfile::tempdir().unwrap();
        let _ = fresh;
        // the WAL now has frames: open a new state by restarting nothing; check the rule on a database reopened again
        HOOKS.lock().remove(&path);
        drop(fx);
        let dir2 = tempfile::tempdir().unwrap();
        let path2 = dir2.path().join("data");
        let mut cfg2 = Config::for_tests(dir2.path());
        cfg2.parallel_min_rows = 1;
        let cfg2 = Arc::new(cfg2);
        {
            let w = Conn::open_writer(&path2, &cfg2).unwrap();
            w.c.execute_batch(SCHEMA).unwrap();
            fill(&w, 22, 3000);
        }
        let w = Conn::open_writer(&path2, &cfg2).unwrap();
        let metrics = Arc::new(DbMetrics::default());
        let fx = Fx {
            readers: Arc::new(Readers::new(
                path2.clone(),
                cfg2.clone(),
                ReadGate::new(metrics.clone()),
            )),
            _dir: dir2,
            w,
            metrics,
            path: path2.clone(),
        };
        let (p3, c3) = (path2.clone(), cfg2.clone());
        HOOKS.lock().insert(
            path2.clone(),
            Arc::new(move |p| {
                if p == Point::UntouchedFirst {
                    let w = Conn::open_writer(&p3, &c3).unwrap();
                    w.c.execute("INSERT INTO t(g, v) VALUES (1, 1)", []).unwrap();
                }
                false
            }),
        );
        fx.same_unsplit(q, "[]", Fallback::Snapshot).await;
        HOOKS.lock().remove(&path2);
    }

    /// The read gate sees a split statement as the one read it is: a RESTART waiting for reads waits for it and gets
    /// through once it ends, and ranges starting while the gate is closed do not wait for it (that would deadlock)
    #[tokio::test(flavor = "multi_thread")]
    async fn the_read_gate_sees_one_read_and_never_blocks_the_ranges() {
        let fx = fixture(SCHEMA, |_| {});
        fx.w.c
            .execute_batch(
                "WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 100000) \
                 INSERT INTO t(id, g, s, v) SELECT i, i % 50, 'x' || (i % 7), i FROM n;",
            )
            .unwrap();
        let gate = fx.readers.gate.clone();
        let seen = Arc::new(parking_lot::Mutex::new(None));
        let (g2, s2) = (gate.clone(), seen.clone());
        HOOKS.lock().insert(
            fx.path.clone(),
            Arc::new(move |p| {
                if p == Point::Ranges {
                    // the write thread closes the gate now (a RESTART after a failed one) and waits for the reads
                    let g3 = g2.clone();
                    let s3 = s2.clone();
                    let active = g2.active();
                    std::thread::spawn(move || {
                        let closed = g3.close();
                        let started = Instant::now();
                        let drained = closed.drain(Duration::from_secs(20));
                        *s3.lock() = Some((active, drained, started.elapsed()));
                    });
                    std::thread::sleep(Duration::from_millis(50));
                }
                false
            }),
        );
        fx.same_split(
            "SELECT s, g, COUNT(*), SUM(v) FROM t WHERE length(s) > 0 GROUP BY s, g",
            "[]",
        )
        .await;
        HOOKS.lock().remove(&fx.path);
        let deadline = Instant::now() + Duration::from_secs(20);
        while seen.lock().is_none() && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(10));
        }
        let (active, drained, _) = seen.lock().unwrap();
        assert_eq!(active, 1, "the split statement is one read for the gate");
        assert!(drained, "the RESTART's wait for reads ended once the statement did");
        assert_eq!(gate.active(), 0);
    }

    /// The estimate of a table's rows from its rowid b-tree: dense rowids, a few rows far away, evenly sparse rowids
    /// (timestamps as ids), clusters, an empty table
    #[test]
    fn rows_are_estimated_from_the_rowid_btree() {
        let dir = tempfile::tempdir().unwrap();
        let c = Connection::open(dir.path().join("x")).unwrap();
        let est = |sql: &str| -> (i128, i128) {
            c.execute_batch(&format!(
                "DROP TABLE IF EXISTS t; CREATE TABLE t(id INTEGER PRIMARY KEY); {sql}"
            ))
            .unwrap();
            let n: i64 = c.query_row("SELECT COUNT(*) FROM t", [], |r| r.get(0)).unwrap();
            let b = rowid_bounds(&c, "t", "rowid").unwrap();
            let e = b.map_or(0, |(lo, hi)| estimate_rows(&c, "t", "rowid", lo, hi).unwrap());
            (n as i128, e)
        };
        let series = |n: u64, step: u64, from: i64| {
            format!(
                "WITH RECURSIVE n(i) AS (SELECT 0 UNION ALL SELECT i + 1 FROM n WHERE i < {n} - 1) \
                 INSERT INTO t SELECT {from} + i * {step} FROM n;"
            )
        };
        let close = |(n, e): (i128, i128), tolerance: f64| {
            assert!(
                ((e - n).abs() as f64) <= n as f64 * tolerance,
                "{e} estimated for {n} rows"
            );
        };
        close(est(&series(200_000, 1, 1)), 0.0);
        close(
            est(&(series(40_000, 1, 1) + "INSERT INTO t VALUES (2000000000), (9000000000000);")),
            0.01,
        );
        // millisecond timestamps as ids: 3000 rows over 3·10⁸ rowids
        close(est(&series(3000, 100_000, 1_700_000_000_000)), 0.1);
        close(
            est(&(series(5000, 1, 1) + &series(5000, 7, 1_000_000) + &series(5000, 1, -9_000_000_000))),
            0.1,
        );
        close(est(&series(1, 1, 5)), 0.0);
        close(est(&series(70, 1_000_000_000, -9_000_000_000_000_000_000)), 0.0);
        assert_eq!(est(""), (0, 0));
    }

    /// A table of a few thousand rows whose rowids span far more than --parallel-min-rows runs on one connection, and
    /// the estimate is made once per table for a while, not per statement
    #[tokio::test(flavor = "multi_thread")]
    async fn sparse_rowids_do_not_make_a_small_table_large() {
        let fx = fixture(SCHEMA, |c| c.parallel_min_rows = 1_000_000);
        fx.w.c
            .execute_batch(
                "WITH RECURSIVE n(i) AS (SELECT 0 UNION ALL SELECT i + 1 FROM n WHERE i < 2999) \
                 INSERT INTO t(id, g, v) SELECT 1700000000000 + i * 100000, i % 7, i FROM n;",
            )
            .unwrap();
        let q = "SELECT g, COUNT(*), SUM(v) FROM t WHERE v > 10 GROUP BY g";
        fx.same_unsplit(q, "[]", Fallback::Small).await;
        let estimates = std::sync::atomic::AtomicUsize::new(0);
        for _ in 0..3 {
            fx.readers.table_rows("T", || {
                estimates.fetch_add(1, Ordering::Relaxed);
                Some(0)
            });
        }
        assert_eq!(
            estimates.load(Ordering::Relaxed),
            0,
            "estimated again within the time it is kept"
        );
        // a table that does hold a million rows is still split
        fx.w.c
            .execute_batch(
                "WITH RECURSIVE n(i) AS (SELECT 0 UNION ALL SELECT i + 1 FROM n WHERE i < 999999) \
                 INSERT INTO t(id, g, v) SELECT 1 + i, i % 7, i FROM n;",
            )
            .unwrap();
        let fresh = fixture(SCHEMA, |c| c.parallel_min_rows = 1_000_000);
        fresh
            .w
            .c
            .execute_batch(
                "WITH RECURSIVE n(i) AS (SELECT 0 UNION ALL SELECT i + 1 FROM n WHERE i < 1099999) \
                 INSERT INTO t(id, g, v) SELECT 1 + i * 3, i % 7, i FROM n;",
            )
            .unwrap();
        fresh.same_split(q, "[]").await;
    }

    #[test]
    fn cuts_follow_the_rows_not_the_rowid_values() {
        let dir = tempfile::tempdir().unwrap();
        let c = Connection::open(dir.path().join("x")).unwrap();
        c.execute_batch(
            "CREATE TABLE t(id INTEGER PRIMARY KEY); \
             WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 40000) INSERT INTO t SELECT i FROM n; \
             INSERT INTO t VALUES (2000000000), (2000000001);",
        )
        .unwrap();
        let r = cuts(&c, "t", "rowid", 1, 2_000_000_001, 4).unwrap();
        assert_eq!(r.first().unwrap().0, 1);
        assert_eq!(r.last().unwrap().1, 2_000_000_001);
        for w in r.windows(2) {
            assert_eq!(w[0].1 + 1, w[1].0, "contiguous");
        }
        let count = |(a, b): (i64, i64)| -> i64 {
            c.query_row("SELECT COUNT(*) FROM t WHERE id BETWEEN ?1 AND ?2", [a, b], |r| {
                r.get(0)
            })
            .unwrap()
        };
        let counts: Vec<i64> = r.iter().map(|&x| count(x)).collect();
        assert!(counts.iter().all(|&n| (9000..=11000).contains(&n)), "{counts:?}");
        // dense: even cuts; a span smaller than the ranges asked for: fewer ranges
        assert_eq!(
            cuts(&c, "t", "rowid", 1, 40000, 4).unwrap(),
            vec![(1, 10000), (10001, 20000), (20001, 30000), (30001, 40000)]
        );
        assert_eq!(cuts(&c, "t", "rowid", 5, 6, 4).unwrap(), vec![(5, 5), (6, 6)]);
    }

    /// Statements of the shape: split, and the result equals one connection's, for several data sets
    #[tokio::test(flavor = "multi_thread")]
    async fn split_results_equal_one_connection() {
        let split_ok: &[(&str, &str)] = &[
            (
                "SELECT COUNT(*) FROM t WHERE v > ?",
                r#"[{"type":"integer","value":"0"}]"#,
            ),
            ("SELECT count(*) FROM t WHERE 1", "[]"),
            (
                "SELECT g, COUNT(*), SUM(v), MIN(v), MAX(v), COUNT(w) FROM t GROUP BY g",
                "[]",
            ),
            (
                "SELECT s, COUNT(*) AS c FROM t WHERE v IS NOT NULL GROUP BY s HAVING COUNT(*) > 2 ORDER BY c DESC, s LIMIT 5",
                "[]",
            ),
            (
                "SELECT g, s, SUM(v) FROM t GROUP BY g, s ORDER BY 3 DESC, 1, 2 LIMIT ? OFFSET 1",
                r#"[{"type":"integer","value":"4"}]"#,
            ),
            (
                "SELECT MAX(s), MIN(s), COUNT(s) FROM t WHERE g = ?",
                r#"[{"type":"integer","value":"1"}]"#,
            ),
            ("SELECT COUNT(*), SUM(CASE WHEN s = 'a' THEN 1 ELSE 0 END) FROM t", "[]"),
            ("SELECT g, MAX(r), MIN(r) FROM t GROUP BY g", "[]"),
            (
                "SELECT g AS grp, SUM(v) total FROM t GROUP BY g HAVING SUM(v) > 0 AND g IS NOT NULL",
                "[]",
            ),
            (
                "SELECT t.g, COUNT(*) FROM t WHERE t.s LIKE 'a%' GROUP BY t.g ORDER BY COUNT(*) DESC",
                "[]",
            ),
            ("select x.s, count(*) from t as x group by x.s order by 2", "[]"),
            ("SELECT \"g\", count(*) FROM \"t\" /* c */ GROUP BY [g] -- end", "[]"),
            ("SELECT COUNT(*) FROM t WHERE rowid % 3 = 0", "[]"),
            (
                "SELECT SUM(v), COUNT(*) FROM t WHERE v BETWEEN ?2 AND ?1",
                r#"[{"type":"integer","value":"500"},{"type":"integer","value":"-500"}]"#,
            ),
            ("SELECT COUNT(*) FROM t WHERE s = :s AND g <> :g", "[]"),
            ("SELECT g, COUNT(*) FROM t GROUP BY g LIMIT 3", "[]"),
            ("SELECT COUNT(b), MAX(s) FROM t WHERE s IS NOT NULL;", "[]"),
        ];
        // equal whichever way SQLite plans them (a constant-false WHERE is planned without a scan)
        let either: &[&str] = &[
            "SELECT g, COUNT(*) FROM t WHERE 0 GROUP BY g",
            "SELECT COUNT(*), SUM(v), MIN(s) FROM t WHERE 0",
        ];
        for seed in 1..=4u64 {
            let fx = fixture(SCHEMA, |_| {});
            fill(&fx.w, seed, 3000);
            for (sql, args) in split_ok {
                if sql.contains(":s") {
                    let stmt = serde_json::json!({"sql": sql, "named_args": [{"name":"s","value":{"type":"text","value":"a"}},{"name":":g","value":{"type":"integer","value":"1"}}]});
                    let before = fx.runs();
                    let split = fx.run(&format!(r#"[{{"type":"execute","stmt":{stmt}}}]"#)).await;
                    let one = fx
                        .run(&format!(
                            r#"[{{"type":"batch","batch":{{"steps":[{{"stmt":{{"sql":"BEGIN"}}}},{{"stmt":{stmt}}},{{"stmt":{{"sql":"COMMIT"}}}}]}}}}]"#
                        ))
                        .await;
                    assert_eq!(
                        split[0]["response"]["result"],
                        one[0]["response"]["result"]["step_results"][1]
                    );
                    assert_eq!(fx.runs(), before + 1);
                    continue;
                }
                fx.same_split(sql, args).await;
            }
            for sql in either {
                let (split, one) = fx.both(sql, "[]").await;
                assert_eq!(split, one, "{sql}");
            }
        }
    }
}
