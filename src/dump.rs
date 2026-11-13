//! `GET /dump` (SQL text of the whole database, written from a snapshot copy so writes and checkpoints go on) and
//! building a database from such a dump (`POST /v1/namespaces/<ns>/create {"dump_url"}`).
//!
//! The output is byte for byte what libsql-server 0.24.32 writes (`connection/dump/exporter.rs`, a port of the
//! sqlite3 shell's `.dump`), except where sqld fails: names containing `"` are quoted correctly, text that is not
//! UTF-8 is written as `CAST(X'…' AS TEXT)` (the same bytes back), infinities are written as `1e999`.

use std::io::{self, BufRead, Read, Write};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use rusqlite::backup::{Backup, StepResult};
use rusqlite::types::ValueRef;

use crate::config::Config;
use crate::conn::Conn;
use crate::reader::ReadGate;
use crate::sql::Completer;

/// A copy of a database in `--db-path/xsqld-tmp`, removed on drop. Made with the online backup API in one read
/// transaction (as fast as copying the file); writing SQL text to a client, which may take its time, then reads the
/// copy, so the database's WAL can be checkpointed meanwhile.
pub struct Snapshot {
    pub conn: rusqlite::Connection,
    path: PathBuf,
}

impl Snapshot {
    /// The copy is one read transaction, so it passes the database's read gate like a read request
    pub fn take(db: &Path, cfg: &Config, gate: &ReadGate) -> anyhow::Result<Snapshot> {
        let Some(_pass) = gate.enter(std::time::Instant::now() + cfg.dump_max()) else {
            anyhow::bail!("dump took longer than --dump-max-ms");
        };
        let dir = cfg.tmp_dir();
        std::fs::create_dir_all(&dir)?;
        let mut id = [0u8; 8];
        getrandom::fill(&mut id).map_err(|e| anyhow::anyhow!("random name: {e}"))?;
        let name: String = id.iter().map(|b| format!("{b:02x}")).collect();
        let path = dir.join(format!("dump-{name}"));
        // built in place so that an error on the way removes the file
        let mut snap = Snapshot {
            conn: rusqlite::Connection::open(&path)?,
            path,
        };
        snap.conn
            .execute_batch("PRAGMA journal_mode = OFF; PRAGMA synchronous = OFF;")?;
        {
            let src = Conn::open_reader(db, cfg)?;
            let backup = Backup::new(&src.c, &mut snap.conn)?;
            let mut tries = 0;
            loop {
                match backup.step(-1)? {
                    StepResult::Done => break,
                    StepResult::More => {}
                    _ if tries < 50 => {
                        tries += 1;
                        std::thread::sleep(Duration::from_millis(20));
                    }
                    other => anyhow::bail!("could not copy the database: {other:?}"),
                }
            }
        }
        Ok(snap)
    }
}

impl Drop for Snapshot {
    fn drop(&mut self) {
        for suffix in ["", "-journal", "-wal", "-shm"] {
            let mut p = self.path.as_os_str().to_owned();
            p.push(suffix);
            let _ = std::fs::remove_file(PathBuf::from(p));
        }
    }
}

pub fn export(c: &rusqlite::Connection, out: &mut impl Write, preserve_rowids: bool) -> anyhow::Result<()> {
    c.execute_batch("BEGIN")?;
    let r = export_in_tx(c, out, preserve_rowids);
    let _ = c.execute_batch("ROLLBACK");
    r
}

fn export_in_tx(c: &rusqlite::Connection, out: &mut impl Write, preserve_rowids: bool) -> anyhow::Result<()> {
    out.write_all(b"PRAGMA foreign_keys=OFF;\nBEGIN TRANSACTION;\n")?;
    let mut writable_schema = false;
    let mut tables = c.prepare(
        "SELECT name, type, sql FROM sqlite_schema AS o WHERE type=='table' AND sql NOT NULL \
         ORDER BY tbl_name='sqlite_sequence', rowid",
    )?;
    let mut rows = tables.query([])?;
    while let Some(row) = rows.next()? {
        let name: String = row.get(0)?;
        let ty: String = row.get(1)?;
        let sql: String = row.get(2)?;
        if name == "sqlite_sequence" {
            out.write_all(b"DELETE FROM sqlite_sequence;\n")?;
        } else if name.starts_with("sqlite_stat") {
            out.write_all(b"ANALYZE sqlite_schema;\n")?;
        } else if name.starts_with("sqlite_") {
            continue;
        } else if sql.starts_with("CREATE VIRTUAL TABLE") {
            if !writable_schema {
                out.write_all(b"PRAGMA writable_schema=ON;\n")?;
                writable_schema = true;
            }
            writeln!(
                out,
                "INSERT INTO sqlite_schema(type,name,tbl_name,rootpage,sql) VALUES('table',{0},{0},0,{1});",
                Escaped(name.as_bytes()),
                Escaped(sql.as_bytes())
            )?;
            continue;
        } else if let Some(rest) = sql.strip_prefix("CREATE TABLE") {
            writeln!(out, "CREATE TABLE IF NOT EXISTS {};", rest.get(1..).unwrap_or_default())?;
        } else {
            writeln!(out, "{sql};")?;
        }
        if ty == "table" {
            dump_rows(c, out, &name, preserve_rowids)?;
        }
    }
    drop(rows);
    let mut rest =
        c.prepare("SELECT sql FROM sqlite_schema AS o WHERE sql NOT NULL AND type IN ('index','trigger','view')")?;
    let mut rows = rest.query([])?;
    while let Some(row) = rows.next()? {
        let sql: String = row.get(0)?;
        writeln!(out, "{sql};")?;
    }
    if writable_schema {
        out.write_all(b"PRAGMA writable_schema=OFF;\n")?;
    }
    out.write_all(b"COMMIT;\n")?;
    Ok(())
}

fn dump_rows(c: &rusqlite::Connection, out: &mut impl Write, table: &str, preserve_rowids: bool) -> anyhow::Result<()> {
    let (rowid, cols) = table_columns(c, table, preserve_rowids)?;
    let mut insert = format!("INSERT INTO {}", Quoted(table));
    let mut select = String::from("SELECT ");
    if let Some(r) = rowid {
        insert.push('(');
        insert.push_str(r);
        for col in &cols {
            insert.push_str(&format!(",{}", Quoted(col)));
        }
        insert.push(')');
        select.push_str(r);
        select.push(',');
    }
    insert.push_str(" VALUES(");
    // select list quoted with SQL rules (sqld writes names with `"` in them unescaped and fails)
    select.push_str(&cols.iter().map(|c| quote_ident(c)).collect::<Vec<_>>().join(","));
    select.push_str(&format!(" FROM {}", quote_ident(table)));
    let ncols = cols.len() + rowid.is_some() as usize;
    let mut st = c.prepare(&select)?;
    let mut rows = st.query([])?;
    while let Some(row) = rows.next()? {
        out.write_all(insert.as_bytes())?;
        for i in 0..ncols {
            if i > 0 {
                out.write_all(b",")?;
            }
            write_value(out, row.get_ref(i)?)?;
        }
        out.write_all(b");\n")?;
    }
    Ok(())
}

/// Columns of a table, plus the rowid column to keep, with sqld's rules (including its quirk: an `INTEGER` primary
/// key that is not the rowid turns rowid preservation on)
fn table_columns(
    c: &rusqlite::Connection,
    table: &str,
    mut preserve: bool,
) -> anyhow::Result<(Option<&'static str>, Vec<String>)> {
    let mut cols = Vec::new();
    let mut pks = 0;
    let mut integer_pk = false;
    let mut st = c.prepare("SELECT name, type, pk FROM pragma_table_info(?1)")?;
    let mut rows = st.query([table])?;
    while let Some(row) = rows.next()? {
        cols.push(row.get::<_, String>(0)?);
        if row.get::<_, i64>(2)? != 0 {
            pks += 1;
            integer_pk = pks == 1 && matches!(row.get_ref(1)?, ValueRef::Text(b"INTEGER"));
        }
    }
    drop(rows);
    if integer_pk {
        let has_pk_index: bool = c.query_row(
            "SELECT EXISTS(SELECT 1 FROM pragma_index_list(?1) WHERE origin='pk')",
            [table],
            |r| r.get(0),
        )?;
        preserve |= has_pk_index;
    }
    let mut rowid = None;
    if preserve {
        // "_row_id_" (sic) as in sqld: it never names the rowid, so only rowid and oid are ever used
        for name in ["rowid", "_row_id_", "oid"] {
            if !cols.iter().any(|c| c == name)
                && c.prepare(&format!("SELECT {name} FROM {} LIMIT 0", quote_ident(table)))
                    .is_ok()
            {
                rowid = Some(name);
                break;
            }
        }
    }
    Ok((rowid, cols))
}

fn write_value(out: &mut impl Write, v: ValueRef) -> io::Result<()> {
    match v {
        ValueRef::Null => out.write_all(b"NULL"),
        ValueRef::Integer(i) => write!(out, "{i}"),
        ValueRef::Real(f) if f == f64::INFINITY => out.write_all(b"1e999"),
        ValueRef::Real(f) if f == f64::NEG_INFINITY => out.write_all(b"-1e999"),
        ValueRef::Real(f) => write!(out, "{f}"),
        ValueRef::Text(t) => write!(out, "{}", Escaped(t)),
        ValueRef::Blob(b) => {
            out.write_all(b"X'")?;
            const HEX: &[u8; 16] = b"0123456789abcdef";
            let mut buf = Vec::with_capacity(b.len() * 2 + 1);
            for byte in b {
                buf.push(HEX[(byte >> 4) as usize]);
                buf.push(HEX[(byte & 15) as usize]);
            }
            buf.push(b'\'');
            out.write_all(&buf)
        }
    }
}

/// `"name"` with inner quotes doubled
fn quote_ident(s: &str) -> String {
    format!("\"{}\"", s.replace('"', "\"\""))
}

/// Identifier quoted only when needed (sqld's `Quoted`, from the sqlite3 shell's `quoteChar`)
struct Quoted<'a>(&'a str);

impl std::fmt::Display for Quoted<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = self.0;
        let Some(first) = s.chars().next() else {
            return f.write_str(s);
        };
        let plain = (first.is_alphabetic() || first == '_')
            && s.chars().all(|c| c.is_alphanumeric() || c == '_')
            // SAFETY: pointer and length of a live &str
            && unsafe { rusqlite::ffi::sqlite3_keyword_check(s.as_ptr().cast(), s.len() as i32) } == 0;
        if plain {
            f.write_str(s)
        } else {
            f.write_str(&quote_ident(s))
        }
    }
}

/// A text value as an SQL literal: `'…'` with `''`, newlines and carriage returns through `replace(…, char(10))`
/// (the sqlite3 shell's `output_quoted_escaped_string`). Text that is not UTF-8 becomes `CAST(X'…' AS TEXT)`, which
/// gives back the same bytes.
struct Escaped<'a>(&'a [u8]);

impl std::fmt::Display for Escaped<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // text with a NUL in it as well: written as it is, the dump cannot be loaded (by sqld neither)
        let Some(s) = std::str::from_utf8(self.0).ok().filter(|s| !s.contains('\0')) else {
            f.write_str("CAST(X'")?;
            for b in self.0 {
                write!(f, "{b:02x}")?;
            }
            return f.write_str("' AS TEXT)");
        };
        if !s.contains(['\'', '\n', '\r']) {
            return write!(f, "'{s}'");
        }
        let nl = s.contains('\n').then(|| unused(s, "\\n", "\\012"));
        let cr = s.contains('\r').then(|| unused(s, "\\r", "\\015"));
        if nl.is_some() {
            f.write_str("replace(")?;
        }
        if cr.is_some() {
            f.write_str("replace(")?;
        }
        f.write_str("'")?;
        for ch in s.chars() {
            match ch {
                '\'' => f.write_str("''")?,
                '\n' => f.write_str(nl.as_deref().unwrap_or_default())?,
                '\r' => f.write_str(cr.as_deref().unwrap_or_default())?,
                c => write!(f, "{c}")?,
            }
        }
        f.write_str("'")?;
        if let Some(t) = cr {
            write!(f, ",'{t}',char(13))")?;
        }
        if let Some(t) = nl {
            write!(f, ",'{t}',char(10))")?;
        }
        Ok(())
    }
}

fn unused(haystack: &str, a: &str, b: &str) -> String {
    if !haystack.contains(a) {
        return a.to_owned();
    }
    if !haystack.contains(b) {
        return b.to_owned();
    }
    (0..)
        .map(|i| format!("({a}{i})"))
        .find(|n| !haystack.contains(n.as_str()))
        .unwrap_or_default()
}

#[derive(Debug)]
pub enum LoadError {
    Io(io::Error),
    /// The dump does not run inside one transaction (`BEGIN TRANSACTION` … `COMMIT`)
    NoTransaction,
    NoCommit,
    Sql {
        line: usize,
        message: String,
    },
    NotUtf8 {
        line: usize,
    },
    /// Not a statement that dumps consist of (see [`allowed_in_dump`])
    NotAllowed {
        line: usize,
        words: String,
    },
    /// A statement longer than `--dump-load-max-statement-bytes`
    TooLong {
        line: usize,
        limit: usize,
    },
    /// The load ran past `--dump-load-timeout-ms`
    TimedOut,
}

impl std::fmt::Display for LoadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LoadError::Io(e) => write!(f, "could not read the dump: {e}"),
            LoadError::NoTransaction => f.write_str("The dump should be wrapped in a transaction"),
            LoadError::NoCommit => f.write_str("The dump does not end with a COMMIT"),
            LoadError::Sql { line, message } => write!(f, "dump line {line}: {message}"),
            LoadError::NotUtf8 { line } => write!(f, "dump line {line} is not valid UTF-8"),
            LoadError::NotAllowed { line, words } => {
                write!(f, "dump line {line}: `{words} …` is not a statement a dump consists of")
            }
            LoadError::TooLong { line, limit } => write!(
                f,
                "dump line {line}: the statement is longer than {limit} bytes (--dump-load-max-statement-bytes)"
            ),
            LoadError::TimedOut => f.write_str("loading the dump took longer than --dump-load-timeout-ms"),
        }
    }
}

/// Limits of [`load`]
pub struct LoadLimits {
    /// Longest statement (`--dump-load-max-statement-bytes`); a longer line is refused before it is read in whole
    pub max_statement_bytes: usize,
    /// The whole load (`--dump-load-timeout-ms`); the connection's deadline interrupts SQL running past it
    pub deadline: Instant,
}

/// Runs an SQL dump on an empty database. Statements end where SQLite says a statement is complete (so trigger bodies
/// spanning lines work, unlike sqld which splits at every line ending in `;`); the text of a statement is read once
/// however many of its lines end in `;` ([`Completer`]). Like sqld: from the third statement on a transaction must
/// be open, and none may be open at the end. Only the statements [`export`] and sqld write are run.
///
/// Like sqld, a view or index whose definition ends in a `--` comment loads: the dump writes it as `<sql>;`, so the
/// `;` is inside the comment and the text never looks complete. Such a statement is run as it stands, and only
/// when SQLite finds it incomplete is the next line taken to belong to it.
pub fn load(conn: &Conn, input: impl BufRead, limits: &LoadLimits) -> Result<(), LoadError> {
    let mut stmt = String::new();
    let mut ends = Completer::default();
    let mut count = 0usize;
    let mut line_no = 0usize;
    let mut input = input;
    let mut raw = Vec::new();
    let failed = |e: LoadError| {
        conn.rollback_all();
        e
    };
    loop {
        if Instant::now() >= limits.deadline {
            return Err(failed(LoadError::TimedOut));
        }
        raw.clear();
        // one byte more than allowed: a longer line is refused without reading it in whole
        let room = limits.max_statement_bytes.saturating_sub(stmt.len()) as u64 + 1;
        let read = (&mut input).take(room).read_until(b'\n', &mut raw);
        if read.map_err(|e| failed(LoadError::Io(e)))? == 0 {
            break;
        }
        line_no += 1;
        if stmt.len() + raw.len() > limits.max_statement_bytes {
            return Err(failed(LoadError::TooLong {
                line: line_no,
                limit: limits.max_statement_bytes,
            }));
        }
        let line = std::str::from_utf8(&raw).map_err(|_| failed(LoadError::NotUtf8 { line: line_no }))?;
        if stmt.is_empty() && (line.trim().is_empty() || line.trim_start().starts_with("--")) {
            continue;
        }
        let line_start = stmt.len();
        stmt.push_str(line);
        if !line.trim_end().ends_with(';') {
            continue;
        }
        let tentative = !ends.complete(&stmt);
        if tentative && !(ends.trailing_line_comment().is_some_and(|c| c >= line_start) && ends.semicolon_would_end()) {
            continue;
        }
        if !allowed_in_dump(&stmt) {
            return Err(failed(LoadError::NotAllowed {
                line: line_no,
                words: crate::sql::leading_words(&stmt, 3).join(" "),
            }));
        }
        if count >= 2 && conn.is_autocommit() {
            return Err(LoadError::NoTransaction);
        }
        if let Err(e) = run_dump_statement(conn, &stmt) {
            if tentative && e.to_string().starts_with("incomplete input") {
                // a comment ending in `;` inside the statement: it goes on in the next line
                continue;
            }
            if Instant::now() >= limits.deadline {
                return Err(failed(LoadError::TimedOut));
            }
            return Err(failed(LoadError::Sql {
                line: line_no,
                message: e.to_string(),
            }));
        }
        count += 1;
        stmt.clear();
        ends = Completer::default();
    }
    if !stmt.trim().is_empty() {
        conn.rollback_all();
        return Err(LoadError::Sql {
            line: line_no,
            message: "incomplete statement at the end of the dump".into(),
        });
    }
    if !conn.is_autocommit() {
        conn.rollback_all();
        return Err(LoadError::NoCommit);
    }
    Ok(())
}

/// Runs one statement of a dump. `DELETE FROM sqlite_sequence` is skipped when the new database has no such table:
/// SQLite keeps `sqlite_sequence` (empty) after the last table with AUTOINCREMENT is dropped, the dump empties it,
/// and the new database only gets one with such a table, so the dump of a database that once had one did not load
/// (on sqld neither)
fn run_dump_statement(conn: &Conn, stmt: &str) -> rusqlite::Result<()> {
    if crate::sql::leading_words(stmt, 4) == ["DELETE", "FROM", "SQLITE_SEQUENCE"] {
        let exists: bool = conn.c.query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE name = 'sqlite_sequence')",
            [],
            |r| r.get(0),
        )?;
        if !exists {
            return Ok(());
        }
    }
    conn.c.execute_batch(stmt)
}

/// The statements of a dump written by [`export`] or sqld: one statement each, schema (no temporary objects), rows,
/// the transaction around them, `sqlite_sequence` and `sqlite_stat*` upkeep. Not `PRAGMA writable_schema`, which
/// would let a dump rewrite the schema table: dumps of databases with virtual tables are refused (README §4).
fn allowed_in_dump(stmt: &str) -> bool {
    if crate::sql::many_statements(stmt) {
        return false;
    }
    let words = crate::sql::leading_words(stmt, 3);
    let words: Vec<&str> = words.iter().map(String::as_str).collect();
    matches!(
        words.as_slice(),
        ["PRAGMA", "FOREIGN_KEYS", "OFF"]
            | ["BEGIN"]
            | ["BEGIN", "TRANSACTION"]
            | ["COMMIT"]
            | ["END"]
            | ["CREATE", "TABLE" | "INDEX" | "VIEW" | "TRIGGER", ..]
            | ["CREATE", "UNIQUE", "INDEX"]
            | ["INSERT", "INTO", ..]
            | ["DELETE", "FROM", "SQLITE_SEQUENCE"]
            | ["ANALYZE", "SQLITE_SCHEMA"]
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;

    /// Database used for the comparison with sqld (the same statements were sent to sqld 0.24.32 one by one)
    const SOURCE: &str = include_str!("../tests/fixtures/dump-source.sql");

    fn db() -> (tempfile::TempDir, Conn, Config) {
        let dir = tempfile::tempdir().unwrap();
        let cfg = Config::for_tests(dir.path());
        let c = Conn::open_writer(&dir.path().join("data"), &cfg).unwrap();
        (dir, c, cfg)
    }

    /// Connection as used to build a database from a dump
    fn loader() -> (tempfile::TempDir, Conn) {
        let dir = tempfile::tempdir().unwrap();
        let c = Conn::open_loader(&dir.path().join("data"), &Config::for_tests(dir.path())).unwrap();
        (dir, c)
    }

    fn dump(c: &Conn) -> String {
        let mut out = Vec::new();
        export(&c.c, &mut out, false).unwrap();
        String::from_utf8(out).unwrap()
    }

    fn load_all(c: &Conn, text: &str) -> Result<(), LoadError> {
        let limits = LoadLimits {
            max_statement_bytes: 1 << 30,
            deadline: Instant::now() + Duration::from_secs(600),
        };
        load(c, text.as_bytes(), &limits)
    }

    /// Exports `setup` and loads the dump into a new database; returns the dump and the error of the load
    fn round_trip(setup: &str) -> (String, Result<(), LoadError>, tempfile::TempDir, Conn) {
        let (_d, c, cfg) = db();
        c.c.execute_batch(setup).unwrap();
        let text = dump(&c);
        let dir2 = tempfile::tempdir().unwrap();
        let c2 = Conn::open_loader(&dir2.path().join("data"), &cfg).unwrap();
        let r = load_all(&c2, &text);
        (text, r, dir2, c2)
    }

    /// Review of 0.1.0: a view or index definition ending in a `--` comment is written as `<sql> -- c;`; sqld loads
    /// that, xsqld waited for the end of a statement that never came
    #[test]
    fn definitions_ending_in_a_line_comment_load_back() {
        let (text, r, _d, c2) = round_trip(
            "CREATE TABLE t(a, b);
             CREATE VIEW v AS SELECT a FROM t -- the view;
             ;
             CREATE INDEX i ON t(a) -- the index
             ;
             CREATE TABLE u(
               a INTEGER, -- not the end;
               b TEXT
             );
             CREATE VIEW w AS SELECT a, -- still going;
               b FROM t;
             CREATE TRIGGER tr AFTER INSERT ON t BEGIN
               UPDATE t SET b = 1; -- inside;
             END;
             INSERT INTO t VALUES (1, 2);",
        );
        assert!(text.contains("FROM t -- the view;\n"), "{text}");
        r.unwrap_or_else(|e| panic!("{e}\n{text}"));
        assert_eq!(dump(&c2), text);
        let objects: i64 =
            c2.c.query_row("SELECT count(*) FROM sqlite_schema", [], |r| r.get(0))
                .unwrap();
        assert_eq!(objects, 6);
    }

    /// Review of 0.1.0: text with a NUL in it was written as it is, and such a dump did not load (sqld: the same)
    #[test]
    fn text_with_nul_round_trips() {
        let (text, r, _d, c2) = round_trip("CREATE TABLE n(v); INSERT INTO n VALUES ('a' || char(0) || 'b');");
        assert!(text.contains("CAST(X'610062' AS TEXT)"), "{text}");
        r.unwrap();
        let (t, h): (String, String) =
            c2.c.query_row("SELECT typeof(v), hex(v) FROM n", [], |r| Ok((r.get(0)?, r.get(1)?)))
                .unwrap();
        assert_eq!((t.as_str(), h.as_str()), ("text", "610062"));
    }

    /// Review of 0.1.0: SQLite keeps `sqlite_sequence` after its last AUTOINCREMENT table is dropped; the dump then
    /// empties a table the new database does not have, and did not load (sqld: the same)
    #[test]
    fn a_leftover_sqlite_sequence_loads_back() {
        let (text, r, _d, c2) = round_trip(
            "CREATE TABLE a(id INTEGER PRIMARY KEY AUTOINCREMENT); INSERT INTO a DEFAULT VALUES; DROP TABLE a;
             CREATE TABLE b(v); INSERT INTO b VALUES (1);",
        );
        assert!(text.contains("DELETE FROM sqlite_sequence;\n"), "{text}");
        r.unwrap();
        assert_eq!(
            c2.c.query_row("SELECT v FROM b", [], |r| r.get::<_, i64>(0)).unwrap(),
            1
        );
        // with a table that has AUTOINCREMENT, the sequence is restored as before
        let (_, r, _d, c2) = round_trip(
            "CREATE TABLE s(id INTEGER PRIMARY KEY AUTOINCREMENT); INSERT INTO s VALUES (41); DELETE FROM s;",
        );
        r.unwrap();
        let seq: i64 =
            c2.c.query_row("SELECT seq FROM sqlite_sequence WHERE name = 's'", [], |r| r.get(0))
                .unwrap();
        assert_eq!(seq, 41);
    }

    /// Review of 0.1.0: loading had no limits (one line of hundreds of MB, a statement that runs for ever)
    #[test]
    fn a_dump_beyond_the_limits_fails() {
        let head = "PRAGMA foreign_keys=OFF;\nBEGIN TRANSACTION;\nCREATE TABLE z(x);\n";
        let (_d, c) = loader();
        let limits = LoadLimits {
            max_statement_bytes: 4096,
            deadline: Instant::now() + Duration::from_secs(600),
        };
        // a statement within the limit, then one line far beyond it, which is not read in whole
        let long = format!(
            "{head}INSERT INTO z VALUES('{}');\nINSERT INTO z VALUES('{}');\nCOMMIT;\n",
            "a".repeat(4000),
            "b".repeat(1 << 20)
        );
        let mut input = std::io::BufReader::with_capacity(1024, std::io::Cursor::new(long.as_bytes()));
        let e = load(&c, &mut input, &limits).unwrap_err();
        assert!(matches!(e, LoadError::TooLong { line: 5, limit: 4096 }), "{e}");
        let line5 = long.find("INSERT INTO z VALUES('b").unwrap() as u64;
        assert!(
            input.get_ref().position() <= line5 + 4097 + 1024,
            "read on past the limit"
        );
        assert!(c.is_autocommit());
        // the same over several lines of one statement
        let lines = format!("{head}CREATE VIEW v AS SELECT\n{}1;\nCOMMIT;\n", "1 +\n".repeat(2000));
        let e = load(&c, lines.as_bytes(), &limits).unwrap_err();
        assert!(matches!(e, LoadError::TooLong { .. }), "{e}");
        // a statement that would run for ever is interrupted at the deadline
        let started = Instant::now();
        let forever = format!(
            "{head}INSERT INTO z WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n) SELECT i FROM n;\nCOMMIT;\n"
        );
        c.set_deadline(Some(started + Duration::from_millis(200)));
        let limits = LoadLimits {
            max_statement_bytes: 1 << 20,
            deadline: started + Duration::from_millis(200),
        };
        let e = load(&c, forever.as_bytes(), &limits).unwrap_err();
        assert!(matches!(e, LoadError::TimedOut), "{e}");
        assert!(started.elapsed() < Duration::from_secs(5));
        assert!(c.is_autocommit());
    }

    /// Output of sqld 0.24.32 for the same database (`GET /dump`, captured 2026-11-10)
    const SQLD_OUTPUT: &str = include_str!("../tests/fixtures/sqld-0.24.32-dump.sql");

    #[test]
    fn matches_sqld_byte_for_byte() {
        let (_d, c, _) = db();
        c.c.execute_batch(SOURCE).unwrap();
        assert_eq!(dump(&c), SQLD_OUTPUT);
    }

    #[test]
    fn dump_loads_back_identically() {
        let (_d, c, cfg) = db();
        c.c.execute_batch(SOURCE).unwrap();
        c.c.execute_batch(
            "CREATE TABLE q(\"we\"\"ird\" TEXT, [x y] INT);
             INSERT INTO q VALUES ('a''b' || char(10) || '\\n' || char(13), 1);",
        )
        .unwrap();
        let text = dump(&c);
        let dir2 = tempfile::tempdir().unwrap();
        let c2 = Conn::open_loader(&dir2.path().join("data"), &cfg).unwrap();
        load_all(&c2, &text).unwrap();
        assert_eq!(dump(&c2), text);
        let seq: i64 =
            c2.c.query_row("SELECT seq FROM sqlite_sequence WHERE name = 'a'", [], |r| r.get(0))
                .unwrap();
        assert_eq!(seq, 10);
    }

    #[test]
    fn load_requires_a_transaction() {
        let (_d, c) = loader();
        let e = load_all(&c, "CREATE TABLE a(x);\nCREATE TABLE b(x);\nCREATE TABLE c(x);\n").unwrap_err();
        assert!(matches!(e, LoadError::NoTransaction));
        let e = load_all(&c, "PRAGMA foreign_keys=OFF;\nBEGIN TRANSACTION;\nCREATE TABLE d(x);\n").unwrap_err();
        assert!(matches!(e, LoadError::NoCommit));
        assert!(c.is_autocommit());
        let e = load_all(&c, "BEGIN;\nATTACH 'x' AS y;\nCOMMIT;\n").unwrap_err();
        assert!(matches!(e, LoadError::NotAllowed { line: 2, .. }), "{e}");
        let ok = "-- comment\n\nPRAGMA foreign_keys=OFF;\nBEGIN TRANSACTION;\nCREATE TABLE IF NOT EXISTS z(x);\nINSERT INTO z VALUES(1);\nCOMMIT;\n";
        load_all(&c, ok).unwrap();
    }

    #[test]
    fn text_that_is_not_utf8_round_trips() {
        // review 2318 S6: written through from_utf8_lossy, such text came back as U+FFFD
        let (_d, c, cfg) = db();
        c.c.execute_batch("CREATE TABLE b(v); INSERT INTO b VALUES (CAST(X'41ff0a42' AS TEXT)), ('ok');")
            .unwrap();
        let text = dump(&c);
        assert!(
            text.contains("INSERT INTO b VALUES(CAST(X'41ff0a42' AS TEXT));\n"),
            "{text}"
        );
        let dir2 = tempfile::tempdir().unwrap();
        let c2 = Conn::open_loader(&dir2.path().join("data"), &cfg).unwrap();
        load_all(&c2, &text).unwrap();
        let back: Vec<(String, String)> =
            c2.c.prepare("SELECT typeof(v), hex(v) FROM b ORDER BY rowid")
                .unwrap()
                .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
                .unwrap()
                .collect::<Result<_, _>>()
                .unwrap();
        assert_eq!(
            back,
            [("text".into(), "41FF0A42".into()), ("text".into(), "6F6B".into())]
        );
    }

    #[test]
    fn load_runs_only_what_dumps_consist_of() {
        // review 2318 S7: a dump could run any statement, e.g. PRAGMA writable_schema
        let (_d, c) = loader();
        let head = "PRAGMA foreign_keys=OFF;\nBEGIN TRANSACTION;\nCREATE TABLE IF NOT EXISTS z(x);\n";
        for bad in [
            "PRAGMA writable_schema=ON;\n",
            "PRAGMA foreign_keys=ON;\n",
            "INSERT INTO z VALUES(1); PRAGMA writable_schema=ON;\n",
            "CREATE TEMP TABLE y(a);\n",
            "DROP TABLE z;\n",
            "UPDATE z SET x = 1;\n",
            "DELETE FROM z;\n",
            "SELECT 1;\n",
            "VACUUM;\n",
        ] {
            let e = load_all(&c, &format!("{head}{bad}COMMIT;\n")).unwrap_err();
            assert!(matches!(e, LoadError::NotAllowed { line: 4, .. }), "{bad}: {e}");
            assert!(c.is_autocommit(), "{bad}");
        }
        let ok = format!(
            "{head}CREATE UNIQUE INDEX zi ON z(x);\nCREATE VIEW zv AS SELECT x FROM z;\n\
             CREATE TRIGGER zt AFTER INSERT ON z BEGIN DELETE FROM z WHERE 0; END;\n\
             CREATE TABLE s(id INTEGER PRIMARY KEY AUTOINCREMENT);\nINSERT INTO z VALUES(1);\n\
             DELETE FROM sqlite_sequence;\nINSERT INTO sqlite_sequence VALUES('s',5);\nANALYZE sqlite_schema;\nCOMMIT;\n"
        );
        load_all(&c, &ok).unwrap();
    }

    #[test]
    fn escaping() {
        let e = |s: &str| Escaped(s.as_bytes()).to_string();
        assert_eq!(e("hello"), "'hello'");
        assert_eq!(e("it's"), "'it''s'");
        assert_eq!(e("a\nb"), "replace('a\\nb','\\n',char(10))");
        assert_eq!(e("a\\nb\nc"), "replace('a\\nb\\012c','\\012',char(10))");
        assert_eq!(e("a\r\n"), "replace(replace('a\\r\\n','\\r',char(13)),'\\n',char(10))");
        assert_eq!(Quoted("abc").to_string(), "abc");
        assert_eq!(Quoted("select").to_string(), "\"select\"");
        assert_eq!(Quoted("a b").to_string(), "\"a b\"");
        assert_eq!(Quoted("1a").to_string(), "\"1a\"");
    }
}
