//! One SQLite connection with the per-connection settings (kept identical to sqld 0.24.32 where they change SQL
//! behaviour, see README §1.1) and a deadline that interrupts running SQL.

use std::path::Path;
use std::sync::Arc;
use std::sync::LazyLock;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use rusqlite::{CachedStatement, Connection, OpenFlags, Statement, limits::Limit};

use crate::config::Config;

static EPOCH: LazyLock<Instant> = LazyLock::new(Instant::now);

/// Name of the savepoint that stands for the client's transaction inside the group transaction
pub const CLIENT_SP: &str = "xsqld_client";
/// Name of the savepoint around one request inside the group transaction
pub const REQUEST_SP: &str = "xsqld_request";

/// Client SQL longer than this is compiled for one use and not kept in the statement cache. Statements worth caching
/// are parameterized and short; long ones carry their values inline and never repeat, and a cache full of them (256
/// per connection, each several times its text in memory) grew a server to gigabytes.
const CACHED_SQL_MAX: usize = 8 * 1024;

pub struct Conn {
    pub c: Connection,
    /// Nanoseconds since EPOCH after which running SQL is interrupted; 0 = no deadline
    deadline: Arc<AtomicU64>,
    /// Write connection: a COMMIT is turned into a ROLLBACK while this is set (see [`Conn::refuse_commits`])
    no_commit: Arc<AtomicBool>,
}

/// A compiled client statement: from the connection's statement cache (returned to it when dropped), or compiled for
/// this one use
pub enum Prepared<'c> {
    Cached(CachedStatement<'c>),
    Once(Statement<'c>),
}

impl<'c> std::ops::Deref for Prepared<'c> {
    type Target = Statement<'c>;
    fn deref(&self) -> &Statement<'c> {
        match self {
            Prepared::Cached(s) => s,
            Prepared::Once(s) => s,
        }
    }
}

impl<'c> std::ops::DerefMut for Prepared<'c> {
    fn deref_mut(&mut self) -> &mut Statement<'c> {
        match self {
            Prepared::Cached(s) => s,
            Prepared::Once(s) => s,
        }
    }
}

impl Conn {
    pub fn open_writer(path: &Path, cfg: &Config) -> rusqlite::Result<Conn> {
        let flags = OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE | OpenFlags::SQLITE_OPEN_NO_MUTEX;
        let mut conn = Self::open(path, flags, cfg)?;
        conn.c.busy_timeout(Duration::from_millis(100))?;
        conn.apply_writer_settings(cfg)?;
        conn.c.authorizer(Some(authorize_shared))?;
        let no_commit = conn.no_commit.clone();
        conn.c.commit_hook(Some(move || no_commit.load(Ordering::Relaxed)))?;
        Ok(conn)
    }

    /// While set, a COMMIT on this connection becomes a ROLLBACK (the statement fails with SQLITE_CONSTRAINT). The
    /// write thread sets it for the time a group transaction is open and clears it for its own COMMIT: whatever client
    /// SQL might still commit inside a group (a spelling of COMMIT the statement classification does not know) then
    /// rolls the group back, which the write thread handles by redoing the group; a commit would leave the requests
    /// before it written and then redone.
    pub fn refuse_commits(&self, on: bool) {
        self.no_commit.store(on, Ordering::Relaxed);
    }

    /// Compiles client SQL, trimmed ([`crate::sql::trimmed`]); statements up to [`CACHED_SQL_MAX`] go through the
    /// connection's statement cache
    pub fn prepare(&self, sql: &str) -> rusqlite::Result<Prepared<'_>> {
        let sql = crate::sql::trimmed(sql);
        if sql.len() <= CACHED_SQL_MAX {
            self.c.prepare_cached(sql).map(Prepared::Cached)
        } else {
            self.c.prepare(sql).map(Prepared::Once)
        }
    }

    pub fn open_reader(path: &Path, cfg: &Config) -> rusqlite::Result<Conn> {
        let flags = OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX;
        let mut conn = Self::open(path, flags, cfg)?;
        conn.c.busy_timeout(Duration::from_millis(1000))?;
        conn.c.authorizer(Some(authorize_shared))?;
        Ok(conn)
    }

    /// Connection used to build a database from an SQL dump
    pub fn open_loader(path: &Path, cfg: &Config) -> rusqlite::Result<Conn> {
        let flags = OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE | OpenFlags::SQLITE_OPEN_NO_MUTEX;
        let conn = Self::open(path, flags, cfg)?;
        conn.c
            .pragma_update_and_check(None, "journal_mode", "WAL", |r| r.get::<_, String>(0))?;
        conn.c.pragma_update(None, "synchronous", "OFF")?;
        Ok(conn)
    }

    fn open(path: &Path, flags: OpenFlags, cfg: &Config) -> rusqlite::Result<Conn> {
        let mut c = Connection::open_with_flags(path, flags)?;
        let deadline = Arc::new(AtomicU64::new(0));
        let d = deadline.clone();
        c.progress_handler(
            1000,
            Some(move || {
                let at = d.load(Ordering::Relaxed);
                at != 0 && (EPOCH.elapsed().as_nanos() as u64) >= at
            }),
        )?;
        c.set_prepared_statement_cache_capacity(256);
        // ATTACH is not allowed (sqld: allow_attach defaults to false)
        c.set_limit(Limit::SQLITE_LIMIT_ATTACHED, 0)?;
        c.set_limit(Limit::SQLITE_LIMIT_LENGTH, cfg.max_value_bytes)?;
        // SQLite's default is 50000 bytes: its matcher recurses once per `%` (a pattern of some 40 KB overflows the
        // stack of a thread and takes the process down) and runs for minutes on a long pattern without reaching the
        // progress handler, so no deadline stops it
        c.set_limit(Limit::SQLITE_LIMIT_LIKE_PATTERN_LENGTH, cfg.max_like_pattern_bytes)?;
        let conn = Conn {
            c,
            deadline,
            no_commit: Arc::new(AtomicBool::new(false)),
        };
        conn.apply_session_settings(cfg)?;
        Ok(conn)
    }

    fn apply_session_settings(&self, cfg: &Config) -> rusqlite::Result<()> {
        self.c.execute_batch(&format!(
            "PRAGMA foreign_keys = ON; PRAGMA recursive_triggers = OFF; PRAGMA cache_size = {};",
            cfg.cache_size
        ))
    }

    fn apply_writer_settings(&self, cfg: &Config) -> rusqlite::Result<()> {
        let mode: String = self
            .c
            .pragma_update_and_check(None, "journal_mode", "WAL", |r| r.get(0))?;
        if !mode.eq_ignore_ascii_case("wal") {
            return Err(rusqlite::Error::SqliteFailure(
                rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_ERROR),
                Some(format!("could not switch to WAL (journal_mode = {mode})")),
            ));
        }
        self.c.execute_batch(&format!(
            "PRAGMA synchronous = {}; PRAGMA wal_autocheckpoint = 0; PRAGMA journal_size_limit = {};",
            cfg.synchronous.pragma(),
            cfg.journal_size_limit
        ))
    }

    pub fn set_deadline(&self, at: Option<Instant>) {
        let v = match at {
            None => 0,
            Some(t) => (t.saturating_duration_since(*EPOCH).as_nanos() as u64).max(1),
        };
        self.deadline.store(v, Ordering::Relaxed);
    }

    pub fn is_autocommit(&self) -> bool {
        self.c.is_autocommit()
    }

    pub fn reset_last_insert_rowid(&self) {
        // SAFETY: the handle belongs to the open connection `self.c`, used only from this thread
        unsafe { rusqlite::ffi::sqlite3_set_last_insert_rowid(self.c.handle(), 0) }
    }

    /// The open transaction has deferred foreign key violations not resolved yet (its COMMIT would fail)
    pub fn deferred_fk_pending(&self) -> bool {
        let (mut cur, mut high) = (0, 0);
        // SAFETY: the handle belongs to the open connection `self.c`, used only from this thread
        unsafe {
            rusqlite::ffi::sqlite3_db_status(
                self.c.handle(),
                rusqlite::ffi::SQLITE_DBSTATUS_DEFERRED_FKS,
                &mut cur,
                &mut high,
                0,
            );
        }
        cur > 0
    }

    /// Roll back whatever transaction is open (no error if none)
    pub fn rollback_all(&self) {
        if !self.c.is_autocommit() {
            let _ = self.c.execute_batch("ROLLBACK");
        }
    }
}

/// Introspection PRAGMAs: they only read, also with an argument (`PRAGMA table_info(t)`, the `pragma_table_info()`
/// table functions)
const PRAGMA_INTROSPECTION: &[&str] = &[
    "foreign_key_check",
    "foreign_key_list",
    "index_info",
    "index_list",
    "index_xinfo",
    "integrity_check",
    "quick_check",
    "table_info",
    "table_list",
    "table_xinfo",
];

/// Settings that may be read (`PRAGMA name`) but not assigned
const PRAGMA_READABLE: &[&str] = &[
    "application_id",
    "auto_vacuum",
    "automatic_index",
    "busy_timeout",
    "cache_size",
    "cache_spill",
    "cell_size_check",
    "collation_list",
    "compile_options",
    "data_version",
    "database_list",
    "encoding",
    "foreign_keys",
    "freelist_count",
    "function_list",
    "journal_mode",
    "journal_size_limit",
    "legacy_alter_table",
    "locking_mode",
    "max_page_count",
    "mmap_size",
    "module_list",
    "page_count",
    "page_size",
    "pragma_list",
    "query_only",
    "recursive_triggers",
    "reverse_unordered_selects",
    "schema_version",
    "secure_delete",
    "synchronous",
    "temp_store",
    "trusted_schema",
    "user_version",
    "wal_autocheckpoint",
];

/// Client SQL on the shared connections (sqld gives each stream a connection of its own; here one write connection
/// serves every stream). Refused at prepare time with SQLite's `not authorized` (SQLITE_AUTH), before anything runs:
/// - PRAGMAs that change connection state, which would carry over to every other stream (`ignore_check_constraints`,
///   `query_only`, `locking_mode = EXCLUSIVE`, `foreign_keys = OFF`, …). Allowed: reading PRAGMAs,
///   `defer_foreign_keys`, which SQLite switches off by itself at the end of each transaction, and `user_version`,
///   which is stored in the database file;
/// - temporary tables, indexes, views, triggers and virtual tables, written `CREATE TEMP …` or `CREATE … temp.name`:
///   they would live on the shared connection, where a temporary trigger on a table of the database runs for every
///   other client's writes and a temporary view or table can stand in front of a real one. Every way of creating
///   one inserts a row into the schema table of `temp`, and that insert is what is refused (`ALTER TABLE … RENAME`
///   of an ordinary table only updates that schema table, which stays allowed);
/// - `load_extension()`, also when extension loading has been switched on; SQLite reports this one as a plain
///   SQLITE_ERROR, `not authorized to use function: load_extension`.
///
/// sqld's parser refuses all of these too, except `foreign_keys = …`; the error is reported in sqld's words
/// ([`crate::sql::prepare_error`]).
fn authorize_shared(ctx: rusqlite::hooks::AuthContext<'_>) -> rusqlite::hooks::Authorization {
    use rusqlite::hooks::{AuthAction, Authorization};
    let allowed = match ctx.action {
        AuthAction::Pragma {
            pragma_name,
            pragma_value,
        } => {
            let name = pragma_name.to_ascii_lowercase();
            name == "defer_foreign_keys"
                || name == "user_version"
                || PRAGMA_INTROSPECTION.contains(&name.as_str())
                || (pragma_value.is_none() && PRAGMA_READABLE.contains(&name.as_str()))
        }
        AuthAction::CreateTempIndex { .. }
        | AuthAction::CreateTempTable { .. }
        | AuthAction::CreateTempTrigger { .. }
        | AuthAction::CreateTempView { .. } => false,
        AuthAction::Insert { .. } => ctx.database_name != Some("temp"),
        AuthAction::Function { function_name } => !function_name.eq_ignore_ascii_case("load_extension"),
        _ => true,
    };
    if allowed {
        Authorization::Allow
    } else {
        Authorization::Deny
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shared_connections_refuse_state_changing_pragmas_and_temp_objects() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = Config::for_tests(dir.path());
        let w = Conn::open_writer(&dir.path().join("data"), &cfg).unwrap();
        w.c.execute_batch("CREATE TABLE t(a INTEGER PRIMARY KEY, b TEXT CHECK (b <> 'bad'))")
            .unwrap();
        for sql in [
            "PRAGMA ignore_check_constraints = ON",
            "PRAGMA query_only = 1",
            "PRAGMA locking_mode = EXCLUSIVE",
            "PRAGMA recursive_triggers = ON",
            "PRAGMA foreign_keys = OFF",
            "PRAGMA main.cache_size = 1",
            "PRAGMA wal_autocheckpoint = 1000",
            "PRAGMA journal_mode = DELETE",
            "PRAGMA writable_schema = ON",
            "PRAGMA optimize",
            "PRAGMA wal_checkpoint(TRUNCATE)",
            "CREATE TEMP TABLE x(a)",
            "CREATE TEMPORARY VIEW v AS SELECT 1",
            "CREATE TEMP TRIGGER tr AFTER INSERT ON t BEGIN SELECT 1; END",
            // review of 0.1.0, finding 2: the same objects, named through the `temp` schema
            "CREATE TABLE temp.x(a)",
            "CREATE TABLE TEMP.x AS SELECT 1 AS a",
            "CREATE VIEW temp.t AS SELECT 1 AS a, 'shadow' AS b",
            "CREATE TRIGGER temp.spy AFTER INSERT ON main.t BEGIN SELECT 1; END",
            "CREATE TRIGGER \"temp\".spy AFTER INSERT ON t BEGIN DELETE FROM t; END",
            "CREATE VIRTUAL TABLE temp.vt USING dbstat(main)",
        ] {
            let e = w.c.prepare(sql).map(|_| ()).unwrap_err();
            assert_eq!(
                e.sqlite_error_code(),
                Some(rusqlite::ErrorCode::AuthorizationForStatementDenied),
                "{sql}"
            );
        }
        // without the authorizer these prepare fine and fail only when run, as long as loading stays switched off
        for sql in ["SELECT load_extension('x')", "SELECT LOAD_EXTENSION('x', 'init')"] {
            let e = w.c.prepare(sql).map(|_| ()).unwrap_err();
            assert!(e.to_string().contains("not authorized to use function"), "{sql}: {e}");
        }
        for sql in [
            "PRAGMA defer_foreign_keys = ON",
            "PRAGMA defer_foreign_keys",
            "PRAGMA user_version = 3",
            "PRAGMA foreign_keys",
            "PRAGMA table_info(t)",
            "PRAGMA main.index_list(t)",
            "PRAGMA foreign_key_list('t')",
            "PRAGMA integrity_check",
            "PRAGMA user_version",
            "SELECT name FROM pragma_table_info('t')",
            "SELECT * FROM pragma_foreign_key_check",
        ] {
            w.c.prepare(sql).map(|_| ()).unwrap_or_else(|e| panic!("{sql}: {e}"));
        }
        assert!(w.c.execute("INSERT INTO t(b) VALUES ('bad')", []).is_err());
        let r = Conn::open_reader(&dir.path().join("data"), &cfg).unwrap();
        assert!(r.c.prepare("PRAGMA cache_size = 1").is_err());
        assert!(r.c.prepare("PRAGMA table_info(t)").is_ok());
        // nothing was left in `temp`, and ordinary schema changes (which also touch the schema table of `temp`) work
        let temp_objects: i64 =
            w.c.query_row("SELECT count(*) FROM sqlite_temp_master", [], |r| r.get(0))
                .unwrap();
        assert_eq!(temp_objects, 0);
        for sql in [
            "CREATE TABLE main.u(id INTEGER PRIMARY KEY, v TEXT, w TEXT)",
            "CREATE INDEX u_v ON u(v)",
            "CREATE VIEW uv AS SELECT v FROM u",
            "CREATE TRIGGER u_ins AFTER INSERT ON u BEGIN UPDATE u SET w = new.v WHERE id = new.id; END",
            "INSERT INTO u(v) VALUES ('x')",
            "ALTER TABLE u ADD COLUMN extra INTEGER",
            "ALTER TABLE u RENAME COLUMN extra TO more",
            "ALTER TABLE u DROP COLUMN more",
            "DROP VIEW uv",
            "ALTER TABLE u RENAME TO u2",
            "CREATE TABLE copy AS SELECT * FROM u2",
            "REINDEX",
            "ANALYZE",
            "DROP TRIGGER u_ins",
            "DROP INDEX u_v",
            "DROP TABLE u2",
        ] {
            w.c.execute_batch(sql).unwrap_or_else(|e| panic!("{sql}: {e}"));
        }
    }

    /// Review of 0.1.0, finding 3: a long LIKE or GLOB pattern is refused before SQLite's matcher sees it
    #[test]
    fn long_like_patterns_are_refused() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = Config::for_tests(dir.path());
        let w = Conn::open_writer(&dir.path().join("data"), &cfg).unwrap();
        let r = Conn::open_reader(&dir.path().join("data"), &cfg).unwrap();
        let limit = cfg.max_like_pattern_bytes as usize;
        // the deepest recursion the limit allows, on a thread with the smallest stack the server's threads have
        let deepest = "%a".repeat(limit / 2);
        let text = "a".repeat(100_000);
        std::thread::Builder::new()
            .stack_size(2 * 1024 * 1024)
            .spawn({
                let (deepest, text) = (deepest.clone(), text.clone());
                let path = dir.path().join("data");
                let cfg = cfg.clone();
                move || {
                    let c = Conn::open_reader(&path, &cfg).unwrap();
                    for op in ["LIKE", "GLOB"] {
                        let pattern = if op == "GLOB" {
                            deepest.replace('%', "*")
                        } else {
                            deepest.clone()
                        };
                        let hit: bool =
                            c.c.query_row(&format!("SELECT ?1 {op} ?2"), [&text, &pattern], |r| r.get(0))
                                .unwrap();
                        assert!(hit, "{op}");
                    }
                }
            })
            .unwrap()
            .join()
            .unwrap();
        let too_long = "%a".repeat(limit / 2 + 1);
        for c in [&w, &r] {
            for sql in ["SELECT ?1 LIKE ?2", "SELECT ?1 GLOB ?2"] {
                let e =
                    c.c.query_row(sql, [&text, &too_long], |r| r.get::<_, bool>(0))
                        .unwrap_err();
                assert!(e.to_string().contains("pattern too complex"), "{sql}: {e}");
            }
        }
        // what took the process down: about 44 KB of `%a`
        let e =
            w.c.query_row("SELECT ?1 LIKE ?2", [&text, &"%a".repeat(22_000)], |r| {
                r.get::<_, bool>(0)
            })
            .unwrap_err();
        assert!(e.to_string().contains("pattern too complex"), "{e}");
    }

    /// Review of 0.1.0, finding 7: long statements are not kept in the statement cache
    #[test]
    fn long_statements_stay_out_of_the_statement_cache() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = Config::for_tests(dir.path());
        let w = Conn::open_writer(&dir.path().join("data"), &cfg).unwrap();
        assert!(matches!(w.prepare("SELECT 1").unwrap(), Prepared::Cached(_)));
        let at_limit = format!("SELECT '{}'", "x".repeat(CACHED_SQL_MAX - 9));
        assert_eq!(at_limit.len(), CACHED_SQL_MAX);
        assert!(matches!(w.prepare(&at_limit).unwrap(), Prepared::Cached(_)));
        let long = format!("SELECT '{}'", "x".repeat(CACHED_SQL_MAX));
        assert!(matches!(w.prepare(&long).unwrap(), Prepared::Once(_)));
        // both ways compile the same text: whitespace around the statement is not part of it
        for sql in [format!("\u{a0}{long}\u{3000}\n"), "\u{a0}SELECT 1\u{3000}".to_owned()] {
            let mut st = w.prepare(&sql).unwrap();
            assert!(st.raw_query().next().unwrap().is_some());
        }
        let mut st = w.prepare(&long).unwrap();
        let len: usize = st.query_row([], |r| r.get::<_, String>(0)).unwrap().len();
        assert_eq!(len, CACHED_SQL_MAX);
    }

    #[test]
    fn refused_commits_roll_back() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = Config::for_tests(dir.path());
        let w = Conn::open_writer(&dir.path().join("data"), &cfg).unwrap();
        w.c.execute_batch("CREATE TABLE t(a)").unwrap();
        w.refuse_commits(true);
        w.c.execute_batch("BEGIN IMMEDIATE; INSERT INTO t VALUES (1)").unwrap();
        let e = w.c.execute_batch("COMMIT").unwrap_err();
        assert_eq!(e.sqlite_error_code(), Some(rusqlite::ErrorCode::ConstraintViolation));
        assert!(w.is_autocommit());
        w.refuse_commits(false);
        let n: i64 = w.c.query_row("SELECT count(*) FROM t", [], |r| r.get(0)).unwrap();
        assert_eq!(n, 0);
        w.c.execute_batch("BEGIN IMMEDIATE; INSERT INTO t VALUES (1); COMMIT")
            .unwrap();
        let n: i64 = w.c.query_row("SELECT count(*) FROM t", [], |r| r.get(0)).unwrap();
        assert_eq!(n, 1);
    }

    #[test]
    fn settings_match_sqld_where_behaviour_depends_on_them() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = Config::for_tests(dir.path());
        let w = Conn::open_writer(&dir.path().join("data"), &cfg).unwrap();
        let get = |c: &Conn, p: &str| {
            c.c.query_row(&format!("PRAGMA {p}"), [], |r| r.get::<_, rusqlite::types::Value>(0))
                .unwrap()
        };
        use rusqlite::types::Value::*;
        assert_eq!(get(&w, "foreign_keys"), Integer(1));
        assert_eq!(get(&w, "recursive_triggers"), Integer(0));
        assert_eq!(get(&w, "journal_mode"), Text("wal".into()));
        assert_eq!(get(&w, "synchronous"), Integer(2));
        assert_eq!(get(&w, "wal_autocheckpoint"), Integer(0));
        assert_eq!(get(&w, "cache_size"), Integer(-2000));
        assert_eq!(get(&w, "trusted_schema"), Integer(1));
        let r = Conn::open_reader(&dir.path().join("data"), &cfg).unwrap();
        assert_eq!(get(&r, "foreign_keys"), Integer(1));
        assert!(r.c.execute("CREATE TABLE x(a)", []).is_err());
        assert!(w.c.execute("ATTACH ':memory:' AS m", []).is_err());
        let version: String = w.c.query_row("SELECT sqlite_version()", [], |r| r.get(0)).unwrap();
        let v: Vec<u32> = version.split('.').map(|p| p.parse().unwrap()).collect();
        assert!(v[0] == 3 && v[1] >= 41, "SQLite {version} is older than 3.41");
    }

    #[test]
    fn deadline_interrupts() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = Config::for_tests(dir.path());
        let w = Conn::open_writer(&dir.path().join("data"), &cfg).unwrap();
        w.set_deadline(Some(Instant::now() + Duration::from_millis(50)));
        let started = Instant::now();
        let err =
            w.c.query_row(
                "WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n) SELECT count(*) FROM n",
                [],
                |r| r.get::<_, i64>(0),
            )
            .unwrap_err();
        assert!(started.elapsed() < Duration::from_secs(2));
        assert_eq!(err.sqlite_error_code(), Some(rusqlite::ErrorCode::OperationInterrupted));
        w.set_deadline(None);
        assert_eq!(w.c.query_row("SELECT 1", [], |r| r.get::<_, i64>(0)).unwrap(), 1);
    }
}
