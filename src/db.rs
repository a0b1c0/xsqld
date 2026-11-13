//! Databases (sqld "namespaces"): one file each, `--db-path` + `--db-file-template`. A database exists when its file
//! exists; it is opened on first use (write thread + read pool) and stays open until deleted or shutdown.

use std::collections::HashMap;
use std::io::BufReader;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use tokio::sync::Mutex as AsyncMutex;

use crate::config::{Config, SIDE_SUFFIXES};
use crate::conn::Conn;
use crate::dump;
use crate::metrics::DbMetrics;
use crate::reader::{ReadGate, Readers};
use crate::stream::Streams;
use crate::writer::Writer;

/// sqld's "replication log is dirty" marker: its presence makes sqld rebuild its log from the database file, so
/// rolling back to sqld never replays an old log over what xsqld wrote (README §1.2). Created, never removed.
const SENTINEL: &str = ".sentinel";

/// A database being built from a dump is this file next to its final place (its WAL and the like follow it)
const LOADING: &str = ".xsqld-loading";

pub struct Db {
    pub name: Arc<str>,
    pub writer: Writer,
    pub readers: Arc<Readers>,
    pub metrics: Arc<DbMetrics>,
}

pub struct Registry {
    pub cfg: Arc<Config>,
    pub streams: Arc<Streams>,
    open: Arc<parking_lot::RwLock<HashMap<Arc<str>, Arc<Db>>>>,
    /// Per database: opening, creating and deleting it, one at a time (other databases are not held up, e.g. while
    /// one is built from a long dump). Opening, creating and deleting hand the guard to their blocking work, so a
    /// cancelled HTTP request cannot let a second one start while the first is still running. An entry lives only
    /// while someone holds or waits for it, so names that never become databases leave nothing behind.
    locks: Locks,
    /// Tests only: how long the first open of a database takes, in milliseconds
    #[cfg(test)]
    open_delay: std::sync::atomic::AtomicU64,
    /// `GET /dump` in progress, each with a copy of its database on disk (`--max-concurrent-dumps`)
    pub dumps: Arc<tokio::sync::Semaphore>,
}

#[derive(Debug)]
pub enum AdminError {
    Exists(String),
    Missing(String),
    BadName(String),
    BadDump(String),
    Internal(anyhow::Error),
}

impl From<anyhow::Error> for AdminError {
    fn from(e: anyhow::Error) -> Self {
        AdminError::Internal(e)
    }
}

impl Registry {
    pub fn new(cfg: Arc<Config>) -> Arc<Self> {
        let streams = Streams::new(cfg.stream_idle());
        Arc::new(Self {
            dumps: Arc::new(tokio::sync::Semaphore::new(cfg.max_concurrent_dumps)),
            cfg,
            streams,
            open: Default::default(),
            locks: Default::default(),
            #[cfg(test)]
            open_delay: Default::default(),
        })
    }

    /// Built before waiting, so that a request cancelled while it waits also cleans up after itself
    async fn lock(&self, ns: &str) -> NsGuard {
        let entry = self.locks.lock().entry(ns.to_owned()).or_default().clone();
        let mut g = NsGuard {
            locks: self.locks.clone(),
            ns: ns.to_owned(),
            entry: Some(entry.clone()),
            guard: None,
        };
        g.guard = Some(entry.lock_owned().await);
        g
    }

    fn path(&self, ns: &str) -> PathBuf {
        self.cfg.db_file(ns)
    }

    pub fn exists(&self, ns: &str) -> bool {
        self.cfg.usable_namespace(ns) && self.path(ns).is_file()
    }

    /// The open database, opening it if its file exists; None if it does not exist
    pub async fn get(&self, ns: &str) -> anyhow::Result<Option<Arc<Db>>> {
        if let Some(db) = self.open.read().get(ns) {
            return Ok(Some(db.clone()));
        }
        if !self.exists(ns) {
            return Ok(None);
        }
        let guard = self.lock(ns).await;
        if let Some(db) = self.open.read().get(ns) {
            return Ok(Some(db.clone()));
        }
        self.start(ns, guard).await
    }

    /// Opens a database and registers it, all under its lock: the blocking work goes on when the request waiting
    /// for it is cancelled, and with the lock let go at that point a DELETE could remove the files in between, after
    /// which the open created the database again (SQLite creates a missing file) or registered one whose files were
    /// gone. None if the database does not exist (any more).
    async fn start(&self, ns: &str, guard: NsGuard) -> anyhow::Result<Option<Arc<Db>>> {
        let path = self.path(ns);
        let name: Arc<str> = ns.into();
        let (cfg, streams, open) = (self.cfg.clone(), self.streams.clone(), self.open.clone());
        #[cfg(test)]
        let delay = std::time::Duration::from_millis(self.open_delay.load(std::sync::atomic::Ordering::Relaxed));
        tokio::task::spawn_blocking(move || -> anyhow::Result<Option<Arc<Db>>> {
            let _guard = guard;
            #[cfg(test)]
            std::thread::sleep(delay);
            if !path.is_file() {
                return Ok(None);
            }
            ensure_sentinel(&path)?;
            let metrics = Arc::new(DbMetrics::default());
            let gate = ReadGate::new(metrics.clone());
            let writer = Writer::start(&path, cfg.clone(), streams, metrics.clone(), gate.clone())?;
            let db = Arc::new(Db {
                name: name.clone(),
                writer,
                readers: Arc::new(Readers::new(path, cfg, gate)),
                metrics,
            });
            open.write().insert(name.clone(), db.clone());
            tracing::info!(ns = &*name, "database opened");
            Ok(Some(db))
        })
        .await?
    }

    pub fn open_dbs(&self) -> Vec<Arc<Db>> {
        let mut v: Vec<_> = self.open.read().values().cloned().collect();
        v.sort_by(|a, b| a.name.cmp(&b.name));
        v
    }

    /// `POST /v1/namespaces/<ns>/create`: an empty database, or one built from an SQL dump (`dump` yields its bytes).
    /// It is opened on first use.
    pub async fn create(&self, ns: &str, dump: Option<DumpRx>) -> Result<(), AdminError> {
        if !self.cfg.usable_namespace(ns) {
            return Err(AdminError::BadName(ns.to_owned()));
        }
        let guard = self.lock(ns).await;
        if self.exists(ns) {
            return Err(AdminError::Exists(ns.to_owned()));
        }
        let path = self.path(ns);
        let cfg = self.cfg.clone();
        tokio::task::spawn_blocking(move || {
            let _guard = guard;
            build(&path, &cfg, dump)
        })
        .await
        .map_err(|e| AdminError::Internal(e.into()))??;
        tracing::info!(ns, "database created");
        Ok(())
    }

    /// `DELETE /v1/namespaces/<ns>`: stops the write thread (open transaction rolled back, queued requests answered
    /// with 503), forgets its streams and removes the files
    pub async fn delete(&self, ns: &str) -> Result<(), AdminError> {
        if !self.cfg.usable_namespace(ns) {
            return Err(AdminError::BadName(ns.to_owned()));
        }
        let guard = self.lock(ns).await;
        let db = self.open.write().remove(ns);
        if db.is_none() && !self.exists(ns) {
            return Err(AdminError::Missing(ns.to_owned()));
        }
        self.streams.kill_namespace(ns);
        let path = self.path(ns);
        let per_dir = self.cfg.per_namespace_dir();
        tokio::task::spawn_blocking(move || -> anyhow::Result<()> {
            let _guard = guard;
            if let Some(db) = db {
                db.writer.stop();
            }
            remove_files(&path, per_dir)
        })
        .await
        .map_err(|e| AdminError::Internal(e.into()))??;
        tracing::info!(ns, "database deleted");
        Ok(())
    }

    /// Stops every write thread (each checkpoints its WAL into the database file)
    pub fn shutdown(&self) {
        let dbs: Vec<_> = self.open.write().drain().map(|(_, d)| d).collect();
        for db in dbs {
            db.writer.stop();
        }
    }
}

type Locks = Arc<parking_lot::Mutex<HashMap<String, Arc<AsyncMutex<()>>>>>;

/// The bytes of an SQL dump on their way in: chunks, then `Ok(None)` when the source has ended properly. A sender
/// that goes away without it (the download was abandoned) is an error for the reader: what arrived so far is not the
/// whole dump, and may well end between two statements.
pub type DumpRx = tokio::sync::mpsc::Receiver<std::io::Result<Option<bytes::Bytes>>>;

/// A database's lock, held. Dropping it releases the lock and removes the map entry when nobody else holds or
/// waits for it: a waiter clones the entry under the map lock, which is held here too, so it is never removed
/// under a waiter.
struct NsGuard {
    locks: Locks,
    ns: String,
    entry: Option<Arc<AsyncMutex<()>>>,
    guard: Option<tokio::sync::OwnedMutexGuard<()>>,
}

impl Drop for NsGuard {
    fn drop(&mut self) {
        let mut locks = self.locks.lock();
        drop(self.guard.take());
        drop(self.entry.take());
        if locks.get(&self.ns).is_some_and(|m| Arc::strong_count(m) == 1) {
            locks.remove(&self.ns);
        }
    }
}

/// Creates `.sentinel` and makes it durable (the file and its directory entry): a sentinel lost in a power cut
/// would let sqld, after a rollback, replay its old replication log over what xsqld wrote
fn ensure_sentinel(db: &Path) -> std::io::Result<()> {
    let Some(dir) = db.parent() else { return Ok(()) };
    let s = dir.join(SENTINEL);
    if !s.exists() {
        std::fs::File::create(s)?.sync_all()?;
        std::fs::File::open(dir)?.sync_all()?;
    }
    Ok(())
}

/// `<db><suffix>`
fn beside(db: &Path, suffix: &str) -> PathBuf {
    let mut p = db.as_os_str().to_owned();
    p.push(suffix);
    PathBuf::from(p)
}

/// Removes these files if they exist; returns the ones removed
fn remove_existing(files: impl IntoIterator<Item = PathBuf>) -> std::io::Result<Vec<PathBuf>> {
    let mut removed = Vec::new();
    for p in files {
        match std::fs::remove_file(&p) {
            Ok(()) => removed.push(p),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(e),
        }
    }
    Ok(removed)
}

/// Removes the files SQLite keeps next to a database file that does not exist: left there by a crash while the
/// database was being deleted (the main file already gone). A new database of that name would otherwise take over
/// the old WAL and show the old content: SQLite checks WAL frames against the WAL's own header, not against the
/// database file. Returns the files removed.
fn remove_stale_side_files(db: &Path) -> std::io::Result<Vec<PathBuf>> {
    if db.exists() {
        return Ok(Vec::new());
    }
    remove_existing(SIDE_SUFFIXES.iter().map(|s| beside(db, s)))
}

/// What a build from a dump that did not finish leaves: the file being loaded and the files SQLite keeps next to it
fn remove_loading_files(db: &Path) -> std::io::Result<Vec<PathBuf>> {
    let tmp = beside(db, LOADING);
    remove_existing(std::iter::once("").chain(SIDE_SUFFIXES).map(|s| beside(&tmp, s)))
}

/// At startup, before any database is opened: removes what a crash can leave next to database files, i.e. the
/// files of a build from a dump that did not finish (`<db>.xsqld-loading*`) and `-wal` / `-shm` / `-journal` files
/// whose database file is gone. Databases are found by `--db-file-template`, in the directory that holds its first
/// part with `{ns}`. Returns the files removed.
pub fn clean_leftovers(cfg: &Config) -> std::io::Result<Vec<PathBuf>> {
    let template = cfg.db_file_template.replace('\\', "/");
    let parts: Vec<&str> = template.split('/').collect();
    let Some(k) = parts.iter().position(|p| p.contains("{ns}")) else {
        return Ok(Vec::new());
    };
    let base = parts[..k].iter().fold(cfg.db_path.clone(), |p, s| p.join(s));
    let (pre, post) = parts[k].split_once("{ns}").unwrap_or_default();
    let is_file = k == parts.len() - 1;
    let entries = match std::fs::read_dir(&base) {
        Ok(d) => d,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(e),
    };
    let after_name: Vec<String> = SIDE_SUFFIXES
        .iter()
        .map(|s| s.to_string())
        .chain(
            std::iter::once("")
                .chain(SIDE_SUFFIXES)
                .map(|s| format!("{LOADING}{s}")),
        )
        .collect();
    let mut seen = std::collections::HashSet::new();
    let mut removed = Vec::new();
    for entry in entries {
        let Some(name) = entry?.file_name().to_str().map(str::to_owned) else {
            continue;
        };
        // a database's file (or directory), or a file name with what SQLite or a build puts after it
        let mut stems = vec![name.as_str()];
        if is_file {
            stems.extend(after_name.iter().filter_map(|s| name.strip_suffix(s.as_str())));
        }
        for stem in stems {
            let Some(ns) = stem.strip_prefix(pre).and_then(|s| s.strip_suffix(post)) else {
                continue;
            };
            if !cfg.usable_namespace(ns) || !seen.insert(ns.to_owned()) {
                continue;
            }
            let db = cfg.db_file(ns);
            removed.extend(remove_loading_files(&db)?);
            removed.extend(remove_stale_side_files(&db)?);
        }
    }
    Ok(removed)
}

/// Builds the database file next to its final place and moves it there when complete, so a half-loaded database is
/// never visible
///
/// A dump is limited to `--dump-load-max-statement-bytes` per statement and `--dump-load-timeout-ms` in all,
/// counted from here (the download goes on meanwhile, so it is included); SQL still running then is interrupted.
fn build(path: &Path, cfg: &Config, dump: Option<DumpRx>) -> Result<(), AdminError> {
    let deadline = std::time::Instant::now() + cfg.dump_load_timeout();
    let dir = path
        .parent()
        .ok_or_else(|| AdminError::Internal(anyhow::anyhow!("database path has no directory")))?;
    std::fs::create_dir_all(dir).map_err(|e| AdminError::Internal(e.into()))?;
    let tmp = beside(path, LOADING);
    let _ = remove_files(&tmp, false);
    let result = (|| -> Result<(), AdminError> {
        let conn = Conn::open_loader(&tmp, cfg).map_err(|e| AdminError::Internal(e.into()))?;
        if let Some(rx) = dump {
            let limits = dump::LoadLimits {
                max_statement_bytes: cfg.dump_load_max_statement_bytes,
                deadline,
            };
            conn.set_deadline(Some(deadline));
            let input = BufReader::with_capacity(256 * 1024, ChannelReader::new(rx, deadline));
            dump::load(&conn, input, &limits).map_err(|e| AdminError::BadDump(e.to_string()))?;
            conn.set_deadline(None);
        }
        conn.c
            .execute_batch("PRAGMA synchronous = FULL; PRAGMA wal_checkpoint(TRUNCATE);")
            .map_err(|e| AdminError::Internal(e.into()))?;
        drop(conn);
        std::fs::File::open(&tmp)
            .and_then(|f| f.sync_all())
            .map_err(|e| AdminError::Internal(e.into()))?;
        // the name is free (checked under the lock), so SQLite files under it belong to no database
        for stale in remove_stale_side_files(path).map_err(|e| AdminError::Internal(e.into()))? {
            tracing::warn!(file = %stale.display(), "removed a file left by a database deleted before");
        }
        std::fs::rename(&tmp, path).map_err(|e| AdminError::Internal(e.into()))?;
        std::fs::File::open(dir)
            .and_then(|d| d.sync_all())
            .map_err(|e| AdminError::Internal(e.into()))?;
        Ok(())
    })();
    if result.is_err() {
        let _ = remove_files(&tmp, false);
    }
    result
}

fn remove_files(db: &Path, per_dir: bool) -> anyhow::Result<()> {
    if per_dir && let Some(dir) = db.parent() {
        match std::fs::remove_dir_all(dir) {
            Err(e) if e.kind() != std::io::ErrorKind::NotFound => return Err(e.into()),
            _ => return Ok(()),
        }
    }
    for suffix in std::iter::once("").chain(SIDE_SUFFIXES) {
        match std::fs::remove_file(beside(db, suffix)) {
            Err(e) if e.kind() != std::io::ErrorKind::NotFound => return Err(e.into()),
            _ => {}
        }
    }
    Ok(())
}

/// Blocking `Read` over the chunks of an HTTP body; the end of the data is the end marker of [`DumpRx`]
struct ChannelReader {
    rx: DumpRx,
    cur: bytes::Bytes,
    ended: bool,
    /// Waiting for data ends here (`--dump-load-timeout-ms`)
    deadline: std::time::Instant,
    rt: tokio::runtime::Handle,
}

impl ChannelReader {
    /// On a blocking thread of the runtime that receives the dump
    fn new(rx: DumpRx, deadline: std::time::Instant) -> Self {
        Self {
            rx,
            cur: bytes::Bytes::new(),
            ended: false,
            deadline,
            rt: tokio::runtime::Handle::current(),
        }
    }
}

impl std::io::Read for ChannelReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        while self.cur.is_empty() {
            if self.ended {
                return Ok(0);
            }
            let next = self
                .rt
                .block_on(tokio::time::timeout_at(self.deadline.into(), self.rx.recv()));
            match next {
                Err(_) => {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::TimedOut,
                        "the dump took longer than --dump-load-timeout-ms",
                    ));
                }
                Ok(Some(Ok(Some(chunk)))) => self.cur = chunk,
                Ok(Some(Ok(None))) => self.ended = true,
                Ok(Some(Err(e))) => return Err(e),
                Ok(None) => {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::UnexpectedEof,
                        "the dump download was abandoned before its end",
                    ));
                }
            }
        }
        let n = buf.len().min(self.cur.len());
        buf[..n].copy_from_slice(&self.cur.split_to(n));
        Ok(n)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn registry() -> (tempfile::TempDir, Arc<Registry>) {
        let dir = tempfile::tempdir().unwrap();
        let cfg = Arc::new(Config::for_tests(dir.path()));
        (dir, Registry::new(cfg))
    }

    /// A complete dump in these chunks
    fn chunks(parts: &[&str]) -> DumpRx {
        let (tx, rx) = tokio::sync::mpsc::channel(parts.len() + 1);
        for p in parts {
            tx.try_send(Ok(Some(bytes::Bytes::copy_from_slice(p.as_bytes()))))
                .unwrap();
        }
        tx.try_send(Ok(None)).unwrap();
        rx
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn create_delete_cycle_uses_sqld_layout() {
        let (dir, r) = registry();
        assert!(r.get("tenant").await.unwrap().is_none());
        r.create("tenant", None).await.unwrap();
        assert!(dir.path().join("dbs/tenant/data").is_file());
        assert!(matches!(r.create("tenant", None).await, Err(AdminError::Exists(_))));
        assert!(r.get("tenant").await.unwrap().is_some());
        assert!(dir.path().join("dbs/tenant/.sentinel").is_file());
        r.delete("tenant").await.unwrap();
        assert!(!dir.path().join("dbs/tenant").exists());
        assert!(matches!(r.delete("tenant").await, Err(AdminError::Missing(_))));
        assert!(matches!(r.create("../x", None).await, Err(AdminError::BadName(_))));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_long_build_does_not_hold_up_other_databases() {
        // review 2318 S4/S5: one lock for all databases kept every first open waiting while a dump loaded
        let (_dir, r) = registry();
        let (tx, rx) = tokio::sync::mpsc::channel(4);
        tx.send(Ok(Some(bytes::Bytes::from_static(
            b"PRAGMA foreign_keys=OFF;\nBEGIN TRANSACTION;\nCREATE TABLE t(a);\n",
        ))))
        .await
        .unwrap();
        let slow = {
            let r = r.clone();
            tokio::spawn(async move { r.create("slow", Some(rx)).await })
        };
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        let quick = async {
            r.create("other", None).await.unwrap();
            assert!(r.get("other").await.unwrap().is_some());
            r.delete("other").await.unwrap();
        };
        tokio::time::timeout(std::time::Duration::from_secs(2), quick)
            .await
            .expect("another database waited for the build");
        assert!(!slow.is_finished());
        tx.send(Ok(Some(bytes::Bytes::from_static(b"COMMIT;\n"))))
            .await
            .unwrap();
        tx.send(Ok(None)).await.unwrap();
        drop(tx);
        slow.await.unwrap().unwrap();
        assert!(r.get("slow").await.unwrap().is_some());
    }

    /// Review of 0.1.0, finding 5: a download abandoned at any point (before the first byte, between two statements,
    /// after the last one) left a database with whatever had arrived
    #[tokio::test(flavor = "multi_thread")]
    async fn an_abandoned_dump_download_creates_nothing() {
        let (dir, r) = registry();
        let whole = [
            "PRAGMA foreign_keys=OFF;\nBEGIN TRANSACTION;\n",
            "CREATE TABLE t(a);\n",
            "INSERT INTO t VALUES(1);\n",
            "COMMIT;\n",
        ];
        for arrived in 0..=whole.len() {
            let (tx, rx) = tokio::sync::mpsc::channel(8);
            for part in &whole[..arrived] {
                tx.send(Ok(Some(bytes::Bytes::from_static(part.as_bytes()))))
                    .await
                    .unwrap();
            }
            drop(tx);
            let e = r.create("tenant", Some(rx)).await.unwrap_err();
            assert!(
                matches!(&e, AdminError::BadDump(m) if m.contains("abandoned")),
                "{arrived} chunks: {e:?}"
            );
            assert!(r.get("tenant").await.unwrap().is_none(), "{arrived} chunks");
            let left: Vec<_> = std::fs::read_dir(dir.path().join("dbs/tenant"))
                .map(|d| d.map(|e| e.unwrap().file_name()).collect())
                .unwrap_or_default();
            assert!(
                left.iter().all(|f| f == ".sentinel"),
                "{arrived} chunks: files left behind: {left:?}"
            );
        }
        r.create("tenant", Some(chunks(&whole))).await.unwrap();
        assert!(r.get("tenant").await.unwrap().is_some());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn create_from_dump_and_failed_load_leaves_nothing() {
        let (dir, r) = registry();
        let good = [
            "PRAGMA foreign_keys=OFF;\nBEGIN TRANSACTION;\nCREATE TABLE IF NOT EXISTS t(a);\nINSERT INTO t ",
            "VALUES(1);\nCOMMIT;\n",
        ];
        r.create("restore-check", Some(chunks(&good))).await.unwrap();
        let db = r.get("restore-check").await.unwrap().unwrap();
        drop(db);
        let c = rusqlite::Connection::open(dir.path().join("dbs/restore-check/data")).unwrap();
        assert_eq!(
            c.query_row("SELECT a FROM t", [], |row| row.get::<_, i64>(0)).unwrap(),
            1
        );

        let bad = [
            "PRAGMA foreign_keys=OFF;\nBEGIN TRANSACTION;\nCREATE TABLE t(a);\nINSERT INTO nope VALUES(1);\nCOMMIT;\n",
        ];
        assert!(matches!(
            r.create("other", Some(chunks(&bad))).await,
            Err(AdminError::BadDump(_))
        ));
        assert!(!r.exists("other"));
        let leftovers: Vec<_> = std::fs::read_dir(dir.path().join("dbs/other")).unwrap().collect();
        assert!(leftovers.is_empty(), "{leftovers:?}");
    }

    fn tables(path: &Path) -> i64 {
        let c = rusqlite::Connection::open(path).unwrap();
        c.query_row("SELECT count(*) FROM sqlite_schema", [], |r| r.get(0))
            .unwrap()
    }

    /// Review of 0.1.0: killed while deleting a database (its file removed, its non-empty `-wal` not yet), a new
    /// database of that name took over the old WAL and showed the old content
    #[tokio::test(flavor = "multi_thread")]
    async fn a_new_database_does_not_take_over_a_stale_wal() {
        let (dir, r) = registry();
        let path = dir.path().join("dbs/tenant/data");
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        let old = rusqlite::Connection::open(&path).unwrap();
        old.execute_batch("PRAGMA journal_mode = WAL; PRAGMA wal_autocheckpoint = 0; CREATE TABLE secret(v); INSERT INTO secret VALUES ('old');")
            .unwrap();
        let wal = std::fs::read(beside(&path, "-wal")).unwrap();
        assert!(!wal.is_empty());
        drop(old);
        // what the crash left: no database file, its WAL
        std::fs::remove_file(&path).unwrap();
        std::fs::write(beside(&path, "-wal"), &wal).unwrap();
        assert!(!r.exists("tenant"));
        r.create("tenant", None).await.unwrap();
        assert!(r.get("tenant").await.unwrap().is_some());
        assert_eq!(tables(&path), 0, "the new database shows the old one's tables");
    }

    /// Review of 0.1.0: right after a restart, a first request to a database cancelled while the database opened,
    /// then a DELETE of it: the open, still going on, created the file again
    #[tokio::test(flavor = "multi_thread")]
    async fn a_cancelled_first_open_does_not_bring_a_deleted_database_back() {
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = Config::for_tests(dir.path());
        cfg.db_file_template = "{ns}.db".into();
        let r = Registry::new(Arc::new(cfg));
        r.create("tenant", None).await.unwrap();
        r.open_delay.store(300, std::sync::atomic::Ordering::Relaxed);
        let first = {
            let r = r.clone();
            tokio::spawn(async move { r.get("tenant").await.map(|d| d.is_some()) })
        };
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        first.abort();
        let _ = first.await;
        r.open_delay.store(0, std::sync::atomic::Ordering::Relaxed);
        r.delete("tenant").await.unwrap();
        // the open has long finished either way
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        assert!(!r.exists("tenant"), "the deleted database is back");
        assert!(r.open_dbs().is_empty());
        assert!(r.get("tenant").await.unwrap().is_none());
    }

    /// Review of 0.1.0: the files of a build that crashed stayed until a database of that name was created again
    #[test]
    fn leftovers_of_a_crash_are_removed_at_startup() {
        for template in ["dbs/{ns}/data", "{ns}.db"] {
            let dir = tempfile::tempdir().unwrap();
            let mut cfg = Config::for_tests(dir.path());
            cfg.db_file_template = template.into();
            let write = |ns: &str, suffix: &str| {
                let p = beside(&cfg.db_file(ns), suffix);
                std::fs::create_dir_all(p.parent().unwrap()).unwrap();
                std::fs::write(&p, b"x").unwrap();
                p
            };
            let gone = [
                write("built", ".xsqld-loading"),
                write("built", ".xsqld-loading-wal"),
                write("built", ".xsqld-loading-journal"),
                write("deleted", "-wal"),
                write("deleted", "-shm"),
                // a database in use and its WAL stay, the files of a build of its name do not
                write("live", ".xsqld-loading"),
            ];
            let kept = [write("live", ""), write("live", "-wal"), write("live", "-shm")];
            let other = dir.path().join("unrelated-wal");
            std::fs::write(&other, b"x").unwrap();
            let mut removed = clean_leftovers(&cfg).unwrap();
            removed.sort();
            let mut expected = gone.to_vec();
            expected.sort();
            assert_eq!(removed, expected, "{template}");
            for p in gone {
                assert!(!p.exists(), "{template}: {}", p.display());
            }
            for p in kept.iter().chain([&other]) {
                assert!(p.exists(), "{template}: {}", p.display());
            }
            assert!(clean_leftovers(&cfg).unwrap().is_empty());
        }
    }

    /// Review of 0.1.0: under a template such as `{ns}/data` the database `xsqld-tmp` is the server's scratch
    /// directory, emptied at every start
    #[tokio::test(flavor = "multi_thread")]
    async fn the_scratch_directory_is_not_a_database() {
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = Config::for_tests(dir.path());
        cfg.db_file_template = "{ns}/data".into();
        let r = Registry::new(Arc::new(cfg));
        assert!(matches!(r.create("xsqld-tmp", None).await, Err(AdminError::BadName(_))));
        std::fs::create_dir_all(dir.path().join("xsqld-tmp")).unwrap();
        rusqlite::Connection::open(dir.path().join("xsqld-tmp/data")).unwrap();
        assert!(r.get("xsqld-tmp").await.unwrap().is_none());
        assert!(matches!(r.delete("xsqld-tmp").await, Err(AdminError::BadName(_))));
        r.create("tenant", None).await.unwrap();
    }

    /// Review of 0.1.0: a dump beyond the load limits fails and leaves no database
    #[tokio::test(flavor = "multi_thread")]
    async fn a_dump_beyond_the_load_limits_creates_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = Config::for_tests(dir.path());
        cfg.dump_load_max_statement_bytes = 1024;
        cfg.dump_load_timeout_ms = 300;
        let r = Registry::new(Arc::new(cfg));
        let long = format!(
            "PRAGMA foreign_keys=OFF;\nBEGIN TRANSACTION;\nCREATE TABLE t(a);\nINSERT INTO t VALUES('{}');\nCOMMIT;\n",
            "x".repeat(2000)
        );
        let e = r.create("long", Some(chunks(&[&long]))).await.unwrap_err();
        assert!(
            matches!(&e, AdminError::BadDump(m) if m.contains("--dump-load-max-statement-bytes")),
            "{e:?}"
        );
        // a download that never ends: the load gives up at its own limit
        let (tx, rx) = tokio::sync::mpsc::channel(4);
        tx.send(Ok(Some(bytes::Bytes::from_static(
            b"PRAGMA foreign_keys=OFF;\nBEGIN TRANSACTION;\n",
        ))))
        .await
        .unwrap();
        let started = std::time::Instant::now();
        let e = r.create("slow", Some(rx)).await.unwrap_err();
        assert!(
            matches!(&e, AdminError::BadDump(m) if m.contains("--dump-load-timeout-ms")),
            "{e:?}"
        );
        assert!(started.elapsed() < std::time::Duration::from_secs(3));
        drop(tx);
        for ns in ["long", "slow"] {
            assert!(!r.exists(ns));
            let left: Vec<_> = std::fs::read_dir(dir.path().join("dbs").join(ns)).unwrap().collect();
            assert!(left.is_empty(), "{ns}: {left:?}");
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn database_locks_are_forgotten_once_nobody_holds_them() {
        let (_dir, r) = registry();
        for i in 0..1000 {
            assert!(matches!(
                r.delete(&format!("missing{i}")).await,
                Err(AdminError::Missing(_))
            ));
        }
        assert!(matches!(
            r.create("bad", Some(chunks(&["INSERT INTO nope VALUES(1);\n"]))).await,
            Err(AdminError::BadDump(_))
        ));
        r.create("tenant", None).await.unwrap();
        assert!(r.get("tenant").await.unwrap().is_some());
        r.delete("tenant").await.unwrap();
        assert_eq!(r.locks.lock().len(), 0);

        // held: a waiter keeps the entry; a waiter cancelled while waiting removes its share
        let held = r.lock("busy").await;
        let waiter = {
            let r = r.clone();
            tokio::spawn(async move { drop(r.lock("busy").await) })
        };
        let cancelled = {
            let r = r.clone();
            tokio::spawn(async move { drop(r.lock("busy").await) })
        };
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        cancelled.abort();
        let _ = cancelled.await;
        assert_eq!(r.locks.lock().len(), 1);
        drop(held);
        waiter.await.unwrap();
        assert_eq!(r.locks.lock().len(), 0);

        let held = r.lock("busy").await;
        let cancelled = {
            let r = r.clone();
            tokio::spawn(async move { drop(r.lock("busy").await) })
        };
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        drop(held);
        cancelled.abort();
        let _ = cancelled.await;
        assert_eq!(r.locks.lock().len(), 0, "the last waiter, cancelled, left its entry");
    }
}
