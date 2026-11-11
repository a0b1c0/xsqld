//! Databases (sqld "namespaces"): one file each, `--db-path` + `--db-file-template`. A database exists when its file
//! exists; it is opened on first use (write thread + read pool) and stays open until deleted or shutdown.

use std::collections::HashMap;
use std::io::BufReader;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use tokio::sync::Mutex as AsyncMutex;

use crate::config::{Config, valid_namespace};
use crate::conn::Conn;
use crate::dump;
use crate::metrics::DbMetrics;
use crate::reader::Readers;
use crate::stream::Streams;
use crate::writer::Writer;

/// sqld's "replication log is dirty" marker: its presence makes sqld rebuild its log from the database file, so
/// rolling back to sqld never replays an old log over what xsqld wrote (README §1.2). Created, never removed.
const SENTINEL: &str = ".sentinel";

pub struct Db {
    pub name: Arc<str>,
    pub writer: Writer,
    pub readers: Arc<Readers>,
    pub metrics: Arc<DbMetrics>,
}

pub struct Registry {
    pub cfg: Arc<Config>,
    pub streams: Arc<Streams>,
    open: parking_lot::RwLock<HashMap<Arc<str>, Arc<Db>>>,
    /// Per database: opening, creating and deleting it, one at a time (other databases are not held up, e.g. while
    /// one is built from a long dump). Creating and deleting hand the guard to their blocking work, so a cancelled
    /// HTTP request cannot let a second one start while the first is still running.
    locks: parking_lot::Mutex<HashMap<String, Arc<AsyncMutex<()>>>>,
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
        })
    }

    async fn lock(&self, ns: &str) -> tokio::sync::OwnedMutexGuard<()> {
        let m = self.locks.lock().entry(ns.to_owned()).or_default().clone();
        m.lock_owned().await
    }

    fn path(&self, ns: &str) -> PathBuf {
        self.cfg.db_file(ns)
    }

    pub fn exists(&self, ns: &str) -> bool {
        valid_namespace(ns) && self.path(ns).is_file()
    }

    /// The open database, opening it if its file exists; None if it does not exist
    pub async fn get(&self, ns: &str) -> anyhow::Result<Option<Arc<Db>>> {
        if let Some(db) = self.open.read().get(ns) {
            return Ok(Some(db.clone()));
        }
        if !self.exists(ns) {
            return Ok(None);
        }
        let _g = self.lock(ns).await;
        if let Some(db) = self.open.read().get(ns) {
            return Ok(Some(db.clone()));
        }
        if !self.exists(ns) {
            return Ok(None);
        }
        let db = self.start(ns).await?;
        Ok(Some(db))
    }

    async fn start(&self, ns: &str) -> anyhow::Result<Arc<Db>> {
        let path = self.path(ns);
        let name: Arc<str> = ns.into();
        let metrics = Arc::new(DbMetrics::default());
        let (cfg, streams, m, p) = (self.cfg.clone(), self.streams.clone(), metrics.clone(), path.clone());
        let writer = tokio::task::spawn_blocking(move || {
            ensure_sentinel(&p)?;
            Writer::start(&p, cfg, streams, m)
        })
        .await??;
        let db = Arc::new(Db {
            name: name.clone(),
            writer,
            readers: Arc::new(Readers::new(path, self.cfg.clone())),
            metrics,
        });
        self.open.write().insert(name, db.clone());
        tracing::info!(ns, "database opened");
        Ok(db)
    }

    pub fn open_dbs(&self) -> Vec<Arc<Db>> {
        let mut v: Vec<_> = self.open.read().values().cloned().collect();
        v.sort_by(|a, b| a.name.cmp(&b.name));
        v
    }

    /// `POST /v1/namespaces/<ns>/create`: an empty database, or one built from an SQL dump (`dump` yields its bytes).
    /// It is opened on first use.
    pub async fn create(
        &self,
        ns: &str,
        dump: Option<tokio::sync::mpsc::Receiver<std::io::Result<bytes::Bytes>>>,
    ) -> Result<(), AdminError> {
        if !valid_namespace(ns) {
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
        if !valid_namespace(ns) {
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

fn ensure_sentinel(db: &Path) -> std::io::Result<()> {
    let Some(dir) = db.parent() else { return Ok(()) };
    let s = dir.join(SENTINEL);
    if !s.exists() {
        std::fs::File::create(s)?;
    }
    Ok(())
}

/// Builds the database file next to its final place and moves it there when complete, so a half-loaded database is
/// never visible
fn build(
    path: &Path,
    cfg: &Config,
    dump: Option<tokio::sync::mpsc::Receiver<std::io::Result<bytes::Bytes>>>,
) -> Result<(), AdminError> {
    let dir = path
        .parent()
        .ok_or_else(|| AdminError::Internal(anyhow::anyhow!("database path has no directory")))?;
    std::fs::create_dir_all(dir).map_err(|e| AdminError::Internal(e.into()))?;
    let mut tmp = path.as_os_str().to_owned();
    tmp.push(".xsqld-loading");
    let tmp = PathBuf::from(tmp);
    let _ = remove_files(&tmp, false);
    let result = (|| -> Result<(), AdminError> {
        let conn = Conn::open_loader(&tmp, cfg).map_err(|e| AdminError::Internal(e.into()))?;
        if let Some(rx) = dump {
            dump::load(&conn, BufReader::with_capacity(256 * 1024, ChannelReader::new(rx)))
                .map_err(|e| AdminError::BadDump(e.to_string()))?;
        }
        conn.c
            .execute_batch("PRAGMA synchronous = FULL; PRAGMA wal_checkpoint(TRUNCATE);")
            .map_err(|e| AdminError::Internal(e.into()))?;
        drop(conn);
        std::fs::File::open(&tmp)
            .and_then(|f| f.sync_all())
            .map_err(|e| AdminError::Internal(e.into()))?;
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
    for suffix in ["", "-wal", "-shm", "-journal"] {
        let mut p = db.as_os_str().to_owned();
        p.push(suffix);
        match std::fs::remove_file(PathBuf::from(p)) {
            Err(e) if e.kind() != std::io::ErrorKind::NotFound => return Err(e.into()),
            _ => {}
        }
    }
    Ok(())
}

/// Blocking `Read` over the chunks of an HTTP body
struct ChannelReader {
    rx: tokio::sync::mpsc::Receiver<std::io::Result<bytes::Bytes>>,
    cur: bytes::Bytes,
}

impl ChannelReader {
    fn new(rx: tokio::sync::mpsc::Receiver<std::io::Result<bytes::Bytes>>) -> Self {
        Self {
            rx,
            cur: bytes::Bytes::new(),
        }
    }
}

impl std::io::Read for ChannelReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        while self.cur.is_empty() {
            match self.rx.blocking_recv() {
                Some(chunk) => self.cur = chunk?,
                None => return Ok(0),
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

    fn chunks(parts: &[&str]) -> tokio::sync::mpsc::Receiver<std::io::Result<bytes::Bytes>> {
        let (tx, rx) = tokio::sync::mpsc::channel(parts.len().max(1));
        for p in parts {
            tx.try_send(Ok(bytes::Bytes::copy_from_slice(p.as_bytes()))).unwrap();
        }
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
        tx.send(Ok(bytes::Bytes::from_static(
            b"PRAGMA foreign_keys=OFF;\nBEGIN TRANSACTION;\nCREATE TABLE t(a);\n",
        )))
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
        tx.send(Ok(bytes::Bytes::from_static(b"COMMIT;\n"))).await.unwrap();
        drop(tx);
        slow.await.unwrap().unwrap();
        assert!(r.get("slow").await.unwrap().is_some());
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
}
