//! Read pool of one database: connections opened read-only, used on blocking threads, so reads never wait for the
//! writer and never block it (WAL). A request runs here only if every statement is read-only by
//! `sqlite3_stmt_readonly()`; otherwise, or if it would leave a transaction open, it goes to the write thread (it has
//! changed nothing, so running it again there is safe).
//!
//! Every read request passes the database's [`ReadGate`] before it starts, which the write thread closes for a
//! moment when the WAL cannot be restarted otherwise (checkpoint starvation, see [`ReadGate`]).

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::{Condvar, Mutex};
use tokio::sync::Semaphore;

use crate::config::Config;
use crate::conn::Conn;
use crate::exec::{self, Exec, Results};
use crate::metrics::DbMetrics;
use crate::proto::StreamRequest;
use crate::sql::{self, Begin, Kind};

pub struct Readers {
    path: PathBuf,
    cfg: Arc<Config>,
    idle: Mutex<Vec<Conn>>,
    permits: Arc<Semaphore>,
    /// Additional connections split statements may hold, all together (`--parallel-pool-percent`)
    parallel_permits: Arc<Semaphore>,
    /// Statements (by text) not split for a reason that will recur: too many groups, sums that are not exact integers
    /// (both found by splitting), or a table or plan that does not allow it, with the schema version it was found at
    unsplittable: Mutex<std::collections::HashMap<String, (crate::metrics::Fallback, Option<i64>)>>,
    /// Estimated rows of tables (by name) and when they were estimated (split aggregates: whether a table is large)
    sizes: Mutex<std::collections::HashMap<String, (Option<i128>, Instant)>>,
    pub gate: Arc<ReadGate>,
}

/// How long an estimate of a table's rows is used before it is made again: tables grow slowly, and missing the moment
/// a table grows past `--parallel-min-rows` by this much only delays splitting
const SIZE_TTL: Duration = Duration::from_secs(10);

/// Most statement texts remembered as not worth splitting; the list starts over when full
const UNSPLITTABLE_MAX: usize = 1024;

/// Additional read connections lent to one split statement (README §3.1); back to the pool when dropped
pub struct Lease<'r> {
    readers: &'r Readers,
    pub conns: Vec<Conn>,
    _permits: (tokio::sync::SemaphorePermit<'r>, tokio::sync::SemaphorePermit<'r>),
}

impl Drop for Lease<'_> {
    fn drop(&mut self) {
        let mut idle = self.readers.idle.lock();
        for c in self.conns.drain(..) {
            c.set_deadline(None);
            // one whose read transaction could not be ended is closed, never lent again with an old snapshot
            if c.is_autocommit() {
                idle.push(c);
            }
        }
    }
}

/// Holds back new read transactions of one database while the write thread runs a RESTART checkpoint that keeps
/// failing because of readers (checkpoint starvation).
///
/// Why: SQLite starts the WAL over only once no read transaction uses it. Reads that follow each other closely,
/// while writes go on, always leave one reader on the WAL, so without help the WAL grows for as long as that
/// pattern lasts. With the gate closed no read starts, the write thread waits for the reads already running to end
/// (at most `--checkpoint-reader-wait-ms`) and then restarts the WAL; reads that start once every frame is in the
/// database file do not use the WAL at all, so opening the gate again does not undo it.
///
/// Bounds: the write thread keeps the gate closed for at most `--checkpoint-reader-wait-ms` plus the time of one
/// RESTART, and only when the RESTART before it failed; after a closed-gate RESTART that still failed (a read ran
/// longer than the wait), the gate stays open for at least four times that wait before the next try. So a read
/// request is held back at most once per try and never past its own deadline, and reads are never held back for
/// more than about a fifth of the time. Nothing a read does is refused or changed: it only starts later.
pub struct ReadGate {
    state: Mutex<GateState>,
    changed: Condvar,
    pub metrics: Arc<DbMetrics>,
}

#[derive(Default)]
struct GateState {
    closed: bool,
    /// Read requests between [`ReadGate::enter`] and the end of their work
    active: usize,
}

/// A read request that passed the gate; ends its share when dropped
pub struct ReadPass<'g>(&'g ReadGate);

impl Drop for ReadPass<'_> {
    fn drop(&mut self) {
        let mut st = self.0.state.lock();
        st.active -= 1;
        if st.active == 0 {
            self.0.changed.notify_all();
        }
    }
}

/// The gate, closed; opened again when dropped
pub struct ClosedGate<'g>(&'g ReadGate);

impl Drop for ClosedGate<'_> {
    fn drop(&mut self) {
        self.0.state.lock().closed = false;
        self.0.changed.notify_all();
    }
}

impl ClosedGate<'_> {
    /// Waits until no read request that passed the gate before it closed is still running, at most `wait`;
    /// returns whether they all ended
    pub fn drain(&self, wait: Duration) -> bool {
        let until = Instant::now() + wait;
        let mut st = self.0.state.lock();
        while st.active > 0 {
            if self.0.changed.wait_until(&mut st, until).timed_out() {
                return st.active == 0;
            }
        }
        true
    }
}

impl ReadGate {
    pub fn new(metrics: Arc<DbMetrics>) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(GateState::default()),
            changed: Condvar::new(),
            metrics,
        })
    }

    /// Lets a read request in, waiting while the gate is closed; None if `deadline` came first. Blocking: called on
    /// the read's own blocking thread.
    pub fn enter(&self, deadline: Instant) -> Option<ReadPass<'_>> {
        let mut st = self.state.lock();
        if st.closed {
            let started = Instant::now();
            while st.closed {
                if self.changed.wait_until(&mut st, deadline).timed_out() && st.closed {
                    self.metrics.read_gate_wait.record(started.elapsed());
                    return None;
                }
            }
            self.metrics.read_gate_wait.record(started.elapsed());
        }
        st.active += 1;
        Some(ReadPass(self))
    }

    /// Read requests that passed the gate and are still running
    #[cfg(test)]
    pub fn active(&self) -> usize {
        self.state.lock().active
    }

    /// Closes the gate: read requests arriving from now on wait until the returned guard is dropped
    pub fn close(&self) -> ClosedGate<'_> {
        self.state.lock().closed = true;
        ClosedGate(self)
    }
}

pub enum ReadOutcome {
    Done {
        results: Results,
        closed: bool,
    },
    /// Not a read-only request: run it on the writer
    NeedsWriter(Vec<StreamRequest>),
    Deadline,
}

impl Readers {
    pub fn new(path: PathBuf, cfg: Arc<Config>, gate: Arc<ReadGate>) -> Self {
        let n = cfg.read_pool_size;
        Self {
            path,
            idle: Mutex::new(Vec::with_capacity(n)),
            permits: Arc::new(Semaphore::new(n)),
            parallel_permits: Arc::new(Semaphore::new(cfg.parallel_permits())),
            unsplittable: Mutex::new(std::collections::HashMap::new()),
            sizes: Mutex::new(std::collections::HashMap::new()),
            cfg,
            gate,
        }
    }

    #[cfg(test)]
    pub fn path(&self) -> &std::path::Path {
        &self.path
    }

    pub fn cfg(&self) -> &Config {
        &self.cfg
    }

    /// Estimated rows of `table`: made by `estimate` at most every [`SIZE_TTL`] per table
    pub fn table_rows(&self, table: &str, estimate: impl FnOnce() -> Option<i128>) -> Option<i128> {
        let key = table.to_ascii_lowercase();
        if let Some((n, at)) = self.sizes.lock().get(&key)
            && at.elapsed() < SIZE_TTL
        {
            return *n;
        }
        let n = estimate();
        let mut m = self.sizes.lock();
        if m.len() >= UNSPLITTABLE_MAX {
            m.clear();
        }
        m.insert(key, (n, Instant::now()));
        n
    }

    pub fn unsplittable(&self, sql: &str) -> Option<(crate::metrics::Fallback, Option<i64>)> {
        self.unsplittable.lock().get(sql).copied()
    }

    pub fn mark_unsplittable(&self, sql: &str, why: crate::metrics::Fallback, schema: Option<i64>) {
        let mut m = self.unsplittable.lock();
        if m.len() >= UNSPLITTABLE_MAX {
            m.clear();
        }
        // statements longer than the cache limit of compiled statements carry their values inline and do not repeat
        if sql.len() <= 8 * 1024 {
            m.insert(sql.to_owned(), (why, schema));
        }
    }

    pub fn metrics(&self) -> &DbMetrics {
        &self.gate.metrics
    }

    /// Up to `want` additional connections for a split statement, as many as are free right now (never waiting),
    /// with `deadline` set; None if not even one is
    pub fn lease_extra(&self, want: usize, deadline: Instant) -> Option<Lease<'_>> {
        let (share, pool) = (1..=want as u32).rev().find_map(|k| {
            let share = self.parallel_permits.try_acquire_many(k).ok()?;
            let pool = self.permits.try_acquire_many(k).ok()?;
            Some((share, pool))
        })?;
        let k = pool.num_permits();
        let mut conns = Vec::with_capacity(k);
        while conns.len() < k {
            let c = match self.idle.lock().pop() {
                Some(c) => c,
                None => Conn::open_reader(&self.path, &self.cfg).ok()?,
            };
            c.set_deadline(Some(deadline));
            conns.push(c);
        }
        Some(Lease {
            readers: self,
            conns,
            _permits: (share, pool),
        })
    }

    pub async fn run(self: &Arc<Self>, requests: Vec<StreamRequest>, deadline: Instant) -> anyhow::Result<ReadOutcome> {
        let permit = match tokio::time::timeout_at(deadline.into(), self.permits.clone().acquire_owned()).await {
            Ok(p) => p?,
            Err(_) => return Ok(ReadOutcome::Deadline),
        };
        let this = self.clone();
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            let Some(_pass) = this.gate.enter(deadline) else {
                return Ok(ReadOutcome::Deadline);
            };
            let conn = match this.idle.lock().pop() {
                Some(c) => c,
                None => Conn::open_reader(&this.path, &this.cfg)?,
            };
            let ctx = crate::parallel::Ctx {
                readers: &this,
                deadline,
            };
            let out = run_read(&conn, requests, deadline, this.cfg.max_response_bytes, &ctx);
            this.idle.lock().push(conn);
            Ok(out)
        })
        .await?
    }
}

/// Could this request be a read? (cheap look at the leading keywords; the real check is in [`run_read`])
pub fn looks_read_only(requests: &[StreamRequest]) -> bool {
    exec::statements(requests).all(|s| {
        matches!(
            sql::classify(s),
            Kind::Select | Kind::Begin(Begin::Deferred) | Kind::Commit | Kind::Rollback | Kind::Empty
        )
    })
}

fn run_read(
    conn: &Conn,
    requests: Vec<StreamRequest>,
    deadline: Instant,
    budget: u64,
    ctx: &crate::parallel::Ctx<'_>,
) -> ReadOutcome {
    // statements that fail before running give the same error on either side
    let read_only = exec::statements(&requests).all(|s| {
        sql::classify(s) == Kind::Empty || sql::many_statements(s) || conn.prepare(s).is_ok_and(|st| st.readonly())
    });
    if !read_only {
        return ReadOutcome::NeedsWriter(requests);
    }
    conn.set_deadline(Some(deadline));
    let mut ex = Exec::native(conn, budget).with_parallel(ctx);
    let results = match ex.run(&requests) {
        Ok(r) => r,
        Err(_) => unreachable!("native execution has no group"),
    };
    conn.set_deadline(None);
    if !conn.is_autocommit() {
        conn.rollback_all();
        return ReadOutcome::NeedsWriter(requests);
    }
    ReadOutcome::Done {
        results,
        closed: ex.closed,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::PipelineReq;

    fn reqs(json: &str) -> Vec<StreamRequest> {
        serde_json::from_str::<PipelineReq>(&format!(r#"{{"requests":{json}}}"#))
            .unwrap()
            .requests
    }

    fn setup() -> (tempfile::TempDir, Arc<Readers>, Conn) {
        let dir = tempfile::tempdir().unwrap();
        let cfg = Arc::new(Config::for_tests(dir.path()));
        let path = dir.path().join("data");
        let w = Conn::open_writer(&path, &cfg).unwrap();
        w.c.execute_batch("CREATE TABLE t(a); INSERT INTO t VALUES (1), (2);")
            .unwrap();
        let gate = ReadGate::new(Arc::new(DbMetrics::default()));
        (dir, Arc::new(Readers::new(path, cfg, gate)), w)
    }

    fn later() -> Instant {
        Instant::now() + std::time::Duration::from_secs(5)
    }

    #[tokio::test]
    async fn reads_run_here_and_writes_go_to_the_writer() {
        let (_d, r, _w) = setup();
        assert!(looks_read_only(&reqs(
            r#"[{"type":"execute","stmt":{"sql":"SELECT * FROM t"}},{"type":"close"}]"#
        )));
        assert!(!looks_read_only(&reqs(
            r#"[{"type":"execute","stmt":{"sql":"BEGIN IMMEDIATE"}}]"#
        )));
        let ReadOutcome::Done { results, closed } = r
            .run(
                reqs(r#"[{"type":"execute","stmt":{"sql":"SELECT count(*) FROM t"}},{"type":"close"}]"#),
                later(),
            )
            .await
            .unwrap()
        else {
            panic!()
        };
        assert!(closed);
        assert_eq!(
            serde_json::to_value(&results).unwrap()[0]["response"]["result"]["rows"][0][0]["value"],
            "2"
        );
        // a SELECT that writes through a function would be caught by stmt_readonly; a CTE that deletes is too
        let out = r
            .run(
                reqs(r#"[{"type":"execute","stmt":{"sql":"WITH x AS (SELECT 1) DELETE FROM t"}}]"#),
                later(),
            )
            .await
            .unwrap();
        assert!(matches!(out, ReadOutcome::NeedsWriter(_)));
        let out = r
            .run(
                reqs(r#"[{"type":"execute","stmt":{"sql":"SELECT * FROM nope"}}]"#),
                later(),
            )
            .await
            .unwrap();
        assert!(
            matches!(out, ReadOutcome::NeedsWriter(_)),
            "unknown tables are resolved on the writer"
        );
    }

    #[tokio::test]
    async fn read_transaction_left_open_goes_to_writer() {
        let (_d, r, _w) = setup();
        let out = r
            .run(
                reqs(r#"[{"type":"execute","stmt":{"sql":"BEGIN"}},{"type":"execute","stmt":{"sql":"SELECT 1"}}]"#),
                later(),
            )
            .await
            .unwrap();
        assert!(matches!(out, ReadOutcome::NeedsWriter(_)));
        let out = r
            .run(
                reqs(r#"[{"type":"batch","batch":{"steps":[{"stmt":{"sql":"BEGIN"}},{"stmt":{"sql":"SELECT * FROM t"}},{"stmt":{"sql":"COMMIT"}}]}}]"#),
                later(),
            )
            .await
            .unwrap();
        assert!(matches!(out, ReadOutcome::Done { .. }));
    }

    #[tokio::test]
    async fn reads_see_committed_writes() {
        let (_d, r, w) = setup();
        let q = || reqs(r#"[{"type":"execute","stmt":{"sql":"SELECT count(*) FROM t"}}]"#);
        let _ = r.run(q(), later()).await.unwrap();
        w.c.execute("INSERT INTO t VALUES (3)", []).unwrap();
        let ReadOutcome::Done { results, .. } = r.run(q(), later()).await.unwrap() else {
            panic!()
        };
        assert_eq!(
            serde_json::to_value(&results).unwrap()[0]["response"]["result"]["rows"][0][0]["value"],
            "3"
        );
    }

    /// Slow reads (here interrupted by their deadline after 1.5 s) mixed with quick ones: with the default pool the
    /// quick ones do not queue behind them
    #[tokio::test(flavor = "multi_thread")]
    async fn quick_reads_do_not_wait_for_slow_ones() {
        let (_d, r, _w) = setup();
        let slow_until = Instant::now() + std::time::Duration::from_millis(1500);
        let slow: Vec<_> = (0..8)
            .map(|_| {
                let r = r.clone();
                tokio::spawn(async move {
                    let q = reqs(
                        r#"[{"type":"execute","stmt":{"sql":"WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n) SELECT count(*) FROM n"}}]"#,
                    );
                    r.run(q, slow_until).await.unwrap()
                })
            })
            .collect();
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        let started = Instant::now();
        let out = r
            .run(
                reqs(r#"[{"type":"execute","stmt":{"sql":"SELECT count(*) FROM t"}}]"#),
                later(),
            )
            .await
            .unwrap();
        assert!(matches!(out, ReadOutcome::Done { .. }));
        assert!(
            started.elapsed() < std::time::Duration::from_millis(500),
            "quick read waited {:?}",
            started.elapsed()
        );
        for s in slow {
            assert!(matches!(s.await.unwrap(), ReadOutcome::Done { .. }));
        }
    }
}
