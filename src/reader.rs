//! Read pool of one database: connections opened read-only, used on blocking threads, so reads never wait for the
//! writer and never block it (WAL). A request runs here only if every statement is read-only by
//! `sqlite3_stmt_readonly()`; otherwise, or if it would leave a transaction open, it goes to the write thread (it has
//! changed nothing, so running it again there is safe).

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use parking_lot::Mutex;
use tokio::sync::Semaphore;

use crate::config::Config;
use crate::conn::Conn;
use crate::exec::{self, Exec, Results};
use crate::proto::StreamRequest;
use crate::sql::{self, Begin, Kind};

pub struct Readers {
    path: PathBuf,
    cfg: Arc<Config>,
    idle: Mutex<Vec<Conn>>,
    permits: Arc<Semaphore>,
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
    pub fn new(path: PathBuf, cfg: Arc<Config>) -> Self {
        let n = cfg.read_pool_size;
        Self {
            path,
            cfg,
            idle: Mutex::new(Vec::with_capacity(n)),
            permits: Arc::new(Semaphore::new(n)),
        }
    }

    pub async fn run(self: &Arc<Self>, requests: Vec<StreamRequest>, deadline: Instant) -> anyhow::Result<ReadOutcome> {
        let permit = match tokio::time::timeout_at(deadline.into(), self.permits.clone().acquire_owned()).await {
            Ok(p) => p?,
            Err(_) => return Ok(ReadOutcome::Deadline),
        };
        let this = self.clone();
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            let conn = match this.idle.lock().pop() {
                Some(c) => c,
                None => Conn::open_reader(&this.path, &this.cfg)?,
            };
            let out = run_read(&conn, requests, deadline, this.cfg.max_response_bytes);
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

fn run_read(conn: &Conn, requests: Vec<StreamRequest>, deadline: Instant, budget: u64) -> ReadOutcome {
    // statements that fail before running give the same error on either side
    let read_only = exec::statements(&requests).all(|s| {
        sql::classify(s) == Kind::Empty
            || sql::many_statements(s)
            || conn.c.prepare_cached(s).is_ok_and(|st| st.readonly())
    });
    if !read_only {
        return ReadOutcome::NeedsWriter(requests);
    }
    conn.set_deadline(Some(deadline));
    let mut ex = Exec::native(conn, budget);
    let results = match ex.run(&requests) {
        Ok(r) => r,
        Err(exec::GroupLost) => unreachable!("native execution has no group"),
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
        (dir, Arc::new(Readers::new(path, cfg)), w)
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
