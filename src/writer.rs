//! The write thread of one database: the only writer, so SQLite never answers "busy" to a client. Write requests
//! wait in a queue; the thread takes up to `--group-max` of them, runs them inside one real transaction (each request
//! in a savepoint, see `exec.rs`) and commits once, so many client transactions share one fsync. Replies are sent
//! only after that COMMIT returned.
//!
//! Requests that cannot share the group transaction run on their own ("solo"): PRAGMA, SAVEPOINT/RELEASE, VACUUM,
//! a request that leaves its transaction open, a request during which SQLite rolled back the group, and every
//! request of a group whose COMMIT failed. A request that leaves a transaction open pins the thread to its stream:
//! the thread then only runs that stream's requests, and rolls the transaction back after `--pinned-tx-timeout-ms`,
//! when the client goes away, or on shutdown. The lock is never held by nobody.
//!
//! Checkpoints: once the WAL holds `--checkpoint-pages` frames the thread wakes the database's checkpoint thread,
//! which runs PASSIVE on its own connection while writes go on (SQLite lets a passive checkpoint run beside the
//! writer). Between groups the write thread itself runs RESTART once the WAL passes `--checkpoint-restart-bytes`,
//! and PASSIVE after `--checkpoint-idle-ms` without work; once `--checkpoint-pages` frames have been written since
//! the checkpoint thread was woken, it waits for that run to end. Neither shrinks the WAL file: the next write starts the
//! WAL over inside blocks that are already allocated, where an fdatasync() needs no file system journal commit. A
//! WAL growing from empty pays one on every commit, so the file is only truncated on shutdown.
//!
//! A RESTART that fails because readers keep using the WAL (reads following each other closely leave one on it at
//! every moment) is tried again with the database's [`ReadGate`] closed: no new read starts, the reads already
//! running get up to `--checkpoint-reader-wait-ms` to end, then the WAL restarts (bounds in [`ReadGate`]).

use std::collections::VecDeque;
use std::path::Path;
use std::sync::atomic::{AtomicI64, Ordering::Relaxed};
use std::sync::mpsc::{self, RecvTimeoutError};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use rusqlite::wal::CheckpointMode;
use tokio::sync::oneshot;

use crate::config::Config;
use crate::conn::{Conn, REQUEST_SP};
use crate::exec::{self, Exec, Results};
use crate::metrics::DbMetrics;
use crate::proto::StreamRequest;
use crate::reader::ReadGate;
use crate::sql;
use crate::stream::Streams;

pub struct Job {
    pub requests: Vec<StreamRequest>,
    pub stream: u64,
    /// The client's stream holds an open transaction on this thread
    pub expect_pinned: bool,
    pub enqueued: Instant,
    pub deadline: Instant,
    pub reply: oneshot::Sender<Outcome>,
    /// Already known to need running on its own
    solo: bool,
}

impl Job {
    pub fn new(
        requests: Vec<StreamRequest>,
        stream: u64,
        expect_pinned: bool,
        deadline: Instant,
    ) -> (Job, oneshot::Receiver<Outcome>) {
        let (reply, rx) = oneshot::channel();
        let solo = exec::statements(&requests).any(|s| sql::classify(s).needs_solo());
        (
            Job {
                requests,
                stream,
                expect_pinned,
                enqueued: Instant::now(),
                deadline,
                reply,
                solo,
            },
            rx,
        )
    }
}

#[derive(Debug)]
pub enum Outcome {
    Done {
        results: Results,
        closed: bool,
        pinned: bool,
    },
    /// Waited in the queue longer than `--queue-timeout-ms`; nothing ran
    QueueTimeout,
    /// `--max-write-queue` requests were already waiting; nothing ran
    QueueFull,
    /// The request's time limit passed before it could run; nothing ran
    Deadline,
    /// The stream's open transaction is gone (timed out); nothing ran
    StreamExpired,
    /// The database is being deleted or the thread stopped
    Unavailable,
}

enum Msg {
    Job(Job),
    Stop(oneshot::Sender<()>),
}

/// Handle to the write thread
pub struct Writer {
    tx: mpsc::Sender<Msg>,
    thread: Mutex<Option<JoinHandle<()>>>,
    metrics: Arc<DbMetrics>,
    max_queue: i64,
}

impl Writer {
    pub fn start(
        path: &Path,
        cfg: Arc<Config>,
        streams: Arc<Streams>,
        metrics: Arc<DbMetrics>,
        gate: Arc<ReadGate>,
    ) -> anyhow::Result<Writer> {
        let (tx, rx) = mpsc::channel();
        let max_queue = cfg.max_write_queue as i64;
        let mut w = Thread::open(path, cfg, rx, streams, metrics.clone(), gate)?;
        let thread = std::thread::Builder::new()
            .name("xsqld-writer".into())
            .spawn(move || w.run())?;
        Ok(Writer {
            tx,
            thread: Mutex::new(Some(thread)),
            metrics,
            max_queue,
        })
    }

    /// Queues a job; answered at once with [`Outcome::QueueFull`] when the queue is full (the stream holding an open
    /// transaction is still let in, it is what the queue waits for) or [`Outcome::Unavailable`] if the thread is gone
    pub fn submit(&self, job: Job) {
        if self.metrics.queue_len.fetch_add(1, Relaxed) >= self.max_queue && !job.expect_pinned {
            self.metrics.queue_len.fetch_sub(1, Relaxed);
            self.metrics.queue_full.fetch_add(1, Relaxed);
            let _ = job.reply.send(Outcome::QueueFull);
            return;
        }
        if let Err(mpsc::SendError(Msg::Job(job))) = self.tx.send(Msg::Job(job)) {
            self.metrics.queue_len.fetch_sub(1, Relaxed);
            let _ = job.reply.send(Outcome::Unavailable);
        }
    }

    /// Stops the thread: queued jobs are answered with [`Outcome::Unavailable`], an open transaction is rolled back,
    /// the WAL is checkpointed into the database file. Blocks until the thread has exited.
    pub fn stop(&self) {
        let (ack, done) = oneshot::channel();
        if self.tx.send(Msg::Stop(ack)).is_ok() {
            let _ = done.blocking_recv();
        }
        if let Some(t) = self.thread.lock().unwrap_or_else(|e| e.into_inner()).take() {
            let _ = t.join();
        }
    }
}

fn page_size(conn: &Conn) -> rusqlite::Result<i64> {
    conn.c.query_row("PRAGMA page_size", [], |r| r.get(0))
}

/// Runs one checkpoint and records its duration. Returns the frames in the WAL and the frames now in the database
/// file (equal: the checkpoint is complete), or None when it could not run.
fn checkpoint(conn: &Conn, mode: CheckpointMode, metrics: &DbMetrics) -> Option<(i64, i64)> {
    let blocking = !matches!(mode, CheckpointMode::PASSIVE);
    // RESTART and TRUNCATE wait for readers through the busy handler: keep that wait short, writes are queueing
    // meanwhile
    if blocking {
        let _ = conn.c.busy_timeout(Duration::from_millis(20));
    }
    let started = Instant::now();
    let r = conn.c.wal_checkpoint_v2(None::<&str>, mode);
    let took = started.elapsed();
    if blocking {
        let _ = conn.c.busy_timeout(Duration::from_millis(100));
        metrics.checkpoint_restart.record(took);
    } else {
        metrics.checkpoint_passive.record(took);
    }
    if took > Duration::from_millis(100) {
        tracing::warn!(ms = took.as_millis() as u64, blocking, "slow checkpoint");
    }
    match r {
        Ok((log, done)) => {
            // short of complete: readers still use frames
            if log != done {
                metrics.checkpoint_busy.fetch_add(1, Relaxed);
            }
            Some((log as i64, done as i64))
        }
        Err(e) => {
            if e.sqlite_error_code() == Some(rusqlite::ErrorCode::DatabaseBusy) {
                metrics.checkpoint_busy.fetch_add(1, Relaxed);
            } else {
                tracing::warn!(error = %e, "checkpoint failed");
            }
            None
        }
    }
}

/// What the write thread knows of the checkpoint thread
#[derive(Default)]
struct CkptState {
    /// Bumped by the write thread when the WAL started over; a run that began before reports nothing
    generation: u64,
    running: bool,
    /// Frames copied into the database file by the last finished run of this generation
    done: i64,
}

/// Thread running PASSIVE checkpoints of one database when the write thread asks for one
struct Checkpointer {
    kick: mpsc::SyncSender<()>,
    thread: JoinHandle<()>,
    state: Arc<(Mutex<CkptState>, Condvar)>,
}

impl Checkpointer {
    fn start(conn: Conn, metrics: Arc<DbMetrics>) -> std::io::Result<Checkpointer> {
        // capacity 1: requests made while a checkpoint is running fold into one more run
        let (kick, rx) = mpsc::sync_channel::<()>(1);
        let state = Arc::new((Mutex::new(CkptState::default()), Condvar::new()));
        let s = state.clone();
        let thread = std::thread::Builder::new()
            .name("xsqld-checkpoint".into())
            .spawn(move || {
                let (m, ended) = &*s;
                let lock = || m.lock().unwrap_or_else(|e| e.into_inner());
                while rx.recv().is_ok() {
                    let generation = {
                        let mut st = lock();
                        st.running = true;
                        st.generation
                    };
                    let r = checkpoint(&conn, CheckpointMode::PASSIVE, &metrics);
                    let mut st = lock();
                    st.running = false;
                    if let Some((_, done)) = r
                        && st.generation == generation
                    {
                        st.done = done;
                    }
                    ended.notify_all();
                }
            })?;
        Ok(Checkpointer { kick, thread, state })
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, CkptState> {
        self.state.0.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn request(&self) {
        let _ = self.kick.try_send(());
    }

    /// Whether a run is going on and how far the last one got; `restarted`: the WAL started over since the last look
    fn observe(&self, restarted: bool) -> (bool, i64) {
        let mut st = self.lock();
        if restarted {
            st.generation += 1;
            st.done = 0;
        }
        (st.running, st.done)
    }

    /// Waits for the running checkpoint to finish; returns how far it got
    fn wait(&self) -> i64 {
        let st = self.state.1.wait_while(self.lock(), |s| s.running);
        st.unwrap_or_else(|e| e.into_inner()).done
    }

    /// The write thread checkpointed itself up to `done` frames
    fn record(&self, done: i64) {
        self.lock().done = done;
    }

    /// Waits for a running checkpoint to finish and ends the thread
    fn stop(self) {
        drop(self.kick);
        let _ = self.thread.join();
    }
}

fn install_wal_hook(conn: &mut Conn, frames: &Arc<AtomicI64>) -> rusqlite::Result<()> {
    let f = frames.clone();
    conn.c.wal_hook(Some(move |_: &mut rusqlite::hooks::Wal, pages: i32| {
        f.store(pages as i64, Relaxed);
        Ok(())
    }))
}

struct Thread {
    conn: Conn,
    cfg: Arc<Config>,
    rx: mpsc::Receiver<Msg>,
    streams: Arc<Streams>,
    metrics: Arc<DbMetrics>,
    /// Frames in the WAL after the last commit (set by the WAL hook)
    frames: Arc<AtomicI64>,
    /// Bytes of a WAL frame are this plus 24
    page_size: i64,
    /// WAL frames after the previous commit (fewer now: the WAL started over)
    last_frames: i64,
    /// A RESTART between groups failed (readers): not tried again before the WAL holds this many frames
    restart_after: i64,
    /// The last RESTART failed and the WAL has not started over since: the next one closes the read gate
    starved: bool,
    /// A RESTART with the gate closed failed too: the next one waits until then, so that reads get the time between
    gate_retry_at: Option<Instant>,
    gate: Arc<ReadGate>,
    /// WAL frames when the checkpoint thread was last woken: about where its run stops copying
    kicked_at: i64,
    /// Taken on shutdown
    checkpointer: Option<Checkpointer>,
    /// Jobs taken from the channel but not yet run (kept in order)
    backlog: VecDeque<Job>,
    /// Stream holding an open transaction on this connection, and since when
    pinned: Option<(u64, Instant)>,
    stop: Option<oneshot::Sender<()>>,
    /// Something was committed since the last complete checkpoint
    dirty: bool,
}

impl Thread {
    fn open(
        path: &Path,
        cfg: Arc<Config>,
        rx: mpsc::Receiver<Msg>,
        streams: Arc<Streams>,
        metrics: Arc<DbMetrics>,
        gate: Arc<ReadGate>,
    ) -> anyhow::Result<Thread> {
        let mut conn = Conn::open_writer(path, &cfg)?;
        let frames = Arc::new(AtomicI64::new(0));
        install_wal_hook(&mut conn, &frames)?;
        let checkpointer = Checkpointer::start(Conn::open_writer(path, &cfg)?, metrics.clone())?;
        let page_size = page_size(&conn)?;
        Ok(Thread {
            conn,
            cfg,
            rx,
            streams,
            metrics,
            frames,
            page_size,
            last_frames: 0,
            restart_after: 0,
            starved: false,
            gate_retry_at: None,
            gate,
            kicked_at: 0,
            checkpointer: Some(checkpointer),
            backlog: VecDeque::new(),
            pinned: None,
            stop: None,
            dirty: false,
        })
    }

    fn run(&mut self) {
        while self.stop.is_none() {
            if self.pinned.is_some() {
                self.pinned_step();
                continue;
            }
            let job = match self.backlog.pop_front() {
                Some(j) => j,
                None => match self.idle_wait() {
                    Ok(Msg::Job(j)) => j,
                    Ok(Msg::Stop(ack)) => {
                        self.stop = Some(ack);
                        break;
                    }
                    Err(RecvTimeoutError::Timeout) => {
                        self.metrics.idle_wakeups.fetch_add(1, Relaxed);
                        self.idle_checkpoint();
                        continue;
                    }
                    Err(RecvTimeoutError::Disconnected) => break,
                },
            };
            let Some(job) = self.admit(job) else { continue };
            if job.expect_pinned {
                let _ = job.reply.send(Outcome::StreamExpired);
                continue;
            }
            if job.solo {
                self.metrics.solo_requests.fetch_add(1, Relaxed);
                self.run_solo(job);
            } else {
                let group = self.gather(job);
                self.run_group(group);
            }
            self.after_commit();
        }
        self.shutdown();
    }

    /// Waits for the next message with nothing queued. Wakes up after `--checkpoint-idle-ms` only when there is
    /// something to checkpoint; with nothing to do (all checkpointed, or `--checkpoint-idle-ms 0`: no idle
    /// checkpoint) it sleeps until the next message, so an idle database costs no CPU.
    fn idle_wait(&self) -> Result<Msg, RecvTimeoutError> {
        if self.dirty && self.cfg.checkpoint_idle_ms > 0 {
            self.rx.recv_timeout(self.cfg.checkpoint_idle())
        } else {
            self.rx.recv().map_err(|_| RecvTimeoutError::Disconnected)
        }
    }

    /// Takes a job off the queue: answers it without running when its client is gone or it waited too long
    fn admit(&self, job: Job) -> Option<Job> {
        self.metrics.queue_len.fetch_sub(1, Relaxed);
        if job.reply.is_closed() {
            return None;
        }
        let now = Instant::now();
        if now.duration_since(job.enqueued) >= self.cfg.queue_timeout() {
            self.metrics.queue_timeouts.fetch_add(1, Relaxed);
            let _ = job.reply.send(Outcome::QueueTimeout);
            return None;
        }
        if now >= job.deadline {
            self.metrics.deadline_exceeded.fetch_add(1, Relaxed);
            let _ = job.reply.send(Outcome::Deadline);
            return None;
        }
        Some(job)
    }

    /// More jobs for the group: whatever is already queued, up to `--group-max`, optionally lingering a little.
    /// Stops before a job that must run on its own (it stays first in the backlog).
    fn gather(&mut self, first: Job) -> Vec<Job> {
        let mut group = vec![first];
        let linger_until =
            (self.cfg.group_linger_us > 0).then(|| Instant::now() + Duration::from_micros(self.cfg.group_linger_us));
        while group.len() < self.cfg.group_max {
            let next = match self.backlog.pop_front() {
                Some(j) => j,
                None => {
                    let msg = match linger_until {
                        Some(until) => match self.rx.recv_timeout(until.saturating_duration_since(Instant::now())) {
                            Ok(m) => m,
                            Err(_) => break,
                        },
                        None => match self.rx.try_recv() {
                            Ok(m) => m,
                            Err(_) => break,
                        },
                    };
                    match msg {
                        Msg::Job(j) => j,
                        Msg::Stop(ack) => {
                            self.stop = Some(ack);
                            break;
                        }
                    }
                }
            };
            if next.expect_pinned || next.solo {
                self.backlog.push_front(next);
                break;
            }
            if let Some(j) = self.admit(next) {
                group.push(j);
            }
        }
        group
    }

    fn run_group(&mut self, jobs: Vec<Job>) {
        let mut pending: VecDeque<Job> = jobs.into();
        let mut solo: Vec<Job> = Vec::new();
        'group: while !pending.is_empty() {
            if !begin_group(&self.conn) {
                solo.extend(pending.drain(..));
                break;
            }
            // from here to the COMMIT below nothing a request runs may commit: the requests before it would stay
            // written and be redone. A COMMIT that gets through the statement classification is rolled back instead
            // and handled as a lost group.
            self.conn.refuse_commits(true);
            let started = Instant::now();
            let mut done: Vec<(Job, Results, bool)> = Vec::with_capacity(pending.len());
            while let Some(job) = pending.pop_front() {
                if !done.is_empty() && started.elapsed() >= self.cfg.group_max_exec() {
                    // commit what is done now; the rest goes back to the front of the queue
                    self.metrics.group_cuts.fetch_add(1, Relaxed);
                    pending.push_front(job);
                    for (i, j) in pending.drain(..).enumerate() {
                        self.metrics.queue_len.fetch_add(1, Relaxed);
                        self.backlog.insert(i, j);
                    }
                    break;
                }
                if Instant::now() >= job.deadline {
                    self.metrics.deadline_exceeded.fetch_add(1, Relaxed);
                    let _ = job.reply.send(Outcome::Deadline);
                    continue;
                }
                let lost =
                    |t: &mut Self, job: Job, done: &mut Vec<(Job, Results, bool)>, pending: &mut VecDeque<Job>| {
                        // SQLite rolled the whole group back: redo the others, this one on its own afterwards
                        t.metrics.group_lost.fetch_add(1, Relaxed);
                        t.conn.rollback_all();
                        for (j, _, _) in done.drain(..).rev() {
                            pending.push_front(j);
                        }
                        job
                    };
                if self.conn.c.execute_batch(&format!("SAVEPOINT {REQUEST_SP}")).is_err() {
                    solo.push(lost(self, job, &mut done, &mut pending));
                    continue 'group;
                }
                self.conn.set_deadline(Some(job.deadline));
                let mut ex = Exec::grouped(&self.conn, self.cfg.max_response_bytes);
                let r = ex.run(&job.requests);
                let (open, closed) = (ex.client_tx_open(), ex.closed);
                self.conn.set_deadline(None);
                // the client's transaction stays open past this request, or a deferred foreign key is unresolved
                // where sqld checks it (at a statement's own commit, see `exec.rs`; a later request of the group must
                // not resolve it)
                let run_alone =
                    matches!(r, Err(exec::Stop::RunAlone)) || (r.is_ok() && (open || self.conn.deferred_fk_pending()));
                match r {
                    Err(exec::Stop::GroupLost) => {
                        solo.push(lost(self, job, &mut done, &mut pending));
                        continue 'group;
                    }
                    _ if run_alone => {
                        // undo it here, run it alone
                        let undo = format!("ROLLBACK TO {REQUEST_SP}; RELEASE {REQUEST_SP};");
                        if self.conn.c.execute_batch(&undo).is_err() || self.conn.is_autocommit() {
                            solo.push(lost(self, job, &mut done, &mut pending));
                            continue 'group;
                        }
                        solo.push(job);
                    }
                    Ok(results) => {
                        if self.conn.c.execute_batch(&format!("RELEASE {REQUEST_SP}")).is_err()
                            || self.conn.is_autocommit()
                        {
                            solo.push(lost(self, job, &mut done, &mut pending));
                            continue 'group;
                        }
                        done.push((job, results, closed));
                    }
                    Err(exec::Stop::RunAlone) => unreachable!("handled above"),
                }
            }
            if done.is_empty() {
                self.conn.rollback_all();
                break;
            }
            let commit_started = Instant::now();
            self.conn.refuse_commits(false);
            match self.conn.c.execute_batch("COMMIT") {
                Ok(()) => {
                    let took = commit_started.elapsed();
                    self.metrics.commit.record(took);
                    if took > Duration::from_millis(100) {
                        tracing::warn!(ms = took.as_millis() as u64, requests = done.len(), "slow commit");
                    }
                    self.metrics.groups.fetch_add(1, Relaxed);
                    self.metrics.group_requests.fetch_add(done.len() as u64, Relaxed);
                    self.metrics.group_size_max.fetch_max(done.len() as u64, Relaxed);
                    self.dirty = true;
                    for (job, results, closed) in done {
                        let _ = job.reply.send(Outcome::Done {
                            results,
                            closed,
                            pinned: false,
                        });
                    }
                }
                Err(e) => {
                    // e.g. a deferred foreign key: SQLite checks those only at the outermost COMMIT
                    tracing::warn!(error = %e, requests = done.len(), "group commit failed, running its requests one by one");
                    self.metrics.group_commit_failures.fetch_add(1, Relaxed);
                    self.conn.rollback_all();
                    let mut redo: Vec<Job> = done.into_iter().map(|(j, _, _)| j).collect();
                    redo.append(&mut solo);
                    solo = redo;
                }
            }
            break;
        }
        // no group transaction is open past this point (requests on their own commit as written)
        self.conn.refuse_commits(false);
        self.metrics.solo_requests.fetch_add(solo.len() as u64, Relaxed);
        let mut solo = solo.into_iter();
        for job in solo.by_ref() {
            if Instant::now() >= job.deadline {
                self.metrics.deadline_exceeded.fetch_add(1, Relaxed);
                let _ = job.reply.send(Outcome::Deadline);
                continue;
            }
            self.run_solo(job);
            if self.pinned.is_some() {
                break;
            }
        }
        // still pinned: the rest waits its turn (in order, ahead of newer jobs)
        for (i, mut job) in solo.enumerate() {
            self.metrics.queue_len.fetch_add(1, Relaxed);
            job.solo = true;
            self.backlog.insert(i, job);
        }
    }

    /// Runs a request as written, on the bare connection (autocommit unless the stream is pinned)
    fn run_solo(&mut self, job: Job) {
        self.conn.set_deadline(Some(job.deadline));
        let mut ex = Exec::native(&self.conn, self.cfg.max_response_bytes);
        let results = match ex.run(&job.requests) {
            Ok(r) => r,
            Err(_) => unreachable!("native execution has no group"),
        };
        let closed = ex.closed;
        self.conn.set_deadline(None);
        let open = !self.conn.is_autocommit();
        // set outside a transaction, SQLite keeps it until the next COMMIT or ROLLBACK, which may be another stream's
        if !open && let Err(e) = self.conn.c.execute_batch("PRAGMA defer_foreign_keys = OFF") {
            tracing::error!(error = %e, "could not reset defer_foreign_keys");
        }
        self.dirty = true;
        self.pinned = open.then(|| (job.stream, self.pinned.map_or_else(Instant::now, |p| p.1)));
        self.metrics.pinned.store(open as i64, Relaxed);
        let sent = job
            .reply
            .send(Outcome::Done {
                results,
                closed,
                pinned: open,
            })
            .is_ok();
        if open && !sent {
            self.unpin("client went away");
        }
    }

    /// Pinned: wait for the next request of the stream holding the transaction; everything else waits
    fn pinned_step(&mut self) {
        let Some((stream, since)) = self.pinned else { return };
        let until = since + self.cfg.pinned_tx_timeout();
        if let Some(pos) = self.backlog.iter().position(|j| j.stream == stream) {
            let job = self.backlog.remove(pos).expect("position is valid");
            self.run_pinned(job);
            return;
        }
        let wake = self.expire_backlog().map_or(until, |next| next.min(until));
        match self.rx.recv_timeout(wake.saturating_duration_since(Instant::now())) {
            Ok(Msg::Job(j)) if j.stream == stream => self.run_pinned(j),
            Ok(Msg::Job(j)) => self.backlog.push_back(j),
            Ok(Msg::Stop(ack)) => self.stop = Some(ack),
            Err(RecvTimeoutError::Timeout) if Instant::now() >= until => {
                self.metrics.pinned_timeouts.fetch_add(1, Relaxed);
                self.unpin("open transaction timed out");
            }
            Err(RecvTimeoutError::Timeout) => {}
            Err(RecvTimeoutError::Disconnected) => self.unpin("shutting down"),
        }
    }

    /// While pinned: answers the queued jobs whose wait or deadline has run out now, as `admit` would once the
    /// transaction ends, instead of keeping their clients until then. Returns when the next one runs out.
    fn expire_backlog(&mut self) -> Option<Instant> {
        let now = Instant::now();
        let queue_timeout = self.cfg.queue_timeout();
        let mut next: Option<Instant> = None;
        let mut i = 0;
        while i < self.backlog.len() {
            let j = &self.backlog[i];
            let runs_out = (j.enqueued + queue_timeout).min(j.deadline);
            if runs_out <= now || j.reply.is_closed() {
                let job = self.backlog.remove(i).expect("index is valid");
                let _ = self.admit(job);
                continue;
            }
            next = Some(next.map_or(runs_out, |n| n.min(runs_out)));
            i += 1;
        }
        next
    }

    fn run_pinned(&mut self, job: Job) {
        self.metrics.queue_len.fetch_sub(1, Relaxed);
        if job.reply.is_closed() {
            self.unpin("client went away");
            return;
        }
        if Instant::now() >= job.deadline {
            let _ = job.reply.send(Outcome::Deadline);
            self.unpin("request deadline passed");
            return;
        }
        self.run_solo(job);
        self.after_commit();
    }

    /// Rolls back the pinned transaction and forgets the stream (its next request gets 400 and opens a new one)
    fn unpin(&mut self, why: &str) {
        if let Some((stream, since)) = self.pinned.take() {
            tracing::warn!(
                why,
                held_ms = since.elapsed().as_millis() as u64,
                "rolling back a transaction left open"
            );
            self.conn.rollback_all();
            self.streams.kill(stream);
            self.metrics.pinned.store(0, Relaxed);
        }
    }

    /// SQLite starts the WAL over at the next write only once every frame is in the database file. Under steady writes
    /// the checkpoint thread never quite catches up, so when it has left a tail of at most `--checkpoint-pages` frames
    /// the write thread copies that tail itself (a short pause) and the WAL restarts. Once the WAL content passes the
    /// RESTART size (readers kept it from restarting, or writes outpace the checkpoint thread) the write thread
    /// runs RESTART whatever the tail, so the WAL stays bounded; after a failure (readers) it tries again only once
    /// `--checkpoint-pages` more frames have been written.
    ///
    /// Once `--checkpoint-pages` frames have been written since the checkpoint thread was woken, the next group waits
    /// for its run to end. On a disk whose random writes are slow the checkpoint thread falls behind otherwise, and
    /// the RESTART that has to copy all it left stops writes for hundreds of milliseconds; the waits cap every run,
    /// and so every pause of the write thread, to about `--checkpoint-pages` frames.
    ///
    /// Checkpoint starvation: when a RESTART fails, readers still used the WAL. Reads that follow each other closely
    /// always leave one there, so retrying the same way fails again and again while the WAL grows. The next RESTART
    /// (after `--checkpoint-pages` more frames) is therefore run with the read gate closed ([`Self::gated_restart`]);
    /// if even that fails (a read longer than `--checkpoint-reader-wait-ms`), the following one waits at least four
    /// times that wait. Invariant: as long as the reads running when the gate closes end within that wait, the WAL
    /// starts over within two RESTART attempts of passing `--checkpoint-restart-bytes`, however closely reads follow
    /// each other; it only grows on while single reads run longer than the wait (each read is still cut off at its
    /// own deadline).
    fn after_commit(&mut self) {
        if self.pinned.is_some() {
            return;
        }
        let frames = self.frames.load(Relaxed);
        self.metrics.wal_frames.store(frames, Relaxed);
        let restarted = frames < self.last_frames;
        self.last_frames = frames;
        if restarted {
            // the WAL started over by itself (a passive checkpoint caught up while no reader was on it): a RESTART
            // that failed before says nothing about the new WAL
            self.kicked_at = 0;
            self.restart_after = 0;
            self.starved = false;
            self.gate_retry_at = None;
        }
        let Some(c) = &self.checkpointer else { return };
        let (running, mut done) = c.observe(restarted);
        let pages = self.cfg.checkpoint_pages as i64;
        let restart_due =
            frames * (self.page_size + 24) >= self.cfg.checkpoint_restart_bytes as i64 && frames >= self.restart_after;
        // while starved, only a RESTART with the gate closed has a chance; it waits its turn
        let gated = restart_due && self.starved && self.gate_retry_at.is_none_or(|at| Instant::now() >= at);
        let restart = restart_due && (!self.starved || gated);
        if running {
            // a RESTART cannot start beside a running checkpoint (it would fail at once); the gated one waits for it
            if frames - self.kicked_at <= pages && !gated {
                return;
            }
            let started = Instant::now();
            done = c.wait();
            self.metrics.checkpoint_wait.record(started.elapsed());
        }
        if frames < pages {
            return;
        }
        if !restart && frames - done > pages {
            c.request();
            self.kicked_at = frames;
            return;
        }
        let r = if gated {
            self.gated_restart()
        } else if restart {
            checkpoint(&self.conn, CheckpointMode::RESTART, &self.metrics)
        } else {
            checkpoint(&self.conn, CheckpointMode::PASSIVE, &self.metrics)
        };
        if let Some((_, done)) = r
            && let Some(c) = &self.checkpointer
        {
            c.record(done);
        }
        if restart {
            if matches!(r, Some((log, done)) if log == done) {
                self.emptied();
            } else {
                self.restart_after = frames + pages;
                if gated {
                    self.gate_retry_at = Some(Instant::now() + 4 * self.cfg.checkpoint_reader_wait());
                }
                self.starved = true;
            }
        }
    }

    /// RESTART with no new read transaction starting meanwhile: closes the read gate, waits up to
    /// `--checkpoint-reader-wait-ms` for the read requests already running to end, runs RESTART (which then only
    /// waits its usual short while, for readers that did not come through the gate, such as a dump's snapshot) and
    /// opens the gate again. Writes wait meanwhile; this only runs after a RESTART failed.
    fn gated_restart(&mut self) -> Option<(i64, i64)> {
        let started = Instant::now();
        let closed = self.gate.close();
        if !closed.drain(self.cfg.checkpoint_reader_wait()) {
            tracing::warn!(
                wait_ms = self.cfg.checkpoint_reader_wait_ms,
                "reads still running after the checkpoint reader wait"
            );
        }
        let r = checkpoint(&self.conn, CheckpointMode::RESTART, &self.metrics);
        drop(closed);
        self.metrics.checkpoint_gate.record(started.elapsed());
        r
    }

    /// Nothing to do for a while: copy the whole WAL into the database file, so that the next write starts the WAL
    /// over. Fails at once while the checkpoint thread is at work or a reader still needs frames; the next idle
    /// period tries again.
    fn idle_checkpoint(&mut self) {
        let r = checkpoint(&self.conn, CheckpointMode::PASSIVE, &self.metrics);
        if let Some((log, done)) = r
            && log == done
        {
            if let Some(c) = &self.checkpointer {
                c.record(done);
            }
            self.dirty = false;
        }
    }

    fn truncate(&mut self) {
        if matches!(checkpoint(&self.conn, CheckpointMode::TRUNCATE, &self.metrics), Some((log, done)) if log == done) {
            self.emptied();
        }
    }

    /// Every frame is in the database file and no reader is left on the WAL: the next write starts it over
    fn emptied(&mut self) {
        self.frames.store(0, Relaxed);
        self.metrics.wal_frames.store(0, Relaxed);
        self.dirty = false;
        self.restart_after = 0;
        self.starved = false;
        self.gate_retry_at = None;
    }

    fn shutdown(&mut self) {
        self.unpin("shutting down");
        for job in self.backlog.drain(..) {
            self.metrics.queue_len.fetch_sub(1, Relaxed);
            let _ = job.reply.send(Outcome::Unavailable);
        }
        while let Ok(msg) = self.rx.try_recv() {
            match msg {
                Msg::Job(job) => {
                    self.metrics.queue_len.fetch_sub(1, Relaxed);
                    let _ = job.reply.send(Outcome::Unavailable);
                }
                Msg::Stop(ack) => {
                    let _ = ack.send(());
                }
            }
        }
        // a running passive checkpoint would make TRUNCATE fail at once (SQLite does not wait for the checkpoint lock)
        if let Some(c) = self.checkpointer.take() {
            c.stop();
        }
        self.truncate();
        if let Some(ack) = self.stop.take() {
            let _ = ack.send(());
        }
    }
}

/// Opens the group transaction. The connection is in autocommit between groups; a transaction left open there is a
/// bug, so it is logged as an error and rolled back before one more try
fn begin_group(conn: &Conn) -> bool {
    match conn.c.execute_batch("BEGIN IMMEDIATE") {
        Ok(()) => true,
        Err(e) => {
            tracing::error!(error = %e, autocommit = conn.is_autocommit(), "could not begin the group transaction");
            conn.rollback_all();
            conn.c.execute_batch("BEGIN IMMEDIATE").is_ok()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::{PipelineReq, StreamResult};
    use std::path::PathBuf;

    fn wal_path(db: &Path) -> PathBuf {
        let mut p = db.as_os_str().to_owned();
        p.push("-wal");
        p.into()
    }

    struct Fixture {
        _dir: tempfile::TempDir,
        path: PathBuf,
        writer: Writer,
        readers: Arc<crate::reader::Readers>,
        streams: Arc<Streams>,
        metrics: Arc<DbMetrics>,
    }

    fn fixture(tweak: impl FnOnce(&mut Config)) -> Fixture {
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = Config::for_tests(dir.path());
        tweak(&mut cfg);
        let cfg = Arc::new(cfg);
        let path = dir.path().join("data");
        rusqlite::Connection::open(&path)
            .unwrap()
            .execute_batch("CREATE TABLE t(a INTEGER PRIMARY KEY, b TEXT UNIQUE)")
            .unwrap();
        let streams = Streams::new(cfg.stream_idle());
        let metrics = Arc::new(DbMetrics::default());
        let gate = ReadGate::new(metrics.clone());
        let writer = Writer::start(&path, cfg.clone(), streams.clone(), metrics.clone(), gate.clone()).unwrap();
        let readers = Arc::new(crate::reader::Readers::new(path.clone(), cfg, gate));
        Fixture {
            _dir: dir,
            path,
            writer,
            readers,
            streams,
            metrics,
        }
    }

    impl Fixture {
        fn job(&self, stream: u64, expect_pinned: bool, requests: &str) -> (Job, oneshot::Receiver<Outcome>) {
            let requests = serde_json::from_str::<PipelineReq>(&format!(r#"{{"requests":{requests}}}"#))
                .unwrap()
                .requests;
            Job::new(
                requests,
                stream,
                expect_pinned,
                Instant::now() + Duration::from_secs(10),
            )
        }

        fn send(&self, stream: u64, expect_pinned: bool, requests: &str) -> Outcome {
            let (job, rx) = self.job(stream, expect_pinned, requests);
            self.writer.submit(job);
            rx.blocking_recv().unwrap()
        }

        fn count(&self) -> i64 {
            let c = rusqlite::Connection::open(&self.path).unwrap();
            c.query_row("SELECT count(*) FROM t", [], |r| r.get(0)).unwrap()
        }
    }

    fn is_ok(o: &Outcome, i: usize) -> bool {
        matches!(o, Outcome::Done { results, .. } if matches!(results[i], StreamResult::Ok { .. }))
    }

    const INSERT_TX: &str = r#"[{"type":"batch","batch":{"steps":[{"stmt":{"sql":"BEGIN IMMEDIATE"}},
        {"condition":{"type":"ok","step":0},"stmt":{"sql":"INSERT INTO t(b) VALUES (?)","args":[{"type":"text","value":"B"}]}},
        {"condition":{"type":"ok","step":1},"stmt":{"sql":"COMMIT"}},
        {"condition":{"type":"not","cond":{"type":"ok","step":2}},"stmt":{"sql":"ROLLBACK"}}]}}]"#;

    #[test]
    fn group_commit_isolates_failed_transactions() {
        let f = fixture(|c| c.group_linger_us = 50_000);
        let jobs: Vec<_> = ["x", "y", "x", "z", "y"]
            .iter()
            .map(|b| f.job(1, false, &INSERT_TX.replace("\"B\"", &format!("\"{b}\""))))
            .collect();
        let rxs: Vec<_> = jobs
            .into_iter()
            .map(|(j, rx)| {
                f.writer.submit(j);
                rx
            })
            .collect();
        let outcomes: Vec<_> = rxs.into_iter().map(|rx| rx.blocking_recv().unwrap()).collect();
        let failed: Vec<bool> = outcomes
            .iter()
            .map(|o| match o {
                Outcome::Done { results, .. } => match &results[0] {
                    StreamResult::Ok {
                        response: crate::proto::StreamResponse::Batch { result },
                    } => result.step_errors[1].is_some(),
                    _ => panic!("{o:?}"),
                },
                _ => panic!("{o:?}"),
            })
            .collect();
        assert_eq!(failed, [false, false, true, false, true]);
        assert_eq!(f.count(), 3);
        assert_eq!(f.metrics.groups.load(Relaxed), 1, "one group of five");
        assert_eq!(f.metrics.group_requests.load(Relaxed), 5);
    }

    /// Review of 0.1.0, finding 1: `\u{feff}COMMIT` was taken for an ordinary statement and committed the group
    /// transaction for real; the group was then redone and the requests before it were written twice
    #[test]
    fn transaction_keywords_behind_whitespace_only_sqlite_skips_stay_inside_their_request() {
        let f = fixture(|c| c.group_linger_us = 100_000);
        let execs = |sqls: &[&str]| {
            let v: Vec<_> = sqls
                .iter()
                .map(|s| serde_json::json!({"type": "execute", "stmt": {"sql": s}}))
                .collect();
            serde_json::to_string(&v).unwrap()
        };
        assert!(is_ok(&f.send(1, false, &execs(&["CREATE TABLE d(v TEXT)"])), 0));
        let hidden = [
            "\u{feff}COMMIT",
            "\u{feff}END",
            "\u{a0}COMMIT",
            " \u{b}COMMIT",
            "\u{3000}ROLLBACK",
        ];
        for (i, hidden) in hidden.iter().enumerate() {
            let jobs = vec![
                f.job(1, false, &execs(&[&format!("INSERT INTO d VALUES ('a{i}')")])),
                f.job(2, false, &execs(&[&format!("INSERT INTO d VALUES ('bad{i}')"), hidden])),
                f.job(3, false, &execs(&[&format!("INSERT INTO d VALUES ('b{i}')")])),
            ];
            let rxs: Vec<_> = jobs
                .into_iter()
                .map(|(j, rx)| {
                    f.writer.submit(j);
                    rx
                })
                .collect();
            let outcomes: Vec<_> = rxs.into_iter().map(|rx| rx.blocking_recv().unwrap()).collect();
            assert!(is_ok(&outcomes[0], 0) && is_ok(&outcomes[2], 0), "{hidden:?}");
            // like a bare COMMIT outside a transaction: the insert stays, the keyword is an error
            assert!(is_ok(&outcomes[1], 0) && !is_ok(&outcomes[1], 1), "{hidden:?}");
            let c = rusqlite::Connection::open(&f.path).unwrap();
            for v in ["a", "bad", "b"] {
                let n: i64 = c
                    .query_row("SELECT count(*) FROM d WHERE v = ?1", [format!("{v}{i}")], |r| r.get(0))
                    .unwrap();
                assert_eq!(n, 1, "{v} after {hidden:?}");
            }
        }
        assert_eq!(f.metrics.group_lost.load(Relaxed), 0);
        // and statements that must run on their own are still recognized
        for sql in [
            "\u{feff}PRAGMA defer_foreign_keys = ON",
            "\u{a0}SAVEPOINT x",
            " \u{b}VACUUM",
        ] {
            assert!(f.job(1, false, &execs(&[sql])).0.solo, "{sql:?}");
        }
    }

    /// The backstop behind the classification: a COMMIT inside the group transaction that is not the write thread's
    /// own rolls the group back; the group is redone and nothing is written twice
    #[test]
    fn a_commit_the_classification_does_not_know_rolls_the_group_back() {
        let f = fixture(|c| c.group_linger_us = 100_000);
        let execs = |sqls: &[&str]| {
            let v: Vec<_> = sqls
                .iter()
                .map(|s| serde_json::json!({"type": "execute", "stmt": {"sql": s}}))
                .collect();
            serde_json::to_string(&v).unwrap()
        };
        assert!(is_ok(&f.send(1, false, &execs(&["CREATE TABLE d(v TEXT)"])), 0));
        let commit = format!("{}COMMIT", sql::UNCLASSIFIED);
        assert_eq!(sql::classify(&commit), sql::Kind::Other);
        let jobs = vec![
            f.job(1, false, &execs(&["INSERT INTO d VALUES ('a')"])),
            f.job(2, false, &execs(&["INSERT INTO d VALUES ('a2')"])),
            f.job(3, false, &execs(&["INSERT INTO d VALUES ('bad')", &commit])),
            f.job(4, false, &execs(&["INSERT INTO d VALUES ('b')"])),
        ];
        let rxs: Vec<_> = jobs
            .into_iter()
            .map(|(j, rx)| {
                f.writer.submit(j);
                rx
            })
            .collect();
        let outcomes: Vec<_> = rxs.into_iter().map(|rx| rx.blocking_recv().unwrap()).collect();
        for i in [0, 1, 3] {
            assert!(is_ok(&outcomes[i], 0), "{:?}", outcomes[i]);
        }
        // on its own, as written: the insert commits by itself, the COMMIT finds no transaction
        assert!(is_ok(&outcomes[2], 0) && !is_ok(&outcomes[2], 1), "{:?}", outcomes[2]);
        let c = rusqlite::Connection::open(&f.path).unwrap();
        let rows: Vec<String> = c
            .prepare("SELECT v FROM d ORDER BY v")
            .unwrap()
            .query_map([], |r| r.get(0))
            .unwrap()
            .map(Result::unwrap)
            .collect();
        assert_eq!(rows, ["a", "a2", "b", "bad"], "each acknowledged insert is there once");
        assert_eq!(f.metrics.group_lost.load(Relaxed), 1);
        // the refusal ends with the group: later groups and requests on their own commit
        assert!(is_ok(&f.send(1, false, &execs(&["INSERT INTO d VALUES ('later')"])), 0));
        assert!(is_ok(&f.send(1, false, &execs(&["PRAGMA user_version = 7"])), 0));
        let later: i64 = c
            .query_row("SELECT count(*) FROM d WHERE v = 'later'", [], |r| r.get(0))
            .unwrap();
        assert_eq!(later, 1);
    }

    #[test]
    fn lost_group_is_redone_without_the_culprit() {
        let f = fixture(|c| c.group_linger_us = 50_000);
        f.send(1, false, r#"[{"type":"execute","stmt":{"sql":"CREATE TABLE r(a UNIQUE ON CONFLICT ROLLBACK)"}},{"type":"execute","stmt":{"sql":"INSERT INTO r VALUES (1)"}}]"#);
        let a = f.job(
            1,
            false,
            r#"[{"type":"execute","stmt":{"sql":"INSERT INTO t(b) VALUES ('a')"}}]"#,
        );
        let bad = f.job(2, false, r#"[{"type":"execute","stmt":{"sql":"INSERT INTO t(b) VALUES ('c')"}},{"type":"execute","stmt":{"sql":"INSERT INTO r VALUES (1)"}}]"#);
        let b = f.job(
            3,
            false,
            r#"[{"type":"execute","stmt":{"sql":"INSERT INTO t(b) VALUES ('b')"}}]"#,
        );
        let rxs: Vec<_> = [a, bad, b]
            .into_iter()
            .map(|(j, rx)| {
                f.writer.submit(j);
                rx
            })
            .collect();
        let o: Vec<_> = rxs.into_iter().map(|rx| rx.blocking_recv().unwrap()).collect();
        assert!(is_ok(&o[0], 0) && is_ok(&o[2], 0));
        // run alone, like sqld would: the first statement commits, the second fails
        assert!(is_ok(&o[1], 0) && !is_ok(&o[1], 1));
        assert_eq!(f.count(), 3);
        assert_eq!(f.metrics.group_lost.load(Relaxed), 1);
    }

    #[test]
    fn open_transaction_pins_then_times_out() {
        let f = fixture(|c| c.pinned_tx_timeout_ms = 200);
        let o = f.send(7, false, r#"[{"type":"execute","stmt":{"sql":"BEGIN"}},{"type":"execute","stmt":{"sql":"INSERT INTO t(b) VALUES ('p')"}}]"#);
        assert!(matches!(o, Outcome::Done { pinned: true, .. }));
        // other streams wait while the transaction is open, then run once it is rolled back
        let started = Instant::now();
        let o = f.send(
            8,
            false,
            r#"[{"type":"execute","stmt":{"sql":"INSERT INTO t(b) VALUES ('q')"}}]"#,
        );
        assert!(is_ok(&o, 0));
        assert!(started.elapsed() >= Duration::from_millis(150));
        assert_eq!(f.metrics.pinned_timeouts.load(Relaxed), 1);
        assert_eq!(f.count(), 1, "the pinned insert was rolled back");
        let o = f.send(7, true, r#"[{"type":"execute","stmt":{"sql":"COMMIT"}}]"#);
        assert!(matches!(o, Outcome::StreamExpired));
    }

    #[test]
    fn queued_jobs_run_out_on_time_while_a_transaction_is_open() {
        let f = fixture(|c| {
            c.pinned_tx_timeout_ms = 4000;
            c.queue_timeout_ms = 300;
        });
        let o = f.send(7, false, r#"[{"type":"execute","stmt":{"sql":"BEGIN"}},{"type":"execute","stmt":{"sql":"INSERT INTO t(b) VALUES ('p')"}}]"#);
        assert!(matches!(o, Outcome::Done { pinned: true, .. }));
        let started = Instant::now();
        let (waits, waits_rx) = f.job(
            8,
            false,
            r#"[{"type":"execute","stmt":{"sql":"INSERT INTO t(b) VALUES ('q')"}}]"#,
        );
        f.writer.submit(waits);
        let (mut due, due_rx) = f.job(
            9,
            false,
            r#"[{"type":"execute","stmt":{"sql":"INSERT INTO t(b) VALUES ('r')"}}]"#,
        );
        due.deadline = started + Duration::from_millis(150);
        f.writer.submit(due);

        assert!(matches!(due_rx.blocking_recv().unwrap(), Outcome::Deadline));
        assert!(
            started.elapsed() < Duration::from_millis(280),
            "{:?}",
            started.elapsed()
        );
        assert!(matches!(waits_rx.blocking_recv().unwrap(), Outcome::QueueTimeout));
        let waited = started.elapsed();
        assert!(
            waited >= Duration::from_millis(300) && waited < Duration::from_secs(1),
            "{waited:?}"
        );

        let o = f.send(7, true, r#"[{"type":"execute","stmt":{"sql":"COMMIT"}}]"#);
        assert!(
            matches!(o, Outcome::Done { pinned: false, .. }),
            "the open transaction is still there"
        );
        assert_eq!(f.metrics.pinned_timeouts.load(Relaxed), 0);
        assert_eq!(f.count(), 1, "the expired requests did not run");
    }

    #[test]
    fn open_transaction_continues_on_its_stream() {
        let f = fixture(|_| {});
        let o = f.send(7, false, r#"[{"type":"execute","stmt":{"sql":"BEGIN"}},{"type":"execute","stmt":{"sql":"INSERT INTO t(b) VALUES ('p')"}}]"#);
        assert!(matches!(o, Outcome::Done { pinned: true, .. }));
        let (other, other_rx) = f.job(
            8,
            false,
            r#"[{"type":"execute","stmt":{"sql":"INSERT INTO t(b) VALUES ('q')"}}]"#,
        );
        f.writer.submit(other);
        let o = f.send(7, true, r#"[{"type":"execute","stmt":{"sql":"INSERT INTO t(b) VALUES ('r')"}},{"type":"execute","stmt":{"sql":"COMMIT"}}]"#);
        assert!(matches!(o, Outcome::Done { pinned: false, .. }));
        assert!(is_ok(&other_rx.blocking_recv().unwrap(), 0));
        assert_eq!(f.count(), 3);
    }

    #[test]
    fn client_gone_releases_pinned_transaction() {
        let f = fixture(|_| {});
        let o = f.send(7, false, r#"[{"type":"execute","stmt":{"sql":"BEGIN IMMEDIATE"}},{"type":"execute","stmt":{"sql":"INSERT INTO t(b) VALUES ('p')"}}]"#);
        assert!(matches!(o, Outcome::Done { pinned: true, .. }));
        let (job, rx) = f.job(7, true, r#"[{"type":"execute","stmt":{"sql":"COMMIT"}}]"#);
        drop(rx);
        f.writer.submit(job);
        let o = f.send(
            8,
            false,
            r#"[{"type":"execute","stmt":{"sql":"INSERT INTO t(b) VALUES ('q')"}}]"#,
        );
        assert!(is_ok(&o, 0));
        assert_eq!(f.count(), 1);
        assert_eq!(f.streams.counts().1, 0);
    }

    fn execs(sqls: &[&str]) -> String {
        let v: Vec<_> = sqls
            .iter()
            .map(|s| serde_json::json!({"type": "execute", "stmt": {"sql": s}}))
            .collect();
        serde_json::to_string(&v).unwrap()
    }

    fn json(o: &Outcome) -> serde_json::Value {
        let Outcome::Done { results, .. } = o else {
            panic!("{o:?}")
        };
        serde_json::to_value(results).unwrap()
    }

    fn rows(f: &Fixture, sql: &str) -> i64 {
        let c = rusqlite::Connection::open(&f.path).unwrap();
        c.query_row(sql, [], |r| r.get(0)).unwrap()
    }

    #[test]
    fn watchdog_probe_runs_alone() {
        let f = fixture(|_| {});
        let o = f.send(1, false, &execs(&["BEGIN IMMEDIATE", "ROLLBACK"]));
        assert!(is_ok(&o, 0) && is_ok(&o, 1));
        assert!(matches!(o, Outcome::Done { pinned: false, .. }));
    }

    #[test]
    fn malformed_tail_after_a_statement_is_an_error_and_writes_go_on() {
        // review 2318 M1: this request made the write thread panic, every later write of the database got 503
        let f = fixture(|_| {});
        let o = f.send(1, false, &execs(&["INSERT INTO t(b) VALUES ('x'); WITH [x"]));
        assert_eq!(json(&o)[0]["error"]["code"], "SQL_MANY_STATEMENTS");
        assert!(is_ok(&f.send(1, false, &execs(&["INSERT INTO t(b) VALUES ('y')"])), 0));
        assert_eq!(f.count(), 1);
    }

    #[test]
    fn pragmas_that_would_change_the_shared_connection_are_refused() {
        // review 2318 M2: every stream shares the write connection, so these would carry over to all of them
        let f = fixture(|_| {});
        assert!(is_ok(
            &f.send(1, false, &execs(&["CREATE TABLE c(v TEXT CHECK (v <> 'bad'))"])),
            0
        ));
        for sql in [
            "PRAGMA ignore_check_constraints = ON",
            "PRAGMA query_only = 1",
            "PRAGMA locking_mode = EXCLUSIVE",
            "PRAGMA foreign_keys = OFF",
            "PRAGMA wal_autocheckpoint = 5",
            "CREATE TEMP TABLE x(a)",
        ] {
            let o = f.send(1, false, &execs(&[sql]));
            let e = &json(&o)[0]["error"];
            assert_eq!(e["code"], "SQL_PARSE_ERROR", "{sql}");
            assert_eq!(
                e["message"],
                format!("SQL string could not be parsed: unsupported statement: {sql}")
            );
        }
        // like sqld, such a statement fails the whole batch before any step runs
        let o = f.send(
            1,
            false,
            r#"[{"type":"batch","batch":{"steps":[{"stmt":{"sql":"INSERT INTO t(b) VALUES ('batch')"}},
                {"stmt":{"sql":"PRAGMA query_only = 1"}}]}}]"#,
        );
        assert_eq!(json(&o)[0]["error"]["code"], "SQL_PARSE_ERROR");
        assert_eq!(f.count(), 0);
        // inside a transaction that the next request of the stream commits
        let o = f.send(2, false, &execs(&["BEGIN", "PRAGMA recursive_triggers = ON"]));
        assert!(matches!(o, Outcome::Done { pinned: true, .. }));
        assert_eq!(json(&o)[1]["error"]["code"], "SQL_PARSE_ERROR");
        assert!(is_ok(&f.send(2, true, &execs(&["COMMIT"])), 0));

        let o = f.send(3, false, &execs(&["INSERT INTO c VALUES ('bad')"]));
        assert_eq!(json(&o)[0]["error"]["code"], "SQLITE_CONSTRAINT");
        assert!(is_ok(&f.send(3, false, &execs(&["INSERT INTO t(b) VALUES ('ok')"])), 0));
        let v = json(&f.send(
            3,
            false,
            &execs(&[
                "PRAGMA foreign_keys",
                "PRAGMA recursive_triggers",
                "PRAGMA wal_autocheckpoint",
                "PRAGMA defer_foreign_keys = ON",
                "PRAGMA table_info(t)",
            ]),
        ));
        let first = |i: usize| v[i]["response"]["result"]["rows"][0][0]["value"].clone();
        assert_eq!((first(0), first(1), first(2)), ("1".into(), "0".into(), "0".into()));
        assert_eq!(v[3]["type"], "ok", "{v}");
        assert_eq!(v[4]["response"]["result"]["rows"].as_array().unwrap().len(), 2);
    }

    #[test]
    fn full_queue_answers_at_once_but_lets_the_pinned_stream_in() {
        let f = fixture(|c| c.max_write_queue = 2);
        let o = f.send(7, false, &execs(&["BEGIN", "INSERT INTO t(b) VALUES ('p')"]));
        assert!(matches!(o, Outcome::Done { pinned: true, .. }));
        let waiting: Vec<_> = (0..2)
            .map(|i| {
                let (job, rx) = f.job(8 + i, false, &execs(&[&format!("INSERT INTO t(b) VALUES ('w{i}')")]));
                f.writer.submit(job);
                rx
            })
            .collect();
        let o = f.send(20, false, &execs(&["INSERT INTO t(b) VALUES ('late')"]));
        assert!(matches!(o, Outcome::QueueFull), "{o:?}");
        assert_eq!(f.metrics.queue_full.load(Relaxed), 1);
        assert!(is_ok(&f.send(7, true, &execs(&["COMMIT"])), 0));
        for rx in waiting {
            assert!(is_ok(&rx.blocking_recv().unwrap(), 0));
        }
        assert_eq!(f.count(), 3);
    }

    /// Four slow inserts queued behind an open transaction, so that they are all waiting when it ends; returns the
    /// largest group and how often a group was committed early
    fn slow_inserts(group_max_exec_ms: u64) -> (u64, u64) {
        let f = fixture(|c| c.group_max_exec_ms = group_max_exec_ms);
        let o = f.send(99, false, &execs(&["BEGIN IMMEDIATE"]));
        assert!(matches!(o, Outcome::Done { pinned: true, .. }));
        let slow = |i: u64| {
            format!(
                "INSERT INTO t(b) SELECT 'slow {i} ' || count(*) FROM \
                 (WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 300000) SELECT i FROM n)"
            )
        };
        let rxs: Vec<_> = (0..4)
            .map(|i| {
                let (job, rx) = f.job(i, false, &execs(&[&slow(i)]));
                f.writer.submit(job);
                rx
            })
            .collect();
        assert!(is_ok(&f.send(99, true, &execs(&["COMMIT"])), 0));
        for rx in rxs {
            assert!(is_ok(&rx.blocking_recv().unwrap(), 0));
        }
        assert_eq!(f.count(), 4);
        (
            f.metrics.group_size_max.load(Relaxed),
            f.metrics.group_cuts.load(Relaxed),
        )
    }

    #[test]
    fn a_group_that_runs_long_is_committed_before_its_next_request() {
        assert_eq!(slow_inserts(60_000), (4, 0), "(largest group, early commits)");
        assert_eq!(slow_inserts(1), (1, 3), "(largest group, early commits)");
    }

    #[test]
    fn begin_group_rolls_back_a_transaction_left_open() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = Config::for_tests(dir.path());
        let conn = Conn::open_writer(&dir.path().join("data"), &cfg).unwrap();
        conn.c
            .execute_batch("CREATE TABLE x(a); BEGIN; INSERT INTO x VALUES (1);")
            .unwrap();
        assert!(begin_group(&conn));
        assert!(!conn.is_autocommit());
        conn.c.execute_batch("COMMIT").unwrap();
        let n: i64 = conn.c.query_row("SELECT count(*) FROM x", [], |r| r.get(0)).unwrap();
        assert_eq!(
            n, 0,
            "the stray transaction was rolled back, not committed with the group"
        );
    }

    #[test]
    fn deferred_foreign_keys_are_checked_per_request_like_sqld() {
        let f = fixture(|c| c.group_linger_us = 50_000);
        let o = f.send(
            1,
            false,
            &execs(&[
                "CREATE TABLE p(id INTEGER PRIMARY KEY)",
                "CREATE TABLE c(id INTEGER PRIMARY KEY, p INTEGER REFERENCES p(id) DEFERRABLE INITIALLY DEFERRED)",
            ]),
        );
        assert!(is_ok(&o, 1));
        // in one group: an orphan child, then a request that would give it a parent only because it sees the child.
        // Run one after the other (either order) the child fails and no parent is made.
        let (a, a_rx) = f.job(1, false, &execs(&["INSERT INTO c VALUES (1, 5)"]));
        let (b, b_rx) = f.job(
            2,
            false,
            &execs(&["INSERT INTO p SELECT p FROM c WHERE p NOT IN (SELECT id FROM p)"]),
        );
        f.writer.submit(a);
        f.writer.submit(b);
        let (a, b) = (a_rx.blocking_recv().unwrap(), b_rx.blocking_recv().unwrap());
        assert_eq!(json(&a)[0]["error"]["code"], "SQLITE_CONSTRAINT", "{a:?}");
        assert!(is_ok(&b, 0));
        assert_eq!(
            (rows(&f, "SELECT count(*) FROM c"), rows(&f, "SELECT count(*) FROM p")),
            (0, 0)
        );
        // in one request: COMMIT fails and the transaction stays open, as in SQLite
        let o = f.send(3, false, &execs(&["BEGIN", "INSERT INTO c VALUES (2, 6)", "COMMIT"]));
        assert_eq!(json(&o)[2]["error"]["code"], "SQLITE_CONSTRAINT", "{o:?}");
        assert!(matches!(o, Outcome::Done { pinned: true, .. }));
        assert!(is_ok(&f.send(3, true, &execs(&["ROLLBACK"])), 0));
        assert_eq!(rows(&f, "SELECT count(*) FROM c"), 0);
    }

    /// Review of 0.1.0: deferred foreign keys were checked at the end of a request, not where sqld checks them, at
    /// the commit of each statement outside a transaction and at the client's COMMIT (checked against sqld 0.24.32)
    #[test]
    fn deferred_foreign_keys_are_checked_per_statement_like_sqld() {
        let f = fixture(|c| c.group_linger_us = 50_000);
        assert!(is_ok(
            &f.send(
                1,
                false,
                &execs(&[
                    "CREATE TABLE p(id INTEGER PRIMARY KEY)",
                    "CREATE TABLE c(id INTEGER PRIMARY KEY, p INTEGER REFERENCES p(id) DEFERRABLE INITIALLY DEFERRED)",
                ]),
            ),
            1
        ));
        // in a group with another request, so that the request runs grouped first
        let (other, other_rx) = f.job(9, false, &execs(&["INSERT INTO t(b) VALUES ('other')"]));
        let (job, rx) = f.job(
            1,
            false,
            &execs(&["INSERT INTO c VALUES (1, 5)", "INSERT INTO p VALUES (5)"]),
        );
        f.writer.submit(other);
        f.writer.submit(job);
        let o = rx.blocking_recv().unwrap();
        assert!(is_ok(&other_rx.blocking_recv().unwrap(), 0));
        assert_eq!(
            json(&o)[0]["error"]["message"],
            "SQLite error: FOREIGN KEY constraint failed",
            "{o:?}"
        );
        assert!(is_ok(&o, 1));
        assert_eq!(
            (rows(&f, "SELECT count(*) FROM c"), rows(&f, "SELECT count(*) FROM p")),
            (0, 1)
        );
        // the client's COMMIT fails and its transaction stays open, whatever follows in the request
        let o = f.send(
            2,
            false,
            &execs(&[
                "BEGIN",
                "INSERT INTO c VALUES (2, 6)",
                "COMMIT",
                "INSERT INTO p VALUES (6)",
                "COMMIT",
            ]),
        );
        let v = json(&o);
        assert_eq!(v[2]["error"]["code"], "SQLITE_CONSTRAINT", "{v}");
        assert!(is_ok(&o, 3) && is_ok(&o, 4), "{v}");
        assert!(matches!(o, Outcome::Done { pinned: false, .. }));
        assert_eq!(
            (rows(&f, "SELECT count(*) FROM c"), rows(&f, "SELECT count(*) FROM p")),
            (1, 2)
        );
        // resolved inside the client's transaction: one group, as before
        let groups = f.metrics.groups.load(Relaxed);
        let o = f.send(
            3,
            false,
            &execs(&[
                "BEGIN",
                "INSERT INTO c VALUES (3, 7)",
                "INSERT INTO p VALUES (7)",
                "COMMIT",
            ]),
        );
        assert!((0..4).all(|i| is_ok(&o, i)), "{o:?}");
        assert_eq!(f.metrics.groups.load(Relaxed), groups + 1, "ran in a group");
    }

    /// Review of 0.1.0: `ROLLBACK TRANSACTION <name> TO <savepoint>` was taken for a whole rollback (checked against
    /// sqld 0.24.32: `no such savepoint`, and the transaction goes on)
    #[test]
    fn a_named_rollback_to_a_savepoint_is_not_a_rollback() {
        let f = fixture(|_| {});
        let o = f.send(
            1,
            false,
            &execs(&[
                "BEGIN",
                "INSERT INTO t(b) VALUES ('kept')",
                "ROLLBACK TRANSACTION t TO nope",
                "COMMIT",
            ]),
        );
        assert_eq!(
            json(&o)[2]["error"]["message"],
            "SQLite error: no such savepoint: nope",
            "{o:?}"
        );
        assert!(is_ok(&o, 3));
        let o = f.send(
            1,
            false,
            &execs(&[
                "BEGIN",
                "INSERT INTO t(b) VALUES ('also kept')",
                "SAVEPOINT s",
                "INSERT INTO t(b) VALUES ('undone')",
                "ROLLBACK TRANSACTION t TO s",
                "COMMIT",
            ]),
        );
        assert!((0..6).all(|i| is_ok(&o, i)), "{o:?}");
        assert_eq!(rows(&f, "SELECT count(*) FROM t WHERE b <> 'undone'"), 2);
        assert_eq!(f.count(), 2);
    }

    /// Review of 0.1.0: a write whose RETURNING rows exceed `--max-response-bytes` was reported as failed although
    /// it was committed (sqld does the same), so a client retrying it wrote twice. Now it is undone.
    #[test]
    fn a_write_whose_returned_rows_are_too_large_is_undone() {
        let f = fixture(|c| c.max_response_bytes = 5000);
        let big = "INSERT INTO t(b) SELECT 'row ' || i || hex(zeroblob(1000)) FROM \
                   (WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 20) SELECT i FROM n) \
                   RETURNING b";
        let too_large = |o: &Outcome, i: usize| json(o)[i]["error"]["code"] == "RESPONSE_TOO_LARGE";
        // grouped
        let o = f.send(
            1,
            false,
            &execs(&[big, "INSERT INTO t(b) VALUES ('after') RETURNING a"]),
        );
        assert!(too_large(&o, 0) && is_ok(&o, 1), "{o:?}");
        assert_eq!(f.count(), 1);
        // on its own, outside a transaction (a PRAGMA makes the request run on its own)
        let o = f.send(1, false, &execs(&["PRAGMA user_version", big]));
        assert!(too_large(&o, 1), "{o:?}");
        assert_eq!(f.count(), 1);
        // inside a client transaction held across requests: only the statement is undone
        let o = f.send(2, false, &execs(&["BEGIN", "INSERT INTO t(b) VALUES ('in tx')"]));
        assert!(matches!(o, Outcome::Done { pinned: true, .. }));
        let o = f.send(2, true, &execs(&[big, "COMMIT"]));
        assert!(too_large(&o, 0) && is_ok(&o, 1), "{o:?}");
        assert_eq!(f.count(), 2);
        // within the limit, nothing changes
        let o = f.send(
            3,
            false,
            &execs(&["INSERT INTO t(b) VALUES ('small') RETURNING b", "PRAGMA user_version"]),
        );
        assert!(is_ok(&o, 0), "{o:?}");
        assert_eq!(json(&o)[0]["response"]["result"]["rows"][0][0]["value"], "small");
        assert_eq!(f.count(), 3);
        assert!(is_ok(
            &f.send(3, false, &execs(&["INSERT INTO t(b) VALUES ('later')"])),
            0
        ));
    }

    #[test]
    fn defer_foreign_keys_set_outside_a_transaction_does_not_reach_other_streams() {
        let f = fixture(|_| {});
        let o = f.send(
            1,
            false,
            &execs(&[
                "CREATE TABLE p(id INTEGER PRIMARY KEY)",
                "CREATE TABLE c(id INTEGER PRIMARY KEY, p INTEGER REFERENCES p(id))",
                "PRAGMA defer_foreign_keys = ON",
            ]),
        );
        assert!(is_ok(&o, 2));
        // sqld: the first statement fails at once (immediate foreign key), the parent is still made
        let o = f.send(
            2,
            false,
            &execs(&["INSERT INTO c VALUES (1, 7)", "INSERT INTO p VALUES (7)"]),
        );
        assert_eq!(json(&o)[0]["error"]["code"], "SQLITE_CONSTRAINT", "{o:?}");
        assert!(is_ok(&o, 1));
        assert_eq!(rows(&f, "SELECT count(*) FROM c"), 0);
    }

    #[test]
    fn vacuum_is_refused_like_sqld() {
        // sqld: unsupported statement; here it could not run anyway, ATTACH is limited to 0 databases (README §4)
        let f = fixture(|_| {});
        assert!(is_ok(&f.send(1, false, &execs(&["INSERT INTO t(b) VALUES ('x')"])), 0));
        let into = f.path.with_file_name("copy");
        for sql in ["VACUUM".to_owned(), format!("vacuum INTO '{}'", into.display())] {
            let o = f.send(1, false, &execs(&[&sql]));
            let e = &json(&o)[0]["error"];
            assert_eq!(e["code"], "SQL_PARSE_ERROR", "{sql}");
            assert_eq!(
                e["message"],
                format!("SQL string could not be parsed: unsupported statement: {sql}")
            );
        }
        assert!(!into.exists());
        assert_eq!(f.count(), 1);
    }

    #[test]
    fn queue_timeout_answers_without_running() {
        let f = fixture(|c| c.queue_timeout_ms = 0);
        let o = f.send(
            1,
            false,
            r#"[{"type":"execute","stmt":{"sql":"INSERT INTO t(b) VALUES ('x')"}}]"#,
        );
        assert!(matches!(o, Outcome::QueueTimeout));
        assert_eq!(f.count(), 0);
    }

    #[test]
    fn idle_checkpoint_keeps_the_wal_file_and_the_next_write_starts_it_over() {
        let f = fixture(|c| c.checkpoint_idle_ms = 50);
        let insert = |i: usize| {
            let o = f.send(
                1,
                false,
                &format!(r#"[{{"type":"execute","stmt":{{"sql":"INSERT INTO t(b) VALUES ('row {i}')"}}}}]"#),
            );
            assert!(matches!(o, Outcome::Done { .. }));
        };
        for i in 0..20 {
            insert(i);
        }
        let deadline = Instant::now() + Duration::from_secs(5);
        while f.metrics.checkpoint_passive.count.load(Relaxed) == 0 && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(10));
        }
        assert!(
            f.metrics.checkpoint_passive.count.load(Relaxed) > 0,
            "no idle checkpoint"
        );
        let wal = || std::fs::metadata(wal_path(&f.path)).map(|m| m.len()).unwrap_or(0);
        let before = wal();
        assert!(before > 0, "an idle checkpoint must not truncate the WAL file");
        insert(20);
        assert_eq!(
            wal(),
            before,
            "the write after an idle checkpoint reuses the WAL from its start"
        );
        let frames = f.metrics.wal_frames.load(Relaxed);
        assert!(frames <= 4, "{frames} frames: the WAL did not start over");
        assert_eq!(f.metrics.checkpoint_restart.count.load(Relaxed), 0);
        assert_eq!(f.count(), 21);
    }

    #[test]
    fn checkpoints_keep_wal_small_and_stop_truncates() {
        let f = fixture(|c| {
            c.checkpoint_pages = 10;
            c.checkpoint_restart_bytes = 64 * 1024;
        });
        for i in 0..200 {
            f.send(
                1,
                false,
                &format!(
                    r#"[{{"type":"execute","stmt":{{"sql":"INSERT INTO t(b) VALUES ('{i}-{}')"}}}}]"#,
                    "x".repeat(500)
                ),
            );
        }
        let wal = std::fs::metadata(wal_path(&f.path)).map(|m| m.len()).unwrap_or(0);
        assert!(wal < 256 * 1024, "wal is {wal} bytes");
        assert!(
            f.metrics.checkpoint_passive.count.load(Relaxed) + f.metrics.checkpoint_restart.count.load(Relaxed) > 0
        );
        f.writer.stop();
        assert_eq!(std::fs::metadata(wal_path(&f.path)).map(|m| m.len()).unwrap_or(0), 0);
        assert_eq!(f.count(), 200);
        let o = f.send(1, false, r#"[{"type":"execute","stmt":{"sql":"SELECT 1"}}]"#);
        assert!(matches!(o, Outcome::Unavailable));
    }

    #[test]
    fn waiting_for_the_checkpoint_thread_ends_with_its_run() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = Config::for_tests(dir.path());
        let path = dir.path().join("data");
        let c = Arc::new(
            Checkpointer::start(Conn::open_writer(&path, &cfg).unwrap(), Arc::new(DbMetrics::default())).unwrap(),
        );
        {
            let mut st = c.lock();
            st.running = true;
            st.done = 7;
        }
        let waiter = {
            let c = c.clone();
            std::thread::spawn(move || c.wait())
        };
        std::thread::sleep(Duration::from_millis(50));
        assert!(!waiter.is_finished(), "returned while the run was going on");
        c.request();
        assert_eq!(waiter.join().unwrap(), 0, "how far the run that just ended got");
        assert_eq!(c.wait(), 0, "nothing running: returns at once");
    }

    #[test]
    fn writes_wait_once_the_checkpoint_thread_is_checkpoint_pages_behind() {
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = Config::for_tests(dir.path());
        cfg.checkpoint_pages = 10;
        let (_tx, rx) = mpsc::channel();
        let mut t = thread(cfg, rx);
        let state = t.checkpointer.as_ref().unwrap().state.clone();
        state.0.lock().unwrap().running = true;
        t.kicked_at = 100;
        t.last_frames = 100;
        // after_commit on another thread: a wrong decision fails the test instead of hanging it
        let after_commit = |mut t: Thread, frames: i64| {
            t.frames.store(frames, Relaxed);
            std::thread::spawn(move || {
                t.after_commit();
                t
            })
        };
        let finished = |h: &JoinHandle<Thread>, within: Duration| {
            let deadline = Instant::now() + within;
            while !h.is_finished() && Instant::now() < deadline {
                std::thread::sleep(Duration::from_millis(5));
            }
            h.is_finished()
        };

        let h = after_commit(t, 110);
        assert!(
            finished(&h, Duration::from_secs(5)),
            "waited with only --checkpoint-pages frames written"
        );
        let t = h.join().unwrap();
        assert_eq!(t.metrics.checkpoint_wait.count.load(Relaxed), 0);

        let h = after_commit(t, 111);
        assert!(
            !finished(&h, Duration::from_millis(100)),
            "did not wait with more than --checkpoint-pages frames written"
        );
        {
            let mut st = state.0.lock().unwrap();
            st.running = false;
            st.done = 100;
        }
        state.1.notify_all();
        assert!(
            finished(&h, Duration::from_secs(5)),
            "still waiting after the run ended"
        );
        let t = h.join().unwrap();
        assert_eq!(t.metrics.checkpoint_wait.count.load(Relaxed), 1);
        assert_eq!(
            t.kicked_at, 111,
            "the checkpoint thread is woken again for the frames written meanwhile"
        );
    }

    /// A write thread outside a running server, for looking at its checkpoint decisions
    fn thread(cfg: Config, rx: mpsc::Receiver<Msg>) -> Thread {
        let metrics = Arc::new(DbMetrics::default());
        Thread::open(
            &cfg.db_path.join("data"),
            Arc::new(cfg.clone()),
            rx,
            Streams::new(Duration::from_secs(60)),
            metrics.clone(),
            ReadGate::new(metrics),
        )
        .unwrap()
    }

    /// Review of 0.1.0: after a failed RESTART the next one waited for `--checkpoint-pages` more frames even when the
    /// WAL had started over by itself in the meantime
    #[test]
    fn a_wal_that_started_over_forgets_the_failed_restart() {
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = Config::for_tests(dir.path());
        cfg.checkpoint_pages = 10;
        cfg.checkpoint_restart_bytes = 20 * 4120;
        let (_tx, rx) = mpsc::channel();
        let mut t = thread(cfg, rx);
        // a RESTART at 500 frames failed: not before 510
        t.last_frames = 500;
        t.restart_after = 510;
        t.starved = true;
        t.gate_retry_at = Some(Instant::now() + Duration::from_secs(3600));
        // the WAL started over and has grown past the RESTART size again
        t.frames.store(30, Relaxed);
        t.after_commit();
        assert_eq!(t.restart_after, 0, "the old WAL's failure was kept");
        assert!(!t.starved && t.gate_retry_at.is_none());
        assert_eq!(
            t.metrics.checkpoint_restart.count.load(Relaxed),
            1,
            "RESTART is due at once on the new WAL"
        );
    }

    /// Review of 0.1.0: `--checkpoint-idle-ms 0` made the write thread spin (a receive with a zero timeout in a loop)
    #[test]
    fn an_idle_write_thread_sleeps() {
        for idle_ms in [0, 20] {
            let f = fixture(|c| c.checkpoint_idle_ms = idle_ms);
            assert!(is_ok(&f.send(1, false, &execs(&["INSERT INTO t(b) VALUES ('x')"])), 0));
            std::thread::sleep(Duration::from_millis(300));
            let wakeups = f.metrics.idle_wakeups.load(Relaxed);
            let expected = if idle_ms == 0 { 0 } else { 1 };
            assert!(
                wakeups <= expected,
                "--checkpoint-idle-ms {idle_ms}: {wakeups} wakeups in 300 ms"
            );
            if idle_ms > 0 {
                assert_eq!(
                    f.metrics.checkpoint_passive.count.load(Relaxed),
                    1,
                    "one idle checkpoint"
                );
            }
            assert!(is_ok(&f.send(1, false, &execs(&["INSERT INTO t(b) VALUES ('y')"])), 0));
            assert_eq!(f.count(), 2);
        }
    }

    /// Review of 0.1.0 (checkpoint starvation): read requests of a few milliseconds following each other closely
    /// leave a reader on the WAL at every moment, so RESTART never found a gap and the WAL grew for as long as the
    /// writes went on (619 MB in 8 s in the review). With the read gate the WAL keeps starting over, and every read
    /// still succeeds.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn reads_following_each_other_closely_do_not_starve_the_restart() {
        const RESTART: u64 = 256 * 1024;
        let f = Arc::new(fixture(|c| {
            c.checkpoint_pages = 32;
            c.checkpoint_restart_bytes = RESTART;
            c.checkpoint_reader_wait_ms = 2_000;
        }));
        let row = "x".repeat(3000);
        let (job, rx) = f.job(
            1,
            false,
            &execs(&["CREATE TABLE big(v TEXT)", "INSERT INTO big VALUES ('seed')"]),
        );
        f.writer.submit(job);
        assert!(is_ok(&rx.await.unwrap(), 1));
        let until = Instant::now() + Duration::from_millis(1500);
        // three clients, each reading back to back: always one of them inside a read
        let readers: Vec<_> = (0..3)
            .map(|_| {
                let f = f.clone();
                tokio::spawn(async move {
                    let mut reads = 0;
                    while Instant::now() < until {
                        let q = serde_json::from_str::<crate::proto::PipelineReq>(&format!(
                            r#"{{"requests":{}}}"#,
                            execs(&[
                                "SELECT (SELECT max(rowid) FROM big), count(*) FROM (WITH RECURSIVE n(i) AS \
                                 (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 200000) SELECT i FROM n)"
                            ])
                        ))
                        .unwrap()
                        .requests;
                        let out = f
                            .readers
                            .run(q, Instant::now() + Duration::from_secs(10))
                            .await
                            .unwrap();
                        let crate::reader::ReadOutcome::Done { results, .. } = out else {
                            panic!("a read did not run")
                        };
                        assert!(matches!(results[0], StreamResult::Ok { .. }), "{results:?}");
                        reads += 1;
                    }
                    reads
                })
            })
            .collect();
        let writers: Vec<_> = (0..2)
            .map(|w| {
                let (f, row) = (f.clone(), row.clone());
                tokio::spawn(async move {
                    let mut i = 0;
                    while Instant::now() < until {
                        let (job, rx) = f.job(
                            10 + w,
                            false,
                            &execs(&[&format!("INSERT INTO big VALUES ('{w}{i}{row}')")]),
                        );
                        f.writer.submit(job);
                        assert!(is_ok(&rx.await.unwrap(), 0));
                        i += 1;
                    }
                    i
                })
            })
            .collect();
        let mut reads = 0;
        for r in readers {
            reads += r.await.unwrap();
        }
        let mut writes = 0;
        for w in writers {
            writes += w.await.unwrap();
        }
        // the WAL file is never truncated while running: its size is the most the WAL ever held
        let wal = std::fs::metadata(wal_path(&f.path)).unwrap().len();
        let written = writes as u64 * 3000;
        assert!(
            reads > 20 && written > 4 * RESTART,
            "{reads} reads, {writes} writes: too little load"
        );
        assert!(
            wal < 3 * RESTART,
            "WAL reached {wal} bytes ({writes} writes of 3 KB, {reads} reads); restarts: {}, gated: {}",
            f.metrics.checkpoint_restart.count.load(Relaxed),
            f.metrics.checkpoint_gate.count.load(Relaxed)
        );
        assert!(
            f.metrics.checkpoint_gate.count.load(Relaxed) > 0,
            "the gate was never needed"
        );
    }
}
