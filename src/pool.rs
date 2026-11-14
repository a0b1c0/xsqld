//! Worker threads shared by all databases, and a timer.
//!
//! A database used to own two threads for as long as it was open (its write thread and its checkpoint thread),
//! whether it had work or not: 3500 open databases were 7000 threads. Now a database takes a worker only while it
//! has something to run (`writer.rs`) and gives it back when its queue is empty; what it must do later (the idle
//! checkpoint, rolling back a transaction left open) it asks the timer for.
//!
//! The pool grows with the work: a task that finds no worker waiting gets a new thread, up to
//! `--max-worker-threads`, and a worker that found nothing to do for [`WORKER_IDLE`] ends. Tasks may block (a
//! commit's fsync, a group lingering for more requests): a blocked worker only costs its thread, the next task gets
//! another one. At the limit tasks wait for a worker to finish its task: thousands of databases whose idle
//! checkpoints fall due in the same second (they all stopped being written to together) then take turns instead of
//! starting a thread each. A task that panics takes the whole process down (`main.rs`: a panic on any thread aborts,
//! and there is a test for a pool task and a timer task), so neither a worker nor the timer thread is ever lost to
//! one and nothing here catches panics. No task waits for another task that has not begun (the writer only waits for a checkpoint
//! that is running, `writer.rs`), so waiting for a worker cannot deadlock.

use std::cmp::Reverse;
use std::collections::{BinaryHeap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, Condvar, LazyLock, Mutex, MutexGuard};
use std::time::{Duration, Instant};

type Task = Box<dyn FnOnce() + Send + 'static>;

/// A worker without work for this long ends (tests do not wait that long)
const WORKER_IDLE: Duration = Duration::from_millis(if cfg!(test) { 400 } else { 10_000 });

#[derive(Default)]
struct Queue {
    tasks: VecDeque<Task>,
    /// Workers waiting for a task, the one that began waiting last at the end. A task goes to that one: the same
    /// few workers then do the work there is, and the others reach [`WORKER_IDLE`] and end. Woken in turn, every
    /// worker would get a task now and then and none would ever end (256 threads stayed for work that 40 did).
    idle: Vec<(u64, Arc<Condvar>)>,
    next_id: u64,
}

struct Pool {
    queue: Mutex<Queue>,
}

static POOL: LazyLock<Pool> = LazyLock::new(|| Pool {
    queue: Mutex::new(Queue::default()),
});

/// Most worker threads at once (`--max-worker-threads`)
static MAX_WORKERS: AtomicU64 = AtomicU64::new(256);

pub fn set_max_workers(n: usize) {
    MAX_WORKERS.store(n.max(1) as u64, Relaxed);
}

/// Worker threads alive now, started in all, and the most alive at once (`/metrics`)
pub static WORKERS: AtomicU64 = AtomicU64::new(0);
pub static WORKERS_STARTED: AtomicU64 = AtomicU64::new(0);
pub static WORKERS_MAX: AtomicU64 = AtomicU64::new(0);

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

/// Runs `task` on a worker thread: on one that is waiting, or on a new one; at the limit, on the first worker that
/// finishes its task
pub fn spawn(task: impl FnOnce() + Send + 'static) {
    let pool = &*POOL;
    let mut q = lock(&pool.queue);
    q.tasks.push_back(Box::new(task));
    if let Some((_, wake)) = q.idle.pop() {
        // taken off the list here, under the lock: the next task wakes another worker or starts one
        drop(q);
        wake.notify_one();
        return;
    }
    // counted under the queue's lock, so the limit holds exactly
    if WORKERS.load(Relaxed) >= MAX_WORKERS.load(Relaxed) {
        return;
    }
    let n = WORKERS.fetch_add(1, Relaxed) + 1;
    q.next_id += 1;
    let id = q.next_id;
    drop(q);
    WORKERS_MAX.fetch_max(n, Relaxed);
    WORKERS_STARTED.fetch_add(1, Relaxed);
    let spawned = std::thread::Builder::new()
        .name("xsqld-worker".into())
        .spawn(move || worker(id));
    if let Err(e) = spawned {
        // under the queue's lock, like every other change of the count
        let _q = lock(&pool.queue);
        WORKERS.fetch_sub(1, Relaxed);
        // the task stays queued: a worker that finishes its task takes it. With no worker at all it would wait
        // for the next spawn, so say so loudly
        tracing::error!(error = %e, "could not start a worker thread");
    }
}

fn worker(id: u64) {
    let pool = &*POOL;
    let wake = Arc::new(Condvar::new());
    let mut q = lock(&pool.queue);
    loop {
        if let Some(task) = q.tasks.pop_front() {
            drop(q);
            task();
            q = lock(&pool.queue);
            continue;
        }
        q.idle.push((id, wake.clone()));
        // on the list: nobody has given this worker a task
        let listed = |q: &mut Queue| q.idle.iter().any(|(i, _)| *i == id);
        let (guard, timeout) = wake
            .wait_timeout_while(q, WORKER_IDLE, listed)
            .unwrap_or_else(|e| e.into_inner());
        q = guard;
        if timeout.timed_out()
            && let Some(at) = q.idle.iter().position(|(i, _)| *i == id)
        {
            q.idle.remove(at);
            if q.tasks.is_empty() {
                // under the queue's lock, like the count in `spawn`
                WORKERS.fetch_sub(1, Relaxed);
                return;
            }
        }
    }
}

struct Timers {
    /// Earliest first; the sequence number keeps entries of the same instant apart (and in order)
    heap: BinaryHeap<(Reverse<Instant>, Reverse<u64>, TimerTask)>,
    seq: u64,
    running: bool,
}

struct TimerTask(Task);

impl PartialEq for TimerTask {
    fn eq(&self, _: &Self) -> bool {
        true
    }
}
impl Eq for TimerTask {}
impl PartialOrd for TimerTask {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for TimerTask {
    fn cmp(&self, _: &Self) -> std::cmp::Ordering {
        std::cmp::Ordering::Equal
    }
}

struct Timer {
    timers: Mutex<Timers>,
    changed: Condvar,
}

static TIMER: LazyLock<Timer> = LazyLock::new(|| Timer {
    timers: Mutex::new(Timers {
        heap: BinaryHeap::new(),
        seq: 0,
        running: false,
    }),
    changed: Condvar::new(),
});

/// Runs `task` once `when` has come, on the timer thread: it must be short and must not block (it looks at some
/// state and hands the work to [`spawn`]). Timers are not cancelled: a task that is no longer needed must find that
/// out itself (the ones of `writer.rs` hold a weak reference and look at the state they find).
pub fn at(when: Instant, task: impl FnOnce() + Send + 'static) {
    let timer = &*TIMER;
    let mut t = lock(&timer.timers);
    t.seq += 1;
    let seq = t.seq;
    t.heap.push((Reverse(when), Reverse(seq), TimerTask(Box::new(task))));
    if !t.running {
        t.running = true;
        if let Err(e) = std::thread::Builder::new()
            .name("xsqld-timer".into())
            .spawn(timer_thread)
        {
            t.running = false;
            tracing::error!(error = %e, "could not start the timer thread");
        }
    }
    drop(t);
    timer.changed.notify_one();
}

fn timer_thread() {
    let timer = &*TIMER;
    let mut t = lock(&timer.timers);
    loop {
        let now = Instant::now();
        match t.heap.peek().map(|(Reverse(when), _, _)| *when) {
            Some(when) if when <= now => {
                let (_, _, TimerTask(task)) = t.heap.pop().expect("peeked");
                drop(t);
                task();
                t = lock(&timer.timers);
            }
            Some(when) => {
                t = timer
                    .changed
                    .wait_timeout(t, when - now)
                    .unwrap_or_else(|e| e.into_inner())
                    .0;
            }
            None => t = timer.changed.wait(t).unwrap_or_else(|e| e.into_inner()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;
    use std::sync::mpsc;

    #[test]
    fn tasks_run_at_once_even_while_others_block() {
        // 20 tasks that all wait for the last one: with a fixed number of workers the last would never start
        let (tx, rx) = mpsc::channel::<()>();
        let rx = Arc::new(Mutex::new(rx));
        let done = Arc::new(AtomicUsize::new(0));
        for _ in 0..20 {
            let (rx, done) = (rx.clone(), done.clone());
            spawn(move || {
                let _ = lock(&rx).recv_timeout(Duration::from_secs(10));
                done.fetch_add(1, Relaxed);
            });
        }
        let (started_tx, started) = mpsc::channel();
        spawn(move || started_tx.send(()).unwrap());
        started
            .recv_timeout(Duration::from_secs(5))
            .expect("the task behind 20 blocked ones did not start");
        for _ in 0..20 {
            tx.send(()).unwrap();
        }
        let until = Instant::now() + Duration::from_secs(5);
        while done.load(Relaxed) < 20 && Instant::now() < until {
            std::thread::sleep(Duration::from_millis(5));
        }
        assert_eq!(done.load(Relaxed), 20);
    }

    #[test]
    fn the_worker_count_follows_the_limit() {
        // other tests of this process use the pool too: only this one changes the limit, upwards, and it looks at
        // the count from below (never more than the limit) and at its own tasks all running
        set_max_workers(300);
        let (tx, rx) = mpsc::channel();
        for i in 0..600 {
            let tx = tx.clone();
            spawn(move || {
                std::thread::sleep(Duration::from_millis(20));
                tx.send(i).unwrap();
            });
            assert!(WORKERS.load(Relaxed) <= 300, "more workers than the limit");
        }
        let mut got: Vec<i32> = (0..600)
            .map(|_| rx.recv_timeout(Duration::from_secs(20)).unwrap())
            .collect();
        got.sort();
        assert_eq!(
            got,
            (0..600).collect::<Vec<_>>(),
            "tasks beyond the limit did not all run"
        );
        assert!(WORKERS_MAX.load(Relaxed) <= 300);
    }

    /// After a burst, work that a few workers can do must not keep all of them: tasks go to the worker that began
    /// waiting last, the others wait out their time and end
    #[test]
    fn workers_a_burst_started_end_while_work_goes_on() {
        let (tx, rx) = mpsc::channel();
        for _ in 0..80 {
            let tx = tx.clone();
            spawn(move || {
                std::thread::sleep(Duration::from_millis(100));
                tx.send(()).unwrap();
            });
        }
        for _ in 0..80 {
            rx.recv_timeout(Duration::from_secs(10)).unwrap();
        }
        let after_burst = WORKERS.load(Relaxed);
        assert!(after_burst >= 80, "{after_burst} workers after 80 tasks at once");
        // one short task at a time, for three times the idle time
        let until = Instant::now() + 3 * WORKER_IDLE;
        while Instant::now() < until {
            let tx = tx.clone();
            spawn(move || tx.send(()).unwrap());
            rx.recv_timeout(Duration::from_secs(10)).unwrap();
            std::thread::sleep(Duration::from_millis(2));
        }
        // (other tests of this process use the pool at the same time: not an exact number)
        let later = WORKERS.load(Relaxed);
        assert!(
            later + 40 <= after_burst,
            "{later} of {after_burst} workers still there: the work kept them all"
        );
    }

    #[test]
    fn timers_fire_in_order_and_not_early() {
        let (tx, rx) = mpsc::channel();
        let start = Instant::now();
        for (i, ms) in [(2, 60u64), (1, 30), (3, 90)] {
            let tx = tx.clone();
            at(start + Duration::from_millis(ms), move || {
                tx.send((i, start.elapsed())).unwrap()
            });
        }
        let got: Vec<(i32, Duration)> = (0..3)
            .map(|_| rx.recv_timeout(Duration::from_secs(5)).unwrap())
            .collect();
        assert_eq!(got.iter().map(|g| g.0).collect::<Vec<_>>(), [1, 2, 3]);
        for (i, took) in got {
            assert!(
                took >= Duration::from_millis(30 * i as u64),
                "timer {i} fired after {took:?}"
            );
        }
    }
}
