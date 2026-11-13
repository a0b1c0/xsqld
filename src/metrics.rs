//! Counters for `GET /metrics` (Prometheus text format). Plain atomics: updating them costs nothing on the hot path.

use std::fmt::Write as _;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering::Relaxed};
use std::time::Duration;

/// Count, total and largest value of a duration; the largest is reset when read
#[derive(Default)]
pub struct Timer {
    pub count: AtomicU64,
    sum_us: AtomicU64,
    max_us: AtomicU64,
}

impl Timer {
    pub fn record(&self, d: Duration) {
        let us = d.as_micros() as u64;
        self.count.fetch_add(1, Relaxed);
        self.sum_us.fetch_add(us, Relaxed);
        self.max_us.fetch_max(us, Relaxed);
    }
}

#[derive(Default)]
pub struct DbMetrics {
    /// Write requests waiting for the write thread
    pub queue_len: AtomicI64,
    pub read_requests: AtomicU64,
    pub write_requests: AtomicU64,
    /// Read requests that turned out to need the writer
    pub read_to_writer: AtomicU64,
    /// Group transactions committed and the client requests in them
    pub groups: AtomicU64,
    pub group_requests: AtomicU64,
    pub group_size_max: AtomicU64,
    /// SQLite rolled back a group transaction (the group is redone without the request that caused it)
    pub group_lost: AtomicU64,
    pub group_commit_failures: AtomicU64,
    /// Groups committed early because they ran past `--group-max-exec-ms`
    pub group_cuts: AtomicU64,
    /// Requests run on their own (PRAGMA, SAVEPOINT, a transaction left open, …)
    pub solo_requests: AtomicU64,
    pub commit: Timer,
    pub checkpoint_passive: Timer,
    /// Checkpoints for which the write thread waits for readers: RESTART between groups, TRUNCATE on shutdown
    pub checkpoint_restart: Timer,
    /// The write thread waiting for the checkpoint thread to catch up before the next group
    pub checkpoint_wait: Timer,
    /// Checkpoints that could not finish because a reader still used old frames
    pub checkpoint_busy: AtomicU64,
    /// RESTART checkpoints run with new read transactions held back (a RESTART failed before: checkpoint
    /// starvation), timed from closing the gate to opening it
    pub checkpoint_gate: Timer,
    /// Read requests held back by such a checkpoint, and for how long
    pub read_gate_wait: Timer,
    /// Times the write thread woke up because it had been idle for `--checkpoint-idle-ms`
    pub idle_wakeups: AtomicU64,
    pub wal_frames: AtomicI64,
    /// 1 while a transaction left open across HTTP requests holds the writer
    pub pinned: AtomicI64,
    pub pinned_timeouts: AtomicU64,
    pub queue_timeouts: AtomicU64,
    pub queue_full: AtomicU64,
    pub deadline_exceeded: AtomicU64,
    /// Split-range parallel aggregation (README §3.1): statements of the shape it takes
    pub parallel_considered: AtomicU64,
    /// … run split into ranges, and the ranges they ran
    pub parallel_runs: AtomicU64,
    pub parallel_ranges: AtomicU64,
    /// … run on one connection instead, by reason ([`Fallback`])
    pub parallel_fallback: [AtomicU64; Fallback::ALL.len()],
    /// … time to combine the partial rows
    pub parallel_merge: Timer,
}

/// Why a statement of the shape split-range parallel aggregation takes ran on one connection
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Fallback {
    /// The table has fewer rows than `--parallel-min-rows` (by the span of its rowids or the estimate)
    Small,
    /// Not an ordinary rowid table, or its columns do not allow an exact split (collation, affinity, names)
    Table,
    /// SQLite's plan or compiled program is not the one splitting keeps (an index is used, an unlisted function, …)
    Plan,
    /// No additional read connection free right away (or a single CPU)
    Pool,
    /// The connections could not be put on one snapshot
    Snapshot,
    /// Run again after splitting: a sum that is not an integer or could overflow in some order
    Inexact,
    /// Run again after splitting: more partial rows than `--parallel-max-partial-rows`
    Rows,
    /// Run again after splitting: a range failed (an error, the deadline) or the combining failed
    Error,
}

impl Fallback {
    pub const ALL: [Fallback; 8] = [
        Fallback::Small,
        Fallback::Table,
        Fallback::Plan,
        Fallback::Pool,
        Fallback::Snapshot,
        Fallback::Inexact,
        Fallback::Rows,
        Fallback::Error,
    ];

    pub fn label(self) -> &'static str {
        match self {
            Fallback::Small => "small",
            Fallback::Table => "table",
            Fallback::Plan => "plan",
            Fallback::Pool => "pool",
            Fallback::Snapshot => "snapshot",
            Fallback::Inexact => "inexact",
            Fallback::Rows => "rows",
            Fallback::Error => "error",
        }
    }
}

impl DbMetrics {
    pub fn render(&self, ns: &str, out: &mut String) {
        let u = |a: &AtomicU64| a.load(Relaxed);
        let i = |a: &AtomicI64| a.load(Relaxed);
        put(out, "write_queue_length", ns, i(&self.queue_len));
        put(out, "read_requests_total", ns, u(&self.read_requests));
        put(out, "write_requests_total", ns, u(&self.write_requests));
        put(out, "read_to_writer_total", ns, u(&self.read_to_writer));
        put(out, "groups_total", ns, u(&self.groups));
        put(out, "group_requests_total", ns, u(&self.group_requests));
        put(out, "group_size_max", ns, self.group_size_max.swap(0, Relaxed));
        put(out, "group_lost_total", ns, u(&self.group_lost));
        put(out, "group_commit_failures_total", ns, u(&self.group_commit_failures));
        put(out, "group_cuts_total", ns, u(&self.group_cuts));
        put(out, "solo_requests_total", ns, u(&self.solo_requests));
        put(out, "checkpoint_busy_total", ns, u(&self.checkpoint_busy));
        put(out, "wal_frames", ns, i(&self.wal_frames));
        put(out, "pinned_streams", ns, i(&self.pinned));
        put(out, "pinned_timeout_rollbacks_total", ns, u(&self.pinned_timeouts));
        put(out, "queue_timeouts_total", ns, u(&self.queue_timeouts));
        put(out, "queue_full_total", ns, u(&self.queue_full));
        put(out, "deadline_exceeded_total", ns, u(&self.deadline_exceeded));
        put(out, "parallel_considered_total", ns, u(&self.parallel_considered));
        put(out, "parallel_runs_total", ns, u(&self.parallel_runs));
        put(out, "parallel_ranges_total", ns, u(&self.parallel_ranges));
        for (f, n) in Fallback::ALL.iter().zip(&self.parallel_fallback) {
            let _ = writeln!(
                out,
                "xsqld_parallel_fallback_total{{ns=\"{ns}\",reason=\"{}\"}} {}",
                f.label(),
                u(n)
            );
        }
        for (name, t) in [
            ("commit", &self.commit),
            ("checkpoint_passive", &self.checkpoint_passive),
            ("checkpoint_restart", &self.checkpoint_restart),
            ("checkpoint_wait", &self.checkpoint_wait),
            ("checkpoint_gate", &self.checkpoint_gate),
            ("read_gate_wait", &self.read_gate_wait),
            ("parallel_merge", &self.parallel_merge),
        ] {
            put(out, &format!("{name}_count"), ns, u(&t.count));
            put(out, &format!("{name}_seconds_sum"), ns, Secs(u(&t.sum_us)));
            put(out, &format!("{name}_seconds_max"), ns, Secs(t.max_us.swap(0, Relaxed)));
        }
    }
}

struct Secs(u64);

impl std::fmt::Display for Secs {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{:.6}", self.0 as f64 / 1e6)
    }
}

/// `xsqld_build_info{version="…",commit="…"} 1`; both values need no escaping (`build.rs`)
pub fn build_info(out: &mut String) {
    let _ = writeln!(
        out,
        "xsqld_build_info{{version=\"{}\",commit=\"{}\"}} 1",
        crate::config::VERSION,
        crate::config::COMMIT
    );
}

pub fn put(out: &mut String, name: &str, ns: &str, v: impl std::fmt::Display) {
    let _ = if ns.is_empty() {
        writeln!(out, "xsqld_{name} {v}")
    } else {
        writeln!(out, "xsqld_{name}{{ns=\"{ns}\"}} {v}")
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn renders() {
        let m = DbMetrics::default();
        m.commit.record(Duration::from_millis(3));
        m.commit.record(Duration::from_millis(5));
        m.groups.fetch_add(2, Relaxed);
        let mut out = String::new();
        m.render("tenant", &mut out);
        assert!(out.contains("xsqld_groups_total{ns=\"tenant\"} 2\n"));
        assert!(out.contains("xsqld_commit_count{ns=\"tenant\"} 2\n"));
        assert!(out.contains("xsqld_commit_seconds_sum{ns=\"tenant\"} 0.008000\n"));
        assert!(out.contains("xsqld_commit_seconds_max{ns=\"tenant\"} 0.005000\n"));
        m.parallel_fallback[Fallback::Pool as usize].fetch_add(1, Relaxed);
        assert!(out.contains("xsqld_parallel_fallback_total{ns=\"tenant\",reason=\"small\"} 0\n"));
        let mut again = String::new();
        m.render("tenant", &mut again);
        assert!(again.contains("xsqld_commit_seconds_max{ns=\"tenant\"} 0.000000\n"));
        assert!(again.contains("xsqld_parallel_fallback_total{ns=\"tenant\",reason=\"pool\"} 1\n"));
    }
}
