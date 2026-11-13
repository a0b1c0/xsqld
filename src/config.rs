use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use clap::{Parser, ValueEnum};

/// Files SQLite keeps next to a database file, named by appending these
pub const SIDE_SUFFIXES: [&str; 3] = ["-wal", "-shm", "-journal"];

/// Largest `--max-like-pattern-bytes`: the deepest recursion of SQLite's LIKE / GLOB matcher such a pattern allows
/// still fits on a 2 MiB thread stack (`long_like_patterns_are_refused` runs it there)
pub const MAX_LIKE_PATTERN_BYTES: i32 = 10_000;

/// Release version; Cargo.toml is its only source (release tags are `xsqld-v<version>`)
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
/// Git commit the binary was built from (`build.rs`), `unknown` without git
pub const COMMIT: &str = env!("XSQLD_GIT_COMMIT");

/// Command line flags. Names and defaults of the first group follow sqld, so an existing
/// `sqld --db-path … --http-listen-addr … --admin-listen-addr … --enable-namespaces` command line works unchanged.
#[derive(Parser, Debug, Clone)]
#[command(
    name = "xsqld",
    version = concat!(env!("CARGO_PKG_VERSION"), " (", env!("XSQLD_GIT_COMMIT"), ")"),
    about = "Local-only SQLite server speaking the sqld Hrana-over-HTTP subset"
)]
pub struct Config {
    /// Data directory; database files are resolved from it with --db-file-template
    #[arg(long, default_value = "data.sqld")]
    pub db_path: PathBuf,
    /// Hrana over HTTP (`/v2/pipeline`, `/dump`, `/health`, `/metrics`)
    #[arg(long, default_value = "127.0.0.1:8080")]
    pub http_listen_addr: SocketAddr,
    /// Admin API (`/v1/namespaces/<ns>/create`, `DELETE /v1/namespaces/<ns>`, `/metrics`)
    #[arg(long, default_value = "127.0.0.1:8081")]
    pub admin_listen_addr: SocketAddr,
    /// Accepted for sqld command line compatibility; namespaces are always on
    #[arg(long, hide = true)]
    pub enable_namespaces: bool,
    /// Accepted for sqld command line compatibility; ignored (there is no replication log)
    #[arg(long, hide = true)]
    pub max_log_size: Option<u64>,

    /// Database file of a namespace, relative to --db-path; `{ns}` is replaced by the namespace name
    #[arg(long, default_value = "dbs/{ns}/data")]
    pub db_file_template: String,
    /// Allow listening on non-loopback addresses (refused by default)
    #[arg(long)]
    pub allow_non_loopback: bool,
    /// Allow `dump_url` hosts other than loopback addresses (refused by default)
    #[arg(long)]
    pub allow_remote_dump_url: bool,
    /// Requests are answered only when their Host header is an IP address, a name without a dot (`localhost`,
    /// `sqld-admin`) or a name under one of these suffixes (comma separated); others get 403. `*` accepts every Host
    #[arg(long, value_delimiter = ',', default_value = "db,local,localhost")]
    pub allowed_host_suffixes: Vec<String>,

    /// PRAGMA synchronous of the write connection
    #[arg(long, value_enum, default_value = "full")]
    pub synchronous: Synchronous,
    /// PRAGMA cache_size of every connection (negative = KiB, like SQLite)
    #[arg(long, default_value_t = -2000, allow_hyphen_values = true)]
    pub cache_size: i64,
    /// Read connections per database: this many slow reads fill the pool and the next reads of that database wait.
    /// Default: the number of CPUs, at least 16
    #[arg(long, default_value_t = default_read_pool_size())]
    pub read_pool_size: usize,

    /// Most client transactions committed together in one group
    #[arg(long, default_value_t = 64)]
    pub group_max: usize,
    /// How long the write thread waits for more work before committing a group, in microseconds
    #[arg(long, default_value_t = 0)]
    pub group_linger_us: u64,
    /// A group that has been running this long is committed before its next request: the requests already done get
    /// their reply instead of waiting for slow ones after them
    #[arg(long, default_value_t = 20)]
    pub group_max_exec_ms: u64,
    /// Most write requests waiting for one database; more are answered with 503 without running
    #[arg(long, default_value_t = 4096)]
    pub max_write_queue: usize,
    /// A write request waiting longer than this in the queue is answered with an error without running
    #[arg(long, default_value_t = 40_000)]
    pub queue_timeout_ms: u64,
    /// Time limit of one HTTP request (queueing plus execution); SQL still running at the deadline is interrupted
    #[arg(long, default_value_t = 40_000)]
    pub request_timeout_ms: u64,
    /// A stream (baton) unused for this long expires
    #[arg(long, default_value_t = 10_000)]
    pub stream_idle_ms: u64,
    /// A transaction left open across HTTP requests is rolled back after this long
    #[arg(long, default_value_t = 5_000)]
    pub pinned_tx_timeout_ms: u64,

    /// Passive checkpoint (checkpoint thread) once the WAL holds this many frames; a tail this long or shorter left by
    /// that thread is copied by the write thread between groups. Writes wait for a checkpoint run once this many
    /// frames have been written since it began
    #[arg(long, default_value_t = 1000)]
    pub checkpoint_pages: u32,
    /// Once the WAL holds this many bytes the write thread checkpoints all of it between groups and waits for
    /// readers (RESTART), so that the WAL starts over
    #[arg(long, default_value_t = 32 * 1024 * 1024)]
    pub checkpoint_restart_bytes: u64,
    /// After a RESTART failed because of readers, the next one holds back new read requests and waits up to this
    /// long for the reads already running to end (checkpoint starvation; README §3)
    #[arg(long, default_value_t = 200)]
    pub checkpoint_reader_wait_ms: u64,
    /// Checkpoint by the write thread after it has been idle this long (0: never). The WAL file keeps its size; it
    /// is only truncated on shutdown
    #[arg(long, default_value_t = 1_000)]
    pub checkpoint_idle_ms: u64,
    /// PRAGMA journal_size_limit of the write connection
    #[arg(long, default_value_t = 64 * 1024 * 1024)]
    pub journal_size_limit: i64,

    /// Largest request body
    #[arg(long, default_value_t = 16 * 1024 * 1024)]
    pub max_request_bytes: usize,
    /// Open client connections per port; further connections wait in the listen backlog
    #[arg(long, default_value_t = 1024)]
    pub max_connections: usize,
    /// Requests being handled at once per port; more are answered with 503
    #[arg(long, default_value_t = 512)]
    pub max_concurrent_requests: usize,
    /// A connection must send a complete request head within this long (also ends idle keep-alive connections)
    #[arg(long, default_value_t = 10_000)]
    pub header_read_timeout_ms: u64,
    /// A request body must arrive within this long after its head
    #[arg(long, default_value_t = 10_000)]
    pub body_read_timeout_ms: u64,
    /// A response the client takes no data of for this long is abandoned and its connection closed
    #[arg(long, default_value_t = 60_000)]
    pub response_write_timeout_ms: u64,
    /// `GET /dump`: the client must take the next chunk within this long, or the dump is abandoned
    #[arg(long, default_value_t = 60_000)]
    pub dump_stall_timeout_ms: u64,
    /// `GET /dump`: longest a dump may take
    #[arg(long, default_value_t = 3_600_000)]
    pub dump_max_ms: u64,
    /// `GET /dump` running at once (each copies its database to disk first); more are answered with 503
    #[arg(long, default_value_t = 2)]
    pub max_concurrent_dumps: usize,
    /// `dump_url`: longest the whole download may take
    #[arg(long, default_value_t = 1_800_000)]
    pub dump_url_timeout_ms: u64,
    /// Creating a database from a dump: longest one statement of the dump may be, in bytes
    #[arg(long, default_value_t = 128 * 1024 * 1024)]
    pub dump_load_max_statement_bytes: usize,
    /// Creating a database from a dump: longest the whole load (download included) may take
    #[arg(long, default_value_t = 1_800_000)]
    pub dump_load_timeout_ms: u64,
    /// Most statements (execute requests plus batch steps) in one HTTP request
    #[arg(long, default_value_t = 4096)]
    pub max_statements: usize,
    /// Largest result of one HTTP request (estimated JSON size of all rows)
    #[arg(long, default_value_t = 64 * 1024 * 1024)]
    pub max_response_bytes: u64,
    /// SQLITE_LIMIT_LENGTH: largest string or blob value (sqld's max_row_size)
    #[arg(long, default_value_t = 5 * 1024 * 1024)]
    pub max_value_bytes: i32,
    /// SQLITE_LIMIT_LIKE_PATTERN_LENGTH: longest LIKE or GLOB pattern (SQLite and sqld: 50000)
    #[arg(long, default_value_t = 1000)]
    pub max_like_pattern_bytes: i32,
}

#[derive(ValueEnum, Debug, Clone, Copy, PartialEq, Eq)]
pub enum Synchronous {
    Off,
    Normal,
    Full,
    Extra,
}

impl Synchronous {
    pub fn pragma(self) -> &'static str {
        match self {
            Self::Off => "OFF",
            Self::Normal => "NORMAL",
            Self::Full => "FULL",
            Self::Extra => "EXTRA",
        }
    }
}

impl Config {
    pub fn queue_timeout(&self) -> Duration {
        Duration::from_millis(self.queue_timeout_ms)
    }
    pub fn request_timeout(&self) -> Duration {
        Duration::from_millis(self.request_timeout_ms)
    }
    pub fn stream_idle(&self) -> Duration {
        Duration::from_millis(self.stream_idle_ms)
    }
    pub fn pinned_tx_timeout(&self) -> Duration {
        Duration::from_millis(self.pinned_tx_timeout_ms)
    }
    pub fn checkpoint_idle(&self) -> Duration {
        Duration::from_millis(self.checkpoint_idle_ms)
    }
    pub fn group_max_exec(&self) -> Duration {
        Duration::from_millis(self.group_max_exec_ms)
    }
    pub fn header_read_timeout(&self) -> Duration {
        Duration::from_millis(self.header_read_timeout_ms)
    }
    pub fn body_read_timeout(&self) -> Duration {
        Duration::from_millis(self.body_read_timeout_ms)
    }
    pub fn dump_stall_timeout(&self) -> Duration {
        Duration::from_millis(self.dump_stall_timeout_ms)
    }
    pub fn dump_max(&self) -> Duration {
        Duration::from_millis(self.dump_max_ms)
    }
    pub fn dump_url_timeout(&self) -> Duration {
        Duration::from_millis(self.dump_url_timeout_ms)
    }
    pub fn dump_load_timeout(&self) -> Duration {
        Duration::from_millis(self.dump_load_timeout_ms)
    }
    pub fn checkpoint_reader_wait(&self) -> Duration {
        Duration::from_millis(self.checkpoint_reader_wait_ms)
    }
    pub fn response_write_timeout(&self) -> Duration {
        Duration::from_millis(self.response_write_timeout_ms)
    }

    /// Is a request with this Host header answered? (`--allowed-host-suffixes`)
    ///
    /// The database is chosen by the Host header and nothing else identifies a client, so a web page whose domain
    /// has been re-pointed at this machine (DNS rebinding) would read and write as a local client. Such a page's
    /// Host is the attacker's domain; the hosts local clients use are IP addresses, single names and names under a
    /// few private suffixes.
    pub fn host_allowed(&self, host: &str) -> bool {
        if self.allowed_host_suffixes.iter().any(|s| s == "*") {
            return true;
        }
        // `[::1]:8080`: an IPv6 address
        if host.starts_with('[') {
            return true;
        }
        let name = host.split(':').next().unwrap_or_default();
        let name = name.strip_suffix('.').unwrap_or(name);
        if !name.contains('.') || name.parse::<std::net::Ipv4Addr>().is_ok() {
            return true;
        }
        let name = name.as_bytes();
        self.allowed_host_suffixes.iter().any(|s| {
            let s = s.trim_start_matches('.').as_bytes();
            !s.is_empty()
                && name.len() > s.len()
                && name[name.len() - s.len() - 1] == b'.'
                && name[name.len() - s.len()..].eq_ignore_ascii_case(s)
        })
    }

    /// Scratch files of the server (copies made for `GET /dump`); emptied at startup
    pub fn tmp_dir(&self) -> PathBuf {
        self.db_path.join("xsqld-tmp")
    }

    /// Database file of a namespace
    pub fn db_file(&self, ns: &str) -> PathBuf {
        self.db_path.join(self.db_file_template.replace("{ns}", ns))
    }

    /// The template puts each namespace in a directory of its own (sqld layout `dbs/{ns}/data`):
    /// deleting a namespace removes that directory
    pub fn per_namespace_dir(&self) -> bool {
        let t = self.db_file_template.replace('\\', "/");
        t.split('/').rev().skip(1).any(|part| part == "{ns}")
    }

    /// A name the server may use as a database: a valid name whose file does not fall inside the server's scratch
    /// directory (`xsqld-tmp` under a template such as `{ns}/data`, emptied at every start)
    pub fn usable_namespace(&self, ns: &str) -> bool {
        valid_namespace(ns) && !self.db_file(ns).starts_with(self.tmp_dir())
    }

    pub fn validate(&self) -> anyhow::Result<()> {
        self.validate_template()?;
        for addr in [self.http_listen_addr, self.admin_listen_addr] {
            if !addr.ip().is_loopback() && !self.allow_non_loopback {
                anyhow::bail!("refusing to listen on non-loopback address {addr} (pass --allow-non-loopback)");
            }
        }
        for (flag, value, min, max) in self.numeric_bounds() {
            if !(min..=max).contains(&value) {
                anyhow::bail!("--{flag} is {value}; it must be between {min} and {max}");
            }
        }
        Ok(())
    }

    /// `--db-file-template` must keep every database inside `--db-path` and the files of one database apart from
    /// those of every other: with a file name ending in `{ns}`, the database `a-wal` would be the WAL of `a`, and
    /// deleting one would remove the other's files
    fn validate_template(&self) -> anyhow::Result<()> {
        let t = &self.db_file_template;
        if !t.contains("{ns}") {
            anyhow::bail!("--db-file-template must contain {{ns}}");
        }
        let path = std::path::Path::new(t);
        if path.is_absolute() || t.split(['/', '\\']).any(|p| p.is_empty() || p == "." || p == "..") {
            anyhow::bail!("--db-file-template must be a relative path below --db-path, without `.` or `..`: {t}");
        }
        let file = path.file_name().and_then(|f| f.to_str()).unwrap_or_default();
        if let Some(at) = file.rfind("{ns}") {
            // the file of database B is a side file of database A when B + S = A + S + side, i.e. when S + side
            // ends with S and what it adds in front of S could be part of a name
            let s = &file[at + "{ns}".len()..];
            for side in SIDE_SUFFIXES {
                let longer = format!("{s}{side}");
                let added = &longer[..side.len()];
                if longer.ends_with(s) && valid_namespace(&format!("a{added}")) {
                    anyhow::bail!(
                        "--db-file-template {t}: the file of a database named `<name>{added}` would be the \
                         `{side}` file of the database `<name>`; put something after {{ns}} in the file name, e.g. \
                         {{ns}}.db"
                    );
                }
            }
        }
        Ok(())
    }

    /// Every numeric flag with the range it must be in. Upper bounds keep durations from overflowing the clock
    /// (`Instant + Duration` panics), counts within what a semaphore holds, and sizes and SQLite limits in a range
    /// where they still limit something (SQLite ignores a negative limit and keeps its own default)
    fn numeric_bounds(&self) -> [(&'static str, i128, i128, i128); 31] {
        const HOUR_MS: i128 = 3_600_000;
        const GIB: i128 = 1 << 30;
        let n = |v: usize| v as i128;
        let u = |v: u64| v as i128;
        [
            ("cache-size", self.cache_size as i128, -(1 << 30), 1 << 30),
            ("read-pool-size", n(self.read_pool_size), 1, 1024),
            ("group-max", n(self.group_max), 1, 65_536),
            ("group-linger-us", u(self.group_linger_us), 0, 1_000_000),
            ("group-max-exec-ms", u(self.group_max_exec_ms), 0, 60_000),
            ("max-write-queue", n(self.max_write_queue), 1, 1_000_000),
            ("queue-timeout-ms", u(self.queue_timeout_ms), 0, HOUR_MS),
            ("request-timeout-ms", u(self.request_timeout_ms), 1, HOUR_MS),
            ("stream-idle-ms", u(self.stream_idle_ms), 1, 24 * HOUR_MS),
            ("pinned-tx-timeout-ms", u(self.pinned_tx_timeout_ms), 1, HOUR_MS),
            ("checkpoint-pages", self.checkpoint_pages as i128, 1, 1_000_000),
            (
                "checkpoint-restart-bytes",
                u(self.checkpoint_restart_bytes),
                4096,
                1024 * GIB,
            ),
            (
                "checkpoint-reader-wait-ms",
                u(self.checkpoint_reader_wait_ms),
                0,
                60_000,
            ),
            ("checkpoint-idle-ms", u(self.checkpoint_idle_ms), 0, HOUR_MS),
            ("journal-size-limit", self.journal_size_limit as i128, -1, 1024 * GIB),
            ("max-request-bytes", n(self.max_request_bytes), 1024, GIB),
            ("max-connections", n(self.max_connections), 1, 1_000_000),
            ("max-concurrent-requests", n(self.max_concurrent_requests), 1, 1_000_000),
            ("header-read-timeout-ms", u(self.header_read_timeout_ms), 1, HOUR_MS),
            ("body-read-timeout-ms", u(self.body_read_timeout_ms), 1, HOUR_MS),
            (
                "response-write-timeout-ms",
                u(self.response_write_timeout_ms),
                1,
                HOUR_MS,
            ),
            ("dump-stall-timeout-ms", u(self.dump_stall_timeout_ms), 1, 24 * HOUR_MS),
            ("dump-max-ms", u(self.dump_max_ms), 0, 7 * 24 * HOUR_MS),
            ("max-concurrent-dumps", n(self.max_concurrent_dumps), 1, 1024),
            ("dump-url-timeout-ms", u(self.dump_url_timeout_ms), 1, 7 * 24 * HOUR_MS),
            (
                "dump-load-max-statement-bytes",
                n(self.dump_load_max_statement_bytes),
                1024,
                GIB,
            ),
            (
                "dump-load-timeout-ms",
                u(self.dump_load_timeout_ms),
                1,
                7 * 24 * HOUR_MS,
            ),
            ("max-statements", n(self.max_statements), 1, 1_000_000),
            ("max-response-bytes", u(self.max_response_bytes), 1024, 16 * GIB),
            // SQLITE_MAX_LENGTH, SQLite's own ceiling
            ("max-value-bytes", self.max_value_bytes as i128, 1, 1_000_000_000),
            // the deepest recursion of SQLite's matcher such a pattern allows still fits on the smallest thread
            // stack of the server (2 MiB; README §1.1)
            (
                "max-like-pattern-bytes",
                self.max_like_pattern_bytes as i128,
                1,
                MAX_LIKE_PATTERN_BYTES as i128,
            ),
        ]
    }

    #[cfg(test)]
    pub fn for_tests(dir: &std::path::Path) -> Self {
        let mut c = Config::parse_from(["xsqld"]);
        c.db_path = dir.to_path_buf();
        c
    }
}

/// One read per CPU at a time, but never fewer than 16 connections: fewer would let a handful of slow queries hold up
/// every quick read of the database. `available_parallelism` follows CPU affinity and cgroup quotas.
fn default_read_pool_size() -> usize {
    std::thread::available_parallelism().map_or(1, |n| n.get()).max(16)
}

/// Namespace names: letters, digits, `-` and `_`, starting with a letter or digit (also keeps paths safe)
pub fn valid_namespace(ns: &str) -> bool {
    let b = ns.as_bytes();
    !b.is_empty()
        && b.len() <= 64
        && b[0].is_ascii_alphanumeric()
        && b.iter().all(|c| c.is_ascii_alphanumeric() || *c == b'-' || *c == b'_')
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::CommandFactory;

    #[test]
    fn namespace_names() {
        assert!(valid_namespace("tenant"));
        assert!(valid_namespace("restore-check"));
        assert!(valid_namespace("TDB_000"));
        assert!(!valid_namespace(""));
        assert!(!valid_namespace("../x"));
        assert!(!valid_namespace(".tmp"));
        assert!(!valid_namespace("a/b"));
        assert!(!valid_namespace(&"a".repeat(65)));
    }

    /// Review of 0.1.0, finding 6: the hosts the local clients use are accepted, a public domain is not
    #[test]
    fn host_allowlist() {
        let c = Config::for_tests(std::path::Path::new("/tmp"));
        for host in [
            "tenant.db",
            "tenant.local",
            "TDB_000.Local:8080",
            "platform.localhost:8080",
            "tenant.db.",
            "sqld-admin",
            "localhost",
            "localhost:8081",
            "127.0.0.1",
            "127.0.0.1:8080",
            "10.0.0.5:8080",
            "[::1]:8080",
            "",
        ] {
            assert!(c.host_allowed(host), "{host}");
        }
        for host in [
            "tenant.evil.example",
            "tenant.evil.example:8080",
            "tenant.db.evil.example",
            "evil.example",
            "tenantdb.xlocal",
            "127.0.0.1.evil.example",
            "a.b",
        ] {
            assert!(!c.host_allowed(host), "{host}");
        }
        let c = Config::parse_from(["xsqld", "--allowed-host-suffixes", "db,.Sqld.Example"]);
        assert!(c.host_allowed("tenant.sqld.example:8080"));
        assert!(c.host_allowed("tenant.db"));
        assert!(!c.host_allowed("tenant.local"));
        assert!(!c.host_allowed("sqld.example"));
        assert!(Config::parse_from(["xsqld", "--allowed-host-suffixes", "*"]).host_allowed("tenant.evil.example"));
        assert!(!Config::parse_from(["xsqld", "--allowed-host-suffixes", ""]).host_allowed("tenant.db"));
    }

    #[test]
    fn file_layout() {
        let c = Config::for_tests(std::path::Path::new("/var/lib/db"));
        assert_eq!(c.db_file("tenant"), PathBuf::from("/var/lib/db/dbs/tenant/data"));
        assert!(c.per_namespace_dir());
        let mut flat = c.clone();
        flat.db_file_template = "{ns}.db".into();
        assert!(!flat.per_namespace_dir());
    }

    #[test]
    fn version_names_the_commit() {
        let e = Config::try_parse_from(["xsqld", "--version"]).unwrap_err();
        assert_eq!(e.kind(), clap::error::ErrorKind::DisplayVersion);
        assert_eq!(e.to_string(), format!("xsqld {VERSION} ({COMMIT})\n"));
        assert!(!COMMIT.is_empty());
    }

    #[test]
    fn read_pool_follows_the_cpus_but_keeps_room_for_slow_queries() {
        let cpus = std::thread::available_parallelism().unwrap().get();
        assert_eq!(Config::parse_from(["xsqld"]).read_pool_size, cpus.max(16));
        assert_eq!(default_read_pool_size(), cpus.max(16));
        assert_eq!(Config::parse_from(["xsqld", "--read-pool-size", "2"]).read_pool_size, 2);
    }

    #[test]
    fn db_path_defaults_to_sqlds_data_directory() {
        assert_eq!(Config::parse_from(["xsqld"]).db_path, PathBuf::from("data.sqld"));
    }

    /// Review of 0.1.0: only lower bounds were checked. A negative `--max-like-pattern-bytes` kept SQLite's 50000
    /// (set_limit ignores negative values), huge durations panicked when added to the clock
    #[test]
    fn every_numeric_flag_is_bounded_on_both_sides() {
        let ok = |args: &[&str]| {
            let mut v = vec!["xsqld"];
            v.extend_from_slice(args);
            Config::try_parse_from(v)
                .map_err(|e| e.to_string())?
                .validate()
                .map_err(|e| e.to_string())
        };
        // the runtime's command line, and the defaults
        ok(&[
            "--db-path",
            "/var/lib/db",
            "--http-listen-addr",
            "127.0.0.1:8080",
            "--admin-listen-addr",
            "127.0.0.1:8081",
            "--enable-namespaces",
            "--max-log-size",
            "32",
        ])
        .unwrap();
        let c = Config::parse_from(["xsqld"]);
        let bounds = c.numeric_bounds();
        let mut flags: Vec<&str> = bounds.iter().map(|b| b.0).collect();
        flags.sort_unstable();
        flags.dedup();
        assert_eq!(flags.len(), bounds.len(), "a flag is listed twice");
        for (flag, value, min, max) in bounds {
            assert!((min..=max).contains(&value), "default of --{flag}");
            for bad in [min - 1, max + 1] {
                let e = ok(&[&format!("--{flag}={bad}")]).unwrap_err();
                assert!(
                    e.contains(&format!("--{flag}")) || e.contains("invalid value"),
                    "--{flag}={bad}: {e}"
                );
            }
            for good in [min, max] {
                ok(&[&format!("--{flag}={good}")]).unwrap_or_else(|e| panic!("--{flag}={good}: {e}"));
            }
        }
        assert!(
            ok(&["--max-like-pattern-bytes=-1"])
                .unwrap_err()
                .contains("must be between 1 and")
        );
        assert!(ok(&["--request-timeout-ms=18446744073709551615"]).is_err());
        // every numeric flag of the command line is in the list
        let numeric = Config::command()
            .get_arguments()
            .filter(|a| a.get_action().takes_values() && !a.is_hide_set())
            .filter(|a| {
                let id = a.get_id().as_str();
                ![
                    "db_path",
                    "http_listen_addr",
                    "admin_listen_addr",
                    "db_file_template",
                    "allowed_host_suffixes",
                    "synchronous",
                ]
                .contains(&id)
            })
            .count();
        assert_eq!(numeric, bounds.len(), "a numeric flag has no bounds");
    }

    /// Review of 0.1.0: `{ns}` at the end of the file name made the database `a-wal` the WAL of `a`; a database
    /// named `xsqld-tmp` under `{ns}/data` was emptied at startup
    #[test]
    fn templates_that_mix_up_databases_are_refused() {
        let with = |t: &str| {
            let mut c = Config::for_tests(std::path::Path::new("/srv/db"));
            c.db_file_template = t.into();
            c
        };
        for t in ["dbs/{ns}/data", "{ns}.db", "{ns}/data", "db-{ns}.sqlite", "{ns}-wal.db"] {
            with(t).validate().unwrap_or_else(|e| panic!("{t}: {e}"));
        }
        for t in [
            "{ns}",
            "dbs/x-{ns}",
            "{ns}-wal",
            "{ns}-journal",
            "/abs/{ns}/data",
            "../{ns}.db",
            "dbs/./{ns}.db",
            "dbs/data",
        ] {
            assert!(with(t).validate().is_err(), "{t}");
        }
        let c = with("{ns}/data");
        assert!(!c.usable_namespace("xsqld-tmp"));
        assert!(c.usable_namespace("tenant"));
        let sqld = with("dbs/{ns}/data");
        assert!(
            sqld.usable_namespace("xsqld-tmp"),
            "sqld's layout keeps it apart from the scratch directory"
        );
        assert!(!sqld.usable_namespace("../x"));
    }

    #[test]
    fn refuses_public_listen() {
        let mut c = Config::for_tests(std::path::Path::new("/tmp"));
        c.http_listen_addr = "0.0.0.0:8080".parse().unwrap();
        assert!(c.validate().is_err());
        c.allow_non_loopback = true;
        assert!(c.validate().is_ok());
    }
}
