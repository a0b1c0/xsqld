use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use clap::{Parser, ValueEnum};

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
    /// Checkpoint by the write thread after it has been idle this long. The WAL file keeps its size; it is only
    /// truncated on shutdown
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

    pub fn validate(&self) -> anyhow::Result<()> {
        if !self.db_file_template.contains("{ns}") {
            anyhow::bail!("--db-file-template must contain {{ns}}");
        }
        for addr in [self.http_listen_addr, self.admin_listen_addr] {
            if !addr.ip().is_loopback() && !self.allow_non_loopback {
                anyhow::bail!("refusing to listen on non-loopback address {addr} (pass --allow-non-loopback)");
            }
        }
        if [
            self.read_pool_size,
            self.group_max,
            self.max_write_queue,
            self.max_connections,
            self.max_concurrent_requests,
            self.max_concurrent_dumps,
        ]
        .contains(&0)
        {
            anyhow::bail!(
                "--read-pool-size, --group-max, --max-write-queue, --max-connections, --max-concurrent-requests and \
                 --max-concurrent-dumps must be at least 1"
            );
        }
        Ok(())
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

    #[test]
    fn refuses_public_listen() {
        let mut c = Config::for_tests(std::path::Path::new("/tmp"));
        c.http_listen_addr = "0.0.0.0:8080".parse().unwrap();
        assert!(c.validate().is_err());
        c.allow_non_loopback = true;
        assert!(c.validate().is_ok());
    }
}
