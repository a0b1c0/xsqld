//! xsqld: a local-only SQLite server speaking a subset of sqld's Hrana over HTTP.
//! One write thread per database with group commit, a read-only connection pool, checkpoints done by the server.

mod alloc;
mod config;
mod conn;
mod db;
mod dump;
mod exec;
mod http;
mod metrics;
mod parallel;
mod proto;
mod reader;
mod shape;
mod sql;
mod stream;
mod writer;

use std::sync::Arc;
use std::time::Duration;

use clap::Parser;

/// A panic anywhere ends the process. Without this a panicking write thread leaves a server that still answers reads
/// and `/health` but refuses every write of that database until someone restarts it; the service manager restarts
/// a process that exits (`Restart=always` under systemd).
fn abort_on_panic() {
    let default = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        let thread = std::thread::current();
        tracing::error!(thread = thread.name().unwrap_or("unnamed"), %info, "panic, aborting");
        default(info);
        std::process::abort();
    }));
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    abort_on_panic();
    alloc::sqlite_on_mimalloc()?;
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .with_writer(std::io::stderr)
        .with_ansi(false)
        .init();
    let cfg = config::Config::parse();
    cfg.validate()?;
    std::fs::create_dir_all(&cfg.db_path)?;
    // dump copies left by a crash
    match std::fs::remove_dir_all(cfg.tmp_dir()) {
        Err(e) if e.kind() != std::io::ErrorKind::NotFound => return Err(e.into()),
        _ => {}
    }
    // files of a build from a dump that did not finish, WAL files of databases deleted halfway
    for file in db::clean_leftovers(&cfg)? {
        tracing::warn!(file = %file.display(), "removed a file left by a crash");
    }
    let cfg = Arc::new(cfg);
    let registry = db::Registry::new(cfg.clone());

    let user = tokio::net::TcpListener::bind(cfg.http_listen_addr).await?;
    let admin = tokio::net::TcpListener::bind(cfg.admin_listen_addr).await?;
    tracing::info!(
        version = config::VERSION,
        commit = config::COMMIT,
        write_threads_per_db = 1,
        read_connections_per_db = cfg.read_pool_size,
        sqlite = rusqlite::version(),
        sqlite_flags = env!("XSQLD_SQLITE_FLAGS"),
        db_path = %cfg.db_path.display(),
        http = %cfg.http_listen_addr,
        admin = %cfg.admin_listen_addr,
        "xsqld started"
    );

    let streams = registry.streams.clone();
    let pinned_limit = cfg.pinned_tx_timeout();
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(1));
        loop {
            tick.tick().await;
            streams.sweep(pinned_limit);
        }
    });

    let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);
    let user_srv = http::serve(user, http::user_router(registry.clone()), cfg.clone(), stop_rx.clone());
    let admin_srv = http::serve(admin, http::admin_router(registry.clone()), cfg.clone(), stop_rx);
    let servers = tokio::spawn(async move { tokio::join!(user_srv, admin_srv) });

    shutdown_signal().await;
    tracing::info!("stopping: finishing requests in flight");
    let _ = stop_tx.send(true);
    if tokio::time::timeout(cfg.request_timeout() + Duration::from_secs(5), servers)
        .await
        .is_err()
    {
        tracing::warn!("requests still running at shutdown");
    }
    let r = registry.clone();
    tokio::task::spawn_blocking(move || r.shutdown()).await?;
    tracing::info!("stopped");
    Ok(())
}

async fn shutdown_signal() {
    let ctrl_c = tokio::signal::ctrl_c();
    #[cfg(unix)]
    {
        let mut term =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).expect("SIGTERM handler");
        tokio::select! {
            _ = ctrl_c => {}
            _ = term.recv() => {}
        }
    }
    #[cfg(not(unix))]
    let _ = ctrl_c.await;
}

#[cfg(test)]
mod tests {
    /// Runs itself again with XSQLD_PANIC_CHILD set: the child panics on a named thread after installing the hook
    #[test]
    fn a_panic_on_any_thread_aborts_the_process() {
        if std::env::var_os("XSQLD_PANIC_CHILD").is_some() {
            super::abort_on_panic();
            let _ = std::thread::Builder::new()
                .name("xsqld-writer".into())
                .spawn(|| panic!("boom"))
                .unwrap()
                .join();
            println!("survived the panic");
            return;
        }
        let out = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "tests::a_panic_on_any_thread_aborts_the_process",
                "--nocapture",
            ])
            .env("XSQLD_PANIC_CHILD", "1")
            .output()
            .unwrap();
        let stdout = String::from_utf8_lossy(&out.stdout);
        assert!(!out.status.success(), "{stdout}");
        assert!(!stdout.contains("survived the panic"), "{stdout}");
        #[cfg(unix)]
        {
            use std::os::unix::process::ExitStatusExt;
            assert_eq!(out.status.signal(), Some(6), "SIGABRT expected: {:?}", out.status);
        }
    }
}
