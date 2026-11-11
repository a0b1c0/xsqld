//! - Guards the SQLite compile flags that `.cargo/config.toml` sets for the bundled SQLite. Cargo reads that file only
//!   when it is started in this directory (or below); built from elsewhere (`--manifest-path`) the flags would be
//!   dropped without a word and every commit on Linux would pay an fsync() instead of an fdatasync().
//! - Embeds the git commit as `XSQLD_GIT_COMMIT`: taken from that environment variable when set (builds from a source
//!   archive), else from `git rev-parse --short HEAD`, else `unknown`.

use std::process::Command;

fn main() {
    println!("cargo:rerun-if-env-changed=LIBSQLITE3_FLAGS");
    let flags = std::env::var("LIBSQLITE3_FLAGS").unwrap_or_default();
    let linux = std::env::var("CARGO_CFG_TARGET_OS").is_ok_and(|os| os == "linux");
    if linux && !flags.split_whitespace().any(|f| f.starts_with("-DHAVE_FDATASYNC")) {
        panic!(
            "LIBSQLITE3_FLAGS lacks -DHAVE_FDATASYNC=1: run cargo inside the xsqld directory so that .cargo/config.toml \
             applies, or set the variable yourself"
        );
    }
    println!("cargo:rustc-env=XSQLD_SQLITE_FLAGS={flags}");

    println!("cargo:rerun-if-env-changed=XSQLD_GIT_COMMIT");
    let commit = match std::env::var("XSQLD_GIT_COMMIT") {
        Ok(c) if !c.trim().is_empty() => c.trim().to_owned(),
        _ => git_commit().unwrap_or_else(|| "unknown".into()),
    };
    // it ends up in a Prometheus label and in JSON
    if !commit
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || b == b'.' || b == b'-' || b == b'_')
    {
        panic!("XSQLD_GIT_COMMIT {commit:?}: only letters, digits, '.', '-' and '_'");
    }
    println!("cargo:rustc-env=XSQLD_GIT_COMMIT={commit}");
}

fn git(args: &[&str]) -> Option<String> {
    let out = Command::new("git").args(args).output().ok()?;
    let s = String::from_utf8(out.stdout).ok()?;
    (out.status.success() && !s.trim().is_empty()).then(|| s.trim().to_owned())
}

/// Short commit of HEAD; asks to be run again when HEAD moves (new commit, checkout)
fn git_commit() -> Option<String> {
    let commit = git(&["rev-parse", "--short", "HEAD"])?;
    let mut watch = vec![git(&["rev-parse", "--git-path", "HEAD"])?];
    if let Some(branch) = git(&["symbolic-ref", "-q", "HEAD"]) {
        watch.extend(git(&["rev-parse", "--git-path", &branch]));
    }
    watch.extend(git(&["rev-parse", "--git-path", "packed-refs"]));
    // a path that does not exist would make cargo run this script on every build
    for p in watch.iter().filter(|p| std::path::Path::new(p).exists()) {
        println!("cargo:rerun-if-changed={p}");
    }
    Some(commit)
}
