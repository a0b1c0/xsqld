//! Memory: mimalloc for Rust and for SQLite. musl's own malloc kept growing under the soak load (16 MB to 791 MB in
//! five minutes, the glibc build stayed under 32 MB with the same load; README §3), and SQLite calls the C library's
//! malloc unless told otherwise.

use std::ffi::{c_int, c_void};

use libmimalloc_sys as mi;
use rusqlite::ffi;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

unsafe extern "C" fn x_malloc(n: c_int) -> *mut c_void {
    unsafe { mi::mi_malloc(n as usize) }
}

unsafe extern "C" fn x_free(p: *mut c_void) {
    unsafe { mi::mi_free(p) }
}

unsafe extern "C" fn x_realloc(p: *mut c_void, n: c_int) -> *mut c_void {
    unsafe { mi::mi_realloc(p, n as usize) }
}

unsafe extern "C" fn x_size(p: *mut c_void) -> c_int {
    c_int::try_from(unsafe { mi::mi_usable_size(p) }).unwrap_or(c_int::MAX)
}

/// SQLite never asks for more than it can count in a `c_int`: a size that would not fit stays unrounded
unsafe extern "C" fn x_roundup(n: c_int) -> c_int {
    usize::try_from(n)
        .ok()
        .and_then(|u| c_int::try_from(unsafe { mi::mi_good_size(u) }).ok())
        .unwrap_or(n)
}

unsafe extern "C" fn x_init(_: *mut c_void) -> c_int {
    ffi::SQLITE_OK
}

unsafe extern "C" fn x_shutdown(_: *mut c_void) {}

/// Hands SQLite's allocations to mimalloc. Must run before SQLite is initialized, i.e. before the first connection.
pub fn sqlite_on_mimalloc() -> anyhow::Result<()> {
    let methods = ffi::sqlite3_mem_methods {
        xMalloc: Some(x_malloc),
        xFree: Some(x_free),
        xRealloc: Some(x_realloc),
        xSize: Some(x_size),
        xRoundup: Some(x_roundup),
        xInit: Some(x_init),
        xShutdown: Some(x_shutdown),
        pAppData: std::ptr::null_mut(),
    };
    // SAFETY: SQLite copies the methods; before sqlite3_initialize it takes them, afterwards it returns SQLITE_MISUSE
    // and keeps its allocator
    let rc = unsafe { ffi::sqlite3_config(ffi::SQLITE_CONFIG_MALLOC, &methods as *const ffi::sqlite3_mem_methods) };
    anyhow::ensure!(
        rc == ffi::SQLITE_OK,
        "sqlite3_config(SQLITE_CONFIG_MALLOC) failed: {rc}"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Whether a block that SQLite allocates now belongs to mimalloc
    fn sqlite_block_is_mimallocs() -> bool {
        let c = rusqlite::Connection::open_in_memory().unwrap();
        c.execute_batch("CREATE TABLE t(v); INSERT INTO t VALUES (randomblob(1000000));")
            .unwrap();
        unsafe {
            let p = ffi::sqlite3_malloc(100);
            assert!(!p.is_null());
            let owned = mi::mi_check_owned(p);
            ffi::sqlite3_free(p);
            owned
        }
    }

    #[test]
    fn sizes_stay_within_c_int() {
        unsafe {
            assert_eq!(x_roundup(-1), -1);
            assert_eq!(x_roundup(c_int::MAX), c_int::MAX);
            let n = x_roundup(100);
            assert!((100..=128).contains(&n), "{n}");
            let p = x_malloc(n);
            assert!(x_size(p) >= n);
            x_free(p);
        }
    }

    /// Runs itself again with XSQLD_ALLOC_CHILD set: the child hands SQLite to mimalloc before its first connection
    #[test]
    fn sqlite_allocates_from_mimalloc() {
        if std::env::var_os("XSQLD_ALLOC_CHILD").is_some() {
            sqlite_on_mimalloc().unwrap();
            assert!(sqlite_block_is_mimallocs(), "SQLite still uses the C library's malloc");
            assert!(sqlite_on_mimalloc().is_err(), "too late once SQLite is initialized");
            return;
        }
        // this process never switched: proves the check tells the two apart
        assert!(!sqlite_block_is_mimallocs());
        let out = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "alloc::tests::sqlite_allocates_from_mimalloc", "--nocapture"])
            .env("XSQLD_ALLOC_CHILD", "1")
            .output()
            .unwrap();
        assert!(
            out.status.success(),
            "{}{}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        );
    }
}
