# Changelog

All notable changes to xsqld are documented in this file. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and xsqld follows
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

`version` in `Cargo.toml` is the single source of the version number. Releases are tagged `xsqld-v<version>`.
`xsqld --version` prints `xsqld <version> (<short commit>)`; `GET /health` returns both as JSON and `GET /metrics`
exposes them as `xsqld_build_info{version="…",commit="…"} 1`. Builds outside a git checkout can set
`XSQLD_GIT_COMMIT`; without it and without git the commit reads `unknown`.

## [0.1.0] - Unreleased

First release: a local-only SQLite server that replaces libsql-server (sqld) 0.24.32 for clients using its Hrana over
HTTP subset, opening sqld's database files in place (`<db-path>/dbs/<namespace>/data`).

### Added

- Hrana over HTTP v2 (`POST /v2/pipeline`): `execute`, `batch` with conditional steps, `close`, and streams kept
  across requests with batons. Results, error codes and error messages match sqld 0.24.32 where clients rely on them.
- `GET /dump`: SQL export byte-identical to sqld's, taken from an online-backup snapshot so that a slow client never
  holds back checkpoints. Non-UTF-8 text is exported as `CAST(X'…' AS TEXT)` and round-trips exactly.
- Admin API: `POST /v1/namespaces/<ns>/create` (empty, or from a dump at `dump_url`, loopback only by default; only
  statements a dump consists of are run) and `DELETE /v1/namespaces/<ns>`.
- One write thread per database with group commit: concurrent write requests share one transaction and one fsync,
  each isolated in its own savepoint, so clients never see `SQLITE_BUSY`. Slow groups are committed early
  (`--group-max-exec-ms`); a transaction left open across requests is rolled back after `--pinned-tx-timeout-ms`.
- Read-only connection pool per database (`--read-pool-size`, default: number of CPUs, at least 16).
- At most `--max-concurrent-dumps` (2) dumps at once, each holding its slot until its export ends.
- Checkpoints run by the server (`wal_autocheckpoint=0`): passive checkpoints on a background thread, RESTART once the
  WAL exceeds `--checkpoint-restart-bytes`, TRUNCATE on shutdown. Writes wait for the background thread once it is
  `--checkpoint-pages` frames behind, so no checkpoint stops writes for long on disks with slow random writes.
- Limits on connections, concurrent requests, write queue length, request size, header and body read time, request
  time, dump duration and `dump_url` download time.
- State-changing PRAGMAs, temporary objects, `VACUUM` and `load_extension()` are refused on the shared connections;
  `defer_foreign_keys` set outside a transaction is reset when its request ends.
- `GET /health` and Prometheus metrics on `GET /metrics`, including `xsqld_build_info`.
- mimalloc as the allocator for Rust and SQLite; any panic aborts the process so that the service manager restarts it.
- Listens on loopback addresses only unless `--allow-non-loopback` is given; accepts sqld's command line flags, with
  sqld's defaults for `--db-path` (`data.sqld`) and `--http-listen-addr` (`127.0.0.1:8080`). Unlike sqld, the admin
  API is on by default (`127.0.0.1:8081`).
