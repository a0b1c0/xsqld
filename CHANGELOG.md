# Changelog

All notable changes to xsqld are documented in this file. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and xsqld follows
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

`version` in `Cargo.toml` is the single source of the version number. Releases are tagged `xsqld-v<version>`.
`xsqld --version` prints `xsqld <version> (<short commit>)`; `GET /health` returns both as JSON and `GET /metrics`
exposes them as `xsqld_build_info{version="…",commit="…"} 1`. Builds outside a git checkout can set
`XSQLD_GIT_COMMIT`; without it and without git the commit reads `unknown`.

## [0.1.1] - 2026-11-11

Fixes for the findings of an external test and a code review of 0.1.0. Upgrading needs no change for clients that
reach xsqld as `<name>.db`, `<name>.local`, `<name>.localhost`, by IP address or by a host name without a dot; other
deployments add their domain to `--allowed-host-suffixes`.

### Security

- Requests are answered only when their `Host` header is an IP address, a name without a dot or a name under one of
  `--allowed-host-suffixes` (default `db,local,localhost`); others get 403 on both ports. After DNS rebinding a web
  page reached every endpoint as its own origin, `GET /dump` of a whole database included, since the database is
  chosen by the Host header and nothing else identifies a client.
- Requests carrying `Origin` or `Sec-Fetch-Site`, which browsers add to cross-site requests, are refused with 403. A
  web page could otherwise write to a database through a cross-site `text/plain` POST, which needs no preflight.
- Objects can no longer be created in the `temp` schema by naming them `temp.<name>` (`CREATE TRIGGER temp.t …`,
  `CREATE VIEW temp.v …`, `CREATE TABLE temp.x …`); only the `CREATE TEMP …` spelling was refused. On the shared write
  connection such a trigger ran for every other client's writes, and a view stood in front of the real table.
- LIKE and GLOB patterns are limited to `--max-like-pattern-bytes` (1000; SQLite's default is 50000). One request
  with a pattern of some 40 KB overflowed the stack of the thread running it and took the process down; long patterns
  also ran for minutes past the request's time limit while holding the write thread.

### Fixed

- A `COMMIT` or `END` behind a byte order mark, a vertical tab or Unicode whitespace was not recognized as a
  transaction keyword and committed the group transaction for real; the group was then redone, and the writes of the
  requests before it, already acknowledged or about to be, were applied twice. Transaction keywords are now recognized
  behind everything SQLite skips, and a commit hook turns any commit inside a group transaction that is not the write
  thread's own into a rollback.
- Creating a database from `dump_url`: when the create request was abandoned while the dump was downloading, a
  database holding whatever had arrived, often nothing, was left in place under that name. A download now has to
  reach its proper end to be loaded.
- Checking that a request holds a single statement took time quadratic in its length for `CREATE TRIGGER` bodies:
  4 MB of semicolons kept a worker thread busy for seconds, 16 MB for minutes. The text is now read once.
- Statements longer than 8 KB are no longer kept in the statement cache. 256 different statements of 4 MB each grew
  the server by several GiB, which was not given back.
- While a transaction left open across requests holds the write thread, queued requests are answered when their own
  queue timeout or deadline runs out (503 / 504, not run), instead of only when that transaction ends.
- The per-database lock used to open, create and delete databases is forgotten when nobody holds or waits for it.
  Before, every name ever used, including deletes of databases that do not exist, kept an entry until restart.

## [0.1.0] - 2026-11-11

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
