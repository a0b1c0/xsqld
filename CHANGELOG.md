# Changelog

All notable changes to xsqld are documented in this file. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and xsqld follows
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

`version` in `Cargo.toml` is the single source of the version number. Releases are tagged `xsqld-v<version>`.
`xsqld --version` prints `xsqld <version> (<short commit>)`; `GET /health` returns both as JSON and `GET /metrics`
exposes them as `xsqld_build_info{version="…",commit="…"} 1`. Builds outside a git checkout can set
`XSQLD_GIT_COMMIT`; without it and without git the commit reads `unknown`.

## [0.1.3] - 2026-11-12

Split-range parallel aggregation (README §3.1): an aggregate over a large table uses several CPU cores. Always on,
nothing to configure: sqld's command line and every 0.1.2 default keep working, and results are byte for byte the
ones a single connection returns (statements for which that cannot be guaranteed run as before).

### Added

- `COUNT(*)`, `COUNT(x)`, integer `SUM`, `MIN` and `MAX` over one ordinary rowid table, with or without `WHERE`,
  `GROUP BY`, `HAVING`, `ORDER BY` and `LIMIT`, run on up to four read connections at once, each on a range of rowids,
  all on one snapshot; SQLite combines the partial rows in an in-memory table. On a 100-million-row table (Apple M4
  Max, through HTTP): a filtered `COUNT` 3.8 s → 1.3 s, a `GROUP BY` of 60 groups 22.9 s → 6.8 s; statements that are
  not split are as fast as before (docs/parallel-aggregate.md).
- Whether a table is large enough is decided by its rows, estimated from the rowid b-tree (16 parts looked at from
  both ends, parts whose ends disagree halved; at most once every 10 s per table), not by the span of its rowids: a
  table of 3000 rows with millisecond timestamps as ids used to be split on every request (p50 0.23 ms over HTTP,
  0.14 ms without splitting).
- Tuning flags with defaults that need no change: `--parallel-max-ranges` (4), `--parallel-min-rows` (1000000),
  `--parallel-pool-percent` (50) and `--parallel-max-partial-rows` (1000000); README §2.1 lists their ranges. There is
  no flag that turns splitting off.
- Metrics `parallel_considered_total`, `parallel_runs_total`, `parallel_ranges_total`, `parallel_fallback_total` (by
  `reason`) and `parallel_merge_*`.

### Changed

- The bundled SQLite is compiled with `-DSQLITE_ENABLE_SNAPSHOT` (`.cargo/config.toml`); `build.rs` refuses to build
  without it on any target.

### Tests

- The shape parser takes the statements it should and refuses some 50 others (DISTINCT, AVG, window functions, CTEs,
  joins, subqueries, bare columns, float sums, …); its tokenizer follows SQLite's.
- Split results equal one connection's for 19 statements over four random data sets (NULLs, mixed case, holes and
  negative rowids, text that is not UTF-8, `0.0` and `-0.0`); 20 kinds of statements run on one connection for the
  reason expected, with the same result (REAL sums, sums that may overflow, NOCASE and untyped grouping columns,
  views, virtual and `WITHOUT ROWID` tables, index plans, non-deterministic functions, too many groups, a busy pool).
- Under writes that keep `COUNT(*)` and `SUM` constant, 150 of 150 split results show the constants (with each
  connection on its own read transaction instead: 139 of 150 do not); the untouched-WAL rule, and a write landing in
  it; a failing range stops the others at once, also one that had not started yet; the deadline; the read gate counts
  a split statement as one read and a closed gate does not hold up its ranges.
- Review of 0.1.3 (fixed before release): `ORDER BY rowid` (or `oid`, `_rowid_`) and double-quoted names of columns
  outside `GROUP BY` in `HAVING` / `ORDER BY` were evaluated against the in-memory table and gave another order or
  other groups; the in-memory database lacked the connections' limits (a `LIKE` pattern of 2000 bytes in `HAVING`
  ran instead of failing, and one of some 40 KB could have overflowed the stack); a table rebuilt between the checks
  and the snapshot was split as the old one. Each has a test.
- Each of 28 protections was removed in turn: its test failed every time.
- The row estimate is exact for dense rowids and for a few rows far from the rest, within 0.5% for evenly sparse ids
  and clusters.
- `tests/compare.py` compares 14 aggregates with sqld, 10 of them split on xsqld (with `--parallel-min-rows 1000`).

## [0.1.2] - 2026-11-12

Fixes for the issues of the 0.1.0 code review that 0.1.1 left open (README §4.1 lists the few that remain). sqld's
command line and every 0.1.1 default keep working; the new flags have defaults that need no change.

### Added

- `--checkpoint-reader-wait-ms` (200), `--response-write-timeout-ms` (60000), `--dump-load-max-statement-bytes`
  (128 MiB) and `--dump-load-timeout-ms` (1800000); README §2.1 lists every flag with its default and range.
- Metrics `checkpoint_gate_*` (RESTARTs run with new reads held back) and `read_gate_wait_*` (reads held back).

### Fixed

- Checkpoint starvation: read requests following each other closely while writes went on always left a reader on
  the WAL, so RESTART never found a gap and the WAL grew without bound (619 MB in 8 s in the review). After a failed
  RESTART the next one holds back new reads, waits up to `--checkpoint-reader-wait-ms` for the running ones and
  restarts the WAL; reads are held back at most that long, never past their deadline, and nothing they do changes.
- After a failed RESTART the next one waited for `--checkpoint-pages` more frames even when the WAL had started over
  by itself.
- `--checkpoint-idle-ms 0` made every write thread spin; it now turns the idle checkpoint off, and an idle write
  thread with nothing to checkpoint sleeps until the next request.
- A client that sent requests and never read the responses kept its connection for good; `--max-connections` such
  clients locked everyone out. A response the client takes no data of for `--response-write-timeout-ms` now closes the
  connection.
- `Config::validate` checked lower bounds only: every numeric flag now has a range, so huge durations no longer panic
  and negative SQLite limits (a negative `--max-like-pattern-bytes` kept SQLite's 50000) are refused at startup.
  `--max-like-pattern-bytes` is at most 10000, the most whose matcher recursion fits on a 2 MiB stack.
- A database created where a crash during a delete had left the old `-wal` took over the old content. Creating a
  database now removes `-wal`, `-shm` and `-journal` files under its name first, and startup removes them for every
  database whose main file is gone.
- A first open cancelled with its request, followed by a `DELETE`, could create the deleted database again: the open
  now runs and registers the database under the database's lock.
- `dump_url`: `http://[::1]/x` without a port was refused; the user name and password of a refused URL, and control
  characters in it, reached the log; the URL was fetched before the database name was checked, using up a one-time
  token for a create that could not succeed.
- Files of a build from a dump that crashed (`*.xsqld-loading*`) stayed until that name was created again; they are
  removed at startup. `.sentinel` and its directory entry are synced when created.
- Loading a dump had no limits: statements are limited to `--dump-load-max-statement-bytes` (a longer line is refused
  before it is read in whole) and the load to `--dump-load-timeout-ms`, after which SQL still running is interrupted;
  either way the create fails with 400 and leaves nothing. A trigger of many lines is no longer read again at every
  line ending in `;`.
- `--db-file-template` values that let databases share files (a file name ending in `{ns}`: the database `a-wal`
  was the WAL of `a`) or leave `--db-path` are refused at startup, and the name `xsqld-tmp` is refused where it would
  be the scratch directory (templates such as `{ns}/data`).
- A trigger whose text starts with Unicode whitespace or a byte order mark was counted as several statements.
- Dumps that did not load back: a view or index definition ending in a `--` comment (sqld loads these); text with a
  NUL in it (now written as `CAST(X'…' AS TEXT)`); `sqlite_sequence` left over after its last AUTOINCREMENT table was
  dropped. The last two fail on sqld too.
- `ROLLBACK TRANSACTION <name> TO <savepoint>` was taken for a whole rollback.
- Deferred foreign keys were checked per request; they are now checked where sqld checks them, at the commit of each
  statement outside a transaction and at the client's `COMMIT`.
- A write whose `RETURNING` rows exceeded `--max-response-bytes` was reported as failed but kept (sqld does the
  same); it is now undone. `RESPONSE_TOO_LARGE` inside a batch fails the batch and stops it, as on sqld.
- As on sqld 0.24.32: `;BEGIN` is `BEGIN`; a named parameter given twice counts once, with its last value; `close`
  may follow `close`; base64 may end in any number of `=`; `ANALYZE` is refused; `PRAGMA threads`, `stats`,
  `soft_heap_limit`, `hard_heap_limit` and `analysis_limit` can be read; a table expression named `replace` no longer
  makes a `SELECT` count as an insert.

### Tests

- `tests/compare.py` counts a failed load on xsqld as a difference, compares `SQL_PARSE_ERROR` texts except for
  syntax errors, has cases for PRAGMAs, temporary objects, savepoints, deferred foreign keys, the details above,
  `RESPONSE_TOO_LARGE` and `preserve_row_ids`, and sends 48 requests at the same time to compare grouped execution.
- The wall-clock limit of `statement_count_reads_the_text_once` is replaced by a count of the bytes read.

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
