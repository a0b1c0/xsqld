# xsqld

A local-only SQLite database server, a drop-in replacement for libsql-server (sqld) 0.24.32. It speaks the subset of sqld's Hrana over HTTP (`POST /v2/pipeline`), `GET /dump` and the admin API for creating and deleting databases; underneath is stock SQLite (rusqlite `bundled`).

It was written for an application runtime that reaches its databases on the same machine through sqld's HTTP API, via an adapter that presents them as Cloudflare D1. "The runtime" and "the adapter" below mean that client; what it relies on decides which sqld behaviours xsqld reproduces exactly.

Version history: [CHANGELOG.md](CHANGELOG.md). License: MIT or Apache-2.0 ([LICENSE-MIT](LICENSE-MIT), [LICENSE-APACHE](LICENSE-APACHE)).

## 1. Compatibility checks against sqld

Checked against the source of libsql-server **0.24.32** (tag `libsql-server-v0.24.32`, commit `40c272de`), plus the official image `ghcr.io/tursodatabase/libsql-server:v0.24.32` run locally with `--enable-namespaces --max-log-size 32`.

### 1.1 Which PRAGMAs sqld sets on each connection

Only two places in the source set connection parameters:

| Location | What it sets |
|---|---|
| `libsql-sys/src/connection.rs` `Connection::open` | `journal_mode=WAL`; `sqlite3_wal_autocheckpoint(1000)` (`DEFAULT_AUTO_CHECKPOINT`); `busy_timeout(100 ms)` |
| `libsql-server/src/connection/connection_core.rs` `CoreConnection::new` | `max_page_count` (default 1000 PB ÷ 4096, i.e. unlimited); `synchronous` = the database's `durability_mode`, default `relaxed` → `NORMAL`; `SQLITE_LIMIT_LENGTH` = `max_row_size`, default **5 MB**; a progress handler every 100 VM instructions (only used to cancel requests) |

`foreign_keys` and `recursive_triggers` are not set in code and take SQLite's compile-time defaults. Values read back with `PRAGMA` at run time (`tenant` database, via `/v2/pipeline`):

| PRAGMA | sqld 0.24.32 | xsqld |
|---|---|---|
| `foreign_keys` | **1** (compile option `SQLITE_DEFAULT_FOREIGN_KEYS`) | 1 (set explicitly on every connection) |
| `recursive_triggers` | 0 | 0 (set explicitly) |
| `defer_foreign_keys` | 0 | 0 |
| `trusted_schema` | 1 | 1 |
| `journal_mode` | wal | wal |
| `synchronous` | 1 (NORMAL) | **2 (FULL)**, configurable (`--synchronous`): an acknowledged write survives a power cut; group commit pays for it |
| `wal_autocheckpoint` | 1000 | **0**: the server runs checkpoints itself (§4.3) |
| `journal_size_limit` | -1 | **64 MB** (configurable) |
| `busy_timeout` | 100 | 100 on the write connection, 1000 on read connections: the server has a single writer and does not rely on it for retries |
| `cache_size` | -2000 (2 MB) | -2000, configurable (`--cache-size`) |
| `page_size` / `auto_vacuum` / `encoding` | 4096 / 0 / UTF-8 | same (database files are opened as they are, unchanged) |
| `temp_store` / `mmap_size` / `locking_mode` | 0 / 0 / normal | same |
| `SQLITE_LIMIT_LENGTH` | 5 MB | 5 MB, configurable (`--max-value-bytes`) |
| `SQLITE_LIMIT_ATTACHED` | 10 (but ATTACH is not allowed by default) | 0: no ATTACH |
| SQLite version | 3.45.1 | 3.54.1 (rusqlite 0.41 `bundled`) |

Conclusion: the two settings that change foreign key and trigger behaviour (`foreign_keys=1`, `recursive_triggers=0`) are the same in xsqld and sqld; the only intentional differences are `synchronous`, `wal_autocheckpoint` and `journal_size_limit`.

### 1.2 Where database files live with namespaces enabled

Source: every `namespace/configurator/*.rs` uses `base_path.join("dbs").join(<name>)`, and the database file is `data` inside it (`open_conn_active_checkpoint` in `connection/legacy.rs`: `path.join("data")`). Directory after creating the `platform` and `tenant` databases with `--db-path /db/data`:

```
<db-path>/.version                     sqld data directory version
<db-path>/metastore/data(-wal,-shm)    sqld's own metadata database (per-database configuration)
<db-path>/dbs/<name>/data              ← the database file (plain SQLite, WAL mode)
<db-path>/dbs/<name>/data-wal
<db-path>/dbs/<name>/data-shm
<db-path>/dbs/<name>/wallog            sqld's replication log (unused by xsqld)
<db-path>/dbs/<name>/.sentinel         sqld "running" marker: present at startup → the log is considered dirty and is recovered from the database file
<db-path>/dbs/<name>/stats.json, tmp/, to_compact/   used by sqld only
```

With `--db-path /srv/db`, for example, the database `tenant` is the file `/srv/db/dbs/tenant/data`.

By default xsqld finds databases by the same rule (`--db-file-template 'dbs/{ns}/data'`, relative to `--db-path`) and opens these files directly: switching moves no data. Files that only sqld uses, such as `metastore/` and `wallog`, are neither read nor changed.

**What rolling back to sqld relies on**: if a database directory contains `.sentinel` at startup, or has no `wallog`, sqld first checkpoints and then rebuilds the replication log from the database file (`ReplicationLogger::open` → `recover` in `replication/primary/logger.rs`); it never lays an old `wallog` over data that xsqld wrote. So when xsqld opens a database without `.sentinel` it creates one and never removes it, and on a clean shutdown it runs a `TRUNCATE` checkpoint so that the database file is self-contained. Rollback drill: §5.

## 2. Interface (shared with sqld)

Two ports, by default 127.0.0.1:8080 (clients) and 127.0.0.1:8081 (admin). Only loopback addresses are accepted; any other address needs `--allow-non-loopback`. The database name is the first label of the `Host` header (`<name>.db`, as the runtime sends it).

| Port | Endpoint | Description |
|---|---|---|
| 8080 | `POST /v2/pipeline` | Hrana over HTTP v2 `execute`, `batch` and `close`; streams continue with batons |
| 8080 | `GET /dump` | Whole-database export as SQL text, byte-identical to sqld 0.24.32 (except non-UTF-8 text, see §4) |
| both | `GET /health`, `GET /metrics` | Health check, returning `{"version", "commit"}` (sqld returns an empty body; clients only look at the status); metrics in Prometheus text format, including `xsqld_build_info{version,commit}` |
| 8081 | `POST /v1/namespaces/<name>/create` | Create a database; the body may carry `dump_url` to create it from a dump (loopback addresses only by default) |
| 8081 | `DELETE /v1/namespaces/<name>` | Delete a database |

Not implemented: Hrana v3 and WebSocket, `sequence` / `describe` / `store_sql` (unused by the runtime; such requests and statements with `sql_id` get 400), replication, authentication (local clients only). `--enable-namespaces` and `--max-log-size` are accepted and ignored, so sqld's command line can be reused as it is. Other flags: `xsqld --help`. `xsqld --version` prints `xsqld <version> (<short commit>)`.

## 3. Internals

- One write thread per database, the only writer, so clients never get "busy". Write requests queue up; the write thread takes up to `--group-max` of them at a time and runs them one after another in a single transaction (one SAVEPOINT per request; a client's `BEGIN` / `COMMIT` / `ROLLBACK` become creating, releasing and rolling back that inner SAVEPOINT). The whole group commits once with one fsync, and replies go out only after the commit (group commit). A failing request rolls back only itself; when SQLite rolls back the whole transaction (`ON CONFLICT ROLLBACK`, interrupt, I/O error), the group is redone without the request that caused it.
- When a group has run for `--group-max-exec-ms` (20 ms) and still has requests left, the finished ones are committed and answered first and the rest go back to the head of the queue: one slow write does not hold up the replies of every request before it in the group.
- Run on their own, outside groups: PRAGMA, SAVEPOINT, VACUUM and the like, and transactions left open across HTTP requests. The latter pin the write thread to that stream; past `--pinned-tx-timeout-ms`, or when the client goes away, the transaction is rolled back, so the write lock is never left without an owner.
- A request that ends with unresolved deferred foreign keys (`DEFERRABLE INITIALLY DEFERRED`) is withdrawn and run again on its own: run alone, SQLite checks it at its own commit, as sqld does; left in the group, a later request in the same group could supply the missing parent row and the outer `COMMIT` would pass. The runtime's schema has no `DEFERRABLE`; this is only a safety net.
- At most `--max-write-queue` (4096) write requests per database; when full, requests get 503 without running. Requests of the stream that pins the writer are still let in (the queue is waiting for that stream).
- Read-only statements (`sqlite3_stmt_readonly`) use the database's `--read-pool-size` read-only connections and skip the write queue. When the pool is full, reads of that database wait, so the pool must be larger than the number of slow queries running at the same time. The default is the number of CPUs (as the process sees them, cgroup quotas included), at least 16: more connections than CPUs add no throughput, but on small machines a pool of 2–4 lets two or three slow reports hold up every quick read.
- Writes are not spread over several threads per database: SQLite admits one writer per database at a time, so a second write thread would only take turns with the first for the write lock while splitting the queue into smaller groups, i.e. more fsyncs for the same work. Concurrency on the write side comes from the clients; the write thread batches it.
- All streams share one write connection (sqld gives each stream a connection of its own), so statements that would change connection state are refused when they are prepared (SQLite authorizer): assigning PRAGMAs (except `defer_foreign_keys`, which resets itself at the end of each transaction, and is reset by xsqld after a request that set it outside a transaction, so that it cannot reach another stream's transaction; and `user_version`, which is stored in the database file header), creating temporary tables, indexes, views and triggers, and `load_extension()`. Reading PRAGMAs (`PRAGMA foreign_keys`, `PRAGMA table_info(t)`, `pragma_table_info()` and so on) works as usual. sqld's parser refuses these statements too (tested; except `foreign_keys = …`), and xsqld returns the same error: `SQL_PARSE_ERROR`, `SQL string could not be parsed: unsupported statement: <statement>`; one such statement in a batch stops the whole batch from running. `VACUUM` / `VACUUM INTO` get the same error (as with sqld; with no attachable databases it could not run anyway). The runtime only uses `defer_foreign_keys` and the reading forms.
- HTTP limits: at most `--max-connections` (1024) connections per port, further ones wait in the listen backlog; at most `--max-concurrent-requests` (512) requests handled at once, more get 503 immediately (`/health` is not counted, so that a watchdog does not mistake busy for dead); the request head must arrive within `--header-read-timeout-ms` (10 s), which also closes idle keep-alive connections; the request body must arrive within `--body-read-timeout-ms` (10 s), or the reply is 408. A request's time limit counts from the moment its head was received.
- `GET /dump` first copies the database to `--db-path/xsqld-tmp/` with SQLite's online backup (one read transaction, as fast as copying the file), then writes the SQL text from the copy, so a slow client does not hold back checkpoints of the live database; the copy is removed afterwards (and the directory is emptied at startup). If the client takes no data for `--dump-stall-timeout-ms` (60 s), or the whole dump takes longer than `--dump-max-ms` (1 hour), the dump is abandoned and the response body ends with an error rather than the normal ending. The cost is disk space for one extra copy of the database while a dump runs; at most `--max-concurrent-dumps` (2) dumps run at once, more get 503.
- Creating, deleting and first opening a database take a per-database lock, so building one database from a dump does not block the others. Besides the limit between two chunks (60 s), a `dump_url` download as a whole is limited by `--dump-url-timeout-ms` (30 minutes); URLs in the log keep only scheme, host and port (tokens are masked whether they are in the path or the query string).
- Memory for Rust and SQLite comes from mimalloc (SQLite through `SQLITE_CONFIG_MALLOC`, set at startup before the first connection). Reason: xsqld is meant to be built as a static musl binary, and musl's own allocator only grew under the soak load. The same load for 5 minutes in a Linux container: musl's allocator 16 → 791 MB, glibc's allocator 15 → 27 MB; with mimalloc the musl build went 42 → 62 MB and stayed flat from the 4th minute, with about 40% more write and read throughput.
- A panic on any thread ends the process at once (`abort`) and the service manager restarts it. No process is left behind that answers reads and `/health` but refuses every write of a database with 503. Under systemd use `Restart=always`, and a `TimeoutStopSec` longer than the drain on SIGTERM (in-flight requests get up to `--request-timeout-ms` plus 5 s, then the TRUNCATE checkpoint runs): 60 s with the defaults. A shorter timeout loses no data, the WAL is replayed at the next start.
- Streams (batons) hold no connection: a baton carries only the stream number, a sequence number and random bytes; an expired or reused baton gets 400, as with sqld.
- The server runs checkpoints itself (`wal_autocheckpoint=0`). When the WAL reaches `--checkpoint-pages` frames, the database's checkpoint thread is woken and runs a PASSIVE checkpoint on its own connection, without stopping writes. When the background checkpoint cannot catch up with a WAL that keeps being written, the write thread copies the remaining tail (no more than `--checkpoint-pages` frames) itself between groups, so that the WAL can start over from the beginning. When the WAL holds more than `--checkpoint-restart-bytes` (32 MB) the write thread runs RESTART, and after `--checkpoint-idle-ms` of idleness it runs PASSIVE. Once `--checkpoint-pages` frames have been written since the checkpoint thread was woken, the next group waits for that run to end (`checkpoint_wait` in the metrics): on a disk with slow random writes the checkpoint thread otherwise falls further and further behind, and the RESTART that copies what it left stops all writes for hundreds of milliseconds (§5.4). The time of every kind of checkpoint goes into the metrics; a group commit slower than 100 ms logs a `slow commit` line (time and number of requests in the group), so slow writes can be traced to the disk flush.
  - Why a separate checkpoint thread: with the write thread running PASSIVE between groups itself, it spent more than half its time on checkpoints (4025 in 60 s, 55 s in total, longest 276 ms) and every write queued behind them; with the background thread, see §5.4.
- The WAL file is truncated only on a clean shutdown, never while running. The reason is the cost of flushing: with `synchronous=FULL` every group commit flushes once, and when WAL frames are written to blocks that are already allocated, `fdatasync` does not wait for a file system journal commit; while the WAL file is still growing it waits every time. After a checkpoint the next write overwrites the WAL from the beginning and the file keeps its size (bounded by `journal_size_limit`). Numbers: §5.6.
- The bundled SQLite must be compiled with `-DHAVE_FDATASYNC=1` (`LIBSQLITE3_FLAGS` in `.cargo/config.toml`). rusqlite's `bundled` does not add it, and SQLite on Linux then uses `fsync`, paying one extra file system journal commit per commit. That file only applies when cargo is run inside this directory, so `build.rs` fails the build for Linux targets that lack the flag; the `sqlite_flags` field of the startup log shows the flags actually used.
- Every request has a time limit (`--request-timeout-ms`, queueing plus execution); SQL still running at the deadline is interrupted (progress handler), so read transactions cannot hold back checkpoints forever either.
- When a database is opened without `.sentinel`, one is created and never removed, and a clean shutdown runs a TRUNCATE checkpoint: on rollback to sqld it rebuilds its replication log from the database file (§1.2).
- `build.rs` embeds the git commit (`git rev-parse --short HEAD`; the environment variable `XSQLD_GIT_COMMIT` overrides it for builds outside a git checkout; `unknown` without either). The startup log shows version, commit, write threads per database (always 1) and read connections per database.

## 4. Known differences from sqld

Places where results differ from sqld without affecting the runtime:

| Item | sqld 0.24.32 | xsqld | Impact |
|---|---|---|---|
| Message of a syntax error | sqld's own parser: `SQL string could not be parsed: syntax error around L1:6: …` | SQLite's wording: `SQL string could not be parsed: near "x": syntax error` | Same error code `SQL_PARSE_ERROR`, and the batch likewise does not run at all. The runtime only matches the wording of constraint errors such as `UNIQUE constraint failed`, which is identical to sqld |
| `last_insert_rowid` when no new row id was produced | One connection per stream, so it carries the value of the stream's earlier requests | Starts from 0 for every request, like a new sqld stream | Only visible when inserting into a table without rowid and running `SELECT last_insert_rowid()`; the runtime does not read it (only the adapter turns it into D1's `meta.last_row_id`), and which stream the pool hands out is not deterministic anyway |
| `rows_read` / `rows_written` | libsql's own counters (rows scanned) | Rows returned / rows changed | The adapter only copies them into D1's `meta`; the runtime does not read them |
| `replication_index` | WAL frame number | `null` | Only used for replication; the runtime does not read it |
| `sqlite_schema.sql` after creating a database from a dump | Statements are parsed and reassembled: comments are lost and whitespace changes | Statements from the dump run as they are; table definitions keep the original text | Some migration guards look at the text of `sqlite_master.sql`; xsqld is the more predictable of the two |
| Multi-line triggers in a dump | Creating the database fails (statements are split at a `;` at the end of a line: `incomplete input`) | Works (statements are split with `sqlite3_complete`) | — |
| `PRAGMA foreign_keys = …` | Runs, affecting only that stream's connection | Refused, with the same error sqld gives for other PRAGMA assignments (§3: all streams share the write connection) | Unused by the runtime |
| Exporting non-UTF-8 text values | The dump fails | Written as `CAST(X'…' AS TEXT)`; loading it back gives the same bytes | — |
| Statements in a dump used to create a database | Anything runs | Only what dumps consist of: `PRAGMA foreign_keys=OFF`, `BEGIN` / `COMMIT`, `CREATE TABLE` / `INDEX` / `VIEW` / `TRIGGER`, `INSERT INTO`, `DELETE FROM sqlite_sequence`, `ANALYZE sqlite_schema`, one statement each; anything else gets 400 | Dumps of databases with virtual tables need `PRAGMA writable_schema`, which xsqld refuses; the runtime's databases have no virtual tables |
| Deferred foreign keys (`DEFERRABLE`) and concurrency | Each request is checked at its own commit | Also checked at the request's own commit (§3), but concurrent requests in one group may run in a different order | Concurrent requests have no defined order, so any order is valid; the runtime has no `DEFERRABLE` |
| `synchronous` and others | See §1.1 | See §1.1 | Intentional (§1.1) |

## 5. Tests (local macOS, Apple silicon)

SQLite 3.54.1 (rusqlite 0.41 `bundled`). sqld is the official image `ghcr.io/tursodatabase/libsql-server:v0.24.32`, run in local Docker.

### 5.1 Unit tests

`cargo test`: 83, all passing. Covered: protocol encoding and decoding, conditional steps, a failing transaction in a group commit rolling back only itself, redoing a group after SQLite rolled back the whole of it, rolling back a pinned transaction on timeout, 400 for expired or reused batons, queue timeouts, interrupting requests at their deadline, checkpoints and the TRUNCATE on shutdown, writes waiting for the checkpoint thread until its run ends, dumps byte-identical to sqld's, creating databases from dumps, the directory layout of created and deleted databases, and the version in `--version`, `/health` and `/metrics`.

Every item raised in code review has a test: random SQL lexer input (with all its prefixes) stays in bounds; the request that made the write thread panic gets an error and writes continue afterwards; a panic ends the process with SIGABRT (tested in a child process); state-changing PRAGMAs, temporary tables and `load_extension()` are refused while reading PRAGMAs works; the live database completes a TRUNCATE checkpoint while a client is not reading `/dump`, and a stalled or overlong dump ends with an error and removes its copy; a full write queue answers 503 but lets the pinned stream in; header and body read timeouts, connection and concurrent request limits; quick reads do not queue while slow queries occupy the read pool; slow groups are committed early; `dump_url` masking, IPv6 `Host` headers, total download time; other databases do not wait while one is being created; non-UTF-8 text round-trips through a dump; database creation accepts only dump statements; a failed group transaction start is rolled back first; deferred foreign keys are checked per request; VACUUM is unavailable; `defer_foreign_keys` set outside a transaction does not reach the next stream; concurrent dumps are limited until their export ends; writes wait for the checkpoint thread exactly when it is `--checkpoint-pages` frames behind. With each fix reverted, its test fails. Another test checks that SQLite's memory really comes from mimalloc (true in a child process that switched, false in a process that did not).

### 5.2 Differential tests

`tests/compare.py`: sends the same requests to sqld and xsqld and compares the results field by field (except `query_duration_ms` and the text of batons).

```bash
docker run -d --name sqld-ref -p 127.0.0.1:18080:8080 -p 127.0.0.1:18081:8081 --entrypoint /bin/sqld \
  ghcr.io/tursodatabase/libsql-server:v0.24.32 --enable-namespaces --http-listen-addr 0.0.0.0:8080 --admin-listen-addr 0.0.0.0:8081
xsqld --db-path /tmp/x --http-listen-addr 127.0.0.1:28080 --admin-listen-addr 127.0.0.1:28081
python3 tests/compare.py
```

Result: all 48 pipeline requests identical; protocol behaviour (continuing, expired and reused batons, `close`, malformed requests, missing databases, the admin API's errors) identical; `GET /dump` of the same data byte-identical; databases created from the dump on both sides export identically again. Covered: round trips of every kind of value (integers as strings, 64-bit limits and 2^53+1, floats such as `1e300` / `-0.0`, Unicode text, empty and non-empty blobs, a 1 MB blob); foreign keys, UNIQUE, CHECK, trigger `RAISE`, views, tables without rowid, UPSERT, `RETURNING`, `unhex()` and JSON functions, `ALTER TABLE`; the wording of UNIQUE / NOT NULL / FOREIGN KEY / CHECK errors; wrong parameter counts, named and numbered parameters; empty results; rollback when a batch fails halfway, all kinds of conditional steps; transactions across requests; the batches the runtime actually sends and its merged session requests; Hrana request types that are not supported.

xsqld defects found and fixed through these comparisons: `SELECT *` missing columns after another connection altered the table (column information must be read after the first step), an empty batch treated as `close`, positional errors such as `no such column` that must be reported as `SQL_INPUT_ERROR`, and a batch with a syntax error that must not run at all.

### 5.3 Replaying the runtime's database migrations

This test lives in the downstream project, not in this repository, because it needs the runtime's source tree. With the runtime's own database client and migration runner, it runs every migration of its tenant database on an empty database, exports it, creates a new database from the dump, and compares row counts per table and the bytes of the dumps.

| | xsqld | sqld 0.24.32 |
|---|---|---|
| Migrations | 85/85, 422 ms; a second run executes nothing again | 85/85, 924 ms |
| Row counts per table after creating from the dump (169 tables) | Same | Same |
| Dump of the recreated database vs. the original dump | Byte-identical | Different (§4: table definitions are rewritten) |

### 5.4 Load tests (short local runs, orders of magnitude only)

The load test scripts live in the downstream project, not in this repository. A fresh data directory for each item. These are short runs on a laptop SSD; the numbers on a Linux server with a cloud volume are in §6. sqld runs in Docker's virtual machine, so its numbers are low and only for reference.

| Item | xsqld | sqld (Docker) |
|---|---|---|
| Single-row writes, 1 / 4 / 16 writers, transactions per second | 5030 / 11142 / 15505 | 803 / 525 / 477 |
| Batches of 100 rows, 1 / 4 / 16 writers, rows per second | 17586 / 22989 / 29488 | — |
| Concurrent writers: p99 with 16 writers; p99 with 64 writers; "busy" errors | 16.5 ms; 23.3 ms; 0 | 125 ms; —; one 5 s timeout |
| Checkpoints under load, 60 s: longest probe read / longest probe write / largest WAL / writes | 18–22 ms / 106–108 ms / 52–55 MB / about 19,000 rows per second (two runs) | — |

The probe write time of the checkpoint test includes waiting behind the writers (p50 14 ms); once or twice every 60 s it just exceeds 100 ms, caused by the TRUNCATE on the write thread. Before the checkpoint thread (with the write thread running PASSIVE itself) the same test gave: longest probe read 95 ms, longest write 132 ms, 12,197 rows per second.

On a cloud block storage volume (about 3800 rows per second, 30 minutes) the checkpoint thread used to fall behind: its runs took about 200 ms, and the RESTART at 32 MB, run about once a second, had to copy everything written meanwhile and stopped writes for 400–900 ms each time (probe write p99 746 ms). Since then writes wait for a checkpoint run once `--checkpoint-pages` frames have been written after it began, which keeps every run and every RESTART to about that many frames. In a local Docker VM limited to 10,000 write IOPS this costs no throughput (4184 against 4225 rows per second, 300 s); the waits added up to 3.9 s. On the cloud volume: §6.

### 5.5 Rollback drill

Same data directory: sqld 0.24.32 creates the `tenant` database, writes 1000 rows and stops cleanly → xsqld opens it, reads those 1000 rows, writes another 1000, exits on SIGTERM (only `data` and `.sentinel` are left in the database directory) → sqld starts again, logs `Replication log is dirty, recovering from database file`, both sets of 1000 rows are there, `PRAGMA integrity_check` is ok, and writing continues normally.

### 5.6 Flushing on Linux (Linux VM of local Docker, ext4)

Single-row writes, 16 writers: about 3800 transactions per second without `-DHAVE_FDATASYNC=1` (SQLite uses `fsync`), about 9000 with it. The idle checkpoint used to be TRUNCATE, which made the first write after idleness wait for a file system journal commit (about 5.5 ms); with PASSIVE and a WAL file that is not truncated while running, that spike is gone. The numbers that count are those on a Linux server (§6).

## 6. Performance

Acceptance runs of the load tests, on a cloud server: 8 dedicated vCPUs, 16 GB of memory, Debian 13, the data on a 100 GB network block storage volume (ext4, fsync p50 0.56 ms, p99 2.7 ms). Static musl build, default settings (`synchronous=FULL`), load generators on the same machine.

| Test | Result |
|---|---|
| Single-row write transactions, 1 / 4 / 16 concurrent writers (30 s each) | 824 / 1654 / 3487 per second; p99 2.6 / 42 / 40 ms; 0 errors |
| Batches of 100 rows, 1 / 4 / 16 writers (30 s each) | 4960 / 7684 / 9561 rows per second |
| Concurrent single-row writers (10 min each) | 16 writers: p99 39 ms; 64 writers: p99 75 ms; 0 "busy" errors |
| 32 writers for 1 hour, a probe writing and reading every 2 s | 0 failed probes; longest probe write 23 ms |
| Checkpoints under continuous load (4 writers of 100-row batches, 30 min) | 3678 rows/s; probe write p99 202 ms, longest 375 ms; longest probe read 7.6 ms; WAL at most 39.5 MB; 0 errors |
| Mixed reads and writes for 3 hours | 811,089 writes and 3,544,246 reads, 0 errors; memory 56 MB on average in the first quarter, 58 MB in the last (at most 61 MB) |
| `kill -9` while writing, 100 rounds | 2,759,776 acknowledged rows, none lost; `integrity_check` ok; restart within 0.21 s |
| Simulated power cut (SysRq `b`: immediate reboot without syncing) while writing, 10 rounds | No acknowledged row lost; `integrity_check` ok |
| Indexed queries on a 10-million-row table, server-side execution time against the same query run directly in SQLite (p50) | 0.88–1.04 times; HTTP round trip p50 0.27 ms (primary key) to 1.6 ms (a 50-row page) |

Probe writes queue behind the writers, so their latency includes waiting for the group in front of them; the probe write p99 of the checkpoint test is mostly that queue, not checkpoint pauses.
