# Split-range parallel aggregation: speed-up and measurements

What the feature does and which statements it takes: [README §3.1](../README.md#31-split-range-parallel-aggregation). This document records how much faster xsqld 0.1.3 is and why it was built the way it was: the experiment run before any product code (phase 1), the threshold it had to meet, and the measurements of the implementation (phase 2).

## Summary

xsqld 0.1.3 runs an aggregate over a large table on several CPU cores: the rows are split into rowid ranges read at once by several connections on one snapshot, and SQLite combines the partial results. The same 100-million-row database (6 GB), served by 0.1.2 and by 0.1.3 with default flags (4 ranges), through the HTTP API, on an Apple M4 Max:

| Statement | 0.1.2 | 0.1.3 | Speed-up |
|---|---|---|---|
| `SELECT COUNT(*) FROM calls WHERE status = 'answered' AND dur > 60` | 3.81 s | 1.27 s | **3.0×** |
| `SELECT day, COUNT(*), SUM(dur), MIN(dur), MAX(dur) FROM calls GROUP BY day` (60 groups) | 22.9 s | 6.78 s | **3.4×** |
| the same with `WHERE status <> 'failed' … ORDER BY 2 DESC LIMIT 10` | 20.7 s | 6.03 s | **3.4×** |

- More ranges scale further: with 8 (`--parallel-max-ranges 8`), measured directly in SQLite, 3.9 times for the filtered `COUNT` and 7.2 times for the `GROUP BY` (§2).
- Everything else is as fast as before: point reads, aggregates on small tables and aggregates SQLite answers through an index (0.09–0.23 ms at p50 over HTTP, within the noise between two runs; §5.2), and a bare `COUNT(*)` (150 ms → 144 ms on the 100-million-row table; §5.1).
- Beside four writers, twice as many aggregates finish in the same 30 s, the WAL grows less (largest 566 MiB → 317 MiB) and writes are as fast (§5.3).
- Results are byte for byte the ones a single connection returns: statements for which that cannot be guaranteed are not split (README §3.1).
- Measured on macOS only: no Linux, cloud-volume or cold-cache numbers yet.

## 1. Method

- Hardware: Apple M4 Max (12 performance and 4 efficiency cores), 64 GiB of memory, internal SSD; macOS 26.6. All runs on this one machine, data in the OS page cache after a warm-up run (cold-cache numbers were not measured).
- SQLite 3.54.1, the bundled build of rusqlite 0.41 (`libsqlite3-sys` 0.39), as xsqld uses it, plus `-DSQLITE_ENABLE_SNAPSHOT`; Rust 1.99, release builds.
- Connections with xsqld's settings: WAL, `foreign_keys = ON`, `recursive_triggers = OFF`, `cache_size = -2000`; read connections opened read-only; the writer with `synchronous = FULL`, `wal_autocheckpoint = 0`, `journal_size_limit = 64 MiB`.
- Phase 1: a measurement program that talks to SQLite directly, so that only the effect of splitting is measured. Phase 2: the same database files served by xsqld 0.1.2 and by 0.1.3, measured over HTTP with a small driver. Both tools are kept with the other load tests in the downstream project, not in this repository.
- Table: `calls(id INTEGER PRIMARY KEY, day INTEGER, status TEXT, dur INTEGER, num INTEGER, note TEXT)`, **100,000,000 rows** (6.0 GB; built in 114 s including the index), `day` 60 values, `status` 4 values (60% `answered`), `dur` 0–3599, `num` 3,000,000 values with an index `calls_num(num)`, `note` 8–31 random letters. A second table of the same kind with 4,000,000 rows for the runs beside a writer.
- Each timing is the median of three runs after one warm-up run; every split result was compared with the single-connection result and was identical.

## 2. Phase 1: speed-up (direct SQLite, 100 M rows)

The ranges of one statement run on as many connections on one snapshot (`sqlite3_snapshot_get` on the first, `sqlite3_snapshot_open` on the others); "setup" is the snapshot plus the bounds lookup, "merge" loading the partial rows into an in-memory table and running the combining statement.

| Query | Ranges | Median ms | Speed-up | Setup ms | Merge ms | Rows |
|---|---|---|---|---|---|---|
| `COUNT(*)` (no WHERE) | 1 | 152 | 1.00 | | | 1 |
| | 2 | 1093 | 0.14 | 0.03 | 0 | |
| | 4 | 746 | 0.20 | 0.03 | 0 | |
| | 8 | 967 | 0.16 | 0.03 | 0 | |
| `COUNT(*) WHERE status = 'answered' AND dur > 60` | 1 | 3330 | 1.00 | | | 1 |
| | 2 | 2146 | 1.55 | 0.03 | 0 | |
| | 4 | **1221** | **2.73** | 0.03 | 0 | |
| | 8 | 863 | 3.86 | 0.03 | 0 | |
| `day, COUNT, SUM, MIN, MAX … GROUP BY day` (60 groups) | 1 | 22220 | 1.00 | | | 60 |
| | 2 | 11333 | 1.96 | 0.03 | 0 | |
| | 4 | **5825** | **3.81** | 0.03 | 0 | |
| | 8 | 3100 | 7.17 | 0.03 | 0 | |
| same with `WHERE status <> 'failed' … ORDER BY 2 DESC LIMIT 10` | 1 | 20154 | 1.00 | | | 10 |
| | 2 | 10711 | 1.88 | 5.33 | 0 | |
| | 4 | **5388** | **3.74** | 0.03 | 0 | |
| | 8 | 2919 | 6.90 | 0.03 | 0 | |
| `num, COUNT(*) GROUP BY num` (3 M groups), index on `num`, split by rowid | 1 | 2905 | 1.00 | | | 3,000,000 |
| | 2 | 15861 | 0.18 | 0.07 | 1597 | |
| | 4 | 10645 | 0.27 | 0.07 | 3024 | |
| | 8 | 10305 | 0.28 | 0.08 | 6396 | |
| same, split by ranges of `num` (`num >= ? AND num < ?`) | 1 | 2858 | 1.00 | | | 3,000,000 |
| | 2 | 1307 | 2.19 | 0.06 | 12 | |
| | 4 | 719 | 3.97 | 0.07 | 12 | |
| | 8 | 395 | 7.23 | 0.07 | 13 | |
| same `NOT INDEXED`, split by rowid | 1 | 26731 | 1.00 | | | 3,000,000 |
| | 2 | 15671 | 1.71 | 0.50 | 1593 | |
| | 4 | 10243 | 2.61 | 0.06 | 2960 | |
| | 8 | 10182 | 2.63 | 0.07 | 6289 | |

Plans (`EXPLAIN QUERY PLAN`): the filtered `COUNT` and the `GROUP BY day` queries are `SCAN calls` (with a temporary b-tree for `GROUP BY`, and one for `ORDER BY`); each range is `SEARCH calls USING INTEGER PRIMARY KEY (rowid>? AND rowid<?)`. The bare `COUNT(*)` is `SCAN calls USING COVERING INDEX calls_num`, compiled to the opcode that counts b-tree cells without reading rows; `GROUP BY num` with its index is `SCAN … USING COVERING INDEX calls_num` and needs no sorting. One measurement of the last row (`NOT INDEXED`, 2 to 8 ranges) overlapped a compile on the same machine; its numbers are less certain than the others.

What the numbers say:

- Statements that read every row and do real work per row scale with the ranges: 2.7–3.8 times with 4, 3.9–7.2 times with 8. `GROUP BY` without an index gains more than linearly from fewer rows per sort (n log n).
- The bare `COUNT(*)` must not be split: counting cells is 5–7 times faster than any range scan.
- A plan that uses an index must not be split by rowid: `GROUP BY num` on its index becomes 4 times slower, because every range has to sort what the index gave in order, and 3 M groups per range have to be merged.
- Ranges of the index key keep the index plan and scale well (3.97 times with 4), but need key bounds and separate handling of `NULL` keys; not implemented (§6).
- Millions of groups without an index still gain (2.6 times) but produce groups × ranges partial rows (12 M here, about 1 GB in memory): bounded in the product by `--parallel-max-partial-rows`.

## 3. Phase 1: one snapshot

### 3.1 Which way

- **A. `sqlite3_snapshot_get` / `sqlite3_snapshot_open`.** Checked in the sources: `libsqlite3-sys` 0.39.1 `build.rs` compiles the bundled amalgamation with a fixed list of `-DSQLITE_ENABLE_*` flags (API_ARMOR, COLUMN_METADATA, DBSTAT_VTAB, FTS3, FTS5, JSON1, LOAD_EXTENSION, RTREE, STAT4, plus some behind cargo features); `SQLITE_ENABLE_SNAPSHOT` is not among them, and the functions are inside `#ifdef SQLITE_ENABLE_SNAPSHOT` in `sqlite3.c`, so a default build does not link. The bindings declare them, and the same `build.rs` adds every `-D…` of the environment variable `LIBSQLITE3_FLAGS`, which xsqld's `.cargo/config.toml` already sets for `HAVE_FDATASYNC`: adding `-DSQLITE_ENABLE_SNAPSHOT` there is all it takes (`build.rs` of xsqld now refuses to build without it).
- **B. The write thread pausing between two commits** while the range connections start their read transactions. Measured: the write thread stops for p50 0.06–0.18 ms, p99 0.2–0.4 ms, at most 2.3–3.1 ms (2, 4, 8 connections; 400 pauses each, 10-row commits back to back); the reader waits for the next gap p50 0.04 ms, p99 2.6–9.2 ms. In xsqld the wait would be for the end of a group, up to `--group-max-exec-ms` (20 ms) plus a flush, and the coupling of the read pool to the write thread is new machinery.

Chosen: **A**. It costs the readers 0.03 ms, stops no writes and keeps reads and writes apart as before.

One case needs its own rule. `sqlite3_snapshot_get` fails while no transaction has been written to the WAL since it was created on disk (documented; it happens right after a start on a checkpointed database, until the first write). The first connection then reads the database file alone (it holds no WAL frames), and no checkpoint can change that file while it holds its read transaction; a second connection that also finds no snapshot to take sees the same untouched WAL, hence the same data. If one of them does get a snapshot, a write came in between, and the statement runs on one connection. The phase-1 run met this case 63 times (the table had just been built and checkpointed).

### 3.2 Same snapshot, not just the same moment

A writer changes rows inside the scanned range all the time (20 updates, deletes and inserts per transaction, 374,475 transactions during the run; a PASSIVE checkpoint every 200). Every iteration counts `status = 'answered'` split into 4 ranges and once more on the first connection inside its read transaction; the two must be equal.

| Setup | Iterations | Equal | Different | Failed to set up |
|---|---|---|---|---|
| Shared snapshot (A) | 300 | **300** | 0 | 0 |
| Each connection its own read transaction (control) | 300 | 25 | 275 | 0 |

## 4. Phase 1: WAL and writes beside split statements

A writer commits 10 rows of 300 bytes at a time as fast as it can (`synchronous = FULL`); a checkpointer runs PASSIVE every 100 commits and RESTART once the WAL exceeds 32 MiB (retrying a failed one after 4 MiB more, as xsqld does); `GROUP BY day` on the 4 M-row table runs in a loop, with a 200 ms pause after each run, for 20 s.

| Reads | Queries | Commits | Commit p50 / p99 / max ms | Largest WAL | RESTART ok / failed |
|---|---|---|---|---|---|
| none | — | 222,215 | 0.06 / 0.13 / 196 | 3 MiB | — |
| 1 connection | 25 | 125,567 | 0.07 / 0.14 / 167 | 201 MiB | 13,360 / 239 |
| 4 ranges | 44 | 177,805 | 0.07 / 0.15 / 253 | 88 MiB | 23,107 / 55 |

Split statements hold their read transactions for a shorter time, so the WAL stays smaller and fewer RESTARTs fail; commit latency is unchanged (the maxima are single macOS flush spikes, in every row). The same loop without the pause (always one statement running) starves RESTART whichever way it runs (WAL past 2 GB in 20 s for both): that is the checkpoint starvation that xsqld's read gate handles (README §3), measured with xsqld itself in §5.3. Two mistakes of the first harness, worth recording: writing rows with ids far beyond the table (2·10⁹) made one range hold every row (the product therefore probes where the rows are, README §3.1), and retrying RESTART after every commit starved `sqlite3_snapshot_open` of the checkpoint lock (xsqld retries only after `--checkpoint-pages` frames).

## 5. Threshold and phase 2

The threshold set for going on: with 4 ranges, the filtered `COUNT` and the `GROUP BY` with few groups at least 2.5 times as fast as one connection, and every range reading exactly the same snapshot. Measured: 2.73 and 3.81 times (3.74 with `WHERE … ORDER BY … LIMIT`), and 300 of 300 identical under writes. Met; implemented in xsqld 0.1.3.

### 5.1 Through xsqld (HTTP, 100 M rows)

`e2e.py split`: the same database files served by xsqld 0.1.2 (no splitting) and by 0.1.3, default flags (so 4 ranges on this machine), one client, `query_duration_ms` of the response (the client's time is within 1 ms of it). Median of three runs after the first; the first run is shown on its own because it also pays, once, for an attempt that ends in a fallback.

| Query | 0.1.2 ms | 0.1.3 ms | Speed-up | 0.1.3 path |
|---|---|---|---|---|
| `COUNT(*) WHERE status = 'answered' AND dur > 60` | 3807 | 1271 | 3.0 | split |
| `day, COUNT, SUM, MIN, MAX … GROUP BY day` | 22937 | 6777 | 3.4 | split |
| same with `WHERE … ORDER BY 2 DESC LIMIT 10` | 20651 | 6027 | 3.4 | split |
| `COUNT(*)` (no WHERE) | 150 | 144 | — | one connection (`plan`: counting shortcut) |
| `num, COUNT(*) GROUP BY num HAVING COUNT(*) > 60` (index on `num`) | 2092 | 2037 | — | one connection (`plan`: index) |
| `note, COUNT(*) WHERE dur > 3500 GROUP BY note HAVING COUNT(*) > 1` (2.8 M groups, no index) | 3426 (first 3410) | 3496 (first 4493) | — | first run split, then `rows`: run again on one connection; later runs not split |

The many-groups statement shows the cost of a split that does not pay: its first run took 1.1 s longer (the split and its partial rows, then the run on one connection); after that its text is remembered and it runs as before.

### 5.2 Statements that are not split: before and after

`e2e.py small`, 20,000 requests each after 500 warm-up requests, one client, HTTP round trip included; each build run twice, alternating, to show the noise between runs.

| Statement | 0.1.2 p50 / p95 / p99 ms (run 1; run 2) | 0.1.3 p50 / p95 / p99 ms (run 1; run 2) |
|---|---|---|
| Point read `SELECT * FROM calls WHERE id = ?` (not the shape) | 0.092 / 0.117 / 0.144; 0.094 / 0.147 / 0.215 | 0.091 / 0.111 / 0.136; 0.094 / 0.134 / 0.190 |
| `SELECT COUNT(*), SUM(v) FROM small WHERE g = ?`, 10,000 rows (the shape, `small`) | 0.224 / 0.257 / 0.280; 0.229 / 0.307 / 0.375 | 0.229 / 0.275 / 0.317; 0.225 / 0.288 / 0.367 |
| `SELECT COUNT(*) FROM calls WHERE num = ?` on its index (the shape, `plan`) | 0.192 / 0.269 / 0.369; 0.090 / 0.137 / 0.179 | 0.096 / 0.144 / 0.210; 0.095 / 0.114 / 0.185 |

No difference beyond the noise between two runs of the same build.

A table of 3000 rows whose rowids are millisecond timestamps 100,000 apart spans 3·10⁸ rowids. 0.1.3 first decided by that span and split it on every request; it now estimates the rows (README §3.1). `SELECT COUNT(*), SUM(v) FROM sp WHERE g = ?`, 20,000 requests, two runs each:

| Table | Span only: p50 / p95 / p99 ms | Estimate: p50 / p95 / p99 ms |
|---|---|---|
| 3000 rows, ids 1700000000000 + i·100000 | 0.236 / 0.389 / 1.842; 0.230 / 0.326 / 0.392 (split every time) | 0.137 / 0.176 / 0.217; 0.135 / 0.186 / 0.233 (not split) |
| 3000 rows, ids 1–3000 (for comparison) | 0.136 / 0.194 / 0.242; 0.136 / 0.182 / 0.227 | 0.135 / 0.185 / 0.241; 0.132 / 0.186 / 0.231 |

The estimate itself (release build, direct): 200,000 dense rows → 200,000; 3,333,334 rows left after deleting every third of 5 M → 3,340,033; 40,000 dense rows and two rows at 2·10⁹ and 9·10¹² → 40,002 (exact); the 3000 timestamp ids → 2,984; three clusters of 5000 (one with ids 7 apart) → 14,999; 70 rows spread over the whole 64-bit range → 70. Each estimate took 0.1–0.22 ms. A first version of 0.1.3 ran the plan checks of the last statement on every request (three more statements compiled): +0.15 ms at p95 and +0.25 ms at p99 in one run. Since then a statement whose table or plan rules out a split is remembered by its text, together with the schema version, and costs only its parse until the schema changes.

### 5.3 Writes beside split statements, with the read gate

`e2e.py wal`, 30 s on the 4 M-row table: 4 writers each sending single-row inserts of 300 bytes back to back, 1 client running the `GROUP BY day` aggregate back to back (always one running: the pattern of checkpoint starvation, README §3); default flags.

| Build | Writes | Write p50 / p99 / max ms | Aggregates | Largest WAL | RESTART | with the read gate closed | Reads held back (longest) |
|---|---|---|---|---|---|---|---|
| 0.1.2 | 256,409 | 0.25 / 0.57 / 1687 | 55 | 566 MiB | 75 | 43 | 32 (40 ms) |
| 0.1.3 (all 112 split, 4 ranges) | 255,654 | 0.25 / 0.67 / 244 | 112 | 317 MiB | 111 | 57 | 54 (92 ms) |

Writes run at the same rate; twice as many aggregates finish; the WAL stays smaller because each read transaction is shorter; the read gate still gets its RESTARTs through (a split statement is one read for it), and no read is held back longer than `--checkpoint-reader-wait-ms` (200 ms). Aggregates that each take longer than that wait (here 270 ms split, 550 ms on one connection) keep the WAL growing while they follow each other without a break: the known issue of README §4.1, unchanged by splitting and made smaller by it.

## 6. Not done, and why

- Ranges of an index key for `GROUP BY` on an indexed column (3.97 times with 4 ranges in §2): needs key bounds that split the rows evenly, separate handling of `NULL` keys and of keys of mixed types, and a plan check of its own. Such statements run on one connection.
- Sums of REAL values, `AVG`, `TOTAL`: the result's last digits depend on the order of additions; not accepted.
- Cold-cache runs, Linux, a cloud volume: not measured. Only macOS on Apple silicon; the Linux build compiles the same code with the same flags.
