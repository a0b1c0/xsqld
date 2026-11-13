#!/usr/bin/env python3
"""Comparison test (README §5.2): the same HTTP requests go to a real sqld 0.24.32 and to xsqld; status codes
and bodies must match field by field.

Not compared (known, documented in README §4): query_duration_ms; replication_index (sqld: WAL frame number, xsqld:
null); rows_read / rows_written (sqld: libsql's own counters; the runtime only copies them into D1's meta); baton
values (random on both sides; only null vs. non-null is compared); the message text of syntax errors (sqld uses its
own parser; SQL_PARSE_ERROR for unsupported statements is compared word for word).

Also compared: requests sent at the same time (xsqld runs them in one group transaction, sqld one by one), each
touching rows of its own so that every order gives the same results; GET /dump with and without preserve_row_ids;
databases created from dumps (a load that fails on xsqld counts as a difference).

Usage:
  docker run -d --name e415-sqld -p 127.0.0.1:18080:8080 -p 127.0.0.1:18081:8081 --entrypoint /bin/sqld \
      ghcr.io/tursodatabase/libsql-server:v0.24.32 --enable-namespaces --http-listen-addr 0.0.0.0:8080 \
      --admin-listen-addr 0.0.0.0:8081
  xsqld --db-path /tmp/x --http-listen-addr 127.0.0.1:28080 --admin-listen-addr 127.0.0.1:28081
  python3 tests/compare.py   [--sqld 127.0.0.1:18080 --sqld-admin 127.0.0.1:18081 --xsqld 127.0.0.1:28080 --xsqld-admin 127.0.0.1:28081]
"""

import argparse
import http.client
import http.server
import json
import os
import sqlite3
import sys
import threading
import time

HERE = os.path.dirname(os.path.abspath(__file__))


def request(addr, method, path, body=None, host=None, ctype="application/json"):
    h, p = addr.rsplit(":", 1)
    c = http.client.HTTPConnection(h, int(p), timeout=60)
    headers = {}
    if host:
        headers["Host"] = host
    if body is not None:
        headers["Content-Type"] = ctype
        body = body if isinstance(body, (bytes, str)) else json.dumps(body)
    c.request(method, path, body=body, headers=headers)
    r = c.getresponse()
    data = r.read()
    c.close()
    return r.status, data


def normalize(status, data):
    try:
        v = json.loads(data)
    except ValueError:
        return status, data.decode("utf-8", "replace")

    def walk(x):
        if isinstance(x, dict):
            out = {}
            for k, val in x.items():
                if k in ("query_duration_ms", "replication_index", "rows_read", "rows_written"):
                    continue
                if k == "baton":
                    out[k] = None if val is None else "<baton>"
                    continue
                out[k] = walk(val)
            if out.get("code") == "SQL_PARSE_ERROR" and "unsupported statement" not in str(out.get("message")):
                out["message"] = "<syntax error>"
            return out
        if isinstance(x, list):
            return [walk(i) for i in x]
        return x

    return status, walk(v)


class Side:
    def __init__(self, name, addr, admin, ns):
        self.name, self.addr, self.admin, self.ns = name, addr, admin, ns
        self.baton = None

    def pipeline(self, requests, baton="keep"):
        b = self.baton if baton == "keep" else baton
        status, data = request(self.addr, "POST", "/v2/pipeline", {"baton": b, "requests": requests}, host=f"{self.ns}.db")
        if status == 200:
            self.baton = json.loads(data).get("baton")
        return status, data


def ex(sql, *args, named=None):
    stmt = {"sql": sql, "args": list(args), "want_rows": True}
    if named:
        stmt["named_args"] = [{"name": k, "value": v} for k, v in named.items()]
    return {"type": "execute", "stmt": stmt}


def i(v):
    return {"type": "integer", "value": str(v)}


def t(v):
    return {"type": "text", "value": v}


def f(v):
    return {"type": "float", "value": v}


def blob(b64):
    return {"type": "blob", "base64": b64}


NULL = {"type": "null"}
CLOSE = {"type": "close"}
# a result past both servers' response limits (sqld 0.24.32's, and xsqld's --max-response-bytes default of 64 MB)
BIG_RESULT = ("SELECT hex(zeroblob(1000000)) FROM (WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n "
              "WHERE i < 40) SELECT i FROM n)")


def d1_batch(*stmts, begin="BEGIN IMMEDIATE"):
    """The batch local-libsql.ts sends: BEGIN, each statement if the previous one succeeded, COMMIT, else ROLLBACK"""
    n = len(stmts)
    steps = [{"stmt": {"sql": begin}}]
    for k, s in enumerate(stmts):
        steps.append({"condition": {"type": "ok", "step": k}, "stmt": s["stmt"]})
    steps.append({"condition": {"type": "ok", "step": n}, "stmt": {"sql": "COMMIT"}})
    steps.append({"condition": {"type": "not", "cond": {"type": "ok", "step": n + 1}}, "stmt": {"sql": "ROLLBACK"}})
    return {"type": "batch", "batch": {"steps": steps}}


SCHEMA = [
    ex("CREATE TABLE p(id INTEGER PRIMARY KEY, name TEXT NOT NULL)"),
    ex(
        "CREATE TABLE c(id INTEGER PRIMARY KEY AUTOINCREMENT, p_id INTEGER NOT NULL REFERENCES p(id), "
        "email TEXT UNIQUE, n INTEGER CHECK (n >= 0), r REAL, b BLOB, at INTEGER NOT NULL DEFAULT 0)"
    ),
    ex("CREATE INDEX c_p ON c(p_id, at)"),
    ex(
        "CREATE TRIGGER c_guard BEFORE INSERT ON c WHEN NEW.email = 'forbidden' "
        "BEGIN SELECT RAISE(ABORT, 'email forbidden'); END"
    ),
    ex("CREATE TABLE kv(k TEXT PRIMARY KEY, v) WITHOUT ROWID"),
    ex("CREATE VIEW c_view AS SELECT c.id, p.name FROM c JOIN p ON p.id = c.p_id"),
]

# Each case: list of (requests, baton) steps; baton "keep" continues the stream, None starts a new one
CASES = {
    "values round trip": [
        [ex("SELECT ?, ?, ?, ?, ?, ?, ?", NULL, i(9223372036854775807), i(-9223372036854775808), f(1.5), t("é ☎ '\""), blob("AAEC/w=="), blob(""))],
        [ex("SELECT 1, 1.0, 0.1, 1e300, -0.0, 'x', x'00', NULL, 9007199254740993, 2.5e-10, 100.0")],
        [ex("SELECT typeof(?), typeof(?), typeof(?), typeof(?)", i(1), f(2), t("3"), blob("BA"))],
    ],
    "schema and writes": [
        SCHEMA,
        [ex("INSERT INTO p(id, name) VALUES (?, ?)", i(1), t("one")), ex("INSERT INTO p(name) VALUES ('two')")],
        [ex("INSERT INTO c(p_id, email, n, r, b) VALUES (1, 'a@x', 1, 1.25, x'0102')"), ex("INSERT INTO c(p_id, email, n) VALUES (2, 'b@x', 2) RETURNING id, email")],
        [ex("UPDATE c SET n = n + 1 WHERE p_id = ?", i(1)), ex("DELETE FROM c WHERE email = 'nobody'"), ex("SELECT changes(), last_insert_rowid(), total_changes() > 0")],
        [ex("INSERT INTO kv VALUES ('a', 1) ON CONFLICT(k) DO UPDATE SET v = excluded.v"), ex("INSERT INTO kv VALUES ('a', 2) ON CONFLICT(k) DO UPDATE SET v = excluded.v"), ex("SELECT * FROM kv")],
        [ex("SELECT * FROM c ORDER BY id"), ex("SELECT * FROM c_view ORDER BY id"), ex("SELECT * FROM c WHERE 0")],
        [ex("SELECT hex(unhex('00ff')), json_extract('{\"a\":[1,2]}', '$.a[1]'), json_group_array(id) FROM c")],
        [ex("CREATE TABLE IF NOT EXISTS p(id)"), ex("ALTER TABLE p ADD COLUMN extra TEXT"), ex("SELECT * FROM p")],
    ],
    "errors keep SQLite text": [
        [ex("INSERT INTO c(p_id, email) VALUES (1, 'a@x')")],
        [ex("INSERT INTO c(p_id, email) VALUES (99, 'z@x')")],
        [ex("INSERT INTO p(id) VALUES (5)")],
        [ex("INSERT INTO c(p_id, n) VALUES (1, -1)")],
        [ex("INSERT INTO c(p_id, email) VALUES (1, 'forbidden')")],
        [ex("INSERT INTO p(id, name) VALUES (1, 'dup')")],
        [ex("SELECT * FROM nope"), ex("SELECT nope FROM p"), ex("INSERT INTO p VALUES (1)")],
        [ex("SELEC 1"), ex("SELECT (1"), ex("")],
        [ex("SELECT 1; SELECT 2"), ex("SELECT 1;"), ex("  -- only a comment")],
        [ex("SELECT ?"), ex("SELECT 1", i(1)), ex("SELECT ?, ?", i(1))],
        [ex("SELECT :a, $b, @c", named={"a": i(1), "b": t("x"), "c": NULL}), ex("SELECT ?2, ?1", i(1), i(2))],
        [ex("COMMIT"), ex("ROLLBACK"), ex("RELEASE nope")],
        [ex("SELECT * FROM p WHERE id = ?", t("1")), ex("SELECT length(?)", blob("A" * 4000))],
    ],
    "runtime batches": [
        [d1_batch(ex("INSERT INTO p(id, name) VALUES (10, 'ten')"), ex("INSERT INTO c(p_id, email) VALUES (10, 'ten@x')"))],
        [d1_batch(ex("INSERT INTO p(id, name) VALUES (11, 'eleven')"), ex("INSERT INTO c(p_id, email) VALUES (11, 'ten@x')"))],
        [ex("SELECT count(*) FROM p WHERE id = 11")],
        [d1_batch(ex("SELECT * FROM p WHERE id = 10"), ex("SELECT count(*) FROM c"), begin="BEGIN")],
        [d1_batch(ex("INSERT INTO p(id, name) VALUES (12, 'x')"), ex("SELEC 1"))],
        [d1_batch(ex("INSERT INTO p(id, name) VALUES (13, 'x')"), ex("SELECT 1; SELECT 2"))],
        [{"type": "batch", "batch": {"steps": [
            {"stmt": {"sql": "SELECT 1"}},
            {"condition": {"type": "error", "step": 0}, "stmt": {"sql": "SELECT 2"}},
            {"condition": {"type": "and", "conds": [{"type": "ok", "step": 0}, {"type": "not", "cond": {"type": "ok", "step": 1}}]}, "stmt": {"sql": "SELECT 3"}},
            {"condition": {"type": "or", "conds": [{"type": "ok", "step": 1}, {"type": "ok", "step": 2}]}, "stmt": {"sql": "SELECT 4"}},
        ]}}],
        [{"type": "batch", "batch": {"steps": []}}],
    ],
    "coalesced executes (session queue)": [
        [ex("SELECT id FROM p WHERE id = 1"), ex("INSERT INTO c(p_id, email) VALUES (1, 'q@x')"), ex("INSERT INTO c(p_id, email) VALUES (1, 'q@x')"), ex("SELECT count(*) FROM c WHERE email = 'q@x'")],
    ],
    "watchdog and admin queries": [
        [ex("SELECT 1")],
        [ex("BEGIN IMMEDIATE"), ex("ROLLBACK")],
        [ex("SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name"), CLOSE],
    ],
    "large values": [
        [ex("INSERT INTO kv VALUES ('big', zeroblob(1000000))"), ex("SELECT length(v), hex(substr(v, 1, 4)) FROM kv WHERE k = 'big'")],
        [ex("SELECT v FROM kv WHERE k = 'big'")],
        [ex("SELECT zeroblob(6000000)")],
        [ex("SELECT ?", t("x" * 200000))],
    ],
    "pragmas": [
        [ex("PRAGMA foreign_keys"), ex("PRAGMA recursive_triggers"), ex("PRAGMA defer_foreign_keys"), ex("PRAGMA trusted_schema")],
        [ex("PRAGMA journal_mode"), ex("PRAGMA page_size"), ex("PRAGMA encoding"), ex("PRAGMA user_version")],
        [ex("PRAGMA table_info(c)"), ex("PRAGMA index_list(c)"), ex("PRAGMA foreign_key_list(c)"), ex("PRAGMA integrity_check")],
        [ex("SELECT name, pk FROM pragma_table_info('c') ORDER BY cid"), ex("PRAGMA threads"), ex("PRAGMA analysis_limit")],
        [ex("PRAGMA user_version = 7"), ex("PRAGMA user_version"), ex("PRAGMA defer_foreign_keys = ON"), ex("PRAGMA defer_foreign_keys")],
        [ex("PRAGMA query_only = 1")],
        [ex("PRAGMA writable_schema = ON")],
        [ex("PRAGMA recursive_triggers = ON")],
        [ex("PRAGMA optimize")],
        [ex("ANALYZE")],
        [ex("VACUUM")],
    ],
    "temporary objects": [
        [ex("CREATE TEMP TABLE tt(a)")],
        [ex("CREATE TEMPORARY VIEW tv AS SELECT 1")],
        [ex("CREATE TEMP TRIGGER ttr AFTER INSERT ON p BEGIN SELECT 1; END")],
        [ex("CREATE TABLE temp.tx(a)")],
        [ex("SELECT count(*) FROM sqlite_temp_master")],
    ],
    "savepoints": [
        [ex("SAVEPOINT a"), ex("INSERT INTO p(id, name) VALUES (30, 'sp')"), ex("SAVEPOINT b"), ex("INSERT INTO p(id, name) VALUES (31, 'sp')"),
         ex("ROLLBACK TO b"), ex("RELEASE a"), ex("SELECT group_concat(id) FROM p WHERE id >= 30")],
        [ex("BEGIN"), ex("SAVEPOINT s"), ex("INSERT INTO p(id, name) VALUES (32, 'sp')"), ex("ROLLBACK TRANSACTION t TO s"),
         ex("ROLLBACK TRANSACTION t TO nope"), ex("COMMIT"), ex("SELECT count(*) FROM p WHERE id = 32")],
        [ex("RELEASE nope"), ex("ROLLBACK TO nope")],
        ([ex("BEGIN"), ex("SAVEPOINT x"), ex("INSERT INTO p(id, name) VALUES (33, 'sp')")], None),
        ([ex("ROLLBACK TO x"), ex("INSERT INTO p(id, name) VALUES (34, 'sp')"), ex("COMMIT")], "keep"),
        [ex("SELECT group_concat(id) FROM p WHERE id IN (33, 34)")],
    ],
    "deferred foreign keys": [
        [ex("CREATE TABLE dp(id INTEGER PRIMARY KEY)"), ex("CREATE TABLE dc(id INTEGER PRIMARY KEY, p INTEGER REFERENCES dp(id) DEFERRABLE INITIALLY DEFERRED)")],
        [ex("INSERT INTO dc VALUES (1, 5)"), ex("INSERT INTO dp VALUES (5)")],
        [ex("BEGIN"), ex("INSERT INTO dc VALUES (2, 6)"), ex("COMMIT"), ex("INSERT INTO dp VALUES (6)"), ex("COMMIT")],
        [ex("BEGIN"), ex("INSERT INTO dc VALUES (3, 7)"), ex("INSERT INTO dp VALUES (7)"), ex("COMMIT")],
        [d1_batch(ex("INSERT INTO dc VALUES (4, 8)"))],
        [ex("INSERT INTO dc VALUES (5, 9) RETURNING id")],
        [ex("SELECT (SELECT group_concat(id) FROM dc), (SELECT group_concat(id) FROM dp)")],
    ],
    "smaller protocol details": [
        [ex(";BEGIN"), ex("INSERT INTO p(id, name) VALUES (40, 'x')"), ex("ROLLBACK"), ex("SELECT count(*) FROM p WHERE id = 40")],
        [{"type": "execute", "stmt": {"sql": "SELECT :a, :a", "named_args": [{"name": "a", "value": i(1)}, {"name": "a", "value": i(2)}]}}],
        [{"type": "execute", "stmt": {"sql": "SELECT :a", "named_args": [{"name": ":a", "value": i(1)}, {"name": "a", "value": i(2)}]}}],
        [ex("SELECT hex(?)", blob("AAE==")), ex("SELECT hex(?)", blob("="))],
        [ex("WITH replace AS (SELECT 1 AS x) SELECT x FROM replace")],
        [CLOSE, CLOSE],
    ],
    "response too large": [
        [ex("SELECT 1"), ex(BIG_RESULT), ex("SELECT 3")],
        [{"type": "batch", "batch": {"steps": [{"stmt": {"sql": "INSERT INTO p(id, name) VALUES (50, 'before')"}}, {"stmt": {"sql": BIG_RESULT}},
                                               {"stmt": {"sql": "INSERT INTO p(id, name) VALUES (51, 'after')"}}]}}, ex("SELECT 4")],
        [ex("SELECT group_concat(id) FROM p WHERE id IN (50, 51)")],
    ],
    "transaction across requests (baton)": [
        ([ex("BEGIN"), ex("INSERT INTO p(id, name) VALUES (20, 'twenty')")], None),
        ([ex("SELECT name FROM p WHERE id = 20")], "keep"),
        ([ex("COMMIT")], "keep"),
        ([ex("SELECT count(*) FROM p WHERE id = 20"), CLOSE], "keep"),
        ([ex("BEGIN"), ex("INSERT INTO p(id, name) VALUES (21, 'x')")], None),
        ([ex("ROLLBACK"), ex("SELECT count(*) FROM p WHERE id = 21")], "keep"),
        ([ex("BEGIN"), ex("INSERT INTO p(id, name) VALUES (22, 'x')"), CLOSE], None),
        ([ex("SELECT count(*) FROM p WHERE id = 22")], None),
    ],
}


def concurrent_cases(a, b, n=48):
    """Requests sent at the same time, each on its own rows: xsqld runs them in one group transaction (each in a
    savepoint), sqld one after the other; any order gives the same results. sqld's answers for contention (busy, or
    429 when it could not open a connection in time) are retried. Run last: sqld 0.24.32 can be left unable to write
    after such load."""
    import concurrent.futures

    setup = [ex("CREATE TABLE g(id INTEGER PRIMARY KEY, k TEXT UNIQUE, n INTEGER CHECK (n >= 0))"),
             ex("CREATE TABLE gp(id INTEGER PRIMARY KEY)"),
             ex("CREATE TABLE gc(id INTEGER PRIMARY KEY, p INTEGER REFERENCES gp(id) DEFERRABLE INITIALLY DEFERRED)"),
             ex("INSERT INTO g VALUES (0, 'taken', 0)")]

    def req_for(k):
        kind = k % 6
        if kind == 0:
            return [ex("INSERT INTO g VALUES (?, ?, 1)", i(k + 1), t(f"k{k}")), ex("SELECT n FROM g WHERE id = ?", i(k + 1))]
        if kind == 1:
            return [ex("INSERT INTO g VALUES (?, 'taken', 1)", i(k + 1))]
        if kind == 2:
            return [d1_batch(ex("INSERT INTO g VALUES (?, ?, 1)", i(k + 1), t(f"k{k}")), ex("INSERT INTO g VALUES (?, ?, -1)", i(k + 1000), t(f"m{k}")))]
        if kind == 3:
            return [ex("BEGIN"), ex("INSERT INTO g VALUES (?, ?, 2)", i(k + 1), t(f"k{k}")), ex("ROLLBACK"), ex("SELECT count(*) FROM g WHERE id = ?", i(k + 1))]
        if kind == 4:
            return [ex("INSERT INTO gc VALUES (?, ?)", i(k), i(k)), ex("INSERT INTO gp VALUES (?)", i(k))]
        return [ex("INSERT INTO g VALUES (?, ?, 3) RETURNING id, k, n", i(k + 1), t(f"k{k}")), ex("SELECT changes()")]

    def send(side, requests):
        for _ in range(100):
            status, data = side.pipeline(requests, None)
            if b"SQLITE_BUSY" not in data and status != 429:
                break
            time.sleep(0.05)
        return normalize(status, data)

    out = {}
    for s in (a, b):
        s.ns_saved = s.ns
        s.ns = s.ns + "-group"
        st, body = request(s.admin, "POST", f"/v1/namespaces/{s.ns}/create", {})
        assert st == 200, body
        s.pipeline(setup, None)
    results = []
    for s in (a, b):
        with concurrent.futures.ThreadPoolExecutor(max_workers=16) as pool:
            results.append(list(pool.map(lambda k: send(s, req_for(k)), range(n))))
    for k in range(n):
        if results[0][k] != results[1][k]:
            out[f"concurrent request {k}"] = (results[0][k], results[1][k])
    rows = [ex("SELECT * FROM g ORDER BY id"), ex("SELECT * FROM gc ORDER BY id"), ex("SELECT * FROM gp ORDER BY id")]
    final = [send(s, rows) for s in (a, b)]
    if final[0] != final[1]:
        out["concurrent: final rows"] = tuple(final)
    for s in (a, b):
        s.ns = s.ns_saved
    return out


def run_case(a, b, steps):
    diffs = []
    a.baton = b.baton = None
    for n, step in enumerate(steps):
        requests, baton = step if isinstance(step, tuple) else (step, None)
        ra = normalize(*a.pipeline(requests, baton))
        rb = normalize(*b.pipeline(requests, baton))
        if ra != rb:
            diffs.append((n, ra, rb))
    return diffs


def protocol_cases(a, b):
    """Stream and HTTP-level behaviour"""
    out = {}

    def both(fn):
        return [fn(s) for s in (a, b)]

    def baton_reuse(s):
        s.baton = None
        s.pipeline([ex("SELECT 1")], None)
        b1 = s.baton
        s.pipeline([ex("SELECT 1")], "keep")
        return [normalize(*s.pipeline([ex("SELECT 1")], b1)), normalize(*s.pipeline([ex("SELECT 1")], "AAAA")),
                normalize(*s.pipeline([ex("SELECT 1"), CLOSE], "keep")), normalize(*s.pipeline([ex("SELECT 1")], "keep"))]

    out["baton reuse / invalid / close"] = both(baton_reuse)
    out["close then execute"] = both(lambda s: normalize(*s.pipeline([CLOSE, ex("SELECT 1")], None)))
    out["empty requests"] = both(lambda s: normalize(*s.pipeline([], None)))
    out["bad json"] = both(lambda s: request(s.addr, "POST", "/v2/pipeline", "not json", host=f"{s.ns}.db")[0])
    out["unknown request type"] = both(lambda s: request(s.addr, "POST", "/v2/pipeline", {"requests": [{"type": "describe"}]}, host=f"{s.ns}.db")[0])
    out["missing namespace"] = both(lambda s: normalize(*request(s.addr, "POST", "/v2/pipeline", {"requests": []}, host="nope-e415.db")))
    out["health"] = both(lambda s: request(s.addr, "GET", "/health")[0])
    out["create existing"] = both(lambda s: normalize(*request(s.admin, "POST", f"/v1/namespaces/{s.ns}/create", {})))
    out["delete missing"] = both(lambda s: normalize(*request(s.admin, "DELETE", "/v1/namespaces/nope-e415")))
    out["create without json"] = both(lambda s: request(s.admin, "POST", "/v1/namespaces/x-e415/create", "{}", ctype="text/plain")[0])
    out["dump url scheme"] = both(lambda s: request(s.admin, "POST", "/v1/namespaces/x-e415/create", {"dump_url": "ftp://x"})[0])
    return {k: v for k, v in out.items() if v[0] != v[1]}


def serve_file(path):
    data = open(path, "rb").read()

    class H(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200)
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def log_message(self, *args):
            pass

    srv = http.server.HTTPServer(("0.0.0.0", 0), H)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    return srv, srv.server_address[1]


def dump_cases(a, b, sqld_dump_host):
    """GET /dump of the same data, and building a database from a dump_url"""
    src = open(os.path.join(HERE, "fixtures", "dump-source.sql"), encoding="utf-8").read()
    stmts, cur = [], ""
    for line in src.splitlines(keepends=True):
        cur += line
        if sqlite3.complete_statement(cur):
            stmts.append(cur.strip())
            cur = ""
    result = {}
    dumps = []
    for s in (a, b):
        ns = s.ns + "-dump"
        request(s.admin, "POST", f"/v1/namespaces/{ns}/create", {})
        status, body = request(s.addr, "POST", "/v2/pipeline", {"requests": [ex(x) for x in stmts]}, host=f"{ns}.db")
        assert status == 200 and all(r["type"] == "ok" for r in json.loads(body)["results"]), body[:300]
        dumps.append(request(s.addr, "GET", "/dump", host=f"{ns}.db")[1])
    result["dump bytes identical"] = dumps[0] == dumps[1]
    with_rowids = [request(s.addr, "GET", "/dump?preserve_row_ids=true", host=f"{s.ns}-dump.db")[1] for s in (a, b)]
    result["dump with preserve_row_ids bytes identical"] = with_rowids[0] == with_rowids[1]
    # a dump without the multi-line trigger (sqld's loader splits statements at every line ending in ';')
    simple = os.path.join(HERE, "fixtures", "_simple-dump.sql")
    with open(simple, "wb") as fh:
        fh.write(dumps[0].split(b"CREATE TRIGGER multi")[0] + b"COMMIT;\n")
    restored = []
    for path, tag in ((simple, "simple"), (os.path.join(HERE, "fixtures", "sqld-0.24.32-dump.sql"), "multiline-trigger")):
        srv, port = serve_file(path)
        row = []
        for s, host in ((a, sqld_dump_host), (b, "127.0.0.1")):
            ns = f"{s.ns}-load-{tag}"
            url = f"http://{host}:{port}/dump/one-time-token"
            status, body = request(s.admin, "POST", f"/v1/namespaces/{ns}/create", {"dump_url": url})
            d = request(s.addr, "GET", "/dump", host=f"{ns}.db")[1] if status == 200 else b""
            row.append((status, body.decode()[:160], d))
        srv.shutdown()
        restored.append((tag, row))
    for tag, ((sa, ma, da), (sb, mb, db)) in restored:
        result[f"load {tag}: sqld"] = f"HTTP {sa} {ma}".strip()
        result[f"load {tag}: xsqld"] = f"HTTP {sb} {mb}".strip()
        # sqld fails on the multi-line trigger (README §4); xsqld must load both
        result[f"load {tag}: xsqld loaded it"] = sb == 200
        if sb == 200:
            src_dump = open(simple, "rb").read() if tag == "simple" else dumps[1]
            result[f"load {tag}: xsqld dump of the loaded database equals the source"] = db == src_dump
        if sa == 200 and sb == 200:
            result[f"load {tag}: both loaded databases dump identically"] = da == db
    os.remove(simple)
    return result


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sqld", default="127.0.0.1:18080")
    ap.add_argument("--sqld-admin", default="127.0.0.1:18081")
    ap.add_argument("--xsqld", default="127.0.0.1:28080")
    ap.add_argument("--xsqld-admin", default="127.0.0.1:28081")
    ap.add_argument("--sqld-dump-host", default="host.docker.internal", help="how sqld (in docker) reaches this host")
    o = ap.parse_args()
    ns = f"cmp{int(time.time())}"
    a = Side("sqld", o.sqld, o.sqld_admin, ns)
    b = Side("xsqld", o.xsqld, o.xsqld_admin, ns)
    for s in (a, b):
        status, body = request(s.admin, "POST", f"/v1/namespaces/{ns}/create", {})
        assert status == 200, (s.name, status, body)
    failed = 0
    total = 0
    for name, steps in CASES.items():
        diffs = run_case(a, b, steps)
        total += len(steps)
        if diffs:
            failed += 1
            print(f"DIFF  {name}")
            for n, ra, rb in diffs:
                print(f"   step {n}\n     sqld : {json.dumps(ra, ensure_ascii=False)[:1500]}\n     xsqld: {json.dumps(rb, ensure_ascii=False)[:1500]}")
        else:
            print(f"same  {name} ({len(steps)} requests)")
    proto = protocol_cases(a, b)
    for k, (va, vb) in proto.items():
        failed += 1
        print(f"DIFF  protocol: {k}\n     sqld : {va}\n     xsqld: {vb}")
    if not proto:
        print("same  protocol: batons, close, bad requests, missing namespace, admin errors")
    for k, v in dump_cases(a, b, o.sqld_dump_host).items():
        print(f"dump  {k}: {v}")
        if v is False:
            failed += 1
    grouped = concurrent_cases(a, b)
    for k, (va, vb) in grouped.items():
        failed += 1
        print(f"DIFF  {k}\n     sqld : {json.dumps(va, ensure_ascii=False)[:1500]}\n     xsqld: {json.dumps(vb, ensure_ascii=False)[:1500]}")
    if not grouped:
        print("same  48 requests sent at the same time, and the rows they left")
    print(f"\n{total} pipeline requests compared; {'all identical' if failed == 0 else f'{failed} groups differ'}")
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
