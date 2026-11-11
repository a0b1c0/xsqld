CREATE TABLE a(id INTEGER PRIMARY KEY AUTOINCREMENT, t TEXT, r REAL, b BLOB, n);
CREATE TABLE w(k TEXT PRIMARY KEY, v) WITHOUT ROWID;
CREATE INDEX a_t ON a(t);
CREATE VIEW v AS SELECT id, t FROM a;
CREATE TRIGGER tr AFTER INSERT ON a BEGIN UPDATE a SET n = coalesce(n, 0) WHERE id = new.id; END;
INSERT INTO a(t, r, b, n) VALUES ('it''s', 1.0, x'00ff', NULL), ('line' || char(10) || 'break' || char(13) || char(9) || 'tab', 1e300, x'', 9223372036854775807), ('', -0.0, NULL, -9223372036854775808), ('ünï ☎', 0.1, x'deadbeef', 3.5), (NULL, 123456789.125, zeroblob(3), 'text');
INSERT INTO w VALUES ('k1', 1), ('k2', NULL);
DELETE FROM a WHERE id = 5;
CREATE TABLE e(x);
CREATE TABLE "sp ace"("a b" TEXT, c);
INSERT INTO "sp ace" VALUES (1, 2);
CREATE TABLE "select"("from");
INSERT INTO "select" VALUES (1);
INSERT INTO a(t, r) VALUES ('nan?', 1e-7), ('big', 1.7976931348623157e308), ('small', 5e-324), ('neg', -2.5), ('int-ish', 100.0);
CREATE TABLE ipk_desc(id INTEGER PRIMARY KEY DESC, v);
INSERT INTO ipk_desc VALUES (5, 'x');
CREATE TABLE fk_parent(id INTEGER PRIMARY KEY);
CREATE TABLE fk_child(p INTEGER REFERENCES fk_parent(id));
INSERT INTO fk_parent VALUES (1);
INSERT INTO fk_child VALUES (1);
CREATE TRIGGER multi BEFORE INSERT ON e
  WHEN NEW.x IS NULL
BEGIN
  SELECT RAISE(ABORT, 'no;');
END;
