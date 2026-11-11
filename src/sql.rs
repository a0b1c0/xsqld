//! Light-weight look at SQL text: what kind of statement it starts with. Only the leading keywords are read
//! (comments and whitespace skipped); everything else is left to SQLite.

use crate::proto;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Begin {
    Deferred,
    Immediate,
    Exclusive,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    /// Only whitespace, comments and semicolons
    Empty,
    Begin(Begin),
    Commit,
    Rollback,
    RollbackTo,
    Savepoint,
    Release,
    Pragma,
    Attach,
    Detach,
    Vacuum,
    Insert,
    Update,
    Delete,
    Select,
    Other,
}

impl Kind {
    /// sqld reports `affected_row_count` only for these (sqlite3_changes() is stale after other statements)
    pub fn is_iud(self) -> bool {
        matches!(self, Kind::Insert | Kind::Update | Kind::Delete)
    }
    pub fn is_insert(self) -> bool {
        self == Kind::Insert
    }
    /// Statements that must not run inside the group transaction (they change connection state, manage
    /// transactions by name or cannot run inside a transaction): such requests run on their own
    pub fn needs_solo(self) -> bool {
        matches!(
            self,
            Kind::RollbackTo
                | Kind::Savepoint
                | Kind::Release
                | Kind::Pragma
                | Kind::Attach
                | Kind::Detach
                | Kind::Vacuum
        )
    }
}

struct Lexer<'a> {
    s: &'a [u8],
    i: usize,
}

/// UTF-8 byte order mark: whitespace for SQLite's tokenizer wherever a token may start. Skipping it in more places
/// than SQLite does (right behind a keyword, where SQLite reads it as part of a name) is harmless: such text fails
/// to compile before the classification is used.
const BOM: &[u8] = b"\xEF\xBB\xBF";

impl<'a> Lexer<'a> {
    /// The text is trimmed like [`trimmed`] does for the statement SQLite compiles
    fn new(sql: &'a str) -> Self {
        Self {
            s: trimmed(sql).as_bytes(),
            i: 0,
        }
    }

    /// Rest of the text; empty once the position has run past the end
    fn rest(&self) -> &'a [u8] {
        self.s.get(self.i..).unwrap_or_default()
    }

    /// Everything SQLite may skip before a keyword has to be skipped here too: a transaction keyword taken for an
    /// ordinary statement would run as it stands inside the group transaction
    fn skip_space(&mut self) {
        loop {
            // 0x0B (vertical tab) is whitespace for SQLite after other whitespace; `is_ascii_whitespace` leaves it out
            while self.i < self.s.len()
                && (self.s[self.i].is_ascii_whitespace() || self.s[self.i] == 0x0B || self.s[self.i] == b';')
            {
                self.i += 1;
            }
            let rest = self.rest();
            if rest.starts_with(BOM) {
                self.i += BOM.len();
            } else if rest.starts_with(b"--") {
                while self.i < self.s.len() && self.s[self.i] != b'\n' {
                    self.i += 1;
                }
            } else if rest.starts_with(b"/*") {
                match rest[2..].windows(2).position(|w| w == b"*/") {
                    Some(p) => self.i += p + 4,
                    None => self.i = self.s.len(),
                }
            } else {
                return;
            }
        }
    }

    /// Next token at the current position: an identifier/keyword (uppercased), or None for anything else.
    /// Quoted strings and identifiers are skipped as a whole; `(`/`)` are returned as markers.
    fn next(&mut self) -> Option<Tok> {
        self.skip_space();
        let c = *self.s.get(self.i)?;
        if c.is_ascii_alphabetic() || c == b'_' {
            let start = self.i;
            while self.i < self.s.len() && (self.s[self.i].is_ascii_alphanumeric() || self.s[self.i] == b'_') {
                self.i += 1;
            }
            return Some(Tok::Word(
                String::from_utf8_lossy(&self.s[start..self.i]).to_ascii_uppercase(),
            ));
        }
        self.i += 1;
        match c {
            b'\'' | b'"' | b'`' => {
                while self.i < self.s.len() {
                    if self.s[self.i] == c {
                        if self.s.get(self.i + 1) == Some(&c) {
                            self.i += 2;
                            continue;
                        }
                        self.i += 1;
                        break;
                    }
                    self.i += 1;
                }
                Some(Tok::Other)
            }
            b'[' => {
                while self.i < self.s.len() && self.s[self.i] != b']' {
                    self.i += 1;
                }
                // past the `]`; an unclosed `[` ends at the end of the text
                self.i = (self.i + 1).min(self.s.len());
                Some(Tok::Other)
            }
            b'(' => Some(Tok::Open),
            b')' => Some(Tok::Close),
            _ => Some(Tok::Other),
        }
    }
}

#[derive(Debug, PartialEq)]
enum Tok {
    Word(String),
    Open,
    Close,
    Other,
}

/// The statement text as it is handed to SQLite: without the Unicode whitespace around it (U+00A0, U+3000, …, which
/// SQLite's tokenizer does not take for whitespace). The statement cache of rusqlite trims its key the same way;
/// trimming here keeps cached and uncached statements, and the classification below, on the same text.
pub fn trimmed(sql: &str) -> &str {
    sql.trim()
}

/// Tests only: text starting with this is classified as an ordinary statement whatever follows, which stands for a
/// spelling of a transaction keyword the classification does not know
#[cfg(test)]
pub const UNCLASSIFIED: &str = "/*unclassified*/";

pub fn classify(sql: &str) -> Kind {
    #[cfg(test)]
    if sql.starts_with(UNCLASSIFIED) {
        return Kind::Other;
    }
    let mut lx = Lexer::new(sql);
    let Some(first) = lx.next() else { return Kind::Empty };
    let Tok::Word(w) = first else { return Kind::Other };
    match w.as_str() {
        "BEGIN" => Kind::Begin(match lx.next() {
            Some(Tok::Word(m)) if m == "IMMEDIATE" => Begin::Immediate,
            Some(Tok::Word(m)) if m == "EXCLUSIVE" => Begin::Exclusive,
            _ => Begin::Deferred,
        }),
        "COMMIT" | "END" => Kind::Commit,
        "ROLLBACK" => {
            let mut t = lx.next();
            if matches!(&t, Some(Tok::Word(m)) if m == "TRANSACTION") {
                t = lx.next();
            }
            if matches!(&t, Some(Tok::Word(m)) if m == "TO") {
                Kind::RollbackTo
            } else {
                Kind::Rollback
            }
        }
        "SAVEPOINT" => Kind::Savepoint,
        "RELEASE" => Kind::Release,
        "PRAGMA" => Kind::Pragma,
        "ATTACH" => Kind::Attach,
        "DETACH" => Kind::Detach,
        "VACUUM" => Kind::Vacuum,
        "INSERT" | "REPLACE" => Kind::Insert,
        "UPDATE" => Kind::Update,
        "DELETE" => Kind::Delete,
        "SELECT" | "VALUES" => Kind::Select,
        "WITH" => with_body(&mut lx),
        _ => Kind::Other,
    }
}

/// `WITH … AS (…) <main statement>`: the first statement keyword outside parentheses
fn with_body(lx: &mut Lexer) -> Kind {
    let mut depth = 0usize;
    while let Some(t) = lx.next() {
        match t {
            Tok::Open => depth += 1,
            Tok::Close => depth = depth.saturating_sub(1),
            Tok::Word(w) if depth == 0 => match w.as_str() {
                "SELECT" | "VALUES" => return Kind::Select,
                "INSERT" | "REPLACE" => return Kind::Insert,
                "UPDATE" => return Kind::Update,
                "DELETE" => return Kind::Delete,
                _ => {}
            },
            _ => {}
        }
    }
    Kind::Other
}

/// The first `n` keywords and bare names of a statement, uppercased; quoted names, literals and punctuation are
/// skipped (`PRAGMA foreign_keys=OFF` → `PRAGMA`, `FOREIGN_KEYS`, `OFF`)
pub fn leading_words(sql: &str, n: usize) -> Vec<String> {
    let mut lx = Lexer::new(sql);
    let mut out = Vec::with_capacity(n);
    while out.len() < n {
        match lx.next() {
            Some(Tok::Word(w)) => out.push(w),
            Some(_) => {}
            None => break,
        }
    }
    out
}

/// More than one statement in the text (sqld refuses these before running anything): the first `;` that ends a
/// statement is followed by something other than whitespace, comments and semicolons. Where a statement ends is
/// decided the way `sqlite3_complete` decides it (a `;` inside `CREATE TRIGGER … BEGIN … END` does not end one), with
/// its state machine run once over the text: asking `sqlite3_complete` at every `;` reads the text from the start each
/// time, seconds of CPU for a trigger body of a few hundred thousand semicolons.
pub fn many_statements(sql: &str) -> bool {
    // tokens of sqlite3_complete
    const SEMI: usize = 0;
    const WS: usize = 1;
    const OTHER: usize = 2;
    const EXPLAIN: usize = 3;
    const CREATE: usize = 4;
    const TEMP: usize = 5;
    const TRIGGER: usize = 6;
    const END: usize = 7;
    // its states: 0 nothing seen, 1 a statement just ended, 2 inside a statement, 3 after EXPLAIN, 4 after CREATE
    // [TEMP], 5 inside a trigger, 6 after `;` inside a trigger, 7 after `; END` inside a trigger
    const NEXT: [[u8; 8]; 8] = [
        [1, 0, 2, 3, 4, 2, 2, 2],
        [1, 1, 2, 3, 4, 2, 2, 2],
        [1, 2, 2, 2, 2, 2, 2, 2],
        [1, 3, 3, 2, 4, 2, 2, 2],
        [1, 4, 2, 2, 2, 4, 5, 2],
        [6, 5, 5, 5, 5, 5, 5, 5],
        [6, 6, 5, 5, 5, 5, 5, 7],
        [1, 7, 5, 5, 5, 5, 5, 5],
    ];
    let id_char = |c: u8| c.is_ascii_alphanumeric() || c == b'_' || c == b'$' || c >= 0x80;
    // SQLite reads the text up to the first NUL: no statement ends after it
    let all = sql.as_bytes();
    let s = &all[..all.iter().position(|&c| c == 0).unwrap_or(all.len())];
    let (mut state, mut i) = (0usize, 0usize);
    while i < s.len() {
        let token = match s[i] {
            b';' => {
                i += 1;
                SEMI
            }
            b' ' | b'\r' | b'\t' | b'\n' | 0x0C => {
                i += 1;
                WS
            }
            b'/' if s.get(i + 1) == Some(&b'*') => match s[i + 2..].windows(2).position(|w| w == b"*/") {
                Some(p) => {
                    i += p + 4;
                    WS
                }
                None => return false,
            },
            b'-' if s.get(i + 1) == Some(&b'-') => {
                while i < s.len() && s[i] != b'\n' {
                    i += 1;
                }
                WS
            }
            // a quoted text ends at the next quote character (a doubled quote reads as two texts: the same ends)
            c @ (b'[' | b'\'' | b'"' | b'`') => {
                let close = if c == b'[' { b']' } else { c };
                match s[i + 1..].iter().position(|&b| b == close) {
                    Some(p) => {
                        i += p + 2;
                        OTHER
                    }
                    None => return false,
                }
            }
            c if id_char(c) => {
                let start = i;
                while i < s.len() && id_char(s[i]) {
                    i += 1;
                }
                let word = &s[start..i];
                let is = |kw: &str| word.eq_ignore_ascii_case(kw.as_bytes());
                if is("create") {
                    CREATE
                } else if is("trigger") {
                    TRIGGER
                } else if is("temp") || is("temporary") {
                    TEMP
                } else if is("end") {
                    END
                } else if is("explain") {
                    EXPLAIN
                } else {
                    OTHER
                }
            }
            _ => {
                i += 1;
                OTHER
            }
        };
        state = NEXT[state][token] as usize;
        if token == SEMI && state == 1 {
            return classify(&sql[i..]) != Kind::Empty;
        }
    }
    false
}

/// Error of preparing a statement: syntax problems become sqld's `SQL_PARSE_ERROR` (sqld finds them with its own
/// parser, so its message text differs: `SQL string could not be parsed: syntax error around L1:6: …`). Statements the
/// connection's authorizer refuses get sqld's text for the same statements, which its parser does not support.
pub fn prepare_error(e: &rusqlite::Error, sql: &str) -> proto::Error {
    if let rusqlite::Error::SqliteFailure(err, _) = e
        && err.extended_code == rusqlite::ffi::SQLITE_AUTH
    {
        return proto::Error::new(
            format!("SQL string could not be parsed: unsupported statement: {sql}"),
            "SQL_PARSE_ERROR",
        );
    }
    let msg = match e {
        rusqlite::Error::SqliteFailure(err, Some(m)) | rusqlite::Error::SqlInputError { error: err, msg: m, .. }
            if err.extended_code == rusqlite::ffi::SQLITE_ERROR =>
        {
            m
        }
        _ => return rusqlite_error(e),
    };
    if msg.ends_with("syntax error") || msg.starts_with("incomplete input") || msg.starts_with("unrecognized token") {
        proto::Error::new(format!("SQL string could not be parsed: {msg}"), "SQL_PARSE_ERROR")
    } else {
        rusqlite_error(e)
    }
}

/// Error code names exactly as sqld 0.24.32 reports them (`hrana/stmt.rs` `sqlite_error_code`):
/// the primary result code mapped through rusqlite's ErrorCode, so plain SQLITE_ERROR is "SQLITE_UNKNOWN"
pub fn sqlite_code_name(primary: i32) -> &'static str {
    use rusqlite::ffi;
    match primary {
        ffi::SQLITE_INTERNAL => "SQLITE_INTERNAL",
        ffi::SQLITE_PERM => "SQLITE_PERM",
        ffi::SQLITE_ABORT => "SQLITE_ABORT",
        ffi::SQLITE_BUSY => "SQLITE_BUSY",
        ffi::SQLITE_LOCKED => "SQLITE_LOCKED",
        ffi::SQLITE_NOMEM => "SQLITE_NOMEM",
        ffi::SQLITE_READONLY => "SQLITE_READONLY",
        ffi::SQLITE_INTERRUPT => "SQLITE_INTERRUPT",
        ffi::SQLITE_IOERR => "SQLITE_IOERR",
        ffi::SQLITE_CORRUPT => "SQLITE_CORRUPT",
        ffi::SQLITE_NOTFOUND => "SQLITE_NOTFOUND",
        ffi::SQLITE_FULL => "SQLITE_FULL",
        ffi::SQLITE_CANTOPEN => "SQLITE_CANTOPEN",
        ffi::SQLITE_PROTOCOL => "SQLITE_PROTOCOL",
        ffi::SQLITE_SCHEMA => "SQLITE_SCHEMA",
        ffi::SQLITE_TOOBIG => "SQLITE_TOOBIG",
        ffi::SQLITE_CONSTRAINT => "SQLITE_CONSTRAINT",
        ffi::SQLITE_MISMATCH => "SQLITE_MISMATCH",
        ffi::SQLITE_MISUSE => "SQLITE_MISUSE",
        ffi::SQLITE_NOLFS => "SQLITE_NOLFS",
        ffi::SQLITE_AUTH => "SQLITE_AUTH",
        ffi::SQLITE_RANGE => "SQLITE_RANGE",
        ffi::SQLITE_NOTADB => "SQLITE_NOTADB",
        _ => "SQLITE_UNKNOWN",
    }
}

/// An SQLite failure as a Hrana error: message `SQLite error: <sqlite3_errmsg>` (the original SQLite text after
/// sqld's fixed prefix; clients match on e.g. `UNIQUE constraint failed: t.c`), code per [`sqlite_code_name`]
pub fn sqlite_error(primary: i32, message: &str) -> proto::Error {
    proto::Error::new(format!("SQLite error: {message}"), sqlite_code_name(primary & 0xff))
}

/// rusqlite error → Hrana error (sqld formats prepare failures the same way as step failures; a prepare failure
/// SQLite can point at, e.g. `no such column`, carries its byte offset)
pub fn rusqlite_error(e: &rusqlite::Error) -> proto::Error {
    match e {
        rusqlite::Error::SqliteFailure(err, Some(msg)) => sqlite_error(err.extended_code, msg),
        rusqlite::Error::SqliteFailure(err, None) => sqlite_error(err.extended_code, &err.to_string()),
        rusqlite::Error::SqlInputError { msg, offset, .. } => proto::Error::new(
            format!("SQL input error: {msg} (at offset {offset})"),
            "SQL_INPUT_ERROR",
        ),
        rusqlite::Error::MultipleStatement => {
            proto::Error::new("SQL string contains more than one statement", "SQL_MANY_STATEMENTS")
        }
        other => proto::Error::new(format!("SQLite error: {other}"), "SQLITE_UNKNOWN"),
    }
}

/// First characters of an SQL text for logs (never the bound values)
pub fn head(sql: &str) -> String {
    let mut out = String::with_capacity(80);
    let mut space = false;
    for c in sql.chars() {
        if c.is_whitespace() {
            space = !out.is_empty();
            continue;
        }
        if space {
            out.push(' ');
            space = false;
        }
        out.push(c);
        if out.len() >= 80 {
            out.push('…');
            break;
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn kinds() {
        assert_eq!(classify("  -- c\n /* x */ select 1"), Kind::Select);
        assert_eq!(classify("BEGIN"), Kind::Begin(Begin::Deferred));
        assert_eq!(classify("begin immediate"), Kind::Begin(Begin::Immediate));
        assert_eq!(classify("BEGIN EXCLUSIVE TRANSACTION"), Kind::Begin(Begin::Exclusive));
        assert_eq!(classify("BEGIN TRANSACTION"), Kind::Begin(Begin::Deferred));
        assert_eq!(classify("commit"), Kind::Commit);
        assert_eq!(classify("END TRANSACTION"), Kind::Commit);
        assert_eq!(classify("ROLLBACK"), Kind::Rollback);
        assert_eq!(classify("ROLLBACK TRANSACTION"), Kind::Rollback);
        assert_eq!(classify("ROLLBACK TO sp"), Kind::RollbackTo);
        assert_eq!(classify("rollback transaction to savepoint sp"), Kind::RollbackTo);
        assert_eq!(classify("SAVEPOINT a"), Kind::Savepoint);
        assert_eq!(classify("PRAGMA defer_foreign_keys = ON"), Kind::Pragma);
        assert_eq!(classify("INSERT INTO t VALUES (1)"), Kind::Insert);
        assert_eq!(classify("REPLACE INTO t VALUES (1)"), Kind::Insert);
        assert_eq!(classify("UPDATE t SET a = 1"), Kind::Update);
        assert_eq!(classify("DELETE FROM t"), Kind::Delete);
        assert_eq!(classify("CREATE TABLE t(a)"), Kind::Other);
        assert_eq!(classify(" ; ;-- only comment"), Kind::Empty);
        assert_eq!(classify(""), Kind::Empty);
    }

    #[test]
    fn with_statements() {
        assert_eq!(classify("WITH x AS (SELECT 1) SELECT * FROM x"), Kind::Select);
        assert_eq!(
            classify("WITH RECURSIVE x(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM x) INSERT INTO t SELECT n FROM x"),
            Kind::Insert
        );
        assert_eq!(
            classify("with a as (delete from t returning *) , \"select\" as (select 1) update t set a=1"),
            Kind::Update
        );
        assert_eq!(
            classify("WITH x AS MATERIALIZED (SELECT ')') DELETE FROM t"),
            Kind::Delete
        );
    }

    #[test]
    fn routing_flags() {
        assert!(classify("PRAGMA foreign_keys").needs_solo());
        assert!(classify("VACUUM").needs_solo());
        assert!(!classify("ROLLBACK").needs_solo());
        assert!(classify("INSERT INTO t DEFAULT VALUES").is_iud());
        assert!(!classify("SELECT changes()").is_iud());
    }

    #[test]
    fn error_text() {
        let e = sqlite_error(rusqlite::ffi::SQLITE_CONSTRAINT_UNIQUE, "UNIQUE constraint failed: t.a");
        assert_eq!(e.message, "SQLite error: UNIQUE constraint failed: t.a");
        assert_eq!(e.code, "SQLITE_CONSTRAINT");
        assert_eq!(sqlite_error(1, "no such table: x").code, "SQLITE_UNKNOWN");
    }

    #[test]
    fn statement_count() {
        assert!(!many_statements("SELECT 1"));
        assert!(!many_statements("SELECT 1; -- done\n ;"));
        assert!(!many_statements("SELECT ';', \"a;b\", [x;y] /* ; */ -- ;\n"));
        assert!(many_statements("SELECT 1; SELECT 2"));
        assert!(!many_statements(
            "CREATE TRIGGER tr AFTER INSERT ON t BEGIN UPDATE t SET a = 1; DELETE FROM t WHERE 0; END;"
        ));
        assert!(many_statements(
            "CREATE TRIGGER tr AFTER INSERT ON t BEGIN SELECT 1; END; SELECT 2"
        ));
        assert!(!many_statements("SELECT 'it''s; fine'"));
    }

    #[test]
    fn parse_errors() {
        let c = rusqlite::Connection::open_in_memory().unwrap();
        let e = |s: &str| prepare_error(&c.prepare(s).unwrap_err(), s);
        assert_eq!(e("SELEC 1").code, "SQL_PARSE_ERROR");
        assert_eq!(e("SELECT (1").code, "SQL_PARSE_ERROR");
        assert_eq!(
            e("SELECT * FROM nope"),
            proto::Error::new("SQLite error: no such table: nope", "SQLITE_UNKNOWN")
        );
    }

    #[test]
    fn unclosed_brackets_and_quotes_stay_inside_the_text() {
        // review 2318 M1: the tail after `;` was classified with the position past the end
        assert!(many_statements("INSERT INTO t(b) VALUES ('x'); WITH [x"));
        assert_eq!(classify("WITH [x"), Kind::Other);
        assert_eq!(classify("["), Kind::Other);
        assert_eq!(classify("SELECT [a"), Kind::Select);
        assert!(!many_statements("SELECT [a; b"));
        assert_eq!(leading_words("WITH [x", 3), ["WITH"]);
        assert_eq!(
            leading_words("PRAGMA foreign_keys=OFF;", 4),
            ["PRAGMA", "FOREIGN_KEYS", "OFF"]
        );
    }

    /// What SQLite skips before the first keyword is skipped by the classification too: taken for an ordinary
    /// statement, `COMMIT` would run as it stands inside the group transaction (review of 0.1.0, finding 1)
    #[test]
    fn whitespace_sqlite_skips_does_not_hide_transaction_keywords() {
        for prefix in [
            "\u{feff}",
            "\u{feff} ",
            " \u{feff}\n",
            "\u{a0}",
            "\u{3000}",
            "\u{2003}\u{a0} ",
            "\u{b}",
            " \u{b}",
            "\u{85}",
            "/* c */\u{feff}-- d\n",
        ] {
            assert_eq!(classify(&format!("{prefix}COMMIT")), Kind::Commit, "{prefix:?}");
            assert_eq!(classify(&format!("{prefix}END")), Kind::Commit, "{prefix:?}");
            assert_eq!(classify(&format!("{prefix}ROLLBACK")), Kind::Rollback, "{prefix:?}");
            assert_eq!(
                classify(&format!("{prefix}BEGIN IMMEDIATE")),
                Kind::Begin(Begin::Immediate),
                "{prefix:?}"
            );
            assert!(
                classify(&format!("{prefix}PRAGMA foreign_keys")).needs_solo(),
                "{prefix:?}"
            );
            assert!(classify(&format!("{prefix}SAVEPOINT a")).needs_solo(), "{prefix:?}");
            assert_eq!(classify(prefix), Kind::Empty, "{prefix:?}");
        }
        assert_eq!(classify("BEGIN\u{feff}EXCLUSIVE"), Kind::Begin(Begin::Exclusive));
        assert_eq!(classify("ROLLBACK\u{feff}TO a"), Kind::RollbackTo);
        assert_eq!(classify("COMMIT\u{3000}"), Kind::Commit);
        assert!(many_statements("SELECT 1;\u{feff}COMMIT"));
        assert!(!many_statements("SELECT 1;\u{feff}"));
        // SQLite agrees: these compile to the bare keyword
        let c = rusqlite::Connection::open_in_memory().unwrap();
        for sql in ["\u{feff}COMMIT", " \u{b}COMMIT", "\u{feff} \u{feff}COMMIT"] {
            c.execute_batch("BEGIN").unwrap();
            c.prepare(trimmed(sql)).unwrap().raw_execute().unwrap();
            assert!(c.is_autocommit(), "{sql:?}");
        }
        // Unicode whitespace is not whitespace for SQLite once the edges are trimmed: an error, not a COMMIT
        assert_eq!(classify("\u{feff}\u{a0}COMMIT"), Kind::Other);
        assert!(c.prepare(trimmed("\u{feff}\u{a0}COMMIT")).is_err());
    }

    /// The former implementation: `sqlite3_complete` asked at every `;` outside quotes and comments
    fn many_statements_by_complete(sql: &str) -> bool {
        fn complete(prefix: &[u8]) -> bool {
            match std::ffi::CString::new(prefix) {
                // SAFETY: a valid NUL-terminated string that outlives the call
                Ok(c) => unsafe { rusqlite::ffi::sqlite3_complete(c.as_ptr()) != 0 },
                Err(_) => false,
            }
        }
        let s = sql.as_bytes();
        let mut i = 0;
        while i < s.len() {
            match s[i] {
                q @ (b'\'' | b'"' | b'`') => {
                    i += 1;
                    while i < s.len() {
                        if s[i] == q {
                            if s.get(i + 1) == Some(&q) {
                                i += 1;
                            } else {
                                break;
                            }
                        }
                        i += 1;
                    }
                }
                b'[' => {
                    while i < s.len() && s[i] != b']' {
                        i += 1;
                    }
                }
                b'-' if s.get(i + 1) == Some(&b'-') => {
                    while i < s.len() && s[i] != b'\n' {
                        i += 1;
                    }
                }
                b'/' if s.get(i + 1) == Some(&b'*') => {
                    i = s[i + 2..]
                        .windows(2)
                        .position(|w| w == b"*/")
                        .map_or(s.len(), |p| i + 2 + p + 1);
                }
                b';' if complete(&s[..=i]) => return classify(&sql[i + 1..]) != Kind::Empty,
                _ => {}
            }
            i += 1;
        }
        false
    }

    /// One pass over the text gives the answers `sqlite3_complete` gave when asked at every `;`
    #[test]
    fn statement_count_agrees_with_sqlite3_complete() {
        const PIECES: &[&str] = &[
            "[",
            "]",
            "'",
            "\"",
            "`",
            ";",
            ";",
            "-",
            "--",
            "/",
            "*",
            "/*",
            "*/",
            "(",
            ")",
            " ",
            "\n",
            "\t",
            "\u{c}",
            "\u{b}",
            "x",
            "_1",
            "$",
            "é",
            "\u{feff}",
            "\0",
            "SELECT",
            "BEGIN",
            "CREATE",
            "create ",
            "TEMP",
            " temporary ",
            "TRIGGER",
            " trigger ",
            "END",
            " end",
            "EXPLAIN",
            "explain ",
            "''",
            "1",
        ];
        let mut seed = 0x2461_u64;
        let mut next = || {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed
        };
        for _ in 0..200_000 {
            let len = (next() % 16) as usize;
            let s: String = (0..len)
                .map(|_| PIECES[(next() % PIECES.len() as u64) as usize])
                .collect();
            assert_eq!(many_statements(&s), many_statements_by_complete(&s), "{s:?}");
        }
    }

    /// Review of 0.1.0, finding 4: a trigger body of semicolons took time quadratic in its length
    #[test]
    fn statement_count_reads_the_text_once() {
        let mut sql = String::from("CREATE TRIGGER tr AFTER INSERT ON t BEGIN ");
        sql.push_str(&";".repeat(4 * 1024 * 1024));
        let started = std::time::Instant::now();
        assert!(!many_statements(&sql));
        sql.push_str("END; SELECT 1");
        assert!(many_statements(&sql));
        assert!(
            started.elapsed() < std::time::Duration::from_secs(2),
            "{:?}",
            started.elapsed()
        );
    }

    /// Random SQL-ish text, including every prefix: the lexer must never index past the end or split a character
    #[test]
    fn random_text_never_panics() {
        const PIECES: &[&str] = &[
            "[", "]", "'", "\"", "`", ";", "-", "--", "/", "*", "/*", "*/", "(", ")", " ", "\n", "\t", "x", "_1", "é",
            "語", "WITH", "SELECT", "BEGIN", "ROLLBACK", "TO", "CREATE", "TRIGGER", "END", "INSERT", "''", "\u{feff}",
            "\u{a0}", "\u{b}",
        ];
        let mut seed = 0x2318_u64;
        let mut next = || {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed
        };
        for _ in 0..20_000 {
            let len = (next() % 24) as usize;
            let s: String = (0..len)
                .map(|_| PIECES[(next() % PIECES.len() as u64) as usize])
                .collect();
            for end in (0..=s.len()).filter(|&e| s.is_char_boundary(e)) {
                let p = &s[..end];
                let _ = classify(p);
                let _ = many_statements(p);
                let _ = leading_words(p, 4);
                let _ = head(p);
            }
        }
    }

    #[test]
    fn heads() {
        assert_eq!(head("  SELECT\n  *\tFROM   t "), "SELECT * FROM t");
        assert!(head(&"x".repeat(200)).chars().count() <= 82);
    }
}
