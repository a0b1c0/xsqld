//! Which statements split-range parallel aggregation takes, and the SQL it runs for them (README §3.1).
//!
//! Only one narrow shape is taken, read token by token with SQLite's own token rules ([`sql::tokenize`]):
//!
//! ```text
//! SELECT item, … FROM table [[AS] alias] [WHERE …] [GROUP BY column, …] [HAVING …] [ORDER BY …] [LIMIT …]
//! ```
//!
//! where every item is a grouping column or one of `COUNT(*)`, `COUNT(expr)`, `SUM(expr)`, `MIN(column)`,
//! `MAX(column)`. Anything else, or anything this reader is not sure about, is "not this shape" and runs as it
//! always did. What this module decides from the text alone is checked again by SQLite before anything runs
//! (`parallel.rs`: the table's kind and columns, the query plan, the compiled program of every statement built here).
//!
//! For a shape it builds three statements: the one run on each range of rowids (the partial aggregates of
//! `WHERE (…) AND rowid BETWEEN :xsqld_lo AND :xsqld_hi`, grouped as the original), the table those partial rows are
//! loaded into (an in-memory database, the same table name, the grouping columns with their declared types) and the
//! statement that combines them there: the original select list, `HAVING`, `ORDER BY` and `LIMIT`, with each
//! aggregate replaced by the aggregate of its partial values. SQLite runs that last statement, so grouping, `NULL`
//! keys, `HAVING`, ordering and `LIMIT` follow SQLite's own rules.

use crate::sql::{self, TokKind, Token};

/// Prefix of every name this module adds (hidden columns, range parameters); statements using such names are not
/// taken
pub const RESERVED: &str = "xsqld_";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggKind {
    Count,
    Sum,
    Min,
    Max,
}

/// One aggregate call of the statement, in the order of the hidden columns
#[derive(Debug)]
pub struct Agg {
    pub kind: AggKind,
    /// Argument as text (parameters renumbered); None for `COUNT(*)`
    arg: Option<String>,
    /// MIN / MAX: the column they take (lowercased, unquoted), checked against the schema
    pub column: Option<String>,
}

/// Where the hidden columns of a `SUM` are in a partial row: the sum, then the count, least and largest value of
/// `(arg)+0` (to prove the sum cannot overflow in any order, see `parallel.rs`)
#[derive(Debug, Clone, Copy)]
pub struct SumCols {
    pub sum: usize,
    pub count: usize,
    pub min: usize,
    pub max: usize,
}

/// A parameter of the original statement and the index SQLite gives it (checked against the compiled statement)
#[derive(Debug, PartialEq, Eq)]
pub struct Param {
    /// `?`, `?3`, `:name`, …
    pub text: String,
    pub index: usize,
    /// Byte offset of the token
    start: usize,
}

#[derive(Debug)]
pub struct Shape {
    /// Table name, unquoted (as SQLite looks it up)
    pub table: String,
    /// `table [AS alias]` as written
    from: String,
    /// Grouping columns: unquoted, lowercased
    pub keys: Vec<String>,
    /// `GROUP BY` terms as written
    group: String,
    pub aggs: Vec<Agg>,
    where_: Option<String>,
    /// Select list of the combining statement
    merge_items: String,
    having: Option<String>,
    order: Option<String>,
    limit: Option<String>,
    /// Aliases of the select list, lowercased (must not be column names, see `parallel.rs`)
    pub aliases: Vec<String>,
    /// Parameters in text order with the indexes SQLite gives them
    pub params: Vec<Param>,
    /// Largest parameter index
    pub param_count: usize,
}

/// Lowercased name of a name token (`"a""b"` → `a"b`, `[x]` → `x`)
fn name_of(sql: &str, t: &Token) -> Option<String> {
    let text = &sql[t.start..t.end];
    let n = match t.kind {
        TokKind::Word => text.to_owned(),
        TokKind::Quoted => {
            let (open, body) = text.split_at(1);
            let body = &body[..body.len() - 1];
            match open {
                "[" => body.to_owned(),
                q => body.replace(&format!("{q}{q}"), q),
            }
        }
        _ => return None,
    };
    Some(n.to_ascii_lowercase())
}

/// `name` quoted for SQL text
pub fn quote(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// Words that never start or end an expression in this shape; meeting one anywhere means "not this shape". Most are
/// constructs that read other tables or rows (subqueries, joins, window functions) or change the meaning of the
/// aggregates (`DISTINCT`, `FILTER`, `ORDER BY` inside an aggregate).
const REFUSED_WORDS: &[&str] = &[
    "CROSS",
    "CURRENT_DATE",
    "CURRENT_TIME",
    "CURRENT_TIMESTAMP",
    "DISTINCT",
    "EXCEPT",
    "EXISTS",
    "FILTER",
    "FULL",
    "INDEXED",
    "INNER",
    "INTERSECT",
    "JOIN",
    "LEFT",
    "MATCH",
    "NATURAL",
    "ON",
    "OUTER",
    "OVER",
    "RAISE",
    "RECURSIVE",
    "REGEXP",
    "RETURNING",
    "RIGHT",
    "UNION",
    "USING",
    "VALUES",
    "WINDOW",
    "WITH",
    "WITHIN",
];

/// Keywords `HAVING` and `ORDER BY` may use besides names (`TRUE` and `FALSE` are not among them: SQLite reads them as
/// a column where the table has one by that name)
const EXPRESSION_WORDS: &[&str] = &[
    "AND", "ASC", "BETWEEN", "CASE", "COLLATE", "DESC", "ELSE", "END", "ESCAPE", "FIRST", "GLOB", "IN", "IS", "ISNULL",
    "LAST", "LIKE", "NOT", "NOTNULL", "NULL", "NULLS", "OR", "THEN", "WHEN",
];

/// Scalar functions allowed in `HAVING` and `ORDER BY` (evaluated by the combining statement): deterministic built-ins.
/// `WHERE` and aggregate arguments run on the database itself; their functions are checked in the compiled program
/// (`parallel.rs`), against the same list.
pub const SCALAR_FUNCTIONS: &[&str] = &[
    "abs",
    "char",
    "coalesce",
    "concat",
    "concat_ws",
    "format",
    "glob",
    "hex",
    "ifnull",
    "iif",
    "instr",
    "length",
    "like",
    "likelihood",
    "likely",
    "lower",
    "ltrim",
    "max",
    "min",
    "nullif",
    "octet_length",
    "printf",
    "quote",
    "replace",
    "round",
    "rtrim",
    "sign",
    "substr",
    "substring",
    "trim",
    "typeof",
    "unhex",
    "unicode",
    "unlikely",
    "upper",
    "zeroblob",
];

struct Parser<'s> {
    sql: &'s str,
    t: Vec<Token>,
}

impl<'s> Parser<'s> {
    fn text(&self, i: usize) -> &'s str {
        &self.sql[self.t[i].start..self.t[i].end]
    }

    fn is_word(&self, i: usize, w: &str) -> bool {
        i < self.t.len() && self.t[i].kind == TokKind::Word && self.text(i).eq_ignore_ascii_case(w)
    }

    fn is_op(&self, i: usize, op: &str) -> bool {
        i < self.t.len() && self.t[i].kind == TokKind::Op && self.text(i) == op
    }

    /// Index of the `)` closing the `(` at `open`
    fn close_of(&self, open: usize) -> Option<usize> {
        let mut depth = 0usize;
        for i in open..self.t.len() {
            if self.is_op(i, "(") {
                depth += 1;
            } else if self.is_op(i, ")") {
                depth -= 1;
                if depth == 0 {
                    return Some(i);
                }
            }
        }
        None
    }

    /// Tokens `from..to` split at the commas outside parentheses
    fn split_commas(&self, from: usize, to: usize) -> Vec<(usize, usize)> {
        let mut out = Vec::new();
        let (mut depth, mut start) = (0usize, from);
        for i in from..to {
            if self.is_op(i, "(") {
                depth += 1;
            } else if self.is_op(i, ")") {
                depth = depth.saturating_sub(1);
            } else if depth == 0 && self.is_op(i, ",") {
                out.push((start, i));
                start = i + 1;
            }
        }
        out.push((start, to));
        out
    }

    /// Text of tokens `from..to`, one space between tokens (SQLite reads it the same: no two tokens here can merge
    /// into one, a `-` `-` pair becomes `- -`), with `replace` substituted for the token ranges it covers
    fn emit(&self, from: usize, to: usize, params: &[Param], replace: &[(usize, usize, String)]) -> String {
        let mut out = String::new();
        let mut i = from;
        while i < to {
            if !out.is_empty() {
                out.push(' ');
            }
            if let Some((_, end, text)) = replace.iter().find(|r| r.0 == i) {
                out.push_str(text);
                i = *end;
                continue;
            }
            if self.t[i].kind == TokKind::Var {
                out.push_str(&param_text(self.text(i), self.t[i].start, params));
            } else {
                out.push_str(self.text(i));
            }
            i += 1;
        }
        out
    }
}

/// A parameter as it appears in the statements built here: anonymous `?` gets its index written out, so that it keeps
/// its value wherever it moves
fn param_text(text: &str, start: usize, params: &[Param]) -> String {
    match params.iter().find(|p| p.start == start) {
        Some(p) if text == "?" => format!("?{}", p.index),
        _ => text.to_owned(),
    }
}

/// Indexes SQLite gives the parameters (`?` the next free one, `?NNN` that one, a name the index of its first use or
/// the next free one), in text order
fn number_params(p: &Parser) -> Option<(Vec<Param>, usize)> {
    let mut out: Vec<Param> = Vec::new();
    let mut max = 0usize;
    for (i, t) in p.t.iter().enumerate() {
        if t.kind != TokKind::Var {
            continue;
        }
        let text = p.text(i);
        let index = if text == "?" {
            max + 1
        } else if let Some(n) = text.strip_prefix('?') {
            let n: usize = n.parse().ok()?;
            if n == 0 || n > 32766 {
                return None;
            }
            n
        } else {
            if text[1..].to_ascii_lowercase().starts_with(RESERVED) {
                return None;
            }
            match out.iter().find(|q| q.text == text) {
                Some(q) => q.index,
                None => max + 1,
            }
        };
        max = max.max(index);
        out.push(Param {
            text: text.to_owned(),
            index,
            start: t.start,
        });
    }
    Some((out, max))
}

/// What the statement is to this module: Some for the shape it takes, None for everything else
pub fn parse(sql: &str) -> Option<Shape> {
    let sql = sql::trimmed(sql);
    let mut t = sql::tokenize(sql)?;
    // one trailing `;` is part of the statement text clients send
    let semis = t
        .iter()
        .rev()
        .take_while(|t| t.kind == TokKind::Op && &sql[t.start..t.end] == ";")
        .count();
    t.truncate(t.len() - semis);
    let p = Parser { sql, t };
    let n = p.t.len();
    if n < 4 || !p.is_word(0, "SELECT") {
        return None;
    }
    for i in 0..n {
        let tk = &p.t[i];
        if tk.kind == TokKind::Op && p.text(i) == ";" {
            return None;
        }
        if tk.kind == TokKind::Word {
            let w = p.text(i);
            if (i > 0 && w.eq_ignore_ascii_case("SELECT"))
                || REFUSED_WORDS.iter().any(|r| w.eq_ignore_ascii_case(r))
                || w.to_ascii_lowercase().starts_with(RESERVED)
            {
                return None;
            }
        }
        if tk.kind == TokKind::Quoted && name_of(sql, tk)?.starts_with(RESERVED) {
            return None;
        }
    }
    let (params, param_count) = number_params(&p)?;

    // clause boundaries: keywords outside parentheses, in their only allowed order
    let mut depth = 0usize;
    let mut marks: Vec<(&str, usize)> = Vec::new();
    for i in 1..n {
        if p.is_op(i, "(") {
            depth += 1;
            continue;
        }
        if p.is_op(i, ")") {
            depth = depth.checked_sub(1)?;
            continue;
        }
        for kw in ["FROM", "WHERE", "GROUP", "HAVING", "ORDER", "LIMIT"] {
            if p.is_word(i, kw) {
                if depth > 0 {
                    return None;
                }
                marks.push((kw, i));
            }
        }
    }
    if depth != 0 || marks.first().map(|m| m.0) != Some("FROM") {
        return None;
    }
    let order_of = |kw: &str| {
        ["FROM", "WHERE", "GROUP", "HAVING", "ORDER", "LIMIT"]
            .iter()
            .position(|k| *k == kw)
    };
    if marks.windows(2).any(|w| order_of(w[0].0) >= order_of(w[1].0)) {
        return None;
    }
    let end_of = |k: usize| marks.get(k + 1).map_or(n, |m| m.1);
    // no empty clause (SQLite refuses those anyway)
    for (k, (kw, at)) in marks.iter().enumerate() {
        let words = if matches!(*kw, "GROUP" | "ORDER") { 2 } else { 1 };
        if end_of(k) <= at + words {
            return None;
        }
    }
    let clause = |kw: &str| marks.iter().position(|m| m.0 == kw).map(|k| (marks[k].1, end_of(k)));

    // FROM name [[AS] alias]
    let (from_at, from_end) = clause("FROM")?;
    let mut i = from_at + 1;
    if i >= from_end || !matches!(p.t[i].kind, TokKind::Word | TokKind::Quoted) {
        return None;
    }
    let table = name_of(sql, &p.t[i])?;
    let table_tok = i;
    i += 1;
    let mut alias = None;
    if i < from_end && p.is_word(i, "AS") {
        i += 1;
    }
    if i < from_end && matches!(p.t[i].kind, TokKind::Word | TokKind::Quoted) {
        alias = Some(name_of(sql, &p.t[i])?);
        i += 1;
    }
    if i != from_end {
        // `schema.table`, a second table, `NOT INDEXED`, …
        return None;
    }
    let from = p.emit(table_tok, from_end, &[], &[]);
    let qualifiers: Vec<String> = std::iter::once(table.clone()).chain(alias.clone()).collect();

    // GROUP BY [qualifier .] column, …
    let mut keys: Vec<String> = Vec::new();
    let mut group = String::new();
    if let Some((at, end)) = clause("GROUP") {
        if !p.is_word(at + 1, "BY") {
            return None;
        }
        for (a, b) in p.split_commas(at + 2, end) {
            let col = column_ref(&p, a, b, &qualifiers)?;
            if keys.contains(&col) {
                return None;
            }
            keys.push(col);
        }
        group = p.emit(at + 2, end, &[], &[]);
    }

    // select list
    let mut aggs: Vec<Agg> = Vec::new();
    let mut aliases: Vec<String> = Vec::new();
    let mut merge_items: Vec<String> = Vec::new();
    let mut list_start = 1;
    if p.is_word(1, "ALL") {
        list_start = 2;
    }
    for (a, b) in p.split_commas(list_start, from_at) {
        if a >= b {
            return None;
        }
        // trailing alias: `AS name`, or a bare name after the expression
        let (expr_end, alias) = if b - a >= 3 && p.is_word(b - 2, "AS") {
            if !matches!(p.t[b - 1].kind, TokKind::Word | TokKind::Quoted | TokKind::Str) {
                return None;
            }
            (b - 2, Some(b - 2))
        } else if b - a >= 2 && matches!(p.t[b - 1].kind, TokKind::Word | TokKind::Quoted) && !p.is_op(b - 2, ".") {
            (b - 1, Some(b - 1))
        } else {
            (b, None)
        };
        if let Some(al) = alias {
            let at = if p.is_word(al, "AS") { al + 1 } else { al };
            let name = match p.t[at].kind {
                TokKind::Str => p.text(at)[1..p.text(at).len() - 1]
                    .replace("''", "'")
                    .to_ascii_lowercase(),
                _ => name_of(sql, &p.t[at])?,
            };
            aliases.push(name);
        }
        let alias_text = alias
            .map(|al| format!(" {}", p.emit(al, b, &[], &[])))
            .unwrap_or_default();
        if let Some((agg, merged)) = aggregate(&p, a, expr_end, &params, aggs.len(), &qualifiers)? {
            aggs.push(agg);
            merge_items.push(format!("{merged}{alias_text}"));
        } else {
            let col = column_ref(&p, a, expr_end, &qualifiers)?;
            if !keys.contains(&col) {
                // a column outside GROUP BY in an aggregate query takes its value from some row of the group
                return None;
            }
            merge_items.push(format!("{}{alias_text}", p.emit(a, expr_end, &[], &[])));
        }
    }
    if aggs.is_empty() {
        // a plain `SELECT k FROM t GROUP BY k` is a different job (DISTINCT); not taken
        return None;
    }

    // HAVING and ORDER BY: aggregates replaced by the aggregate of their partial columns; everything else must be a
    // grouping column, an alias, an ordinal, a literal, a parameter or a deterministic function the combining
    // statement can run (the in-memory table holds only the grouping columns, so a reference to any other column
    // fails to compile there, which `parallel.rs` takes as "not this shape")
    let rewrite = |at: usize, end: usize, aggs: &mut Vec<Agg>| -> Option<String> {
        let mut repl = Vec::new();
        let mut i = at;
        while i < end {
            let calls = i + 1 < end && p.is_op(i + 1, "(");
            if calls && p.t[i].kind == TokKind::Quoted {
                return None;
            }
            // a name, outside the aggregates: a grouping column, an alias or a qualifier of one (`q.g`), nothing else.
            // The in-memory table has names a column reference could reach and the table does not mean the same by:
            // its own rowid (`rowid`, `oid`, `_rowid_`), its hidden columns
            if !calls && matches!(p.t[i].kind, TokKind::Word | TokKind::Quoted) {
                let name = name_of(sql, &p.t[i])?;
                let qualifier = p.is_op(i + 1, ".") && qualifiers.contains(&name);
                let keyword = p.t[i].kind == TokKind::Word
                    && (EXPRESSION_WORDS.iter().any(|w| p.text(i).eq_ignore_ascii_case(w))
                        || (i > at && p.is_word(i - 1, "COLLATE")));
                if !(qualifier || keyword || keys.contains(&name) || aliases.contains(&name)) {
                    return None;
                }
            }
            if calls && p.t[i].kind == TokKind::Word {
                let close = p.close_of(i + 1)?;
                if close >= end {
                    return None;
                }
                if let Some((agg, merged)) = aggregate(&p, i, close + 1, &params, aggs.len(), &qualifiers)? {
                    aggs.push(agg);
                    repl.push((i, close + 1, merged));
                    i = close + 1;
                    continue;
                }
                let f = p.text(i).to_ascii_lowercase();
                let args = p.split_commas(i + 2, close).len();
                if !SCALAR_FUNCTIONS.contains(&f.as_str()) || (matches!(f.as_str(), "min" | "max") && args < 2) {
                    return None;
                }
            }
            i += 1;
        }
        Some(p.emit(at, end, &params, &repl))
    };
    let having = match clause("HAVING") {
        Some((at, end)) => Some(rewrite(at + 1, end, &mut aggs)?),
        None => None,
    };
    let order = match clause("ORDER") {
        Some((at, end)) if p.is_word(at + 1, "BY") => Some(rewrite(at + 2, end, &mut aggs)?),
        Some(_) => return None,
        None => None,
    };
    let limit = match clause("LIMIT") {
        Some((at, end)) => {
            // only numbers, parameters and arithmetic: nothing that reads a row
            for k in at + 1..end {
                let ok = match p.t[k].kind {
                    TokKind::Int | TokKind::Var => true,
                    TokKind::Op => ["+", "-", "*", "(", ")", ","].contains(&p.text(k)),
                    TokKind::Word => p.is_word(k, "OFFSET"),
                    _ => false,
                };
                if !ok {
                    return None;
                }
            }
            Some(p.emit(at + 1, end, &params, &[]))
        }
        None => None,
    };
    let where_ = match clause("WHERE") {
        Some((at, end)) => {
            // an alias in WHERE would name a column of the select list that the range statement does not have
            for k in at + 1..end {
                if let Some(nm) = name_of(sql, &p.t[k])
                    && aliases.contains(&nm)
                {
                    return None;
                }
            }
            Some(p.emit(at + 1, end, &params, &[]))
        }
        None => None,
    };
    Some(Shape {
        table,
        from,
        keys,
        group,
        aggs,
        where_,
        merge_items: merge_items.join(", "),
        having,
        order,
        limit,
        aliases,
        params,
        param_count,
    })
}

/// `column` or `qualifier.column` filling tokens `a..b` exactly: the column, lowercased
fn column_ref(p: &Parser, a: usize, b: usize, qualifiers: &[String]) -> Option<String> {
    let name = |i: usize| -> Option<String> {
        matches!(p.t[i].kind, TokKind::Word | TokKind::Quoted).then(|| name_of(p.sql, &p.t[i]))?
    };
    match b.checked_sub(a)? {
        1 => name(a),
        3 if p.is_op(a + 1, ".") => {
            let q = name(a)?;
            qualifiers.contains(&q).then_some(())?;
            name(a + 2)
        }
        _ => None,
    }
}

/// Tokens `a..b` as one aggregate call this module splits: Some((the aggregate, its combining form)); Ok(None) when
/// the tokens are not an aggregate call at all; None (outer) for an aggregate it does not split (`AVG`, `TOTAL`,
/// `GROUP_CONCAT`, …), which makes the whole statement "not this shape"
#[allow(clippy::option_option)]
fn aggregate(
    p: &Parser,
    a: usize,
    b: usize,
    params: &[Param],
    next: usize,
    qualifiers: &[String],
) -> Option<Option<(Agg, String)>> {
    if b < a + 3 || p.t[a].kind != TokKind::Word || !p.is_op(a + 1, "(") || p.close_of(a + 1) != Some(b - 1) {
        if b > a && p.t[a].kind == TokKind::Word && is_other_aggregate(p.text(a)) {
            return None;
        }
        return Some(None);
    }
    let f = p.text(a).to_ascii_lowercase();
    let (arg_a, arg_b) = (a + 2, b - 1);
    let args = p.split_commas(arg_a, arg_b);
    let kind = match f.as_str() {
        "count" => AggKind::Count,
        "sum" => AggKind::Sum,
        // MIN / MAX of two or more arguments are scalar functions
        "min" if args.len() == 1 => AggKind::Min,
        "max" if args.len() == 1 => AggKind::Max,
        "min" | "max" => return Some(None),
        other if is_other_aggregate(other) => return None,
        _ => return Some(None),
    };
    if args.len() != 1 || p.is_word(arg_a, "ALL") {
        return None;
    }
    let col = format!("{RESERVED}p{next}");
    let star = arg_a == arg_b || (arg_b == arg_a + 1 && p.is_op(arg_a, "*"));
    let arg = if star {
        if kind != AggKind::Count {
            return None;
        }
        None
    } else {
        // no aggregate inside an aggregate (SQLite refuses it anyway) and no float literal in a SUM: a sum of floats
        // is not split (its last digits depend on the order of the additions)
        for k in arg_a..arg_b {
            if p.t[k].kind == TokKind::Float && kind == AggKind::Sum {
                return None;
            }
            if p.t[k].kind == TokKind::Word && p.is_op(k + 1, "(") {
                let g = p.text(k).to_ascii_lowercase();
                if matches!(g.as_str(), "count" | "sum") || is_other_aggregate(&g) {
                    return None;
                }
            }
        }
        Some(p.emit(arg_a, arg_b, params, &[]))
    };
    let column = match kind {
        AggKind::Min | AggKind::Max => Some(column_ref(p, arg_a, arg_b, qualifiers)?),
        _ => None,
    };
    let merged = match kind {
        AggKind::Count | AggKind::Sum => format!("SUM({col})"),
        AggKind::Min => format!("MIN({col})"),
        AggKind::Max => format!("MAX({col})"),
    };
    Some(Some((Agg { kind, arg, column }, merged)))
}

/// Aggregate (and window) functions of SQLite other than the four split here
fn is_other_aggregate(name: &str) -> bool {
    [
        "avg",
        "total",
        "group_concat",
        "string_agg",
        "json_group_array",
        "json_group_object",
        "jsonb_group_array",
        "jsonb_group_object",
        "median",
        "percentile",
        "percentile_cont",
        "percentile_disc",
        "row_number",
        "rank",
        "dense_rank",
        "percent_rank",
        "cume_dist",
        "ntile",
        "lag",
        "lead",
        "first_value",
        "last_value",
        "nth_value",
    ]
    .contains(&name.to_ascii_lowercase().as_str())
}

impl Shape {
    /// The range statement: partial aggregates of the rows with `rid` (a name of the rowid) between `:xsqld_lo` and
    /// `:xsqld_hi`
    pub fn range_sql(&self, rid: &str) -> String {
        let mut cols: Vec<String> = self.keys_text();
        for a in &self.aggs {
            cols.push(match (a.kind, &a.arg) {
                (AggKind::Count, None) => "COUNT(*)".into(),
                (AggKind::Count, Some(x)) => format!("COUNT({x})"),
                (AggKind::Sum, Some(x)) => format!("SUM({x})"),
                (AggKind::Min, Some(x)) => format!("MIN({x})"),
                (AggKind::Max, Some(x)) => format!("MAX({x})"),
                (_, None) => unreachable!("only COUNT takes *"),
            });
        }
        for a in &self.aggs {
            if let (AggKind::Sum, Some(x)) = (a.kind, &a.arg) {
                cols.push(format!("COUNT({x})"));
                cols.push(format!("MIN(({x}) + 0)"));
                cols.push(format!("MAX(({x}) + 0)"));
            }
        }
        let range = format!("{rid} BETWEEN :{RESERVED}lo AND :{RESERVED}hi");
        let filter = match &self.where_ {
            Some(w) => format!("({w}) AND {range}"),
            None => range,
        };
        let mut s = format!("SELECT {} FROM {} WHERE {filter}", cols.join(", "), self.from);
        if !self.group.is_empty() {
            s.push_str(" GROUP BY ");
            s.push_str(&self.group);
        }
        s
    }

    fn keys_text(&self) -> Vec<String> {
        self.keys.iter().map(|k| quote(k)).collect()
    }

    /// Positions of the hidden columns of each SUM in a partial row
    pub fn sum_cols(&self) -> Vec<SumCols> {
        let base = self.keys.len();
        let mut extra = base + self.aggs.len();
        let mut out = Vec::new();
        for (i, a) in self.aggs.iter().enumerate() {
            if a.kind == AggKind::Sum {
                out.push(SumCols {
                    sum: base + i,
                    count: extra,
                    min: extra + 1,
                    max: extra + 2,
                });
                extra += 3;
            }
        }
        out
    }

    /// Columns of a partial row
    pub fn width(&self) -> usize {
        self.keys.len() + self.aggs.len() + 3 * self.aggs.iter().filter(|a| a.kind == AggKind::Sum).count()
    }

    /// The in-memory table the partial rows go to: the table's name, the grouping columns with `decltypes` (their
    /// declared types, so that comparisons in HAVING and ORDER BY see the same affinity), one untyped column per
    /// partial value
    pub fn parts_ddl(&self, decltypes: &[String]) -> String {
        let mut cols: Vec<String> = self
            .keys
            .iter()
            .zip(decltypes)
            .map(|(k, t)| format!("{} {t}", quote(k)))
            .collect();
        for i in 0..self.aggs.len() {
            cols.push(format!("{RESERVED}p{i}"));
        }
        for i in 0..3 * self.aggs.iter().filter(|a| a.kind == AggKind::Sum).count() {
            cols.push(format!("{RESERVED}b{i}"));
        }
        format!("CREATE TABLE {}({})", quote(&self.table), cols.join(", "))
    }

    /// The combining statement, run on the in-memory table
    pub fn merge_sql(&self) -> String {
        let mut s = format!("SELECT {} FROM {}", self.merge_items, self.from);
        if !self.group.is_empty() {
            s.push_str(" GROUP BY ");
            s.push_str(&self.group);
        }
        for (kw, part) in [
            ("HAVING", &self.having),
            ("ORDER BY", &self.order),
            ("LIMIT", &self.limit),
        ] {
            if let Some(x) = part {
                s.push_str(&format!(" {kw} {x}"));
            }
        }
        s
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn takes_the_shape_with_comments_quotes_aliases_and_parameters() {
        for sql in [
            "SELECT COUNT(*) FROM t",
            "select count(*) from t where a = ? and b like 'x%'",
            "SELECT ALL g, COUNT(*) FROM t GROUP BY g",
            "SELECT /* c */ g, -- line\n COUNT(x) AS n FROM \"t\" GROUP BY g;",
            "SELECT [g], SUM(v) total FROM `t` AS q GROUP BY q.[g] HAVING SUM(v) > ?1 ORDER BY total DESC LIMIT :n",
            "SELECT q.g, MIN(s), MAX(s) FROM t q WHERE rowid > 0 GROUP BY q.g ORDER BY 1 LIMIT 5 OFFSET ?",
            "SELECT COUNT(*), SUM(CASE WHEN s = 'a' THEN 1 ELSE 0 END) FROM t WHERE v IN (1, 2, 3)",
            "SELECT g, COUNT(*) FROM t GROUP BY g HAVING max(COUNT(*), 2) > 3 AND abs(g) < 5",
            "SELECT COUNT(*) FROM t WHERE s = 'select' AND \"from\" = 1",
        ] {
            assert!(parse(sql).is_some(), "{sql}");
        }
    }

    #[test]
    fn refuses_everything_else() {
        for sql in [
            "SELECT DISTINCT g FROM t",
            "SELECT COUNT(DISTINCT g) FROM t",
            "SELECT AVG(v) FROM t",
            "SELECT TOTAL(v) FROM t",
            "SELECT group_concat(s) FROM t",
            "SELECT g, COUNT(*) OVER (PARTITION BY g) FROM t",
            "SELECT COUNT(*) FILTER (WHERE v > 0) FROM t",
            "WITH x AS (SELECT 1) SELECT COUNT(*) FROM t",
            "SELECT COUNT(*) FROM t JOIN u USING (id)",
            "SELECT COUNT(*) FROM t, u",
            "SELECT COUNT(*) FROM (SELECT * FROM t)",
            "SELECT COUNT(*) FROM t WHERE g IN (SELECT g FROM u)",
            "SELECT COUNT(*) FROM t WHERE EXISTS (SELECT 1)",
            "SELECT COUNT(*) FROM main.t",
            "SELECT COUNT(*) FROM t INDEXED BY i",
            "SELECT COUNT(*) FROM t NOT INDEXED",
            "SELECT COUNT(*) FROM t UNION SELECT 1",
            "SELECT g, s, COUNT(*) FROM t GROUP BY g",
            "SELECT g + 1, COUNT(*) FROM t GROUP BY g",
            "SELECT g, COUNT(*) FROM t GROUP BY g + 0",
            "SELECT g, COUNT(*) FROM t GROUP BY 1",
            "SELECT g, COUNT(*) FROM t GROUP BY g, g",
            "SELECT g, COUNT(*) FROM t GROUP BY g COLLATE NOCASE",
            "SELECT COUNT(*) + 1 FROM t",
            "SELECT MIN(v, 2) FROM t",
            "SELECT MIN(v + 1) FROM t",
            "SELECT SUM(v * 1.5) FROM t",
            "SELECT SUM(ALL v) FROM t",
            "SELECT SUM(v ORDER BY g) FROM t",
            "SELECT g FROM t GROUP BY g",
            "SELECT COUNT(*) AS c FROM t WHERE c > 0",
            "SELECT g, COUNT(*) FROM t GROUP BY g HAVING random() > 0",
            "SELECT g, COUNT(*) FROM t GROUP BY g ORDER BY avg(v)",
            "SELECT g, COUNT(*) FROM t GROUP BY g HAVING \"count\"(*) > 1",
            "SELECT g, COUNT(*) FROM t GROUP BY g LIMIT (SELECT 1)",
            "SELECT g, COUNT(*) FROM t GROUP BY g LIMIT g",
            "SELECT g, COUNT(*) FROM t GROUP BY g LIMIT 1.5",
            "SELECT COUNT(*) FROM t WHERE v = :xsqld_lo",
            "SELECT COUNT(*) AS xsqld_p0 FROM t",
            "SELECT COUNT(*) FROM t WHERE v > CURRENT_TIMESTAMP",
            "SELECT COUNT(*) FROM t WHERE v = $a::b",
            "SELECT COUNT(*) FROM t WHERE v = #x",
            "SELECT COUNT(*) FROM t WHERE v = 'open",
            "SELECT COUNT(*) FROM t WHERE v = 1_000",
            "SELECT COUNT(*) FROM t WHERE v = 12abc",
            "SELECT COUNT(*) FROM t; SELECT 1",
            "SELECT COUNT(*) FROM t WHERE v MATCH 'x'",
            "SELECT COUNT(*) FROM t WHERE (v > 0",
            "SELECT COUNT(*) WHERE 1",
            "VALUES (1)",
            "SELECT COUNT(*) FROM t WHERE ORDER BY 1",
            "SELECT COUNT(*) FROM t ORDER BY 1 WHERE v > 0",
        ] {
            assert!(parse(sql).is_none(), "{sql}");
        }
    }

    #[test]
    fn builds_range_and_combining_statements() {
        let s = parse(
            "SELECT q.g AS k, COUNT(*) c, SUM(v), MIN(s) FROM \"t\" AS q WHERE v > ? \
             GROUP BY q.g HAVING MAX(s) > ? ORDER BY c DESC, COUNT(v) LIMIT ?",
        )
        .unwrap();
        assert_eq!(s.table, "t");
        assert_eq!(s.keys, ["g"]);
        assert_eq!(s.aliases, ["k", "c"]);
        assert_eq!(
            s.range_sql("rowid"),
            "SELECT \"g\", COUNT(*), SUM(v), MIN(s), MAX(s), COUNT(v), COUNT(v), MIN((v) + 0), MAX((v) + 0) \
             FROM \"t\" AS q WHERE (v > ?1) AND rowid BETWEEN :xsqld_lo AND :xsqld_hi GROUP BY q . g"
        );
        assert_eq!(
            s.parts_ddl(&["INTEGER".into()]),
            "CREATE TABLE \"t\"(\"g\" INTEGER, xsqld_p0, xsqld_p1, xsqld_p2, xsqld_p3, xsqld_p4, xsqld_b0, xsqld_b1, xsqld_b2)"
        );
        assert_eq!(
            s.merge_sql(),
            "SELECT q . g AS k, SUM(xsqld_p0) c, SUM(xsqld_p1), MIN(xsqld_p2) FROM \"t\" AS q GROUP BY q . g \
             HAVING MAX(xsqld_p3) > ?2 ORDER BY c DESC , SUM(xsqld_p4) LIMIT ?3"
        );
        assert_eq!(s.width(), 9);
        let sc = s.sum_cols();
        assert_eq!((sc[0].sum, sc[0].count, sc[0].min, sc[0].max), (2, 6, 7, 8));
    }

    /// Parameters get the numbers SQLite gives them: `?` the next free one, `?NNN` that one, a name the number of its
    /// first use
    #[test]
    fn numbers_parameters_like_sqlite() {
        let s =
            parse("SELECT COUNT(*) FROM t WHERE a = ? AND b = ?5 AND c = ? AND d = :a AND e = :a AND f = @b").unwrap();
        let got: Vec<(&str, usize)> = s.params.iter().map(|p| (p.text.as_str(), p.index)).collect();
        assert_eq!(got, [("?", 1), ("?5", 5), ("?", 6), (":a", 7), (":a", 7), ("@b", 8)]);
        assert_eq!(s.param_count, 8);
        assert!(s.range_sql("rowid").contains("a = ?1 AND b = ?5 AND c = ?6 AND d = :a"));
        // the check against SQLite's own numbering
        let c = rusqlite::Connection::open_in_memory().unwrap();
        c.execute_batch("CREATE TABLE t(a, b, c, d, e, f)").unwrap();
        let st = c
            .prepare("SELECT COUNT(*) FROM t WHERE a = ? AND b = ?5 AND c = ? AND d = :a AND e = :a AND f = @b")
            .unwrap();
        assert_eq!(st.parameter_count(), s.param_count);
        for p in &s.params {
            match p.text.as_str() {
                "?" => assert_eq!(st.parameter_name(p.index), None),
                t => assert_eq!(st.parameter_name(p.index), Some(t)),
            }
        }
    }

    #[test]
    fn tokenizes_like_sqlite() {
        use crate::sql::{TokKind::*, tokenize};
        let kinds = |s: &str| tokenize(s).map(|t| t.iter().map(|t| t.kind).collect::<Vec<_>>());
        assert_eq!(
            kinds("a.\"b\"\"c\" [d] `e` 'f''g' x'0A' 1 0x1F 1.5 .5 1e3 ? ?2 :n @n $n -- c\n/* d */ <> ->> ||"),
            Some(vec![
                Word, Op, Quoted, Quoted, Quoted, Str, Blob, Int, Int, Float, Float, Float, Var, Var, Var, Var, Var,
                Op, Op, Op
            ])
        );
        for bad in [
            "'open", "\"open", "[open", "x'0'", "1_0", "1x", "$a::b", "$a(b)", "#a", "a ! b", "a\0b", ":",
        ] {
            assert_eq!(tokenize(bad), None, "{bad:?}");
        }
        // an unterminated block comment runs to the end, as in SQLite
        assert_eq!(kinds("a /* b"), Some(vec![Word]));
    }
}
