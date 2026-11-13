//! Hrana over HTTP v2, JSON encoding: the subset the clients use (`execute`, `batch`, `close`).
//! Field names, order and value encodings follow libsql-server 0.24.32 (`libsql-hrana/src/proto.rs`).

use std::fmt;

use base64::Engine as _;
use base64::engine::{DecodePaddingMode, GeneralPurpose, GeneralPurposeConfig};
use serde::de::{self, Deserializer, Visitor};
use serde::ser::{SerializeMap, Serializer};
use serde::{Deserialize, Serialize};

/// Blobs: written without padding (like sqld), read with or without it
pub const BASE64: GeneralPurpose = GeneralPurpose::new(
    &base64::alphabet::STANDARD,
    GeneralPurposeConfig::new()
        .with_encode_padding(false)
        .with_decode_padding_mode(DecodePaddingMode::Indifferent),
);

#[derive(Deserialize, Debug)]
pub struct PipelineReq {
    #[serde(default)]
    pub baton: Option<String>,
    pub requests: Vec<StreamRequest>,
}

#[derive(Deserialize, Debug, Clone)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum StreamRequest {
    Close,
    Execute { stmt: Stmt },
    Batch { batch: Batch },
}

#[derive(Deserialize, Debug, Clone)]
pub struct Stmt {
    #[serde(default)]
    pub sql: Option<String>,
    #[serde(default)]
    pub sql_id: Option<i32>,
    #[serde(default)]
    pub args: Vec<Value>,
    #[serde(default)]
    pub named_args: Vec<NamedArg>,
    /// Ignored like sqld 0.24.32 does: rows are always returned
    #[serde(default)]
    #[allow(dead_code)]
    pub want_rows: Option<bool>,
}

#[derive(Deserialize, Debug, Clone)]
pub struct NamedArg {
    pub name: String,
    pub value: Value,
}

#[derive(Deserialize, Debug, Clone)]
pub struct Batch {
    pub steps: Vec<BatchStep>,
}

#[derive(Deserialize, Debug, Clone)]
pub struct BatchStep {
    #[serde(default)]
    pub condition: Option<BatchCond>,
    pub stmt: Stmt,
}

#[derive(Deserialize, Debug, Clone)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum BatchCond {
    Ok { step: u32 },
    Error { step: u32 },
    Not { cond: Box<BatchCond> },
    And { conds: Vec<BatchCond> },
    Or { conds: Vec<BatchCond> },
    IsAutocommit,
}

/// A value: `null`, `integer` (decimal string), `float`, `text`, `blob` (`base64`)
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Null,
    Integer(i64),
    Float(f64),
    Text(String),
    Blob(Vec<u8>),
}

impl Serialize for Value {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let mut m = s.serialize_map(Some(2))?;
        match self {
            Value::Null => m.serialize_entry("type", "null")?,
            Value::Integer(i) => {
                m.serialize_entry("type", "integer")?;
                m.serialize_entry("value", &i.to_string())?;
            }
            Value::Float(f) => {
                m.serialize_entry("type", "float")?;
                m.serialize_entry("value", f)?;
            }
            Value::Text(t) => {
                m.serialize_entry("type", "text")?;
                m.serialize_entry("value", t)?;
            }
            Value::Blob(b) => {
                m.serialize_entry("type", "blob")?;
                m.serialize_entry("base64", &BASE64.encode(b))?;
            }
        }
        m.end()
    }
}

impl<'de> Deserialize<'de> for Value {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        struct Raw {
            #[serde(rename = "type")]
            ty: String,
            #[serde(default)]
            value: Option<RawScalar>,
            #[serde(default)]
            base64: Option<String>,
        }
        let raw = Raw::deserialize(d)?;
        let missing = |f: &'static str| de::Error::missing_field(f);
        Ok(match raw.ty.as_str() {
            "null" => Value::Null,
            "integer" => {
                match raw.value.ok_or_else(|| missing("value"))? {
                    RawScalar::Str(s) => Value::Integer(s.parse().map_err(|_| {
                        de::Error::invalid_value(de::Unexpected::Str(&s), &"decimal integer as a string")
                    })?),
                    RawScalar::Int(i) => Value::Integer(i),
                    RawScalar::Float(_) => return Err(de::Error::custom("integer value must be a string")),
                }
            }
            "float" => match raw.value.ok_or_else(|| missing("value"))? {
                RawScalar::Float(f) => Value::Float(f),
                RawScalar::Int(i) => Value::Float(i as f64),
                RawScalar::Str(_) => return Err(de::Error::custom("float value must be a number")),
            },
            "text" => match raw.value.ok_or_else(|| missing("value"))? {
                RawScalar::Str(s) => Value::Text(s),
                _ => return Err(de::Error::custom("text value must be a string")),
            },
            "blob" => {
                let b = raw.base64.ok_or_else(|| missing("base64"))?;
                // like sqld: any number of `=` at the end
                Value::Blob(
                    BASE64
                        .decode(b.trim_end_matches('=').as_bytes())
                        .map_err(|e| de::Error::custom(format!("bad base64: {e}")))?,
                )
            }
            other => {
                return Err(de::Error::unknown_variant(
                    other,
                    &["null", "integer", "float", "text", "blob"],
                ));
            }
        })
    }
}

enum RawScalar {
    Str(String),
    Int(i64),
    Float(f64),
}

impl<'de> Deserialize<'de> for RawScalar {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct V;
        impl Visitor<'_> for V {
            type Value = RawScalar;
            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str("a string or a number")
            }
            fn visit_str<E: de::Error>(self, v: &str) -> Result<RawScalar, E> {
                Ok(RawScalar::Str(v.to_owned()))
            }
            fn visit_string<E: de::Error>(self, v: String) -> Result<RawScalar, E> {
                Ok(RawScalar::Str(v))
            }
            fn visit_i64<E: de::Error>(self, v: i64) -> Result<RawScalar, E> {
                Ok(RawScalar::Int(v))
            }
            fn visit_u64<E: de::Error>(self, v: u64) -> Result<RawScalar, E> {
                i64::try_from(v).map(RawScalar::Int).or(Ok(RawScalar::Float(v as f64)))
            }
            fn visit_f64<E: de::Error>(self, v: f64) -> Result<RawScalar, E> {
                Ok(RawScalar::Float(v))
            }
        }
        d.deserialize_any(V)
    }
}

// ---- responses ----

#[derive(Serialize, Debug)]
pub struct PipelineResp {
    pub baton: Option<String>,
    pub base_url: Option<String>,
    pub results: Vec<StreamResult>,
}

#[derive(Serialize, Debug, Clone)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum StreamResult {
    Ok { response: StreamResponse },
    Error { error: Error },
}

#[derive(Serialize, Debug, Clone)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum StreamResponse {
    Close,
    Execute { result: StmtResult },
    Batch { result: BatchResult },
}

#[derive(Serialize, Debug, Clone, PartialEq)]
pub struct Error {
    pub message: String,
    pub code: String,
}

impl Error {
    pub fn new(message: impl Into<String>, code: &str) -> Self {
        Self {
            message: message.into(),
            code: code.to_owned(),
        }
    }
}

#[derive(Serialize, Debug, Clone, Default)]
pub struct StmtResult {
    pub cols: Vec<Col>,
    pub rows: Vec<Vec<Value>>,
    pub affected_row_count: u64,
    #[serde(serialize_with = "opt_i64_as_str")]
    pub last_insert_rowid: Option<i64>,
    /// No replication: always null
    pub replication_index: Option<String>,
    pub rows_read: u64,
    pub rows_written: u64,
    pub query_duration_ms: f64,
}

#[derive(Serialize, Debug, Clone)]
pub struct Col {
    pub name: Option<String>,
    pub decltype: Option<String>,
}

#[derive(Serialize, Debug, Clone, Default)]
pub struct BatchResult {
    pub step_results: Vec<Option<StmtResult>>,
    pub step_errors: Vec<Option<Error>>,
    pub replication_index: Option<String>,
}

fn opt_i64_as_str<S: Serializer>(v: &Option<i64>, s: S) -> Result<S::Ok, S::Error> {
    match v {
        Some(i) => s.serialize_str(&i.to_string()),
        None => s.serialize_none(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn values_round_trip() {
        let req: PipelineReq = serde_json::from_str(
            r#"{"baton":null,"requests":[{"type":"execute","stmt":{"sql":"SELECT ?,?,?,?,?","args":[
                {"type":"null"},{"type":"integer","value":"9223372036854775807"},{"type":"float","value":1.5},
                {"type":"text","value":"é"},{"type":"blob","base64":"AQ=="}],"want_rows":true}},
                {"type":"batch","batch":{"steps":[{"stmt":{"sql":"BEGIN"}},
                {"condition":{"type":"not","cond":{"type":"ok","step":0}},"stmt":{"sql":"ROLLBACK"}}]}},
                {"type":"close"}]}"#,
        )
        .unwrap();
        let StreamRequest::Execute { stmt } = &req.requests[0] else {
            panic!()
        };
        assert_eq!(
            stmt.args,
            vec![
                Value::Null,
                Value::Integer(i64::MAX),
                Value::Float(1.5),
                Value::Text("é".into()),
                Value::Blob(vec![1])
            ]
        );
        assert!(matches!(req.requests[2], StreamRequest::Close));
        let out = serde_json::to_string(&stmt.args).unwrap();
        assert_eq!(
            out,
            r#"[{"type":"null"},{"type":"integer","value":"9223372036854775807"},{"type":"float","value":1.5},{"type":"text","value":"é"},{"type":"blob","base64":"AQ"}]"#
        );
    }

    #[test]
    fn rejects_bad_values() {
        let bad = |v: &str| serde_json::from_str::<Value>(v).is_err();
        assert!(bad(r#"{"type":"integer","value":"1.5"}"#));
        assert!(bad(r#"{"type":"integer"}"#));
        assert!(bad(r#"{"type":"blob","base64":"!!"}"#));
        assert!(bad(r#"{"type":"date","value":"x"}"#));
        assert!(serde_json::from_str::<PipelineReq>(r#"{"requests":[{"type":"describe"}]}"#).is_err());
        // review of 0.1.0: sqld 0.24.32 takes any number of `=` at the end, and nothing else
        let blob = |b: &str| serde_json::from_str::<Value>(&format!(r#"{{"type":"blob","base64":"{b}"}}"#));
        for (b, v) in [
            ("AAE", vec![0, 1]),
            ("AAE=", vec![0, 1]),
            ("AAE==", vec![0, 1]),
            ("AAE=====", vec![0, 1]),
            ("=", vec![]),
        ] {
            assert_eq!(blob(b).unwrap(), Value::Blob(v), "{b}");
        }
        for b in ["AA=E", "A", "A="] {
            assert!(blob(b).is_err(), "{b}");
        }
    }

    #[test]
    fn result_shape_matches_sqld() {
        let r = StmtResult {
            cols: vec![Col {
                name: Some("a".into()),
                decltype: None,
            }],
            rows: vec![vec![Value::Float(2.0)]],
            affected_row_count: 1,
            last_insert_rowid: Some(7),
            ..Default::default()
        };
        let out = serde_json::to_string(&StreamResult::Ok {
            response: StreamResponse::Execute { result: r },
        })
        .unwrap();
        assert_eq!(
            out,
            r#"{"type":"ok","response":{"type":"execute","result":{"cols":[{"name":"a","decltype":null}],"rows":[[{"type":"float","value":2.0}]],"affected_row_count":1,"last_insert_rowid":"7","replication_index":null,"rows_read":0,"rows_written":0,"query_duration_ms":0.0}}}"#
        );
        let close = serde_json::to_string(&StreamResult::Ok {
            response: StreamResponse::Close,
        })
        .unwrap();
        assert_eq!(close, r#"{"type":"ok","response":{"type":"close"}}"#);
    }
}
