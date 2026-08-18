//! Strict JSON admission, mirroring the kernel protocol's rules: duplicate
//! member names are refused rather than last-wins resolved, and integers
//! outside the I-JSON safe range are refused rather than silently rounded.
//!
//! Both rules exist because two SDK decoders must read identical bytes into
//! identical values. Duplicate keys are exactly where JavaScript's
//! `JSON.parse` (last wins) and a streaming parser (first wins, or error)
//! diverge, and a 2^53-adjacent integer is where JavaScript quietly loses
//! precision Rust would keep.

use serde::Deserialize;
use serde_json::{Map, Value};
use std::fmt;

/// Why a byte sequence was refused before any typed decoding ran.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StrictJsonError {
    /// Not JSON at all, or trailing bytes after the value.
    Syntax(String),
    /// An object carries the same member name twice.
    DuplicateMember(String),
    /// An integer outside `-(2^53-1)..=2^53-1`.
    UnsafeInteger(String),
}

impl fmt::Display for StrictJsonError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StrictJsonError::Syntax(detail) => write!(f, "not strict JSON: {detail}"),
            StrictJsonError::DuplicateMember(name) => {
                write!(f, "duplicate member name {name:?}")
            }
            StrictJsonError::UnsafeInteger(value) => {
                write!(f, "integer {value} outside the I-JSON safe range")
            }
        }
    }
}

pub(crate) const SAFE_MAX: u64 = (1 << 53) - 1;
pub(crate) const SAFE_MIN: i64 = -((1 << 53) - 1);

/// Parse one JSON value under the strict rules.
///
/// The typed decode (`serde_json::from_value` with `deny_unknown_fields`)
/// happens after this admission, so an attacker never reaches a typed
/// deserializer with bytes these rules refuse.
pub fn parse(bytes: &[u8]) -> Result<Value, StrictJsonError> {
    refuse_overflowing_integers(bytes)?;
    let mut deserializer = serde_json::Deserializer::from_slice(bytes);
    let value = StrictValue::deserialize(&mut deserializer).map_err(classify)?;
    deserializer
        .end()
        .map_err(|error| StrictJsonError::Syntax(error.to_string()))?;
    Ok(value.0)
}

/// An integer literal wider than 64 bits never reaches the visitor:
/// serde's parser rounds it to `f64` first, and a rounded integer admitted
/// as a float is exactly the divergence this module refuses (CPython's
/// decoder would keep the exact value JavaScript's loses). Only overflow
/// is caught here, on the raw token; in-range literals stay the visitor's
/// range check.
fn refuse_overflowing_integers(bytes: &[u8]) -> Result<(), StrictJsonError> {
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'"' => {
                // Skip the string body; an escape hides quotes, not tokens.
                i += 1;
                while i < bytes.len() {
                    match bytes[i] {
                        b'\\' => i += 2,
                        b'"' => {
                            i += 1;
                            break;
                        }
                        _ => i += 1,
                    }
                }
            }
            b'-' | b'0'..=b'9' => {
                let start = i;
                if bytes[i] == b'-' {
                    i += 1;
                }
                while i < bytes.len() && bytes[i].is_ascii_digit() {
                    i += 1;
                }
                if i < bytes.len() && matches!(bytes[i], b'.' | b'e' | b'E') {
                    // A float token; consume it whole so its digits are not
                    // rescanned as a fresh integer. Malformed tails are the
                    // parser's syntax error to raise.
                    while i < bytes.len()
                        && matches!(bytes[i], b'.' | b'e' | b'E' | b'+' | b'-' | b'0'..=b'9')
                    {
                        i += 1;
                    }
                    continue;
                }
                let token = std::str::from_utf8(&bytes[start..i]).unwrap_or("");
                let fits = if let Some(digits) = token.strip_prefix('-') {
                    digits.is_empty() || token.parse::<i64>().is_ok()
                } else {
                    token.parse::<u64>().is_ok()
                };
                if !fits {
                    return Err(StrictJsonError::UnsafeInteger(token.to_owned()));
                }
            }
            _ => i += 1,
        }
    }
    Ok(())
}

/// A duplicate-name or unsafe-integer refusal is raised inside serde as a
/// custom error; everything else is syntax.
fn classify(error: serde_json::Error) -> StrictJsonError {
    let text = error.to_string();
    if let Some(name) = text.strip_prefix(DUPLICATE_PREFIX) {
        let name = name.split(" at line").next().unwrap_or(name);
        return StrictJsonError::DuplicateMember(name.to_owned());
    }
    if let Some(value) = text.strip_prefix(UNSAFE_PREFIX) {
        let value = value.split(" at line").next().unwrap_or(value);
        return StrictJsonError::UnsafeInteger(value.to_owned());
    }
    StrictJsonError::Syntax(text)
}

const DUPLICATE_PREFIX: &str = "strict-duplicate-member:";
const UNSAFE_PREFIX: &str = "strict-unsafe-integer:";

/// `serde_json::Value` built through a visitor that enforces the strict
/// rules while the parser walks the input, so refusal happens at the first
/// violation rather than after a full parse.
struct StrictValue(Value);

impl<'de> Deserialize<'de> for StrictValue {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_any(StrictVisitor)
    }
}

struct StrictVisitor;

impl<'de> serde::de::Visitor<'de> for StrictVisitor {
    type Value = StrictValue;

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("a strict JSON value")
    }

    fn visit_bool<E>(self, value: bool) -> Result<Self::Value, E> {
        Ok(StrictValue(Value::Bool(value)))
    }

    fn visit_i64<E: serde::de::Error>(self, value: i64) -> Result<Self::Value, E> {
        if value < SAFE_MIN {
            return Err(E::custom(format!("{UNSAFE_PREFIX}{value}")));
        }
        Ok(StrictValue(Value::from(value)))
    }

    fn visit_u64<E: serde::de::Error>(self, value: u64) -> Result<Self::Value, E> {
        if value > SAFE_MAX {
            return Err(E::custom(format!("{UNSAFE_PREFIX}{value}")));
        }
        Ok(StrictValue(Value::from(value)))
    }

    fn visit_f64<E>(self, value: f64) -> Result<Self::Value, E> {
        Ok(StrictValue(Value::from(value)))
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E> {
        Ok(StrictValue(Value::String(value.to_owned())))
    }

    fn visit_string<E>(self, value: String) -> Result<Self::Value, E> {
        Ok(StrictValue(Value::String(value)))
    }

    fn visit_unit<E>(self) -> Result<Self::Value, E> {
        Ok(StrictValue(Value::Null))
    }

    fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
    where
        A: serde::de::SeqAccess<'de>,
    {
        let mut items = Vec::new();
        while let Some(StrictValue(item)) = seq.next_element()? {
            items.push(item);
        }
        Ok(StrictValue(Value::Array(items)))
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: serde::de::MapAccess<'de>,
    {
        let mut object = Map::new();
        while let Some(key) = map.next_key::<String>()? {
            let StrictValue(value) = map.next_value()?;
            if object.insert(key.clone(), value).is_some() {
                return Err(serde::de::Error::custom(format!("{DUPLICATE_PREFIX}{key}")));
            }
        }
        Ok(StrictValue(Value::Object(object)))
    }
}
