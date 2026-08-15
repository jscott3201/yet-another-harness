use super::{
    ClientMessage, MAX_COMMAND_BYTES, MAX_CONTROL_FRAME_BYTES, MAX_FRAME_BYTES, MAX_PAYLOAD_BYTES,
};
use serde::Deserialize;
use serde::de::{DeserializeSeed, Error, MapAccess, SeqAccess, Visitor};
use serde_json::value::RawValue;
use serde_json::{Map, Number, Value};
use std::collections::BTreeSet;
use std::fmt;

pub(super) fn decode_client_message(bytes: &[u8]) -> Result<ClientMessage, serde_json::Error> {
    let mut deserializer = serde_json::Deserializer::from_slice(bytes);
    let value = UniqueValue.deserialize(&mut deserializer)?;
    deserializer.end()?;
    validate_numbers(&value).map_err(<serde_json::Error as Error>::custom)?;
    serde_json::from_value(value)
}

pub(super) fn validate_wire_limits(bytes: &[u8]) -> Result<(), WireLimitError> {
    #[derive(Deserialize)]
    struct Frame {
        kind: String,
        message: Box<RawValue>,
    }
    #[derive(Deserialize)]
    struct RawCommand {
        payload: Box<RawValue>,
    }

    if bytes.len() > MAX_FRAME_BYTES {
        return Err(WireLimitError::Frame);
    }
    let frame: Frame = serde_json::from_slice(bytes)
        .map_err(|error| WireLimitError::Invalid(error.to_string()))?;
    if frame.kind != "command" {
        if bytes.len() > MAX_CONTROL_FRAME_BYTES {
            return Err(WireLimitError::Frame);
        }
        return Ok(());
    }
    if frame.message.get().len() > MAX_COMMAND_BYTES {
        return Err(WireLimitError::Command);
    }
    let command: RawCommand = serde_json::from_str(frame.message.get())
        .map_err(|error| WireLimitError::Invalid(error.to_string()))?;
    if command.payload.get().len() > MAX_PAYLOAD_BYTES {
        return Err(WireLimitError::Payload);
    }
    Ok(())
}

pub(super) enum WireLimitError {
    Invalid(String),
    Frame,
    Command,
    Payload,
}

struct UniqueValue;

impl<'de> DeserializeSeed<'de> for UniqueValue {
    type Value = Value;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_any(UniqueValueVisitor)
    }
}

struct UniqueValueVisitor;

impl<'de> Visitor<'de> for UniqueValueVisitor {
    type Value = Value;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a JSON value without duplicate object names")
    }

    fn visit_bool<E>(self, value: bool) -> Result<Value, E> {
        Ok(Value::Bool(value))
    }

    fn visit_i64<E>(self, value: i64) -> Result<Value, E> {
        Ok(Value::Number(Number::from(value)))
    }

    fn visit_u64<E>(self, value: u64) -> Result<Value, E> {
        Ok(Value::Number(Number::from(value)))
    }

    fn visit_f64<E>(self, value: f64) -> Result<Value, E>
    where
        E: Error,
    {
        Number::from_f64(value)
            .map(Value::Number)
            .ok_or_else(|| E::custom("non-finite JSON number"))
    }

    fn visit_str<E>(self, value: &str) -> Result<Value, E> {
        Ok(Value::String(value.to_owned()))
    }

    fn visit_string<E>(self, value: String) -> Result<Value, E> {
        Ok(Value::String(value))
    }

    fn visit_none<E>(self) -> Result<Value, E> {
        Ok(Value::Null)
    }

    fn visit_unit<E>(self) -> Result<Value, E> {
        Ok(Value::Null)
    }

    fn visit_some<D>(self, deserializer: D) -> Result<Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        UniqueValue.deserialize(deserializer)
    }

    fn visit_seq<A>(self, mut sequence: A) -> Result<Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let mut values = Vec::new();
        while let Some(value) = sequence.next_element_seed(UniqueValue)? {
            values.push(value);
        }
        Ok(Value::Array(values))
    }

    fn visit_map<A>(self, mut object: A) -> Result<Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        let mut names = BTreeSet::new();
        let mut values = Map::new();
        while let Some(name) = object.next_key::<String>()? {
            if !names.insert(name.clone()) {
                return Err(A::Error::custom(format!(
                    "duplicate JSON object name {name:?}"
                )));
            }
            values.insert(name, object.next_value_seed(UniqueValue)?);
        }
        Ok(Value::Object(values))
    }
}

fn validate_numbers(value: &Value) -> Result<(), String> {
    const MAX_SAFE_INTEGER: f64 = 9_007_199_254_740_991.0;
    match value {
        Value::Number(number) => {
            let numeric = number.as_f64().ok_or_else(|| {
                "JSON number cannot be represented as an I-JSON number".to_owned()
            })?;
            if numeric.fract() == 0.0 && numeric.abs() > MAX_SAFE_INTEGER {
                return Err(
                    "JSON integer exceeds the I-JSON safe range; encode it as a string".into(),
                );
            }
        }
        Value::Array(values) => {
            for value in values {
                validate_numbers(value)?;
            }
        }
        Value::Object(values) => {
            for value in values.values() {
                validate_numbers(value)?;
            }
        }
        _ => {}
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn raw_limits_accept_the_boundary_and_reject_one_more_byte() {
        let payload = format!("\"{}\"", "x".repeat(MAX_PAYLOAD_BYTES - 2));
        let frame = format!(r#"{{"kind":"command","message":{{"payload":{payload}}}}}"#);
        assert!(validate_wire_limits(frame.as_bytes()).is_ok());

        let payload = format!("\"{}\"", "x".repeat(MAX_PAYLOAD_BYTES - 1));
        let frame = format!(r#"{{"kind":"command","message":{{"payload":{payload}}}}}"#);
        assert!(matches!(
            validate_wire_limits(frame.as_bytes()),
            Err(WireLimitError::Payload)
        ));

        let prefix = r#"{"payload":null,"padding":""#;
        let suffix = r#""}"#;
        let padding = "x".repeat(MAX_COMMAND_BYTES - prefix.len() - suffix.len());
        let message = format!("{prefix}{padding}{suffix}");
        let frame = format!(r#"{{"kind":"command","message":{message}}}"#);
        assert!(validate_wire_limits(frame.as_bytes()).is_ok());

        let message = format!("{prefix}{padding}x{suffix}");
        let frame = format!(r#"{{"kind":"command","message":{message}}}"#);
        assert!(matches!(
            validate_wire_limits(frame.as_bytes()),
            Err(WireLimitError::Command)
        ));
    }
}
