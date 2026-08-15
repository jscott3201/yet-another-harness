use chrono::{DateTime, Utc};

pub(super) fn validate_timestamp(ms: u64) -> Result<(), String> {
    const MAX_WIRE_TIMESTAMP_MS: u64 = 253_402_300_799_999;
    let valid = ms <= MAX_WIRE_TIMESTAMP_MS
        && i64::try_from(ms)
            .ok()
            .and_then(DateTime::<Utc>::from_timestamp_millis)
            .is_some();
    if valid {
        Ok(())
    } else {
        Err("logical clock exceeds RFC 3339 timestamp range".into())
    }
}

pub(super) fn wire_json_len(value: &serde_json::Value) -> usize {
    let mut value = value.clone();
    stringify_integers(&mut value);
    serde_json::to_vec(&value)
        .expect("semantic event payload serializes")
        .len()
}

pub(super) fn bounded_detail(detail: &str) -> String {
    detail
        .chars()
        .take(crate::protocol::MAX_ERROR_DETAIL_CHARS)
        .collect()
}

fn stringify_integers(value: &mut serde_json::Value) {
    match value {
        serde_json::Value::Number(number) if number.is_i64() || number.is_u64() => {
            *value = serde_json::Value::String(number.to_string());
        }
        serde_json::Value::Array(values) => values.iter_mut().for_each(stringify_integers),
        serde_json::Value::Object(values) => values.values_mut().for_each(stringify_integers),
        _ => {}
    }
}
