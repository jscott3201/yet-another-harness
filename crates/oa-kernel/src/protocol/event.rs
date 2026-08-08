use super::*;
use chrono::{DateTime, Utc};

pub(crate) fn project(record: crate::store::EventRecord) -> Result<Event, String> {
    let event_id = record.event_id.clone();
    if [
        Some(record.event_id.as_str()),
        Some(record.aggregate_id.as_str()),
        Some(record.command_id.as_str()),
        Some(record.actor_id.as_str()),
        record.causation_id.as_deref(),
        record.correlation_id.as_deref(),
    ]
    .into_iter()
    .flatten()
    .any(|identifier| !crate::ids::valid_wire_identifier(identifier))
    {
        return Err(format!(
            "durable event {event_id} contains an invalid wire identifier"
        ));
    }
    let mut payload: JsonObject = serde_json::from_str(&record.payload)
        .map_err(|error| format!("durable event {event_id} has invalid JSON: {error}"))?;
    payload.values_mut().for_each(stringify_integers);
    let ordinal = u32::try_from(record.ordinal)
        .map_err(|_| format!("durable event {event_id} has an invalid ordinal"))?;
    let projected = Event {
        event_id: record.event_id,
        cursor: DecimalU64::new(record.cursor),
        stream_class: StreamClass::DurableSemantic,
        aggregate_kind: record.aggregate_kind,
        aggregate_id: record.aggregate_id,
        aggregate_version: DecimalU64::new(record.aggregate_version),
        ordinal: BoundedU32::new(ordinal),
        event_kind: record.event_kind,
        event_version: EVENT_VERSION,
        occurred_at: Rfc3339Timestamp::new(format_timestamp(record.occurred_at_ms)?)
            .map_err(|error| format!("durable event {event_id} {error}"))?,
        actor: Actor {
            principal_kind: record.actor_kind,
            principal_id: record.actor_id,
        },
        command_id: Some(record.command_id),
        causation_id: record.causation_id,
        correlation_id: record.correlation_id,
        payload,
    };
    if serde_json::to_vec(&projected.payload).unwrap().len() > MAX_EVENT_PAYLOAD_BYTES
        || serde_json::to_vec(&projected).unwrap().len() > MAX_EVENT_BYTES
    {
        return Err(format!(
            "durable event {event_id} exceeds the protocol size limit"
        ));
    }
    Ok(projected)
}

pub(crate) fn format_timestamp(ms: u64) -> Result<String, String> {
    let ms = i64::try_from(ms).map_err(|_| "timestamp exceeds RFC 3339 range".to_owned())?;
    DateTime::<Utc>::from_timestamp_millis(ms)
        .ok_or_else(|| "timestamp exceeds RFC 3339 range".to_owned())
        .map(|timestamp| timestamp.format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string())
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
