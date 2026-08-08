use super::project::format_timestamp;
use super::*;

#[test]
fn timestamps_cover_minutes_and_calendar_dates() {
    assert_eq!(
        format_timestamp(61_000).unwrap(),
        "1970-01-01T00:01:01.000Z"
    );
    assert_eq!(
        format_timestamp(1_700_000_000_123).unwrap(),
        "2023-11-14T22:13:20.123Z"
    );
}

#[test]
fn malformed_durable_event_data_fails_projection() {
    let dir = tempfile::tempdir().unwrap();
    let store = crate::store::Store::create_project(dir.path(), "adapter-a", "p-1").unwrap();
    let adapter =
        InProcessAdapter::new(crate::funnel::Funnel::new(store, 0).unwrap(), "p-1").unwrap();
    let event = crate::store::EventRecord {
        cursor: 1,
        event_id: "event-1".into(),
        aggregate_kind: "run".into(),
        aggregate_id: "run-1".into(),
        aggregate_version: 1,
        ordinal: 0,
        event_kind: "run.opened".into(),
        payload: "{".into(),
        receipt_key: "run/run-1/command-1".into(),
        command_id: "command-1".into(),
        actor_kind: "daemon".into(),
        actor_id: "daemon-local".into(),
        occurred_at_ms: 0,
        causation_id: None,
        correlation_id: None,
    };
    assert!(adapter.event(event).unwrap_err().contains("invalid JSON"));
}

#[test]
fn adapter_startup_rejects_malformed_durable_events() {
    let dir = tempfile::tempdir().unwrap();
    let store = crate::store::Store::create_project(dir.path(), "adapter-a", "p-1").unwrap();
    store.insert_test_event(&crate::store::EventRecord {
        cursor: 1,
        event_id: "event-1".into(),
        aggregate_kind: "run".into(),
        aggregate_id: "run-1".into(),
        aggregate_version: 1,
        ordinal: 0,
        event_kind: "run.opened".into(),
        payload: "{".into(),
        receipt_key: "run/run-1/command-1".into(),
        command_id: "command-1".into(),
        actor_kind: "daemon".into(),
        actor_id: "daemon-local".into(),
        occurred_at_ms: 0,
        causation_id: None,
        correlation_id: None,
    });
    let funnel = crate::funnel::Funnel::new(store, 0).unwrap();
    assert!(InProcessAdapter::new(funnel, "p-1").is_err());
}

#[test]
fn resume_enforces_the_exact_response_byte_limit() {
    let dir = tempfile::tempdir().unwrap();
    let store = crate::store::Store::create_project(dir.path(), "adapter-a", "p-1").unwrap();
    let adapter =
        InProcessAdapter::new(crate::funnel::Funnel::new(store, 0).unwrap(), "p-1").unwrap();
    let mut projected = Vec::new();
    let mut cursor = 1;
    loop {
        let record = test_event(cursor, crate::protocol::MAX_EVENT_PAYLOAD_BYTES - 14);
        let event = adapter.event(record.clone()).unwrap();
        let mut candidate = projected.clone();
        candidate.push(event);
        let encoded = serde_json::to_vec(&ServerMessage::Events(candidate)).unwrap();
        if encoded.len() > crate::protocol::MAX_RESUME_BYTES {
            let empty_record = test_event(cursor, 0);
            let empty_event = adapter.event(empty_record.clone()).unwrap();
            let mut minimum = projected.clone();
            minimum.push(empty_event);
            let minimum_len = serde_json::to_vec(&ServerMessage::Events(minimum))
                .unwrap()
                .len();
            let padding = crate::protocol::MAX_RESUME_BYTES - minimum_len;
            let final_record = test_event(cursor, padding);
            projected.push(adapter.event(final_record.clone()).unwrap());
            adapter.funnel.store().insert_test_event(&final_record);
            break;
        }
        projected.push(adapter.event(record.clone()).unwrap());
        adapter.funnel.store().insert_test_event(&record);
        cursor += 1;
    }

    let request = serde_json::to_vec(&ClientMessage::Resume {
        project_id: "p-1".into(),
        after_cursor: DecimalU64::new(0),
    })
    .unwrap();
    let response = adapter.handle_json(&request);
    assert_eq!(response.len(), crate::protocol::MAX_RESUME_BYTES);
    assert_eq!(
        serde_json::from_slice::<ServerMessage>(&response).unwrap(),
        ServerMessage::Events(projected)
    );

    let overflow = test_event(cursor + 1, 0);
    adapter.funnel.store().insert_test_event(&overflow);
    let ServerMessage::Error(error) =
        serde_json::from_slice::<ServerMessage>(&adapter.handle_json(&request)).unwrap()
    else {
        panic!("cumulative response overflow must be rejected")
    };
    assert_eq!(error.kind, ErrorKind::ResourceExhausted);
}

fn test_event(cursor: u64, padding: usize) -> crate::store::EventRecord {
    crate::store::EventRecord {
        cursor,
        event_id: format!("event-{cursor}"),
        aggregate_kind: "run".into(),
        aggregate_id: format!("run-{cursor}"),
        aggregate_version: 1,
        ordinal: 0,
        event_kind: "run.opened".into(),
        payload: serde_json::to_string(&serde_json::json!({ "padding": "x".repeat(padding) }))
            .unwrap(),
        receipt_key: format!("run/run-{cursor}/command-{cursor}"),
        command_id: format!("command-{cursor}"),
        actor_kind: "daemon".into(),
        actor_id: "daemon-local".into(),
        occurred_at_ms: 0,
        causation_id: None,
        correlation_id: None,
    }
}

#[test]
fn funnel_rejects_an_unrepresentable_clock_tick() {
    let dir = tempfile::tempdir().unwrap();
    let store = crate::store::Store::create(dir.path(), "clock-test").unwrap();
    let funnel = crate::funnel::Funnel::new(store, 0).unwrap();
    assert!(funnel.tick(u64::MAX).is_err());
}

#[test]
fn funnel_rejects_year_ten_thousand() {
    let dir = tempfile::tempdir().unwrap();
    let store = crate::store::Store::create(dir.path(), "clock-test").unwrap();
    assert!(crate::funnel::Funnel::new(store, 253_402_300_800_000).is_err());
}
