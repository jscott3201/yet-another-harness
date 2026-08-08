use super::*;
use crate::protocol::event::format_timestamp;
use std::sync::{Barrier, mpsc};
use std::thread;

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
fn poisoned_adapter_refuses_receipt_lookup() {
    let dir = tempfile::tempdir().unwrap();
    let store = crate::store::Store::create_project(dir.path(), "adapter-a", "p-1").unwrap();
    let adapter =
        InProcessAdapter::new(crate::funnel::Funnel::new(store, 0).unwrap(), "p-1").unwrap();
    adapter.funnel.poison("uncertain commit".into());
    let error = adapter
        .get_receipt(
            Scope {
                scope_kind: ScopeKind::Run,
                scope_id: "run-1".into(),
            },
            "command-1",
        )
        .unwrap_err();
    assert_eq!(error.kind, ErrorKind::Unavailable);
}

#[test]
fn adapter_startup_rejects_malformed_dispatch_receipt_claims() {
    let dir = tempfile::tempdir().unwrap();
    let store = crate::store::Store::create_project(dir.path(), "adapter-a", "p-1").unwrap();
    store.insert_test_receipt(
        "unit/u-1/command-1",
        &crate::store::ReceiptRecord {
            command_type: "unit.dispatch".into(),
            receipt_version: 1,
            request_digest: crate::ids::Digest::of_bytes(b"request").to_string(),
            principal_kind: "daemon".into(),
            principal_id: "daemon-local".into(),
            status: "completed".into(),
            result: serde_json::json!({ "unit_id": "u-1", "version": 2 }).to_string(),
            first_cursor: None,
            last_cursor: None,
        },
    );
    let funnel = crate::funnel::Funnel::new(store, 0).unwrap();
    assert!(InProcessAdapter::new(funnel, "p-1").is_err());
}

#[test]
fn receipt_lookup_waits_for_command_publication() {
    let dir = tempfile::tempdir().unwrap();
    let store = crate::store::Store::create_project(dir.path(), "adapter-a", "p-1").unwrap();
    let adapter = Arc::new(
        InProcessAdapter::new(crate::funnel::Funnel::new(store, 0).unwrap(), "p-1").unwrap(),
    );
    let command: Command = serde_json::from_value(serde_json::json!({
        "protocol_version": 1,
        "command_id": "command-1",
        "scope": { "scope_kind": "run", "scope_id": "run-1" },
        "payload_schema_version": 1,
        "target": { "aggregate_kind": "run", "aggregate_id": "run-1" },
        "expected_versions": [],
        "authority_epoch": "1",
        "command_type": "run.open",
        "payload": { "run_id": "run-1", "goal_work_item_id": "wi-1" },
        "request_digest": "blake3:0000000000000000000000000000000000000000000000000000000000000000"
    }))
    .unwrap();
    let mut command = command;
    command.request_digest = request_digest(&command).unwrap().to_string();
    let paused = Arc::new(Barrier::new(2));
    let release = Arc::new(Barrier::new(2));
    *adapter.command_test_hook.lock().unwrap() = Some(Arc::new({
        let paused = Arc::clone(&paused);
        let release = Arc::clone(&release);
        move || {
            paused.wait();
            release.wait();
        }
    }));

    let submitter = {
        let adapter = Arc::clone(&adapter);
        thread::spawn(move || adapter.submit(&command).unwrap())
    };
    paused.wait();
    let (sent, received) = mpsc::channel();
    let lookup = {
        let adapter = Arc::clone(&adapter);
        thread::spawn(move || {
            sent.send(adapter.get_receipt(
                Scope {
                    scope_kind: ScopeKind::Run,
                    scope_id: "run-1".into(),
                },
                "command-1",
            ))
            .unwrap();
        })
    };
    assert!(
        received
            .recv_timeout(std::time::Duration::from_millis(50))
            .is_err()
    );
    release.wait();
    assert_eq!(submitter.join().unwrap().outcome, ReceiptOutcome::Completed);
    assert_eq!(
        received.recv().unwrap().unwrap().outcome,
        ReceiptOutcome::Completed
    );
    lookup.join().unwrap();
}

#[test]
fn funnel_rejects_year_ten_thousand() {
    let dir = tempfile::tempdir().unwrap();
    let store = crate::store::Store::create(dir.path(), "clock-test").unwrap();
    assert!(crate::funnel::Funnel::new(store, 253_402_300_800_000).is_err());
}
