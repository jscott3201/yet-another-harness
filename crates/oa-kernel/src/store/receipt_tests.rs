use super::*;
use crate::effect::{EffectIntent, EffectState, RetryClass, ReversibilityClass, TargetEnumeration};
use crate::ids::{AttemptEpoch, AuthorityEpoch, Digest, Stamp, Uuid7};

fn receipt_pairs(command_type: &str, receipt_version: u64) -> PropertyMap {
    PropertyMap::from_pairs([
        (db("receipt_key"), Value::String(db("run/run-1/command-1"))),
        (db("command_type"), Value::String(db(command_type))),
        (db("receipt_version"), Value::Uint(receipt_version)),
        (
            db("request_digest"),
            Value::String(db(Digest::of_bytes(b"request").as_str())),
        ),
        (db("principal_kind"), Value::String(db("daemon"))),
        (db("principal_id"), Value::String(db("daemon-local"))),
        (db("status"), Value::String(db("completed"))),
        (db("result"), Value::String(db("{}"))),
    ])
    .unwrap()
}

fn insert_receipt(store: &Store, pairs: PropertyMap) {
    let mut txn = store.shared.begin_write();
    txn.mutator()
        .create_node(LabelSet::single(db("Receipt")), pairs)
        .unwrap();
    txn.commit().unwrap();
}

#[allow(clippy::too_many_arguments)]
fn insert_completed_with_event(
    store: &Store,
    key: &str,
    command_type: &str,
    result: &str,
    event_kind: &str,
    aggregate_kind: &str,
    aggregate_id: &str,
    aggregate_version: u64,
    payload: &str,
) {
    let command_id = key.rsplit('/').next().unwrap();
    let holder = matches!(
        command_type,
        "unit.progress_report"
            | "token.reissue"
            | "effect.prepare"
            | "effect.dispatch"
            | "effect.record_dispatched"
    );
    let (principal_kind, principal_id) = if holder {
        ("agent", "holder-1")
    } else {
        ("daemon", "daemon-local")
    };
    store.insert_test_event(&EventRecord {
        cursor: 1,
        event_id: "event-1".into(),
        aggregate_kind: aggregate_kind.into(),
        aggregate_id: aggregate_id.into(),
        aggregate_version,
        ordinal: 0,
        event_kind: event_kind.into(),
        payload: payload.into(),
        receipt_key: key.into(),
        command_id: command_id.into(),
        actor_kind: principal_kind.into(),
        actor_id: principal_id.into(),
        occurred_at_ms: 1,
        causation_id: None,
        correlation_id: None,
    });
    store.insert_test_receipt(
        key,
        &ReceiptRecord {
            command_type: command_type.into(),
            receipt_version: 1,
            request_digest: Digest::of_bytes(b"request").to_string(),
            principal_kind: principal_kind.into(),
            principal_id: principal_id.into(),
            status: "completed".into(),
            result: result.into(),
            first_cursor: Some(1),
            last_cursor: Some(1),
        },
    );
}

fn insert_effect(store: &Store, operation_key: &str, unit_id: &str) {
    let intent_id = Uuid7::mint(1, 1);
    let intent = EffectIntent {
        effect_intent_id: intent_id,
        version: 1,
        unit_id: unit_id.into(),
        attempt_epoch: AttemptEpoch(1),
        stamp: Stamp(0),
        authority_epoch: AuthorityEpoch(1),
        adapter_id: "test".into(),
        adapter_version: "1".into(),
        operation_key: operation_key.into(),
        logical_operation_id: Uuid7::mint(1, 2),
        request_digest: Digest::of_bytes(b"effect request"),
        retry_class: RetryClass::SafeIdempotent,
        reversibility_class: ReversibilityClass::Bufferable,
        approval_ref: None,
        policy_snapshot_digest: Digest::of_bytes(b"policy"),
        state: EffectState::Prepared,
        terminal: None,
        target_enumeration: TargetEnumeration::PostHoc,
        targets: Vec::new(),
        decomposable: false,
        parent_effect_intent_id: None,
        compensation_intent_id: None,
        dispatched_at: None,
        settled_at: None,
        next_reconcile_at: None,
    };
    let mut txn = store.shared.begin_write();
    txn.mutator()
        .create_node(
            LabelSet::single(db("Effect")),
            PropertyMap::from_pairs([
                (db("operation_key"), Value::String(db(operation_key))),
                (
                    db("effect_intent_id"),
                    Value::String(db(&intent_id.to_string())),
                ),
                (db("unit_id"), Value::String(db(unit_id))),
                (db("attempt_epoch"), Value::Uint(1)),
                (db("version"), Value::Uint(1)),
                (db("state"), Value::String(db("prepared"))),
                (
                    db("record"),
                    Value::String(db(&serde_json::to_string(&intent).unwrap())),
                ),
            ])
            .unwrap(),
        )
        .unwrap();
    txn.commit().unwrap();
}

#[test]
fn recovery_rejects_unknown_receipt_command_type() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::create(dir.path(), "inst-1").unwrap();
    insert_receipt(&store, receipt_pairs("unknown.command", 1));
    drop(store);
    assert!(matches!(
        Store::recover(dir.path(), "inst-2"),
        Err(StoreError::Internal(_))
    ));
}

#[test]
fn recovery_rejects_unsupported_receipt_version() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::create(dir.path(), "inst-1").unwrap();
    insert_receipt(&store, receipt_pairs("run.open", 2));
    drop(store);
    assert!(matches!(
        Store::recover(dir.path(), "inst-2"),
        Err(StoreError::Internal(_))
    ));
}

#[test]
fn recovery_rejects_semantic_receipt_corruption_before_claiming_authority() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::create(dir.path(), "inst-1").unwrap();
    let mut receipt = receipt_pairs("run.open", 1);
    receipt
        .set(
            db("result"),
            Value::String(db(r#"{"run_id":"other","version":999}"#)),
        )
        .unwrap();
    insert_receipt(&store, receipt);
    drop(store);

    assert!(matches!(
        Store::recover(dir.path(), "inst-2"),
        Err(StoreError::Internal(_))
    ));
    let shared =
        SharedGraph::recover_closed(dir.path(), GraphId::new(GRAPH_ID), graph_type()).unwrap();
    let read = shared.read();
    let authority = read
        .live_nodes()
        .iter()
        .filter_map(|row| read.node_id_for_row(RowIndex::new(row)))
        .find(|node| {
            read.node_labels(*node)
                .is_some_and(|labels| labels.contains(&db("Authority")))
        })
        .unwrap();
    assert_eq!(
        read.node_properties(authority)
            .unwrap()
            .get(&db("authority_epoch"))
            .and_then(value_u64),
        Some(1)
    );
}

#[test]
fn recovery_rejects_a_receipt_for_another_project() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::create_project(dir.path(), "inst-1", "p-1").unwrap();
    let mut receipt = receipt_pairs("effect.prepare", 1);
    receipt
        .set(
            db("receipt_key"),
            Value::String(db("project/p-2/command-1")),
        )
        .unwrap();
    receipt
        .set(
            db("result"),
            Value::String(db(
                r#"{"operation_key":"op-1","effect_intent_id":"effect-1","version":1,"existing":true}"#,
            )),
        )
        .unwrap();
    insert_receipt(&store, receipt);
    drop(store);
    assert!(matches!(
        Store::recover(dir.path(), "inst-2"),
        Err(StoreError::Internal(_))
    ));
}

#[test]
fn recovery_rejects_an_unbound_no_event_prepare_receipt() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::create(dir.path(), "inst-1").unwrap();
    let mut receipt = receipt_pairs("effect.prepare", 1);
    receipt
        .set(db("receipt_key"), Value::String(db("global/g/command-1")))
        .unwrap();
    receipt
        .set(
            db("result"),
            Value::String(db(
                r#"{"operation_key":"missing","effect_intent_id":"effect-1","version":1,"existing":true}"#,
            )),
        )
        .unwrap();
    insert_receipt(&store, receipt);
    drop(store);
    assert!(matches!(
        Store::recover(dir.path(), "inst-2"),
        Err(StoreError::Internal(_))
    ));
}

#[test]
fn recovery_rejects_extra_rejection_result_fields() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::create(dir.path(), "inst-1").unwrap();
    let mut receipt = receipt_pairs("run.open", 1);
    receipt
        .set(db("status"), Value::String(db("rejected")))
        .unwrap();
    receipt
        .set(
            db("result"),
            Value::String(db(
                r#"{"error_kind":"invalid_request","detail":"bad","extra":true}"#,
            )),
        )
        .unwrap();
    insert_receipt(&store, receipt);
    drop(store);
    assert!(matches!(
        Store::recover(dir.path(), "inst-2"),
        Err(StoreError::Internal(_))
    ));
}

#[test]
fn recovery_rejects_non_durable_rejection_kinds() {
    for kind in ["outcome_unknown", "internal"] {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::create(dir.path(), "inst-1").unwrap();
        let mut receipt = receipt_pairs("run.open", 1);
        receipt
            .set(db("status"), Value::String(db("rejected")))
            .unwrap();
        receipt
            .set(
                db("result"),
                Value::String(db(&format!(r#"{{"error_kind":"{kind}","detail":"bad"}}"#))),
            )
            .unwrap();
        insert_receipt(&store, receipt);
        drop(store);
        assert!(matches!(
            Store::recover(dir.path(), "inst-2"),
            Err(StoreError::Internal(_))
        ));
    }
}

#[test]
fn recovery_rejects_unbounded_rejection_detail() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::create(dir.path(), "inst-1").unwrap();
    let mut receipt = receipt_pairs("run.open", 1);
    receipt
        .set(db("status"), Value::String(db("rejected")))
        .unwrap();
    receipt
        .set(
            db("result"),
            Value::String(db(&serde_json::json!({
                "error_kind": "invalid_request",
                "detail": "x".repeat(crate::protocol::MAX_ERROR_DETAIL_CHARS + 1),
            })
            .to_string())),
        )
        .unwrap();
    insert_receipt(&store, receipt);
    drop(store);
    assert!(matches!(
        Store::recover(dir.path(), "inst-2"),
        Err(StoreError::Internal(_))
    ));
}

#[test]
fn recovery_rejects_run_close_result_that_disagrees_with_its_event() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::create(dir.path(), "inst-1").unwrap();
    insert_completed_with_event(
        &store,
        "run/run-1/command-1",
        "run.close",
        r#"{"run_id":"run-1","version":2,"status":"closed_failure"}"#,
        "run.closed",
        "run",
        "run-1",
        2,
        r#"{"outcome":"closed_success"}"#,
    );
    drop(store);
    assert!(matches!(
        Store::recover(dir.path(), "inst-2"),
        Err(StoreError::Internal(_))
    ));
}

#[test]
fn recovery_rejects_effect_dispatch_result_that_disagrees_with_its_event_branch() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::create(dir.path(), "inst-1").unwrap();
    insert_effect(&store, "op-1", "unit-1");
    insert_completed_with_event(
        &store,
        "unit/unit-1/command-1",
        "effect.dispatch",
        r#"{"operation_key":"op-1","version":2,"state":"dispatching"}"#,
        "effect.settled",
        "effect",
        "op-1",
        2,
        r#"{"terminal":"cancelled"}"#,
    );
    drop(store);
    assert!(matches!(
        Store::recover(dir.path(), "inst-2"),
        Err(StoreError::Internal(_))
    ));
}

#[test]
fn recovery_rejects_event_bearing_existing_prepare_receipt() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::create(dir.path(), "inst-1").unwrap();
    insert_effect(&store, "op-1", "unit-1");
    insert_completed_with_event(
        &store,
        "unit/unit-1/command-1",
        "effect.prepare",
        &format!(
            r#"{{"operation_key":"op-1","effect_intent_id":"{}","version":1,"existing":true}}"#,
            Uuid7::mint(1, 1)
        ),
        "effect.prepared",
        "effect",
        "op-1",
        1,
        &format!(r#"{{"effect_intent_id":"{}"}}"#, Uuid7::mint(1, 1)),
    );
    drop(store);
    assert!(matches!(
        Store::recover(dir.path(), "inst-2"),
        Err(StoreError::Internal(_))
    ));
}

#[test]
fn recovery_rejects_prepare_receipt_scoped_to_another_unit() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::create(dir.path(), "inst-1").unwrap();
    insert_effect(&store, "op-1", "unit-1");
    let mut receipt = receipt_pairs("effect.prepare", 1);
    receipt
        .set(
            db("receipt_key"),
            Value::String(db("unit/unit-2/command-1")),
        )
        .unwrap();
    receipt
        .set(
            db("result"),
            Value::String(db(&format!(
                r#"{{"operation_key":"op-1","effect_intent_id":"{}","version":1,"existing":true}}"#,
                Uuid7::mint(1, 1)
            ))),
        )
        .unwrap();
    insert_receipt(&store, receipt);
    drop(store);
    assert!(matches!(
        Store::recover(dir.path(), "inst-2"),
        Err(StoreError::Internal(_))
    ));
}

#[test]
fn recovery_rejects_unknown_cancellation_result_discriminants() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::create(dir.path(), "inst-1").unwrap();
    insert_completed_with_event(
        &store,
        "global/g/command-1",
        "cancel.request",
        r#"{"cancel_request_id":"cancel-1","version":1,"status":"requested","root_kind":"unknown","root_id":"run-1"}"#,
        "cancel_request.requested",
        "cancel_request",
        "cancel-1",
        1,
        r#"{"root_kind":"unknown","root_id":"run-1","status":"requested"}"#,
    );
    drop(store);
    assert!(matches!(
        Store::recover(dir.path(), "inst-2"),
        Err(StoreError::Internal(_))
    ));
}

#[test]
fn recovery_rejects_completed_receipt_with_impossible_scope() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::create(dir.path(), "inst-1").unwrap();
    insert_completed_with_event(
        &store,
        "unit/unit-1/command-1",
        "run.open",
        r#"{"run_id":"run-1","version":1}"#,
        "run.opened",
        "run",
        "run-1",
        1,
        r#"{"goal_work_item_id":"wi-1"}"#,
    );
    drop(store);
    assert!(matches!(
        Store::recover(dir.path(), "inst-2"),
        Err(StoreError::Internal(_))
    ));
}

#[test]
fn recovery_rejects_creation_event_without_its_aggregate_row() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::create(dir.path(), "inst-1").unwrap();
    insert_completed_with_event(
        &store,
        "run/run-1/command-1",
        "run.open",
        r#"{"run_id":"run-1","version":1}"#,
        "run.opened",
        "run",
        "run-1",
        1,
        r#"{"goal_work_item_id":"wi-1"}"#,
    );
    drop(store);
    assert!(matches!(
        Store::recover(dir.path(), "inst-2"),
        Err(StoreError::Internal(_))
    ));
}

#[test]
fn recovery_rejects_unexpected_event_payload_fields() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::create(dir.path(), "inst-1").unwrap();
    insert_completed_with_event(
        &store,
        "run/run-1/command-1",
        "run.open",
        r#"{"run_id":"run-1","version":1}"#,
        "run.opened",
        "run",
        "run-1",
        1,
        r#"{"goal_work_item_id":"wi-1","attempt_token":"secret"}"#,
    );
    drop(store);
    assert!(matches!(
        Store::recover(dir.path(), "inst-2"),
        Err(StoreError::Internal(_))
    ));
}

#[test]
fn recovery_rejects_non_wire_safe_event_before_claiming_authority() {
    for (occurred_at_ms, payload) in [
        (
            u64::MAX,
            r#"{"work_item_id":"w1","run_id":"r1"}"#.to_owned(),
        ),
        (
            1,
            serde_json::json!({ "padding": "x".repeat(crate::protocol::MAX_EVENT_PAYLOAD_BYTES) })
                .to_string(),
        ),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::create(dir.path(), "inst-1").unwrap();
        let event = EventRecord {
            cursor: 1,
            event_id: "event-1".into(),
            aggregate_kind: "unit".into(),
            aggregate_id: "unit-1".into(),
            aggregate_version: 1,
            ordinal: 0,
            event_kind: "unit.admitted".into(),
            payload,
            receipt_key: "unit/unit-1/command-1".into(),
            command_id: "command-1".into(),
            actor_kind: "daemon".into(),
            actor_id: "daemon-local".into(),
            occurred_at_ms,
            causation_id: None,
            correlation_id: None,
        };
        store.insert_test_event(&event);
        store.insert_test_receipt(
            "unit/unit-1/command-1",
            &ReceiptRecord {
                command_type: "unit.admit".into(),
                receipt_version: 1,
                request_digest: Digest::of_bytes(b"request").to_string(),
                principal_kind: "daemon".into(),
                principal_id: "daemon-local".into(),
                status: "completed".into(),
                result: r#"{"unit_id":"unit-1","version":1}"#.into(),
                first_cursor: Some(1),
                last_cursor: Some(1),
            },
        );
        drop(store);
        assert!(matches!(
            Store::recover(dir.path(), "inst-2"),
            Err(StoreError::Internal(_))
        ));
        let shared =
            SharedGraph::recover_closed(dir.path(), GraphId::new(GRAPH_ID), graph_type()).unwrap();
        let read = shared.read();
        let authority = read
            .live_nodes()
            .iter()
            .filter_map(|row| read.node_id_for_row(RowIndex::new(row)))
            .find(|node| {
                read.node_labels(*node)
                    .is_some_and(|labels| labels.contains(&db("Authority")))
            })
            .unwrap();
        assert_eq!(
            read.node_properties(authority)
                .unwrap()
                .get(&db("authority_epoch"))
                .and_then(value_u64),
            Some(1)
        );
    }
}

#[test]
fn cancellation_park_event_must_match_the_delivered_effect() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::create(dir.path(), "inst-1").unwrap();
    insert_effect(&store, "op-1", "unit-1");
    let result = serde_json::json!({ "member_id": Uuid7::mint(1, 99).to_string() });
    let error = super::receipt_event::validate_cancel_park(
        &store,
        result.as_object().unwrap(),
        &EventRecord {
            cursor: 2,
            event_id: "event-2".into(),
            aggregate_kind: "effect".into(),
            aggregate_id: "op-1".into(),
            aggregate_version: 1,
            ordinal: 0,
            event_kind: "effect.reconciling".into(),
            payload: r#"{"next_reconcile_at":null,"source":"cancel_delivery"}"#.into(),
            receipt_key: "global/g/command-1".into(),
            command_id: "command-1".into(),
            actor_kind: "daemon".into(),
            actor_id: "daemon-local".into(),
            occurred_at_ms: 1,
            causation_id: None,
            correlation_id: None,
        },
    )
    .unwrap_err();
    assert!(matches!(error, StoreError::Internal(_)));
}

#[test]
fn recovery_rejects_holder_receipts_outside_the_unit_namespace() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::create(dir.path(), "inst-1").unwrap();
    let mut receipt = receipt_pairs("unit.progress_report", 1);
    receipt
        .set(db("receipt_key"), Value::String(db("global/g/command-1")))
        .unwrap();
    receipt
        .set(db("principal_kind"), Value::String(db("agent")))
        .unwrap();
    receipt
        .set(
            db("result"),
            Value::String(db(r#"{"unit_id":"unit-1","version":1}"#)),
        )
        .unwrap();
    insert_receipt(&store, receipt);
    drop(store);
    assert!(matches!(
        Store::recover(dir.path(), "inst-2"),
        Err(StoreError::Internal(_))
    ));
}

#[test]
fn recovery_rejects_impossible_effect_dispatch_settlement_payload() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::create(dir.path(), "inst-1").unwrap();
    insert_effect(&store, "op-1", "unit-1");
    insert_completed_with_event(
        &store,
        "unit/unit-1/command-1",
        "effect.dispatch",
        r#"{"operation_key":"op-1","version":2,"state":"settled"}"#,
        "effect.settled",
        "effect",
        "op-1",
        2,
        r#"{"terminal":"succeeded","rule_4":false}"#,
    );
    drop(store);
    assert!(matches!(
        Store::recover(dir.path(), "inst-2"),
        Err(StoreError::Internal(_))
    ));
}
