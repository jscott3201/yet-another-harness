use super::*;
use crate::cancel::{
    CancelKind, CancelPolicy, CancelReason, CancelRequest, CancelScope, CancelStatus,
};
use crate::ids::{Digest, Uuid7};

fn create_raw(store: &Store, label: &str, pairs: PropertyMap) -> NodeId {
    let mut transaction = store.shared.begin_write();
    let node = transaction
        .mutator()
        .create_node(LabelSet::single(db(label)), pairs)
        .unwrap();
    transaction.commit().unwrap();
    node
}

fn update_raw(store: &Store, node: NodeId, field: &str, value: Value) {
    let mut transaction = store.shared.begin_write();
    transaction
        .mutator()
        .update_node(node, no_labels(), props_set([(db(field), value)]))
        .unwrap();
    transaction.commit().unwrap();
}

#[test]
fn recovery_rejects_wire_invalid_event_identifiers() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::create(dir.path(), "inst-1").unwrap();
    let mut event = super::tests::event_pairs("e1", 1, "u1", 1);
    event
        .set(db("aggregate_id"), Value::String(db("invalid/id")))
        .unwrap();
    event
        .set(db("agg_ver_ord"), Value::String(db("unit/invalid/id/1/0")))
        .unwrap();
    create_raw(&store, "Event", event);
    let mut receipt = super::review_tests::pairs_for("Receipt", "unit/u1/e1");
    receipt
        .set(db("command_type"), Value::String(db("unit.admit")))
        .unwrap();
    receipt
        .set(db("principal_kind"), Value::String(db("daemon")))
        .unwrap();
    receipt
        .set(db("principal_id"), Value::String(db("test")))
        .unwrap();
    receipt
        .set(
            db("result"),
            Value::String(db(r#"{"unit_id":"u1","version":1}"#)),
        )
        .unwrap();
    receipt.set(db("first_cursor"), Value::Uint(1)).unwrap();
    receipt.set(db("last_cursor"), Value::Uint(1)).unwrap();
    create_raw(&store, "Receipt", receipt);
    drop(store);
    match Store::recover(dir.path(), "inst-2") {
        Err(StoreError::Internal(detail)) => {
            assert!(detail.contains("invalid wire identifier"), "{detail}")
        }
        Err(error) => panic!("unexpected recovery error: {error:?}"),
        Ok(_) => panic!("wire-invalid event identifier recovered"),
    }
}

pub(super) fn cancellation_delivery_history(
    request_id: &str,
    aggregate_version: u64,
    cursor: u64,
    status: &str,
) -> (PropertyMap, PropertyMap) {
    let command_id = "cancel-delivery";
    let receipt_key = format!("global/g/{command_id}");
    let event = PropertyMap::from_pairs([
        (db("event_id"), Value::String(db("cancel-delivery-event"))),
        (db("cursor"), Value::Uint(cursor)),
        (
            db("agg_ver_ord"),
            Value::String(db(&format!(
                "cancel_request/{request_id}/{aggregate_version}/0"
            ))),
        ),
        (db("aggregate_kind"), Value::String(db("cancel_request"))),
        (db("aggregate_id"), Value::String(db(request_id))),
        (db("aggregate_version"), Value::Uint(aggregate_version)),
        (db("ordinal"), Value::Uint(0)),
        (
            db("event_kind"),
            Value::String(db("cancel_request.delivered")),
        ),
        (
            db("payload"),
            Value::String(db(&serde_json::json!({
                "member_id": "u1",
                "delivered_at": 2,
                "observed_at": 3,
                "outcome": "observed_stopped",
                "status": status,
            })
            .to_string())),
        ),
        (db("receipt_key"), Value::String(db(&receipt_key))),
        (db("command_id"), Value::String(db(command_id))),
        (db("actor_kind"), Value::String(db("daemon"))),
        (db("actor_id"), Value::String(db("daemon-local"))),
        (db("occurred_at_ms"), Value::Uint(3)),
    ])
    .unwrap();
    let receipt = PropertyMap::from_pairs([
        (db("receipt_key"), Value::String(db(&receipt_key))),
        (
            db("command_type"),
            Value::String(db("cancel.record_delivery")),
        ),
        (db("receipt_version"), Value::Uint(1)),
        (
            db("request_digest"),
            Value::String(db(Digest::of_bytes(command_id.as_bytes()).as_str())),
        ),
        (db("principal_kind"), Value::String(db("daemon"))),
        (db("principal_id"), Value::String(db("daemon-local"))),
        (db("status"), Value::String(db("completed"))),
        (
            db("result"),
            Value::String(db(&serde_json::json!({
                "cancel_request_id": request_id,
                "version": aggregate_version,
                "member_id": "u1",
                "outcome": "observed_stopped",
                "status": status,
            })
            .to_string())),
        ),
        (db("first_cursor"), Value::Uint(cursor)),
        (db("last_cursor"), Value::Uint(cursor)),
    ])
    .unwrap();
    (event, receipt)
}

pub(super) fn cancellation_request_history(
    request_id: &str,
    cursor: u64,
) -> (PropertyMap, PropertyMap) {
    let command_id = "cancel-request";
    let receipt_key = format!("global/g/{command_id}");
    let event = PropertyMap::from_pairs([
        (db("event_id"), Value::String(db("cancel-request-event"))),
        (db("cursor"), Value::Uint(cursor)),
        (
            db("agg_ver_ord"),
            Value::String(db(&format!("cancel_request/{request_id}/1/0"))),
        ),
        (db("aggregate_kind"), Value::String(db("cancel_request"))),
        (db("aggregate_id"), Value::String(db(request_id))),
        (db("aggregate_version"), Value::Uint(1)),
        (db("ordinal"), Value::Uint(0)),
        (
            db("event_kind"),
            Value::String(db("cancel_request.requested")),
        ),
        (
            db("payload"),
            Value::String(db(
                r#"{"root_kind":"execution_unit","root_id":"u1","reason":"owner_request","policy":"attached_cascade","status":"requested","members":1}"#,
            )),
        ),
        (db("receipt_key"), Value::String(db(&receipt_key))),
        (db("command_id"), Value::String(db(command_id))),
        (db("actor_kind"), Value::String(db("daemon"))),
        (db("actor_id"), Value::String(db("daemon-local"))),
        (db("occurred_at_ms"), Value::Uint(1)),
    ])
    .unwrap();
    let receipt = PropertyMap::from_pairs([
        (db("receipt_key"), Value::String(db(&receipt_key))),
        (db("command_type"), Value::String(db("cancel.request"))),
        (db("receipt_version"), Value::Uint(1)),
        (
            db("request_digest"),
            Value::String(db(Digest::of_bytes(command_id.as_bytes()).as_str())),
        ),
        (db("principal_kind"), Value::String(db("daemon"))),
        (db("principal_id"), Value::String(db("daemon-local"))),
        (db("status"), Value::String(db("completed"))),
        (
            db("result"),
            Value::String(db(&serde_json::json!({
                "cancel_request_id": request_id,
                "version": 1,
                "status": "requested",
                "root_kind": "execution_unit",
                "root_id": "u1",
            })
            .to_string())),
        ),
        (db("first_cursor"), Value::Uint(cursor)),
        (db("last_cursor"), Value::Uint(cursor)),
    ])
    .unwrap();
    (event, receipt)
}

#[test]
fn recovery_rejects_ineligible_empty_cancellation_roots() {
    for (root_exists, policy) in [
        (false, CancelPolicy::RootOnly),
        (true, CancelPolicy::RootOnly),
        (true, CancelPolicy::AttachedCascade),
    ] {
        let directory = tempfile::tempdir().unwrap();
        let store = Store::create(directory.path(), "inst-1").unwrap();
        if root_exists {
            create_raw(
                &store,
                "Run",
                super::review_tests::pairs_for("Run", "missing-run"),
            );
        }
        let request_id = Uuid7::mint(1, 1);
        let scope = CancelScope::freeze(CancelKind::Run, "missing-run", vec![]).unwrap();
        let request = CancelRequest {
            cancel_request_id: request_id,
            reason: CancelReason::OwnerRequest,
            policy,
            scope: scope.clone(),
            status: CancelStatus::Settled,
            requested_at: 1,
            requested_by_kind: "daemon".into(),
        };
        let pairs = PropertyMap::from_pairs([
            (
                db("cancel_request_id"),
                Value::String(db(&request_id.to_string())),
            ),
            (db("version"), Value::Uint(1)),
            (db("root_kind"), Value::String(db("run"))),
            (db("root_id"), Value::String(db("missing-run"))),
            (
                db("policy"),
                Value::String(db(match policy {
                    CancelPolicy::RootOnly => "root_only",
                    CancelPolicy::AttachedCascade => "attached_cascade",
                })),
            ),
            (db("reason"), Value::String(db("owner_request"))),
            (
                db("scope"),
                Value::String(db(&serde_json::to_string(&scope).unwrap())),
            ),
            (db("status"), Value::String(db("settled"))),
            (
                db("record"),
                Value::String(db(&serde_json::to_string(&request).unwrap())),
            ),
        ])
        .unwrap();
        create_raw(&store, "CancelRequest", pairs);
        drop(store);
        assert!(matches!(
            Store::recover(directory.path(), "inst-2"),
            Err(StoreError::Internal(_))
        ));
    }
}

#[test]
fn recovery_rejects_delivery_outside_frozen_leaf_first_version() {
    let directory = tempfile::tempdir().unwrap();
    let store = Store::create(directory.path(), "inst-1").unwrap();
    create_raw(&store, "Unit", super::review_tests::pairs_for("Unit", "u1"));
    let (request_id, request_pairs, _, delivery_pairs) =
        super::review_tests::recovery_cancel_pairs();
    let request = create_raw(&store, "CancelRequest", request_pairs);
    let mut record: CancelRequest = serde_json::from_str(
        &store
            .shared
            .read()
            .node_properties(request)
            .unwrap()
            .get(&db("record"))
            .and_then(value_str)
            .unwrap(),
    )
    .unwrap();
    record.status = CancelStatus::Settled;
    update_raw(&store, request, "version", Value::Uint(2));
    update_raw(&store, request, "status", Value::String(db("settled")));
    update_raw(
        &store,
        request,
        "record",
        Value::String(db(&serde_json::to_string(&record).unwrap())),
    );
    create_raw(&store, "CancelDelivery", delivery_pairs);
    let (event, receipt) = cancellation_delivery_history(&request_id, 3, 1, "settled");
    create_raw(&store, "Event", event);
    create_raw(&store, "Receipt", receipt);
    drop(store);
    assert!(matches!(
        Store::recover(directory.path(), "inst-2"),
        Err(StoreError::Internal(_))
    ));
}

#[test]
fn recovery_rejects_foreign_tree_scope_members() {
    let directory = tempfile::tempdir().unwrap();
    let store = Store::create(directory.path(), "inst-1").unwrap();
    create_raw(&store, "Run", super::review_tests::pairs_for("Run", "r1"));
    let mut unit = super::review_tests::pairs_for("Unit", "u2");
    unit.set(db("run_id"), Value::String(db("r2"))).unwrap();
    create_raw(&store, "Unit", unit);
    let request_id = Uuid7::mint(1, 1);
    let scope = CancelScope::freeze(
        CancelKind::Run,
        "r1",
        vec![crate::cancel::MemberInput {
            member_kind: CancelKind::ExecutionUnit,
            member_id: "u2".into(),
            attachment: crate::cancel::Attachment::Attached,
        }],
    )
    .unwrap();
    let request = CancelRequest {
        cancel_request_id: request_id,
        reason: CancelReason::OwnerRequest,
        policy: CancelPolicy::AttachedCascade,
        scope: scope.clone(),
        status: CancelStatus::Requested,
        requested_at: 1,
        requested_by_kind: "daemon".into(),
    };
    let pairs = PropertyMap::from_pairs([
        (
            db("cancel_request_id"),
            Value::String(db(&request_id.to_string())),
        ),
        (db("version"), Value::Uint(1)),
        (db("root_kind"), Value::String(db("run"))),
        (db("root_id"), Value::String(db("r1"))),
        (db("policy"), Value::String(db("attached_cascade"))),
        (db("reason"), Value::String(db("owner_request"))),
        (
            db("scope"),
            Value::String(db(&serde_json::to_string(&scope).unwrap())),
        ),
        (db("status"), Value::String(db("requested"))),
        (
            db("record"),
            Value::String(db(&serde_json::to_string(&request).unwrap())),
        ),
    ])
    .unwrap();
    create_raw(&store, "CancelRequest", pairs);
    drop(store);
    assert!(matches!(
        Store::recover(directory.path(), "inst-2"),
        Err(StoreError::Internal(_))
    ));
}

#[test]
fn recovery_rejects_nonempty_root_without_creation_history() {
    let directory = tempfile::tempdir().unwrap();
    let store = Store::create(directory.path(), "inst-1").unwrap();
    create_raw(&store, "Unit", super::review_tests::pairs_for("Unit", "u1"));
    let (_, request_pairs, _, _) = super::review_tests::recovery_cancel_pairs();
    create_raw(&store, "CancelRequest", request_pairs);
    drop(store);
    assert!(matches!(
        Store::recover(directory.path(), "inst-2"),
        Err(StoreError::Internal(_))
    ));
}

#[test]
fn recovery_rejects_delivery_status_outside_its_lifecycle_phase() {
    let directory = tempfile::tempdir().unwrap();
    let store = Store::create(directory.path(), "inst-1").unwrap();
    create_raw(&store, "Unit", super::review_tests::pairs_for("Unit", "u1"));
    let (request_id, request_pairs, _, delivery_pairs) =
        super::review_tests::recovery_cancel_pairs();
    let request = create_raw(&store, "CancelRequest", request_pairs);
    let mut record: CancelRequest = serde_json::from_str(
        &store
            .shared
            .read()
            .node_properties(request)
            .unwrap()
            .get(&db("record"))
            .and_then(value_str)
            .unwrap(),
    )
    .unwrap();
    record.status = CancelStatus::Settled;
    update_raw(&store, request, "version", Value::Uint(2));
    update_raw(&store, request, "status", Value::String(db("settled")));
    update_raw(
        &store,
        request,
        "record",
        Value::String(db(&serde_json::to_string(&record).unwrap())),
    );
    create_raw(&store, "CancelDelivery", delivery_pairs);
    let (request_event, request_receipt) = cancellation_request_history(&request_id, 1);
    create_raw(&store, "Event", request_event);
    create_raw(&store, "Receipt", request_receipt);
    let (event, receipt) = cancellation_delivery_history(&request_id, 2, 2, "delivering");
    create_raw(&store, "Event", event);
    create_raw(&store, "Receipt", receipt);
    drop(store);
    assert!(matches!(
        Store::recover(directory.path(), "inst-2"),
        Err(StoreError::Internal(_))
    ));
}

#[test]
fn recovery_rejects_effect_attempt_ownership_drift() {
    let directory = tempfile::tempdir().unwrap();
    let store = Store::create(directory.path(), "inst-1").unwrap();
    let intent_id = Uuid7::mint(1, 1).to_string();
    let effect = create_raw(
        &store,
        "Effect",
        super::review_tests::effect_pairs("op1", &intent_id),
    );
    let record = store
        .shared
        .read()
        .node_properties(effect)
        .unwrap()
        .get(&db("record"))
        .and_then(value_str)
        .unwrap();
    let mut intent: crate::effect::EffectIntent = serde_json::from_str(&record).unwrap();
    intent.attempt_epoch = crate::ids::AttemptEpoch(2);
    update_raw(
        &store,
        effect,
        "record",
        Value::String(db(&serde_json::to_string(&intent).unwrap())),
    );
    drop(store);
    assert!(matches!(
        Store::recover(directory.path(), "inst-2"),
        Err(StoreError::Internal(_))
    ));
}

#[test]
fn recovery_rejects_wrong_initial_request_status() {
    let directory = tempfile::tempdir().unwrap();
    let store = Store::create(directory.path(), "inst-1").unwrap();
    create_raw(&store, "Unit", super::review_tests::pairs_for("Unit", "u1"));
    let (request_id, request_pairs, _, _) = super::review_tests::recovery_cancel_pairs();
    create_raw(&store, "CancelRequest", request_pairs);
    let (mut event, mut receipt) = cancellation_request_history(&request_id, 1);
    event.set(
        db("payload"),
        Value::String(db(
            r#"{"root_kind":"execution_unit","root_id":"u1","reason":"owner_request","policy":"attached_cascade","status":"settled","members":1}"#,
        )),
    ).unwrap();
    receipt
        .set(
            db("result"),
            Value::String(db(&serde_json::json!({
                "cancel_request_id": request_id,
                "version": 1,
                "status": "settled",
                "root_kind": "execution_unit",
                "root_id": "u1",
            })
            .to_string())),
        )
        .unwrap();
    create_raw(&store, "Event", event);
    create_raw(&store, "Receipt", receipt);
    drop(store);
    assert!(matches!(
        Store::recover(directory.path(), "inst-2"),
        Err(StoreError::Internal(_))
    ));
}
