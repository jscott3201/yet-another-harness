//! Tests added from the 3-lens adversarial review: lifecycle guard,
//! full book rebuild, every unique and immutable flag provoked, and the
//! WriterLockHeld surface pinned.

use super::*;
use selene_core::DbString;

fn pairs_for(label: &str, key: &str) -> PropertyMap {
    let p = |pairs: Vec<(DbString, Value)>| PropertyMap::from_pairs(pairs).expect("pairs");
    let s = |v: &str| Value::String(db(v));
    match label {
        "Authority" => p(vec![
            (db("authority_key"), s(key)),
            (db("authority_epoch"), Value::Uint(1)),
            (db("holder_instance_id"), s("inst-x")),
            (db("status"), s("active")),
        ]),
        "Unit" => p(vec![
            (db("unit_id"), s(key)),
            (db("version"), Value::Uint(1)),
            (db("current_attempt_epoch"), Value::Uint(1)),
            (db("stamp"), Value::Uint(0)),
            (db("status"), s("admitted")),
            (db("work_item_id"), s("w1")),
            (db("run_id"), s("r1")),
            (db("record"), s("{}")),
        ]),
        "Attempt" => p(vec![
            (db("attempt_key"), s(key)),
            (db("attempt_id"), s(&format!("aid-{key}"))),
            (db("unit_id"), s("u1")),
            (db("attempt_epoch"), Value::Uint(1)),
            (db("holder_id"), s("h1")),
            (db("status"), s("active")),
        ]),
        "Run" => p(vec![
            (db("run_id"), s(key)),
            (db("version"), Value::Uint(1)),
            (db("status"), s("open")),
            (db("goal_work_item_id"), s("w1")),
            (db("record"), s("{}")),
        ]),
        "CancelRequest" => p(vec![
            (db("cancel_request_id"), s(key)),
            (db("version"), Value::Uint(1)),
            (db("root_kind"), s("execution_unit")),
            (db("root_id"), s("u1")),
            (db("policy"), s("attached_cascade")),
            (db("reason"), s("owner_request")),
            (db("scope"), s("[]")),
            (db("status"), s("requested")),
            (db("record"), s("{}")),
        ]),
        "CancelDelivery" => p(vec![
            (db("delivery_key"), s(key)),
            (db("cancel_request_id"), s("cr1")),
            (db("member_id"), s("u1")),
            (db("member_kind"), s("execution_unit")),
            (db("outcome"), s("observed_stopped")),
            (db("record"), s("{}")),
        ]),
        "Lease" => p(vec![
            (db("unit_id"), s(key)),
            (db("attempt_epoch"), Value::Uint(1)),
            (db("holder_id"), s("h1")),
            (db("status"), s("active")),
            (db("version"), Value::Uint(1)),
        ]),
        "WorkItem" => p(vec![
            (db("work_item_id"), s(key)),
            (db("version"), Value::Uint(1)),
            (db("status"), s("ready")),
            (db("acceptance_contract_digest"), s("blake3:00")),
            (db("declared_write_scope"), s("[]")),
            (db("record"), s("{}")),
        ]),
        "Receipt" => p(vec![
            (db("receipt_key"), s(key)),
            (db("request_digest"), s("blake3:00")),
            (db("status"), s("completed")),
            (db("result"), s("{}")),
        ]),
        "Evidence" => p(vec![
            (db("evidence_key"), s(key)),
            (db("work_item_id"), s("w1")),
            (db("artifact_set_digest"), s("blake3:00")),
            (db("record"), s("{}")),
        ]),
        other => panic!("no pair builder for {other}"),
    }
}

fn effect_pairs(operation_key: &str, intent_id: &str) -> PropertyMap {
    PropertyMap::from_pairs([
        (db("operation_key"), Value::String(db(operation_key))),
        (db("effect_intent_id"), Value::String(db(intent_id))),
        (db("unit_id"), Value::String(db("u1"))),
        (db("version"), Value::Uint(1)),
        (db("state"), Value::String(db("prepared"))),
        (db("record"), Value::String(db("{}"))),
    ])
    .expect("effect pairs")
}

fn create_raw(store: &Store, label: &str, pairs: PropertyMap) -> Result<NodeId, String> {
    let mut txn = store.shared.begin_write();
    let node = {
        let mut m = txn.mutator();
        m.create_node(LabelSet::single(db(label)), pairs)
    };
    match node {
        Ok(n) => match txn.commit() {
            Ok(_) => Ok(n),
            Err(e) => Err(format!("{e:?}")),
        },
        Err(e) => {
            txn.rollback();
            Err(format!("{e:?}"))
        }
    }
}

#[test]
fn create_refuses_an_existing_control_graph() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::create(dir.path(), "inst-1").unwrap();
    drop(store);
    match Store::create(dir.path(), "inst-2").err() {
        Some(StoreError::AlreadyInitialized(_)) => {}
        other => panic!("expected AlreadyInitialized, got {other:?}"),
    }
    // The lawful reopen still works and takes the epoch forward.
    let store = Store::recover(dir.path(), "inst-2").unwrap();
    assert_eq!(store.authority_epoch(), AuthorityEpoch(2));
}

#[test]
fn recover_rebuilds_every_book() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::create(dir.path(), "inst-1").unwrap();
    for (label, key) in [
        ("Unit", "u1"),
        ("Attempt", "u1/1"),
        ("Lease", "u1"),
        ("WorkItem", "w1"),
        ("Receipt", "r1"),
        ("Evidence", "ev1"),
        ("Run", "r1"),
        ("CancelRequest", "cr1"),
        ("CancelDelivery", "cr1|u1"),
    ] {
        create_raw(&store, label, pairs_for(label, key)).unwrap();
    }
    create_raw(&store, "Effect", effect_pairs("op1", "ei1")).unwrap();
    super::tests::append_event(&store, "e1", "u1", 1);
    drop(store);
    let store = Store::recover(dir.path(), "inst-2").unwrap();
    assert!(store.unit_node("u1").is_some(), "unit book");
    assert!(store.attempt_node("u1/1").is_some(), "attempt book");
    assert!(store.lease_node("u1").is_some(), "lease book");
    assert!(store.work_item_node("w1").is_some(), "work item book");
    assert!(store.receipt_node("r1").is_some(), "receipt book");
    assert!(store.effect_node("op1").is_some(), "effect book");
    assert!(store.evidence_node("ev1").is_some(), "evidence book");
    assert!(store.run_node("r1").is_some(), "run book");
    assert!(
        store.cancel_request_node("cr1").is_some(),
        "cancel request book"
    );
    assert!(
        store.cancel_delivery_node("cr1|u1").is_some(),
        "cancel delivery book"
    );
    assert!(
        store.books.lock().unwrap().events.contains_key("e1"),
        "event book"
    );
}

#[test]
fn every_unique_key_rejects_a_duplicate() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::create(dir.path(), "inst-1").unwrap();
    // The Authority singleton: create() already committed the row.
    assert!(
        create_raw(&store, "Authority", pairs_for("Authority", "control")).is_err(),
        "second Authority row must be unrepresentable"
    );
    for (label, key) in [
        ("Unit", "u1"),
        ("Attempt", "u1/1"),
        ("Lease", "u1"),
        ("WorkItem", "w1"),
        ("Receipt", "r1"),
        ("Evidence", "ev1"),
        ("Run", "r1"),
        ("CancelRequest", "cr1"),
        ("CancelDelivery", "cr1|u1"),
    ] {
        create_raw(&store, label, pairs_for(label, key)).unwrap();
        assert!(
            create_raw(&store, label, pairs_for(label, key)).is_err(),
            "duplicate {label} key must fail commit"
        );
    }
    create_raw(&store, "Effect", effect_pairs("op1", "ei1")).unwrap();
    assert!(
        create_raw(&store, "Effect", effect_pairs("op1", "ei2")).is_err(),
        "duplicate operation_key must fail"
    );
    assert!(
        create_raw(&store, "Effect", effect_pairs("op2", "ei1")).is_err(),
        "duplicate effect_intent_id must fail"
    );
}

#[test]
fn duplicate_cursor_rejects_at_commit() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::create(dir.path(), "inst-1").unwrap();
    let cursor = super::tests::append_event(&store, "e1", "u1", 1);
    let mut txn = store.shared.begin_write();
    {
        let mut m = txn.mutator();
        m.create_node(
            LabelSet::single(db("Event")),
            super::tests::event_pairs("e2", cursor, "u2", 1),
        )
        .expect("mutator accepts; uniqueness checks at commit");
    }
    assert!(txn.commit().is_err(), "cursor reuse must fail commit");
}

#[test]
fn immutable_fields_reject_updates() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::create(dir.path(), "inst-1").unwrap();
    let cases = [
        ("Unit", pairs_for("Unit", "u1"), "work_item_id"),
        (
            "WorkItem",
            pairs_for("WorkItem", "w1"),
            "acceptance_contract_digest",
        ),
        ("Receipt", pairs_for("Receipt", "r1"), "request_digest"),
        ("Evidence", pairs_for("Evidence", "ev1"), "record"),
        // §5.2 rule 2: a committed scope can never be widened.
        ("CancelRequest", pairs_for("CancelRequest", "cr1"), "scope"),
        (
            "CancelDelivery",
            pairs_for("CancelDelivery", "cr1|u1"),
            "outcome",
        ),
    ];
    for (label, pairs, frozen) in cases {
        let node = create_raw(&store, label, pairs).unwrap();
        let mut txn = store.shared.begin_write();
        let result = txn.mutator().update_node(
            node,
            no_labels(),
            props_set([(db(frozen), Value::String(db("tampered")))]),
        );
        txn.rollback();
        assert!(result.is_err(), "{label}.{frozen} must be immutable");
    }
}

#[test]
fn recover_under_a_live_holder_reports_writer_lock_held() {
    let dir = tempfile::tempdir().unwrap();
    let _held = Store::create(dir.path(), "inst-1").unwrap();
    match Store::recover(dir.path(), "inst-2").err() {
        Some(StoreError::Graph(e)) => {
            let detail = format!("{e:?}");
            assert!(
                detail.contains("WriterLockHeld"),
                "expected WriterLockHeld, got {detail}"
            );
        }
        other => panic!("expected Graph(WriterLockHeld), got {other:?}"),
    }
}

#[test]
fn missing_lease_with_present_unit_is_not_found() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::create(dir.path(), "inst-1").unwrap();
    let read = super::tests::FakeFenceRead {
        unit: Some(UnitFence {
            attempt_epoch: AttemptEpoch(3),
            stamp: Stamp(1),
        }),
        lease: None,
    };
    let claims = AttemptTokenClaims {
        unit_id: "u1".into(),
        attempt_epoch: AttemptEpoch(3),
        stamp: Stamp(1),
        authority_epoch: store.authority_epoch(),
        holder_id: "h1".into(),
    };
    assert!(matches!(
        store.check_holder_fence(&read, &claims),
        Err(StoreRejection::NotFound { .. })
    ));
}
