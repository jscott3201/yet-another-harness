//! Store-layer unit tests (formerly the inline `tests` module).

use super::*;

pub(crate) fn event_pairs(event_id: &str, cursor: u64, agg: &str, ver: u64) -> PropertyMap {
    PropertyMap::from_pairs([
        (db("event_id"), Value::String(db(event_id))),
        (db("cursor"), Value::Uint(cursor)),
        (
            db("agg_ver_ord"),
            // Kind-prefixed: aggregate ids are unique per kind only, so
            // the cross-kind namespace needs the discriminator.
            Value::String(db(&format!("unit/{agg}/{ver}/0"))),
        ),
        (db("aggregate_kind"), Value::String(db("unit"))),
        (db("aggregate_id"), Value::String(db(agg))),
        (db("aggregate_version"), Value::Uint(ver)),
        (db("ordinal"), Value::Uint(0)),
        (db("event_kind"), Value::String(db("test.evt"))),
        (db("payload"), Value::String(db("{}"))),
        (db("command_id"), Value::String(db("cmd-1"))),
    ])
    .expect("event property map")
}

pub(crate) fn append_event(store: &Store, event_id: &str, agg: &str, ver: u64) -> u64 {
    // Allocation under the open write txn — the allocate_cursor
    // ordering contract.
    let mut txn = store.shared.begin_write();
    let cursor = store.allocate_cursor();
    let node = {
        let mut m = txn.mutator();
        m.create_node(
            LabelSet::single(db("Event")),
            event_pairs(event_id, cursor, agg, ver),
        )
        .expect("event create")
    };
    txn.commit().expect("event commit");
    store.book_insert(BookKind::Event, event_id.to_owned(), node);
    cursor
}

#[test]
fn authority_epoch_increments_on_every_takeover() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::create(dir.path(), "inst-1").unwrap();
    assert_eq!(store.authority_epoch(), AuthorityEpoch(1));
    drop(store);
    let store = Store::recover(dir.path(), "inst-2").unwrap();
    assert_eq!(store.authority_epoch(), AuthorityEpoch(2));
    drop(store);
    let store = Store::recover(dir.path(), "inst-3").unwrap();
    assert_eq!(store.authority_epoch(), AuthorityEpoch(3));
}

#[test]
fn cursor_allocator_restores_strictly_above_committed_max() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::create(dir.path(), "inst-1").unwrap();
    append_event(&store, "e1", "u1", 1);
    append_event(&store, "e2", "u1", 2);
    // Burn an allocation with no commit behind it: the value must stay
    // burned across recovery only if committed — an aborted allocation
    // MAY be reused after restart because nothing durable observed it;
    // what is forbidden is allocating at or below the committed max.
    let burned = store.allocate_cursor();
    assert_eq!(burned, 3);
    drop(store);
    let store = Store::recover(dir.path(), "inst-2").unwrap();
    // The uncommitted allocation was not durable, so recovery restores
    // to committed-max + 1 = 3 — and this probe burns 3 in doing so.
    assert_eq!(store.allocate_cursor(), 3);
    // The next committed event therefore lands at cursor 4, and the
    // floor after another recovery sits strictly above it.
    let committed_at = append_event(&store, "e3", "u1", 3);
    assert_eq!(committed_at, 4);
    drop(store);
    let store = Store::recover(dir.path(), "inst-3").unwrap();
    assert_eq!(store.allocate_cursor(), 5);
}

#[test]
fn duplicate_event_identity_rejects_at_commit() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::create(dir.path(), "inst-1").unwrap();
    append_event(&store, "e1", "u1", 1);
    // Same event_id, fresh cursor and composite: unique on event_id.
    let cursor = store.allocate_cursor();
    let mut txn = store.shared.begin_write();
    {
        let mut m = txn.mutator();
        m.create_node(
            LabelSet::single(db("Event")),
            event_pairs("e1", cursor, "u1", 9),
        )
        .expect("mutator accepts; uniqueness is checked at commit");
    }
    assert!(txn.commit().is_err(), "duplicate event_id must fail commit");
    // Same (aggregate, version, ordinal) composite under a new id.
    let cursor = store.allocate_cursor();
    let mut txn = store.shared.begin_write();
    {
        let mut m = txn.mutator();
        m.create_node(
            LabelSet::single(db("Event")),
            event_pairs("e9", cursor, "u1", 1),
        )
        .expect("mutator accepts; uniqueness is checked at commit");
    }
    assert!(
        txn.commit().is_err(),
        "duplicate agg_ver_ord must fail commit"
    );
}

#[test]
fn committed_event_payload_is_immutable() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::create(dir.path(), "inst-1").unwrap();
    append_event(&store, "e1", "u1", 1);
    let node = store
        .books
        .lock()
        .unwrap()
        .events
        .get("e1")
        .copied()
        .unwrap();
    let mut txn = store.shared.begin_write();
    let result = txn.mutator().update_node(
        node,
        no_labels(),
        props_set([(db("payload"), Value::String(db("tampered")))]),
    );
    txn.rollback();
    assert!(result.is_err(), "immutable payload update must be rejected");
}

#[test]
fn journal_delete_requests_reject_typed() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::create(dir.path(), "inst-1").unwrap();
    assert_eq!(
        store.request_journal_delete("e1"),
        StoreRejection::JournalImmutable {
            detail: "delete of committed event e1".to_owned()
        }
    );
}

pub(crate) struct FakeFenceRead {
    pub(crate) unit: Option<UnitFence>,
    pub(crate) lease: Option<LeaseFence>,
}

impl UnitFenceRead for FakeFenceRead {
    fn unit_fence(&self, _unit_id: &str) -> Option<UnitFence> {
        self.unit.as_ref().map(|u| UnitFence {
            attempt_epoch: u.attempt_epoch,
            stamp: u.stamp,
        })
    }
    fn lease_fence(&self, _unit_id: &str) -> Option<LeaseFence> {
        self.lease.as_ref().map(|l| LeaseFence {
            holder_id: l.holder_id.clone(),
            status: l.status.clone(),
        })
    }
}

fn claims(store: &Store) -> AttemptTokenClaims {
    AttemptTokenClaims {
        unit_id: "u1".into(),
        attempt_epoch: AttemptEpoch(3),
        stamp: Stamp(1),
        authority_epoch: store.authority_epoch(),
        holder_id: "h1".into(),
    }
}

fn healthy_read() -> FakeFenceRead {
    FakeFenceRead {
        unit: Some(UnitFence {
            attempt_epoch: AttemptEpoch(3),
            stamp: Stamp(1),
        }),
        lease: Some(LeaseFence {
            holder_id: "h1".into(),
            status: "active".into(),
        }),
    }
}

#[test]
fn holder_fence_checks_all_five_axes() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::create(dir.path(), "inst-1").unwrap();
    let ok = claims(&store);
    assert_eq!(store.check_holder_fence(&healthy_read(), &ok), Ok(()));

    // Axis 1: authority epoch behind current.
    let mut stale = ok.clone();
    stale.authority_epoch = AuthorityEpoch(0);
    assert!(matches!(
        store.check_holder_fence(&healthy_read(), &stale),
        Err(StoreRejection::FenceRejected { .. })
    ));

    // Axis 2: attempt epoch superseded.
    let mut stale = ok.clone();
    stale.attempt_epoch = AttemptEpoch(2);
    assert!(matches!(
        store.check_holder_fence(&healthy_read(), &stale),
        Err(StoreRejection::FenceRejected { .. })
    ));

    // Axis 3: stamp bumped (A4's axis — no new attempt required).
    let mut stale = ok.clone();
    stale.stamp = Stamp(0);
    assert!(matches!(
        store.check_holder_fence(&healthy_read(), &stale),
        Err(StoreRejection::FenceRejected { .. })
    ));

    // Axis 4: lease not active.
    let mut read = healthy_read();
    read.lease = Some(LeaseFence {
        holder_id: "h1".into(),
        status: "revoked".into(),
    });
    assert!(matches!(
        store.check_holder_fence(&read, &ok),
        Err(StoreRejection::FenceRejected { .. })
    ));

    // Axis 5: wrong holder for the active lease.
    let mut read = healthy_read();
    read.lease = Some(LeaseFence {
        holder_id: "h2".into(),
        status: "active".into(),
    });
    assert!(matches!(
        store.check_holder_fence(&read, &ok),
        Err(StoreRejection::FenceRejected { .. })
    ));

    // Missing unit is NotFound, not a fence rejection.
    let read = FakeFenceRead {
        unit: None,
        lease: None,
    };
    assert!(matches!(
        store.check_holder_fence(&read, &ok),
        Err(StoreRejection::NotFound { .. })
    ));
}
