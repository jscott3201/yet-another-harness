//! Non-kill correctness cells against a real WAL-backed store (EXP-001 §6):
//! journal-mutation/duplicate, snapshot-plus-poll handoff, and the in-process
//! half of writer-lock contention. Kill cells live in the orchestrator, not
//! here — these run under `cargo test` because they inject no process death.

use std::collections::BTreeSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use exp001_harness::schema::{Batching, CommandKind, SemanticEvent};
use exp001_harness::store::{poll_wal, ApplyError, PersistError, Rejection, Store, TypeViolation};
use exp001_harness::workload::{CommitSpec, UnitPool, WriterStream};
use selene_core::Change;
use selene_graph::GraphError;

/// Claim → apply → release loop for one writer; returns applied specs.
fn drive(store: &Store, pool: &mut UnitPool, stream: &mut WriterStream, steps: u32) -> Vec<CommitSpec> {
    let mut applied = Vec::new();
    for _ in 0..steps {
        let spec = stream.next_spec(pool).expect("free slot");
        match store.apply(&spec) {
            Ok(_) => {
                assert!(
                    spec.expect_reject.is_none(),
                    "planned-stale spec was accepted: {spec:?} — bar 6 violation"
                );
                pool.release(spec.slot, &spec, true);
                applied.push(spec);
            }
            Err(ApplyError::Rejected(r)) => {
                assert!(
                    spec.expect_reject.is_some(),
                    "unplanned funnel rejection {r:?} for {spec:?}"
                );
                pool.release(spec.slot, &spec, false);
            }
            Err(ApplyError::Graph(e)) => panic!("store error: {e}"),
        }
    }
    applied
}

#[test]
fn funnel_round_trip_and_recovery() {
    let dir = tempfile::tempdir().expect("tempdir");
    let applied = {
        let store = Store::create(dir.path(), Batching::Off).expect("create");
        let mut pool = UnitPool::new(8);
        let mut stream = WriterStream::new(0xE1, 0);
        let applied = drive(&store, &mut pool, &mut stream, 300);
        assert!(!applied.is_empty());
        applied
        // store drops here: clean close, committer joins.
    };

    let store = Store::recover(dir.path()).expect("recover");
    // Every applied event is present with byte-identical payload, through a
    // rebuilt address book — proving recovery and the book rebuild together.
    for spec in &applied {
        let ev = &spec.events[0];
        let payload = store
            .event_payload(ev.event_id)
            .unwrap_or_else(|| panic!("event {} lost after recovery", ev.event_id));
        assert_eq!(payload, ev.payload, "payload mutated across recovery");
    }
    // Units resumed at their final versions: replay the applied specs against
    // the recovered state.
    let mut last: std::collections::HashMap<u64, &CommitSpec> = Default::default();
    for spec in &applied {
        last.insert(spec.unit_id, spec);
    }
    for (unit_id, spec) in last {
        if spec.new_phase == exp001_harness::workload::UnitPhase::Terminal {
            continue; // retired units keep their row but no further claims exist
        }
        let (version, _, _) = store.unit_state(unit_id).expect("unit survives recovery");
        assert_eq!(version, spec.expected_version + 1, "unit {unit_id} version diverged");
    }
}

#[test]
fn journal_immutability_cell() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = Store::create(dir.path(), Batching::Off).expect("create");
    let mut pool = UnitPool::new(4);
    let mut stream = WriterStream::new(0xE2, 0);
    let applied = drive(&store, &mut pool, &mut stream, 50);
    let target = &applied[10].events[0];
    let before = store.event_payload(target.event_id).expect("payload");

    // Update arm: store-enforced, rejects at the mutator call (immutable
    // property), before any commit.
    let err = store.try_update_event(target.event_id).expect_err("update must reject");
    assert!(
        matches!(err, GraphError::TypeViolation(TypeViolation::ImmutablePropertyUpdate { .. })),
        "wrong rejection: {err}"
    );

    // Delete arm: funnel-enforced (R26a) — typed, store untouched.
    let rejection = store.try_delete_event(target.event_id);
    assert_eq!(rejection, Rejection::JournalDelete { event_id: target.event_id });

    // Duplicate arms: same event_id, then same composite key with a fresh id.
    let dup_id = SemanticEvent { payload: "{}".into(), ..target.clone() };
    let err = store.try_duplicate_event(&dup_id).expect_err("dup event_id must reject");
    assert!(
        matches!(err, GraphError::TypeViolation(TypeViolation::UniquePropertyDuplicate { .. })),
        "wrong rejection: {err}"
    );
    let dup_composite = SemanticEvent { event_id: 0xDEAD_BEEF, ..target.clone() };
    let err = store.try_duplicate_event(&dup_composite).expect_err("dup composite must reject");
    assert!(
        matches!(err, GraphError::TypeViolation(TypeViolation::UniquePropertyDuplicate { .. })),
        "wrong rejection: {err}"
    );

    // Byte identity across all four attempts.
    let after = store.event_payload(target.event_id).expect("payload");
    assert_eq!(before, after, "journal bytes changed under rejected mutations");
}

#[test]
fn stale_lease_rejects_typed() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = Store::create(dir.path(), Batching::Off).expect("create");
    let mut pool = UnitPool::new(2);
    let mut stream = WriterStream::new(0xE3, 0);

    // Drive until some unit is Working under epoch >= 1, then hand-craft a
    // renewal claiming a superseded epoch.
    let applied = drive(&store, &mut pool, &mut stream, 30);
    let dispatch = applied
        .iter()
        .rev()
        .find(|s| s.kind == CommandKind::Dispatch)
        .expect("a dispatch happened");
    let (version, epoch, _) = store.unit_state(dispatch.unit_id).expect("unit exists");

    let mut stale = dispatch.clone();
    stale.kind = CommandKind::LeaseRenewal;
    stale.expected_version = version;
    stale.attempt_epoch = epoch.wrapping_sub(1);
    match store.apply(&stale) {
        Err(ApplyError::Rejected(Rejection::StaleLease { claimed_epoch, current_epoch, .. })) => {
            assert_eq!(claimed_epoch, epoch.wrapping_sub(1));
            assert_eq!(current_epoch, epoch);
        }
        other => panic!("stale lease not rejected typed: {other:?}"),
    }
}

/// The R26c amended cell: snapshot at cursor C, then WAL-poll from C gated by
/// the durability watermark, while a writer commits concurrently. The union
/// must cover every committed transition — no invisible gap (I12).
#[test]
fn snapshot_plus_poll_handoff() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = Arc::new(Store::create(dir.path(), Batching::Off).expect("create"));
    let watermark = Arc::new(AtomicU64::new(0));
    let committed: Arc<Mutex<Vec<u64>>> = Arc::new(Mutex::new(Vec::new()));

    let writer = {
        let store = Arc::clone(&store);
        let watermark = Arc::clone(&watermark);
        let committed = Arc::clone(&committed);
        std::thread::spawn(move || {
            let mut pool = UnitPool::new(8);
            let mut stream = WriterStream::new(0xE4, 0);
            for _ in 0..200 {
                let spec = stream.next_spec(&mut pool).expect("free slot");
                match store.apply(&spec) {
                    Ok(applied) => {
                        // Publish order: record the commit BEFORE advancing the
                        // watermark so a racing poller can never trust a
                        // sequence whose event it might miss in `committed`.
                        committed.lock().unwrap().push(spec.events[0].event_id);
                        if let Some(seq) = applied.durable_at {
                            watermark.fetch_max(seq, Ordering::SeqCst);
                        }
                        pool.release(spec.slot, &spec, true);
                    }
                    Err(ApplyError::Rejected(_)) => pool.release(spec.slot, &spec, false),
                    Err(ApplyError::Graph(e)) => panic!("store error: {e}"),
                }
            }
        })
    };

    // Mid-stream handoff: no pause between snapshot read and poll start.
    while watermark.load(Ordering::SeqCst) < 20 {
        std::hint::spin_loop();
    }
    let cursor = watermark.load(Ordering::SeqCst);
    let snapshot_events: BTreeSet<u64> = store.snapshot_event_ids().into_iter().collect();

    writer.join().expect("writer thread");
    let final_watermark = watermark.load(Ordering::SeqCst);
    let polled = poll_wal(&Store::wal_path(dir.path()), cursor, final_watermark).expect("poll");
    let polled_events: BTreeSet<u64> = polled
        .iter()
        .flat_map(|(_, changes)| changes.iter())
        .filter_map(|c| match c {
            Change::NodeCreated { properties, .. } => properties
                .get(&selene_core::db_string("event_id").unwrap())
                .and_then(|v| match v {
                    selene_core::Value::Uint(u) => Some(*u),
                    _ => None,
                }),
            _ => None,
        })
        .collect();

    let all_committed: BTreeSet<u64> = committed.lock().unwrap().iter().copied().collect();
    let union: BTreeSet<u64> = snapshot_events.union(&polled_events).copied().collect();
    let missing: Vec<u64> = all_committed.difference(&union).copied().collect();
    assert!(
        missing.is_empty(),
        "invisible gap: {} committed events in neither snapshot nor poll: {missing:?}",
        missing.len()
    );
}

/// In-process half of the WriterLockHeld cell: a second open against a held
/// store must fail fast, typed, without corrupting the holder. The
/// second-daemon takeover choreography belongs to the kill orchestrator.
#[test]
fn second_open_fails_fast_with_writer_lock_held() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = Store::create(dir.path(), Batching::Off).expect("create");
    let mut pool = UnitPool::new(2);
    let mut stream = WriterStream::new(0xE5, 0);
    drive(&store, &mut pool, &mut stream, 10);

    match Store::recover(dir.path()) {
        Ok(_) => panic!("second open must fail while the store is held"),
        Err(err) => assert!(
            matches!(err, GraphError::Persist(PersistError::WriterLockHeld)),
            "wrong contention error: {err}"
        ),
    }

    // The holder is unharmed: it can still commit after the failed contender.
    let survivors = drive(&store, &mut pool, &mut stream, 10);
    assert!(!survivors.is_empty());
}
