//! Funnel core semantics: §2.2 claim/resolve + replay, P5.2 idempotency,
//! the I2 version CAS, journal discipline (exactly one event set per
//! completed command), and concurrent-retry serialization.

mod common;

use common::*;
use yah_kernel::error::ErrorKind;
use yah_kernel::funnel::{Method, Submission, token_from_result};
use yah_kernel::ids::{Digest, Uuid7};

#[test]
fn work_item_create_completes_replays_and_guards_the_digest() {
    let mut ctx = Ctx::new();
    let cmd = ctx.create_work_item("wi-1");

    let first = completed(ctx.funnel.submit(&cmd));
    assert_eq!(first["work_item_id"], "wi-1");

    // Same triple, same digest: the stored answer, byte-identical.
    assert_eq!(replayed(ctx.funnel.submit(&cmd)), first);

    // Same command_id, different request: idempotency_conflict, no transition.
    let mut altered = cmd.clone();
    altered.request_digest = Digest::of_bytes(b"something else");
    assert_eq!(
        rejected(ctx.funnel.submit(&altered)),
        (ErrorKind::IdempotencyConflict, false)
    );

    // The conflict did not disturb the original receipt.
    assert_eq!(replayed(ctx.funnel.submit(&cmd)), first);
}

#[test]
fn credential_bearing_receipt_replay_is_bound_to_the_original_principal() {
    let mut ctx = Ctx::new();
    ctx.seed_unit();
    let dispatch = ctx.dispatch_cmd("u-1", "h1", 1);
    let first = completed(ctx.funnel.submit(&dispatch));
    assert!(token_from_result(&first).is_some());

    let mut intruder = dispatch.clone();
    intruder.principal_id = "daemon-other".into();
    assert_eq!(
        rejected(ctx.funnel.submit(&intruder)),
        (ErrorKind::Unauthorized, false)
    );
    assert_eq!(replayed(ctx.funnel.submit(&dispatch)), first);
}

#[test]
fn receipt_replay_is_bound_to_the_original_command_type() {
    let mut ctx = Ctx::new();
    ctx.seed_unit();
    let token = ctx.dispatch("u-1", "h1", 1);
    let prepare = ctx.prepare_cmd(
        "shared request",
        "u-1",
        token.clone(),
        effect_spec(Uuid7::mint(7, 90), "req"),
    );
    completed(ctx.funnel.submit(&prepare));

    let mut progress = ctx.progress_cmd("shared request", "u-1", token, Some(2), "n");
    progress.command_id = prepare.command_id;
    assert_eq!(
        rejected(ctx.funnel.submit(&progress)),
        (ErrorKind::IdempotencyConflict, false)
    );
}

#[test]
fn version_conflict_is_transient_and_never_persisted() {
    let mut ctx = Ctx::new();
    ctx.seed_unit();
    let token = ctx.dispatch("u-1", "h1", 1); // unit now v2

    let stale = ctx.progress_cmd("progress stale", "u-1", token, Some(1), "n");
    assert_eq!(
        rejected(ctx.funnel.submit(&stale)),
        (ErrorKind::VersionConflict, false)
    );
    // Not persisted: the retry re-validates rather than replaying.
    assert_eq!(
        rejected(ctx.funnel.submit(&stale)),
        (ErrorKind::VersionConflict, false)
    );

    // Same command_id, corrected expectation (new digest): accepted — proof
    // no rejected receipt was written for the conflict.
    let mut corrected = stale.clone();
    corrected.request_digest = Digest::of_bytes(b"progress corrected");
    corrected.expected_version = Some(2);
    assert_eq!(completed(ctx.funnel.submit(&corrected))["version"], 2);
}

#[test]
fn mutation_without_expected_version_is_invalid_request() {
    // I2: expected_versions is REQUIRED on mutations of existing state —
    // omitting it must not degrade the CAS to a blind write.
    let mut ctx = Ctx::new();
    ctx.seed_unit();

    let blind = ctx.authority_cmd(
        "blind dispatch",
        None,
        Method::UnitDispatch {
            unit_id: "u-1".into(),
            holder_id: "h1".into(),
        },
    );
    assert_eq!(
        rejected(ctx.funnel.submit(&blind)),
        (ErrorKind::InvalidRequest, false)
    );
    // Shape-deterministic, so it persisted and replays.
    assert_eq!(
        rejected(ctx.funnel.submit(&blind)),
        (ErrorKind::InvalidRequest, true)
    );

    // Same rule on the holder side.
    let token = ctx.dispatch("u-1", "h1", 1);
    let blind_progress = ctx.progress_cmd("blind progress", "u-1", token, None, "n");
    assert_eq!(
        rejected(ctx.funnel.submit(&blind_progress)),
        (ErrorKind::InvalidRequest, false)
    );
}

#[test]
fn funnel_rejects_a_receipt_scope_that_disagrees_with_the_method() {
    let mut ctx = Ctx::new();
    let mut open = ctx.open_run("run-1");
    open.scope_kind = yah_kernel::funnel::ScopeKind::Run;
    open.scope_id = "other-run".into();
    assert_eq!(
        rejected(ctx.funnel.submit(&open)),
        (ErrorKind::InvalidRequest, false)
    );
    assert!(ctx.funnel.store().journal().unwrap().is_empty());
}

#[test]
fn funnel_never_persists_a_foreign_project_receipt() {
    let mut ctx = Ctx::new();
    let mut work_item = ctx.create_work_item("wi-foreign");
    work_item.scope_kind = yah_kernel::funnel::ScopeKind::Project;
    work_item.scope_id = "other-project".into();
    assert_eq!(
        rejected(ctx.funnel.submit(&work_item)),
        (ErrorKind::InvalidRequest, false)
    );
    assert!(ctx.funnel.store().journal().unwrap().is_empty());

    let dir = ctx.dir;
    drop(ctx.funnel);
    let recovered = yah_kernel::store::Store::recover(dir.path(), "kernel-b").unwrap();
    assert_eq!(recovered.project_id(), "default");
}

#[test]
fn funnel_never_persists_a_holder_receipt_outside_its_unit_scope() {
    let mut ctx = Ctx::new();
    ctx.seed_unit();
    let token = ctx.dispatch("u-1", "h1", 1);
    let mut progress = ctx.progress_cmd("progress", "u-1", token, Some(2), "n");
    progress.scope_kind = yah_kernel::funnel::ScopeKind::Global;
    progress.scope_id = "g".into();
    assert_eq!(
        rejected(ctx.funnel.submit(&progress)),
        (ErrorKind::InvalidRequest, false)
    );

    let dir = ctx.dir;
    drop(ctx.funnel);
    yah_kernel::store::Store::recover(dir.path(), "kernel-b").unwrap();
}

#[test]
fn funnel_rejects_invalid_method_identifiers_before_persistence() {
    let mut ctx = Ctx::new();
    let mut work_item = ctx.create_work_item("bad/id");
    work_item.scope_kind = yah_kernel::funnel::ScopeKind::Global;
    assert_eq!(
        rejected(ctx.funnel.submit(&work_item)),
        (ErrorKind::InvalidRequest, false)
    );
    assert!(ctx.funnel.store().journal().unwrap().is_empty());

    let dir = ctx.dir;
    drop(ctx.funnel);
    yah_kernel::store::Store::recover(dir.path(), "kernel-b").unwrap();
}

#[test]
fn shared_id_across_kinds_commits_cleanly() {
    // Aggregate ids are unique per KIND: a work item and a unit sharing the
    // string "x" must not collide the journal's derived composite.
    let mut ctx = Ctx::new();
    let run = ctx.open_run(Ctx::RUN);
    completed(ctx.funnel.submit(&run));
    let wi = ctx.create_work_item("x");
    completed(ctx.funnel.submit(&wi));
    let admit = ctx.admit("x", "x");
    completed(ctx.funnel.submit(&admit));

    // The run open is fixture setup for a different aggregate; this test is
    // about the work_item/unit pair that share the id "x".
    let journal: Vec<_> = ctx
        .funnel
        .store()
        .journal()
        .unwrap()
        .into_iter()
        .filter(|e| e.aggregate_kind != "run")
        .collect();
    assert_eq!(journal.len(), 2);
    assert_eq!(journal[0].aggregate_kind, "work_item");
    assert_eq!(journal[1].aggregate_kind, "unit");
    assert!(journal.iter().all(|e| e.aggregate_id == "x"));
    assert!(journal.iter().all(|e| e.aggregate_version == 1));
}

#[test]
fn journal_appends_exactly_one_event_set_per_completed_command() {
    // Obligation 2's journal half: one event set per semantic transition.
    // Fence observations, replays, and unsupported commands append none.
    let mut ctx = Ctx::new();
    let run = ctx.open_run(Ctx::RUN);
    completed(ctx.funnel.submit(&run));
    let wi = ctx.create_work_item("wi-1");
    completed(ctx.funnel.submit(&wi));
    let admit = ctx.admit("u-1", "wi-1");
    completed(ctx.funnel.submit(&admit));
    let dispatch = ctx.dispatch_cmd("u-1", "h1", 1);
    let token = token_from_result(&completed(ctx.funnel.submit(&dispatch))).expect("token");
    let progress = ctx.progress_cmd("progress 1", "u-1", token.clone(), Some(2), "step");
    completed(ctx.funnel.submit(&progress));

    let journal = ctx.funnel.store().journal().unwrap();
    assert_eq!(journal.len(), 4);
    let kinds: Vec<&str> = journal.iter().map(|e| e.event_kind.as_str()).collect();
    assert_eq!(
        kinds,
        [
            "run.opened",
            "work_item.created",
            "unit.admitted",
            "unit.dispatched"
        ]
    );
    assert!(
        journal.windows(2).all(|w| w[0].cursor < w[1].cursor),
        "cursors strictly ascend"
    );
    let unit_versions: Vec<u64> = journal
        .iter()
        .filter(|e| e.aggregate_kind == "unit")
        .map(|e| e.aggregate_version)
        .collect();
    assert_eq!(unit_versions, [1, 2], "contiguous per aggregate");

    // Replays append nothing.
    for cmd in [&run, &wi, &admit, &dispatch, &progress] {
        replayed(ctx.funnel.submit(cmd));
    }
    assert_eq!(ctx.funnel.store().journal().unwrap().len(), 4);

    // Reissue remains fail-closed until policy revalidation exists.
    let reissue = ctx.holder_cmd(
        "reissue",
        token,
        None,
        Method::TokenReissue {
            unit_id: "u-1".into(),
        },
    );
    assert_eq!(
        rejected(ctx.funnel.submit(&reissue)),
        (ErrorKind::CapabilityUnsupported, false)
    );
    assert_eq!(ctx.funnel.store().journal().unwrap().len(), 4);
}

#[test]
fn concurrent_identical_retry_replays_instead_of_erroring() {
    // ADR-002 P6.3 reconnect-retry shape under real threads: byte-identical
    // concurrent submits must resolve Completed + Replayed (in either
    // order), never a unique-constraint `internal`.
    let mut ctx = Ctx::new();
    let run = ctx.open_run(Ctx::RUN);
    completed(ctx.funnel.submit(&run));
    let wi = ctx.create_work_item("wi-1");
    completed(ctx.funnel.submit(&wi));

    for n in 0..10 {
        let unit = format!("u-{n}");
        let admit = ctx.admit(&unit, "wi-1");
        completed(ctx.funnel.submit(&admit));
        let cmd = ctx.dispatch_cmd(&unit, "h1", 1);

        let funnel = &ctx.funnel;
        let (a, b) = std::thread::scope(|s| {
            let ha = s.spawn(|| funnel.submit(&cmd));
            let hb = s.spawn(|| funnel.submit(&cmd));
            (ha.join().expect("thread a"), hb.join().expect("thread b"))
        });

        let (done, replay) = match (&a, &b) {
            (Submission::Completed { .. }, Submission::Replayed { .. }) => (a, b),
            (Submission::Replayed { .. }, Submission::Completed { .. }) => (b, a),
            other => panic!("expected Completed+Replayed, got {other:?}"),
        };
        assert_eq!(completed(done), replayed(replay));
    }
}
