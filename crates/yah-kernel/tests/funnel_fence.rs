//! Fence and lifecycle behavior over the funnel: the §3.3 five-axis fence
//! plus the sealed unit binding, §3.4 stamp/reissue, attempt supersession,
//! rejection-persistence classes, and the kill/recover halves of
//! obligations 2 and A10.

mod common;

use common::*;
use yah_kernel::error::ErrorKind;
use yah_kernel::funnel::{Command, Funnel, Method, PrincipalKind, ScopeKind, token_from_result};
use yah_kernel::ids::{AuthorityEpoch, Digest, Uuid7};
use yah_kernel::store::{AttemptTokenClaims, Store};

#[test]
fn dispatch_mints_epoch_attempt_lease_token_and_supersedes() {
    let mut ctx = Ctx::new();
    ctx.seed_unit();

    let token = ctx.dispatch("u-1", "h1", 1);
    assert_eq!(token.unit_id, "u-1");
    assert_eq!(token.attempt_epoch.0, 1); // §3.1: first acquisition mints 1
    assert_eq!(token.stamp.0, 0);
    assert_eq!(token.authority_epoch, ctx.authority());
    assert_eq!(token.holder_id, "h1");
    assert_eq!(
        ctx.funnel.store().attempt_status("u-1", 1).as_deref(),
        Some("active")
    );

    // Reassignment mints the next epoch; the prior attempt flips to
    // superseded in the same transaction (§1.2: one active per unit).
    let token2 = ctx.dispatch("u-1", "h2", 2);
    assert_eq!(token2.attempt_epoch.0, 2);
    assert_eq!(token2.holder_id, "h2");
    assert_eq!(
        ctx.funnel.store().attempt_status("u-1", 1).as_deref(),
        Some("superseded")
    );
    assert_eq!(
        ctx.funnel.store().attempt_status("u-1", 2).as_deref(),
        Some("active")
    );
}

#[test]
fn holder_progress_observes_the_fence_without_mutating_semantic_state() {
    let mut ctx = Ctx::new();
    ctx.seed_unit();
    let token = ctx.dispatch("u-1", "h1", 1); // v2

    let p1 = ctx.progress_cmd("progress 1", "u-1", token.clone(), Some(2), "one");
    assert_eq!(completed(ctx.funnel.submit(&p1))["version"], 2);

    let p2 = ctx.progress_cmd("progress 2", "u-1", token, Some(2), "two");
    assert_eq!(completed(ctx.funnel.submit(&p2))["version"], 2);
    assert_eq!(ctx.funnel.store().journal().unwrap().len(), 4);
}

#[test]
fn holder_principal_must_match_the_sealed_token() {
    let mut ctx = Ctx::new();
    ctx.seed_unit();
    let token = ctx.dispatch("u-1", "h1", 1);
    let mut forged = ctx.progress_cmd("forged holder", "u-1", token, Some(2), "ignored");
    forged.principal_id = "h2".into();

    assert_eq!(
        rejected(ctx.funnel.submit(&forged)),
        (ErrorKind::Unauthorized, false)
    );
    assert_eq!(
        rejected(ctx.funnel.submit(&forged)),
        (ErrorKind::Unauthorized, true)
    );
    assert_eq!(ctx.funnel.store().journal().unwrap().len(), 4);
}

#[test]
fn superseded_token_is_fence_rejected_and_the_rejection_replays() {
    let mut ctx = Ctx::new();
    ctx.seed_unit();
    let t1 = ctx.dispatch("u-1", "h1", 1);
    let t2 = ctx.dispatch("u-1", "h2", 2); // supersedes t1, v3

    let stale = ctx.progress_cmd("progress from h1", "u-1", t1, Some(3), "late");
    assert_eq!(
        rejected(ctx.funnel.submit(&stale)),
        (ErrorKind::FenceRejected, false)
    );
    // Fence staleness is permanent (epochs are monotonic): persisted.
    assert_eq!(
        rejected(ctx.funnel.submit(&stale)),
        (ErrorKind::FenceRejected, true)
    );

    // The current holder is unaffected.
    let live = ctx.progress_cmd("progress from h2", "u-1", t2, Some(3), "current");
    completed(ctx.funnel.submit(&live));
}

#[test]
fn stamp_bump_kills_tokens_and_reissue_fails_closed() {
    let mut ctx = Ctx::new();
    ctx.seed_unit();
    let t1 = ctx.dispatch("u-1", "h1", 1); // v2

    let bump = ctx.authority_cmd(
        "bump",
        Some(2),
        Method::StampBump {
            unit_id: "u-1".into(),
        },
    );
    assert_eq!(completed(ctx.funnel.submit(&bump))["stamp"], 1); // v3
    let journal_len = ctx.funnel.store().journal().unwrap().len();

    let dead = ctx.progress_cmd("progress dead token", "u-1", t1.clone(), Some(3), "n");
    assert_eq!(
        rejected(ctx.funnel.submit(&dead)),
        (ErrorKind::FenceRejected, false)
    );

    let reissue = ctx.holder_cmd(
        "reissue",
        t1,
        None,
        Method::TokenReissue {
            unit_id: "u-1".into(),
        },
    );
    assert_eq!(
        rejected(ctx.funnel.submit(&reissue)),
        (ErrorKind::CapabilityUnsupported, false)
    );
    assert_eq!(ctx.funnel.store().journal().unwrap().len(), journal_len);
}

#[test]
fn cross_unit_and_ghost_tokens_are_fence_rejected() {
    // The token's sealed unit_id must bind to the method target: a token
    // for a sibling unit — or for no unit at all — is unresolvable HERE.
    let mut ctx = Ctx::new();
    ctx.seed_unit(); // wi-1, u-1
    let admit2 = ctx.admit("u-2", "wi-1");
    completed(ctx.funnel.submit(&admit2));
    let t_u1 = ctx.dispatch("u-1", "h1", 1);
    let t_u2 = ctx.dispatch("u-2", "h1", 1);

    // Same holder, same epoch/stamp coordinates — only the unit differs.
    let cross = ctx.progress_cmd("cross progress", "u-2", t_u1.clone(), Some(2), "n");
    assert_eq!(
        rejected(ctx.funnel.submit(&cross)),
        (ErrorKind::FenceRejected, false)
    );

    let cross_reissue = ctx.holder_cmd(
        "cross reissue",
        t_u1,
        None,
        Method::TokenReissue {
            unit_id: "u-2".into(),
        },
    );
    assert_eq!(
        rejected(ctx.funnel.submit(&cross_reissue)),
        (ErrorKind::CapabilityUnsupported, false)
    );

    // A token naming a unit that does not exist resolves nowhere.
    let ghost = AttemptTokenClaims {
        unit_id: "u-ghost".into(),
        attempt_epoch: yah_kernel::ids::AttemptEpoch(1),
        stamp: yah_kernel::ids::Stamp(0),
        authority_epoch: ctx.authority(),
        holder_id: "h1".into(),
        nonce: "ghost-token".into(),
    };
    let ghost_progress = ctx.progress_cmd("ghost progress", "u-1", ghost, Some(2), "n");
    assert_eq!(
        rejected(ctx.funnel.submit(&ghost_progress)),
        (ErrorKind::FenceRejected, false)
    );

    // The properly bound token still works.
    let live = ctx.progress_cmd("real progress", "u-2", t_u2, Some(2), "n");
    assert_eq!(completed(ctx.funnel.submit(&live))["version"], 2);
}

#[test]
fn healable_not_found_is_not_persisted() {
    // not_found depends on state a later command can create, so persisting
    // it would poison the byte-identical retry forever (P6.3).
    let mut ctx = Ctx::new();
    // Open the run first so the not_found under test is the work item's.
    let run = ctx.open_run(Ctx::RUN);
    completed(ctx.funnel.submit(&run));
    let admit = ctx.admit("u-1", "wi-1");
    assert_eq!(
        rejected(ctx.funnel.submit(&admit)),
        (ErrorKind::NotFound, false)
    );
    assert_eq!(
        rejected(ctx.funnel.submit(&admit)),
        (ErrorKind::NotFound, false)
    );

    let wi = ctx.create_work_item("wi-1");
    completed(ctx.funnel.submit(&wi));

    // The same command — same command_id, same digest — now succeeds.
    completed(ctx.funnel.submit(&admit));
}

#[test]
fn deterministic_rejections_persist_and_replay() {
    let mut ctx = Ctx::new();

    // Authority method carrying a token is malformed — shape-deterministic.
    let run = ctx.open_run(Ctx::RUN);
    completed(ctx.funnel.submit(&run));
    let mut with_token = ctx.create_work_item("wi-x");
    with_token.attempt_token = Some(AttemptTokenClaims {
        unit_id: "u".into(),
        attempt_epoch: yah_kernel::ids::AttemptEpoch(1),
        stamp: yah_kernel::ids::Stamp(0),
        authority_epoch: ctx.authority(),
        holder_id: "h".into(),
        nonce: "malformed-token".into(),
    });
    assert_eq!(
        rejected(ctx.funnel.submit(&with_token)),
        (ErrorKind::InvalidRequest, false)
    );
    assert_eq!(
        rejected(ctx.funnel.submit(&with_token)),
        (ErrorKind::InvalidRequest, true)
    );

    // Missing authority epoch.
    let mut no_epoch = ctx.create_work_item("wi-y");
    no_epoch.authority_epoch = None;
    assert_eq!(
        rejected(ctx.funnel.submit(&no_epoch)),
        (ErrorKind::InvalidRequest, false)
    );

    // Duplicate creation can never heal (no deletes in this layer).
    let wi = ctx.create_work_item("wi-1");
    completed(ctx.funnel.submit(&wi));
    let dup = ctx.authority_cmd(
        "create wi-1 again",
        None,
        Method::WorkItemCreate {
            work_item_id: "wi-1".into(),
            acceptance_contract_digest: Digest::of_bytes(b"contract"),
            declared_write_scope: vec![],
        },
    );
    assert_eq!(
        rejected(ctx.funnel.submit(&dup)),
        (ErrorKind::InvalidRequest, false)
    );
    assert_eq!(
        rejected(ctx.funnel.submit(&dup)),
        (ErrorKind::InvalidRequest, true)
    );

    // Duplicate admit likewise.
    let admit = ctx.admit("u-1", "wi-1");
    completed(ctx.funnel.submit(&admit));
    let dup_admit = ctx.authority_cmd(
        "admit u-1 again",
        None,
        Method::UnitAdmit {
            unit_id: "u-1".into(),
            work_item_id: "wi-1".into(),
            run_id: Ctx::RUN.into(),
        },
    );
    assert_eq!(
        rejected(ctx.funnel.submit(&dup_admit)),
        (ErrorKind::InvalidRequest, false)
    );
}

#[test]
fn envelope_authority_epoch_is_checked_on_holder_methods() {
    // ADR-002 §5: a prior epoch is fence_rejected on ANY method — even when
    // the token itself is current.
    let mut ctx = Ctx::new();
    ctx.seed_unit();
    let token = ctx.dispatch("u-1", "h1", 1);

    let mut behind = ctx.progress_cmd("behind epoch", "u-1", token.clone(), Some(2), "n");
    behind.authority_epoch = Some(AuthorityEpoch(0));
    assert_eq!(
        rejected(ctx.funnel.submit(&behind)),
        (ErrorKind::FenceRejected, false)
    );
    // Envelope authority is outside the request digest, so a corrected retry
    // with this command id must remain possible.
    assert_eq!(
        rejected(ctx.funnel.submit(&behind)),
        (ErrorKind::FenceRejected, false)
    );

    // Ahead is a split-brain signature a lawful takeover could legitimize:
    // rejected but NOT persisted.
    let mut ahead = ctx.progress_cmd("ahead epoch", "u-1", token, Some(2), "n");
    ahead.authority_epoch = Some(AuthorityEpoch(99));
    assert_eq!(
        rejected(ctx.funnel.submit(&ahead)),
        (ErrorKind::FenceRejected, false)
    );
    assert_eq!(
        rejected(ctx.funnel.submit(&ahead)),
        (ErrorKind::FenceRejected, false)
    );
}

#[test]
fn recover_replays_receipts_and_fences_out_the_prior_lifetime() {
    // Obligation 2 (in-process half): kill after commit, recover, resubmit
    // → stable answers, no duplicate transitions. Obligation A10
    // (in-process half): both authority-epoch fence directions.
    let mut ctx = Ctx::new();
    let run = ctx.open_run(Ctx::RUN);
    completed(ctx.funnel.submit(&run));
    let wi = ctx.create_work_item("wi-1");
    let first = completed(ctx.funnel.submit(&wi));
    let admit = ctx.admit("u-1", "wi-1");
    completed(ctx.funnel.submit(&admit));
    let old_token = ctx.dispatch("u-1", "h1", 1); // v2
    let old_epoch = ctx.authority();

    // "Kill": drop the funnel (and its store, releasing the writer lock).
    let dir = ctx.dir;
    drop(ctx.funnel);
    let store = Store::recover(dir.path(), "kernel-b").expect("recover");
    let new_epoch = store.authority_epoch();
    assert!(new_epoch.0 > old_epoch.0, "recover claims authority");

    // SAME logical clock as the dead lifetime: minted ids stay unique
    // because the authority epoch rides the entropy — the clock-advance
    // courtesy is for ordering, not correctness.
    let funnel = Funnel::new(store, 1_000).unwrap();

    // Resubmitted pre-kill command: replayed from the persisted receipt.
    assert_eq!(replayed(funnel.submit(&wi)), first);

    // Authority command still carrying the dead epoch: fenced.
    let stale_authority = Command {
        command_id: Uuid7::mint(2, 1).to_string(),
        scope_kind: ScopeKind::Global,
        scope_id: "g".into(),
        request_digest: Digest::of_bytes(b"create wi-2 stale"),
        principal_kind: PrincipalKind::Daemon,
        principal_id: "daemon-local".into(),
        expected_version: None,
        authority_epoch: Some(old_epoch),
        attempt_token: None,
        causation_id: None,
        correlation_id: None,
        method: Method::WorkItemCreate {
            work_item_id: "wi-2".into(),
            acceptance_contract_digest: Digest::of_bytes(b"contract"),
            declared_write_scope: vec![],
        },
    };
    assert_eq!(
        rejected(funnel.submit(&stale_authority)),
        (ErrorKind::FenceRejected, false)
    );

    // Holder token minted under the dead authority epoch: fenced, and not
    // reissuable — reauth cannot cross an authority takeover.
    let stale_progress = Command {
        command_id: Uuid7::mint(2, 2).to_string(),
        scope_kind: ScopeKind::Unit,
        scope_id: "u-1".into(),
        request_digest: Digest::of_bytes(b"progress stale lifetime"),
        principal_kind: PrincipalKind::Agent,
        principal_id: "h1".into(),
        expected_version: Some(2),
        authority_epoch: None,
        attempt_token: Some(old_token.clone()),
        causation_id: None,
        correlation_id: None,
        method: Method::ProgressReport {
            unit_id: "u-1".into(),
        },
    };
    assert_eq!(
        rejected(funnel.submit(&stale_progress)),
        (ErrorKind::FenceRejected, false)
    );
    let stale_reissue = Command {
        command_id: Uuid7::mint(2, 3).to_string(),
        scope_kind: ScopeKind::Unit,
        scope_id: "u-1".into(),
        request_digest: Digest::of_bytes(b"reissue stale lifetime"),
        principal_kind: PrincipalKind::Agent,
        principal_id: "h1".into(),
        expected_version: None,
        authority_epoch: None,
        attempt_token: Some(old_token),
        causation_id: None,
        correlation_id: None,
        method: Method::TokenReissue {
            unit_id: "u-1".into(),
        },
    };
    assert_eq!(
        rejected(funnel.submit(&stale_reissue)),
        (ErrorKind::CapabilityUnsupported, false)
    );

    // The new lifetime proceeds: re-dispatch under the new epoch works.
    let redispatch = Command {
        command_id: Uuid7::mint(2, 4).to_string(),
        scope_kind: ScopeKind::Global,
        scope_id: "g".into(),
        request_digest: Digest::of_bytes(b"dispatch after recover"),
        principal_kind: PrincipalKind::Daemon,
        principal_id: "daemon-local".into(),
        expected_version: Some(2),
        authority_epoch: Some(new_epoch),
        attempt_token: None,
        causation_id: None,
        correlation_id: None,
        method: Method::UnitDispatch {
            unit_id: "u-1".into(),
            holder_id: "h1".into(),
        },
    };
    let token = token_from_result(&completed(funnel.submit(&redispatch)))
        .expect("redispatch mints a live token");
    assert_eq!(token.attempt_epoch.0, 2);
    assert_eq!(token.authority_epoch, new_epoch);
    // The prior lifetime's attempt was superseded by the new dispatch.
    assert_eq!(
        funnel.store().attempt_status("u-1", 1).as_deref(),
        Some("superseded")
    );

    let live = Command {
        command_id: Uuid7::mint(2, 5).to_string(),
        scope_kind: ScopeKind::Unit,
        scope_id: "u-1".into(),
        request_digest: Digest::of_bytes(b"progress new lifetime"),
        principal_kind: PrincipalKind::Agent,
        principal_id: "h1".into(),
        expected_version: Some(3),
        authority_epoch: None,
        attempt_token: Some(token),
        causation_id: None,
        correlation_id: None,
        method: Method::ProgressReport {
            unit_id: "u-1".into(),
        },
    };
    completed(funnel.submit(&live));
}
