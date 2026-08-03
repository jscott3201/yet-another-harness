//! §4 effect lifecycle over the funnel, closed-loop against the scripted
//! fake-effect backend. Shapes proven here: idempotent prepare + the
//! cross-lifetime operation key (row A13), the two-step dispatch that makes
//! the R20 approval predicate a gate rather than a complaint, declared vs
//! post-hoc target classification (the §5-independent half of A5), the
//! withheld-outcome reconcile parking (the §5-independent half of A16), and
//! the §3.3 authorization classes on both sides. Rows A6 and A14 proper
//! (cancellation races) land with the §5 layer, not here.

mod common;

use common::*;
use oa_kernel::effect::fake::{FakeEffectBackend, Observation, ScriptedOutcome};
use oa_kernel::effect::{EffectTerminal, RetryClass, ReversibilityClass};
use oa_kernel::error::ErrorKind;
use oa_kernel::funnel::{Funnel, Method, PrincipalKind, SettleEvidence};
use oa_kernel::ids::Uuid7;
use oa_kernel::store::Store;

fn prepare(
    ctx: &mut Ctx,
    digest_src: &str,
    token: &oa_kernel::store::AttemptTokenClaims,
    spec: oa_kernel::funnel::EffectSpec,
) -> (String, Uuid7, bool) {
    let cmd = ctx.prepare_cmd(digest_src, "u-1", token.clone(), spec);
    let result = completed(ctx.funnel.submit(&cmd));
    let key = result["operation_key"].as_str().expect("key").to_owned();
    let id = Uuid7::try_from(result["effect_intent_id"].as_str().expect("id").to_owned())
        .expect("intent id parses");
    (key, id, result["existing"].as_bool().expect("existing"))
}

fn record_json(ctx: &Ctx, key: &str) -> serde_json::Value {
    let row = ctx
        .funnel
        .store()
        .effect_record(key)
        .expect("effect row exists");
    serde_json::from_str(&row.record).expect("record parses")
}

#[test]
fn prepare_is_idempotent_and_rejects_every_divergent_axis() {
    let mut ctx = Ctx::new();
    ctx.seed_unit();
    let token = ctx.dispatch("u-1", "h1", 1);

    let logical_op = Uuid7::mint(7, 1);
    let (key, id, existing) = prepare(&mut ctx, "p1", &token, effect_spec(logical_op, "req"));
    assert!(!existing);
    let journal_len = ctx.funnel.store().journal().len();

    // Re-preparing the same logical operation returns the existing intent —
    // no second record, no event, no version bump (§4.1).
    let (key2, id2, existing2) = prepare(&mut ctx, "p2", &token, effect_spec(logical_op, "req"));
    assert!(existing2);
    assert_eq!((key2.as_str(), id2), (key.as_str(), id));
    assert_eq!(ctx.funnel.store().journal().len(), journal_len);
    assert_eq!(
        ctx.funnel.store().effect_record(&key).expect("row").version,
        1
    );

    // Every declared axis is a divergence, including the two the first
    // review missed: declared_targets and approval_ref.
    let base = effect_spec(logical_op, "req");
    let variants: Vec<(&str, oa_kernel::funnel::EffectSpec)> = vec![
        (
            "adapter_id",
            oa_kernel::funnel::EffectSpec {
                adapter_id: "other".into(),
                ..base.clone()
            },
        ),
        (
            "adapter_version",
            oa_kernel::funnel::EffectSpec {
                adapter_version: "9".into(),
                ..base.clone()
            },
        ),
        (
            "retry_class",
            oa_kernel::funnel::EffectSpec {
                retry_class: RetryClass::NoRetry,
                ..base.clone()
            },
        ),
        (
            "reversibility_class",
            oa_kernel::funnel::EffectSpec {
                reversibility_class: ReversibilityClass::Irreversible,
                ..base.clone()
            },
        ),
        (
            "decomposable",
            oa_kernel::funnel::EffectSpec {
                decomposable: true,
                ..base.clone()
            },
        ),
        (
            "compensation_intent_id",
            oa_kernel::funnel::EffectSpec {
                compensation_intent_id: Some(Uuid7::mint(1, 1)),
                ..base.clone()
            },
        ),
        (
            "approval_ref",
            oa_kernel::funnel::EffectSpec {
                approval_ref: Some(Uuid7::mint(2, 2)),
                ..base.clone()
            },
        ),
        (
            "declared_targets",
            declared_spec(logical_op, "req", &["a", "b"]),
        ),
    ];
    for (axis, spec) in variants {
        let cmd = ctx.prepare_cmd(&format!("divergent {axis}"), "u-1", token.clone(), spec);
        assert_eq!(
            rejected(ctx.funnel.submit(&cmd)),
            (ErrorKind::InvalidRequest, false),
            "divergence on {axis} must be refused, not silently ignored"
        );
    }
    // None of the refusals touched the stored intent.
    assert_eq!(
        ctx.funnel.store().effect_record(&key).expect("row").version,
        1
    );
    assert!(record_json(&ctx, &key)["approval_ref"].is_null());

    // A different request digest is a different logical operation.
    let (key3, _, existing3) =
        prepare(&mut ctx, "p3", &token, effect_spec(logical_op, "other req"));
    assert!(!existing3);
    assert_ne!(key3, key);
}

#[test]
fn enumeration_mode_is_declared_not_inferred() {
    // §4.3: post-hoc is an adapter DECLARATION. An empty target list must
    // never be readable as "post-hoc", or an adapter that skipped its
    // enumeration duty would silently buy the untrusted path.
    let mut ctx = Ctx::new();
    ctx.seed_unit();
    let token = ctx.dispatch("u-1", "h1", 1);

    let mut declared_but_empty = declared_spec(Uuid7::mint(7, 20), "req", &["a"]);
    declared_but_empty.declared_targets.clear();
    let cmd = ctx.prepare_cmd("declared empty", "u-1", token.clone(), declared_but_empty);
    assert_eq!(
        rejected(ctx.funnel.submit(&cmd)),
        (ErrorKind::InvalidRequest, false)
    );

    let mut post_hoc_with_targets = effect_spec(Uuid7::mint(7, 21), "req");
    post_hoc_with_targets.declared_targets = vec![target_declared("a", "want", "pre")];
    let cmd2 = ctx.prepare_cmd("post_hoc with targets", "u-1", token, post_hoc_with_targets);
    assert_eq!(
        rejected(ctx.funnel.submit(&cmd2)),
        (ErrorKind::InvalidRequest, false)
    );
}

#[test]
fn dispatch_is_a_committed_transition_before_the_adapter_acts() {
    // §3.3 row 5: `prepared -> dispatching` authorizes the adapter. The
    // record must show the authorization separately from the report.
    let mut ctx = Ctx::new();
    ctx.seed_unit();
    let token = ctx.dispatch("u-1", "h1", 1);
    let (key, _, _) = prepare(
        &mut ctx,
        "prepare",
        &token,
        effect_spec(Uuid7::mint(7, 30), "req"),
    );

    // record_dispatched before the authorizing transition is refused — and
    // NOT persisted, because a later effect.dispatch makes it lawful.
    let premature = ctx.record_dispatched_cmd("premature", "u-1", token.clone(), &key, 1, 100);
    assert_eq!(
        rejected(ctx.funnel.submit(&premature)),
        (ErrorKind::InvalidRequest, false)
    );
    assert_eq!(
        rejected(ctx.funnel.submit(&premature)),
        (ErrorKind::InvalidRequest, false),
        "state-dependent refusals must re-validate, never replay stale"
    );

    let d = ctx.dispatch_effect_cmd("dispatch", "u-1", token.clone(), &key, 1);
    completed(ctx.funnel.submit(&d));
    assert_eq!(
        ctx.funnel.store().effect_record(&key).expect("row").state,
        "dispatching"
    );
    // The refusal never persisted, so the same shape succeeds once the
    // state is right (at the version the dispatch moved the effect to).
    let record = ctx.record_dispatched_cmd("record", "u-1", token.clone(), &key, 2, 100);
    completed(ctx.funnel.submit(&record));
    let row = ctx.funnel.store().effect_record(&key).expect("row");
    assert_eq!(row.state, "dispatched");
    assert_eq!(record_json(&ctx, &key)["dispatched_at"], 100);

    let kinds: Vec<String> = ctx
        .funnel
        .store()
        .journal()
        .iter()
        .filter(|e| e.aggregate_kind == "effect")
        .map(|e| e.event_kind.clone())
        .collect();
    assert_eq!(
        kinds,
        ["effect.prepared", "effect.dispatching", "effect.dispatched"]
    );
}

#[test]
fn irreversible_without_approval_cannot_reach_dispatching() {
    // R20's predicate at the boundary that matters: the gate must fire
    // BEFORE the adapter is authorized, and it is permanent for this
    // intent — no method in this milestone can attach an approval, so the
    // lawful path is a new logical operation once the approval commits.
    let mut ctx = Ctx::new();
    ctx.seed_unit();
    let token = ctx.dispatch("u-1", "h1", 1);

    let mut spec = effect_spec(Uuid7::mint(7, 5), "req");
    spec.reversibility_class = ReversibilityClass::Irreversible;
    let (key, _, _) = prepare(&mut ctx, "prepare", &token, spec.clone());

    let d = ctx.dispatch_effect_cmd("dispatch", "u-1", token.clone(), &key, 1);
    assert_eq!(
        rejected(ctx.funnel.submit(&d)),
        (ErrorKind::ApprovalRequired, false)
    );
    // Permanently deterministic for this intent: persisted and replayed.
    assert_eq!(
        rejected(ctx.funnel.submit(&d)),
        (ErrorKind::ApprovalRequired, true)
    );
    // Nothing was authorized, so nothing can have been dispatched.
    assert_eq!(
        ctx.funnel.store().effect_record(&key).expect("row").state,
        "prepared"
    );

    // A holder cannot heal it by re-preparing with an approval it invented.
    let mut self_approved = spec.clone();
    self_approved.approval_ref = Some(Uuid7::mint(9, 9999));
    let attach = ctx.prepare_cmd("self approve", "u-1", token.clone(), self_approved);
    assert_eq!(
        rejected(ctx.funnel.submit(&attach)),
        (ErrorKind::InvalidRequest, false)
    );
    assert!(record_json(&ctx, &key)["approval_ref"].is_null());

    // The lawful path: a NEW logical operation carrying the committed
    // approval — safe precisely because the gate proved nothing dispatched.
    let mut approved = effect_spec(Uuid7::mint(7, 6), "req");
    approved.reversibility_class = ReversibilityClass::Irreversible;
    approved.approval_ref = Some(Uuid7::mint(7, 7));
    let (key2, _, _) = prepare(&mut ctx, "prepare approved", &token, approved);
    let d2 = ctx.dispatch_effect_cmd("dispatch approved", "u-1", token, &key2, 1);
    completed(ctx.funnel.submit(&d2));
}

#[test]
fn operation_key_survives_kill_recover_and_rework() {
    // Row A13: dispatch, kill, takeover, re-dispatch — the target sees
    // exactly one operation key for the logical operation.
    let mut backend = FakeEffectBackend::new();
    let mut ctx = Ctx::new();
    ctx.seed_unit();
    let t1 = ctx.dispatch("u-1", "h1", 1);

    let logical_op = Uuid7::mint(7, 1);
    let (key, id, _) = prepare(&mut ctx, "prepare", &t1, effect_spec(logical_op, "req"));
    backend.script(&key, ScriptedOutcome::Withheld(EffectTerminal::Succeeded));
    let v = ctx.dispatch_and_record("first", "u-1", &t1, &key, 1, 100);
    assert_eq!(v, 3);
    backend
        .dispatch(&backend_intent(&key, id, "u-1"))
        .expect("scripted");

    // Kill the kernel; the external world (the backend) survives.
    let dir = ctx.dir;
    drop(ctx.funnel);
    let store = Store::recover(dir.path(), "kernel-b").expect("recover");
    let mut ctx = Ctx::resume(dir, Funnel::new(store, 1_000), 100);

    // Takeover: new epoch, new holder, same logical operation.
    let t2 = ctx.dispatch("u-1", "h2", 2);
    let (key_after, id_after, existing) = prepare(
        &mut ctx,
        "prepare after recover",
        &t2,
        effect_spec(logical_op, "req"),
    );
    assert!(existing, "re-prepare returns the existing intent");
    assert_eq!((key_after.as_str(), id_after), (key.as_str(), id));

    // Retry the dispatch under the new epoch: same key at the target.
    let v2 = ctx.dispatch_and_record("retry", "u-1", &t2, &key, 3, 150);
    assert_eq!(v2, 5);
    backend
        .dispatch(&backend_intent(&key, id, "u-1"))
        .expect("scripted");
    assert_eq!(backend.dispatch_count(&key), 2);
    assert_eq!(backend.distinct_keys(), vec![key.as_str()]);

    // The AUTHORITY settles the revealed outcome — no token, and epoch 1's
    // holder is long gone.
    assert_eq!(
        backend.reconcile(&key),
        Observation::Terminal(EffectTerminal::Succeeded)
    );
    let settle = ctx.settle_clean(
        "settle",
        "u-1",
        &key,
        5,
        EffectTerminal::Succeeded,
        vec![],
        200,
    );
    completed(ctx.funnel.submit(&settle));
    let row = ctx.funnel.store().effect_record(&key).expect("row");
    assert_eq!(
        (row.state.as_str(), row.terminal.as_deref()),
        ("settled", Some("succeeded"))
    );
}

#[test]
fn settle_is_authority_class_and_survives_stamp_bumps() {
    // §3.3 row 6: a stamp bump kills the holder's token, but the KNOWN
    // outcome still settles — fencing settle would manufacture unknown.
    let mut ctx = Ctx::new();
    ctx.seed_unit();
    let token = ctx.dispatch("u-1", "h1", 1); // unit v2

    let (key, _, _) = prepare(
        &mut ctx,
        "prepare",
        &token,
        effect_spec(Uuid7::mint(7, 9), "req"),
    );
    let v = ctx.dispatch_and_record("first", "u-1", &token, &key, 1, 100);
    let (key2, _, _) = prepare(
        &mut ctx,
        "prepare 2",
        &token,
        effect_spec(Uuid7::mint(7, 10), "req"),
    );
    ctx.dispatch_and_record("second", "u-1", &token, &key2, 1, 110);

    // Authority invalidates the holder (row A4)...
    let bump = ctx.authority_cmd(
        "bump",
        Some(2),
        Method::StampBump {
            unit_id: "u-1".into(),
        },
    );
    completed(ctx.funnel.submit(&bump));
    // ...the holder is genuinely dead...
    let dead = ctx.dispatch_effect_cmd("dead dispatch", "u-1", token.clone(), &key2, 3);
    assert_eq!(
        rejected(ctx.funnel.submit(&dead)),
        (ErrorKind::FenceRejected, false)
    );
    // ...and the settle still lands, tokenless, under the current epoch.
    let settle = ctx.settle_clean(
        "settle",
        "u-1",
        &key,
        v,
        EffectTerminal::Succeeded,
        vec![],
        200,
    );
    completed(ctx.funnel.submit(&settle));

    // A settle carrying a token, or one from a non-daemon principal, is
    // malformed for the authority class.
    let mut with_token = ctx.settle_clean(
        "settle token",
        "u-1",
        &key2,
        3,
        EffectTerminal::Unknown,
        vec![],
        300,
    );
    with_token.attempt_token = Some(token);
    assert_eq!(
        rejected(ctx.funnel.submit(&with_token)),
        (ErrorKind::InvalidRequest, false)
    );
    let mut as_agent = ctx.settle_clean(
        "settle agent",
        "u-1",
        &key2,
        3,
        EffectTerminal::Unknown,
        vec![],
        310,
    );
    as_agent.principal_kind = PrincipalKind::Agent;
    assert_eq!(
        rejected(ctx.funnel.submit(&as_agent)),
        (ErrorKind::InvalidRequest, false)
    );
    // The daemon's own settle is unaffected.
    let ok = ctx.settle_clean(
        "settle ok",
        "u-1",
        &key2,
        3,
        EffectTerminal::Unknown,
        vec![],
        320,
    );
    completed(ctx.funnel.submit(&ok));
}

#[test]
fn holder_methods_require_the_agent_principal() {
    // The other half of the class split: a daemon may not act as a holder.
    let mut ctx = Ctx::new();
    ctx.seed_unit();
    let token = ctx.dispatch("u-1", "h1", 1);

    let mut as_daemon = ctx.prepare_cmd(
        "prepare as daemon",
        "u-1",
        token,
        effect_spec(Uuid7::mint(7, 40), "req"),
    );
    as_daemon.principal_kind = PrincipalKind::Daemon;
    assert_eq!(
        rejected(ctx.funnel.submit(&as_daemon)),
        (ErrorKind::InvalidRequest, false)
    );
}

#[test]
fn settle_legality_follows_the_lifecycle_and_5_3_disjuncts() {
    let mut ctx = Ctx::new();
    ctx.seed_unit();
    let token = ctx.dispatch("u-1", "h1", 1);

    // From `prepared`: no dispatch evidence exists, so succeeded/failed and
    // even `unknown` are refused — I8 proves nothing happened.
    let (k1, _, _) = prepare(
        &mut ctx,
        "p1",
        &token,
        effect_spec(Uuid7::mint(7, 11), "req"),
    );
    for terminal in [
        EffectTerminal::Succeeded,
        EffectTerminal::Failed,
        EffectTerminal::Unknown,
    ] {
        let c = ctx.settle_clean(
            &format!("settle {terminal:?}"),
            "u-1",
            &k1,
            1,
            terminal,
            vec![],
            100,
        );
        assert_eq!(
            rejected(ctx.funnel.submit(&c)),
            (ErrorKind::InvalidRequest, false),
            "{terminal:?} must not be settleable from prepared"
        );
    }
    let park = ctx.park_cmd("park prepared", "u-1", &k1, 1, 5_000);
    assert_eq!(
        rejected(ctx.funnel.submit(&park)),
        (ErrorKind::InvalidRequest, false)
    );
    // §5.2 rule 4: proved never dispatched settles cancelled.
    let cancel = ctx.settle_clean(
        "cancel prepared",
        "u-1",
        &k1,
        1,
        EffectTerminal::Cancelled,
        vec![],
        200,
    );
    completed(ctx.funnel.submit(&cancel));

    // From `dispatched`: cancelled needs §5.3's SECOND disjunct.
    let (k2, _, _) = prepare(
        &mut ctx,
        "p2",
        &token,
        effect_spec(Uuid7::mint(7, 12), "req"),
    );
    let v = ctx.dispatch_and_record("d2", "u-1", &token, &k2, 1, 300);
    let bare_cancel = ctx.settle_clean(
        "cancel dispatched",
        "u-1",
        &k2,
        v,
        EffectTerminal::Cancelled,
        vec![],
        400,
    );
    assert_eq!(
        rejected(ctx.funnel.submit(&bare_cancel)),
        (ErrorKind::InvalidRequest, false)
    );
    let reported = ctx.settle_cmd(
        "cancel reported",
        "u-1",
        &k2,
        v,
        EffectTerminal::Cancelled,
        vec![],
        SettleEvidence {
            clean_wait_status: false,
            target_reported_cancellation: true,
        },
        500,
    );
    completed(ctx.funnel.submit(&reported));

    // Compensated needs a registered compensation intent and a class that
    // admits compensation at all.
    let (k3, _, _) = prepare(
        &mut ctx,
        "p3",
        &token,
        effect_spec(Uuid7::mint(7, 13), "req"),
    );
    let v3 = ctx.dispatch_and_record("d3", "u-1", &token, &k3, 1, 600);
    let no_comp = ctx.settle_clean(
        "compensate bare",
        "u-1",
        &k3,
        v3,
        EffectTerminal::Compensated,
        vec![],
        700,
    );
    assert_eq!(
        rejected(ctx.funnel.submit(&no_comp)),
        (ErrorKind::InvalidRequest, false)
    );

    let mut irreversible = effect_spec(Uuid7::mint(7, 14), "req");
    irreversible.reversibility_class = ReversibilityClass::Irreversible;
    irreversible.approval_ref = Some(Uuid7::mint(7, 15));
    irreversible.compensation_intent_id = Some(Uuid7::mint(7, 16));
    let (k4, _, _) = prepare(&mut ctx, "p4", &token, irreversible);
    let v4 = ctx.dispatch_and_record("d4", "u-1", &token, &k4, 1, 800);
    let irr_comp = ctx.settle_clean(
        "compensate irreversible",
        "u-1",
        &k4,
        v4,
        EffectTerminal::Compensated,
        vec![],
        900,
    );
    assert_eq!(
        rejected(ctx.funnel.submit(&irr_comp)),
        (ErrorKind::InvalidRequest, false),
        "§5.3 R20: compensation is by definition unavailable for irreversible"
    );

    let mut reversible = effect_spec(Uuid7::mint(7, 17), "req");
    reversible.reversibility_class = ReversibilityClass::ReversibleExternal;
    reversible.compensation_intent_id = Some(Uuid7::mint(7, 18));
    let (k5, _, _) = prepare(&mut ctx, "p5", &token, reversible);
    let v5 = ctx.dispatch_and_record("d5", "u-1", &token, &k5, 1, 1_000);
    let ok = ctx.settle_clean(
        "compensate ok",
        "u-1",
        &k5,
        v5,
        EffectTerminal::Compensated,
        vec![],
        1_100,
    );
    completed(ctx.funnel.submit(&ok));
}

#[test]
fn withheld_outcome_parks_reconciling_then_settles() {
    // The §5-independent half of A16: no outcome at observe time → the
    // intent parks at `reconciling` with a DURABLE backoff position.
    let mut backend = FakeEffectBackend::new();
    let mut ctx = Ctx::new();
    ctx.seed_unit();
    let token = ctx.dispatch("u-1", "h1", 1);

    let (key, id, _) = prepare(
        &mut ctx,
        "prepare",
        &token,
        effect_spec(Uuid7::mint(7, 2), "req"),
    );
    backend.script(&key, ScriptedOutcome::Withheld(EffectTerminal::Failed));
    let v = ctx.dispatch_and_record("d", "u-1", &token, &key, 1, 100);
    backend
        .dispatch(&backend_intent(&key, id, "u-1"))
        .expect("scripted");

    assert_eq!(backend.observe(&key), Observation::Pending);
    let park = ctx.park_cmd("park", "u-1", &key, v, 5_000);
    completed(ctx.funnel.submit(&park));
    let row = ctx.funnel.store().effect_record(&key).expect("row");
    assert_eq!(row.state, "reconciling");
    assert_eq!(record_json(&ctx, &key)["next_reconcile_at"], 5_000);

    assert_eq!(
        backend.reconcile(&key),
        Observation::Terminal(EffectTerminal::Failed)
    );
    let settle = ctx.settle_clean(
        "settle",
        "u-1",
        &key,
        v + 1,
        EffectTerminal::Failed,
        vec![],
        6_000,
    );
    completed(ctx.funnel.submit(&settle));
    assert_eq!(record_json(&ctx, &key)["settled_at"], 6_000);

    // Terminal is write-once: re-settling is refused and persists.
    let resettle = ctx.settle_clean(
        "resettle",
        "u-1",
        &key,
        v + 2,
        EffectTerminal::Succeeded,
        vec![],
        7_000,
    );
    assert_eq!(
        rejected(ctx.funnel.submit(&resettle)),
        (ErrorKind::InvalidRequest, false)
    );
    assert_eq!(
        rejected(ctx.funnel.submit(&resettle)),
        (ErrorKind::InvalidRequest, true)
    );

    let effects: Vec<(String, u64)> = ctx
        .funnel
        .store()
        .journal()
        .iter()
        .filter(|e| e.aggregate_kind == "effect")
        .map(|e| (e.event_kind.clone(), e.aggregate_version))
        .collect();
    assert_eq!(
        effects,
        [
            ("effect.prepared".into(), 1),
            ("effect.dispatching".into(), 2),
            ("effect.dispatched".into(), 3),
            ("effect.reconciling".into(), 4),
            ("effect.settled".into(), 5),
        ]
    );
}

#[test]
fn no_retry_class_refuses_re_dispatch() {
    // §4.2: automatic retry is rejected for adapters that declared
    // no_retry — refused at the authorizing boundary, before the adapter.
    let mut ctx = Ctx::new();
    ctx.seed_unit();
    let token = ctx.dispatch("u-1", "h1", 1);

    let mut spec = effect_spec(Uuid7::mint(7, 13), "req");
    spec.retry_class = RetryClass::NoRetry;
    let (key, _, _) = prepare(&mut ctx, "prepare", &token, spec);
    let v = ctx.dispatch_and_record("d", "u-1", &token, &key, 1, 100);

    let again = ctx.dispatch_effect_cmd("redispatch", "u-1", token, &key, v);
    assert_eq!(
        rejected(ctx.funnel.submit(&again)),
        (ErrorKind::InvalidRequest, false)
    );
}

#[test]
fn effect_ownership_binds_to_the_unit_on_every_arm() {
    // A foreign operation_key is a malformed (unit, key) pair, and the
    // binding is checked BEFORE any property of the foreign row can leak.
    let mut ctx = Ctx::new();
    ctx.seed_unit(); // wi-1, u-1
    let admit2 = ctx.admit("u-2", "wi-1");
    completed(ctx.funnel.submit(&admit2));
    let t1 = ctx.dispatch("u-1", "h1", 1);
    let t2 = ctx.dispatch("u-2", "h2", 1);

    let logical_op = Uuid7::mint(7, 14);
    let (key, _, _) = prepare(&mut ctx, "prepare", &t1, effect_spec(logical_op, "req"));

    // Holder of u-2 naming u-1's key, with a wildly wrong expected version:
    // the binding fires first, so u-1's real version never leaks.
    let foreign = ctx.holder_cmd(
        "foreign dispatch",
        t2.clone(),
        Some(77),
        Method::EffectDispatch {
            unit_id: "u-2".into(),
            operation_key: key.clone(),
        },
    );
    match ctx.funnel.submit(&foreign) {
        oa_kernel::funnel::Submission::Rejected { kind, detail, .. } => {
            assert_eq!(kind, ErrorKind::InvalidRequest);
            assert!(detail.contains("belongs to unit u-1"), "got: {detail}");
            assert!(
                !detail.contains("actual 1"),
                "must not leak the foreign version: {detail}"
            );
        }
        other => panic!("expected Rejected, got {other:?}"),
    }

    // Sibling unit on the SAME work item derives the same key; capture is
    // refused rather than resolving to u-1's intent.
    let sibling = ctx.holder_cmd(
        "sibling prepare",
        t2,
        None,
        Method::EffectPrepare {
            unit_id: "u-2".into(),
            spec: effect_spec(logical_op, "req"),
        },
    );
    assert_eq!(
        rejected(ctx.funnel.submit(&sibling)),
        (ErrorKind::InvalidRequest, false)
    );

    // The authority arms bind too: naming u-2 for u-1's key is malformed.
    let wrong = ctx.settle_clean(
        "settle wrong unit",
        "u-2",
        &key,
        1,
        EffectTerminal::Cancelled,
        vec![],
        200,
    );
    assert_eq!(
        rejected(ctx.funnel.submit(&wrong)),
        (ErrorKind::InvalidRequest, false)
    );

    // The owning unit proceeds normally.
    ctx.dispatch_and_record("owner", "u-1", &t1, &key, 1, 300);
}

#[test]
fn effect_mutations_require_the_version_cas() {
    let mut ctx = Ctx::new();
    ctx.seed_unit();
    let t1 = ctx.dispatch("u-1", "h1", 1);
    let t2 = ctx.dispatch("u-1", "h2", 2); // supersedes t1

    let stale = ctx.prepare_cmd(
        "stale prepare",
        "u-1",
        t1,
        effect_spec(Uuid7::mint(7, 8), "req"),
    );
    assert_eq!(
        rejected(ctx.funnel.submit(&stale)),
        (ErrorKind::FenceRejected, false)
    );

    let (key, _, _) = prepare(
        &mut ctx,
        "live prepare",
        &t2,
        effect_spec(Uuid7::mint(7, 8), "req"),
    );
    let stale_v = ctx.dispatch_effect_cmd("stale version", "u-1", t2.clone(), &key, 9);
    assert_eq!(
        rejected(ctx.funnel.submit(&stale_v)),
        (ErrorKind::VersionConflict, false)
    );
    let blind = ctx.holder_cmd(
        "blind",
        t2,
        None,
        Method::EffectDispatch {
            unit_id: "u-1".into(),
            operation_key: key,
        },
    );
    assert_eq!(
        rejected(ctx.funnel.submit(&blind)),
        (ErrorKind::InvalidRequest, false)
    );
}
