//! §5 cancellation protocol over the funnel: obligation 4 (cancellation-tree
//! settlement), the A14 prepare/dispatch race close, A6 (dispatched effect
//! parks reconciling), the rule-4 no-admission gate, the write-once delivery
//! contract, and the leaf-first delivery order.
//!
//! Every test is a closed loop against the store: submit commands through
//! the funnel and assert durable rows (journal, effect_record,
//! cancel_request_row, cancel_delivery_rows), not in-memory results.

mod common;

use common::*;
use oa_kernel::cancel::{
    CancelKind, CancelPolicy, CancelReason, DeliveryOutcome, MAX_SCOPE_MEMBERS,
};
use oa_kernel::effect::EffectTerminal;
use oa_kernel::error::ErrorKind;
use oa_kernel::funnel::{Funnel, RunOutcome, SettleEvidence, Submission, token_from_result};
use oa_kernel::ids::Uuid7;
use oa_kernel::store::Store;

fn prepare(
    ctx: &mut Ctx,
    digest_src: &str,
    unit: &str,
    token: &oa_kernel::store::AttemptTokenClaims,
    spec: oa_kernel::funnel::EffectSpec,
) -> (String, Uuid7, bool) {
    let cmd = ctx.prepare_cmd(digest_src, unit, token.clone(), spec);
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

/// run-1/wi-1/u-1, dispatch attempt epoch 1, prepare ONE effect and drive it
/// to `dispatched` (v3). Returns (attempt_id, effect key, intent id, version).
fn seed_dispatched_effect(ctx: &mut Ctx) -> (String, String, Uuid7, u64) {
    ctx.seed_unit();
    let dispatch = ctx.dispatch_cmd("u-1", "h1", 1);
    let result = completed(ctx.funnel.submit(&dispatch));
    let attempt_id = result["attempt_id"]
        .as_str()
        .expect("attempt id")
        .to_owned();
    let token = token_from_result(&result).expect("token");
    let (key, id, _) = prepare(
        ctx,
        "prepare",
        "u-1",
        &token,
        effect_spec(Uuid7::mint(7, 1), "req"),
    );
    let v = ctx.dispatch_and_record("d", "u-1", &token, &key, 1, 100);
    (attempt_id, key, id, v)
}

#[test]
fn cancel_request_commits_durable_before_any_signal() {
    // I10: the request row (frozen scope, status requested) is durable with
    // exactly one event and NO delivery row yet — the signal is the driver's
    // contract, not the commit's.
    let mut ctx = Ctx::new();
    ctx.seed_unit();
    let dispatch = ctx.dispatch_cmd("u-1", "h1", 1);
    completed(ctx.funnel.submit(&dispatch));

    let req = ctx.cancel_request_cmd(
        "cancel run",
        CancelKind::Run,
        "run-1",
        CancelReason::OwnerRequest,
        CancelPolicy::AttachedCascade,
        vec![member_input(CancelKind::ExecutionUnit, "u-1")],
    );
    let result = completed(ctx.funnel.submit(&req));
    let request_id = result["cancel_request_id"].as_str().expect("request id");
    assert_eq!(result["status"], "requested");
    assert_eq!(result["root_id"], "run-1");

    // One event on the cancel_request aggregate, v1.
    let events: Vec<_> = ctx
        .funnel
        .store()
        .journal()
        .unwrap()
        .into_iter()
        .filter(|e| e.aggregate_kind == "cancel_request")
        .collect();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].event_kind, "cancel_request.requested");
    assert_eq!(events[0].aggregate_version, 1);

    // Durable row reads back as requested with the frozen scope; NO delivery
    // row exists anywhere.
    let row = ctx
        .funnel
        .store()
        .cancel_request_row(request_id)
        .expect("request row");
    assert_eq!(row.status, "requested");
    assert_eq!(row.version, 1);
    assert!(row.scope.contains("u-1"));
    assert!(ctx.funnel.store().cancel_delivery_rows().is_empty());
}

#[test]
fn rule_4_bars_admission_after_a_committed_cancel() {
    // §5.2 rule 4: the SAME committed request bars a new unit, a new
    // attempt, and a new intent — the tree is cancelled, and later-created
    // members are governed by the request, never retroactively added
    // (rule 2). The gate is shape-deterministic: it persists.
    let mut ctx = Ctx::new();
    ctx.seed_unit();
    let token = ctx.dispatch("u-1", "h1", 1);

    let req = ctx.cancel_request_cmd(
        "cancel run",
        CancelKind::Run,
        "run-1",
        CancelReason::OwnerRequest,
        CancelPolicy::RootOnly,
        vec![member_input(CancelKind::ExecutionUnit, "u-1")],
    );
    completed(ctx.funnel.submit(&req));

    // A new unit into the cancelled run.
    let admit = ctx.admit("u-2", "wi-1");
    assert_eq!(
        rejected(ctx.funnel.submit(&admit)),
        (ErrorKind::InvalidRequest, false)
    );
    assert_eq!(
        rejected(ctx.funnel.submit(&admit)),
        (ErrorKind::InvalidRequest, true),
        "a committed cancellation cannot be un-committed — the rejection persists"
    );

    // A new attempt for the existing unit.
    let dispatch = ctx.dispatch_cmd("u-1", "h2", 2);
    assert_eq!(
        rejected(ctx.funnel.submit(&dispatch)),
        (ErrorKind::InvalidRequest, false)
    );

    // A new intent under the cancelled tree (holder token minted before).
    let prep = ctx.prepare_cmd(
        "prepare barred",
        "u-1",
        token,
        effect_spec(Uuid7::mint(7, 2), "req"),
    );
    assert_eq!(
        rejected(ctx.funnel.submit(&prep)),
        (ErrorKind::InvalidRequest, false)
    );
}

#[test]
fn delivery_order_is_leaf_first_and_pair_is_write_once() {
    // §5.2 rule 3: a frozen [effect, attempt] scope must deliver the effect
    // (leaf, order 0) before the attempt (order 1); a re-delivery of a pair
    // is refused and PERSISTS — the write-once §5.1 pair.
    let mut ctx = Ctx::new();
    let (attempt_id, _key, intent, _) = seed_dispatched_effect(&mut ctx);

    let req = ctx.cancel_request_cmd(
        "cancel run",
        CancelKind::Run,
        "run-1",
        CancelReason::OwnerRequest,
        CancelPolicy::AttachedCascade,
        vec![
            member_input(CancelKind::Attempt, &attempt_id),
            member_input(CancelKind::EffectIntent, &intent.to_string()),
        ],
    );
    let request_id = completed(ctx.funnel.submit(&req))["cancel_request_id"]
        .as_str()
        .expect("request id")
        .to_owned();

    // Leaf-first violation: the attempt (order 1) before the effect (0).
    let early = ctx.cancel_delivery_cmd(
        "attempt before leaf",
        &request_id,
        &attempt_id,
        200,
        Some(200),
        DeliveryOutcome::ObservedStopped,
    );
    assert_eq!(
        rejected(ctx.funnel.submit(&early)),
        (ErrorKind::InvalidRequest, false)
    );
    // The rejection is NOT persisted (healable): once the true leaf is
    // delivered, the very same command becomes lawful, so a retry must
    // re-validate rather than replay a stale stored rejection.
    assert_eq!(
        rejected(ctx.funnel.submit(&early)),
        (ErrorKind::InvalidRequest, false)
    );

    // The leaf first...
    let leaf = ctx.cancel_deliver_stopped(&request_id, &intent.to_string(), 210);
    let result = completed(ctx.funnel.submit(&leaf));
    assert_eq!(result["status"], "delivering", "attempt still on the wire");
    assert_eq!(result["outcome"], "observed_stopped");

    // ...and the SAME early attempt command, rejected before, is now lawful.
    let retry = ctx.cancel_delivery_cmd(
        "retry attempt after leaf",
        &request_id,
        &attempt_id,
        205,
        Some(205),
        DeliveryOutcome::ObservedStopped,
    );
    let result = completed(ctx.funnel.submit(&retry));
    assert_eq!(result["status"], "settled");

    // Write-once: re-delivering the pair is refused and persists.
    let again = ctx.cancel_deliver_stopped(&request_id, &intent.to_string(), 230);
    assert_eq!(
        rejected(ctx.funnel.submit(&again)),
        (ErrorKind::InvalidRequest, false)
    );
    assert_eq!(
        rejected(ctx.funnel.submit(&again)),
        (ErrorKind::InvalidRequest, true)
    );

    // The whole walk was one event per command with measured versions.
    let events: Vec<(String, u64)> = ctx
        .funnel
        .store()
        .journal()
        .unwrap()
        .into_iter()
        .filter(|e| e.aggregate_kind == "cancel_request")
        .map(|e| (e.event_kind.clone(), e.aggregate_version))
        .collect();
    assert_eq!(
        events,
        [
            ("cancel_request.requested".into(), 1),
            ("cancel_request.delivered".into(), 2),
            ("cancel_request.delivered".into(), 3),
        ]
    );
    // Durably: two delivery rows, keyed by the derived UNIQUE pair.
    let rows = ctx.funnel.store().cancel_delivery_rows();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].cancel_request_id, request_id);
    assert!(rows[0].delivery_key.contains('|'));
}

#[test]
fn unresponsive_member_never_settles_the_request() {
    // §5.1/§5.3: an unresponsive outcome is a recorded observation — the
    // request reaches observed_partial, not settled, and the write-once
    // pair means the unresponsive row can never be upgraded in place.
    let mut ctx = Ctx::new();
    let (attempt_id, _key, intent, _) = seed_dispatched_effect(&mut ctx);

    let req = ctx.cancel_request_cmd(
        "cancel run",
        CancelKind::Run,
        "run-1",
        CancelReason::OwnerRequest,
        CancelPolicy::AttachedCascade,
        vec![
            member_input(CancelKind::Attempt, &attempt_id),
            member_input(CancelKind::EffectIntent, &intent.to_string()),
        ],
    );
    let request_id = completed(ctx.funnel.submit(&req))["cancel_request_id"]
        .as_str()
        .expect("request id")
        .to_owned();

    // The effect (leaf, order 0) is recorded UNRESPONSIVE — an observation,
    // not a placeholder filled later.
    let leaf = ctx.cancel_delivery_cmd(
        "deliver unresponsive",
        &request_id,
        &intent.to_string(),
        300,
        None,
        DeliveryOutcome::Unresponsive,
    );
    let result = completed(ctx.funnel.submit(&leaf));
    assert_eq!(result["outcome"], "unresponsive");

    // All members delivered, one undischarged -> observed_partial, never
    // settled.
    let parent = ctx.cancel_deliver_stopped(&request_id, &attempt_id, 310);
    let result = completed(ctx.funnel.submit(&parent));
    assert_eq!(result["status"], "observed_partial");
    let row = ctx
        .funnel
        .store()
        .cancel_request_row(&request_id)
        .expect("row");
    assert_eq!(row.status, "observed_partial");
    // Upgrading the unresponsive row is impossible (write-once).
    let again = ctx.cancel_deliver_stopped(&request_id, &intent.to_string(), 320);
    assert_eq!(
        rejected(ctx.funnel.submit(&again)),
        (ErrorKind::InvalidRequest, false)
    );
}

#[test]
fn a6_dispatched_effect_without_outcome_parks_reconciling() {
    // A6: a dispatched effect with no authoritative outcome stays
    // `reconciling` with terminal unset — cancel NEVER converts uncertain
    // into cancelled (I7).
    let mut ctx = Ctx::new();
    let (_, key, intent, v) = seed_dispatched_effect(&mut ctx);

    let req = ctx.cancel_request_cmd(
        "cancel effect",
        CancelKind::Run,
        "run-1",
        CancelReason::PolicyViolation,
        CancelPolicy::AttachedCascade,
        vec![member_input(CancelKind::EffectIntent, &intent.to_string())],
    );
    let request_id = completed(ctx.funnel.submit(&req))["cancel_request_id"]
        .as_str()
        .expect("request id")
        .to_owned();

    let delivery = ctx.cancel_deliver_stopped(&request_id, &intent.to_string(), 400);
    let result = completed(ctx.funnel.submit(&delivery));
    assert_eq!(result["status"], "settled", "one member, discharged");

    let row = ctx.funnel.store().effect_record(&key).expect("effect row");
    assert_eq!(
        row.state, "reconciling",
        "dispatch evidence parks, not cancels"
    );
    assert_eq!(row.terminal, None, "terminal stays unset (§5.3)");
    assert_eq!(
        row.version,
        v + 1,
        "the park bumps the effect's own version"
    );
    assert_eq!(record_json(&ctx, &key)["state"], "reconciling");
}

#[test]
fn a14_prepared_settles_cancelled_and_dispatching_follows_5_3() {
    // A14: an intent whose `prepared -> dispatching` had NOT committed when
    // the applicable cancellation committed settles terminal=cancelled on
    // its next dispatch attempt (rule-4 second disjunct); an intent already
    // dispatching follows §5.3 (reconciling).
    let mut ctx = Ctx::new();
    ctx.seed_unit();
    let token = ctx.dispatch("u-1", "h1", 1);

    // P stays prepared; D reaches `dispatching` (never recorded dispatched).
    let (p_key, _, _) = prepare(
        &mut ctx,
        "prepared P",
        "u-1",
        &token,
        effect_spec(Uuid7::mint(7, 3), "req P"),
    );
    let (d_key, d_intent, _) = prepare(
        &mut ctx,
        "prepared D",
        "u-1",
        &token,
        effect_spec(Uuid7::mint(7, 4), "req D"),
    );
    let d = ctx.dispatch_effect_cmd("dispatch D", "u-1", token.clone(), &d_key, 1);
    completed(ctx.funnel.submit(&d));
    assert_eq!(
        ctx.funnel.store().effect_record(&d_key).expect("d").state,
        "dispatching"
    );

    let req = ctx.cancel_request_cmd(
        "cancel run",
        CancelKind::Run,
        "run-1",
        CancelReason::OwnerRequest,
        CancelPolicy::RootOnly,
        vec![member_input(
            CancelKind::EffectIntent,
            &d_intent.to_string(),
        )],
    );
    let request_id = completed(ctx.funnel.submit(&req))["cancel_request_id"]
        .as_str()
        .expect("request id")
        .to_owned();

    // P cannot lawfully dispatch after the cancel: the funnel settles it
    // cancelled instead of emitting `dispatching` — one settlement event,
    // no authorization.
    let dp = ctx.dispatch_effect_cmd("dispatch P after cancel", "u-1", token.clone(), &p_key, 1);
    let result = completed(ctx.funnel.submit(&dp));
    assert_eq!(result["state"], "settled");
    let row = ctx.funnel.store().effect_record(&p_key).expect("p");
    assert_eq!(
        (row.state.as_str(), row.terminal.as_deref()),
        ("settled", Some("cancelled"))
    );
    let kinds: Vec<String> = ctx
        .funnel
        .store()
        .journal()
        .unwrap()
        .into_iter()
        .filter(|e| e.aggregate_kind == "effect" && e.aggregate_id == p_key)
        .map(|e| e.event_kind.clone())
        .collect();
    assert_eq!(kinds, ["effect.prepared", "effect.settled"]);

    // The already-dispatching intent follows §5.3: its delivery parks it
    // at reconciling, terminal unset.
    let delivery = ctx.cancel_deliver_stopped(&request_id, &d_intent.to_string(), 500);
    completed(ctx.funnel.submit(&delivery));
    let d_row = ctx.funnel.store().effect_record(&d_key).expect("d");
    assert_eq!(
        (d_row.state.as_str(), d_row.terminal.as_deref()),
        ("reconciling", None)
    );
    let d_events: Vec<_> = ctx
        .funnel
        .store()
        .journal()
        .unwrap()
        .into_iter()
        .filter(|event| event.aggregate_kind == "effect" && event.aggregate_id == d_key)
        .map(|event| (event.event_kind, event.aggregate_version, event.ordinal))
        .collect();
    assert_eq!(
        d_events,
        [
            ("effect.prepared".into(), 1, 0),
            ("effect.dispatching".into(), 2, 0),
            ("effect.reconciling".into(), 3, 1),
        ]
    );

    let dir = ctx.dir;
    drop(ctx.funnel);
    let store = Store::recover(dir.path(), "kernel-b").expect("recover internal receipts");
    let funnel = Funnel::new(store, 1_000).unwrap();
    assert_eq!(
        funnel
            .store()
            .effect_record(&p_key)
            .expect("prepared branch")
            .terminal
            .as_deref(),
        Some("cancelled")
    );
    assert_eq!(
        funnel
            .store()
            .effect_record(&d_key)
            .expect("dispatching branch")
            .state,
        "reconciling"
    );
}

fn request_id(result: &serde_json::Value) -> String {
    result["cancel_request_id"]
        .as_str()
        .expect("request id")
        .to_owned()
}

#[test]
fn obligation_4_run_cannot_close_success_until_members_discharge() {
    // Obligation 4 in one loop: cancel a RUN with an in-flight effect. The
    // effect parks reconciling, the run cannot close success while any
    // member lacks a discharged row OR while the parked effect remains, and
    // the request settles only when every member is discharged.
    let mut ctx = Ctx::new();
    ctx.seed_unit();
    let dispatch = ctx.dispatch_cmd("u-1", "h1", 1);
    let result = completed(ctx.funnel.submit(&dispatch));
    let attempt_id = result["attempt_id"]
        .as_str()
        .expect("attempt id")
        .to_owned();
    let token = token_from_result(&result).expect("token");
    let (key, intent, _) = prepare(
        &mut ctx,
        "prepare",
        "u-1",
        &token,
        effect_spec(Uuid7::mint(7, 30), "req"),
    );
    // v3 = prepared(1) -> dispatching(2) -> dispatched(3).
    let v = ctx.dispatch_and_record("d", "u-1", &token, &key, 1, 100);

    let req = ctx.cancel_request_cmd(
        "cancel run",
        CancelKind::Run,
        "run-1",
        CancelReason::BudgetExhausted,
        CancelPolicy::AttachedCascade,
        vec![
            member_input(CancelKind::Attempt, &attempt_id),
            member_input(CancelKind::EffectIntent, &intent.to_string()),
        ],
    );
    let request_id = request_id(&completed(ctx.funnel.submit(&req)));

    // I11 + the cancel arm: the run stays open.
    let close = ctx.close_run_cmd("run-1", 1, RunOutcome::ClosedSuccess);
    match ctx.funnel.submit(&close) {
        Submission::Rejected { kind, detail, .. } => {
            assert_eq!(kind, ErrorKind::InvalidRequest);
            assert!(detail.contains("cancel_request"), "got: {detail}");
        }
        other => panic!("expected Rejected, got {other:?}"),
    }

    // Deliver the effect (leaf, order 0) — parks reconciling; the attempt
    // member is still undelivered — the run still cannot close.
    let delivery = ctx.cancel_deliver_stopped(&request_id, &intent.to_string(), 600);
    completed(ctx.funnel.submit(&delivery));
    assert_eq!(
        ctx.funnel.store().effect_record(&key).expect("row").state,
        "reconciling"
    );
    let close2 = ctx.close_run_cmd("run-1", 1, RunOutcome::ClosedSuccess);
    match ctx.funnel.submit(&close2) {
        Submission::Rejected { detail, .. } => {
            assert!(detail.contains("cancel_request"), "got: {detail}");
        }
        other => panic!("expected Rejected, got {other:?}"),
    }

    // Deliver the attempt — the request settles.
    let parent = ctx.cancel_deliver_stopped(&request_id, &attempt_id, 610);
    assert_eq!(completed(ctx.funnel.submit(&parent))["status"], "settled");
    assert_eq!(
        ctx.funnel
            .store()
            .cancel_request_row(&request_id)
            .expect("row")
            .status,
        "settled"
    );

    // Still no success close: the reconciling effect blocks I11 on its own.
    let close3 = ctx.close_run_cmd("run-1", 1, RunOutcome::ClosedSuccess);
    match ctx.funnel.submit(&close3) {
        Submission::Rejected { detail, .. } => {
            assert!(
                detail.contains("reconciling") || detail.contains("effect"),
                "got: {detail}"
            );
        }
        other => panic!("expected Rejected, got {other:?}"),
    }

    // The effect reveals its outcome (target-reported cancellation) and the
    // owner settles it; ONLY THEN does the run close success.
    let settle = ctx.settle_cmd(
        "settle cancelled",
        "u-1",
        &key,
        v + 1,
        EffectTerminal::Cancelled,
        vec![],
        SettleEvidence {
            clean_wait_status: false,
            target_reported_cancellation: true,
        },
        700,
    );
    completed(ctx.funnel.submit(&settle));
    let close4 = ctx.close_run_cmd("run-1", 1, RunOutcome::ClosedSuccess);
    completed(ctx.funnel.submit(&close4));
}

#[test]
fn own_tree_rejects_a_foreign_run_member_persistently() {
    // finding-2 walk: resolution to an id is NOT membership. A scope rooted
    // run-1 that names a unit living under run-2 freezes (kind-depth is
    // right) but fails the own-tree walk — a persisted InvalidRequest,
    // because the unit's run is immutable so the rejection can never heal.
    let mut ctx = Ctx::new();
    ctx.seed_unit(); // run-1, wi-1, u-1
    let run2 = ctx.open_run("run-2");
    completed(ctx.funnel.submit(&run2));
    let wi2 = ctx.create_work_item("wi-2");
    completed(ctx.funnel.submit(&wi2));
    let admit2 = ctx.admit_into("u-2", "wi-2", "run-2");
    completed(ctx.funnel.submit(&admit2));

    let req = ctx.cancel_request_cmd(
        "cancel run-1 with a run-2 member",
        CancelKind::Run,
        "run-1",
        CancelReason::OwnerRequest,
        CancelPolicy::AttachedCascade,
        vec![member_input(CancelKind::ExecutionUnit, "u-2")],
    );
    assert_eq!(
        rejected(ctx.funnel.submit(&req)),
        (ErrorKind::InvalidRequest, false)
    );
    assert_eq!(
        rejected(ctx.funnel.submit(&req)),
        (ErrorKind::InvalidRequest, true),
        "own-tree mismatch is permanent — the persistent receipt replays it"
    );
    // Nothing durable was written by the rejected request.
    assert!(ctx.funnel.store().cancel_delivery_rows().is_empty());
}

#[test]
fn attempt_and_intent_rooted_cancels_gate_the_same_child() {
    // Non-Run roots: a cancel rooted at the current attempt bars a NEW
    // intent under it (rule 4 still fires through the lineage); a cancel
    // rooted AT the effect intent bars the intent's own dispatch — the
    // effect-intent-rooted half of the A14 race (fix 2 regression).
    let mut ctx = Ctx::new();
    ctx.seed_unit();
    let dp2 = ctx.dispatch_cmd("u-1", "h1", 1);
    let result = completed(ctx.funnel.submit(&dp2));
    let attempt_id = result["attempt_id"]
        .as_str()
        .expect("attempt id")
        .to_owned();
    let token = token_from_result(&result).expect("token");
    let (p_key, p_intent, _) = prepare(
        &mut ctx,
        "prepared",
        "u-1",
        &token,
        effect_spec(Uuid7::mint(7, 40), "req"),
    );

    // Attempt-rooted request named its own intent as the scope member.
    let req_a = ctx.cancel_request_cmd(
        "cancel attempt",
        CancelKind::Attempt,
        &attempt_id,
        CancelReason::OwnerRequest,
        CancelPolicy::RootOnly,
        vec![member_input(
            CancelKind::EffectIntent,
            &p_intent.to_string(),
        )],
    );
    completed(ctx.funnel.submit(&req_a));

    // A NEW intent under that attempt is barred by the rule-4 lineage.
    let prep2 = ctx.prepare_cmd(
        "prepare barred under attempt",
        "u-1",
        token.clone(),
        effect_spec(Uuid7::mint(7, 41), "req other"),
    );
    match ctx.funnel.submit(&prep2) {
        Submission::Rejected { kind, detail, .. } => {
            assert_eq!(kind, ErrorKind::InvalidRequest);
            assert!(detail.contains("rule-4"), "got: {detail}");
        }
        other => panic!("expected Rejected, got {other:?}"),
    }

    // Effect-intent-rooted request against the SAME prepared intent; its
    // dispatch must settle cancelled instead of authorizing (A14). The
    // intent is a leaf, so the only lawful scope member is the root itself.
    let req2 = ctx.cancel_request_cmd(
        "cancel intent",
        CancelKind::EffectIntent,
        &p_intent.to_string(),
        CancelReason::OwnerRequest,
        CancelPolicy::RootOnly,
        vec![member_input(
            CancelKind::EffectIntent,
            &p_intent.to_string(),
        )],
    );
    completed(ctx.funnel.submit(&req2));

    let dp = ctx.dispatch_effect_cmd("dispatch after intent cancel", "u-1", token, &p_key, 1);
    let result = completed(ctx.funnel.submit(&dp));
    assert_eq!(result["state"], "settled");
    let row = ctx.funnel.store().effect_record(&p_key).expect("p2");
    assert_eq!(row.terminal.as_deref(), Some("cancelled"));
}

#[test]
fn oversize_scope_is_refused_whole_and_persists() {
    // §5.1 MAX_SCOPE_MEMBERS: truncating the scope would strand members —
    // the freeze refuses whole, and the shape error persists.
    let mut ctx = Ctx::new();
    ctx.seed_unit();
    let dp2 = ctx.dispatch_cmd("u-1", "h1", 1);
    completed(ctx.funnel.submit(&dp2));

    let many: Vec<_> = (0..=MAX_SCOPE_MEMBERS as u64)
        .map(|n| member_input(CancelKind::EffectIntent, &format!("fake-{n}")))
        .collect();
    let req = ctx.cancel_request_cmd(
        "oversize scope",
        CancelKind::Attempt,
        "does-not-exist",
        CancelReason::OwnerRequest,
        CancelPolicy::AttachedCascade,
        many,
    );
    assert_eq!(
        rejected(ctx.funnel.submit(&req)),
        (ErrorKind::InvalidRequest, false)
    );
    assert_eq!(
        rejected(ctx.funnel.submit(&req)),
        (ErrorKind::InvalidRequest, true),
        "TooLarge is shape-deterministic — the persistent receipt replays it"
    );
}

#[test]
fn a_cancelled_run_does_not_bar_a_sibling_run() {
    // Negative isolation: a cancel rooted run-1 bars admission into run-1
    // only; admitting into run-2 remains lawful (rule-4 lineage is the root
    // that cancel owns, not the whole world).
    let mut ctx = Ctx::new();
    ctx.seed_unit();
    let dp2 = ctx.dispatch_cmd("u-1", "h1", 1);
    completed(ctx.funnel.submit(&dp2));

    let req = ctx.cancel_request_cmd(
        "cancel run-1",
        CancelKind::Run,
        "run-1",
        CancelReason::OwnerRequest,
        CancelPolicy::RootOnly,
        vec![member_input(CancelKind::ExecutionUnit, "u-1")],
    );
    completed(ctx.funnel.submit(&req));

    let run2 = ctx.open_run("run-2");
    completed(ctx.funnel.submit(&run2));
    let wi2 = ctx.create_work_item("wi-2");
    completed(ctx.funnel.submit(&wi2));
    let admit2 = ctx.admit_into("u-2", "wi-2", "run-2");
    completed(ctx.funnel.submit(&admit2));
    // And a dispatch inside the sibling run is lawful too.
    let dispatch = ctx.dispatch_cmd("u-2", "h2", 1);
    completed(ctx.funnel.submit(&dispatch));
}

#[test]
fn two_requests_on_the_same_root_settle_independently() {
    // Two cancel.request commands for the same root are two distinct
    // frozen requests, both durable, each settling on its own delivery —
    // the frozen snapshot (rule 2) is per-request, not a singleton.
    let mut ctx = Ctx::new();
    ctx.seed_unit();
    let dp2 = ctx.dispatch_cmd("u-1", "h1", 1);
    completed(ctx.funnel.submit(&dp2));

    let scope = vec![member_input(CancelKind::ExecutionUnit, "u-1")];
    let req1 = ctx.cancel_request_cmd(
        "first request",
        CancelKind::Run,
        "run-1",
        CancelReason::OwnerRequest,
        CancelPolicy::AttachedCascade,
        scope.clone(),
    );
    let id1 = request_id(&completed(ctx.funnel.submit(&req1)));
    let req2 = ctx.cancel_request_cmd(
        "second request",
        CancelKind::Run,
        "run-1",
        CancelReason::OwnerRequest,
        CancelPolicy::AttachedCascade,
        scope.clone(),
    );
    let id2 = request_id(&completed(ctx.funnel.submit(&req2)));
    assert_ne!(id1, id2);

    // Both durable, both requested.
    assert_eq!(
        ctx.funnel
            .store()
            .cancel_request_row(&id1)
            .expect("r1")
            .status,
        "requested"
    );
    assert_eq!(
        ctx.funnel
            .store()
            .cancel_request_row(&id2)
            .expect("r2")
            .status,
        "requested"
    );

    // Settle request 1 only: request 2 must stay requested.
    let d1 = ctx.cancel_deliver_stopped(&id1, "u-1", 700);
    completed(ctx.funnel.submit(&d1));
    assert_eq!(
        ctx.funnel
            .store()
            .cancel_request_row(&id1)
            .expect("r1")
            .status,
        "settled"
    );
    assert_eq!(
        ctx.funnel
            .store()
            .cancel_request_row(&id2)
            .expect("r2")
            .status,
        "requested"
    );

    // Settle request 2 independently.
    let d2 = ctx.cancel_deliver_stopped(&id2, "u-1", 800);
    completed(ctx.funnel.submit(&d2));
    assert_eq!(
        ctx.funnel
            .store()
            .cancel_request_row(&id2)
            .expect("r2")
            .status,
        "settled"
    );
}
