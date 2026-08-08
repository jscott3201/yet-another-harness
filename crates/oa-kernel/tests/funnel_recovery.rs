mod common;

use common::*;
use oa_kernel::cancel::{CancelKind, CancelPolicy, CancelReason, DeliveryOutcome};
use oa_kernel::effect::EffectTerminal;
use oa_kernel::error::ErrorKind;
use oa_kernel::funnel::RunOutcome;
use oa_kernel::funnel::{Funnel, PrincipalKind};
use oa_kernel::ids::Uuid7;
use oa_kernel::store::Store;

fn prepare(
    ctx: &mut Ctx,
    token: &oa_kernel::store::AttemptTokenClaims,
    logical_op: Uuid7,
) -> (String, Uuid7) {
    let command = ctx.prepare_cmd(
        "prepare",
        "u-1",
        token.clone(),
        effect_spec(logical_op, "req"),
    );
    let result = completed(ctx.funnel.submit(&command));
    (
        result["operation_key"].as_str().unwrap().to_owned(),
        Uuid7::try_from(result["effect_intent_id"].as_str().unwrap().to_owned()).unwrap(),
    )
}

#[test]
fn redispatch_history_recovers_after_a_later_dispatched_at() {
    let mut ctx = Ctx::new();
    ctx.seed_unit();
    let t1 = ctx.dispatch("u-1", "h1", 1);
    let logical_op = Uuid7::mint(7, 1);
    let (key, _) = prepare(&mut ctx, &t1, logical_op);
    ctx.dispatch_and_record("first", "u-1", &t1, &key, 1, 100);

    let dir = ctx.dir;
    drop(ctx.funnel);
    let store = Store::recover(dir.path(), "kernel-b").unwrap();
    let mut ctx = Ctx::resume(dir, Funnel::new(store, 1_000).unwrap(), 100);
    let t2 = ctx.dispatch("u-1", "h2", 2);
    let command = ctx.prepare_cmd(
        "prepare again",
        "u-1",
        t2.clone(),
        effect_spec(logical_op, "req"),
    );
    assert!(
        completed(ctx.funnel.submit(&command))["existing"]
            .as_bool()
            .unwrap()
    );
    ctx.dispatch_and_record("retry", "u-1", &t2, &key, 3, 150);
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

    let dir = ctx.dir;
    drop(ctx.funnel);
    Store::recover(dir.path(), "kernel-c").unwrap();
}

#[test]
fn deterministic_wrong_principal_receipts_recover() {
    let mut ctx = Ctx::new();
    ctx.seed_unit();
    let token = ctx.dispatch("u-1", "h1", 1);
    let (key, _) = prepare(&mut ctx, &token, Uuid7::mint(7, 2));
    ctx.dispatch_and_record("dispatch", "u-1", &token, &key, 1, 100);

    let mut authority_as_agent = ctx.settle_clean(
        "settle as agent",
        "u-1",
        &key,
        3,
        EffectTerminal::Unknown,
        vec![],
        200,
    );
    authority_as_agent.principal_kind = PrincipalKind::Agent;
    rejected(ctx.funnel.submit(&authority_as_agent));

    let mut holder_as_daemon = ctx.prepare_cmd(
        "prepare as daemon",
        "u-1",
        token,
        effect_spec(Uuid7::mint(7, 3), "req-2"),
    );
    holder_as_daemon.principal_kind = PrincipalKind::Daemon;
    rejected(ctx.funnel.submit(&holder_as_daemon));

    let dir = ctx.dir;
    drop(ctx.funnel);
    Store::recover(dir.path(), "kernel-b").unwrap();
}

#[test]
fn delivery_observation_must_match_its_outcome() {
    let mut ctx = Ctx::new();
    ctx.seed_unit();
    let dispatch = ctx.dispatch_cmd("u-1", "h1", 1);
    let attempt_id = completed(ctx.funnel.submit(&dispatch))["attempt_id"]
        .as_str()
        .unwrap()
        .to_owned();
    let request = ctx.cancel_request_cmd(
        "cancel attempt",
        CancelKind::Attempt,
        &attempt_id,
        CancelReason::OwnerRequest,
        CancelPolicy::RootOnly,
        vec![member_input(CancelKind::Attempt, &attempt_id)],
    );
    let request_id = completed(ctx.funnel.submit(&request))["cancel_request_id"]
        .as_str()
        .unwrap()
        .to_owned();
    for (observed_at, outcome) in [
        (None, DeliveryOutcome::ObservedStopped),
        (Some(10), DeliveryOutcome::Unresponsive),
        (Some(9), DeliveryOutcome::ObservedStopped),
    ] {
        let delivery = ctx.cancel_delivery_cmd(
            "invalid observation",
            &request_id,
            &attempt_id,
            10,
            observed_at,
            outcome,
        );
        assert_eq!(
            rejected(ctx.funnel.submit(&delivery)),
            (ErrorKind::InvalidRequest, false)
        );
    }
    assert!(ctx.funnel.store().cancel_delivery_rows().is_empty());
}

#[test]
fn empty_cancellation_cannot_reserve_a_future_or_live_root() {
    let mut ctx = Ctx::new();
    let cancel = ctx.cancel_request_cmd(
        "empty future cancel",
        CancelKind::Run,
        "future-run",
        CancelReason::OwnerRequest,
        CancelPolicy::RootOnly,
        vec![],
    );
    assert_eq!(
        rejected(ctx.funnel.submit(&cancel)),
        (ErrorKind::NotFound, false)
    );

    let open = ctx.open_run("future-run");
    completed(ctx.funnel.submit(&open));
    assert_eq!(
        rejected(ctx.funnel.submit(&cancel)),
        (ErrorKind::InvalidRequest, false)
    );

    let work_item = ctx.create_work_item("future-work");
    completed(ctx.funnel.submit(&work_item));
    let admit = ctx.admit_into("future-unit", "future-work", "future-run");
    completed(ctx.funnel.submit(&admit));
}

#[test]
fn empty_cancellation_of_a_terminal_root_settles_immediately_and_recovers() {
    let mut ctx = Ctx::new();
    let open = ctx.open_run("closed-run");
    completed(ctx.funnel.submit(&open));
    let close = ctx.close_run_cmd("closed-run", 1, RunOutcome::ClosedFailure);
    completed(ctx.funnel.submit(&close));
    let cascade = ctx.cancel_request_cmd(
        "empty terminal cascade",
        CancelKind::Run,
        "closed-run",
        CancelReason::OwnerRequest,
        CancelPolicy::AttachedCascade,
        vec![],
    );
    assert_eq!(
        rejected(ctx.funnel.submit(&cascade)),
        (ErrorKind::InvalidRequest, false)
    );
    let cancel = ctx.cancel_request_cmd(
        "empty terminal cancel",
        CancelKind::Run,
        "closed-run",
        CancelReason::OwnerRequest,
        CancelPolicy::RootOnly,
        vec![],
    );
    let result = completed(ctx.funnel.submit(&cancel));
    assert_eq!(result["status"], "settled");
    let request_id = result["cancel_request_id"].as_str().unwrap();
    assert_eq!(
        ctx.funnel
            .store()
            .cancel_request_row(request_id)
            .unwrap()
            .status,
        "settled"
    );

    let dir = ctx.dir;
    drop(ctx.funnel);
    let store = Store::recover(dir.path(), "kernel-b").unwrap();
    Funnel::new(store, 1_000).unwrap();
}

#[test]
fn attempt_cancellation_does_not_bar_a_successor_attempt() {
    let mut ctx = Ctx::new();
    ctx.seed_unit();
    let dispatch = ctx.dispatch_cmd("u-1", "h1", 1);
    let attempt_id = completed(ctx.funnel.submit(&dispatch))["attempt_id"]
        .as_str()
        .unwrap()
        .to_owned();
    let request = ctx.cancel_request_cmd(
        "cancel first attempt",
        CancelKind::Attempt,
        &attempt_id,
        CancelReason::OwnerRequest,
        CancelPolicy::RootOnly,
        vec![member_input(CancelKind::Attempt, &attempt_id)],
    );
    completed(ctx.funnel.submit(&request));

    let successor = ctx.dispatch_cmd("u-1", "h2", 2);
    assert_eq!(completed(ctx.funnel.submit(&successor))["attempt_epoch"], 2);
}

#[test]
fn predecessor_cancellation_bars_its_effect_under_a_successor_token() {
    let mut ctx = Ctx::new();
    ctx.seed_unit();
    let dispatch = ctx.dispatch_cmd("u-1", "h1", 1);
    let first = completed(ctx.funnel.submit(&dispatch));
    let first_token = oa_kernel::funnel::token_from_result(&first).unwrap();
    let first_attempt = first["attempt_id"].as_str().unwrap();
    let mut spec = effect_spec(Uuid7::mint(7, 4), "req");
    spec.reversibility_class = oa_kernel::effect::ReversibilityClass::Irreversible;
    let prepare = ctx.prepare_cmd("prepare", "u-1", first_token, spec);
    let prepared = completed(ctx.funnel.submit(&prepare));
    let key = prepared["operation_key"].as_str().unwrap().to_owned();
    let intent_id = prepared["effect_intent_id"].as_str().unwrap().to_owned();
    let request = ctx.cancel_request_cmd(
        "cancel first attempt effect",
        CancelKind::Attempt,
        first_attempt,
        CancelReason::OwnerRequest,
        CancelPolicy::AttachedCascade,
        vec![member_input(CancelKind::EffectIntent, &intent_id)],
    );
    completed(ctx.funnel.submit(&request));

    let successor = ctx.dispatch("u-1", "h2", 2);
    let dispatch_effect = ctx.dispatch_effect_cmd("dispatch old effect", "u-1", successor, &key, 1);
    let result = completed(ctx.funnel.submit(&dispatch_effect));
    assert_eq!(result["state"], "settled");
    let stored = ctx.funnel.store().effect_record(&key).unwrap();
    assert_eq!(stored.terminal.as_deref(), Some("cancelled"));

    let dir = ctx.dir;
    drop(ctx.funnel);
    Store::recover(dir.path(), "kernel-b").unwrap();
}
