//! §4.3 target classification: the declared-enumeration merge and the
//! post-hoc observation rules, plus the §4.1 list constraints. These are
//! the tests that pin "the ledger records observations, never the caller's
//! conclusions" — the §5-independent half of row A5.

mod common;

use common::*;
use yah_kernel::effect::{EffectTerminal, TargetState};
use yah_kernel::error::ErrorKind;
use yah_kernel::funnel::SettleEvidence;
use yah_kernel::ids::Uuid7;

fn prepare(
    ctx: &mut Ctx,
    digest_src: &str,
    token: &yah_kernel::store::AttemptTokenClaims,
    spec: yah_kernel::funnel::EffectSpec,
) -> String {
    let cmd = ctx.prepare_cmd(digest_src, "u-1", token.clone(), spec);
    let result = completed(ctx.funnel.submit(&cmd));
    result["operation_key"].as_str().expect("key").to_owned()
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
fn declared_targets_are_classified_by_digest_never_by_assertion() {
    // The §5-independent half of A5: 3-of-5 applied settles failed, never
    // succeeded — by omission, by assertion, or by smuggling.
    let mut ctx = Ctx::new();
    ctx.seed_unit();
    let token = ctx.dispatch("u-1", "h1", 1);

    let ids = ["f1", "f2", "f3", "f4", "f5"];
    let key = prepare(
        &mut ctx,
        "prepare",
        &token,
        declared_spec(Uuid7::mint(7, 3), "req", &ids),
    );
    let v = ctx.dispatch_and_record("d", "u-1", &token, &key, 1, 100);

    // f1-f3 landed (observed == expected), f4-f5 untouched (== pre).
    let observed: Vec<_> = ids
        .iter()
        .enumerate()
        .map(|(i, id)| target_observed(id, if i < 3 { "want" } else { "pre" }))
        .collect();

    for (label, terminal, rows) in [
        (
            "honest partial as succeeded",
            EffectTerminal::Succeeded,
            observed.clone(),
        ),
        (
            "omit the unapplied rows",
            EffectTerminal::Succeeded,
            observed[..3].to_vec(),
        ),
        ("empty row set", EffectTerminal::Succeeded, vec![]),
    ] {
        let c = ctx.settle_clean(label, "u-1", &key, v, terminal, rows, 200);
        assert_eq!(
            rejected(ctx.funnel.submit(&c)),
            (ErrorKind::InvalidRequest, false),
            "{label} must not settle succeeded"
        );
    }

    // A row asserting Applied with a wrong digest is reclassified.
    let mut lying = observed.clone();
    lying[3].observed_digest = Some(yah_kernel::ids::Digest::of_bytes(b"garbled"));
    lying[3].state = TargetState::Applied; // ignored
    let lie = ctx.settle_clean("lie", "u-1", &key, v, EffectTerminal::Succeeded, lying, 260);
    assert_eq!(
        rejected(ctx.funnel.submit(&lie)),
        (ErrorKind::InvalidRequest, false)
    );

    // Undeclared rows are a shape violation.
    let mut smuggled = observed.clone();
    smuggled.push(target_observed("zz", "want"));
    let smuggle = ctx.settle_clean(
        "smuggle",
        "u-1",
        &key,
        v,
        EffectTerminal::Failed,
        smuggled,
        270,
    );
    assert_eq!(
        rejected(ctx.funnel.submit(&smuggle)),
        (ErrorKind::InvalidRequest, false)
    );

    // The lawful settlement: failed, carrying the partial record.
    let ok = ctx.settle_clean(
        "failed",
        "u-1",
        &key,
        v,
        EffectTerminal::Failed,
        observed,
        300,
    );
    completed(ctx.funnel.submit(&ok));
    let record = record_json(&ctx, &key);
    let states: Vec<&str> = record["targets"]
        .as_array()
        .expect("targets")
        .iter()
        .map(|t| t["state"].as_str().expect("state"))
        .collect();
    assert_eq!(
        states,
        [
            "applied",
            "applied",
            "applied",
            "not_applied",
            "not_applied"
        ]
    );
}

#[test]
fn post_hoc_targets_need_observation_and_a_clean_wait_status() {
    // §4.3 post-hoc: `applied` only when the daemon observed the write AND
    // the process reported a clean wait status; the caller's asserted state
    // is never consulted.
    let mut ctx = Ctx::new();
    ctx.seed_unit();
    let token = ctx.dispatch("u-1", "h1", 1);

    // Rows with no observed digest cannot be applied, whatever they claim.
    let k1 = prepare(
        &mut ctx,
        "p1",
        &token,
        effect_spec(Uuid7::mint(7, 50), "req"),
    );
    let v1 = ctx.dispatch_and_record("d1", "u-1", &token, &k1, 1, 100);
    let mut unevidenced = target_observed("a", "x");
    unevidenced.observed_digest = None;
    unevidenced.state = TargetState::Applied; // ignored
    let c = ctx.settle_clean(
        "unevidenced",
        "u-1",
        &k1,
        v1,
        EffectTerminal::Succeeded,
        vec![unevidenced],
        200,
    );
    assert_eq!(
        rejected(ctx.funnel.submit(&c)),
        (ErrorKind::InvalidRequest, false)
    );

    // A dirty wait status classifies every row unknown, so succeeded and
    // failed are both refused — unknown is the honest terminal.
    let dirty = SettleEvidence {
        clean_wait_status: false,
        target_reported_cancellation: false,
    };
    for terminal in [EffectTerminal::Succeeded, EffectTerminal::Failed] {
        let c = ctx.settle_cmd(
            &format!("dirty {terminal:?}"),
            "u-1",
            &k1,
            v1,
            terminal,
            vec![target_observed("a", "x")],
            dirty,
            300,
        );
        assert_eq!(
            rejected(ctx.funnel.submit(&c)),
            (ErrorKind::InvalidRequest, false)
        );
    }
    let unknown = ctx.settle_cmd(
        "dirty unknown",
        "u-1",
        &k1,
        v1,
        EffectTerminal::Unknown,
        vec![target_observed("a", "x")],
        dirty,
        400,
    );
    completed(ctx.funnel.submit(&unknown));
    assert_eq!(record_json(&ctx, &k1)["targets"][0]["state"], "unknown");

    // Clean status plus observation is the applied path.
    let k2 = prepare(
        &mut ctx,
        "p2",
        &token,
        effect_spec(Uuid7::mint(7, 51), "req"),
    );
    let v2 = ctx.dispatch_and_record("d2", "u-1", &token, &k2, 1, 500);
    let ok = ctx.settle_clean(
        "clean",
        "u-1",
        &k2,
        v2,
        EffectTerminal::Succeeded,
        vec![target_observed("a", "x")],
        600,
    );
    completed(ctx.funnel.submit(&ok));
    assert_eq!(record_json(&ctx, &k2)["targets"][0]["state"], "applied");
}

#[test]
fn target_lists_are_bounded_and_unique() {
    let mut ctx = Ctx::new();
    ctx.seed_unit();
    let token = ctx.dispatch("u-1", "h1", 1);
    let key = prepare(
        &mut ctx,
        "prepare",
        &token,
        effect_spec(Uuid7::mint(7, 60), "req"),
    );
    let v = ctx.dispatch_and_record("d", "u-1", &token, &key, 1, 100);

    let dup = vec![target_observed("a", "x"), target_observed("a", "y")];
    let c = ctx.settle_clean("dup", "u-1", &key, v, EffectTerminal::Unknown, dup, 200);
    assert_eq!(
        rejected(ctx.funnel.submit(&c)),
        (ErrorKind::InvalidRequest, false)
    );

    let long_id = "z".repeat(300);
    let c2 = ctx.settle_clean(
        "long",
        "u-1",
        &key,
        v,
        EffectTerminal::Unknown,
        vec![target_observed(&long_id, "x")],
        300,
    );
    assert_eq!(
        rejected(ctx.funnel.submit(&c2)),
        (ErrorKind::InvalidRequest, false)
    );

    let many: Vec<_> = (0..1025)
        .map(|n| target_observed(&format!("t{n}"), "x"))
        .collect();
    let c3 = ctx.settle_clean("many", "u-1", &key, v, EffectTerminal::Unknown, many, 400);
    assert_eq!(
        rejected(ctx.funnel.submit(&c3)),
        (ErrorKind::InvalidRequest, false)
    );
}
