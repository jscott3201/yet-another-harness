//! §4 effect-ledger method arms. Authorization classes follow the §3.3
//! boundary table: `effect.prepare` and `effect.dispatch` /
//! `effect.record_dispatched` are HOLDER class (row 5), while
//! `effect.settle` and `effect.park_reconciling` are AUTHORITY class
//! (row 6) — the latter MUST NOT require a matching attempt_epoch, stamp,
//! or active lease, or the recovery scan could never settle a superseded
//! attempt's dispatched effects and a stamp bump would manufacture
//! `unknown` out of a known outcome.
//!
//! `prepared -> dispatching` is a SEPARATE committed transition from
//! recording the dispatch (§3.3 row 5): the adapter MUST NOT perform the
//! operation unless that transition committed, which is what makes the R20
//! approval predicate a gate rather than a post-hoc complaint. Collapsing
//! the two would let an unapproved irreversible action escape and leave the
//! ledger merely refusing to record it.
//!
//! Legality and target classification live in [`super::effect_rules`]; this
//! file is the transaction shape and the authorization order.

use super::effect_rules::{
    check_target_list, classify_settle_targets, declared_at_prepare, settle_legality, state_wire,
    terminal_wire,
};
use super::*;
use crate::effect::{
    EffectError, EffectState, EffectTerminal, RetryClass, ReversibilityClass, TargetEnumeration,
    TargetObservation,
};

/// Caller-declared §4.1 fields of an intent — everything the funnel does not
/// derive (the key), mint (the intent id), or stamp from the fence (the
/// epoch axes).
#[derive(Clone, Debug)]
pub struct EffectSpec {
    pub logical_operation_id: Uuid7,
    pub request_digest: Digest,
    pub adapter_id: String,
    pub adapter_version: String,
    pub retry_class: RetryClass,
    pub reversibility_class: ReversibilityClass,
    /// §4.3 adapter declaration. `Declared` REQUIRES a non-empty
    /// `declared_targets`; `PostHoc` requires it to be empty.
    pub target_enumeration: TargetEnumeration,
    pub approval_ref: Option<Uuid7>,
    pub policy_snapshot_digest: Digest,
    pub declared_targets: Vec<TargetObservation>,
    pub decomposable: bool,
    pub compensation_intent_id: Option<Uuid7>,
}

fn require_effect<'p>(pre: &'p PreRead, operation_key: &str) -> Result<&'p EffectRow, Rejection> {
    pre.effect.as_ref().ok_or_else(|| {
        // Not persisted: the intent may be prepared later.
        (
            ErrorKind::NotFound,
            format!("effect {operation_key}"),
            false,
        )
    })
}

fn parse_record(row: &EffectRow) -> Result<EffectIntent, Rejection> {
    serde_json::from_str(&row.record).map_err(|e| {
        (
            ErrorKind::Internal,
            format!("effect record unreadable: {e}"),
            false,
        )
    })
}

/// The intent named by the key must belong to the unit the method names.
/// This is a malformed (unit_id, operation_key) PAIR, not a fence failure —
/// the caller's own authorization may be perfectly current — so it is
/// `invalid_request`, and it is checked before any other property of the
/// foreign row can leak into the rejection detail.
fn intent_binds(intent: &EffectIntent, unit_id: &str) -> Result<(), Rejection> {
    if intent.unit_id != unit_id {
        return Err((
            ErrorKind::InvalidRequest,
            format!(
                "effect belongs to unit {}, method targets {}",
                intent.unit_id, unit_id
            ),
            true,
        ));
    }
    Ok(())
}

/// I2 on the effect's own version axis (the unit does not advance).
fn expected_effect_version(cmd: &Command, row: &EffectRow) -> Result<(), Rejection> {
    match cmd.expected_version {
        Some(expected) if expected != row.version => Err((
            ErrorKind::VersionConflict,
            format!("expected effect version {expected}, actual {}", row.version),
            false,
        )),
        Some(_) => Ok(()),
        None => Err((
            ErrorKind::InvalidRequest,
            "effect mutation requires expected_version".into(),
            true,
        )),
    }
}

fn already_settled(intent: &EffectIntent, operation_key: &str) -> Result<(), Rejection> {
    if intent.terminal.is_some() {
        return Err((
            ErrorKind::InvalidRequest,
            format!("effect {operation_key} already settled"),
            true,
        ));
    }
    Ok(())
}

fn next_version(version: u64) -> Result<u64, Rejection> {
    version.checked_add(1).ok_or_else(|| {
        (
            ErrorKind::ResourceExhausted,
            "effect version space exhausted".into(),
            true,
        )
    })
}

fn update_accepted(
    row: &EffectRow,
    intent: EffectIntent,
    terminal: Option<&'static str>,
    event_kind: &'static str,
    payload: serde_json::Value,
) -> Result<Accepted, Rejection> {
    let state = state_wire(intent.state);
    let record = serde_json::to_string(&intent).expect("intent serializes");
    Ok(Accepted {
        plan: Plan::UpdateEffect {
            effect_node: row.node,
            new_version: intent.version,
            state,
            terminal,
            record,
        },
        events: vec![EventDraft {
            aggregate_kind: "effect",
            aggregate_id: intent.effect_intent_id.to_string(),
            aggregate_version: intent.version,
            event_kind,
            payload,
        }],
        result: json!({
            "operation_key": intent.operation_key,
            "version": intent.version,
            "state": state,
        }),
    })
}

/// Shared opening for every arm that mutates an existing intent: resolve,
/// parse, bind to the unit, then CAS. Binding precedes the version check so
/// a foreign intent's version and terminal state cannot leak through a
/// rejection detail.
fn resolve_bound<'p>(
    cmd: &Command,
    pre: &'p PreRead,
    unit_id: &str,
    operation_key: &str,
) -> Result<(&'p EffectRow, EffectIntent), Rejection> {
    let row = require_effect(pre, operation_key)?;
    let intent = parse_record(row)?;
    intent_binds(&intent, unit_id)?;
    expected_effect_version(cmd, row)?;
    Ok((row, intent))
}

impl Funnel {
    pub(super) fn validate_effect(
        &self,
        cmd: &Command,
        pre: &PreRead,
    ) -> Result<Accepted, Rejection> {
        match &cmd.method {
            Method::EffectPrepare { unit_id, spec } => {
                self.validate_prepare(cmd, pre, unit_id, spec)
            }
            Method::EffectDispatch {
                unit_id,
                operation_key,
            } => self.validate_dispatch(cmd, pre, unit_id, operation_key),
            Method::EffectRecordDispatched {
                unit_id,
                operation_key,
                dispatched_at,
            } => {
                self.holder_gate(cmd, pre, unit_id)?;
                let (row, mut intent) = resolve_bound(cmd, pre, unit_id, operation_key)?;
                already_settled(&intent, operation_key)?;
                if intent.state != EffectState::Dispatching {
                    return Err((
                        ErrorKind::InvalidRequest,
                        format!(
                            "record_dispatched needs state dispatching, found {:?}",
                            intent.state
                        ),
                        // State-dependent: a later effect.dispatch makes
                        // this lawful, so the retry must re-validate.
                        false,
                    ));
                }
                intent.state = EffectState::Dispatched;
                intent.dispatched_at = Some(*dispatched_at);
                intent.version = next_version(row.version)?;
                update_accepted(
                    row,
                    intent,
                    None,
                    "effect.dispatched",
                    json!({ "dispatched_at": dispatched_at }),
                )
            }
            Method::EffectSettle {
                unit_id,
                operation_key,
                terminal,
                targets,
                evidence,
                settled_at,
            } => {
                // §3.3 row 6: AUTHORITY class — no token, no attempt/stamp/
                // lease fence, or a superseded attempt's known outcome could
                // never be recorded.
                self.authority_gate(cmd)?;
                let (row, mut intent) = resolve_bound(cmd, pre, unit_id, operation_key)?;
                already_settled(&intent, operation_key)?;
                check_target_list(targets)?;
                if let Some(reason) = settle_legality(&intent, *terminal, *evidence) {
                    // State-dependent: a later dispatch (or a target query
                    // that yields the missing evidence) can make the same
                    // command lawful, so this must not persist.
                    return Err((ErrorKind::InvalidRequest, reason, false));
                }
                intent.targets = classify_settle_targets(&intent, targets, *evidence)?;
                intent.settle(*terminal).map_err(|e| {
                    let detail = match e {
                        EffectError::TerminalAlreadySet { existing } => {
                            format!("terminal already set to {existing:?}")
                        }
                        EffectError::SucceededWithUnappliedTarget { target_id } => {
                            format!("succeeded with unapplied target {target_id}")
                        }
                        EffectError::FailedWithUnknownTarget { target_id } => {
                            format!("failed would mask unknown target {target_id}")
                        }
                    };
                    // A classification violation is a property of the
                    // observations submitted with THIS command, so the
                    // byte-identical retry gets the same answer.
                    (ErrorKind::InvalidRequest, detail, true)
                })?;
                intent.state = EffectState::Settled;
                intent.settled_at = Some(*settled_at);
                intent.version = next_version(row.version)?;
                let wire = terminal_wire(*terminal);
                update_accepted(
                    row,
                    intent,
                    Some(wire),
                    "effect.settled",
                    json!({ "terminal": terminal, "targets": targets.len() }),
                )
            }
            Method::EffectParkReconciling {
                unit_id,
                operation_key,
                next_reconcile_at,
            } => {
                // §3.3 row 6: authority class, like settle.
                self.authority_gate(cmd)?;
                let (row, mut intent) = resolve_bound(cmd, pre, unit_id, operation_key)?;
                already_settled(&intent, operation_key)?;
                // Reconciling is post-dispatch parking (§4.1): an intent
                // that never reached dispatching has nothing to reconcile.
                if !matches!(
                    intent.state,
                    EffectState::Dispatching
                        | EffectState::Dispatched
                        | EffectState::Observing
                        | EffectState::Reconciling
                ) {
                    return Err((
                        ErrorKind::InvalidRequest,
                        format!("cannot park state {:?} at reconciling", intent.state),
                        // State-dependent, like the settle legality guard.
                        false,
                    ));
                }
                intent.state = EffectState::Reconciling;
                intent.next_reconcile_at = Some(*next_reconcile_at);
                intent.version = next_version(row.version)?;
                update_accepted(
                    row,
                    intent,
                    None,
                    "effect.reconciling",
                    json!({ "next_reconcile_at": next_reconcile_at }),
                )
            }
            _ => unreachable!("validate routes only effect methods here"),
        }
    }

    /// §2.1/§3.3 authority class: `principal_kind = daemon` under the
    /// CURRENT authority epoch, and no token. The principal half is not
    /// decoration — the epoch alone is a value every holder receives in its
    /// own dispatch result, so an epoch-only gate would let any holder
    /// write the authoritative outcome of its own effects.
    pub(super) fn authority_gate(&self, cmd: &Command) -> Result<(), Rejection> {
        if cmd.principal_kind != PrincipalKind::Daemon {
            return Err((
                ErrorKind::InvalidRequest,
                format!(
                    "authority method requires principal_kind daemon, got {:?}",
                    cmd.principal_kind
                ),
                true,
            ));
        }
        if cmd.attempt_token.is_some() {
            return Err((
                ErrorKind::InvalidRequest,
                "authority method must not carry an attempt token".into(),
                true,
            ));
        }
        let current = self.store.authority_epoch();
        match cmd.authority_epoch {
            Some(e) if e == current => Ok(()),
            Some(e) => Err(Self::epoch_mismatch(e, current)),
            None => Err((
                ErrorKind::InvalidRequest,
                "authority method requires authority_epoch".into(),
                true,
            )),
        }
    }

    /// §3.3 row 5's `prepared -> dispatching`: the transition that
    /// AUTHORIZES the adapter to act. Every predicate that must hold before
    /// the world is touched is enforced here — fence, approval (R20), and
    /// retry safety — because after this commits the operation may happen.
    fn validate_dispatch(
        &self,
        cmd: &Command,
        pre: &PreRead,
        unit_id: &str,
        operation_key: &str,
    ) -> Result<Accepted, Rejection> {
        self.holder_gate(cmd, pre, unit_id)?;
        let (row, mut intent) = resolve_bound(cmd, pre, unit_id, operation_key)?;
        already_settled(&intent, operation_key)?;
        // Re-dispatch of an intent that already reached the world is the
        // §4.1 retry shape. `no_retry` adapters offer neither a stable
        // idempotency key nor an authoritative result query, so automatic
        // retry for that class is rejected (§4.2's trailing paragraph).
        // NOTE: `query_then_retry` is NOT gated here — its query-before-
        // retry obligation belongs to the §9 reconcile path, which this
        // milestone has not built; this boundary only refuses the class the
        // spec forbids outright.
        if intent.state != EffectState::Prepared {
            if intent.retry_class == RetryClass::NoRetry {
                return Err((
                    ErrorKind::InvalidRequest,
                    format!("retry_class no_retry refuses re-dispatch of {operation_key}"),
                    true,
                ));
            }
            if !matches!(
                intent.state,
                EffectState::Dispatched | EffectState::Reconciling
            ) {
                return Err((
                    ErrorKind::InvalidRequest,
                    format!("cannot dispatch from state {:?}", intent.state),
                    false,
                ));
            }
        }
        // §5.2 rule 4's second disjunct (A14): an intent whose
        // `prepared -> dispatching` transition did NOT commit before an
        // applicable cancellation committed settles `cancelled` instead of
        // dispatching — the adapter that read a prepared intent cannot
        // lawfully perform the operation. `prepared` is §5.3 disjunct 1
        // (proved never dispatched), so the settle legality path admits it.
        if intent.state == EffectState::Prepared
            && pre
                .cancel
                .as_ref()
                .is_some_and(|c| !c.admission_blockers.is_empty())
        {
            let settled_at = *self.clock_ms.lock().expect("clock");
            let terminal = EffectTerminal::Cancelled;
            intent.settle(terminal).expect("prepared settles cancelled");
            intent.settled_at = Some(settled_at);
            intent.version = next_version(row.version)?;
            return update_accepted(
                row,
                intent,
                Some(terminal_wire(terminal)),
                "effect.settled",
                json!({
                    "terminal": "cancelled",
                    "settled_at": settled_at,
                    "rule_4": true,
                }),
            );
        }
        // R20: an irreversible intent MUST NOT reach `dispatching` without a
        // committed approval. This rejection is not durable: a later
        // cancellation changes the lawful result from approval_required to
        // a cancelled settlement without authorizing the adapter.
        if intent.reversibility_class == ReversibilityClass::Irreversible
            && intent.approval_ref.is_none()
        {
            return Err((
                ErrorKind::ApprovalRequired,
                format!("irreversible effect {operation_key} has no committed approval_ref"),
                false,
            ));
        }
        intent.state = EffectState::Dispatching;
        intent.version = next_version(row.version)?;
        update_accepted(
            row,
            intent,
            None,
            "effect.dispatching",
            json!({ "operation_key": operation_key }),
        )
    }

    fn validate_prepare(
        &self,
        cmd: &Command,
        pre: &PreRead,
        unit_id: &str,
        spec: &EffectSpec,
    ) -> Result<Accepted, Rejection> {
        let token = self.holder_gate(cmd, pre, unit_id)?;
        // §5.2 rule 4: a committed ancestor cancellation bars a NEW intent
        // under that lineage — the freeze snapshot governs existing members;
        // later-created members are blocked, not retroactively added (rule 2).
        if pre
            .cancel
            .as_ref()
            .is_some_and(|c| !c.admission_blockers.is_empty())
        {
            return Err((
                ErrorKind::InvalidRequest,
                format!(
                    "prepare barred by committed cancellation: {}",
                    pre.cancel
                        .as_ref()
                        .expect("checked")
                        .admission_blockers
                        .iter()
                        .map(|b| format!("{} ({})", b.member, b.reason))
                        .collect::<Vec<_>>()
                        .join("; ")
                ),
                true,
            ));
        }
        check_target_list(&spec.declared_targets)?;
        // §4.3: the enumeration mode is declared, and the declaration must
        // match the payload — otherwise "declared with nothing enumerated"
        // would silently buy the untrusted post-hoc classification.
        match spec.target_enumeration {
            TargetEnumeration::Declared if spec.declared_targets.is_empty() => {
                return Err((
                    ErrorKind::InvalidRequest,
                    "target_enumeration=declared requires a non-empty declared_targets".into(),
                    true,
                ));
            }
            TargetEnumeration::PostHoc if !spec.declared_targets.is_empty() => {
                return Err((
                    ErrorKind::InvalidRequest,
                    "target_enumeration=post_hoc must not declare targets".into(),
                    true,
                ));
            }
            _ => {}
        }
        let key = pre.effect_key.clone().expect("prepare derives the key");
        if let Some(existing) = &pre.effect {
            let intent = parse_record(existing)?;
            intent_binds(&intent, unit_id)?;
            // §4.1: re-preparing the same logical operation RETURNS the
            // existing intent — it never mutates it. A digest-equal spec
            // that disagrees on any declared axis is a contradictory
            // prepare, not a retry.
            let declared_same = intent.target_enumeration == spec.target_enumeration
                && intent.targets.len() == spec.declared_targets.len()
                && intent
                    .targets
                    .iter()
                    .zip(declared_at_prepare(&spec.declared_targets).iter())
                    .all(|(a, b)| {
                        a.target_id == b.target_id
                            && a.expected_digest == b.expected_digest
                            && a.pre_digest == b.pre_digest
                    });
            let divergent = intent.adapter_id != spec.adapter_id
                || intent.adapter_version != spec.adapter_version
                || intent.retry_class != spec.retry_class
                || intent.reversibility_class != spec.reversibility_class
                || intent.policy_snapshot_digest != spec.policy_snapshot_digest
                || intent.decomposable != spec.decomposable
                || intent.compensation_intent_id != spec.compensation_intent_id
                || intent.approval_ref != spec.approval_ref
                || !declared_same;
            if divergent {
                return Err((
                    ErrorKind::InvalidRequest,
                    format!("divergent re-prepare of existing intent {key}"),
                    true,
                ));
            }
            return Ok(Accepted {
                plan: Plan::Nothing,
                events: vec![],
                result: json!({
                    "operation_key": key,
                    "effect_intent_id": intent.effect_intent_id.to_string(),
                    "version": intent.version,
                    "existing": true,
                }),
            });
        }
        let intent = EffectIntent {
            effect_intent_id: self.mint_id(),
            version: 1,
            unit_id: unit_id.to_owned(),
            attempt_epoch: token.attempt_epoch,
            stamp: token.stamp,
            authority_epoch: token.authority_epoch,
            adapter_id: spec.adapter_id.clone(),
            adapter_version: spec.adapter_version.clone(),
            operation_key: key.clone(),
            logical_operation_id: spec.logical_operation_id,
            request_digest: spec.request_digest.clone(),
            retry_class: spec.retry_class,
            reversibility_class: spec.reversibility_class,
            approval_ref: spec.approval_ref,
            policy_snapshot_digest: spec.policy_snapshot_digest.clone(),
            state: EffectState::Prepared,
            terminal: None,
            target_enumeration: spec.target_enumeration,
            targets: declared_at_prepare(&spec.declared_targets),
            decomposable: spec.decomposable,
            parent_effect_intent_id: None,
            compensation_intent_id: spec.compensation_intent_id,
            dispatched_at: None,
            settled_at: None,
            next_reconcile_at: None,
        };
        let effect_intent_id = intent.effect_intent_id.to_string();
        let record = serde_json::to_string(&intent).expect("intent serializes");
        Ok(Accepted {
            plan: Plan::CreateEffect {
                operation_key: key.clone(),
                effect_intent_id: effect_intent_id.clone(),
                unit_id: unit_id.to_owned(),
                attempt_epoch: token.attempt_epoch.0,
                record,
            },
            events: vec![EventDraft {
                aggregate_kind: "effect",
                aggregate_id: effect_intent_id.clone(),
                aggregate_version: 1,
                event_kind: "effect.prepared",
                payload: json!({
                    "effect_intent_id": effect_intent_id,
                    "adapter_id": spec.adapter_id,
                    "retry_class": spec.retry_class,
                    "reversibility_class": spec.reversibility_class,
                    "target_enumeration": spec.target_enumeration,
                }),
            }],
            result: json!({
                "operation_key": key,
                "effect_intent_id": effect_intent_id,
                "version": 1,
                "existing": false,
            }),
        })
    }
}
