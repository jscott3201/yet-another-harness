//! The single mutation funnel (ADR-001 §2.2): every accepted command is one
//! Selene transaction that claims-or-resolves the receipt, validates
//! authority and fence, CASes the named aggregate, appends semantic events,
//! and finalizes the receipt — commit or nothing.
//!
//! Method registry discipline (§2.1): every method carries exactly one
//! authorization class. Authority methods require the current
//! `authority_epoch` and no token; holder methods require the full §3.3
//! five-axis fence AND that the token's sealed `unit_id` names the method's
//! target. The reserved `token.reissue` method fails closed until an
//! authority-issued policy and approval revalidation record exists.
//!
//! `submit` is serialized end to end by the funnel gate: Selene's writer
//! lock releases at seal, before the commit's durability wait, so the
//! address-book update and the next command's claim step would otherwise
//! race the commit tail (observed: a concurrent identical retry drew
//! `internal` instead of `Replayed`). One command at a time IS the §2.2
//! funnel; the gate makes it literal.
//!
//! A durable commit error is not proof of rejection (§2.2): the write may be
//! durable but unpublished. The funnel answers `outcome_unknown`, poisons
//! itself, and rejects everything after as `unavailable`. Errors Selene
//! classifies before durability return `internal` without poisoning. Recovery
//! is the reconcile path for an uncertain commit.
//!
//! Time and identity are injected: the funnel owns a logical clock advanced
//! by the caller and mints Uuid7s from (clock, authority_epoch, counter).
//! The epoch rides the entropy high bits, so two lifetimes cannot mint the
//! same event id even at the same logical millisecond (epochs are monotonic
//! across recover; the unique `event_id` constraint remains the backstop
//! beyond 4096 takeovers, where the 12-bit rand_a field wraps).

use crate::effect::EffectIntent;
use crate::error::ErrorKind;
use crate::ids::{AttemptEpoch, AuthorityEpoch, Digest, Stamp, Uuid7};
use crate::store::{
    AttemptTokenClaims, BookKind, LeaseFence, Store, StoreError, StoreRejection, UnitFence,
    UnitFenceRead, db, no_labels, props_set, value_str, value_u64,
};
use selene_core::{LabelSet, NodeId, PropertyMap, Value};
use serde_json::json;
use std::sync::Mutex;

mod cancel;
mod cancel_resolution;
pub(crate) mod cancel_rules;
mod command;
mod effect_rules;
mod effects;
mod lifecycle;
mod plan;
mod receipt;
mod reject;
mod replay;
mod run_rules;
#[cfg(test)]
#[path = "unit_tests.rs"]
mod tests;
mod validate;
mod wire;

use plan::{EventDraft, Plan};

pub use command::{Command, Method, PrincipalKind, RunOutcome, ScopeKind, Submission};
pub use effect_rules::SettleEvidence;
pub use effects::EffectSpec;
pub use replay::token_from_result;

/// Fence-relevant unit row, pre-read under the open write transaction.
struct UnitRow {
    node: NodeId,
    version: u64,
    epoch: u64,
    stamp: u64,
    /// §4.1 operation-key derivation input for `effect.prepare`.
    work_item_id: String,
}

/// Effect row pre-read for the §4 methods, keyed by operation_key.
struct EffectRow {
    node: NodeId,
    version: u64,
    /// The full serialized [`EffectIntent`]. The indexed `state`/`terminal`
    /// columns exist for the recovery scan's snapshot queries; the funnel
    /// itself reads legality off this record, so there is exactly one
    /// source of truth per decision.
    record: String,
}

struct LeaseRow {
    node: NodeId,
    holder_id: String,
    status: String,
    version: u64,
}

/// Pre-read fence state, honest about which unit it was read for: the fence
/// resolves the TOKEN's sealed unit_id through this seam, and answering for
/// any other unit would silently re-point the token at the method's target
/// (the wildcard-credential hole). A lookup for a different unit returns
/// `None` and the fence fails closed.
struct PreRead {
    for_unit: Option<String>,
    unit: Option<UnitRow>,
    lease: Option<LeaseRow>,
    /// The operation key this command resolves to: carried by the update
    /// methods, derived from (work_item_id, logical_operation_id,
    /// request_digest) for `effect.prepare`.
    effect_key: Option<String>,
    effect: Option<EffectRow>,
    /// The run named by a §1.2 run method, and — for a close — the I11
    /// blockers read in the same transaction as the CAS that would close it.
    run: Option<RunRow>,
    run_blockers: Vec<run_rules::Blocker>,
    /// §5 pre-read data: the rule-4 admission gate for a child this command
    /// would create, and the frozen-scope/delivery state a cancellation
    /// command reads. Computed in the same transaction as every other fence
    /// input, from the same write-side working graph.
    cancel: Option<cancel::CancelPreRead>,
}

struct RunRow {
    node: NodeId,
    version: u64,
    status: String,
}

impl UnitFenceRead for PreRead {
    fn unit_fence(&self, unit_id: &str) -> Option<UnitFence> {
        if self.for_unit.as_deref() != Some(unit_id) {
            return None;
        }
        self.unit.as_ref().map(|u| UnitFence {
            attempt_epoch: AttemptEpoch(u.epoch),
            stamp: Stamp(u.stamp),
        })
    }
    fn lease_fence(&self, unit_id: &str) -> Option<LeaseFence> {
        if self.for_unit.as_deref() != Some(unit_id) {
            return None;
        }
        self.lease.as_ref().map(|l| LeaseFence {
            holder_id: l.holder_id.clone(),
            status: l.status.clone(),
        })
    }
}

/// What an accepted command will do, staged as concrete values so the
/// mutation phase performs no reads.
struct Accepted {
    plan: Plan,
    events: Vec<EventDraft>,
    result: serde_json::Value,
}

type Rejection = (ErrorKind, String, /* persist receipt */ bool);

pub struct Funnel {
    store: Store,
    /// Serializes `submit` end to end and carries the §2.2 poison latch:
    /// `Some(detail)` after an uncertain commit, at which point every
    /// subsequent submit answers `unavailable` until recovery.
    gate: Mutex<Option<String>>,
    clock_ms: Mutex<u64>,
    mint_seq: Mutex<u64>,
}

impl Funnel {
    pub fn submit(&self, cmd: &Command) -> Submission {
        let mut gate = self.gate.lock().expect("funnel gate");
        if let Some(detail) = gate.as_ref() {
            return Submission::Rejected {
                kind: ErrorKind::Unavailable,
                detail: format!("funnel poisoned by uncertain commit: {detail}"),
                replayed: false,
            };
        }
        if !receipt::address_is_valid(cmd, self.store.project_id()) {
            return Submission::Rejected {
                kind: ErrorKind::InvalidRequest,
                detail:
                    "receipt scope and command identifiers must use the wire identifier grammar"
                        .into(),
                replayed: false,
            };
        }

        let receipt_key = format!(
            "{}/{}/{}",
            cmd.scope_kind.wire(),
            cmd.scope_id,
            cmd.command_id
        );

        let mut txn = self.store.shared().begin_write();

        // §2.2 step 2: claim or resolve. A stored receipt with an equal
        // digest replays its stable answer; an unequal digest is
        // idempotency_conflict with no transition (I3).
        if let Some(node) = self.store.receipt_node(&receipt_key) {
            let stored = txn.read().node_properties(node).map(|p| {
                (
                    p.get(&db("command_type")).and_then(value_str),
                    p.get(&db("receipt_version")).and_then(value_u64),
                    p.get(&db("request_digest")).and_then(value_str),
                    p.get(&db("principal_kind")).and_then(value_str),
                    p.get(&db("principal_id")).and_then(value_str),
                    p.get(&db("status")).and_then(value_str),
                    p.get(&db("result")).and_then(value_str),
                )
            });
            txn.rollback();
            return Self::replay_stored(cmd, cmd.method.wire(), stored);
        }

        // §2.2 steps 1 + 3: pre-read fence state from the WRITE-side working
        // graph (the published snapshot can lag — G02 trap), then validate.
        let pre = {
            let read = txn.read();
            let unit = cmd.method.unit_id().and_then(|uid| {
                let node = self.store.unit_node(uid)?;
                let props = read.node_properties(node)?;
                Some(UnitRow {
                    node,
                    version: props.get(&db("version")).and_then(value_u64).unwrap_or(0),
                    epoch: props
                        .get(&db("current_attempt_epoch"))
                        .and_then(value_u64)
                        .unwrap_or(0),
                    stamp: props.get(&db("stamp")).and_then(value_u64).unwrap_or(0),
                    work_item_id: props
                        .get(&db("work_item_id"))
                        .and_then(value_str)
                        .unwrap_or_default(),
                })
            });
            let lease = cmd.method.unit_id().and_then(|uid| {
                let node = self.store.lease_node(uid)?;
                let props = read.node_properties(node)?;
                Some(LeaseRow {
                    node,
                    holder_id: props.get(&db("holder_id")).and_then(value_str)?,
                    status: props.get(&db("status")).and_then(value_str)?,
                    version: props.get(&db("version")).and_then(value_u64).unwrap_or(0),
                })
            });
            let effect_key = match &cmd.method {
                Method::EffectPrepare { spec, .. } => unit.as_ref().map(|u| {
                    EffectIntent::derive_operation_key(
                        &u.work_item_id,
                        spec.logical_operation_id,
                        &spec.request_digest,
                    )
                }),
                _ => cmd.method.operation_key().map(str::to_owned),
            };
            let effect = effect_key.as_deref().and_then(|key| {
                let node = self.store.effect_node(key)?;
                let props = read.node_properties(node)?;
                Some(EffectRow {
                    node,
                    version: props.get(&db("version")).and_then(value_u64).unwrap_or(0),
                    record: props.get(&db("record")).and_then(value_str)?,
                })
            });
            let run_id = match &cmd.method {
                Method::RunOpen { run_id, .. } | Method::RunClose { run_id, .. } => Some(run_id),
                _ => None,
            };
            let run = run_id.and_then(|rid| {
                let node = self.store.run_node(rid)?;
                let props = read.node_properties(node)?;
                Some(RunRow {
                    node,
                    version: props.get(&db("version")).and_then(value_u64).unwrap_or(0),
                    status: props.get(&db("status")).and_then(value_str)?,
                })
            });
            let run_blockers = match &cmd.method {
                Method::RunClose {
                    run_id,
                    outcome: RunOutcome::ClosedSuccess,
                } => run_rules::success_blockers(read, &self.store, run_id),
                _ => Vec::new(),
            };
            // §5 pre-read from the same working graph: the rule-4 admission
            // gate for a child this command would create, and the frozen
            // scope / delivery state a cancellation command reads.
            let cancel = self.cancel_pre_read(cmd, read, &unit, &effect);
            PreRead {
                for_unit: cmd.method.unit_id().map(str::to_owned),
                unit,
                lease,
                effect_key,
                effect,
                run,
                run_blockers,
                cancel,
            }
        };

        let accepted = match self.validate(cmd, &pre) {
            Ok(a) => a,
            Err((kind, detail, persist)) => {
                if !persist {
                    txn.rollback();
                    return Submission::Rejected {
                        kind,
                        detail,
                        replayed: false,
                    };
                }
                // §2.3: rejections that can never heal persist so a
                // timed-out retry gets the same answer — with no
                // transition. version_conflict and not_found do NOT
                // persist: both depend on mutable state a later command
                // can change, so a retry must re-validate.
                let detail = wire::bounded_detail(&detail);
                let result = json!({ "error_kind": kind, "detail": detail });
                let node = {
                    let mut m = txn.mutator();
                    m.create_node(
                        LabelSet::single(db("Receipt")),
                        receipt::properties(
                            cmd,
                            cmd.method.wire(),
                            &receipt_key,
                            "rejected",
                            &result,
                            None,
                        ),
                    )
                };
                let persistence_error = match node {
                    Ok(n) => match txn.commit() {
                        Ok(_) => {
                            self.store.book_insert(BookKind::Receipt, receipt_key, n);
                            None
                        }
                        // The domain answer (rejection, no transition) is
                        // certain either way; the engine's health is not —
                        // poison so the next submit halts (§2.2).
                        Err(error) => {
                            if let StoreError::CommitUnknown(detail) =
                                crate::store::commit_error(error)
                            {
                                *gate = Some(detail);
                                None
                            } else {
                                Some("rejection receipt commit failed before durability".to_owned())
                            }
                        }
                    },
                    Err(error) => {
                        txn.rollback();
                        Some(format!("cannot stage rejection receipt: {error:?}"))
                    }
                };
                if let Some(detail) = persistence_error {
                    return Submission::Rejected {
                        kind: ErrorKind::Internal,
                        detail,
                        replayed: false,
                    };
                }
                return Submission::Rejected {
                    kind,
                    detail,
                    replayed: false,
                };
            }
        };
        let over_limit = accepted.events.iter().any(|event| {
            wire::wire_json_len(&event.payload) > crate::protocol::MAX_EVENT_PAYLOAD_BYTES
        }) || serde_json_canonicalizer::to_vec(&accepted.result)
            .map(|result| result.len() > crate::protocol::MAX_RESULT_BYTES)
            .unwrap_or(true);
        if over_limit {
            let kind = ErrorKind::PayloadTooLarge;
            let detail = "inline result or semantic event payload exceeds its protocol limit; use an artifact reference".to_owned();
            let result = json!({ "error_kind": kind, "detail": detail });
            let node = {
                let mut m = txn.mutator();
                m.create_node(
                    LabelSet::single(db("Receipt")),
                    receipt::properties(
                        cmd,
                        cmd.method.wire(),
                        &receipt_key,
                        "rejected",
                        &result,
                        None,
                    ),
                )
            };
            let persistence_error = match node {
                Ok(node) => match txn.commit() {
                    Ok(_) => {
                        self.store.book_insert(BookKind::Receipt, receipt_key, node);
                        None
                    }
                    Err(error) => {
                        if let StoreError::CommitUnknown(detail) = crate::store::commit_error(error)
                        {
                            *gate = Some(detail);
                            None
                        } else {
                            Some("oversize rejection commit failed before durability".to_owned())
                        }
                    }
                },
                Err(error) => {
                    txn.rollback();
                    Some(format!("cannot stage oversize rejection: {error:?}"))
                }
            };
            if let Some(detail) = persistence_error {
                return Submission::Rejected {
                    kind: ErrorKind::Internal,
                    detail,
                    replayed: false,
                };
            }
            return Submission::Rejected {
                kind,
                detail,
                replayed: false,
            };
        }

        // §2.2 steps 4–7: apply the staged plan, append events, finalize
        // the completed receipt. No reads below this line.
        let mut new_books: Vec<(BookKind, String, NodeId)> = Vec::new();
        let mut cursors: Vec<u64> = Vec::new();
        let staged: Result<(), String> = (|| {
            let mut m = txn.mutator();
            let s = |v: &str| Value::String(db(v));
            match &accepted.plan {
                Plan::CreateWorkItem {
                    work_item_id,
                    acceptance_contract_digest,
                    declared_write_scope,
                } => {
                    let n = m
                        .create_node(
                            LabelSet::single(db("WorkItem")),
                            PropertyMap::from_pairs([
                                (db("work_item_id"), s(work_item_id)),
                                (db("version"), Value::Uint(1)),
                                (db("status"), s("ready")),
                                (
                                    db("acceptance_contract_digest"),
                                    s(acceptance_contract_digest),
                                ),
                                (db("declared_write_scope"), s(declared_write_scope)),
                                (db("record"), s("{}")),
                            ])
                            .expect("work item property map"),
                        )
                        .map_err(|e| format!("{e:?}"))?;
                    new_books.push((BookKind::WorkItem, work_item_id.clone(), n));
                }
                Plan::CreateRun {
                    run_id,
                    goal_work_item_id,
                } => {
                    let n = m
                        .create_node(
                            LabelSet::single(db("Run")),
                            PropertyMap::from_pairs([
                                (db("run_id"), s(run_id)),
                                (db("version"), Value::Uint(1)),
                                (db("status"), s("open")),
                                (db("goal_work_item_id"), s(goal_work_item_id)),
                                (db("record"), s("{}")),
                            ])
                            .expect("run property map"),
                        )
                        .map_err(|e| format!("{e:?}"))?;
                    new_books.push((BookKind::Run, run_id.clone(), n));
                }
                Plan::CloseRun {
                    run_node,
                    new_version,
                    status,
                } => {
                    m.update_node(
                        *run_node,
                        no_labels(),
                        props_set([
                            (db("version"), Value::Uint(*new_version)),
                            (db("status"), s(status)),
                        ]),
                    )
                    .map_err(|e| format!("{e:?}"))?;
                }
                Plan::CreateUnit {
                    unit_id,
                    work_item_id,
                    run_id,
                } => {
                    let n = m
                        .create_node(
                            LabelSet::single(db("Unit")),
                            PropertyMap::from_pairs([
                                (db("unit_id"), s(unit_id)),
                                (db("version"), Value::Uint(1)),
                                (db("current_attempt_epoch"), Value::Uint(0)),
                                (db("stamp"), Value::Uint(0)),
                                (db("status"), s("admitted")),
                                (db("work_item_id"), s(work_item_id)),
                                (db("run_id"), s(run_id)),
                                (db("record"), s("{}")),
                            ])
                            .expect("unit property map"),
                        )
                        .map_err(|e| format!("{e:?}"))?;
                    new_books.push((BookKind::Unit, unit_id.clone(), n));
                }
                Plan::Dispatch {
                    unit_node,
                    unit_id,
                    new_version,
                    new_epoch,
                    stamp,
                    authority_epoch,
                    holder_id,
                    attempt_id,
                    token_nonce,
                    existing_lease,
                    prior_attempt,
                } => {
                    m.update_node(
                        *unit_node,
                        no_labels(),
                        props_set([
                            (db("version"), Value::Uint(*new_version)),
                            (db("current_attempt_epoch"), Value::Uint(*new_epoch)),
                            (db("status"), s("dispatched")),
                        ]),
                    )
                    .map_err(|e| format!("{e:?}"))?;
                    if let Some(prior) = prior_attempt {
                        m.update_node(
                            *prior,
                            no_labels(),
                            props_set([(db("status"), s("superseded"))]),
                        )
                        .map_err(|e| format!("{e:?}"))?;
                    }
                    let attempt_key = format!("{unit_id}/{new_epoch}");
                    let a = m
                        .create_node(
                            LabelSet::single(db("Attempt")),
                            PropertyMap::from_pairs([
                                (db("attempt_key"), s(&attempt_key)),
                                (db("attempt_id"), s(attempt_id)),
                                (db("unit_id"), s(unit_id)),
                                (db("attempt_epoch"), Value::Uint(*new_epoch)),
                                (db("stamp"), Value::Uint(*stamp)),
                                (db("authority_epoch"), Value::Uint(*authority_epoch)),
                                (db("holder_id"), s(holder_id)),
                                (db("token_nonce"), s(token_nonce)),
                                (db("status"), s("active")),
                            ])
                            .expect("attempt property map"),
                        )
                        .map_err(|e| format!("{e:?}"))?;
                    new_books.push((
                        BookKind::Attempt {
                            attempt_id: attempt_id.clone(),
                        },
                        attempt_key,
                        a,
                    ));
                    match existing_lease {
                        Some((lease_node, lease_version)) => {
                            m.update_node(
                                *lease_node,
                                no_labels(),
                                props_set([
                                    (db("attempt_epoch"), Value::Uint(*new_epoch)),
                                    (db("holder_id"), s(holder_id)),
                                    (db("status"), s("active")),
                                    (
                                        db("version"),
                                        Value::Uint(lease_version.checked_add(1).ok_or_else(
                                            || "lease version space exhausted".to_owned(),
                                        )?),
                                    ),
                                ]),
                            )
                            .map_err(|e| format!("{e:?}"))?;
                        }
                        None => {
                            let l = m
                                .create_node(
                                    LabelSet::single(db("Lease")),
                                    PropertyMap::from_pairs([
                                        (db("unit_id"), s(unit_id)),
                                        (db("attempt_epoch"), Value::Uint(*new_epoch)),
                                        (db("holder_id"), s(holder_id)),
                                        (db("status"), s("active")),
                                        (db("version"), Value::Uint(1)),
                                    ])
                                    .expect("lease property map"),
                                )
                                .map_err(|e| format!("{e:?}"))?;
                            new_books.push((BookKind::Lease, unit_id.clone(), l));
                        }
                    }
                }
                Plan::BumpUnit {
                    unit_node,
                    new_version,
                    new_stamp,
                } => {
                    let mut set = vec![(db("version"), Value::Uint(*new_version))];
                    if let Some(stamp) = new_stamp {
                        set.push((db("stamp"), Value::Uint(*stamp)));
                    }
                    m.update_node(*unit_node, no_labels(), props_set(set))
                        .map_err(|e| format!("{e:?}"))?;
                }
                Plan::CreateEffect {
                    operation_key,
                    effect_intent_id,
                    unit_id,
                    attempt_epoch,
                    record,
                } => {
                    let n = m
                        .create_node(
                            LabelSet::single(db("Effect")),
                            PropertyMap::from_pairs([
                                (db("operation_key"), s(operation_key)),
                                (db("effect_intent_id"), s(effect_intent_id)),
                                (db("unit_id"), s(unit_id)),
                                (db("attempt_epoch"), Value::Uint(*attempt_epoch)),
                                (db("version"), Value::Uint(1)),
                                (db("state"), s("prepared")),
                                (db("record"), s(record)),
                            ])
                            .expect("effect property map"),
                        )
                        .map_err(|e| format!("{e:?}"))?;
                    new_books.push((
                        BookKind::Effect {
                            effect_intent_id: effect_intent_id.clone(),
                        },
                        operation_key.clone(),
                        n,
                    ));
                }
                Plan::UpdateEffect {
                    effect_node,
                    new_version,
                    state,
                    terminal,
                    record,
                } => {
                    let mut set = vec![
                        (db("version"), Value::Uint(*new_version)),
                        (db("state"), s(state)),
                        (db("record"), s(record)),
                    ];
                    if let Some(t) = terminal {
                        set.push((db("terminal"), s(t)));
                    }
                    m.update_node(*effect_node, no_labels(), props_set(set))
                        .map_err(|e| format!("{e:?}"))?;
                }
                Plan::CreateCancelRequest(p) => {
                    new_books.extend(cancel::apply_create_request(&mut m, p)?);
                }
                Plan::CancelRecordDelivery(p) => {
                    new_books.extend(cancel::apply_record_delivery(&mut m, p)?);
                }
                Plan::Nothing => {}
            }

            for (ordinal, draft) in accepted.events.iter().enumerate() {
                let ordinal = u64::try_from(ordinal)
                    .map_err(|_| "semantic event ordinal is out of range".to_owned())?;
                // Cursor allocation under the open write txn — the
                // allocate_cursor ordering contract.
                let cursor = self
                    .store
                    .allocate_cursor()
                    .map_err(|error| format!("{error:?}"))?;
                let event_id = self.mint_id().to_string();
                let n = m
                    .create_node(
                        LabelSet::single(db("Event")),
                        PropertyMap::from_pairs([
                            (db("event_id"), s(&event_id)),
                            (db("cursor"), Value::Uint(cursor)),
                            (
                                // Kind-prefixed: aggregate ids are unique per
                                // kind only, so cross-kind id reuse must not
                                // collide the derived composite.
                                db("agg_ver_ord"),
                                s(&format!(
                                    "{}/{}/{}/{}",
                                    draft.aggregate_kind,
                                    draft.aggregate_id,
                                    draft.aggregate_version,
                                    ordinal,
                                )),
                            ),
                            (db("aggregate_kind"), s(draft.aggregate_kind)),
                            (db("aggregate_id"), s(&draft.aggregate_id)),
                            (
                                db("aggregate_version"),
                                Value::Uint(draft.aggregate_version),
                            ),
                            (db("ordinal"), Value::Uint(ordinal)),
                            (db("event_kind"), s(draft.event_kind)),
                            (db("payload"), s(&draft.payload.to_string())),
                            (db("receipt_key"), s(&receipt_key)),
                            (db("command_id"), s(&cmd.command_id)),
                            (db("actor_kind"), s(cmd.principal_kind.wire())),
                            (db("actor_id"), s(&cmd.principal_id)),
                            (
                                db("occurred_at_ms"),
                                Value::Uint(*self.clock_ms.lock().expect("clock")),
                            ),
                        ])
                        .expect("event property map"),
                    )
                    .map_err(|e| format!("{e:?}"))?;
                if let Some(causation_id) = &cmd.causation_id {
                    m.update_node(
                        n,
                        no_labels(),
                        props_set([(db("causation_id"), s(causation_id))]),
                    )
                    .map_err(|e| format!("{e:?}"))?;
                }
                if let Some(correlation_id) = &cmd.correlation_id {
                    m.update_node(
                        n,
                        no_labels(),
                        props_set([(db("correlation_id"), s(correlation_id))]),
                    )
                    .map_err(|e| format!("{e:?}"))?;
                }
                new_books.push((BookKind::Event, cursor.to_string(), n));
                cursors.push(cursor);
            }

            let r = m
                .create_node(
                    LabelSet::single(db("Receipt")),
                    receipt::properties(
                        cmd,
                        cmd.method.wire(),
                        &receipt_key,
                        "completed",
                        &accepted.result,
                        cursors.first().copied().zip(cursors.last().copied()),
                    ),
                )
                .map_err(|e| format!("{e:?}"))?;
            new_books.push((BookKind::Receipt, receipt_key.clone(), r));
            Ok(())
        })();

        if let Err(detail) = staged {
            txn.rollback();
            return Submission::Rejected {
                kind: ErrorKind::Internal,
                detail,
                replayed: false,
            };
        }
        // §2.2 step 8. Only a durable/unclassified error has an uncertain
        // outcome. Selene validation and cancellation errors happened before
        // durability and must not demand reconciliation.
        if let Err(error) = txn.commit() {
            return match crate::store::commit_error(error) {
                StoreError::CommitUnknown(detail) => {
                    *gate = Some(detail.clone());
                    Submission::Rejected {
                        kind: ErrorKind::OutcomeUnknown,
                        detail: format!("commit outcome uncertain: {detail}"),
                        replayed: false,
                    }
                }
                error => Submission::Rejected {
                    kind: ErrorKind::Internal,
                    detail: format!("commit rejected before durability: {error:?}"),
                    replayed: false,
                },
            };
        }
        for (kind, key, node) in new_books {
            self.store.book_insert(kind, key, node);
        }
        Submission::Completed {
            result: accepted.result,
        }
    }
}
