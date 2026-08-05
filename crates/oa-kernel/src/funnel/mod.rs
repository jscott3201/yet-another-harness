//! The single mutation funnel (ADR-001 §2.2): every accepted command is one
//! Selene transaction that claims-or-resolves the receipt, validates
//! authority and fence, CASes the named aggregate, appends semantic events,
//! and finalizes the receipt — commit or nothing.
//!
//! Method registry discipline (§2.1): every method carries exactly one
//! authorization class. Authority methods require the current
//! `authority_epoch` and no token; holder methods require the full §3.3
//! five-axis fence AND that the token's sealed `unit_id` names the method's
//! target; `token.reissue` is the single `holder_reauth` method — it
//! forgives a stale stamp (that is its purpose, §3.4) but nothing else.
//!
//! `submit` is serialized end to end by the funnel gate: Selene's writer
//! lock releases at seal, before the commit's durability wait, so the
//! address-book update and the next command's claim step would otherwise
//! race the commit tail (observed: a concurrent identical retry drew
//! `internal` instead of `Replayed`). One command at a time IS the §2.2
//! funnel; the gate makes it literal.
//!
//! A commit error is not proof of rejection (§2.2): the write may be durable
//! but unpublished. The funnel answers `outcome_unknown`, poisons itself,
//! and rejects everything after as `unavailable` — recovery (a new lifetime
//! over `Store::recover`) is the reconcile path, because resubmitting the
//! command there resolves it by durable receipt.
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
    AttemptTokenClaims, BookKind, LeaseFence, Store, StoreRejection, UnitFence, UnitFenceRead, db,
    no_labels, props_set, value_str, value_u64,
};
use selene_core::{LabelSet, NodeId, PropertyMap, Value};
use serde_json::json;
use std::sync::Mutex;

mod cancel;
mod cancel_rules;
mod command;
mod effect_rules;
mod effects;
mod plan;
mod replay;
mod run_rules;
#[cfg(test)]
#[path = "unit_tests.rs"]
mod tests;
mod validate;

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
    pub fn new(store: Store, clock_ms: u64) -> Funnel {
        Funnel {
            store,
            gate: Mutex::new(None),
            clock_ms: Mutex::new(clock_ms),
            mint_seq: Mutex::new(0),
        }
    }

    pub fn store(&self) -> &Store {
        &self.store
    }

    pub fn into_store(self) -> Store {
        self.store
    }

    /// Advance the logical clock (monotonic; lower values are ignored).
    pub fn tick(&self, to_ms: u64) {
        let mut c = self.clock_ms.lock().expect("clock");
        *c = (*c).max(to_ms);
    }

    fn mint_id(&self) -> Uuid7 {
        let ms = *self.clock_ms.lock().expect("clock");
        let mut seq = self.mint_seq.lock().expect("seq");
        *seq += 1;
        // Authority epoch in the entropy high bits: cross-lifetime
        // uniqueness by construction, not by caller clock discipline.
        let entropy = (u128::from(self.store.authority_epoch().0) << 64) | u128::from(*seq);
        Uuid7::mint(ms, entropy)
    }

    pub fn submit(&self, cmd: &Command) -> Submission {
        let mut gate = self.gate.lock().expect("funnel gate");
        if let Some(detail) = gate.as_ref() {
            return Submission::Rejected {
                kind: ErrorKind::Unavailable,
                detail: format!("funnel poisoned by uncertain commit: {detail}"),
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
                    p.get(&db("request_digest")).and_then(value_str),
                    p.get(&db("status")).and_then(value_str),
                    p.get(&db("result")).and_then(value_str),
                )
            });
            txn.rollback();
            return Self::replay_stored(cmd, stored);
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
                let result = json!({ "error_kind": kind, "detail": detail });
                let node = {
                    let mut m = txn.mutator();
                    m.create_node(
                        LabelSet::single(db("Receipt")),
                        PropertyMap::from_pairs([
                            (db("receipt_key"), Value::String(db(&receipt_key))),
                            (
                                db("request_digest"),
                                Value::String(db(cmd.request_digest.as_str())),
                            ),
                            (db("status"), Value::String(db("rejected"))),
                            (db("result"), Value::String(db(&result.to_string()))),
                        ])
                        .expect("rejection receipt property map"),
                    )
                };
                match node {
                    Ok(n) => match txn.commit() {
                        Ok(_) => self.store.book_insert(BookKind::Receipt, receipt_key, n),
                        // The domain answer (rejection, no transition) is
                        // certain either way; the engine's health is not —
                        // poison so the next submit halts (§2.2).
                        Err(e) => *gate = Some(format!("{e:?}")),
                    },
                    Err(_) => txn.rollback(),
                }
                return Submission::Rejected {
                    kind,
                    detail,
                    replayed: false,
                };
            }
        };

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
                    holder_id,
                    attempt_id,
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
                                (db("holder_id"), s(holder_id)),
                                (db("status"), s("active")),
                            ])
                            .expect("attempt property map"),
                        )
                        .map_err(|e| format!("{e:?}"))?;
                    new_books.push((BookKind::Attempt, attempt_key, a));
                    match existing_lease {
                        Some((lease_node, lease_version)) => {
                            m.update_node(
                                *lease_node,
                                no_labels(),
                                props_set([
                                    (db("attempt_epoch"), Value::Uint(*new_epoch)),
                                    (db("holder_id"), s(holder_id)),
                                    (db("status"), s("active")),
                                    (db("version"), Value::Uint(lease_version + 1)),
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
                    record,
                } => {
                    let n = m
                        .create_node(
                            LabelSet::single(db("Effect")),
                            PropertyMap::from_pairs([
                                (db("operation_key"), s(operation_key)),
                                (db("effect_intent_id"), s(effect_intent_id)),
                                (db("unit_id"), s(unit_id)),
                                (db("version"), Value::Uint(1)),
                                (db("state"), s("prepared")),
                                (db("record"), s(record)),
                            ])
                            .expect("effect property map"),
                        )
                        .map_err(|e| format!("{e:?}"))?;
                    new_books.push((BookKind::Effect, operation_key.clone(), n));
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

            for draft in &accepted.events {
                // Cursor allocation under the open write txn — the
                // allocate_cursor ordering contract.
                let cursor = self.store.allocate_cursor();
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
                                    "{}/{}/{}/0",
                                    draft.aggregate_kind,
                                    draft.aggregate_id,
                                    draft.aggregate_version
                                )),
                            ),
                            (db("aggregate_kind"), s(draft.aggregate_kind)),
                            (db("aggregate_id"), s(&draft.aggregate_id)),
                            (
                                db("aggregate_version"),
                                Value::Uint(draft.aggregate_version),
                            ),
                            (db("ordinal"), Value::Uint(0)),
                            (db("event_kind"), s(draft.event_kind)),
                            (db("payload"), s(&draft.payload.to_string())),
                            (db("command_id"), s(&cmd.command_id.to_string())),
                        ])
                        .expect("event property map"),
                    )
                    .map_err(|e| format!("{e:?}"))?;
                new_books.push((BookKind::Event, event_id, n));
                cursors.push(cursor);
            }

            let mut receipt_pairs = vec![
                (db("receipt_key"), s(&receipt_key)),
                (db("request_digest"), s(cmd.request_digest.as_str())),
                (db("status"), s("completed")),
                (db("result"), s(&accepted.result.to_string())),
            ];
            if let (Some(first), Some(last)) = (cursors.first(), cursors.last()) {
                receipt_pairs.push((db("first_cursor"), Value::Uint(*first)));
                receipt_pairs.push((db("last_cursor"), Value::Uint(*last)));
            }
            let r = m
                .create_node(
                    LabelSet::single(db("Receipt")),
                    PropertyMap::from_pairs(receipt_pairs).expect("receipt property map"),
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
        // §2.2 step 8. An Err here is NOT proof of rejection — the write
        // may be durable but unpublished — so the answer is outcome_unknown
        // and the funnel halts; a recovered lifetime resolves the command
        // by resubmitting against its durable receipt.
        if let Err(e) = txn.commit() {
            let detail = format!("{e:?}");
            *gate = Some(detail.clone());
            return Submission::Rejected {
                kind: ErrorKind::OutcomeUnknown,
                detail: format!("commit outcome uncertain: {detail}"),
                replayed: false,
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
