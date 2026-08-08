//! §5 cancellation method arms: `cancel.request` commits the frozen scope
//! every member is delivered against (durable before any signal, §5.2
//! rule 1 / I10), and `cancel.record_delivery` advances one member through
//! the write-once delivery contract (§5.2 rule 3) while parking a
//! dispatch-evidence effect at `reconciling` (§5.3).
//!
//! Both methods are §3.3 AUTHORITY class: a cancel of an already-terminal
//! tree, and the recovery scan's late deliveries, must not be fenced on
//! attempt/stamp/lease any more than `effect.settle` is — a delivery
//! records an observation about the world.
//!
//! The rules that are pure functions of a scope live in
//! [`super::cancel_rules`]; this file is the transaction shape, the
//! own-tree membership walk (finding 2), and the authorization order.

use super::plan::{CancelDeliveryPlan, CancelRequestPlan, EffectParkPlan, EventDraft, Plan};
use super::*;
use crate::cancel::{
    CancelDelivery, CancelKind, CancelPolicy, CancelReason, CancelRequest, CancelScope,
    CancelStatus, DeliveryOutcome, MemberInput, ScopeError,
};
use crate::effect::{EffectIntent, EffectState};
use crate::error::ErrorKind;
use crate::ids::Uuid7;
use crate::store::{BookKind, db, no_labels, props_set, value_str, value_u64};
use selene_core::{LabelSet, NodeId, PropertyMap, Value};
use selene_graph::{Mutator, SeleneGraph};
use serde_json::json;

/// §5 data the funnel pre-reads in the same transaction as every other
/// fence input: the rule-4 admission gate for a child this command would
/// create, and the frozen scope / delivery state a cancellation command
/// reads. Computed from the write-side working graph, never the published
/// snapshot (the G02 trap [`super::run_rules`] documents).
pub(super) struct CancelPreRead {
    /// §5.2 rule 4: applicable committed ancestor cancellations.
    pub(super) admission_blockers: Vec<super::cancel_rules::Blocker>,
    /// Own-tree resolution for each proposed scope member (`cancel.request`).
    pub(super) member_links: Vec<MemberLink>,
    /// The committed request a delivery names, when it resolves
    /// (`cancel.record_delivery`).
    pub(super) request: Option<RequestData>,
    /// Existing delivery rows for the named request.
    pub(super) deliveries: Vec<super::cancel_rules::DeliverySummary>,
    /// The effect this delivery targets, when the member is an EffectIntent.
    pub(super) effect_row: Option<EffectRow>,
    /// The owning (unit_id, attempt_epoch) of an Attempt root —
    /// `cancel.request` only; the root's own row anchors the member check.
    pub(super) root_attempt: Option<(String, u64)>,
}

/// How one proposed member resolves UP its own-root chain. Resolution to an
/// id is not membership (finding 2): an id may resolve while the row lives
/// under a different run, and the frozen scope must not claim it.
pub(super) struct MemberLink {
    pub member_id: String,
    /// The member's parent unit id (Attempt/EffectIntent members).
    pub parent_unit: Option<String>,
    /// The member's topmost run id (every kind resolves up to one).
    pub run_id: Option<String>,
    /// The effect member's attempt epoch (an Attempt root compares it).
    pub attempt_epoch: Option<u64>,
}

/// The committed request a delivery names, with its frozen scope and the
/// full record whose status the advance regenerates.
pub(super) struct RequestData {
    pub node: NodeId,
    pub version: u64,
    pub record: CancelRequest,
}

fn prop_str(p: &PropertyMap, name: &str) -> Option<String> {
    p.get(&db(name)).and_then(value_str)
}

fn unit_run_id(read: &SeleneGraph, store: &Store, unit_id: &str) -> Option<String> {
    let node = store.unit_node(unit_id)?;
    let props = read.node_properties(node)?;
    prop_str(props, "run_id")
}

/// `attempt_id` -> `(unit_id, run_id)`: the attempt row addresses its unit,
/// the unit row addresses its run.
fn attempt_to_unit(
    read: &SeleneGraph,
    store: &Store,
    attempt_id: &str,
) -> Option<(String, String)> {
    let node = store.attempt_id_node(read, attempt_id)?;
    let props = read.node_properties(node)?;
    let unit_id = prop_str(props, "unit_id")?;
    let run_id = unit_run_id(read, store, &unit_id)?;
    Some((unit_id, run_id))
}

/// `effect_intent_id` -> `(unit_id, run_id, attempt_epoch)`. The epoch rides
/// the serialized intent record, so an Attempt root can compare it exactly.
fn effect_to_unit(
    read: &SeleneGraph,
    store: &Store,
    effect_intent_id: &str,
) -> Option<(String, String, Option<u64>)> {
    let node = store.effect_intent_id_node(read, effect_intent_id)?;
    let props = read.node_properties(node)?;
    let unit_id = prop_str(props, "unit_id")?;
    let run_id = unit_run_id(read, store, &unit_id)?;
    let attempt_epoch = prop_str(props, "record")
        .and_then(|r| serde_json::from_str::<EffectIntent>(&r).ok())
        .map(|i| i.attempt_epoch.0);
    Some((unit_id, run_id, attempt_epoch))
}

/// Resolve one proposed member up its own-root chain.
fn member_link(read: &SeleneGraph, store: &Store, m: &MemberInput) -> MemberLink {
    let (parent_unit, run_id, attempt_epoch) = match m.member_kind {
        CancelKind::Run => (None, Some(m.member_id.clone()), None),
        CancelKind::ExecutionUnit => (None, unit_run_id(read, store, &m.member_id), None),
        CancelKind::Attempt => {
            let (unit, run) = attempt_to_unit(read, store, &m.member_id).unwrap_or_default();
            (Some(unit), Some(run), None)
        }
        CancelKind::EffectIntent => {
            let (unit, run, epoch) = effect_to_unit(read, store, &m.member_id).unwrap_or_default();
            (Some(unit), Some(run), epoch)
        }
    };
    MemberLink {
        member_id: m.member_id.clone(),
        parent_unit,
        run_id,
        attempt_epoch,
    }
}

/// The lineage of a candidate child, root-first, as the rule-4 gate reads
/// it: run + unit [+ current attempt] — and, for a DISPATCH, the intent
/// itself. A new unit's lineage is its run alone; a new attempt's and a new
/// effect's include the unit; the effect arm carries the unit's current
/// attempt (brief finding 6), because §5.1 roots cancellations at an
/// attempt id, not at the `unit/epoch` composite the book addresses rows
/// by.
fn adoption_lineage(
    method: &Method,
    read: &SeleneGraph,
    store: &Store,
    effect: &Option<EffectRow>,
) -> Option<Vec<(CancelKind, String)>> {
    match method {
        Method::UnitAdmit { run_id, .. } => Some(vec![(CancelKind::Run, run_id.clone())]),
        Method::UnitDispatch { unit_id, .. } | Method::EffectPrepare { unit_id, .. } => {
            unit_lineage(read, store, unit_id)
        }
        // The dispatch arm's "child" is the intent itself being admitted to
        // act, so an EffectIntent-rooted cancellation — a sibling, not an
        // ancestor — must ALSO gate it: without the intent's own root in
        // the lineage, a cancel rooted AT the intent never matches, and a
        // prepared intent would dispatch past its own committed
        // cancellation. UnitAdmit/UnitDispatch/EffectPrepare lineages stay
        // unchanged (their cancelled root is by construction an ancestor).
        Method::EffectDispatch { unit_id, .. } => {
            let mut lineage = unit_lineage(read, store, unit_id)?;
            if let Some(row) = effect
                && let Some(props) = read.node_properties(row.node)
                && let Some(intent_id) = prop_str(props, "effect_intent_id")
            {
                lineage.push((CancelKind::EffectIntent, intent_id));
            }
            Some(lineage)
        }
        _ => None,
    }
}

/// Run + unit [+ current attempt] lineage for a child of `unit_id`. The
/// attempt in the lineage is the unit's current one — attempt keys ascend
/// by epoch (`unit/epoch`) and the funnel serializes dispatch, so the max
/// key is the live attempt.
fn unit_lineage(
    read: &SeleneGraph,
    store: &Store,
    unit_id: &str,
) -> Option<Vec<(CancelKind, String)>> {
    let run_id = unit_run_id(read, store, unit_id)?;
    let current: Option<String> = store
        .attempt_entries()
        .iter()
        .filter(|(key, _)| key.starts_with(&format!("{unit_id}/")))
        .filter_map(|(key, node)| {
            let epoch = key
                .rsplit_once('/')
                .and_then(|(_, e)| e.parse::<u64>().ok())?;
            let props = read.node_properties(*node)?;
            let attempt_id = prop_str(props, "attempt_id");
            attempt_id.map(|id| (epoch, id))
        })
        .max_by_key(|(epoch, _)| *epoch)
        .map(|(_, id)| id);
    let mut lineage = vec![
        (CancelKind::Run, run_id),
        (CancelKind::ExecutionUnit, unit_id.to_owned()),
    ];
    if let Some(id) = current {
        lineage.push((CancelKind::Attempt, id));
    }
    Some(lineage)
}

fn kind_wire(k: CancelKind) -> &'static str {
    k.wire()
}

fn policy_wire(p: CancelPolicy) -> &'static str {
    match p {
        CancelPolicy::AttachedCascade => "attached_cascade",
        CancelPolicy::RootOnly => "root_only",
    }
}

fn reason_wire(r: CancelReason) -> &'static str {
    match r {
        CancelReason::OwnerRequest => "owner_request",
        CancelReason::BudgetExhausted => "budget_exhausted",
        CancelReason::PolicyViolation => "policy_violation",
        CancelReason::SupersededByEpoch => "superseded_by_epoch",
        CancelReason::DependencyFailed => "dependency_failed",
        CancelReason::ShutdownDrain => "shutdown_drain",
    }
}

fn status_wire(s: CancelStatus) -> &'static str {
    match s {
        CancelStatus::Requested => "requested",
        CancelStatus::Delivering => "delivering",
        CancelStatus::ObservedPartial => "observed_partial",
        CancelStatus::Settled => "settled",
    }
}

fn outcome_from_wire(w: &str) -> Option<DeliveryOutcome> {
    match w {
        "observed_stopped" => Some(DeliveryOutcome::ObservedStopped),
        "unresponsive" => Some(DeliveryOutcome::Unresponsive),
        "already_terminal" => Some(DeliveryOutcome::AlreadyTerminal),
        "detached_declined" => Some(DeliveryOutcome::DetachedDeclined),
        _ => None,
    }
}

/// Freeze errors are shape-deterministic: the retry gets the same answer —
/// persist=true, per the effects.rs persist map.
fn scope_rejection(e: ScopeError) -> Rejection {
    let detail = match e {
        ScopeError::TooLarge { count } => format!("scope of {count} members exceeds the cap"),
        ScopeError::DuplicateMember { member_id } => {
            format!("duplicate scope member {member_id}")
        }
        ScopeError::NotUnderRoot {
            member_kind,
            member_id,
        } => format!("member {member_kind:?}:{member_id} is not under the root"),
    };
    (ErrorKind::InvalidRequest, detail, true)
}

fn not_under_root(
    root_kind: CancelKind,
    root_id: &str,
    member_kind: CancelKind,
    member_id: &str,
) -> Rejection {
    (
        ErrorKind::InvalidRequest,
        format!(
            "member {member_kind:?}:{member_id} does not live under root {root_kind:?}:{root_id} (own-tree check)"
        ),
        true,
    )
}

impl Funnel {
    /// §5 pre-read: computed from the same transaction and write-side graph
    /// as every other fence input. For child-admission methods this is the
    /// rule-4 blocker set; for the two cancellation methods it is the
    /// frozen scope / delivery state the arms decide against.
    pub(super) fn cancel_pre_read(
        &self,
        cmd: &Command,
        read: &SeleneGraph,
        _unit: &Option<UnitRow>,
        effect: &Option<EffectRow>,
    ) -> Option<CancelPreRead> {
        match &cmd.method {
            // The rule-4 gate applies to every admission the kernel knows:
            // UnitAdmit (new unit), UnitDispatch (new attempt),
            // EffectPrepare (new intent) and EffectDispatch (the intent's
            // authorized-to-act transition, whose race closed by this very
            // gate — A14).
            Method::UnitAdmit { .. }
            | Method::UnitDispatch { .. }
            | Method::EffectPrepare { .. }
            | Method::EffectDispatch { .. } => {
                let lineage = adoption_lineage(&cmd.method, read, &self.store, effect)?;
                let child_kind = match &cmd.method {
                    Method::UnitAdmit { .. } => CancelKind::ExecutionUnit,
                    Method::UnitDispatch { .. } => CancelKind::Attempt,
                    _ => CancelKind::EffectIntent,
                };
                Some(CancelPreRead {
                    admission_blockers: cancel_rules::admission_blockers(
                        read,
                        &self.store,
                        child_kind,
                        &lineage,
                    ),
                    member_links: Vec::new(),
                    request: None,
                    deliveries: Vec::new(),
                    effect_row: None,
                    root_attempt: None,
                })
            }
            Method::CancelRequest {
                root_kind,
                root_id,
                proposed,
                ..
            } => {
                let member_links = proposed
                    .iter()
                    .map(|m| member_link(read, &self.store, m))
                    .collect();
                let root_attempt = if *root_kind == CancelKind::Attempt {
                    self.store.attempt_id_node(read, root_id).and_then(|node| {
                        let props = read.node_properties(node)?;
                        let unit = prop_str(props, "unit_id")?;
                        // `attempt_epoch` is an indexed NUMERIC column, not a
                        // string — a `value_str` read would always miss and
                        // every Attempt-rooted scope would fail its own walk.
                        let epoch = props.get(&db("attempt_epoch")).and_then(value_u64)?;
                        Some((unit, epoch))
                    })
                } else {
                    None
                };
                Some(CancelPreRead {
                    admission_blockers: Vec::new(),
                    member_links,
                    request: None,
                    deliveries: Vec::new(),
                    effect_row: None,
                    root_attempt,
                })
            }
            Method::CancelRecordDelivery {
                cancel_request_id,
                member_id,
                ..
            } => {
                let request = self.request_data(read, cancel_request_id);
                let deliveries = self.delivery_summaries(read, cancel_request_id);
                let effect_row = request
                    .as_ref()
                    .and_then(|r| r.record.scope.member(member_id))
                    .filter(|m| m.member_kind == CancelKind::EffectIntent)
                    .and_then(|m| self.effect_for_intent(read, &m.member_id));
                Some(CancelPreRead {
                    admission_blockers: Vec::new(),
                    member_links: Vec::new(),
                    request,
                    deliveries,
                    effect_row,
                    root_attempt: None,
                })
            }
            _ => None,
        }
    }

    /// Resolve and decode the committed request a delivery names, through
    /// the write-side working graph so an in-flight advance is seen.
    /// Resolve and decode the committed request a delivery names, through
    /// the SAME working graph the outer submit transaction reads — never a
    /// nested write (the outer txn already holds Selene's writer lock).
    fn request_data(&self, read: &SeleneGraph, request_id: &str) -> Option<RequestData> {
        let node = self.store.cancel_request_node(request_id)?;
        let props = read.node_properties(node)?;
        let version = props.get(&db("version")).and_then(value_u64)?;
        let record = prop_str(props, "record")?;
        serde_json::from_str::<CancelRequest>(&record)
            .ok()
            .map(|record| RequestData {
                node,
                version,
                record,
            })
    }

    /// Existing delivery rows for one request, through the working graph.
    fn delivery_summaries(
        &self,
        read: &SeleneGraph,
        request_id: &str,
    ) -> Vec<super::cancel_rules::DeliverySummary> {
        self.store
            .cancel_delivery_entries()
            .into_iter()
            .filter_map(|(_, node)| {
                let props = read.node_properties(node)?;
                if prop_str(props, "cancel_request_id").as_deref() != Some(request_id) {
                    return None;
                }
                let member_id = prop_str(props, "member_id")?;
                let outcome = prop_str(props, "outcome")?;
                Some(super::cancel_rules::DeliverySummary {
                    member_id,
                    outcome: outcome_from_wire(&outcome)?,
                })
            })
            .collect()
    }

    /// The effect one delivery member names, when it resolves.
    fn effect_for_intent(&self, read: &SeleneGraph, member_id: &str) -> Option<EffectRow> {
        let node = self.store.effect_intent_id_node(read, member_id)?;
        let props = read.node_properties(node)?;
        Some(EffectRow {
            node,
            version: props.get(&db("version")).and_then(value_u64)?,
            record: prop_str(props, "record")?,
        })
    }

    /// `cancel.request`: freeze the genesis scope, verify own-tree
    /// membership (finding 2), and commit the request — NO signal in this
    /// transaction (I10); the durable row precedes every delivery.
    fn validate_cancel_commit(&self, cmd: &Command, pre: &PreRead) -> Result<Accepted, Rejection> {
        self.authority_gate(cmd)?;
        let Method::CancelRequest {
            root_kind,
            root_id,
            reason,
            policy,
            proposed,
        } = &cmd.method
        else {
            unreachable!("routed only from cancel methods")
        };
        let cpre = pre.cancel.as_ref().expect("cancel pre present");
        let scope = CancelScope::freeze(*root_kind, root_id.clone(), proposed.clone())
            .map_err(scope_rejection)?;
        for member in scope.members() {
            let is_root = member.member_kind == *root_kind && member.member_id == *root_id;
            if is_root {
                continue;
            }
            let Some(link) = cpre
                .member_links
                .iter()
                .find(|l| l.member_id == member.member_id)
            else {
                return Err(not_under_root(
                    *root_kind,
                    root_id,
                    member.member_kind,
                    &member.member_id,
                ));
            };
            let ok = match (*root_kind, member.member_kind) {
                (CancelKind::Run, _) => link.run_id.as_deref() == Some(root_id),
                (CancelKind::ExecutionUnit, CancelKind::Attempt | CancelKind::EffectIntent) => {
                    link.parent_unit.as_deref() == Some(root_id)
                }
                (CancelKind::Attempt, CancelKind::EffectIntent) => match &cpre.root_attempt {
                    Some((unit, epoch)) => {
                        link.parent_unit.as_deref() == Some(unit)
                            && link.attempt_epoch == Some(*epoch)
                    }
                    None => false,
                },
                _ => false,
            };
            if !ok {
                return Err(not_under_root(
                    *root_kind,
                    root_id,
                    member.member_kind,
                    &member.member_id,
                ));
            }
        }

        let now = *self.clock_ms.lock().expect("clock");
        let request = CancelRequest {
            cancel_request_id: self.mint_id(),
            reason: *reason,
            policy: *policy,
            scope: scope.clone(),
            status: CancelStatus::Requested,
            requested_at: now,
            requested_by_kind: "daemon".to_owned(),
        };
        let request_id = request.cancel_request_id.to_string();
        let scope_json = serde_json::to_string(&scope).expect("scope serializes");
        let record = serde_json::to_string(&request).expect("request serializes");
        Ok(Accepted {
            plan: Plan::CreateCancelRequest(CancelRequestPlan {
                request_id: request_id.clone(),
                root_kind: kind_wire(*root_kind).to_owned(),
                root_id: root_id.clone(),
                policy: policy_wire(*policy).to_owned(),
                reason: reason_wire(*reason).to_owned(),
                scope: scope_json,
                record,
            }),
            events: vec![EventDraft {
                aggregate_kind: "cancel_request",
                aggregate_id: request_id.clone(),
                aggregate_version: 1,
                event_kind: "cancel_request.requested",
                payload: json!({
                    "root_kind": kind_wire(*root_kind),
                    "root_id": root_id,
                    "reason": reason_wire(*reason),
                    "policy": policy_wire(*policy),
                    "status": "requested",
                    "members": scope.members().len(),
                }),
            }],
            result: json!({
                "cancel_request_id": request_id,
                "status": "requested",
                "root_kind": kind_wire(*root_kind),
                "root_id": root_id,
            }),
        })
    }

    /// `cancel.record_delivery`: the write-once delivery for one pair, in
    /// the frozen scope's leaf-first order. ONE row carries delivered_at +
    /// observed_at (Option) + outcome; `observed_at = None` means
    /// outcome=Unresponsive (terminal but unsettled), never a placeholder
    /// filled later. The request status advances by pure function.
    pub(super) fn validate_record_delivery(
        &self,
        cmd: &Command,
        pre: &PreRead,
    ) -> Result<Accepted, Rejection> {
        self.authority_gate(cmd)?;
        let Method::CancelRecordDelivery {
            cancel_request_id,
            member_id,
            delivered_at,
            observed_at,
            outcome,
        } = &cmd.method
        else {
            unreachable!("routed only from cancel methods")
        };
        let cpre = pre.cancel.as_ref().expect("cancel pre present");
        let Some(request) = &cpre.request else {
            return Err((
                ErrorKind::NotFound,
                format!("cancel_request {cancel_request_id}"),
                false,
            ));
        };
        let Some(member) = request.record.scope.member(member_id) else {
            return Err((
                ErrorKind::InvalidRequest,
                format!("member {member_id} is not in the frozen scope of {cancel_request_id}"),
                true,
            ));
        };
        if cancel_rules::already_delivered(&cpre.deliveries, member_id) {
            return Err((
                ErrorKind::InvalidRequest,
                format!("delivery already recorded for member {member_id} (§5.1 write-once)"),
                true,
            ));
        }
        if !cancel_rules::delivery_leaf_first(&request.record.scope, &cpre.deliveries, member_id) {
            // NOT persisted: once the true next member is delivered, this
            // SAME byte-identical command becomes lawful, and the durable
            // receipt replay must not return a stale rejection without
            // re-validating (the healable class of the effects.rs persist
            // map — unlike the write-once pair above, which cannot heal).
            return Err((
                ErrorKind::InvalidRequest,
                format!(
                    "member {member_id} is not the next undelivered member of {cancel_request_id} (§5.2 rule 3, leaf-first)"
                ),
                false,
            ));
        }

        let mut deliveries = cpre.deliveries.clone();
        deliveries.push(cancel_rules::DeliverySummary {
            member_id: member_id.clone(),
            outcome: *outcome,
        });
        let status = cancel_rules::request_status_after(&request.record.scope, &deliveries);
        let status_wire = status_wire(status).to_owned();
        let mut updated = request.record.clone();
        updated.status = status;

        // §5.3: a delivered member with dispatch evidence and no
        // authoritative outcome parks at `reconciling`, terminal unset.
        let park_effect = cpre
            .effect_row
            .as_ref()
            .map(|row| {
                let mut intent: EffectIntent =
                    serde_json::from_str(&row.record).map_err(|error| {
                        (
                            ErrorKind::Internal,
                            format!("effect record unreadable: {error}"),
                            false,
                        )
                    })?;
                let has_dispatch_evidence = matches!(
                    intent.state,
                    EffectState::Dispatching | EffectState::Dispatched | EffectState::Observing
                );
                if !has_dispatch_evidence || intent.terminal.is_some() {
                    return Ok(None);
                }
                intent.state = EffectState::Reconciling;
                intent.version = row.version.checked_add(1).ok_or_else(|| {
                    (
                        ErrorKind::ResourceExhausted,
                        "effect version space exhausted".into(),
                        true,
                    )
                })?;
                let record = serde_json::to_string(&intent).expect("intent serializes");
                Ok(Some(EffectParkPlan {
                    effect_node: row.node,
                    effect_version: intent.version,
                    effect_record: record,
                }))
            })
            .transpose()?
            .flatten();

        let request_version_after = request.version.checked_add(1).ok_or_else(|| {
            (
                ErrorKind::ResourceExhausted,
                "cancel request version space exhausted".into(),
                true,
            )
        })?;

        let delivery_record = {
            let request_uuid = Uuid7::try_from(cancel_request_id.clone()).map_err(|_| {
                (
                    ErrorKind::Internal,
                    "cancel_request_id is not a minted uuid".into(),
                    false,
                )
            })?;
            serde_json::to_string(&CancelDelivery {
                cancel_request_id: request_uuid,
                member_id: member_id.clone(),
                member_kind: member.member_kind,
                delivered_at: *delivered_at,
                observed_at: *observed_at,
                outcome: *outcome,
            })
            .expect("delivery serializes")
        };

        let plan = Plan::CancelRecordDelivery(CancelDeliveryPlan {
            delivery_key: format!("{cancel_request_id}|{member_id}"),
            request_id: cancel_request_id.clone(),
            request_node: request.node,
            request_version_after,
            request_status_after: status_wire.clone(),
            request_record_after: serde_json::to_string(&updated).expect("record serializes"),
            member_id: member_id.clone(),
            member_kind: kind_wire(member.member_kind).to_owned(),
            outcome: outcome.wire().to_owned(),
            delivery_record,
            park_effect,
        });
        Ok(Accepted {
            plan,
            events: vec![EventDraft {
                aggregate_kind: "cancel_request",
                aggregate_id: cancel_request_id.clone(),
                aggregate_version: request_version_after,
                event_kind: "cancel_request.delivered",
                payload: json!({
                    "member_id": member_id,
                    "delivered_at": delivered_at,
                    "observed_at": observed_at,
                    "outcome": outcome.wire(),
                    "status": status_wire,
                }),
            }],
            result: json!({
                "cancel_request_id": cancel_request_id,
                "member_id": member_id,
                "outcome": outcome.wire(),
                "status": status_wire,
            }),
        })
    }

    /// §5 entry point routed by `validate.rs`.
    pub(super) fn validate_cancel(
        &self,
        cmd: &Command,
        pre: &PreRead,
    ) -> Result<Accepted, Rejection> {
        match &cmd.method {
            Method::CancelRequest { .. } => self.validate_cancel_commit(cmd, pre),
            Method::CancelRecordDelivery { .. } => self.validate_record_delivery(cmd, pre),
            _ => unreachable!("validate routes only cancel methods here"),
        }
    }
}

/// Apply half of `cancel.request`: create the immutable request row.
pub(super) fn apply_create_request(
    m: &mut Mutator<'_, '_>,
    p: &CancelRequestPlan,
) -> Result<Vec<(BookKind, String, NodeId)>, String> {
    let s = |v: &str| Value::String(db(v));
    let node = m
        .create_node(
            LabelSet::single(db("CancelRequest")),
            PropertyMap::from_pairs([
                (db("cancel_request_id"), s(&p.request_id)),
                (db("version"), Value::Uint(1)),
                (db("root_kind"), s(&p.root_kind)),
                (db("root_id"), s(&p.root_id)),
                (db("policy"), s(&p.policy)),
                (db("reason"), s(&p.reason)),
                (db("scope"), s(&p.scope)),
                (db("status"), s("requested")),
                (db("record"), s(&p.record)),
            ])
            .expect("cancel request property map"),
        )
        .map_err(|e| format!("{e:?}"))?;
    Ok(vec![(BookKind::CancelRequest, p.request_id.clone(), node)])
}

/// Apply one `cancel.record_delivery`: the write-once delivery row, the
/// request status advance, and the §5.3 park — one transaction, purely
/// additive (the delivery row is immutable in every property).
pub(super) fn apply_record_delivery(
    m: &mut Mutator<'_, '_>,
    p: &CancelDeliveryPlan,
) -> Result<Vec<(BookKind, String, NodeId)>, String> {
    let s = |v: &str| Value::String(db(v));
    let mut books = Vec::new();
    let delivery = m
        .create_node(
            LabelSet::single(db("CancelDelivery")),
            PropertyMap::from_pairs([
                (db("delivery_key"), s(&p.delivery_key)),
                (db("cancel_request_id"), s(&p.request_id)),
                (db("member_id"), s(&p.member_id)),
                (db("member_kind"), s(&p.member_kind)),
                (db("outcome"), s(&p.outcome)),
                (db("record"), s(&p.delivery_record)),
            ])
            .expect("cancel delivery property map"),
        )
        .map_err(|e| format!("{e:?}"))?;
    books.push((BookKind::CancelDelivery, p.delivery_key.clone(), delivery));
    m.update_node(
        p.request_node,
        no_labels(),
        props_set([
            (db("version"), Value::Uint(p.request_version_after)),
            (db("status"), s(&p.request_status_after)),
            (db("record"), s(&p.request_record_after)),
        ]),
    )
    .map_err(|e| format!("{e:?}"))?;
    if let Some(park) = &p.park_effect {
        m.update_node(
            park.effect_node,
            no_labels(),
            props_set([
                (db("version"), Value::Uint(park.effect_version)),
                (db("state"), s("reconciling")),
                (db("record"), s(&park.effect_record)),
            ]),
        )
        .map_err(|e| format!("{e:?}"))?;
    }
    Ok(books)
}
