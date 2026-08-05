//! I11's close predicate (ADR-001 §5.2 rule 7): a run MUST NOT close
//! `success` while any required child, review, integration, approval, or
//! external effect is active, unknown, or uncompensated.
//!
//! MILE-001's kernel models the effect arm of that list; reviews,
//! integrations and approvals arrive with §8. The predicate is written so
//! adding them is adding a scan, not restating the rule — every blocker
//! returns a caller-readable reason, because "run cannot close" with no
//! named member is the kind of rejection that gets debugged by guessing.
//!
//! The scan reads through the open write transaction's working graph, never
//! the published snapshot: a snapshot can lag a sealed commit, and a stale
//! read here would let a run close success over an effect that had just
//! been dispatched.

use crate::cancel::{CancelKind, CancelScope, DeliveryOutcome};
use crate::effect::{EffectState, EffectTerminal};
use crate::store::{Store, db, value_str};
use selene_graph::SeleneGraph;

/// Why one member forbids a closed_success.
pub(super) struct Blocker {
    pub member: String,
    pub reason: String,
}

/// Members of `run_id` that forbid closing it `success`. Empty means the
/// close is lawful.
pub(super) fn success_blockers(graph: &SeleneGraph, store: &Store, run_id: &str) -> Vec<Blocker> {
    let units: Vec<String> = store
        .unit_entries()
        .into_iter()
        .filter(|(_, node)| {
            graph
                .node_properties(*node)
                .and_then(|p| p.get(&db("run_id")).and_then(value_str))
                .is_some_and(|r| r == run_id)
        })
        .map(|(unit_id, _)| unit_id)
        .collect();

    let mut blockers = Vec::new();
    for (operation_key, node) in store.effect_entries() {
        let Some(props) = graph.node_properties(node) else {
            continue;
        };
        let owner = props.get(&db("unit_id")).and_then(value_str);
        if !owner.is_some_and(|u| units.contains(&u)) {
            continue;
        }
        let state = props.get(&db("state")).and_then(value_str);
        let terminal = props.get(&db("terminal")).and_then(value_str);
        if let Some(reason) = effect_blocks(state.as_deref(), terminal.as_deref()) {
            blockers.push(Blocker {
                member: operation_key,
                reason,
            });
        }
    }
    // §5.2 rule 7 / I11 (finding 3): a committed cancellation rooted in
    // this run forbids a success close while ANY scope member has no
    // discharged delivery row. The obligation-4 shape: a run whose effects
    // reconcile under a cancel must stay open until the request settles.
    blockers.extend(cancel_blocks(graph, store, run_id));
    blockers
}

/// The cancellation-rooted half of I11. A request rooted at `run_id` — or
/// at a unit/attempt/effect inside it — resolves upward to this run, and a
/// request status other than `settled` means at least one member is not
/// discharged.
fn cancel_blocks(graph: &SeleneGraph, store: &Store, run_id: &str) -> Vec<Blocker> {
    // Delivered member rows per request, from the SAME working graph the
    // close is validated against (no snapshot lag — the run and the
    // delivery advance in one command stream behind the funnel gate).
    let delivered_by_request: std::collections::HashMap<String, Vec<(String, bool)>> = store
        .cancel_delivery_entries()
        .into_iter()
        .filter_map(|(_, node)| {
            let props = graph.node_properties(node)?;
            let request_id = props.get(&db("cancel_request_id")).and_then(value_str)?;
            let member_id = props.get(&db("member_id")).and_then(value_str)?;
            let outcome = props.get(&db("outcome")).and_then(value_str)?;
            let discharged = outcome_from_wire(&outcome).is_some_and(|o| o.is_discharged());
            Some((request_id, (member_id, discharged)))
        })
        .fold(std::collections::HashMap::new(), |mut acc, (req, row)| {
            acc.entry(req).or_default().push(row);
            acc
        });

    let mut blockers = Vec::new();
    for node in store.cancel_request_nodes() {
        let Some(props) = graph.node_properties(node) else {
            continue;
        };
        let Some(request_id) = props.get(&db("cancel_request_id")).and_then(value_str) else {
            continue;
        };
        let Some(root_kind) = props.get(&db("root_kind")).and_then(value_str) else {
            continue;
        };
        let Some(root_id) = props.get(&db("root_id")).and_then(value_str) else {
            continue;
        };
        let Some(scope_json) = props.get(&db("scope")).and_then(value_str) else {
            continue;
        };
        let Some(status) = props.get(&db("status")).and_then(value_str) else {
            continue;
        };
        if status == "settled" {
            continue;
        }
        let Some(root) = kind_from_wire(&root_kind) else {
            continue;
        };
        let Some(my_run) = root_run(graph, store, root, &root_id) else {
            continue;
        };
        if my_run != run_id {
            continue;
        }
        let Ok(scope) = serde_json::from_str::<CancelScope>(&scope_json) else {
            continue;
        };
        let Some(missing) = scope.members().iter().find(|m| {
            !delivered_by_request.get(&request_id).is_some_and(|v| {
                v.iter()
                    .any(|(member, discharged)| member == &m.member_id && *discharged)
            })
        }) else {
            continue;
        };
        blockers.push(Blocker {
            member: format!("cancel_request {request_id} member {}", missing.member_id),
            reason: format!(
                "committed cancellation of {root_kind}:{root_id} not settled (status {status})"
            ),
        });
    }
    blockers
}

/// `(root_kind, root_id)` -> the run that owns the root. Runs own
/// themselves; every other kind resolves up through its unit — finding 3's
/// "rooted run-or-self" lineage.
fn root_run(
    graph: &SeleneGraph,
    store: &Store,
    root_kind: CancelKind,
    root_id: &str,
) -> Option<String> {
    match root_kind {
        CancelKind::Run => Some(root_id.to_owned()),
        CancelKind::ExecutionUnit => {
            let node = store.unit_node(root_id)?;
            graph
                .node_properties(node)
                .and_then(|p| p.get(&db("run_id")).and_then(value_str))
        }
        CancelKind::Attempt => {
            let node = store.attempt_id_node(graph, root_id)?;
            let unit = graph
                .node_properties(node)
                .and_then(|p| p.get(&db("unit_id")).and_then(value_str))?;
            let unit_node = store.unit_node(&unit)?;
            graph
                .node_properties(unit_node)
                .and_then(|p| p.get(&db("run_id")).and_then(value_str))
        }
        CancelKind::EffectIntent => {
            let node = store.effect_intent_id_node(graph, root_id)?;
            let unit = graph
                .node_properties(node)
                .and_then(|p| p.get(&db("unit_id")).and_then(value_str))?;
            let unit_node = store.unit_node(&unit)?;
            graph
                .node_properties(unit_node)
                .and_then(|p| p.get(&db("run_id")).and_then(value_str))
        }
    }
}

fn kind_from_wire(wire: &str) -> Option<CancelKind> {
    match wire {
        "run" => Some(CancelKind::Run),
        "execution_unit" => Some(CancelKind::ExecutionUnit),
        "attempt" => Some(CancelKind::Attempt),
        "effect_intent" => Some(CancelKind::EffectIntent),
        _ => None,
    }
}

fn outcome_from_wire(wire: &str) -> Option<DeliveryOutcome> {
    match wire {
        "observed_stopped" => Some(DeliveryOutcome::ObservedStopped),
        "unresponsive" => Some(DeliveryOutcome::Unresponsive),
        "already_terminal" => Some(DeliveryOutcome::AlreadyTerminal),
        "detached_declined" => Some(DeliveryOutcome::DetachedDeclined),
        _ => None,
    }
}

/// The per-effect half of I11, mirroring the effect state truth table.
/// `None` means this effect does not block a success close.
///
/// An unreadable state blocks: I11 is a safety bar, and a member whose state
/// cannot be determined is exactly the "unknown" case the rule names.
fn effect_blocks(state: Option<&str>, terminal: Option<&str>) -> Option<String> {
    let settled = wire(EffectState::Settled);
    match state {
        Some(s) if s == settled => match terminal {
            // Settled `unknown` is I11's "unknown" arm verbatim: the effect
            // is no longer in flight but its outcome was never proved, so a
            // run closing success over it would be asserting an outcome
            // nobody observed. §5.3's lawful close for this is an explicit
            // owner accept-unknown, which is a §11.2.6 escalation, not a
            // silent pass here.
            Some(t) if t == wire_terminal(EffectTerminal::Unknown) => {
                Some("settled with terminal `unknown` — needs an explicit accept-unknown".into())
            }
            Some(_) => None,
            None => Some("settled with no terminal recorded".into()),
        },
        Some(other) => Some(format!("state `{other}` is not settled")),
        None => Some("state unreadable".into()),
    }
}

fn wire(state: EffectState) -> &'static str {
    match state {
        EffectState::Prepared => "prepared",
        EffectState::Dispatching => "dispatching",
        EffectState::Dispatched => "dispatched",
        EffectState::Observing => "observing",
        EffectState::Reconciling => "reconciling",
        EffectState::Compensating => "compensating",
        EffectState::Settled => "settled",
    }
}

fn wire_terminal(t: EffectTerminal) -> &'static str {
    match t {
        EffectTerminal::Succeeded => "succeeded",
        EffectTerminal::Failed => "failed",
        EffectTerminal::Cancelled => "cancelled",
        EffectTerminal::Compensated => "compensated",
        EffectTerminal::Unknown => "unknown",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_reconciling_effect_blocks_a_success_close() {
        // §5.3's parking state is precisely where a cancelled-but-dispatched
        // effect waits, and obligation 4 requires the enclosing run stay open
        // while it does.
        assert!(effect_blocks(Some("reconciling"), None).is_some());
        assert!(effect_blocks(Some("dispatched"), None).is_some());
        assert!(effect_blocks(Some("prepared"), None).is_some());
    }

    #[test]
    fn a_settled_known_outcome_does_not_block() {
        for t in ["succeeded", "failed", "cancelled", "compensated"] {
            assert!(
                effect_blocks(Some("settled"), Some(t)).is_none(),
                "settled/{t} should not block"
            );
        }
    }

    #[test]
    fn a_settled_unknown_still_blocks() {
        assert!(
            effect_blocks(Some("settled"), Some("unknown")).is_some(),
            "I11 names `unknown` alongside `active` — reaching a terminal is \
             not the same as having proved an outcome"
        );
    }

    #[test]
    fn an_unreadable_state_blocks() {
        assert!(effect_blocks(None, None).is_some());
        assert!(effect_blocks(Some("settled"), None).is_some());
    }
}
