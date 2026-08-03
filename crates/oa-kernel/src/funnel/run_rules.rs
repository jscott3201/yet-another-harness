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

use crate::effect::{EffectState, EffectTerminal};
use crate::store::{Store, db, value_str};
use selene_graph::SeleneGraph;

/// Why one member forbids a `closed_success`.
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
    blockers
}

/// The per-effect half of I11, split out so its truth table is testable
/// without a store. `None` means this effect does not block a success close.
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
