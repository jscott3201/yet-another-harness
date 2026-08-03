//! Pure §4 legality and classification rules, separated from the funnel
//! arms so each rule can be read against its ADR clause without the
//! transaction plumbing around it.
//!
//! The governing principle: the ledger does not trust the caller's
//! conclusions, only its observations. Callers contribute digests, wait
//! status, and query answers; every `TargetState` and every terminal
//! legality decision is computed here from the prepare-time declaration.

use super::*;
use crate::effect::{EffectState, ReversibilityClass, TargetEnumeration, TargetState};
use std::collections::{BTreeMap, BTreeSet};

/// Evidence the settling authority carries from outside the graph. Kept
/// separate from the target rows because these are statements about the
/// operation as a whole, not about one target.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SettleEvidence {
    /// §4.3 post-hoc input: the process reported a clean wait status. A
    /// dirty status classifies every observed target `unknown`, because a
    /// failed process may have written anything.
    pub clean_wait_status: bool,
    /// §5.3's SECOND disjunct: the target authoritatively reported
    /// cancellation. Without it, a dispatched effect may not settle
    /// `cancelled` — a cancel request must never convert an uncertain
    /// effect into a proven one (I7).
    pub target_reported_cancellation: bool,
}

/// §4.1 list constraints, enforced at every write that carries targets.
/// Order is normalized server-side rather than rejected — sortedness is a
/// storage property of the record, not a caller obligation.
pub(super) fn check_target_list(targets: &[TargetObservation]) -> Result<(), Rejection> {
    if targets.len() > 1024 {
        return Err((
            ErrorKind::InvalidRequest,
            format!("{} target rows exceed the 1024 cap", targets.len()),
            true,
        ));
    }
    let mut seen = BTreeSet::new();
    for t in targets {
        if t.target_id.len() > 256 {
            return Err((
                ErrorKind::InvalidRequest,
                format!(
                    "target_id of {} bytes exceeds the 256 cap",
                    t.target_id.len()
                ),
                true,
            ));
        }
        if !seen.insert(&t.target_id) {
            return Err((
                ErrorKind::InvalidRequest,
                format!("duplicate target_id {}", t.target_id),
                true,
            ));
        }
    }
    Ok(())
}

pub(super) fn sorted_by_target_id(mut targets: Vec<TargetObservation>) -> Vec<TargetObservation> {
    targets.sort_by(|a, b| a.target_id.cmp(&b.target_id));
    targets
}

/// Prepare-time normalization: a declared row begins unobserved. Whatever
/// state the caller put on it is discarded — no evidence exists yet.
pub(super) fn declared_at_prepare(targets: &[TargetObservation]) -> Vec<TargetObservation> {
    sorted_by_target_id(
        targets
            .iter()
            .cloned()
            .map(|mut t| {
                t.observed_digest = None;
                t.observed_at = None;
                t.state = TargetState::Unknown;
                t
            })
            .collect(),
    )
}

/// §4.3 declared-target settlement: the prepare-time identity and digests
/// are immutable, the caller contributes only `observed_digest` and
/// `observed_at`, states are recomputed by digest comparison, and a
/// declared target with no observation stays `unknown` — so omitting rows
/// can never manufacture a success.
pub(super) fn merge_declared(
    declared: &[TargetObservation],
    submitted: &[TargetObservation],
) -> Result<Vec<TargetObservation>, Rejection> {
    let by_id: BTreeMap<&str, &TargetObservation> = submitted
        .iter()
        .map(|t| (t.target_id.as_str(), t))
        .collect();
    for t in submitted {
        if !declared.iter().any(|d| d.target_id == t.target_id) {
            return Err((
                ErrorKind::InvalidRequest,
                format!("settle names undeclared target {}", t.target_id),
                true,
            ));
        }
    }
    Ok(declared
        .iter()
        .map(|d| {
            let mut row = d.clone();
            match by_id.get(d.target_id.as_str()) {
                Some(obs) => {
                    row.observed_digest = obs.observed_digest.clone();
                    row.observed_at = obs.observed_at;
                }
                None => {
                    row.observed_digest = None;
                    row.observed_at = None;
                }
            }
            row.classify_declared();
            row
        })
        .collect())
}

/// §4.3 post-hoc settlement: there is no expected digest to compare, so a
/// target classifies `applied` only when the daemon observed the write AND
/// the process reported a clean wait status; a dirty status or a gap in
/// observation classifies `unknown`. The caller's asserted state is never
/// consulted.
pub(super) fn classify_post_hoc(
    submitted: &[TargetObservation],
    evidence: SettleEvidence,
) -> Vec<TargetObservation> {
    sorted_by_target_id(
        submitted
            .iter()
            .cloned()
            .map(|mut t| {
                t.expected_digest = None;
                t.state = if evidence.clean_wait_status && t.observed_digest.is_some() {
                    TargetState::Applied
                } else {
                    TargetState::Unknown
                };
                t
            })
            .collect(),
    )
}

/// Which current state may settle which terminal, per §4.2's lifecycle
/// order, §5.2 rule 4, §5.3's two disjuncts, and R20's close-selection
/// amendment. `Settled` never reaches here: write-once rejects first.
///
/// Returns the reason a transition is unlawful, or `None` when it is
/// permitted — the reason is diagnostic only; callers branch on the kind.
pub(super) fn settle_legality(
    intent: &EffectIntent,
    terminal: EffectTerminal,
    evidence: SettleEvidence,
) -> Option<String> {
    use EffectState::*;
    let state = intent.state;
    // Dispatch evidence exists from the moment `prepared -> dispatching`
    // commits: §3.3 row 5 makes that commit the authorization for the
    // adapter to act, so anything at or past it may have touched the world.
    let has_dispatch_evidence = matches!(
        state,
        Dispatching | Dispatched | Observing | Reconciling | Compensating
    );
    match terminal {
        EffectTerminal::Succeeded | EffectTerminal::Failed => {
            if has_dispatch_evidence {
                None
            } else {
                Some(format!(
                    "{terminal:?} needs dispatch evidence; state is {state:?} (§9 step 6 forbids synthetic outcomes)"
                ))
            }
        }
        EffectTerminal::Cancelled => {
            // §5.3 disjunct 1: proved never dispatched. Only `prepared`
            // proves it — §5.2 rule 4 groups `dispatching` with dispatched.
            if state == Prepared {
                return None;
            }
            // §5.3 disjunct 2: the target authoritatively reported it.
            if has_dispatch_evidence && evidence.target_reported_cancellation {
                return None;
            }
            Some(format!(
                "cancelled from {state:?} requires an authoritative target-reported cancellation (§5.3); \
                 a cancel request MUST NOT convert an uncertain effect into cancelled (I7)"
            ))
        }
        EffectTerminal::Compensated => {
            if !has_dispatch_evidence {
                return Some(format!(
                    "compensated from {state:?}: nothing was dispatched to compensate"
                ));
            }
            if intent.reversibility_class == ReversibilityClass::Irreversible {
                return Some(
                    "compensated is unavailable for an irreversible effect (§5.3 R20); \
                     an unproven outcome surfaces to the owner for accept-unknown"
                        .into(),
                );
            }
            if intent.compensation_intent_id.is_none() {
                return Some(
                    "compensated requires a registered compensation intent (§4.2 step 5)".into(),
                );
            }
            None
        }
        EffectTerminal::Unknown => {
            // An intent that never left `prepared` has a KNOWN outcome:
            // I8 forbids the adapter acting without the dispatch commit.
            if has_dispatch_evidence {
                None
            } else {
                Some(format!(
                    "unknown from {state:?}: I8 proves no adapter acted, so the outcome is not uncertain"
                ))
            }
        }
    }
}

/// Settle-time target classification, dispatched on the PREPARE-TIME
/// declaration rather than on whether rows happen to be present.
pub(super) fn classify_settle_targets(
    intent: &EffectIntent,
    submitted: &[TargetObservation],
    evidence: SettleEvidence,
) -> Result<Vec<TargetObservation>, Rejection> {
    match intent.target_enumeration {
        TargetEnumeration::Declared => merge_declared(&intent.targets, submitted),
        TargetEnumeration::PostHoc => Ok(classify_post_hoc(submitted, evidence)),
    }
}

pub(super) fn terminal_wire(t: EffectTerminal) -> &'static str {
    match t {
        EffectTerminal::Succeeded => "succeeded",
        EffectTerminal::Failed => "failed",
        EffectTerminal::Cancelled => "cancelled",
        EffectTerminal::Compensated => "compensated",
        EffectTerminal::Unknown => "unknown",
    }
}

pub(super) fn state_wire(s: EffectState) -> &'static str {
    match s {
        EffectState::Prepared => "prepared",
        EffectState::Dispatching => "dispatching",
        EffectState::Dispatched => "dispatched",
        EffectState::Observing => "observing",
        EffectState::Reconciling => "reconciling",
        EffectState::Compensating => "compensating",
        EffectState::Settled => "settled",
    }
}
