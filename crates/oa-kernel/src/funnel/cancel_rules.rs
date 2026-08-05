//! Pure §5 predicates, split from the funnel arms so each rule can be read
//! against its ADR-001 §5 clause without the transaction plumbing — the same
//! split `effect_rules` uses for §4.
//!
//! What is pure here: request status advancement, delivery ordering, and the
//! shape of a rule-4 admission blocker. What is deliberately NOT here:
//! `CancelRequest::governs` — that predicate is scope-membership based and
//! answers "which EXISTING member inherits", which is the wrong question for
//! rule 4's "may a NEW child be admitted" (a child is never in the frozen
//! scope), so [`admission_blockers`] scans the committed requests' root
//! lineage instead.

use crate::cancel::{CancelKind, CancelScope, CancelStatus, DeliveryOutcome, ScopeMember};
use crate::store::{Store, db, value_str};
use selene_graph::SeleneGraph;

/// Why one committed cancellation forbids an admission or dispatch. Same
/// shape as [`super::run_rules::Blocker`], so I11-style join formatting is
/// shared.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct Blocker {
    pub member: String,
    pub reason: String,
}

/// One recorded delivery, as settlement and legality read it. `delivered_at`
/// and `observed_at` do not affect lifecycle advance — a row exists or it
/// does not — so they are omitted; the outcome is what decides discharge.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct DeliverySummary {
    pub member_id: String,
    pub outcome: DeliveryOutcome,
}

/// The scope member that must next receive a delivery row, under §5.2
/// rule 3: the undelivered member with the smallest `order_index` (leaf
/// first). `None` once every member has a row.
pub(super) fn next_undelivered<'s>(
    scope: &'s CancelScope,
    deliveries: &[DeliverySummary],
) -> Option<&'s ScopeMember> {
    scope
        .members()
        .iter()
        .filter(|m| !deliveries.iter().any(|d| d.member_id == m.member_id))
        .min_by_key(|m| m.order_index)
}

/// §5.2 rule 3's leaf-first gate, as a filter matter of fact: `member_id`
/// may receive its delivery now only when it is the next undelivered member.
/// The write-once pair is checked by the caller.
pub(super) fn delivery_leaf_first(
    scope: &CancelScope,
    deliveries: &[DeliverySummary],
    member_id: &str,
) -> bool {
    next_undelivered(scope, deliveries).is_some_and(|next| next.member_id == member_id)
}

/// §5.1 status advance, a pure function of the frozen scope and the rows so
/// far: `requested -> delivering -> observed_partial -> settled`.
///
/// - `Requested`: nothing delivered yet.
/// - `Delivering`: at least one member signalled, at least one not —
///   transports still in flight.
/// - `ObservedPartial`: every member has a row, but at least one outcome is
///   not discharged (an `Unresponsive` member lands here and stays: the
///   honest terminal-but-unsettled shape, §5.1).
/// - `Settled`: every scope member has a discharged delivery row. The
///   request's policy and its members' attachments play NO role in
///   settlement — the frozen scope is the record of what the tree looked
///   like, and rule 7 settles only what was frozen.
pub(super) fn request_status_after(
    scope: &CancelScope,
    deliveries: &[DeliverySummary],
) -> CancelStatus {
    if deliveries.is_empty() {
        return CancelStatus::Requested;
    }
    let every_discharged = scope.members().iter().all(|m| {
        deliveries
            .iter()
            .any(|d| d.member_id == m.member_id && d.outcome.is_discharged())
    });
    if every_discharged {
        CancelStatus::Settled
    } else if scope
        .members()
        .iter()
        .all(|m| deliveries.iter().any(|d| d.member_id == m.member_id))
    {
        CancelStatus::ObservedPartial
    } else {
        CancelStatus::Delivering
    }
}

/// Whether the pair (request, member) already has a delivery row — the
/// write-once half of §5.1's `UNIQUE(cancel_request_id, member_id)`.
pub(super) fn already_delivered(deliveries: &[DeliverySummary], member_id: &str) -> bool {
    deliveries.iter().any(|d| d.member_id == member_id)
}

/// Whether a committed cancel request rooted at `run_id` matters for a
/// child being admitted under that run. Every committed request whose root
/// sits in the child's ancestor lineage bars admission, under BOTH policies
/// (owner's literal rule-4 ruling; a `root_only` cancel of an ancestor no
/// less closes the tree to new children).
fn root_in_lineage(root_kind: CancelKind, root_id: &str, lineage: &[(CancelKind, String)]) -> bool {
    lineage
        .iter()
        .any(|(kind, id)| *kind == root_kind && id == root_id)
}

/// §5.2 rule 4's admission gate: every committed cancellation request whose
/// root lies in `child_lineage` (the would-be child's ancestors, root-first)
/// forbids admitting `child_kind`. `child_lineage` is supplied by the caller
/// so the same predicate serves the unit, attempt, and effect admission
/// points — the effect one feeds the A14 dispatch race, which is why it is
/// the single implementation both validate paths share.
///
/// Reads run through the open write transaction's working graph, never the
/// published snapshot (the G02 trap [`super::run_rules`] documents).
pub(super) fn admission_blockers(
    graph: &SeleneGraph,
    store: &Store,
    child_kind: CancelKind,
    child_lineage: &[(CancelKind, String)],
) -> Vec<Blocker> {
    let mut blockers = Vec::new();
    for node in store.cancel_request_nodes() {
        let Some(props) = graph.node_properties(node) else {
            continue;
        };
        let Some(root_kind) = props.get(&db("root_kind")).and_then(value_str) else {
            continue;
        };
        let Some(root_id) = props.get(&db("root_id")).and_then(value_str) else {
            continue;
        };
        let Some(kind) = kind_from_wire(&root_kind) else {
            continue;
        };
        if root_in_lineage(kind, &root_id, child_lineage) {
            blockers.push(Blocker {
                member: format!("{root_kind}:{root_id}"),
                reason: format!(
                    "rule-4: committed cancellation of {root_kind}:{root_id} bars admitting {child_kind:?} below it"
                ),
            });
        }
    }
    blockers
}

pub(super) fn kind_from_wire(wire: &str) -> Option<CancelKind> {
    match wire {
        "run" => Some(CancelKind::Run),
        "execution_unit" => Some(CancelKind::ExecutionUnit),
        "attempt" => Some(CancelKind::Attempt),
        "effect_intent" => Some(CancelKind::EffectIntent),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cancel::{Attachment, MemberInput};
    use crate::ids::Uuid7;

    fn id(n: u64) -> String {
        Uuid7::mint(1_700_000_000_000, n as u128).to_string()
    }

    fn member(kind: CancelKind, n: u64) -> MemberInput {
        MemberInput {
            member_kind: kind,
            member_id: id(n),
            attachment: Attachment::Attached,
        }
    }

    fn scope(proposed: Vec<MemberInput>) -> CancelScope {
        CancelScope::freeze(CancelKind::Run, id(1), proposed).expect("scope freezes")
    }

    fn summary(member_id: &str, outcome: DeliveryOutcome) -> DeliverySummary {
        DeliverySummary {
            member_id: member_id.to_owned(),
            outcome,
        }
    }

    #[test]
    fn status_advances_requested_to_delivering_to_settled() {
        // Two members: effect first (leaf), then attempt. No rows -> still
        // requested; one row -> delivering; all discharged -> settled.
        let s = scope(vec![
            member(CancelKind::EffectIntent, 40),
            member(CancelKind::Attempt, 30),
        ]);
        assert_eq!(request_status_after(&s, &[]), CancelStatus::Requested);
        assert_eq!(
            request_status_after(&s, &[summary(&id(40), DeliveryOutcome::ObservedStopped)]),
            CancelStatus::Delivering
        );
        assert_eq!(
            request_status_after(
                &s,
                &[
                    summary(&id(40), DeliveryOutcome::ObservedStopped),
                    summary(&id(30), DeliveryOutcome::ObservedStopped),
                ]
            ),
            CancelStatus::Settled
        );
    }

    #[test]
    fn unresponsive_is_terminal_but_never_settles() {
        // §5.1: an unresponsive member is a recorded observation, not a
        // placeholder — but it does NOT discharge (validation/07:95-96).
        let s = scope(vec![member(CancelKind::EffectIntent, 40)]);
        assert_eq!(
            request_status_after(&s, &[summary(&id(40), DeliveryOutcome::Unresponsive)]),
            CancelStatus::ObservedPartial,
            "delivered but undischarged stays observed_partial, never settled"
        );
    }

    #[test]
    fn all_members_delivered_with_an_undischarged_row_stays_observed_partial() {
        // All rows present, one undischarged -> observed_partial, not
        // delivering (nothing is left to deliver) and not settled.
        let s = scope(vec![member(CancelKind::EffectIntent, 40)]);
        assert_eq!(
            request_status_after(&s, &[summary(&id(40), DeliveryOutcome::Unresponsive)]),
            CancelStatus::ObservedPartial
        );
        assert_eq!(
            request_status_after(&s, &[summary(&id(40), DeliveryOutcome::AlreadyTerminal)]),
            CancelStatus::Settled,
            "a discharged outcome settles even when it arrived for the only member"
        );
    }

    #[test]
    fn root_and_cascade_scopes_settle_alike() {
        // The settle definition does NOT depend on the policy the request
        // was created with: both a root_only and an attached_cascade scope
        // settle only when every frozen member discharges. The policy axis
        // decides who INHERITS, rule 7 decides what settles.
        let root_only = CancelScope::freeze(
            CancelKind::Run,
            id(1),
            vec![member(CancelKind::ExecutionUnit, 20)],
        )
        .expect("freezes");
        assert_eq!(
            request_status_after(
                &root_only,
                &[summary(&id(20), DeliveryOutcome::ObservedStopped)]
            ),
            CancelStatus::Settled
        );
    }

    #[test]
    fn leaf_first_picks_the_undelivered_minimum() {
        let s = scope(vec![
            member(CancelKind::EffectIntent, 40),
            member(CancelKind::Attempt, 30),
        ]);
        // EffectIntent (order 0) must deliver before Attempt (order 1).
        assert!(delivery_leaf_first(&s, &[], &id(40)));
        assert!(!delivery_leaf_first(&s, &[], &id(30)));
        // Once the leaf is delivered, the attempt is next.
        assert!(delivery_leaf_first(
            &s,
            &[summary(&id(40), DeliveryOutcome::ObservedStopped)],
            &id(30)
        ));
        assert!(!delivery_leaf_first(
            &s,
            &[summary(&id(40), DeliveryOutcome::ObservedStopped)],
            &id(40)
        ));
    }

    #[test]
    fn the_write_once_pair_identity_is_member_scoped() {
        assert!(!already_delivered(&[], &id(1)));
        let rows = vec![summary(&id(1), DeliveryOutcome::ObservedStopped)];
        assert!(already_delivered(&rows, &id(1)));
        assert!(!already_delivered(&rows, &id(2)));
    }

    #[test]
    fn root_lineage_matching_ignores_policy_axis() {
        // The lineage matcher is policy-blind by construction: both a
        // root_only and an attached_cascade request at the same root block
        // the SAME new-child admission (owner's literal rule-4 ruling).
        let lineage = vec![
            (CancelKind::Run, id(1).clone()),
            (CancelKind::ExecutionUnit, id(20).clone()),
        ];
        assert!(root_in_lineage(CancelKind::Run, &id(1), &lineage));
        assert!(root_in_lineage(
            CancelKind::ExecutionUnit,
            &id(20),
            &lineage
        ));
        // A sibling run does not match.
        assert!(!root_in_lineage(CancelKind::Run, &id(2), &lineage));
        // A request rooted at the child itself (a unit not yet existing) is
        // not an ancestor of the child and does not match.
        assert!(!root_in_lineage(
            CancelKind::ExecutionUnit,
            &id(99),
            &lineage
        ));
    }
}
