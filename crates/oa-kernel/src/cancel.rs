//! Cancellation records (ADR-001 §5.1) and the two rules that are pure
//! functions of a scope: leaf-first ordering and settlement.
//!
//! The protocol itself — durable-before-signal, the frozen snapshot, the
//! no-admission-after-cancel gate, and §5.3's reconciling path — lives in
//! the funnel, because every one of those is a transaction-time decision.
//! What lives here is what can be decided from a scope alone, so the funnel
//! and the §9 recovery scan compute it one way (the same split
//! [`crate::effect`] uses for §4.3 classification).
//!
//! The one thing this module deliberately does NOT offer is a constructor
//! that takes an `order_index` from a caller. Leaf-first order is what makes
//! delivery safe — a parent signalled before its children can strand a
//! descendant nobody is still watching — so the ordering is computed from
//! the ownership tree in [`CancelScope::freeze`] and the field is not
//! settable from outside.

use crate::ids::Uuid7;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

/// §5.1's cap on a frozen scope. A cancellation spanning more members than
/// this is a decomposition failure upstream, not a scope to truncate:
/// silently dropping members would produce exactly the unrecorded-state
/// outcome MILE-001 obligation 4 forbids.
pub const MAX_SCOPE_MEMBERS: usize = 4096;

/// The §1.2 cancellation-ownership tree, root-to-leaf. Discriminant order is
/// the tree depth and [`CancelKind::depth`] depends on it, so reordering
/// these variants changes delivery order — keep them root-first.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CancelKind {
    Run,
    ExecutionUnit,
    Attempt,
    EffectIntent,
}

impl CancelKind {
    /// Depth in the ownership tree; larger is closer to a leaf.
    pub fn depth(self) -> u8 {
        match self {
            CancelKind::Run => 0,
            CancelKind::ExecutionUnit => 1,
            CancelKind::Attempt => 2,
            CancelKind::EffectIntent => 3,
        }
    }

    /// Whether `self` can own a member of kind `other`, directly or
    /// transitively. Strict: a kind does not contain itself, so a `root_only`
    /// cancellation of a unit never reaches a second unit.
    pub fn contains(self, other: CancelKind) -> bool {
        self.depth() < other.depth()
    }

    pub fn wire(self) -> &'static str {
        match self {
            CancelKind::Run => "run",
            CancelKind::ExecutionUnit => "execution_unit",
            CancelKind::Attempt => "attempt",
            CancelKind::EffectIntent => "effect_intent",
        }
    }
}

/// §5.1 reasons. These are not interchangeable labels: `SupersededByEpoch`
/// and `ShutdownDrain` are daemon-internal (§3.6 delivers fence loss as
/// cancellation), while `OwnerRequest` traces to a human principal.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CancelReason {
    OwnerRequest,
    BudgetExhausted,
    PolicyViolation,
    SupersededByEpoch,
    DependencyFailed,
    ShutdownDrain,
}

/// §5.2 rule 5's parent-close policy.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CancelPolicy {
    /// Attached descendants inherit the cancellation.
    AttachedCascade,
    /// Only the named root; descendants keep running under their own owners.
    RootOnly,
}

/// §5.2 rule 5. There is no implicit fire-and-forget: a detached member is an
/// explicit record with its own owner, which is why declining is a recordable
/// delivery outcome rather than silence.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Attachment {
    Attached,
    Detached,
}

/// §5.1 request lifecycle.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CancelStatus {
    Requested,
    Delivering,
    ObservedPartial,
    Settled,
}

/// §5.1 per-member delivery outcome. `Unresponsive` is the honest one: a
/// signal is not proof of death (§5.3, `validation/07:95-96`), so an
/// unresponsive member leaves the request unsettled rather than closing it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeliveryOutcome {
    ObservedStopped,
    Unresponsive,
    AlreadyTerminal,
    DetachedDeclined,
}

impl DeliveryOutcome {
    /// Whether this outcome discharges its member for §5.2 rule 7. An
    /// `Unresponsive` member does not: that is the whole point of recording
    /// it separately from `ObservedStopped`.
    pub fn is_discharged(self) -> bool {
        match self {
            DeliveryOutcome::ObservedStopped
            | DeliveryOutcome::AlreadyTerminal
            | DeliveryOutcome::DetachedDeclined => true,
            DeliveryOutcome::Unresponsive => false,
        }
    }

    pub fn wire(self) -> &'static str {
        match self {
            DeliveryOutcome::ObservedStopped => "observed_stopped",
            DeliveryOutcome::Unresponsive => "unresponsive",
            DeliveryOutcome::AlreadyTerminal => "already_terminal",
            DeliveryOutcome::DetachedDeclined => "detached_declined",
        }
    }
}

/// One frozen scope member. `order_index` is assigned by
/// [`CancelScope::freeze`] and is not caller-settable — see the module note.
///
/// `member_id` is an opaque identifier string, not a parsed `Uuid7`: ADR-001
/// §1 fixes ids as ADR-002 P3.3 opaque strings with `Uuid7` naming only the
/// minting convention, and the funnel addresses units, attempts and effects
/// by that string everywhere else.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScopeMember {
    pub member_kind: CancelKind,
    pub member_id: String,
    pub attachment: Attachment,
    pub order_index: u32,
}

/// A member as the caller describes it, before ordering is imposed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MemberInput {
    pub member_kind: CancelKind,
    pub member_id: String,
    pub attachment: Attachment,
}

/// Why a proposed scope could not be frozen. These are funnel rejections,
/// not panics: a caller-supplied scope is untrusted input.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ScopeError {
    /// More than [`MAX_SCOPE_MEMBERS`].
    TooLarge { count: usize },
    /// The same `member_id` twice. Two rows for one member would let a
    /// member be both discharged and outstanding.
    DuplicateMember { member_id: String },
    /// A member the root cannot own. Freezing it would produce a request
    /// that claims authority over a sibling subtree.
    NotUnderRoot {
        member_kind: CancelKind,
        member_id: String,
    },
}

/// The frozen §5.2 rule-2 snapshot: leaf-first, deduplicated, and closed
/// under the root. Constructed only by [`CancelScope::freeze`].
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CancelScope {
    root_kind: CancelKind,
    root_id: String,
    members: Vec<ScopeMember>,
}

impl CancelScope {
    pub(crate) fn validate(&self) -> bool {
        if self.members.len() > MAX_SCOPE_MEMBERS
            || !crate::ids::valid_wire_identifier(&self.root_id)
        {
            return false;
        }
        let mut seen = HashSet::with_capacity(self.members.len());
        for (index, member) in self.members.iter().enumerate() {
            let is_root = member.member_id == self.root_id && member.member_kind == self.root_kind;
            if (!is_root && !self.root_kind.contains(member.member_kind))
                || !crate::ids::valid_wire_identifier(&member.member_id)
                || member.order_index != index as u32
                || !seen.insert(member.member_id.as_str())
            {
                return false;
            }
        }
        self.members.windows(2).all(|members| {
            members[0].member_kind.depth() > members[1].member_kind.depth()
                || (members[0].member_kind == members[1].member_kind
                    && members[0].member_id < members[1].member_id)
        })
    }

    /// Freeze a proposed scope (§5.2 rule 2). Members are ordered leaf-first
    /// — deepest kind first, ties broken by `member_id` so the order is
    /// deterministic across processes and replayable from the journal — and
    /// `order_index` is assigned densely from 0.
    ///
    /// The root itself is a member when named in `proposed`; it is not
    /// synthesized. The funnel accepts an empty no-op scope only after it
    /// verifies that the root already exists and is terminal.
    pub fn freeze(
        root_kind: CancelKind,
        root_id: impl Into<String>,
        proposed: Vec<MemberInput>,
    ) -> Result<Self, ScopeError> {
        let root_id = root_id.into();
        if proposed.len() > MAX_SCOPE_MEMBERS {
            return Err(ScopeError::TooLarge {
                count: proposed.len(),
            });
        }
        for m in &proposed {
            let is_root = m.member_id == root_id && m.member_kind == root_kind;
            if !is_root && !root_kind.contains(m.member_kind) {
                return Err(ScopeError::NotUnderRoot {
                    member_kind: m.member_kind,
                    member_id: m.member_id.clone(),
                });
            }
        }
        let mut seen = HashSet::with_capacity(proposed.len());
        for m in &proposed {
            if !seen.insert(m.member_id.as_str()) {
                return Err(ScopeError::DuplicateMember {
                    member_id: m.member_id.clone(),
                });
            }
        }

        let mut ordered = proposed;
        ordered.sort_by(|a, b| {
            b.member_kind
                .depth()
                .cmp(&a.member_kind.depth())
                .then_with(|| a.member_id.cmp(&b.member_id))
        });
        let members = ordered
            .into_iter()
            .enumerate()
            .map(|(i, m)| ScopeMember {
                member_kind: m.member_kind,
                member_id: m.member_id,
                attachment: m.attachment,
                // The length check above bounds this well under u32::MAX.
                order_index: i as u32,
            })
            .collect();
        Ok(CancelScope {
            root_kind,
            root_id,
            members,
        })
    }

    pub fn root_kind(&self) -> CancelKind {
        self.root_kind
    }

    pub fn root_id(&self) -> &str {
        &self.root_id
    }

    pub fn members(&self) -> &[ScopeMember] {
        &self.members
    }

    /// The member row for `member_id`, if the freeze captured it.
    pub fn member(&self, member_id: &str) -> Option<&ScopeMember> {
        [
            CancelKind::EffectIntent,
            CancelKind::Attempt,
            CancelKind::ExecutionUnit,
            CancelKind::Run,
        ]
        .into_iter()
        .find_map(|kind| {
            self.members
                .binary_search_by(|member| {
                    kind.depth()
                        .cmp(&member.member_kind.depth())
                        .then_with(|| member.member_id.as_str().cmp(member_id))
                })
                .ok()
                .map(|index| &self.members[index])
        })
    }

    /// Members in the order delivery must follow (§5.2 rule 3).
    pub fn delivery_order(&self) -> impl Iterator<Item = &ScopeMember> {
        self.members.iter()
    }
}

/// §5.1 `CancelRequest`. The scope is frozen at construction, so there is no
/// path that adds a member after commit — §5.2 rule 2's "later-created
/// members are governed by rule 4, not retroactively added" is structural
/// here rather than a discipline the funnel has to remember.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CancelRequest {
    pub cancel_request_id: Uuid7,
    pub reason: CancelReason,
    pub policy: CancelPolicy,
    pub scope: CancelScope,
    pub status: CancelStatus,
    pub requested_at: u64,
    /// §2.1 principal kind. As with [`crate::funnel::Command`], MILE-001
    /// carries the kind alone; `principal_id` belongs to the daemon
    /// boundary that ADR-002 P14.4 exempts in-process.
    pub requested_by_kind: String,
}

impl CancelRequest {
    /// Whether this request governs `member_id` under §5.2 rule 5's policy
    /// axis. `RootOnly` reaches the root alone even though the frozen scope
    /// may list descendants — the scope records what the tree looked like,
    /// the policy decides who inherits.
    pub fn governs(&self, member_id: &str) -> bool {
        match self.policy {
            CancelPolicy::RootOnly => self.scope.root_id == member_id,
            CancelPolicy::AttachedCascade => self
                .scope
                .member(member_id)
                .is_some_and(|m| m.attachment == Attachment::Attached),
        }
    }
}

/// §5.1 `CancelDelivery`. `UNIQUE(cancel_request_id, member_id)` is carried
/// in the store by a derived key; the pair lives here so the record is
/// self-describing in the journal.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CancelDelivery {
    pub cancel_request_id: Uuid7,
    pub member_id: String,
    pub member_kind: CancelKind,
    /// §5.2 rule 3 records delivery and observation separately: a signal
    /// sent is not a stop observed.
    pub delivered_at: u64,
    pub observed_at: Option<u64>,
    pub outcome: DeliveryOutcome,
}

#[cfg(test)]
#[path = "cancel_tests.rs"]
mod tests;
