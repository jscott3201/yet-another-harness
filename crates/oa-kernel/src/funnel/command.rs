//! The §2.1 command envelope and method registry: the closed set of
//! mutations the funnel accepts, and the vocabulary every one of them is
//! stated in. Split from `funnel/mod.rs` when the §5 methods pushed it past
//! the 700-LOC cap; the registry is a self-contained contract, so it is the
//! natural seam.
//!
//! Every method here declares exactly one §2.1 authorization class, and the
//! class is enforced in `validate.rs` rather than encoded in the type — the
//! table in ADR-001 §3.3 is the source of truth, and duplicating it as a
//! type-level tag would create a second place for it to drift.

use crate::effect::{EffectTerminal, TargetObservation};
use crate::error::ErrorKind;
use crate::funnel::{EffectSpec, SettleEvidence};
use crate::ids::{AuthorityEpoch, Digest, Uuid7};
use crate::store::AttemptTokenClaims;

/// §2.1's `PrincipalRef.principal_kind`. MILE-001 carries the kind alone —
/// `principal_id` and `capability_id` belong to the daemon boundary, whose
/// in-process derivation ADR-002 P14.4 exempts — but the KIND is what the
/// §2.1/§3.3 authorization classes are stated in terms of, so omitting it
/// would leave the authority class resting entirely on an epoch integer the
/// holder is handed in its own dispatch result.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PrincipalKind {
    Owner,
    DelegateHuman,
    Agent,
    Daemon,
}

/// §1.2 Run terminal statuses reachable by an explicit close. `open` and
/// `active` are not here: they are entry states, not close outcomes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RunOutcome {
    ClosedSuccess,
    ClosedFailure,
    Cancelled,
}

impl RunOutcome {
    pub(super) fn wire(self) -> &'static str {
        match self {
            RunOutcome::ClosedSuccess => "closed_success",
            RunOutcome::ClosedFailure => "closed_failure",
            RunOutcome::Cancelled => "cancelled",
        }
    }
}

/// ADR-002 §5 scope kinds; the receipt key is the P5.2 idempotency triple.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ScopeKind {
    Global,
    Project,
    Run,
    Unit,
}

impl ScopeKind {
    pub(super) fn wire(self) -> &'static str {
        match self {
            ScopeKind::Global => "global",
            ScopeKind::Project => "project",
            ScopeKind::Run => "run",
            ScopeKind::Unit => "unit",
        }
    }
}

#[derive(Clone, Debug)]
pub struct Command {
    pub command_id: Uuid7,
    pub scope_kind: ScopeKind,
    pub scope_id: String,
    /// P3.4's digest over the canonical request; equality decides replay vs
    /// `idempotency_conflict` (§2.2, I3).
    pub request_digest: Digest,
    /// Simplified §2.1 `expected_versions`: the version of the ONE
    /// aggregate the method mutates — the unit for unit methods, the effect
    /// for effect methods. REQUIRED on every mutation of existing state
    /// (I2's optimistic-concurrency rule); `None` is lawful only for pure
    /// creations (including idempotent `effect.prepare`) and for
    /// `token.reissue` (no transition).
    pub expected_version: Option<u64>,
    /// §2.1 principal. Authority-class methods require `Daemon`; holder
    /// methods require `Agent` — without this axis the authority class
    /// would rest entirely on an epoch integer the holder is handed in its
    /// own dispatch result.
    pub principal_kind: PrincipalKind,
    pub authority_epoch: Option<AuthorityEpoch>,
    pub attempt_token: Option<AttemptTokenClaims>,
    pub method: Method,
}

#[derive(Clone, Debug)]
pub enum Method {
    /// AUTHORITY: open the §1.2 cancellation-ownership root. A run must
    /// exist before a unit can be admitted into it, because §5.2 rule 4
    /// resolves a member up to its run.
    RunOpen {
        run_id: String,
        goal_work_item_id: String,
    },
    /// AUTHORITY: §5.2 rule 7 / I11 — a run MUST NOT close `success` while
    /// any required child, review, integration, approval, or effect is
    /// active, unknown, or uncompensated. Closing `failure` or `cancelled`
    /// carries no such bar: those are honest terminal states for a run with
    /// unresolved members, and refusing them would leave no lawful close at
    /// all for a run whose effects never resolve.
    RunClose { run_id: String, outcome: RunOutcome },
    /// Authority: create a work item (pure creation).
    WorkItemCreate {
        work_item_id: String,
        acceptance_contract_digest: Digest,
        declared_write_scope: Vec<String>,
    },
    /// Authority: admit a unit against a work item. Epoch starts at 0 —
    /// §3.1 mints 1 on the first acquisition (dispatch).
    UnitAdmit {
        unit_id: String,
        work_item_id: String,
        /// §1.2's owning run. Required so §5.2 rule 4 can resolve a member
        /// up to its run when looking for an applicable cancellation.
        run_id: String,
    },
    /// Authority: acquire or reassign — increments `current_attempt_epoch`,
    /// creates the Attempt row (superseding the prior one, §1.2's at most
    /// one active attempt per unit), upserts the single lease row to the
    /// new holder, and mints the attempt token (§3.1, §3.2).
    UnitDispatch { unit_id: String, holder_id: String },
    /// Holder: the fence-exercising state callback (obligation 3's shape).
    ProgressReport { unit_id: String, note: String },
    /// Authority: §3.4's second invalidation axis — kills outstanding
    /// tokens without minting an attempt, lease untouched (row A4).
    StampBump { unit_id: String },
    /// Holder-reauth: re-arm after a stamp bump. No journal event and no
    /// version bump — minting a token is not a domain transition.
    TokenReissue { unit_id: String },
    /// Holder: §4.2 step 1 — durably record the intent before any dispatch.
    /// Idempotent on the derived operation key: re-preparing an existing key
    /// returns the existing intent, never a second record (§4.1, row A13).
    EffectPrepare { unit_id: String, spec: EffectSpec },
    /// Holder (§3.3 row 5): `prepared -> dispatching` — the transition that
    /// AUTHORIZES the adapter to act, re-validating approval, retry safety,
    /// and the fence. An adapter MUST NOT perform the operation unless this
    /// committed.
    EffectDispatch {
        unit_id: String,
        operation_key: String,
    },
    /// Holder: §4.2 step 2's second half — `dispatching -> dispatched`,
    /// recording that the adapter was invoked. Dispatch is recorded;
    /// dispatch is not success.
    EffectRecordDispatched {
        unit_id: String,
        operation_key: String,
        dispatched_at: u64,
    },
    /// AUTHORITY (§3.3 row 6): §4.2 step 3 — atomically record the
    /// write-once terminal and the settle-time observations (§4.3 legality
    /// and lifecycle legality enforced). Deliberately NOT holder-fenced:
    /// settling records an observation about the world, and fencing it on
    /// attempt/stamp/lease would let a stamp bump manufacture `unknown`
    /// out of a known outcome.
    EffectSettle {
        unit_id: String,
        operation_key: String,
        terminal: EffectTerminal,
        targets: Vec<TargetObservation>,
        /// Statements about the operation as a whole that only the settling
        /// authority can make (§4.3 wait status, §5.3's target-reported
        /// cancellation).
        evidence: SettleEvidence,
        settled_at: u64,
    },
    /// AUTHORITY (§3.3 row 6, like settle): §4.2 step 4's parking state — a
    /// dispatched nonterminal intent waits at `reconciling` with its §11.1
    /// backoff position durable (row A16).
    EffectParkReconciling {
        unit_id: String,
        operation_key: String,
        next_reconcile_at: u64,
    },
    /// AUTHORITY (§3.3 row 6): §5 `cancel.request` — commit the frozen
    /// scope BEFORE any signal (I10). The scope is snapshotted at commit;
    /// later-created members are governed by rule 4, never retroactively
    /// added (§5.2 rule 2).
    CancelRequest {
        root_kind: crate::cancel::CancelKind,
        root_id: String,
        reason: crate::cancel::CancelReason,
        policy: crate::cancel::CancelPolicy,
        /// The members to freeze, leaf-first order imposed by
        /// `CancelScope::freeze`, UNIQUE by member_id enforced there.
        proposed: Vec<crate::cancel::MemberInput>,
    },
    /// AUTHORITY (§3.3 row 6): §5 `cancelRecordDelivery` — the write-once
    /// delivery + observation for one scope member. One row carries
    /// `delivered_at` + `observed_at` (Option) + `outcome`;
    /// `observed_at = None` means outcome=Unresponsive (terminal but
    /// unsettled, never a placeholder filled later).
    CancelRecordDelivery {
        cancel_request_id: String,
        member_id: String,
        delivered_at: u64,
        observed_at: Option<u64>,
        outcome: crate::cancel::DeliveryOutcome,
    },
}

impl Method {
    pub(super) fn unit_id(&self) -> Option<&str> {
        match self {
            Method::RunOpen { .. } | Method::RunClose { .. } => None,
            Method::WorkItemCreate { .. } => None,
            Method::UnitAdmit { unit_id, .. }
            | Method::UnitDispatch { unit_id, .. }
            | Method::ProgressReport { unit_id, .. }
            | Method::StampBump { unit_id }
            | Method::TokenReissue { unit_id }
            | Method::EffectPrepare { unit_id, .. }
            | Method::EffectDispatch { unit_id, .. }
            | Method::EffectRecordDispatched { unit_id, .. }
            | Method::EffectSettle { unit_id, .. }
            | Method::EffectParkReconciling { unit_id, .. } => Some(unit_id),
            Method::CancelRequest { .. } | Method::CancelRecordDelivery { .. } => None,
        }
    }

    pub(super) fn operation_key(&self) -> Option<&str> {
        match self {
            Method::EffectDispatch { operation_key, .. }
            | Method::EffectRecordDispatched { operation_key, .. }
            | Method::EffectSettle { operation_key, .. }
            | Method::EffectParkReconciling { operation_key, .. } => Some(operation_key),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Submission {
    Completed {
        result: serde_json::Value,
    },
    /// A completed receipt returned to a retry — byte-identical result.
    Replayed {
        result: serde_json::Value,
    },
    Rejected {
        kind: ErrorKind,
        detail: String,
        /// True when this rejection was served from a persisted receipt.
        replayed: bool,
    },
}
