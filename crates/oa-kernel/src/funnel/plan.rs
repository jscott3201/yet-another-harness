//! The staged mutation plan and event draft, split from `funnel/mod.rs` by
//! the same 700-LOC seam the §2.1 command registry used: the plan is a
//! self-contained contract, and the §5 cancellation methods add two more
//! variants whose payloads live here so the funnel core stays a thin
//! dispatcher.
//!
//! A `Plan` is what an accepted command WILL do, staged as concrete values so
//! the mutation phase performs no reads. `EventDraft` pairs one semantic
//! event with the aggregate version it stamps.

use selene_core::NodeId;

/// What an accepted command will do, staged as concrete values so the
/// mutation phase performs no reads.
pub(super) enum Plan {
    CreateRun {
        run_id: String,
        goal_work_item_id: String,
    },
    CloseRun {
        run_node: NodeId,
        new_version: u64,
        status: &'static str,
    },
    CreateWorkItem {
        work_item_id: String,
        acceptance_contract_digest: String,
        declared_write_scope: String,
    },
    CreateUnit {
        unit_id: String,
        work_item_id: String,
        run_id: String,
    },
    Dispatch {
        unit_node: NodeId,
        unit_id: String,
        new_version: u64,
        new_epoch: u64,
        stamp: u64,
        authority_epoch: u64,
        holder_id: String,
        /// Minted in the plan step, not the apply step: the apply closure
        /// runs inside the Selene mutator and must stay a pure writer.
        attempt_id: String,
        token_nonce: String,
        existing_lease: Option<(NodeId, u64)>,
        /// §1.2: at most one active attempt per unit — the prior epoch's
        /// row flips to `superseded` in this same transaction (§3.1).
        prior_attempt: Option<NodeId>,
    },
    BumpUnit {
        unit_node: NodeId,
        new_version: u64,
        new_stamp: Option<u64>,
    },
    CreateEffect {
        operation_key: String,
        effect_intent_id: String,
        unit_id: String,
        attempt_epoch: u64,
        record: String,
    },
    UpdateEffect {
        effect_node: NodeId,
        new_version: u64,
        state: &'static str,
        /// Set exactly once, at settle — the store column mirrors the
        /// record's write-once terminal.
        terminal: Option<&'static str>,
        record: String,
    },
    /// §5.1: create the immutable request row. The scope freeze decides the
    /// row's shape; the apply step writes it byte-for-byte.
    CreateCancelRequest(CancelRequestPlan),
    /// §5: one transaction that writes the write-once delivery row, advances
    /// the request version/status, and parks a dispatched-effect member at
    /// `reconciling` (§5.3) when the delivered member is that effect.
    CancelRecordDelivery(CancelDeliveryPlan),
    Nothing,
}

/// Staged values for creating a `CancelRequest` row (plan only; the scope
/// freeze itself happens in validation, which decides the row's shape).
pub(super) struct CancelRequestPlan {
    pub request_id: String,
    pub root_kind: String,
    pub root_id: String,
    pub policy: String,
    pub reason: String,
    pub status: String,
    /// The frozen scope, serialized once so the row and the journal event
    /// carry the same bytes.
    pub scope: String,
    /// The full serialized [`crate::cancel::CancelRequest`] — the store
    /// column `record` mirrors the row for the recover scan, exactly as the
    /// effect ledger carries its intent.
    pub record: String,
}

/// One `cancel.record_delivery` transaction, staged as concrete values.
pub(super) struct CancelDeliveryPlan {
    /// Book key — the derived `UNIQUE(cancel_request_id, member_id)`.
    pub delivery_key: String,
    /// The aggregate the delivered event stamps (the request id).
    pub request_id: String,
    pub request_node: NodeId,
    pub request_version_after: u64,
    pub request_status_after: String,
    /// The request `record` with the advanced status — regenerated in
    /// validation and written here so the mutation phase performs no reads.
    pub request_record_after: String,
    pub member_id: String,
    pub member_kind: String,
    pub order_index: u32,
    pub outcome: String,
    /// The immutable `CancelDelivery` row record.
    pub delivery_record: String,
    /// §5.3: when the delivered member is a dispatched effect with no
    /// outcome, park it at `reconciling` in this same transaction.
    pub park_effect: Option<EffectParkPlan>,
}

/// The §5.3 park half of a delivery: set `reconciling`, terminal stays
/// unset, version bumped.
pub(super) struct EffectParkPlan {
    pub effect_node: NodeId,
    pub effect_version: u64,
    pub effect_record: String,
}

pub(super) struct EventDraft {
    pub aggregate_kind: &'static str,
    pub aggregate_id: String,
    pub aggregate_version: u64,
    pub event_kind: &'static str,
    pub payload: serde_json::Value,
}
