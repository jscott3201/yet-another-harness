//! Closed control-graph schema (`oa.control`) and the Selene value/diff
//! helpers every store and funnel write path shares.

use selene_core::{
    DbString, LabelDiff, LabelSet, PropertyDiff, PropertyValueType, Value, db_string,
};
use selene_graph::{GraphTypeDef, NodeTypeDef, PropertyTypeDef, ValidationMode};

pub(crate) fn db(s: &str) -> DbString {
    db_string(s).expect("kernel identifiers are valid db strings")
}

pub(crate) fn value_u64(v: &Value) -> Option<u64> {
    match v {
        Value::Uint(u) => Some(*u),
        Value::Int(i) => u64::try_from(*i).ok(),
        _ => None,
    }
}

pub(crate) fn value_str(v: &Value) -> Option<String> {
    match v {
        Value::String(s) => Some(s.as_str().to_string()),
        _ => None,
    }
}

pub(crate) fn no_labels() -> LabelDiff {
    LabelDiff::new([], []).expect("empty label diff is valid")
}

pub(crate) fn props_set(set: impl IntoIterator<Item = (DbString, Value)>) -> PropertyDiff {
    PropertyDiff::new(set, []).expect("property diff keys are distinct")
}

fn prop_def(
    name: &str,
    value_type: PropertyValueType,
    required: bool,
    immutable: bool,
    unique: bool,
) -> PropertyTypeDef {
    PropertyTypeDef {
        name: db(name),
        value_type,
        list_element_type: None,
        required,
        default: None,
        immutable,
        unique,
        decimal_type: None,
        character_string_type: None,
        byte_string_type: None,
        record_field_types: None,
    }
}

fn node_type(name: &str, label: &str, properties: Vec<PropertyTypeDef>) -> NodeTypeDef {
    NodeTypeDef {
        name: db(name),
        key_labels: LabelSet::single(db(label)),
        properties,
        validation_mode: ValidationMode::Strict,
    }
}

/// The closed control-graph schema (ADR-001 §1 records, store-checkable
/// subset). Composite uniques ride derived string keys because Selene's
/// `unique` is single-property (G02 finding, R26a). Full records travel as
/// canonical-JSON `record` payloads beside the typed fence/CAS fields; the
/// typed fields are the ones transactions compare, so divergence between
/// them and the payload is an auditor-checkable defect, not a truth split.
pub fn graph_type() -> GraphTypeDef {
    use PropertyValueType::{String as PStr, Uint};
    GraphTypeDef {
        name: db("oa.control"),
        node_types: vec![
            // §7.3: singleton per control graph; the fixed unique key makes
            // a second row unrepresentable.
            node_type(
                "oa.authority",
                "Authority",
                vec![
                    prop_def("authority_key", PStr, true, true, true),
                    prop_def("authority_epoch", Uint, true, false, false),
                    prop_def("holder_instance_id", PStr, true, false, false),
                    prop_def("prior_instance_id", PStr, false, false, false),
                    prop_def("project_id", PStr, true, true, true),
                    prop_def("token_key", PStr, true, true, true),
                    prop_def("min_retained_cursor", Uint, true, false, false),
                    prop_def("status", PStr, true, false, false),
                ],
            ),
            // Holder identity deliberately absent: §1.2's ExecutionUnit has
            // none — the fence reads the holder from the lease (§3.3), and a
            // second copy here would invite checking the wrong record.
            node_type(
                "oa.unit",
                "Unit",
                vec![
                    prop_def("unit_id", PStr, true, true, true),
                    prop_def("version", Uint, true, false, false),
                    prop_def("current_attempt_epoch", Uint, true, false, false),
                    prop_def("stamp", Uint, true, false, false),
                    prop_def("status", PStr, true, false, false),
                    prop_def("work_item_id", PStr, true, true, false),
                    // §1.2's ExecutionUnit.run_id. Required, not optional:
                    // §5.2 rule 4 resolves a member up to its run to find an
                    // applicable ancestor cancellation, and a unit with no
                    // run would make that walk answer "none applicable" —
                    // silently admitting work under a cancelled run.
                    prop_def("run_id", PStr, true, true, false),
                    prop_def("record", PStr, true, false, false),
                ],
            ),
            node_type(
                "oa.attempt",
                "Attempt",
                vec![
                    // UNIQUE(unit_id, attempt_epoch) as a derived key.
                    prop_def("attempt_key", PStr, true, true, true),
                    // §1.2's Attempt.attempt_id. The derived key addresses a
                    // row; this is the identity a CancelRequest roots at,
                    // since §5.1 roots are ids and not composite keys.
                    prop_def("attempt_id", PStr, true, true, true),
                    prop_def("unit_id", PStr, true, true, false),
                    prop_def("attempt_epoch", Uint, true, true, false),
                    prop_def("stamp", Uint, true, true, false),
                    prop_def("authority_epoch", Uint, true, true, false),
                    prop_def("holder_id", PStr, true, true, false),
                    prop_def("token_nonce", PStr, true, true, false),
                    prop_def("status", PStr, true, false, false),
                ],
            ),
            // One lease row per unit (unique key = unit_id): "at most one
            // active lease per unit" holds by construction; lease history
            // lives in the journal, not in superseded rows.
            node_type(
                "oa.lease",
                "Lease",
                vec![
                    prop_def("unit_id", PStr, true, true, true),
                    prop_def("attempt_epoch", Uint, true, false, false),
                    prop_def("holder_id", PStr, true, false, false),
                    prop_def("status", PStr, true, false, false),
                    prop_def("version", Uint, true, false, false),
                ],
            ),
            node_type(
                "oa.work_item",
                "WorkItem",
                vec![
                    prop_def("work_item_id", PStr, true, true, true),
                    prop_def("version", Uint, true, false, false),
                    prop_def("status", PStr, true, false, false),
                    // Pinned (§1.1): a contract change mints a NEW work item
                    // (§6.1); re-pinning a committed row is unlawful even
                    // for drafts, since drafts can re-mint too.
                    prop_def("acceptance_contract_digest", PStr, true, true, false),
                    prop_def("declared_write_scope", PStr, true, false, false),
                    prop_def("record", PStr, true, false, false),
                ],
            ),
            // §2.3: deterministic rejections persist too; status/result stay
            // mutable only for the §2.2 outcome_unknown resolution path.
            node_type(
                "oa.receipt",
                "Receipt",
                vec![
                    prop_def("receipt_key", PStr, true, true, true),
                    prop_def("request_digest", PStr, true, true, false),
                    prop_def("principal_kind", PStr, true, true, false),
                    prop_def("principal_id", PStr, true, true, false),
                    prop_def("status", PStr, true, false, false),
                    prop_def("result", PStr, true, false, false),
                    prop_def("first_cursor", Uint, false, false, false),
                    prop_def("last_cursor", Uint, false, false, false),
                ],
            ),
            // §2.4: every payload-bearing property immutable; deletes are
            // funnel-rejected `journal_immutable`.
            node_type(
                "oa.event",
                "Event",
                vec![
                    prop_def("event_id", PStr, true, true, true),
                    prop_def("cursor", Uint, true, true, true),
                    prop_def("agg_ver_ord", PStr, true, true, true),
                    prop_def("aggregate_kind", PStr, true, true, false),
                    prop_def("aggregate_id", PStr, true, true, false),
                    prop_def("aggregate_version", Uint, true, true, false),
                    prop_def("ordinal", Uint, true, true, false),
                    prop_def("event_kind", PStr, true, true, false),
                    prop_def("payload", PStr, true, true, false),
                    prop_def("receipt_key", PStr, true, true, false),
                    prop_def("command_id", PStr, true, true, false),
                    prop_def("actor_kind", PStr, true, true, false),
                    prop_def("actor_id", PStr, true, true, false),
                    prop_def("occurred_at_ms", Uint, true, true, false),
                    prop_def("causation_id", PStr, false, true, false),
                    prop_def("correlation_id", PStr, false, true, false),
                ],
            ),
            node_type(
                "oa.effect",
                "Effect",
                vec![
                    prop_def("operation_key", PStr, true, true, true),
                    prop_def("effect_intent_id", PStr, true, true, true),
                    prop_def("unit_id", PStr, true, true, false),
                    // The effect's own §1 aggregate-version axis: settle
                    // events stamp this, never the unit's version (§3.3
                    // settle does not advance unit state).
                    prop_def("version", Uint, true, false, false),
                    prop_def("state", PStr, true, false, false),
                    // Absent = unset; write-once is funnel-enforced (settle).
                    prop_def("terminal", PStr, false, false, false),
                    prop_def("record", PStr, true, false, false),
                ],
            ),
            // §1.2 cancellation-ownership root. Present so I11's close
            // predicate has an aggregate to guard: obligation 4 requires
            // that the enclosing run cannot close success while a member is
            // still reconciling.
            node_type(
                "oa.run",
                "Run",
                vec![
                    prop_def("run_id", PStr, true, true, true),
                    prop_def("version", Uint, true, false, false),
                    prop_def("status", PStr, true, false, false),
                    prop_def("goal_work_item_id", PStr, true, true, false),
                    prop_def("record", PStr, true, false, false),
                ],
            ),
            // §5.1. `scope` is its own IMMUTABLE property rather than a
            // field inside the mutable `record`: rule 2's freeze then holds
            // by construction, so no funnel path can widen a committed
            // scope even by mistake. `status` stays mutable — the request
            // walks requested -> delivering -> observed_partial -> settled.
            node_type(
                "oa.cancel_request",
                "CancelRequest",
                vec![
                    prop_def("cancel_request_id", PStr, true, true, true),
                    prop_def("version", Uint, true, false, false),
                    prop_def("root_kind", PStr, true, true, false),
                    prop_def("root_id", PStr, true, true, false),
                    prop_def("policy", PStr, true, true, false),
                    prop_def("reason", PStr, true, true, false),
                    prop_def("scope", PStr, true, true, false),
                    prop_def("status", PStr, true, false, false),
                    prop_def("record", PStr, true, false, false),
                ],
            ),
            // §5.1 UNIQUE(cancel_request_id, member_id) as a derived key.
            // Write-once in every property: a delivery row records what was
            // observed for one member, and rewriting an observation is how
            // an `unresponsive` member would silently become `stopped`.
            node_type(
                "oa.cancel_delivery",
                "CancelDelivery",
                vec![
                    prop_def("delivery_key", PStr, true, true, true),
                    prop_def("cancel_request_id", PStr, true, true, false),
                    prop_def("member_id", PStr, true, true, false),
                    prop_def("member_kind", PStr, true, true, false),
                    prop_def("outcome", PStr, true, true, false),
                    prop_def("record", PStr, true, true, false),
                ],
            ),
            // §8.1: immutable once sealed — every property immutable.
            node_type(
                "oa.evidence",
                "Evidence",
                vec![
                    prop_def("evidence_key", PStr, true, true, true),
                    prop_def("work_item_id", PStr, true, true, false),
                    prop_def("artifact_set_digest", PStr, true, true, false),
                    prop_def("record", PStr, true, true, false),
                ],
            ),
        ],
        edge_types: Vec::new(),
    }
}
