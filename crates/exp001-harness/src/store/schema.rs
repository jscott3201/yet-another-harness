//! Closed EXP-001 graph schema and shared Selene value/diff helpers
//! (split from the store to honor the per-file LOC cap).

use selene_core::{
    DbString, LabelDiff, LabelSet, PropertyDiff, PropertyValueType, Value, db_string,
};
use selene_graph::{GraphTypeDef, NodeTypeDef, PropertyTypeDef, ValidationMode};

pub(super) fn db(s: &str) -> DbString {
    db_string(s).expect("harness strings are valid db strings")
}

pub(super) fn value_u64(v: &Value) -> Option<u64> {
    match v {
        Value::Uint(u) => Some(*u),
        Value::Int(i) => u64::try_from(*i).ok(),
        _ => None,
    }
}

pub(super) fn no_labels() -> LabelDiff {
    LabelDiff::new([], []).expect("empty label diff is valid")
}

pub(super) fn props_set(set: impl IntoIterator<Item = (DbString, Value)>) -> PropertyDiff {
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

/// The closed-graph schema. Illustrative per EXP-001 §4 — ADR-001 owns the
/// real byte-bounded schema; this is the minimum that makes the §7 bars
/// store-checkable.
pub fn graph_type() -> GraphTypeDef {
    use PropertyValueType::{String as PStr, Uint};
    GraphTypeDef {
        name: db("exp001.harness"),
        node_types: vec![
            node_type(
                "exp001.unit",
                "Unit",
                vec![
                    prop_def("unit_id", Uint, true, true, true),
                    prop_def("phase", PStr, true, false, false),
                    prop_def("version", Uint, true, false, false),
                    prop_def("attempt_epoch", Uint, true, false, false),
                    prop_def("holder_id", Uint, true, false, false),
                    prop_def("artifact_ref", PStr, false, false, false),
                ],
            ),
            node_type(
                "exp001.attempt",
                "Attempt",
                vec![
                    prop_def("attempt_key", PStr, true, true, true),
                    prop_def("unit_id", Uint, true, true, false),
                    prop_def("attempt_epoch", Uint, true, true, false),
                    prop_def("holder_id", Uint, true, true, false),
                    prop_def("state", PStr, true, false, false),
                ],
            ),
            node_type(
                "exp001.lease",
                "Lease",
                vec![
                    prop_def("lease_key", PStr, true, true, true),
                    prop_def("holder_id", Uint, true, true, false),
                    prop_def("expiry", Uint, true, false, false),
                    prop_def("last_renewal", Uint, true, false, false),
                ],
            ),
            node_type(
                "exp001.effect",
                "Effect",
                vec![
                    prop_def("intent_id", Uint, true, true, true),
                    prop_def("operation_key", PStr, true, true, false),
                    prop_def("state", PStr, true, true, false),
                    prop_def("unit_id", Uint, true, true, false),
                    prop_def("attempt_epoch", Uint, true, true, false),
                ],
            ),
            node_type(
                "exp001.receipt",
                "Receipt",
                vec![
                    prop_def("receipt_key", PStr, true, true, true),
                    prop_def("request_digest", PStr, true, true, false),
                    prop_def("transition_ref", Uint, true, true, false),
                ],
            ),
            node_type(
                "exp001.event",
                "Event",
                vec![
                    prop_def("event_id", Uint, true, true, true),
                    // Derived composite key: Selene's `unique` is
                    // single-property, so `(aggregate_id, aggregate_version,
                    // ordinal)` rides one derived string (R26a).
                    prop_def("agg_ver_ord", PStr, true, true, true),
                    prop_def("aggregate_id", Uint, true, true, false),
                    prop_def("aggregate_version", Uint, true, true, false),
                    prop_def("ordinal", Uint, true, true, false),
                    prop_def("kind", PStr, true, true, false),
                    prop_def("payload", PStr, true, true, false),
                    prop_def("causation_ref", Uint, false, true, false),
                ],
            ),
            node_type(
                "exp001.artifact",
                "Artifact",
                vec![prop_def("artifact_digest", PStr, true, true, true)],
            ),
        ],
        edge_types: Vec::new(),
    }
}
