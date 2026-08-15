use super::*;
use crate::cancel::CancelDelivery;
use crate::effect::EffectIntent;
use crate::store::receipt::{boolean, string};

pub(super) fn validate(
    store: &Store,
    command_type: &str,
    result: &serde_json::Map<String, serde_json::Value>,
    record: &ReceiptRecord,
) -> Result<bool, StoreError> {
    let events = events(store, record)?;
    let event = events.first();
    if command_type == "effect.prepare" {
        match (boolean(result, "existing")?, event.as_ref()) {
            (true, None) => return Ok(true),
            (true, Some(_)) => {
                return Err(StoreError::Internal(
                    "existing prepare receipt unexpectedly owns an event".into(),
                ));
            }
            (false, None) => {
                return Err(StoreError::Internal(
                    "new prepare receipt is missing its event".into(),
                ));
            }
            (false, Some(_)) => {}
        }
    }
    if matches!(command_type, "unit.progress_report" | "token.reissue") {
        if !events.is_empty() {
            return Err(StoreError::Internal(
                "no-event receipt unexpectedly owns events".into(),
            ));
        }
        return Ok(true);
    }
    let event = event.ok_or_else(|| StoreError::Internal("receipt is missing its event".into()))?;
    validate_identity(command_type, result, event)?;
    let payload: serde_json::Map<String, serde_json::Value> = serde_json::from_str(&event.payload)
        .map_err(|error| {
            StoreError::Internal(format!("receipt event payload is invalid: {error}"))
        })?;
    validate_payload(command_type, result, event, &payload)?;
    validate_aggregate(store, command_type, result, event, &payload)?;
    if command_type == "cancel.record_delivery" {
        if let Some(effect_event) = events.get(1) {
            validate_cancel_park(store, result, effect_event)?;
        }
    } else if events.len() != 1 {
        return Err(StoreError::Internal(
            "receipt owns an unexpected number of events".into(),
        ));
    }
    Ok(false)
}

fn events(store: &Store, record: &ReceiptRecord) -> Result<Vec<EventRecord>, StoreError> {
    let Some((first, last)) = record.first_cursor.zip(record.last_cursor) else {
        return Ok(Vec::new());
    };
    let count = last
        .checked_sub(first)
        .and_then(|distance| distance.checked_add(1))
        .and_then(|count| usize::try_from(count).ok())
        .ok_or_else(|| StoreError::Internal("receipt event range is invalid".into()))?;
    let events = store.events_after_limit(first - 1, count)?;
    if events.len() != count {
        return Err(StoreError::Internal("receipt events are missing".into()));
    }
    Ok(events)
}

pub(super) fn validate_cancel_park(
    store: &Store,
    result: &serde_json::Map<String, serde_json::Value>,
    event: &EventRecord,
) -> Result<(), StoreError> {
    let payload: serde_json::Map<String, serde_json::Value> = serde_json::from_str(&event.payload)
        .map_err(|error| StoreError::Internal(format!("park event payload is invalid: {error}")))?;
    if event.aggregate_kind != "effect"
        || event.event_kind != "effect.reconciling"
        || event.ordinal != 1
        || !exact(&payload, &["next_reconcile_at", "source"])
        || !payload
            .get("next_reconcile_at")
            .is_some_and(serde_json::Value::is_null)
        || payload.get("source").and_then(serde_json::Value::as_str) != Some("cancel_delivery")
    {
        return Err(StoreError::Internal(
            "cancellation receipt has an invalid effect park event".into(),
        ));
    }
    let node = store
        .effect_intent_id_node(&event.aggregate_id)
        .ok_or_else(|| StoreError::Internal("park event has no Effect row".into()))?;
    let read = store.shared.read();
    let props = read
        .node_properties(node)
        .ok_or_else(|| StoreError::Internal("park Effect row is unreadable".into()))?;
    let request_id = string(result, "cancel_request_id")?;
    let member_id = string(result, "member_id")?;
    let delivery_node = store
        .cancel_delivery_node(&format!("{request_id}|{member_id}"))
        .ok_or_else(|| StoreError::Internal("park event has no CancelDelivery row".into()))?;
    if read
        .node_properties(delivery_node)
        .and_then(|properties| properties.get(&db("member_kind")).and_then(value_str))
        .as_deref()
        != Some("effect_intent")
    {
        return Err(StoreError::Internal(
            "effect park event is not for an effect-intent delivery".into(),
        ));
    }
    if props
        .get(&db("effect_intent_id"))
        .and_then(value_str)
        .as_deref()
        != Some(member_id)
    {
        return Err(StoreError::Internal(
            "effect park event disagrees with the delivered member".into(),
        ));
    }
    version_at_least(props, event.aggregate_version, "Effect")
}

fn validate_identity(
    command_type: &str,
    result: &serde_json::Map<String, serde_json::Value>,
    event: &EventRecord,
) -> Result<(), StoreError> {
    let (event_kinds, aggregate_kind, identity_field) = match command_type {
        "run.open" => (&["run.opened"][..], "run", Some("run_id")),
        "run.close" => (&["run.closed"][..], "run", Some("run_id")),
        "work_item.create" => (
            &["work_item.created"][..],
            "work_item",
            Some("work_item_id"),
        ),
        "unit.admit" => (&["unit.admitted"][..], "unit", Some("unit_id")),
        "unit.dispatch" => (&["unit.dispatched"][..], "unit", Some("unit_id")),
        "unit.stamp_bump" => (&["unit.stamp_bumped"][..], "unit", Some("unit_id")),
        "effect.prepare" => (&["effect.prepared"][..], "effect", None),
        "effect.dispatch" => (
            &["effect.dispatching", "effect.settled"][..],
            "effect",
            None,
        ),
        "effect.record_dispatched" => (&["effect.dispatched"][..], "effect", None),
        "effect.settle" => (&["effect.settled"][..], "effect", None),
        "effect.park_reconciling" => (&["effect.reconciling"][..], "effect", None),
        "cancel.request" => (
            &["cancel_request.requested"][..],
            "cancel_request",
            Some("cancel_request_id"),
        ),
        "cancel.record_delivery" => (
            &["cancel_request.delivered"][..],
            "cancel_request",
            Some("cancel_request_id"),
        ),
        _ => return Err(StoreError::Internal("receipt has no event contract".into())),
    };
    let identity_matches = match identity_field {
        Some(field) => event.aggregate_id == string(result, field)?,
        None => true,
    };
    if !event_kinds.contains(&event.event_kind.as_str())
        || event.aggregate_kind != aggregate_kind
        || !identity_matches
        || event.ordinal != 0
        || result
            .get("version")
            .and_then(serde_json::Value::as_u64)
            .is_some_and(|version| version != event.aggregate_version)
        || matches!(
            command_type,
            "run.open" | "work_item.create" | "unit.admit" | "effect.prepare" | "cancel.request"
        ) && event.aggregate_version != 1
    {
        return Err(StoreError::Internal(
            "receipt result disagrees with its event".into(),
        ));
    }
    Ok(())
}

fn validate_payload(
    command_type: &str,
    result: &serde_json::Map<String, serde_json::Value>,
    event: &EventRecord,
    payload: &serde_json::Map<String, serde_json::Value>,
) -> Result<(), StoreError> {
    let valid = match command_type {
        "run.open" => {
            exact(payload, &["goal_work_item_id"]) && valid_id(payload, "goal_work_item_id")
        }
        "run.close" => exact(payload, &["outcome"]) && same(result, "status", payload, "outcome"),
        "work_item.create" => {
            exact(
                payload,
                &["acceptance_contract_digest", "declared_write_scope"],
            ) && payload
                .get("acceptance_contract_digest")
                .and_then(serde_json::Value::as_str)
                .is_some_and(|digest| crate::ids::Digest::try_from(digest.to_owned()).is_ok())
                && payload
                    .get("declared_write_scope")
                    .and_then(serde_json::Value::as_array)
                    .is_some_and(|scope| scope.iter().all(serde_json::Value::is_string))
        }
        "unit.admit" => {
            exact(payload, &["work_item_id", "run_id"])
                && valid_id(payload, "work_item_id")
                && valid_id(payload, "run_id")
        }
        "unit.dispatch" => {
            exact(payload, &["attempt_epoch", "attempt_id", "holder_id"])
                && same(result, "attempt_epoch", payload, "attempt_epoch")
                && same(result, "attempt_id", payload, "attempt_id")
                && same(result, "holder_id", payload, "holder_id")
        }
        "unit.stamp_bump" => exact(payload, &["stamp"]) && same(result, "stamp", payload, "stamp"),
        "effect.prepare" => {
            exact(
                payload,
                &[
                    "effect_intent_id",
                    "adapter_id",
                    "retry_class",
                    "reversibility_class",
                    "target_enumeration",
                ],
            ) && same(result, "effect_intent_id", payload, "effect_intent_id")
                && valid_id(payload, "adapter_id")
                && one_of(
                    payload,
                    "retry_class",
                    &[
                        "safe_idempotent",
                        "safe_with_operation_key",
                        "query_then_retry",
                        "no_retry",
                    ],
                )
                && one_of(
                    payload,
                    "reversibility_class",
                    &["bufferable", "reversible_external", "irreversible"],
                )
                && one_of(payload, "target_enumeration", &["declared", "post_hoc"])
        }
        "effect.dispatch" if event.event_kind == "effect.dispatching" => {
            exact(payload, &["operation_key"])
                && same(result, "operation_key", payload, "operation_key")
                && string(result, "state").ok() == Some("dispatching")
        }
        "effect.dispatch" => {
            exact(payload, &["terminal", "settled_at", "rule_4"])
                && string(result, "state").ok() == Some("settled")
                && payload.get("terminal").and_then(serde_json::Value::as_str) == Some("cancelled")
                && payload
                    .get("settled_at")
                    .and_then(serde_json::Value::as_u64)
                    .is_some()
                && payload.get("rule_4").and_then(serde_json::Value::as_bool) == Some(true)
        }
        "effect.record_dispatched" => {
            exact(payload, &["dispatched_at"])
                && payload
                    .get("dispatched_at")
                    .and_then(serde_json::Value::as_u64)
                    .is_some()
        }
        "effect.settle" => {
            exact(payload, &["terminal", "targets"])
                && one_of(
                    payload,
                    "terminal",
                    &["succeeded", "failed", "cancelled", "compensated", "unknown"],
                )
                && payload
                    .get("targets")
                    .and_then(serde_json::Value::as_u64)
                    .is_some()
        }
        "effect.park_reconciling" => {
            exact(payload, &["next_reconcile_at"])
                && payload
                    .get("next_reconcile_at")
                    .and_then(serde_json::Value::as_u64)
                    .is_some()
        }
        "cancel.request" => {
            exact(
                payload,
                &[
                    "root_kind",
                    "root_id",
                    "reason",
                    "policy",
                    "status",
                    "members",
                ],
            ) && same(result, "root_kind", payload, "root_kind")
                && same(result, "root_id", payload, "root_id")
                && same(result, "status", payload, "status")
                && one_of(
                    payload,
                    "reason",
                    &[
                        "owner_request",
                        "budget_exhausted",
                        "policy_violation",
                        "superseded_by_epoch",
                        "dependency_failed",
                        "shutdown_drain",
                    ],
                )
                && one_of(payload, "policy", &["attached_cascade", "root_only"])
                && payload
                    .get("members")
                    .and_then(serde_json::Value::as_u64)
                    .is_some()
        }
        "cancel.record_delivery" => {
            exact(
                payload,
                &[
                    "member_id",
                    "delivered_at",
                    "observed_at",
                    "outcome",
                    "status",
                ],
            ) && same(result, "member_id", payload, "member_id")
                && same(result, "outcome", payload, "outcome")
                && same(result, "status", payload, "status")
                && payload
                    .get("delivered_at")
                    .and_then(serde_json::Value::as_u64)
                    .is_some()
                && payload
                    .get("observed_at")
                    .is_some_and(|value| value.is_null() || value.as_u64().is_some())
        }
        _ => false,
    };
    if valid {
        Ok(())
    } else {
        Err(StoreError::Internal(
            "receipt event payload does not match its command contract".into(),
        ))
    }
}

fn validate_aggregate(
    store: &Store,
    command_type: &str,
    result: &serde_json::Map<String, serde_json::Value>,
    event: &EventRecord,
    payload: &serde_json::Map<String, serde_json::Value>,
) -> Result<(), StoreError> {
    match command_type {
        "run.open" | "run.close" => {
            let node = store
                .run_node(string(result, "run_id")?)
                .ok_or_else(|| StoreError::Internal("receipt has no Run row".into()))?;
            let read = store.shared.read();
            let props = read
                .node_properties(node)
                .ok_or_else(|| StoreError::Internal("receipt Run row is unreadable".into()))?;
            version_at_least(props, event.aggregate_version, "Run")?;
            if command_type == "run.open"
                && props
                    .get(&db("goal_work_item_id"))
                    .and_then(value_str)
                    .as_deref()
                    != payload
                        .get("goal_work_item_id")
                        .and_then(serde_json::Value::as_str)
            {
                return Err(StoreError::Internal(
                    "run receipt disagrees with its Run row".into(),
                ));
            }
            if command_type == "run.close"
                && props.get(&db("status")).and_then(value_str).as_deref()
                    != Some(string(result, "status")?)
            {
                return Err(StoreError::Internal(
                    "close receipt disagrees with its Run row".into(),
                ));
            }
        }
        "work_item.create" => validate_work_item(store, result, event, payload)?,
        "unit.admit" | "unit.dispatch" | "unit.stamp_bump" => {
            validate_unit(store, command_type, result, event, payload)?
        }
        command if command.starts_with("effect.") => {
            let node = store
                .effect_node(string(result, "operation_key")?)
                .ok_or_else(|| StoreError::Internal("receipt has no Effect row".into()))?;
            let read = store.shared.read();
            let props = read
                .node_properties(node)
                .ok_or_else(|| StoreError::Internal("receipt Effect row is unreadable".into()))?;
            if props
                .get(&db("effect_intent_id"))
                .and_then(value_str)
                .as_deref()
                != Some(event.aggregate_id.as_str())
            {
                return Err(StoreError::Internal(
                    "effect receipt disagrees with its event aggregate id".into(),
                ));
            }
            version_at_least(props, event.aggregate_version, "Effect")?;
            if command_type == "effect.prepare" {
                let intent: EffectIntent = serde_json::from_str(
                    &props
                        .get(&db("record"))
                        .and_then(value_str)
                        .ok_or_else(|| {
                            StoreError::Internal("Effect record is unreadable".into())
                        })?,
                )
                .map_err(|error| {
                    StoreError::Internal(format!("Effect record is invalid: {error}"))
                })?;
                if intent.effect_intent_id.to_string() != string(result, "effect_intent_id")?
                    || intent.adapter_id != string(payload, "adapter_id")?
                    || serde_json::to_value(intent.retry_class).ok().as_ref()
                        != payload.get("retry_class")
                    || serde_json::to_value(intent.reversibility_class)
                        .ok()
                        .as_ref()
                        != payload.get("reversibility_class")
                    || serde_json::to_value(intent.target_enumeration)
                        .ok()
                        .as_ref()
                        != payload.get("target_enumeration")
                {
                    return Err(StoreError::Internal(
                        "prepare receipt disagrees with its Effect row".into(),
                    ));
                }
            }
            let intent: EffectIntent = serde_json::from_str(
                &props
                    .get(&db("record"))
                    .and_then(value_str)
                    .ok_or_else(|| StoreError::Internal("Effect record is unreadable".into()))?,
            )
            .map_err(|error| StoreError::Internal(format!("Effect record is invalid: {error}")))?;
            if (command_type == "effect.dispatch"
                && event.event_kind == "effect.settled"
                && (intent.terminal != Some(crate::effect::EffectTerminal::Cancelled)
                    || serde_json::to_value(intent.settled_at).ok().as_ref()
                        != payload.get("settled_at")))
                || (command_type == "effect.settle"
                    && (serde_json::to_value(intent.terminal).ok().as_ref()
                        != payload.get("terminal")
                        || intent.targets.len() as u64
                            != payload
                                .get("targets")
                                .and_then(serde_json::Value::as_u64)
                                .unwrap_or(u64::MAX)))
            {
                return Err(StoreError::Internal(
                    "effect receipt disagrees with its Effect record".into(),
                ));
            }
        }
        "cancel.request" | "cancel.record_delivery" => {
            let node = store
                .cancel_request_node(string(result, "cancel_request_id")?)
                .ok_or_else(|| StoreError::Internal("receipt has no CancelRequest row".into()))?;
            let read = store.shared.read();
            let props = read.node_properties(node).ok_or_else(|| {
                StoreError::Internal("receipt CancelRequest row is unreadable".into())
            })?;
            version_at_least(props, event.aggregate_version, "CancelRequest")?;
            if command_type == "cancel.request"
                && (props.get(&db("root_kind")).and_then(value_str).as_deref()
                    != Some(string(result, "root_kind")?)
                    || props.get(&db("root_id")).and_then(value_str).as_deref()
                        != Some(string(result, "root_id")?)
                    || props.get(&db("reason")).and_then(value_str).as_deref()
                        != payload.get("reason").and_then(serde_json::Value::as_str)
                    || props.get(&db("policy")).and_then(value_str).as_deref()
                        != payload.get("policy").and_then(serde_json::Value::as_str)
                    || props
                        .get(&db("scope"))
                        .and_then(value_str)
                        .and_then(|scope| {
                            serde_json::from_str::<crate::cancel::CancelScope>(&scope).ok()
                        })
                        .map(|scope| scope.members().len() as u64)
                        != payload.get("members").and_then(serde_json::Value::as_u64))
            {
                return Err(StoreError::Internal(
                    "cancel receipt disagrees with its CancelRequest row".into(),
                ));
            }
            if command_type == "cancel.record_delivery" {
                validate_cancel_delivery(store, result, payload)?;
            }
        }
        _ => {}
    }
    Ok(())
}

fn validate_work_item(
    store: &Store,
    result: &serde_json::Map<String, serde_json::Value>,
    event: &EventRecord,
    payload: &serde_json::Map<String, serde_json::Value>,
) -> Result<(), StoreError> {
    let node = store
        .work_item_node(string(result, "work_item_id")?)
        .ok_or_else(|| StoreError::Internal("receipt has no WorkItem row".into()))?;
    let read = store.shared.read();
    let props = read
        .node_properties(node)
        .ok_or_else(|| StoreError::Internal("receipt WorkItem row is unreadable".into()))?;
    version_at_least(props, event.aggregate_version, "WorkItem")?;
    let scope = serde_json::from_str::<serde_json::Value>(
        &props
            .get(&db("declared_write_scope"))
            .and_then(value_str)
            .ok_or_else(|| StoreError::Internal("WorkItem scope is unreadable".into()))?,
    )
    .map_err(|error| StoreError::Internal(format!("WorkItem scope is invalid: {error}")))?;
    if props
        .get(&db("acceptance_contract_digest"))
        .and_then(value_str)
        .as_deref()
        != payload
            .get("acceptance_contract_digest")
            .and_then(serde_json::Value::as_str)
        || Some(&scope) != payload.get("declared_write_scope")
    {
        return Err(StoreError::Internal(
            "work item receipt disagrees with its WorkItem row".into(),
        ));
    }
    Ok(())
}

fn validate_unit(
    store: &Store,
    command_type: &str,
    result: &serde_json::Map<String, serde_json::Value>,
    event: &EventRecord,
    payload: &serde_json::Map<String, serde_json::Value>,
) -> Result<(), StoreError> {
    let node = store
        .unit_node(string(result, "unit_id")?)
        .ok_or_else(|| StoreError::Internal("receipt has no Unit row".into()))?;
    let read = store.shared.read();
    let props = read
        .node_properties(node)
        .ok_or_else(|| StoreError::Internal("receipt Unit row is unreadable".into()))?;
    version_at_least(props, event.aggregate_version, "Unit")?;
    if command_type == "unit.admit"
        && (props
            .get(&db("work_item_id"))
            .and_then(value_str)
            .as_deref()
            != payload
                .get("work_item_id")
                .and_then(serde_json::Value::as_str)
            || props.get(&db("run_id")).and_then(value_str).as_deref()
                != payload.get("run_id").and_then(serde_json::Value::as_str))
    {
        return Err(StoreError::Internal(
            "unit receipt disagrees with its Unit row".into(),
        ));
    }
    if command_type == "unit.stamp_bump"
        && props
            .get(&db("stamp"))
            .and_then(value_u64)
            .is_none_or(|stamp| {
                stamp < crate::store::receipt::integer(result, "stamp").unwrap_or(u64::MAX)
            })
    {
        return Err(StoreError::Internal(
            "stamp receipt exceeds its Unit row".into(),
        ));
    }
    Ok(())
}

fn validate_cancel_delivery(
    store: &Store,
    result: &serde_json::Map<String, serde_json::Value>,
    payload: &serde_json::Map<String, serde_json::Value>,
) -> Result<(), StoreError> {
    let request_id = string(result, "cancel_request_id")?;
    let member_id = string(result, "member_id")?;
    let node = store
        .cancel_delivery_node(&format!("{request_id}|{member_id}"))
        .ok_or_else(|| StoreError::Internal("receipt has no CancelDelivery row".into()))?;
    let read = store.shared.read();
    let record = read
        .node_properties(node)
        .and_then(|props| props.get(&db("record")).and_then(value_str))
        .ok_or_else(|| StoreError::Internal("receipt CancelDelivery row is unreadable".into()))?;
    let delivery: CancelDelivery = serde_json::from_str(&record).map_err(|error| {
        StoreError::Internal(format!("CancelDelivery record is invalid: {error}"))
    })?;
    let observation_is_valid = match (delivery.outcome, delivery.observed_at) {
        (crate::cancel::DeliveryOutcome::Unresponsive, None) => true,
        (crate::cancel::DeliveryOutcome::Unresponsive, Some(_)) | (_, None) => false,
        (_, Some(observed_at)) => observed_at >= delivery.delivered_at,
    };
    if delivery.member_id != member_id
        || !observation_is_valid
        || delivery.delivered_at
            != payload
                .get("delivered_at")
                .and_then(serde_json::Value::as_u64)
                .unwrap_or(u64::MAX)
        || serde_json::to_value(delivery.observed_at).ok().as_ref() != payload.get("observed_at")
        || serde_json::to_value(delivery.outcome).ok().as_ref() != payload.get("outcome")
    {
        return Err(StoreError::Internal(
            "delivery receipt disagrees with its CancelDelivery row".into(),
        ));
    }
    Ok(())
}

fn version_at_least(props: &PropertyMap, version: u64, row: &str) -> Result<(), StoreError> {
    if props
        .get(&db("version"))
        .and_then(value_u64)
        .is_none_or(|stored| stored < version)
    {
        Err(StoreError::Internal(format!(
            "receipt version exceeds its {row} row"
        )))
    } else {
        Ok(())
    }
}

fn exact(object: &serde_json::Map<String, serde_json::Value>, keys: &[&str]) -> bool {
    object.len() == keys.len() && keys.iter().all(|key| object.contains_key(*key))
}

fn same(
    left: &serde_json::Map<String, serde_json::Value>,
    left_field: &str,
    right: &serde_json::Map<String, serde_json::Value>,
    right_field: &str,
) -> bool {
    left.get(left_field) == right.get(right_field)
}

fn valid_id(object: &serde_json::Map<String, serde_json::Value>, field: &str) -> bool {
    object
        .get(field)
        .and_then(serde_json::Value::as_str)
        .is_some_and(crate::ids::valid_wire_identifier)
}

fn one_of(
    object: &serde_json::Map<String, serde_json::Value>,
    field: &str,
    values: &[&str],
) -> bool {
    object
        .get(field)
        .and_then(serde_json::Value::as_str)
        .is_some_and(|value| values.contains(&value))
}
