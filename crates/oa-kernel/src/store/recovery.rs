use super::*;
use crate::cancel::{CancelDelivery, CancelRequest, CancelScope};
use crate::effect::EffectIntent;
use crate::ids::Digest;

pub(super) fn decode_token_key(value: &str) -> Option<[u8; 32]> {
    if value.len() != 64 {
        return None;
    }
    let mut key = [0_u8; 32];
    for (index, byte) in key.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&value[index * 2..index * 2 + 2], 16).ok()?;
    }
    Some(key)
}

pub(crate) fn commit_error(error: GraphError) -> StoreError {
    match error {
        GraphError::Durable { reason } => StoreError::CommitUnknown(reason),
        GraphError::TypeViolation(_) | GraphError::Core(_) | GraphError::Cancelled => {
            StoreError::Graph(error)
        }
        _ => StoreError::CommitUnknown(format!("unclassified Selene commit outcome: {error:?}")),
    }
}

fn required_string(props: &PropertyMap, field: &str, row: &str) -> Result<String, StoreError> {
    props
        .get(&db(field))
        .and_then(value_str)
        .ok_or_else(|| StoreError::Internal(format!("{row} has invalid {field}")))
}

fn wire_name(value: &impl serde::Serialize) -> Result<String, StoreError> {
    serde_json::to_value(value)
        .ok()
        .and_then(|value| value.as_str().map(str::to_owned))
        .ok_or_else(|| {
            StoreError::Internal("cancellation enum does not serialize as a string".into())
        })
}

fn required_u64(props: &PropertyMap, field: &str, row: &str) -> Result<u64, StoreError> {
    props
        .get(&db(field))
        .and_then(value_u64)
        .ok_or_else(|| StoreError::Internal(format!("{row} has invalid {field}")))
}

pub(super) fn validate_attempt(props: &PropertyMap) -> Result<(), StoreError> {
    let row = "Attempt row";
    let unit_id = required_string(props, "unit_id", row)?;
    let epoch = required_u64(props, "attempt_epoch", row)?;
    if required_string(props, "attempt_key", row)? != format!("{unit_id}/{epoch}") {
        return Err(StoreError::Internal(
            "Attempt attempt_key disagrees with its fields".into(),
        ));
    }
    Ok(())
}

pub(super) fn validate_effect(props: &PropertyMap) -> Result<(), StoreError> {
    let row = "Effect row";
    let intent: EffectIntent = serde_json::from_str(&required_string(props, "record", row)?)
        .map_err(|error| StoreError::Internal(format!("{row} has invalid record: {error}")))?;
    let terminal = match props.get(&db("terminal")) {
        Some(value) => value_str(value)
            .map(Some)
            .ok_or_else(|| StoreError::Internal(format!("{row} has invalid terminal")))?,
        None => None,
    };
    if required_string(props, "operation_key", row)? != intent.operation_key
        || required_string(props, "effect_intent_id", row)? != intent.effect_intent_id.to_string()
        || required_string(props, "unit_id", row)? != intent.unit_id
        || required_u64(props, "version", row)? != intent.version
        || required_string(props, "state", row)? != wire_name(&intent.state)?
        || terminal != intent.terminal.as_ref().map(wire_name).transpose()?
    {
        return Err(StoreError::Internal(
            "Effect indexed fields disagree with its record".into(),
        ));
    }
    Ok(())
}

pub(super) fn validate_cancel_request(props: &PropertyMap) -> Result<(), StoreError> {
    let row = "CancelRequest row";
    let request_id = required_string(props, "cancel_request_id", row)?;
    let root_kind = required_string(props, "root_kind", row)?;
    let root_id = required_string(props, "root_id", row)?;
    let policy = required_string(props, "policy", row)?;
    let reason = required_string(props, "reason", row)?;
    let status = required_string(props, "status", row)?;
    let scope: CancelScope = serde_json::from_str(&required_string(props, "scope", row)?)
        .map_err(|error| StoreError::Internal(format!("{row} has invalid scope: {error}")))?;
    let request: CancelRequest = serde_json::from_str(&required_string(props, "record", row)?)
        .map_err(|error| StoreError::Internal(format!("{row} has invalid record: {error}")))?;
    if request.cancel_request_id.to_string() != request_id
        || scope != request.scope
        || scope.root_kind().wire() != root_kind
        || scope.root_id() != root_id
        || wire_name(&request.policy)? != policy
        || wire_name(&request.reason)? != reason
        || wire_name(&request.status)? != status
    {
        return Err(StoreError::Internal(
            "CancelRequest indexed fields disagree with its record".into(),
        ));
    }
    Ok(())
}

pub(super) fn validate_cancel_delivery(props: &PropertyMap) -> Result<(), StoreError> {
    let row = "CancelDelivery row";
    let request_id = required_string(props, "cancel_request_id", row)?;
    let member_id = required_string(props, "member_id", row)?;
    let member_kind = required_string(props, "member_kind", row)?;
    let outcome = required_string(props, "outcome", row)?;
    let delivery_key = required_string(props, "delivery_key", row)?;
    let delivery: CancelDelivery = serde_json::from_str(&required_string(props, "record", row)?)
        .map_err(|error| StoreError::Internal(format!("{row} has invalid record: {error}")))?;
    if delivery_key != format!("{request_id}|{member_id}")
        || delivery.cancel_request_id.to_string() != request_id
        || delivery.member_id != member_id
        || delivery.member_kind.wire() != member_kind
        || delivery.outcome.wire() != outcome
    {
        return Err(StoreError::Internal(
            "CancelDelivery indexed fields disagree with its record".into(),
        ));
    }
    Ok(())
}

pub(super) fn validate_receipt(props: &PropertyMap) -> Result<(), StoreError> {
    let record = super::read::receipt_record(props)?;
    let key = required_string(props, "receipt_key", "Receipt row")?;
    let mut parts = key.split('/');
    let scope_kind = parts.next();
    let scope_id = parts.next();
    let command_id = parts.next();
    if !matches!(scope_kind, Some("global" | "project" | "run" | "unit"))
        || scope_id.is_none_or(str::is_empty)
        || command_id.is_none_or(str::is_empty)
        || parts.next().is_some()
        || Digest::try_from(record.request_digest.clone()).is_err()
        || !matches!(
            record.principal_kind.as_str(),
            "owner" | "delegate_human" | "agent" | "daemon"
        )
        || record.principal_id.is_empty()
    {
        return Err(StoreError::Internal("Receipt identity is invalid".into()));
    }
    if record.status == "rejected" {
        let value: serde_json::Value = serde_json::from_str(&record.result).map_err(|error| {
            StoreError::Internal(format!("Receipt rejection result is invalid: {error}"))
        })?;
        let error_kind = value
            .get("error_kind")
            .cloned()
            .and_then(|kind| serde_json::from_value::<crate::error::ErrorKind>(kind).ok());
        if error_kind.is_none()
            || value
                .get("detail")
                .and_then(|detail| detail.as_str())
                .is_none()
        {
            return Err(StoreError::Internal(
                "Receipt rejection result has invalid fields".into(),
            ));
        }
    }
    Ok(())
}
