use super::*;

pub(super) fn valid_command_type(command_type: &str) -> bool {
    matches!(
        command_type,
        "run.open"
            | "run.close"
            | "work_item.create"
            | "unit.admit"
            | "unit.dispatch"
            | "unit.progress_report"
            | "unit.stamp_bump"
            | "token.reissue"
            | "effect.prepare"
            | "effect.dispatch"
            | "effect.record_dispatched"
            | "effect.settle"
            | "effect.park_reconciling"
            | "cancel.request"
            | "cancel.record_delivery"
    )
}

pub(super) fn validate_rejection_result(result: &str) -> Result<(), StoreError> {
    let object: serde_json::Map<String, serde_json::Value> =
        serde_json::from_str(result).map_err(|error| {
            StoreError::Internal(format!("Receipt rejection result is invalid: {error}"))
        })?;
    let error_kind = object
        .get("error_kind")
        .cloned()
        .and_then(|kind| serde_json::from_value::<crate::error::ErrorKind>(kind).ok());
    let durable_kind = error_kind.is_some_and(|kind| {
        matches!(
            kind,
            crate::error::ErrorKind::InvalidRequest
                | crate::error::ErrorKind::Unauthorized
                | crate::error::ErrorKind::FenceRejected
                | crate::error::ErrorKind::ResourceExhausted
                | crate::error::ErrorKind::PayloadTooLarge
        )
    });
    if object.len() != 2
        || !durable_kind
        || object
            .get("detail")
            .and_then(serde_json::Value::as_str)
            .is_none_or(|detail| detail.chars().count() > crate::protocol::MAX_ERROR_DETAIL_CHARS)
    {
        return Err(StoreError::Internal(
            "Receipt rejection result has invalid fields".into(),
        ));
    }
    Ok(())
}

impl Store {
    pub(crate) fn validate_all_receipt_semantics(&self) -> Result<(), StoreError> {
        for key in self.receipt_keys() {
            let record = self.receipt(&key)?.ok_or_else(|| {
                StoreError::Internal(format!("receipt book entry {key} is missing"))
            })?;
            self.validate_receipt_semantics(&key, &record)?;
        }
        Ok(())
    }

    pub(crate) fn validate_receipt_semantics(
        &self,
        key: &str,
        record: &ReceiptRecord,
    ) -> Result<(), StoreError> {
        let (scope_kind, scope_id, _) = address(key)?;
        if scope_kind == "project" && scope_id != self.project_id {
            return Err(StoreError::Internal(
                "project receipt belongs to another control graph".into(),
            ));
        }
        if record.status == "rejected" {
            return validate_rejection_result(&record.result);
        }
        let object: serde_json::Map<String, serde_json::Value> =
            serde_json::from_str(&record.result).map_err(|error| {
                StoreError::Internal(format!("receipt result is invalid: {error}"))
            })?;
        if object.contains_key("attempt_token")
            || (record.command_type != "unit.dispatch" && object.contains_key("token_nonce"))
        {
            return Err(StoreError::Internal(
                "receipt result contains misplaced credential fields".into(),
            ));
        }
        validate_shape(&record.command_type, &object)?;
        validate_provenance(scope_kind, &record.command_type, &record.principal_kind)?;
        validate_result_identifiers(&record.command_type, &object)?;
        validate_completed_scope(scope_kind, scope_id, &record.command_type, &object)?;
        validate_effect_scope(self, scope_kind, scope_id, &record.command_type, &object)?;
        if record
            .first_cursor
            .zip(record.last_cursor)
            .is_some_and(|(first, last)| {
                first != last
                    && !(record.command_type == "cancel.record_delivery"
                        && first.checked_add(1) == Some(last))
            })
        {
            return Err(StoreError::Internal(
                "receipt_version 1 has an invalid semantic event range".into(),
            ));
        }
        let no_event = receipt_event::validate(self, &record.command_type, &object, record)?;
        validate_no_event_state(self, &record.command_type, &object, no_event)?;
        if record.command_type == "unit.dispatch" {
            let claims = claims(&object)?;
            self.validate_dispatch_claims(
                &claims,
                string(&object, "attempt_id")?,
                integer(&object, "version")?,
            )?;
        }
        let mut projected = object;
        for field in ["version", "attempt_epoch", "stamp", "authority_epoch"] {
            if let Some(number) = projected.get(field).and_then(serde_json::Value::as_u64) {
                projected.insert(field.into(), serde_json::Value::String(number.to_string()));
            }
        }
        if serde_json::to_vec(&projected)
            .map_err(|error| {
                StoreError::Internal(format!("receipt result cannot encode: {error}"))
            })?
            .len()
            > crate::protocol::MAX_RESULT_BYTES
        {
            return Err(StoreError::Internal(
                "projected receipt result exceeds its protocol limit".into(),
            ));
        }
        Ok(())
    }
}

fn validate_completed_scope(
    scope_kind: &str,
    scope_id: &str,
    command_type: &str,
    object: &serde_json::Map<String, serde_json::Value>,
) -> Result<(), StoreError> {
    let valid = match command_type {
        "run.open" | "run.close" => {
            scope_kind == "global" || (scope_kind == "run" && scope_id == string(object, "run_id")?)
        }
        "work_item.create" => matches!(scope_kind, "global" | "project"),
        "unit.progress_report" | "token.reissue" => {
            scope_kind == "unit" && scope_id == string(object, "unit_id")?
        }
        "unit.admit" | "unit.dispatch" | "unit.stamp_bump" => {
            scope_kind == "global"
                || (scope_kind == "unit" && scope_id == string(object, "unit_id")?)
        }
        "effect.prepare" | "effect.dispatch" | "effect.record_dispatched" => scope_kind == "unit",
        "effect.settle" | "effect.park_reconciling" => {
            matches!(scope_kind, "global" | "unit")
        }
        "cancel.request" | "cancel.record_delivery" => scope_kind == "global",
        _ => false,
    };
    if !valid {
        return Err(StoreError::Internal(
            "completed receipt has an invalid command scope".into(),
        ));
    }
    Ok(())
}

fn validate_provenance(
    scope_kind: &str,
    command_type: &str,
    principal_kind: &str,
) -> Result<(), StoreError> {
    let holder = matches!(
        command_type,
        "unit.progress_report"
            | "token.reissue"
            | "effect.prepare"
            | "effect.dispatch"
            | "effect.record_dispatched"
    );
    if (holder && (scope_kind != "unit" || principal_kind != "agent"))
        || (!holder && principal_kind != "daemon")
    {
        return Err(StoreError::Internal(
            "receipt disagrees with its command authorization class".into(),
        ));
    }
    Ok(())
}

fn validate_effect_scope(
    store: &Store,
    scope_kind: &str,
    scope_id: &str,
    command_type: &str,
    object: &serde_json::Map<String, serde_json::Value>,
) -> Result<(), StoreError> {
    if scope_kind != "unit" || !command_type.starts_with("effect.") {
        return Ok(());
    }
    let operation_key = string(object, "operation_key")?;
    let effect = store
        .effect_node(operation_key)
        .ok_or_else(|| StoreError::Internal("effect receipt has no Effect row".into()))?;
    let read = store.shared.read();
    if read
        .node_properties(effect)
        .and_then(|props| props.get(&db("unit_id")).and_then(value_str))
        .as_deref()
        != Some(scope_id)
    {
        return Err(StoreError::Internal(
            "effect receipt disagrees with its unit scope".into(),
        ));
    }
    Ok(())
}

fn validate_result_identifiers(
    command_type: &str,
    object: &serde_json::Map<String, serde_json::Value>,
) -> Result<(), StoreError> {
    let fields: &[&str] = match command_type {
        "run.open" | "run.close" => &["run_id"],
        "work_item.create" => &["work_item_id"],
        "unit.admit" | "unit.progress_report" | "unit.stamp_bump" => &["unit_id"],
        "unit.dispatch" => &["unit_id", "attempt_id", "holder_id", "token_nonce"],
        "effect.prepare" => &["effect_intent_id"],
        "cancel.request" => &["cancel_request_id", "root_id"],
        "cancel.record_delivery" => &["cancel_request_id", "member_id"],
        _ => &[],
    };
    if fields
        .iter()
        .any(|field| !crate::ids::valid_wire_identifier(string(object, field).unwrap_or_default()))
    {
        return Err(StoreError::Internal(
            "receipt result contains an invalid identifier".into(),
        ));
    }
    if command_type.starts_with("effect.")
        && crate::ids::Digest::try_from(string(object, "operation_key")?.to_owned()).is_err()
    {
        return Err(StoreError::Internal(
            "effect receipt contains an invalid operation key".into(),
        ));
    }
    Ok(())
}

fn address(key: &str) -> Result<(&str, &str, &str), StoreError> {
    let mut parts = key.split('/');
    let scope_kind = parts.next().unwrap_or_default();
    let scope_id = parts.next().unwrap_or_default();
    let command_id = parts.next().unwrap_or_default();
    if parts.next().is_some()
        || !matches!(scope_kind, "global" | "project" | "run" | "unit")
        || !crate::ids::valid_wire_identifier(scope_id)
        || !crate::ids::valid_wire_identifier(command_id)
    {
        return Err(StoreError::Internal("receipt key is invalid".into()));
    }
    Ok((scope_kind, scope_id, command_id))
}

pub(super) fn string<'a>(
    object: &'a serde_json::Map<String, serde_json::Value>,
    field: &str,
) -> Result<&'a str, StoreError> {
    object
        .get(field)
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| StoreError::Internal(format!("receipt result has invalid {field}")))
}

pub(super) fn integer(
    object: &serde_json::Map<String, serde_json::Value>,
    field: &str,
) -> Result<u64, StoreError> {
    object
        .get(field)
        .and_then(serde_json::Value::as_u64)
        .ok_or_else(|| StoreError::Internal(format!("receipt result has invalid {field}")))
}

pub(super) fn boolean(
    object: &serde_json::Map<String, serde_json::Value>,
    field: &str,
) -> Result<bool, StoreError> {
    object
        .get(field)
        .and_then(serde_json::Value::as_bool)
        .ok_or_else(|| StoreError::Internal(format!("receipt result has invalid {field}")))
}

fn validate_shape(
    command_type: &str,
    object: &serde_json::Map<String, serde_json::Value>,
) -> Result<(), StoreError> {
    match command_type {
        "run.open" => required(object, &["run_id"], &["version"]),
        "run.close" => {
            required(object, &["run_id", "status"], &["version"])?;
            one_of(
                string(object, "status")?,
                &["closed_success", "closed_failure", "cancelled"],
            )
        }
        "work_item.create" => required(object, &["work_item_id"], &["version"]),
        "unit.admit" | "unit.progress_report" => required(object, &["unit_id"], &["version"]),
        "unit.dispatch" => required(
            object,
            &["unit_id", "attempt_id", "holder_id", "token_nonce"],
            &["version", "attempt_epoch", "stamp", "authority_epoch"],
        ),
        "unit.stamp_bump" => required(object, &["unit_id"], &["version", "stamp"]),
        "effect.prepare" => {
            required(object, &["operation_key", "effect_intent_id"], &["version"])?;
            boolean(object, "existing").map(|_| ())
        }
        "effect.dispatch" => {
            required(object, &["operation_key", "state"], &["version"])?;
            one_of(string(object, "state")?, &["dispatching", "settled"])
        }
        "effect.record_dispatched" => {
            required(object, &["operation_key", "state"], &["version"])?;
            one_of(string(object, "state")?, &["dispatched"])
        }
        "effect.settle" => {
            required(object, &["operation_key", "state"], &["version"])?;
            one_of(string(object, "state")?, &["settled"])
        }
        "effect.park_reconciling" => {
            required(object, &["operation_key", "state"], &["version"])?;
            one_of(string(object, "state")?, &["reconciling"])
        }
        "cancel.request" => required(
            object,
            &["cancel_request_id", "status", "root_kind", "root_id"],
            &["version"],
        )
        .and_then(|()| one_of(string(object, "status")?, &["requested", "settled"]))
        .and_then(|()| {
            one_of(
                string(object, "root_kind")?,
                &["run", "execution_unit", "attempt", "effect_intent"],
            )
        }),
        "cancel.record_delivery" => required(
            object,
            &["cancel_request_id", "member_id", "outcome", "status"],
            &["version"],
        )
        .and_then(|()| {
            one_of(
                string(object, "outcome")?,
                &[
                    "observed_stopped",
                    "unresponsive",
                    "already_terminal",
                    "detached_declined",
                ],
            )
        })
        .and_then(|()| {
            one_of(
                string(object, "status")?,
                &["delivering", "observed_partial", "settled"],
            )
        }),
        "token.reissue" => Err(StoreError::Internal(
            "completed token.reissue receipt is unsupported".into(),
        )),
        _ => Err(StoreError::Internal(
            "receipt has unsupported command_type".into(),
        )),
    }?;
    exact_keys(command_type, object)?;
    Ok(())
}

fn one_of(value: &str, allowed: &[&str]) -> Result<(), StoreError> {
    if allowed.contains(&value) {
        Ok(())
    } else {
        Err(StoreError::Internal(
            "receipt result contains an invalid enum value".into(),
        ))
    }
}

fn exact_keys(
    command_type: &str,
    object: &serde_json::Map<String, serde_json::Value>,
) -> Result<(), StoreError> {
    let keys: &[&str] = match command_type {
        "run.open" => &["run_id", "version"],
        "run.close" => &["run_id", "version", "status"],
        "work_item.create" => &["work_item_id", "version"],
        "unit.admit" | "unit.progress_report" => &["unit_id", "version"],
        "unit.dispatch" => &[
            "unit_id",
            "version",
            "attempt_epoch",
            "attempt_id",
            "stamp",
            "authority_epoch",
            "holder_id",
            "token_nonce",
        ],
        "unit.stamp_bump" => &["unit_id", "version", "stamp"],
        "effect.prepare" => &["operation_key", "effect_intent_id", "version", "existing"],
        "effect.dispatch"
        | "effect.record_dispatched"
        | "effect.settle"
        | "effect.park_reconciling" => &["operation_key", "version", "state"],
        "cancel.request" => &[
            "cancel_request_id",
            "version",
            "status",
            "root_kind",
            "root_id",
        ],
        "cancel.record_delivery" => &[
            "cancel_request_id",
            "version",
            "member_id",
            "outcome",
            "status",
        ],
        _ => return Ok(()),
    };
    if object.len() == keys.len() && keys.iter().all(|key| object.contains_key(*key)) {
        Ok(())
    } else {
        Err(StoreError::Internal(
            "receipt result does not match its command_type/version schema".into(),
        ))
    }
}

fn required(
    object: &serde_json::Map<String, serde_json::Value>,
    strings: &[&str],
    integers: &[&str],
) -> Result<(), StoreError> {
    for field in strings {
        string(object, field)?;
    }
    for field in integers {
        integer(object, field)?;
    }
    Ok(())
}

fn validate_no_event_state(
    store: &Store,
    command_type: &str,
    object: &serde_json::Map<String, serde_json::Value>,
    no_event: bool,
) -> Result<(), StoreError> {
    if command_type == "unit.progress_report" && no_event {
        let unit_id = string(object, "unit_id")?;
        let unit = store
            .unit_node(unit_id)
            .ok_or_else(|| StoreError::Internal("progress receipt has no Unit row".into()))?;
        let read = store.shared.read();
        let version = read
            .node_properties(unit)
            .and_then(|props| props.get(&db("version")).and_then(value_u64))
            .ok_or_else(|| StoreError::Internal("progress Unit row is unreadable".into()))?;
        if version < integer(object, "version")? {
            return Err(StoreError::Internal(
                "progress receipt version exceeds its Unit row".into(),
            ));
        }
    }
    if command_type == "effect.prepare" && no_event {
        let operation_key = string(object, "operation_key")?;
        let receipt_version = integer(object, "version")?;
        let effect = store
            .effect_node(operation_key)
            .ok_or_else(|| StoreError::Internal("prepare receipt has no Effect row".into()))?;
        let read = store.shared.read();
        let props = read
            .node_properties(effect)
            .ok_or_else(|| StoreError::Internal("prepare Effect row is unreadable".into()))?;
        if props
            .get(&db("effect_intent_id"))
            .and_then(value_str)
            .as_deref()
            != Some(string(object, "effect_intent_id")?)
            || props
                .get(&db("version"))
                .and_then(value_u64)
                .is_none_or(|version| version < receipt_version)
            || !boolean(object, "existing")?
        {
            return Err(StoreError::Internal(
                "prepare receipt disagrees with its Effect row".into(),
            ));
        }
    }
    Ok(())
}

fn claims(
    object: &serde_json::Map<String, serde_json::Value>,
) -> Result<AttemptTokenClaims, StoreError> {
    Ok(AttemptTokenClaims {
        unit_id: string(object, "unit_id")?.to_owned(),
        attempt_epoch: AttemptEpoch(integer(object, "attempt_epoch")?),
        stamp: Stamp(integer(object, "stamp")?),
        authority_epoch: AuthorityEpoch(integer(object, "authority_epoch")?),
        holder_id: string(object, "holder_id")?.to_owned(),
        nonce: string(object, "token_nonce")?.to_owned(),
    })
}
