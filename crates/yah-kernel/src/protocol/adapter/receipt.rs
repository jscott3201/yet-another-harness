use super::*;
use crate::protocol::translate::validate_wire_id;
use crate::store::ReceiptRecord;

impl InProcessAdapter {
    pub fn get_receipt(
        &self,
        scope: Scope,
        command_id: impl Into<String>,
    ) -> Result<Receipt, Error> {
        let request = serde_json::to_vec(&ClientMessage::GetReceipt {
            project_id: self.project_id.clone(),
            scope,
            command_id: command_id.into(),
        })
        .map_err(|error| {
            protocol_error(
                ErrorKind::Internal,
                &format!("typed receipt lookup does not serialize: {error}"),
            )
        })?;
        match serde_json::from_slice(&self.handle_json(&request)).map_err(|error| {
            protocol_error(
                ErrorKind::Internal,
                &format!("adapter emitted invalid JSON: {error}"),
            )
        })? {
            ServerMessage::Receipt(receipt) => Ok(receipt),
            ServerMessage::Error(error) => Err(error),
            other => Err(protocol_error(
                ErrorKind::Internal,
                &format!("receipt lookup returned an invalid response: {other:?}"),
            )),
        }
    }

    pub(super) fn handle_get_receipt(
        &self,
        project_id: &str,
        scope: Scope,
        command_id: &str,
    ) -> ServerMessage {
        if project_id != self.project_id {
            return ServerMessage::Error(protocol_error(
                ErrorKind::InvalidRequest,
                "receipt belongs to a different project",
            ));
        }
        if let Err(detail) =
            validate_wire_id(&scope.scope_id).and_then(|()| validate_wire_id(command_id))
        {
            return ServerMessage::Error(protocol_error(ErrorKind::InvalidRequest, &detail));
        }
        let _command = self.command_gate.lock().expect("command gate");
        if let Some(detail) = self.funnel.poison_detail() {
            return ServerMessage::Error(protocol_error(ErrorKind::Unavailable, &detail));
        }
        let key = format!(
            "{}/{}/{}",
            scope.scope_kind.wire(),
            scope.scope_id,
            command_id
        );
        let stored = match self.funnel.store().receipt(&key) {
            Ok(Some(stored)) => stored,
            Ok(None) => {
                return ServerMessage::Error(protocol_error(
                    ErrorKind::NotFound,
                    "receipt not found",
                ));
            }
            Err(error) => {
                let detail = format!("durable receipt is unreadable: {error:?}");
                self.funnel.poison(detail.clone());
                return ServerMessage::Error(protocol_error(ErrorKind::Internal, &detail));
            }
        };
        if !public_command_type(&stored.command_type) {
            return ServerMessage::Error(protocol_error(ErrorKind::NotFound, "receipt not found"));
        }
        if let Err(error) = self
            .funnel
            .store()
            .validate_receipt_semantics(&key, &stored)
        {
            let detail = format!("durable receipt is invalid: {error:?}");
            self.funnel.poison(detail.clone());
            return ServerMessage::Error(protocol_error(ErrorKind::Internal, &detail));
        }
        match self.project_lookup_receipt(scope, command_id.to_owned(), stored) {
            Ok(receipt) => ServerMessage::Receipt(receipt),
            Err(detail) => {
                self.funnel.poison(detail.clone());
                ServerMessage::Error(protocol_error(ErrorKind::Internal, &detail))
            }
        }
    }

    pub(super) fn project_lookup_receipt(
        &self,
        scope: Scope,
        command_id: String,
        stored: ReceiptRecord,
    ) -> Result<Receipt, String> {
        if !public_command_type(&stored.command_type) {
            return Err("receipt command_type is not exposed by Adapter 1".into());
        }
        let receipt_version = u32::try_from(stored.receipt_version)
            .map(BoundedU32::new)
            .map_err(|_| "durable receipt has an invalid receipt_version".to_owned())?;
        let event_cursors = event_cursors(&stored)?;
        if stored.status == "rejected" {
            let result: JsonObject = serde_json::from_str(&stored.result)
                .map_err(|error| format!("durable rejection result is invalid: {error}"))?;
            let kind = result
                .get("error_kind")
                .cloned()
                .and_then(|value| serde_json::from_value::<KernelErrorKind>(value).ok())
                .ok_or_else(|| "durable rejection has an invalid error_kind".to_owned())?;
            let detail = result
                .get("detail")
                .and_then(Value::as_str)
                .ok_or_else(|| "durable rejection has an invalid detail".to_owned())?;
            return Ok(Receipt {
                command_id,
                scope,
                outcome: ReceiptOutcome::Rejected,
                state_version: None,
                event_cursors,
                error: Some(protocol_error(kind.into(), detail)),
                result: None,
                receipt_version,
            });
        }

        let result: Value = serde_json::from_str(&stored.result)
            .map_err(|error| format!("durable receipt result is invalid: {error}"))?;
        let state_version = result
            .get("version")
            .and_then(Value::as_u64)
            .map(DecimalU64::new);
        let result = self.project_lookup_result(&scope, &stored, result)?;
        if serde_json::to_vec(&result)
            .map_err(|error| format!("projected receipt result does not serialize: {error}"))?
            .len()
            > crate::protocol::MAX_RESULT_BYTES
        {
            return Err("projected receipt result exceeds its protocol limit".into());
        }
        Ok(Receipt {
            command_id,
            scope,
            outcome: ReceiptOutcome::Completed,
            state_version,
            event_cursors,
            error: None,
            result: Some(result),
            receipt_version,
        })
    }

    pub(super) fn validate_stored_receipt(
        &self,
        key: &str,
        stored: ReceiptRecord,
    ) -> Result<(), String> {
        let (scope, command_id) = receipt_address(key)?;
        self.funnel
            .store()
            .validate_receipt_semantics(key, &stored)
            .map_err(|error| format!("durable receipt is invalid: {error:?}"))?;
        if public_command_type(&stored.command_type) {
            self.project_lookup_receipt(scope, command_id, stored)
                .map(|_| ())
        } else {
            Ok(())
        }
    }

    fn project_lookup_result(
        &self,
        _scope: &Scope,
        stored: &ReceiptRecord,
        result: Value,
    ) -> Result<JsonObject, String> {
        let mut object = result
            .as_object()
            .cloned()
            .ok_or_else(|| "durable receipt result must be a JSON object".to_owned())?;
        if object.contains_key("attempt_token") {
            return Err("durable receipt result contains a projected attempt_token".into());
        }
        if stored.command_type == "unit.dispatch" {
            for field in [
                "attempt_epoch",
                "stamp",
                "authority_epoch",
                "holder_id",
                "token_nonce",
            ] {
                object.remove(field);
            }
        } else if object.contains_key("token_nonce") {
            return Err("non-dispatch durable result contains token claims".into());
        }
        project::stringify_result_integers(&mut object);
        Ok(object.into_iter().collect())
    }
}

fn public_command_type(command_type: &str) -> bool {
    matches!(
        command_type,
        "run.open"
            | "run.close"
            | "work_item.create"
            | "unit.admit"
            | "unit.dispatch"
            | "unit.progress_report"
            | "unit.stamp_bump"
    )
}

fn receipt_address(key: &str) -> Result<(Scope, String), String> {
    let mut parts = key.split('/');
    let scope_kind = match parts.next() {
        Some("global") => ScopeKind::Global,
        Some("project") => ScopeKind::Project,
        Some("run") => ScopeKind::Run,
        Some("unit") => ScopeKind::Unit,
        _ => return Err("durable receipt has an invalid scope kind".into()),
    };
    let scope_id = parts
        .next()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| "durable receipt has an invalid scope id".to_owned())?;
    let command_id = parts
        .next()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| "durable receipt has an invalid command id".to_owned())?;
    if parts.next().is_some() {
        return Err("durable receipt key has too many components".into());
    }
    validate_wire_id(scope_id)?;
    validate_wire_id(command_id)?;
    Ok((
        Scope {
            scope_kind,
            scope_id: scope_id.to_owned(),
        },
        command_id.to_owned(),
    ))
}

fn event_cursors(stored: &ReceiptRecord) -> Result<Vec<DecimalU64>, String> {
    let Some((first, last)) = stored.first_cursor.zip(stored.last_cursor) else {
        return Ok(Vec::new());
    };
    let count = last
        .checked_sub(first)
        .and_then(|distance| distance.checked_add(1))
        .and_then(|count| usize::try_from(count).ok())
        .filter(|count| *count <= crate::protocol::MAX_RESUME_EVENTS)
        .ok_or_else(|| "durable receipt event cursor range is invalid".to_owned())?;
    Ok((first..=last).take(count).map(DecimalU64::new).collect())
}
