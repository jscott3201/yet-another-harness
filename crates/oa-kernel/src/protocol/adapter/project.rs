use super::*;
use crate::protocol::{EVENT_VERSION, MAX_EVENT_BYTES, MAX_EVENT_PAYLOAD_BYTES};
use chrono::{DateTime, Utc};

impl InProcessAdapter {
    pub(super) fn project_submission(&self, command: &Command, submission: Submission) -> Receipt {
        let key = format!(
            "{}/{}/{}",
            command.scope.scope_kind.wire(),
            command.scope.scope_id,
            command.command_id
        );
        let stored = match self.funnel.store().receipt(&key) {
            Ok(stored) => stored,
            Err(error) => {
                let detail = format!("durable receipt is unreadable: {error:?}");
                self.funnel.poison(detail.clone());
                return self.rejection_receipt(Some(command), ErrorKind::Internal, &detail);
            }
        };
        let event_cursors = || -> Result<Vec<DecimalU64>, String> {
            let Some((first, last)) = stored
                .as_ref()
                .and_then(|receipt| receipt.first_cursor.zip(receipt.last_cursor))
            else {
                return Ok(Vec::new());
            };
            let count = last
                .checked_sub(first)
                .and_then(|distance| distance.checked_add(1))
                .and_then(|count| usize::try_from(count).ok())
                .filter(|count| *count <= crate::protocol::MAX_RESUME_EVENTS)
                .ok_or_else(|| "durable receipt event cursor range is invalid".to_owned())?;
            Ok((first..=last).take(count).map(DecimalU64::new).collect())
        };
        match submission {
            Submission::Completed { result } => match event_cursors() {
                Ok(cursors) => {
                    self.project_completed(command, ReceiptOutcome::Completed, result, cursors)
                }
                Err(detail) => self.corrupt_receipt(command, detail),
            },
            Submission::Replayed { result } => match event_cursors() {
                Ok(cursors) => {
                    self.project_completed(command, ReceiptOutcome::Replayed, result, cursors)
                }
                Err(detail) => self.corrupt_receipt(command, detail),
            },
            Submission::Rejected {
                kind: KernelErrorKind::Internal,
                detail,
                replayed: true,
            } => self.corrupt_receipt(command, detail),
            Submission::Rejected { kind, detail, .. } => Receipt {
                command_id: command.command_id.clone(),
                scope: command.scope.clone(),
                outcome: if kind == KernelErrorKind::OutcomeUnknown {
                    ReceiptOutcome::OutcomeUnknown
                } else {
                    ReceiptOutcome::Rejected
                },
                state_version: None,
                event_cursors: Vec::new(),
                error: Some(protocol_error(kind.into(), &detail)),
                result: None,
                receipt_version: super::super::RECEIPT_VERSION,
            },
        }
    }

    fn corrupt_receipt(&self, command: &Command, detail: String) -> Receipt {
        self.funnel.poison(detail.clone());
        self.rejection_receipt(Some(command), ErrorKind::Internal, &detail)
    }

    fn project_completed(
        &self,
        command: &Command,
        outcome: ReceiptOutcome,
        result: Value,
        event_cursors: Vec<DecimalU64>,
    ) -> Receipt {
        let state_version = result
            .get("version")
            .and_then(Value::as_u64)
            .map(DecimalU64::new);
        let result = match self.project_result(command, result) {
            Ok(result) => result,
            Err(detail) => {
                self.funnel.poison(detail.clone());
                return self.rejection_receipt(Some(command), ErrorKind::Internal, &detail);
            }
        };
        Receipt {
            command_id: command.command_id.clone(),
            scope: command.scope.clone(),
            outcome,
            state_version,
            event_cursors,
            error: None,
            result: Some(result),
            receipt_version: super::super::RECEIPT_VERSION,
        }
    }

    pub(super) fn event(&self, event: crate::store::EventRecord) -> Result<Event, String> {
        let event_id = event.event_id.clone();
        let mut payload: JsonObject = serde_json::from_str(&event.payload).map_err(|error| {
            format!("durable event {} has invalid JSON: {error}", event.event_id)
        })?;
        payload.values_mut().for_each(stringify_json_integers);
        let ordinal = u32::try_from(event.ordinal)
            .map_err(|_| format!("durable event {event_id} has an invalid ordinal"))?;
        let projected = Event {
            event_id: event.event_id,
            cursor: DecimalU64::new(event.cursor),
            stream_class: StreamClass::DurableSemantic,
            aggregate_kind: event.aggregate_kind,
            aggregate_id: event.aggregate_id,
            aggregate_version: DecimalU64::new(event.aggregate_version),
            ordinal: BoundedU32::new(ordinal),
            event_kind: event.event_kind,
            event_version: EVENT_VERSION,
            occurred_at: Rfc3339Timestamp::new(format_timestamp(event.occurred_at_ms)?)
                .map_err(|error| format!("durable event {event_id} {error}"))?,
            actor: Actor {
                principal_kind: event.actor_kind,
                principal_id: event.actor_id,
            },
            command_id: Some(event.command_id),
            causation_id: event.causation_id,
            correlation_id: event.correlation_id,
            payload,
        };
        if serde_json::to_vec(&projected.payload).unwrap().len() > MAX_EVENT_PAYLOAD_BYTES
            || serde_json::to_vec(&projected).unwrap().len() > MAX_EVENT_BYTES
        {
            return Err(format!(
                "durable event {event_id} exceeds the protocol size limit"
            ));
        }
        Ok(projected)
    }

    pub(super) fn rejection_receipt(
        &self,
        command: Option<&Command>,
        kind: ErrorKind,
        detail: &str,
    ) -> Receipt {
        Receipt {
            command_id: command
                .map(|command| command.command_id.clone())
                .unwrap_or_default(),
            scope: command
                .map(|command| command.scope.clone())
                .unwrap_or(Scope {
                    scope_kind: ScopeKind::Global,
                    scope_id: "protocol".into(),
                }),
            outcome: ReceiptOutcome::Rejected,
            state_version: None,
            event_cursors: Vec::new(),
            error: Some(protocol_error(kind, detail)),
            result: None,
            receipt_version: super::super::RECEIPT_VERSION,
        }
    }

    fn project_result(&self, command: &Command, result: Value) -> Result<JsonObject, String> {
        let mut object = result
            .as_object()
            .cloned()
            .ok_or_else(|| "durable receipt result must be a JSON object".to_owned())?;
        if object.contains_key("attempt_token") {
            return Err("durable receipt result contains a projected attempt_token".into());
        }
        if let CommandBody::UnitDispatch(payload) = &command.body {
            let claims = claims_from_result(&object)?;
            if claims.unit_id != payload.unit_id || claims.holder_id != payload.holder_id {
                return Err("durable dispatch result claims do not match the command".into());
            }
            let current = self
                .funnel
                .store()
                .validate_dispatch_claims(&claims)
                .map_err(|error| format!("durable dispatch result is invalid: {error:?}"))?;
            let token = token_for(self.funnel.store().token_key(), &self.project_id, &claims);
            let mut tokens = self.tokens.lock().expect("token registry");
            if current {
                if let Some(previous) = tokens.by_unit.insert(claims.unit_id.clone(), token.clone())
                {
                    tokens.by_token.remove(&previous);
                }
                tokens.by_token.insert(token.clone(), claims);
            }
            object.insert("attempt_token".into(), Value::String(token));
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
        for field in ["version", "attempt_epoch", "stamp", "authority_epoch"] {
            if let Some(number) = object.get(field).and_then(Value::as_u64) {
                object.insert(field.into(), Value::String(number.to_string()));
            }
        }
        Ok(object.into_iter().collect())
    }
}

fn claims_from_result(
    result: &serde_json::Map<String, Value>,
) -> Result<AttemptTokenClaims, String> {
    let string = |field: &str| {
        result
            .get(field)
            .and_then(Value::as_str)
            .map(str::to_owned)
            .ok_or_else(|| format!("durable dispatch result has invalid {field}"))
    };
    let integer = |field: &str| {
        result
            .get(field)
            .and_then(Value::as_u64)
            .ok_or_else(|| format!("durable dispatch result has invalid {field}"))
    };
    Ok(AttemptTokenClaims {
        unit_id: string("unit_id")?,
        attempt_epoch: AttemptEpoch(integer("attempt_epoch")?),
        stamp: Stamp(integer("stamp")?),
        authority_epoch: AuthorityEpoch(integer("authority_epoch")?),
        holder_id: string("holder_id")?,
        nonce: string("token_nonce")?,
    })
}

fn token_for(key: &[u8; 32], project_id: &str, claims: &AttemptTokenClaims) -> String {
    let canonical = serde_json_canonicalizer::to_vec(&json!({
        "project_id": project_id,
        "unit_id": claims.unit_id,
        "attempt_epoch": claims.attempt_epoch.0.to_string(),
        "stamp": claims.stamp.0.to_string(),
        "authority_epoch": claims.authority_epoch.0.to_string(),
        "holder_id": claims.holder_id,
        "nonce": claims.nonce,
    }))
    .expect("token claims canonicalize");
    blake3::keyed_hash(key, &canonical).to_hex().to_string()
}

fn stringify_json_integers(value: &mut Value) {
    match value {
        Value::Number(number) if number.is_i64() || number.is_u64() => {
            *value = Value::String(number.to_string());
        }
        Value::Array(values) => values.iter_mut().for_each(stringify_json_integers),
        Value::Object(object) => object.values_mut().for_each(stringify_json_integers),
        _ => {}
    }
}

pub(super) fn format_timestamp(ms: u64) -> Result<String, String> {
    let ms = i64::try_from(ms).map_err(|_| "timestamp exceeds RFC 3339 range".to_owned())?;
    DateTime::<Utc>::from_timestamp_millis(ms)
        .ok_or_else(|| "timestamp exceeds RFC 3339 range".to_owned())
        .map(|timestamp| timestamp.format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string())
}
