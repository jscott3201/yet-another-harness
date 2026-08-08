use super::json::{WireLimitError, decode_client_message, validate_wire_limits};
use super::translate::{into_funnel, validate_envelope};
use super::types::*;
use crate::error::ErrorKind as KernelErrorKind;
use crate::funnel::{Funnel, Submission};
use crate::ids::{AttemptEpoch, AuthorityEpoch, Digest, Stamp};
use crate::store::{AttemptTokenClaims, StoreError};
use serde::Serialize;
use serde_json::{Value, json};
use std::collections::{BTreeMap, VecDeque};
use std::sync::{Arc, Mutex};

mod project;
mod subscription;

pub struct InProcessAdapter {
    funnel: Funnel,
    project_id: String,
    tokens: Mutex<TokenRegistry>,
    subscriptions: Mutex<BTreeMap<String, DurableSubscription>>,
    next_subscription_id: Mutex<u64>,
    command_gate: Mutex<()>,
    stream_gate: Mutex<()>,
    published_cursor: Mutex<u64>,
}

#[derive(Default)]
struct TokenRegistry {
    by_token: BTreeMap<String, AttemptTokenClaims>,
    by_unit: BTreeMap<String, String>,
}

pub(super) struct DurableSubscription {
    queue: VecDeque<(Arc<Event>, usize)>,
    queued_bytes: usize,
    last_queued_cursor: u64,
    last_delivered_cursor: u64,
    available_cursor: u64,
    closed: Option<SubscriptionClosure>,
}

pub(super) enum SubscriptionClosure {
    SlowConsumer(SlowConsumer),
    Internal(String),
}

impl InProcessAdapter {
    pub fn new(funnel: Funnel, project_id: impl Into<String>) -> Result<Self, Error> {
        let project_id = project_id.into();
        if funnel.store().project_id() != project_id {
            return Err(protocol_error(
                ErrorKind::InvalidRequest,
                "adapter project must match the durable control graph",
            ));
        }
        let published_cursor = funnel.store().latest_cursor();
        let adapter = Self {
            funnel,
            project_id,
            tokens: Mutex::new(TokenRegistry::default()),
            subscriptions: Mutex::new(BTreeMap::new()),
            next_subscription_id: Mutex::new(0),
            command_gate: Mutex::new(()),
            stream_gate: Mutex::new(()),
            published_cursor: Mutex::new(published_cursor),
        };
        adapter.validate_durable_events()?;
        adapter.validate_durable_receipts()?;
        Ok(adapter)
    }

    pub fn handle_json(&self, bytes: &[u8]) -> Vec<u8> {
        let response = match validate_wire_limits(bytes) {
            Err(WireLimitError::Frame) => ServerMessage::Error(protocol_error(
                ErrorKind::PayloadTooLarge,
                "encoded protocol frame exceeds its limit",
            )),
            Err(WireLimitError::Command) => ServerMessage::Error(protocol_error(
                ErrorKind::PayloadTooLarge,
                "encoded command exceeds 1 MiB",
            )),
            Err(WireLimitError::Payload) => ServerMessage::Error(protocol_error(
                ErrorKind::PayloadTooLarge,
                "encoded command payload exceeds 256 KiB",
            )),
            Err(WireLimitError::Invalid(error)) => ServerMessage::Error(protocol_error(
                ErrorKind::InvalidRequest,
                &format!("invalid JSON message: {error}"),
            )),
            Ok(()) => match decode_client_message(bytes) {
                Ok(message) => self.handle(message),
                Err(e) => ServerMessage::Error(protocol_error(
                    ErrorKind::InvalidRequest,
                    &format!("invalid JSON message: {e}"),
                )),
            },
        };
        serde_json::to_vec(&response).expect("server message serializes")
    }

    pub fn submit(&self, command: &Command) -> Result<Receipt, Error> {
        let request = serde_json::to_vec(&ClientMessage::Command(Box::new(command.clone())))
            .map_err(|error| {
                protocol_error(
                    ErrorKind::Internal,
                    &format!("typed command does not serialize: {error}"),
                )
            })?;
        let response = self.handle_json(&request);
        match serde_json::from_slice(&response).map_err(|error| {
            protocol_error(
                ErrorKind::Internal,
                &format!("adapter emitted invalid JSON: {error}"),
            )
        })? {
            ServerMessage::Receipt(receipt) => Ok(receipt),
            ServerMessage::Error(error) => Err(error),
            other => Err(protocol_error(
                ErrorKind::Internal,
                &format!("command returned non-receipt: {other:?}"),
            )),
        }
    }

    pub fn resume(&self, after_cursor: u64) -> Result<Vec<Event>, Error> {
        let request = serde_json::to_vec(&ClientMessage::Resume {
            project_id: self.project_id.clone(),
            after_cursor: DecimalU64::new(after_cursor),
        })
        .expect("resume serializes");
        match serde_json::from_slice(&self.handle_json(&request)).expect("valid response") {
            ServerMessage::Events(events) => Ok(events),
            ServerMessage::CursorExpired(expired) => Err(protocol_error(
                ErrorKind::CursorExpired,
                &format!(
                    "cursor is older than retained floor {}",
                    expired.min_retained_cursor.get()
                ),
            )),
            ServerMessage::Error(error) => Err(error),
            other => Err(protocol_error(
                ErrorKind::Internal,
                &format!("resume returned non-stream response: {other:?}"),
            )),
        }
    }

    pub fn set_min_retained_cursor(&self, cursor: u64) -> Result<(), Error> {
        let _stream = self.stream_gate.lock().expect("stream gate");
        if let Some(detail) = self.funnel.poison_detail() {
            return Err(protocol_error(ErrorKind::Unavailable, &detail));
        }
        match self.funnel.store().set_min_retained_cursor(cursor) {
            Ok(()) => Ok(()),
            Err(StoreError::Rejected(crate::store::StoreRejection::InvalidRequest { detail })) => {
                Err(protocol_error(ErrorKind::InvalidRequest, &detail))
            }
            Err(StoreError::CommitUnknown(detail)) => {
                self.funnel.poison(detail.clone());
                Err(protocol_error(ErrorKind::OutcomeUnknown, &detail))
            }
            Err(error) => Err(protocol_error(
                ErrorKind::Internal,
                &format!("cannot persist retention floor: {error:?}"),
            )),
        }
    }

    fn handle(&self, message: ClientMessage) -> ServerMessage {
        match message {
            ClientMessage::Command(command) => {
                ServerMessage::Receipt(self.handle_command(*command))
            }
            ClientMessage::Resume {
                project_id,
                after_cursor,
            } => self.handle_resume(&project_id, after_cursor.get()),
            ClientMessage::Subscribe {
                project_id,
                after_cursor,
            } => self.handle_subscribe(&project_id, after_cursor.get()),
            ClientMessage::SubscriptionPoll { subscription_id } => {
                self.handle_subscription_poll(&subscription_id)
            }
            ClientMessage::SubscriptionClose { subscription_id } => {
                self.handle_subscription_close(&subscription_id)
            }
        }
    }

    fn handle_command(&self, command: Command) -> Receipt {
        let _command = self.command_gate.lock().expect("command gate");
        let request_digest = match validate_request_digest(&command) {
            Ok(digest) => digest,
            Err(detail) => {
                return self.rejection_receipt(Some(&command), ErrorKind::InvalidRequest, &detail);
            }
        };
        if command.scope.scope_kind != ScopeKind::Global {
            let current = self.funnel.store().authority_epoch();
            match command.authority_epoch.as_ref().map(DecimalU64::get) {
                Some(epoch) if epoch == current.0 => {}
                Some(epoch) => {
                    return self.rejection_receipt(
                        Some(&command),
                        ErrorKind::FenceRejected,
                        &format!(
                            "authority epoch {epoch} does not match current {}",
                            current.0
                        ),
                    );
                }
                None => {
                    return self.rejection_receipt(
                        Some(&command),
                        ErrorKind::InvalidRequest,
                        "control-graph command requires authority_epoch",
                    );
                }
            }
        }
        if let Err(detail) = validate_envelope(&command) {
            let holder_command = matches!(command.body, CommandBody::ProgressReport(_));
            let claims = holder_command
                .then(|| self.resolve_token(&command).ok().flatten())
                .flatten();
            if claims.as_ref().is_some_and(|claims| {
                command.scope.scope_kind != ScopeKind::Unit
                    || command.scope.scope_id != claims.unit_id
                    || command.target.aggregate_kind != "unit"
                    || command.target.aggregate_id != claims.unit_id
                    || !matches!(
                        &command.body,
                        CommandBody::ProgressReport(payload) if payload.unit_id == claims.unit_id
                    )
            }) {
                return self.rejection_receipt(
                    Some(&command),
                    ErrorKind::FenceRejected,
                    "attempt_token is sealed for another unit",
                );
            }
            let principal_is_known = !holder_command || claims.is_some();
            return if principal_is_known {
                self.reject_keyed(
                    &command,
                    request_digest,
                    claims,
                    KernelErrorKind::InvalidRequest,
                    &detail,
                )
            } else {
                self.rejection_receipt(Some(&command), ErrorKind::InvalidRequest, &detail)
            };
        }
        if matches!(command.body, CommandBody::WorkItemCreate(_))
            && (command.scope.scope_kind != ScopeKind::Project
                || command.scope.scope_id != self.project_id)
        {
            return self.reject_keyed(
                &command,
                request_digest,
                None,
                KernelErrorKind::InvalidRequest,
                "work_item.create requires the current project receipt scope",
            );
        }
        if command.scope.scope_kind == ScopeKind::Project
            && command.scope.scope_id != self.project_id
        {
            return self.reject_keyed(
                &command,
                request_digest,
                None,
                KernelErrorKind::InvalidRequest,
                "command scope does not match the control graph project",
            );
        }
        let claims = match self.resolve_token(&command) {
            Ok(claims) => claims,
            Err(detail) => {
                return self.rejection_receipt(Some(&command), ErrorKind::FenceRejected, &detail);
            }
        };
        let internal = match into_funnel(&command, claims.clone()) {
            Ok(command) => command,
            Err(detail) => {
                return self.reject_keyed(
                    &command,
                    request_digest,
                    claims,
                    KernelErrorKind::InvalidRequest,
                    &detail,
                );
            }
        };
        let submission = self.funnel.submit(&internal);
        let receipt = self.project_submission(&command, submission);
        if let Some(cursor) = receipt.event_cursors.last() {
            self.publish_through(cursor.get());
        }
        receipt
    }

    fn handle_resume(&self, project_id: &str, after_cursor: u64) -> ServerMessage {
        if project_id != self.project_id {
            return ServerMessage::Error(protocol_error(
                ErrorKind::InvalidRequest,
                "cursor belongs to a different project",
            ));
        }
        let _stream = self.stream_gate.lock().expect("stream gate");
        if let Some(detail) = self.funnel.poison_detail() {
            return ServerMessage::Error(protocol_error(ErrorKind::Unavailable, &detail));
        }
        let min_retained_cursor = self.funnel.store().min_retained_cursor();
        if min_retained_cursor > 1 && after_cursor < min_retained_cursor {
            return ServerMessage::CursorExpired(CursorExpired {
                min_retained_cursor: DecimalU64::new(min_retained_cursor),
            });
        }
        let retained = match self
            .funnel
            .store()
            .events_after_limit(after_cursor, super::MAX_RESUME_EVENTS + 1)
        {
            Ok(events) => events,
            Err(error) => {
                let detail = format!("durable event stream is unreadable: {error:?}");
                self.funnel.poison(detail.clone());
                return ServerMessage::Error(protocol_error(ErrorKind::Internal, &detail));
            }
        };
        if retained.is_empty() && after_cursor > self.latest_cursor() {
            return ServerMessage::Error(protocol_error(
                ErrorKind::InvalidRequest,
                "cursor is ahead of the project journal",
            ));
        }
        if retained.len() > super::MAX_RESUME_EVENTS {
            return ServerMessage::Error(protocol_error(
                ErrorKind::ResourceExhausted,
                "resume exceeds 1024 events; use a durable subscription",
            ));
        }
        let mut events = Vec::with_capacity(retained.len());
        let mut encoded_bytes = serde_json::to_vec(&ServerMessage::Events(Vec::new()))
            .expect("empty events response serializes")
            .len();
        for record in retained {
            let event = match self.event(record) {
                Ok(event) => event,
                Err(detail) => {
                    self.funnel.poison(detail.clone());
                    return ServerMessage::Error(protocol_error(ErrorKind::Internal, &detail));
                }
            };
            let event_bytes = serde_json::to_vec(&event)
                .expect("projected event serializes")
                .len();
            let next_bytes = encoded_bytes
                .checked_add(usize::from(!events.is_empty()))
                .and_then(|bytes| bytes.checked_add(event_bytes));
            if next_bytes.is_none_or(|bytes| bytes > super::MAX_RESUME_BYTES) {
                return ServerMessage::Error(protocol_error(
                    ErrorKind::ResourceExhausted,
                    "resume exceeds the response byte limit; use a durable subscription",
                ));
            }
            encoded_bytes = next_bytes.expect("response byte total checked");
            events.push(event);
        }
        ServerMessage::Events(events)
    }

    pub(super) fn latest_cursor(&self) -> u64 {
        self.funnel.store().latest_cursor()
    }

    fn reject_keyed(
        &self,
        command: &Command,
        request_digest: Digest,
        claims: Option<AttemptTokenClaims>,
        kind: KernelErrorKind,
        detail: &str,
    ) -> Receipt {
        let principal = claims.as_ref().map_or_else(
            || (crate::funnel::PrincipalKind::Daemon, "daemon-local".into()),
            |claims| {
                (
                    crate::funnel::PrincipalKind::Agent,
                    claims.holder_id.clone(),
                )
            },
        );
        let internal = crate::funnel::Command {
            command_id: command.command_id.clone(),
            scope_kind: match command.scope.scope_kind {
                ScopeKind::Global => crate::funnel::ScopeKind::Global,
                ScopeKind::Project => crate::funnel::ScopeKind::Project,
                ScopeKind::Run => crate::funnel::ScopeKind::Run,
                ScopeKind::Unit => crate::funnel::ScopeKind::Unit,
            },
            scope_id: command.scope.scope_id.clone(),
            request_digest,
            expected_version: None,
            principal_kind: principal.0,
            principal_id: principal.1,
            authority_epoch: command
                .authority_epoch
                .as_ref()
                .map(|epoch| AuthorityEpoch(epoch.get())),
            attempt_token: claims,
            causation_id: command.causation_id.clone(),
            correlation_id: command.correlation_id.clone(),
            method: crate::funnel::Method::ProgressReport {
                unit_id: command.target.aggregate_id.clone(),
            },
        };
        let submission = self.funnel.reject_keyed(&internal, kind, detail);
        self.project_submission(command, submission)
    }

    fn resolve_token(&self, command: &Command) -> Result<Option<AttemptTokenClaims>, String> {
        command
            .attempt_token
            .as_ref()
            .map(|token| {
                self.tokens
                    .lock()
                    .expect("token registry")
                    .by_token
                    .get(token)
                    .cloned()
                    .ok_or_else(|| "attempt_token is stale or unresolvable".into())
            })
            .transpose()
    }

    fn validate_durable_events(&self) -> Result<(), Error> {
        let mut cursor = 0;
        loop {
            let records = self
                .funnel
                .store()
                .events_after_limit(cursor, super::MAX_RESUME_EVENTS)
                .map_err(|error| {
                    protocol_error(
                        ErrorKind::Internal,
                        &format!("durable event stream is unreadable: {error:?}"),
                    )
                })?;
            if records.is_empty() {
                return Ok(());
            }
            for record in records {
                cursor = record.cursor;
                self.event(record)
                    .map_err(|detail| protocol_error(ErrorKind::Internal, &detail))?;
            }
        }
    }

    fn validate_durable_receipts(&self) -> Result<(), Error> {
        for key in self.funnel.store().receipt_keys() {
            self.funnel.store().receipt(&key).map_err(|error| {
                protocol_error(
                    ErrorKind::Internal,
                    &format!("durable receipt is unreadable: {error:?}"),
                )
            })?;
        }
        self.funnel
            .store()
            .validate_event_receipts()
            .map_err(|error| {
                protocol_error(
                    ErrorKind::Internal,
                    &format!("durable event ownership is invalid: {error:?}"),
                )
            })?;
        Ok(())
    }
}

pub fn request_digest(command: &Command) -> Result<Digest, serde_json::Error> {
    #[derive(Serialize)]
    struct Preimage<'a> {
        command_type: &'a str,
        payload_schema_version: BoundedU32,
        target: &'a Target,
        expected_versions: &'a [ExpectedVersion],
        payload: Value,
    }
    let command_type = command.body.command_type();
    let bytes = serde_json_canonicalizer::to_vec(&Preimage {
        command_type: command_type.wire(),
        payload_schema_version: command.payload_schema_version,
        target: &command.target,
        expected_versions: &command.expected_versions,
        payload: command.body.payload_json(),
    })?;
    Ok(Digest::of_bytes(&bytes))
}

fn validate_request_digest(command: &Command) -> Result<Digest, String> {
    let claimed = Digest::try_from(command.request_digest.clone())?;
    let actual =
        request_digest(command).map_err(|e| format!("cannot canonicalize request: {e}"))?;
    if claimed != actual {
        return Err("request_digest does not match canonical command preimage".into());
    }
    Ok(claimed)
}

pub(super) fn protocol_error(kind: ErrorKind, detail: &str) -> Error {
    let detail: String = detail.chars().take(super::MAX_ERROR_DETAIL_CHARS).collect();
    let retryable = matches!(
        kind,
        ErrorKind::VersionConflict
            | ErrorKind::ApprovalRequired
            | ErrorKind::ResourceExhausted
            | ErrorKind::DeadlineExceeded
            | ErrorKind::Unavailable
    );
    Error {
        kind,
        reason: "protocol.rejected".into(),
        message: detail.clone(),
        retryable,
        retry_after_ms: None,
        reconcile_required: kind == ErrorKind::OutcomeUnknown,
        occurrence_id: format!(
            "error:{}.{}",
            kind as u8,
            &blake3::hash(detail.as_bytes()).to_hex()[..48]
        ),
        details: Some(BTreeMap::from([("detail".into(), Value::String(detail))])),
    }
}

impl From<KernelErrorKind> for ErrorKind {
    fn from(kind: KernelErrorKind) -> Self {
        match kind {
            KernelErrorKind::InvalidRequest => Self::InvalidRequest,
            KernelErrorKind::Unauthenticated => Self::Unauthenticated,
            KernelErrorKind::Unauthorized => Self::Unauthorized,
            KernelErrorKind::NotFound => Self::NotFound,
            KernelErrorKind::VersionConflict => Self::VersionConflict,
            KernelErrorKind::IdempotencyConflict => Self::IdempotencyConflict,
            KernelErrorKind::FenceRejected => Self::FenceRejected,
            KernelErrorKind::JournalImmutable => Self::JournalImmutable,
            KernelErrorKind::PolicyDenied => Self::PolicyDenied,
            KernelErrorKind::CapabilityUnsupported => Self::CapabilityUnsupported,
            KernelErrorKind::ApprovalRequired => Self::ApprovalRequired,
            KernelErrorKind::ResourceExhausted => Self::ResourceExhausted,
            KernelErrorKind::PayloadTooLarge => Self::PayloadTooLarge,
            KernelErrorKind::CursorExpired => Self::CursorExpired,
            KernelErrorKind::SlowConsumer => Self::SlowConsumer,
            KernelErrorKind::Cancelled => Self::Cancelled,
            KernelErrorKind::DeadlineExceeded => Self::DeadlineExceeded,
            KernelErrorKind::Unavailable => Self::Unavailable,
            KernelErrorKind::OutcomeUnknown => Self::OutcomeUnknown,
            KernelErrorKind::Internal => Self::Internal,
        }
    }
}

#[cfg(test)]
#[path = "adapter/tests.rs"]
mod tests;
