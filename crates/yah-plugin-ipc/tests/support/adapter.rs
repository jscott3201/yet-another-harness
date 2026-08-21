//! The production adapter: applies model actions to a real
//! [`HostSession`] and lifts what comes back into the model's observable
//! fact types. The mapping is deliberately mechanical — wire enums to fact
//! enums, `AppError` discriminants to names — so the comparison sees only
//! what a host embedding the session could see.

#![allow(dead_code)]

use yah_plugin_ipc::frame;
use yah_plugin_ipc::session::{AppError, HostSession, SessionConfig, SessionEvent};
use yah_plugin_ipc::types::*;

use super::model_facts::{Action, EventFact, Kind, OutcomeFact, WOutcome, WireFact};

/// Ceilings the model mirrors exactly.
pub const WORKER_CALLS_MAX: u32 = 4;
pub const HOST_CALLS_MAX: u32 = 4;
pub const LIVE_HANDLES_MAX: u32 = 4;

pub fn model_config(budget: Option<u64>) -> SessionConfig {
    SessionConfig {
        features: Vec::new(),
        ceilings: Ceilings {
            host_calls_in_flight: HOST_CALLS_MAX,
            worker_calls_in_flight: WORKER_CALLS_MAX,
            live_handles: LIVE_HANDLES_MAX,
            initial_stream_credit: 16,
            max_stream_credit: 1024,
        },
        retired_operation_budget: budget,
    }
}

pub struct Adapter {
    pub session: HostSession,
    /// Real offers returned by `offer_artifact`, keyed by handle: a
    /// spilled reply must describe the held artifact exactly, and only
    /// the session knows the digest it minted.
    offers: std::collections::HashMap<u64, ArtifactOffer>,
    /// The most recent application refusal, drained by the harness as an
    /// `AppErr` event fact.
    pending_err: Option<&'static str>,
}

impl Adapter {
    pub fn new(budget: Option<u64>) -> Self {
        Self {
            session: HostSession::new(model_config(budget)),
            offers: std::collections::HashMap::new(),
            pending_err: None,
        }
    }

    pub fn apply(&mut self, action: &Action) {
        match *action {
            Action::Hello => self.feed(&WorkerMessage::Hello(Hello {
                protocol_versions: vec![1],
                sdk_name: "model".into(),
                sdk_version: "0".into(),
                features: vec![],
                required_features: vec![],
            })),
            Action::HelloBadVersion => self.feed(&WorkerMessage::Hello(Hello {
                protocol_versions: vec![2],
                sdk_name: "model".into(),
                sdk_version: "0".into(),
                features: vec![],
                required_features: vec![],
            })),
            Action::HelloUnknownRequired => self.feed(&WorkerMessage::Hello(Hello {
                protocol_versions: vec![1],
                sdk_name: "model".into(),
                sdk_version: "0".into(),
                features: vec![],
                required_features: vec!["no-such-feature".into()],
            })),
            Action::NonHelloFirst => self.feed(&WorkerMessage::Goodbye(Goodbye {
                reason: "early".into(),
            })),
            Action::HelloAgain => self.feed(&WorkerMessage::Hello(Hello {
                protocol_versions: vec![1],
                sdk_name: "model".into(),
                sdk_version: "0".into(),
                features: vec![],
                required_features: vec![],
            })),
            Action::WorkerCall { id, stream } => self.feed(&WorkerMessage::Call(Call {
                call_id: CallId(id),
                method: "tool.run".into(),
                deadline_ms: None,
                stream,
                payload: serde_json::json!(null),
            })),
            Action::ArtifactRead {
                id,
                handle,
                ok_range,
            } => {
                self.feed(&WorkerMessage::Call(Call {
                    call_id: CallId(id),
                    method: "artifact.read".into(),
                    deadline_ms: None,
                    stream: false,
                    payload: serde_json::json!({
                        "handle": handle, "offset": 0, "len": if ok_range { 1 } else { 0 }
                    }),
                }));
            }
            Action::WorkerReply { id, ref outcome } => {
                self.feed(&WorkerMessage::Reply(Reply {
                    call_id: CallId(id),
                    outcome: self.wire_outcome(outcome),
                }));
            }
            Action::StreamOpen { id, credit } => {
                self.feed(&WorkerMessage::StreamOpen(StreamOpen {
                    call_id: CallId(id),
                    credit,
                }))
            }
            Action::StreamData {
                id,
                seq,
                more,
                lossless,
                dropped,
            } => {
                self.feed(&WorkerMessage::StreamData(StreamData {
                    call_id: CallId(id),
                    seq,
                    more,
                    class: if lossless {
                        StreamClass::Lossless
                    } else {
                        StreamClass::Lossy
                    },
                    dropped,
                    payload: serde_json::json!(null),
                }));
            }
            Action::Credit { id, additional } => self.feed(&WorkerMessage::Credit(Credit {
                call_id: CallId(id),
                additional,
            })),
            Action::WorkerCancel { id, target_call } => {
                self.feed(&WorkerMessage::Cancel(Cancel {
                    call_id: CallId(id),
                    target: if target_call {
                        CancelTarget::Call
                    } else {
                        CancelTarget::Stream
                    },
                }));
            }
            Action::Release { handle, kind } => self.feed(&WorkerMessage::Release(Release {
                handle: HandleId(handle),
                kind: wire_kind(kind),
            })),
            Action::ReleaseAck { handle, kind } => {
                self.feed(&WorkerMessage::ReleaseAck(ReleaseAck {
                    handle: HandleId(handle),
                    kind: wire_kind(kind),
                }));
            }
            Action::Goodbye => self.feed(&WorkerMessage::Goodbye(Goodbye {
                reason: "done".into(),
            })),
            Action::Eof => self.session.end_of_input(),
            Action::HostCall { deadline_ms } => {
                let result =
                    self.session
                        .call_worker("m", serde_json::json!(null), deadline_ms, false);
                self.push_err(result.err());
            }
            Action::Tick { now_ms } => self.session.tick(now_ms),
            Action::HostCancel { id, target_call } => {
                let result = self.session.cancel(
                    CallId(id),
                    if target_call {
                        CancelTarget::Call
                    } else {
                        CancelTarget::Stream
                    },
                );
                self.push_err(result.err());
            }
            Action::AnswerWorkerCall { id, ref outcome } => {
                let outcome = match outcome {
                    WOutcome::Spilled { handle, bytes } => Outcome::Spilled {
                        artifact: self.offers.get(handle).map_or_else(
                            || ArtifactOffer {
                                handle: HandleId(*handle),
                                bytes: u64::from(*bytes),
                                media_type: "text/plain".into(),
                                digest_blake3: "a".repeat(64),
                            },
                            |offer| {
                                let mut offer = offer.clone();
                                offer.bytes = u64::from(*bytes);
                                offer
                            },
                        ),
                    },
                    other => self.wire_outcome(other),
                };
                let result = self.session.reply_to_worker(CallId(id), outcome);
                self.push_err(result.err());
            }
            Action::Mint { id } => {
                let result = self.session.mint_capability_handle(CallId(id));
                self.push_err(result.err());
            }
            Action::OfferArtifact { id, bytes } => {
                match self.session.offer_artifact(
                    CallId(id),
                    vec![0u8; bytes as usize],
                    "text/plain",
                ) {
                    Ok(offer) => {
                        self.offers.insert(offer.handle.0, offer);
                    }
                    Err(error) => self.push_err(Some(error)),
                }
            }
            Action::HostRelease { handle, kind } => {
                let result = self
                    .session
                    .release_worker_handle(HandleId(handle), wire_kind(kind));
                self.push_err(result.err());
            }
            Action::HostOpenStream { id, credit } => {
                let result = self.session.open_stream(CallId(id), credit);
                self.push_err(result.err());
            }
            Action::HostGrantCredit { id, additional } => {
                let result = self.session.grant_credit(CallId(id), additional);
                self.push_err(result.err());
            }
            Action::HostStreamItem { id, lossless, more } => {
                let result = self.session.stream_item(
                    CallId(id),
                    if lossless {
                        StreamClass::Lossless
                    } else {
                        StreamClass::Lossy
                    },
                    more,
                    serde_json::json!(null),
                );
                self.push_err(result.err());
            }
            Action::HostNoteDrops { id, dropped } => {
                let result = self.session.note_lossy_drops(CallId(id), dropped);
                self.push_err(result.err());
            }
        }
    }

    fn wire_outcome(&self, outcome: &WOutcome) -> Outcome {
        match *outcome {
            WOutcome::Ok => Outcome::Ok {
                result: serde_json::json!(null),
            },
            WOutcome::Spilled { handle, bytes } => Outcome::Spilled {
                artifact: ArtifactOffer {
                    handle: HandleId(handle),
                    bytes: u64::from(bytes),
                    media_type: "text/plain".into(),
                    digest_blake3: "a".repeat(64),
                },
            },
            WOutcome::ErrUnknownCall => Outcome::Err {
                error: WireError {
                    kind: WireErrorKind::UnknownCall,
                    message: "unknown-call".into(),
                    retryable: false,
                    reconcile_required: false,
                },
            },
            WOutcome::ErrInternal => Outcome::Err {
                error: WireError {
                    kind: WireErrorKind::Internal,
                    message: "internal".into(),
                    retryable: false,
                    reconcile_required: false,
                },
            },
            WOutcome::Cancelled => Outcome::Cancelled {
                reason: CancelReason::Requested,
            },
        }
    }

    fn feed(&mut self, message: &WorkerMessage) {
        let bytes = serde_json::to_vec(message).expect("model frames serialize");
        self.session.feed(&frame::encode(&bytes));
    }

    fn push_err(&mut self, error: Option<AppError>) {
        if let Some(error) = error {
            self.pending_err = Some(app_error_name(&error));
        }
    }

    /// The most recent application refusal, drained by the harness as an
    /// `AppErr` event fact.
    pub fn pending_err(&mut self) -> Option<&'static str> {
        self.pending_err.take()
    }

    pub fn drain(&mut self) -> (Vec<WireFact>, Vec<EventFact>) {
        let wire = self
            .session
            .drain_outbox()
            .into_iter()
            .map(wire_fact)
            .collect();
        let events = self
            .session
            .drain_events()
            .into_iter()
            .map(event_fact)
            .collect();
        (wire, events)
    }

    pub fn gauges(&self) -> (bool, u32, u64, (u32, u32), u32) {
        (
            self.session.is_closed(),
            self.session.live_handles(),
            self.session.retired_operations(),
            self.session.in_flight_calls(),
            self.session.pending_releases(),
        )
    }
}

pub fn app_error_name(error: &AppError) -> &'static str {
    match error {
        AppError::NotActive => "NotActive",
        AppError::UnknownCall => "UnknownCall",
        AppError::HandleCeiling => "HandleCeiling",
        AppError::CallCeiling => "CallCeiling",
        AppError::SpillRequired { .. } => "SpillRequired",
        AppError::PayloadTooLarge { .. } => "PayloadTooLarge",
        AppError::InvalidField(_) => "InvalidField",
        AppError::ReleasePending => "ReleasePending",
        AppError::AlreadyReleased => "AlreadyReleased",
        AppError::UnknownWorkerHandle => "UnknownWorkerHandle",
        AppError::StreamViolation(_) => "StreamViolation",
        AppError::AlreadySettled => "AlreadySettled",
        AppError::SessionRetired => "SessionRetired",
    }
}

fn wire_kind(kind: Kind) -> HandleKind {
    match kind {
        Kind::Capability => HandleKind::Capability,
        Kind::Artifact => HandleKind::Artifact,
    }
}

fn wire_fact(message: HostMessage) -> WireFact {
    match message {
        HostMessage::Accept(_) => WireFact::Accept,
        HostMessage::Refuse(refuse) => WireFact::Refuse(kind_str(refuse.error.kind)),
        HostMessage::Call(call) => WireFact::Call {
            call_id: call.call_id.0,
        },
        HostMessage::Reply(reply) => WireFact::Reply {
            call_id: reply.call_id.0,
            outcome: outcome_fact(&reply.outcome),
        },
        HostMessage::StreamOpen(open) => WireFact::StreamOpen {
            call_id: open.call_id.0,
            credit: open.credit,
        },
        HostMessage::StreamData(data) => WireFact::StreamData {
            call_id: data.call_id.0,
            seq: data.seq,
            more: data.more,
            lossless: matches!(data.class, StreamClass::Lossless),
            dropped: data.dropped,
        },
        HostMessage::Credit(credit) => WireFact::Credit {
            call_id: credit.call_id.0,
            additional: credit.additional,
        },
        HostMessage::Cancel(cancel) => WireFact::Cancel {
            call_id: cancel.call_id.0,
            target_call: matches!(cancel.target, CancelTarget::Call),
        },
        HostMessage::Release(release) => WireFact::Release {
            handle: release.handle.0,
            kind: fact_kind(release.kind),
        },
        HostMessage::ReleaseAck(ack) => WireFact::ReleaseAck {
            handle: ack.handle.0,
            kind: fact_kind(ack.kind),
        },
        HostMessage::Goodbye(_) => WireFact::Goodbye,
    }
}

fn event_fact(event: SessionEvent) -> EventFact {
    match event {
        SessionEvent::Negotiated { .. } => EventFact::Negotiated,
        SessionEvent::CallDelivered { call_id, .. } => {
            EventFact::CallDelivered { call_id: call_id.0 }
        }
        SessionEvent::HostCallSettled { call_id, outcome } => EventFact::HostCallSettled {
            call_id: call_id.0,
            outcome: outcome_fact(&outcome),
        },
        SessionEvent::HostCallLost {
            call_id,
            error,
            reconcile,
        } => EventFact::HostCallLost {
            call_id: call_id.0,
            kind: kind_str(error),
            reconcile,
        },
        SessionEvent::StreamOpened { call_id, credit } => EventFact::StreamOpened {
            call_id: call_id.0,
            credit,
        },
        SessionEvent::StreamItem {
            call_id,
            seq,
            more,
            class,
            dropped,
            ..
        } => EventFact::StreamItem {
            call_id: call_id.0,
            seq,
            more,
            lossless: matches!(class, StreamClass::Lossless),
            dropped,
        },
        SessionEvent::CreditGranted {
            call_id,
            additional,
        } => EventFact::CreditGranted {
            call_id: call_id.0,
            additional,
        },
        SessionEvent::CancelRequested { call_id, target } => EventFact::CancelRequested {
            call_id: call_id.0,
            target_call: matches!(target, CancelTarget::Call),
        },
        SessionEvent::HandleReleased { handle, kind } => EventFact::HandleReleased {
            handle: handle.0,
            kind: fact_kind(kind),
        },
        SessionEvent::WorkerHandleReleased { handle, kind } => EventFact::WorkerHandleReleased {
            handle: handle.0,
            kind: fact_kind(kind),
        },
        SessionEvent::HandlesReclaimed { handles } => EventFact::HandlesReclaimed {
            count: handles.len() as u32,
        },
        SessionEvent::WorkerGoodbye { .. } => EventFact::WorkerGoodbye,
        SessionEvent::CallRefused { call_id, kind } => EventFact::CallRefused {
            call_id: call_id.0,
            kind: kind_str(kind),
        },
        SessionEvent::Fatal { kind, .. } => EventFact::Fatal {
            kind: kind_str(kind),
        },
    }
}

fn outcome_fact(outcome: &Outcome) -> OutcomeFact {
    match outcome {
        Outcome::Ok { .. } => OutcomeFact::Ok,
        Outcome::Spilled { artifact } => OutcomeFact::Spilled {
            handle: artifact.handle.0,
        },
        Outcome::Err { error } => OutcomeFact::Err {
            kind: kind_str(error.kind),
            retryable: error.retryable,
        },
        Outcome::Cancelled { .. } => OutcomeFact::Cancelled,
    }
}

fn fact_kind(kind: HandleKind) -> Kind {
    match kind {
        HandleKind::Capability => Kind::Capability,
        HandleKind::Artifact => Kind::Artifact,
    }
}

pub fn kind_str(kind: WireErrorKind) -> &'static str {
    match kind {
        WireErrorKind::UnsupportedVersion => "unsupported-version",
        WireErrorKind::UnknownRequiredFeature => "unknown-required-feature",
        WireErrorKind::NegotiationRequired => "negotiation-required",
        WireErrorKind::InvalidFrame => "invalid-frame",
        WireErrorKind::FrameTooLarge => "frame-too-large",
        WireErrorKind::PayloadTooLarge => "payload-too-large",
        WireErrorKind::UnknownCall => "unknown-call",
        WireErrorKind::DuplicateCall => "duplicate-call",
        WireErrorKind::UnknownMethod => "unknown-method",
        WireErrorKind::ResourceExhausted => "resource-exhausted",
        WireErrorKind::DeadlineExceeded => "deadline-exceeded",
        WireErrorKind::UnknownHandle => "unknown-handle",
        WireErrorKind::InvalidRead => "invalid-read",
        WireErrorKind::OutcomeUnknown => "outcome-unknown",
        WireErrorKind::Internal => "internal",
        WireErrorKind::Cancelled => "cancelled",
    }
}
