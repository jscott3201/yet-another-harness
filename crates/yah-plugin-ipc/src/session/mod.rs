//! The host side of one worker session, as a pure state machine.
//!
//! Bytes go in through [`HostSession::feed`], time goes in through
//! [`HostSession::tick`], and two queues come out: frames to transmit
//! ([`HostSession::drain_outbox`]) and facts for the embedding host
//! ([`HostSession::drain_events`]). Nothing here does IO, sleeps, or reads
//! a clock, which is what lets every fixture in `tests/` drive races —
//! completion against cancel, release against disconnect, deadline against
//! reply — deterministically, byte by byte.
//!
//! The embedding host is the application layer: it answers worker calls
//! (capability semantics live there, behind the broker), initiates its own
//! calls, and grants credit. The session enforces only protocol law —
//! negotiation order, bounds, ceilings, id spaces, terminal-exactly-once,
//! credit, handle lifetime — and every law it enforces has a fixture.
//!
//! Faults split two ways. A refusable fault answers one call with a
//! [`WireErrorKind`] and the session continues. A fatal fault poisons the
//! session: the host says goodbye, in-flight host calls settle
//! outcome-unknown (the worker may have acted), and every later input is
//! ignored. There is no resync path on purpose — after a framing or id
//! violation, later bytes are unattributable.

mod calls;
mod handles;
mod negotiate;
mod streams;

use crate::frame::{EndOfInput, FrameDecoder, FrameStreamError};
use crate::types::*;
use crate::{
    DEFAULT_HOST_CALLS_IN_FLIGHT, DEFAULT_LIVE_HANDLES, DEFAULT_WORKER_CALLS_IN_FLIGHT,
    INITIAL_STREAM_CREDIT, MAX_ARTIFACT_READ_BYTES, MAX_CALL_PAYLOAD_BYTES,
    MAX_CONTROL_FRAME_BYTES, MAX_FRAME_BYTES, MAX_INLINE_RESULT_BYTES, MAX_STREAM_DATA_BYTES,
};
use std::collections::{BTreeMap, BTreeSet};

/// Everything negotiable about a session, with the crate defaults.
#[derive(Clone, Debug)]
pub struct SessionConfig {
    pub features: Vec<String>,
    pub ceilings: Ceilings,
}

impl Default for SessionConfig {
    fn default() -> Self {
        Self {
            features: Vec::new(),
            ceilings: Ceilings {
                host_calls_in_flight: DEFAULT_HOST_CALLS_IN_FLIGHT,
                worker_calls_in_flight: DEFAULT_WORKER_CALLS_IN_FLIGHT,
                live_handles: DEFAULT_LIVE_HANDLES,
                initial_stream_credit: INITIAL_STREAM_CREDIT,
            },
        }
    }
}

/// The wire limits this build enforces, in the shape the accept frame
/// announces them.
pub fn wire_limits() -> WireLimits {
    WireLimits {
        max_frame_bytes: MAX_FRAME_BYTES as u64,
        max_control_frame_bytes: MAX_CONTROL_FRAME_BYTES as u64,
        max_call_payload_bytes: MAX_CALL_PAYLOAD_BYTES as u64,
        max_inline_result_bytes: MAX_INLINE_RESULT_BYTES as u64,
        max_stream_data_bytes: MAX_STREAM_DATA_BYTES as u64,
        max_artifact_read_bytes: MAX_ARTIFACT_READ_BYTES as u64,
    }
}

/// A fact the session established, for the embedding host.
#[derive(Clone, Debug, PartialEq)]
pub enum SessionEvent {
    /// Negotiation completed; the accept has been queued.
    Negotiated {
        sdk_name: String,
        sdk_version: String,
    },
    /// A worker call passed every protocol check and awaits an answer.
    CallDelivered {
        call_id: CallId,
        method: String,
        payload: serde_json::Value,
        stream: bool,
        deadline_ms: Option<u32>,
    },
    /// A host call received its terminal frame.
    HostCallSettled { call_id: CallId, outcome: Outcome },
    /// A host call was settled locally without a worker terminal:
    /// deadline, fatal fault, or disconnect. `reconcile` mirrors
    /// outcome-unknown: the worker may have acted.
    HostCallLost {
        call_id: CallId,
        error: WireErrorKind,
        reconcile: bool,
    },
    /// The worker acked a streaming host call and granted credit.
    StreamOpened { call_id: CallId, credit: u32 },
    /// One stream item on a host call, already validated.
    StreamItem {
        call_id: CallId,
        seq: u64,
        more: bool,
        class: StreamClass,
        dropped: u64,
        payload: serde_json::Value,
    },
    /// The worker granted the host more credit on a worker call's stream.
    CreditGranted { call_id: CallId, additional: u32 },
    /// The worker asked to cancel a call the host is serving, or to stop
    /// its stream. The call still owes its terminal reply.
    CancelRequested {
        call_id: CallId,
        target: CancelTarget,
    },
    /// The worker released a handle; the ack has been queued.
    HandleReleased { handle: HandleId, kind: HandleKind },
    /// The session reclaimed handles without a release frame: auto-release
    /// on a failed minting call, goodbye, disconnect, or fatal fault.
    HandlesReclaimed { count: u32 },
    /// The worker said goodbye; in-flight work settles as cancelled.
    WorkerGoodbye { reason: String },
    /// A refusable protocol fault was answered on one call.
    CallRefused {
        call_id: CallId,
        kind: WireErrorKind,
    },
    /// The session hit a fatal fault and is closed.
    Fatal { kind: WireErrorKind, detail: String },
}

/// Session lifecycle.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Phase {
    AwaitingHello,
    Active,
    /// Closed cleanly (goodbye either way) or by fatal fault.
    Closed,
}

/// One worker-initiated call the host is serving.
#[derive(Debug)]
struct WorkerCall {
    stream: bool,
    /// Set once the host app opened the stream: credit the host may still
    /// spend sending lossless items.
    stream_credit: Option<u32>,
    next_seq: u64,
    lossy_dropped: u64,
    last_item_sent: bool,
    /// Handles minted while serving this call, reclaimed if its terminal
    /// outcome is err or cancelled: a refused or cancelled acquire must not
    /// leak what it briefly held.
    minted: Vec<HandleId>,
}

/// One host-initiated call in flight toward the worker.
#[derive(Debug)]
struct HostCall {
    stream: bool,
    stream_open: bool,
    /// Credit the worker may still spend on lossless items toward us.
    credit_left: u32,
    highest_seq: Option<u64>,
    lossy_dropped: u64,
    last_item_seen: bool,
    /// Deadline instant in session time, if the call carries a budget.
    deadline_at: Option<u64>,
    /// The host asked for the stream to stop; items are validated but not
    /// delivered as events.
    stream_muted: bool,
}

/// A live handle the host owns on the worker's behalf.
#[derive(Debug)]
struct Handle {
    kind: HandleKind,
    /// Present for artifact handles: the offered bytes, held for pull-reads.
    artifact: Option<ArtifactBytes>,
}

#[derive(Debug)]
struct ArtifactBytes {
    bytes: Vec<u8>,
    media_type: String,
}

/// Refused by the session's own application-facing API — the host asked for
/// something protocol law forbids.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AppError {
    /// The session is not active.
    NotActive,
    /// The named call is not in flight.
    UnknownCall,
    /// The live-handle ceiling is reached; the grant must be refused.
    HandleCeiling,
    /// The host's own in-flight ceiling is reached; initiate later.
    CallCeiling,
    /// An inline result over the ceiling; spill it instead.
    SpillRequired { bytes: usize },
    /// A stream item with no credit left, a credit grant over the ceiling,
    /// a second stream open, or an item after the last one.
    StreamViolation(&'static str),
    /// A reply already went out for that call.
    AlreadySettled,
}

/// The host side of one worker connection.
#[derive(Debug)]
pub struct HostSession {
    config: SessionConfig,
    phase: Phase,
    decoder: FrameDecoder,
    now_ms: u64,
    worker_calls: BTreeMap<CallId, WorkerCall>,
    host_calls: BTreeMap<CallId, HostCall>,
    /// Worker call ids that reached a terminal state. An id is spent for
    /// the session once answered — reuse is the same correlation break as
    /// a duplicate in flight.
    retired_worker_calls: BTreeSet<CallId>,
    /// Host call ids that reached a terminal state; a late worker reply to
    /// one is a tolerated race, a reply to an id in neither map is forgery.
    retired_host_calls: BTreeSet<CallId>,
    next_host_call: u64,
    handles: BTreeMap<HandleId, Handle>,
    /// Ids that must never be reused within the session, released or
    /// reclaimed alike — the release-ack contract.
    retired_handles: BTreeSet<HandleId>,
    next_handle: u64,
    live_capability_handles: u32,
    outbox: Vec<HostMessage>,
    events: Vec<SessionEvent>,
}

impl HostSession {
    pub fn new(config: SessionConfig) -> Self {
        Self {
            config,
            phase: Phase::AwaitingHello,
            decoder: FrameDecoder::new(),
            now_ms: 0,
            worker_calls: BTreeMap::new(),
            host_calls: BTreeMap::new(),
            retired_worker_calls: BTreeSet::new(),
            retired_host_calls: BTreeSet::new(),
            next_host_call: 1,
            handles: BTreeMap::new(),
            retired_handles: BTreeSet::new(),
            next_handle: 1,
            live_capability_handles: 0,
            outbox: Vec::new(),
            events: Vec::new(),
        }
    }

    /// Frames queued for the transport, in order.
    pub fn drain_outbox(&mut self) -> Vec<HostMessage> {
        std::mem::take(&mut self.outbox)
    }

    /// Facts established since the last drain, in order.
    pub fn drain_events(&mut self) -> Vec<SessionEvent> {
        std::mem::take(&mut self.events)
    }

    /// Live handles the worker holds right now. The deterministic-release
    /// fixtures read this while the session is active: after teardown a
    /// zero is unconditional and proves nothing.
    pub fn live_handles(&self) -> u32 {
        self.live_capability_handles + self.artifact_handle_count()
    }

    pub fn is_closed(&self) -> bool {
        self.phase == Phase::Closed
    }

    /// Feed transport bytes; chunk boundaries are meaningless.
    pub fn feed(&mut self, bytes: &[u8]) {
        if self.phase == Phase::Closed {
            return;
        }
        self.decoder.feed(bytes);
        loop {
            match self.decoder.next_frame() {
                Ok(Some(frame)) => {
                    self.on_frame(&frame);
                    if self.phase == Phase::Closed {
                        return;
                    }
                }
                Ok(None) => return,
                Err(error) => {
                    let kind = match error {
                        FrameStreamError::FrameTooLarge { .. } => WireErrorKind::FrameTooLarge,
                        FrameStreamError::EmptyFrame => WireErrorKind::InvalidFrame,
                    };
                    self.fatal(kind, "frame stream violation");
                    return;
                }
            }
        }
    }

    /// The transport reached end-of-input. A clean end after a goodbye is
    /// an orderly close; anything else is a lost worker, and every
    /// in-flight host call settles outcome-unknown — a disconnect is loss
    /// of the worker, never proof that its external actions failed.
    pub fn end_of_input(&mut self) {
        if self.phase == Phase::Closed {
            return;
        }
        let truncated = !matches!(self.decoder.finish(), EndOfInput::Clean);
        let kind = if truncated {
            WireErrorKind::InvalidFrame
        } else {
            WireErrorKind::OutcomeUnknown
        };
        let detail = if truncated {
            "worker disconnected mid-frame"
        } else {
            "worker disconnected"
        };
        self.fatal(kind, detail);
    }

    /// Advance session time; expired budgets settle their calls.
    pub fn tick(&mut self, now_ms: u64) {
        self.now_ms = self.now_ms.max(now_ms);
        if self.phase != Phase::Active {
            return;
        }
        self.expire_deadlines();
    }

    /// One decoded frame. Length-class checks happen here, where the raw
    /// size is still known.
    fn on_frame(&mut self, raw: &[u8]) {
        let value = match crate::strict::parse(raw) {
            Ok(value) => value,
            Err(error) => {
                self.fatal(WireErrorKind::InvalidFrame, &error.to_string());
                return;
            }
        };
        let message: WorkerMessage = match serde_json::from_value(value) {
            Ok(message) => message,
            Err(error) => {
                // Covers unknown frame tags, unknown enum members, unknown
                // fields, and shape violations alike: closed within a
                // version, refused as one kind.
                self.fatal(WireErrorKind::InvalidFrame, &error.to_string());
                return;
            }
        };
        if is_control(&message) && raw.len() > MAX_CONTROL_FRAME_BYTES {
            self.fatal(WireErrorKind::FrameTooLarge, "control frame over bound");
            return;
        }
        match self.phase {
            Phase::AwaitingHello => self.on_pre_negotiation(message),
            Phase::Active => self.on_active(message),
            Phase::Closed => {}
        }
    }

    fn on_active(&mut self, message: WorkerMessage) {
        match message {
            WorkerMessage::Hello(_) => {
                self.fatal(WireErrorKind::NegotiationRequired, "second hello")
            }
            WorkerMessage::Call(call) => self.on_worker_call(call),
            WorkerMessage::Reply(reply) => self.on_worker_reply(reply),
            WorkerMessage::StreamOpen(open) => self.on_stream_open(open),
            WorkerMessage::StreamData(data) => self.on_stream_data(data),
            WorkerMessage::Credit(credit) => self.on_credit(credit),
            WorkerMessage::Cancel(cancel) => self.on_worker_cancel(cancel),
            WorkerMessage::Release(release) => self.on_release(release),
            WorkerMessage::ReleaseAck(_) => {
                // The host releases nothing worker-owned in v1; an ack out
                // of nowhere is a desync.
                self.fatal(WireErrorKind::UnknownHandle, "unsolicited release-ack")
            }
            WorkerMessage::Goodbye(goodbye) => self.on_goodbye(goodbye),
        }
    }

    fn on_goodbye(&mut self, goodbye: Goodbye) {
        self.events.push(SessionEvent::WorkerGoodbye {
            reason: goodbye.reason,
        });
        // Orderly end: in-flight host calls settle cancelled rather than
        // outcome-unknown — the worker told us it stopped.
        let in_flight: Vec<CallId> = self.host_calls.keys().copied().collect();
        for call_id in in_flight {
            self.settle_host_call_locally(call_id, WireErrorKind::Cancelled, false);
        }
        self.reclaim_all_handles();
        self.phase = Phase::Closed;
    }

    /// Poison the session. Queued after this: nothing.
    fn fatal(&mut self, kind: WireErrorKind, detail: &str) {
        if self.phase == Phase::Closed {
            return;
        }
        let in_flight: Vec<CallId> = self.host_calls.keys().copied().collect();
        for call_id in in_flight {
            self.settle_host_call_locally(call_id, WireErrorKind::OutcomeUnknown, true);
        }
        self.reclaim_all_handles();
        if self.phase == Phase::Active {
            self.outbox.push(HostMessage::Goodbye(Goodbye {
                reason: bounded_reason(kind),
            }));
        }
        self.phase = Phase::Closed;
        self.events.push(SessionEvent::Fatal {
            kind,
            detail: detail.chars().take(crate::MAX_ERROR_DETAIL_CHARS).collect(),
        });
    }
}

/// The goodbye a fatal fault sends names the kind and nothing else: no
/// internal detail crosses the boundary on the way out.
fn bounded_reason(kind: WireErrorKind) -> String {
    format!("protocol fault: {}", kind_name(kind))
}

pub(crate) fn kind_name(kind: WireErrorKind) -> &'static str {
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

fn is_control(message: &WorkerMessage) -> bool {
    matches!(
        message,
        WorkerMessage::Hello(_)
            | WorkerMessage::Credit(_)
            | WorkerMessage::Cancel(_)
            | WorkerMessage::Release(_)
            | WorkerMessage::ReleaseAck(_)
            | WorkerMessage::Goodbye(_)
            | WorkerMessage::StreamOpen(_)
    )
}
