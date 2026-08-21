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
    MAX_CONTROL_FRAME_BYTES, MAX_ERROR_DETAIL_CHARS, MAX_FRAME_BYTES, MAX_INLINE_RESULT_BYTES,
    MAX_MEDIA_TYPE_CHARS, MAX_METHOD_CHARS, MAX_SDK_IDENTITY_CHARS, MAX_STREAM_CREDIT,
    MAX_STREAM_DATA_BYTES, MAX_WIRE_ID,
};
use std::collections::{BTreeMap, BTreeSet};

/// Everything negotiable about a session, with the crate defaults.
#[derive(Clone, Debug)]
pub struct SessionConfig {
    pub features: Vec<String>,
    pub ceilings: Ceilings,
    /// Total retired correlation entries (spent call ids and handle ids,
    /// offered worker handle ids, reclaimed handle ids) the session may
    /// hold. `None` — the crate default — is the documented status quo:
    /// no-reuse memory grows for the session's lifetime, and bounding it
    /// is the supervising driver's job. A budget bounds that memory
    /// without a wire change: at the budget, new admissions are refused
    /// (worker calls with a non-retryable `resource-exhausted`, host
    /// applications with [`AppError::SessionRetired`]) and no new id is
    /// spent, while every already-admitted call, terminal, release, and
    /// ack completes exactly as before. Retirements from admitted work
    /// may pass the budget; the worst-case overshoot is
    /// `worker_calls_in_flight + 2*host_calls_in_flight + 2*live_handles`
    /// entries (an admitted call retires one id; a spilled terminal
    /// retires the call id and the offered handle; a released or
    /// reclaimed handle and each acked worker release retire one), so the
    /// bound stays strict.
    pub retired_operation_budget: Option<u64>,
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
                max_stream_credit: MAX_STREAM_CREDIT,
            },
            retired_operation_budget: None,
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
    /// The worker acknowledged a release the host initiated for a
    /// worker-held handle; that handle's id is spent.
    WorkerHandleReleased { handle: HandleId, kind: HandleKind },
    /// The session reclaimed handles without a release frame: auto-release
    /// on a failed minting call, goodbye, disconnect, or fatal fault. The
    /// ids are named so the embedding host can drop its own mirrors of
    /// them in the same breath — a handle reclaimed here must not stay
    /// invocable anywhere else.
    HandlesReclaimed { handles: Vec<HandleId> },
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
    /// Hashed at mint, so a hand-built outbound offer can be checked
    /// against the artifact it claims to name without re-hashing.
    digest_blake3: String,
}

/// Refused by the session's own application-facing API — the host asked for
/// something protocol law forbids.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AppError {
    /// The session is not active.
    NotActive,
    /// The named call is not in flight.
    UnknownCall,
    /// A handle-shaped gauge is at the `live_handles` ceiling: a grant
    /// that must be refused, or a pending-release table full of releases
    /// the worker has not acked yet.
    HandleCeiling,
    /// The host's own in-flight ceiling is reached; initiate later.
    CallCeiling,
    /// An inline result over the ceiling; spill it instead.
    SpillRequired { bytes: usize },
    /// An outbound payload over its frame class's byte bound. The session
    /// refuses to queue a frame the other side is contracted to kill.
    PayloadTooLarge { bytes: usize },
    /// An outbound value the session refuses to put on the wire. Where an
    /// inbound admission twin exists (field bounds, the I-JSON integer
    /// rule) the string matches it; the offer-identity rules are
    /// outbound-only, guarding what frame admission cannot see.
    InvalidField(&'static str),
    /// A release for that worker-held handle is already on the wire.
    ReleasePending,
    /// The worker already acknowledged releasing that handle; its id is
    /// spent.
    AlreadyReleased,
    /// The worker never offered that handle, so there is nothing to
    /// release — a release frame for it would arm a desync against an
    /// innocent worker.
    UnknownWorkerHandle,
    /// A stream item with no credit left, a credit grant over the ceiling,
    /// a second stream open, or an item after the last one.
    StreamViolation(&'static str),
    /// A reply already went out for that call.
    AlreadySettled,
    /// The session's retired-operation budget is exhausted: this session
    /// has remembered every id it will remember, and this new admission —
    /// a host call, a handle mint, an offer, a release — would spend one
    /// more. Not retryable on this session; the driver's remedy is to
    /// finish in-flight work and retire the activation. Never a wire
    /// fault: the worker did nothing wrong.
    SessionRetired,
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
    /// Handle ids the worker released, spent forever. Never minted from
    /// again (the monotonic counter guarantees that); read at release time
    /// so a double release names itself instead of masquerading as a
    /// forged handle.
    retired_handles: BTreeSet<HandleId>,
    /// Handle ids the host reclaimed without a release frame, with the
    /// kind each had. A worker release for one of these is a tolerated
    /// race, not a desync: its release may have crossed the reclaiming
    /// terminal on the wire.
    reclaimed_handles: BTreeMap<HandleId, HandleKind>,
    next_handle: u64,
    /// Live handles of both kinds, the gauge the `live_handles` ceiling
    /// bounds. Artifact handles count too: each pins its bytes host-side.
    live_handle_count: u32,
    /// Worker-held handles the host has asked to release, awaiting the
    /// ack, with the kind the release named — an ack must echo it back.
    pending_worker_releases: BTreeMap<HandleId, HandleKind>,
    /// Worker-held handle ids the worker confirmed released; spent forever,
    /// so a second release for one is the application's bug to hear about.
    retired_worker_handles: BTreeSet<HandleId>,
    /// Every handle id the worker has ever offered, with the kind its
    /// offer carried. Membership and kind gate the host's own releases,
    /// and a repeat offer is the id-reuse desync the never-reuse law
    /// promises against.
    offered_worker_handles: BTreeMap<HandleId, HandleKind>,
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
            reclaimed_handles: BTreeMap::new(),
            next_handle: 1,
            live_handle_count: 0,
            pending_worker_releases: BTreeMap::new(),
            retired_worker_handles: BTreeSet::new(),
            offered_worker_handles: BTreeMap::new(),
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

    /// Live handles the worker holds right now, both kinds. The
    /// deterministic-release fixtures read this while the session is
    /// active: after teardown a zero is unconditional and proves nothing.
    pub fn live_handles(&self) -> u32 {
        self.live_handle_count
    }

    /// Calls currently in flight: `(host-initiated, worker-initiated)`.
    /// Each is bounded by its negotiated ceiling; this is the observation
    /// half of that law.
    pub fn in_flight_calls(&self) -> (u32, u32) {
        (self.host_calls.len() as u32, self.worker_calls.len() as u32)
    }

    /// Worker-held handles awaiting a release ack. Bounded by the
    /// `live_handles` ceiling.
    pub fn pending_releases(&self) -> u32 {
        self.pending_worker_releases.len() as u32
    }

    /// Total correlation entries the session will remember forever:
    /// spent call ids (both directions), spent handle ids (released,
    /// reclaimed, and worker-acked), and every worker-offered handle id.
    /// These collections never shrink while the session lives — the
    /// no-reuse law — so this count is the memory the session has spent
    /// on correlation, and [`SessionConfig::retired_operation_budget`]
    /// bounds it. That budget is an enforceable cardinality bound that
    /// upper-bounds the correlation state a session can hold; it is not
    /// a byte-exact account of allocator behavior, which node sizes and
    /// spare capacity keep outside the session's sight.
    pub fn retired_operations(&self) -> u64 {
        (self.retired_worker_calls.len()
            + self.retired_host_calls.len()
            + self.retired_handles.len()
            + self.reclaimed_handles.len()
            + self.retired_worker_handles.len()
            + self.offered_worker_handles.len()) as u64
    }

    /// Whether a new admission would push correlation memory past the
    /// configured budget. Admissions check this; retirements from
    /// already-admitted work never do.
    pub(super) fn budget_full(&self) -> bool {
        self.config
            .retired_operation_budget
            .is_some_and(|budget| self.retired_operations() >= budget)
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
            // Bounds are checked per phase, not here: a bounds-violating
            // frame before negotiation still deserves the refuse frame
            // that names the rule, and `fatal` sends nothing pre-accept.
            Phase::AwaitingHello => self.on_pre_negotiation(message),
            Phase::Active => self.on_active(message),
            Phase::Closed => {}
        }
    }

    fn on_active(&mut self, message: WorkerMessage) {
        if let Err(reason) = validate_bounds(&message) {
            // Field bounds are part of strict decoding: the generated
            // schemas carry the same numbers, so a schema-conformant SDK
            // never trips this and a non-conformant one is refused at the
            // same line on both sides.
            self.fatal(WireErrorKind::InvalidFrame, reason);
            return;
        }
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
            WorkerMessage::ReleaseAck(ack) => self.on_release_ack(ack),
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
        // Acks that will never come: worker-held handles die with the
        // worker, so pending releases are void, not still owed. The
        // reclaimed table dies with the session too — after close, no
        // frame is read, so no racing release can arrive to consult it.
        // Worker calls in flight die with their answers; keeping them in
        // the map would leave the in-flight gauge lying after close.
        self.pending_worker_releases.clear();
        self.reclaimed_handles.clear();
        self.worker_calls.clear();
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
        self.pending_worker_releases.clear();
        self.reclaimed_handles.clear();
        self.worker_calls.clear();
        if self.phase == Phase::Active {
            self.outbox.push(HostMessage::Goodbye(Goodbye {
                reason: bounded_reason(kind),
            }));
        }
        self.phase = Phase::Closed;
        self.events.push(SessionEvent::Fatal {
            kind,
            detail: clip_detail(detail),
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

/// Field bounds, enforced at admission and mirrored byte-for-byte by the
/// `#[schemars]` attributes on the wire types — the generated schemas and
/// this function must refuse the same frames. Bounds that depend on
/// negotiated config (credit ceilings, in-flight ceilings) are enforced in
/// their handlers, not here. Runs on active-phase frames; the first hello's
/// bounds are checked in negotiation, where a violation still earns the
/// refuse frame that names the rule.
fn validate_bounds(message: &WorkerMessage) -> Result<(), &'static str> {
    // The upper arm is defense in depth: strict parsing already refuses any
    // integer past the I-JSON bound, so only zero can reach this check from
    // the wire.
    let id_ok = |id: u64| (1..=MAX_WIRE_ID).contains(&id);
    let name_ok = |name: &str| {
        let chars = name.chars().count();
        (1..=MAX_SDK_IDENTITY_CHARS).contains(&chars)
    };
    match message {
        WorkerMessage::Hello(hello) => {
            if !name_ok(&hello.sdk_name) || !name_ok(&hello.sdk_version) {
                return Err("sdk identity outside its length bound");
            }
        }
        WorkerMessage::Call(call) => {
            if !id_ok(call.call_id.0) {
                return Err("call id outside wire range");
            }
            let method_chars = call.method.chars().count();
            if method_chars == 0 || method_chars > MAX_METHOD_CHARS {
                return Err("method name outside its length bound");
            }
        }
        WorkerMessage::Reply(reply) => {
            if !id_ok(reply.call_id.0) {
                return Err("call id outside wire range");
            }
            match &reply.outcome {
                Outcome::Err { error } => {
                    if error.message.chars().count() > MAX_ERROR_DETAIL_CHARS {
                        return Err("error message outside its length bound");
                    }
                }
                Outcome::Spilled { artifact } => {
                    if !id_ok(artifact.handle.0) {
                        return Err("handle id outside wire range");
                    }
                    let media_chars = artifact.media_type.chars().count();
                    if media_chars == 0 || media_chars > MAX_MEDIA_TYPE_CHARS {
                        return Err("media type outside its length bound");
                    }
                    if artifact.digest_blake3.len() != 64
                        || !artifact
                            .digest_blake3
                            .bytes()
                            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
                    {
                        return Err("digest is not 64 lowercase hex characters");
                    }
                }
                Outcome::Ok { .. } | Outcome::Cancelled { .. } => {}
            }
        }
        WorkerMessage::StreamOpen(open) => {
            if !id_ok(open.call_id.0) {
                return Err("call id outside wire range");
            }
        }
        WorkerMessage::StreamData(data) => {
            if !id_ok(data.call_id.0) {
                return Err("call id outside wire range");
            }
        }
        WorkerMessage::Credit(credit) => {
            if !id_ok(credit.call_id.0) {
                return Err("call id outside wire range");
            }
        }
        WorkerMessage::Cancel(cancel) => {
            if !id_ok(cancel.call_id.0) {
                return Err("call id outside wire range");
            }
        }
        WorkerMessage::Release(release) => {
            if !id_ok(release.handle.0) {
                return Err("handle id outside wire range");
            }
        }
        WorkerMessage::ReleaseAck(ack) => {
            if !id_ok(ack.handle.0) {
                return Err("handle id outside wire range");
            }
        }
        WorkerMessage::Goodbye(goodbye) => {
            if goodbye.reason.chars().count() > crate::MAX_GOODBYE_REASON_CHARS {
                return Err("goodbye reason outside its length bound");
            }
        }
    }
    Ok(())
}

/// Bound an internal detail string for the event log; wire messages carry
/// only fixed strings and [`kind_name`] values.
pub(super) fn clip_detail(detail: &str) -> String {
    detail.chars().take(MAX_ERROR_DETAIL_CHARS).collect()
}

/// The outbound mirror of the strict parser's integer rule: every integer
/// a peer running [`crate::strict`] admission would refuse is refused here
/// before the frame is queued. Floats pass, exactly as they do inbound.
pub(super) fn value_within_ijson(value: &serde_json::Value) -> bool {
    match value {
        serde_json::Value::Number(number) => {
            if let Some(unsigned) = number.as_u64() {
                unsigned <= crate::strict::SAFE_MAX
            } else if let Some(signed) = number.as_i64() {
                signed >= crate::strict::SAFE_MIN
            } else {
                true
            }
        }
        serde_json::Value::Array(items) => items.iter().all(value_within_ijson),
        serde_json::Value::Object(members) => members.values().all(value_within_ijson),
        _ => true,
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
