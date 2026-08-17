//! Wire types for worker protocol v1.
//!
//! Direction is part of the type: [`WorkerMessage`] is everything a worker
//! may send, [`HostMessage`] everything a host may send. The two share
//! component types but not an envelope, so "the worker sent an accept" is
//! unrepresentable rather than merely invalid.
//!
//! Identifier spaces are disjoint by construction: a [`CallId`] is minted by
//! whichever side initiates that call (each direction numbers its own calls),
//! and a [`HandleId`] is minted only by the side that owns the resource
//! behind it. A handle is a name, not a capability — the authority lives in
//! the owner's table entry, and a forged or stale id fails the table lookup.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use ts_rs::TS;

/// One call or one stream, numbered by the side that initiated it.
///
/// Wire form is a JSON number in `1..=2^53-1` so every SDK reads the same
/// value; zero is reserved as never-valid. `number` on the TypeScript side,
/// not `bigint`: `JSON.parse` produces a `number`, and the I-JSON bound is
/// exactly what makes that safe.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, JsonSchema, TS,
)]
#[ts(type = "number")]
pub struct CallId(pub u64);

/// A live resource named by its owner: a capability handle the host minted,
/// or an artifact handle minted by whichever side holds the bytes.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, JsonSchema, TS,
)]
#[ts(type = "number")]
pub struct HandleId(pub u64);

/// Everything a worker may put on the wire.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, TS)]
#[serde(tag = "frame", rename_all = "kebab-case", deny_unknown_fields)]
pub enum WorkerMessage {
    /// First frame on the wire, exactly once. Anything else first — or a
    /// second hello — is refused and the connection ends.
    Hello(Hello),
    Call(Call),
    Reply(Reply),
    StreamOpen(StreamOpen),
    StreamData(StreamData),
    Credit(Credit),
    Cancel(Cancel),
    Release(Release),
    ReleaseAck(ReleaseAck),
    /// Orderly end. Distinguishes a worker that finished from one that was
    /// lost: after a goodbye, in-flight work settles as cancelled; after a
    /// bare disconnect it settles as outcome-unknown.
    Goodbye(Goodbye),
}

/// Everything a host may put on the wire.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, TS)]
#[serde(tag = "frame", rename_all = "kebab-case", deny_unknown_fields)]
pub enum HostMessage {
    /// Terminal answer to a hello the host can serve.
    Accept(Accept),
    /// Terminal answer to a hello the host cannot serve; the connection
    /// ends after this frame. Names every version the host does speak, so
    /// the worker fails with a diagnostic instead of a bare close.
    Refuse(Refuse),
    Call(Call),
    Reply(Reply),
    StreamOpen(StreamOpen),
    StreamData(StreamData),
    Credit(Credit),
    Cancel(Cancel),
    Release(Release),
    ReleaseAck(ReleaseAck),
    Goodbye(Goodbye),
}

/// The worker introduces itself. Negotiation is startup-only and stateful —
/// this lane is one activation-scoped process with a lifetime, not a
/// horizontally scaled stateless service — so no later frame carries a
/// version.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, TS)]
#[serde(deny_unknown_fields)]
pub struct Hello {
    /// Protocol versions the worker can speak, best-preferred first.
    pub protocol_versions: Vec<u32>,
    #[schemars(length(min = 1, max = 64))]
    pub sdk_name: String,
    #[schemars(length(min = 1, max = 64))]
    pub sdk_version: String,
    /// Features the worker can use if offered.
    pub features: Vec<String>,
    /// Features the worker cannot run without. An entry the host does not
    /// know is a refusal — fail closed on unknown required features.
    pub required_features: Vec<String>,
}

/// The host's half of negotiation: the chosen version, the feature set in
/// force, and every bound and ceiling the session will enforce. Announced
/// rather than negotiable: the worker's remedy for a ceiling it cannot live
/// with is goodbye.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, TS)]
#[serde(deny_unknown_fields)]
pub struct Accept {
    pub protocol_version: u32,
    pub features: Vec<String>,
    pub limits: WireLimits,
    pub ceilings: Ceilings,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, TS)]
#[serde(deny_unknown_fields)]
pub struct Refuse {
    pub error: WireError,
    pub supported_versions: Vec<u32>,
}

/// Byte bounds the session enforces per frame class, echoed to the worker
/// so both sides refuse at the same line.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, TS)]
#[serde(deny_unknown_fields)]
pub struct WireLimits {
    #[ts(type = "number")]
    pub max_frame_bytes: u64,
    #[ts(type = "number")]
    pub max_control_frame_bytes: u64,
    #[ts(type = "number")]
    pub max_call_payload_bytes: u64,
    #[ts(type = "number")]
    pub max_inline_result_bytes: u64,
    #[ts(type = "number")]
    pub max_stream_data_bytes: u64,
    #[ts(type = "number")]
    pub max_artifact_read_bytes: u64,
}

/// Count ceilings the session enforces by refusal.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, TS)]
#[serde(deny_unknown_fields)]
pub struct Ceilings {
    /// Host-initiated calls that may be in flight toward the worker.
    pub host_calls_in_flight: u32,
    /// Worker-initiated calls that may be in flight toward the host.
    pub worker_calls_in_flight: u32,
    /// Live resource handles the worker may hold at once.
    pub live_handles: u32,
    /// Frames of credit a stream opens with.
    pub initial_stream_credit: u32,
}

/// One request. The initiator mints the id; the receiver answers with
/// exactly one terminal [`Reply`] carrying the same id, whatever else
/// happens — cancellation included.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, TS)]
#[serde(deny_unknown_fields)]
pub struct Call {
    pub call_id: CallId,
    #[schemars(length(min = 1, max = 128))]
    pub method: String,
    /// Relative millisecond budget, not an instant: a sandboxed worker that
    /// has been denied clock access can still count down, and the host
    /// enforces the deadline regardless of what the worker does.
    pub deadline_ms: Option<u32>,
    /// Whether the caller wants incremental results. A receiver that agrees
    /// answers with [`StreamOpen`] before any data; the terminal reply still
    /// ends the call either way.
    pub stream: bool,
    pub payload: Value,
}

/// The single terminal frame of a call.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, TS)]
#[serde(deny_unknown_fields)]
pub struct Reply {
    pub call_id: CallId,
    pub outcome: Outcome,
}

/// How a call ended. Three-valued on purpose: cancelled is an outcome, not
/// an error — a call that was asked to stop and stopped did what it was
/// told. A completion racing a cancel is legal and the completion wins.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, TS)]
#[serde(tag = "outcome", rename_all = "kebab-case", deny_unknown_fields)]
pub enum Outcome {
    /// The result, inline. Domain failures ride here as data the method's
    /// contract defines — a capability refusal is an answer, exactly as in
    /// the Wasm lane — so the protocol error family stays closed and small.
    Ok {
        result: Value,
    },
    /// The result was over the inline ceiling and spilled: the replier
    /// holds the bytes behind an artifact handle for the caller to
    /// pull-read. Never a truncated inline result.
    Spilled {
        artifact: ArtifactOffer,
    },
    /// The protocol boundary failed the call.
    Err {
        error: WireError,
    },
    Cancelled {
        reason: CancelReason,
    },
}

/// Who ended a cancelled call.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema, TS)]
#[serde(rename_all = "kebab-case")]
pub enum CancelReason {
    /// The caller asked.
    Requested,
    /// The receiver's own enforcement ended it (deadline, shutdown).
    Enforced,
}

/// Acknowledges a `stream: true` call and grants initial credit. Mandatory
/// before any data frame, so "did this stream ever start" is decidable.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, TS)]
#[serde(deny_unknown_fields)]
pub struct StreamOpen {
    pub call_id: CallId,
    pub credit: u32,
}

/// One incremental item on a stream. `seq` is monotonic from zero per
/// stream; `more: false` marks the last data frame, after which only the
/// terminal reply may follow — end-of-stream is decidable frame by frame,
/// without a timeout.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, TS)]
#[serde(deny_unknown_fields)]
pub struct StreamData {
    pub call_id: CallId,
    #[ts(type = "number")]
    pub seq: u64,
    pub more: bool,
    pub class: StreamClass,
    /// Cumulative count of lossy frames dropped before this one, so a
    /// consumer can see the gap it was spared rather than infer it.
    #[ts(type = "number")]
    pub dropped: u64,
    pub payload: Value,
}

/// The two delivery classes, shared vocabulary with the kernel's event
/// lane: lossless data consumes credit and the producer must stop at zero;
/// lossy data may be dropped oldest-first under pressure, counted.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema, TS)]
#[serde(rename_all = "kebab-case")]
pub enum StreamClass {
    Lossless,
    Lossy,
}

/// Grants the producer more frames of credit on one stream. Credit is the
/// wire contract for backpressure: in-process advisories like `drain()` are
/// suggestions a hostile producer ignores, a spent window is a rule the
/// receiver enforces.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, TS)]
#[serde(deny_unknown_fields)]
pub struct Credit {
    pub call_id: CallId,
    pub additional: u32,
}

/// Ask the receiver to stop. Advisory by nature — Node and CPython can only
/// honor it at cooperative points — which is why the call still owes its
/// terminal frame and the host keeps its own deadline enforcement.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, TS)]
#[serde(deny_unknown_fields)]
pub struct Cancel {
    pub call_id: CallId,
    pub target: CancelTarget,
}

/// The two things a cancel can mean: stop working, or keep working but stop
/// streaming to me.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema, TS)]
#[serde(rename_all = "kebab-case")]
pub enum CancelTarget {
    Call,
    Stream,
}

/// Release a handle to its owner. Explicit and acknowledged: finalizers do
/// not run reliably in any lane this protocol serves, so release is a
/// two-party fact, not a hope.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, TS)]
#[serde(deny_unknown_fields)]
pub struct Release {
    pub handle: HandleId,
    pub kind: HandleKind,
}

/// The owner confirms the handle is gone and its id may never be reused
/// within the session — the acknowledgement closes the release/reuse race.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, TS)]
#[serde(deny_unknown_fields)]
pub struct ReleaseAck {
    pub handle: HandleId,
    pub kind: HandleKind,
}

/// What a handle names.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema, TS)]
#[serde(rename_all = "kebab-case")]
pub enum HandleKind {
    /// A brokered capability the host granted for this activation.
    Capability,
    /// Spilled bytes held by one side for the other to pull-read.
    Artifact,
}

/// A spilled result: the replier keeps the bytes and the caller pull-reads
/// them in bounded chunks, then must release the handle. The offer names
/// size and digest up front so the reader can refuse before the first pull,
/// and verify after the last.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, TS)]
#[serde(deny_unknown_fields)]
pub struct ArtifactOffer {
    pub handle: HandleId,
    #[ts(type = "number")]
    pub bytes: u64,
    #[schemars(length(min = 1, max = 128))]
    pub media_type: String,
    /// BLAKE3 of the full content, hex. Whoever stores the artifact
    /// durably mints its durable reference from bytes it hashed itself —
    /// a wire offer is a claim, not provenance.
    #[schemars(length(min = 64, max = 64))]
    pub digest_blake3: String,
}

/// Orderly shutdown notice; either side may send it.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, TS)]
#[serde(deny_unknown_fields)]
pub struct Goodbye {
    #[schemars(length(max = 256))]
    pub reason: String,
}

/// The closed protocol-boundary error family, keyed by the caller's
/// recovery action. Domain failures never appear here — they are data on a
/// successful call — and messages carry no host path, registration id, or
/// type name, because an error that leaks internals is a tour a hostile
/// worker did not have to ask for.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, TS)]
#[serde(deny_unknown_fields)]
pub struct WireError {
    pub kind: WireErrorKind,
    #[schemars(length(max = 512))]
    pub message: String,
    /// Whether the same call may be retried on this session as-is.
    pub retryable: bool,
    /// Whether the caller must reconcile external state before retrying —
    /// set on every outcome-unknown settlement.
    pub reconcile_required: bool,
}

/// Closed within a negotiated version. An unknown member is `invalid-frame`
/// and the connection ends; evolution happens at the handshake version
/// gate.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema, TS)]
#[serde(rename_all = "kebab-case")]
pub enum WireErrorKind {
    /// No version in the hello is one the host speaks.
    UnsupportedVersion,
    /// A required feature is unknown to the receiving side.
    UnknownRequiredFeature,
    /// A frame arrived before negotiation completed, or a second hello.
    NegotiationRequired,
    /// The frame failed strict decoding: bad JSON, duplicate member names,
    /// numbers outside the I-JSON safe range, an unknown tag or enum
    /// member, or a field violating its bound.
    InvalidFrame,
    /// The length prefix exceeded the frame class's byte bound.
    FrameTooLarge,
    /// A payload exceeded its class bound inside a well-formed frame.
    PayloadTooLarge,
    /// A reply, cancel, credit, or stream frame named a call id that is not
    /// in flight in that direction.
    UnknownCall,
    /// A call id already in flight in the same direction was reused.
    DuplicateCall,
    /// The receiver does not offer the named method.
    UnknownMethod,
    /// A directional in-flight ceiling, credit window, or handle ceiling
    /// was exceeded; refused rather than queued.
    ResourceExhausted,
    /// The call's millisecond budget expired; the host is the enforcer.
    DeadlineExceeded,
    /// The session ended in an orderly goodbye before this call's terminal
    /// frame arrived. Distinct from a worker terminal with
    /// [`Outcome::Cancelled`], which is the call itself answering.
    Cancelled,
    /// A release, read, or use named a handle the owner does not hold.
    UnknownHandle,
    /// An artifact pull-read went outside the offered byte range or over
    /// the per-read bound.
    InvalidRead,
    /// The peer is gone and the outcome of in-flight work is unknowable
    /// from this side. Reconciliation is required before unsafe repetition.
    OutcomeUnknown,
    /// The receiving side failed internally; details stay on its side.
    Internal,
}
