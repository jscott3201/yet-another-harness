// Generated from yah-plugin-ipc Rust protocol types. Do not edit.

export type JsonValue = null | boolean | number | string | JsonValue[] | { [key: string]: JsonValue };

export type CallId = number;

export type HandleId = number;

export type Hello = { 
/**
 * Protocol versions the worker can speak, best-preferred first.
 */
protocol_versions: Array<number>, sdk_name: string, sdk_version: string, 
/**
 * Features the worker can use if offered.
 */
features: Array<string>, 
/**
 * Features the worker cannot run without. An entry the host does not
 * know is a refusal — fail closed on unknown required features.
 */
required_features: Array<string>, };

export type Accept = { protocol_version: number, features: Array<string>, limits: WireLimits, ceilings: Ceilings, };

export type Refuse = { error: WireError, supported_versions: Array<number>, };

export type WireLimits = { max_frame_bytes: number, max_control_frame_bytes: number, max_call_payload_bytes: number, max_inline_result_bytes: number, max_stream_data_bytes: number, max_artifact_read_bytes: number, };

export type Ceilings = { 
/**
 * Host-initiated calls that may be in flight toward the worker.
 */
host_calls_in_flight: number, 
/**
 * Worker-initiated calls that may be in flight toward the host.
 */
worker_calls_in_flight: number, 
/**
 * Live resource handles the worker may hold at once.
 */
live_handles: number, 
/**
 * Frames of credit a stream opens with.
 */
initial_stream_credit: number, };

export type Call = { call_id: CallId, method: string, 
/**
 * Relative millisecond budget, not an instant: a sandboxed worker that
 * has been denied clock access can still count down, and the host
 * enforces the deadline regardless of what the worker does.
 */
deadline_ms: number | null, 
/**
 * Whether the caller wants incremental results. A receiver that agrees
 * answers with [`StreamOpen`] before any data; the terminal reply still
 * ends the call either way.
 */
stream: boolean, payload: JsonValue, };

export type Reply = { call_id: CallId, outcome: Outcome, };

export type Outcome = { "outcome": "ok", result: JsonValue, } | { "outcome": "spilled", artifact: ArtifactOffer, } | { "outcome": "err", error: WireError, } | { "outcome": "cancelled", reason: CancelReason, };

export type CancelReason = "requested" | "enforced";

export type StreamOpen = { call_id: CallId, credit: number, };

export type StreamData = { call_id: CallId, seq: number, more: boolean, class: StreamClass, 
/**
 * Cumulative count of lossy frames dropped before this one, so a
 * consumer can see the gap it was spared rather than infer it.
 */
dropped: number, payload: JsonValue, };

export type StreamClass = "lossless" | "lossy";

export type Credit = { call_id: CallId, additional: number, };

export type Cancel = { call_id: CallId, target: CancelTarget, };

export type CancelTarget = "call" | "stream";

export type Release = { handle: HandleId, kind: HandleKind, };

export type ReleaseAck = { handle: HandleId, kind: HandleKind, };

export type HandleKind = "capability" | "artifact";

export type ArtifactOffer = { handle: HandleId, bytes: number, media_type: string, 
/**
 * BLAKE3 of the full content, hex. Whoever stores the artifact
 * durably mints its durable reference from bytes it hashed itself —
 * a wire offer is a claim, not provenance.
 */
digest_blake3: string, };

export type Goodbye = { reason: string, };

export type WireError = { kind: WireErrorKind, message: string, 
/**
 * Whether the same call may be retried on this session as-is.
 */
retryable: boolean, 
/**
 * Whether the caller must reconcile external state before retrying —
 * set on every outcome-unknown settlement.
 */
reconcile_required: boolean, };

export type WireErrorKind = "unsupported-version" | "unknown-required-feature" | "negotiation-required" | "invalid-frame" | "frame-too-large" | "payload-too-large" | "unknown-call" | "duplicate-call" | "unknown-method" | "resource-exhausted" | "deadline-exceeded" | "cancelled" | "unknown-handle" | "invalid-read" | "outcome-unknown" | "internal";

export type WorkerMessage = { "frame": "hello" } & Hello | { "frame": "call" } & Call | { "frame": "reply" } & Reply | { "frame": "stream-open" } & StreamOpen | { "frame": "stream-data" } & StreamData | { "frame": "credit" } & Credit | { "frame": "cancel" } & Cancel | { "frame": "release" } & Release | { "frame": "release-ack" } & ReleaseAck | { "frame": "goodbye" } & Goodbye;

export type HostMessage = { "frame": "accept" } & Accept | { "frame": "refuse" } & Refuse | { "frame": "call" } & Call | { "frame": "reply" } & Reply | { "frame": "stream-open" } & StreamOpen | { "frame": "stream-data" } & StreamData | { "frame": "credit" } & Credit | { "frame": "cancel" } & Cancel | { "frame": "release" } & Release | { "frame": "release-ack" } & ReleaseAck | { "frame": "goodbye" } & Goodbye;
