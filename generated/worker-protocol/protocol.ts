// Generated from yah-plugin-ipc Rust protocol types and constants. Do not edit.

export const PROTOCOL_VERSION = 1 as const;

export const FRAME_PREFIX_BYTES = 4 as const;

export const MAX_WIRE_ID = 9007199254740991 as const;

export const MAX_WIRE_UINT32 = 4294967295 as const;

export const DEFAULT_WIRE_LIMITS = Object.freeze({
  max_frame_bytes: 1052672,
  max_control_frame_bytes: 16384,
  max_call_payload_bytes: 262144,
  max_inline_result_bytes: 65536,
  max_stream_data_bytes: 65536,
  max_artifact_read_bytes: 24576,
} as const satisfies Readonly<WireLimits>);

export const DEFAULT_CEILINGS = Object.freeze({
  host_calls_in_flight: 16,
  worker_calls_in_flight: 32,
  live_handles: 16,
  initial_stream_credit: 16,
  max_stream_credit: 1024,
} as const satisfies Readonly<Ceilings>);

export const WIRE_FIELD_LIMITS = Object.freeze({
  error_detail_chars: 512,
  method_chars: 128,
  media_type_chars: 128,
  sdk_identity_chars: 64,
  goodbye_reason_chars: 256,
} as const);

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
 * Live resource handles the worker may hold at once, capability and
 * artifact alike — an artifact handle pins its bytes host-side, so an
 * uncounted kind would be unbounded memory behind a bounded gauge.
 */
live_handles: number, 
/**
 * The opening credit window an SDK should grant when it has no
 * better number. Advisory, not enforced: any opening grant in
 * `1..=max_stream_credit` is legal.
 */
initial_stream_credit: number, 
/**
 * Ceiling on one stream's outstanding credit — the unspent frames a
 * producer may hold at a moment, checked at open and at every
 * widening. Spent credit leaves the window, so grants over a stream's
 * lifetime may sum past this.
 */
max_stream_credit: number, };

export type Call = { call_id: CallId, method: string, 
/**
 * Relative millisecond budget, not an instant: a sandboxed worker that
 * has been denied clock access can still count down, and the host
 * enforces the deadline regardless of what the worker does.
 * `nullable` because serde writes an absent budget as `null` — the
 * TypeScript type must admit the value actually on the wire.
 */
deadline_ms?: number | null, 
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

export type ArtifactOffer = { handle: HandleId, 
/**
 * At least one: a spill of zero bytes is not a spill, on either side.
 */
bytes: number, media_type: string, 
/**
 * BLAKE3 of the full content, hex. Whoever stores the artifact
 * durably mints its durable reference from bytes it hashed itself —
 * a wire offer is a claim, not provenance.
 */
digest_blake3: string, };

export type Goodbye = { reason: string, };

export type WireError = { kind: WireErrorKind, message: string, 
/**
 * Whether the same request may be retried on this session under a
 * fresh call id — the refused id is spent, so as-is resubmission is
 * the `duplicate-call` fault.
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
