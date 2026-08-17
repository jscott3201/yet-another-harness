//! The bounded framed bidirectional worker protocol (v1).
//!
//! This crate is the wire contract between the YAH host and a supervised
//! worker process (Node or CPython), specified before either worker runtime
//! exists. The Rust types here are the source of truth: they generate the
//! checked-in JSON Schema and TypeScript under `generated/worker-protocol/`,
//! and the fixture corpus in `tests/` pins the semantics a worker SDK must
//! satisfy — handshake, calls, streams, cancellation, errors, artifact
//! spill, and resource handles.
//!
//! Everything is sans-io on purpose. [`frame`] turns bytes into frames and
//! back, incrementally and with every bound checked before allocation;
//! [`session`] is a pure state machine over decoded frames with an injected
//! millisecond clock. The process driver (IPC-002) supplies the socket or
//! pipe, the spawned child, and the kill path; nothing in this crate can
//! block, sleep, or spawn.
//!
//! What this crate does not do: authenticate the peer (the bootstrap fd and
//! peer-credential checks belong to the process driver), supervise or
//! restart a worker, or define any capability's semantics. Capability
//! refusals are answers inside a successful call, exactly as in the Wasm
//! lane; the closed [`types::WireErrorKind`] family names only failures of
//! the protocol boundary itself.

pub mod frame;
pub mod generate;
pub mod session;
pub mod strict;
pub mod types;

/// Negotiated in the handshake; the only version this crate speaks.
///
/// Enum members are closed within a negotiated version: a decoder that meets
/// an unknown frame tag or enum member answers `invalid-frame` and the
/// connection ends. Evolution happens here, at the version gate, never by
/// per-member leniency — two SDK decoders must never disagree about what an
/// unrecognised member means.
pub const PROTOCOL_VERSION: u32 = 1;

/// Outer bound for any frame, checked against the length prefix before a
/// single payload byte is allocated.
pub const MAX_FRAME_BYTES: usize = 1024 * 1024 + 4096;

/// Bound for control frames (handshake, cancel, credit, release, goodbye).
pub const MAX_CONTROL_FRAME_BYTES: usize = 16 * 1024;

/// Bound for one call's request payload.
pub const MAX_CALL_PAYLOAD_BYTES: usize = 256 * 1024;

/// Inline result ceiling. A larger result must spill through an artifact
/// handle; silent truncation is not an option the protocol offers. This is
/// deliberately not the kernel's 4 KiB receipt ceiling — tool output is the
/// worker lane's common case, receipts are the kernel's.
pub const MAX_INLINE_RESULT_BYTES: usize = 64 * 1024;

/// Bound for one stream data frame's payload.
pub const MAX_STREAM_DATA_BYTES: usize = 64 * 1024;

/// Bound for one artifact pull-read, in raw bytes. Chunks cross the wire
/// hex-encoded inside an ordinary call result, so this is sized to keep a
/// maximal chunk (doubled by hex, plus envelope) under
/// [`MAX_INLINE_RESULT_BYTES`] — more round trips for a large artifact, and
/// no third framing rule for two SDKs to get subtly wrong.
pub const MAX_ARTIFACT_READ_BYTES: usize = 24 * 1024;

/// Character bound on any error message crossing the boundary.
pub const MAX_ERROR_DETAIL_CHARS: usize = 512;

/// Character bound on a call's method name.
pub const MAX_METHOD_CHARS: usize = 128;

/// Frames of initial credit a stream opens with, and the ceiling any credit
/// grant may reach. Credit is counted in frames, not bytes: every stream
/// frame is already byte-bounded, so frame count is the dimension a hostile
/// producer could still flood.
pub const INITIAL_STREAM_CREDIT: u32 = 16;
pub const MAX_STREAM_CREDIT: u32 = 1024;

/// Default directional in-flight ceilings, announced in the handshake
/// accept. Exceeding one is refused with `resource-exhausted`, never
/// queued: refusal is the only bound that does not become host memory.
pub const DEFAULT_HOST_CALLS_IN_FLIGHT: u32 = 16;
pub const DEFAULT_WORKER_CALLS_IN_FLIGHT: u32 = 32;

/// Default live-handle ceiling per activation, mirroring the Wasm lane's
/// `max_capability_handles` default.
pub const DEFAULT_LIVE_HANDLES: u32 = 16;

/// Largest identifier the wire accepts for call and handle ids: the I-JSON
/// safe-integer bound, so every SDK reads the same number.
pub const MAX_WIRE_ID: u64 = (1 << 53) - 1;
