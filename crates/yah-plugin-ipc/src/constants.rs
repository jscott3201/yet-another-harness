//! Protocol-v1 limits and defaults shared by the Rust implementation and
//! generated worker metadata.

/// Negotiated in the handshake; the only version this crate speaks.
///
/// Enum members are closed within a negotiated version: a decoder that meets
/// an unknown frame tag or enum member answers `invalid-frame` and the
/// connection ends. Evolution happens here, at the version gate, never by
/// per-member leniency — two SDK decoders must never disagree about what an
/// unrecognised member means.
pub const PROTOCOL_VERSION: u32 = 1;

/// Bytes in the big-endian frame-length prefix.
pub const FRAME_PREFIX_BYTES: usize = std::mem::size_of::<u32>();

/// Outer bound for any frame, checked against the length prefix before a
/// single payload byte is allocated.
pub const MAX_FRAME_BYTES: usize = 1024 * 1024 + 4096;

/// Bound for control frames: hello, accept, refuse, stream-open, credit,
/// cancel, release, release-ack, and goodbye — everything that is not a
/// call, reply, or stream-data frame.
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

/// Character bound on an artifact offer's media type.
pub const MAX_MEDIA_TYPE_CHARS: usize = 128;

/// Character bound on each half of a hello's SDK identity.
pub const MAX_SDK_IDENTITY_CHARS: usize = 64;

/// Character bound on a goodbye's reason.
pub const MAX_GOODBYE_REASON_CHARS: usize = 256;

/// The opening credit window an SDK should use when it has no better
/// number — announced in the accept as `ceilings.initial_stream_credit`
/// but advisory, not enforced: any opening grant in `1..=max_stream_credit`
/// is legal. Credit is counted in frames, not bytes: every stream frame is
/// already byte-bounded, so frame count is the dimension a hostile
/// producer could still flood. `MAX_STREAM_CREDIT` is the default ceiling
/// any window may reach, and that one is enforced.
pub const INITIAL_STREAM_CREDIT: u32 = 16;
pub const MAX_STREAM_CREDIT: u32 = 1024;

/// Default directional in-flight ceilings, announced in the handshake
/// accept. Exceeding one is refused with `resource-exhausted`, never
/// queued — a queue here is unbounded memory the other side controls,
/// where a refusal retains only the spent id.
pub const DEFAULT_HOST_CALLS_IN_FLIGHT: u32 = 16;
pub const DEFAULT_WORKER_CALLS_IN_FLIGHT: u32 = 32;

/// Default live-handle ceiling per activation, mirroring the Wasm lane's
/// `max_capability_handles` default.
pub const DEFAULT_LIVE_HANDLES: u32 = 16;

/// Largest integer any counted wire field may carry — call and handle
/// ids, offer byte counts, drop counts: the I-JSON safe-integer bound, so
/// every SDK reads the same number.
pub const MAX_WIRE_ID: u64 = (1 << 53) - 1;

/// Largest value admitted by generated fields whose Rust type is `u32`.
pub const MAX_WIRE_UINT32: u32 = u32::MAX;
