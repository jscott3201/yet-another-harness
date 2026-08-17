# Worker Process Wire Protocol

This page documents protocol v1 between the YAH host and a supervised worker
process, implemented in `crates/yah-plugin-ipc`. It is the wire contract the
future Node and CPython process drivers and their worker SDKs must satisfy,
specified and fixture-pinned before either worker runtime exists.

The crate is sans-io: `frame` turns bytes into frames incrementally, and
`session` is a pure state machine with an injected millisecond clock. Nothing
in it can block, sleep, spawn, or open a socket. The process driver that
supplies the transport, the spawned child, the bootstrap file descriptor, and
the kill path is not implemented; neither is any worker-side SDK.
Eighty-eight deterministic fixtures in `crates/yah-plugin-ipc/tests/` drive
the host side of every rule below with a scripted byte-level peer.

## Framing and strict JSON

A frame is a 4-byte big-endian byte count followed by exactly that many bytes
of one JSON value. The prefix is checked against `MAX_FRAME_BYTES` (1 MiB +
4 KiB) as soon as the four prefix bytes arrive, without waiting for the
payload (bytes the transport already handed over are discarded at the
poison, so exposure is one transport read, never a frame); a zero-length or
oversize
declaration poisons the connection. Framing carries no resync marker on
purpose — after one violation, later bytes are unattributable, so there is no
resync path.

Frame JSON is strict, mirroring the kernel protocol's rules:

- A duplicate member name is refused, not last-wins resolved.
- An integer outside the I-JSON safe range (±2^53−1) is refused, not rounded.
- Unknown members and unknown enum values are refused. Enums are closed
  within a negotiated version: evolution happens at the version gate in the
  handshake, never by per-member leniency.
- Field bounds are enforced at admission and mirrored by the generated
  schemas: ids in `1..=2^53−1`, SDK identity 1–64 characters, method names
  1–128, goodbye reasons at most 256, error messages at most 512, and a
  spilled offer's digest exactly 64 lowercase hex characters. A frame a
  conformant schema validator would refuse, this host refuses at the same
  line.

All three exist for the same reason: two SDK decoders must read identical
bytes into identical values, and duplicate keys and 2^53-adjacent integers
are exactly where JavaScript's `JSON.parse` and a Rust decoder diverge.

Field names are `snake_case`; frame tags and enum values are `kebab-case`,
matching the capability vocabulary the Wasm lane already established.

## Frame vocabulary

The two directions have distinct closed frame sets (`WorkerMessage`,
`HostMessage`), discriminated by a `frame` tag. Worker frames: `hello`,
`call`, `reply`, `stream-open`, `stream-data`, `credit`, `cancel`, `release`,
`release-ack`, `goodbye`. Host frames replace `hello` with the two handshake
terminals `accept` and `refuse`. Control frames — everything that is not a
call, reply, or stream-data frame: handshake, stream-open, credit, cancel,
release, release-ack, goodbye — are bounded at 16 KiB; a call payload at
256 KiB; an inline result at 64 KiB; a stream data frame at 64 KiB. The
bounds bind the host's own application API too: an outbound payload over its
class bound is refused before it reaches the wire, because the session will
not queue a frame the other side is contracted to kill.

## Handshake

The first worker frame must be a `hello` naming the protocol versions the
worker speaks, its SDK identity, and two feature lists: `features` it can use
if offered and `required_features` it cannot run without. Anything else
first, or a second hello, ends the connection.

The host answers `accept` with the chosen version, the feature intersection
in force, and every byte bound and count ceiling the session will enforce —
announced, not negotiable; a worker that cannot live with a ceiling says
goodbye. A version mismatch is refused with the host's supported list, so the
worker fails with a diagnostic instead of a bare close. A required feature
the host does not know fails closed.

Negotiation is startup-only and stateful. This lane is one activation-scoped
process with a lifetime, not a horizontally scaled stateless service, so no
later frame re-carries a version.

## Calls

Either side calls the other: a `call` carries a caller-chosen id, a method
name, an optional millisecond deadline, a stream flag, and a JSON payload.
Ids are per-direction spaces; each id must be nonzero, within the I-JSON
bound, and never reused for the lifetime of the session — a refused call
retires its id exactly as an answered one does, and reuse is the same
correlation break as a duplicate in flight.

Every call gets exactly one terminal `reply`, with one of four outcomes:
`ok` with an inline result, `spilled` with an artifact offer, `err` with a
wire error, or `cancelled` with a reason. A second terminal for the same
call is refused on the sending side; on the receiving side it is
indistinguishable from a late reply racing a local settlement, so it is
ignored, while a terminal for an id never minted is fatal.

In-flight ceilings are enforced by refusal (`resource-exhausted`,
retryable), never by queueing: refusal is the only bound that does not
become host memory. A deadline the worker outlives is enforced by the host —
a `cancel` goes out and the call settles as `deadline-exceeded` with
reconciliation required, because the worker may have acted before the budget
ran out. A late reply after that settlement is a tolerated race, not a
fault.

## Streams

A call with the stream flag set may deliver items before its terminal. The
callee acknowledges with `stream-open`, granting initial credit; each
`stream-data` frame carries a monotonic sequence number, a continuation bit,
a class, and a running drop count. The terminal `reply` still ends the call.

Credit is counted in frames, not bytes — every stream frame is already
byte-bounded, so frame count is the dimension a hostile producer could still
flood. `lossless` items spend credit and exceeding the window is fatal;
`lossy` items (progress, logs) spend none but must carry a monotonic count
of what was dropped. `credit` frames widen the window up to the
`max_stream_credit` ceiling the accept announced (1024 by default), in both
directions: the worker widens the host's window with credit frames, and the
host widens the worker's through the same frame the other way. A consumer can `cancel` with the stream target to unsubscribe:
items are still validated and still spend credit, but stop being delivered;
the terminal still lands.

## Errors and faults

`WireErrorKind` is a closed set of sixteen kinds naming failures of the
protocol boundary itself. Capability refusals are not wire errors: they are
answers inside a successful call, exactly as in the Wasm lane.

Faults split two ways. A refusable fault answers one call and the session
continues: an oversize call payload, a ceiling, a bad artifact read. A fatal
fault poisons the session: framing violations, strict-JSON and field-bound
violations, id reuse, credit overdraw, handle desyncs. On a
fatal fault the host says goodbye naming the kind and nothing else, settles
in-flight work, and ignores every later input.

Nothing the worker sent is echoed back in any refusal, and error details are
bounded at 512 characters — an echo is a reflection surface.

## Artifact spill

A result over the inline ceiling must spill; silent truncation is not an
option the protocol offers. The producing side keeps the bytes and replies
`spilled` with an `ArtifactOffer`: a handle, the byte count, a media type,
and a BLAKE3 digest, so the reader can refuse before the first pull and
verify after the last.

The consumer pull-reads through ordinary calls to `artifact.read`
(handle, offset, length), served by the session itself. Chunks cross the
wire hex-encoded inside a normal call result, bounded at 24 KiB of raw bytes
per read so a maximal chunk stays under the inline ceiling — more round
trips for a large artifact, and no third framing rule for two SDKs to get
subtly wrong. Reads outside the offer, over the chunk bound, or of zero
length are refused per-call (`invalid-read`).

An artifact handle must be explicitly released when the reader is done.

## Resource handles

Handles carry both artifact offers and brokered capability grants. No lane
this protocol serves runs finalizers reliably — the Wasm lane measured GC
never releasing an undisposed handle — so release is wire law: an explicit
`release` frame naming the handle and its kind, acknowledged by
`release-ack`, backed by a host-side live-handle gauge with a ceiling.

Releasing a handle not held, releasing twice, and releasing with the wrong
kind are all fatal — the same desync the Wasm lane traps as a double
dispose. A released id is never minted again, which closes the release/reuse
race. The mechanism is symmetric: the host releases a worker-held handle
with the same `release` frame and reads the worker's `release-ack`; an ack
for a release the host never sent is fatal. The paths a release frame never
travels are reclaimed host-side: a call that minted handles and then settled
`err` or `cancelled` reclaims them, and goodbye, disconnect, and fatal
faults reclaim everything.

## Cancellation and loss

A cancelled call still terminates: the worker acknowledges a `cancel` by
answering, usually with the `cancelled` outcome. Silence would be
indistinguishable from a lost worker. A completion that raced the cancel
wins — the work happened, and the outcome says so.

Loss is classified, not collapsed:

- After a worker `goodbye`, in-flight host calls settle as `cancelled`
  without reconciliation — the worker said it stopped.
- After a bare disconnect, clean byte boundary or mid-frame, they settle as
  `outcome-unknown` with reconciliation required. A disconnect is loss of
  the worker, never proof that its external actions failed.

## Generated reference

The Rust types in `crates/yah-plugin-ipc/src/types.rs` are the source of
truth and generate three checked-in artifacts:

- `generated/worker-protocol/worker.schema.json`
- `generated/worker-protocol/host.schema.json`
- `generated/worker-protocol/protocol.ts`

Run `cargo run --locked --manifest-path tools/protocol-codegen/Cargo.toml`
after changing protocol types; the same command with `-- --check` is what the
local gate runs, and it rejects stale generated files.

## Not implemented

- The process driver: transport, spawn, bootstrap fd, peer-credential
  attestation, kill, and restart all live outside this crate.
- Any worker SDK. The TypeScript declarations and JSON Schemas are
  generated; no Node or CPython worker exists to load them.
- Reconnect or resume. A session that faults or loses its transport is
  over; there is deliberately no resync path.
- Any binding from the capability broker to these handles. The wire encoding
  for a brokered resource exists; nothing yet mints one from a real grant.
- Worker-side enforcement evidence. The fixtures drive the host session
  only; a worker SDK must pass its own conformance against the same corpus
  shape.
