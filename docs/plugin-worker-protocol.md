# Worker Process Wire Protocol

This page documents protocol v1 between the YAH host and a supervised worker
process, implemented in `crates/yah-plugin-ipc`. It is the wire contract the
supervised process driver serves and the future Node and CPython worker SDKs
must satisfy, specified and fixture-pinned before either worker runtime
exists.

The crate is sans-io: `frame` turns bytes into frames incrementally, and
`session` is a pure state machine with an injected millisecond clock. Nothing
in it can block, sleep, spawn, or open a socket. The real IO lives in the
[process driver](#process-driver) (`crates/yah-plugin-proc`), which spawns
the worker, owns the transport and the kill path, and authenticates the
bootstrap; no worker-side SDK exists yet. One hundred thirteen
deterministic fixtures in `crates/yah-plugin-ipc/tests/` drive the host
side of every rule below with a scripted byte-level peer.

## Framing and strict JSON

A frame is a 4-byte big-endian byte count followed by exactly that many
bytes of one JSON value. The prefix is checked against `MAX_FRAME_BYTES`
(1 MiB + 4 KiB) as soon as the four prefix bytes arrive, without waiting
for the payload — bytes the transport already handed over are discarded at
the poison, so exposure is one transport read, never a frame. A zero-length
or oversize declaration poisons the connection. Framing carries no resync
marker on purpose: after one violation, later bytes are unattributable, so
there is no resync path.

Frame JSON is strict, mirroring the kernel protocol's rules:

- A duplicate member name is refused, not last-wins resolved.
- An integer outside the I-JSON safe range (±2^53−1) is refused, not
  rounded — including a literal too wide for a 64-bit lane, caught on the
  raw token before the parser can round it to a float.
- Unknown members and unknown enum values are refused. Enums are closed
  within a negotiated version: evolution happens at the version gate in the
  handshake, never by per-member leniency.
- Field bounds are enforced at admission and mirrored by the generated
  schemas: ids in `1..=2^53−1`, SDK identity 1–64 characters, method names
  1–128, media types 1–128, goodbye reasons at most 256, error messages at
  most 512, a spilled offer's byte count at least one, and its digest
  exactly 64 lowercase hex characters. A frame a conformant schema
  validator would refuse, this host refuses at the same line.

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
bounds bind the host's own application API too, field bounds included: an
outbound payload over its class bound, a method name or media type outside
its length bound, or a hand-built offer the admission rules would refuse is
refused before it reaches the wire, because the session will not queue a
frame the other side is contracted to kill. The one asymmetry is
deliberate: an outbound error message over the detail bound is clipped
rather than refused, so reporting one error never manufactures a second.

## Handshake

The first worker frame must be a `hello` naming the protocol versions the
worker speaks, its SDK identity, and two feature lists: `features` it can use
if offered and `required_features` it cannot run without. Anything else
first, or a second hello, ends the connection.

The host answers `accept` with the chosen version, the feature intersection
in force, and every byte bound and count ceiling the session will enforce —
announced, not negotiable; a worker that cannot live with a ceiling says
goodbye. One announced number is advisory rather than law:
`initial_stream_credit` is the opening window an SDK should grant when it
has no better one, and any opening grant in `1..=max_stream_credit` is
legal. A version mismatch is refused with the host's supported list, so the
worker fails with a diagnostic instead of a bare close. A required feature
the host does not know fails closed. A hello whose own fields fail
admission — an SDK identity outside its length bound — is refused with the
same diagnostic shape. Before negotiation, only a frame the host cannot
decode at all — a framing violation, a strict-JSON refusal, an over-bound
control frame — closes without a diagnostic frame.

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
retryable — under a fresh id, since the refused one is spent), never by
queueing: a queue here is unbounded memory a hostile worker controls. A
refusal does retain one thing, the spent id. Ids are never reused, so
every id the session has seen stays in a retired set for the session's
lifetime; that is a few bytes per call, and bounding it is the supervising
driver's job, by bounding how long a session lives. A deadline the worker
outlives is enforced by the host — a `cancel` goes out and the call
settles as `deadline-exceeded` with reconciliation required, because the
worker may have acted before the budget ran out. A late reply after that
settlement is a tolerated race, not a fault.

## Streams

A call with the stream flag set may deliver items before its terminal. The
callee acknowledges with `stream-open`, granting initial credit; each
`stream-data` frame carries a monotonic sequence number, a continuation bit,
a class, and a running drop count. The terminal `reply` still ends the call.

Credit is counted in frames, not bytes — every stream frame is already
byte-bounded, so frame count is the dimension a hostile producer could still
flood. `lossless` items spend credit and exceeding the window is fatal;
`lossy` items (progress, logs) spend none but must carry a monotonic count
of what was dropped. The count rides data frames and nothing else: drops
recorded after a stream's final item are refused on the recording side,
and drops recorded before a terminal that follows with no further item are
lost — the gap information has no frame left to travel on. `credit`
frames widen the window up to the
`max_stream_credit` ceiling the accept announced (1024 by default) — a
bound on the outstanding, unspent window at any moment, not on the sum of
grants over the stream's life — and they widen it in both directions: the
worker widens the host's window with credit frames, and the host widens the
worker's through the same frame the other way. A consumer can `cancel` with
the stream target to unsubscribe: items are still validated and still spend
credit, but stop being delivered; the terminal still lands.

## Errors and faults

`WireErrorKind` is a closed set of sixteen kinds naming failures of the
protocol boundary itself. Capability refusals are not wire errors: they are
answers inside a successful call, exactly as in the Wasm lane.

Faults split two ways. A refusable fault answers one call and the session
continues: an oversize call payload, a ceiling, and everything a served
`artifact.read` can get wrong — a malformed read payload, a handle that is
unknown, released, or the wrong kind, a read outside the offer — because
those arrive inside a call the session can answer. A fatal fault poisons
the session: framing violations, strict-JSON and field-bound violations, id
reuse, credit overdraw, handle desyncs — a spilled offer reusing a handle
id among them — an oversize inline result or stream item, where the
sender had the spill path or the byte bound and violated it instead, and
the stream-order family: data before open or after the last item, an open
on a call that did not ask to stream, a sequence gap, a second open, a
credit value of zero or past the ceiling at open or at widening, a drop
count going backwards, a zero-byte spilled offer. A kind can sit on both
sides of the split:
`invalid-frame` and `unknown-handle` are fatal at frame admission
and refusable inside a served read, `payload-too-large` is refusable on a
call payload and fatal on a result or stream item, and
`resource-exhausted` is refusable at a ceiling and fatal on a credit
overdraw. On a fatal fault the host says goodbye
naming the kind and nothing else, settles in-flight work, and ignores every
later input.

Nothing the worker sent is echoed back in any refusal, and error details
are bounded at 512 characters in both directions — over the bound is fatal
inbound and clipped outbound — because an echo is a reflection surface.

## Artifact spill

A result over the inline ceiling must spill; silent truncation is not an
option the protocol offers. The producing side keeps the bytes and replies
`spilled` with an `ArtifactOffer`: a handle, the byte count, a media type,
and a BLAKE3 digest, so the reader can refuse before the first pull and
verify after the last.

On the sending side, a spilled reply must name an offer minted while
serving that same call — any other call's failing terminal could reclaim
it behind the reader's back — and the offer must describe the held
artifact exactly: size, media type, digest. On the receiving side, an
offer's handle id must be fresh: handle ids are never reused, so a repeat
is the same correlation break a duplicate call id is.

The consumer pull-reads through ordinary calls to `artifact.read`, served
by the session itself. The request payload is `{"handle", "offset",
"len"}` — exactly those three members, unknown members refused — and the
result is `{"bytes_hex", "media_type"}`; these two shapes live in this
prose and the serving code, not yet in the generated artifacts. Chunks cross the
wire hex-encoded inside a normal call result, bounded at 24 KiB of raw bytes
per read so a maximal chunk stays under the inline ceiling — more round
trips for a large artifact, and no third framing rule for two SDKs to get
subtly wrong. Reads outside the offer, over the chunk bound, or of zero
length are refused per-call (`invalid-read`). A read clears the same
admission bar as any other call — payload bound, then the in-flight
ceiling — and is then answered in the same turn, so it never occupies the
slot it was admitted under.

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
with the same `release` frame and reads the worker's `release-ack` — and
only a handle the worker actually offered; the session refuses its own
application a release for any other id, which would arm a desync against
a worker that did nothing wrong. An ack
for a release the host never sent is fatal, and so is an ack naming a kind
the release did not; an acked id is spent, so the host refuses its own
application a second release of it. The paths a release frame never
travels are reclaimed host-side: a call that minted handles and then settled
`err` or `cancelled` reclaims them, and goodbye, disconnect, and fatal
faults reclaim everything — including pending releases, whose acks will
never come. One crossing is tolerated: a worker that learned a handle id
mid-call may have a `release` in flight when the reclaiming terminal
passes it on the wire, so a release naming a reclaimed id is answered with
the ordinary ack rather than a fault — the ack spends the id, and a second
release for it is the double-release desync again.

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

## Process driver

`crates/yah-plugin-proc` supervises one worker process per activation
under the host's driver contract. The bootstrap is authenticated by
construction: the host spawns the worker with one end of a Unix socketpair
as fd 3, and holding that inherited descriptor is the whole credential —
no token in argv (world-readable on most systems), nothing in the
environment, which is cleared to a PATH allowlist, and nothing in the
descriptor table above the channel: everything there is marked
close-on-exec at spawn, so even descriptors the host itself inherited
without the flag cannot ride into a worker (marked rather than closed,
so a misconfigured worker path still fails the spawn by name instead of
masquerading as a protocol disconnect). Node adopts the channel
with `new net.Socket({ fd: 3 })`, CPython with `socket.socket(fileno=3)`.
Stdout and stderr stay what they look like: bounded diagnostic text,
retained tail-first, never protocol bytes.

One activation is one process is one session. A worker that dies poisons
its activation — there is no resume — health reports why, and the driver
watches the process itself, not just its socket, so the death is seen
even when a descendant the worker spawned keeps the channel open.
Deactivation reclaims the process (goodbye, a grace window, `SIGKILL` to
the worker's whole process group, reap, unconditionally in that order)
and settles work the worker still holds as outcome-unknown and
reconcile-required rather than dropping its waiter. Recovery is a fresh
activation with a fresh process. The pump's reads, writes, deadline
ticks, and child-exit watch are independent arms of one event loop, so a
worker that stops draining its socket stalls only its own bytes — and is
cut off at the outbound buffer cap rather than buffered toward host
memory exhaustion — never the clock or the shutdown; and an activation
its composition abandons without deactivating reclaims its own worker
once the last driver handle drops. Loss stays classified through every
exit: a goodbye the worker managed to send settles in-flight work
cancelled without reconciliation even when a descendant keeps the socket
open past its death, while a bare disconnect settles it outcome-unknown,
reconcile-required. A worker that shuts only its read half kills only
the outbound direction — wherever the kernel surfaces that to writers
(Linux does; macOS accepts the writes outright, so the state does not
arise there): the session stays open for its goodbye, health reports
the half-death, and a call that provably never reached the worker
settles at once, cancelled, without reconciliation. The driver passes
the host's five portable lifecycle conformance cases against real
child processes, and a scripted fake worker pins restart, disconnect
with in-flight work, cancellation, deadline expiry with an observed
late answer, the handshake clock, the outbound cap, the forced kill
path with its group sweep, spawn-failure reporting, and the
bootstrap's environment and descriptor table end to end. The fake
worker is a test fixture, not an SDK.

The pathname-socket lane with kernel-attested peer credentials
(`getpeereid`, `SO_PEERCRED`) is deliberately absent: it exists to attach
to a process the host did not spawn, and no current scenario does that.

## Not implemented

- Restart policy. The driver supervises one process per activation and
  never respawns inside one; whether and when a fresh activation follows a
  death is the composition's decision, deliberately deferred.
- Any worker SDK. The TypeScript declarations and JSON Schemas are
  generated, and the driver will spawn anything that speaks the protocol
  over fd 3 — but no Node or CPython worker exists to load them.
- Reconnect or resume. A session that faults or loses its transport is
  over; there is deliberately no resync path.
- Retired-id forgetting. Ids are never reused, so the session remembers
  every call and handle id it has seen, and that memory grows with call
  count for the session's lifetime. The process driver bounds it by
  bounding the session, not through any protocol mechanism.
- Any binding from the capability broker to these handles. The wire encoding
  for a brokered resource exists; nothing yet mints one from a real grant.
- Worker-side enforcement evidence. The fixtures drive the host session
  only; a worker SDK must pass its own conformance against the same corpus
  shape.
