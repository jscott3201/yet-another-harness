# Application Protocol

Open Agent's application protocol separates commands, receipts, views, events,
and server requests from any transport. Adapter 1 is an in-process adapter that
still serializes and deserializes JSON so tests exercise the wire shapes.

Adapter 1 assumes a trusted caller in the daemon process. It does not establish
a transport identity, isolate subscriptions between sessions, or protect token
keys from a reader that already has the control-graph files. Those boundaries
belong to the daemon and transport adapters.

## Source of Truth

Rust types under `crates/oa-kernel/src/protocol/` are authoritative. They
generate:

- client and server JSON Schemas under `generated/protocol/`;
- TypeScript at `generated/protocol/protocol.ts`.

Regenerate after changing a protocol type:

```bash
cargo run --locked -p oa-kernel --bin generate-protocol
```

Check without writing:

```bash
cargo run --locked -p oa-kernel --bin generate-protocol -- --check
```

The pre-commit and full local gates run the check. Checked-in artifacts make a
wire-shape change visible in code review.

Selene is pinned to an exact public Git revision. Hosted CI runs generated-file
checks, locked workspace Clippy, and locked workspace tests.

## Implemented Adapter 1 Slice

- Forced JSON encode and decode for commands and responses.
- Closed command and error enums.
- Canonical decimal strings for `u64` wire values.
- RFC 8785 canonical command preimages with BLAKE3 request digests; raw JSON
  rejects duplicate object names and integers outside the I-JSON safe range.
- Raw inbound limits: 1 MiB per command, 256 KiB per command payload, 16 KiB
  per control frame, and 1 MiB plus 4 KiB per outer frame. Results and semantic
  event payloads over their inline limits fail closed until artifact
  indirection is implemented.
- Typed command payload variants for the implemented kernel methods.
- Project-bound command submission and cursor namespaces.
- Durable replay of authority commands and deterministic keyed rejections after
  digest validation, plus receipt-owned event cursor projection. Adapter 1 uses
  the synthetic `daemon-local` principal for authority commands. Holder replay
  requires a token resolvable in the current adapter lifetime; after restart,
  an unresolvable holder failure is unkeyed because Adapter 1 cannot bind it to
  a verified holder.
- `authority_epoch` is envelope state rather than digest input. Adapter 1 checks
  it before receipt lookup, so a caller must refresh the epoch after takeover
  before replaying the same command ID.
- Opaque wire holder tokens kept one per active unit. Authority takeover drops
  the in-memory index and fences every token from the prior lifetime. Replaying
  a stale dispatch receipt can reproduce its token value but does not reactivate
  that token. The Adapter 1 control graph is not a credential vault.
- Cursor resume in responses bounded to 1024 events and 1 MiB plus 4 KiB of
  serialized JSON. Larger catch-up requires an in-memory subscription over the
  durable event journal. Adapter 1 returns typed expiry below the durable floor
  and advertises the floor plus the 14-day/1,000,000-event defaults.
- A bounded 1024-event in-memory subscription queue with typed `slow_consumer`
  closure, shared event frames, a 16 MiB aggregate queue budget, at most 64
  open subscriptions per project, and cursor-based reconnect after restart.
- Durable actor, timestamp, causation, and correlation event metadata.
- Fail-closed startup when retained event or receipt records cannot be decoded,
  bounded, or linked by their durable ownership fields. Stream reads repeat
  event projection validation; dispatch-result claims are checked when replayed.

## Current Command Methods

| Method | Purpose |
|---|---|
| `run.open` | Create a run root |
| `run.close` | Close a run with an explicit outcome |
| `work_item.create` | Create a work item and its pinned acceptance contract |
| `unit.admit` | Admit an execution unit into a run |
| `unit.dispatch` | Open a new attempt and holder lease |
| `unit.progress_report` | Validate a holder fence; extension fields are rejected until the ephemeral progress plane exists |
| `unit.stamp_bump` | Invalidate outstanding holder tokens |

Effect and cancellation funnel methods exist internally. None are exposed
through the first public protocol registry yet.

## Remaining Adapter 1 Work

- Race-free bootstrap: snapshot plus live stream in one server operation.
- A bounded progress queue with oldest-first dropping and one dropped-count
  marker. Adapter 1 advertises the planned 256-entry capacity but does not
  deliver progress events yet.
- Durable server requests for approvals and restart re-presentation.
- Receipt lookup queries and current-state views.
- Content-addressed artifact storage and `ArtifactRef` substitution for results
  or semantic event payloads that exceed inline limits.
- Authority-issued token reauthorization after policy and approval validation;
  reissue attempts currently fail closed.
- Age/count retention pruning and retention checkpoints.
- Full ADR-002 conformance traces and fault injection.
- Read-side handling for unknown event and error enum members across protocol
  versions.

UDS, named-pipe, HTTP/SSE, remote, QUIC, and MCP work are outside this Adapter 1
lane.
