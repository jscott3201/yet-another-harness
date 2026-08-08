# Project Status

Open Agent is pre-0.1. The repository contains a tested kernel foundation, not
an installable agent runtime.

## Gate Status

| Gate | Evidence | Status |
|---|---|---|
| G02: storage fan-in and crash recovery | Atomic state, receipt, and journal commits under kill/reopen, writer takeover, and corruption drills | Passed: [report](gates/G02-storage-fanin-recovery.md) |
| G01: model-free kernel | Command, effect, cancellation, protocol, replay, and coordination semantics without live models or real effects | In progress |
| Adapters 2-4 | The application conformance corpus over UDS, named pipe, and loopback HTTP/SSE | Not started |
| Installation and execution | Clean-machine install, daemon lifecycle, and real execution-backend tests | Not started |
| Multi-agent evaluation | Frozen live-model evaluation arms, budgets, and thresholds | Blocked on G01 and unresolved evaluation parameters |

## Implemented

- Deterministic provider fixtures for OpenAI Responses and Anthropic Messages
  stream shapes.
- Typed provider normalization and provider error classification.
- A single Selene-backed mutation funnel for state, semantic events, and
  durable command receipts.
- Attempt epochs, authority epochs, leases, stamps, and stale-holder rejection.
- Internal-funnel external-effect preparation, dispatch evidence, settlement,
  and durable parking for outcomes that still need reconciliation. The
  query-before-retry reconciliation worker is not implemented.
- Internal-funnel durable cancellation requests, frozen scopes, delivery
  records, and cancellation-aware admission. Adapter 1 does not expose effect
  or cancellation commands yet.
- Adapter 1 foundations: forced JSON round trips, canonical request digests,
  duplicate-name and unsafe-integer rejection, generated JSON Schema and
  TypeScript, principal-bound receipt projection, scoped read-only receipt
  lookup, opaque holder tokens, durable cursor resume bounded by event count and
  response bytes, project binding, retained-cursor expiry, strict durable event
  and receipt reads, and R33 retention advertisement. Receipt lookup covers
  physically retained public-command rows and never returns holder credentials;
  internal effect and cancellation receipts are validated but not exposed.
- Adapter 1 in-memory subscriptions over durable events: the normative
  1024-event queue, typed `slow_consumer` closure, a 16 MiB aggregate queue
  budget, a 64-subscription project cap, last-delivered cursor, and bounded
  cursor reconnect.
- A 700-line review cap for all tracked or unignored reviewable files in local
  and CI text gates, including checked-in generated protocol artifacts.

## Not Implemented

- Daemon process, discovery, startup, drain, or shutdown lifecycle.
- End-user CLI or GUI.
- Live model-provider adapters.
- Real process, filesystem, Git, or network effect execution.
- Sandbox enforcement.
- Race-free bootstrap snapshots.
- Progress event delivery, bounded queues, and dropped-count markers. The
  protocol currently advertises the planned 256-entry capacity only.
- Artifact storage and oversized result/event `ArtifactRef` substitution.
- Policy- and approval-backed token reauthorization after a stamp bump.
- Durable server-to-client approval requests.
- Protocol views and logical receipt expiry under the future retention floor.
- Retention pruning and verified retention checkpoints.
- Replay verifier over all implemented aggregates.
- UDS, Windows named-pipe, HTTP/SSE, remote, or QUIC adapters.
- MCP lifecycle or OAuth support.
- User/project configuration resolution, effective-value provenance, opt-in
  grants, generated configuration schemas, or extension admission.
- Ratatui terminal client, responsive panes, workflow/configuration screens, or
  TUI reconnect and terminal-restoration tests.

## Gate Discipline

A gate passes only when every mandatory row has deterministic evidence. Partial
implementation can move a gate to in progress but cannot weaken its pass bar.
The current G01 design has 26 mandatory exit obligations.
