# Project Status

Open Agent is pre-0.1 and is undergoing an architectural pivot from a narrow
model-free reliability kernel to a complete Rust-native, graph-backed,
plugin-extensible agent harness.

There is no usable release. The repository contains a tested foundation that may
be integrated into the new architecture; it does not yet contain the
composition runtime, plugin host, agent loop, sandbox, daemon, or client.

## Evidence Status

| Area | Evidence | Status |
|---|---|---|
| G02 storage fan-in and crash recovery | Atomic state, receipt, and journal commits under kill/reopen, writer takeover, and corruption drills | Passed across 1,440 scored trials: [report](gates/G02-storage-fanin-recovery.md) |
| Current model-free kernel | Deterministic tests for command, fencing, cancellation, effect, provider, recovery, and Adapter 1 behavior | Available for reuse; pivot integration has not started |
| Contextual composition runtime | No implementation in this repository | Not started |
| Plugin manifest, SDK, and conformance suite | No implementation in this repository | Not started |
| Wasm, Node/TypeScript, and Python plugin drivers | No implementation in this repository | Not started |
| Selene work, session, memory, evidence, and plugin-lineage domains | Only the current kernel graph exists | Design/spike stage |
| Full harness vertical slice | No live model, tool execution, memory loop, or subagent | Not started |
| Sandbox, daemon, CLI, and UI | No implementation in this repository | Not started |

The earlier G01 gate definition and its fixed obligation count are no longer the
project roadmap. Individual tests and invariants remain useful evidence, but
the gate is being re-scoped around the new harness architecture.

## Implemented Foundation

### Selene-backed mutation and recovery

- A single mutation funnel for aggregate state, semantic events, and durable
  command receipts.
- Atomic commit of accepted state transitions, event cursor ranges, and
  receipts.
- Project identity, authority epochs, attempt epochs, stamps, leases, and
  stale-holder rejection.
- Kill/reopen, takeover, audit, and corruption evidence from the G02 harness.

### External effects and cancellation

- External-effect preparation, dispatch evidence, settlement, uncertainty,
  and durable parking.
- The `no_retry` class and target-observation validation.
- Durable cancellation requests with frozen scopes, delivery records, and
  cancellation-aware admission.
- Separation between sending cancellation and proving that work stopped.

Query-before-retry reconciliation and real effect execution are not
implemented.

### Provider normalization

- Deterministic OpenAI Responses and Anthropic Messages fixture catalogs.
- Stream parsing, normalized events, usage, finish reasons, malformed stream
  handling, typed errors, tool calls, and cancellation cases.
- No live provider transport or credential handling.

### Adapter 1 protocol experiment

- Forced JSON round trips and canonical request digests.
- Generated JSON Schema and TypeScript artifacts.
- Idempotent command receipts, scoped receipt lookup, opaque holder tokens,
  durable cursor resume, and retained-cursor expiry.
- In-memory subscriptions over durable events with bounded queues and typed
  slow-consumer closure.

This adapter is evidence for current kernel behavior. It is not yet the public
plugin SDK, daemon protocol, or promised compatibility surface.

## Pivot Work

The next useful proof is a narrow vertical slice containing:

1. a Rust component lifecycle and service-dependency graph;
2. nested local effect scopes and provider recomposition;
3. a small plugin manifest and capability grant;
4. one Selene graph/memory host capability;
5. one Wasm component plus one modern Node or Python process plugin;
6. a durable external action demonstrating that plugin unload and action
   settlement remain distinct; and
7. crash/restart reconstruction with behavioral conformance tests.

After that slice, the harness can grow model, prompt, tool, session, workflow,
subagent, sandbox, daemon, and client components without fixing every final
crate or protocol boundary in advance.

## Not Implemented

- Contextual component registry, service graph, effect scopes, desired-state
  reconciler, or isolation realms.
- Plugin packages, manifests, admission, installation, updates, SDKs, or
  language conformance tests.
- Wasmtime/WIT host or sandboxed Node/TypeScript and CPython workers.
- Durable memory capture, retrieval, ranking, summaries, or evidence lineage.
- Live model providers, prompt assembly, tool execution, workflows, schedules,
  goals, or subagents.
- Filesystem, process, Git, network, container, or VM sandbox enforcement.
- Artifact storage and oversized-result substitution.
- Approval requests, credential brokerage, or effective capability grants.
- Daemon lifecycle, discovery, installation, CLI, TUI, web UI, or native
  embedding SDK.
- MCP lifecycle, remote transports, multi-user identity, or hosted operation.
- End-to-end live-model evaluations and release packaging.

## Working Method

Public documentation distinguishes implemented behavior from direction. The
detailed pivot specification and sprint tracker remain local while the design
is fluid. A design becomes a project commitment when a vertical slice and its
negative, recovery, and conformance tests establish the behavior.

See the [architecture](architecture.md) for the target shape and the root
[README](../README.md#prior-art-and-attribution) for pinned prior-art sources.
