# Project Status

Yet Another Harness (YAH) is pre-0.1 and is undergoing an architectural pivot
from a narrow model-free reliability kernel to a complete Rust-native,
graph-backed, plugin-extensible agent harness.

There is no usable release. The repository contains a tested reliability
foundation and the first process-local composition lifecycle, cleanup, and
typed service-binding primitives; it does not yet contain a runnable
composition host, plugin host, agent loop, sandbox, daemon, or client.

## Evidence Status

| Area | Evidence | Status |
|---|---|---|
| G02 storage fan-in and crash recovery | Atomic state, receipt, and journal commits under kill/reopen, writer takeover, and corruption drills | Passed across 1,440 scored trials: [report](gates/G02-storage-fanin-recovery.md) |
| Current model-free kernel | Deterministic tests for command, fencing, cancellation, effect, provider, recovery, and Adapter 1 behavior | Available for reuse; pivot integration has not started |
| Contextual composition runtime | Component definition/instance/scope identities, epoch-fenced lifecycle, reversible nested effects, typed requirements, and exact revocable provider bindings | Initial slices; no callbacks, contextual provider selection, or reconciler |
| Plugin manifest, SDK, and conformance suite | No implementation in this repository | Not started |
| Wasm, Node/TypeScript, and Python plugin drivers | No implementation in this repository | Not started |
| Selene work, session, memory, evidence, and plugin-lineage domains | Only the current kernel graph exists | Design/spike stage |
| Full harness vertical slice | No live model, tool execution, memory loop, or subagent | Not started |
| Sandbox, daemon, CLI, and UI | No implementation in this repository | Not started |

The earlier G01 gate definition and its fixed obligation count are no longer the
project roadmap. Individual tests and invariants remain useful evidence, but
the gate is being re-scoped around the new harness architecture.

## Implemented Foundation

### Live component lifecycle

- A small `yah-compose` crate separated from Selene and the durable
  external-effect kernel.
- Opaque component definition, component instance, and scope identities with
  explicit scope parentage.
- Pending, starting, active, failed, stopping, and removed states.
- Instance-incarnation-bound, monotonic activation epochs that reject stale
  start completions, failure reports, and stop requests or completions without
  mutating the current activation.
- Controlled stop targets for recomposition back to pending or terminal removal.
- Retained last-failure diagnostics after teardown and retry.

Lifecycle bookkeeping remains synchronous. It does not invoke component code,
resolve services, or reconcile desired state.

### Reversible local effect scopes

- Generated, activation-bound root and nested scope identities, separate
  diagnostic labels, and downward-only read-only cancellation observation.
- One deterministic reverse-order stack for synchronous cleanup, asynchronous
  cleanup, and child scopes.
- Cached close reports and resumable pending close. When close is driven to
  completion, every callback is attempted once; repeated or resumed closes do
  not rerun completed callbacks.
- Aggregation of returned errors and unwind panics without short-circuiting.
- Explicit separation from durable external-effect settlement.

Cleanup is executor-neutral and sequential. The current layer does not provide
deadlines, forced termination, concurrent admission/close, task supervision, or
proof that cancellation stopped work. Dropping a scope requests cancellation
but does not run its cleanup.

### Typed live services

- Stable semantic service IDs paired with exact process-local Rust contract
  types, including unsized trait contracts.
- Required-service declarations on component definitions and deterministic
  ready/missing reports that let a caller leave an ineligible instance pending.
- Multiple provider candidates in deterministic provider-registration-ID
  order, with no implicit ranking, publication-order promise, or selection.
- Provider publication only after synchronous withdrawal cleanup is admitted to
  the active provider's effect scope.
- Exact provider-registration bindings whose handles gate every call and fail
  closed when the provider scope, consumer scope, or registry is revoked.
- Generated provider identities reuse their unique effect registration, so a
  stale handle or delayed cleanup cannot target a replacement publication.

The registry is one flat, process-local visibility domain. It reports readiness
but does not mutate lifecycle state. Contextual inheritance and isolation,
provider-selection epochs, watches, dependent stop/rebind/start, and automatic
reconciliation remain future work. Live service values are not serialized or
stored in Selene.

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

The next useful proof extends the composition core into a narrow vertical slice
containing:

1. executable component callbacks and service-dependency reconciliation;
2. provider selection and recomposition over the existing typed bindings,
   epoch-fenced lifecycle, and effect scopes;
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

- Component callback runner, contextual service inheritance/isolation,
  provider-selection epochs, dependency reconciliation, concurrent scope/task
  supervisor, desired-state reconciler, or isolation realms.
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
