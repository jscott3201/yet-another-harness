# Project Status

Yet Another Harness (YAH) is pre-0.1 and is undergoing an architectural pivot
from a narrow model-free reliability kernel to a complete Rust-native,
graph-backed, plugin-extensible agent harness.

There is no usable release. The repository contains a tested reliability
foundation and the first process-local composition lifecycle, cleanup, typed
service-binding, exact-assignment dependency reconciliation, and fenced desired
component-revision primitives, plus an independently authored semantic
conformance corpus; it does not yet contain a runnable composition host, plugin
host, agent loop, sandbox, daemon, or client.

## Evidence Status

| Area | Evidence | Status |
|---|---|---|
| G02 storage fan-in and crash recovery | Atomic state, receipt, and journal commits under kill/reopen, writer takeover, and corruption drills | Passed across 1,440 scored trials: [report](gates/G02-storage-fanin-recovery.md) |
| Current model-free kernel | Deterministic tests for command, fencing, cancellation, effect, provider, recovery, and Adapter 1 behavior | Available for reuse; pivot integration has not started |
| Contextual composition runtime | Component definition/instance/scope identities, epoch-fenced lifecycle, reversible nested effects, typed requirements, exact revocable bindings, exact-assignment recomposition, fenced desired revisions, and six cross-layer semantic cases | Initial slices with deterministic conformance evidence; no callbacks, provider-ranking policy, contextual visibility, or host-wide scheduler |
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

The registry is one flat, process-local visibility domain. Its low-level
inventory and binding operations do not choose providers or mutate lifecycle.
Live service values are not serialized or stored in Selene.

### Dependency reconciliation

- A reconciled component uniquely owns a consumed, frozen definition, its live
  instance, one activation effect scope, and one immutable provider selection.
- Caller-supplied assignments must cover every frozen requirement with one
  exact currently visible registration; missing, unassigned, ambiguous, and
  unavailable choices remain distinguishable while the instance is pending.
- Provider-selection epochs reuse the process-unique activation epoch because
  changing any assignment always requires a fresh activation.
- Assignment change or selected-provider withdrawal enters controlled stop and
  synchronously cancels the old activation before returning. Existing handles
  fail closed and never follow a replacement in place.
- Start completion revalidates every committed exact provider. Clean teardown
  must reach pending before a later level-triggered pass can start current
  assignments; non-clean cleanup reports remain observable and blocked in
  stopping.
- Exact-epoch activation failure records the inferred starting or active phase,
  synchronously seals the same activation effects, and enters controlled
  teardown toward pending without selecting automatic retry policy.

This layer deliberately supplies no implicit first-provider policy. Additional
candidates do not disturb an explicit live assignment. Registry watches,
background convergence, callback execution, contextual inheritance/isolation,
provider ranking, cycles, and retry policy remain future work.

### Desired component revisions

- A stable component slot mints process-incarnation-bound tokens around
  caller-sequenced, level-triggered desired generations for enabled, disabled,
  or removed state.
- Opaque component revision IDs freeze one definition, scope, and loader-owned
  configuration/factory identity. Reusing an ID with different modeled content
  is rejected without touching the live revision.
- Assignment-only changes reactivate the same mounted instance. Revision
  changes, disable, and removal synchronously seal the old activation and reach
  terminal removal before any replacement mounts.
- Disabled intent retains its desired revision but owns no mounted component;
  re-enabling therefore creates a fresh instance incarnation.
- Desired changes received during teardown replace the stored desired state but
  never reverse the frozen stop already in progress. The next pass applies only
  the latest accepted generation.
- Terminal cleanup errors preserve the old applied revision and block progress.
  An explicit epoch-fenced abandonment records the non-clean report and may
  advance the frozen stop target without rerunning consumed cleanup.

This is a process-local single-component mechanism. Whole-graph scheduling,
callback execution, durable configuration payloads, automatic watches,
concurrent desired writers, rollback, retry/backoff, and persistence remain
unimplemented. Exact provider registration IDs are live values and are not
durable desired state.

### Composition semantic conformance

- Six independently authored black-box cases compose the public lifecycle,
  effect, service, dependency, and desired-state APIs rather than restating one
  module's unit behavior.
- The cases cover tree-aware cleanup, provider readiness and explicit pending
  assignment, controlled exact replacement, separate registry visibility
  domains, activation-failure rollback with sibling survival, and latest-intent
  revision churn during suspended cleanup.
- Manual polling and explicit signals keep the scenarios deterministic. The
  corpus records observable traces, revocation, retained reports, epochs, and
  mounted revisions.
- The [corpus contract](composition-conformance.md) documents its pinned Cordis
  inspiration and the behaviors deliberately not copied.

Registry-domain separation is a coarse process-local boundary, not contextual
scope inheritance or shared isolation realms. Desired churn is caller-driven,
not filesystem HMR. Concurrent fault injection remains a separate validation
slice.

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

1. executable component callbacks over the dependency reconciler;
2. provider policy and contextual recomposition over the existing exact
   assignments, typed bindings, epoch-fenced lifecycle, and effect scopes;
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
  automatic provider ranking or registry watches, concurrent scope/task
  supervisor, host-wide desired-graph scheduler, or isolation realms.
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
