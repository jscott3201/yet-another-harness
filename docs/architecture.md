# Architecture

Yet Another Harness (YAH) is being developed as a Rust-native, graph-backed,
plugin-extensible agent harness. This document describes the stable direction
and marks the boundary between the existing reliability kernel and the target
harness. It is not a frozen crate map or protocol specification.

## System Shape

```text
clients and embedding SDKs
            |
            v
      Rust harness host
      /      |       \
     /       |        \
agent     composition   policy and
runtime      runtime     approval
     \       |        /
      \      |       /
       durable commands
            |
            v
       Selene graph
 work, sessions, memory,
 evidence, artifacts, effects
            |
            v
 capability-scoped plugins
 Rust | Wasm | Node/TS | Python
```

The Rust host is the authority process. It owns lifecycle decisions, policy,
durable state transitions, plugin grants, recovery, and external-effect
reconciliation. An extension can contribute behavior, but it cannot grant
itself authority or commit around the host.

## Contextual Composition

The live runtime is a graph of component definitions and component instances.
A component may provide services, require services, register event handlers,
and create child effect scopes.

The initial lifecycle model is intentionally small:

```text
pending -> starting -> active
pending -> removed
starting | active -> failed
starting | active | failed -> stopping -> pending | removed
```

- A missing required service leaves the consumer pending.
- A compatible provider makes the consumer eligible to start.
- Provider replacement or disappearance causes controlled recomposition of
  affected consumers.
- Every registration belongs to an effect scope. Closing a scope unwinds its
  owned effects, including nested scopes, in reverse registration order. Before
  the first cleanup, explicit close drains mediated service calls already
  admitted against that scope subtree.
- Isolation realms and interceptors can narrow the services and policies
  visible to a subtree.

Composition realms scope service visibility; they are not a security boundary
for hostile code.

The current `yah-compose` slice implements the state bookkeeping behind this
model, including incarnation-bound monotonic activation epochs, controlled
stop/removal, activation-owned effect-scope cleanup, typed required-service
declarations, and revocable provider bindings. A registry inventories every
compatible provider candidate in deterministic provider-registration-ID order,
but callers must bind one exact registration; it does not silently select or
switch a provider.

Provider publication is admitted into an active component's effect scope before
it becomes discoverable. Each handle is fenced by both provider and consumer
scope activity and cancellation, limits provider access to a checked callback,
and fails closed rather than following a replacement. An explicit close seals
new calls synchronously and, while driven, waits for admitted callbacks before
running any provider or consumer cleanup. This boundary does not supervise
spawned tasks or authority deliberately escaped by a trusted service contract.

The dependency reconciler consumes and freezes one mounted definition, live
instance, activation effect scope, and immutable provider selection as one
owned aggregate. Provider choice is explicit input: every declared requirement
must map to one exact live registration, and extra assignments are rejected.
The selection epoch is the activation epoch because a selection never changes
in place. Losing an assigned provider or submitting a different assignment
begins an epoch-fenced stop and synchronously seals the consumer effect scope;
clean teardown must finish before a later pass can start a fresh activation.
A failed cleanup report leaves the component blocked in `stopping` rather than
risk duplicating a resource that may still be live.

An exact-epoch activation failure records its starting or active phase, begins
controlled teardown toward pending, and synchronously seals the same owned
effect scope. Retry remains a later explicit reconciliation decision; this
layer does not run callbacks or choose backoff policy.

One stable desired-state slot adds a caller-sequenced generation token bound to
that slot's process-unique incarnation. A revision freezes one exact
definition and scope; the loader remains responsible for the configuration and
factory inputs named by its opaque revision ID. Assignment-only changes reuse
the mounted instance and follow dependency recomposition, while a revision
change removes the old instance before mounting a fresh incarnation. Disabled
intent retains the desired revision for inspection but owns no mounted
component, and removed intent retains neither. Repeated identical generations
remain level-triggered; older generations and conflicting generation or
revision reuse fail without touching live state.

Desired invalidation synchronously seals starting or active effects before the
reconciliation call returns. Cleanup is driven separately and remains
resumable if its future is dropped. A non-clean terminal report blocks disable,
replacement, and removal by default. An explicit epoch-fenced abandonment can
advance the already-recorded target without rerunning cleanup, but records that
policy decision because a reported resource may still be live.

Reconciliation is level-triggered by its composition authority. Registry
inventory, exact-assignment validation, and binding use the consumer's
immutable scope lineage: a provider is visible in its own scope and
descendants of that scope within the same root tree. Independently minted roots
remain isolated even when their display IDs match. The registry installs no
watches or background loop, and additional candidates do not override an
explicit assignment. This layer does not yet execute activation callbacks,
rank providers, implement shared/named realms or dynamic reparenting, or
schedule a desired component graph.

Live service values, futures, closures, guest resources, and process handles
are not durable graph values. They remain in an in-memory registry whose state
can be reconstructed from durable configuration and plugin identity.

This model is inspired by Cordis and its paper on spatiotemporal composability,
but will be implemented idiomatically in Rust rather than porting JavaScript
Proxy or Node module behavior. The pinned sources are listed in the root
[README](../README.md#prior-art-and-attribution).
The independently authored [semantic conformance corpus](composition-conformance.md)
records the observable subset currently proved and its deliberate non-claims.

## Harness Components

The agent runtime is assembled from capabilities rather than hard-wired global
singletons. Expected extension seats include:

- model and streaming adapters;
- prompt, tool-schema, and context contributors;
- tool registries and execution middleware;
- session projections and compaction policies;
- graph and memory capture, retrieval, ranking, and summarization;
- workflows, goals, schedules, and subagent drivers;
- filesystem, process, sandbox, artifact, and credential backends;
- policy, approval, telemetry, and user-interface contributions.

A seat has a harness-owned service definition, one or more providers, and
consumers. Provider-specific objects do not become canonical session, work, or
memory state.

## Durable Graph

Selene is the durable semantic substrate. The target graph links several
domains:

| Domain | Representative durable concepts |
|---|---|
| Work | Goal, work item, dependency, attempt, lease, decision, verification |
| Session | Turn, model request, message, tool call, projection, compaction |
| Memory | Observation, memory, source, summary, retrieval trace, feedback |
| Evidence | Receipt, event, artifact, test result, review, provenance |
| Extensions | Plugin package, revision, capability request, grant, evaluation, activation intent |
| Effects | Prepared action, dispatch evidence, observation, settlement, uncertainty |

Graph writes pass through host commands that validate identity, expected
version, grant scope, and invariants. Plugins receive namespaced graph and
memory capabilities, never an unrestricted Selene handle.

The existing kernel already commits aggregate state, command receipts, and
semantic events atomically. Its schema and closed command enum are experiments,
not permanent limits on the target graph.

## Local and External Effects

YAH uses the word *effect* for two related but distinct mechanisms.

### Reversible local effects

Service registrations, event listeners, tool definitions, background tasks,
and similar live resources belong to a component effect scope. They can be
released during normal unload or recomposition.

The implemented scope tree accepts synchronous and asynchronous cleanup,
requests cancellation before cleanup, and unwinds registrations and nested
scopes serially in reverse registration order. Close reports are cached for
idempotence, aggregate returned errors and panics without short-circuiting, and
a later `close()` can resume pending cleanup after an earlier close waiter is
dropped. Before the first cleanup, explicit close rejects new mediated service
and capability calls and drains callback-scoped work already admitted against
the relevant scope trees. Dropping the scope itself only requests cancellation
and abandons the drain and unrun cleanup, so its owner must drive close to
completion. This layer does not supervise tasks, impose deadlines, force a
callback to return, or prove that cooperatively cancelled work terminated.

### Durable external effects

Filesystem writes, Git operations, subprocesses, network calls, model requests,
and remote tools can escape the authority process. An effectful action must
record intent before dispatch, then record dispatch evidence and an
authoritative outcome when one is available.

```text
prepared -> dispatching -> dispatched -> settled
                              \-> reconciling (parked)
```

The kernel can durably park an effect for reconciliation; no production
reconciliation worker exists yet.

Unloading a component does not prove that a dispatched external action failed
or was reversed. The existing prepare/dispatch/settle/uncertain machinery is
therefore preserved beneath the new composition runtime.

## Plugin Boundary

YAH will expose one semantic plugin model through several execution drivers:

| Driver | Trust and execution model |
|---|---|
| Built-in Rust | Trusted, statically linked first-party implementation |
| Wasm Component | Explicit WIT imports/exports, Wasmtime limits, capability-oriented host calls |
| Node/TypeScript | Modern ESM SDK in a separately sandboxed process |
| CPython | Latest stable CPython in a separately sandboxed process, with PyO3-backed ergonomics where useful |
| Native embedding | Optional UniFFI bindings for supported foreign-language applications embedding the Rust library; not a plugin sandbox or universal plugin ABI |
| Browser / JS host | Optional `wasm-bindgen` utilities for Rust/Wasm consumed by JavaScript; not the Wasmtime guest ABI |

Rust dynamic libraries are not a public plugin contract. Untrusted Node,
Python, and native code does not run inside the authority process. Process
protocols and WIT may encode values differently while conforming to the same
plugin lifecycle and capability behavior.

The implemented `yah-plugin-host` boundary validates strict manifest and
revision data, defines a runtime-neutral driver lifecycle, and brokers exact
activation-scoped typed capability handles. A host-owned
prepared-activation guard binds an exact package revision to a process-local
provider-selection epoch, transfers deactivation into that component's effect
scope before the first start poll, contains ordinary start unwind failures,
revalidates composition readiness before publishing active, and exposes
exact-activation advisory health. A trusted effective grant snapshot selects an
immutable subset of manifest requests and exact broker registrations; the start
permit exposes the resulting weak context only after cleanup admission, and
synchronous calls join the activation cleanup drain. The object-safe driver
futures choose no executor. An executor-neutral host-side conformance runner
decorates a trusted subject and drives five stable lifecycle cases through
these public APIs; its reference fake and one trusted local authoring driver
are evidence for the harness, not for multi-runtime equivalence. That runnable
example also exercises one example-only process-local capability through exact
grant, revocation, and provider replacement. These values and interfaces do
not load or verify a package, authorize a reserved namespace or built-in
registration, compute policy/approval, or implement a production execution
backend. See the
[driver lifecycle contract](plugin-driver.md) and
[capability contract](plugin-capabilities.md), plus the separate
[conformance boundary](plugin-driver-conformance.md).

The `yah-plugin-wasm` crate separately owns a
[versioned WIT conformance world](wasm-plugin-contract.md) and the first driver
to execute against it. Parser tests fix the exact baseline
logging/cancellation imports and lifecycle/tool fixture exports, while a
Wasmtime-backed driver compiles fixture components, instantiates one store per
activation, and passes the portable lifecycle corpus. Deactivation drops the
store without consulting guest code. This is an executable ABI draft, not a
package loader, capability resource bridge, or sandbox.

Each plugin revision has a content identity, manifest, requested capabilities,
configuration, and execution driver. Admission separates:

1. what the package requests;
2. what policy permits;
3. what a user or administrator grants; and
4. what the selected sandbox actually enforces.

The effective capability set is their intersection. Missing enforcement cannot
be repaired by optimistic labeling.

## Sandbox and Credentials

The WIT draft contains only explicitly named baseline interfaces and no WASI
imports, and the Wasm driver links exactly those, though its checked-in
fixtures import nothing and so never call back. It does not yet enforce
memory, deadline, fuel, or host-call limits, so it may run only the
checked-in fixture corpus and not untrusted code. Full-language workers will be
launched through
a selected sandbox backend. Each backend must advertise and be tested for the
controls it actually enforces; unsupported required controls must fail
admission. No plugin sandbox is implemented or audited yet.

Node permission flags, Python isolated mode, import controls, and audit hooks
may be used as defense in depth. They are not substitutes for OS, container, or
VM isolation when code is untrusted.

[NVIDIA OpenShell](https://github.com/NVIDIA/OpenShell/tree/d51a653f9cedeafa602364df61b74c4bd5a9495e)
is a candidate implementation behind that backend contract for full-language
plugins and whole agent workspaces. It is not a replacement for Selene or the
Rust authority process, and it is unrelated to the trusted container used to
compile and test YAH. Adoption requires fail-closed capability translation,
pinned images and policies, crash reconciliation, and negative tests; its
current alpha, single-player status keeps it an optional spike.

Plugins receive narrow credential handles or brokered operations rather than
the daemon's ambient environment. All graph mutations and irreversible actions
remain attributable to the plugin identity and active grant.

## Recovery

The target composition graph must be reconstructible. After a host restart,
the planned host must be able to reload approved plugin revisions, rebuild
service bindings, and reactivate eligible components from durable desired
state. This composition recovery is not implemented yet.

The existing kernel already requires stale holders to remain fenced,
cancellation delivery to remain distinct from observation, and dispatched
external effects without proof to remain unsettled. The new harness must
additionally ensure that plugin restarts receive fresh live handles and that
future session, work, memory, and evidence identities survive process loss.

## Implemented Boundary

Today the repository contains the initial process-local component lifecycle,
effect-scope core, typed revocable service registry, strict plugin manifest,
runtime-neutral prepared-driver lifecycle, and exact activation-scoped
capability broker, plus the reusable host-side driver conformance testkit and a
trusted local authoring example; a provisional WIT conformance world and the
Wasmtime driver that executes fixture components against it; the
Selene-backed reliability
kernel; provider normalization fixtures; an in-process protocol experiment;
and the G02 storage evidence harness. General component callbacks,
shared/named service realms, policy interception, automatic registry watches
and provider ranking, production capability families, dynamic grant policy,
durable attempt handles, production Wasm/process drivers, plugin service
contributions, graph memory domains, sandbox, live agent loop, daemon, and
clients described above are not implemented yet. The example's built-in driver
and greeting trait are local evidence, not production extension or capability
surfaces.

See [project status](project-status.md) for the exact current boundary and
[protocol](protocol.md) for the existing Adapter 1 experiment.
