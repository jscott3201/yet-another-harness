# Project Status

Yet Another Harness (YAH) is pre-0.1 and is undergoing an architectural pivot
from a narrow model-free reliability kernel to a complete Rust-native,
graph-backed, plugin-extensible agent harness.

There is no usable release. The repository contains a tested reliability
foundation and the first process-local composition lifecycle, cleanup, typed
service-binding, exact-assignment dependency reconciliation, and fenced desired
component-revision primitives, plus independently authored semantic and fault
corpora, a strict plugin manifest, and a runtime-neutral prepared-driver
lifecycle with activation-scoped capability brokerage. One trusted local
authoring example connects an example-only host capability to a built-in driver
and passes the reusable lifecycle corpus. A provisional WIT conformance world
now backs a Wasmtime driver that compiles, instantiates, and calls checked-in
fixture components and passes the same lifecycle corpus, and a brokered
capability now crosses that ABI: a fixture component acquires a granted
capability as an opaque resource through its activation's admitted context and
observes refusal, revocation, and fencing as named codes. A framed strict-JSON
wire protocol for future Node/CPython worker processes is specified, generated,
and fixture-pinned on the host side, with no process driver or worker SDK
behind it. The repository does not yet contain a general callback host, package
loader, agent loop, sandbox, daemon, or client. Guest resource limits exist only inside the Wasmtime driver,
and they bound what a guest can cost, not what it can reach. No cross-runtime
equivalence is proven.

## Evidence Status

| Area | Evidence | Status |
|---|---|---|
| G02 storage fan-in and crash recovery | Atomic state, receipt, and journal commits under kill/reopen, writer takeover, and corruption drills | Passed across 1,440 scored trials: [report](gates/G02-storage-fanin-recovery.md) |
| Current model-free kernel | Deterministic tests for command, fencing, cancellation, effect, provider, recovery, and Adapter 1 behavior | Available for reuse; pivot integration has not started |
| Contextual composition runtime | Component definition/instance/scope identities, immutable ancestor-scoped service visibility, epoch-fenced lifecycle, reversible nested effects, typed requirements, exact revocable bindings, exact-assignment recomposition, fenced desired revisions, six cross-layer semantic cases, and four concurrent fault cases | Initial slices with deterministic conformance evidence; no callbacks, task supervisor, provider-ranking policy, shared/named realms, or host-wide scheduler |
| Plugin manifest and revision vocabulary | Strict bounded TOML, canonical package/service/capability/version/path types, typed driver entrypoints, request-only declarations, and host-supplied revision identity | Initial data-contract slice; no loading, admission, grants, or execution |
| Plugin driver lifecycle | Dyn-compatible exact-revision driver preparation, cancellation-safe host start ownership, effect-owned deactivation, readiness revalidation, panic containment, and fenced advisory health | Deterministic reference and trusted local authoring drivers pass; no production backend, loader, scheduler, or general callback runner |
| Activation-scoped capability broker | Revision-bound requested subsets, exact typed registrations, weak mediated handles, provider/activation ABA fencing, and synchronous call drain before cleanup | One example-only local greeting round trip passes, and the Wasm driver carries handles for the portable text contract into guests through its store's resource table; no policy engine, durable attempt scope, production capability family, or hostile-code sandbox, and the worker wire protocol's handle encoding is not yet bound to this broker |
| Host-side driver conformance | Five stable cases cover ready lifecycle, pending-start cancellation, returned start/deactivation failures, and shared-driver isolation through public host APIs; a start may return pending before it acquires, so the pending-start case polls to acquisition under a bound and names that bound when it is spent | Reusable runner is reference-proven, including a reference driver that never acquires so the bound is exercised rather than assumed, and passed by the local authoring driver; no production guest backend or cross-runtime equivalence claim |
| Wasm Component ABI draft | Versioned `yah:plugin@0.1.0` WIT world with exact logging/cancellation/capabilities imports, lifecycle/fixture-tool exports, host and guest canonical binding compilation, parser-checked interface identities, and named-source regressions | Contract evidence from three generators sharing one parser, and two example guests built by different toolchains that call every import the world offers - logging, cancellation, and capabilities - and answer identically; there is no WASI access |
| Wasmtime component driver | Engine-owned compilation with inert preparation, one store and instance per exact activation, store-drop deactivation with no guest hook, and a smoke test that activates a component and calls its fixture tool | Passes the five portable lifecycle cases against checked-in text fixtures, and runs authored components supplied as bytes through the same lifecycle, limits, and teardown; each start permit's capability context rides in its activation's store, so a guest resolves only its own admitted grants; no package loader, artifact cache, or sandbox claim |
| Wasm resource and deadline limits | Memory and table ceilings summed across every memory and table one activation owns, ceilings on how many memories, tables, and instances it may hold, a per-memory address-space reservation, growth reservation, and guard region all sized to the byte ceiling rather than to a 4 GiB memory, a host-owned stack per activation with a yield at every epoch tick so a computing guest cannot hold its thread, a host-owned guest recursion bound distinct from that stack and refused at build time when the pair leaves less than the host's own headroom for the frames above it, a per-store epoch deadline re-armed per call that terminates a guest which never returns, refusal to enter a stopped activation, and log retention that copies text and the field vector out of the allocations they arrived in, whether or not anything was clipped | Paired fixtures show the same guest refused under a tight ceiling and admitted under a generous one, a two-memory guest refused against a total its largest memory alone would clear, a many-empty-memory guest refused against a count its byte total never charges for, a runaway killed under a watchdog, a live call stopped by a kill under an effectively infinite budget, a short call refused on entry, a compute-bound guest and a healthy sibling sharing one thread where resuming in place instead of yielding makes the sibling wait for the whole runaway, a fixed-depth recursion refused under one stack bound and completed under a larger one where leaving either bound unset fails the pair, a stack pair with no room for host frames refused before an engine is built and a tighter pair accepted once the host lowered the headroom it owns, the portable pending-start case run against a guest whose start section holds the fiber long enough that instantiation yields before it acquires, asserted by poll count so the case fails rather than passes vacuously if it does not, retained host bytes asserted against the documented ceiling, a flood of empty-string fields that leaves the host holding under 1% of the vector the guest sent, and values under the byte ceiling that arrive in a large allocation and are not retained in it; the summed table-*element* ceiling still has unit evidence only - the new fixture holds empty tables, so it charges the count and never the total - the instance-count ceiling is enforced through the limiter hook but exercised by no case at all, globals are unbounded because Wasmtime exposes no limiter hook for them, no case yet drives two guest calls far enough to test budget isolation rather than interleaving, and the per-call deadline re-arm is claimed above but unexercised on the invoke path, measured rather than assumed: deleting that one line leaves all 70 `yah-plugin-wasm` tests passing, re-measured after the capability corpus began calling the tool repeatedly on one live store; the entry check's fault branch is exercised only against a trapped store, which Wasmtime poisons and would refuse without it, so the host-panic case the branch uniquely covers - a caught panic leaves a store Wasmtime still considers callable - has no test, example guests now enter the guest-to-host path and their records are asserted released at teardown, but nothing yet drives a guest into the per-poll panic guard, and there is no fuel metering or sandbox claim; the live-capability-handle ceiling is a `WasmLimits` knob like the rest but its evidence lives in the capability-transport row |
| Example guest plugins | A Rust and a TypeScript plugin implementing the same world, built from source at gate time with no committed binary and with every build output written under `target/` so the readonly container mount is not a special case, run through the host's own activation lifecycle under its limits | Both answer thirteen inputs identically apart from the value of the field naming the guest: nine echo cases, of which five - two whitespace cases, `1.0`, an integer past 2^53, and input that is not JSON - are measured to diverge under the `JSON.parse` round trip the TypeScript guest used to do, non-ASCII diverged in the byte count the guests log rather than in the answer, and the remaining three are controls that must stay identical rather than demonstrated divergences; and four capability cases that acquire, invoke, and release a brokered grant, where the provider's two refusals are rendered from different error surfaces - wit-bindgen's UpperCamelCase variants hand-mapped against componentize-js's native kebab-case strings, which is where a `Debug`-format shortcut would diverge - an ungranted acquire is answered `not-granted` by both rather than trapping, and every handle is released by the guest itself, Rust scope drop against an explicit `Symbol.dispose`, asserted at zero while the activation is still live because teardown would zero it for any guest; both reach the host through the logging, cancellation, and capabilities imports with retained records asserted released at teardown, and a guest that declines is reported as declining and then as gone; `jco componentize` is not byte-reproducible, so the component is pinned by its locked input and verified by behaviour rather than by hash; the pair is an authoring example and a contract check, not a guest SDK, a package format, or a performance claim |
| Wasm negative corpus and declared surface | Malformed component bytes are refused before a driver exists and the refusal names which way they were malformed; a component may not declare a root-level import the `conformance` world does not, matched by exact name and checked at build time before any store exists; a component that does not export the world is refused when the host binds it | Five header shapes each refused with their own cause - empty input, truncated, a core module handed to a component parser, an unknown binary version, a junk section id - four of which required rendering the error's whole source chain, since Wasmtime displays those four as `failed to parse WebAssembly module` and differs only underneath, the fifth being a text-parse failure that never reaches the binary path; an undeclared *empty* instance import refused by name, which is the case Wasmtime does not refuse on its own and which compiled, activated and answered a tool call through this driver before the check existed; the world's own imports admitted as the control, and the allowlist cross-checked against the WIT source so it cannot drift from the world it names; a component with none of the world's exports refused at activation; a guest aliasing 200 list elements at one buffer refused under a 64 KiB host-call budget and admitted under 64 MiB - out of a 64 KiB memory, which is what separates that budget from the memory ceiling - with the clip count at the field ceiling as corroboration; the table *count* refused at eight against a bound of four and admitted at sixteen, naming that ceiling rather than merely not naming the deadline. An empty instance transfers no authority, so this closes a claim rather than an escape. The check is exact-name and root-level, and three things fall outside it. Two are pinned by a case that asserts the gap still builds, so closing either fails a test rather than leaving this row stale: an undeclared import inside a *nested* component is accepted, and so is an allowlisted name imported as the wrong item kind. The third is read from the pinned runtime rather than exercised - `wasmtime-environ`'s `alternate_lookup_key` resolves `x:y/z@0.1.2` against a host's `@0.1.0`, so a semver-compatible name the linker would accept is refused by this check; no case in the corpus puts one through. Extra *exports* are admitted unchecked, nothing bounds what a component costs to compile or how large it may be, and the per-poll panic guard still has no case and no measurement of the premise it rests on - all disclosed rather than fixed |
| Wasm capability transport | The `capabilities` import carries brokered grants into guests as opaque resources: `acquire` resolves an activation-admitted grant into a store-local table entry wrapping an activation-scoped broker handle, every `invoke` re-enters the exact registration's revocation gate and the activation's cancellation and pre-cleanup fences, and each failure is a named WIT error code rather than a trap | Evidence paired between the host seam and the fixture component, except where named: a granted call answers through the provider; an absent grant refuses `not-granted`; provider withdrawal splits into `revoked` for a held resource and `unavailable` for a fresh acquire, and a replacement registration is never reachable through the old grant; the stale-activation window between stop beginning and cleanup finishing refuses on both paths; a live-handle ceiling refuses at the bound, admits under a generous one, and frees on both release paths - guest `resource.drop` and store teardown; carried for the portable text contract only, and no longer from a hand-written fixture only: both toolchain-built example guests acquire, invoke, and deterministically release the same transport through their generated bindings; `invalid-id` has host evidence only (the fixture bakes a well-formed ID), and the never-observed paths are documented rather than exercised - the four broker variants unreachable from a start context, call `exhausted` (admission-space exhaustion has never been produced), the two defensive in-crate arms behind Wasmtime's own trap on forged indices and the ceiling's pre-push check, and the swallowed delete failure in the guest's own drop path, which produces no code at all |
| Worker process wire protocol | Sans-io framed strict-JSON protocol v1 for future Node/CPython workers: length-prefixed frames refused from the prefix alone, duplicate-member, unsafe-integer, and field-bound rejection mirrored by the generated schemas, hello/accept negotiation that fails closed on unknown required features and carries required-only features into the set in force, per-direction calls with never-reused ids and one terminal each — a served artifact pull-read retires its id like every other terminal — frame-counted stream credit split into lossless and lossy classes and widenable from either side up to an announced ceiling, host-enforced deadlines, byte and field bounds that also refuse the host application's own outbound frames, artifact spill behind digest-carrying offers with bounded hex pull-reads, and explicit acknowledged handle release in both directions, capability and artifact handles counted against one live-handle ceiling, with host-side reclamation on every path a release frame cannot travel — err and cancelled terminals, goodbye, disconnect, and fatal fault | One hundred thirteen deterministic fixtures drive the host session against a scripted byte-level peer, including the loss split (goodbye settles in-flight work cancelled, a bare disconnect settles it outcome-unknown with reconciliation required), refusals that echo nothing the worker sent, and paired ceiling cases refused at the bound and admitted below it; a three-lens adversarial review of the first cut found and closed a served-read id-reuse hole, an artifact exemption from the handle ceiling, and two missing API halves (host credit grants, host-initiated release), and three further passes closed the outbound half — field bounds and an I-JSON integer mirror now bind the host application's own frames, a raw-token scan refuses integer literals the parser would round to floats, a spilled reply must exactly describe an artifact the session holds and ride the call it was minted for, only worker-offered handle ids are releasable and none is ever offered twice, the generated TypeScript admits the null a budget-free call actually carries, release-acks are matched against the kind the release named, a release racing a reclaiming terminal is acked instead of fatal, and served reads clear the same admission ceiling as any call — each fix fixture-pinned; generated JSON Schema and TypeScript are checked in and gate-checked; no transport, process driver, or worker SDK exists, the capability broker is not yet bound to these handles, and worker-side conformance has no implementation to run against |
| Node/TypeScript and Python plugin drivers | Only the [wire contract](plugin-worker-protocol.md) exists; no spawn, transport, driver, or SDK implementation | Not started beyond the wire contract |
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
- A private hierarchical activity fence rejects new mediated service or
  capability calls at close and drains already-admitted synchronous callbacks
  before any cleanup. Scope-bound service handles and callback-scoped host
  activity tokens cannot leak the underlying guard; parent close includes
  descendant activity while direct child close remains isolated from its
  parent and siblings.
- Aggregation of returned errors and unwind panics without short-circuiting.
- Explicit separation from durable external-effect settlement.

Cleanup is executor-neutral and sequential. The current layer does not provide
deadlines, forced termination, concurrent effect registration, multi-owner
close, or task supervision. A mediated synchronous callback that never returns
can keep explicit close pending. Dropping a scope requests cancellation but
does not drain admitted calls or run its cleanup.

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
- Explicit provider or consumer close waits for calls already admitted through
  those handles before releasing activation effects. Binding creates only a
  weak handle; each call's temporary strong value is covered by both admissions,
  including during callback unwind.
- Generated provider identities reuse their unique effect registration, so a
  stale handle or delayed cleanup cannot target a replacement publication.

The registry is process-local. Inventory and binding derive visibility from an
immutable scope lineage: providers flow to the same scope and descendants,
while independently minted roots are isolated regardless of matching display
IDs. These operations do not choose providers or mutate lifecycle. Live
service values are not serialized or stored in Selene.

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
background convergence, callback execution, shared/named realms, dynamic
reparenting, provider ranking, cycles, and retry policy remain future work.

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

### Composition semantic and fault conformance

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
- Four deterministic fault cases use OS-thread barriers and manual polling to
  prove parent/child provider drain, isolated consumer-child close, callback
  unwind release, resumable close, and final provider-value destructor panic
  containment during exact-epoch activation failure.

Registry-domain separation remains a coarse process-local boundary; CMP-008
adds immutable scope ancestry within a registry, but not shared/named realms or
policy interception. Desired churn is caller-driven, not filesystem HMR. The
fault corpus covers synchronous mediated service calls, not async tasks,
deadlines, reentrant self-close, escaped authority, executor or waker failure,
or concurrent effect registration.

### Plugin manifest, revision, and driver lifecycle

- A `yah-plugin-host` crate with no direct production async-runtime, Selene,
  Wasm, or process-driver dependency.
- A strict, 64 KiB-bounded `yah-plugin.toml` decoder with an explicit schema
  version and unknown-field rejection at every table.
- Canonical namespaced package IDs and independently versioned service and
  capability contract IDs, exact package SemVer, and explicit SDK version
  requirements.
- One tagged entrypoint for built-in Rust, Wasm Component, Node process, or
  Python process; guest paths are portable package-relative logical paths.
- Distinct capability-request values and service declarations that make no
  grant, namespace-ownership, provider-selection, or publication-authority
  claim.
- Immutable package revision values built from package ID, exact version, and
  a host-supplied canonical digest; digest computation and verification remain
  outside this layer.
- An object-safe shared `PluginDriver` that prepares inert per-activation
  controls and returns owned executor-neutral `Send` futures.
- An exact activation identity plus host-only start/stop permits binding one
  package revision to one provider-selection epoch. The host registers
  deactivation in the component effect scope before any start future can be
  constructed or polled; health uses a cancellation-fenced handle.
- A cancellation-safe start waiter, exact readiness revalidation, contained
  start construction/poll/drop panics, exactly-once resumable deactivation,
  and close-report aggregation for teardown failures.
- Cloneable exact-activation health handles whose nonblocking healthy,
  degraded, unhealthy, error, and panic observations never mutate lifecycle.
- Immutable effective grants that bind one host-selected plugin revision to a
  subset of requested capability IDs and exact broker registrations.
- Invariant typed capability definitions, process-unique registration
  identities, weak start contexts, and cloneable handles that never follow a
  provider or activation replacement.
- Synchronous capability calls that join the activation effect scope's
  pre-cleanup activity drain, reject late results, and retain no provider after
  the call lease ends.
- An executor-neutral conformance target/runner contract with independently
  trusted package revision input, bounded private boundary observations,
  structured per-case failures, and a trusted resource-state probe.
- Five independently runnable portable cases for ready/healthy/clean stop,
  pending-start cancellation, returned start failure, cached blocked
  deactivation failure, and two exact activations sharing one driver.
- An aggregate runner that preserves every fresh-subject result and a
  deterministic reference adapter plus negative metadata, lifecycle,
  isolation, probe, cleanup, and final-driver-drop evidence.
- A runnable trusted local authoring example whose checked manifest requests an
  example-only greeting capability, whose built-in consumer distinguishes
  request from grant, and whose exact handle remains revoked across clean stop
  and provider replacement while the same driver family passes all five
  portable lifecycle cases.
- A `yah-plugin-wasm` crate with one canonical, versioned WIT conformance world;
  pinned Wasmtime and `wit-bindgen` host/guest binding compilation; and parser
  assertions for its exact imports, exports, functions, and declared
  package/interface identities, plus source regressions for deferred names.
- A Wasmtime component driver that compiles its fixture components before any
  activation is prepared, gives each exact activation its own store and
  instance, releases that store on deactivation without a guest hook, and
  passes all five portable lifecycle cases beside a smoke test that activates a
  component and calls its fixture tool across the canonical ABI.

Manifest parsing is not installation or admission. In particular, a built-in
lane declaration cannot name a linked factory and still requires out-of-band
trusted provenance plus exact host registration. Package extraction,
signatures, configuration snapshots, policy/approval calculation, concrete
production capability families and execution backends, plugin-provided
services, process-IPC capability bindings, durable attempts, and actual
multi-runtime or guest-semantic conformance are not implemented; the WIT
capability-resource binding exists for the portable text contract only. See
the [driver lifecycle contract](plugin-driver.md) and
[capability broker contract](plugin-capabilities.md), plus the
[driver conformance contract](plugin-driver-conformance.md).

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

The next useful proof extends the landed composition, plugin-host, and
Wasm driver contracts into a narrow vertical slice containing:

1. one Selene graph/memory host capability;
2. one Wasm component plus one modern Node or Python process plugin that runs
   the portable host-driver cases;
3. a durable external action demonstrating that plugin unload and action
   settlement remain distinct; and
4. crash/restart reconstruction with behavioral conformance tests.

After that slice, the harness can grow model, prompt, tool, session, workflow,
subagent, sandbox, daemon, and client components without fixing every final
crate or protocol boundary in advance.

## Not Implemented

- Component callback runner, shared/named realms and dynamic visibility,
  automatic provider ranking or registry watches, concurrent effect-registration
  or task supervisor, or host-wide desired-graph scheduler.
- Plugin package loading, verification, admission, installation, updates,
  production execution backends or capability families, plugin service
  contributions, policy-derived grants, language SDKs, guest-semantic corpus,
  or demonstrated cross-runtime equivalence.
- Capability transport beyond the portable text contract. Both example guests
  now consume a brokered capability through their generated bindings, so the
  transport's toolchain-portability claim is measured rather than open; what
  remains unbuilt is any capability family other than text.
- Fuel metering, a bound on what a component costs to compile, a check of a
  guest's exports, or sandboxed Node/TypeScript and CPython workers.
- Durable memory capture, retrieval, ranking, summaries, or evidence lineage.
- Live model providers, prompt assembly, tool execution, workflows, schedules,
  goals, or subagents.
- Filesystem, process, Git, network, container, or VM sandbox enforcement.
- Durable artifact storage. Oversized-result substitution now has a wire
  mechanism in the worker protocol, but no live call path produces, stores,
  or retrieves an artifact.
- Approval requests, credential brokerage, dynamic grant policy, or durable
  attempt-scoped capability authority.
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
