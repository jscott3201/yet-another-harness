# Yet Another Harness

**YAH is a Rust-native, graph-backed, plugin-extensible harness for building
and operating agents.**

Yet Another Harness (YAH) is an early-stage effort to build a complete agent
harness whose runtime truth, composition model, memory, and extension
boundaries are owned in Rust. The harness is designed around a live graph of
components and a durable Selene graph of work, sessions, memory, evidence,
artifacts, and external effects.

The project is currently pivoting from a narrow reliability kernel into this
larger architecture. The existing kernel is a useful foundation, not a frozen
specification.

> **Status: pre-0.1 and not ready for use.** There is no installable daemon,
> live agent loop, plugin runtime, sandbox, or end-user client yet. APIs and
> crate boundaries will change.

## Direction

YAH treats an agent harness as a composition of replaceable components, not one
privileged loop surrounded by callbacks. Model adapters, tools, prompt and
context contributors, memory strategies, subagent drivers, execution backends,
policies, storage projections, and user surfaces all attach through explicit
capabilities.

The intended result has four defining properties:

- **Rust owns authority.** Lifecycle, policy, durable state transitions,
  permissions, recovery, and external-effect reconciliation remain in the Rust
  host.
- **Selene stores durable meaning.** Work, attempts, sessions, memories,
  evidence, decisions, artifacts, plugin revisions, and provenance form a
  queryable graph rather than a collection of unrelated logs.
- **Plugins are first-class.** Built-in Rust components and sandboxed Wasm,
  JavaScript/TypeScript, and Python extensions share one semantic SDK surface.
- **Composition is reactive.** Components declare the services they provide
  and require. Activation, replacement, failure, and unload have scoped,
  reversible local effects.

## Target Architecture

```text
                    CLI / UI / API / embedding SDKs
                                  |
                                  v
+-------------------------------------------------------------------+
|                         Rust harness                              |
|                                                                   |
|  agent/session loop     contextual composition    policy/approval |
|  tools + providers  <-> services + effect scopes <-> capabilities |
|  workflows/subagents          lifecycle             observability |
+-------------------------------+-----------------------------------+
                                |
                 durable commands, facts, and evidence
                                |
                                v
+-------------------------------------------------------------------+
|                         Selene graph                              |
| work | sessions | memory | evidence | artifacts | plugin lineage  |
|       receipts | leases | fencing | external-effect ledger        |
+-------------------------------------------------------------------+
                                |
               explicit, capability-scoped host interfaces
                                |
        +-----------------------+------------------------+
        |                       |                        |
  built-in Rust          Wasm components       sandboxed processes
                         Wasmtime + WIT       Node/TS and CPython
```

### Contextual composition

The live runtime will use an idiomatic Rust interpretation of the contextual
composition model described by Cordis:

- component instances move through explicit pending, starting, active,
  stopping, failed, and removed states;
- required services keep consumers pending until a compatible provider exists;
- replacing a provider causes controlled recomposition of its dependents;
- registrations and other local effects belong to nested scopes and unwind
  automatically;
- isolation realms and policy interception narrow what a component can see or
  do.

Live values, closures, and guest handles remain in memory. Selene records
durable desired state, identities, relationships, decisions, and evidence.

### Durable graph and memory

Selene is more than a persistence adapter. It is the substrate for linking:

- goals, work items, attempts, dependencies, decisions, and verification;
- sessions, model turns, tool calls, external effects, and receipts;
- observations, memories, summaries, source evidence, and retrieval traces;
- artifacts, code locations, plugin revisions, evaluations, and provenance.

Plugins will access graph and memory capabilities through namespaced host
commands and queries. They will not receive a raw database handle or bypass the
mutation and authority boundaries.

### Plugin SDK and execution lanes

One semantic plugin model will be presented through language-native SDKs. The
transport and isolation mechanism may differ by lane.

| Lane | Intended use | Boundary |
|---|---|---|
| Built-in Rust | First-party components and trusted integrations | Statically linked Rust traits |
| Wasm Component | Portable third-party plugins | Wasmtime, WIT imports/exports, explicit limits |
| Node.js / TypeScript | Modern ESM plugins and npm packages allowed by sandbox policy | ESM-first SDK over a sandboxed process protocol |
| CPython | Modern Python plugins and packages supported by the selected worker and sandbox | Latest stable CPython in a sandboxed process, with PyO3-backed SDK support |
| Native embedding | Supported foreign-language applications embedding the Rust library | Optional UniFFI bindings; not a plugin sandbox or universal plugin ABI |
| Browser / JS host | Rust-backed web and JavaScript utilities | Optional `wasm-bindgen` surface |

The compatibility policy will favor current runtimes rather than historical
versions: the latest stable CPython line, the current Node.js release and active
LTS line, modern ESM, and contemporary TypeScript syntax.

Untrusted Python, JavaScript, and native code will not execute inside the Rust
authority process. In-process Wasm is a target only when explicit WIT imports
and host-enforced resource limits are in place. Node and Python plugins will
run in supervised worker processes constrained by an OS sandbox, container, or
stronger isolation backend. Runtime permission switches are defense in depth,
not the security boundary. No plugin sandbox has been implemented or audited
yet.

### Two kinds of effects

The design deliberately separates two concerns:

1. **Local reversible effects** register services, tools, listeners, tasks, and
   other live resources. Closing their scope unwinds them.
2. **Durable external effects** may escape the process through files, Git,
   subprocesses, networks, providers, or remote tools. They use the existing
   prepare, dispatch, settle, and reconciling/parking state machinery; the
   query-before-retry reconciliation worker remains future work.

The current local scope implementation binds cleanup to one activation,
requests cancellation before teardown, unwinds mixed synchronous/asynchronous
and nested registrations in deterministic reverse order, and reports every
returned error or unwind panic without short-circuiting later cleanup. Explicit
close also rejects new mediated service and capability calls and drains calls
already admitted against the relevant scope trees before running any cleanup.
Owners must drive close to completion; dropping a scope requests cancellation
but does not drain calls or run registered cleanup. The drain covers synchronous
callback-scoped admission, not spawned tasks, escaped authority, or a callback
that never returns. Cancellation is still a request, not proof that other work
or an escaped action stopped.

A clean plugin unload cannot prove that an external action did not happen. That
distinction is a hard runtime invariant.

## Existing Foundation

The first pivot implementation is a small live composition core with explicit
component definition, instance, and scope identities; incarnation-bound
activation epochs that fence start completions, failure reports, and stop
requests or completions; activation-owned reversible effect scopes; and a
process-local typed service registry. Required services produce deterministic
missing reports, provider publication is owned by effect cleanup, and handles
bind one exact provider registration, fail closed when either activation scope
is sealed, and keep explicit cleanup pending until admitted synchronous calls
release both activations. A level-triggered reconciled component freezes its
mounted definition and one explicit, exact provider assignment per activation;
changing or losing an assigned provider cancels and tears down the old
activation before a replacement can start. Exact-epoch activation failures
likewise seal their effects and target pending after clean teardown; non-clean
cleanup remains blocked. Contextual service visibility follows immutable
scope-tree ancestry: providers are visible in their own scope and descendants,
while independently minted roots are isolated even when their display IDs
match. Component callbacks, registry watches, shared/named realms, dynamic
reparenting, and automatic provider ranking remain deferred.

A stable desired component slot now fences caller-sequenced generations bound
to a process-unique slot incarnation and immutable component/configuration
revision identities. It creates enabled
revisions, replaces changed revisions only after controlled removal, and keeps
disabled or removed intent unmounted. Stale generations and revision-ID reuse
fail without disturbing the live instance; cleanups that report failure remain
blocked unless the owning authority explicitly records abandonment. This is a
single-component mechanism, not a host-wide scheduler or durable desired graph.

An independently authored
[composition semantic conformance corpus](docs/composition-conformance.md)
now exercises nested cleanup, explicit pending dependency convergence, exact
provider replacement, registry-domain separation, activation failure rollback,
and latest-desired revision churn across those public primitives. A companion
deterministic fault corpus covers concurrent provider and consumer close,
hierarchical call draining, callback unwind, resumable close, and final
provider-value destructor panic containment. The corpora do not claim
shared/named realms, automatic injection, file HMR, or task supervision.

A `yah-plugin-host` crate now validates the first
`yah-plugin.toml` contract: canonical package, service, and capability
identities; exact package and SDK version vocabulary; typed built-in, Wasm,
Node, and Python entrypoints; bounded request-only declarations; and
host-supplied package revision digests. Parsing is not admission: no package is
loaded or verified, no namespace or built-in code is authorized, and no
capability is granted by this layer. See the
[manifest contract](docs/plugin-manifest.md).

The same crate now defines an object-safe, runtime-neutral
[`PluginDriver` lifecycle](docs/plugin-driver.md). A host-owned activation guard
binds one package revision to one exact composition epoch, admits deactivation
cleanup before constructing driver start, preserves pending start across
dropped waiters, contains ordinary unwind failures, revalidates composition
readiness before publishing active, and fences advisory health after stop.

An [activation-scoped capability broker](docs/plugin-capabilities.md) now maps a
trusted host-selected subset of manifest requests to exact typed provider
registrations. The driver receives its weak, revocable context only in the
post-cleanup-admission start permit. Synchronous capability calls participate in
the activation's pre-cleanup drain, and stale contexts never follow provider or
activation replacement. This is an in-process authority seam, not package
admission or a hostile-code sandbox. A runnable
[local authoring example](docs/plugin-authoring.md) now exercises one
example-only synchronous capability through denial, exact grant, stop, and
provider replacement with a trusted built-in driver. It is not a production
capability family or execution backend; no policy engine, durable attempt
binding, scheduler, guest runtime, or sandbox exists yet.

A reusable [driver conformance testkit](docs/plugin-driver-conformance.md) now
drives independently described `PluginDriver` subjects through five portable
host-lifecycle cases without the agent or daemon. Its private instrumentation
records exact boundary calls, cancellation, cleanup, and activation isolation;
a trusted fixture probe confirms resource state. The deterministic reference
fake, the trusted local authoring driver, and the Wasmtime component driver all
pass. No process backend has passed yet, and the Wasm fixtures exercise one
activation and one tool call rather than guest semantics, so this is not a
cross-runtime, guest-ABI, loader, sandbox, or portable capability-transport
certification.

A separate `yah-plugin-wasm` crate owns the
[WIT conformance world](docs/wasm-plugin-contract.md) and the first driver that
executes against it. Pinned Wasmtime and `wit-bindgen` macros compile host and
guest Rust bindings from the same versioned source, parser tests freeze its two
baseline imports and two fixture exports, and a Wasmtime driver compiles
checked-in component fixtures, gives each activation its own store, and drops
that store to deactivate without asking guest code. It loads no plugin package.
It enforces host-owned memory and table ceilings, a call deadline that stops a
guest which will not stop itself, and a bound on what one host call may retain.
Those bound a guest's cost, not its authority: it still runs in the host
process, so the driver runs only its own fixtures and makes no
capability-transport, WASI, or sandbox claim.

The repository already contains a model-free Rust kernel and evidence harness:

- atomic Selene commits for current state, semantic events, and command
  receipts;
- authority and attempt epochs, leases, fencing, and stale-holder rejection;
- durable cancellation scopes and delivery observations;
- external-effect preparation, dispatch evidence, settlement, uncertainty, and
  parking;
- provider stream normalization fixtures for OpenAI Responses and Anthropic
  Messages shapes;
- an in-process JSON protocol slice with generated schemas and TypeScript
  bindings;
- a passed storage fan-in and crash-recovery gate across 1,440 scored trials.

These mechanics are candidates for integration into the new harness. The
current closed command surface, crate layout, and earlier product plan are not
treated as architectural authority.

## Roadmap

The pivot is organized around executable vertical slices rather than a large
up-front specification:

1. Build the Rust composition kernel and an independent semantic conformance
   corpus informed by Cordis lifecycle behavior.
2. Extend the landed plugin manifest vocabulary with driver lifecycle,
   effective capability grants, and a reusable host-side driver conformance
   testkit before claiming cross-runtime equivalence.
3. Connect scoped local effects to the existing durable external-effect and
   recovery machinery.
4. Establish Selene-native work, session, memory, evidence, and plugin-lineage
   graphs.
5. Prove Wasm, Node/TypeScript, and Python plugins against the same tool and
   graph capabilities.
6. Deliver one useful agent vertical slice: session, model provider, prompt
   assembly, tools, sandboxed execution, memory, and a bounded subagent.
7. Add the daemon, CLI and UI surfaces, adversarial sandbox tests, evaluations,
   packaging, and release discipline.

Detailed working specifications and sprint tracking are intentionally kept out
of the public repository while the pivot is being explored. Public documents
describe implemented behavior and stable direction; tests and evidence decide
when an experiment becomes a commitment.

## Prior Art and Attribution

YAH is being developed independently in Rust. It currently vendors no code from
the projects below, but it deliberately learns from them. DeepSeek Harness
itself is [powered by Cordis](https://github.com/deepseek-ai/deepseek-harness/blob/47f943859bef60e4160492346772ded9b24f765a/README.md),
so we credit DeepSeek Harness for harness-level implementation influences and
Cordis and its paper for the underlying composition model.

- [DeepSeek Harness](https://github.com/deepseek-ai/deepseek-harness/tree/47f943859bef60e4160492346772ded9b24f765a)
  demonstrates a product architecture in which the model adapter, tool
  registry, session log, agent loop, sandbox, UI, and other capabilities are
  composed as plugins. Concept-level influences from its implementation
  include plugin-first product decomposition, service-definition/provider/
  consumer seams, layered profiles and bundles, a guarded tool pipeline, a
  provider-backed subagent taxonomy, and the distinction between durable
  session facts and live extension events. See its pinned
  [architecture](https://github.com/deepseek-ai/deepseek-harness/blob/47f943859bef60e4160492346772ded9b24f765a/docs/architecture.md),
  [capability-seam catalog](https://github.com/deepseek-ai/deepseek-harness/blob/47f943859bef60e4160492346772ded9b24f765a/docs/capability-seams.md),
  [tool pipeline](https://github.com/deepseek-ai/deepseek-harness/blob/47f943859bef60e4160492346772ded9b24f765a/docs/tool-execution-pipeline.md),
  and [subagent model](https://github.com/deepseek-ai/deepseek-harness/blob/47f943859bef60e4160492346772ded9b24f765a/docs/subsystems/subagent.md).
- [Cordis](https://github.com/cordiverse/cordis/tree/8cc9e33fab69e2d0476d126baaf2acb24e6a6ab4)
  supplies the semantic inspiration for contexts, reactive dependencies,
  fibers/component instances, service isolation, and reversible effect scopes.
  The accompanying paper by Yifan Shi, Wei Zhang, and Tianyi Cui,
  [*A Programming Paradigm for Spatiotemporal Composability*](https://github.com/cordiverse/paper/blob/948a07b369c62adb3b12e102458be5c18dfb69b9/paper.pdf),
  is the conceptual reference. The target design will implement these ideas
  idiomatically in Rust and combine them with the existing durability and
  authority kernel plus planned graph and memory domains.
- [NVIDIA OpenShell](https://github.com/NVIDIA/OpenShell/tree/d51a653f9cedeafa602364df61b74c4bd5a9495e)
  informs the planned sandbox-backend boundary for full-language plugins and
  autonomous workspaces. Its gateway, policy compiler, supervisor, credential
  brokerage, and replaceable compute drivers are useful reference points. YAH
  would integrate it behind a generic execution contract rather than make it
  the harness control plane or a required dependency; the reviewed version is
  explicitly alpha and single-player.

The pinned links document the sources reviewed for this pivot. Upstream
projects are evolving independently and do not define YAH compatibility.

## Repository

| Path | Contents |
|---|---|
| `crates/yah-compose/` | Process-local component identity, epoch-fenced lifecycle, reversible effect scopes, typed revocable services, exact-assignment dependency reconciliation, and fenced desired component revisions |
| `crates/yah-plugin-host/` | Strict plugin manifest/revision contracts, runtime-neutral driver lifecycle, exact activation-scoped capability grants/handles, and reusable host-side conformance cases |
| `crates/yah-plugin-wasm/` | Provisional versioned WIT conformance world, its compile-checked host and guest bindings, and the Wasmtime component driver that runs fixture components against it |
| `crates/yah-kernel/` | Current model-free durability, authority, effect, cancellation, provider, and protocol kernel |
| `crates/exp001-harness/` | Storage fan-in and crash-recovery evidence harness |
| `generated/protocol/` | Checked-in JSON Schemas and TypeScript bindings for the current protocol experiment |
| `docs/` | Public architecture, protocol, development, status, and gate evidence |

The workspace pins Selene to an exact public Git revision so local and hosted
builds use the same storage implementation.

## Development

The pinned toolchain is Rust 1.97.1 and the test runner is cargo-nextest
0.9.143:

```bash
bash scripts/install-nextest.sh
bash scripts/test.sh
```

Run the complete local gate before a pull request:

```bash
bash scripts/full-gate.sh
```

To run the hosted Linux environment and commands through Docker while keeping
the native macOS loop fast:

```bash
bash scripts/container-test.sh
```

See [development](docs/development.md) for repository checks and
[project status](docs/project-status.md) for the implemented boundary.

## License

Licensed under either the [Apache License, Version 2.0](LICENSE-APACHE) or the
[MIT License](LICENSE-MIT), at your option.
