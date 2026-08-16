# Wasm Plugin Contract

YAH's first WebAssembly Component Model contract is a pre-1.0 conformance
world. It fixes the smallest host/guest shape needed for the first driver, and
a Wasmtime-backed driver now compiles, instantiates, and calls components
against it under host-owned resource and deadline limits. Packaging and
hostile-code containment are not part of it.

The canonical source is
[`crates/yah-plugin-wasm/wit/yah-plugin.wit`](../crates/yah-plugin-wasm/wit/yah-plugin.wit):

- package `yah:plugin@0.1.0`;
- world `conformance`;
- imports `logging` and `cancellation`; and
- exports `lifecycle` and `fixture-tool`.

## World boundary

| Direction | Interface | Initial purpose |
|---|---|---|
| Guest to host | `logging` import | Structured level, message, and inert string fields |
| Guest to host | `cancellation` import | Cooperative read-only cancellation observation |
| Host to guest | `lifecycle` export | One fallible activation entry point |
| Host to guest | `fixture-tool` export | One fallible JSON-shaped request/response used by driver fixtures |

Component imports are statically required. This fixed conformance profile is
therefore not a universal permission set and is not derived from an SDK-003
effective grant snapshot. Logging and cancellation are baseline host context
for this profile. A future loader must reject a component whose required
imports cannot be linked; it must not install privileged trap stubs for denied
capabilities.

The world imports no WASI package and declares no filesystem, network,
environment, clock, random, graph, memory, or artifact interface. It also
declares no typed activation, grant, provider-registration, or other scalar
bearer field. Exact revision, activation, cancellation, and grant authority
remain in host-owned store and resource-table state rather than crossing the
ABI as forgeable typed values. Generic JSON can still carry arbitrary text, so
a future driver must validate it as one bounded JSON value and must never treat
caller-supplied IDs or tokens within it as authority.

`fixture-tool` is only the first portable call shape for the driver and guest
examples. Its input and output strings are intended to contain one UTF-8 JSON
value, but neither the driver nor this contract parses or bounds them. They do
not define the production tool registry or authorize actions. Typed domain
contracts and bounded resource handles will be added only when their host
owners exist.

The guest exposes activation readiness but no deactivation or health callback.
The runtime-neutral host remains lifecycle authority and must always tear down
the engine, store, resources, and capability bindings even if guest code is
unavailable or faulty. A later guest notification hook, if useful, cannot
become the owner of cleanup completion.

## Version and identity axes

The WIT package version is the Component ABI version. It is independent from:

- the `yah-plugin.toml` manifest schema version;
- the plugin package's exact SemVer and content digest;
- the host SDK version requirement;
- service and capability contract `/vN` identities; and
- process-local plugin revision and activation identities.

The manifest remains the package descriptor. Guests do not repeat package or
revision identity in this world, and matching the WIT package name is not
package provenance or admission.

## Contract evidence

Tests compile Wasmtime host bindings and `wit-bindgen` guest bindings directly
from the canonical WIT, then use `wit-parser` to assert the exact package,
world, directional interface set, and function inventory. The workspace pins
all three exactly so a toolchain change is deliberate. Wasmtime is now a normal
dependency of the crate because the driver executes; `wit-bindgen` and
`wit-parser` remain development-only. Generated Rust is not checked in, and the
existing Adapter 1 schema generator remains a separate protocol experiment.

Three independent consumers agree on one source: the host generator, the guest
generator, and a neutral reader. They are not independent implementations -
all three reach `wit-parser`, two of them at the same version - so this shows
the generators agree, not that the grammar has been cross-checked.

## The driver

`yah-plugin-wasm` owns a `WasmComponentDriver` implementing the host's
`PluginDriver`. One driver object holds an engine and its compiled components;
each activation owns a store and instance keyed by exact activation identity,
so deactivating one activation cannot disturb another on the same driver.

Compilation happens when the driver is built, which is what keeps `prepare`
inert: by the time the host prepares an activation, nothing remains to compile
or load. The store is reachable from the prepared control rather than only from
the start future, because the host destroys that future to cancel a pending
start and anything held solely inside it would never be released.

Deactivation drops the store. That releases the instance, its memories, and
every host binding the linker installed, and it does not consult guest code:
the world exports no guest deactivation hook, so a faulty guest is never asked
to agree to its own shutdown.

A guest that never returns can still delay its own teardown, but only for a
bounded time. Deactivation stops the guest before asking for the lock it holds,
and the guest reaches that decision at its next epoch deadline. A stop is also
read on the way into a call, not only at a deadline: a call short enough to
return before the epoch advances would otherwise never consult it.

Two bounds apply, and only one of them is currently demonstrated. The
mechanism's own bound is a single tick: once deactivation raises the stop, the
next deadline the guest reaches ends the call. What the fixture corpus actually
exercises is the budget ceiling, `call_budget_ticks * epoch_tick`, because no
case yet tears an activation down while one of its calls is still running. The
one-tick figure is a claim about the code, not a measured result.

The driver passes the five portable host lifecycle cases, and a separate smoke
test compiles a component, activates it, calls `fixture-tool.invoke`, and drops
the store.

### Limits

Every activation runs under host-owned bounds it cannot raise. Two of them work
differently, and the difference matters.

A memory or table ceiling *refuses*. The guest asks to grow, the host declines,
and `memory.grow` answers -1 — a value the guest can see and handle. A guest
that ignores the refusal fails on its own terms.

A call deadline *terminates*. The world's cancellation import is advisory, so a
guest that never asks whether it should stop would otherwise run forever. The
driver advances its engine's epoch on a timer, and a guest that outlives its
budget is trapped without being consulted.

Deadlines are per-store and relative to the engine's epoch at the moment they
are armed, and the driver re-arms before every call. That re-arming is load
bearing: a deadline is absolute, so a store left idle since instantiation is
already past the one it was given, and its next call would be charged for time
it never ran. Because each store carries its own deadline, one timer bounds
every activation without coupling them: the tick that kills a call out of
budget leaves a sibling with budget untouched. Deactivation uses
the same mechanism to stop an in-flight call before waiting on the lock that
call holds, which is what bounds teardown behind a runaway guest.

The host also bounds what it retains from one `logging` call — record count,
message bytes, and field count — and counts what it dropped or clipped, so the
loss is visible rather than silent.

These bound what a guest can *cost*. They do not bound what it can *reach*:
guest code runs in the authority process with no sandbox.

### Fixture components

The corpus carries its guests as component text under
`crates/yah-plugin-wasm/guests/`. Text keeps the canonical-ABI shape reviewable
in a diff, and building guests from a real language toolchain would need a
second Rust target in the gate container. Those belong with the guest SDK work,
so these files are corpus rather than an authoring example.

The fixtures import nothing. Host logging and cancellation are linked and
proved linkable, but no guest here calls back through them.

## Deliberate limits

This slice does not provide:

- a package loader, admission path, or any execution of code that did not come
  from the checked-in fixture corpus;
- guest SDK artifacts or a Rust/TypeScript component build;
- capability-resource tables or graph, memory, artifact, tool-registry, or
  durable-effect host APIs, and no route for a guest to reach a granted
  capability;
- WIT async/streams, fuel metering, or per-call output bounds on the ABI
  itself;
- WASI ambient authority, sandbox enforcement, malformed-component coverage,
  or cross-runtime equivalence.

WIT strings and lists are not byte-bounded by the ABI. What bounds them is the
memory ceiling, which caps how large a value a guest can construct, and the
host's own retention limits on what it keeps. Bounds are not containment: a
guest still runs in the host process with no sandbox, so this world remains
unsuitable for executing untrusted input.
