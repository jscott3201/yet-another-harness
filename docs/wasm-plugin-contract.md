# Wasm Plugin Contract

YAH's first WebAssembly Component Model contract is a pre-1.0 conformance
world. It fixes the smallest host/guest shape needed for the first driver, and
a Wasmtime-backed driver now compiles, instantiates, and calls components
against it. Packaging, resource limits, and containment are not part of it.

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

That is not yet the same as being unable to delay it. The store lock is held
for the duration of a guest call, so a guest that never returns from `activate`
also blocks that activation's deactivation. Nothing here can interrupt running
guest code; only the deadline and interruption limits below can, and they are
not implemented.

The driver passes the five portable host lifecycle cases, and a separate smoke
test compiles a component, activates it, calls `fixture-tool.invoke`, and drops
the store.

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
- WIT async/streams, deadlines, epoch interruption, fuel, memory/table limits,
  or bounded host-call output;
- WASI ambient authority, sandbox enforcement, malformed-component coverage,
  or cross-runtime equivalence.

WIT strings and lists are not byte-bounded by this draft, and the host retains
only a fixed number of guest log records before dropping them. Until the driver
enforces memory, call, deadline, and output limits, this world is unsuitable
for executing untrusted input.
