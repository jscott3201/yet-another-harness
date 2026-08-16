# Wasm Plugin Contract

YAH's first WebAssembly Component Model contract is a compile-checked,
pre-1.0 conformance world. It fixes the smallest host/guest shape needed for
the first driver spike without claiming that a component can yet be loaded or
executed.

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
value, but this compile-only slice does not parse or bound them. They do not
define the production tool registry or authorize actions. Typed domain
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

## Compile evidence

`yah-plugin-wasm` owns no normal dependency yet. Tests compile Wasmtime host
bindings and `wit-bindgen` guest bindings directly from the canonical WIT, then
use `wit-parser` to assert the exact package, world, directional interface set,
and function inventory. The workspace pins these three development tools so a
toolchain change is deliberate. Generated Rust is not checked in, and the
existing Adapter 1 schema generator remains a separate protocol experiment.

This evidence proves that the native Rust binding generators agree on the
source contract. It does not compile a guest component, instantiate Wasmtime,
link SDK-003 resources, or run the portable driver corpus.

## Deliberate limits

This slice does not provide:

- a `PluginDriver`, Wasmtime engine/store/linker, package loader, or executed
  component;
- guest SDK artifacts or a Rust/TypeScript component build;
- capability-resource tables or graph, memory, artifact, tool-registry, or
  durable-effect host APIs;
- WIT async/streams, deadlines, interruption, fuel, memory/table limits, or
  bounded host-call output;
- WASI ambient authority, sandbox enforcement, malformed-component coverage,
  or cross-runtime equivalence.

WIT strings and lists are not byte-bounded by this draft. Until the Wasm driver
enforces memory, call, deadline, and output limits, this world is unsuitable
for executing untrusted input.
