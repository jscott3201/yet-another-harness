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

## Why components rather than a byte ABI

The guest path is the Component Model and WIT, driven from Wasmtime directly,
rather than a plugin framework layered over it. The alternative considered was
Extism, and the comparison was settled by measurement rather than preference.

Extism is not a different runtime — it embeds Wasmtime, and it embeds a line
that has already left support by the time it ships: its current release carries
a major whose two-month support window closed weeks earlier. Its ABI passes
bytes rather than types, so the compile-checked world described below would
become a convention that nothing enforces: a host and guest that disagree about
a signature link successfully and fail at call time, or, with compatible
widths, do not fail at all. It has no per-activation store, no ceilings on
memory, table, or instance counts, and — for plugins built from a shared
compiled plugin — one engine whose deadline timer traps every concurrent call
when any one of them times out or is cancelled. Each of those is something the
driver here deliberately does the other way, with a test that fails if it stops
doing so.

What that buys Extism is reach this path does not have: maintained guest
toolkits for roughly ten languages, against Rust and TypeScript here. That is
the cost of this choice, and it is a real one.

Extism also has a packaging convention this path lacks — a manifest format and
an artifact shape. Neither has verification: Extism's manifest carries an
optional content hash and no signatures, and the Component Model's registry
effort was archived with its OCI-based successor offering no verification
either. So a loader here will own identity, versioning, and verification
itself. That is a real cost of this path and it lands on a later slice.

Two consequences already apply. A loader must cache compiled artifacts:
compilation costs roughly seventy-five to two hundred and seventy times
instantiation for the fixtures here, and around sixteen hundred times for a
JavaScript guest, so compiling per activation is not viable. And Component
Model async stays off — it works end to end under this pin, but the JavaScript
toolchain cannot compile a world that uses it, and enabling it would make async
a Rust-only capability.

Run `cargo run -p yah-plugin-wasm --example startup_cost --release` for the
compile-versus-instantiate figures on the checked-in fixtures.

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

Two example guests implement this world: `examples/guests/rust-example`, built
with `wit-bindgen` and `cargo build --target wasm32-unknown-unknown`, and
`examples/guests/ts-example`, built with `jco componentize`. Neither is
committed as a binary — `scripts/build-guests.sh` builds both from source and
the gate runs it before the tests (DEC-038). They answer one tool call
identically and differ only in the field each uses to name itself, which is
what makes the world a contract rather than a Rust convention. They are also
the only guests here that call a host import, so the host's guest-to-host path
is exercised by them and by nothing else.

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

The mechanism's own bound is a single tick: once deactivation raises the stop,
the next deadline the guest reaches ends the call. A case does kill a live call
— under a budget large enough that only the kill can end it — so the stop is
demonstrated. The *one-tick* figure is not: that case bounds the stop generously
rather than at one tick, because a tight bound would fail on a loaded machine
for reasons that have nothing to do with the mechanism.

The driver passes the five portable host lifecycle cases, and a separate smoke
test compiles a component, activates it, calls `fixture-tool.invoke`, and drops
the store.

### Limits

Every activation runs under host-owned bounds it cannot raise. Two of them work
differently, and the difference matters.

A memory or table ceiling *refuses*. The guest asks to grow, the host declines,
and `memory.grow` answers -1 — a value the guest can see and handle. A guest
that ignores the refusal fails on its own terms.

A memory claimed at instantiation is the same refusal with nowhere to put the
answer. The limiter still sees the request, but there is no `memory.grow`
instruction to hand -1 back to, so the refusal aborts instantiation and the
guest never executes anything. That is the path the two-memory fixture takes.

Counts are bounded as well as totals, and for a reason the totals do not cover:
Wasmtime reserves an address-space window per linear memory whether or not that
memory holds any pages. A memory declared with zero pages therefore costs the
byte ceiling nothing and costs the host a reservation, so a guest bounded only
by bytes could exhaust the host's address space without ever exceeding its
"memory ceiling". The driver bounds the number of memories, tables, and
instances — but not globals, which Wasmtime exposes no limiter hook for and
which therefore have no host bound; they cost sixteen bytes apiece per core
instance, so a component may spend them freely up to whatever the validator
allows. The driver sizes the per-memory reservation to the byte ceiling rather than
leaving it at Wasmtime's 4 GiB default — including the reservation a memory
gets when it outgrows the first one, which otherwise defaults to 2 GiB, and the
guard region on each side, which otherwise defaults to 32 MiB and would dominate
what a memory costs at this ceiling.

A guest call also runs on a stack of its own. That is what lets the deadline
tick do something other than trap: with budget left the guest yields the thread
back to the host's executor and resumes, so a guest that computes without ever
calling a host import cannot starve its neighbours. The world stays
synchronous — nothing in it declares `async func` — so this is Wasmtime's fiber
support and not Component Model async, which the JavaScript toolchain cannot
yet compile.

Two host-owned numbers govern that stack, and they are not the same. One sizes
the stack a call runs on; the other bounds how deep the guest may recurse on
it. Setting only the first would leave the recursion bound at Wasmtime's
default rather than the host's. The driver also requires room between them:
Wasmtime rejects a recursion bound larger than the stack but accepts one a page
smaller, and that pair aborts the process on the first guest call that runs
deep enough to use its bound — rather than failing anything a host could
handle — so the driver refuses it at build time instead. How much room is the
host's number too, defaulting to twenty times the deepest host frames measured
above a guest here. The stack is charged per *activation*, not per call in flight:
Wasmtime parks a finished call's stack in its store and reuses it, releasing it
only when the store is dropped, and since instantiation is itself a guest call
every live activation holds one from its first call until teardown. A host
sizing this is pricing how many plugins it keeps alive.

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
budget leaves a sibling with budget untouched. Kill isolation is demonstrated —
one activation's stop does not reach another's, on one engine under one ticker.
Two guest calls now do run at once in one case, which shows they interleave;
what that case does not show is budget isolation, since the healthy guest
finishes within a tick and never approaches a budget of its own. Deactivation
uses the same mechanism to stop an in-flight call before waiting on the lock
that call holds, which is what bounds teardown behind a runaway guest.

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
- guest SDK artifacts. The Rust and TypeScript example guests WSM-005 added are
  an authoring example and a contract check, not an SDK;
- capability-resource tables or graph, memory, artifact, tool-registry, or
  durable-effect host APIs, and no route for a guest to reach a granted
  capability;
- WIT async/streams, fuel metering, or per-call output bounds on the ABI
  itself;
- WASI ambient authority, sandbox enforcement, or cross-runtime equivalence;
- any bound on what a component costs to *compile*. `for_component` validates
  and compiles before a store, a limiter, or a deadline exists, and nothing
  caps input size, section count, or compile time. A component with 20,000
  trivial functions is 385 KB and compiles in about a second; nothing here
  refuses a larger one.
- any rule about a component's *exports* beyond requiring the world's own. A
  guest that also exports an undeclared interface is admitted, because the host
  binds the exports it wants and never enumerates the rest.

WIT strings and lists are not byte-bounded by the ABI, and the memory ceiling
does not bound them either: a guest can point every element of a list at one
buffer, so a small guest memory can name a very large lifted value. Two other
things bound them. The driver sets a per-host-call byte budget on the store
(`host_call_bytes`), charged as the canonical ABI copies a value out of guest
memory. It bounds one call, not a store's lifetime: Wasmtime copies the
allowance into each lift and never writes it back, so it refills per call.

The host also clips and counts what it keeps from a `logging` call — record
count, message bytes, and field count — and copies what it keeps into its own
allocations. The copy is the point. A lifted value carries whatever capacity
the guest's chosen string encoding produced, and a vector of fields collected
in place would be the guest's own buffer, so retaining either as it arrived
would retain far more than the ceiling names. This holds on the path where
nothing is clipped as well: a value under the ceiling by length can still have
arrived in a much larger allocation.

The host-call budget is enforced but unexercised: no checked-in fixture imports
anything, so no guest-to-host call happens anywhere in the corpus. The
retention limits are exercised directly.

Bounds are not containment: a guest still runs in the host process with no
sandbox, so this world remains unsuitable for executing untrusted input.
