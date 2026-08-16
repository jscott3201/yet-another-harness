# Local Plugin Authoring Example

The `local-capability` Cargo example is the first executable authoring proof
for YAH's runtime-neutral plugin contracts. It connects one example-only host
capability provider to one trusted, statically linked `BuiltinRust` driver
consumer and runs that same driver family through the five portable lifecycle
cases.

Run it from the repository root:

```console
cargo run --locked -p yah-plugin-host --example local-capability
```

The example is also a test target, so its manifest, authority, replacement,
cleanup, and conformance checks run in the ordinary workspace test lane.

## What the example contains

The source lives in `crates/yah-plugin-host/examples/local_capability/`:

- `yah-plugin.toml` requests `example.local-greeting/v1`, declares the trusted
  built-in lane, and makes no service claims;
- `contract.rs` defines a synchronous `Greeting` trait whose operation borrows
  inert text and returns an owned inert value;
- `driver.rs` implements `PluginDriver` and per-activation prepared control
  using only public host APIs;
- `conformance.rs` adapts the same driver family to the reusable five-case
  lifecycle corpus; and
- `main.rs` performs the exact-grant integration and reports both proofs.

The example synthesizes an unverified fixture digest at runtime. Parsing the
checked-in manifest and constructing a `PluginRevision` do not verify package
bytes or authorize linked code. The executable directly wires a known local
driver because trusted built-in provenance and registration remain host
responsibilities.

## Authority flow

The example keeps request, provider, grant, and use separate:

1. The manifest requests the capability. That request grants nothing.
2. The host registers one exact `Arc<dyn Greeting>` in `CapabilityBroker` and
   retains the unique registration owner.
3. The host constructs `EffectiveCapabilityGrants` from that exact
   registration. A capability absent from the manifest is rejected.
4. Driver preparation stays inert. Only the post-cleanup-admission
   `DriverStartPermit` carries `PluginStartContext`.
5. The driver resolves an invariant typed `CapabilityHandle<dyn Greeting>` and
   invokes the provider through `try_with`; it never receives a raw owning
   `Arc` or retains an unmediated provider reference outside that call.
6. In the normal path, component removal synchronously revokes the retained
   handle, explicit teardown deactivates exactly once, and only then does the
   host withdraw the provider registration.

The executable first proves that a requested-but-ungranted capability remains
denied, then demonstrates the normal stop-before-provider-withdraw order. A
separate negative probe deliberately withdraws a provider while its consumer
is still active, installs a fresh registration, and proves that the old handle
fails immediately and never retargets. Provider withdrawal does not itself
transition the component lifecycle, so the host then explicitly stops that
consumer before activating a fresh consumer against the replacement.

Capability contract operations in this initial seam are synchronous. They
must not return or clone raw authority, wait for their own activation to close,
or pretend a rejected late result rolled back an external action. Durable or
irreversible effects still require the prepare/dispatch/settle boundary owned
by the reliability kernel.

## Conformance profile

The reusable driver corpus deliberately supplies empty effective grants. The
local driver treats the greeting capability as optional in that profile and
records the broker's typed denial while continuing its lifecycle. The separate
integration run supplies the exact nonempty grant. Keeping these proofs
separate avoids turning one process-local Rust trait into a premature portable
guest capability ABI.

The local target passes ready lifecycle, pending-start cancellation, returned
start failure, returned deactivation failure, and shared-driver isolation. It
is a trusted authoring example, not a production execution backend or
cross-runtime certification.

## Deliberate limits

This example does not implement plugin-provided services. Manifest service
entries are compatibility and routing claims only; no plugin callback/provider
lifecycle bridge exists yet. It also does not implement package loading,
provenance verification, policy or approval, production capability families,
configuration delivery, a guest ABI, WIT or IPC resource tables, sandboxing,
async capability calls, durable attempt authority, Selene persistence, or
Wasm/Node/Python equivalence.

Use the [manifest contract](plugin-manifest.md),
[driver lifecycle](plugin-driver.md),
[capability broker](plugin-capabilities.md), and
[driver conformance contract](plugin-driver-conformance.md) as the normative
boundaries behind the example.
