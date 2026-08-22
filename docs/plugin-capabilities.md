# Plugin Capability Brokerage

YAH's first capability broker is a process-local, runtime-neutral authority
projection for one plugin activation. It connects request-only manifest data to
explicit host decisions without pretending to implement policy, sandboxing, or
durable work authority.

## Requests, registrations, and effective grants

The three layers remain distinct:

1. a `CapabilityRequest` says what a package would like to use;
2. a `CapabilityProviderRegistration<T>` gives the host one exact typed
   implementation; and
3. `EffectiveCapabilityGrants` records the trusted authority's chosen subset as
   exact capability-to-registration bindings for one package revision.

Omitting a request denies it. Grant construction rejects undeclared and
duplicate capability IDs. Broker admission rejects foreign, withdrawn, or
mismatched registrations. The first registration of a capability ID fixes its
process-local Rust contract type for that broker's lifetime.

The snapshot does not compute policy. Its caller must already have intersected
the manifest request with repository policy, approval, and what the selected
backend can actually enforce. The snapshot is neither serialized authority nor
proof that package bytes, provenance, namespaces, or linked built-in code were
admitted.

## Start boundary

The host uses the grant snapshot as the expected package revision and driver
lane. `HostPluginActivation::prepare` rejects driver-reported metadata that
does not match that host-selected subject and validates every exact registration
before invoking inert driver preparation.

`PluginActivationRequest` still carries no actionable authority. Only after the
component effect scope accepts exact driver deactivation does the host mint the
one-shot `DriverStartPermit`. That permit contains a `PluginStartContext` bound
to the real component activation, its read-only cancellation, its pre-cleanup
activity fence, and the immutable exact grant bindings.

This metadata comparison is not package admission: a future loader must still
establish trusted provenance and select the expected revision and lane.

## Handles and revocation

`PluginStartContext::handle` resolves only a granted `CapabilityDefinition<T>`.
The resulting cloneable `CapabilityHandle<T>`:

- binds one exact plugin activation and one exact provider registration;
- holds weak references, so leaked handles cannot keep the broker or provider
  alive;
- never follows a replacement registration;
- exposes no `Deref`, `AsRef`, raw `Arc`, or ID-to-authority constructor; and
- mediates every synchronous use through `try_with`.

The handle enters both the exact provider gate and the composition effect
scope's activity fence before upgrading the weak provider. Activation close or
raw scope drop synchronously rejects new calls. A call admitted before the
revocation boundary may finish, but explicit close waits for it before running
any activation cleanup. If cancellation or withdrawal wins the post-call
availability check, the result is rejected as stale; revocation that linearizes
after that check does not retroactively reject a successful call. Stale-result
rejection is not rollback or proof that the operation was never dispatched;
external mutation still belongs behind the durable prepare/dispatch/settle
boundary.

Dropping or explicitly withdrawing a provider registration revokes its old
handles. A later registration receives a fresh identity, so an old grant or
context cannot retarget through an ABA replacement. Provider registrations are
host-owned adapters: their owners retain the strong value and own its broader
lifecycle; provider destructors must be inert and non-panicking. Withdrawal
does not drain unrelated provider-owned effects, so these initial providers
must be self-contained or have a host lifetime that outlives admitted calls.
Component-owned provider cleanup integration waits for a concrete capability
provider lifecycle.

`try_with` is a trusted in-process contract seam, not a hostile-code sandbox.
Capability contract authors must not return or clone raw authority that bypasses
the handle. Runtime adapters map these semantics onto authenticated opaque
resource tables rather than serialize process-local IDs. The Wasm driver keeps
an activation-scoped handle behind each guest-held resource. The process driver
now applies the same rule to [`TextCapability`]: its pump owns a table keyed by
session-local wire handle names, and every invoke uses the broker handle stored
there. Neither table index nor wire name is bearer authority.

[`TextCapability`]: ../crates/yah-plugin-host/src/capability/text.rs

The runnable [local authoring example](plugin-authoring.md) demonstrates this
flow with an example-only synchronous greeting contract. It proves denial,
exact grant, activation revocation, and non-retargeting provider replacement;
it does not turn that Rust trait into a production capability family or guest
ABI.

## Deliberate boundary

This slice does not implement:

- policy evaluation, approval, audit, expiration, or in-place grant changes;
- concrete filesystem, network, secret, graph, tool, or model capabilities;
- configuration or typed capability-specific constraint schemas;
- async calls, streams, task supervision, deadlines, rate limits, or forced
  cancellation;
- durable work-attempt identity or child invocation scopes;
- transport bindings beyond the portable text contract. Wasm carries it through
  its WIT resource, and the [worker wire protocol](plugin-worker-protocol.md)
  carries it through two versioned worker calls plus the protocol's existing
  release frame. Richer or typed-per-capability encodings do not exist;
- provider cleanup ownership, durable external effects, or Selene persistence;
  or
- automatic lifecycle changes after a host policy decision.

Durable attempts require the full kernel fence, not a copied scalar epoch. They
will bind into capability calls when a real invocation owner exists. Grant
changes likewise require a fresh activation until later security policy defines
audited revocation and dependent scheduling.

The separate [WIT conformance world](wasm-plugin-contract.md) now carries
these handles to a Wasm guest as opaque resources, for providers registered
under the portable text contract. The static import never becomes a grant:
the authority is the resource `acquire` returns against the activation's
admitted snapshot, and a denied capability is an observable refusal rather
than a missing or stubbed import.
