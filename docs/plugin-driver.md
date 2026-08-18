# Plugin Driver Lifecycle

YAH's runtime-neutral driver contract connects one exact plugin package
revision to one process-local composition activation. It defines lifecycle
ownership and failure behavior without loading a package, choosing an executor,
computing capability policy, or implementing a Wasm or process backend.

## Two-phase activation

The object-safe `PluginDriver` prepares an inert
`PreparedDriverActivation`. Preparation may validate and allocate in-memory
control state, but it must not execute plugin code, start a task or process, or
acquire an external resource.

`HostPluginActivation` then enforces this order:

1. validate the exact starting component epoch and obtain its read-only
   cancellation plus callback-scoped activity token;
2. read driver metadata and validate its revision, lane, and exact capability
   registrations against a host-selected effective grant snapshot;
3. derive the `PluginActivationId`, then prepare and identity-check the inert
   driver control;
4. register its asynchronous deactivation as the oldest plugin effect; and
5. only after successful registration, mint the one-shot permit containing the
   exact [capability context](plugin-capabilities.md) that can construct and poll
   driver start.

Later activation effects therefore unwind before driver deactivation. A
driver must retain partial resources in its prepared control rather than only
inside the start future, so the registered finalizer can clean a never-polled,
partially starting, ready, or failed activation.

The driver traits use owned boxed `Send + 'static` standard-library futures.
They are dyn-compatible and make no Tokio or other executor part of the
production contract. One `Arc<dyn PluginDriver>` may prepare multiple exact
activations. Start and deactivation use host-only exact-activation permits;
health uses an exact cancellation-fenced handle.

Grant, preparation, or metadata failure before finalizer admission deliberately
leaves the component `Starting`: the owner must retry, reconcile a newer desired
snapshot, or report failure for that exact epoch. A future loader/admission
boundary must still prove package provenance and SDK compatibility;
driver-reported metadata and a syntactically valid revision are not admission.

## Readiness, cancellation, and cleanup

A successful driver start reports readiness only. The host still calls the
component slot's exact-epoch `complete_start`, which revalidates current desired
assignments and provider availability before publishing `Active`. A late
success cannot revive an activation that desired state or dependency changes
already sealed.

The host guard stores the start future. Dropping a borrowing start waiter does
not destroy it, and a later waiter resumes the same operation. Driver-returned
errors and ordinary unwind panics from start construction, polling, and future
destruction are contained and routed through exact activation failure. Known
failure and cancellation paths seal the component before destroying a pending
future. `panic = "abort"`, executor or waker failure, and process abort remain
outside this boundary.

Desired reconciliation while start is pending goes through the guard, so the
component synchronously requests cancellation before the pending future is
dropped. Deactivation is owned by the existing effect scope: it runs once,
remains resumable when a `finish_stop` waiter is dropped, and contributes
returned errors or unwind panics to the cached `CloseReport` without skipping
older cleanup. Non-clean teardown follows the composition core's fail-closed
policy and remains blocked until the unique slot authority explicitly accepts
abandonment.

Dropping the whole guard before controlled release records an activation
failure and requests cancellation synchronously. Drop cannot await, does not
run asynchronous deactivation, and never reports successful teardown; the
component owner must still drive its retained stop to completion. Dropping the
component slot itself retains the broader documented effect-scope abandonment
boundary.

Deactivation is the final backend-resource shutdown, not a guest callback that
can depend on later activation effects: those effects have already unwound.
Prepared controls and driver objects have host-owned aliases disposed under
cleanup panic containment. Their destructors are not cleanup authority; driver
implementations must make them inert and non-panicking, and must release every
activation resource from `deactivate`.

## Health

An active `PluginActivationHandle` exposes a nonblocking advisory health
snapshot: healthy, degraded, or unhealthy. The handle checks exact activation
cancellation before and after consulting the driver, so an old handle fails
after stop or replacement. A driver error means health is unknown. Health is
not activation readiness, proof of backend liveness, a restart trigger, or an
authority decision. Health must be thread-safe and nonblocking if it overlaps
deactivation. The host suppresses a result made stale by concurrent
cancellation, but SDK-002 does not drain health calls.

## Deliberate boundary

The adjacent capability slice implements immutable activation-scoped effective
grant bindings and mediated synchronous handles. The reusable
[host-side conformance testkit](plugin-driver-conformance.md) now exercises five
portable lifecycle cases against an independently described driver subject; a
deterministic reference fake, the trusted
[local authoring driver](plugin-authoring.md), the Wasmtime component driver,
and the [worker process driver](plugin-worker-protocol.md) pass, but no package
is ever loaded. Call deadlines and forced termination remain each driver's own
mechanism, never the host contract's: the Wasmtime driver enforces them with
epochs and fuel, the process driver with a handshake clock, per-call session
deadlines, and a process-group kill path, and the host contract itself states
no deadline. These layers do not implement
package loading or admission, policy/approval evaluation, configuration
delivery, general component callbacks, async invocation
draining, recurring health checks, restart/backoff policy, task supervision, WIT
execution/resource transport, sandboxing, or durable
activation identity or work-attempt fencing. Portable nonempty capability
transport, guest ABI behavior, backend containment, and multi-runtime
equivalence remain later evidence profiles. A separate
[WIT draft](wasm-plugin-contract.md) fixes the first host/guest binding shape,
and a Wasmtime driver now passes this same corpus against it.
