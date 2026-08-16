# Composition Semantic Conformance

The `yah-compose` semantic conformance corpus is an independently authored set
of black-box scenarios over the crate's public API. It tests observable
composition behavior across lifecycle, effect, service, dependency, and
desired-state layers rather than repeating each layer's unit assertions.

The initial corpus lives in
`crates/yah-compose/tests/semantic_conformance.rs` and contains six cases:

| Case | YAH contract |
|---|---|
| `CMP006-CLEANUP` | Activation-bound nested synchronous and asynchronous effects unwind once in tree-aware reverse order; cancellation is visible before cleanup is polled, and one returned cleanup error does not skip older cleanup. |
| `CMP006-PENDING` | A required service remains missing while its provider is starting, becomes visible only after active publication, and still requires an explicit exact assignment before the consumer starts. |
| `CMP006-REPLACE` | An extra candidate does not displace the selected provider. Changing the exact assignment synchronously revokes the old consumer handle, completes teardown, and binds the replacement under a fresh epoch. |
| `CMP006-DOMAIN` | Separate service registries are separate visibility domains even for the same semantic service ID. Foreign registration IDs cannot bind, and revocation in one domain does not affect another. |
| `CMP006-FAILURE` | An exact-epoch starting or active failure records its inferred phase, seals activation effects, cleanly rolls back to pending, preserves diagnostics, and leaves retry to a later explicit reconciliation pass; non-clean teardown follows the general blocking policy. |
| `CMP006-CHURN` | Desired revision changes received while teardown is suspended replace stored intent without retargeting the in-flight stop; skipped revisions never mount and only the latest accepted revision starts afterward. |

These cases are deterministic. Suspended cleanup is polled manually and
resumed through an explicit signal; the corpus does not depend on sleeps,
filesystem timing, or background reconciliation.

## Concurrency and fault injection

The companion corpus lives in
`crates/yah-compose/tests/concurrency_fault_injection.rs` and contains four
deterministic cases:

| Case | YAH contract |
|---|---|
| `CMP007-PROVIDER-DRAIN` | Parent close immediately hides a child-scope provider, rejects late calls, and waits for several admitted calls before touching a newer parent cleanup or withdrawing and releasing the provider. A dropped close waiter resumes the same drain. |
| `CMP007-CONSUMER-DRAIN` | Closing one consumer child does not revoke a handle bound in its sibling, while parent close rejects new use and waits for an admitted call before releasing consumer effects. The independent provider stays visible. |
| `CMP007-CALLBACK-PANIC` | A callback panic propagates to its caller but releases both provider and consumer activity admissions, allowing simultaneous explicit closes to finish and release their effects once. |
| `CMP007-DESTRUCTOR-PANIC` | Exact-epoch active failure waits for an admitted call; the final registry-created provider value is then released inside cleanup panic containment, older cleanup continues, and the non-clean report remains blocked until explicit abandonment. |

OS-thread barriers establish call admission and release points; manual polling
proves the close futures are pending before release. The cases use no sleeps,
probabilistic scheduling assertions, or background runtime.

## Contextual visibility

`crates/yah-compose/tests/contextual_visibility.rs` proves that provider
visibility is authorized by immutable scope-node ancestry rather than display
IDs. Providers flow to their own scope and descendants, sibling and independent
root scopes cannot observe or bind them, and exact reconciled assignments use
the same filtered inventory before start and after withdrawal. Invisible exact
IDs fail uniformly as unavailable without revealing provider metadata.

## Boundaries

- Dependency convergence is explicit. YAH does not currently choose a unique
  provider or automatically react to registry changes.
- Registry-domain separation is not Cordis-style shared isolation labels or a
  hostile-code sandbox. CMP-008 separately proves immutable same-scope and
  ancestor visibility across inventory, exact assignment, binding, and
  withdrawal; shared/named realms and dynamic reparenting remain deferred.
- Desired revision churn is not file watching, debounce, module HMR, loader
  rollback, or durable desired-state reconstruction.
- Failure reporting is a host-facing lifecycle seam, not a component callback
  runner or automatic retry/backoff policy.
- Local cleanup does not compensate or settle an external action. Durable
  external effects remain owned by the kernel ledger.
- The fault corpus establishes explicit-close draining only for synchronous
  calls mediated by `ServiceHandle::try_with`. It does not establish task or
  future supervision, deadlines, forced termination, safe reentrant self-close,
  escaped service authority, concurrent effect registration, executor/waker
  fault containment, or a general concurrency model.

## Behavioral provenance

The cases were informed by the disposal, service readiness, fiber lifecycle,
isolation, and reload behavior in the pinned
[Cordis source snapshot](https://github.com/cordiverse/cordis/tree/8cc9e33fab69e2d0476d126baaf2acb24e6a6ab4)
and the pinned
[Cordis paper revision](https://github.com/cordiverse/paper/blob/948a07b369c62adb3b12e102458be5c18dfb69b9/paper.pdf).
No Cordis source, fixture, test body, or paper material was translated into
this corpus. YAH deliberately retains explicit provider assignment,
fail-closed epoch fencing, sequential reported cleanup, and caller-driven
desired state rather than claiming Cordis compatibility.
