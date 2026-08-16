# Plugin Driver Conformance

`yah_plugin_host::conformance` is a reusable, executor-neutral host-side
testkit for implementations of the public `PluginDriver` contract. A driver
test supplies a trusted `DriverConformanceTarget`; the harness creates fresh
composition, registry, capability-broker, and activation ownership for every
case and drives the subject only through public host APIs.

The first corpus is reference-proven by a deterministic built-in fake and is
also passed by the trusted [local authoring driver](plugin-authoring.md) and by
the [Wasmtime component driver](wasm-plugin-contract.md), which runs it against
components Wasmtime compiles and instantiates. That is evidence that other
driver families can use the harness without the agent, daemon, or private
permit constructors. It is not evidence that a production Node or Python
backend works, that guest semantics are covered, or that any two runtimes are
equivalent.

## Target contract

For each named case, a target returns a fresh `DriverConformanceSubject` with:

- an independently trusted `PluginRevision` selected by the fixture;
- the shared `Arc<dyn PluginDriver>` being tested; and
- a trusted observation-only probe for the fixture resource state.

The revision must not be derived from the driver's reported revision or lane.
Ordinary `HostPluginActivation::prepare` independently checks both values and
the prepared activation identity. The probe must reject unknown exact
activation IDs and must not retain a driver, prepared control, broker, or the
actual backend resource. It is trusted test instrumentation, not plugin
authority; a target and probe that collude can make their own result
meaningless.

The runner privately decorates the supplied driver and records bounded
boundary evidence: prepare, start, health, and deactivation counts; pending and
terminal polls; future drops; exact permit matches; and cancellation observed
at drop or teardown. It does not catch or rewrite panics from admitted driver
boundary calls and does not construct host-only permits or contexts. Existing
host lifecycle containment remains authoritative. Before admission, a rejected
subject's untransferred driver is disposed under panic containment; once a
subject is admitted, the decorator's final driver owner is transferred into
ordinary host cleanup. Wrapping a valid target therefore does not move its
final destructor outside the host's cleanup report.

A semantic assertion or probe failure after preparation still seals the exact
activation and awaits best-effort teardown before the case returns. A terminal
non-clean report is recorded and explicitly abandoned only after its evidence
is cached; an API failure that prevents terminal cleanup is reported as
`Incomplete`. Failures involving multiple owners seal every owner before
awaiting any one cleanup and drive those cleanup waiters together, so one
pending deactivation cannot leave its sibling active. The original phase and
summary remain primary. If a probe cannot observe a resource, the failed
observation carries no resource state and preserves that first probe error
through final teardown, even if a later probe recovers; missing evidence is
never treated as success.

## Portable cases

The stable ordered case list is:

| Case ID | Required behavior |
|---|---|
| `driver.ready-lifecycle` | Preparation is inert; start becomes ready; health is healthy; stop fences health; a later host cleanup runs before one cancellation-aware clean deactivation. |
| `driver.pending-start-cancellation` | Start stays pending across dropped and resumed waiters; removal cancels before the stored future is dropped; one deactivation releases the partial fixture resource. |
| `driver.returned-start-failure` | Start acquires a partial fixture resource and returns `Failed`; the exact activation seals and deactivation returns it cleanly to pending. |
| `driver.returned-deactivation-failure` | Start succeeds; deactivation releases its fixture resource and returns an error once; the non-clean report is cached and blocked until the harness explicitly records abandonment. |
| `driver.shared-driver-isolation` | One shared driver serves two exact activations; stopping one revokes and deactivates only that activation while the sibling remains healthy, then each stops once. |

`run_driver_conformance_case` is the primitive so a test framework can isolate
and externally bound each case. `run_driver_conformance` is a convenience that
runs the ordered list and retains every pass or structured case/phase failure;
one setup failure does not suppress later fresh subjects. The returned futures
are `Send` and choose no executor. The harness deliberately provides no sleep,
deadline, or internal runtime.

Every initial case uses an empty effective grant snapshot. This verifies that
drivers enter through the SDK-003 post-cleanup-admission start boundary; it
does not claim capability transport or guest resource-table conformance.
Process-local activation IDs and Rust trait objects in reports are diagnostic
live values, not serialized fixtures or a guest ABI.

The local authoring example separately exercises one nonempty process-local
typed grant. That exact Rust-trait round trip is not part of the portable
corpus and does not define guest capability transport.

## Separate evidence profiles

The existing host-only adversarial tests retain Rust unwind, future-destructor,
prepared-control, and panic-payload faults. Those cases validate trusted Rust
host containment and are not portable requirements for a Wasm guest or remote
worker. Backend implementations will also need their own containment corpora.

Deferred profiles include:

- executed nonempty capability round trips and capability-bearing guest
  fixture semantics;
- guest components built from a language toolchain, WIT resource transport,
  process-IPC encoding, and malformed-message cases;
- package loading, SDK negotiation, configuration, and real-composition smoke;
- worker loss, deadlines, forced termination, output or resource exhaustion;
- filesystem, network, process, secret, raw-Selene, and sandbox negatives;
- durable work attempts, external-effect settlement, retries, and recovery; and
- runtime-version matrices and multi-backend equivalence claims.

Passing this initial corpus is a host-lifecycle compatibility result. It is not
package admission, provenance, sandbox, capability-security, or production
certification.
