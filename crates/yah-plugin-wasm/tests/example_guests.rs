//! The example plugins, built from source, run under the real driver.
//!
//! These are not fixtures. The corpus in `crates/yah-plugin-wasm/guests` is
//! hand-written component text, each piece shaped to make one host bound
//! observable; these two are what a plugin author writes, compiled by the
//! toolchain that author would use. What they prove is different: that the WIT
//! world is a contract two unrelated toolchains can satisfy, and that the host
//! cannot tell which one it is talking to.
//!
//! They also close a gap the fixtures leave. No checked-in fixture imports
//! anything, so the host's guest-to-host path - its byte budget, its retained
//! log records, and the panic guard around each poll - had no case that entered
//! it. Both guests call `logging` and `cancellation`, so it is entered here.
//!
//! Artifacts come from `scripts/build-guests.sh` rather than from this test,
//! because building them needs two toolchains and one of them needs Node. A
//! missing artifact fails loudly with that instruction; nothing is skipped.

#[path = "support/fixtures.rs"]
mod fixtures;

use std::{path::PathBuf, sync::Arc};

use fixtures::revision;
use yah_compose::{
    ComponentDefinition, ComponentRevision, ComponentSlot, ComponentSlotOutcome,
    DesiredComponentState, ProviderAssignments, ProviderSelectionEpoch, ReconcileOutcome, Scope,
    ServiceRegistry,
};
use yah_plugin_host::{
    CapabilityBroker, DriverConformanceCase, EffectiveCapabilityGrants, HostPluginActivation,
    PluginRevision,
};
use yah_plugin_wasm::{
    LogRecord, ResourceState, WasmComponentDriver, WasmLimits, WasmObserver,
    bindings::yah::plugin::logging::LogLevel,
};

/// Where `scripts/build-guests.sh` leaves what it built.
fn artifact(name: &str) -> Vec<u8> {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../target/guests")
        .join(name);
    std::fs::read(&path).unwrap_or_else(|error| {
        panic!(
            "example guest artifact {} is missing ({error}). Run: bash scripts/build-guests.sh",
            path.display()
        )
    })
}

/// The Rust guest as a component.
///
/// `cargo build --target wasm32-unknown-unknown` produces a *core* module; the
/// component is that module plus the world it was generated against. Encoding
/// it here rather than in the build script keeps the guest toolchain to one
/// command, and the encoder is the same crate Wasmtime uses to read the result.
/// It is also the step a package loader will own, so doing it in the open is
/// worth more than hiding it behind a CLI.
fn rust_component() -> Vec<u8> {
    let core = artifact("rust-example.core.wasm");
    wit_component::ComponentEncoder::default()
        .module(&core)
        .expect("the core module carries its component type section")
        .validate(true)
        .encode()
        .expect("the core module encodes as a component")
}

fn ts_component() -> Vec<u8> {
    artifact("ts-example.component.wasm")
}

struct Rig {
    slot: ComponentSlot,
    registry: ServiceRegistry,
    broker: CapabilityBroker,
    grants: EffectiveCapabilityGrants,
    epoch: ProviderSelectionEpoch,
    revision: PluginRevision,
}

impl Rig {
    fn new(label: &str, digest: char) -> Self {
        let revision = revision(DriverConformanceCase::ReadyLifecycle, digest)
            .expect("fixture revision is valid");
        let registry = ServiceRegistry::new();
        let mut slot = ComponentSlot::new(label).expect("slot label is canonical");
        let desired = DesiredComponentState::enabled(
            slot.generation(1),
            ComponentRevision::new(
                format!("{label}.revision"),
                ComponentDefinition::new(format!("{label}.component")),
                Scope::root(format!("{label}.scope")),
            ),
            ProviderAssignments::new(),
        );
        let epoch = match slot
            .reconcile(&registry, desired)
            .expect("fresh component begins start")
        {
            ComponentSlotOutcome::Mounted {
                component: ReconcileOutcome::StartBegun { selection },
                ..
            } => selection.epoch(),
            other => panic!("fresh component did not begin start: {other:?}"),
        };
        let grants = EffectiveCapabilityGrants::empty(&revision);
        Self {
            slot,
            registry,
            broker: CapabilityBroker::new().expect("broker is constructible"),
            grants,
            epoch,
            revision,
        }
    }
}

/// What one guest did when the host activated it and called its tool.
struct GuestRun {
    answer: String,
    records: Vec<LogRecord>,
    cancellation_polls: usize,
}

/// Drive one authored component through the host's whole activation lifecycle.
///
/// Deliberately the host's path, not a linker this test built: preparation,
/// activation under the driver's limits and call deadline, a tool call on the
/// live store, then teardown that must leave the resource released. A guest
/// that only worked outside those bounds would pass a weaker test and fail a
/// real activation.
async fn run_guest(label: &str, digest: char, component: &[u8]) -> GuestRun {
    let mut rig = Rig::new(label, digest);
    let (driver, observer): (Arc<WasmComponentDriver>, WasmObserver) =
        WasmComponentDriver::for_component(
            rig.revision.id().clone(),
            component,
            WasmLimits::default(),
        )
        .expect("the example component compiles under the driver's own bounds");

    let mut activation = HostPluginActivation::prepare(
        &mut rig.slot,
        rig.epoch,
        &rig.broker,
        &rig.grants,
        driver.clone(),
    )
    .expect("preparation succeeds");
    let id = activation.id().clone();

    activation
        .activate(&rig.registry)
        .await
        .expect("the example guest activates");
    assert_eq!(observer.resource_state(&id), Ok(ResourceState::Live));

    let answer = driver
        .call_fixture_tool(&id, "{\"n\":1}")
        .await
        .expect("the example guest answers its tool call");

    let host = observer
        .host_observer(&id)
        .expect("a live activation has a host observer");
    let run = GuestRun {
        answer,
        records: host.records(),
        cancellation_polls: host.cancellation_polls(),
    };

    assert!(
        !run.records.is_empty(),
        "the guest must have logged before teardown, or releasing records proves nothing"
    );

    let (slot, _handle) = activation.release_active().expect("active releases");
    let removed = DesiredComponentState::removed(slot.generation(2));
    slot.reconcile(&rig.registry, removed)
        .expect("component begins stopping");
    slot.finish_stop(rig.epoch)
        .await
        .expect("cleanup completes");
    assert_eq!(observer.resource_state(&id), Ok(ResourceState::Released));
    // Teardown drops what the guest wrote. Record contents are guest-sized and
    // an observation outlives its store, so keeping them would make every
    // stopped activation a leak the host chose. Until these guests existed,
    // nothing had ever put a record there for teardown to release.
    assert!(
        host.records().is_empty(),
        "teardown must release the guest's log records: {:?}",
        host.records()
    );
    run
}

/// The claim the pair exists to support.
///
/// Same world, same input, same answer apart from the one field each guest
/// uses to name itself. If the host could tell them apart in any other way,
/// the world would not be the contract.
#[tokio::test]
async fn both_example_guests_answer_the_same_tool_call_the_same_way() {
    let rust = run_guest("wasm.example.rust", '1', &rust_component()).await;
    let typescript = run_guest("wasm.example.ts", '2', &ts_component()).await;

    assert_eq!(rust.answer, "{\"echo\":{\"n\":1},\"from\":\"rust\"}");
    assert_eq!(
        typescript.answer,
        "{\"echo\":{\"n\":1},\"from\":\"typescript\"}"
    );
    assert_eq!(
        rust.answer.replace("rust", "typescript"),
        typescript.answer,
        "the two guests must differ only in how they name themselves"
    );
}

/// The gap the fixture corpus leaves: a guest that calls back into the host.
///
/// Both imports, from both guests, observed on the host side. The fixtures
/// import nothing, so before this the host's guest-to-host path was enforced
/// and never entered.
#[tokio::test]
async fn both_example_guests_reach_the_host_through_its_imports() {
    for (name, component) in [("rust", rust_component()), ("typescript", ts_component())] {
        let digest = if name == "rust" { '3' } else { '4' };
        let run = run_guest(&format!("wasm.example.imports.{name}"), digest, &component).await;

        assert_eq!(
            run.records.len(),
            2,
            "{name} guest should log once at activation and once per call: {:?}",
            run.records
        );
        assert_eq!(run.records[0].level, LogLevel::Info);
        assert!(
            run.records[0].message.contains("activated"),
            "{name}: {:?}",
            run.records[0]
        );
        assert_eq!(run.records[1].level, LogLevel::Debug);
        // The field carries the request size, which is the one thing in these
        // records the guest computed rather than spelled out.
        assert_eq!(
            run.records[1].fields,
            vec![("bytes".to_owned(), "7".to_owned())],
            "{name} guest should report the request size it saw"
        );
        assert!(
            run.cancellation_polls >= 1,
            "{name} guest should ask whether it was cancelled"
        );
    }
}
