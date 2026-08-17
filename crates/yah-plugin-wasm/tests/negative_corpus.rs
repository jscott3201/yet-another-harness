//! Components the host must refuse, at whichever stage it can see them.
//!
//! The corpus in `guests/` is the other half of this: well-formed components
//! that misbehave once they *run*, held to the host's resource bounds in
//! `resource_limits.rs`. These are refused earlier, and the point of the file
//! is which refusal happens where - malformed bytes and an undeclared import
//! never build a driver at all, while a component missing the world's exports
//! builds one and fails when the host asks it for the interfaces it promised.
//! `for_component` is the loader-shaped entry point - it takes bytes from a
//! caller and returns a [`WasmDriverBuildError`] - so this is where a package
//! loader's first refusal will live, and the build-time cases are asserted
//! through it rather than through a `Component::new` a test called itself.
//!
//! Written as component text, including the byte-level cases: `wat` accepts
//! `(component binary "...")` and passes the literal bytes through unvalidated,
//! so a truncated header or a bad version stays reviewable in a diff and no
//! `.wasm` is committed (DEC-038). The one trap worth naming: `Component::new`
//! runs `wat::parse_bytes` first, which treats input *not* starting with
//! `\0asm` as text - so handing it raw bad-magic bytes reports a text-parse
//! error about an unexpected character rather than anything about magic. The
//! `(component binary ...)` wrapper is what puts the bytes on the binary path.

use yah_compose::{
    ComponentDefinition, ComponentRevision, ComponentSlot, ComponentSlotOutcome,
    DesiredComponentState, ProviderAssignments, ProviderSelectionEpoch, ReconcileOutcome, Scope,
    ServiceRegistry,
};
use yah_plugin_wasm::{ResourceState, WasmComponentDriver, WasmLimits};

#[path = "support/fixtures.rs"]
mod fixtures;

use fixtures::revision;
use yah_plugin_host::{
    CapabilityBroker, DriverConformanceCase, EffectiveCapabilityGrants, HostPluginActivation,
    PluginRevision, PluginStartError,
};

/// The compose-side scaffolding one activation needs.
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
        let revision =
            revision(DriverConformanceCase::ReadyLifecycle, digest).expect("fixture revision");
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

/// Build a driver from `text`, returning the refusal summary.
fn refusal(label_digest: char, text: &str) -> String {
    let revision =
        revision(DriverConformanceCase::ReadyLifecycle, label_digest).expect("fixture revision");
    match WasmComponentDriver::for_component(
        revision.id().clone(),
        text.as_bytes(),
        WasmLimits::default(),
    ) {
        Ok(_) => panic!("these bytes must not build a driver"),
        Err(error) => error.summary().to_owned(),
    }
}

/// Malformed input is refused, and the refusal says which way it was malformed.
///
/// One case per distinct failure the header can have, because a single
/// "did not compile" assertion would pass while the driver confused a truncated
/// component with a core module - and those want different operator responses.
#[test]
fn malformed_component_bytes_are_refused_and_named() {
    // (case, component text, a substring the refusal must carry)
    let cases: &[(&str, &str, &str)] = &[
        ("empty input", "", "expected at least one module field"),
        (
            "header only, nothing after it",
            r#"(component binary "\00asm")"#,
            "unexpected end-of-file",
        ),
        (
            "a core module where a component belongs",
            r#"(component binary "\00asm\01\00\00\00")"#,
            "attempted to parse a wasm module with a component parser",
        ),
        (
            "component layer with an unknown version",
            r#"(component binary "\00asm\ff\ff\05\00")"#,
            "unknown binary version",
        ),
        (
            "well-formed header, junk section id",
            r#"(component binary "\00asm\0d\00\01\00\ff\ff\ff\ff")"#,
            "malformed section id",
        ),
    ];

    for (case, text, expected) in cases {
        let summary = refusal('8', text);
        assert!(
            summary.contains(expected),
            "{case}: refusal should carry {expected:?}, but was {summary:?}"
        );
    }
}

/// A guest may not declare an import the world does not.
///
/// The empty-instance case is the one this rule exists for. Wasmtime refuses an
/// import it cannot satisfy, but an instance with no functions asks it to
/// satisfy nothing, so before `surface::check_import_surface` this component
/// compiled, activated, and answered a tool call with the host none the wiser.
#[test]
fn a_component_may_not_import_outside_the_world() {
    // One expected name per row, not a disjunction shared across the table: a
    // shared `contains(a) || contains(b)` is satisfied for every row by whichever
    // name happens to appear, so a check that reported a constant would pass it.
    let cases: &[(&str, &str, &str)] = &[
        (
            "an empty instance, which Wasmtime alone does not refuse",
            r#"(component
                 (type $empty (instance))
                 (import "wasi:cli/environment@0.2.0" (instance (type $empty))))"#,
            "wasi:cli/environment@0.2.0",
        ),
        (
            "a bare function import",
            r#"(component (import "escape" (func)))"#,
            "escape",
        ),
    ];

    for (case, text, expected) in cases {
        let summary = refusal('9', text);
        assert!(
            summary.contains(expected),
            "{case}: the refusal must name {expected:?}, but was {summary:?}"
        );
        assert!(
            summary.contains("does not declare"),
            "{case}: the refusal must say the world does not declare it, but was {summary:?}"
        );
    }
}

/// The rule refuses what is undeclared, not everything.
///
/// Without this the previous test would pass against a driver that refused
/// every import, including the two the world does declare - which would break
/// every real guest while looking like containment.
#[test]
fn the_world_s_own_imports_are_not_refused() {
    let revision = revision(DriverConformanceCase::ReadyLifecycle, 'a').expect("fixture revision");
    let text = r#"(component
         (type $empty (instance))
         (import "yah:plugin/logging@0.1.0" (instance (type $empty)))
         (import "yah:plugin/cancellation@0.1.0" (instance (type $empty))))"#;
    assert!(
        WasmComponentDriver::for_component(
            revision.id().clone(),
            text.as_bytes(),
            WasmLimits::default(),
        )
        .is_ok(),
        "a component importing exactly the world's interfaces must build"
    );
}

/// A component that does not implement the world is refused when it is asked to.
///
/// This one compiles and builds a driver: nothing about its bytes is wrong and
/// it imports nothing, so neither earlier gate has anything to say. What it
/// lacks is the exports the world requires, and the host finds that out at the
/// moment it tries to bind them - which is why the case is here rather than
/// with the build-time refusals above.
#[tokio::test]
async fn a_component_that_implements_none_of_the_world_is_refused_at_activation() {
    let mut rig = Rig::new("wasm.negative.no-exports", 'f');
    // Two tokens, and a valid component. Every requirement it fails is a
    // requirement of the world rather than of WebAssembly.
    let (driver, observer) = WasmComponentDriver::for_component(
        rig.revision.id().clone(),
        b"(component)",
        WasmLimits::default(),
    )
    .expect("an empty component is well-formed, so the driver builds");

    let mut activation =
        HostPluginActivation::prepare(&mut rig.slot, rig.epoch, &rig.broker, &rig.grants, driver)
            .expect("preparation succeeds");
    let id = activation.id().clone();

    let failure = match activation.activate(&rig.registry).await {
        Err(PluginStartError::Driver { failure, .. }) => failure,
        other => {
            panic!("a component with none of the world's exports must not activate: {other:?}")
        }
    };
    assert!(
        failure.summary().contains("yah:plugin/lifecycle@0.1.0"),
        "the refusal must name the interface the guest did not export: {}",
        failure.summary()
    );

    activation.finish_stop().await.expect("cleanup completes");
    assert_eq!(observer.resource_state(&id), Ok(ResourceState::NotAcquired));
}
