//! End-to-end evidence that a real component instantiates, answers, and stops.
//!
//! The lifecycle corpus in `driver_conformance.rs` proves host-facing driver
//! semantics. This file proves the other half: that the world's exports are
//! callable across the canonical ABI and that shutdown is entirely host-owned.
//!
//! The fixtures smoked here import nothing. The world's imports are linked and
//! proved linkable, but no guest in this file calls back through them; guests
//! that exercise the import path live in `example_guests.rs` (logging,
//! cancellation) and `capability_transport.rs` (capabilities).

use wasmtime::{
    Engine, Store,
    component::{Component, HasSelf, Linker},
};
use yah_plugin_wasm::{
    GuestProgram,
    bindings::{Conformance, exports::yah::plugin::lifecycle::ErrorCode},
    host::{HostObserver, HostState},
};

struct Harness {
    engine: Engine,
    component: Component,
    linker: Linker<HostState>,
}

impl Harness {
    fn new(program: GuestProgram) -> Self {
        let engine = Engine::default();
        let component = Component::new(&engine, program.text()).expect("fixture compiles");
        let mut linker: Linker<HostState> = Linker::new(&engine);
        Conformance::add_to_linker::<HostState, HasSelf<HostState>>(&mut linker, |state| state)
            .expect("host imports link");
        Self {
            engine,
            component,
            linker,
        }
    }

    async fn instantiate(&self) -> (Store<HostState>, Conformance) {
        let mut store = Store::new(&self.engine, HostState::new(HostObserver::new()));
        let bindings = Conformance::instantiate_async(&mut store, &self.component, &self.linker)
            .await
            .expect("fixture instantiates");
        (store, bindings)
    }
}

#[tokio::test]
async fn conformant_component_activates_and_answers_a_fixture_tool_call() {
    let harness = Harness::new(GuestProgram::Conformant);
    let (mut store, bindings) = harness.instantiate().await;

    bindings
        .yah_plugin_lifecycle()
        .call_activate(&mut store)
        .await
        .expect("activate does not trap")
        .expect("conformant fixture activates");

    let answered = bindings
        .yah_plugin_fixture_tool()
        .call_invoke(&mut store, "{\"ping\":true}")
        .await
        .expect("invoke does not trap")
        .expect("conformant fixture answers");

    assert_eq!(answered, "{\"activated\":true}");
}

#[tokio::test]
async fn activate_failure_component_returns_the_declared_guest_error() {
    let harness = Harness::new(GuestProgram::ActivateFailure);
    let (mut store, bindings) = harness.instantiate().await;

    let refused = bindings
        .yah_plugin_lifecycle()
        .call_activate(&mut store)
        .await
        .expect("activate does not trap")
        .expect_err("failure fixture refuses activation");

    assert_eq!(refused.code, ErrorCode::Failed);
    assert_eq!(refused.message, "fixture refused activation");
}

#[tokio::test]
async fn dropping_the_store_is_the_whole_shutdown_and_leaves_the_engine_reusable() {
    let harness = Harness::new(GuestProgram::Conformant);
    let (mut first, bindings) = harness.instantiate().await;
    bindings
        .yah_plugin_lifecycle()
        .call_activate(&mut first)
        .await
        .expect("activate does not trap")
        .expect("conformant fixture activates");

    // The world exports no guest deactivation hook, so this drop is the entire
    // teardown path. Nothing asks guest code for permission to stop.
    drop(first);

    // Host-owned artifacts outlive the activation they served: a fresh store
    // instantiates from the same compiled component with no residue from the
    // one just torn down.
    let (mut second, reborn) = harness.instantiate().await;
    let answered = reborn
        .yah_plugin_fixture_tool()
        .call_invoke(&mut second, "{}")
        .await
        .expect("invoke does not trap")
        .expect("fixture answers after the prior store was dropped");
    assert_eq!(answered, "{\"activated\":true}");
}
