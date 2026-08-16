//! Where an activation's startup time actually goes.
//!
//! The question is architectural, not a benchmark. Compiling a component and
//! instantiating one are different costs paid at different times by different
//! parts of the system: a loader pays compilation once and could cache it,
//! while every activation pays instantiation and no cache removes that. Which
//! dominates decides whether a package loader must carry a compiled-artifact
//! cache at all.
//!
//! These fixtures are hand-written text components of a few kilobytes, so the
//! figures are a floor, not a prediction. A guest built from a language
//! toolchain costs more to compile and no less to instantiate.
//!
//! Run with: cargo run -p yah-plugin-wasm --example startup_cost --release

use std::time::Instant;

use wasmtime::{
    Config, Engine, ResourceLimiter, Store,
    component::{Component, HasSelf, Linker},
};
use yah_plugin_wasm::{GuestProgram, HostObserver, HostState, WasmLimits, bindings::Conformance};

fn main() -> wasmtime::Result<()> {
    let limits = WasmLimits::default();
    let mut config = Config::new();
    config.epoch_interruption(true);
    config.memory_reservation(limits.memory_reservation_bytes);
    config.memory_reservation_for_growth(limits.memory_reservation_bytes);
    config.memory_guard_size(limits.memory_guard_bytes);
    let engine = Engine::new(&config)?;

    // One compile before any measurement. The first compilation in a process
    // pays for engine state every later one reuses, and reporting that as the
    // cost of compiling a component would overstate it by an order of
    // magnitude.
    let cold = Instant::now();
    let _ = Component::new(&engine, GuestProgram::Conformant.text())?;
    let cold = cold.elapsed();
    println!("first compile in the process (engine warm-up included): {cold:?}\n");

    println!(
        "{:<16} {:>9} {:>11} {:>13} {:>12}",
        "fixture", "source B", "compile", "instantiate", "compiled B"
    );

    for program in [
        GuestProgram::Conformant,
        GuestProgram::ActivateFailure,
        GuestProgram::Runaway,
        GuestProgram::MemoryHog,
        GuestProgram::MultiMemory,
        GuestProgram::ManyMemories,
    ] {
        let text = program.text();

        let started = Instant::now();
        let component = Component::new(&engine, text)?;
        let compile = started.elapsed();
        let compiled = component.serialize()?.len();

        let mut linker: Linker<HostState> = Linker::new(&engine);
        Conformance::add_to_linker::<HostState, HasSelf<HostState>>(&mut linker, |state| state)?;

        // Only the guests that instantiate can be timed instantiating. The
        // count fixture is refused by the host's own ceiling, which is the
        // point of it.
        let rounds = 500;
        let started = Instant::now();
        let mut instantiated = 0u32;
        for _ in 0..rounds {
            let mut store =
                Store::new(&engine, HostState::with_limits(HostObserver::new(), limits));
            // The same store setup the driver uses. Without the limiter the
            // ceilings are not in force and this would measure a store the
            // host never builds - the count fixture would instantiate here
            // and be refused in production.
            store.limiter(|state: &mut HostState| state.limiter() as &mut dyn ResourceLimiter);
            store.set_hostcall_fuel(limits.host_call_bytes);
            store.set_epoch_deadline(1);
            if Conformance::instantiate(&mut store, &component, &linker).is_ok() {
                instantiated += 1;
            }
        }
        let instantiate = if instantiated > 0 {
            format!("{:?}", started.elapsed() / instantiated)
        } else {
            "refused".to_owned()
        };

        println!(
            "{:<16} {:>9} {:>11?} {:>13} {:>12}",
            format!("{program:?}"),
            text.len(),
            compile,
            instantiate,
            compiled
        );
    }

    // What a loader could cache, and what it would save.
    let compiled = Component::new(&engine, GuestProgram::Conformant.text())?.serialize()?;
    let rounds = 50;
    let started = Instant::now();
    for _ in 0..rounds {
        // Safety: the bytes came from `serialize` on this same engine in this
        // same process, which is exactly the precondition.
        let restored = unsafe { Component::deserialize(&engine, &compiled)? };
        std::hint::black_box(&restored);
    }
    println!(
        "\nloading a precompiled component instead of compiling it: {:?}",
        started.elapsed() / rounds
    );
    Ok(())
}
