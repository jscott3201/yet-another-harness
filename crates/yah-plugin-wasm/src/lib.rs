//! Compile-checked WIT contract for YAH's future Wasm Component driver.
//!
//! This crate does not yet load or execute a component. The canonical WIT
//! source defines one provisional conformance world whose bindings are checked
//! in tests against the pinned host and guest generators. Wasmtime becomes a
//! runtime dependency only when the crate owns a real driver.

/// Fully versioned WIT package selected by the compile-time probes.
pub const WIT_PACKAGE: &str = "yah:plugin@0.1.0";

/// Provisional world used by the first host-driver conformance component.
pub const WIT_WORLD: &str = "conformance";
