//! Wasmtime component driver for YAH's provisional WIT contract.
//!
//! The canonical WIT source defines one conformance world. Host and guest
//! bindings are checked against it, and a Wasmtime-backed [`PluginDriver`]
//! instantiates fixture components against the same source.
//!
//! [`PluginDriver`]: yah_plugin_host::PluginDriver
//!
//! Activations run under host-owned bounds ([`limits`]): ceilings on total
//! memory and table size and on how many memories, tables, and instances one
//! activation may hold, a call deadline, and caps on what a guest-to-host call
//! may transfer and what the host retains from it.
//!
//! This crate does not load plugin packages, meter fuel, transport capability
//! grants across the ABI, or contain hostile guest code. Bounding what a guest
//! costs is not isolating what it can reach, and its fixture components are
//! corpus, not a guest SDK.

pub mod bindings;
pub mod driver;
pub mod guest;
pub mod host;
pub mod limits;

pub use driver::{
    ResourceState, WasmActivationPlan, WasmComponentDriver, WasmDriverBuildError, WasmObserver,
};
pub use guest::GuestProgram;
pub use host::{HostObserver, HostState, LogRecord, RETAINED_LOG_RECORDS};
pub use limits::{EpochTicker, GuestInterrupt, WasmLimits};

/// Fully versioned WIT package the driver and contract tests share.
pub const WIT_PACKAGE: &str = "yah:plugin@0.1.0";

/// Provisional world the host driver and its fixture components implement.
pub const WIT_WORLD: &str = "conformance";
