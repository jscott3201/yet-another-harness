//! Wasmtime component driver for YAH's provisional WIT contract.
//!
//! The canonical WIT source defines one conformance world. Host and guest
//! bindings are checked against it, and a Wasmtime-backed [`PluginDriver`]
//! instantiates fixture components against the same source.
//!
//! [`PluginDriver`]: yah_plugin_host::PluginDriver
//!
//! This crate does not load plugin packages, enforce memory, deadline, fuel, or
//! host-call limits, transport capability grants across the ABI, or contain
//! hostile guest code. Its fixture components are corpus, not a guest SDK.

pub mod bindings;
pub mod driver;
pub mod guest;
pub mod host;

pub use driver::{
    ResourceState, WasmActivationPlan, WasmComponentDriver, WasmDriverBuildError, WasmObserver,
};
pub use guest::GuestProgram;
pub use host::{HostObserver, HostState, LogRecord, RETAINED_LOG_RECORDS};

/// Fully versioned WIT package the driver and contract tests share.
pub const WIT_PACKAGE: &str = "yah:plugin@0.1.0";

/// Provisional world the host driver and its fixture components implement.
pub const WIT_WORLD: &str = "conformance";
