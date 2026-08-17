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
//! activation may hold, the stack one call runs on and how deep the guest may
//! recurse on it, a call deadline, and caps on what a guest-to-host call may
//! transfer and what the host retains from it.
//!
//! Guest calls run on their own stack and yield the thread at each epoch tick,
//! so one guest cannot starve another by computing.
//!
//! Granted capabilities reach a guest as opaque WIT resources: the authority
//! is the resource `acquire` returns - activation-scoped through the broker
//! handle behind it - never the import itself, and every use re-enters the
//! broker's revocation and fencing gates ([`capability`]).
//!
//! This crate does not load plugin packages, meter fuel, or contain hostile
//! guest code. Bounding what a guest costs is not isolating what it can reach,
//! and its fixture components are corpus, not a guest SDK.

pub mod bindings;
pub mod capability;
pub mod driver;
pub mod guest;
pub mod host;
pub mod limits;
pub mod surface;

pub use capability::GrantedCapability;
pub use driver::{
    ResourceState, WasmActivationPlan, WasmComponentDriver, WasmDriverBuildError, WasmObserver,
};
pub use guest::GuestProgram;
pub use host::{HostObserver, HostState, LogRecord, RETAINED_LOG_RECORDS};
pub use limits::{EpochTicker, GuestInterrupt, HOST_STACK_HEADROOM_BYTES, WasmLimits};
pub use surface::{WORLD_IMPORTS, check_import_surface};

/// Fully versioned WIT package the driver and contract tests share.
pub const WIT_PACKAGE: &str = "yah:plugin@0.1.0";

/// Provisional world the host driver and its fixture components implement.
pub const WIT_WORLD: &str = "conformance";
