//! EXP-001 gate harness (G02): Selene fan-in and crash recovery.
//!
//! Library face: everything the `exp001` binary's roles (orchestrator,
//! worker, auditor) and the integration tests share.

pub mod audit;
pub mod bench;
pub mod manifest;
pub mod orchestrate;
pub mod plan;
pub mod schema;
pub mod sidecar;
pub mod store;
pub mod worker;
pub mod workload;
