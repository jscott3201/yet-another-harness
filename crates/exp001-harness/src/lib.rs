//! EXP-001 gate harness (G02): Selene fan-in and crash recovery.
//!
//! Library face: everything the `exp001` binary's roles (orchestrator,
//! worker, auditor) and the integration tests share.

pub mod manifest;
pub mod plan;
pub mod schema;
pub mod store;
pub mod workload;
