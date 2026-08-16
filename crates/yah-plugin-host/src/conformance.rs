//! Reusable host-side semantic conformance for [`PluginDriver`](crate::PluginDriver).
//!
//! The harness exercises a trusted driver fixture through the same public
//! composition, capability, and host-activation APIs used by a real host. It
//! does not expose host-only permits, define a guest ABI, or certify package
//! loading, sandboxing, capability transport, or backend containment.

mod cases;
mod model;
mod observe;
mod run;

pub use model::{
    DriverActivationObservation, DriverConformanceCase, DriverConformanceCaseReport,
    DriverConformanceCaseResult, DriverConformanceFailure, DriverConformancePhase,
    DriverConformanceProbe, DriverConformanceProbeError, DriverConformanceReport,
    DriverConformanceResourceState, DriverConformanceSetupError, DriverConformanceSubject,
    DriverConformanceTarget, DriverConformanceTeardown, DriverConformanceTerminal,
};
pub use run::{run_driver_conformance, run_driver_conformance_case};
