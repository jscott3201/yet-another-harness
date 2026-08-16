//! Typed, process-local service publication and binding.
//!
//! This module deliberately inventories provider candidates without ranking
//! them. Low-level callers bind one exact registration; [`crate::ReconciledComponent`]
//! freezes caller-selected assignments for one activation and owns dependent
//! lifecycle convergence.

mod handle;
mod model;
mod registry;

pub use handle::{ServiceHandle, ServiceHandleError};
pub use model::{
    ProviderCandidate, ProviderRegistrationId, RequiredService, RequirementCandidates,
    RequirementStatus, ServiceDefinition, ServiceProvider, ServiceRegistryError,
    ServiceRequirement,
};
pub use registry::ServiceRegistry;
