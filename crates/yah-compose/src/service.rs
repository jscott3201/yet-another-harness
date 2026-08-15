//! Typed, process-local service publication and binding.
//!
//! This module deliberately inventories provider candidates without selecting
//! between them. A caller binds one exact registration; the future reconciler
//! will own selection epochs and dependent lifecycle convergence.

mod handle;
mod model;
mod registry;

pub use handle::{ServiceHandle, ServiceHandleError};
pub use model::{
    ProviderCandidate, ProviderRegistrationId, RequiredService, RequirementStatus,
    ServiceDefinition, ServiceProvider, ServiceRegistryError, ServiceRequirement,
};
pub use registry::ServiceRegistry;
