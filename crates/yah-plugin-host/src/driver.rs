//! Runtime-neutral plugin driver lifecycle.
//!
//! A driver first prepares an inert, exact-activation control object. The host
//! then transfers deactivation into the component's effect scope before it
//! constructs or polls the activation future. This order gives partially
//! started drivers one cleanup owner without choosing an executor, loader, or
//! concrete execution backend. Actionable capabilities arrive only inside the
//! start permit minted after cleanup admission.

mod activation;
mod control;
mod health;
mod model;

pub use activation::{ActivatePlugin, HostPluginActivation};
pub use health::PluginActivationHandle;
pub use model::{
    DriverActivationError, DriverActivationErrorKind, DriverDeactivationError, DriverFuture,
    DriverHealthError, DriverPrepareError, DriverStartPermit, DriverStopPermit,
    HostPluginActivationError, PluginActivationId, PluginActivationRequest, PluginDriver,
    PluginHealth, PluginHealthError, PluginStartError, PreparedDriverActivation,
};
