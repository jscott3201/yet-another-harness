//! Runtime-neutral plugin driver lifecycle.
//!
//! A driver first prepares an inert, exact-activation control object. The host
//! then transfers deactivation into the component's effect scope before it
//! constructs or polls the activation future. This order gives partially
//! started drivers one cleanup owner without choosing an executor, loader, or
//! capability model in this crate.

mod activation;
mod control;
mod model;

pub use activation::{ActivatePlugin, HostPluginActivation, PluginActivationHandle};
pub use model::{
    DriverActivationError, DriverActivationErrorKind, DriverDeactivationError, DriverFuture,
    DriverHealthError, DriverPrepareError, DriverStartPermit, DriverStopPermit,
    HostPluginActivationError, PluginActivationId, PluginActivationRequest, PluginDriver,
    PluginHealth, PluginHealthError, PluginStartError, PreparedDriverActivation,
};
