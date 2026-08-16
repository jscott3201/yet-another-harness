//! Typed activation-scoped host capability brokerage.
//!
//! Manifest requests are declarations, not authority. A trusted host chooses
//! an immutable subset of those requests and binds each grant to one exact
//! broker registration. Drivers receive the resulting context only through the
//! start permit minted after deactivation cleanup is admitted.

mod broker;
mod handle;
mod model;

pub use broker::{
    CapabilityBroker, CapabilityBrokerError, CapabilityProviderRegistration, PluginStartContext,
};
pub use handle::{CapabilityHandle, CapabilityHandleError};
pub use model::{
    CapabilityDefinition, CapabilityGrant, CapabilityGrantError, CapabilityRegistrationId,
    EffectiveCapabilityGrants,
};
