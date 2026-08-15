//! Live composition primitives for Yet Another Harness.
//!
//! This crate owns process-local component identity, scope relationships, and
//! lifecycle transitions. It deliberately has no Selene dependency and none of
//! its values are durable records. The durable kernel and future desired-state
//! reconciler may decide *which* components should exist; this crate governs a
//! live instance while it exists.
//!
//! The current slice is intentionally narrow. It does not yet run component
//! callbacks, own reversible effect scopes, resolve services, or reconcile a
//! component graph.

mod component;
mod id;
mod lifecycle;
mod scope;

pub use component::{ComponentDefinition, ComponentInstance};
pub use id::{ComponentId, ComponentInstanceId, ScopeId};
pub use lifecycle::{
    ActivationEpoch, ComponentFailure, ComponentState, ComponentStateKind, FailurePhase,
    LifecycleAction, LifecycleError, StopTarget,
};
pub use scope::{Scope, ScopeError};
