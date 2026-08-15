//! Live composition primitives for Yet Another Harness.
//!
//! This crate owns process-local component identity, scope relationships, and
//! lifecycle transitions plus activation-owned reversible effect scopes. It
//! deliberately has no Selene dependency and none of its values are durable
//! records. The durable kernel and future desired-state reconciler may decide
//! *which* components should exist; this crate governs a live instance while it
//! exists.
//!
//! The current slice is intentionally narrow. It does not yet run component
//! callbacks, resolve services, or reconcile a component graph.

mod component;
mod effect_scope;
mod id;
mod lifecycle;
mod scope;

pub use component::{ComponentDefinition, ComponentInstance};
pub use effect_scope::{
    CleanupError, CleanupFailure, CleanupFailureKind, CleanupOutcome, CleanupRecord, CleanupResult,
    CloseReport, CloseScope, CloseStep, EffectRegistrationId, EffectScope, EffectScopeError,
    EffectScopeId, EffectScopeState, ScopeCancellation,
};
pub use id::{ComponentId, ComponentInstanceId, ScopeId};
pub use lifecycle::{
    ActivationEpoch, ComponentFailure, ComponentState, ComponentStateKind, FailurePhase,
    LifecycleAction, LifecycleError, StopTarget,
};
pub use scope::{Scope, ScopeError};
