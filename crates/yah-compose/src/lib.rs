//! Live composition primitives for Yet Another Harness.
//!
//! This crate owns process-local component identity, scope relationships,
//! lifecycle transitions, activation-owned reversible effect scopes, and
//! typed live service bindings. It deliberately has no Selene dependency and
//! none of its values are durable records. The durable kernel and future
//! desired-state reconciler may decide *which* components should exist; this
//! crate governs a live instance while it exists.
//!
//! The dependency layer owns one frozen mounted component and converges it
//! toward caller-selected exact providers through fenced start and teardown.
//! It does not yet run component callbacks, choose provider policy, watch the
//! registry, or reconcile a desired component graph.

mod component;
mod effect_scope;
mod id;
mod lifecycle;
mod reconcile;
mod scope;
mod service;

pub use component::{ComponentDefinition, ComponentDefinitionError, ComponentInstance};
pub use effect_scope::{
    CleanupError, CleanupFailure, CleanupFailureKind, CleanupOutcome, CleanupRecord, CleanupResult,
    CloseReport, CloseScope, CloseStep, EffectRegistrationId, EffectScope, EffectScopeError,
    EffectScopeId, EffectScopeState, ScopeCancellation,
};
pub use id::{ComponentId, ComponentInstanceId, ScopeId, ServiceId};
pub use lifecycle::{
    ActivationEpoch, ComponentFailure, ComponentState, ComponentStateKind, FailurePhase,
    LifecycleAction, LifecycleError, StopTarget,
};
pub use reconcile::{
    DependencyIssue, DependencyReadiness, DependencyStopReason, ProviderAssignments,
    ProviderChange, ProviderSelection, ProviderSelectionEpoch, ReconcileError, ReconcileOutcome,
    ReconciledComponent, StopCompletion,
};
pub use scope::{Scope, ScopeError};
pub use service::{
    ProviderCandidate, ProviderRegistrationId, RequiredService, RequirementCandidates,
    RequirementStatus, ServiceDefinition, ServiceHandle, ServiceHandleError, ServiceProvider,
    ServiceRegistry, ServiceRegistryError, ServiceRequirement,
};
