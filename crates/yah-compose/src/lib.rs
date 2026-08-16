//! Live composition primitives for Yet Another Harness.
//!
//! This crate owns process-local component identity, scope relationships,
//! lifecycle transitions, activation-owned reversible effect scopes, and
//! typed live service bindings. It deliberately has no Selene dependency and
//! none of its values are durable records. The durable kernel and future
//! desired-state authority may decide *which* components should exist; this
//! crate governs one process-local desired slot and its live instance.
//! Explicit effect-scope close drains synchronous service calls already
//! admitted against the provider and consumer activation trees before running
//! local cleanup; it is not a task supervisor or deadline mechanism.
//!
//! The dependency layer owns one frozen mounted component and converges it
//! toward caller-selected exact providers through fenced start and teardown.
//! A desired component slot additionally fences create, revision replacement,
//! disable, removal, and exact-epoch activation failure without claiming
//! host-wide graph scheduling. It does not yet run component callbacks, choose
//! provider policy, or watch the registry.

mod component;
mod desired_state;
mod effect_scope;
mod id;
mod lifecycle;
mod reconcile;
mod scope;
mod service;

pub use component::{ComponentDefinition, ComponentDefinitionError, ComponentInstance};
pub use desired_state::{
    ComponentRevision, ComponentSlot, ComponentSlotError, ComponentSlotOutcome,
    DesiredComponentState, DesiredGeneration, StopDisposition, StopRecord,
};
pub use effect_scope::{
    CleanupError, CleanupFailure, CleanupFailureKind, CleanupOutcome, CleanupRecord, CleanupResult,
    CloseReport, CloseScope, CloseStep, EffectRegistrationId, EffectScope, EffectScopeError,
    EffectScopeId, EffectScopeState, ScopeCancellation,
};
pub use id::{ComponentId, ComponentInstanceId, ComponentRevisionId, ScopeId, ServiceId};
pub use lifecycle::{
    ActivationEpoch, ComponentFailure, ComponentState, ComponentStateKind, FailurePhase,
    LifecycleAction, LifecycleError, StopTarget,
};
pub use reconcile::{
    ComponentStopReason, DependencyIssue, DependencyReadiness, DependencyStopReason,
    DesiredStopReason, ProviderAssignments, ProviderChange, ProviderSelection,
    ProviderSelectionEpoch, ReconcileError, ReconcileOutcome, ReconciledComponent, StopCompletion,
};
pub use scope::{Scope, ScopeError};
pub use service::{
    ProviderCandidate, ProviderRegistrationId, RequiredService, RequirementCandidates,
    RequirementStatus, ServiceDefinition, ServiceHandle, ServiceHandleError, ServiceProvider,
    ServiceRegistry, ServiceRegistryError, ServiceRequirement,
};
