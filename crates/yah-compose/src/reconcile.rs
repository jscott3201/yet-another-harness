//! Provider-selection epochs and level-triggered dependency reconciliation.
//!
//! [`ReconciledComponent`] owns a frozen mounted definition, its live instance,
//! one immutable exact provider assignment per activation, and that activation's
//! effect scope. Provider choice remains explicit policy input. Changing or
//! losing an assigned provider seals and tears down the old activation before a
//! fresh selection can start; handles never switch providers in place.
//! Provider ranking, requested removal, activation callbacks/failures, and
//! host-wide desired-state scheduling remain higher-layer responsibilities.

mod model;
mod reconciled_component;

pub use model::{
    DependencyIssue, DependencyReadiness, DependencyStopReason, ProviderAssignments,
    ProviderChange, ProviderSelection, ProviderSelectionEpoch, ReconcileError, ReconcileOutcome,
    StopCompletion,
};
pub use reconciled_component::ReconciledComponent;
