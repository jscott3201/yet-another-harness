//! Fenced, level-triggered desired state for one stable component slot.
//!
//! A [`ComponentSlot`] owns at most one mounted [`crate::ReconciledComponent`]
//! and compares it with caller-supplied desired generations. Configuration is
//! represented by an opaque immutable revision identity; payload loading and
//! validation remain host responsibilities. This module deliberately does not
//! schedule a graph, persist intent, rank providers, or run callbacks.

mod component_slot;
mod model;

pub use component_slot::ComponentSlot;
pub use model::{
    ComponentRevision, ComponentSlotError, ComponentSlotOutcome, DesiredComponentState,
    DesiredGeneration, StopDisposition, StopRecord,
};
