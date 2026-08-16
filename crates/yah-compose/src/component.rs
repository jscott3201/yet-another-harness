//! Component definition and live instance identity.

use std::sync::atomic::{AtomicU64, Ordering};

use crate::{
    ActivationEpoch, ComponentFailure, ComponentId, ComponentInstanceId, ComponentState,
    LifecycleError, RequiredService, Scope, ScopeId, ServiceId, ServiceRequirement, StopTarget,
};

static NEXT_INSTANCE_INCARNATION: AtomicU64 = AtomicU64::new(1);

/// Stable identity of a component definition.
///
/// Factories and manifest metadata remain deferred until a real activation
/// contract exercises them. Required services are declared here so a mounted
/// reconciled component can freeze them before it starts an instance.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ComponentDefinition {
    id: ComponentId,
    requirements: Vec<RequiredService>,
}

impl ComponentDefinition {
    pub fn new(id: impl Into<ComponentId>) -> Self {
        Self {
            id: id.into(),
            requirements: Vec::new(),
        }
    }

    pub fn id(&self) -> &ComponentId {
        &self.id
    }

    /// Declare one exact required service.
    ///
    /// # Errors
    ///
    /// Returns [`ComponentDefinitionError::DuplicateRequirement`] if this
    /// definition already declares the same semantic service identity.
    pub fn require<T: ?Sized + Send + Sync + 'static>(
        &mut self,
        requirement: &ServiceRequirement<T>,
    ) -> Result<(), ComponentDefinitionError> {
        if self
            .requirements
            .iter()
            .any(|declared| declared.service_id() == requirement.service_id())
        {
            return Err(ComponentDefinitionError::DuplicateRequirement {
                component_id: self.id.clone(),
                service_id: requirement.service_id().clone(),
            });
        }
        self.requirements.push(requirement.erased());
        Ok(())
    }

    pub fn requirements(&self) -> &[RequiredService] {
        &self.requirements
    }
}

/// Invalid service declarations on one component definition.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ComponentDefinitionError {
    DuplicateRequirement {
        component_id: ComponentId,
        service_id: ServiceId,
    },
}

impl std::fmt::Display for ComponentDefinitionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::DuplicateRequirement {
                component_id,
                service_id,
            } => write!(
                f,
                "component {component_id} declares service {service_id} more than once"
            ),
        }
    }
}

impl std::error::Error for ComponentDefinitionError {}

/// One mounted component definition in one live scope.
///
/// The instance is intentionally not `Clone`: duplicating it would duplicate
/// lifecycle authority. Its state transitions are synchronous bookkeeping;
/// future callback runners will use the returned activation epoch to fence
/// asynchronous completion.
#[derive(Debug)]
pub struct ComponentInstance {
    id: ComponentInstanceId,
    definition_id: ComponentId,
    scope_id: ScopeId,
    scope: Scope,
    state: ComponentState,
    incarnation: u64,
    last_activation: u64,
    last_failure: Option<ComponentFailure>,
}

impl ComponentInstance {
    /// Create one live incarnation of a mounted component.
    ///
    /// # Errors
    ///
    /// Returns [`LifecycleError::InstanceIncarnationExhausted`] only if the
    /// process-wide incarnation counter is exhausted.
    pub fn new(
        id: impl Into<ComponentInstanceId>,
        definition: &ComponentDefinition,
        scope: &Scope,
    ) -> Result<Self, LifecycleError> {
        let incarnation = NEXT_INSTANCE_INCARNATION
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
                value.checked_add(1)
            })
            .map_err(|_| LifecycleError::InstanceIncarnationExhausted)?;
        Ok(Self {
            id: id.into(),
            definition_id: definition.id.clone(),
            scope_id: scope.id().clone(),
            scope: scope.clone(),
            state: ComponentState::Pending,
            incarnation,
            last_activation: 0,
            last_failure: None,
        })
    }

    pub fn id(&self) -> &ComponentInstanceId {
        &self.id
    }

    pub fn definition_id(&self) -> &ComponentId {
        &self.definition_id
    }

    pub fn scope_id(&self) -> &ScopeId {
        &self.scope_id
    }

    pub fn scope(&self) -> &Scope {
        &self.scope
    }

    pub fn state(&self) -> &ComponentState {
        &self.state
    }

    /// Return the most recent activation or runtime failure, including after
    /// teardown has returned the instance to a stable state.
    pub fn last_failure(&self) -> Option<&ComponentFailure> {
        self.last_failure.as_ref()
    }

    /// Begin a fresh activation attempt from `Pending`.
    pub fn begin_start(&mut self) -> Result<ActivationEpoch, LifecycleError> {
        self.state
            .require_action(crate::LifecycleAction::BeginStart)?;
        let next = self
            .last_activation
            .checked_add(1)
            .ok_or(LifecycleError::ActivationEpochExhausted)?;
        let activation = ActivationEpoch::new(self.incarnation, next);
        self.last_activation = next;
        self.state = ComponentState::Starting { activation };
        Ok(activation)
    }

    /// Publish a starting instance as active if the callback belongs to the
    /// current activation attempt.
    pub fn complete_start(&mut self, activation: ActivationEpoch) -> Result<(), LifecycleError> {
        self.state
            .require_activation(crate::LifecycleAction::CompleteStart, activation)?;
        match self.state {
            ComponentState::Starting { .. } => {
                self.state = ComponentState::Active { activation };
                Ok(())
            }
            _ => Err(self.state.invalid(crate::LifecycleAction::CompleteStart)),
        }
    }

    /// Record a valid activation or runtime failure.
    pub fn mark_failed(
        &mut self,
        activation: ActivationEpoch,
        summary: impl Into<String>,
    ) -> Result<(), LifecycleError> {
        self.state
            .require_activation(crate::LifecycleAction::MarkFailed, activation)?;
        let phase = match self.state {
            ComponentState::Starting { .. } => crate::FailurePhase::Starting,
            ComponentState::Active { .. } => crate::FailurePhase::Active,
            _ => return Err(self.state.invalid(crate::LifecycleAction::MarkFailed)),
        };
        let failure = ComponentFailure::new(phase, summary);
        self.last_failure = Some(failure.clone());
        self.state = ComponentState::Failed {
            activation,
            failure,
        };
        Ok(())
    }

    /// Close admission and enter controlled teardown.
    ///
    /// Starting, active, and failed instances all pass through `Stopping` so a
    /// later effect-scope layer has one place to perform rollback and cleanup.
    /// The caller must present the activation it intends to stop, which keeps a
    /// delayed stop request from affecting a replacement activation.
    pub fn begin_stop(
        &mut self,
        activation: ActivationEpoch,
        target: StopTarget,
    ) -> Result<(), LifecycleError> {
        self.state
            .require_activation(crate::LifecycleAction::BeginStop, activation)?;
        let prior_failure = match &self.state {
            ComponentState::Failed { failure, .. } => Some(failure.clone()),
            ComponentState::Starting { .. } | ComponentState::Active { .. } => None,
            _ => return Err(self.state.invalid(crate::LifecycleAction::BeginStop)),
        };
        self.state = ComponentState::Stopping {
            activation,
            target,
            prior_failure,
        };
        Ok(())
    }

    /// Finish teardown for the current activation and reach its requested
    /// stable target.
    ///
    /// This is bookkeeping after teardown succeeds; it does not run cleanup.
    /// The effect-scope layer will own cleanup execution and error aggregation.
    pub fn complete_stop(&mut self, activation: ActivationEpoch) -> Result<(), LifecycleError> {
        self.state
            .require_activation(crate::LifecycleAction::CompleteStop, activation)?;
        let target = match self.state {
            ComponentState::Stopping { target, .. } => target,
            _ => return Err(self.state.invalid(crate::LifecycleAction::CompleteStop)),
        };
        self.state = match target {
            StopTarget::Pending => ComponentState::Pending,
            StopTarget::Removed => ComponentState::Removed,
        };
        Ok(())
    }

    /// Remove an instance that never started or has already stopped.
    pub fn remove_pending(&mut self) -> Result<(), LifecycleError> {
        match self.state {
            ComponentState::Pending => {
                self.state = ComponentState::Removed;
                Ok(())
            }
            _ => Err(self.state.invalid(crate::LifecycleAction::RemovePending)),
        }
    }

    #[cfg(test)]
    pub(crate) fn set_last_activation_for_test(&mut self, value: u64) {
        self.last_activation = value;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn activation_epoch_overflow_rejects_without_mutation() {
        let definition = ComponentDefinition::new("test.component");
        let scope = Scope::root("root");
        let mut instance = ComponentInstance::new("instance", &definition, &scope).unwrap();
        instance.set_last_activation_for_test(u64::MAX);

        assert_eq!(
            instance.begin_start(),
            Err(LifecycleError::ActivationEpochExhausted)
        );
        assert_eq!(instance.state(), &ComponentState::Pending);
    }
}
