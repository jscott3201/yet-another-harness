use std::future::Future;

use crate::{
    CleanupResult, CloseReport, ComponentDefinition, ComponentFailure, ComponentInstance,
    ComponentInstanceId, ComponentState, ComponentStateKind, EffectRegistrationId, EffectScope,
    ProviderCandidate, ProviderRegistrationId, RequirementCandidates, Scope, ScopeCancellation,
    ScopeId, ServiceHandle, ServiceProvider, ServiceRegistry, ServiceRequirement, StopTarget,
};

use super::{
    ComponentStopReason, DependencyIssue, DependencyReadiness, DependencyStopReason,
    DesiredStopReason, ProviderAssignments, ProviderChange, ProviderSelection,
    ProviderSelectionEpoch, ReconcileError, ReconcileOutcome, StopCompletion,
};

mod desired_stop;

/// Unique live owner of one mounted component's dependency lifecycle.
///
/// The component definition is consumed and frozen at mount. One activation's
/// provider assignments and effect scope stay together until teardown finishes;
/// callers receive no mutable access to either underlying lifecycle object.
/// Dropping this owner requests effect-scope cancellation but cannot drive
/// asynchronous cleanup; normal owners must reconcile and finish clean stops.
#[must_use = "reconciled components must be driven through controlled teardown"]
pub struct ReconciledComponent {
    definition: ComponentDefinition,
    instance: ComponentInstance,
    activation: Option<ActivationResources>,
    last_close_report: Option<CloseReport>,
}

impl ReconciledComponent {
    /// Mount a definition as one pending live component.
    pub fn mount(
        instance_id: impl Into<ComponentInstanceId>,
        definition: ComponentDefinition,
        scope: &Scope,
    ) -> Result<Self, ReconcileError> {
        let instance = ComponentInstance::new(instance_id, &definition, scope)?;
        Ok(Self {
            definition,
            instance,
            activation: None,
            last_close_report: None,
        })
    }

    pub fn definition(&self) -> &ComponentDefinition {
        &self.definition
    }

    pub fn instance_id(&self) -> &ComponentInstanceId {
        self.instance.id()
    }

    pub fn scope_id(&self) -> &ScopeId {
        self.instance.scope_id()
    }

    pub fn state(&self) -> &ComponentState {
        self.instance.state()
    }

    pub fn last_failure(&self) -> Option<&ComponentFailure> {
        self.instance.last_failure()
    }

    pub fn selection(&self) -> Option<&ProviderSelection> {
        self.activation
            .as_ref()
            .map(|resources| &resources.selection)
    }

    /// Most recent terminal effect-scope report, including a report that has
    /// blocked this component in `Stopping`.
    pub fn last_close_report(&self) -> Option<&CloseReport> {
        self.last_close_report.as_ref()
    }

    /// Snapshot all frozen requirements and visible provider candidates.
    pub fn inventory(
        &self,
        registry: &ServiceRegistry,
    ) -> Result<Vec<RequirementCandidates>, ReconcileError> {
        Ok(registry.inventory(&self.definition)?)
    }

    /// Validate a desired assignment against one current registry inventory
    /// without changing lifecycle state.
    pub fn readiness(
        &self,
        registry: &ServiceRegistry,
        desired: &ProviderAssignments,
    ) -> Result<DependencyReadiness, ReconcileError> {
        self.validate_assignment_keys(desired)?;
        Ok(match self.resolve_assignments(registry, desired)? {
            AssignmentResolution::Ready(_) => DependencyReadiness::Ready,
            AssignmentResolution::Pending(issues) => DependencyReadiness::Pending(issues),
        })
    }

    /// Reconcile this component toward one explicit exact provider assignment.
    ///
    /// This operation is level-triggered: the composition authority calls it
    /// after changing assignments or observing registry changes. It does not
    /// rank providers or install registry watches. Losing or changing a
    /// committed provider starts controlled teardown and synchronously seals
    /// the activation effect scope before this method returns.
    pub fn reconcile(
        &mut self,
        registry: &ServiceRegistry,
        desired: &ProviderAssignments,
    ) -> Result<ReconcileOutcome, ReconcileError> {
        match self.instance.state().clone() {
            ComponentState::Pending => {
                self.validate_assignment_keys(desired)?;
                self.reconcile_pending(registry, desired)
            }
            ComponentState::Starting { .. } => {
                self.validate_assignment_keys(desired)?;
                self.reconcile_live(registry, desired, ComponentStateKind::Starting)
            }
            ComponentState::Active { .. } => {
                self.validate_assignment_keys(desired)?;
                self.reconcile_live(registry, desired, ComponentStateKind::Active)
            }
            ComponentState::Failed { .. } => Err(ReconcileError::InvalidState {
                operation: "reconcile dependencies",
                state: ComponentStateKind::Failed,
            }),
            ComponentState::Stopping { target, .. } => {
                let resources = self
                    .activation
                    .as_ref()
                    .expect("reconciled stopping state owns activation resources");
                Ok(ReconcileOutcome::Stopping {
                    selection_epoch: resources.selection.epoch(),
                    target,
                    reason: resources
                        .stop_reason
                        .clone()
                        .expect("reconciled stopping state records a reason"),
                    cleanup_blocked: resources
                        .close_report
                        .as_ref()
                        .is_some_and(|report| !report.is_clean()),
                })
            }
            ComponentState::Removed => Ok(ReconcileOutcome::Removed),
        }
    }

    /// Bind a declared typed requirement to this starting activation's exact
    /// committed provider. The caller never supplies a provider ID directly.
    pub fn bind<T: ?Sized + Send + Sync + 'static>(
        &self,
        selection_epoch: ProviderSelectionEpoch,
        registry: &ServiceRegistry,
        requirement: &ServiceRequirement<T>,
    ) -> Result<ServiceHandle<T>, ReconcileError> {
        if self.instance.state().kind() != ComponentStateKind::Starting {
            return Err(ReconcileError::InvalidState {
                operation: "bind dependencies",
                state: self.instance.state().kind(),
            });
        }
        let resources = self.resources(selection_epoch)?;
        let declared = self
            .definition
            .requirements()
            .iter()
            .find(|declared| declared.service_id() == requirement.service_id())
            .ok_or_else(|| ReconcileError::UndeclaredRequirement {
                service_id: requirement.service_id().clone(),
            })?;
        if declared.contract() != requirement.contract() {
            return Err(ReconcileError::RequirementContractMismatch {
                service_id: requirement.service_id().clone(),
                expected: declared.contract_name(),
                received: requirement.contract().name(),
            });
        }
        let provider_id = resources
            .selection
            .assignments()
            .provider_for(requirement.service_id())
            .expect("a committed selection covers every frozen requirement");
        Ok(registry.bind(&self.instance, &resources.effects, requirement, provider_id)?)
    }

    /// Publish a service from an active reconciled activation while retaining
    /// withdrawal ownership in that activation's effect scope.
    pub fn provide<T: ?Sized + Send + Sync + 'static>(
        &mut self,
        selection_epoch: ProviderSelectionEpoch,
        registry: &mut ServiceRegistry,
        provider: ServiceProvider<T>,
    ) -> Result<ProviderCandidate, ReconcileError> {
        if self.instance.state().kind() != ComponentStateKind::Active {
            return Err(ReconcileError::InvalidState {
                operation: "publish a service",
                state: self.instance.state().kind(),
            });
        }
        self.require_epoch(selection_epoch)?;
        let resources = self
            .activation
            .as_mut()
            .expect("an active reconciled component owns activation resources");
        Ok(registry.provide(&self.instance, &mut resources.effects, provider)?)
    }

    /// Admit one synchronous activation cleanup without exposing the owned
    /// effect scope itself.
    pub fn defer_sync<F>(
        &mut self,
        selection_epoch: ProviderSelectionEpoch,
        label: impl Into<String>,
        cleanup: F,
    ) -> Result<EffectRegistrationId, ReconcileError>
    where
        F: FnOnce() -> CleanupResult + Send + 'static,
    {
        self.require_effect_admission(selection_epoch)?;
        let resources = self
            .activation
            .as_mut()
            .expect("a live reconciled activation owns effects");
        Ok(resources.effects.defer_sync(label, cleanup)?)
    }

    /// Admit one asynchronous activation cleanup without exposing the owned
    /// effect scope itself.
    pub fn defer_async<F, Fut>(
        &mut self,
        selection_epoch: ProviderSelectionEpoch,
        label: impl Into<String>,
        cleanup: F,
    ) -> Result<EffectRegistrationId, ReconcileError>
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = CleanupResult> + Send + 'static,
    {
        self.require_effect_admission(selection_epoch)?;
        let resources = self
            .activation
            .as_mut()
            .expect("a live reconciled activation owns effects");
        Ok(resources.effects.defer_async(label, cleanup)?)
    }

    /// Observe cancellation for this exact activation without receiving
    /// cancellation authority.
    pub fn cancellation(
        &self,
        selection_epoch: ProviderSelectionEpoch,
    ) -> Result<ScopeCancellation, ReconcileError> {
        Ok(self.resources(selection_epoch)?.effects.cancellation())
    }

    /// Publish a starting activation only if its frozen assignment is still
    /// desired and every exact provider remains available.
    ///
    /// An invalidated start enters controlled teardown and returns
    /// [`ReconcileOutcome::StopBegun`] instead of becoming active.
    pub fn complete_start(
        &mut self,
        selection_epoch: ProviderSelectionEpoch,
        registry: &ServiceRegistry,
        desired: &ProviderAssignments,
    ) -> Result<ReconcileOutcome, ReconcileError> {
        self.validate_assignment_keys(desired)?;
        if self.instance.state().kind() != ComponentStateKind::Starting {
            return Err(ReconcileError::InvalidState {
                operation: "complete start",
                state: self.instance.state().kind(),
            });
        }
        self.require_epoch(selection_epoch)?;
        let committed = self
            .selection()
            .expect("a starting reconciled component owns a selection")
            .assignments()
            .clone();
        if desired != &committed {
            let reason = ComponentStopReason::Dependency(DependencyStopReason::AssignmentChanged(
                self.assignment_changes(&committed, desired),
            ));
            return self.begin_stop(reason, StopTarget::Pending);
        }

        match self.resolve_assignments(registry, &committed)? {
            AssignmentResolution::Ready(_) => {
                self.instance.complete_start(selection_epoch.activation())?;
                Ok(ReconcileOutcome::Active { selection_epoch })
            }
            AssignmentResolution::Pending(issues) => self.begin_stop(
                ComponentStopReason::Dependency(DependencyStopReason::ProviderUnavailable(issues)),
                StopTarget::Pending,
            ),
        }
    }

    /// Drive the already-sealed activation scope to a terminal close report.
    ///
    /// Dropping a pending future leaves the same owned scope in place; another
    /// call resumes it. A non-clean report remains blocked in `Stopping` so a
    /// later activation cannot duplicate an effect whose cleanup failed.
    pub async fn finish_stop(
        &mut self,
        selection_epoch: ProviderSelectionEpoch,
    ) -> Result<StopCompletion, ReconcileError> {
        if self.instance.state().kind() != ComponentStateKind::Stopping {
            return Err(ReconcileError::InvalidState {
                operation: "finish stop",
                state: self.instance.state().kind(),
            });
        }
        self.require_epoch(selection_epoch)?;
        let target = match self.instance.state() {
            ComponentState::Stopping { target, .. } => *target,
            _ => unreachable!("finish_stop validates the stopping state"),
        };
        let reason = self
            .activation
            .as_ref()
            .and_then(|resources| resources.stop_reason.clone())
            .expect("a stopping reconciled component records its reason");
        let report = {
            let resources = self
                .activation
                .as_mut()
                .expect("a stopping reconciled component owns activation resources");
            resources.effects.close().await
        };
        self.last_close_report = Some(report.clone());
        let resources = self
            .activation
            .as_mut()
            .expect("a stopping reconciled component owns activation resources");
        resources.close_report = Some(report.clone());

        if !report.is_clean() {
            return Ok(StopCompletion::Blocked {
                selection_epoch,
                target,
                reason,
                report,
            });
        }

        self.instance.complete_stop(selection_epoch.activation())?;
        self.activation = None;
        Ok(StopCompletion::Completed {
            selection_epoch,
            target,
            reason,
            report,
        })
    }

    fn reconcile_pending(
        &mut self,
        registry: &ServiceRegistry,
        desired: &ProviderAssignments,
    ) -> Result<ReconcileOutcome, ReconcileError> {
        debug_assert!(self.activation.is_none());
        match self.resolve_assignments(registry, desired)? {
            AssignmentResolution::Pending(issues) => Ok(ReconcileOutcome::Pending {
                readiness: DependencyReadiness::Pending(issues),
            }),
            AssignmentResolution::Ready(providers) => {
                let activation = self.instance.begin_start()?;
                let effects = match EffectScope::new(
                    format!("{} activation {activation}", self.instance.id()),
                    activation,
                ) {
                    Ok(effects) => effects,
                    Err(error) => {
                        self.instance.begin_stop(activation, StopTarget::Pending)?;
                        self.instance.complete_stop(activation)?;
                        return Err(error.into());
                    }
                };
                let selection = ProviderSelection::new(
                    self.instance.id().clone(),
                    activation,
                    desired.clone(),
                    providers,
                );
                self.activation = Some(ActivationResources {
                    selection: selection.clone(),
                    effects,
                    stop_reason: None,
                    close_report: None,
                });
                Ok(ReconcileOutcome::StartBegun { selection })
            }
        }
    }

    fn reconcile_live(
        &mut self,
        registry: &ServiceRegistry,
        desired: &ProviderAssignments,
        state: ComponentStateKind,
    ) -> Result<ReconcileOutcome, ReconcileError> {
        let resources = self
            .activation
            .as_ref()
            .expect("a live reconciled component owns activation resources");
        let selection_epoch = resources.selection.epoch();
        let committed = resources.selection.assignments().clone();
        if desired != &committed {
            let reason = ComponentStopReason::Dependency(DependencyStopReason::AssignmentChanged(
                self.assignment_changes(&committed, desired),
            ));
            return self.begin_stop(reason, StopTarget::Pending);
        }

        match self.resolve_assignments(registry, &committed)? {
            AssignmentResolution::Pending(issues) => self.begin_stop(
                ComponentStopReason::Dependency(DependencyStopReason::ProviderUnavailable(issues)),
                StopTarget::Pending,
            ),
            AssignmentResolution::Ready(_) => match state {
                ComponentStateKind::Starting => {
                    Ok(ReconcileOutcome::AwaitingStart { selection_epoch })
                }
                ComponentStateKind::Active => Ok(ReconcileOutcome::Active { selection_epoch }),
                _ => unreachable!("only starting and active states call reconcile_live"),
            },
        }
    }

    fn begin_stop(
        &mut self,
        reason: ComponentStopReason,
        target: StopTarget,
    ) -> Result<ReconcileOutcome, ReconcileError> {
        let selection_epoch = self
            .activation
            .as_ref()
            .expect("reconciled stop requires activation resources")
            .selection
            .epoch();
        self.instance
            .begin_stop(selection_epoch.activation(), target)?;
        let resources = self
            .activation
            .as_mut()
            .expect("reconciled stop retains activation resources");
        resources.stop_reason = Some(reason.clone());
        drop(resources.effects.close());
        Ok(ReconcileOutcome::StopBegun {
            selection_epoch,
            target,
            reason,
        })
    }

    fn resolve_assignments(
        &self,
        registry: &ServiceRegistry,
        assignments: &ProviderAssignments,
    ) -> Result<AssignmentResolution, ReconcileError> {
        let inventory = registry.inventory(&self.definition)?;
        let mut providers = Vec::with_capacity(inventory.len());
        let mut issues = Vec::new();
        for group in inventory {
            let service_id = group.requirement().service_id().clone();
            let candidates = group.candidates();
            match assignments.provider_for(&service_id) {
                Some(assigned) => {
                    if let Some(candidate) = candidates
                        .iter()
                        .find(|candidate| candidate.id() == assigned)
                    {
                        providers.push(candidate.clone());
                    } else {
                        issues.push(DependencyIssue::AssignedProviderUnavailable {
                            service_id,
                            assigned,
                            available: candidate_ids(candidates),
                        });
                    }
                }
                None => match candidates {
                    [] => issues.push(DependencyIssue::MissingProvider { service_id }),
                    [candidate] => issues.push(DependencyIssue::Unassigned {
                        service_id,
                        candidate: candidate.id(),
                    }),
                    _ => issues.push(DependencyIssue::Ambiguous {
                        service_id,
                        candidates: candidate_ids(candidates),
                    }),
                },
            }
        }
        if issues.is_empty() {
            Ok(AssignmentResolution::Ready(providers))
        } else {
            Ok(AssignmentResolution::Pending(issues))
        }
    }

    fn validate_assignment_keys(
        &self,
        assignments: &ProviderAssignments,
    ) -> Result<(), ReconcileError> {
        for (service_id, _) in assignments.iter() {
            if !self
                .definition
                .requirements()
                .iter()
                .any(|requirement| requirement.service_id() == service_id)
            {
                return Err(ReconcileError::UndeclaredAssignment {
                    service_id: service_id.clone(),
                });
            }
        }
        Ok(())
    }

    fn assignment_changes(
        &self,
        previous: &ProviderAssignments,
        next: &ProviderAssignments,
    ) -> Vec<ProviderChange> {
        self.definition
            .requirements()
            .iter()
            .filter_map(|requirement| {
                let service_id = requirement.service_id();
                let previous = previous.provider_for(service_id);
                let next = next.provider_for(service_id);
                (previous != next).then(|| ProviderChange::new(service_id.clone(), previous, next))
            })
            .collect()
    }

    fn resources(
        &self,
        received: ProviderSelectionEpoch,
    ) -> Result<&ActivationResources, ReconcileError> {
        self.require_epoch(received)?;
        Ok(self
            .activation
            .as_ref()
            .expect("selection epoch validation requires activation resources"))
    }

    fn require_epoch(&self, received: ProviderSelectionEpoch) -> Result<(), ReconcileError> {
        let Some(resources) = &self.activation else {
            return Err(ReconcileError::NoCurrentSelection { received });
        };
        let expected = resources.selection.epoch();
        if received != expected {
            return Err(ReconcileError::StaleSelection { expected, received });
        }
        Ok(())
    }

    fn require_effect_admission(
        &self,
        received: ProviderSelectionEpoch,
    ) -> Result<(), ReconcileError> {
        match self.instance.state().kind() {
            ComponentStateKind::Starting | ComponentStateKind::Active => {
                self.require_epoch(received)
            }
            state => Err(ReconcileError::InvalidState {
                operation: "register an activation effect",
                state,
            }),
        }
    }
}

struct ActivationResources {
    selection: ProviderSelection,
    effects: EffectScope,
    stop_reason: Option<ComponentStopReason>,
    close_report: Option<CloseReport>,
}

enum AssignmentResolution {
    Ready(Vec<ProviderCandidate>),
    Pending(Vec<DependencyIssue>),
}

fn candidate_ids(candidates: &[ProviderCandidate]) -> Vec<ProviderRegistrationId> {
    candidates.iter().map(ProviderCandidate::id).collect()
}
