use std::{
    collections::BTreeMap,
    future::Future,
    sync::atomic::{AtomicU64, Ordering},
};

use crate::{
    CleanupResult, ComponentInstanceId, ComponentRevisionId, ComponentState, DesiredStopReason,
    EffectRegistrationId, ProviderCandidate, ProviderSelection, ProviderSelectionEpoch,
    ReconcileError, ReconcileOutcome, ReconciledComponent, ScopeCancellation, ServiceHandle,
    ServiceProvider, ServiceRegistry, ServiceRequirement, StopCompletion, StopTarget,
};

use super::{
    ComponentRevision, ComponentSlotError, ComponentSlotOutcome, DesiredComponentState,
    DesiredGeneration, StopDisposition, StopRecord,
};

static NEXT_SLOT_INCARNATION: AtomicU64 = AtomicU64::new(1);

/// Stable desired-state owner for at most one mounted component revision.
///
/// The slot synchronously seals any activation invalidated by an accepted
/// desired generation. It never exposes mutable access to the mounted
/// component or its effect scope. Dropping a slot requests cancellation but,
/// like [`crate::EffectScope`], cannot drive asynchronous cleanup.
#[must_use = "component slots must be reconciled and driven through controlled teardown"]
pub struct ComponentSlot {
    instance_id: ComponentInstanceId,
    incarnation: u64,
    desired: Option<DesiredComponentState>,
    known_revisions: BTreeMap<ComponentRevisionId, ComponentRevision>,
    mounted: Option<MountedRevision>,
    last_stop: Option<StopRecord>,
}

impl ComponentSlot {
    /// Create an empty process-unique desired component slot.
    ///
    /// # Errors
    ///
    /// Returns [`ComponentSlotError::SlotIncarnationExhausted`] only if the
    /// process-wide slot-incarnation counter is exhausted.
    pub fn new(instance_id: impl Into<ComponentInstanceId>) -> Result<Self, ComponentSlotError> {
        let incarnation = NEXT_SLOT_INCARNATION
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
                value.checked_add(1)
            })
            .map_err(|_| ComponentSlotError::SlotIncarnationExhausted)?;
        Ok(Self {
            instance_id: instance_id.into(),
            incarnation,
            desired: None,
            known_revisions: BTreeMap::new(),
            mounted: None,
            last_stop: None,
        })
    }

    pub fn instance_id(&self) -> &ComponentInstanceId {
        &self.instance_id
    }

    pub fn desired(&self) -> Option<&DesiredComponentState> {
        self.desired.as_ref()
    }

    /// Mint a caller-sequenced desired generation bound to this exact slot
    /// incarnation. A token from a dropped/recreated slot is rejected.
    pub const fn generation(&self, sequence: u64) -> DesiredGeneration {
        DesiredGeneration::new(self.incarnation, sequence)
    }

    pub fn desired_generation(&self) -> Option<DesiredGeneration> {
        self.desired.as_ref().map(DesiredComponentState::generation)
    }

    pub fn desired_revision(&self) -> Option<&ComponentRevisionId> {
        self.desired
            .as_ref()
            .and_then(DesiredComponentState::revision)
            .map(ComponentRevision::id)
    }

    pub fn applied_revision(&self) -> Option<&ComponentRevisionId> {
        self.mounted.as_ref().map(|mounted| &mounted.revision_id)
    }

    pub fn live_state(&self) -> Option<&ComponentState> {
        self.mounted
            .as_ref()
            .map(|mounted| mounted.component.state())
    }

    pub fn selection(&self) -> Option<&ProviderSelection> {
        self.mounted
            .as_ref()
            .and_then(|mounted| mounted.component.selection())
    }

    pub fn last_stop(&self) -> Option<&StopRecord> {
        self.last_stop.as_ref()
    }

    /// Accept and reconcile one desired snapshot in a single synchronous pass.
    ///
    /// Older generations and conflicting reuse do not mutate desired or live
    /// state. Repeating an identical generation still rechecks current
    /// provider availability. Accepted invalidation seals the current effect
    /// scope before this method returns. Errors detected before admission,
    /// including invalid same-revision assignments, leave the stored desired
    /// state unchanged. [`ComponentSlotOutcome::ConvergenceBlocked`] instead
    /// means the snapshot was accepted; desired content changes then need a
    /// newer generation, while an identical snapshot may recheck external
    /// convergence conditions.
    pub fn reconcile(
        &mut self,
        registry: &ServiceRegistry,
        desired: DesiredComponentState,
    ) -> Result<ComponentSlotOutcome, ComponentSlotError> {
        self.accept_desired(desired)?;
        let desired = self
            .desired
            .clone()
            .expect("successful desired admission stores a snapshot");
        let result = match desired {
            DesiredComponentState::Enabled {
                generation,
                revision,
                assignments,
            } => self.reconcile_enabled(registry, generation, &revision, &assignments),
            DesiredComponentState::Disabled {
                generation,
                revision,
            } => self.reconcile_disabled(generation, &revision),
            DesiredComponentState::Removed { generation } => self.reconcile_removed(generation),
        };
        match result {
            Ok(outcome) => Ok(outcome),
            Err(ComponentSlotError::Reconcile(error)) => {
                Ok(ComponentSlotOutcome::ConvergenceBlocked {
                    generation: self
                        .desired_generation()
                        .expect("reconciliation owns an accepted desired generation"),
                    desired_revision: self.desired_revision().cloned(),
                    applied_revision: self.applied_revision().cloned(),
                    error,
                })
            }
            Err(error) => Err(error),
        }
    }

    /// Complete the current start against the slot's latest exact assignment.
    pub fn complete_start(
        &mut self,
        selection_epoch: ProviderSelectionEpoch,
        registry: &ServiceRegistry,
    ) -> Result<ReconcileOutcome, ComponentSlotError> {
        let assignments = self.active_assignments()?.clone();
        Ok(self
            .mounted_component_mut()?
            .complete_start(selection_epoch, registry, &assignments)?)
    }

    pub fn bind<T: ?Sized + Send + Sync + 'static>(
        &self,
        selection_epoch: ProviderSelectionEpoch,
        registry: &ServiceRegistry,
        requirement: &ServiceRequirement<T>,
    ) -> Result<ServiceHandle<T>, ComponentSlotError> {
        self.require_active_revision()?;
        Ok(self
            .mounted_component()?
            .bind(selection_epoch, registry, requirement)?)
    }

    pub fn provide<T: ?Sized + Send + Sync + 'static>(
        &mut self,
        selection_epoch: ProviderSelectionEpoch,
        registry: &mut ServiceRegistry,
        provider: ServiceProvider<T>,
    ) -> Result<ProviderCandidate, ComponentSlotError> {
        self.require_active_revision()?;
        Ok(self
            .mounted_component_mut()?
            .provide(selection_epoch, registry, provider)?)
    }

    pub fn defer_sync<F>(
        &mut self,
        selection_epoch: ProviderSelectionEpoch,
        label: impl Into<String>,
        cleanup: F,
    ) -> Result<EffectRegistrationId, ComponentSlotError>
    where
        F: FnOnce() -> CleanupResult + Send + 'static,
    {
        self.require_active_revision()?;
        Ok(self
            .mounted_component_mut()?
            .defer_sync(selection_epoch, label, cleanup)?)
    }

    pub fn defer_async<F, Fut>(
        &mut self,
        selection_epoch: ProviderSelectionEpoch,
        label: impl Into<String>,
        cleanup: F,
    ) -> Result<EffectRegistrationId, ComponentSlotError>
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = CleanupResult> + Send + 'static,
    {
        self.require_active_revision()?;
        Ok(self
            .mounted_component_mut()?
            .defer_async(selection_epoch, label, cleanup)?)
    }

    pub fn cancellation(
        &self,
        selection_epoch: ProviderSelectionEpoch,
    ) -> Result<ScopeCancellation, ComponentSlotError> {
        self.require_active_revision()?;
        Ok(self.mounted_component()?.cancellation(selection_epoch)?)
    }

    /// Record an activation failure and synchronously seal its effects.
    ///
    /// Clean cleanup returns the mounted revision to pending; a non-clean
    /// report remains blocked in stopping. Retry remains an explicit later
    /// reconciliation pass against the latest desired state.
    pub fn fail_activation(
        &mut self,
        selection_epoch: ProviderSelectionEpoch,
        summary: impl Into<String>,
    ) -> Result<ReconcileOutcome, ComponentSlotError> {
        self.require_active_revision()?;
        Ok(self
            .mounted_component_mut()?
            .fail_activation(selection_epoch, summary)?)
    }

    /// Resume the current sealed scope in place and retain its terminal report.
    ///
    /// If this future is dropped, the mounted component remains in the slot and
    /// another call resumes the same cleanup. A clean `Removed` target drops the
    /// mounted revision only after the await completes.
    pub async fn finish_stop(
        &mut self,
        selection_epoch: ProviderSelectionEpoch,
    ) -> Result<StopRecord, ComponentSlotError> {
        let applied_revision = self
            .applied_revision()
            .cloned()
            .ok_or_else(|| self.no_mounted_error())?;
        let completion = self
            .mounted
            .as_mut()
            .expect("applied revision requires a mounted component")
            .component
            .finish_stop(selection_epoch)
            .await?;
        self.record_completion(applied_revision, completion)
    }

    /// Explicitly accept a terminal non-clean teardown without retrying it.
    ///
    /// This operation may permit later reconciliation to duplicate a resource
    /// whose cleanup reported failure. It is never automatic and requires the
    /// exact blocked selection epoch.
    pub fn abandon_failed_cleanup(
        &mut self,
        selection_epoch: ProviderSelectionEpoch,
    ) -> Result<StopRecord, ComponentSlotError> {
        let applied_revision = self
            .applied_revision()
            .cloned()
            .ok_or_else(|| self.no_mounted_error())?;
        let completion = self
            .mounted
            .as_mut()
            .expect("applied revision requires a mounted component")
            .component
            .abandon_failed_cleanup(selection_epoch)?;
        self.record_completion(applied_revision, completion)
    }

    fn accept_desired(&mut self, desired: DesiredComponentState) -> Result<(), ComponentSlotError> {
        if desired.generation().slot_incarnation() != self.incarnation {
            return Err(ComponentSlotError::ForeignDesiredGeneration {
                expected: self.generation(desired.generation().sequence()),
                received: desired.generation(),
            });
        }
        let changed = if let Some(current) = &self.desired {
            if desired.generation() < current.generation() {
                return Err(ComponentSlotError::StaleDesired {
                    current: current.generation(),
                    received: desired.generation(),
                });
            }
            if desired.generation() == current.generation() {
                if &desired == current {
                    false
                } else {
                    return Err(ComponentSlotError::DesiredGenerationConflict {
                        generation: desired.generation(),
                    });
                }
            } else {
                true
            }
        } else {
            true
        };

        if let Some(revision) = desired.revision()
            && let Some(known) = self.known_revisions.get(revision.id())
            && known != revision
        {
            return Err(ComponentSlotError::RevisionIdentityConflict {
                revision: revision.id().clone(),
            });
        }

        if changed
            && let DesiredComponentState::Enabled {
                revision,
                assignments,
                ..
            } = &desired
            && self
                .applied_revision()
                .is_none_or(|applied| applied == revision.id())
        {
            validate_assignment_keys(revision, assignments)?;
        }

        if !changed {
            return Ok(());
        }

        if let Some(revision) = desired.revision() {
            self.known_revisions
                .entry(revision.id().clone())
                .or_insert_with(|| revision.clone());
        }
        self.desired = Some(desired);
        Ok(())
    }

    fn reconcile_enabled(
        &mut self,
        registry: &ServiceRegistry,
        generation: DesiredGeneration,
        revision: &ComponentRevision,
        assignments: &crate::ProviderAssignments,
    ) -> Result<ComponentSlotOutcome, ComponentSlotError> {
        let Some(applied_revision) = self.applied_revision().cloned() else {
            let component = self.mount_and_reconcile(registry, revision, assignments)?;
            return Ok(ComponentSlotOutcome::Mounted {
                generation,
                applied_revision: revision.id().clone(),
                component,
            });
        };

        if applied_revision == *revision.id() {
            let component = self
                .mounted
                .as_mut()
                .expect("applied revision requires a mounted component")
                .component
                .reconcile(registry, assignments)?;
            return Ok(self.map_current(generation, applied_revision, component));
        }

        let reason = DesiredStopReason::RevisionChanged {
            previous: applied_revision.clone(),
            desired: revision.id().clone(),
        };
        let component = self
            .mounted
            .as_mut()
            .expect("applied revision requires a mounted component")
            .component
            .request_stop(reason.clone(), StopTarget::Removed)?;
        if matches!(component, ReconcileOutcome::Removed) {
            self.mounted = None;
            return Ok(ComponentSlotOutcome::Unmounted {
                generation,
                applied_revision,
                reason,
            });
        }
        Ok(self.map_stop(generation, applied_revision, component))
    }

    fn reconcile_disabled(
        &mut self,
        generation: DesiredGeneration,
        revision: &ComponentRevision,
    ) -> Result<ComponentSlotOutcome, ComponentSlotError> {
        let Some(applied_revision) = self.applied_revision().cloned() else {
            return Ok(ComponentSlotOutcome::Disabled {
                generation,
                desired_revision: revision.id().clone(),
            });
        };
        let component = self
            .mounted
            .as_mut()
            .expect("applied revision requires a mounted component")
            .component
            .request_stop(DesiredStopReason::Disabled, StopTarget::Removed)?;
        if matches!(component, ReconcileOutcome::Removed) {
            self.mounted = None;
            return Ok(ComponentSlotOutcome::Unmounted {
                generation,
                applied_revision,
                reason: DesiredStopReason::Disabled,
            });
        }
        Ok(self.map_stop(generation, applied_revision, component))
    }

    fn reconcile_removed(
        &mut self,
        generation: DesiredGeneration,
    ) -> Result<ComponentSlotOutcome, ComponentSlotError> {
        let Some(applied_revision) = self.applied_revision().cloned() else {
            return Ok(ComponentSlotOutcome::Removed { generation });
        };
        let component = self
            .mounted
            .as_mut()
            .expect("applied revision requires a mounted component")
            .component
            .request_stop(DesiredStopReason::Removed, StopTarget::Removed)?;
        if matches!(component, ReconcileOutcome::Removed) {
            self.mounted = None;
            return Ok(ComponentSlotOutcome::Unmounted {
                generation,
                applied_revision,
                reason: DesiredStopReason::Removed,
            });
        }
        Ok(self.map_stop(generation, applied_revision, component))
    }

    fn mount_and_reconcile(
        &mut self,
        registry: &ServiceRegistry,
        revision: &ComponentRevision,
        assignments: &crate::ProviderAssignments,
    ) -> Result<ReconcileOutcome, ComponentSlotError> {
        validate_assignment_keys(revision, assignments)?;
        let component = ReconciledComponent::mount(
            self.instance_id.clone(),
            revision.definition().clone(),
            revision.scope(),
        )?;
        self.mounted = Some(MountedRevision {
            revision_id: revision.id().clone(),
            component,
        });
        Ok(self
            .mounted
            .as_mut()
            .expect("mounted component was just stored")
            .component
            .reconcile(registry, assignments)?)
    }

    fn map_current(
        &self,
        generation: DesiredGeneration,
        applied_revision: ComponentRevisionId,
        component: ReconcileOutcome,
    ) -> ComponentSlotOutcome {
        match component {
            ReconcileOutcome::StopBegun {
                selection_epoch,
                target,
                reason,
            } => ComponentSlotOutcome::StopBegun {
                generation,
                applied_revision,
                selection_epoch,
                target,
                reason,
            },
            ReconcileOutcome::Stopping {
                selection_epoch,
                target,
                reason,
                cleanup_blocked,
            } => ComponentSlotOutcome::Stopping {
                generation,
                applied_revision,
                selection_epoch,
                target,
                reason,
                cleanup_blocked,
            },
            component => ComponentSlotOutcome::Reconciled {
                generation,
                applied_revision,
                component,
            },
        }
    }

    fn map_stop(
        &self,
        generation: DesiredGeneration,
        applied_revision: ComponentRevisionId,
        component: ReconcileOutcome,
    ) -> ComponentSlotOutcome {
        match component {
            ReconcileOutcome::StopBegun {
                selection_epoch,
                target,
                reason,
            } => ComponentSlotOutcome::StopBegun {
                generation,
                applied_revision,
                selection_epoch,
                target,
                reason,
            },
            ReconcileOutcome::Stopping {
                selection_epoch,
                target,
                reason,
                cleanup_blocked,
            } => ComponentSlotOutcome::Stopping {
                generation,
                applied_revision,
                selection_epoch,
                target,
                reason,
                cleanup_blocked,
            },
            _ => unreachable!("requested live stop returns stop or stopping"),
        }
    }

    fn record_completion(
        &mut self,
        applied_revision: ComponentRevisionId,
        completion: StopCompletion,
    ) -> Result<StopRecord, ComponentSlotError> {
        let (selection_epoch, target, reason, disposition, report) = match completion {
            StopCompletion::Completed {
                selection_epoch,
                target,
                reason,
                report,
            } => (
                selection_epoch,
                target,
                reason,
                StopDisposition::Completed,
                report,
            ),
            StopCompletion::Blocked {
                selection_epoch,
                target,
                reason,
                report,
            } => (
                selection_epoch,
                target,
                reason,
                StopDisposition::Blocked,
                report,
            ),
            StopCompletion::Abandoned {
                selection_epoch,
                target,
                reason,
                report,
            } => (
                selection_epoch,
                target,
                reason,
                StopDisposition::Abandoned,
                report,
            ),
        };
        let record = StopRecord::new(
            applied_revision,
            selection_epoch,
            target,
            reason,
            disposition,
            report,
        );
        self.last_stop = Some(record.clone());
        if target == StopTarget::Removed && disposition != StopDisposition::Blocked {
            self.mounted = None;
        }
        Ok(record)
    }

    fn active_assignments(&self) -> Result<&crate::ProviderAssignments, ComponentSlotError> {
        let mounted = self
            .mounted
            .as_ref()
            .ok_or_else(|| self.no_mounted_error())?;
        match self.desired.as_ref() {
            Some(DesiredComponentState::Enabled {
                revision,
                assignments,
                ..
            }) if revision.id() == &mounted.revision_id => Ok(assignments),
            desired => Err(ComponentSlotError::MountedRevisionNotEnabled {
                desired: desired
                    .and_then(DesiredComponentState::revision)
                    .map(|revision| revision.id().clone()),
                applied: mounted.revision_id.clone(),
            }),
        }
    }

    fn require_active_revision(&self) -> Result<(), ComponentSlotError> {
        self.active_assignments().map(|_| ())
    }

    fn mounted_component(&self) -> Result<&ReconciledComponent, ComponentSlotError> {
        self.mounted
            .as_ref()
            .map(|mounted| &mounted.component)
            .ok_or_else(|| self.no_mounted_error())
    }

    fn mounted_component_mut(&mut self) -> Result<&mut ReconciledComponent, ComponentSlotError> {
        let instance_id = self.instance_id.clone();
        self.mounted
            .as_mut()
            .map(|mounted| &mut mounted.component)
            .ok_or(ComponentSlotError::NoMountedComponent { instance_id })
    }

    fn no_mounted_error(&self) -> ComponentSlotError {
        ComponentSlotError::NoMountedComponent {
            instance_id: self.instance_id.clone(),
        }
    }
}

struct MountedRevision {
    revision_id: ComponentRevisionId,
    component: ReconciledComponent,
}

fn validate_assignment_keys(
    revision: &ComponentRevision,
    assignments: &crate::ProviderAssignments,
) -> Result<(), ComponentSlotError> {
    for (service_id, _) in assignments.iter() {
        if !revision
            .definition()
            .requirements()
            .iter()
            .any(|requirement| requirement.service_id() == service_id)
        {
            return Err(ReconcileError::UndeclaredAssignment {
                service_id: service_id.clone(),
            }
            .into());
        }
    }
    Ok(())
}
