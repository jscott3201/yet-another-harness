use super::*;

impl ReconciledComponent {
    /// Request a desired-state stop without exposing the owned lifecycle.
    pub(crate) fn request_stop(
        &mut self,
        reason: DesiredStopReason,
        target: StopTarget,
    ) -> Result<ReconcileOutcome, ReconcileError> {
        match self.instance.state().clone() {
            ComponentState::Pending => {
                if target != StopTarget::Removed {
                    return Err(ReconcileError::InvalidState {
                        operation: "stop a pending component without removing it",
                        state: ComponentStateKind::Pending,
                    });
                }
                self.instance.remove_pending()?;
                Ok(ReconcileOutcome::Removed)
            }
            ComponentState::Starting { .. }
            | ComponentState::Active { .. }
            | ComponentState::Failed { .. } => {
                self.begin_stop(ComponentStopReason::Desired(reason), target)
            }
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

    /// Explicitly advance a terminal non-clean stop without retrying cleanup.
    ///
    /// This is crate-private so only the desired-state authority can expose the
    /// risk as an observable policy decision. Cleanup callbacks are `FnOnce`;
    /// abandoning may leave an external resource live and never runs them
    /// again.
    pub(crate) fn abandon_failed_cleanup(
        &mut self,
        selection_epoch: ProviderSelectionEpoch,
    ) -> Result<StopCompletion, ReconcileError> {
        if self.instance.state().kind() != ComponentStateKind::Stopping {
            return Err(ReconcileError::InvalidState {
                operation: "abandon failed cleanup",
                state: self.instance.state().kind(),
            });
        }
        self.require_epoch(selection_epoch)?;
        let target = match self.instance.state() {
            ComponentState::Stopping { target, .. } => *target,
            _ => unreachable!("cleanup abandonment validates the stopping state"),
        };
        let report = self
            .activation
            .as_ref()
            .and_then(|resources| resources.close_report.clone())
            .filter(|report| !report.is_clean())
            .ok_or(ReconcileError::CleanupNotBlocked)?;
        let reason = self
            .activation
            .as_ref()
            .and_then(|resources| resources.stop_reason.clone())
            .expect("a blocked reconciled component records its stop reason");

        self.instance.complete_stop(selection_epoch.activation())?;
        self.activation = None;
        Ok(StopCompletion::Abandoned {
            selection_epoch,
            target,
            reason,
            report,
        })
    }
}
