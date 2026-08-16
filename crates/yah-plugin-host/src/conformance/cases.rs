use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll, Waker},
};

use yah_compose::{ComponentSlotOutcome, DesiredComponentState, StopDisposition, StopTarget};

use crate::{
    CapabilityBroker, DriverActivationErrorKind, PluginActivationId, PluginHealth,
    PluginHealthError, PluginStartError,
};

use super::{
    DriverActivationObservation, DriverConformanceCase, DriverConformanceCaseReport,
    DriverConformanceFailure, DriverConformancePhase, DriverConformanceProbe,
    DriverConformanceResourceState, DriverConformanceSubject, DriverConformanceTarget,
    DriverConformanceTeardown, DriverConformanceTerminal,
    observe::{ObservationHandle, observe_driver},
};

mod deactivation_failure;
mod isolation;
mod rig;
mod teardown;

pub(super) async fn run(
    target: &dyn DriverConformanceTarget,
    case: DriverConformanceCase,
) -> Result<DriverConformanceCaseReport, DriverConformanceFailure> {
    let rig = CaseRig::new(target, case)?;
    match case {
        DriverConformanceCase::ReadyLifecycle => ready_lifecycle(rig).await,
        DriverConformanceCase::PendingStartCancellation => pending_start_cancellation(rig).await,
        DriverConformanceCase::ReturnedStartFailure => returned_start_failure(rig).await,
        DriverConformanceCase::ReturnedDeactivationFailure => deactivation_failure::run(rig).await,
        DriverConformanceCase::SharedDriverIsolation => isolation::run(rig).await,
    }
}

use rig::CaseRig;

async fn ready_lifecycle(
    rig: CaseRig,
) -> Result<DriverConformanceCaseReport, DriverConformanceFailure> {
    let (registry, mut slot, epoch) = rig.begin_component("ready")?;
    let broker = CapabilityBroker::new().map_err(|error| {
        rig.failure(
            DriverConformancePhase::Setup,
            format!("capability broker could not be created: {error}"),
            &[],
            DriverConformanceTeardown::NotStarted,
        )
    })?;
    let mut activation = rig.prepare(&mut slot, epoch, &broker)?;
    let id = activation.id().clone();
    let prepared = match rig.observation(&id, DriverConformanceTeardown::NotStarted) {
        Ok(observation) => observation,
        Err(primary) => {
            return Err(teardown::activation_failure(
                &rig,
                &mut activation,
                primary,
                std::slice::from_ref(&id),
            )
            .await);
        }
    };
    if let Err(primary) = require(
        prepared.start_factory_calls() == 0
            && prepared.resource_state() == Some(DriverConformanceResourceState::NotAcquired),
        &rig,
        DriverConformancePhase::Preparation,
        "inert preparation constructed start or acquired the fixture resource",
        std::slice::from_ref(&id),
        DriverConformanceTeardown::NotStarted,
    ) {
        return Err(teardown::activation_failure(
            &rig,
            &mut activation,
            primary,
            std::slice::from_ref(&id),
        )
        .await);
    }

    let handle = match activation.activate(&registry).await {
        Ok(handle) => handle,
        Err(error) => {
            let primary = rig.failure(
                DriverConformancePhase::Start,
                format!("ready fixture did not activate: {error}"),
                std::slice::from_ref(&id),
                DriverConformanceTeardown::NotStarted,
            );
            return Err(teardown::activation_failure(
                &rig,
                &mut activation,
                primary,
                std::slice::from_ref(&id),
            )
            .await);
        }
    };
    if let Err(primary) = require(
        matches!(handle.health(), Ok(PluginHealth::Healthy)),
        &rig,
        DriverConformancePhase::Health,
        "active fixture did not report healthy",
        std::slice::from_ref(&id),
        DriverConformanceTeardown::NotStarted,
    ) {
        return Err(teardown::activation_failure(
            &rig,
            &mut activation,
            primary,
            std::slice::from_ref(&id),
        )
        .await);
    }
    let (slot, _) = match activation.release_active() {
        Ok(released) => released,
        Err(error) => {
            let primary = rig.failure(
                DriverConformancePhase::Start,
                format!("active fixture could not release host ownership: {error}"),
                std::slice::from_ref(&id),
                DriverConformanceTeardown::NotStarted,
            );
            return Err(teardown::slot_failure(
                &rig,
                &mut slot,
                epoch,
                primary,
                std::slice::from_ref(&id),
            )
            .await);
        }
    };
    let checkpoint = rig.observations.clone();
    if let Err(error) = slot.defer_sync(epoch, "conformance later cleanup", move || {
        checkpoint.mark_later_cleanup();
        Ok(())
    }) {
        let primary = rig.failure(
            DriverConformancePhase::Teardown,
            format!("later cleanup checkpoint could not be registered: {error}"),
            std::slice::from_ref(&id),
            DriverConformanceTeardown::NotStarted,
        );
        return Err(
            teardown::slot_failure(&rig, slot, epoch, primary, std::slice::from_ref(&id)).await,
        );
    }
    let stop = match slot.reconcile(
        &registry,
        DesiredComponentState::removed(slot.generation(2)),
    ) {
        Ok(stop) => stop,
        Err(error) => {
            let primary = rig.failure(
                DriverConformancePhase::Cancellation,
                format!("active fixture could not begin removal: {error}"),
                std::slice::from_ref(&id),
                DriverConformanceTeardown::NotStarted,
            );
            return Err(teardown::slot_failure(
                &rig,
                slot,
                epoch,
                primary,
                std::slice::from_ref(&id),
            )
            .await);
        }
    };
    if let Err(primary) = require(
        matches!(stop, ComponentSlotOutcome::StopBegun { .. })
            && matches!(handle.health(), Err(PluginHealthError::Inactive { .. })),
        &rig,
        DriverConformancePhase::Cancellation,
        "removal did not synchronously fence the active health handle",
        std::slice::from_ref(&id),
        DriverConformanceTeardown::NotStarted,
    ) {
        return Err(
            teardown::slot_failure(&rig, slot, epoch, primary, std::slice::from_ref(&id)).await,
        );
    }
    let record = match slot.finish_stop(epoch).await {
        Ok(record) => record,
        Err(error) => {
            let primary = rig.failure(
                DriverConformancePhase::Teardown,
                format!("clean deactivation could not finish: {error}"),
                std::slice::from_ref(&id),
                DriverConformanceTeardown::NotStarted,
            );
            return Err(teardown::slot_failure(
                &rig,
                slot,
                epoch,
                primary,
                std::slice::from_ref(&id),
            )
            .await);
        }
    };
    let observation = match rig.observation(&id, DriverConformanceTeardown::Clean) {
        Ok(observation) => observation,
        Err(primary) => {
            return Err(teardown::slot_failure(
                &rig,
                slot,
                epoch,
                primary,
                std::slice::from_ref(&id),
            )
            .await);
        }
    };
    if let Err(primary) = require(
        record.disposition() == StopDisposition::Completed
            && record.report().is_clean()
            && observation.prepare_calls() == 1
            && observation.prepare_succeeded()
            && observation.start_factory_calls() == 1
            && observation.start_terminal() == Some(DriverConformanceTerminal::Succeeded)
            && observation.start_permit_matched() == Some(true)
            && observation.health_calls() >= 1
            && observation.deactivation_factory_calls() == 1
            && observation.deactivation_terminal() == Some(DriverConformanceTerminal::Succeeded)
            && observation.deactivation_permit_matched() == Some(true)
            && observation.deactivation_saw_cancellation()
            && observation.deactivation_saw_later_cleanup()
            && observation.resource_state() == Some(DriverConformanceResourceState::Released),
        &rig,
        DriverConformancePhase::Teardown,
        "ready lifecycle evidence did not match the exact clean contract",
        std::slice::from_ref(&id),
        DriverConformanceTeardown::Clean,
    ) {
        return Err(
            teardown::slot_failure(&rig, slot, epoch, primary, std::slice::from_ref(&id)).await,
        );
    }
    Ok(rig.report(vec![observation], DriverConformanceTeardown::Clean))
}

async fn pending_start_cancellation(
    rig: CaseRig,
) -> Result<DriverConformanceCaseReport, DriverConformanceFailure> {
    let (registry, mut slot, epoch) = rig.begin_component("pending")?;
    let broker = broker(&rig)?;
    let removed = DesiredComponentState::removed(slot.generation(2));
    let mut activation = rig.prepare(&mut slot, epoch, &broker)?;
    let id = activation.id().clone();
    let mut first = Box::pin(activation.activate(&registry));
    let first_poll = poll_once(first.as_mut());
    drop(first);
    if let Err(primary) = require(
        first_poll.is_pending(),
        &rig,
        DriverConformancePhase::Start,
        "pending-start fixture completed on its first poll",
        std::slice::from_ref(&id),
        DriverConformanceTeardown::NotStarted,
    ) {
        return Err(teardown::activation_failure(
            &rig,
            &mut activation,
            primary,
            std::slice::from_ref(&id),
        )
        .await);
    }
    let before_cancel = match rig.observation(&id, DriverConformanceTeardown::NotStarted) {
        Ok(observation) => observation,
        Err(primary) => {
            return Err(teardown::activation_failure(
                &rig,
                &mut activation,
                primary,
                std::slice::from_ref(&id),
            )
            .await);
        }
    };
    if let Err(primary) = require(
        before_cancel.start_factory_calls() == 1
            && before_cancel.start_pending_polls() == 1
            && before_cancel.start_drops() == 0
            && before_cancel.resource_state() == Some(DriverConformanceResourceState::Live),
        &rig,
        DriverConformancePhase::Start,
        "dropping the start waiter did not retain the same pending operation",
        std::slice::from_ref(&id),
        DriverConformanceTeardown::NotStarted,
    ) {
        return Err(teardown::activation_failure(
            &rig,
            &mut activation,
            primary,
            std::slice::from_ref(&id),
        )
        .await);
    }
    let mut resumed = Box::pin(activation.activate(&registry));
    let resumed_poll = poll_once(resumed.as_mut());
    drop(resumed);
    if let Err(primary) = require(
        resumed_poll.is_pending(),
        &rig,
        DriverConformancePhase::Start,
        "resumed pending start unexpectedly completed",
        std::slice::from_ref(&id),
        DriverConformanceTeardown::NotStarted,
    ) {
        return Err(teardown::activation_failure(
            &rig,
            &mut activation,
            primary,
            std::slice::from_ref(&id),
        )
        .await);
    }
    if let Err(error) = activation.reconcile(&registry, removed) {
        let primary = rig.failure(
            DriverConformancePhase::Cancellation,
            format!("pending activation could not begin removal: {error}"),
            std::slice::from_ref(&id),
            DriverConformanceTeardown::NotStarted,
        );
        return Err(teardown::activation_failure(
            &rig,
            &mut activation,
            primary,
            std::slice::from_ref(&id),
        )
        .await);
    }
    if let Err(primary) = require(
        activation.cancellation().is_cancelled(),
        &rig,
        DriverConformancePhase::Cancellation,
        "pending activation was not synchronously cancelled",
        std::slice::from_ref(&id),
        DriverConformanceTeardown::NotStarted,
    ) {
        return Err(teardown::activation_failure(
            &rig,
            &mut activation,
            primary,
            std::slice::from_ref(&id),
        )
        .await);
    }
    let record = match activation.finish_stop().await {
        Ok(record) => record,
        Err(error) => {
            let primary = rig.failure(
                DriverConformancePhase::Teardown,
                format!("pending activation cleanup could not finish: {error}"),
                std::slice::from_ref(&id),
                DriverConformanceTeardown::NotStarted,
            );
            return Err(teardown::activation_failure(
                &rig,
                &mut activation,
                primary,
                std::slice::from_ref(&id),
            )
            .await);
        }
    };
    let observation = match rig.observation(&id, DriverConformanceTeardown::Clean) {
        Ok(observation) => observation,
        Err(primary) => {
            return Err(teardown::activation_record_failure(
                &rig,
                &mut activation,
                record,
                primary,
                std::slice::from_ref(&id),
            ));
        }
    };
    if let Err(primary) = require(
        record.report().is_clean()
            && observation.start_factory_calls() == 1
            && observation.start_pending_polls() >= 2
            && observation.start_terminal().is_none()
            && observation.start_drops() == 1
            && observation.start_drop_saw_cancellation()
            && observation.deactivation_factory_calls() == 1
            && observation.deactivation_saw_cancellation()
            && observation.deactivation_terminal() == Some(DriverConformanceTerminal::Succeeded)
            && observation.resource_state() == Some(DriverConformanceResourceState::Released),
        &rig,
        DriverConformancePhase::Teardown,
        "pending cancellation evidence did not match the exact cleanup contract",
        std::slice::from_ref(&id),
        DriverConformanceTeardown::Clean,
    ) {
        return Err(teardown::activation_record_failure(
            &rig,
            &mut activation,
            record,
            primary,
            std::slice::from_ref(&id),
        ));
    }
    Ok(rig.report(vec![observation], DriverConformanceTeardown::Clean))
}

async fn returned_start_failure(
    rig: CaseRig,
) -> Result<DriverConformanceCaseReport, DriverConformanceFailure> {
    let (registry, mut slot, epoch) = rig.begin_component("start-failure")?;
    let broker = broker(&rig)?;
    let mut activation = rig.prepare(&mut slot, epoch, &broker)?;
    let id = activation.id().clone();
    let failure = activation.activate(&registry).await;
    if let Err(primary) = require(
        matches!(
            failure,
            Err(PluginStartError::Driver { ref failure, .. })
                if failure.kind() == DriverActivationErrorKind::Failed
        ) && activation.cancellation().is_cancelled(),
        &rig,
        DriverConformancePhase::Start,
        "returned start failure did not seal the exact activation",
        std::slice::from_ref(&id),
        DriverConformanceTeardown::NotStarted,
    ) {
        return Err(teardown::activation_failure(
            &rig,
            &mut activation,
            primary,
            std::slice::from_ref(&id),
        )
        .await);
    }
    let before_cleanup = match rig.observation(&id, DriverConformanceTeardown::NotStarted) {
        Ok(observation) => observation,
        Err(primary) => {
            return Err(teardown::activation_failure(
                &rig,
                &mut activation,
                primary,
                std::slice::from_ref(&id),
            )
            .await);
        }
    };
    if let Err(primary) = require(
        before_cleanup.resource_state() == Some(DriverConformanceResourceState::Live),
        &rig,
        DriverConformancePhase::Start,
        "failed start did not retain its partial resource for deactivation",
        std::slice::from_ref(&id),
        DriverConformanceTeardown::NotStarted,
    ) {
        return Err(teardown::activation_failure(
            &rig,
            &mut activation,
            primary,
            std::slice::from_ref(&id),
        )
        .await);
    }
    let record = match activation.finish_stop().await {
        Ok(record) => record,
        Err(error) => {
            let primary = rig.failure(
                DriverConformancePhase::Teardown,
                format!("start-failure rollback could not finish: {error}"),
                std::slice::from_ref(&id),
                DriverConformanceTeardown::NotStarted,
            );
            return Err(teardown::activation_failure(
                &rig,
                &mut activation,
                primary,
                std::slice::from_ref(&id),
            )
            .await);
        }
    };
    let observation = match rig.observation(&id, DriverConformanceTeardown::Clean) {
        Ok(observation) => observation,
        Err(primary) => {
            return Err(teardown::activation_record_failure(
                &rig,
                &mut activation,
                record,
                primary,
                std::slice::from_ref(&id),
            ));
        }
    };
    if let Err(primary) = require(
        record.disposition() == StopDisposition::Completed
            && record.target() == StopTarget::Pending
            && record.report().is_clean()
            && observation.start_terminal() == Some(DriverConformanceTerminal::ReturnedError)
            && observation.start_drop_saw_cancellation()
            && observation.deactivation_factory_calls() == 1
            && observation.deactivation_saw_cancellation()
            && observation.resource_state() == Some(DriverConformanceResourceState::Released),
        &rig,
        DriverConformancePhase::Teardown,
        "start-failure rollback evidence was incomplete",
        std::slice::from_ref(&id),
        DriverConformanceTeardown::Clean,
    ) {
        return Err(teardown::activation_record_failure(
            &rig,
            &mut activation,
            record,
            primary,
            std::slice::from_ref(&id),
        ));
    }
    Ok(rig.report(vec![observation], DriverConformanceTeardown::Clean))
}

fn broker(rig: &CaseRig) -> Result<CapabilityBroker, DriverConformanceFailure> {
    CapabilityBroker::new().map_err(|error| {
        rig.failure(
            DriverConformancePhase::Setup,
            format!("capability broker could not be created: {error}"),
            &[],
            DriverConformanceTeardown::NotStarted,
        )
    })
}

fn require(
    condition: bool,
    rig: &CaseRig,
    phase: DriverConformancePhase,
    summary: impl Into<String>,
    activations: &[PluginActivationId],
    teardown: DriverConformanceTeardown,
) -> Result<(), DriverConformanceFailure> {
    if condition {
        Ok(())
    } else {
        Err(rig.failure(phase, summary, activations, teardown))
    }
}

fn poll_once<F: Future>(future: Pin<&mut F>) -> Poll<F::Output> {
    future.poll(&mut Context::from_waker(Waker::noop()))
}
