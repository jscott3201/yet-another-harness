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

/// Polls the pending-start case will spend waiting for a driver to acquire.
///
/// Sized for a driver that yields its way there. Each pass polls once and does
/// not wait, so this rescues a driver that hands the thread back repeatedly -
/// the wasm driver's guest yields once per epoch tick while it instantiates,
/// and how many that is depends on how fast the machine runs the guest. It is
/// generous because a poll costs nothing and the bound is a backstop, not a
/// schedule.
///
/// It does not rescue a driver parked on a waker: nothing here wakes anything,
/// so such a driver spends every pass and fails. That is the intended answer -
/// this case is about a start that stays pending, and a driver that cannot make
/// progress under repeated polling has a different problem than this case can
/// describe.
const PENDING_START_ACQUIRE_POLLS: u64 = 512;

async fn pending_start_cancellation(
    rig: CaseRig,
) -> Result<DriverConformanceCaseReport, DriverConformanceFailure> {
    let (registry, mut slot, epoch) = rig.begin_component("pending")?;
    let broker = broker(&rig)?;
    let removed = DesiredComponentState::removed(slot.generation(2));
    let mut activation = rig.prepare(&mut slot, epoch, &broker)?;
    let id = activation.id().clone();
    // Poll to the state this case is about rather than assuming one poll
    // reaches it. A driver may return `Pending` for reasons of its own before
    // it has acquired anything: the wasm driver runs instantiation as a guest
    // call on its own stack, and that call yields whenever the host's epoch
    // ticks, so a tick landing mid-instantiation returns `Pending` with nothing
    // acquired yet. What this case tests is what a *retained* pending start
    // does when its waiter is dropped, which is a different question from how
    // many polls a driver takes to get there.
    //
    // Each pass drops the waiter, not the driver's operation - that retention
    // is the property under test, and the count assertions below still hold
    // across any number of passes. The bound is what keeps a driver that never
    // acquires a failure rather than a hang.
    //
    // The observation the loop stops on is the one the case goes on to check.
    // Probing again afterwards would consult the target twice for one decision,
    // and a probe is allowed to answer differently the second time: a probe
    // that failed once would have its failure read by the loop, discarded as
    // "not acquired yet", and replaced by whatever the next call returned. That
    // is why a failed probe stops the loop rather than retrying it, and why a
    // poll that came back `Ready` does not probe at all - that is not this
    // case's shape, and the requirement below reports it from an observation of
    // its own.
    let mut first_poll;
    let mut passes = 0u64;
    let mut acquired = false;
    let mut before_cancel = None;
    loop {
        let mut first = Box::pin(activation.activate(&registry));
        first_poll = poll_once(first.as_mut());
        drop(first);
        passes += 1;
        if first_poll.is_ready() {
            break;
        }
        let seen = rig.observation(&id, DriverConformanceTeardown::NotStarted);
        acquired = seen
            .as_ref()
            .is_ok_and(|seen| seen.resource_state() == Some(DriverConformanceResourceState::Live));
        let stop = seen.is_err() || acquired || passes >= PENDING_START_ACQUIRE_POLLS;
        before_cancel = Some(seen);
        if stop {
            break;
        }
    }
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
    // A probe that could not answer is reported before anything is concluded
    // from what it did not say: `acquired` is false whenever the probe failed,
    // so checking the bound first would report every probe failure as a driver
    // that never acquired.
    let before_cancel = match before_cancel
        .unwrap_or_else(|| rig.observation(&id, DriverConformanceTeardown::NotStarted))
    {
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
    // Named separately from the retention check below, which is about something
    // else entirely. A driver that spent every poll without acquiring has not
    // failed to retain its operation, and reporting it as though it had would
    // send an author looking in the wrong place.
    if let Err(primary) = require(
        acquired,
        &rig,
        DriverConformancePhase::Start,
        format!(
            "start returned pending without acquiring its resource, after {passes} poll{}",
            if passes == 1 { "" } else { "s" }
        ),
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
    if let Err(primary) = require(
        before_cancel.start_factory_calls() == 1
            // One pending poll per pass, no more: the host must drive the
            // driver's operation once per poll and must not restart it. The
            // factory count says the operation was not rebuilt; this says it
            // was not polled behind the case's back.
            && before_cancel.start_pending_polls() == passes
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
