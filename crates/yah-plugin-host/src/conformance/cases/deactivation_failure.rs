use super::*;

pub(super) async fn run(
    rig: CaseRig,
) -> Result<DriverConformanceCaseReport, DriverConformanceFailure> {
    let (registry, mut slot, epoch) = rig.begin_component("deactivation-failure")?;
    let broker = broker(&rig)?;
    let mut activation = rig.prepare(&mut slot, epoch, &broker)?;
    let id = activation.id().clone();
    if let Err(error) = activation.activate(&registry).await {
        let primary = rig.failure(
            DriverConformancePhase::Start,
            format!("deactivation-failure fixture did not activate: {error}"),
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
    let (slot, _) = match activation.release_active() {
        Ok(released) => released,
        Err(error) => {
            let primary = rig.failure(
                DriverConformancePhase::Start,
                format!("deactivation-failure fixture could not release: {error}"),
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
    if let Err(error) = slot.reconcile(
        &registry,
        DesiredComponentState::removed(slot.generation(2)),
    ) {
        let primary = rig.failure(
            DriverConformancePhase::Cancellation,
            format!("deactivation-failure fixture could not stop: {error}"),
            std::slice::from_ref(&id),
            DriverConformanceTeardown::NotStarted,
        );
        return Err(
            teardown::slot_failure(&rig, slot, epoch, primary, std::slice::from_ref(&id)).await,
        );
    }
    let first = match slot.finish_stop(epoch).await {
        Ok(record) => record,
        Err(error) => {
            let primary = rig.failure(
                DriverConformancePhase::Teardown,
                format!("failed deactivation did not reach a report: {error}"),
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
    if first.disposition() != StopDisposition::Blocked {
        let primary = rig.failure(
            DriverConformancePhase::Teardown,
            "deactivation-failure fixture did not produce a blocked report",
            std::slice::from_ref(&id),
            if first.report().is_clean() {
                DriverConformanceTeardown::Clean
            } else {
                DriverConformanceTeardown::Incomplete
            },
        );
        return Err(
            teardown::slot_failure(&rig, slot, epoch, primary, std::slice::from_ref(&id)).await,
        );
    }
    let repeated = match slot.finish_stop(epoch).await {
        Ok(record) => record,
        Err(error) => {
            let primary = rig.failure(
                DriverConformancePhase::Teardown,
                format!("cached failed deactivation was unavailable: {error}"),
                std::slice::from_ref(&id),
                DriverConformanceTeardown::Blocked,
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
    let before_abandon = match rig.observation(&id, DriverConformanceTeardown::Blocked) {
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
        first == repeated
            && first.disposition() == StopDisposition::Blocked
            && first.report().failure_count() == 1
            && before_abandon.deactivation_factory_calls() == 1
            && before_abandon.deactivation_terminal()
                == Some(DriverConformanceTerminal::ReturnedError)
            && before_abandon.resource_state() == Some(DriverConformanceResourceState::Released),
        &rig,
        DriverConformancePhase::Teardown,
        "failed deactivation was not cached and blocked exactly once",
        std::slice::from_ref(&id),
        DriverConformanceTeardown::Blocked,
    ) {
        return Err(
            teardown::slot_failure(&rig, slot, epoch, primary, std::slice::from_ref(&id)).await,
        );
    }
    let abandoned = match slot.abandon_failed_cleanup(epoch) {
        Ok(record) => record,
        Err(error) => {
            let primary = rig.failure(
                DriverConformancePhase::Teardown,
                format!("blocked cleanup could not be explicitly abandoned: {error}"),
                std::slice::from_ref(&id),
                DriverConformanceTeardown::Blocked,
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
    let observation = match rig.observation(&id, DriverConformanceTeardown::BlockedThenAbandoned) {
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
        abandoned.disposition() == StopDisposition::Abandoned
            && slot.live_state().is_none()
            && observation.deactivation_factory_calls() == 1,
        &rig,
        DriverConformancePhase::Teardown,
        "explicit abandonment did not preserve exactly-once failed teardown",
        std::slice::from_ref(&id),
        DriverConformanceTeardown::BlockedThenAbandoned,
    ) {
        return Err(
            teardown::slot_failure(&rig, slot, epoch, primary, std::slice::from_ref(&id)).await,
        );
    }
    Ok(rig.report(
        vec![observation],
        DriverConformanceTeardown::BlockedThenAbandoned,
    ))
}
