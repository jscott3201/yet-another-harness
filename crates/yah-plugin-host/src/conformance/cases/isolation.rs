use yah_compose::{ComponentStateKind, DesiredComponentState, StopDisposition};

use crate::{PluginActivationId, PluginHealth, PluginHealthError};

use super::{
    CaseRig, DriverConformanceCaseReport, DriverConformanceFailure, DriverConformancePhase,
    DriverConformanceResourceState, DriverConformanceTeardown, broker, require, teardown,
};

pub(super) async fn run(
    rig: CaseRig,
) -> Result<DriverConformanceCaseReport, DriverConformanceFailure> {
    let (registry_a, mut slot_a, epoch_a) = rig.begin_component("shared-a")?;
    let (registry_b, mut slot_b, epoch_b) = rig.begin_component("shared-b")?;
    let broker = broker(&rig)?;

    let driver_a = rig.clone_driver()?;
    let mut activation_a = rig.prepare_with_driver(&mut slot_a, epoch_a, &broker, driver_a)?;
    let id_a = activation_a.id().clone();
    let driver_b = match rig.take_driver() {
        Ok(driver) => driver,
        Err(primary) => {
            return Err(
                teardown::activation_failure(&rig, &mut activation_a, primary, &[id_a]).await,
            );
        }
    };
    let expected_b = PluginActivationId::new(rig.revision.id().clone(), epoch_b);
    let mut activation_b = match rig.prepare_with_driver(&mut slot_b, epoch_b, &broker, driver_b) {
        Ok(activation) => activation,
        Err(primary) => {
            return Err(teardown::activation_failure(
                &rig,
                &mut activation_a,
                primary,
                &[id_a, expected_b],
            )
            .await);
        }
    };
    let id_b = activation_b.id().clone();
    let ids = [id_a.clone(), id_b.clone()];

    if let Err(primary) = require(
        id_a != id_b,
        &rig,
        DriverConformancePhase::Isolation,
        "shared driver received duplicate activation identities",
        &ids,
        DriverConformanceTeardown::NotStarted,
    ) {
        return Err(teardown::activation_pair_failure(
            &rig,
            &mut activation_a,
            &mut activation_b,
            primary,
            &ids,
        )
        .await);
    }

    let handle_a = match activation_a.activate(&registry_a).await {
        Ok(handle) => handle,
        Err(error) => {
            let primary = rig.failure(
                DriverConformancePhase::Start,
                format!("first shared activation failed: {error}"),
                &ids,
                DriverConformanceTeardown::NotStarted,
            );
            return Err(teardown::activation_pair_failure(
                &rig,
                &mut activation_a,
                &mut activation_b,
                primary,
                &ids,
            )
            .await);
        }
    };
    let handle_b = match activation_b.activate(&registry_b).await {
        Ok(handle) => handle,
        Err(error) => {
            let primary = rig.failure(
                DriverConformancePhase::Start,
                format!("second shared activation failed: {error}"),
                &ids,
                DriverConformanceTeardown::NotStarted,
            );
            return Err(teardown::activation_pair_failure(
                &rig,
                &mut activation_a,
                &mut activation_b,
                primary,
                &ids,
            )
            .await);
        }
    };

    let (slot_a, _) = match activation_a.release_active() {
        Ok(released) => released,
        Err(error) => {
            let primary = rig.failure(
                DriverConformancePhase::Isolation,
                format!("first shared activation could not release: {error}"),
                &ids,
                DriverConformanceTeardown::NotStarted,
            );
            return Err(teardown::activation_slot_failure(
                &rig,
                &mut activation_b,
                &mut slot_a,
                epoch_a,
                primary,
                &ids,
            )
            .await);
        }
    };
    let (slot_b, _) = match activation_b.release_active() {
        Ok(released) => released,
        Err(error) => {
            let primary = rig.failure(
                DriverConformancePhase::Isolation,
                format!("second shared activation could not release: {error}"),
                &ids,
                DriverConformanceTeardown::NotStarted,
            );
            return Err(teardown::slot_pair_failure(
                &rig,
                slot_a,
                epoch_a,
                &mut slot_b,
                epoch_b,
                primary,
                &ids,
            )
            .await);
        }
    };

    if let Err(error) = slot_a.reconcile(
        &registry_a,
        DesiredComponentState::removed(slot_a.generation(2)),
    ) {
        let primary = rig.failure(
            DriverConformancePhase::Isolation,
            format!("first shared activation could not stop: {error}"),
            &ids,
            DriverConformanceTeardown::NotStarted,
        );
        return Err(teardown::slot_pair_failure(
            &rig, slot_a, epoch_a, slot_b, epoch_b, primary, &ids,
        )
        .await);
    }
    let first_record = match slot_a.finish_stop(epoch_a).await {
        Ok(record) => record,
        Err(error) => {
            let primary = rig.failure(
                DriverConformancePhase::Isolation,
                format!("first shared activation cleanup failed: {error}"),
                &ids,
                DriverConformanceTeardown::NotStarted,
            );
            return Err(teardown::slot_pair_failure(
                &rig, slot_a, epoch_a, slot_b, epoch_b, primary, &ids,
            )
            .await);
        }
    };
    if first_record.disposition() != StopDisposition::Completed || !first_record.report().is_clean()
    {
        let primary = rig.failure(
            DriverConformancePhase::Isolation,
            "first shared activation did not stop cleanly",
            &ids,
            DriverConformanceTeardown::NotStarted,
        );
        return Err(teardown::slot_pair_failure(
            &rig, slot_a, epoch_a, slot_b, epoch_b, primary, &ids,
        )
        .await);
    }

    let after_a = match rig.observation(&id_a, DriverConformanceTeardown::Clean) {
        Ok(observation) => observation,
        Err(primary) => {
            return Err(teardown::slot_pair_failure(
                &rig, slot_a, epoch_a, slot_b, epoch_b, primary, &ids,
            )
            .await);
        }
    };
    let before_b = match rig.observation(&id_b, DriverConformanceTeardown::NotStarted) {
        Ok(observation) => observation,
        Err(primary) => {
            return Err(teardown::slot_pair_failure(
                &rig, slot_a, epoch_a, slot_b, epoch_b, primary, &ids,
            )
            .await);
        }
    };
    if let Err(primary) = require(
        matches!(handle_a.health(), Err(PluginHealthError::Inactive { .. }))
            && matches!(handle_b.health(), Ok(PluginHealth::Healthy))
            && slot_b.live_state().map(|state| state.kind()) == Some(ComponentStateKind::Active)
            && after_a.deactivation_factory_calls() == 1
            && after_a.resource_state() == Some(DriverConformanceResourceState::Released)
            && before_b.deactivation_factory_calls() == 0
            && before_b.resource_state() == Some(DriverConformanceResourceState::Live),
        &rig,
        DriverConformancePhase::Isolation,
        "stopping the first activation disturbed or deactivated its sibling",
        &ids,
        DriverConformanceTeardown::Clean,
    ) {
        return Err(teardown::slot_pair_failure(
            &rig, slot_a, epoch_a, slot_b, epoch_b, primary, &ids,
        )
        .await);
    }

    if let Err(error) = slot_b.reconcile(
        &registry_b,
        DesiredComponentState::removed(slot_b.generation(2)),
    ) {
        let primary = rig.failure(
            DriverConformancePhase::Isolation,
            format!("second shared activation could not stop: {error}"),
            &ids,
            DriverConformanceTeardown::Clean,
        );
        return Err(teardown::slot_pair_failure(
            &rig, slot_a, epoch_a, slot_b, epoch_b, primary, &ids,
        )
        .await);
    }
    let second_record = match slot_b.finish_stop(epoch_b).await {
        Ok(record) => record,
        Err(error) => {
            let primary = rig.failure(
                DriverConformancePhase::Isolation,
                format!("second shared activation cleanup failed: {error}"),
                &ids,
                DriverConformanceTeardown::Clean,
            );
            return Err(teardown::slot_pair_failure(
                &rig, slot_a, epoch_a, slot_b, epoch_b, primary, &ids,
            )
            .await);
        }
    };
    if second_record.disposition() != StopDisposition::Completed
        || !second_record.report().is_clean()
    {
        let primary = rig.failure(
            DriverConformancePhase::Isolation,
            "second shared activation did not stop cleanly",
            &ids,
            DriverConformanceTeardown::Clean,
        );
        return Err(teardown::slot_pair_failure(
            &rig, slot_a, epoch_a, slot_b, epoch_b, primary, &ids,
        )
        .await);
    }

    let final_a = match rig.observation(&id_a, DriverConformanceTeardown::Clean) {
        Ok(observation) => observation,
        Err(primary) => {
            return Err(teardown::slot_pair_failure(
                &rig, slot_a, epoch_a, slot_b, epoch_b, primary, &ids,
            )
            .await);
        }
    };
    let final_b = match rig.observation(&id_b, DriverConformanceTeardown::Clean) {
        Ok(observation) => observation,
        Err(primary) => {
            return Err(teardown::slot_pair_failure(
                &rig, slot_a, epoch_a, slot_b, epoch_b, primary, &ids,
            )
            .await);
        }
    };
    if let Err(primary) = require(
        final_a.deactivation_factory_calls() == 1
            && final_a.resource_state() == Some(DriverConformanceResourceState::Released)
            && final_b.deactivation_factory_calls() == 1
            && final_b.deactivation_saw_cancellation()
            && final_b.resource_state() == Some(DriverConformanceResourceState::Released),
        &rig,
        DriverConformancePhase::Isolation,
        "shared driver did not clean each exact activation once",
        &ids,
        DriverConformanceTeardown::Clean,
    ) {
        return Err(teardown::slot_pair_failure(
            &rig, slot_a, epoch_a, slot_b, epoch_b, primary, &ids,
        )
        .await);
    }
    Ok(rig.report(vec![final_a, final_b], DriverConformanceTeardown::Clean))
}
