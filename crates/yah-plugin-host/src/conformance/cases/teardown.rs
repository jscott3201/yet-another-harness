use std::{
    future::{Future, poll_fn},
    task::Poll,
};

use yah_compose::{
    ComponentSlot, ComponentStateKind, ProviderSelectionEpoch, StopDisposition, StopRecord,
};

use crate::{HostPluginActivation, PluginActivationId};

use super::{CaseRig, DriverConformanceFailure, DriverConformanceTeardown};

pub(super) struct TeardownAttempt {
    state: DriverConformanceTeardown,
    notes: Vec<String>,
}

impl TeardownAttempt {
    fn new(state: DriverConformanceTeardown) -> Self {
        Self {
            state,
            notes: Vec::new(),
        }
    }

    fn incomplete(note: impl Into<String>) -> Self {
        Self {
            state: DriverConformanceTeardown::Incomplete,
            notes: vec![note.into()],
        }
    }
}

pub(super) async fn settle_activation(
    activation: &mut HostPluginActivation<'_>,
) -> TeardownAttempt {
    let notes = seal_activation(activation);
    finish_sealed_activation(activation, notes).await
}

fn seal_activation(activation: &mut HostPluginActivation<'_>) -> Vec<String> {
    let mut notes = Vec::new();
    if !activation.cancellation().is_cancelled()
        && let Err(error) = activation.fail_activation("driver conformance assertion failed")
    {
        notes.push(format!("could not seal the activation: {error}"));
    }
    notes
}

async fn finish_sealed_activation(
    activation: &mut HostPluginActivation<'_>,
    mut notes: Vec<String>,
) -> TeardownAttempt {
    if !activation.cancellation().is_cancelled() {
        return TeardownAttempt::incomplete(join_notes(notes));
    }

    match activation.finish_stop().await {
        Ok(record) => settle_activation_record(activation, record, notes),
        Err(error) => {
            notes.push(format!("could not finish activation cleanup: {error}"));
            TeardownAttempt {
                state: DriverConformanceTeardown::Incomplete,
                notes,
            }
        }
    }
}

pub(super) async fn activation_failure(
    rig: &CaseRig,
    activation: &mut HostPluginActivation<'_>,
    primary: DriverConformanceFailure,
    activations: &[PluginActivationId],
) -> DriverConformanceFailure {
    let attempt = settle_activation(activation).await;
    finish_failure(rig, primary, activations, &[attempt])
}

pub(super) async fn activation_pair_failure(
    rig: &CaseRig,
    first: &mut HostPluginActivation<'_>,
    second: &mut HostPluginActivation<'_>,
    primary: DriverConformanceFailure,
    activations: &[PluginActivationId],
) -> DriverConformanceFailure {
    let first_notes = seal_activation(first);
    let second_notes = seal_activation(second);
    let (first_attempt, second_attempt) = join_attempts(
        finish_sealed_activation(first, first_notes),
        finish_sealed_activation(second, second_notes),
    )
    .await;
    finish_failure(rig, primary, activations, &[first_attempt, second_attempt])
}

pub(super) fn activation_record_failure(
    rig: &CaseRig,
    activation: &mut HostPluginActivation<'_>,
    record: StopRecord,
    primary: DriverConformanceFailure,
    activations: &[PluginActivationId],
) -> DriverConformanceFailure {
    let attempt = settle_activation_record(activation, record, Vec::new());
    finish_failure(rig, primary, activations, &[attempt])
}

pub(super) async fn settle_slot(
    slot: &mut ComponentSlot,
    epoch: ProviderSelectionEpoch,
) -> TeardownAttempt {
    let notes = seal_slot(slot, epoch);
    finish_sealed_slot(slot, epoch, notes).await
}

fn seal_slot(slot: &mut ComponentSlot, epoch: ProviderSelectionEpoch) -> Vec<String> {
    let mut notes = Vec::new();
    match slot.live_state().map(|state| state.kind()) {
        Some(
            ComponentStateKind::Starting | ComponentStateKind::Active | ComponentStateKind::Failed,
        ) => {
            if let Err(error) = slot.fail_activation(epoch, "driver conformance assertion failed") {
                notes.push(format!("could not seal the component activation: {error}"));
            }
        }
        Some(ComponentStateKind::Stopping) => {}
        Some(ComponentStateKind::Pending | ComponentStateKind::Removed) | None => {}
    }
    notes
}

async fn finish_sealed_slot(
    slot: &mut ComponentSlot,
    epoch: ProviderSelectionEpoch,
    mut notes: Vec<String>,
) -> TeardownAttempt {
    match slot.live_state().map(|state| state.kind()) {
        Some(ComponentStateKind::Stopping) => {}
        Some(ComponentStateKind::Pending | ComponentStateKind::Removed) | None => {
            return match slot.last_stop().cloned() {
                Some(record) => settle_slot_record(slot, epoch, record, notes),
                None => TeardownAttempt::new(DriverConformanceTeardown::NotStarted),
            };
        }
        Some(
            ComponentStateKind::Starting | ComponentStateKind::Active | ComponentStateKind::Failed,
        ) => {
            notes.push("component did not enter stopping during best-effort cleanup".to_owned());
            return TeardownAttempt {
                state: DriverConformanceTeardown::Incomplete,
                notes,
            };
        }
    }

    match slot.finish_stop(epoch).await {
        Ok(record) => settle_slot_record(slot, epoch, record, notes),
        Err(error) => {
            notes.push(format!("could not finish component cleanup: {error}"));
            TeardownAttempt {
                state: DriverConformanceTeardown::Incomplete,
                notes,
            }
        }
    }
}

pub(super) async fn slot_failure(
    rig: &CaseRig,
    slot: &mut ComponentSlot,
    epoch: ProviderSelectionEpoch,
    primary: DriverConformanceFailure,
    activations: &[PluginActivationId],
) -> DriverConformanceFailure {
    let attempt = settle_slot(slot, epoch).await;
    finish_failure(rig, primary, activations, &[attempt])
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn activation_slot_failure(
    rig: &CaseRig,
    activation: &mut HostPluginActivation<'_>,
    slot: &mut ComponentSlot,
    slot_epoch: ProviderSelectionEpoch,
    primary: DriverConformanceFailure,
    activations: &[PluginActivationId],
) -> DriverConformanceFailure {
    let activation_notes = seal_activation(activation);
    let slot_notes = seal_slot(slot, slot_epoch);
    let (activation_attempt, slot_attempt) = join_attempts(
        finish_sealed_activation(activation, activation_notes),
        finish_sealed_slot(slot, slot_epoch, slot_notes),
    )
    .await;
    finish_failure(
        rig,
        primary,
        activations,
        &[activation_attempt, slot_attempt],
    )
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn slot_pair_failure(
    rig: &CaseRig,
    first: &mut ComponentSlot,
    first_epoch: ProviderSelectionEpoch,
    second: &mut ComponentSlot,
    second_epoch: ProviderSelectionEpoch,
    primary: DriverConformanceFailure,
    activations: &[PluginActivationId],
) -> DriverConformanceFailure {
    let first_notes = seal_slot(first, first_epoch);
    let second_notes = seal_slot(second, second_epoch);
    let (first_attempt, second_attempt) = join_attempts(
        finish_sealed_slot(first, first_epoch, first_notes),
        finish_sealed_slot(second, second_epoch, second_notes),
    )
    .await;
    finish_failure(rig, primary, activations, &[first_attempt, second_attempt])
}

pub(super) fn finish_failure(
    rig: &CaseRig,
    primary: DriverConformanceFailure,
    activations: &[PluginActivationId],
    attempts: &[TeardownAttempt],
) -> DriverConformanceFailure {
    let state = attempts
        .iter()
        .map(|attempt| attempt.state)
        .max_by_key(|state| severity(*state))
        .unwrap_or(primary.teardown());
    let notes = attempts
        .iter()
        .flat_map(|attempt| attempt.notes.iter())
        .cloned()
        .collect::<Vec<_>>();
    let summary = if notes.is_empty() {
        primary.summary().to_owned()
    } else {
        format!(
            "{}; best-effort teardown: {}",
            primary.summary(),
            notes.join("; ")
        )
    };
    rig.finalized_failure(&primary, summary, activations, state)
}

async fn join_attempts<First, Second>(
    first: First,
    second: Second,
) -> (TeardownAttempt, TeardownAttempt)
where
    First: Future<Output = TeardownAttempt>,
    Second: Future<Output = TeardownAttempt>,
{
    let mut first = Box::pin(first);
    let mut second = Box::pin(second);
    let mut first_output = None;
    let mut second_output = None;
    poll_fn(move |context| {
        if first_output.is_none()
            && let Poll::Ready(output) = first.as_mut().poll(context)
        {
            first_output = Some(output);
        }
        if second_output.is_none()
            && let Poll::Ready(output) = second.as_mut().poll(context)
        {
            second_output = Some(output);
        }
        match (first_output.take(), second_output.take()) {
            (Some(first), Some(second)) => Poll::Ready((first, second)),
            (first, second) => {
                first_output = first;
                second_output = second;
                Poll::Pending
            }
        }
    })
    .await
}

fn settle_activation_record(
    activation: &mut HostPluginActivation<'_>,
    record: StopRecord,
    mut notes: Vec<String>,
) -> TeardownAttempt {
    match record.disposition() {
        StopDisposition::Completed if record.report().is_clean() => TeardownAttempt {
            state: DriverConformanceTeardown::Clean,
            notes,
        },
        StopDisposition::Blocked => {
            notes.push(format!(
                "cleanup reported {} failure(s)",
                record.report().failure_count()
            ));
            match activation.abandon_failed_cleanup() {
                Ok(_) => TeardownAttempt {
                    state: DriverConformanceTeardown::BlockedThenAbandoned,
                    notes,
                },
                Err(error) => {
                    notes.push(format!("blocked cleanup could not be abandoned: {error}"));
                    TeardownAttempt {
                        state: DriverConformanceTeardown::Blocked,
                        notes,
                    }
                }
            }
        }
        StopDisposition::Abandoned => TeardownAttempt {
            state: DriverConformanceTeardown::BlockedThenAbandoned,
            notes,
        },
        StopDisposition::Completed => {
            notes.push("cleanup completed with a non-clean report".to_owned());
            TeardownAttempt {
                state: DriverConformanceTeardown::Incomplete,
                notes,
            }
        }
    }
}

fn settle_slot_record(
    slot: &mut ComponentSlot,
    epoch: ProviderSelectionEpoch,
    record: StopRecord,
    mut notes: Vec<String>,
) -> TeardownAttempt {
    match record.disposition() {
        StopDisposition::Completed if record.report().is_clean() => TeardownAttempt {
            state: DriverConformanceTeardown::Clean,
            notes,
        },
        StopDisposition::Blocked => {
            notes.push(format!(
                "cleanup reported {} failure(s)",
                record.report().failure_count()
            ));
            match slot.abandon_failed_cleanup(epoch) {
                Ok(_) => TeardownAttempt {
                    state: DriverConformanceTeardown::BlockedThenAbandoned,
                    notes,
                },
                Err(error) => {
                    notes.push(format!("blocked cleanup could not be abandoned: {error}"));
                    TeardownAttempt {
                        state: DriverConformanceTeardown::Blocked,
                        notes,
                    }
                }
            }
        }
        StopDisposition::Abandoned => TeardownAttempt {
            state: DriverConformanceTeardown::BlockedThenAbandoned,
            notes,
        },
        StopDisposition::Completed => {
            notes.push("cleanup completed with a non-clean report".to_owned());
            TeardownAttempt {
                state: DriverConformanceTeardown::Incomplete,
                notes,
            }
        }
    }
}

const fn severity(state: DriverConformanceTeardown) -> u8 {
    match state {
        DriverConformanceTeardown::NotStarted => 0,
        DriverConformanceTeardown::Clean => 1,
        DriverConformanceTeardown::BlockedThenAbandoned => 2,
        DriverConformanceTeardown::Blocked => 3,
        DriverConformanceTeardown::Incomplete => 4,
    }
}

fn join_notes(notes: Vec<String>) -> String {
    if notes.is_empty() {
        "activation did not enter stopping".to_owned()
    } else {
        notes.join("; ")
    }
}
