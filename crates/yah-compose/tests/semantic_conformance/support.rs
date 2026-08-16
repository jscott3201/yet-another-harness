use std::{
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex, OnceLock},
    task::{Context, Poll, Waker},
};

use yah_compose::{
    ComponentDefinition, ComponentRevision, ComponentSlot, ComponentSlotOutcome,
    DesiredComponentState, ProviderAssignments, ProviderCandidate, ProviderSelectionEpoch,
    ReconcileOutcome, Scope, ServiceDefinition, ServiceHandle, ServiceRegistry,
};

#[derive(Clone, Default)]
pub(super) struct Trace(Arc<Mutex<Vec<&'static str>>>);

impl Trace {
    pub(super) fn push(&self, entry: &'static str) {
        self.0.lock().unwrap().push(entry);
    }

    pub(super) fn entries(&self) -> Vec<&'static str> {
        self.0.lock().unwrap().clone()
    }
}

#[derive(Debug)]
pub(super) struct Message(pub(super) &'static str);

pub(super) struct ActiveProvider {
    pub(super) slot: ComponentSlot,
    pub(super) revision: ComponentRevision,
    pub(super) epoch: ProviderSelectionEpoch,
    pub(super) candidate: ProviderCandidate,
}

fn composition_scope() -> &'static Scope {
    static SCOPE: OnceLock<Scope> = OnceLock::new();
    SCOPE.get_or_init(|| Scope::root("semantic-conformance.tests"))
}

pub(super) fn revision(
    id: &str,
    _scope: &str,
    required: Option<&ServiceDefinition<Message>>,
) -> ComponentRevision {
    let mut definition = ComponentDefinition::new(format!("{id}.component"));
    if let Some(service) = required {
        definition.require(&service.required()).unwrap();
    }
    ComponentRevision::new(id, definition, composition_scope().clone())
}

pub(super) fn enabled(
    slot: &ComponentSlot,
    sequence: u64,
    revision: &ComponentRevision,
    assignments: &ProviderAssignments,
) -> DesiredComponentState {
    DesiredComponentState::enabled(
        slot.generation(sequence),
        revision.clone(),
        assignments.clone(),
    )
}

pub(super) fn start_epoch(outcome: ComponentSlotOutcome) -> ProviderSelectionEpoch {
    match outcome {
        ComponentSlotOutcome::Mounted {
            component: ReconcileOutcome::StartBegun { selection },
            ..
        }
        | ComponentSlotOutcome::Reconciled {
            component: ReconcileOutcome::StartBegun { selection },
            ..
        } => selection.epoch(),
        outcome => panic!("expected a fresh activation, got {outcome:?}"),
    }
}

pub(super) fn assignment(candidate: &ProviderCandidate) -> ProviderAssignments {
    let mut assignments = ProviderAssignments::new();
    assignments.assign(candidate);
    assignments
}

pub(super) fn start_provider(
    registry: &mut ServiceRegistry,
    service: &ServiceDefinition<Message>,
    label: &str,
    value: &'static str,
) -> ActiveProvider {
    let mut slot = ComponentSlot::new(format!("{label}.slot")).unwrap();
    let revision = revision(
        &format!("{label}.revision"),
        &format!("{label}.scope"),
        None,
    );
    let assignments = ProviderAssignments::new();
    let epoch = start_epoch(
        slot.reconcile(registry, enabled(&slot, 1, &revision, &assignments))
            .unwrap(),
    );
    assert!(matches!(
        slot.complete_start(epoch, registry).unwrap(),
        ReconcileOutcome::Active { .. }
    ));
    let candidate = slot
        .provide(epoch, registry, service.provider(Message(value)))
        .unwrap();
    ActiveProvider {
        slot,
        revision,
        epoch,
        candidate,
    }
}

pub(super) fn start_consumer(
    slot: &mut ComponentSlot,
    registry: &ServiceRegistry,
    service: &ServiceDefinition<Message>,
    revision: &ComponentRevision,
    assignments: &ProviderAssignments,
    sequence: u64,
) -> (ProviderSelectionEpoch, ServiceHandle<Message>) {
    let epoch = start_epoch(
        slot.reconcile(registry, enabled(slot, sequence, revision, assignments))
            .unwrap(),
    );
    let handle = slot.bind(epoch, registry, &service.required()).unwrap();
    assert!(matches!(
        slot.complete_start(epoch, registry).unwrap(),
        ReconcileOutcome::Active { .. }
    ));
    (epoch, handle)
}

pub(super) fn poll_once<F: Future>(future: Pin<&mut F>) -> Poll<F::Output> {
    future.poll(&mut Context::from_waker(Waker::noop()))
}
