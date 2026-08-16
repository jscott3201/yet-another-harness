use std::{
    future::Future,
    panic::{AssertUnwindSafe, catch_unwind},
    pin::Pin,
    sync::{
        Arc, Barrier, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    task::{Context, Poll, Waker},
};

use yah_compose::{
    ComponentDefinition, ComponentRevision, ComponentSlot, ComponentSlotOutcome,
    DesiredComponentState, ProviderAssignments, ProviderSelectionEpoch, ReconcileOutcome, Scope,
    ServiceRegistry, StopDisposition,
};
use yah_plugin_host::{
    CapabilityBroker, CapabilityBrokerError, CapabilityDefinition, CapabilityGrantError,
    CapabilityHandleError, CapabilityId, CapabilityRequest, DriverActivationError,
    DriverDeactivationError, DriverFuture, DriverHealthError, DriverKind, DriverPrepareError,
    DriverStartPermit, DriverStopPermit, EffectiveCapabilityGrants, HostPluginActivation,
    HostPluginActivationError, PackageDigest, PluginActivationId, PluginActivationRequest,
    PluginDriver, PluginEntrypoint, PluginHealth, PluginManifest, PluginPackageId, PluginRevision,
    PluginRevisionId, PluginStartContext, PluginVersion, PreparedDriverActivation,
    SdkVersionRequirement,
};

#[derive(Default)]
struct CaptureState {
    prepare_calls: AtomicUsize,
    start_calls: AtomicUsize,
    deactivate_calls: AtomicUsize,
    context: Mutex<Option<PluginStartContext>>,
}

struct CaptureDriver {
    revision: PluginRevisionId,
    kind: DriverKind,
    state: Arc<CaptureState>,
}

impl CaptureDriver {
    fn new(revision: &PluginRevision) -> (Arc<Self>, Arc<CaptureState>) {
        let state = Arc::new(CaptureState::default());
        (
            Arc::new(Self {
                revision: revision.id().clone(),
                kind: revision.manifest().entrypoint().driver(),
                state: Arc::clone(&state),
            }),
            state,
        )
    }

    fn with_identity(
        revision: PluginRevisionId,
        kind: DriverKind,
    ) -> (Arc<Self>, Arc<CaptureState>) {
        let state = Arc::new(CaptureState::default());
        (
            Arc::new(Self {
                revision,
                kind,
                state: Arc::clone(&state),
            }),
            state,
        )
    }
}

impl PluginDriver for CaptureDriver {
    fn kind(&self) -> DriverKind {
        self.kind
    }

    fn revision_id(&self) -> &PluginRevisionId {
        &self.revision
    }

    fn prepare(
        &self,
        request: PluginActivationRequest,
    ) -> Result<Arc<dyn PreparedDriverActivation>, DriverPrepareError> {
        self.state.prepare_calls.fetch_add(1, Ordering::AcqRel);
        Ok(Arc::new(CapturedActivation {
            id: request.id().clone(),
            state: Arc::clone(&self.state),
        }))
    }
}

struct CapturedActivation {
    id: PluginActivationId,
    state: Arc<CaptureState>,
}

impl PreparedDriverActivation for CapturedActivation {
    fn id(&self) -> &PluginActivationId {
        &self.id
    }

    fn start(&self, permit: DriverStartPermit) -> DriverFuture<Result<(), DriverActivationError>> {
        assert_eq!(permit.id(), &self.id);
        self.state.start_calls.fetch_add(1, Ordering::AcqRel);
        *self.state.context.lock().unwrap() = Some(permit.context().clone());
        Box::pin(async { Ok(()) })
    }

    fn health(&self) -> Result<PluginHealth, DriverHealthError> {
        Ok(PluginHealth::Healthy)
    }

    fn deactivate(
        &self,
        permit: DriverStopPermit,
    ) -> DriverFuture<Result<(), DriverDeactivationError>> {
        assert_eq!(permit.id(), &self.id);
        self.state.deactivate_calls.fetch_add(1, Ordering::AcqRel);
        Box::pin(async { Ok(()) })
    }
}

fn plugin_revision(name: &str, digest: char, requested: &[&str]) -> PluginRevision {
    let manifest = PluginManifest::new(
        PluginPackageId::new(name).unwrap(),
        PluginVersion::new("1.0.0").unwrap(),
        SdkVersionRequirement::new(">=0.1.0, <0.2.0").unwrap(),
        PluginEntrypoint::BuiltinRust {},
        vec![],
        vec![],
        requested
            .iter()
            .map(|id| CapabilityRequest::new(CapabilityId::new(*id).unwrap()))
            .collect(),
    )
    .unwrap();
    PluginRevision::new(
        manifest,
        PackageDigest::new(format!("blake3:{}", digest.to_string().repeat(64))).unwrap(),
    )
}

fn component_revision(name: &str) -> ComponentRevision {
    ComponentRevision::new(
        format!("{name}.revision"),
        ComponentDefinition::new(format!("{name}.component")),
        Scope::root(format!("{name}.scope")),
    )
}

fn begin_start(
    slot: &mut ComponentSlot,
    registry: &ServiceRegistry,
    revision: &ComponentRevision,
    sequence: u64,
) -> ProviderSelectionEpoch {
    let desired = DesiredComponentState::enabled(
        slot.generation(sequence),
        revision.clone(),
        ProviderAssignments::new(),
    );
    match slot.reconcile(registry, desired).unwrap() {
        ComponentSlotOutcome::Mounted {
            component: ReconcileOutcome::StartBegun { selection },
            ..
        }
        | ComponentSlotOutcome::Reconciled {
            component: ReconcileOutcome::StartBegun { selection },
            ..
        } => selection.epoch(),
        outcome => panic!("expected start, got {outcome:?}"),
    }
}

fn poll_once<F: Future>(future: Pin<&mut F>) -> Poll<F::Output> {
    future.poll(&mut Context::from_waker(Waker::noop()))
}

#[test]
fn grants_are_an_exact_requested_subset_and_contracts_do_not_drift() {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<CapabilityBroker>();
    assert_send_sync::<PluginStartContext>();
    assert_send_sync::<yah_plugin_host::CapabilityHandle<AtomicUsize>>();

    let requested = CapabilityId::new("yah.test.counter/v1").unwrap();
    let extra = CapabilityId::new("yah.test.extra/v1").unwrap();
    let counter = CapabilityDefinition::<AtomicUsize>::new(requested.clone());
    let extra_definition = CapabilityDefinition::<AtomicBool>::new(extra);
    let mut broker = CapabilityBroker::new().unwrap();
    let registration = broker
        .register(&counter, Arc::new(AtomicUsize::new(0)))
        .unwrap();
    assert!(matches!(
        broker.register(&counter, Arc::new(AtomicUsize::new(1))),
        Err(CapabilityBrokerError::DuplicateProvider { .. })
    ));
    let extra_registration = broker
        .register(&extra_definition, Arc::new(AtomicBool::new(false)))
        .unwrap();
    let revision = plugin_revision("acme.grants", '1', &[requested.as_str()]);

    let grants = EffectiveCapabilityGrants::new(&revision, [registration.grant()]).unwrap();
    assert!(grants.contains(&requested));
    assert_eq!(grants.granted_capabilities().count(), 1);
    assert!(matches!(
        EffectiveCapabilityGrants::new(&revision, [registration.grant(), registration.grant()]),
        Err(CapabilityGrantError::Duplicate { .. })
    ));
    assert!(matches!(
        EffectiveCapabilityGrants::new(&revision, [extra_registration.grant()]),
        Err(CapabilityGrantError::NotRequested { .. })
    ));

    drop(registration);
    let wrong_contract = CapabilityDefinition::<AtomicBool>::new(requested);
    assert!(matches!(
        broker.register(&wrong_contract, Arc::new(AtomicBool::new(false))),
        Err(CapabilityBrokerError::ContractTypeMismatch { .. })
    ));
}

#[tokio::test]
async fn callback_unwind_releases_activity_before_teardown() {
    let capability_id = CapabilityId::new("yah.test.unwind/v1").unwrap();
    let definition = CapabilityDefinition::<AtomicUsize>::new(capability_id.clone());
    let plugin = plugin_revision("acme.unwind", '8', &[capability_id.as_str()]);
    let mut broker = CapabilityBroker::new().unwrap();
    let registration = broker
        .register(&definition, Arc::new(AtomicUsize::new(1)))
        .unwrap();
    let grants = EffectiveCapabilityGrants::new(&plugin, [registration.grant()]).unwrap();
    let registry = ServiceRegistry::new();
    let revision = component_revision("unwind");
    let mut slot = ComponentSlot::new("unwind.slot").unwrap();
    let epoch = begin_start(&mut slot, &registry, &revision, 1);
    let (driver, state) = CaptureDriver::new(&plugin);
    let mut activation =
        HostPluginActivation::prepare(&mut slot, epoch, &broker, &grants, driver).unwrap();
    activation.activate(&registry).await.unwrap();
    let context = state.context.lock().unwrap().clone().unwrap();
    let handle = context.handle(&definition).unwrap();

    let panic = catch_unwind(AssertUnwindSafe(|| {
        let _ = handle.try_with::<()>(|_| panic!("capability callback panicked"));
    }));
    assert!(panic.is_err());
    let (slot, _) = activation.release_active().unwrap();
    slot.reconcile(
        &registry,
        DesiredComponentState::removed(slot.generation(2)),
    )
    .unwrap();
    assert!(slot.finish_stop(epoch).await.unwrap().report().is_clean());
    assert_eq!(state.deactivate_calls.load(Ordering::Acquire), 1);
}

#[tokio::test]
async fn same_slot_reactivation_never_revives_old_context_or_handle() {
    let capability_id = CapabilityId::new("yah.test.activation-aba/v1").unwrap();
    let definition = CapabilityDefinition::<usize>::new(capability_id.clone());
    let plugin = plugin_revision("acme.activation-aba", '9', &[capability_id.as_str()]);
    let mut broker = CapabilityBroker::new().unwrap();
    let registration = broker.register(&definition, Arc::new(41usize)).unwrap();
    let grants = EffectiveCapabilityGrants::new(&plugin, [registration.grant()]).unwrap();
    let registry = ServiceRegistry::new();
    let revision = component_revision("activation-aba");
    let mut slot = ComponentSlot::new("activation-aba.slot").unwrap();

    let old_epoch = begin_start(&mut slot, &registry, &revision, 1);
    let (old_driver, old_state) = CaptureDriver::new(&plugin);
    let mut old_activation =
        HostPluginActivation::prepare(&mut slot, old_epoch, &broker, &grants, old_driver).unwrap();
    old_activation.activate(&registry).await.unwrap();
    let old_context = old_state.context.lock().unwrap().clone().unwrap();
    let old_handle = old_context.handle(&definition).unwrap();
    let (slot, _) = old_activation.release_active().unwrap();

    let mut sibling_slot = ComponentSlot::new("activation-aba.sibling-slot").unwrap();
    let sibling_epoch = begin_start(&mut sibling_slot, &registry, &revision, 1);
    let (sibling_driver, sibling_state) = CaptureDriver::new(&plugin);
    let mut sibling = HostPluginActivation::prepare(
        &mut sibling_slot,
        sibling_epoch,
        &broker,
        &grants,
        sibling_driver,
    )
    .unwrap();
    sibling.activate(&registry).await.unwrap();
    let sibling_context = sibling_state.context.lock().unwrap().clone().unwrap();
    let sibling_handle = sibling_context.handle(&definition).unwrap();
    let (sibling_slot, _) = sibling.release_active().unwrap();

    slot.fail_activation(old_epoch, "reactivate exact plugin")
        .unwrap();
    assert_eq!(sibling_handle.try_with(|value| *value).unwrap(), 41);
    assert!(
        slot.finish_stop(old_epoch)
            .await
            .unwrap()
            .report()
            .is_clean()
    );

    let new_epoch = begin_start(slot, &registry, &revision, 2);
    assert_ne!(old_epoch, new_epoch);
    let (new_driver, new_state) = CaptureDriver::new(&plugin);
    let mut new_activation =
        HostPluginActivation::prepare(slot, new_epoch, &broker, &grants, new_driver).unwrap();
    new_activation.activate(&registry).await.unwrap();
    let new_context = new_state.context.lock().unwrap().clone().unwrap();
    assert!(matches!(
        old_context.handle(&definition),
        Err(CapabilityBrokerError::ActivationInactive { .. })
    ));
    assert!(matches!(
        old_handle.try_with(|_| ()),
        Err(CapabilityHandleError::Revoked { .. })
    ));
    assert_eq!(
        new_context
            .handle(&definition)
            .unwrap()
            .try_with(|value| *value)
            .unwrap(),
        41
    );

    let (slot, _) = new_activation.release_active().unwrap();
    slot.reconcile(
        &registry,
        DesiredComponentState::removed(slot.generation(3)),
    )
    .unwrap();
    assert!(
        slot.finish_stop(new_epoch)
            .await
            .unwrap()
            .report()
            .is_clean()
    );
    assert_eq!(sibling_handle.try_with(|value| *value).unwrap(), 41);
    sibling_slot
        .fail_activation(sibling_epoch, "finish sibling fixture")
        .unwrap();
    assert!(
        sibling_slot
            .finish_stop(sibling_epoch)
            .await
            .unwrap()
            .report()
            .is_clean()
    );
}

#[tokio::test]
async fn broker_drop_revokes_handles_without_retaining_provider_authority() {
    let capability_id = CapabilityId::new("yah.test.broker-drop/v1").unwrap();
    let definition = CapabilityDefinition::<usize>::new(capability_id.clone());
    let plugin = plugin_revision("acme.broker-drop", 'a', &[capability_id.as_str()]);
    let mut broker = CapabilityBroker::new().unwrap();
    let registration = broker.register(&definition, Arc::new(17usize)).unwrap();
    let grants = EffectiveCapabilityGrants::new(&plugin, [registration.grant()]).unwrap();
    let registry = ServiceRegistry::new();
    let revision = component_revision("broker-drop");
    let mut slot = ComponentSlot::new("broker-drop.slot").unwrap();
    let epoch = begin_start(&mut slot, &registry, &revision, 1);
    let (driver, state) = CaptureDriver::new(&plugin);
    let mut activation =
        HostPluginActivation::prepare(&mut slot, epoch, &broker, &grants, driver).unwrap();
    activation.activate(&registry).await.unwrap();
    let context = state.context.lock().unwrap().clone().unwrap();
    let handle = context.handle(&definition).unwrap();
    let (slot, _) = activation.release_active().unwrap();

    drop(broker);
    assert!(matches!(
        handle.try_with(|_| ()),
        Err(CapabilityHandleError::Revoked { .. })
    ));
    assert!(matches!(
        context.handle(&definition),
        Err(CapabilityBrokerError::ProviderUnavailable { .. })
    ));
    assert_eq!(*registration.withdraw(), 17);

    slot.reconcile(
        &registry,
        DesiredComponentState::removed(slot.generation(2)),
    )
    .unwrap();
    assert!(slot.finish_stop(epoch).await.unwrap().report().is_clean());
}

#[tokio::test]
async fn start_receives_only_exact_grants_and_stop_revokes_every_clone() {
    let capability_id = CapabilityId::new("yah.test.counter/v1").unwrap();
    let denied_id = CapabilityId::new("yah.test.denied/v1").unwrap();
    let definition = CapabilityDefinition::<AtomicUsize>::new(capability_id.clone());
    let denied = CapabilityDefinition::<AtomicUsize>::new(denied_id);
    let mut broker = CapabilityBroker::new().unwrap();
    let registration = broker
        .register(&definition, Arc::new(AtomicUsize::new(1)))
        .unwrap();
    let plugin = plugin_revision(
        "acme.context",
        '2',
        &[capability_id.as_str(), denied.id().as_str()],
    );
    let grants = EffectiveCapabilityGrants::new(&plugin, [registration.grant()]).unwrap();
    let registry = ServiceRegistry::new();
    let revision = component_revision("context");
    let mut slot = ComponentSlot::new("context.slot").unwrap();
    let epoch = begin_start(&mut slot, &registry, &revision, 1);
    let (driver, state) = CaptureDriver::new(&plugin);
    let mut activation =
        HostPluginActivation::prepare(&mut slot, epoch, &broker, &grants, driver).unwrap();

    assert_eq!(state.start_calls.load(Ordering::Acquire), 0);
    activation.activate(&registry).await.unwrap();
    let context = state.context.lock().unwrap().clone().unwrap();
    assert_eq!(context.activation_id(), activation.id());
    assert!(context.is_granted(&capability_id));
    assert!(matches!(
        context.handle(&denied),
        Err(CapabilityBrokerError::NotGranted { .. })
    ));
    let wrong_contract = CapabilityDefinition::<AtomicBool>::new(capability_id);
    assert!(matches!(
        context.handle(&wrong_contract),
        Err(CapabilityBrokerError::ContractTypeMismatch { .. })
    ));
    let handle = context.handle(&definition).unwrap();
    let clone = handle.clone();
    assert_eq!(
        handle
            .try_with(|value| value.load(Ordering::Acquire))
            .unwrap(),
        1
    );
    let (slot, _) = activation.release_active().unwrap();

    slot.reconcile(
        &registry,
        DesiredComponentState::removed(slot.generation(2)),
    )
    .unwrap();
    assert!(matches!(
        handle.try_with(|_| ()),
        Err(CapabilityHandleError::Revoked { .. })
    ));
    assert!(matches!(
        clone.try_with(|_| ()),
        Err(CapabilityHandleError::Revoked { .. })
    ));
    assert!(matches!(
        context.handle(&definition),
        Err(CapabilityBrokerError::ActivationInactive { .. })
    ));
    assert!(slot.finish_stop(epoch).await.unwrap().report().is_clean());
}

#[tokio::test]
async fn withdrawal_and_replacement_never_retarget_an_old_context() {
    let capability_id = CapabilityId::new("yah.test.replace/v1").unwrap();
    let definition = CapabilityDefinition::<usize>::new(capability_id.clone());
    let plugin = plugin_revision("acme.replace", '3', &[capability_id.as_str()]);
    let registry = ServiceRegistry::new();
    let revision = component_revision("replace");
    let mut broker = CapabilityBroker::new().unwrap();
    let old_registration = broker.register(&definition, Arc::new(1usize)).unwrap();
    let old_grants = EffectiveCapabilityGrants::new(&plugin, [old_registration.grant()]).unwrap();
    let mut old_slot = ComponentSlot::new("replace.old-slot").unwrap();
    let old_epoch = begin_start(&mut old_slot, &registry, &revision, 1);
    let (old_driver, old_state) = CaptureDriver::new(&plugin);
    let mut old_activation =
        HostPluginActivation::prepare(&mut old_slot, old_epoch, &broker, &old_grants, old_driver)
            .unwrap();
    old_activation.activate(&registry).await.unwrap();
    let old_context = old_state.context.lock().unwrap().clone().unwrap();
    let old_handle = old_context.handle(&definition).unwrap();
    let (old_slot, _) = old_activation.release_active().unwrap();

    let old_provider = old_registration.withdraw();
    assert!(matches!(
        old_handle.try_with(|_| ()),
        Err(CapabilityHandleError::Revoked { .. })
    ));
    drop(old_provider);
    let replacement = broker.register(&definition, Arc::new(2usize)).unwrap();
    assert_ne!(old_handle.registration_id(), replacement.id());
    assert!(matches!(
        old_context.handle(&definition),
        Err(CapabilityBrokerError::ProviderUnavailable { .. })
    ));

    let new_grants = EffectiveCapabilityGrants::new(&plugin, [replacement.grant()]).unwrap();
    let mut new_slot = ComponentSlot::new("replace.new-slot").unwrap();
    let new_epoch = begin_start(&mut new_slot, &registry, &revision, 1);
    let (new_driver, new_state) = CaptureDriver::new(&plugin);
    let mut new_activation =
        HostPluginActivation::prepare(&mut new_slot, new_epoch, &broker, &new_grants, new_driver)
            .unwrap();
    new_activation.activate(&registry).await.unwrap();
    let new_context = new_state.context.lock().unwrap().clone().unwrap();
    let new_handle = new_context.handle(&definition).unwrap();
    assert_eq!(new_handle.try_with(|value| *value).unwrap(), 2);

    old_slot
        .reconcile(
            &registry,
            DesiredComponentState::removed(old_slot.generation(2)),
        )
        .unwrap();
    old_slot.finish_stop(old_epoch).await.unwrap();
    drop(new_activation);
    new_slot.finish_stop(new_epoch).await.unwrap();
}

#[tokio::test]
async fn admitted_call_drains_before_cleanup_and_late_results_fail_closed() {
    let capability_id = CapabilityId::new("yah.test.drain/v1").unwrap();
    let definition = CapabilityDefinition::<AtomicUsize>::new(capability_id.clone());
    let plugin = plugin_revision("acme.drain", '4', &[capability_id.as_str()]);
    let mut broker = CapabilityBroker::new().unwrap();
    let registration = broker
        .register(&definition, Arc::new(AtomicUsize::new(7)))
        .unwrap();
    let grants = EffectiveCapabilityGrants::new(&plugin, [registration.grant()]).unwrap();
    let registry = ServiceRegistry::new();
    let revision = component_revision("drain");
    let mut slot = ComponentSlot::new("drain.slot").unwrap();
    let epoch = begin_start(&mut slot, &registry, &revision, 1);
    let (driver, state) = CaptureDriver::new(&plugin);
    let mut activation =
        HostPluginActivation::prepare(&mut slot, epoch, &broker, &grants, driver).unwrap();
    activation.activate(&registry).await.unwrap();
    let context = state.context.lock().unwrap().clone().unwrap();
    let handle = context.handle(&definition).unwrap();
    let (slot, _) = activation.release_active().unwrap();
    let entered = Arc::new(Barrier::new(2));
    let release = Arc::new(Barrier::new(2));
    let worker_handle = handle.clone();
    let worker_entered = Arc::clone(&entered);
    let worker_release = Arc::clone(&release);
    let worker = std::thread::spawn(move || {
        worker_handle.try_with(|value| {
            worker_entered.wait();
            worker_release.wait();
            value.load(Ordering::Acquire)
        })
    });
    entered.wait();

    drop(registration.withdraw());
    assert!(matches!(
        context.handle(&definition),
        Err(CapabilityBrokerError::ProviderUnavailable { .. })
    ));
    slot.reconcile(
        &registry,
        DesiredComponentState::removed(slot.generation(2)),
    )
    .unwrap();
    assert!(matches!(
        handle.try_with(|_| ()),
        Err(CapabilityHandleError::Revoked { .. })
    ));
    let mut first_finish = Box::pin(slot.finish_stop(epoch));
    assert!(poll_once(first_finish.as_mut()).is_pending());
    drop(first_finish);
    assert_eq!(state.deactivate_calls.load(Ordering::Acquire), 0);

    release.wait();
    assert!(matches!(
        worker.join().unwrap(),
        Err(CapabilityHandleError::Revoked { .. })
    ));
    let record = slot.finish_stop(epoch).await.unwrap();
    assert_eq!(record.disposition(), StopDisposition::Completed);
    assert_eq!(state.deactivate_calls.load(Ordering::Acquire), 1);
}

#[test]
fn trusted_grant_subject_rejects_driver_revision_lane_and_foreign_broker() {
    let capability_id = CapabilityId::new("yah.test.subject/v1").unwrap();
    let definition = CapabilityDefinition::<usize>::new(capability_id.clone());
    let expected = plugin_revision("acme.subject", '5', &[capability_id.as_str()]);
    let other = plugin_revision("acme.other", '6', &[capability_id.as_str()]);
    let mut broker = CapabilityBroker::new().unwrap();
    let registration = broker.register(&definition, Arc::new(1usize)).unwrap();
    let grants = EffectiveCapabilityGrants::new(&expected, [registration.grant()]).unwrap();
    let registry = ServiceRegistry::new();
    let revision = component_revision("subject");

    let mut wrong_revision_slot = ComponentSlot::new("subject.revision-slot").unwrap();
    let epoch = begin_start(&mut wrong_revision_slot, &registry, &revision, 1);
    let (wrong_revision, state) = CaptureDriver::new(&other);
    assert!(matches!(
        HostPluginActivation::prepare(
            &mut wrong_revision_slot,
            epoch,
            &broker,
            &grants,
            wrong_revision
        ),
        Err(HostPluginActivationError::DriverRevisionMismatch { .. })
    ));
    assert_eq!(state.prepare_calls.load(Ordering::Acquire), 0);

    let mut wrong_lane_slot = ComponentSlot::new("subject.lane-slot").unwrap();
    let epoch = begin_start(&mut wrong_lane_slot, &registry, &revision, 1);
    let (wrong_lane, state) =
        CaptureDriver::with_identity(expected.id().clone(), DriverKind::NodeProcess);
    assert!(matches!(
        HostPluginActivation::prepare(&mut wrong_lane_slot, epoch, &broker, &grants, wrong_lane),
        Err(HostPluginActivationError::DriverKindMismatch { .. })
    ));
    assert_eq!(state.prepare_calls.load(Ordering::Acquire), 0);

    let foreign_broker = CapabilityBroker::new().unwrap();
    let mut foreign_slot = ComponentSlot::new("subject.foreign-slot").unwrap();
    let epoch = begin_start(&mut foreign_slot, &registry, &revision, 1);
    let (driver, state) = CaptureDriver::new(&expected);
    assert!(matches!(
        HostPluginActivation::prepare(&mut foreign_slot, epoch, &foreign_broker, &grants, driver),
        Err(HostPluginActivationError::Capability(
            CapabilityBrokerError::ForeignRegistration { .. }
        ))
    ));
    assert_eq!(state.prepare_calls.load(Ordering::Acquire), 0);
}

#[tokio::test]
async fn stale_handles_do_not_retain_registered_provider_values() {
    struct DropFlag(Arc<AtomicBool>);
    impl Drop for DropFlag {
        fn drop(&mut self) {
            self.0.store(true, Ordering::Release);
        }
    }

    let dropped = Arc::new(AtomicBool::new(false));
    let id = CapabilityId::new("yah.test.weak/v1").unwrap();
    let definition = CapabilityDefinition::<DropFlag>::new(id.clone());
    let plugin = plugin_revision("acme.weak", '7', &[id.as_str()]);
    let mut broker = CapabilityBroker::new().unwrap();
    let registration = broker
        .register(&definition, Arc::new(DropFlag(Arc::clone(&dropped))))
        .unwrap();
    let grants = EffectiveCapabilityGrants::new(&plugin, [registration.grant()]).unwrap();
    let registry = ServiceRegistry::new();
    let revision = component_revision("weak");
    let mut slot = ComponentSlot::new("weak.slot").unwrap();
    let epoch = begin_start(&mut slot, &registry, &revision, 1);
    let (driver, state) = CaptureDriver::new(&plugin);
    let mut activation =
        HostPluginActivation::prepare(&mut slot, epoch, &broker, &grants, driver).unwrap();
    activation.activate(&registry).await.unwrap();
    let context = state.context.lock().unwrap().clone().unwrap();
    let handle = context.handle(&definition).unwrap();

    drop(registration);
    assert!(dropped.load(Ordering::Acquire));
    assert!(matches!(
        handle.try_with(|_| ()),
        Err(CapabilityHandleError::Revoked { .. })
    ));
}
