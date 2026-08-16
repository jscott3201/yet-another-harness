#![allow(dead_code)]

use std::{
    collections::{HashMap, VecDeque},
    future::Future,
    pin::Pin,
    sync::{
        Arc, Barrier, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    task::{Context, Poll, Waker},
};

use yah_plugin_host::{
    DriverActivationError, DriverDeactivationError, DriverFuture, DriverHealthError, DriverKind,
    DriverPrepareError, DriverStartPermit, DriverStopPermit, PluginActivationId,
    PluginActivationRequest, PluginDriver, PluginHealth, PluginRevisionId,
    PreparedDriverActivation,
};

#[derive(Clone, Default)]
pub(super) struct Gate {
    inner: Arc<GateInner>,
}

#[derive(Default)]
struct GateInner {
    ready: AtomicBool,
    waker: Mutex<Option<Waker>>,
}

impl Gate {
    pub(super) fn release(&self) {
        self.inner.ready.store(true, Ordering::Release);
        if let Some(waker) = self.inner.waker.lock().unwrap().take() {
            waker.wake();
        }
    }

    fn poll(&self, cx: &mut Context<'_>) -> Poll<()> {
        if self.inner.ready.load(Ordering::Acquire) {
            Poll::Ready(())
        } else {
            *self.inner.waker.lock().unwrap() = Some(cx.waker().clone());
            if self.inner.ready.load(Ordering::Acquire) {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        }
    }
}

#[derive(Clone)]
pub(super) enum PrepareMode {
    Ready,
    Error(&'static str),
    Panic,
    WrongRevision(PluginRevisionId),
}

#[derive(Clone)]
pub(super) enum StartMode {
    Ready,
    Error(&'static str),
    Pending(Gate),
    PanicFactory,
    PanicPoll,
    ReadyDropPanic,
    PendingDropPanic(Gate),
}

#[derive(Clone)]
pub(super) enum DeactivateMode {
    Ready,
    Error(&'static str),
    ErrorDropPanic(&'static str),
    Pending(Gate),
    PanicFactory,
    PanicPoll,
    PanicPollDropPanic,
    DropPanic,
}

#[derive(Clone)]
pub(super) enum HealthMode {
    Value(PluginHealth),
    Error(&'static str),
    Panic,
    BlockedDropPanic(HealthBlock),
}

#[derive(Clone)]
pub(super) struct HealthBlock {
    entered: Arc<Barrier>,
    released: Arc<Barrier>,
}

impl HealthBlock {
    pub(super) fn new() -> Self {
        Self {
            entered: Arc::new(Barrier::new(2)),
            released: Arc::new(Barrier::new(2)),
        }
    }

    pub(super) fn wait_until_called(&self) {
        self.entered.wait();
    }

    pub(super) fn release(&self) {
        self.released.wait();
    }

    fn block(&self) {
        self.entered.wait();
        self.released.wait();
    }
}

struct DropPanickingHealthPayload;

impl Drop for DropPanickingHealthPayload {
    fn drop(&mut self) {
        panic!("health panic payload drop panic");
    }
}

#[derive(Clone)]
pub(super) struct FakePlan {
    pub(super) prepare: PrepareMode,
    pub(super) start: StartMode,
    pub(super) deactivate: DeactivateMode,
    pub(super) health: HealthMode,
    pub(super) prepared_drop_panics: bool,
}

impl FakePlan {
    pub(super) fn ready() -> Self {
        Self {
            prepare: PrepareMode::Ready,
            start: StartMode::Ready,
            deactivate: DeactivateMode::Ready,
            health: HealthMode::Value(PluginHealth::Healthy),
            prepared_drop_panics: false,
        }
    }
}

#[derive(Default)]
struct Trace(Arc<Mutex<Vec<String>>>);

impl Trace {
    fn push(&self, event: impl Into<String>) {
        self.0.lock().unwrap().push(event.into());
    }

    fn entries(&self) -> Vec<String> {
        self.0.lock().unwrap().clone()
    }
}

pub(super) struct ActivationProbe {
    cancellation: yah_compose::ScopeCancellation,
    resource_open: AtomicBool,
    start_constructs: AtomicUsize,
    start_polls: AtomicUsize,
    start_drops: AtomicUsize,
    start_drop_saw_cancellation: AtomicBool,
    deactivate_constructs: AtomicUsize,
    deactivate_polls: AtomicUsize,
    deactivate_saw_cancellation: AtomicBool,
    health: Mutex<HealthMode>,
}

impl ActivationProbe {
    pub(super) fn resource_is_open(&self) -> bool {
        self.resource_open.load(Ordering::Acquire)
    }

    pub(super) fn start_constructs(&self) -> usize {
        self.start_constructs.load(Ordering::Acquire)
    }

    pub(super) fn start_polls(&self) -> usize {
        self.start_polls.load(Ordering::Acquire)
    }

    pub(super) fn start_drops(&self) -> usize {
        self.start_drops.load(Ordering::Acquire)
    }

    pub(super) fn start_drop_saw_cancellation(&self) -> bool {
        self.start_drop_saw_cancellation.load(Ordering::Acquire)
    }

    pub(super) fn deactivate_constructs(&self) -> usize {
        self.deactivate_constructs.load(Ordering::Acquire)
    }

    pub(super) fn deactivate_polls(&self) -> usize {
        self.deactivate_polls.load(Ordering::Acquire)
    }

    pub(super) fn deactivate_saw_cancellation(&self) -> bool {
        self.deactivate_saw_cancellation.load(Ordering::Acquire)
    }

    pub(super) fn set_health(&self, health: HealthMode) {
        *self.health.lock().unwrap() = health;
    }
}

pub(super) struct FakeDriver {
    revision: PluginRevisionId,
    plans: Mutex<VecDeque<FakePlan>>,
    probes: Mutex<HashMap<PluginActivationId, Arc<ActivationProbe>>>,
    trace: Trace,
    prepare_calls: AtomicUsize,
    drop_panics: bool,
}

impl FakeDriver {
    pub(super) fn new(
        revision: PluginRevisionId,
        plans: impl IntoIterator<Item = FakePlan>,
    ) -> Self {
        Self {
            revision,
            plans: Mutex::new(plans.into_iter().collect()),
            probes: Mutex::new(HashMap::new()),
            trace: Trace::default(),
            prepare_calls: AtomicUsize::new(0),
            drop_panics: false,
        }
    }

    pub(super) fn with_drop_panic(mut self) -> Self {
        self.drop_panics = true;
        self
    }

    pub(super) fn probe(&self, id: &PluginActivationId) -> Arc<ActivationProbe> {
        Arc::clone(
            self.probes
                .lock()
                .unwrap()
                .get(id)
                .expect("fake prepared this exact activation"),
        )
    }

    pub(super) fn trace(&self) -> Vec<String> {
        self.trace.entries()
    }

    pub(super) fn record(&self, event: impl Into<String>) {
        self.trace.push(event);
    }

    pub(super) fn prepare_calls(&self) -> usize {
        self.prepare_calls.load(Ordering::Acquire)
    }
}

impl Drop for FakeDriver {
    fn drop(&mut self) {
        if self.drop_panics {
            panic!("plugin driver drop panic");
        }
    }
}

impl PluginDriver for FakeDriver {
    fn kind(&self) -> DriverKind {
        DriverKind::BuiltinRust
    }

    fn revision_id(&self) -> &PluginRevisionId {
        &self.revision
    }

    fn prepare(
        &self,
        request: PluginActivationRequest,
    ) -> Result<Arc<dyn PreparedDriverActivation>, DriverPrepareError> {
        self.prepare_calls.fetch_add(1, Ordering::AcqRel);
        self.trace.push(format!("prepare:{}", request.id()));
        let plan = self
            .plans
            .lock()
            .unwrap()
            .pop_front()
            .expect("fake has one plan per activation");
        match &plan.prepare {
            PrepareMode::Error(summary) => return Err(DriverPrepareError::new(*summary)),
            PrepareMode::Panic => panic!("prepare panic"),
            PrepareMode::Ready | PrepareMode::WrongRevision(_) => {}
        }
        let id = match &plan.prepare {
            PrepareMode::WrongRevision(revision) => {
                PluginActivationId::new(revision.clone(), request.id().selection_epoch())
            }
            _ => request.id().clone(),
        };
        let probe = Arc::new(ActivationProbe {
            cancellation: request.cancellation().clone(),
            resource_open: AtomicBool::new(false),
            start_constructs: AtomicUsize::new(0),
            start_polls: AtomicUsize::new(0),
            start_drops: AtomicUsize::new(0),
            start_drop_saw_cancellation: AtomicBool::new(false),
            deactivate_constructs: AtomicUsize::new(0),
            deactivate_polls: AtomicUsize::new(0),
            deactivate_saw_cancellation: AtomicBool::new(false),
            health: Mutex::new(plan.health.clone()),
        });
        self.probes
            .lock()
            .unwrap()
            .insert(id.clone(), Arc::clone(&probe));
        Ok(Arc::new(FakePrepared {
            id,
            plan,
            probe,
            trace: self.trace.clone(),
        }))
    }
}

impl Clone for Trace {
    fn clone(&self) -> Self {
        Self(Arc::clone(&self.0))
    }
}

struct FakePrepared {
    id: PluginActivationId,
    plan: FakePlan,
    probe: Arc<ActivationProbe>,
    trace: Trace,
}

impl PreparedDriverActivation for FakePrepared {
    fn id(&self) -> &PluginActivationId {
        &self.id
    }

    fn start(&self, permit: DriverStartPermit) -> DriverFuture<Result<(), DriverActivationError>> {
        assert_eq!(permit.id(), &self.id);
        self.probe.start_constructs.fetch_add(1, Ordering::AcqRel);
        self.trace.push(format!("start:construct:{}", self.id));
        if matches!(self.plan.start, StartMode::PanicFactory) {
            panic!("start factory panic");
        }
        Box::pin(FakeStartFuture {
            id: self.id.clone(),
            mode: self.plan.start.clone(),
            probe: Arc::clone(&self.probe),
            trace: self.trace.clone(),
        })
    }

    fn health(&self) -> Result<PluginHealth, DriverHealthError> {
        self.trace.push(format!("health:{}", self.id));
        let mode = self.probe.health.lock().unwrap().clone();
        match mode {
            HealthMode::Value(health) => Ok(health),
            HealthMode::Error(summary) => Err(DriverHealthError::new(summary)),
            HealthMode::Panic => panic!("health panic"),
            HealthMode::BlockedDropPanic(block) => {
                block.block();
                std::panic::panic_any(DropPanickingHealthPayload);
            }
        }
    }

    fn deactivate(
        &self,
        permit: DriverStopPermit,
    ) -> DriverFuture<Result<(), DriverDeactivationError>> {
        assert_eq!(permit.id(), &self.id);
        self.probe
            .deactivate_constructs
            .fetch_add(1, Ordering::AcqRel);
        self.trace.push(format!("deactivate:construct:{}", self.id));
        if matches!(self.plan.deactivate, DeactivateMode::PanicFactory) {
            panic!("deactivate factory panic");
        }
        Box::pin(FakeDeactivateFuture {
            id: self.id.clone(),
            mode: self.plan.deactivate.clone(),
            probe: Arc::clone(&self.probe),
            trace: self.trace.clone(),
        })
    }
}

impl Drop for FakePrepared {
    fn drop(&mut self) {
        if self.plan.prepared_drop_panics {
            panic!("prepared activation drop panic");
        }
    }
}

struct FakeStartFuture {
    id: PluginActivationId,
    mode: StartMode,
    probe: Arc<ActivationProbe>,
    trace: Trace,
}

impl Future for FakeStartFuture {
    type Output = Result<(), DriverActivationError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        this.probe.start_polls.fetch_add(1, Ordering::AcqRel);
        this.probe.resource_open.store(true, Ordering::Release);
        this.trace.push(format!("start:poll:{}", this.id));
        match &this.mode {
            StartMode::Ready | StartMode::ReadyDropPanic => Poll::Ready(Ok(())),
            StartMode::Error(summary) => Poll::Ready(Err(DriverActivationError::failed(*summary))),
            StartMode::Pending(gate) | StartMode::PendingDropPanic(gate) => gate.poll(cx).map(Ok),
            StartMode::PanicPoll => panic!("start poll panic"),
            StartMode::PanicFactory => unreachable!("the factory panics before a future exists"),
        }
    }
}

impl Drop for FakeStartFuture {
    fn drop(&mut self) {
        self.probe.start_drops.fetch_add(1, Ordering::AcqRel);
        self.probe
            .start_drop_saw_cancellation
            .store(self.probe.cancellation.is_cancelled(), Ordering::Release);
        self.trace.push(format!("start:drop:{}", self.id));
        if matches!(
            self.mode,
            StartMode::ReadyDropPanic | StartMode::PendingDropPanic(_)
        ) {
            panic!("start future drop panic");
        }
    }
}

struct FakeDeactivateFuture {
    id: PluginActivationId,
    mode: DeactivateMode,
    probe: Arc<ActivationProbe>,
    trace: Trace,
}

impl Future for FakeDeactivateFuture {
    type Output = Result<(), DriverDeactivationError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        this.probe.deactivate_polls.fetch_add(1, Ordering::AcqRel);
        this.probe
            .deactivate_saw_cancellation
            .store(this.probe.cancellation.is_cancelled(), Ordering::Release);
        this.trace.push(format!("deactivate:poll:{}", this.id));
        let result = match &this.mode {
            DeactivateMode::Ready | DeactivateMode::DropPanic => Poll::Ready(Ok(())),
            DeactivateMode::Error(summary) | DeactivateMode::ErrorDropPanic(summary) => {
                Poll::Ready(Err(DriverDeactivationError::new(*summary)))
            }
            DeactivateMode::Pending(gate) => gate.poll(cx).map(|()| Ok(())),
            DeactivateMode::PanicPoll | DeactivateMode::PanicPollDropPanic => {
                panic!("deactivate poll panic")
            }
            DeactivateMode::PanicFactory => {
                unreachable!("the factory panics before a future exists")
            }
        };
        if result.is_ready() {
            this.probe.resource_open.store(false, Ordering::Release);
        }
        result
    }
}

impl Drop for FakeDeactivateFuture {
    fn drop(&mut self) {
        self.trace.push(format!("deactivate:drop:{}", self.id));
        if matches!(
            self.mode,
            DeactivateMode::DropPanic
                | DeactivateMode::ErrorDropPanic(_)
                | DeactivateMode::PanicPollDropPanic
        ) {
            panic!("deactivate future drop panic");
        }
    }
}
