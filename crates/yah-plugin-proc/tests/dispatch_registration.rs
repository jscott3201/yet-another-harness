//! Application-owned worker-to-host method registration contracts.

#[path = "support/fixtures.rs"]
mod fixtures;
#[path = "support/rig.rs"]
mod rig;

use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};

use fixtures::{lifecycle_revision, worker_program};
use rig::{Rig, as_trait, diagnostics_show, endpoint, settled_within, stop_active};
use serde_json::json;
use yah_plugin_host::{DriverKind, HostPluginActivation};
use yah_plugin_ipc::types::Outcome;
use yah_plugin_proc::{
    CallTerminal, ProcActivationPlan, ProcLimits, ProcessPluginDriver, WorkerMethod,
    WorkerMethodFailure, WorkerMethodRegistrationError, WorkerMethodRegistry, WorkerMethodRequest,
    WorkerMethodResult, WorkerMethodResultError,
};

/// An application-owned method that holds the dispatch permit until its test
/// opens the gate.
struct BlockingMethod {
    entered: AtomicUsize,
    open: AtomicBool,
}

impl WorkerMethod for BlockingMethod {
    fn invoke(
        &self,
        request: &WorkerMethodRequest,
    ) -> Result<WorkerMethodResult, yah_plugin_proc::WorkerMethodFailure> {
        self.entered.fetch_add(1, Ordering::AcqRel);
        while !self.open.load(Ordering::Acquire) {
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
        Ok(
            WorkerMethodResult::new(json!({ "served": request.payload()["call"] }))
                .expect("the fixture result is inline"),
        )
    }
}

/// Application methods are named once before activation. They share the
/// dispatch queue and provider permit with built-ins without receiving pump
/// authority, so a blocked method cannot stall a host-to-worker call.
#[tokio::test]
async fn registered_methods_are_frozen_bounded_and_off_pump() {
    let revision = lifecycle_revision("registered-method", '3');
    let method = Arc::new(BlockingMethod {
        entered: AtomicUsize::new(0),
        open: AtomicBool::new(false),
    });
    let mut methods = WorkerMethodRegistry::new();
    methods
        .register(
            "application.hold",
            Arc::clone(&method) as Arc<dyn WorkerMethod>,
        )
        .expect("the application method registers");
    assert_eq!(
        methods
            .register(
                "application.hold",
                Arc::clone(&method) as Arc<dyn WorkerMethod>
            )
            .expect_err("a duplicate method is refused"),
        WorkerMethodRegistrationError::DuplicateName,
    );
    assert_eq!(
        methods
            .register(
                "artifact.read",
                Arc::clone(&method) as Arc<dyn WorkerMethod>
            )
            .expect_err("the protocol-owned method cannot be replaced"),
        WorkerMethodRegistrationError::ReservedName,
    );
    assert_eq!(
        methods
            .register("", Arc::clone(&method) as Arc<dyn WorkerMethod>)
            .expect_err("empty method names are outside the bound"),
        WorkerMethodRegistrationError::InvalidName,
    );
    assert!(matches!(
        WorkerMethodResult::new(json!(
            "x".repeat(yah_plugin_ipc::MAX_INLINE_RESULT_BYTES + 1)
        )),
        Err(WorkerMethodResultError::TooLarge { .. })
    ));
    methods
        .register(
            "x".repeat(yah_plugin_ipc::MAX_METHOD_CHARS),
            Arc::clone(&method) as Arc<dyn WorkerMethod>,
        )
        .expect("the exact method-name boundary admits");
    assert_eq!(
        methods
            .register(
                "x".repeat(yah_plugin_ipc::MAX_METHOD_CHARS + 1),
                Arc::clone(&method) as Arc<dyn WorkerMethod>,
            )
            .expect_err("one beyond the method-name boundary refuses"),
        WorkerMethodRegistrationError::InvalidName,
    );
    let (driver, observer) = ProcessPluginDriver::scripted_with_limits_and_methods(
        revision.id().clone(),
        DriverKind::NodeProcess,
        worker_program(),
        vec![ProcActivationPlan::worker(
            "registered-method:application.hold",
        )],
        ProcLimits {
            dispatch_queue_capacity: 2,
            provider_concurrency: 1,
            ..ProcLimits::default()
        },
        methods,
    );
    let mut rig = Rig::new("proc.registered-method", &revision);
    let mut activation = HostPluginActivation::prepare(
        &mut rig.slot,
        rig.epoch,
        &rig.broker,
        &rig.grants,
        as_trait(&driver),
    )
    .expect("preparation succeeds");
    let id = activation.id().clone();
    let _handle = activation.activate(&rig.registry).await.expect("starts");
    diagnostics_show(
        &observer,
        &id,
        "registered:unknown:UnknownMethod:echoed=false",
    )
    .await;

    for _ in 0..500 {
        if method.entered.load(Ordering::Acquire) == 1 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    assert_eq!(
        method.entered.load(Ordering::Acquire),
        1,
        "the first registered callback occupies the one shared provider slot"
    );
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(100), async {
            loop {
                if method.entered.load(Ordering::Acquire) > 1 {
                    return;
                }
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
        })
        .await
        .is_err(),
        "the second registered call stays behind the shared provider bound"
    );

    let call = endpoint(&driver, &id)
        .call("tool.echo", json!({ "pump": "still moves" }), None)
        .await
        .expect("the blocked registered method does not stall pump admission");
    assert_eq!(
        settled_within(call).await,
        CallTerminal::Completed(Outcome::Ok {
            result: json!({ "pump": "still moves" }),
        }),
        "the pump receives and settles its own call while the method is blocked"
    );

    method.open.store(true, Ordering::Release);
    diagnostics_show(&observer, &id, "registered:done").await;
    assert_eq!(
        method.entered.load(Ordering::Acquire),
        2,
        "both registered calls ran through the one bounded dispatcher lane"
    );
    stop_active(activation, &rig.registry, rig.epoch).await;
}

struct CancellationMethod {
    entered: AtomicUsize,
    observed: AtomicBool,
    open: AtomicBool,
}

impl WorkerMethod for CancellationMethod {
    fn invoke(
        &self,
        request: &WorkerMethodRequest,
    ) -> Result<WorkerMethodResult, WorkerMethodFailure> {
        self.entered.fetch_add(1, Ordering::AcqRel);
        while !self.open.load(Ordering::Acquire) {
            if request.cancellation().is_cancelled() {
                self.observed.store(true, Ordering::Release);
            }
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
        WorkerMethodResult::new(json!({ "late": true }))
            .map_err(|_| WorkerMethodFailure::failed("fixture result rejected"))
    }
}

async fn wait_atomic(counter: &AtomicUsize, value: usize, message: &str) {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while counter.load(Ordering::Acquire) != value {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("{message}"));
}

#[tokio::test]
async fn worker_cancellation_during_dispatch_is_visible_and_retires_one_reply() {
    let revision = lifecycle_revision("method-cancel", '4');
    let method = Arc::new(CancellationMethod {
        entered: AtomicUsize::new(0),
        observed: AtomicBool::new(false),
        open: AtomicBool::new(false),
    });
    let mut methods = WorkerMethodRegistry::new();
    methods
        .register(
            "application.cancel",
            Arc::clone(&method) as Arc<dyn WorkerMethod>,
        )
        .expect("method registers");
    let (driver, observer) = ProcessPluginDriver::scripted_with_methods(
        revision.id().clone(),
        DriverKind::NodeProcess,
        worker_program(),
        vec![ProcActivationPlan::worker("registered-cancel-during")],
        methods,
    );
    let mut rig = Rig::new("proc.method-cancel", &revision);
    let mut activation = HostPluginActivation::prepare(
        &mut rig.slot,
        rig.epoch,
        &rig.broker,
        &rig.grants,
        as_trait(&driver),
    )
    .expect("preparation succeeds");
    let id = activation.id().clone();
    let _handle = activation.activate(&rig.registry).await.expect("starts");
    wait_atomic(&method.entered, 1, "the callback did not start").await;

    let control = endpoint(&driver, &id)
        .call("control.cancel", json!(null), None)
        .await
        .expect("control call admits");
    assert!(matches!(
        settled_within(control).await,
        CallTerminal::Completed(Outcome::Ok { .. })
    ));
    diagnostics_show(&observer, &id, "method-cancel:cancelled").await;
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while !method.observed.load(Ordering::Acquire) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the callback observes worker cancellation");

    // The worker has its cancelled terminal while the trusted synchronous
    // callback is still running. Letting it finish cannot produce another.
    method.open.store(true, Ordering::Release);
    let sibling = endpoint(&driver, &id)
        .call("application.echo", json!({ "sibling": true }), None)
        .await
        .expect("the pump remains available");
    assert!(matches!(
        settled_within(sibling).await,
        CallTerminal::Completed(Outcome::Ok { .. })
    ));
    stop_active(activation, &rig.registry, rig.epoch).await;
}

#[tokio::test]
async fn worker_cancellation_before_dispatch_skips_the_queued_callback() {
    let revision = lifecycle_revision("queued-cancel", '5');
    let held = Arc::new(BlockingMethod {
        entered: AtomicUsize::new(0),
        open: AtomicBool::new(false),
    });
    let cancelled = Arc::new(CancellationMethod {
        entered: AtomicUsize::new(0),
        observed: AtomicBool::new(false),
        open: AtomicBool::new(true),
    });
    let mut methods = WorkerMethodRegistry::new();
    methods
        .register(
            "application.hold",
            Arc::clone(&held) as Arc<dyn WorkerMethod>,
        )
        .expect("hold registers");
    methods
        .register(
            "application.cancel",
            Arc::clone(&cancelled) as Arc<dyn WorkerMethod>,
        )
        .expect("cancel registers");
    let (driver, observer) = ProcessPluginDriver::scripted_with_limits_and_methods(
        revision.id().clone(),
        DriverKind::NodeProcess,
        worker_program(),
        vec![ProcActivationPlan::worker("registered-cancel-queued")],
        ProcLimits {
            provider_concurrency: 1,
            dispatch_queue_capacity: 2,
            ..ProcLimits::default()
        },
        methods,
    );
    let mut rig = Rig::new("proc.queued-cancel", &revision);
    let mut activation = HostPluginActivation::prepare(
        &mut rig.slot,
        rig.epoch,
        &rig.broker,
        &rig.grants,
        as_trait(&driver),
    )
    .expect("preparation succeeds");
    let id = activation.id().clone();
    let _handle = activation.activate(&rig.registry).await.expect("starts");
    wait_atomic(
        &held.entered,
        1,
        "the first callback did not occupy the slot",
    )
    .await;
    diagnostics_show(&observer, &id, "method-cancel:queued").await;
    assert_eq!(
        cancelled.entered.load(Ordering::Acquire),
        0,
        "a call cancelled while queued never reaches provider code"
    );
    held.open.store(true, Ordering::Release);
    diagnostics_show(&observer, &id, "method-cancel:queued").await;
    stop_active(activation, &rig.registry, rig.epoch).await;
}

#[tokio::test]
async fn registered_method_flood_refuses_past_the_shared_queue_bound() {
    let revision = lifecycle_revision("method-flood", '7');
    let held = Arc::new(BlockingMethod {
        entered: AtomicUsize::new(0),
        open: AtomicBool::new(false),
    });
    let mut methods = WorkerMethodRegistry::new();
    methods
        .register(
            "application.hold",
            Arc::clone(&held) as Arc<dyn WorkerMethod>,
        )
        .expect("method registers");
    let (driver, observer) = ProcessPluginDriver::scripted_with_limits_and_methods(
        revision.id().clone(),
        DriverKind::NodeProcess,
        worker_program(),
        vec![ProcActivationPlan::worker("registered-flood:4")],
        ProcLimits {
            provider_concurrency: 1,
            dispatch_queue_capacity: 1,
            ..ProcLimits::default()
        },
        methods,
    );
    let mut rig = Rig::new("proc.method-flood", &revision);
    let mut activation = HostPluginActivation::prepare(
        &mut rig.slot,
        rig.epoch,
        &rig.broker,
        &rig.grants,
        as_trait(&driver),
    )
    .expect("preparation succeeds");
    let id = activation.id().clone();
    let _handle = activation.activate(&rig.registry).await.expect("starts");
    wait_atomic(
        &held.entered,
        1,
        "the first callback did not occupy the slot",
    )
    .await;
    diagnostics_show(&observer, &id, "ResourceExhausted:retryable=true").await;
    held.open.store(true, Ordering::Release);
    diagnostics_show(&observer, &id, "method-flood:done").await;
    let tail = observer
        .diagnostics_tail(&id, yah_plugin_proc::DiagnosticStream::Stdout)
        .unwrap_or_default();
    let refused = tail.matches("ResourceExhausted:retryable=true").count();
    let completed = tail.matches(":ok").count();
    assert!(
        refused >= 1,
        "at least one call crosses the queue bound: {tail}"
    );
    assert_eq!(
        refused + completed,
        4,
        "every flooded call has one terminal: {tail}"
    );
    stop_active(activation, &rig.registry, rig.epoch).await;
}

struct PanicMethod;

impl WorkerMethod for PanicMethod {
    fn invoke(
        &self,
        _request: &WorkerMethodRequest,
    ) -> Result<WorkerMethodResult, WorkerMethodFailure> {
        panic!("panic-secret")
    }
}

struct ResultMethod(serde_json::Value);

impl WorkerMethod for ResultMethod {
    fn invoke(
        &self,
        _request: &WorkerMethodRequest,
    ) -> Result<WorkerMethodResult, WorkerMethodFailure> {
        WorkerMethodResult::new(self.0.clone())
            .map_err(|_| WorkerMethodFailure::failed("fixture result rejected"))
    }
}

struct FailureMethod;

impl WorkerMethod for FailureMethod {
    fn invoke(
        &self,
        _request: &WorkerMethodRequest,
    ) -> Result<WorkerMethodResult, WorkerMethodFailure> {
        Err(WorkerMethodFailure::failed("é".repeat(600)))
    }
}

#[tokio::test]
async fn method_panic_and_unicode_failure_are_bounded_without_poisoning_siblings() {
    for (suffix, mode, expected) in [
        ("panic", "registered-panic", "method-panic:2:ok"),
        (
            "failure",
            "registered-failure",
            "method-failure:Internal:chars=512",
        ),
    ] {
        let revision = lifecycle_revision(suffix, '6');
        let mut methods = WorkerMethodRegistry::new();
        methods
            .register("application.panic", Arc::new(PanicMethod))
            .expect("panic method registers");
        methods
            .register(
                "application.ok",
                Arc::new(ResultMethod(json!({ "ok": true }))),
            )
            .expect("sibling registers");
        methods
            .register("application.failure", Arc::new(FailureMethod))
            .expect("failure method registers");
        let (driver, observer) = ProcessPluginDriver::scripted_with_methods(
            revision.id().clone(),
            DriverKind::NodeProcess,
            worker_program(),
            vec![ProcActivationPlan::worker(mode)],
            methods,
        );
        let mut rig = Rig::new(&format!("proc.{suffix}"), &revision);
        let mut activation = HostPluginActivation::prepare(
            &mut rig.slot,
            rig.epoch,
            &rig.broker,
            &rig.grants,
            as_trait(&driver),
        )
        .expect("preparation succeeds");
        let id = activation.id().clone();
        let _handle = activation.activate(&rig.registry).await.expect("starts");
        diagnostics_show(&observer, &id, expected).await;
        if suffix == "panic" {
            diagnostics_show(&observer, &id, "method-panic:1:Internal:echoed=false").await;
        }
        let call = endpoint(&driver, &id)
            .call("application.echo", json!({ "alive": true }), None)
            .await
            .expect("pump survives provider outcome");
        assert!(matches!(
            settled_within(call).await,
            CallTerminal::Completed(Outcome::Ok { .. })
        ));
        stop_active(activation, &rig.registry, rig.epoch).await;
    }
}
