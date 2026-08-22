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
    WorkerMethodRegistrationError, WorkerMethodRegistry, WorkerMethodRequest, WorkerMethodResult,
    WorkerMethodResultError,
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
    for reserved in ["artifact.read", "capability.invoke"] {
        assert_eq!(
            methods
                .register(reserved, Arc::clone(&method) as Arc<dyn WorkerMethod>)
                .expect_err("protocol-owned methods cannot be replaced"),
            WorkerMethodRegistrationError::ReservedName,
        );
    }
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
