//! The release-ack contract: an admitted worker-handle release reports
//! success only when the worker's ack names the handle. Withheld acks,
//! disconnects, goodbyes, fatal desyncs, and teardown all settle typed —
//! never as acknowledged success — and every waiter is reclaimed.

#[path = "support/fixtures.rs"]
mod fixtures;
#[path = "support/rig.rs"]
mod rig;

use fixtures::{lifecycle_revision, worker_program};
use rig::{Rig, as_trait, endpoint, settled_within, stop_active};
use serde_json::json;
use yah_plugin_host::{DriverKind, HostPluginActivation};
use yah_plugin_ipc::types::Outcome;
use yah_plugin_proc::{
    Availability, CallTerminal, EndpointError, ProcActivationPlan, ProcessPluginDriver, Refusal,
};

/// Boot a release-script activation; returns the pieces the release
/// evidence needs. The activation borrows the rig, so this binds in the
/// caller's scope like `poisoned_spill!`.
macro_rules! release_script {
    ($label:expr, $mode:expr, $driver:ident, $observer:ident, $rig:ident, $activation:ident, $ep:ident, $offer:ident, $id:ident, $handle:ident) => {
        let revision = lifecycle_revision($label, '9');
        let ($driver, $observer) = ProcessPluginDriver::scripted(
            revision.id().clone(),
            DriverKind::NodeProcess,
            worker_program(),
            vec![ProcActivationPlan::worker($mode)],
        );
        let mut $rig = Rig::new(&format!("proc.{}", $label), &revision);
        let mut $activation = HostPluginActivation::prepare(
            &mut $rig.slot,
            $rig.epoch,
            &$rig.broker,
            &$rig.grants,
            as_trait(&$driver),
        )
        .expect("preparation is inert and succeeds");
        let $id = $activation.id().clone();
        let $handle = $activation.activate(&$rig.registry).await.expect("starts");
        let $ep = endpoint(&$driver, &$id);
        let call = $ep
            .call("tool.make", json!(null), None)
            .await
            .expect("admits");
        let $offer = match settled_within(call).await {
            CallTerminal::Completed(Outcome::Spilled { artifact }) => artifact,
            other => panic!("expected the spilled offer, got {other:?}"),
        };
    };
}

/// An admitted release is not reported success at queue time: the caller
/// stays pending until the worker's ack names the handle, the gauges name
/// the in-flight waiter, a duplicate is refused as pending, and the
/// delayed ack resolves Ok exactly once.
#[tokio::test]
async fn a_release_reports_success_only_after_the_worker_ack() {
    release_script!(
        "release-ack",
        "release-later:700",
        driver,
        observer,
        rig,
        activation,
        ep,
        offer,
        id,
        _handle
    );

    let release = ep.release_worker_handle(offer.handle);
    tokio::pin!(release);
    // The negative bound: well inside the worker's 700 ms hold, nothing
    // has resolved — queueing the frame is not acknowledgement.
    let early = tokio::time::timeout(std::time::Duration::from_millis(250), &mut release).await;
    assert!(
        early.is_err(),
        "an admitted release must stay pending until the worker acks: {early:?}"
    );
    assert_eq!(
        observer.gauges(&id).expect("live").pending_releases,
        1,
        "the endpoint names its in-flight release"
    );
    // A second application release for the same handle is a named
    // refusal while the first is still owed its ack.
    assert_eq!(
        ep.release_worker_handle(offer.handle).await,
        Err(EndpointError::Refused(Refusal::ReleasePending))
    );
    // The exact ack arrives; success resolves exactly once.
    tokio::time::timeout(std::time::Duration::from_secs(5), &mut release)
        .await
        .expect("the ack lands within bound")
        .expect("acknowledged success");
    assert_eq!(
        observer.gauges(&id).expect("live").pending_releases,
        0,
        "the ack retires the waiter"
    );
    stop_active(activation, &rig.registry, rig.epoch).await;
}

/// A worker that withholds the ack forever keeps the caller pending —
/// never an early success — and deactivation settles the waiter as a
/// typed loss instead of acknowledged success.
#[tokio::test]
async fn a_withheld_ack_stays_pending_and_settles_lost_on_teardown() {
    release_script!(
        "release-hold",
        "release-withhold",
        driver,
        observer,
        rig,
        activation,
        ep,
        offer,
        id,
        _handle
    );

    let release = ep.release_worker_handle(offer.handle);
    tokio::pin!(release);
    let early = tokio::time::timeout(std::time::Duration::from_millis(500), &mut release).await;
    assert!(
        early.is_err(),
        "a withheld ack must keep the caller pending"
    );
    assert_eq!(observer.gauges(&id).expect("live").pending_releases, 1);
    // Deactivation says goodbye before the kill: the waiter learns now.
    stop_active(activation, &rig.registry, rig.epoch).await;
    let settled = tokio::time::timeout(std::time::Duration::from_secs(5), release)
        .await
        .expect("teardown settles the waiter");
    assert_eq!(
        settled,
        Err(EndpointError::ReleaseLost { orderly: true }),
        "a goodbye before the ack is never acknowledged success"
    );
}

/// A disconnect between the host's release and the worker's ack resolves
/// non-success with the bare-loss classification.
#[tokio::test]
async fn a_disconnect_before_the_ack_is_a_typed_loss() {
    release_script!(
        "release-die",
        "release-die",
        driver,
        observer,
        rig,
        activation,
        ep,
        offer,
        id,
        _handle
    );
    let result = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        ep.release_worker_handle(offer.handle),
    )
    .await
    .expect("the waiter settles when the process dies");
    assert_eq!(
        result,
        Err(EndpointError::ReleaseLost { orderly: false }),
        "a bare disconnect leaves the worker's table state unknown"
    );
    if let Some(gauges) = observer.gauges(&id) {
        assert_eq!(gauges.pending_releases, 0);
    }
    stop_active(activation, &rig.registry, rig.epoch).await;
}

/// A goodbye before the ack is an orderly end, but still not an
/// acknowledged one.
#[tokio::test]
async fn a_goodbye_before_the_ack_is_never_acknowledged_success() {
    release_script!(
        "release-bye",
        "release-goodbye",
        driver,
        _observer,
        rig,
        activation,
        ep,
        offer,
        id,
        _handle
    );
    let result = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        ep.release_worker_handle(offer.handle),
    )
    .await
    .expect("the waiter settles on the goodbye");
    assert_eq!(result, Err(EndpointError::ReleaseLost { orderly: true }));
    stop_active(activation, &rig.registry, rig.epoch).await;
}

/// An unsolicited ack for a handle the host never asked about faults the
/// session outright; afterwards the endpoint is gone.
#[tokio::test]
async fn an_unsolicited_ack_is_fatal_and_the_endpoint_closes() {
    let revision = lifecycle_revision("release-bogus", '9');
    let (driver, _observer) = ProcessPluginDriver::scripted(
        revision.id().clone(),
        DriverKind::NodeProcess,
        worker_program(),
        vec![ProcActivationPlan::worker("release-bogus-ack")],
    );
    let mut rig = Rig::new("proc.release-bogus", &revision);
    let mut activation = HostPluginActivation::prepare(
        &mut rig.slot,
        rig.epoch,
        &rig.broker,
        &rig.grants,
        as_trait(&driver),
    )
    .expect("preparation is inert and succeeds");
    let id = activation.id().clone();
    let _handle = activation.activate(&rig.registry).await.expect("starts");
    let ep = endpoint(&driver, &id);
    // The worker sends its unsolicited ack right after hello; the
    // session faults, and the endpoint closes with the retained cause.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    while matches!(
        ep.availability(),
        Availability::Active | Availability::Negotiating
    ) {
        assert!(
            std::time::Instant::now() < deadline,
            "a fatal ack desync must end the endpoint"
        );
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    assert!(matches!(ep.availability(), Availability::Closed { .. }));
    assert!(ep.call("anything", json!(null), None).await.is_err());
    let _ = ep;
    stop_active(activation, &rig.registry, rig.epoch).await;
}
