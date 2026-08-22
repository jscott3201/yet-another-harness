//! The spilled-artifact contract: completion-gated verification, exact
//! chunk lengths, canonical hex, and metadata that must agree with the
//! offer. Every claim drives a real supervised worker over the real
//! transport, including workers that lie on purpose.

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
    ArtifactReader, CallTerminal, EndpointError, ProcActivationPlan, ProcessPluginDriver, Refusal,
};

/// The pattern the spill fixtures fill their artifacts with, so tests can
/// compute honest digests over prefixes without a second wire round trip.
fn artifact_pattern(len: u64) -> Vec<u8> {
    (0..len).map(|i| (i % 251) as u8).collect()
}

/// Verification is gated on completion: a digest over zero bytes or over
/// a strict prefix proves nothing about the artifact, so early verifies
/// are refused even when the hash would match what was read so far.
#[tokio::test]
async fn artifact_verification_requires_the_full_read() {
    let revision = lifecycle_revision("verify", 'e');
    let (driver, _observer) = ProcessPluginDriver::scripted(
        revision.id().clone(),
        DriverKind::NodeProcess,
        worker_program(),
        vec![ProcActivationPlan::worker("spill:200000")],
    );
    let mut rig = Rig::new("proc.verify", &revision);
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

    let call = ep
        .call("tool.make", json!(null), None)
        .await
        .expect("admits");
    let real_offer = match settled_within(call).await {
        CallTerminal::Completed(Outcome::Spilled { artifact }) => artifact,
        other => panic!("expected the spilled offer, got {other:?}"),
    };

    // Zero reads: an offer whose digest is BLAKE3 of nothing must still
    // be refused — the completion gate fires before any hash compare.
    let mut empty_digest_offer = real_offer.clone();
    empty_digest_offer.digest_blake3 = blake3::hash(&[]).to_hex().to_string();
    let reader = ArtifactReader::new(&ep, empty_digest_offer, 1 << 20).expect("within limit");
    assert_eq!(
        reader.verify(),
        Err(EndpointError::Refused(Refusal::InvalidField(
            "artifact verified before it was fully read"
        ))),
        "a digest over zero bytes buys nothing before the first pull"
    );

    // One strict prefix: the digest matches exactly what was read, and
    // verification is still refused.
    let chunk_len = yah_plugin_ipc::MAX_ARTIFACT_READ_BYTES;
    let prefix = artifact_pattern(chunk_len as u64);
    let mut prefix_offer = real_offer.clone();
    prefix_offer.digest_blake3 = blake3::hash(&prefix).to_hex().to_string();
    let mut reader = ArtifactReader::new(&ep, prefix_offer, 1 << 20).expect("within limit");
    let chunk = reader
        .next_chunk()
        .await
        .expect("the first chunk pulls")
        .expect("not yet exhausted");
    assert_eq!(chunk, prefix, "the fixture's chunk matches the pattern");
    assert_eq!(
        reader.verify(),
        Err(EndpointError::Refused(Refusal::InvalidField(
            "artifact verified before it was fully read"
        ))),
        "a digest over a strict prefix proves nothing"
    );

    // The same reader, driven to completion against the true digest,
    // verifies — the gate passes only complete reads.
    let mut full_offer = real_offer.clone();
    full_offer.digest_blake3 = blake3::hash(&artifact_pattern(200_000))
        .to_hex()
        .to_string();
    let mut reader = ArtifactReader::new(&ep, full_offer.clone(), 1 << 20).expect("within limit");
    while reader.next_chunk().await.expect("chunks pull").is_some() {}
    reader.verify().expect("complete read plus matching digest");

    // Complete read plus wrong digest: rejected.
    let mut wrong_offer = real_offer;
    wrong_offer.digest_blake3 = blake3::hash(b"wrong").to_hex().to_string();
    let mut reader = ArtifactReader::new(&ep, wrong_offer, 1 << 20).expect("within limit");
    while reader.next_chunk().await.expect("chunks pull").is_some() {}
    assert_eq!(
        reader.verify(),
        Err(EndpointError::Refused(Refusal::InvalidField(
            "artifact digest mismatch"
        ))),
        "completion without the matching digest is still refused"
    );

    stop_active(activation, &rig.registry, rig.epoch).await;
}

/// A zero-byte offer is illegal by protocol law, whatever its metadata.
#[tokio::test]
async fn zero_byte_offers_are_refused_before_any_pull() {
    let revision = lifecycle_revision("zero", 'f');
    let (driver, _observer) = ProcessPluginDriver::scripted(
        revision.id().clone(),
        DriverKind::NodeProcess,
        worker_program(),
        vec![ProcActivationPlan::worker("spill:64")],
    );
    let mut rig = Rig::new("proc.zero", &revision);
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
    let call = ep
        .call("tool.make", json!(null), None)
        .await
        .expect("admits");
    let mut offer = match settled_within(call).await {
        CallTerminal::Completed(Outcome::Spilled { artifact }) => artifact,
        other => panic!("expected the spilled offer, got {other:?}"),
    };
    offer.bytes = 0;
    assert!(ArtifactReader::new(&ep, offer, 1 << 20).is_err());
    stop_active(activation, &rig.registry, rig.epoch).await;
}

/// Boot one poisoned-spill activation in the caller's scope: the
/// activation borrows the rig's slot, so the macro binds the pieces as
/// statements instead of returning them.
macro_rules! poisoned_spill {
    ($label:expr, $mode:expr, $driver:ident, $rig:ident, $activation:ident, $ep:ident, $offer:ident) => {
        let revision = lifecycle_revision($label, '8');
        let ($driver, _observer) = ProcessPluginDriver::scripted(
            revision.id().clone(),
            DriverKind::NodeProcess,
            worker_program(),
            vec![ProcActivationPlan::worker(match $mode {
                "short" => "spill-poison:short",
                "long" => "spill-poison:long",
                "media" => "spill-poison:media",
                "upper" => "spill-poison:upper",
                "junk" => "spill-poison:junk",
                other => unreachable!("no such poison mode: {other}"),
            })],
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
        let id = $activation.id().clone();
        let _handle = $activation.activate(&$rig.registry).await.expect("starts");
        let $ep = endpoint(&$driver, &id);
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

/// Chunk replies that disagree with the request's length — short or long —
/// are refused, never hashed.
#[tokio::test]
async fn artifact_chunks_must_match_the_requested_length() {
    for mode in ["short", "long"] {
        poisoned_spill!(
            Box::leak(format!("poison-len-{}", mode).into_boxed_str()),
            mode,
            driver,
            rig,
            activation,
            ep,
            offer
        );
        let mut reader = ArtifactReader::new(&ep, offer, 1 << 20).expect("within limit");
        assert_eq!(
            reader.next_chunk().await,
            Err(EndpointError::Refused(Refusal::InvalidField(
                "artifact read chunk length disagrees with the request"
            ))),
            "mode {mode} must refuse on the first chunk"
        );
        stop_active(activation, &rig.registry, rig.epoch).await;
    }
}

/// A reply that contradicts the offer's media type is refused; so is
/// noncanonical hex — uppercase digits, sign prefixes, anything outside
/// lowercase ASCII pairs.
#[tokio::test]
async fn artifact_metadata_and_hex_are_canonical() {
    for (mode, needle) in [
        ("media", "media type"),
        ("upper", "canonical"),
        ("junk", "canonical"),
    ] {
        poisoned_spill!(
            Box::leak(format!("poison-meta-{}", mode).into_boxed_str()),
            mode,
            driver,
            rig,
            activation,
            ep,
            offer
        );
        let mut reader = ArtifactReader::new(&ep, offer, 1 << 20).expect("within limit");
        match reader.next_chunk().await {
            Err(EndpointError::Refused(Refusal::InvalidField(detail))) => {
                assert!(
                    detail.contains(needle),
                    "mode {mode}: unexpected refusal {detail}"
                );
            }
            other => panic!("mode {mode}: expected a typed refusal, got {other:?}"),
        }
        stop_active(activation, &rig.registry, rig.epoch).await;
    }
}
