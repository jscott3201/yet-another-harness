//! The example plugins, built from source, run under the real driver.
//!
//! These are not fixtures. The corpus in `crates/yah-plugin-wasm/guests` is
//! hand-written component text, each piece shaped to make one host bound
//! observable; these two are what a plugin author writes, compiled by the
//! toolchain that author would use. What they prove is different: that the WIT
//! world is a contract two unrelated toolchains can satisfy, and that the host
//! cannot tell which one it is talking to.
//!
//! They also prove the guest-to-host path from a real toolchain. Both guests
//! call `logging` and `cancellation`, so the byte budget, retained log
//! records, and per-poll panic guard are entered by authored code here, not
//! only by the flood and capability fixtures the corpus hand-writes.
//!
//! Artifacts come from `scripts/build-guests.sh` rather than from this test,
//! because building them needs two toolchains and one of them needs Node. A
//! missing artifact fails loudly with that instruction; nothing is skipped.

#[path = "support/fixtures.rs"]
mod fixtures;

use std::{path::PathBuf, sync::Arc};

use fixtures::revision;
use yah_compose::{
    ComponentDefinition, ComponentRevision, ComponentSlot, ComponentSlotOutcome,
    DesiredComponentState, ProviderAssignments, ProviderSelectionEpoch, ReconcileOutcome, Scope,
    ServiceRegistry,
};
use yah_plugin_host::{
    CapabilityBroker, DriverConformanceCase, EffectiveCapabilityGrants, HostPluginActivation,
    PluginRevision,
};
use yah_plugin_wasm::{
    LogRecord, ResourceState, WasmComponentDriver, WasmLimits, WasmObserver,
    bindings::yah::plugin::logging::LogLevel,
};

/// Where `scripts/build-guests.sh` leaves what it built.
fn artifact(name: &str) -> Vec<u8> {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../target/guests")
        .join(name);
    std::fs::read(&path).unwrap_or_else(|error| {
        panic!(
            "example guest artifact {} is missing ({error}). Run: bash scripts/build-guests.sh",
            path.display()
        )
    })
}

/// The Rust guest as a component.
///
/// `cargo build --target wasm32-unknown-unknown` produces a *core* module; the
/// component is that module plus the world it was generated against. Encoding
/// it here rather than in the build script keeps the guest toolchain to one
/// command, and the encoder is the same crate Wasmtime uses to read the result.
/// It is also the step a package loader will own, so doing it in the open is
/// worth more than hiding it behind a CLI.
fn rust_component() -> Vec<u8> {
    let core = artifact("rust-example.core.wasm");
    wit_component::ComponentEncoder::default()
        .module(&core)
        .expect("the core module carries its component type section")
        .validate(true)
        .encode()
        .expect("the core module encodes as a component")
}

fn ts_component() -> Vec<u8> {
    artifact("ts-example.component.wasm")
}

struct Rig {
    slot: ComponentSlot,
    registry: ServiceRegistry,
    broker: CapabilityBroker,
    grants: EffectiveCapabilityGrants,
    epoch: ProviderSelectionEpoch,
    revision: PluginRevision,
}

impl Rig {
    fn new(label: &str, digest: char) -> Self {
        let revision = revision(DriverConformanceCase::ReadyLifecycle, digest)
            .expect("fixture revision is valid");
        let registry = ServiceRegistry::new();
        let mut slot = ComponentSlot::new(label).expect("slot label is canonical");
        let desired = DesiredComponentState::enabled(
            slot.generation(1),
            ComponentRevision::new(
                format!("{label}.revision"),
                ComponentDefinition::new(format!("{label}.component")),
                Scope::root(format!("{label}.scope")),
            ),
            ProviderAssignments::new(),
        );
        let epoch = match slot
            .reconcile(&registry, desired)
            .expect("fresh component begins start")
        {
            ComponentSlotOutcome::Mounted {
                component: ReconcileOutcome::StartBegun { selection },
                ..
            } => selection.epoch(),
            other => panic!("fresh component did not begin start: {other:?}"),
        };
        let grants = EffectiveCapabilityGrants::empty(&revision);
        Self {
            slot,
            registry,
            broker: CapabilityBroker::new().expect("broker is constructible"),
            grants,
            epoch,
            revision,
        }
    }
}

/// What one guest did when the host activated it and called its tool.
struct GuestRun {
    /// One answer per [`EQUIVALENCE_INPUTS`] entry, in order.
    answers: Vec<String>,
    records: Vec<LogRecord>,
    cancellation_polls: usize,
}

/// Drive one authored component through the host's whole activation lifecycle.
///
/// Deliberately the host's path, not a linker this test built: preparation,
/// activation under the driver's limits and call deadline, a tool call on the
/// live store, then teardown that must leave the resource released. A guest
/// that only worked outside those bounds would pass a weaker test and fail a
/// real activation.
async fn run_guest(label: &str, digest: char, component: &[u8]) -> GuestRun {
    let mut rig = Rig::new(label, digest);
    let (driver, observer): (Arc<WasmComponentDriver>, WasmObserver) =
        WasmComponentDriver::for_component(
            rig.revision.id().clone(),
            component,
            WasmLimits::default(),
        )
        .expect("the example component compiles under the driver's own bounds");

    let mut activation = HostPluginActivation::prepare(
        &mut rig.slot,
        rig.epoch,
        &rig.broker,
        &rig.grants,
        driver.clone(),
    )
    .expect("preparation succeeds");
    let id = activation.id().clone();

    activation
        .activate(&rig.registry)
        .await
        .expect("the example guest activates");
    assert_eq!(observer.resource_state(&id), Ok(ResourceState::Live));

    // Every input on one activation, because that is also how a plugin is used:
    // a tool is called repeatedly on a store that stays live between calls.
    let mut answers = Vec::with_capacity(EQUIVALENCE_INPUTS.len());
    for input in EQUIVALENCE_INPUTS {
        answers.push(
            observer
                .call_fixture_tool(&id, input)
                .await
                .unwrap_or_else(|error| panic!("guest refused {input}: {error:?}")),
        );
    }

    let host = observer
        .host_observer(&id)
        .expect("a live activation has a host observer");
    let run = GuestRun {
        answers,
        records: host.records(),
        cancellation_polls: host.cancellation_polls(),
    };

    assert!(
        !run.records.is_empty(),
        "the guest must have logged before teardown, or releasing records proves nothing"
    );

    let (slot, _handle) = activation.release_active().expect("active releases");
    let removed = DesiredComponentState::removed(slot.generation(2));
    slot.reconcile(&rig.registry, removed)
        .expect("component begins stopping");
    slot.finish_stop(rig.epoch)
        .await
        .expect("cleanup completes");
    assert_eq!(observer.resource_state(&id), Ok(ResourceState::Released));
    // Teardown drops what the guest wrote. Record contents are guest-sized and
    // an observation outlives its store, so keeping them would make every
    // stopped activation a leak the host chose. Until these guests existed,
    // nothing had ever put a record there for teardown to release.
    assert!(
        host.records().is_empty(),
        "teardown must release the guest's log records: {:?}",
        host.records()
    );
    run
}

/// Inputs chosen because a JavaScript guest could plausibly answer them
/// differently from a Rust one.
///
/// Five of the nine diverged while the TypeScript guest round-tripped its input
/// through `JSON.parse` and `JSON.stringify`: both whitespace cases come back
/// normalised, `1.0` comes back as `1`, an integer past 2^53 comes back
/// rounded, and input that is not JSON throws, which reaches the host as a trap
/// rather than as the `invalid-input` the world declares.
///
/// The other four - canonical JSON, non-ASCII, escapes, and a bare array -
/// survive a round trip unchanged, and are here as controls: they must stay
/// identical, and a change that broke them would be caught by the same
/// assertion. Non-ASCII earns its place twice over, because it diverged in the
/// `bytes` field the guests log rather than in the answer, which is a
/// disagreement no amount of canonical JSON would have surfaced.
///
/// Testing only the canonical case is what made the equivalence claim look
/// true.
const EQUIVALENCE_INPUTS: &[&str] = &[
    r#"{"n":1}"#,
    r#"{"n": 1}"#,
    r#"{ "a" : [1, 2] }"#,
    r#"{"s":"héllo ☃"}"#,
    r#"{"q":"a\"b\\c"}"#,
    r#"[1,2,3]"#,
    r#"not json"#,
    r#"{"b":1.0}"#,
    r#"{"big":12345678901234567890}"#,
];

/// The two claims the pair exists to support, over one pair of runs.
///
/// Same world, same input, same answer apart from the value of the one field
/// each guest uses to name itself; and both guests reaching the host through
/// the logging and cancellation imports. If the host could tell them apart in
/// any other way, the world would not be the contract - so this asks across inputs that
/// give the two toolchains every opportunity to disagree, not just the one that
/// suits them.
///
/// One test rather than two, because Nextest runs each test in its own process
/// and the two properties are views of the same run. Split, the pair compiled
/// both components twice instead of once - the TypeScript one is the 12 MB half
/// of that - and ran two multi-second compiles concurrently, mid-run, beside the
/// two long subscription cases. The assertions stay separated, and each still
/// names which property and which guest broke.
///
/// What prompted it: the split failed the pre-push gate twice, both times on a
/// three-`assert_eq!` test in another crate overrunning the suite's 1s
/// leak-timeout; merged, the gate has been clean four times running. The
/// mechanism is *not* established, and the obvious guess is wrong: this build
/// resolves Wasmtime without `parallel-compilation`, so the compile runs on one
/// thread, and this case measures 15.04s user against 15.28s real on a ten-core
/// machine - 0.98 cores, not ten. `scripts/test.sh ci` also passed on its own
/// either way, which no explanation resting on suite composition alone
/// accounts for. Halving the work is what correlates with the gate passing;
/// why that is enough is open.
#[tokio::test]
async fn both_example_guests_behave_identically_through_the_host() {
    let rust = run_guest("wasm.example.rust", '1', &rust_component()).await;
    let typescript = run_guest("wasm.example.ts", '2', &ts_component()).await;

    assert_answers_are_indistinguishable(&rust, &typescript);
    assert_reached_the_host("rust", &rust);
    assert_reached_the_host("typescript", &typescript);
}

/// Same input, same answer, apart from the value of the self-naming field.
fn assert_answers_are_indistinguishable(rust: &GuestRun, typescript: &GuestRun) {
    assert_eq!(rust.answers[0], "{\"echo\":{\"n\":1},\"from\":\"rust\"}");
    assert_eq!(
        typescript.answers[0],
        "{\"echo\":{\"n\":1},\"from\":\"typescript\"}"
    );
    for (index, input) in EQUIVALENCE_INPUTS.iter().enumerate() {
        // Anchored on the whole envelope tail rather than replacing "rust"
        // wherever it appears. The loose form passes today only because no
        // input happens to contain that word: the answer echoes the input, so
        // one added case carrying it would have the rust answer's echo rewritten
        // and the typescript answer's left alone, and the two would be compared
        // after this test had mangled one of them. Stripping a required suffix
        // also asserts the envelope shape, which the replace form never did.
        let rust_body = rust.answers[index]
            .strip_suffix(r#","from":"rust"}"#)
            .unwrap_or_else(|| {
                panic!(
                    "the rust guest must answer {input} in the world's envelope, but it \
                     answered {}",
                    rust.answers[index]
                )
            });
        assert_eq!(
            format!(r#"{rust_body},"from":"typescript"}}"#),
            typescript.answers[index],
            "the two guests must differ only in how they name themselves, but they \
             answered {input} differently"
        );
    }
}

/// A guest that refuses is reported as refusing, not as failing.
///
/// Both examples are written to return `invalid-input` for an empty request,
/// which is the world's own error rather than a trap; this case drives the Rust
/// one, because what is under test is the driver's handling rather than either
/// guest's. The driver has to carry that back as a returned error: a guest that
/// declined is not a guest that broke, and the distinction is the difference
/// between retrying the call and tearing the plugin down.
///
/// The same case pins the weak handle the observation holds. After teardown the
/// core is gone, so the driver cannot reach a store at all - and reports that,
/// rather than reaching a live-looking one.
#[tokio::test]
async fn a_guest_that_declines_is_reported_as_declining_and_then_as_gone() {
    let mut rig = Rig::new("wasm.example.declines", '5');
    let (driver, observer): (Arc<WasmComponentDriver>, WasmObserver) =
        WasmComponentDriver::for_component(
            rig.revision.id().clone(),
            &rust_component(),
            WasmLimits::default(),
        )
        .expect("the example component compiles");
    let mut activation = HostPluginActivation::prepare(
        &mut rig.slot,
        rig.epoch,
        &rig.broker,
        &rig.grants,
        driver.clone(),
    )
    .expect("preparation succeeds");
    let id = activation.id().clone();
    activation
        .activate(&rig.registry)
        .await
        .expect("the example guest activates");

    let declined = observer
        .call_fixture_tool(&id, "")
        .await
        .expect_err("an empty request is refused by the guest");
    assert!(
        declined.summary().contains("InvalidInput"),
        "the guest's own error code must survive to the host: {}",
        declined.summary()
    );
    // Refusing is not failing: the activation is still live and still answers.
    assert_eq!(observer.resource_state(&id), Ok(ResourceState::Live));
    observer
        .call_fixture_tool(&id, EQUIVALENCE_INPUTS[0])
        .await
        .expect("a declined call must not poison the activation");

    let (slot, _handle) = activation.release_active().expect("active releases");
    let removed = DesiredComponentState::removed(slot.generation(2));
    slot.reconcile(&rig.registry, removed)
        .expect("component begins stopping");
    slot.finish_stop(rig.epoch)
        .await
        .expect("cleanup completes");

    let gone = observer
        .call_fixture_tool(&id, EQUIVALENCE_INPUTS[0])
        .await
        .expect_err("a torn-down activation has nothing to call");
    assert!(
        gone.summary().contains("no live activation"),
        "the driver must report the activation as gone rather than as storeless, \
         which is what the observation's weak handle is for: {}",
        gone.summary()
    );
}

/// Toolchain-built guests calling back into the host, observed host-side.
///
/// Logging and cancellation both, from authored code rather than from the
/// hand-written flood and capability fixtures that also enter this path.
fn assert_reached_the_host(name: &str, run: &GuestRun) {
    assert_eq!(
        run.records.len(),
        EQUIVALENCE_INPUTS.len() + 1,
        "{name} guest should log once at activation and once per call: {:?}",
        run.records
    );
    assert_eq!(run.records[0].level, LogLevel::Info);
    assert!(
        run.records[0].message.contains("activated"),
        "{name}: {:?}",
        run.records[0]
    );
    assert_eq!(run.records[1].level, LogLevel::Debug);
    // The field carries the request size, which is the one thing in these
    // records the guest computed rather than spelled out - so it is the one
    // that can disagree between toolchains without the answer changing.
    // Read on a non-ASCII input on purpose: a JavaScript string's length is
    // in UTF-16 code units, so an ASCII-only assertion cannot tell the two
    // quantities apart, and this one reported 15 where Rust reported 18
    // until the TypeScript guest was made to count bytes.
    let non_ascii = EQUIVALENCE_INPUTS
        .iter()
        .position(|input| !input.is_ascii())
        .expect("the corpus must hold a non-ASCII input for this to mean anything");
    assert_eq!(
        run.records[non_ascii + 1].fields,
        vec![(
            "bytes".to_owned(),
            EQUIVALENCE_INPUTS[non_ascii].len().to_string()
        )],
        "{name} guest should report the request size in UTF-8 bytes"
    );
    assert!(
        run.cancellation_polls >= 1,
        "{name} guest should ask whether it was cancelled"
    );
}
