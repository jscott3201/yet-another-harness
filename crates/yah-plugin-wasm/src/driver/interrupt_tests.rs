//! In-crate cases for the parts of the interrupt the host path cannot reach.
//!
//! Split from `driver.rs` to keep that file under the repository's
//! reviewable-size cap; these drive `ActivationCore` directly and are not
//! reachable through the host, for the reason the parent module records.

use std::{thread, time::Duration, time::Instant};

use super::*;

/// One engine, as the driver has: activations that shared nothing could not
/// falsify an engine-scoped or ticker-scoped kill, which is the only way
/// "one activation's kill reaches another" could plausibly go wrong.
fn engine(limits: WasmLimits) -> Engine {
    limits.engine().expect("engine accepts its configuration")
}

/// Drive one future to completion on this thread.
///
/// These cases reach past the host into `ActivationCore`, whose calls are
/// futures now that a guest call runs on its own stack. A current-thread
/// runtime is the smallest thing that can resume one.
fn block_on<T>(future: impl std::future::Future<Output = T>) -> T {
    tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("a current-thread runtime is constructible")
        .block_on(future)
}

fn core(engine: &Engine, program: GuestProgram, limits: WasmLimits) -> Arc<ActivationCore> {
    let component = Component::new(engine, program.text()).expect("fixture component compiles");
    Arc::new(ActivationCore {
        engine: engine.clone(),
        component,
        limits,
        interrupt: GuestInterrupt::new(),
        observation: Arc::new(ActivationObservation::new(HostObserver::new())),
        live: tokio::sync::Mutex::new(None),
    })
}

/// A budget so large that only a kill can end the call.
///
/// If the budget could also end it, this case would pass with `kill` gutted
/// - which is exactly the hole it exists to close.
fn kill_only_limits() -> WasmLimits {
    WasmLimits {
        epoch_tick: Duration::from_millis(5),
        call_budget_ticks: u64::MAX,
        ..WasmLimits::default()
    }
}

#[test]
fn a_kill_stops_a_call_that_is_already_running() {
    let limits = kill_only_limits();
    let engine = engine(limits);
    let core = core(&engine, GuestProgram::Runaway, limits);
    let ticker = EpochTicker::start(&engine, limits.epoch_tick);
    block_on(core.instantiate()).expect("the runaway instantiates");

    // The result comes back over a channel rather than from `join`. The
    // budget here is effectively infinite by design, so if the kill fails
    // to land the guest runs forever - and `join` would hang the suite
    // instead of failing it, which is the failure mode this case exists to
    // detect. A leaked thread is the price of reporting that.
    let (done, result) = std::sync::mpsc::channel();
    let calling = Arc::clone(&core);
    thread::spawn(move || done.send(block_on(calling.call_activate())));

    // Wait for evidence the guest is actually inside the call, rather than
    // sleeping and hoping. A nonzero tick count means the guest reached an
    // epoch deadline, which it can only do from inside `activate`. Sleeping
    // instead would let a slow scheduler turn this into a test of the entry
    // check, which passes for the wrong reason.
    while core.interrupt.ticks_used() == 0 {
        thread::sleep(Duration::from_millis(1));
    }
    let killed_at = Instant::now();
    core.interrupt.kill();

    let outcome = result
        .recv_timeout(limits.epoch_tick * 200)
        .expect("a killed call must return; it did not");
    let stopped_after = killed_at.elapsed();
    drop(ticker);

    let failure = outcome.expect_err("a killed call must not report success");
    // A host-ordered stop must not be reported as a guest overrun. The
    // budget here is effectively infinite, so "exceeded its call deadline"
    // would be a false statement about why the call ended.
    assert!(
        failure.summary().contains("stopped by the host"),
        "a kill must be named as a kill, not as a deadline: {}",
        failure.summary()
    );
    // The mechanism's claim is one tick. The receive above already bounds
    // this; recording it keeps the number in the failure message.
    assert!(
        stopped_after < limits.epoch_tick * 200,
        "the kill took {stopped_after:?}, which is not a tick-bounded stop"
    );
}

#[test]
fn a_killed_activation_is_refused_before_it_enters_the_guest() {
    let limits = kill_only_limits();
    let engine = engine(limits);
    let core = core(&engine, GuestProgram::Conformant, limits);
    let _ticker = EpochTicker::start(&engine, limits.epoch_tick);
    block_on(core.instantiate()).expect("the conformant guest instantiates");

    // This guest returns in microseconds, so it would never reach an epoch
    // deadline and never consult the flag. Entry is the only place a stop
    // can be read in time.
    core.interrupt.kill();
    let failure =
        block_on(core.call_activate()).expect_err("a killed activation must not run guest code");
    assert!(
        failure.summary().contains("stopped before activate"),
        "the refusal must name why it was refused: {}",
        failure.summary()
    );
}

/// The reason guest calls run on their own stack at all.
///
/// Both activations are driven by ONE current-thread runtime, so there is
/// exactly one thread between them. A guest call that held that thread
/// would starve its neighbour completely: the healthy activation could not
/// even begin until the runaway hit its deadline. The runaway's budget here
/// is long enough that such a wait would be unmistakable.
#[test]
fn a_compute_bound_guest_does_not_starve_a_sibling_on_the_same_thread() {
    let limits = WasmLimits {
        epoch_tick: Duration::from_millis(5),
        // Long enough that "the healthy one waited for the runaway" and
        // "the healthy one interleaved with it" cannot be confused.
        call_budget_ticks: 200,
        ..WasmLimits::default()
    };
    let engine = engine(limits);
    let spinner = core(&engine, GuestProgram::Runaway, limits);
    let healthy = core(&engine, GuestProgram::Conformant, limits);
    let ticker = EpochTicker::start(&engine, limits.epoch_tick);
    // The assertion below counts ticks the ticker actually delivered rather
    // than milliseconds. The two agree on an idle machine and diverge on a
    // loaded one: the ticker's sleeps stretch under load, so a wall-clock
    // bound tightens exactly when the machine is least able to meet it,
    // while the property being claimed - the sibling gets in within a tick
    // or two - is about ticks and holds at any speed.
    let ticks = ticker.tick_counter();

    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("a current-thread runtime is constructible");
    runtime.block_on(async {
        spinner
            .instantiate()
            .await
            .expect("the runaway instantiates");
        healthy
            .instantiate()
            .await
            .expect("the healthy guest instantiates");

        let started = Instant::now();
        let started_ticks = ticks.load(Ordering::Acquire);
        let mut healthy_waited = None;
        let (spun, _) = tokio::join!(spinner.call_activate(), async {
            let outcome = healthy.call_activate().await;
            healthy_waited = Some((
                ticks.load(Ordering::Acquire).saturating_sub(started_ticks),
                started.elapsed(),
            ));
            outcome.expect("a healthy guest activates while a sibling spins");
        });
        let spinner_took = started.elapsed();

        let spun = spun.expect_err("the runaway must still hit its deadline");
        assert!(
            spun.summary().contains("call deadline"),
            "the runaway must end on its deadline, not some other way: {}",
            spun.summary()
        );

        let (healthy_ticks, healthy_took) = healthy_waited.expect("the healthy guest completed");
        // Measured against the mechanism's own unit, not against the
        // runaway's total. A ratio would accept a sibling that waited
        // dozens of ticks as long as the runaway ran longer still; the
        // property being claimed is that the sibling gets in within about
        // one tick. The shipped code lands on exactly one when idle and at
        // ordinary load, and on a handful when the machine is oversubscribed
        // several times over, so ten leaves room without being so wide that
        // it stops failing anything that yields rarely instead of every
        // tick.
        assert!(
            healthy_ticks < 10,
            "the healthy guest waited {healthy_ticks} ticks ({healthy_took:?}) \
             against the runaway's {spinner_took:?}; it is not interleaving \
             per tick"
        );
    });
}

/// The tool path reads the same entry check the lifecycle path does.
///
/// `call_fixture_tool` is the first entry point a host can drive repeatedly, so
/// it is the first one where "this activation was stopped" has to be re-read
/// rather than assumed from the last call.
#[test]
fn a_killed_activation_is_refused_before_it_enters_the_guest_tool() {
    let limits = kill_only_limits();
    let engine = engine(limits);
    let core = core(&engine, GuestProgram::Conformant, limits);
    let _ticker = EpochTicker::start(&engine, limits.epoch_tick);
    block_on(core.instantiate()).expect("the conformant guest instantiates");
    block_on(core.call_activate()).expect("it activates");
    // It answers while live, or the refusal below would prove nothing.
    block_on(core.call_fixture_tool("{}")).expect("a live guest answers its tool");

    core.interrupt.kill();
    let failure =
        block_on(core.call_fixture_tool("{}")).expect_err("a killed activation must not run");
    assert!(
        failure.summary().contains("stopped before invoke"),
        "the refusal must name why it was refused: {}",
        failure.summary()
    );
}

/// A faulted activation refuses the next call, which is what health promises.
///
/// Wasmtime poisons a store whose guest trapped, so this particular fault would
/// also be refused one layer out - but a host panic caught mid-call leaves a
/// store Wasmtime still considers callable, and the host's own check is what
/// covers both. The runaway fixture traps on `invoke` because lowering the
/// argument runs its trapping allocator, which is a fault the corpus already
/// has.
#[test]
fn a_faulted_activation_refuses_the_next_tool_call() {
    let limits = kill_only_limits();
    let engine = engine(limits);
    let core = core(&engine, GuestProgram::Runaway, limits);
    let _ticker = EpochTicker::start(&engine, limits.epoch_tick);
    block_on(core.instantiate()).expect("the runaway guest instantiates");

    let trapped = block_on(core.call_fixture_tool("{}"))
        .expect_err("the runaway fixture traps when its allocator runs");
    assert!(
        trapped.summary().contains("trapped"),
        "the first failure must be the trap itself: {}",
        trapped.summary()
    );
    assert!(
        core.observation.faulted.load(Ordering::Acquire),
        "a trap must mark the activation unable to be entered"
    );

    let refused = block_on(core.call_fixture_tool("{}"))
        .expect_err("a faulted activation must not be entered again");
    // Asserted on the host guard's own wording, not on "earlier failure", which
    // both messages carry. Wasmtime poisons a trapped store itself, so the loose
    // form was satisfied by `describe_guest_failure`'s `CannotEnterComponent`
    // arm - "guest invoke was refused because an earlier failure poisoned this
    // activation" - and passed with the guard it exists for deleted.
    assert!(
        refused.summary().contains("left it unable to be entered"),
        "the refusal must come from the host's own entry check rather than from \
         Wasmtime's poisoned store: {}",
        refused.summary()
    );
}

#[test]
fn a_live_activation_still_runs_after_a_sibling_core_is_killed() {
    let limits = kill_only_limits();
    // Both cores on one engine under one ticker, as the driver runs them.
    let engine = engine(limits);
    let killed = core(&engine, GuestProgram::Conformant, limits);
    let live = core(&engine, GuestProgram::Conformant, limits);
    let _ticker = EpochTicker::start(&engine, limits.epoch_tick);
    block_on(killed.instantiate()).expect("first guest instantiates");
    block_on(live.instantiate()).expect("second guest instantiates");

    killed.interrupt.kill();
    block_on(live.call_activate()).expect("a kill on one activation must not reach another");
}
