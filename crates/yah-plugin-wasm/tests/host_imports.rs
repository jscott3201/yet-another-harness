//! Evidence for the two host imports and the fixture allocator's bounds.
//!
//! The checked-in fixture components import nothing, so a guest call cannot
//! reach `logging` or `cancellation`. These tests drive the host side directly
//! instead, so the retention bound and the cancellation signal are held by
//! something other than a comment.

use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use wasmtime::{
    Engine, Store,
    component::{Component, HasSelf, Linker},
};
use yah_plugin_wasm::{
    GuestProgram, RETAINED_LOG_RECORDS, WasmLimits,
    bindings::{
        Conformance,
        yah::plugin::{
            cancellation::Host as CancellationHost,
            logging::{Host as LoggingHost, LogField, LogLevel},
        },
    },
    host::{HostObserver, HostState},
};

#[test]
fn log_retention_is_bounded_and_counts_what_it_drops() {
    let observer = HostObserver::new();
    let mut state = HostState::new(observer.clone());

    let overflow = 5;
    for index in 0..RETAINED_LOG_RECORDS + overflow {
        state.log(
            LogLevel::Info,
            format!("record {index}"),
            vec![LogField {
                key: "index".to_owned(),
                value: index.to_string(),
            }],
        );
    }

    assert_eq!(observer.records().len(), RETAINED_LOG_RECORDS);
    assert_eq!(observer.dropped_records(), overflow);
    // Retention keeps the earliest records, so the bound cannot be satisfied by
    // silently rotating the buffer.
    assert_eq!(observer.records()[0].message, "record 0");
}

#[test]
fn cancellation_import_reports_the_host_signal() {
    let signal = Arc::new(AtomicBool::new(false));
    let probe = Arc::clone(&signal);
    let observer = HostObserver::observing(Arc::new(move || probe.load(Ordering::Acquire)));
    let mut state = HostState::new(observer.clone());

    assert!(!state.is_cancelled());
    signal.store(true, Ordering::Release);
    assert!(state.is_cancelled());
    assert_eq!(observer.cancellation_polls(), 2);
}

#[test]
fn advisory_cancel_is_independent_of_the_host_signal() {
    let observer = HostObserver::new();
    let mut state = HostState::new(observer.clone());

    assert!(!state.is_cancelled());
    observer.cancel();
    assert!(state.is_cancelled());
}

#[test]
fn fixture_allocator_traps_instead_of_returning_an_out_of_range_pointer() {
    let engine = Engine::default();
    let component = Component::new(&engine, GuestProgram::Conformant.text()).expect("compiles");
    let mut linker: Linker<HostState> = Linker::new(&engine);
    Conformance::add_to_linker::<HostState, HasSelf<HostState>>(&mut linker, |state| state)
        .expect("host imports link");
    let mut store = Store::new(&engine, HostState::new(HostObserver::new()));
    let bindings = Conformance::instantiate(&mut store, &component, &linker).expect("instantiates");

    // The fixture owns one 64KiB page, so lowering this string cannot fit.
    let oversized = "x".repeat(100_000);
    let refused = bindings
        .yah_plugin_fixture_tool()
        .call_invoke(&mut store, &oversized)
        .expect_err("an unsatisfiable allocation cannot succeed");

    // The point is *where* the refusal comes from. The allocator traps inside
    // the guest, so the failure carries a wasm backtrace. An allocator that
    // returned the pointer anyway would instead be caught one layer out, by
    // Wasmtime rejecting an out-of-range `realloc` result - and would leave its
    // bump pointer advanced past the end of memory.
    let reported = format!("{refused:?}");
    assert!(
        !reported.contains("realloc return"),
        "the fixture must refuse before Wasmtime has to reject a bad pointer: {reported}"
    );
    assert!(
        reported.contains("wasm backtrace"),
        "expected a guest-side trap: {reported}"
    );
}

#[test]
fn log_values_are_clipped_to_the_byte_ceiling_and_the_loss_is_counted() {
    let limits = WasmLimits {
        max_log_message_bytes: 16,
        max_log_fields: 2,
        ..WasmLimits::default()
    };
    let observer = HostObserver::new();
    let mut state = HostState::with_limits(observer.clone(), limits);

    // A multi-byte character straddling the ceiling must not be split. The
    // character is three bytes and the ceiling is 16, so no whole number of
    // them lands on the boundary: the clip *must* step back to 15 to stay on a
    // character boundary. A two-byte character would divide the ceiling evenly
    // and never exercise that step.
    state.log(
        LogLevel::Warn,
        "…".repeat(40),
        (0..10)
            .map(|index| LogField {
                key: format!("k{index}"),
                value: "v".repeat(100),
            })
            .collect(),
    );

    let records = observer.records();
    assert_eq!(records.len(), 1);
    // 15, not 16: the ceiling is not a multiple of the character width, so the
    // clip had to walk back. Asserting the exact length is what makes a
    // regression to naive byte truncation fail here.
    assert_eq!(
        records[0].message.len(),
        15,
        "clip must stop at the last whole character below the ceiling: {:?}",
        records[0].message
    );
    assert!(
        records[0].message.chars().all(|c| c == '…'),
        "clipping split a character: {:?}",
        records[0].message
    );
    assert_eq!(records[0].fields.len(), limits.max_log_fields);
    assert!(observer.truncated_values() > 0);
}

#[test]
fn the_epoch_ticker_thread_stops_when_its_ticker_is_dropped() {
    let mut config = wasmtime::Config::new();
    config.epoch_interruption(true);
    let engine = Engine::new(&config).expect("engine accepts epoch interruption");

    let tick = std::time::Duration::from_millis(5);
    let ticker = yah_plugin_wasm::EpochTicker::start(&engine, tick);
    let counter = ticker.tick_counter();

    std::thread::sleep(tick * 20);
    assert!(
        counter.load(std::sync::atomic::Ordering::Acquire) > 0,
        "a live ticker must advance the epoch"
    );

    drop(ticker);
    let after_drop = counter.load(std::sync::atomic::Ordering::Acquire);
    std::thread::sleep(tick * 20);
    let settled = counter.load(std::sync::atomic::Ordering::Acquire);

    // At most one tick may land: the thread reads the stop flag between sleeps.
    // The sleeps above are a wide multiple of the tick so a loaded machine
    // still sees the thread run before the drop and stop after it.
    assert!(
        settled <= after_drop + 1,
        "the ticker thread kept running after its ticker was dropped: {after_drop} -> {settled}"
    );
}
