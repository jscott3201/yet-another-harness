//! Host state backing the two imports the conformance world requires.
//!
//! Both imports are baseline context rather than granted capabilities. Logging
//! accepts inert strings and cancellation is read-only, so neither carries
//! authority a guest could spend. Granted capabilities stay out of this world
//! until a capability-resource profile exists to carry them.

use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};

use crate::{
    bindings::yah::plugin::{cancellation, logging},
    limits::{ActivationLimiter, WasmLimits},
};

/// How many log records one activation retains before the host stops storing.
///
/// A guest controls how often it logs, so unbounded retention would let it grow
/// host memory without touching a capability. Dropped records are counted so
/// the loss stays visible instead of silent.
pub const RETAINED_LOG_RECORDS: usize = 64;

/// One structured record a guest emitted through the `logging` import.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LogRecord {
    pub level: logging::LogLevel,
    pub message: String,
    pub fields: Vec<(String, String)>,
}

/// Whatever the host consults to answer the guest's cancellation import.
///
/// Taking a probe rather than a concrete token keeps this crate independent of
/// the composition layer's cancellation type and lets the import be exercised
/// directly in tests.
pub type CancellationSource = Arc<dyn Fn() -> bool + Send + Sync>;

/// Shared, activation-scoped view of what the guest observed and emitted.
///
/// The driver keeps this outside the Wasmtime store so health and evidence
/// reads never contend with a store lock held across a guest call.
#[derive(Clone, Default)]
pub struct HostObserver {
    cancelled: Arc<AtomicBool>,
    source: Option<CancellationSource>,
    records: Arc<Mutex<Vec<LogRecord>>>,
    dropped: Arc<AtomicUsize>,
    truncated: Arc<AtomicUsize>,
    cancellation_polls: Arc<AtomicUsize>,
}

impl HostObserver {
    pub fn new() -> Self {
        Self::default()
    }

    /// Answer the guest's cancellation import from a host-owned signal.
    pub fn observing(source: CancellationSource) -> Self {
        Self {
            source: Some(source),
            ..Self::default()
        }
    }

    /// Ask every live instance sharing this observer to stop cooperatively.
    ///
    /// This is advisory, and independent of the host signal: a guest that never
    /// calls `is-cancelled` observes neither, which is why host-owned teardown
    /// remains the real authority.
    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::Release);
    }

    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
            || self.source.as_ref().is_some_and(|source| source())
    }

    pub fn records(&self) -> Vec<LogRecord> {
        self.records
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    pub fn dropped_records(&self) -> usize {
        self.dropped.load(Ordering::Acquire)
    }

    /// Drop retained records, keeping the counts that describe them.
    ///
    /// Record contents are guest-sized, so an observation that outlives its
    /// store must not keep them. What was seen, dropped, and clipped is small
    /// and stays.
    pub fn release_records(&self) {
        self.records
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clear();
    }

    /// Values clipped to the byte ceiling, counted so the loss is not silent.
    pub fn truncated_values(&self) -> usize {
        self.truncated.load(Ordering::Acquire)
    }

    pub fn cancellation_polls(&self) -> usize {
        self.cancellation_polls.load(Ordering::Acquire)
    }
}

/// Per-store host state handed to the generated import implementations.
///
/// The store's resource limiter lives here because Wasmtime resolves it through
/// the store's data, so the ceilings travel with the activation rather than
/// with the engine every activation shares.
pub struct HostState {
    observer: HostObserver,
    limits: WasmLimits,
    limiter: ActivationLimiter,
}

impl HostState {
    pub fn new(observer: HostObserver) -> Self {
        Self::with_limits(observer, WasmLimits::default())
    }

    pub fn with_limits(observer: HostObserver, limits: WasmLimits) -> Self {
        Self {
            observer,
            limits,
            limiter: limits.limiter(),
        }
    }

    pub const fn observer(&self) -> &HostObserver {
        &self.observer
    }

    /// The ceilings Wasmtime consults when the guest asks to grow.
    pub const fn limiter(&mut self) -> &mut ActivationLimiter {
        &mut self.limiter
    }
}

impl logging::Host for HostState {
    fn log(&mut self, level: logging::LogLevel, message: String, fields: Vec<logging::LogField>) {
        let mut records = self
            .observer
            .records
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if records.len() >= RETAINED_LOG_RECORDS {
            self.observer.dropped.fetch_add(1, Ordering::AcqRel);
            return;
        }
        // A guest chooses how large each value is, so the count bound alone
        // does not bound the bytes. Retention is truncated rather than refused:
        // the record is evidence, and a clipped record is better evidence than
        // none. Truncation is counted so the doc claim stays checkable.
        let message = truncate_utf8(message, self.limits.max_log_message_bytes, &self.observer);
        let fields = fields
            .into_iter()
            .take(self.limits.max_log_fields)
            .map(|field| {
                (
                    truncate_utf8(field.key, self.limits.max_log_message_bytes, &self.observer),
                    truncate_utf8(
                        field.value,
                        self.limits.max_log_message_bytes,
                        &self.observer,
                    ),
                )
            })
            .collect::<Vec<_>>();
        records.push(LogRecord {
            level,
            message,
            fields,
        });
    }
}

/// Clip `value` to `limit` bytes without splitting a character.
fn truncate_utf8(mut value: String, limit: usize, observer: &HostObserver) -> String {
    if value.len() <= limit {
        return value;
    }
    observer.truncated.fetch_add(1, Ordering::AcqRel);
    let mut end = limit;
    while end > 0 && !value.is_char_boundary(end) {
        end -= 1;
    }
    value.truncate(end);
    value
}

impl cancellation::Host for HostState {
    fn is_cancelled(&mut self) -> bool {
        self.observer
            .cancellation_polls
            .fetch_add(1, Ordering::AcqRel);
        self.observer.is_cancelled()
    }
}
