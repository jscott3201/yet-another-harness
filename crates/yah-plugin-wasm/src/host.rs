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

use crate::bindings::yah::plugin::{cancellation, logging};

/// How many log records one activation retains before the host stops storing.
///
/// A guest controls how often it logs, so unbounded retention would let it grow
/// host memory without touching a capability. Dropped records are counted so
/// the loss stays visible instead of silent. Byte bounds on the strings
/// themselves belong with the rest of the resource limits.
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

    pub fn cancellation_polls(&self) -> usize {
        self.cancellation_polls.load(Ordering::Acquire)
    }
}

/// Per-store host state handed to the generated import implementations.
pub struct HostState {
    observer: HostObserver,
}

impl HostState {
    pub const fn new(observer: HostObserver) -> Self {
        Self { observer }
    }

    pub const fn observer(&self) -> &HostObserver {
        &self.observer
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
        records.push(LogRecord {
            level,
            message,
            fields: fields
                .into_iter()
                .map(|field| (field.key, field.value))
                .collect(),
        });
    }
}

impl cancellation::Host for HostState {
    fn is_cancelled(&mut self) -> bool {
        self.observer
            .cancellation_polls
            .fetch_add(1, Ordering::AcqRel);
        self.observer.is_cancelled()
    }
}
