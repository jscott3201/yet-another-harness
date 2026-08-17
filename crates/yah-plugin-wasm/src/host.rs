//! Host state backing the three imports the conformance world requires.
//!
//! Logging and cancellation are baseline context rather than granted
//! capabilities: inert strings one way, a read-only bit the other, so neither
//! carries authority a guest could spend. The `capabilities` import is the
//! exception this module exists to keep honest - its implementation lives in
//! [`crate::capability`], and the authority it mediates enters here only as
//! the activation's admitted [`PluginStartContext`], never as a provider
//! reference the state could hold past revocation.

use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};

use wasmtime::component::ResourceTable;
use yah_plugin_host::PluginStartContext;

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
    capability_acquires: Arc<AtomicUsize>,
    capability_acquire_refusals: Arc<AtomicUsize>,
    capability_calls: Arc<AtomicUsize>,
    capability_call_refusals: Arc<AtomicUsize>,
    live_capability_handles: Arc<AtomicUsize>,
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

    /// Host heap the retained records actually hold, in bytes.
    ///
    /// This reads capacity, not length, and reads it from the stored records
    /// rather than a copy. Both matter: a clipped `String` keeps whatever
    /// capacity the guest's message arrived with unless the clip reallocates,
    /// and `records()` hands out clones whose capacity is exactly their length,
    /// so a copy cannot show the difference.
    ///
    /// The same is true of the vectors, which is why they are counted too. A
    /// guest can send a very large number of fields whose strings are all
    /// empty; every string capacity is then zero while the field vector behind
    /// them is enormous. Counting only the strings would report nothing.
    pub fn retained_bytes(&self) -> usize {
        let records = self
            .records
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let spine = records.capacity() * std::mem::size_of::<LogRecord>();
        records
            .iter()
            .map(|record| {
                record.message.capacity()
                    + record.fields.capacity() * std::mem::size_of::<(String, String)>()
                    + record
                        .fields
                        .iter()
                        .map(|(key, value)| key.capacity() + value.capacity())
                        .sum::<usize>()
            })
            .sum::<usize>()
            + spine
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

    /// Capability acquires the guest attempted, admitted or not.
    pub fn capability_acquires(&self) -> usize {
        self.capability_acquires.load(Ordering::Acquire)
    }

    /// Acquires refused, for any reason the `acquire-error-code` enum names.
    ///
    /// Counted as an admitted/refused pair rather than per code: the code
    /// itself is evidence the guest's own answer already carries, and a
    /// counter nothing asserts is not evidence.
    pub fn capability_acquire_refusals(&self) -> usize {
        self.capability_acquire_refusals.load(Ordering::Acquire)
    }

    /// Capability calls the guest attempted through a held resource.
    pub fn capability_calls(&self) -> usize {
        self.capability_calls.load(Ordering::Acquire)
    }

    /// Calls refused before or by the provider.
    pub fn capability_call_refusals(&self) -> usize {
        self.capability_call_refusals.load(Ordering::Acquire)
    }

    /// Capability resources the guest holds live - a gauge, not a total.
    ///
    /// Decremented when the guest calls `resource.drop` and equally when the
    /// store drops with resources still held, because both paths drop the table
    /// entry - so the decrement is a release, not a `drop` call, and a guest
    /// that never drops anything still reads zero here after teardown.
    pub fn live_capability_handles(&self) -> usize {
        self.live_capability_handles.load(Ordering::Acquire)
    }

    pub(crate) fn count_capability_acquire_attempt(&self) {
        self.capability_acquires.fetch_add(1, Ordering::AcqRel);
    }

    pub(crate) fn count_capability_acquire_refusal(&self) {
        self.capability_acquire_refusals
            .fetch_add(1, Ordering::AcqRel);
    }

    pub(crate) fn count_capability_call_attempt(&self) {
        self.capability_calls.fetch_add(1, Ordering::AcqRel);
    }

    pub(crate) fn count_capability_call_refusal(&self) {
        self.capability_call_refusals.fetch_add(1, Ordering::AcqRel);
    }

    pub(crate) fn live_capability_handles_counter(&self) -> Arc<AtomicUsize> {
        Arc::clone(&self.live_capability_handles)
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
    /// The activation's admitted capability context, when one exists.
    ///
    /// `None` means the store was built outside the host lifecycle - direct
    /// linker tests, mostly - and reads as the absence of every grant rather
    /// than as a distinct state a guest could observe.
    context: Option<PluginStartContext>,
    /// Live guest-held capability entries, keyed by the handles Wasmtime
    /// manages. Capacity is left at its default because the real ceiling is
    /// [`WasmLimits::max_capability_handles`], enforced before any push.
    table: ResourceTable,
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
            context: None,
            table: ResourceTable::new(),
        }
    }

    /// State for an activation the host admitted, grants and all.
    pub fn with_grants(
        observer: HostObserver,
        limits: WasmLimits,
        context: PluginStartContext,
    ) -> Self {
        Self {
            context: Some(context),
            ..Self::with_limits(observer, limits)
        }
    }

    pub const fn observer(&self) -> &HostObserver {
        &self.observer
    }

    /// The ceilings Wasmtime consults when the guest asks to grow.
    pub const fn limiter(&mut self) -> &mut ActivationLimiter {
        &mut self.limiter
    }

    pub(crate) const fn limits(&self) -> &WasmLimits {
        &self.limits
    }

    pub(crate) const fn capability_context(&self) -> Option<&PluginStartContext> {
        self.context.as_ref()
    }

    pub(crate) const fn capability_table(&mut self) -> &mut ResourceTable {
        &mut self.table
    }

    pub(crate) const fn capability_table_ref(&self) -> &ResourceTable {
        &self.table
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
        // Built by pushing into a pre-sized vector rather than by `collect`.
        // `LogField` and `(String, String)` have the same size and alignment,
        // which is exactly the condition for Rust's in-place collect
        // specialisation: `collect` would hand back the guest's own buffer with
        // its length set to the field ceiling and its capacity untouched. The
        // host would then retain a vector sized by the guest while holding only
        // the fields it kept - the same escape as clipping a `String` in place,
        // one level up, and a larger one, because a field costs 48 bytes even
        // when both of its strings are empty.
        let kept = self.limits.max_log_fields.min(fields.len());
        let mut clipped = Vec::with_capacity(kept);
        for field in fields.into_iter().take(kept) {
            clipped.push((
                truncate_utf8(field.key, self.limits.max_log_message_bytes, &self.observer),
                truncate_utf8(
                    field.value,
                    self.limits.max_log_message_bytes,
                    &self.observer,
                ),
            ));
        }
        let fields = clipped;
        records.push(LogRecord {
            level,
            message,
            fields,
        });
    }
}

/// Clip `value` to `limit` bytes without splitting a character.
///
/// The clipped text is copied into a fresh allocation rather than shortened in
/// place. `String::truncate` moves `len` and leaves `capacity` alone, so
/// retaining the clipped `String` would retain every byte the guest sent: a
/// 1 MiB message clipped to 4 KiB would still hold 1 MiB of host heap, and the
/// ceiling would bound only the length field. Since the point of the ceiling is
/// what the host *keeps*, the copy is the ceiling.
fn truncate_utf8(mut value: String, limit: usize, observer: &HostObserver) -> String {
    if value.len() <= limit {
        // Under the ceiling by length, but not necessarily by allocation. The
        // capacity comes from the canonical ABI's lift, and a guest chooses its
        // own `string-encoding` on `canon lower`: decoding `latin1+utf16`
        // allocates about twice the byte length. Returning the string as it
        // arrived would leave the host holding that slack, so a bound that only
        // applied when something was clipped would not be a bound at all.
        // `shrink_to_fit` is a no-op when the allocation already fits.
        value.shrink_to_fit();
        return value;
    }
    observer.truncated.fetch_add(1, Ordering::AcqRel);
    let mut end = limit;
    while end > 0 && !value.is_char_boundary(end) {
        end -= 1;
    }
    let mut clipped = String::with_capacity(end);
    clipped.push_str(&value[..end]);
    clipped
}

impl cancellation::Host for HostState {
    fn is_cancelled(&mut self) -> bool {
        self.observer
            .cancellation_polls
            .fetch_add(1, Ordering::AcqRel);
        self.observer.is_cancelled()
    }
}
