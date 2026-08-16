//! Host-owned bounds every activation runs under.
//!
//! These are the host's numbers, not the guest's: nothing a component declares
//! or asks for can raise them.
//!
//! The bounds do different jobs, and the difference is not cosmetic. A memory
//! or table ceiling *refuses*: the guest asks to grow, the host declines, and
//! the guest sees the refusal and may handle it. A call deadline *terminates*:
//! a guest that stops making progress is stopped without being consulted.
//!
//! All of these bound what a guest can cost. None of them bounds what it can
//! reach. Guest code runs in the authority process, so this is resource
//! accounting, not isolation.

use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    thread,
    time::Duration,
};

use wasmtime::{Engine, ResourceLimiter, UpdateDeadline};

/// Bounds applied to every activation one driver runs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WasmLimits {
    /// Ceiling on one activation's linear memory, summed across every memory.
    pub memory_bytes: usize,
    /// Ceiling on one activation's table entries, summed across every table.
    pub table_elements: usize,
    /// How many linear memories one activation may hold.
    ///
    /// A byte ceiling alone does not bound a memory *count*, and a count is its
    /// own cost: Wasmtime reserves an address-space window per memory whether
    /// or not the memory has any pages in it. A guest declaring many empty
    /// memories is charged nothing by [`Self::memory_bytes`] and can still
    /// exhaust the host's address space.
    pub max_memories: usize,
    /// How many tables one activation may hold, for the same reason.
    pub max_tables: usize,
    /// How many core instances one activation may hold.
    pub max_instances: usize,
    /// Address space Wasmtime reserves per linear memory.
    ///
    /// Wasmtime's default is 4 GiB, which lets a memory grow without ever
    /// moving. That trade only makes sense when a memory is allowed to reach
    /// 4 GiB; here the host's own ceiling is far lower, so the reservation is
    /// sized to the ceiling and the difference stops being address space a
    /// guest can claim for free.
    pub memory_reservation_bytes: u64,
    /// Bytes one guest-to-host call may transfer into host memory.
    ///
    /// A guest can point every element of a list at the same buffer, so the
    /// memory ceiling does not bound what a single call costs the host: a small
    /// guest memory can name a very large lifted value. This bounds the lift
    /// itself, which is the only place that aliasing is paid for.
    pub host_call_bytes: usize,
    /// How often the driver advances its engine's epoch.
    pub epoch_tick: Duration,
    /// Epoch ticks one guest call may run before it is trapped.
    ///
    /// This is a ceiling on a runaway call, not a latency target: a call that
    /// returns promptly never approaches it.
    pub call_budget_ticks: u64,
    /// Bytes of one log message the host will retain.
    pub max_log_message_bytes: usize,
    /// Structured fields the host will retain from one log call.
    pub max_log_fields: usize,
}

impl WasmLimits {
    /// The wall-clock bound on how long a runaway call may run.
    ///
    /// This is not a floor. The ticker free-runs from driver construction, so
    /// the first increment after a call is armed can land immediately and
    /// spend a tick the call never used: a call is guaranteed only
    /// `(call_budget_ticks - 1) * epoch_tick`. Under load the ticker's sleeps
    /// stretch and the real figure grows past this; the call still terminates.
    pub const fn call_deadline(&self) -> Duration {
        let ticks = if self.call_budget_ticks > u32::MAX as u64 {
            u32::MAX
        } else {
            self.call_budget_ticks as u32
        };
        self.epoch_tick.saturating_mul(ticks)
    }

    /// A fresh limiter enforcing these ceilings for one activation.
    pub const fn limiter(&self) -> ActivationLimiter {
        ActivationLimiter {
            limits: *self,
            memory_bytes: 0,
            table_elements: 0,
        }
    }
}

/// Enforces one activation's ceilings across every memory and table it owns.
///
/// Wasmtime's own `StoreLimits` applies its ceiling to each memory
/// *individually*, and one store may hold thousands. A component that declares
/// many memories would then be admitted far above the total the host believed
/// it had set, without violating anything. This tracks the running total
/// instead, so the ceiling means what it says.
#[derive(Clone, Copy, Debug)]
pub struct ActivationLimiter {
    limits: WasmLimits,
    memory_bytes: usize,
    table_elements: usize,
}

impl ActivationLimiter {
    /// Bytes of linear memory this activation currently holds.
    pub const fn memory_bytes(&self) -> usize {
        self.memory_bytes
    }
}

impl ResourceLimiter for ActivationLimiter {
    // Counts, not just totals. Wasmtime defaults each of these to 10,000, and
    // an empty memory costs the byte ceiling nothing while still costing the
    // host an address-space reservation - so without these three, the byte
    // ceiling bounds the wrong resource.
    fn memories(&self) -> usize {
        self.limits.max_memories
    }

    fn tables(&self) -> usize {
        self.limits.max_tables
    }

    fn instances(&self) -> usize {
        self.limits.max_instances
    }

    fn memory_growing(
        &mut self,
        current: usize,
        desired: usize,
        _maximum: Option<usize>,
    ) -> wasmtime::Result<bool> {
        let growth = desired.saturating_sub(current);
        let total = self.memory_bytes.saturating_add(growth);
        if total > self.limits.memory_bytes {
            return Ok(false);
        }
        self.memory_bytes = total;
        Ok(true)
    }

    fn table_growing(
        &mut self,
        current: usize,
        desired: usize,
        _maximum: Option<usize>,
    ) -> wasmtime::Result<bool> {
        let growth = desired.saturating_sub(current);
        let total = self.table_elements.saturating_add(growth);
        if total > self.limits.table_elements {
            return Ok(false);
        }
        self.table_elements = total;
        Ok(true)
    }
}

impl Default for WasmLimits {
    /// Bounds chosen to contain the fixture corpus, not tuned for production.
    ///
    /// The memory ceiling is generous enough for a component that allocates
    /// while staying far below anything that would pressure the host, and the
    /// call deadline is long enough that a slow machine will not trap a
    /// well-behaved fixture. The counts are small because a component with any
    /// need for more than a handful of memories or tables is not something this
    /// driver is meant to run yet.
    fn default() -> Self {
        Self {
            memory_bytes: 16 * 1024 * 1024,
            table_elements: 10_000,
            max_memories: 4,
            max_tables: 4,
            max_instances: 16,
            memory_reservation_bytes: 16 * 1024 * 1024,
            host_call_bytes: 4 * 1024 * 1024,
            epoch_tick: Duration::from_millis(10),
            call_budget_ticks: 100,
            max_log_message_bytes: 4 * 1024,
            max_log_fields: 32,
        }
    }
}

/// Advances one engine's epoch so stalled guest calls reach their deadline.
///
/// Epoch deadlines are relative to the engine's current epoch, so a store only
/// traps once the counter passes the deadline it was given. One shared ticker
/// therefore bounds every activation without coupling them: a call with budget
/// left is unaffected by the same increment that kills a call without any.
///
/// This is why the driver can bound a runaway guest at all. Nothing can lower a
/// stuck store's deadline from outside, because that would need exclusive
/// access to the store the stuck call is already holding.
pub struct EpochTicker {
    stopped: Arc<AtomicBool>,
    ticks: Arc<AtomicU64>,
}

impl EpochTicker {
    /// Start advancing `engine`'s epoch every `tick`.
    ///
    /// The thread holds its own engine handle and exits on the next tick after
    /// the ticker is dropped. It is deliberately not joined: teardown must not
    /// panic or block, and waiting on a sleeping thread would block it.
    pub fn start(engine: &Engine, tick: Duration) -> Self {
        let stopped = Arc::new(AtomicBool::new(false));
        let ticks = Arc::new(AtomicU64::new(0));
        let engine = engine.clone();
        let flag = Arc::clone(&stopped);
        let counter = Arc::clone(&ticks);
        thread::spawn(move || {
            while !flag.load(Ordering::Acquire) {
                thread::sleep(tick);
                engine.increment_epoch();
                counter.fetch_add(1, Ordering::AcqRel);
            }
        });
        Self { stopped, ticks }
    }

    /// A handle to the tick count that outlives the ticker.
    ///
    /// The thread is detached, so "it stopped" is only checkable from something
    /// that survives the drop. One tick may still land after the stop flag is
    /// set, because the thread reads it between sleeps.
    pub fn tick_counter(&self) -> Arc<AtomicU64> {
        Arc::clone(&self.ticks)
    }

    pub fn ticks(&self) -> u64 {
        self.ticks.load(Ordering::Acquire)
    }
}

impl Drop for EpochTicker {
    fn drop(&mut self) {
        self.stopped.store(true, Ordering::Release);
    }
}

/// One activation's answer to "should this guest call keep running?".
///
/// The decision runs inside the guest's own epoch-deadline callback, on the
/// guest's thread. That is what makes it usable from a caller who cannot touch
/// the store: every per-store knob Wasmtime exposes needs exclusive access to
/// the store, which is exactly what a stuck call is holding.
///
/// So the host does not reach in and stop the guest. It leaves an answer the
/// guest is compelled to read at its next deadline.
#[derive(Clone, Default)]
pub struct GuestInterrupt {
    state: Arc<InterruptState>,
}

#[derive(Default)]
struct InterruptState {
    killed: AtomicBool,
    ticks: AtomicU64,
}

impl GuestInterrupt {
    pub fn new() -> Self {
        Self::default()
    }

    /// Stop this activation, in flight and on entry.
    ///
    /// A call already running ends at its next epoch deadline, so its latency
    /// is bounded by one tick rather than by anything the guest does. A call
    /// that has not started yet is refused outright, which matters because a
    /// short call can return without ever reaching a deadline and would
    /// otherwise never see this flag at all.
    ///
    /// The flag is deliberately sticky: [`Self::begin_call`] resets the tick
    /// budget, but nothing resets a kill.
    pub fn kill(&self) {
        self.state.killed.store(true, Ordering::Release);
    }

    pub fn is_killed(&self) -> bool {
        self.state.killed.load(Ordering::Acquire)
    }

    /// Give the next guest call a full budget.
    pub fn begin_call(&self) {
        self.state.ticks.store(0, Ordering::Release);
    }

    /// Ticks the current call has consumed.
    pub fn ticks_used(&self) -> u64 {
        self.state.ticks.load(Ordering::Acquire)
    }

    /// Decide, at one epoch deadline, whether the guest continues.
    ///
    /// Extending by a single tick rather than the whole remaining budget is
    /// deliberate: it is what makes [`Self::kill`] take effect within one tick
    /// instead of whenever the budget happens to run out.
    pub fn on_deadline(&self, budget_ticks: u64) -> UpdateDeadline {
        if self.is_killed() {
            return UpdateDeadline::Interrupt;
        }
        let used = self
            .state
            .ticks
            .fetch_add(1, Ordering::AcqRel)
            .saturating_add(1);
        if used >= budget_ticks {
            UpdateDeadline::Interrupt
        } else {
            UpdateDeadline::Continue(1)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn limiter(memory_bytes: usize, table_elements: usize) -> ActivationLimiter {
        WasmLimits {
            memory_bytes,
            table_elements,
            ..WasmLimits::default()
        }
        .limiter()
    }

    /// The fixture corpus drives the memory ceiling through a real component;
    /// the table ceiling has no such fixture, so its evidence is here.
    #[test]
    fn a_table_is_refused_past_the_ceiling_and_granted_below_it() {
        let mut limits = limiter(usize::MAX, 100);
        assert_eq!(limits.table_growing(0, 100, None).ok(), Some(true));
        assert_eq!(limits.table_growing(100, 101, None).ok(), Some(false));
    }

    /// The escape this limiter exists to close: Wasmtime's own `StoreLimits`
    /// applies its ceiling per table, so N tables each at the ceiling admit N
    /// times what the host asked for.
    #[test]
    fn the_table_ceiling_is_a_total_not_a_per_table_allowance() {
        let mut limits = limiter(usize::MAX, 100);
        assert_eq!(limits.table_growing(0, 60, None).ok(), Some(true));
        // A second table, well under the ceiling on its own, is refused because
        // the pair is over it.
        assert_eq!(limits.table_growing(0, 60, None).ok(), Some(false));
    }

    #[test]
    fn the_memory_ceiling_is_a_total_not_a_per_memory_allowance() {
        let mut limits = limiter(1024, usize::MAX);
        assert_eq!(limits.memory_growing(0, 600, None).ok(), Some(true));
        assert_eq!(limits.memory_growing(0, 600, None).ok(), Some(false));
        assert_eq!(limits.memory_bytes(), 600);
    }

    /// A refusal must not consume budget, or a guest that handles -1 and asks
    /// for less would be charged for the request that was declined.
    #[test]
    fn a_refused_request_leaves_the_running_total_untouched() {
        let mut limits = limiter(1024, usize::MAX);
        assert_eq!(limits.memory_growing(0, 900, None).ok(), Some(true));
        assert_eq!(limits.memory_growing(900, 2048, None).ok(), Some(false));
        assert_eq!(limits.memory_bytes(), 900);
        assert_eq!(limits.memory_growing(900, 1024, None).ok(), Some(true));
    }

    /// `call_deadline` multiplies a `u64` budget into a `Duration`, which takes
    /// a `u32`. A budget past that must saturate, not wrap to a short deadline.
    ///
    /// The input is one past `u32::MAX`, not `u64::MAX`. `u64::MAX as u32` is
    /// `u32::MAX` - exactly what saturation produces - so `u64::MAX` is the one
    /// point in the domain where a truncating implementation and a saturating
    /// one agree, and a test that used it could not fail.
    #[test]
    fn an_enormous_tick_budget_saturates_rather_than_wrapping() {
        let limits = WasmLimits {
            epoch_tick: Duration::from_millis(1),
            call_budget_ticks: u32::MAX as u64 + 1,
            ..WasmLimits::default()
        };
        assert_eq!(
            limits.call_deadline(),
            Duration::from_millis(u32::MAX as u64)
        );
    }
}
