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

use wasmtime::{Config, Engine, ResourceLimiter, UpdateDeadline};

/// Default stack reserved above guest code on the same fiber.
///
/// A guest call runs on a fiber carrying two things: the guest's own frames,
/// bounded by [`WasmLimits::guest_stack_bytes`], and the host frames above
/// them, which are the trampoline, the canonical ABI's lift and lower, and
/// whatever a host import does when the guest calls one. Wasmtime refuses only
/// when the guest bound *exceeds* the fiber, so two numbers that are merely
/// close are accepted at build time and then overflow the fiber during a call
/// that runs deep enough to use its whole depth bound, which aborts the
/// process. Every other bound in this crate either refuses the guest or fails
/// the driver build, so this one is checked rather than left as a caution.
///
/// Measured demand above the guest region is far smaller. Recursion alone needs
/// about 10 KiB on aarch64 in a debug host build, where frames are fattest, and
/// about 3 KiB in release. Review measured about 9 KiB with the cancellation
/// import called at maximum depth and 11 KiB with the logging import there,
/// using fixtures built outside the tree - no checked-in fixture imports
/// anything, so those two are not reproducible from this repo and the recursion
/// figure is the one to trust here. This default is more than twenty times the
/// worst of them, because being wrong one way costs address space and being
/// wrong the other way aborts the process. It is a default rather than a constant of the crate precisely
/// because the right number is a property of the host: it moves with target,
/// optimisation level, and what an import does above the guest, so
/// [`WasmLimits::host_stack_headroom_bytes`] carries it and a host that has
/// measured its own can say so.
pub const HOST_STACK_HEADROOM_BYTES: usize = 256 * 1024;

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
    /// Address space Wasmtime reserves per linear memory, guards excluded.
    ///
    /// Wasmtime's default is 4 GiB, which lets a memory grow without ever
    /// moving. That trade only makes sense when a memory is allowed to reach
    /// 4 GiB; here the host's own ceiling is far lower, so the reservation is
    /// sized to the ceiling and the difference stops being address space a
    /// guest can claim for free.
    ///
    /// This is not the whole per-memory cost. A memory is bracketed by a guard
    /// region on each side, so the address space one memory occupies is this
    /// plus twice [`Self::memory_guard_bytes`]. Nothing derives this field from
    /// [`Self::memory_bytes`]: a caller that raises the ceiling and leaves this
    /// alone gets memories that outgrow their reservation and are re-mapped,
    /// which costs more address space, not less.
    pub memory_reservation_bytes: u64,
    /// Stack a guest call runs on, in bytes.
    ///
    /// A guest call runs on a stack of its own so it can yield the thread back
    /// to the executor mid-call. The cost is per *activation*, not per call in
    /// flight: Wasmtime parks a finished call's stack in its store and reuses
    /// it, releasing it only when the store is dropped. Since instantiation is
    /// itself a guest call, every live activation holds one of these from its
    /// first call until teardown, idle or not. A host sizing this is pricing
    /// how many plugins it will keep alive, not how many are working.
    ///
    /// This sizes the stack; it does not bound how deep the guest may recurse
    /// on it. That is [`Self::guest_stack_bytes`], and the two move together:
    /// this must exceed it by at least [`Self::host_stack_headroom_bytes`],
    /// which [`Self::engine`] checks.
    pub call_stack_bytes: usize,
    /// How deep guest code may recurse before it is trapped, in bytes.
    ///
    /// This is the bound that actually stops a runaway recursion, and it is
    /// separate from the stack the call runs on: the fiber must hold this plus
    /// [`Self::host_stack_headroom_bytes`] of host frames above it. Wasmtime
    /// defaults it to 512 KiB whether or not a host thinks about it, so leaving
    /// it unset would mean the guest's depth bound was Wasmtime's choice rather
    /// than the host's.
    pub guest_stack_bytes: usize,
    /// Fiber a guest call may not use, held for the host frames above it.
    ///
    /// Defaults to [`HOST_STACK_HEADROOM_BYTES`]; see it for what runs there
    /// and what it was measured against. A host that has measured its own
    /// target may lower this, at the risk the constant's doc describes, and one
    /// whose imports do more work above the guest than these should raise it.
    pub host_stack_headroom_bytes: usize,
    /// Address space reserved on each side of a linear memory.
    ///
    /// The guard is what lets Wasmtime turn an out-of-bounds guest access into
    /// a fault instead of a bounds check on every access, so it is worth
    /// keeping. Wasmtime's default is 32 MiB per side, sized for a memory that
    /// may reach 4 GiB; against this host's ceiling that is most of what an
    /// activation costs.
    pub memory_guard_bytes: u64,
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

    /// An engine carrying these bounds, or a sentence saying why not.
    ///
    /// Every engine in this crate is built here. That is not tidiness: these
    /// knobs were set in three places once, and the recursion bound was missing
    /// from all three, because a setting that has to be repeated is a setting
    /// that will eventually be applied to only some of them.
    ///
    /// The bounds a `Config` cannot express (counts, totals, the call deadline)
    /// are enforced per store instead, by [`Self::limiter`] and the epoch
    /// deadline. This covers only what the engine itself owns.
    pub fn engine(&self) -> Result<Engine, String> {
        // Checked here because Wasmtime does not check it: it rejects a guest
        // bound larger than the fiber and accepts one a page smaller, and the
        // second aborts the process on the first call that runs deep rather
        // than failing anything the host can report.
        //
        // The two directions are reported separately because "leaves N" is only
        // meaningful when there is something left; a fiber smaller than the
        // bound it carries leaves less than nothing, and saying "leaves 0"
        // would describe the boundary case rather than this one.
        if self.call_stack_bytes < self.guest_stack_bytes {
            return Err(format!(
                "call_stack_bytes ({}) must be larger than guest_stack_bytes ({}), \
                 with {} bytes over it for host frames",
                self.call_stack_bytes, self.guest_stack_bytes, self.host_stack_headroom_bytes
            ));
        }
        let headroom = self.call_stack_bytes - self.guest_stack_bytes;
        if headroom < self.host_stack_headroom_bytes {
            return Err(format!(
                "call_stack_bytes ({}) must exceed guest_stack_bytes ({}) by at least {} \
                 bytes for host frames, but leaves {headroom}",
                self.call_stack_bytes, self.guest_stack_bytes, self.host_stack_headroom_bytes
            ));
        }
        let mut config = Config::new();
        config.epoch_interruption(true);
        // Wasmtime reserves 4 GiB per linear memory by default so a memory can
        // grow without moving. That trade is only worth its address space when
        // a memory may actually reach 4 GiB, and the host's own ceiling is far
        // lower. Sizing the reservation to the ceiling is what stops a guest
        // from claiming address space the byte ceiling never charges it for.
        config.memory_reservation(self.memory_reservation_bytes);
        // A memory that outgrows its reservation is re-mapped with a *new*
        // reservation, and that one defaults to 2 GiB - so leaving it alone
        // would let a guest reopen a multi-gigabyte window simply by growing.
        config.memory_reservation_for_growth(self.memory_reservation_bytes);
        // Wasmtime brackets every memory with a 32 MiB guard on each side, so
        // the address space one memory costs is the reservation plus 64 MiB
        // unless this is set too. The guard exists to turn out-of-bounds
        // accesses into faults without a bounds check; it stays, but sized to
        // something proportionate to the ceiling rather than to a 4 GiB memory.
        config.memory_guard_size(self.memory_guard_bytes);
        // Wasmtime parks a finished call's stack in its store and reuses it,
        // so this is one allocation per live activation rather than per call
        // in flight. Its 2 MiB default is sized for arbitrary guest code.
        config.async_stack_size(self.call_stack_bytes);
        // The stack size is not the recursion bound - this is. Left unset it
        // would sit at Wasmtime's 512 KiB, which would make how deep a guest
        // may recurse Wasmtime's decision rather than the host's.
        config.max_wasm_stack(self.guest_stack_bytes);
        Engine::new(&config)
            .map_err(|error| format!("engine did not accept its configuration: {error}"))
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
            memory_guard_bytes: 1024 * 1024,
            call_stack_bytes: 1024 * 1024,
            guest_stack_bytes: 512 * 1024,
            host_stack_headroom_bytes: HOST_STACK_HEADROOM_BYTES,
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
    /// is bounded by one tick rather than by anything the guest does - provided
    /// something is still polling that call, since a guest call runs on a fiber
    /// and reaches a deadline only when resumed. A call that has not started
    /// yet is refused outright, which matters because a short call can return
    /// without ever reaching a deadline and would otherwise never see this flag
    /// at all.
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
    ///
    /// Continuing *yields* rather than resuming in place. A guest call runs on
    /// its own stack, but a stack only gives the executor back when something
    /// hands it back, and a guest that computes without calling a host import
    /// never would: it would hold the thread until it finished or trapped, and
    /// running on a fiber would have moved the blocking rather than removed
    /// it. Yielding here is the point at which a compute-bound guest becomes
    /// cooperative, and it costs one scheduling round-trip per tick.
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
            UpdateDeadline::Yield(1)
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

    /// The fixture corpus drives the memory ceiling and the table *count*
    /// through real components; the summed table-element ceiling has no such
    /// fixture - `many-tables.wat` holds empty tables, so it charges the count
    /// and never the total - so its evidence is here.
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

    /// The pair Wasmtime accepts and then aborts on. A fiber the same size as
    /// the guest bound it carries passes `Engine::new` and overflows on the
    /// first call that runs deep enough to use that bound - the fixtures that
    /// stay shallow complete under it, which is what makes the pair dangerous
    /// rather than obviously broken. A host that got this wrong would lose the
    /// process rather than see an error. There is no test for the abort itself,
    /// for the obvious reason.
    ///
    /// The pair is equal-sized rather than inverted so that it reaches the
    /// headroom comparison: an inverted pair is refused for its direction
    /// before the headroom is ever looked at.
    #[test]
    fn a_stack_pair_with_no_room_for_host_frames_is_refused() {
        let limits = WasmLimits {
            call_stack_bytes: 512 * 1024,
            guest_stack_bytes: 512 * 1024,
            ..WasmLimits::default()
        };
        let refusal = limits
            .engine()
            .expect_err("a fiber with no headroom must be refused");
        assert!(
            refusal.contains("host frames"),
            "the refusal must name what the headroom is for: {refusal}"
        );
    }

    /// The other half: the refusal must not be reachable by every pair, or it
    /// would be a check that no configuration passes.
    #[test]
    fn a_stack_pair_with_room_to_spare_is_accepted() {
        assert!(WasmLimits::default().engine().is_ok());
    }

    /// A fiber smaller than the bound it carries is a different mistake, and
    /// subtracting would report it as the boundary case rather than as itself.
    #[test]
    fn a_fiber_smaller_than_its_guest_bound_is_refused_as_that() {
        let limits = WasmLimits {
            call_stack_bytes: 512 * 1024,
            guest_stack_bytes: 1024 * 1024,
            ..WasmLimits::default()
        };
        let refusal = limits
            .engine()
            .expect_err("a fiber under its own guest bound must be refused");
        assert!(
            refusal.contains("must be larger than"),
            "the refusal must name the direction it failed in: {refusal}"
        );
    }

    /// The headroom is the host's number, so a host that has measured its own
    /// target can spend less on it. Without this the check would be a floor the
    /// crate imposes rather than a bound the host owns.
    #[test]
    fn a_host_may_lower_the_headroom_it_owns() {
        let limits = WasmLimits {
            call_stack_bytes: 576 * 1024,
            guest_stack_bytes: 512 * 1024,
            host_stack_headroom_bytes: 64 * 1024,
            ..WasmLimits::default()
        };
        assert!(limits.engine().is_ok());
        assert!(
            WasmLimits {
                host_stack_headroom_bytes: HOST_STACK_HEADROOM_BYTES,
                ..limits
            }
            .engine()
            .is_err(),
            "the same pair must fail under the default headroom, or this proves nothing"
        );
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
