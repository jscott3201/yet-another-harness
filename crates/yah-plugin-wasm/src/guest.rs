//! Guest components the driver corpus instantiates, carried as component text.
//!
//! These are fixtures, not a guest SDK and not an authoring example. They exist
//! so the driver can be proved against a component that Wasmtime actually
//! compiles and runs, rather than against a Rust fake standing in for one. Text
//! keeps the corpus reviewable in a diff; a checked-in `.wasm` would not be.
//!
//! Building these from a real language toolchain would need a second Rust
//! target in the gate container. That belongs with the guest SDK work, not
//! here.

/// Which fixture component an activation instantiates.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum GuestProgram {
    /// Activates cleanly and returns one fixed fixture-tool response.
    ///
    /// `invoke` ignores its input; the answer is a constant.
    Conformant,
    /// Activates with a returned `guest-error` rather than a trap.
    ActivateFailure,
    /// Never returns from `activate`, so only the host's deadline stops it.
    Runaway,
    /// Grows linear memory a fixed number of pages, trapping if refused.
    MemoryHog,
    /// Declares two linear memories, to test that the ceiling is a total.
    MultiMemory,
    /// Declares many empty memories, which cost bytes nothing and space plenty.
    ManyMemories,
    /// Declares many empty tables, for the same reason and the other ceiling.
    ManyTables,
    /// Descends a fixed number of frames, deep enough to reach a tight bound.
    DeepRecursion,
    /// Runs guest code during instantiation, long enough to yield the thread.
    SlowStart,
    /// Lifts far more bytes into the host than its own memory holds.
    ///
    /// Every element of the list it logs aliases one buffer, so the memory
    /// ceiling does not bound what the call costs; `host_call_bytes` does.
    HostCallFlood,
    /// Acquires a brokered capability at activation and answers tool calls
    /// through it, reporting every refusal code it observes instead of
    /// trapping on it.
    CapabilityConsumer,
}

impl GuestProgram {
    /// Component text compiled once per driver.
    pub const fn text(self) -> &'static str {
        match self {
            Self::Conformant => CONFORMANT,
            Self::ActivateFailure => ACTIVATE_FAILURE,
            Self::Runaway => RUNAWAY,
            Self::MemoryHog => MEMORY_HOG,
            Self::MultiMemory => MULTI_MEMORY,
            Self::ManyMemories => MANY_MEMORIES,
            Self::ManyTables => MANY_TABLES,
            Self::DeepRecursion => DEEP_RECURSION,
            Self::SlowStart => SLOW_START,
            Self::HostCallFlood => HOST_CALL_FLOOD,
            Self::CapabilityConsumer => CAPABILITY_CONSUMER,
        }
    }
}

const CONFORMANT: &str = include_str!("../guests/conformant.wat");
const ACTIVATE_FAILURE: &str = include_str!("../guests/activate-failure.wat");
const RUNAWAY: &str = include_str!("../guests/runaway.wat");
const MEMORY_HOG: &str = include_str!("../guests/memory-hog.wat");
const MULTI_MEMORY: &str = include_str!("../guests/multi-memory.wat");
const MANY_MEMORIES: &str = include_str!("../guests/many-memories.wat");
const MANY_TABLES: &str = include_str!("../guests/many-tables.wat");
const DEEP_RECURSION: &str = include_str!("../guests/deep-recursion.wat");
const SLOW_START: &str = include_str!("../guests/slow-start.wat");
const HOST_CALL_FLOOD: &str = include_str!("../guests/host-call-flood.wat");
const CAPABILITY_CONSUMER: &str = include_str!("../guests/capability-consumer.wat");
