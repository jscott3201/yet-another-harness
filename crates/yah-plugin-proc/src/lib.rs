//! Supervised process driver for the worker wire protocol.
//!
//! [`yah_plugin_ipc`] specifies protocol v1 as a sans-io state machine; this
//! crate owns everything that machine deliberately cannot: the socketpair,
//! the spawned child, the byte pump between the two, the deadline clock, and
//! the kill path. One activation is one process is one session — the
//! protocol has no reconnect or resume, so a worker that dies poisons its
//! activation, health says so, and recovery is a fresh activation with a
//! fresh process.
//!
//! The bootstrap is authenticated by construction rather than by secret: the
//! host passes one end of a Unix socketpair as fd [`WORKER_CHANNEL_FD`], and
//! holding that fd is the credential — nothing else can hold it, because
//! nothing else was ever handed it. No token rides argv (world-readable on
//! most systems), nothing rides the environment (inherited by every later
//! child), and the worker's stdout and stderr stay what they look like:
//! bounded diagnostic text, never protocol bytes. The pathname-socket lane
//! with kernel-attested peer credentials (`getpeereid`/`SO_PEERCRED`) is
//! deliberately absent: it exists to attach to a process the host did not
//! spawn, and no current scenario does that.
//!
//! This driver contains no policy and no sandbox: bounding what a worker
//! costs is the session's ceilings, and isolating what it can reach is a
//! separate security slice. The spawned command runs with the host's own
//! ambient authority.

mod bootstrap;
mod dispatch;
mod driver;
mod endpoint;
mod pump;
mod shared;

pub use dispatch::{
    WorkerMethod, WorkerMethodCancellation, WorkerMethodFailure, WorkerMethodFailureCode,
    WorkerMethodRegistrationError, WorkerMethodRegistry, WorkerMethodRequest, WorkerMethodResult,
    WorkerMethodResultError,
};
pub use driver::{ProcActivationPlan, ProcObserver, ProcessPluginDriver, ResourceState};
pub use endpoint::{
    ActivationEndpoint, ArtifactReader, Availability, CallTerminal, EndpointError, PendingCall,
    Refusal, StreamCall, StreamFrame,
};
pub use shared::{CallEnd, DiagnosticStream};

/// The file descriptor the worker's protocol channel arrives on.
///
/// Fixed rather than negotiated: the worker learns nothing from argv or the
/// environment, so the number itself is part of the bootstrap contract. Both
/// runtime lanes can adopt an inherited descriptor by number — Node with
/// `new net.Socket({ fd: 3 })`, CPython with `socket.socket(fileno=3)`.
pub const WORKER_CHANNEL_FD: i32 = 3;

/// Host-owned bounds for one activation's process lifecycle.
///
/// These bound the driver, not the protocol: byte and count ceilings live in
/// the session's announced limits. Everything here is a wall-clock or memory
/// bound on the supervision itself.
#[derive(Clone, Copy, Debug)]
pub struct ProcLimits {
    /// Budget for the worker's hello to arrive and negotiation to complete,
    /// measured from spawn. A worker that connects and says nothing is not
    /// an error the protocol can see, so the driver enforces the clock.
    pub handshake_deadline_ms: u64,
    /// How long each bounded phase of an orderly shutdown waits: the
    /// goodbye flush against a worker that stopped draining, and the
    /// window between the goodbye and `SIGKILL` — so an exit can spend it
    /// twice, and the driver's deactivation bound budgets for both. The
    /// goodbye is a courtesy; the kill is the guarantee.
    pub kill_grace_ms: u64,
    /// How often session time advances for deadline enforcement, in
    /// milliseconds; zero is clamped to one at start (the clock's floor).
    pub tick_interval_ms: u64,
    /// Bytes of stdout and stderr retained per stream, oldest discarded
    /// first. Diagnostics are evidence, not a channel, so they are bounded
    /// like one.
    pub diagnostics_cap_bytes: usize,
    /// Bytes of encoded frames the host will hold for a worker that is not
    /// draining its channel before it declares the worker dead. The
    /// session's ceilings bound each frame and the in-flight count, but
    /// only the driver sees the transport back-pressure, so only the
    /// driver can bound what it buffers. The default holds well over a
    /// full complement of maximum-size in-flight calls; a value below one
    /// maximum-size frame is clamped up to it at start, because a cap
    /// under what the session itself admits would accuse a conformant
    /// worker of not draining a frame it was never given.
    pub outbound_buffer_cap_bytes: usize,
    /// Slots in the pump's command channel — the queue of call and cancel
    /// requests the driver hands the pump. Bounded on purpose: a caller
    /// flood must hit an observable rejection, not an unbounded backlog.
    /// The default matches the session's default host in-flight ceiling,
    /// so the channel never rejects a call the session would have
    /// admitted; a value below one is clamped to one at start. Shutdown
    /// does not ride this channel at all — it has a dedicated
    /// always-deliverable signal, so deactivation cannot be dropped
    /// behind a command flood.
    pub command_channel_capacity: usize,
    /// Retired correlation entries the worker session may remember before
    /// it stops making and taking new ones (see
    /// [`yah_plugin_ipc::session::SessionConfig::retired_operation_budget`]).
    /// `Some` by default: the driver is the supervisor the protocol docs
    /// name as the bounding authority, and a long-lived hostile worker
    /// must not grow host correlation memory without a ceiling. The
    /// overshoot past the budget is bounded by the session's negotiated
    /// in-flight and live-handle ceilings.
    pub retired_operation_budget: Option<u64>,
    /// Slots in the worker-to-host dispatch queue — the lane that routes
    /// admitted worker calls to application providers off the pump task.
    /// Bounded on purpose: a
    /// worker flood must hit an observable, refusable bound, not an
    /// unbounded backlog of host work. A value below one is clamped to
    /// one at start.
    pub dispatch_queue_capacity: usize,
    /// Provider calls the dispatcher may run concurrently. Providers are
    /// host-registered synchronous code; the bound keeps one slow or
    /// panicking provider family from occupying every dispatch slot
    /// while others starve. A value below one is clamped to one at
    /// start.
    pub provider_concurrency: usize,
}

impl Default for ProcLimits {
    fn default() -> Self {
        Self {
            handshake_deadline_ms: 5_000,
            kill_grace_ms: 500,
            tick_interval_ms: 10,
            diagnostics_cap_bytes: 64 * 1024,
            outbound_buffer_cap_bytes: 8 * 1024 * 1024,
            command_channel_capacity: 16,
            retired_operation_budget: Some(1_000_000),
            dispatch_queue_capacity: 16,
            provider_concurrency: 4,
        }
    }
}
