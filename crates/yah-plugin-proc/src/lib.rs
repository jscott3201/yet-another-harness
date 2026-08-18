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
mod driver;
mod pump;

pub use bootstrap::WorkerCommand;
pub use driver::{
    PendingCall, ProcActivationPlan, ProcObserver, ProcessPluginDriver, ResourceState,
};
pub use pump::{CallEnd, DiagnosticStream};

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
    /// How long an orderly shutdown waits between the host's goodbye and
    /// `SIGKILL`. The goodbye is a courtesy; the kill is the guarantee.
    pub kill_grace_ms: u64,
    /// How often session time advances for deadline enforcement.
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
}

impl Default for ProcLimits {
    fn default() -> Self {
        Self {
            handshake_deadline_ms: 5_000,
            kill_grace_ms: 500,
            tick_interval_ms: 10,
            diagnostics_cap_bytes: 64 * 1024,
            outbound_buffer_cap_bytes: 8 * 1024 * 1024,
        }
    }
}
