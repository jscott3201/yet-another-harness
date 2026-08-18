//! Authenticated worker spawn: the inherited fd is the credential.
//!
//! One Unix socketpair per activation. The host keeps one end; the other is
//! duplicated onto fd [`crate::WORKER_CHANNEL_FD`] between fork and exec,
//! where `dup2` clearing close-on-exec is the entire hand-off mechanism —
//! the duplicate survives exec, every other descriptor Rust opened does
//! not. Possession of that fd is the whole credential: no token in argv, no
//! secret in the environment, and the environment itself is cleared to an
//! allowlist so the worker starts from what the host chose, not what the
//! host happened to inherit.
//!
//! The worker is also made its own process-group leader between fork and
//! exec, so the driver's kill path can sweep everything the worker spawned
//! rather than only the worker itself.

use std::ffi::OsString;
use std::io;
use std::os::fd::IntoRawFd;
use std::path::PathBuf;
use std::process::Stdio;

use crate::WORKER_CHANNEL_FD;

/// The command one activation spawns, chosen by the host.
///
/// Deliberately narrow: program and arguments only. The driver owns stdio
/// disposition, the environment allowlist, and the channel fd; a caller
/// that could override those could also un-authenticate the bootstrap.
#[derive(Clone, Debug)]
pub struct WorkerCommand {
    program: PathBuf,
    args: Vec<OsString>,
}

impl WorkerCommand {
    pub fn new(program: impl Into<PathBuf>) -> Self {
        Self {
            program: program.into(),
            args: Vec::new(),
        }
    }

    pub fn arg(mut self, arg: impl Into<OsString>) -> Self {
        self.args.push(arg.into());
        self
    }
}

/// A spawned worker: the live child and the host's end of its channel.
pub(crate) struct SpawnedWorker {
    pub child: tokio::process::Child,
    pub channel: tokio::net::UnixStream,
    /// The child's pid, which is also its process-group id: `pre_exec` makes
    /// the worker a group leader so the kill path can sweep anything the
    /// worker spawned, not just the worker.
    pub pgid: i32,
}

/// Spawn `command` with the protocol channel on fd 3.
///
/// Must run inside a tokio runtime (the child's stdio and the channel
/// register with the reactor).
pub(crate) fn spawn_worker(command: &WorkerCommand) -> io::Result<SpawnedWorker> {
    let (host_end, worker_end) = std::os::unix::net::UnixStream::pair()?;
    host_end.set_nonblocking(true)?;
    let channel = tokio::net::UnixStream::from_std(host_end)?;

    let mut spawn = tokio::process::Command::new(&command.program);
    spawn
        .args(&command.args)
        .env_clear()
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        // The safety net, not the shutdown path: deactivation kills and
        // reaps deliberately, this covers a driver dropped without it.
        .kill_on_drop(true);
    // PATH alone: enough to exec interpreter shebangs, and nothing a later
    // child should not inherit. Secrets never ride the environment.
    if let Some(path) = std::env::var_os("PATH") {
        spawn.env("PATH", path);
    }

    // Ownership of the raw fd passes to this function; the child's copy is
    // made in pre_exec and the parent's original is closed after spawn on
    // every path, success and failure alike.
    let mut worker_fd = worker_end.into_raw_fd();
    if worker_fd == WORKER_CHANNEL_FD {
        // `dup2` onto itself would not clear close-on-exec, so the hand-off
        // below requires source and target to differ. Move the descriptor
        // out of the way here in the parent, where failure is an ordinary
        // error, so pre_exec has exactly one path — the one every test runs.
        let moved = unsafe { libc::fcntl(worker_fd, libc::F_DUPFD_CLOEXEC, WORKER_CHANNEL_FD + 1) };
        let move_error = (moved == -1).then(io::Error::last_os_error);
        unsafe {
            libc::close(worker_fd);
        }
        if let Some(error) = move_error {
            return Err(error);
        }
        worker_fd = moved;
    }
    unsafe {
        spawn.pre_exec(move || {
            // Group leadership first: the kill path signals the group, so a
            // worker's own children die with it instead of orphaning with
            // the host's ambient authority.
            if libc::setpgid(0, 0) == -1 {
                return Err(io::Error::last_os_error());
            }
            if libc::dup2(worker_fd, WORKER_CHANNEL_FD) == -1 {
                return Err(io::Error::last_os_error());
            }
            Ok(())
        });
    }
    let spawned = spawn.spawn();
    // SAFETY: `worker_fd` was released by `into_raw_fd` above and is owned
    // here; nothing else closes it in the parent.
    unsafe {
        libc::close(worker_fd);
    }
    let child = spawned?;
    let pgid = child.id().map_or_else(
        || Err(io::Error::other("a freshly spawned child has no pid")),
        |pid| Ok(pid as i32),
    )?;
    Ok(SpawnedWorker {
        child,
        channel,
        pgid,
    })
}
