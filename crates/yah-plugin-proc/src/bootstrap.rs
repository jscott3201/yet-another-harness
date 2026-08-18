//! Authenticated worker spawn: the inherited fd is the credential.
//!
//! One Unix socketpair per activation. The host keeps one end; the other is
//! duplicated onto fd [`crate::WORKER_CHANNEL_FD`] between fork and exec,
//! where `dup2` clearing close-on-exec is the entire hand-off mechanism.
//! Everything above the channel is then marked close-on-exec before exec:
//! descriptors Rust opened carry the flag anyway, but descriptors the host
//! itself inherited without it — from a shell, a hook, a supervisor —
//! would otherwise ride into every worker, and possession of the channel
//! must be the only hand-off there is. Marking rather than closing keeps
//! the standard library's own exec-status pipe alive up to exec, so a
//! worker path that does not exist still fails the spawn instead of
//! masquerading as a worker that ran and disconnected. Possession of that
//! fd is the whole credential: no token in argv, no
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
    // Computed here in the parent: directory scans and sysconf are not
    // async-signal-safe, and the descriptors std's own spawn machinery
    // opens after this point are born close-on-exec and need no visit.
    let fd_ceiling = fd_table_ceiling();
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
            retire_ambient_descriptors(fd_ceiling);
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

/// One above the highest descriptor this process holds open, from the fd
/// directory; the rlimit ceiling only as a fallback, since a generous
/// `ulimit -n` would otherwise cost a million wasted probes per spawn.
fn fd_table_ceiling() -> i32 {
    let listing = if cfg!(target_os = "linux") {
        "/proc/self/fd"
    } else {
        "/dev/fd"
    };
    if let Ok(entries) = std::fs::read_dir(listing) {
        // The scan's own directory descriptor appears in the listing, so
        // the +1 below never undercounts the live table.
        let mut highest = WORKER_CHANNEL_FD;
        for entry in entries.flatten() {
            if let Some(fd) = entry
                .file_name()
                .to_str()
                .and_then(|name| name.parse::<i32>().ok())
            {
                highest = highest.max(fd);
            }
        }
        return highest + 1;
    }
    match unsafe { libc::sysconf(libc::_SC_OPEN_MAX) } {
        ceiling if ceiling > 0 => ceiling as i32,
        _ => i16::MAX as i32,
    }
}

/// Mark every descriptor above the channel close-on-exec, between fork and
/// exec.
///
/// Marked, not closed: descriptors already carrying the flag — everything
/// Rust opens, including the standard library's exec-status pipe — die at
/// exec on their own, and closing that pipe here would turn a failed exec
/// into a silently "successful" spawn. What this retires is the ambient
/// descriptors the host inherited without the flag, which would otherwise
/// survive exec into the worker and everything the worker spawns. Runs
/// inside `pre_exec`, so only async-signal-safe calls: `fcntl` (and on
/// Linux the `close_range` syscall), with the ceiling precomputed by the
/// parent.
fn retire_ambient_descriptors(ceiling: i32) {
    #[cfg(target_os = "linux")]
    {
        // CLOSE_RANGE_CLOEXEC (Linux 5.11+); older kernels fall through.
        let from = (WORKER_CHANNEL_FD + 1) as libc::c_uint;
        let flags = 1u32 << 2;
        if unsafe { libc::syscall(libc::SYS_close_range, from, libc::c_uint::MAX, flags) } == 0 {
            return;
        }
    }
    for fd in (WORKER_CHANNEL_FD + 1)..ceiling {
        unsafe {
            let flags = libc::fcntl(fd, libc::F_GETFD);
            if flags != -1 && flags & libc::FD_CLOEXEC == 0 {
                libc::fcntl(fd, libc::F_SETFD, flags | libc::FD_CLOEXEC);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// On kernels with `close_range` the ceiling goes unread, so this is
    /// the one place the scan itself is pinned on every platform: a sweep
    /// beneath a real hazard is a sweep that missed it.
    #[test]
    fn the_ceiling_covers_an_armed_high_descriptor() {
        let armed = unsafe { libc::fcntl(0, libc::F_DUPFD, 200) };
        assert!(armed >= 200, "the probe descriptor exists");
        let ceiling = fd_table_ceiling();
        unsafe {
            libc::close(armed);
        }
        assert!(
            ceiling > armed,
            "the ceiling ({ceiling}) covers the armed descriptor ({armed})"
        );
    }
}
