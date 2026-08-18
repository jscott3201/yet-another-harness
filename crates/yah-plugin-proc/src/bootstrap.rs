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
    let worker_fd = worker_end.into_raw_fd();
    unsafe {
        spawn.pre_exec(move || {
            if worker_fd == WORKER_CHANNEL_FD {
                // `dup2` onto itself would leave close-on-exec set; clear
                // the flag directly instead.
                let flags = libc::fcntl(WORKER_CHANNEL_FD, libc::F_GETFD);
                if flags == -1 {
                    return Err(io::Error::last_os_error());
                }
                if libc::fcntl(WORKER_CHANNEL_FD, libc::F_SETFD, flags & !libc::FD_CLOEXEC) == -1 {
                    return Err(io::Error::last_os_error());
                }
            } else if libc::dup2(worker_fd, WORKER_CHANNEL_FD) == -1 {
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
    Ok(SpawnedWorker { child, channel })
}
