//! Scripted worker for the process driver's tests.
//!
//! Speaks protocol v1 over inherited fd 3 with plain blocking IO — no SDK,
//! no runtime, no cleverness, so what a test observes is the driver's
//! behaviour and not this binary's. The single argument names the script:
//!
//! - `conformant`: hello, then serve echo calls until goodbye or EOF.
//! - `silent`: connect and never speak; the pending-start worker.
//! - `bad-version`: offer only protocol version 99 and read the refusal.
//! - `crash-after-hello`: complete the handshake, then exit without goodbye.
//! - `exit-mid-call`: take one call and exit without answering it.
//! - `cancel-ack`: hold each call until its cancel arrives, then answer
//!   with the cancelled outcome.
//! - `env-report`: print the inherited environment variable names to
//!   stdout (the diagnostics lane), then behave as `conformant`.
//!
//! This is a test fixture, not a worker SDK: it implements exactly what its
//! scripts need and nothing else.

use std::io::{Read, Write};
use std::os::fd::FromRawFd;
use std::os::unix::net::UnixStream;

use yah_plugin_ipc::PROTOCOL_VERSION;
use yah_plugin_ipc::frame::{self, FrameDecoder};
use yah_plugin_ipc::types::*;
use yah_plugin_proc::WORKER_CHANNEL_FD;

fn main() {
    let mode = std::env::args().nth(1).unwrap_or_default();
    // SAFETY: the driver contract places the channel on this fd and nothing
    // else in this process owns it.
    let channel = unsafe { UnixStream::from_raw_fd(WORKER_CHANNEL_FD) };
    let mut wire = Wire::new(channel);
    let code = run(&mode, &mut wire);
    std::process::exit(code);
}

fn run(mode: &str, wire: &mut Wire) -> i32 {
    match mode {
        "silent" => {
            // Never a byte; exit only when the host is gone.
            while wire.next_frame().is_some() {}
            0
        }
        "bad-version" => {
            wire.send_hello(&[99]);
            match wire.next_frame() {
                Some(HostMessage::Refuse(refuse)) => {
                    eprintln!("refused as expected: {:?}", refuse.error.kind);
                    2
                }
                other => {
                    eprintln!("expected a refuse, got {other:?}");
                    70
                }
            }
        }
        "crash-after-hello" => {
            if !wire.handshake() {
                return 70;
            }
            // No goodbye on purpose: this is the bare-disconnect script.
            70
        }
        "exit-mid-call" => {
            if !wire.handshake() {
                return 70;
            }
            loop {
                match wire.next_frame() {
                    Some(HostMessage::Call(_)) => return 70,
                    Some(_) => {}
                    None => return 70,
                }
            }
        }
        "cancel-ack" => {
            if !wire.handshake() {
                return 70;
            }
            let mut held: Option<CallId> = None;
            loop {
                match wire.next_frame() {
                    Some(HostMessage::Call(call)) => held = Some(call.call_id),
                    Some(HostMessage::Cancel(cancel)) => {
                        if held.take() == Some(cancel.call_id) {
                            wire.send(&WorkerMessage::Reply(Reply {
                                call_id: cancel.call_id,
                                outcome: Outcome::Cancelled {
                                    reason: CancelReason::Requested,
                                },
                            }));
                        }
                    }
                    Some(HostMessage::Goodbye(_)) | None => return 0,
                    Some(_) => {}
                }
            }
        }
        "env-report" => {
            let mut names: Vec<String> = std::env::vars_os()
                .map(|(name, _)| name.to_string_lossy().into_owned())
                .collect();
            names.sort();
            println!("env:{}", names.join(","));
            let _ = std::io::stdout().flush();
            serve_conformant(wire)
        }
        "conformant" => serve_conformant(wire),
        other => {
            eprintln!("unknown fake-worker mode {other:?}");
            64
        }
    }
}

/// Handshake, then answer every call by echoing its payload, until the host
/// says goodbye or closes the channel.
fn serve_conformant(wire: &mut Wire) -> i32 {
    if !wire.handshake() {
        return 70;
    }
    loop {
        match wire.next_frame() {
            Some(HostMessage::Call(call)) => {
                wire.send(&WorkerMessage::Reply(Reply {
                    call_id: call.call_id,
                    outcome: Outcome::Ok {
                        result: call.payload,
                    },
                }));
            }
            Some(HostMessage::Goodbye(_)) | None => return 0,
            Some(_) => {}
        }
    }
}

struct Wire {
    channel: UnixStream,
    decoder: FrameDecoder,
}

impl Wire {
    fn new(channel: UnixStream) -> Self {
        Self {
            channel,
            decoder: FrameDecoder::new(),
        }
    }

    fn send_hello(&mut self, versions: &[u32]) {
        self.send(&WorkerMessage::Hello(Hello {
            protocol_versions: versions.to_vec(),
            sdk_name: "fake-worker".to_owned(),
            sdk_version: "0.0.1".to_owned(),
            features: Vec::new(),
            required_features: Vec::new(),
        }));
    }

    /// Hello and await the accept. False on refusal or a closed channel.
    fn handshake(&mut self) -> bool {
        self.send_hello(&[PROTOCOL_VERSION]);
        matches!(self.next_frame(), Some(HostMessage::Accept(_)))
    }

    fn send(&mut self, message: &WorkerMessage) {
        let bytes = serde_json::to_vec(message).expect("worker message serializes");
        if self.channel.write_all(&frame::encode(&bytes)).is_err() {
            // The host is gone; nothing useful remains to do.
            std::process::exit(0);
        }
    }

    /// The next decoded host frame, or `None` once the channel is done.
    fn next_frame(&mut self) -> Option<HostMessage> {
        let mut chunk = [0u8; 4096];
        loop {
            match self.decoder.next_frame() {
                Ok(Some(bytes)) => {
                    let message: HostMessage =
                        serde_json::from_slice(&bytes).expect("host frame decodes");
                    return Some(message);
                }
                Ok(None) => {}
                Err(error) => panic!("host framing violation: {error:?}"),
            }
            match self.channel.read(&mut chunk) {
                Ok(0) | Err(_) => return None,
                Ok(count) => self.decoder.feed(&chunk[..count]),
            }
        }
    }
}
