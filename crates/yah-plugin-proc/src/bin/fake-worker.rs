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
//! - `deaf`: complete the handshake, then never touch the channel again —
//!   the worker that ignores back-pressure, goodbyes, and end-of-input
//!   alike, so only `SIGKILL` ends it.
//! - `deaf-with-helper`: deaf, plus a sleeping helper (pid on stdout)
//!   spawned first — the worst-case deactivation: every grace window
//!   spent, a descendant still owed to the group sweep.
//! - `leave-group`: move into the host's process group, then go deaf —
//!   reclaimable only by a direct kill of this pid.
//! - `read-shut-linger`: after one call, shut the read half and never
//!   speak again — no goodbye, no end-of-file, no exit.
//! - `spawn-helper`: complete the handshake, spawn a sleeping helper that
//!   inherits the channel fd (printing its pid to stdout), and exit — the
//!   death only the process, not the socket, can reveal.
//! - `goodbye-mid-call`: on the first call, spawn the same fd-holding
//!   helper, send a goodbye, and exit without answering — the polite quit
//!   only a drained buffer distinguishes from a bare disconnect.
//! - `late-reply`: read one call, stop reading, answer it a beat later,
//!   and exit — the terminal that lands while the host is deactivating.
//! - `goodbye-then-linger`: read one call, shut the read half so the
//!   host's writes fail first, send a goodbye a beat later, then linger
//!   until killed — the goodbye a failing write must not eclipse.
//! - `bootstrap-report`: print the inherited environment variable names
//!   and open file descriptors to stdout (the diagnostics lane), then
//!   behave as `conformant`.
//! - `half-prefix`: handshake, then write two bytes of a length prefix
//!   and exit — the partial prefix only a bounded drain can classify.
//! - `half-payload`: handshake, then write a legal prefix declaring 64
//!   bytes, deliver ten, and exit — the partial payload at end-of-input.
//! - `trickle-goodbye`: handshake, then send a goodbye one byte at a
//!   time and exit — input that arrives slower than one read.
//! - `flood`: handshake, then send 400 calls as fast as the wire takes
//!   them without ever reading the refusals, then go deaf — the inbound
//!   flood that must die at the in-flight ceiling, not in host memory.
//! - `diag-then-die`: handshake, print a last line to stdout, exit —
//!   diagnostics written immediately before death.
//! - `hoard-drain-shut:<hoard_ms>:<idle_ms>:<linger_ms>`: read nothing
//!   for hoard_ms — the host's writes pile up in its outbound buffer —
//!   then echo everything until the channel is idle, then shut the read
//!   half and linger: the half-closed output whose buffer the host must
//!   actually release, observed while the worker is still alive.
//! - `chaos`: handshake, print a diagnostic line, flood 300 calls, go
//!   deaf — every pressure at once.
//! - `capability-cycle:<id>`: handshake, then drive one full cycle
//!   against the host's application dispatcher — acquire the named
//!   capability, invoke it, release it — printing each result to stdout,
//!   then serve echo calls until goodbye.
//! - `capability-hostile`: handshake, then probe the dispatcher's refusal
//!   surface — an unknown method, a forged handle, a malformed id, a
//!   double release — printing each refusal kind to stdout, then serve
//!   echo calls until goodbye.
//! - `stream-items:<count>`: handshake, then on the first stream call
//!   open the stream with credit 4, send `count` lossless items (waiting
//!   for credit frames as the window demands), and answer the call ok —
//!   a stream cancel stops production and answers at once.
//! - `capability-flood:<n>`: handshake, acquire the text capability,
//!   then fire `n` invokes back to back without reading replies, printing
//!   each reply's disposition as it arrives — the concurrent load that
//!   meets the dispatcher's queue and concurrency bounds.
//! - `spill:<bytes>`: handshake, then answer the first call with a
//!   spilled offer of `bytes` pattern-filled bytes held worker-side, then
//!   serve `artifact.read` pull requests for it until goodbye — the
//!   digest-carrying offer the host verifies.
//! - `release-*`: spill an offer like `spill`, then misbehave on the
//!   host's Release frame: `release-withhold` never acks,
//!   `release-later:<ms>` acks after a delay, `release-die` exits before
//!   acking, `release-goodbye` says goodbye instead of acking,
//!   `release-ack-wrong-kind` acks with the wrong handle kind, and
//!   `release-bogus-ack` sends an unsolicited ack right after hello —
//!   every loss path a release waiter must survive, typed.
//! - `spill-poison:<mode>`: spill an offer that violates the reader's
//!   contract — wrong chunk lengths (`short`, `long`), a contradictory
//!   media type (`media`), noncanonical hex (`upper`, `junk`), or a
//!   digest over the wrong bytes (`digest`) or over zero bytes
//!   (`empty-digest`) — and otherwise serve honestly.
//! - `stream-stall:<credit>`: open the host's stream, spend the initial
//!   credit window on lossless items, then stop and print one line per
//!   Credit frame received — the overgrant detector.
//! - `stream-lossy-flood:<count>`: open the host's stream, flood `count`
//!   lossy items past any window, then send one final credited lossless
//!   item before the terminal — the reservation detector.
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

#[path = "fake-worker/scripts.rs"]
mod scripts;

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
                            // The answered id on the diagnostics lane lets
                            // a test prove this reply was really sent —
                            // e.g. as a tolerated late terminal.
                            println!("answered:{}", cancel.call_id.0);
                            let _ = std::io::stdout().flush();
                        }
                    }
                    Some(HostMessage::Goodbye(_)) | None => return 0,
                    Some(_) => {}
                }
            }
        }
        "deaf" => {
            if !wire.handshake() {
                return 70;
            }
            // Not even an EOF read: this worker's only exit is the kill.
            loop {
                std::thread::sleep(std::time::Duration::from_secs(3600));
            }
        }
        "deaf-with-helper" => {
            if !wire.handshake() {
                return 70;
            }
            // Deaf AND holding a descendant: the shape that spends the
            // whole deactivation budget — the goodbye flush, the reap
            // grace — and still owes the group sweep a helper.
            match std::process::Command::new("/bin/sleep").arg("30").spawn() {
                Ok(helper) => {
                    println!("helper:{}", helper.id());
                    let _ = std::io::stdout().flush();
                }
                Err(error) => {
                    eprintln!("helper did not spawn: {error}");
                    return 70;
                }
            }
            loop {
                std::thread::sleep(std::time::Duration::from_secs(3600));
            }
        }
        "leave-group" => {
            if !wire.handshake() {
                return 70;
            }
            // Move out of the group the bootstrap made this process lead,
            // then go deaf: the group sweep now signals an empty group,
            // and only a direct kill of this pid reclaims anything.
            let joined = unsafe { libc::setpgid(0, libc::getpgid(libc::getppid())) == 0 };
            println!("left-group:{}", if joined { "ok" } else { "err" });
            let _ = std::io::stdout().flush();
            loop {
                std::thread::sleep(std::time::Duration::from_secs(3600));
            }
        }
        "read-shut-linger" => {
            if !wire.handshake() {
                return 70;
            }
            // Shut the read half after one call and never speak again: no
            // goodbye, no end-of-file, no exit — the half-death only
            // health can report and only deactivation can end.
            loop {
                match wire.next_frame() {
                    Some(HostMessage::Call(_)) => {
                        let _ = wire.channel.shutdown(std::net::Shutdown::Read);
                        loop {
                            std::thread::sleep(std::time::Duration::from_secs(3600));
                        }
                    }
                    Some(HostMessage::Goodbye(_)) | None => return 0,
                    Some(_) => {}
                }
            }
        }
        "spawn-helper" => {
            if !wire.handshake() {
                return 70;
            }
            // The helper inherits fd 3 (and this process's group), so the
            // host's socket never reaches end-of-file on this exit. Its
            // pid rides the diagnostics lane so a test can prove the
            // group sweep really took it.
            match std::process::Command::new("/bin/sleep").arg("30").spawn() {
                Ok(helper) => {
                    println!("helper:{}", helper.id());
                    let _ = std::io::stdout().flush();
                    0
                }
                Err(error) => {
                    eprintln!("helper did not spawn: {error}");
                    70
                }
            }
        }
        "goodbye-then-linger" => {
            if !wire.handshake() {
                return 70;
            }
            // Read one call, then shut this end's READ half — the host's
            // next write fails while nothing is yet readable, so the
            // failing write provably comes first — and only then send the
            // goodbye, lingering until killed. The goodbye must still
            // decide the close.
            loop {
                match wire.next_frame() {
                    Some(HostMessage::Call(_)) => {
                        let _ = wire.channel.shutdown(std::net::Shutdown::Read);
                        std::thread::sleep(std::time::Duration::from_millis(200));
                        wire.send(&WorkerMessage::Goodbye(Goodbye {
                            reason: "worker stopping".to_owned(),
                        }));
                        loop {
                            std::thread::sleep(std::time::Duration::from_secs(3600));
                        }
                    }
                    Some(HostMessage::Goodbye(_)) | None => return 0,
                    Some(_) => {}
                }
            }
        }
        "late-reply" => {
            if !wire.handshake() {
                return 70;
            }
            // Read one call, go deaf, then answer it a beat later and
            // exit: the terminal that lands in the host's receive buffer
            // while the host is already deactivating.
            loop {
                match wire.next_frame() {
                    Some(HostMessage::Call(call)) => {
                        std::thread::sleep(std::time::Duration::from_millis(25));
                        wire.send(&WorkerMessage::Reply(Reply {
                            call_id: call.call_id,
                            outcome: Outcome::Ok {
                                result: call.payload,
                            },
                        }));
                        return 0;
                    }
                    Some(HostMessage::Goodbye(_)) | None => return 0,
                    Some(_) => {}
                }
            }
        }
        "goodbye-mid-call" => {
            if !wire.handshake() {
                return 70;
            }
            // A worker that quits politely with work in hand: goodbye,
            // then exit without answering — while a spawned helper keeps
            // fd 3 open, so only the buffered goodbye distinguishes this
            // from a bare disconnect.
            loop {
                match wire.next_frame() {
                    Some(HostMessage::Call(_)) => {
                        let _ = std::process::Command::new("/bin/sleep").arg("30").spawn();
                        wire.send(&WorkerMessage::Goodbye(Goodbye {
                            reason: "worker stopping".to_owned(),
                        }));
                        return 0;
                    }
                    Some(HostMessage::Goodbye(_)) | None => return 0,
                    Some(_) => {}
                }
            }
        }
        "bootstrap-report" => {
            let mut names: Vec<String> = std::env::vars_os()
                .map(|(name, _)| name.to_string_lossy().into_owned())
                .collect();
            names.sort();
            println!("env:{}", names.join(","));
            // Probe by fcntl rather than listing /dev/fd, which would open
            // a descriptor of its own and report it.
            let fds: Vec<String> = (0..64)
                .filter(|&fd| unsafe { libc::fcntl(fd, libc::F_GETFD) } != -1)
                .map(|fd| fd.to_string())
                .collect();
            println!("fds:{}", fds.join(","));
            let _ = std::io::stdout().flush();
            serve_conformant(wire)
        }
        "conformant" => serve_conformant(wire),
        "half-prefix" => {
            if !wire.handshake() {
                return 70;
            }
            // Two bytes of a four-byte prefix, then gone: end-of-input
            // lands mid-prefix.
            let _ = wire.write_raw(&[0, 0]);
            70
        }
        "half-payload" => {
            if !wire.handshake() {
                return 70;
            }
            // A legal prefix declaring 64 bytes, ten delivered, then gone:
            // end-of-input lands mid-frame.
            let mut raw = 64u32.to_be_bytes().to_vec();
            raw.extend_from_slice(b"[{\"partial\":");
            let _ = wire.write_raw(&raw);
            70
        }
        "trickle-goodbye" => {
            if !wire.handshake() {
                return 70;
            }
            // The goodbye, one byte per millisecond: slower than any
            // single read, well inside the drain's wall-clock bound.
            let bytes = serde_json::to_vec(&WorkerMessage::Goodbye(Goodbye {
                reason: "trickled".to_owned(),
            }))
            .expect("goodbye serializes");
            let framed = frame::encode(&bytes);
            for byte in framed {
                let _ = wire.write_raw(&[byte]);
                std::thread::sleep(std::time::Duration::from_millis(1));
            }
            0
        }
        "flood" => {
            if !wire.handshake() {
                return 70;
            }
            for id in 1..=400u64 {
                wire.send(&WorkerMessage::Call(Call {
                    call_id: CallId(id),
                    method: "flood".to_owned(),
                    deadline_ms: None,
                    stream: false,
                    payload: serde_json::json!(null),
                }));
            }
            println!("flooded:400");
            let _ = std::io::stdout().flush();
            loop {
                std::thread::sleep(std::time::Duration::from_secs(3600));
            }
        }
        "diag-then-die" => {
            if !wire.handshake() {
                return 70;
            }
            println!("last-words:retained");
            let _ = std::io::stdout().flush();
            70
        }
        "chaos" => {
            if !wire.handshake() {
                return 70;
            }
            println!("chaos:begin");
            let _ = std::io::stdout().flush();
            for id in 1..=300u64 {
                wire.send(&WorkerMessage::Call(Call {
                    call_id: CallId(id),
                    method: "chaos".to_owned(),
                    deadline_ms: None,
                    stream: false,
                    payload: serde_json::json!(null),
                }));
            }
            loop {
                std::thread::sleep(std::time::Duration::from_secs(3600));
            }
        }
        m if m.starts_with("hoard-drain-shut") => {
            if !wire.handshake() {
                return 70;
            }
            // `hoard-drain-shut:<hoard_ms>:<idle_ms>:<linger_ms>`: read
            // nothing for hoard_ms — the host's writes pile up in its
            // outbound buffer — then echo every frame until the channel
            // goes quiet for idle_ms, shut the read half, and linger.
            // The host's buffer is empty and its socket buffer drained
            // when the read half dies, so its next write fails into a
            // half-close that must release what the hoard allocated.
            let mut delays = m.split(':').skip(1);
            let hoard: u64 = delays.next().and_then(|v| v.parse().ok()).unwrap_or(400);
            let idle: u64 = delays.next().and_then(|v| v.parse().ok()).unwrap_or(300);
            let linger: u64 = delays.next().and_then(|v| v.parse().ok()).unwrap_or(30_000);
            std::thread::sleep(std::time::Duration::from_millis(hoard));
            let _ = wire
                .channel
                .set_read_timeout(Some(std::time::Duration::from_millis(idle)));
            // Ack small: an echoed 250-KiB payload would exceed the
            // host's inline-result bound and fault the session.
            while let Some(HostMessage::Call(call)) = wire.next_frame() {
                wire.send(&WorkerMessage::Reply(Reply {
                    call_id: call.call_id,
                    outcome: Outcome::Ok {
                        result: serde_json::json!({ "acked": call.call_id.0 }),
                    },
                }));
            }
            let _ = wire.channel.shutdown(std::net::Shutdown::Read);
            std::thread::sleep(std::time::Duration::from_millis(linger));
            0
        }
        m if m.starts_with("capability-cycle:") => {
            let capability = m.split(':').nth(1).unwrap_or("text.upper").to_owned();
            scripts::capability_cycle(wire, &capability)
        }
        "capability-hostile" => scripts::capability_hostile(wire),
        m if m.starts_with("stream-items:") => {
            let count: u64 = m
                .split(':')
                .nth(1)
                .and_then(|v| v.parse().ok())
                .unwrap_or(4);
            scripts::stream_items(wire, count)
        }
        m if m.starts_with("capability-flood:") => {
            let count: u64 = m
                .split(':')
                .nth(1)
                .and_then(|v| v.parse().ok())
                .unwrap_or(4);
            scripts::capability_flood(wire, count)
        }
        m if m.starts_with("release-withhold") => scripts::release_withhold(wire),
        m if m.starts_with("release-die") => scripts::release_die(wire),
        m if m.starts_with("release-goodbye") => scripts::release_goodbye(wire),
        m if m.starts_with("release-ack-wrong-kind") => scripts::release_ack_wrong_kind(wire),
        m if m.starts_with("release-bogus-ack") => scripts::release_bogus_ack(wire),
        m if m.starts_with("release-later:") => {
            let ms: u64 = m
                .split(':')
                .nth(1)
                .and_then(|v| v.parse().ok())
                .unwrap_or(500);
            scripts::release_later(wire, ms)
        }
        m if m.starts_with("spill-poison:") => {
            let mode = m.split(':').nth(1).unwrap_or("short").to_owned();
            scripts::spill_poison(wire, &mode)
        }
        m if m.starts_with("stream-stall:") => {
            let credit: u32 = m
                .split(':')
                .nth(1)
                .and_then(|v| v.parse().ok())
                .unwrap_or(16);
            scripts::stream_stall(wire, credit)
        }
        m if m.starts_with("stream-lossy-flood:") => {
            let count: u64 = m
                .split(':')
                .nth(1)
                .and_then(|v| v.parse().ok())
                .unwrap_or(3000);
            scripts::stream_lossy_flood(wire, count)
        }
        m if m.starts_with("spill:") => {
            let bytes: usize = m
                .split(':')
                .nth(1)
                .and_then(|v| v.parse().ok())
                .unwrap_or(4096);
            scripts::spill(wire, bytes)
        }

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

    /// Write raw bytes to the channel, ignoring failure — the partial
    /// and trickled writes are the point.
    fn write_raw(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.channel.write(bytes)
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
