//! Application-call, stream, spill, and release scripts.

use std::io::Write;

use yah_plugin_ipc::types::*;

use super::Wire;

#[path = "scripts/capability.rs"]
mod capability;
#[path = "scripts/spill.rs"]
mod spill;

pub use capability::{
    acquire_probe as capability_acquire_probe, basic as capability_basic,
    cancel_during as capability_cancel_during, cancel_queued as capability_cancel_queued,
    handle_limit as capability_handle_limit,
    malformed_and_unknown as capability_malformed_and_unknown,
    provider_outcomes as capability_provider_outcomes, reclaim as capability_reclaim,
    release_order as capability_release_order, replacement as capability_replacement,
};

pub use spill::{
    release_bogus_ack, release_die, release_goodbye, release_later, release_withhold, spill,
    spill_poison,
};

fn call(id: u64, method: &str, payload: serde_json::Value) -> WorkerMessage {
    WorkerMessage::Call(Call {
        call_id: CallId(id),
        method: method.to_owned(),
        deadline_ms: None,
        stream: false,
        payload,
    })
}

fn flush() {
    let _ = std::io::stdout().flush();
}

/// Exercise an immutable registered method twice while continuing to answer
/// host calls. The first probe is an unknown worker-authored method whose text
/// must not be reflected by the refusal.
pub fn registered_method(wire: &mut Wire, method: &str) -> i32 {
    if !wire.handshake() {
        return 70;
    }
    wire.send(&call(1, "host.demand.secrets", serde_json::json!(null)));
    match wire.next_frame() {
        Some(HostMessage::Reply(Reply {
            call_id: CallId(1),
            outcome: Outcome::Err { error },
        })) => println!(
            "registered:unknown:{:?}:echoed={}",
            error.kind,
            error.message.contains("host.demand.secrets")
        ),
        other => println!("registered:unknown:unexpected:{other:?}"),
    }
    flush();
    wire.send(&call(2, method, serde_json::json!({ "call": 2 })));
    wire.send(&call(3, method, serde_json::json!({ "call": 3 })));
    let mut completed = 0;
    loop {
        match wire.next_frame() {
            Some(HostMessage::Call(host)) => wire.send(&WorkerMessage::Reply(Reply {
                call_id: host.call_id,
                outcome: Outcome::Ok {
                    result: host.payload,
                },
            })),
            Some(HostMessage::Reply(Reply {
                call_id: CallId(2 | 3),
                outcome: Outcome::Ok { .. },
            })) => {
                completed += 1;
                if completed == 2 {
                    println!("registered:done");
                    flush();
                    return super::serve_after_handshake(wire);
                }
            }
            Some(HostMessage::Goodbye(_)) | None => return 0,
            Some(_) => {}
        }
    }
}

/// Send a worker call, then wait for a host control call before cancelling it.
/// This gives the host a deterministic barrier proving cancellation happened
/// during callback execution rather than before dispatch.
pub fn registered_cancel_during(wire: &mut Wire) -> i32 {
    if !wire.handshake() {
        return 70;
    }
    wire.send(&call(1, "application.cancel", serde_json::json!(null)));
    let mut cancelled = false;
    loop {
        match wire.next_frame() {
            Some(HostMessage::Call(host)) if host.method == "control.cancel" => {
                wire.send(&WorkerMessage::Cancel(Cancel {
                    call_id: CallId(1),
                    target: CancelTarget::Call,
                }));
                cancelled = true;
                wire.send(&WorkerMessage::Reply(Reply {
                    call_id: host.call_id,
                    outcome: Outcome::Ok {
                        result: serde_json::json!({ "cancel_sent": true }),
                    },
                }));
                println!("method-cancel:sent");
                flush();
            }
            Some(HostMessage::Reply(Reply {
                call_id: CallId(1),
                outcome: Outcome::Cancelled { .. },
            })) if cancelled => {
                println!("method-cancel:cancelled");
                flush();
                return super::serve_after_handshake(wire);
            }
            Some(HostMessage::Goodbye(_)) | None => return 0,
            Some(_) => {}
        }
    }
}

/// Occupy the sole provider slot, queue a second worker call, and cancel the
/// queued call before it can dispatch.
pub fn registered_cancel_queued(wire: &mut Wire) -> i32 {
    if !wire.handshake() {
        return 70;
    }
    wire.send(&call(1, "application.hold", serde_json::json!(null)));
    wire.send(&call(2, "application.cancel", serde_json::json!(null)));
    wire.send(&WorkerMessage::Cancel(Cancel {
        call_id: CallId(2),
        target: CancelTarget::Call,
    }));
    loop {
        match wire.next_frame() {
            Some(HostMessage::Reply(Reply {
                call_id: CallId(2),
                outcome: Outcome::Cancelled { .. },
            })) => {
                println!("method-cancel:queued");
                flush();
            }
            Some(HostMessage::Reply(Reply {
                call_id: CallId(1), ..
            })) => return super::serve_after_handshake(wire),
            Some(HostMessage::Goodbye(_)) | None => return 0,
            Some(_) => {}
        }
    }
}

/// A panicking method and a sibling call. Both terminals are reported without
/// reflecting panic text.
pub fn registered_panic(wire: &mut Wire) -> i32 {
    if !wire.handshake() {
        return 70;
    }
    wire.send(&call(1, "application.panic", serde_json::json!(null)));
    wire.send(&call(2, "application.ok", serde_json::json!(null)));
    let mut seen = 0;
    loop {
        match wire.next_frame() {
            Some(HostMessage::Reply(Reply { call_id, outcome }))
                if matches!(call_id, CallId(1 | 2)) =>
            {
                seen += 1;
                match outcome {
                    Outcome::Ok { .. } => println!("method-panic:{}:ok", call_id.0),
                    Outcome::Err { error } => println!(
                        "method-panic:{}:{:?}:echoed={}",
                        call_id.0,
                        error.kind,
                        error.message.contains("panic-secret")
                    ),
                    other => println!("method-panic:{}:{other:?}", call_id.0),
                }
                flush();
                if seen == 2 {
                    return super::serve_after_handshake(wire);
                }
            }
            Some(HostMessage::Goodbye(_)) | None => return 0,
            Some(_) => {}
        }
    }
}

/// Report the bounded Unicode detail returned by an application-authored
/// domain failure.
pub fn registered_failure(wire: &mut Wire) -> i32 {
    if !wire.handshake() {
        return 70;
    }
    wire.send(&call(1, "application.failure", serde_json::json!(null)));
    match wire.next_frame() {
        Some(HostMessage::Reply(Reply {
            call_id: CallId(1),
            outcome: Outcome::Err { error },
        })) => println!(
            "method-failure:{:?}:chars={}",
            error.kind,
            error.message.chars().count()
        ),
        other => println!("method-failure:unexpected:{other:?}"),
    }
    flush();
    super::serve_after_handshake(wire)
}

/// Flood one registered method without reading replies, exposing the shared
/// dispatch queue and provider-concurrency bounds.
pub fn registered_flood(wire: &mut Wire, count: u64) -> i32 {
    if !wire.handshake() {
        return 70;
    }
    for id in 1..=count {
        wire.send(&call(
            id,
            "application.hold",
            serde_json::json!({ "call": id }),
        ));
    }
    let mut seen = 0;
    while seen < count {
        match wire.next_frame() {
            Some(HostMessage::Reply(Reply { call_id, outcome })) => {
                seen += 1;
                match outcome {
                    Outcome::Ok { .. } => println!("method-flood:{}:ok", call_id.0),
                    Outcome::Err { error } => println!(
                        "method-flood:{}:{:?}:retryable={}",
                        call_id.0, error.kind, error.retryable
                    ),
                    other => println!("method-flood:{}:{other:?}", call_id.0),
                }
                flush();
            }
            Some(HostMessage::Goodbye(_)) | None => return 0,
            Some(_) => {}
        }
    }
    println!("method-flood:done");
    flush();
    super::serve_after_handshake(wire)
}

pub fn stream_items(wire: &mut Wire, count: u64) -> i32 {
    if !wire.handshake() {
        return 70;
    }
    let call_id = loop {
        match wire.next_frame() {
            Some(HostMessage::Call(call)) if call.stream => break call.call_id,
            Some(HostMessage::Goodbye(_)) | None => return 0,
            Some(_) => {}
        }
    };
    wire.send(&WorkerMessage::StreamOpen(StreamOpen {
        call_id,
        credit: 4,
    }));
    let mut credit = 4u32;
    for seq in 0..count {
        while credit == 0 {
            match wire.next_frame() {
                Some(HostMessage::Credit(frame)) if frame.call_id == call_id => {
                    credit += frame.additional;
                }
                Some(HostMessage::Cancel(cancel)) if cancel.call_id == call_id => {
                    return answer_cancelled(wire, call_id);
                }
                Some(HostMessage::Goodbye(_)) | None => return 0,
                Some(_) => {}
            }
        }
        credit -= 1;
        wire.send(&WorkerMessage::StreamData(StreamData {
            call_id,
            seq,
            more: seq + 1 < count,
            class: StreamClass::Lossless,
            dropped: 0,
            payload: serde_json::json!({ "n": seq }),
        }));
    }
    wire.send(&WorkerMessage::Reply(Reply {
        call_id,
        outcome: Outcome::Ok {
            result: serde_json::json!({ "streamed": count }),
        },
    }));
    super::serve_after_handshake(wire)
}

pub fn stream_stall(wire: &mut Wire, initial_credit: u32) -> i32 {
    if !wire.handshake() {
        return 70;
    }
    let call_id = loop {
        match wire.next_frame() {
            Some(HostMessage::Call(call)) if call.stream => break call.call_id,
            Some(HostMessage::Goodbye(_)) | None => return 0,
            Some(_) => {}
        }
    };
    wire.send(&WorkerMessage::StreamOpen(StreamOpen {
        call_id,
        credit: initial_credit,
    }));
    for seq in 0..initial_credit {
        wire.send(&WorkerMessage::StreamData(StreamData {
            call_id,
            seq: u64::from(seq),
            more: true,
            class: StreamClass::Lossless,
            dropped: 0,
            payload: serde_json::json!({ "n": seq }),
        }));
    }
    println!("stall:sent:{initial_credit}");
    flush();
    let mut grants = 0u64;
    loop {
        match wire.next_frame() {
            Some(HostMessage::Credit(frame)) if frame.call_id == call_id => {
                grants += 1;
                println!("stall:credit:{}", frame.additional);
                flush();
            }
            Some(HostMessage::Call(probe)) if probe.method == "control.probe" => {
                println!("stall:probe:{grants}");
                flush();
                wire.send(&WorkerMessage::Reply(Reply {
                    call_id: probe.call_id,
                    outcome: Outcome::Ok {
                        result: serde_json::json!({ "grants": grants }),
                    },
                }));
            }
            Some(HostMessage::Cancel(cancel)) if cancel.call_id == call_id => {
                return answer_cancelled(wire, call_id);
            }
            Some(HostMessage::Goodbye(_)) | None => return 0,
            Some(_) => {}
        }
    }
}

pub fn stream_lossy_flood(wire: &mut Wire, count: u64) -> i32 {
    if !wire.handshake() {
        return 70;
    }
    let call_id = loop {
        match wire.next_frame() {
            Some(HostMessage::Call(call)) if call.stream => break call.call_id,
            Some(HostMessage::Goodbye(_)) | None => return 0,
            Some(_) => {}
        }
    };
    wire.send(&WorkerMessage::StreamOpen(StreamOpen {
        call_id,
        credit: 4,
    }));
    for seq in 0..count {
        wire.send(&WorkerMessage::StreamData(StreamData {
            call_id,
            seq,
            more: true,
            class: StreamClass::Lossy,
            dropped: 0,
            payload: serde_json::json!({ "n": seq }),
        }));
    }
    wire.send(&WorkerMessage::StreamData(StreamData {
        call_id,
        seq: count,
        more: false,
        class: StreamClass::Lossless,
        dropped: 0,
        payload: serde_json::json!({ "final": true }),
    }));
    wire.send(&WorkerMessage::Reply(Reply {
        call_id,
        outcome: Outcome::Ok {
            result: serde_json::json!({ "streamed": count }),
        },
    }));
    loop {
        match wire.next_frame() {
            Some(HostMessage::Cancel(cancel)) if cancel.call_id == call_id => {
                return answer_cancelled(wire, call_id);
            }
            Some(HostMessage::Goodbye(_)) | None => return 0,
            Some(_) => {}
        }
    }
}

fn answer_cancelled(wire: &mut Wire, call_id: CallId) -> i32 {
    wire.send(&WorkerMessage::Reply(Reply {
        call_id,
        outcome: Outcome::Cancelled {
            reason: CancelReason::Requested,
        },
    }));
    0
}
