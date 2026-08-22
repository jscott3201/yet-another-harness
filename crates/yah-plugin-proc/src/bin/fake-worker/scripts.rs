//! The application-facing scripts: capability cycles through the host's
//! dispatcher, hostile refusal probes, worker-to-host item streams, and
//! spilled-artifact pull-reads. Split from the transport-only scripts so
//! each stays reviewable against the surface it exercises.

use yah_plugin_ipc::types::*;

use super::Wire;

/// Acquire, invoke, and release one granted text capability through
/// the dispatcher, reporting each step on stdout.
#[path = "scripts/spill.rs"]
mod spill;

pub use spill::{
    release_ack_wrong_kind, release_bogus_ack, release_die, release_goodbye, release_later,
    release_withhold, spill, spill_poison,
};

pub fn capability_cycle(wire: &mut Wire, capability: &str) -> i32 {
    if !wire.handshake() {
        return 70;
    }
    // Acquire: the host mints a wire handle for the granted
    // capability.
    wire.send(&WorkerMessage::Call(Call {
        call_id: CallId(1),
        method: "capability.acquire".to_owned(),
        deadline_ms: None,
        stream: false,
        payload: serde_json::json!({ "capability": capability }),
    }));
    let handle = match wire.next_frame() {
        Some(HostMessage::Reply(Reply {
            call_id: CallId(1),
            outcome: Outcome::Ok { result },
        })) => result["handle"].as_u64(),
        other => {
            println!("cap:acquire:unexpected:{other:?}");
            std::io::Write::flush(&mut std::io::stdout()).ok();
            return 70;
        }
    };
    let Some(handle) = handle else {
        println!("cap:acquire:no-handle");
        std::io::Write::flush(&mut std::io::stdout()).ok();
        return 70;
    };
    println!("cap:acquired:{handle}");
    std::io::Write::flush(&mut std::io::stdout()).ok();
    // Invoke through the same handle: the provider's answer
    // crosses as the call result.
    wire.send(&WorkerMessage::Call(Call {
        call_id: CallId(2),
        method: "capability.invoke".to_owned(),
        deadline_ms: None,
        stream: false,
        payload: serde_json::json!({ "handle": handle, "input": "hola" }),
    }));
    match wire.next_frame() {
        Some(HostMessage::Reply(Reply {
            call_id: CallId(2),
            outcome: Outcome::Ok { result },
        })) => println!("cap:invoked:{result}"),
        other => println!("cap:invoke:unexpected:{other:?}"),
    }
    std::io::Write::flush(&mut std::io::stdout()).ok();
    // A second invoke through the same handle: a handle stays invocable
    // until its release — single-use handles would strand the id against
    // the live-handle ceiling.
    wire.send(&WorkerMessage::Call(Call {
        call_id: CallId(4),
        method: "capability.invoke".to_owned(),
        deadline_ms: None,
        stream: false,
        payload: serde_json::json!({ "handle": handle, "input": "hola" }),
    }));
    match wire.next_frame() {
        Some(HostMessage::Reply(Reply {
            call_id: CallId(4),
            outcome: Outcome::Ok { result },
        })) => println!("cap:reinvoked:{result}"),
        other => println!("cap:reinvoke:unexpected:{other:?}"),
    }
    std::io::Write::flush(&mut std::io::stdout()).ok();
    // An over-bound result comes back as a spilled offer, not a lost
    // terminal.
    wire.send(&WorkerMessage::Call(Call {
        call_id: CallId(6),
        method: "capability.invoke".to_owned(),
        deadline_ms: None,
        stream: false,
        payload: serde_json::json!({ "handle": handle, "input": "big" }),
    }));
    match wire.next_frame() {
        Some(HostMessage::Reply(Reply {
            call_id: CallId(6),
            outcome,
        })) => match outcome {
            Outcome::Spilled { artifact } => println!("cap:big:spilled:{}", artifact.bytes),
            Outcome::Ok { .. } => println!("cap:big:inline"),
            other => println!("cap:big:{other:?}"),
        },
        other => println!("cap:big:unexpected:{other:?}"),
    }
    std::io::Write::flush(&mut std::io::stdout()).ok();
    // Release: the id is spent; a second release must be refused.
    wire.send(&WorkerMessage::Call(Call {
        call_id: CallId(5),
        method: "capability.release".to_owned(),
        deadline_ms: None,
        stream: false,
        payload: serde_json::json!({ "handle": handle }),
    }));
    match wire.next_frame() {
        Some(HostMessage::Reply(Reply {
            call_id: CallId(5),
            outcome: Outcome::Ok { .. },
        })) => println!("cap:released:{handle}"),
        other => println!("cap:release:unexpected:{other:?}"),
    }
    std::io::Write::flush(&mut std::io::stdout()).ok();
    super::serve_after_handshake(wire)
}

/// Acquire one capability, invoke it once, and remain in the already-negotiated
/// session until the host ends it. This isolates the lifecycle test's blocked
/// callback from the full-cycle fixture's later protocol work.
pub fn capability_hold(wire: &mut Wire, capability: &str) -> i32 {
    if !wire.handshake() {
        return 70;
    }
    wire.send(&WorkerMessage::Call(Call {
        call_id: CallId(1),
        method: "capability.acquire".to_owned(),
        deadline_ms: None,
        stream: false,
        payload: serde_json::json!({ "capability": capability }),
    }));
    let handle = match wire.next_frame() {
        Some(HostMessage::Reply(Reply {
            call_id: CallId(1),
            outcome: Outcome::Ok { result },
        })) => result["handle"].as_u64(),
        _ => None,
    };
    let Some(handle) = handle else {
        return 70;
    };
    println!("hold:acquired:{handle}");
    std::io::Write::flush(&mut std::io::stdout()).ok();
    wire.send(&WorkerMessage::Call(Call {
        call_id: CallId(2),
        method: "capability.invoke".to_owned(),
        deadline_ms: None,
        stream: false,
        payload: serde_json::json!({ "handle": handle, "input": "hold" }),
    }));
    loop {
        match wire.next_frame() {
            Some(HostMessage::Goodbye(_)) | None => return 0,
            Some(_) => {}
        }
    }
}

/// Exercise an independently registered method twice under a bounded handler
/// slot, while continuing to answer an ordinary host call from the pump.
pub fn registered_method(wire: &mut Wire, method: &str) -> i32 {
    if !wire.handshake() {
        return 70;
    }
    wire.send(&WorkerMessage::Call(Call {
        call_id: CallId(1),
        method: "application.unknown".to_owned(),
        deadline_ms: None,
        stream: false,
        payload: serde_json::json!(null),
    }));
    match wire.next_frame() {
        Some(HostMessage::Reply(Reply {
            call_id: CallId(1),
            outcome: Outcome::Err { error },
        })) => println!(
            "registered:unknown:{:?}:echoed={}",
            error.kind,
            error.message.contains("application.unknown")
        ),
        other => println!("registered:unknown:unexpected:{other:?}"),
    }
    std::io::Write::flush(&mut std::io::stdout()).ok();
    for call_id in [CallId(2), CallId(3)] {
        wire.send(&WorkerMessage::Call(Call {
            call_id,
            method: method.to_owned(),
            deadline_ms: None,
            stream: false,
            payload: serde_json::json!({ "call": call_id.0 }),
        }));
    }
    let mut completed = 0;
    loop {
        match wire.next_frame() {
            Some(HostMessage::Call(call)) => wire.send(&WorkerMessage::Reply(Reply {
                call_id: call.call_id,
                outcome: Outcome::Ok {
                    result: call.payload,
                },
            })),
            Some(HostMessage::Reply(Reply {
                call_id: CallId(2 | 3),
                outcome: Outcome::Ok { .. },
            })) => {
                completed += 1;
                if completed == 2 {
                    println!("registered:done");
                    std::io::Write::flush(&mut std::io::stdout()).ok();
                    return super::serve_after_handshake(wire);
                }
            }
            Some(HostMessage::Goodbye(_)) | None => return 0,
            Some(_) => {}
        }
    }
}

/// Probe the dispatcher's refusal surface: unknown method, forged
/// handle, malformed id, double release.
pub fn capability_hostile(wire: &mut Wire) -> i32 {
    if !wire.handshake() {
        return 70;
    }
    // An unknown method: refused, never echoed.
    wire.send(&WorkerMessage::Call(Call {
        call_id: CallId(1),
        method: "host.demand.secrets".to_owned(),
        deadline_ms: None,
        stream: false,
        payload: serde_json::json!(null),
    }));
    if let Some(HostMessage::Reply(Reply {
        call_id: CallId(1),
        outcome: Outcome::Err { error },
    })) = wire.next_frame()
    {
        println!(
            "hostile:method:{:?}:echoed={}",
            error.kind,
            error.message.contains("host.demand")
        );
    }
    std::io::Write::flush(&mut std::io::stdout()).ok();
    // A forged handle: the same bounded refusal an unknown id
    // gets.
    wire.send(&WorkerMessage::Call(Call {
        call_id: CallId(2),
        method: "capability.invoke".to_owned(),
        deadline_ms: None,
        stream: false,
        payload: serde_json::json!({ "handle": 4_000_000, "input": "x" }),
    }));
    if let Some(HostMessage::Reply(Reply {
        call_id: CallId(2),
        outcome: Outcome::Err { error },
    })) = wire.next_frame()
    {
        println!("hostile:forged:{:?}", error.kind);
    }
    std::io::Write::flush(&mut std::io::stdout()).ok();
    // A malformed capability id: refused before any broker work.
    wire.send(&WorkerMessage::Call(Call {
        call_id: CallId(3),
        method: "capability.acquire".to_owned(),
        deadline_ms: None,
        stream: false,
        payload: serde_json::json!({ "capability": "" }),
    }));
    if let Some(HostMessage::Reply(Reply {
        call_id: CallId(3),
        outcome: Outcome::Err { error },
    })) = wire.next_frame()
    {
        println!("hostile:malformed:{:?}", error.kind);
    }
    std::io::Write::flush(&mut std::io::stdout()).ok();
    // A double release: the first spends the id, the second is a
    // fault-shaped refusal.
    wire.send(&WorkerMessage::Call(Call {
        call_id: CallId(4),
        method: "capability.acquire".to_owned(),
        deadline_ms: None,
        stream: false,
        payload: serde_json::json!({ "capability": "test.text-upper/v1" }),
    }));
    let handle = match wire.next_frame() {
        Some(HostMessage::Reply(Reply {
            call_id: CallId(4),
            outcome: Outcome::Ok { result },
        })) => result["handle"].as_u64(),
        _ => None,
    };
    if let Some(handle) = handle {
        for call_id in [5u64, 6] {
            wire.send(&WorkerMessage::Call(Call {
                call_id: CallId(call_id),
                method: "capability.release".to_owned(),
                deadline_ms: None,
                stream: false,
                payload: serde_json::json!({ "handle": handle }),
            }));
        }
        // Replies arrive in completion order; collect both,
        // then report by call id.
        let mut verdicts = std::collections::BTreeMap::new();
        while verdicts.len() < 2 {
            match wire.next_frame() {
                Some(HostMessage::Reply(Reply {
                    call_id: CallId(id),
                    outcome,
                })) => {
                    let verdict = match outcome {
                        Outcome::Ok { .. } => "Ok",
                        Outcome::Err { .. } => "Err",
                        _ => "Other",
                    };
                    verdicts.insert(id, verdict);
                }
                Some(HostMessage::Goodbye(_)) | None => return 0,
                Some(_) => {}
            }
        }
        for (id, verdict) in &verdicts {
            println!("hostile:release{id}:{verdict}");
        }
    }
    std::io::Write::flush(&mut std::io::stdout()).ok();
    super::serve_after_handshake(wire)
}

/// Open a stream on the host's first stream call and send `count`
/// lossless items within the credit window.
pub fn stream_items(wire: &mut Wire, count: u64) -> i32 {
    if !wire.handshake() {
        return 70;
    }
    // Wait for the host's stream call, ack it with a credit
    // window, then send the items.
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
    // A conformant producer spends its announced credit and waits
    // for the host's Credit frames before sending past it.
    let mut credit = 4u32;
    for seq in 0..count {
        while credit == 0 {
            match wire.next_frame() {
                Some(HostMessage::Credit(credit_frame)) if credit_frame.call_id == call_id => {
                    credit += credit_frame.additional;
                }
                // A stream cancel means the consumer stopped listening:
                // stop producing and answer at once.
                Some(HostMessage::Cancel(cancel))
                    if cancel.call_id == call_id && cancel.target == CancelTarget::Stream =>
                {
                    wire.send(&WorkerMessage::Reply(Reply {
                        call_id,
                        outcome: Outcome::Cancelled {
                            reason: CancelReason::Requested,
                        },
                    }));
                    return 0;
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
    super::serve_conformant(wire)
}

/// Fire `count` concurrent invokes at the dispatcher without reading
/// replies, reporting each as it lands.
pub fn capability_flood(wire: &mut Wire, count: u64) -> i32 {
    if !wire.handshake() {
        return 70;
    }
    wire.send(&WorkerMessage::Call(Call {
        call_id: CallId(1),
        method: "capability.acquire".to_owned(),
        deadline_ms: None,
        stream: false,
        payload: serde_json::json!({ "capability": "test.text-upper/v1" }),
    }));
    let handle = match wire.next_frame() {
        Some(HostMessage::Reply(Reply {
            call_id: CallId(1),
            outcome: Outcome::Ok { result },
        })) => result["handle"].as_u64(),
        _ => None,
    };
    let Some(handle) = handle else {
        println!("flood:no-handle");
        std::io::Write::flush(&mut std::io::stdout()).ok();
        return 70;
    };
    // Fire every invoke before reading any reply: the dispatcher
    // must bound what it cannot start.
    for call_id in 2..=1 + count {
        wire.send(&WorkerMessage::Call(Call {
            call_id: CallId(call_id),
            method: "capability.invoke".to_owned(),
            deadline_ms: None,
            stream: false,
            payload: serde_json::json!({ "handle": handle, "input": "x" }),
        }));
    }
    // Replies arrive in completion order, not send order: each
    // is reported the moment it lands, named by its call id.
    let mut seen = 0usize;
    while seen < count as usize {
        match wire.next_frame() {
            Some(HostMessage::Reply(Reply {
                call_id: CallId(id),
                outcome,
            })) => {
                seen += 1;
                match outcome {
                    Outcome::Ok { .. } => println!("flood:{id}:ok"),
                    Outcome::Err { error } => {
                        println!("flood:{id}:{:?}:retryable={}", error.kind, error.retryable)
                    }
                    other => println!("flood:{id}:{other:?}"),
                }
                std::io::Write::flush(&mut std::io::stdout()).ok();
            }
            Some(HostMessage::Goodbye(_)) | None => return 0,
            Some(_) => {}
        }
    }
    println!("flood:done");
    std::io::Write::flush(&mut std::io::stdout()).ok();
    super::serve_conformant(wire)
}

/// Open the host's stream, spend the announced initial credit on lossless
/// items, then stop and report every Credit frame the host grants. The
/// overgrant detector: a correct host grants nothing until this worker's
/// consumer drains, and exactly what it drains after.
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
    let mut credit = initial_credit;
    for seq in 0..initial_credit {
        while credit == 0 {
            match wire.next_frame() {
                Some(HostMessage::Cancel(cancel))
                    if cancel.call_id == call_id && cancel.target == CancelTarget::Stream =>
                {
                    return answer_cancelled(wire, call_id);
                }
                Some(HostMessage::Credit(f)) if f.call_id == call_id => credit += f.additional,
                Some(HostMessage::Goodbye(_)) | None => return 0,
                Some(_) => {}
            }
        }
        credit -= 1;
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
    std::io::Write::flush(&mut std::io::stdout()).ok();
    loop {
        match wire.next_frame() {
            Some(HostMessage::Credit(frame)) if frame.call_id == call_id => {
                println!("stall:credit:{}", frame.additional);
                std::io::Write::flush(&mut std::io::stdout()).ok();
            }
            Some(HostMessage::Cancel(cancel))
                if cancel.call_id == call_id && cancel.target == CancelTarget::Stream =>
            {
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

/// Open the host's stream, flood `count` lossy items past any window,
/// then send one final credited lossless item before the terminal. The
/// reservation detector: a host that lets lossy frames eat capacity
/// reserved for outstanding credit drops the final lossless frame.
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
    // Exactly one terminal still ends the call.
    wire.send(&WorkerMessage::Reply(Reply {
        call_id,
        outcome: Outcome::Ok {
            result: serde_json::json!({ "streamed": count }),
        },
    }));
    println!("flood:sent:{count}");
    std::io::Write::flush(&mut std::io::stdout()).ok();
    // Stay connected without speaking again — a second handshake would
    // fault the session and drown the evidence this script exists for.
    // A stream cancel is answered, though: a muted consumer ends the
    // stream half and the call still owes its terminal.
    loop {
        match wire.next_frame() {
            Some(HostMessage::Cancel(cancel))
                if cancel.call_id == call_id && cancel.target == CancelTarget::Stream =>
            {
                return answer_cancelled(wire, call_id);
            }
            Some(HostMessage::Goodbye(_)) | None => return 0,
            Some(_) => {}
        }
    }
}
