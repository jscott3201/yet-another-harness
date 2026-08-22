use std::io::Write;

use yah_plugin_ipc::types::*;
use yah_plugin_proc::{TEXT_CAPABILITY_ACQUIRE_METHOD, TEXT_CAPABILITY_INVOKE_METHOD};

use super::super::Wire;

pub const CAPABILITY_ID: &str = "yah.test.text/v1";

pub fn basic(wire: &mut Wire) -> i32 {
    if !wire.handshake() {
        return 70;
    }
    let handle = acquire(wire, 1, CAPABILITY_ID).expect("basic acquire succeeds");
    println!("capability:acquire:ok:handle={}", handle.0);
    report("invoke", reply(wire, invoke(2, handle, "hello")));
    release(wire, handle);
    println!("capability:release:ack");
    report("released", reply(wire, invoke(3, handle, "after-release")));
    flush();
    super::super::serve_after_handshake(wire)
}

pub fn acquire_probe(wire: &mut Wire) -> i32 {
    if !wire.handshake() {
        return 70;
    }
    report("invalid", reply(wire, acquire_call(1, "not a capability")));
    report("target", reply(wire, acquire_call(2, CAPABILITY_ID)));
    flush();
    super::super::serve_after_handshake(wire)
}

pub fn replacement(wire: &mut Wire) -> i32 {
    if !wire.handshake() {
        return 70;
    }
    let handle = acquire(wire, 1, CAPABILITY_ID).expect("replacement acquire succeeds");
    println!("capability:replacement:ready");
    flush();
    loop {
        match wire.next_frame() {
            Some(HostMessage::Call(call)) if call.method == "control.continue" => {
                wire.send(&WorkerMessage::Reply(Reply {
                    call_id: call.call_id,
                    outcome: Outcome::Ok {
                        result: serde_json::json!({ "continued": true }),
                    },
                }));
                break;
            }
            Some(HostMessage::Goodbye(_)) | None => return 0,
            Some(_) => {}
        }
    }
    report("held", reply(wire, invoke(2, handle, "held")));
    report("fresh", reply(wire, acquire_call(3, CAPABILITY_ID)));
    flush();
    super::super::serve_after_handshake(wire)
}

pub fn provider_outcomes(wire: &mut Wire) -> i32 {
    if !wire.handshake() {
        return 70;
    }
    let handle = acquire(wire, 1, CAPABILITY_ID).expect("provider acquire succeeds");
    for (id, input) in [
        (2, "bad"),
        (3, "fail"),
        (4, "oversize"),
        (5, "panic"),
        (6, "ok"),
    ] {
        report(input, reply(wire, invoke(id, handle, input)));
    }
    release(wire, handle);
    println!("capability:provider-outcomes:done");
    flush();
    super::super::serve_after_handshake(wire)
}

pub fn malformed_and_unknown(wire: &mut Wire) -> i32 {
    if !wire.handshake() {
        return 70;
    }
    wire.send(&call(
        1,
        TEXT_CAPABILITY_ACQUIRE_METHOD,
        serde_json::json!({ "capability": CAPABILITY_ID, "extra": true }),
    ));
    report("malformed-acquire", reply_for(wire, CallId(1)));
    wire.send(&call(
        2,
        TEXT_CAPABILITY_INVOKE_METHOD,
        serde_json::json!({ "handle": 1, "input": "x", "extra": true }),
    ));
    report("malformed-invoke", reply_for(wire, CallId(2)));
    report(
        "forged",
        reply(wire, invoke(3, HandleId(999_999), "forged")),
    );
    let handle = acquire(wire, 4, CAPABILITY_ID).expect("valid acquire survives malformed calls");
    release(wire, handle);
    report("released", reply(wire, invoke(5, handle, "released")));
    println!("capability:malformed:done");
    flush();
    super::super::serve_after_handshake(wire)
}

pub fn handle_limit(wire: &mut Wire) -> i32 {
    if !wire.handshake() {
        return 70;
    }
    let mut handles = Vec::new();
    for index in 0..yah_plugin_ipc::DEFAULT_LIVE_HANDLES {
        handles.push(
            acquire(wire, u64::from(index) + 1, CAPABILITY_ID)
                .expect("each handle through the ceiling succeeds"),
        );
    }
    report(
        "at-limit",
        reply(
            wire,
            acquire_call(
                u64::from(yah_plugin_ipc::DEFAULT_LIVE_HANDLES) + 1,
                CAPABILITY_ID,
            ),
        ),
    );
    let released = handles.remove(0);
    release(wire, released);
    let replacement = acquire(
        wire,
        u64::from(yah_plugin_ipc::DEFAULT_LIVE_HANDLES) + 2,
        CAPABILITY_ID,
    )
    .expect("release makes one slot available");
    println!(
        "capability:limit:replacement={} monotonic={}",
        replacement.0,
        replacement.0 > released.0
    );
    handles.push(replacement);
    for handle in handles {
        release(wire, handle);
    }
    println!("capability:limit:done");
    flush();
    super::super::serve_after_handshake(wire)
}

pub fn release_order(wire: &mut Wire) -> i32 {
    if !wire.handshake() {
        return 70;
    }
    let handle = acquire(wire, 1, CAPABILITY_ID).expect("ordered acquire succeeds");
    wire.send(&invoke(2, handle, "blocked"));
    wire.send(&WorkerMessage::Release(Release {
        handle,
        kind: HandleKind::Capability,
    }));
    let mut completed = false;
    let mut acked = false;
    while !completed || !acked {
        match wire.next_frame() {
            Some(HostMessage::ReleaseAck(ack)) if ack.handle == handle => {
                acked = true;
                println!("capability:ordered:release-ack");
                flush();
            }
            Some(HostMessage::Reply(reply)) if reply.call_id == CallId(2) => {
                completed = true;
                report("ordered-before", reply.outcome);
            }
            Some(HostMessage::Goodbye(_)) | None => return 0,
            Some(_) => {}
        }
    }
    report("ordered-after", reply(wire, invoke(3, handle, "after")));
    flush();
    super::super::serve_after_handshake(wire)
}

pub fn cancel_queued(wire: &mut Wire) -> i32 {
    if !wire.handshake() {
        return 70;
    }
    let handle = acquire(wire, 1, CAPABILITY_ID).expect("queued acquire succeeds");
    wire.send(&call(2, "application.hold", serde_json::json!(null)));
    wire.send(&invoke(3, handle, "must-not-run"));
    wire.send(&acquire_call(4, CAPABILITY_ID));
    for call_id in [CallId(3), CallId(4)] {
        wire.send(&WorkerMessage::Cancel(Cancel {
            call_id,
            target: CancelTarget::Call,
        }));
    }
    let mut cancelled = 0;
    let mut held_done = false;
    while cancelled < 2 || !held_done {
        match wire.next_frame() {
            Some(HostMessage::Reply(Reply {
                call_id: CallId(3 | 4),
                outcome: Outcome::Cancelled { .. },
            })) => {
                cancelled += 1;
                println!("capability:queued:cancelled={cancelled}");
                flush();
            }
            Some(HostMessage::Reply(Reply {
                call_id: CallId(2), ..
            })) => held_done = true,
            Some(HostMessage::Goodbye(_)) | None => return 0,
            Some(_) => {}
        }
    }
    release(wire, handle);
    println!("capability:queued:done");
    flush();
    super::super::serve_after_handshake(wire)
}

pub fn cancel_during(wire: &mut Wire) -> i32 {
    if !wire.handshake() {
        return 70;
    }
    let handle = acquire(wire, 1, CAPABILITY_ID).expect("during acquire succeeds");
    wire.send(&invoke(2, handle, "blocked"));
    loop {
        match wire.next_frame() {
            Some(HostMessage::Call(call)) if call.method == "control.cancel" => {
                wire.send(&WorkerMessage::Cancel(Cancel {
                    call_id: CallId(2),
                    target: CancelTarget::Call,
                }));
                wire.send(&WorkerMessage::Reply(Reply {
                    call_id: call.call_id,
                    outcome: Outcome::Ok {
                        result: serde_json::json!({ "cancelled": true }),
                    },
                }));
            }
            Some(HostMessage::Reply(Reply {
                call_id: CallId(2),
                outcome: Outcome::Cancelled { .. },
            })) => {
                wire.send(&acquire_call(3, CAPABILITY_ID));
                wire.send(&invoke(4, handle, "stale-held"));
                println!("capability:during:cancelled");
                println!("capability:during:stale-queued");
                flush();
                return super::super::serve_after_handshake(wire);
            }
            Some(HostMessage::Goodbye(_)) | None => return 0,
            Some(_) => {}
        }
    }
}

pub fn reclaim(wire: &mut Wire, ending: &str) -> i32 {
    if !wire.handshake() {
        return 70;
    }
    let handle = acquire(wire, 1, CAPABILITY_ID).expect("reclaim acquire succeeds");
    println!("capability:reclaim:acquired={}", handle.0);
    flush();
    match ending {
        "goodbye" => {
            wire.send(&WorkerMessage::Goodbye(Goodbye {
                reason: "fixture goodbye".to_owned(),
            }));
            0
        }
        "disconnect" => 70,
        "fatal" => {
            wire.send(&WorkerMessage::Release(Release {
                handle,
                kind: HandleKind::Artifact,
            }));
            while wire.next_frame().is_some() {}
            0
        }
        _ => super::super::serve_after_handshake(wire),
    }
}

fn call(id: u64, method: &str, payload: serde_json::Value) -> WorkerMessage {
    WorkerMessage::Call(Call {
        call_id: CallId(id),
        method: method.to_owned(),
        deadline_ms: None,
        stream: false,
        payload,
    })
}

fn acquire_call(id: u64, capability: &str) -> WorkerMessage {
    call(
        id,
        TEXT_CAPABILITY_ACQUIRE_METHOD,
        serde_json::json!({ "capability": capability }),
    )
}

fn invoke(id: u64, handle: HandleId, input: &str) -> WorkerMessage {
    call(
        id,
        TEXT_CAPABILITY_INVOKE_METHOD,
        serde_json::json!({ "handle": handle, "input": input }),
    )
}

fn acquire(wire: &mut Wire, id: u64, capability: &str) -> Option<HandleId> {
    match reply(wire, acquire_call(id, capability)) {
        Outcome::Ok { result } => result
            .pointer("/ok/handle")
            .and_then(serde_json::Value::as_u64)
            .map(HandleId),
        _ => None,
    }
}

fn reply(wire: &mut Wire, message: WorkerMessage) -> Outcome {
    let call_id = match &message {
        WorkerMessage::Call(call) => call.call_id,
        _ => unreachable!("reply helper sends calls only"),
    };
    wire.send(&message);
    reply_for(wire, call_id)
}

fn reply_for(wire: &mut Wire, call_id: CallId) -> Outcome {
    loop {
        match wire.next_frame() {
            Some(HostMessage::Reply(reply)) if reply.call_id == call_id => return reply.outcome,
            Some(HostMessage::Call(call)) => wire.send(&WorkerMessage::Reply(Reply {
                call_id: call.call_id,
                outcome: Outcome::Ok {
                    result: call.payload,
                },
            })),
            Some(HostMessage::Goodbye(_)) | None => std::process::exit(0),
            Some(_) => {}
        }
    }
}

fn release(wire: &mut Wire, handle: HandleId) {
    wire.send(&WorkerMessage::Release(Release {
        handle,
        kind: HandleKind::Capability,
    }));
    loop {
        match wire.next_frame() {
            Some(HostMessage::ReleaseAck(ack))
                if ack.handle == handle && ack.kind == HandleKind::Capability =>
            {
                return;
            }
            Some(HostMessage::Goodbye(_)) | None => std::process::exit(0),
            Some(_) => {}
        }
    }
}

fn report(label: &str, outcome: Outcome) {
    match outcome {
        Outcome::Ok { result } => {
            if let Some(code) = result
                .pointer("/error/code")
                .and_then(|value| value.as_str())
            {
                let chars = result
                    .pointer("/error/message")
                    .and_then(|value| value.as_str())
                    .map_or(0, |message| message.chars().count());
                println!("capability:{label}:domain:{code}:chars={chars}");
            } else if let Some(output) = result
                .pointer("/ok/output")
                .and_then(|value| value.as_str())
            {
                println!("capability:{label}:ok:{output}");
            } else if let Some(handle) = result
                .pointer("/ok/handle")
                .and_then(|value| value.as_u64())
            {
                println!("capability:{label}:ok:handle={handle}");
            } else {
                println!("capability:{label}:unexpected-ok:{result}");
            }
        }
        Outcome::Err { error } => println!(
            "capability:{label}:wire:{:?}:echoed={}",
            error.kind,
            error.message.contains(label)
        ),
        Outcome::Cancelled { .. } => println!("capability:{label}:cancelled"),
        Outcome::Spilled { .. } => println!("capability:{label}:unexpected-spill"),
    }
    flush();
}

fn flush() {
    let _ = std::io::stdout().flush();
}
