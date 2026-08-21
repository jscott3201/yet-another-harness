//! The application-facing scripts: capability cycles through the host's
//! dispatcher, hostile refusal probes, worker-to-host item streams, and
//! spilled-artifact pull-reads. Split from the transport-only scripts so
//! each stays reviewable against the surface it exercises.

use yah_plugin_ipc::types::*;

use super::Wire;

/// Acquire, invoke, and release one granted text capability through
/// the dispatcher, reporting each step on stdout.
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
    super::serve_conformant(wire)
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
    super::serve_conformant(wire)
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

/// Answer the first call with a spilled offer held worker-side, then
/// serve pull reads until goodbye.
pub fn spill(wire: &mut Wire, bytes: usize) -> i32 {
    if !wire.handshake() {
        return 70;
    }
    // The worker's own bytes, pattern-filled so corruption is
    // detectable beyond the digest alone.
    let payload: Vec<u8> = (0..bytes).map(|i| (i % 251) as u8).collect();
    let digest = blake3::hash(&payload).to_hex().to_string();
    // The first call gets the spilled offer; the worker keeps the
    // bytes and serves pull reads behind the handle.
    let handle = loop {
        match wire.next_frame() {
            Some(HostMessage::Call(call)) => {
                wire.send(&WorkerMessage::Reply(Reply {
                    call_id: call.call_id,
                    outcome: Outcome::Spilled {
                        artifact: ArtifactOffer {
                            handle: HandleId(7),
                            bytes: bytes as u64,
                            media_type: "application/octet-stream".to_owned(),
                            digest_blake3: digest.clone(),
                        },
                    },
                }));
                break HandleId(7);
            }
            Some(HostMessage::Goodbye(_)) | None => return 0,
            Some(_) => {}
        }
    };
    // Serve pull reads until the host says goodbye.
    loop {
        match wire.next_frame() {
            Some(HostMessage::Call(call)) => {
                let read: Result<serde_json::Value, _> =
                    serde_json::from_value(call.payload.clone());
                let chunk = read.ok().and_then(|value| {
                    let offset = value["offset"].as_u64()? as usize;
                    let len = value["len"].as_u64()? as usize;
                    payload
                        .get(offset..offset + len)
                        .map(|slice| slice.iter().map(|b| format!("{b:02x}")).collect::<String>())
                });
                match chunk {
                    Some(hex) => wire.send(&WorkerMessage::Reply(Reply {
                        call_id: call.call_id,
                        outcome: Outcome::Ok {
                            result: serde_json::json!({
                                "bytes_hex": hex,
                                "media_type": "application/octet-stream",
                            }),
                        },
                    })),
                    None => wire.send(&WorkerMessage::Reply(Reply {
                        call_id: call.call_id,
                        outcome: Outcome::Err {
                            error: WireError {
                                kind: WireErrorKind::InvalidRead,
                                message: "read outside the offered range".to_owned(),
                                retryable: false,
                                reconcile_required: false,
                            },
                        },
                    })),
                }
            }
            Some(HostMessage::Release(release)) if release.handle == handle => {
                // Acknowledge the explicit release the host owes a
                // spilled handle.
                wire.send(&WorkerMessage::ReleaseAck(ReleaseAck {
                    handle: release.handle,
                    kind: release.kind,
                }));
            }
            Some(HostMessage::Goodbye(_)) | None => return 0,
            Some(_) => {}
        }
    }
}
