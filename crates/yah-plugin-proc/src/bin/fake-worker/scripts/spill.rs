//! Spill-and-release scripts: the worker offers an artifact, then either
//! serves it honestly or lies per the poison modes, and answers the
//! host's Release frame in the way each release script's evidence needs.
//!
//! `spill_then` is the shared skeleton: offer on the first call, then
//! hand the wire to one release behaviour.

use super::*;
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

/// Answer the first call with a spilled offer held worker-side, then run
/// one release script's behaviour on the host's Release frame.
fn spill_then<F>(wire: &mut Wire, on_release: F) -> i32
where
    F: FnOnce(&mut Wire, Release) -> i32,
{
    if !wire.handshake() {
        return 70;
    }
    let payload: Vec<u8> = (0..4096).map(|i| (i % 251) as u8).collect();
    let digest = blake3::hash(&payload).to_hex().to_string();
    loop {
        match wire.next_frame() {
            Some(HostMessage::Call(call)) => {
                wire.send(&WorkerMessage::Reply(Reply {
                    call_id: call.call_id,
                    outcome: Outcome::Spilled {
                        artifact: ArtifactOffer {
                            handle: HandleId(7),
                            bytes: payload.len() as u64,
                            media_type: "application/octet-stream".to_owned(),
                            digest_blake3: digest,
                        },
                    },
                }));
                break;
            }
            Some(HostMessage::Goodbye(_)) | None => return 0,
            Some(_) => {}
        }
    }
    loop {
        match wire.next_frame() {
            Some(HostMessage::Release(release)) => return on_release(wire, release),
            Some(HostMessage::Goodbye(_)) | None => return 0,
            Some(_) => {}
        }
    }
}

/// Receive the release and deliberately withhold the acknowledgement:
/// the worker that leaves the host's release pending indefinitely.
pub fn release_withhold(wire: &mut Wire) -> i32 {
    spill_then(wire, |wire, release| {
        println!("release:withheld:{release:?}");
        std::io::Write::flush(&mut std::io::stdout()).ok();
        loop {
            match wire.next_frame() {
                Some(HostMessage::Goodbye(_)) | None => return 0,
                Some(_) => {}
            }
        }
    })
}

/// Receive the release, hold the acknowledgement for a beat, then ack:
/// the pending-then-resolves path.
pub fn release_later(wire: &mut Wire, delay_ms: u64) -> i32 {
    spill_then(wire, |wire, release| {
        std::thread::sleep(std::time::Duration::from_millis(delay_ms));
        wire.send(&WorkerMessage::ReleaseAck(ReleaseAck {
            handle: release.handle,
            kind: release.kind,
        }));
        println!("release:acked");
        std::io::Write::flush(&mut std::io::stdout()).ok();
        loop {
            match wire.next_frame() {
                Some(HostMessage::Goodbye(_)) | None => return 0,
                Some(_) => {}
            }
        }
    })
}

/// Receive the release and exit without acking: the disconnect that
/// lands between the host's release and its acknowledgement.
pub fn release_die(wire: &mut Wire) -> i32 {
    spill_then(wire, |_wire, release| {
        println!("release:got:{release:?}");
        std::io::Write::flush(&mut std::io::stdout()).ok();
        0
    })
}

/// Receive the release and answer a goodbye instead of the ack: the
/// orderly end that must not masquerade as acknowledged success.
pub fn release_goodbye(wire: &mut Wire) -> i32 {
    spill_then(wire, |wire, release| {
        println!("release:got:{release:?}");
        std::io::Write::flush(&mut std::io::stdout()).ok();
        wire.send(&WorkerMessage::Goodbye(Goodbye {
            reason: "leaving before the ack".to_owned(),
        }));
        0
    })
}

/// Receive the release and ack with the wrong kind: the desync the
/// session must treat as fatal, not as success.
pub fn release_ack_wrong_kind(wire: &mut Wire) -> i32 {
    spill_then(wire, |wire, release| {
        wire.send(&WorkerMessage::ReleaseAck(ReleaseAck {
            handle: release.handle,
            kind: HandleKind::Capability,
        }));
        println!("release:acked-wrong-kind");
        std::io::Write::flush(&mut std::io::stdout()).ok();
        loop {
            match wire.next_frame() {
                Some(HostMessage::Goodbye(_)) | None => return 0,
                Some(_) => {}
            }
        }
    })
}

/// Send an unsolicited release-ack for a handle the host never asked
/// about: the wrong-handle desync, fatal by protocol law.
pub fn release_bogus_ack(wire: &mut Wire) -> i32 {
    if !wire.handshake() {
        return 70;
    }
    wire.send(&WorkerMessage::ReleaseAck(ReleaseAck {
        handle: HandleId(999),
        kind: HandleKind::Artifact,
    }));
    println!("release:bogus-ack-sent");
    std::io::Write::flush(&mut std::io::stdout()).ok();
    loop {
        match wire.next_frame() {
            Some(HostMessage::Goodbye(_)) | None => return 0,
            Some(_) => {}
        }
    }
}

/// Answer the first call with a spilled offer whose metadata or chunks
/// violate the reader's contract, per mode: `short` and `long` reply
/// with the wrong chunk length, `media` contradicts the offered media
/// type, `upper` and `junk` serve noncanonical hex, `digest` offers the
/// wrong digest, and `empty-digest` offers the digest of zero bytes —
/// the claim only a completion-gated verifier can survive.
pub fn spill_poison(wire: &mut Wire, mode: &str) -> i32 {
    if !wire.handshake() {
        return 70;
    }
    let payload: Vec<u8> = (0..4096).map(|i| (i % 251) as u8).collect();
    let digest = match mode {
        "digest" => blake3::hash(b"wrong").to_hex().to_string(),
        "empty-digest" => blake3::hash(&[]).to_hex().to_string(),
        _ => blake3::hash(&payload).to_hex().to_string(),
    };
    loop {
        match wire.next_frame() {
            Some(HostMessage::Call(call)) => {
                wire.send(&WorkerMessage::Reply(Reply {
                    call_id: call.call_id,
                    outcome: Outcome::Spilled {
                        artifact: ArtifactOffer {
                            handle: HandleId(7),
                            bytes: payload.len() as u64,
                            media_type: "application/octet-stream".to_owned(),
                            digest_blake3: digest,
                        },
                    },
                }));
                break;
            }
            Some(HostMessage::Goodbye(_)) | None => return 0,
            Some(_) => {}
        }
    }
    let hex_of = |slice: &[u8]| -> String {
        let mut out = String::new();
        for byte in slice {
            if mode == "upper" {
                out.push_str(&format!("{byte:02X}"));
            } else if mode == "junk" && out.is_empty() {
                out.push_str("0x");
            } else {
                out.push_str(&format!("{byte:02x}"));
            }
        }
        out
    };
    loop {
        match wire.next_frame() {
            Some(HostMessage::Call(call)) => {
                let read: Result<serde_json::Value, _> =
                    serde_json::from_value(call.payload.clone());
                let served = read.ok().and_then(|value| {
                    let offset = value["offset"].as_u64()? as usize;
                    let len = value["len"].as_u64()? as usize;
                    let end = offset + len;
                    let slice = payload.get(offset..end)?;
                    let owned = match mode {
                        // One byte short.
                        "short" => slice[..slice.len().saturating_sub(1).max(1)].to_vec(),
                        // One byte past the request.
                        "long" => {
                            let mut grown = slice.to_vec();
                            grown.push(payload[0]);
                            grown
                        }
                        _ => slice.to_vec(),
                    };
                    Some(hex_of(&owned))
                });
                let media = if mode == "media" {
                    "text/plain"
                } else {
                    "application/octet-stream"
                };
                match served {
                    Some(hex) => wire.send(&WorkerMessage::Reply(Reply {
                        call_id: call.call_id,
                        outcome: Outcome::Ok {
                            result: serde_json::json!({
                                "bytes_hex": hex,
                                "media_type": media,
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
                };
            }
            Some(HostMessage::Release(release)) => {
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
