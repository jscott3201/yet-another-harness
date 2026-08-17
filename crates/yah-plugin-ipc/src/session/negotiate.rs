//! Negotiation: the first frame decides everything.
//!
//! LSP's pre-initialize rule, adapted: any frame other than a hello before
//! negotiation completes is refused and the connection ends. No later frame
//! carries a version — this lane is one activation-scoped process with a
//! lifetime, so per-frame versioning would be pure overhead (the shape
//! MCP's 2026 revision adopted exists to keep horizontally scaled stateless
//! HTTP servers, a constraint this lane does not have).

use super::{HostSession, Phase, SessionEvent, wire_limits};
use crate::PROTOCOL_VERSION;
use crate::types::*;

impl HostSession {
    pub(super) fn on_pre_negotiation(&mut self, message: WorkerMessage) {
        let hello = match message {
            WorkerMessage::Hello(hello) => hello,
            _ => {
                // Refuse, name the rule, close. The refuse frame is the
                // one diagnostic a misordered worker gets.
                self.outbox.push(HostMessage::Refuse(Refuse {
                    error: refusal(
                        WireErrorKind::NegotiationRequired,
                        "the first frame must be a hello",
                    ),
                    supported_versions: vec![PROTOCOL_VERSION],
                }));
                self.phase = Phase::Closed;
                self.events.push(SessionEvent::Fatal {
                    kind: WireErrorKind::NegotiationRequired,
                    detail: "first frame was not a hello".to_owned(),
                });
                return;
            }
        };
        if !hello.protocol_versions.contains(&PROTOCOL_VERSION) {
            // The refusal lists what the host does speak, so the worker
            // fails with an actionable diagnostic instead of a bare close.
            self.outbox.push(HostMessage::Refuse(Refuse {
                error: refusal(
                    WireErrorKind::UnsupportedVersion,
                    "no offered protocol version is supported",
                ),
                supported_versions: vec![PROTOCOL_VERSION],
            }));
            self.phase = Phase::Closed;
            self.events.push(SessionEvent::Fatal {
                kind: WireErrorKind::UnsupportedVersion,
                detail: format!("worker offered {:?}", hello.protocol_versions),
            });
            return;
        }
        let unknown: Vec<&String> = hello
            .required_features
            .iter()
            .filter(|feature| !self.config.features.contains(feature))
            .collect();
        if !unknown.is_empty() {
            // Fail closed on unknown required features: running without a
            // feature the worker cannot live without is not a degraded
            // mode, it is a different program.
            self.outbox.push(HostMessage::Refuse(Refuse {
                error: refusal(
                    WireErrorKind::UnknownRequiredFeature,
                    "a required feature is not offered by this host",
                ),
                supported_versions: vec![PROTOCOL_VERSION],
            }));
            self.phase = Phase::Closed;
            self.events.push(SessionEvent::Fatal {
                kind: WireErrorKind::UnknownRequiredFeature,
                detail: format!("unknown required features: {unknown:?}"),
            });
            return;
        }
        // The feature set in force is the intersection; the ceilings are
        // announced, not negotiated — a worker that cannot live with one
        // says goodbye.
        let features: Vec<String> = self
            .config
            .features
            .iter()
            .filter(|feature| hello.features.contains(feature))
            .cloned()
            .collect();
        self.outbox.push(HostMessage::Accept(Accept {
            protocol_version: PROTOCOL_VERSION,
            features,
            limits: wire_limits(),
            ceilings: self.config.ceilings.clone(),
        }));
        self.phase = Phase::Active;
        self.events.push(SessionEvent::Negotiated {
            sdk_name: hello.sdk_name,
            sdk_version: hello.sdk_version,
        });
    }
}

fn refusal(kind: WireErrorKind, message: &str) -> WireError {
    WireError {
        kind,
        message: message.to_owned(),
        retryable: false,
        reconcile_required: false,
    }
}
