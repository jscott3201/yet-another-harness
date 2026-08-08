//! Receipt replay resolution and the in-process token projection
//! (split from the funnel core to honor the per-file LOC cap).

use super::*;

type StoredReceipt = (
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
);

impl Funnel {
    pub(super) fn replay_stored(cmd: &Command, stored: Option<StoredReceipt>) -> Submission {
        let Some((
            Some(digest),
            Some(principal_kind),
            Some(principal_id),
            Some(status),
            Some(result),
        )) = stored
        else {
            return Submission::Rejected {
                kind: ErrorKind::Internal,
                detail: "receipt row unreadable".into(),
                replayed: false,
            };
        };
        if digest != cmd.request_digest.as_str() {
            return Submission::Rejected {
                kind: ErrorKind::IdempotencyConflict,
                detail: "same command_id, different request digest".into(),
                replayed: false,
            };
        }
        if principal_kind != cmd.principal_kind.wire() || principal_id != cmd.principal_id {
            return Submission::Rejected {
                kind: ErrorKind::Unauthorized,
                detail: "receipt belongs to a different principal".into(),
                replayed: false,
            };
        }
        let result: serde_json::Value = match serde_json::from_str(&result) {
            Ok(result) => result,
            Err(error) => {
                return Submission::Rejected {
                    kind: ErrorKind::Internal,
                    detail: format!("receipt result is invalid JSON: {error}"),
                    replayed: false,
                };
            }
        };
        if status == "completed" {
            Submission::Replayed { result }
        } else {
            let kind = result
                .get("error_kind")
                .cloned()
                .and_then(|v| serde_json::from_value(v).ok())
                .unwrap_or(ErrorKind::Internal);
            let detail = result
                .get("detail")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_owned();
            Submission::Rejected {
                kind,
                detail,
                replayed: true,
            }
        }
    }
}

/// Rebuild token claims from a dispatch or reissue result — the in-process
/// stand-in for the daemon's sealed token (MILE-001 carries claims unsealed;
/// the MAC boundary is INSTALL-001's).
pub fn token_from_result(result: &serde_json::Value) -> Option<AttemptTokenClaims> {
    Some(AttemptTokenClaims {
        unit_id: result.get("unit_id")?.as_str()?.to_owned(),
        attempt_epoch: AttemptEpoch(result.get("attempt_epoch")?.as_u64()?),
        stamp: Stamp(result.get("stamp")?.as_u64()?),
        authority_epoch: AuthorityEpoch(result.get("authority_epoch")?.as_u64()?),
        holder_id: result.get("holder_id")?.as_str()?.to_owned(),
        nonce: result.get("token_nonce")?.as_str()?.to_owned(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn malformed_stored_result_fails_closed() {
        let command = Command {
            command_id: "command-1".into(),
            scope_kind: ScopeKind::Project,
            scope_id: "project-1".into(),
            request_digest: Digest::of_bytes(b"request"),
            expected_version: None,
            principal_kind: PrincipalKind::Daemon,
            principal_id: "daemon-local".into(),
            authority_epoch: None,
            attempt_token: None,
            causation_id: None,
            correlation_id: None,
            method: Method::ProgressReport {
                unit_id: "unit-1".into(),
            },
        };
        let stored = Some((
            Some(command.request_digest.to_string()),
            Some("daemon".into()),
            Some("daemon-local".into()),
            Some("completed".into()),
            Some("{".into()),
        ));
        assert!(matches!(
            Funnel::replay_stored(&command, stored),
            Submission::Rejected {
                kind: ErrorKind::Internal,
                ..
            }
        ));
    }
}
