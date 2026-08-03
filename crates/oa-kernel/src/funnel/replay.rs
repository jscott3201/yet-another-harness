//! Receipt replay resolution and the in-process token projection
//! (split from the funnel core to honor the per-file LOC cap).

use super::*;

impl Funnel {
    pub(super) fn replay_stored(
        cmd: &Command,
        stored: Option<(Option<String>, Option<String>, Option<String>)>,
    ) -> Submission {
        let Some((Some(digest), Some(status), Some(result))) = stored else {
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
        let result: serde_json::Value =
            serde_json::from_str(&result).unwrap_or(serde_json::Value::Null);
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
    })
}
