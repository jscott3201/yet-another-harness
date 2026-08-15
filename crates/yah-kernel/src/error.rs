//! Error vocabulary.
//!
//! Two taxonomies live here and they do not mix: [`ErrorKind`] is the closed
//! ADR-002 §10 wire enum (adding a member is protocol-major), and
//! [`ProviderErrorKind`] is the R5.3 typed classification a normalized model
//! call surfaces instead of a raw provider payload. Kernel-internal
//! rejections that never reach the wire under their own name — ADR-001's
//! `scope_rejected`, `journal_immutable` before projection — carry their
//! ADR-001 name in kernel types and project onto this enum only at the
//! adapter (R19: `scope_rejected` travels as `policy_denied` with the
//! offending paths in `details`).

use serde::{Deserialize, Serialize};

/// ADR-002 §10 error taxonomy. Clients branch only on this; `reason` and
/// `details` are diagnostic. Closed: an unknown member is a decode failure.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorKind {
    InvalidRequest,
    Unauthenticated,
    Unauthorized,
    NotFound,
    VersionConflict,
    IdempotencyConflict,
    FenceRejected,
    JournalImmutable,
    PolicyDenied,
    CapabilityUnsupported,
    ApprovalRequired,
    ResourceExhausted,
    PayloadTooLarge,
    CursorExpired,
    SlowConsumer,
    Cancelled,
    DeadlineExceeded,
    Unavailable,
    OutcomeUnknown,
    Internal,
}

impl ErrorKind {
    /// P10 table column: whether a client may retry on this kind alone.
    pub fn retryable(self) -> bool {
        matches!(
            self,
            ErrorKind::VersionConflict
                | ErrorKind::ApprovalRequired
                | ErrorKind::ResourceExhausted
                | ErrorKind::DeadlineExceeded
                | ErrorKind::Unavailable
        )
    }

    /// P10 table column: `outcome_unknown` is the only kind demanding
    /// reconciliation before any retry.
    pub fn reconcile_required(self) -> bool {
        self == ErrorKind::OutcomeUnknown
    }
}

/// Typed provider failure classes the R5.3 fixture catalog must cover, one
/// fixture per member. `UnsupportedCapability` is the R5.2 capability-matrix
/// failure path.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderErrorKind {
    RateLimit,
    InvalidRequest,
    ContextLengthExceeded,
    ServerError,
    UnsupportedCapability,
}

impl ProviderErrorKind {
    /// Transient classes are retry candidates up to the configured ceiling
    /// (MILE-001 `retry_ceiling` row, default 3 per R25c); the rest are
    /// terminal on first sight.
    pub fn transient(self) -> bool {
        matches!(
            self,
            ProviderErrorKind::RateLimit | ProviderErrorKind::ServerError
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wire_names_are_snake_case() {
        assert_eq!(
            serde_json::to_string(&ErrorKind::FenceRejected).unwrap(),
            "\"fence_rejected\""
        );
        assert_eq!(
            serde_json::to_string(&ProviderErrorKind::ContextLengthExceeded).unwrap(),
            "\"context_length_exceeded\""
        );
    }

    #[test]
    fn unknown_member_is_a_decode_failure() {
        assert!(serde_json::from_str::<ErrorKind>("\"weird_new_kind\"").is_err());
    }
}
