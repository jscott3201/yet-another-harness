use std::{error::Error, fmt};

/// A host-implemented capability a guest can consume as opaque text.
///
/// This is the portable subset of a capability contract: one synchronous call,
/// text in, text out, no streams, handles, or callbacks. Runtime adapters -
/// Wasm now, process workers later - carry exactly this shape across their
/// ABIs, so a provider written against it is reachable from every guest lane
/// without depending on any of them.
///
/// Contract requirements, inherited from [`CapabilityHandle::try_with`] and
/// load-bearing for the Wasm lane in particular:
///
/// - `invoke` must not block indefinitely. It runs inside a guest call whose
///   epoch deadline can only interrupt *guest* code; time spent here extends
///   the guest's deactivation bound by exactly that much.
/// - `invoke` must not call back into the driver or wait for closure of its
///   own activation. The store lock is held for the duration of the guest
///   call, so re-entering the driver deadlocks on it.
/// - `invoke` must not return or leak raw authority that bypasses the handle.
///
/// [`CapabilityHandle::try_with`]: crate::CapabilityHandle::try_with
pub trait TextCapability: Send + Sync + 'static {
    /// Run one call against this capability.
    ///
    /// # Errors
    ///
    /// Returns [`TextCapabilityFailure`] when the provider refuses the input
    /// or cannot complete the call. Refusal is an ordinary return, not a
    /// revocation: the grant stays live and the caller may try again.
    fn invoke(&self, input: &str) -> Result<String, TextCapabilityFailure>;
}

/// Why a [`TextCapability`] provider refused or failed one call.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TextCapabilityFailure {
    pub code: TextCapabilityFailureCode,
    pub message: String,
}

impl TextCapabilityFailure {
    pub fn invalid_input(message: impl Into<String>) -> Self {
        Self {
            code: TextCapabilityFailureCode::InvalidInput,
            message: message.into(),
        }
    }

    pub fn failed(message: impl Into<String>) -> Self {
        Self {
            code: TextCapabilityFailureCode::Failed,
            message: message.into(),
        }
    }
}

/// The two ways a provider distinguishes its own call failures.
///
/// Deliberately smaller than any transport's error surface: everything about
/// grants, revocation, and activation liveness belongs to the handle and the
/// adapter, not to the provider.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TextCapabilityFailureCode {
    InvalidInput,
    Failed,
}

impl fmt::Display for TextCapabilityFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let code = match self.code {
            TextCapabilityFailureCode::InvalidInput => "invalid input",
            TextCapabilityFailureCode::Failed => "failed",
        };
        write!(f, "capability call {code}: {}", self.message)
    }
}

impl Error for TextCapabilityFailure {}
