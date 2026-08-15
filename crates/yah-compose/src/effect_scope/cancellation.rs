use std::fmt;

use tokio_util::sync::CancellationToken;

/// Read-only cancellation observation for work owned by an effect scope.
///
/// Cancellation is a request, not proof that work stopped. Only the owning
/// effect scope can issue it.
#[derive(Clone)]
pub struct ScopeCancellation {
    token: CancellationToken,
}

impl ScopeCancellation {
    pub(super) fn new(token: CancellationToken) -> Self {
        Self { token }
    }

    pub fn is_cancelled(&self) -> bool {
        self.token.is_cancelled()
    }

    pub async fn cancelled(&self) {
        self.token.cancelled().await;
    }
}

impl fmt::Debug for ScopeCancellation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ScopeCancellation")
            .field("is_cancelled", &self.is_cancelled())
            .finish()
    }
}
