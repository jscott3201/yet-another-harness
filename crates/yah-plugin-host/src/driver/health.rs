use std::{
    fmt,
    panic::{AssertUnwindSafe, catch_unwind},
    sync::Weak,
};

use yah_compose::ScopeCancellation;

use crate::DriverKind;

use super::{
    PluginActivationId, PluginHealth, PluginHealthError, PreparedDriverActivation,
    activation::consume_panic_payload, control::PreparedControl,
};

/// Cloneable exact-activation handle for advisory health observation.
#[derive(Clone)]
pub struct PluginActivationHandle {
    id: PluginActivationId,
    kind: DriverKind,
    cancellation: ScopeCancellation,
    control: Weak<PreparedControl>,
}

impl PluginActivationHandle {
    pub(super) fn new(
        id: PluginActivationId,
        kind: DriverKind,
        cancellation: ScopeCancellation,
        control: Weak<PreparedControl>,
    ) -> Self {
        Self {
            id,
            kind,
            cancellation,
            control,
        }
    }

    pub const fn id(&self) -> &PluginActivationId {
        &self.id
    }

    pub const fn kind(&self) -> DriverKind {
        self.kind
    }

    /// Read the driver's current nonblocking health snapshot.
    ///
    /// Cancellation is checked before and after the driver call so a stopped
    /// or replaced activation cannot surface a fresh observation. Health never
    /// mutates lifecycle state. A probe may race teardown, but its panic payload
    /// is normalized before cancellation can suppress the stale result.
    pub fn health(&self) -> Result<PluginHealth, PluginHealthError> {
        if self.cancellation.is_cancelled() {
            return Err(PluginHealthError::Inactive {
                activation_id: self.id.clone(),
            });
        }
        let Some(control) = self.control.upgrade() else {
            return Err(PluginHealthError::Inactive {
                activation_id: self.id.clone(),
            });
        };
        let observed = match catch_unwind(AssertUnwindSafe(|| {
            control.with_prepared(PreparedDriverActivation::health)
        })) {
            Ok(Some(Ok(health))) => Ok(health),
            Ok(Some(Err(error))) => Err(PluginHealthError::Driver {
                activation_id: self.id.clone(),
                error,
            }),
            Ok(None) => Err(PluginHealthError::Inactive {
                activation_id: self.id.clone(),
            }),
            Err(payload) => Err(PluginHealthError::DriverPanicked {
                activation_id: self.id.clone(),
                summary: consume_panic_payload(payload),
            }),
        };
        if self.cancellation.is_cancelled() {
            return Err(PluginHealthError::Inactive {
                activation_id: self.id.clone(),
            });
        }
        observed
    }
}

impl fmt::Debug for PluginActivationHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PluginActivationHandle")
            .field("id", &self.id)
            .field("kind", &self.kind)
            .field("cancelled", &self.cancellation.is_cancelled())
            .finish_non_exhaustive()
    }
}
