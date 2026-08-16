use std::{error::Error, fmt, future::Future, pin::Pin, sync::Arc};

use yah_compose::{
    ComponentSlotError, ProviderSelectionEpoch, ReconcileOutcome, ScopeCancellation,
};

use crate::{DriverKind, PluginRevisionId};

macro_rules! simple_error {
    ($name:ident) => {
        impl $name {
            pub fn new(summary: impl Into<String>) -> Self {
                Self {
                    summary: summary.into(),
                }
            }

            pub fn summary(&self) -> &str {
                &self.summary
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(&self.summary)
            }
        }

        impl Error for $name {}
    };
}

/// Owned future returned by a runtime-neutral driver operation.
///
/// Futures are executor-neutral, movable between threads, and independent of
/// a borrow from the driver trait object. Tokio is not part of this contract.
pub type DriverFuture<T> = Pin<Box<dyn Future<Output = T> + Send + 'static>>;

/// Exact process-local identity of one plugin revision activation.
///
/// The package revision is syntactically validated but is not proof of package
/// admission. The provider-selection epoch fences this identity to one live
/// component activation and must not be persisted or reused after restart.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct PluginActivationId {
    revision: Arc<PluginRevisionId>,
    selection_epoch: ProviderSelectionEpoch,
}

impl PluginActivationId {
    pub fn new(revision: PluginRevisionId, selection_epoch: ProviderSelectionEpoch) -> Self {
        Self {
            revision: Arc::new(revision),
            selection_epoch,
        }
    }

    pub fn revision(&self) -> &PluginRevisionId {
        &self.revision
    }

    pub const fn selection_epoch(&self) -> ProviderSelectionEpoch {
        self.selection_epoch
    }
}

impl fmt::Display for PluginActivationId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}@{}", self.revision, self.selection_epoch)
    }
}

/// Read-only input used to prepare one exact driver activation.
///
/// This request remains inert identity, cancellation, and metadata. SDK-003
/// will deliver actionable capability handles through a separate start
/// context only after deactivation cleanup has been admitted. Drivers cannot
/// issue cancellation through this value.
#[derive(Clone, Debug)]
pub struct PluginActivationRequest {
    id: PluginActivationId,
    cancellation: ScopeCancellation,
}

impl PluginActivationRequest {
    pub(crate) fn new(id: PluginActivationId, cancellation: ScopeCancellation) -> Self {
        Self { id, cancellation }
    }

    pub const fn id(&self) -> &PluginActivationId {
        &self.id
    }

    pub const fn cancellation(&self) -> &ScopeCancellation {
        &self.cancellation
    }
}

/// Object-safe execution lane capable of preparing multiple exact activations.
///
/// `prepare` must be inert: it may validate and allocate in-memory control
/// state, but it must not execute plugin code, start a task or process, or
/// acquire an external resource. That work begins only after the host has
/// admitted deactivation cleanup and calls [`PreparedDriverActivation::start`].
/// The reported revision and lane describe the host-selected driver; neither
/// is package-admission proof. Driver destruction is not shutdown authority:
/// implementations must keep `Drop` inert and non-panicking, while exact
/// activation resources remain owned by their prepared controls.
pub trait PluginDriver: Send + Sync + 'static {
    fn kind(&self) -> DriverKind;

    fn revision_id(&self) -> &PluginRevisionId;

    fn prepare(
        &self,
        request: PluginActivationRequest,
    ) -> Result<Arc<dyn PreparedDriverActivation>, DriverPrepareError>;
}

/// Driver-owned control state for one exact activation.
///
/// Partial resources created while `start` is polled must be retained here,
/// not solely in the returned future. `deactivate` must clean `NeverStarted`,
/// `Starting`, and `Active` states for this exact identity. The host invokes it
/// once through effect-scope cleanup; implementations should still reject or
/// harmlessly absorb stale backend events for other identities.
///
/// The control's destructor is not cleanup authority: it must be inert and
/// non-panicking. All live activation resources must be released by
/// `deactivate`, independently of any concurrent or stale health observer.
pub trait PreparedDriverActivation: Send + Sync + 'static {
    fn id(&self) -> &PluginActivationId;

    /// Construct the activation operation after cleanup ownership is secured.
    /// The opaque permit cannot be minted outside this crate.
    fn start(&self, permit: DriverStartPermit) -> DriverFuture<Result<(), DriverActivationError>>;

    /// Return a thread-safe, nonblocking advisory health snapshot.
    ///
    /// Health is distinct from activation readiness and never changes
    /// composition lifecycle or restart policy on its own. It may race
    /// deactivation; the host fences the result but does not drain this call.
    fn health(&self) -> Result<PluginHealth, DriverHealthError>;

    /// Stop and release every resource for the permit's exact activation.
    ///
    /// This is final backend-resource shutdown, not a guest hook that may rely
    /// on other activation effects. It runs after cancellation and after later
    /// effects have unwound, and it must succeed independently of health-handle
    /// lifetime.
    fn deactivate(
        &self,
        permit: DriverStopPermit,
    ) -> DriverFuture<Result<(), DriverDeactivationError>>;
}

/// Opaque authority to begin the exact prepared activation.
#[derive(Debug)]
pub struct DriverStartPermit {
    id: PluginActivationId,
    _private: (),
}

impl DriverStartPermit {
    pub(crate) fn new(id: PluginActivationId) -> Self {
        Self { id, _private: () }
    }

    pub const fn id(&self) -> &PluginActivationId {
        &self.id
    }
}

/// Opaque authority to deactivate the exact prepared activation.
#[derive(Debug)]
pub struct DriverStopPermit {
    id: PluginActivationId,
    _private: (),
}

impl DriverStopPermit {
    pub(crate) fn new(id: PluginActivationId) -> Self {
        Self { id, _private: () }
    }

    pub const fn id(&self) -> &PluginActivationId {
        &self.id
    }
}

/// Advisory health reported by a driver for one active activation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PluginHealth {
    Healthy,
    Degraded { summary: String },
    Unhealthy { summary: String },
}

impl PluginHealth {
    pub fn degraded(summary: impl Into<String>) -> Self {
        Self::Degraded {
            summary: summary.into(),
        }
    }

    pub fn unhealthy(summary: impl Into<String>) -> Self {
        Self::Unhealthy {
            summary: summary.into(),
        }
    }
}

/// Why inert activation preparation was rejected by a driver.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DriverPrepareError {
    summary: String,
}

simple_error!(DriverPrepareError);

/// Why driver deactivation did not finish cleanly.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DriverDeactivationError {
    summary: String,
}

simple_error!(DriverDeactivationError);

/// Why a nonblocking driver health snapshot was unavailable.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DriverHealthError {
    summary: String,
}

simple_error!(DriverHealthError);

/// Class of activation failure returned to the host.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum DriverActivationErrorKind {
    Cancelled,
    Failed,
    Panicked,
}

/// Terminal failure from constructing, polling, or destroying activation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DriverActivationError {
    kind: DriverActivationErrorKind,
    summary: String,
}

impl DriverActivationError {
    pub fn cancelled(summary: impl Into<String>) -> Self {
        Self {
            kind: DriverActivationErrorKind::Cancelled,
            summary: summary.into(),
        }
    }

    pub fn failed(summary: impl Into<String>) -> Self {
        Self {
            kind: DriverActivationErrorKind::Failed,
            summary: summary.into(),
        }
    }

    pub(crate) fn panicked(summary: impl Into<String>) -> Self {
        Self {
            kind: DriverActivationErrorKind::Panicked,
            summary: summary.into(),
        }
    }

    pub const fn kind(&self) -> DriverActivationErrorKind {
        self.kind
    }

    pub fn summary(&self) -> &str {
        &self.summary
    }

    pub(crate) fn with_future_drop_panic(self, summary: String) -> Self {
        let prior = self.summary;
        Self::panicked(format!(
            "activation failed before its future destructor panicked: {prior}; destructor panic: {summary}"
        ))
    }
}

impl fmt::Display for DriverActivationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "plugin activation {:?}: {}", self.kind, self.summary)
    }
}

impl Error for DriverActivationError {}

/// Failure returned while preparing the host-owned activation guard.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum HostPluginActivationError {
    Composition(ComponentSlotError),
    Driver(DriverPrepareError),
    DriverPanicked {
        summary: String,
    },
    PreparedIdentityMismatch {
        expected: PluginActivationId,
        received: PluginActivationId,
    },
    DriverControlDropPanicked {
        summary: String,
    },
}

impl fmt::Display for HostPluginActivationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Composition(error) => error.fmt(f),
            Self::Driver(error) => write!(f, "driver rejected activation preparation: {error}"),
            Self::DriverPanicked { summary } => {
                write!(f, "driver panicked while preparing activation: {summary}")
            }
            Self::PreparedIdentityMismatch { expected, received } => write!(
                f,
                "prepared activation identity {received} does not match requested {expected}"
            ),
            Self::DriverControlDropPanicked { summary } => {
                write!(
                    f,
                    "rejected plugin driver control panicked while dropping: {summary}"
                )
            }
        }
    }
}

impl Error for HostPluginActivationError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Composition(error) => Some(error),
            Self::Driver(error) => Some(error),
            Self::DriverPanicked { .. }
            | Self::PreparedIdentityMismatch { .. }
            | Self::DriverControlDropPanicked { .. } => None,
        }
    }
}

impl From<ComponentSlotError> for HostPluginActivationError {
    fn from(error: ComponentSlotError) -> Self {
        Self::Composition(error)
    }
}

/// Terminal failure from driving an admitted plugin activation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PluginStartError {
    Driver {
        activation_id: PluginActivationId,
        failure: DriverActivationError,
        stop: Result<Box<ReconcileOutcome>, Box<ComponentSlotError>>,
    },
    Superseded {
        activation_id: PluginActivationId,
        stop: Option<Box<ReconcileOutcome>>,
    },
    Composition {
        activation_id: PluginActivationId,
        error: Box<ComponentSlotError>,
        stop: Option<Result<Box<ReconcileOutcome>, Box<ComponentSlotError>>>,
    },
}

impl fmt::Display for PluginStartError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Driver {
                activation_id,
                failure,
                ..
            } => write!(f, "plugin activation {activation_id} failed: {failure}"),
            Self::Superseded { activation_id, .. } => write!(
                f,
                "plugin activation {activation_id} was superseded before readiness"
            ),
            Self::Composition {
                activation_id,
                error,
                ..
            } => write!(
                f,
                "composition rejected plugin activation {activation_id}: {error}"
            ),
        }
    }
}

impl Error for PluginStartError {}

/// Rejected health observation for an inactive or failing exact activation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PluginHealthError {
    Inactive {
        activation_id: PluginActivationId,
    },
    Driver {
        activation_id: PluginActivationId,
        error: DriverHealthError,
    },
    DriverPanicked {
        activation_id: PluginActivationId,
        summary: String,
    },
}

impl fmt::Display for PluginHealthError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Inactive { activation_id } => {
                write!(f, "plugin activation {activation_id} is no longer active")
            }
            Self::Driver {
                activation_id,
                error,
            } => write!(
                f,
                "plugin activation {activation_id} health is unknown: {error}"
            ),
            Self::DriverPanicked {
                activation_id,
                summary,
            } => write!(
                f,
                "plugin activation {activation_id} health probe panicked: {summary}"
            ),
        }
    }
}

impl Error for PluginHealthError {}
