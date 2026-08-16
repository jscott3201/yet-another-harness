use std::{error::Error, fmt, sync::Arc};

use crate::{DriverKind, PluginActivationId, PluginDriver, PluginRevision};

/// Stable host-side scenario exercised by the shared driver harness.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum DriverConformanceCase {
    ReadyLifecycle,
    PendingStartCancellation,
    ReturnedStartFailure,
    ReturnedDeactivationFailure,
    SharedDriverIsolation,
}

impl DriverConformanceCase {
    pub const ALL: [Self; 5] = [
        Self::ReadyLifecycle,
        Self::PendingStartCancellation,
        Self::ReturnedStartFailure,
        Self::ReturnedDeactivationFailure,
        Self::SharedDriverIsolation,
    ];

    pub const fn id(self) -> &'static str {
        match self {
            Self::ReadyLifecycle => "driver.ready-lifecycle",
            Self::PendingStartCancellation => "driver.pending-start-cancellation",
            Self::ReturnedStartFailure => "driver.returned-start-failure",
            Self::ReturnedDeactivationFailure => "driver.returned-deactivation-failure",
            Self::SharedDriverIsolation => "driver.shared-driver-isolation",
        }
    }

    pub const fn description(self) -> &'static str {
        match self {
            Self::ReadyLifecycle => {
                "ready activation, healthy observation, fencing, and clean deactivation"
            }
            Self::PendingStartCancellation => {
                "pending start is cancelled before drop and cleaned exactly once"
            }
            Self::ReturnedStartFailure => {
                "returned start failure rolls the exact activation back to pending"
            }
            Self::ReturnedDeactivationFailure => {
                "returned teardown failure is cached, blocked, and explicitly abandoned"
            }
            Self::SharedDriverIsolation => {
                "one shared driver keeps two activation identities and lifecycles isolated"
            }
        }
    }
}

impl fmt::Display for DriverConformanceCase {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.id())
    }
}

/// Runner phase in which a conformance case failed.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum DriverConformancePhase {
    Setup,
    Preparation,
    Start,
    Health,
    Cancellation,
    Teardown,
    Isolation,
    Probe,
}

impl fmt::Display for DriverConformancePhase {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Setup => "setup",
            Self::Preparation => "preparation",
            Self::Start => "start",
            Self::Health => "health",
            Self::Cancellation => "cancellation",
            Self::Teardown => "teardown",
            Self::Isolation => "isolation",
            Self::Probe => "probe",
        })
    }
}

/// Terminal result observed at the driver future boundary.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum DriverConformanceTerminal {
    Succeeded,
    ReturnedError,
}

/// State of the fixture resource independently observed by trusted test code.
///
/// Missing evidence is represented on [`DriverActivationObservation`] together
/// with the probe error; it is not a state a target may return successfully.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum DriverConformanceResourceState {
    NotAcquired,
    Live,
    Released,
}

/// Last teardown state reached while running one case.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum DriverConformanceTeardown {
    /// No driver cleanup was admitted or driven.
    NotStarted,
    /// Cleanup reached a terminal report without failures.
    Clean,
    /// Cleanup reached a non-clean terminal report that could not be abandoned.
    Blocked,
    /// A non-clean report was cached before explicit owner abandonment.
    BlockedThenAbandoned,
    /// The host could not reach a terminal cleanup report.
    Incomplete,
}

/// Failure while a trusted test adapter constructs one fresh subject.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DriverConformanceSetupError {
    summary: String,
}

impl DriverConformanceSetupError {
    pub fn new(summary: impl Into<String>) -> Self {
        Self {
            summary: summary.into(),
        }
    }

    pub fn summary(&self) -> &str {
        &self.summary
    }
}

impl fmt::Display for DriverConformanceSetupError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.summary)
    }
}

impl Error for DriverConformanceSetupError {}

/// Failure from a trusted fixture probe.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DriverConformanceProbeError {
    summary: String,
}

impl DriverConformanceProbeError {
    pub fn new(summary: impl Into<String>) -> Self {
        Self {
            summary: summary.into(),
        }
    }

    pub fn summary(&self) -> &str {
        &self.summary
    }
}

impl fmt::Display for DriverConformanceProbeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.summary)
    }
}

impl Error for DriverConformanceProbeError {}

/// Trusted, observation-only fixture seam for backend-owned test resources.
///
/// A probe must reject unknown exact activation identities and must not retain
/// the driver, a prepared activation, a broker, or the actual resource being
/// observed. It is test infrastructure, not plugin authority.
pub trait DriverConformanceProbe: Send + Sync + 'static {
    fn resource_state(
        &self,
        activation: &PluginActivationId,
    ) -> Result<DriverConformanceResourceState, DriverConformanceProbeError>;
}

/// One fresh driver subject supplied for a named conformance case.
///
/// `expected_revision` is trusted fixture input and must not be derived from
/// `driver.revision_id()`. The harness consumes this value and independently
/// validates the driver's reported revision and lane through the ordinary host
/// activation path.
#[must_use = "a conformance subject must be consumed by the runner"]
pub struct DriverConformanceSubject {
    pub(super) expected_revision: PluginRevision,
    pub(super) driver: Arc<dyn PluginDriver>,
    pub(super) probe: Arc<dyn DriverConformanceProbe>,
}

impl DriverConformanceSubject {
    pub fn new(
        expected_revision: PluginRevision,
        driver: Arc<dyn PluginDriver>,
        probe: Arc<dyn DriverConformanceProbe>,
    ) -> Self {
        Self {
            expected_revision,
            driver,
            probe,
        }
    }

    /// Consume the subject into the independently trusted fixture inputs.
    ///
    /// This is primarily useful for testkit self-tests and adapters that wrap
    /// another target. It does not mint host permits or make the probe an
    /// authority source.
    pub fn into_parts(
        self,
    ) -> (
        PluginRevision,
        Arc<dyn PluginDriver>,
        Arc<dyn DriverConformanceProbe>,
    ) {
        (self.expected_revision, self.driver, self.probe)
    }
}

impl fmt::Debug for DriverConformanceSubject {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DriverConformanceSubject")
            .field("expected_revision", &self.expected_revision)
            .finish_non_exhaustive()
    }
}

/// Trusted adapter that configures one fresh logical subject per case.
///
/// The requested case is also the fixture script: pending-start must remain
/// pending until cancellation, returned-failure cases must return the named
/// error class, and shared-driver isolation must support two activations on the
/// same driver object. The adapter is not an authority boundary and may not
/// mint host permits or activation identities itself.
pub trait DriverConformanceTarget: Send + Sync {
    fn name(&self) -> &str;

    fn kind(&self) -> DriverKind;

    fn subject(
        &self,
        case: DriverConformanceCase,
    ) -> Result<DriverConformanceSubject, DriverConformanceSetupError>;
}

/// Bounded observations recorded by the harness's private driver decorator.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DriverActivationObservation {
    activation: PluginActivationId,
    prepare_calls: u64,
    prepare_succeeded: bool,
    start_factory_calls: u64,
    start_polls: u64,
    start_pending_polls: u64,
    start_terminal: Option<DriverConformanceTerminal>,
    start_drops: u64,
    start_drop_saw_cancellation: bool,
    start_permit_matched: Option<bool>,
    health_calls: u64,
    deactivation_factory_calls: u64,
    deactivation_polls: u64,
    deactivation_pending_polls: u64,
    deactivation_terminal: Option<DriverConformanceTerminal>,
    deactivation_drops: u64,
    deactivation_saw_cancellation: bool,
    deactivation_saw_later_cleanup: bool,
    deactivation_permit_matched: Option<bool>,
    resource_state: Option<DriverConformanceResourceState>,
    resource_probe_error: Option<String>,
}

impl DriverActivationObservation {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn new(
        activation: PluginActivationId,
        prepare_calls: u64,
        prepare_succeeded: bool,
        start_factory_calls: u64,
        start_polls: u64,
        start_pending_polls: u64,
        start_terminal: Option<DriverConformanceTerminal>,
        start_drops: u64,
        start_drop_saw_cancellation: bool,
        start_permit_matched: Option<bool>,
        health_calls: u64,
        deactivation_factory_calls: u64,
        deactivation_polls: u64,
        deactivation_pending_polls: u64,
        deactivation_terminal: Option<DriverConformanceTerminal>,
        deactivation_drops: u64,
        deactivation_saw_cancellation: bool,
        deactivation_saw_later_cleanup: bool,
        deactivation_permit_matched: Option<bool>,
        resource_state: Option<DriverConformanceResourceState>,
        resource_probe_error: Option<String>,
    ) -> Self {
        Self {
            activation,
            prepare_calls,
            prepare_succeeded,
            start_factory_calls,
            start_polls,
            start_pending_polls,
            start_terminal,
            start_drops,
            start_drop_saw_cancellation,
            start_permit_matched,
            health_calls,
            deactivation_factory_calls,
            deactivation_polls,
            deactivation_pending_polls,
            deactivation_terminal,
            deactivation_drops,
            deactivation_saw_cancellation,
            deactivation_saw_later_cleanup,
            deactivation_permit_matched,
            resource_state,
            resource_probe_error,
        }
    }

    pub const fn activation(&self) -> &PluginActivationId {
        &self.activation
    }

    pub const fn prepare_calls(&self) -> u64 {
        self.prepare_calls
    }

    pub const fn prepare_succeeded(&self) -> bool {
        self.prepare_succeeded
    }

    pub const fn start_factory_calls(&self) -> u64 {
        self.start_factory_calls
    }

    pub const fn start_polls(&self) -> u64 {
        self.start_polls
    }

    pub const fn start_pending_polls(&self) -> u64 {
        self.start_pending_polls
    }

    pub const fn start_terminal(&self) -> Option<DriverConformanceTerminal> {
        self.start_terminal
    }

    pub const fn start_drops(&self) -> u64 {
        self.start_drops
    }

    pub const fn start_drop_saw_cancellation(&self) -> bool {
        self.start_drop_saw_cancellation
    }

    pub const fn start_permit_matched(&self) -> Option<bool> {
        self.start_permit_matched
    }

    pub const fn health_calls(&self) -> u64 {
        self.health_calls
    }

    pub const fn deactivation_factory_calls(&self) -> u64 {
        self.deactivation_factory_calls
    }

    pub const fn deactivation_polls(&self) -> u64 {
        self.deactivation_polls
    }

    pub const fn deactivation_pending_polls(&self) -> u64 {
        self.deactivation_pending_polls
    }

    pub const fn deactivation_terminal(&self) -> Option<DriverConformanceTerminal> {
        self.deactivation_terminal
    }

    pub const fn deactivation_drops(&self) -> u64 {
        self.deactivation_drops
    }

    pub const fn deactivation_saw_cancellation(&self) -> bool {
        self.deactivation_saw_cancellation
    }

    pub const fn deactivation_saw_later_cleanup(&self) -> bool {
        self.deactivation_saw_later_cleanup
    }

    pub const fn deactivation_permit_matched(&self) -> Option<bool> {
        self.deactivation_permit_matched
    }

    /// Independently probed fixture-resource evidence retained for this observation.
    ///
    /// A missing value is never valid passing evidence. On a failed case,
    /// [`Self::resource_probe_error`] preserves why the trusted probe could not
    /// produce a state.
    pub const fn resource_state(&self) -> Option<DriverConformanceResourceState> {
        self.resource_state
    }

    /// Probe failure retained in place of resource state.
    ///
    /// Failed cases preserve the first known error through best-effort
    /// teardown instead of replacing it if a later diagnostic probe recovers.
    pub fn resource_probe_error(&self) -> Option<&str> {
        self.resource_probe_error.as_deref()
    }
}

/// Successful evidence for one case.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DriverConformanceCaseReport {
    case: DriverConformanceCase,
    activations: Vec<DriverActivationObservation>,
    teardown: DriverConformanceTeardown,
}

impl DriverConformanceCaseReport {
    pub(super) fn new(
        case: DriverConformanceCase,
        activations: Vec<DriverActivationObservation>,
        teardown: DriverConformanceTeardown,
    ) -> Self {
        Self {
            case,
            activations,
            teardown,
        }
    }

    pub const fn case(&self) -> DriverConformanceCase {
        self.case
    }

    pub fn activations(&self) -> &[DriverActivationObservation] {
        &self.activations
    }

    pub const fn teardown(&self) -> DriverConformanceTeardown {
        self.teardown
    }
}

/// Structured failure from one runner-controlled phase.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DriverConformanceFailure {
    target: String,
    kind: DriverKind,
    case: DriverConformanceCase,
    phase: DriverConformancePhase,
    summary: String,
    observations: Vec<DriverActivationObservation>,
    teardown: DriverConformanceTeardown,
}

impl DriverConformanceFailure {
    pub(super) fn new(
        target: String,
        kind: DriverKind,
        case: DriverConformanceCase,
        phase: DriverConformancePhase,
        summary: impl Into<String>,
        observations: Vec<DriverActivationObservation>,
        teardown: DriverConformanceTeardown,
    ) -> Self {
        Self {
            target,
            kind,
            case,
            phase,
            summary: summary.into(),
            observations,
            teardown,
        }
    }

    pub fn target(&self) -> &str {
        &self.target
    }

    pub const fn kind(&self) -> DriverKind {
        self.kind
    }

    pub const fn case(&self) -> DriverConformanceCase {
        self.case
    }

    pub const fn phase(&self) -> DriverConformancePhase {
        self.phase
    }

    pub fn summary(&self) -> &str {
        &self.summary
    }

    pub fn observations(&self) -> &[DriverActivationObservation] {
        &self.observations
    }

    pub const fn teardown(&self) -> DriverConformanceTeardown {
        self.teardown
    }
}

impl fmt::Display for DriverConformanceFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "driver conformance target {:?} failed {} during {}: {}",
            self.target, self.case, self.phase, self.summary
        )
    }
}

impl Error for DriverConformanceFailure {}

/// Result of one independently runnable case.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DriverConformanceCaseResult {
    Passed(DriverConformanceCaseReport),
    Failed(DriverConformanceFailure),
}

impl DriverConformanceCaseResult {
    pub const fn case(&self) -> DriverConformanceCase {
        match self {
            Self::Passed(report) => report.case(),
            Self::Failed(failure) => failure.case(),
        }
    }

    pub const fn passed(&self) -> bool {
        matches!(self, Self::Passed(_))
    }

    pub const fn failure(&self) -> Option<&DriverConformanceFailure> {
        match self {
            Self::Passed(_) => None,
            Self::Failed(failure) => Some(failure),
        }
    }
}

/// Aggregate result that preserves every independently executed case.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DriverConformanceReport {
    target: String,
    kind: DriverKind,
    cases: Vec<DriverConformanceCaseResult>,
}

impl DriverConformanceReport {
    pub(super) fn new(
        target: String,
        kind: DriverKind,
        cases: Vec<DriverConformanceCaseResult>,
    ) -> Self {
        Self {
            target,
            kind,
            cases,
        }
    }

    pub fn target(&self) -> &str {
        &self.target
    }

    pub const fn kind(&self) -> DriverKind {
        self.kind
    }

    pub fn cases(&self) -> &[DriverConformanceCaseResult] {
        &self.cases
    }

    pub fn failures(&self) -> impl Iterator<Item = &DriverConformanceFailure> {
        self.cases.iter().filter_map(|result| result.failure())
    }

    pub fn is_conformant(&self) -> bool {
        self.cases.iter().all(DriverConformanceCaseResult::passed)
    }
}

impl fmt::Display for DriverConformanceReport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let passed = self.cases.iter().filter(|case| case.passed()).count();
        write!(
            f,
            "driver conformance target {:?}: {passed}/{} cases passed",
            self.target,
            self.cases.len()
        )?;
        for failure in self.failures() {
            write!(f, "\n- {failure}")?;
        }
        Ok(())
    }
}
