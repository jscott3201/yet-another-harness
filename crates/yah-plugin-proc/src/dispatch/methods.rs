//! Immutable registrations for application-owned worker methods.
//!
//! The process driver freezes these before it prepares an activation. A
//! handler receives only an already-bounded JSON request and returns an
//! already-bounded JSON result; it never receives a session, frame, endpoint,
//! or command sender.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use yah_compose::ScopeCancellation;
use yah_plugin_ipc::{MAX_INLINE_RESULT_BYTES, MAX_METHOD_CHARS};

const RESERVED_EXACT: [&str; 3] = [
    "artifact.read",
    super::TEXT_CAPABILITY_ACQUIRE_METHOD,
    super::TEXT_CAPABILITY_INVOKE_METHOD,
];

/// One bounded worker request delivered to a registered application method.
///
/// The dispatcher constructs this only after the session has admitted the
/// request and the pump has checked the byte ceiling.
pub struct WorkerMethodRequest {
    payload: serde_json::Value,
    cancellation: WorkerMethodCancellation,
}

impl WorkerMethodRequest {
    pub(super) fn new(payload: serde_json::Value, cancellation: WorkerMethodCancellation) -> Self {
        Self {
            payload,
            cancellation,
        }
    }

    /// The JSON payload admitted for this call.
    pub const fn payload(&self) -> &serde_json::Value {
        &self.payload
    }

    /// Cooperative cancellation for this exact worker call.
    pub const fn cancellation(&self) -> &WorkerMethodCancellation {
        &self.cancellation
    }

    /// Shorthand for [`WorkerMethodCancellation::is_cancelled`].
    pub fn is_cancelled(&self) -> bool {
        self.cancellation.is_cancelled()
    }
}

/// Read-only cooperative cancellation for one registered method call.
///
/// A worker cancel retires the protocol reply path immediately; activation
/// closure also marks this view cancelled. Neither event can interrupt a
/// synchronous callback. A handler that performs long work must poll this
/// view and return cooperatively when it can.
#[derive(Clone)]
pub struct WorkerMethodCancellation {
    requested: Arc<AtomicBool>,
    activation: ScopeCancellation,
}

impl WorkerMethodCancellation {
    pub(crate) fn new(activation: ScopeCancellation) -> Self {
        Self {
            requested: Arc::new(AtomicBool::new(false)),
            activation,
        }
    }

    pub(crate) fn request(&self) {
        self.requested.store(true, Ordering::Release);
    }

    /// Whether the worker cancelled this call or its activation is closing.
    pub fn is_cancelled(&self) -> bool {
        self.requested.load(Ordering::Acquire) || self.activation.is_cancelled()
    }
}

impl fmt::Debug for WorkerMethodCancellation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WorkerMethodCancellation")
            .field("cancelled", &self.is_cancelled())
            .finish()
    }
}

/// A bounded inline result from a registered application method.
pub struct WorkerMethodResult {
    value: serde_json::Value,
}

impl WorkerMethodResult {
    /// Validate a result before it crosses the worker wire.
    pub fn new(value: serde_json::Value) -> Result<Self, WorkerMethodResultError> {
        let bytes =
            serde_json::to_vec(&value).map_err(|_| WorkerMethodResultError::NotSerializable)?;
        if bytes.len() > MAX_INLINE_RESULT_BYTES {
            return Err(WorkerMethodResultError::TooLarge { bytes: bytes.len() });
        }
        Ok(Self { value })
    }

    pub(super) fn into_inner(self) -> serde_json::Value {
        self.value
    }
}

/// Why a registered method result could not enter the inline response lane.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WorkerMethodResultError {
    NotSerializable,
    TooLarge { bytes: usize },
}

/// Stable class for a registered method's domain failure.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WorkerMethodFailureCode {
    InvalidInput,
    Failed,
}

/// A bounded domain failure a registered method may return.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkerMethodFailure {
    code: WorkerMethodFailureCode,
    message: String,
}

impl WorkerMethodFailure {
    pub fn invalid_input(message: impl Into<String>) -> Self {
        Self::new(WorkerMethodFailureCode::InvalidInput, message)
    }

    pub fn failed(message: impl Into<String>) -> Self {
        Self::new(WorkerMethodFailureCode::Failed, message)
    }

    fn new(code: WorkerMethodFailureCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message
                .into()
                .chars()
                .take(yah_plugin_ipc::MAX_ERROR_DETAIL_CHARS)
                .collect(),
        }
    }

    pub const fn code(&self) -> WorkerMethodFailureCode {
        self.code
    }

    pub fn message(&self) -> &str {
        &self.message
    }
}

/// Synchronous application behavior made available to one process worker.
///
/// Calls run off the pump under the activation's shared bounded dispatcher
/// concurrency. Implementations must return; cancellation retires the worker
/// call and reply path but cannot interrupt a synchronous callback.
pub trait WorkerMethod: Send + Sync + 'static {
    fn invoke(
        &self,
        request: &WorkerMethodRequest,
    ) -> Result<WorkerMethodResult, WorkerMethodFailure>;
}

/// Immutable method registry consumed when a process driver is constructed.
#[derive(Clone, Default)]
pub struct WorkerMethodRegistry {
    methods: BTreeMap<String, Arc<dyn WorkerMethod>>,
}

impl WorkerMethodRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register one application-owned method before any activation exists.
    pub fn register(
        &mut self,
        name: impl AsRef<str>,
        method: Arc<dyn WorkerMethod>,
    ) -> Result<(), WorkerMethodRegistrationError> {
        let name = name.as_ref();
        if name.is_empty() || name.chars().count() > MAX_METHOD_CHARS {
            return Err(WorkerMethodRegistrationError::InvalidName);
        }
        if is_reserved(name) {
            return Err(WorkerMethodRegistrationError::ReservedName);
        }
        if self.methods.contains_key(name) {
            return Err(WorkerMethodRegistrationError::DuplicateName);
        }
        self.methods.insert(name.to_owned(), method);
        Ok(())
    }

    pub(super) fn into_methods(self) -> BTreeMap<String, Arc<dyn WorkerMethod>> {
        self.methods
    }
}

/// Why a method could not join a driver's immutable registration set.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WorkerMethodRegistrationError {
    InvalidName,
    ReservedName,
    DuplicateName,
}

impl fmt::Display for WorkerMethodRegistrationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidName => "worker method name is outside the protocol bound",
            Self::ReservedName => "worker method name belongs to a protocol-owned family",
            Self::DuplicateName => "worker method name is already registered",
        })
    }
}

impl std::error::Error for WorkerMethodRegistrationError {}

fn is_reserved(name: &str) -> bool {
    RESERVED_EXACT.contains(&name)
}
