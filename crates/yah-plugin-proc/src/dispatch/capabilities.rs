use std::panic::AssertUnwindSafe;

use serde::Serialize;
use tokio::sync::oneshot;
use yah_plugin_host::{
    CapabilityBrokerError, CapabilityDefinition, CapabilityHandle, CapabilityHandleError,
    CapabilityId, TextCapability, TextCapabilityFailure, TextCapabilityFailureCode,
};
use yah_plugin_ipc::session::AppError;
use yah_plugin_ipc::types::{CancelReason, HandleId, Outcome};

use super::{DispatchRequest, Dispatcher};
use crate::shared::PumpCommand;

/// Acquire one portable text capability from the activation's exact grant snapshot.
pub const TEXT_CAPABILITY_ACQUIRE_METHOD: &str = "yah.capability.text.acquire/v1";

/// Invoke one portable text capability handle held by this process activation.
pub const TEXT_CAPABILITY_INVOKE_METHOD: &str = "yah.capability.text.invoke/v1";

pub(crate) type DispatchedTextCapability = CapabilityHandle<dyn TextCapability>;

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct AcquireRequest {
    capability: String,
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct InvokeRequest {
    handle: HandleId,
    input: String,
}

pub(crate) struct PreparedInvoke {
    pub handle: HandleId,
    pub input: String,
}

#[derive(Serialize)]
struct Success<T> {
    ok: T,
}

#[derive(Serialize)]
struct Acquired {
    handle: HandleId,
}

#[derive(Serialize)]
struct Invoked {
    output: String,
}

#[derive(Serialize)]
struct DomainFailure<'a> {
    error: DomainError<'a>,
}

#[derive(Serialize)]
struct DomainError<'a> {
    code: &'a str,
    message: String,
}

pub(crate) fn decode_invoke(payload: serde_json::Value) -> Result<PreparedInvoke, ()> {
    serde_json::from_value::<InvokeRequest>(payload)
        .map(|request| PreparedInvoke {
            handle: request.handle,
            input: request.input,
        })
        .map_err(|_| ())
}

pub(crate) fn malformed_invoke() -> Outcome {
    super::refusal(
        yah_plugin_ipc::types::WireErrorKind::InvalidFrame,
        "malformed text capability invoke request",
        false,
    )
}

pub(crate) fn unknown_handle() -> Outcome {
    super::refusal(
        yah_plugin_ipc::types::WireErrorKind::UnknownHandle,
        "no such text capability handle is held by this activation",
        false,
    )
}

pub(crate) fn exhausted_result() -> Outcome {
    domain(
        "exhausted",
        "the text capability result cannot enter the inline response lane",
    )
}

impl Dispatcher {
    pub(super) async fn acquire_capability(&self, request: &DispatchRequest) -> Outcome {
        if request.cancellation.is_cancelled() {
            return cancelled();
        }
        let acquire = match serde_json::from_value::<AcquireRequest>(request.payload()) {
            Ok(acquire) => acquire,
            Err(_) => {
                return super::refusal(
                    yah_plugin_ipc::types::WireErrorKind::InvalidFrame,
                    "malformed text capability acquire request",
                    false,
                );
            }
        };
        let id = match CapabilityId::new(acquire.capability) {
            Ok(id) => id,
            Err(_) => {
                return domain(
                    "invalid-id",
                    "the requested capability id is not well-formed",
                );
            }
        };
        let definition = CapabilityDefinition::<dyn TextCapability>::new(id);
        let capability = match self.context.handle(&definition) {
            Ok(capability) => capability,
            Err(error) => return acquire_failure(error),
        };
        if request.cancellation.is_cancelled() {
            return cancelled();
        }
        let minted = self
            .command(|done: oneshot::Sender<Result<HandleId, AppError>>| {
                PumpCommand::MintCapability {
                    call_id: request.call_id,
                    capability,
                    done,
                }
            })
            .await;
        if request.cancellation.is_cancelled() {
            return cancelled();
        }
        match minted {
            Some(Ok(handle)) => success(Success {
                ok: Acquired { handle },
            }),
            Some(Err(AppError::HandleCeiling)) => domain(
                "handle-limit",
                "the activation's shared live-handle ceiling is exhausted",
            ),
            Some(Err(AppError::SessionRetired)) => domain(
                "exhausted",
                "the activation's correlation budget is exhausted",
            ),
            Some(Err(AppError::NotActive | AppError::UnknownCall | AppError::AlreadySettled))
            | None => domain(
                "exhausted",
                "the activation ended before the capability handle was minted",
            ),
            Some(Err(
                AppError::CallCeiling
                | AppError::SpillRequired { .. }
                | AppError::PayloadTooLarge { .. }
                | AppError::InvalidField(_)
                | AppError::ReleasePending
                | AppError::AlreadyReleased
                | AppError::UnknownWorkerHandle
                | AppError::StreamViolation(_),
            )) => domain("exhausted", "the capability handle could not be admitted"),
        }
    }

    pub(super) async fn invoke_capability(
        &self,
        request: &DispatchRequest,
        capability: DispatchedTextCapability,
        input: String,
    ) -> Outcome {
        if request.cancellation.is_cancelled() {
            return cancelled();
        }
        let result = tokio::task::spawn_blocking(move || {
            std::panic::catch_unwind(AssertUnwindSafe(|| {
                capability.try_with(|provider| provider.invoke(&input))
            }))
        })
        .await;
        if request.cancellation.is_cancelled() {
            return cancelled();
        }
        match result {
            Ok(Ok(Ok(Ok(output)))) => output_result(output),
            Ok(Ok(Ok(Err(failure)))) => provider_failure(failure),
            Ok(Ok(Err(CapabilityHandleError::Revoked { .. }))) => domain(
                "revoked",
                "the text capability grant is revoked for this activation",
            ),
            Ok(Ok(Err(CapabilityHandleError::AdmissionExhausted { .. }))) => domain(
                "exhausted",
                "the text capability has no call admission left",
            ),
            Ok(Err(_)) | Err(_) => {
                domain("failed", "the text capability provider failed unexpectedly")
            }
        }
    }
}

fn output_result(output: String) -> Outcome {
    let result = Success {
        ok: Invoked { output },
    };
    match serde_json::to_value(result) {
        Ok(value)
            if serde_json::to_vec(&value)
                .is_ok_and(|bytes| bytes.len() <= yah_plugin_ipc::MAX_INLINE_RESULT_BYTES) =>
        {
            Outcome::Ok { result: value }
        }
        Ok(_) | Err(_) => exhausted_result(),
    }
}

fn provider_failure(failure: TextCapabilityFailure) -> Outcome {
    let code = match failure.code {
        TextCapabilityFailureCode::InvalidInput => "invalid-input",
        TextCapabilityFailureCode::Failed => "failed",
    };
    domain(code, clip(failure.message))
}

fn acquire_failure(error: CapabilityBrokerError) -> Outcome {
    match error {
        CapabilityBrokerError::NotGranted { .. } => domain(
            "not-granted",
            "the requested capability is not granted to this activation",
        ),
        CapabilityBrokerError::ActivationInactive { .. } => domain(
            "revoked",
            "the activation is closing or closed, and its grants are revoked",
        ),
        CapabilityBrokerError::ProviderUnavailable { .. } => domain(
            "unavailable",
            "the granted text capability provider is withdrawn or replaced",
        ),
        CapabilityBrokerError::ContractTypeMismatch { .. } => domain(
            "mismatched",
            "the capability is not granted under the portable text contract",
        ),
        CapabilityBrokerError::BrokerIncarnationExhausted
        | CapabilityBrokerError::RegistrationIdExhausted
        | CapabilityBrokerError::DuplicateProvider { .. }
        | CapabilityBrokerError::ForeignRegistration { .. } => domain(
            "unavailable",
            "the capability's exact registration is unavailable",
        ),
    }
}

fn success(value: impl Serialize) -> Outcome {
    match serde_json::to_value(value) {
        Ok(result) => Outcome::Ok { result },
        Err(_) => exhausted_result(),
    }
}

fn domain(code: &'static str, message: impl Into<String>) -> Outcome {
    let value = DomainFailure {
        error: DomainError {
            code,
            message: clip(message.into()),
        },
    };
    Outcome::Ok {
        result: serde_json::to_value(value)
            .expect("the static text capability domain envelope serializes"),
    }
}

fn cancelled() -> Outcome {
    Outcome::Cancelled {
        reason: CancelReason::Requested,
    }
}

fn clip(message: impl AsRef<str>) -> String {
    message
        .as_ref()
        .chars()
        .take(yah_plugin_ipc::MAX_ERROR_DETAIL_CHARS)
        .collect()
}
