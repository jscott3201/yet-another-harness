//! Bounded worker-to-host application dispatch.
//!
//! Calls admitted by the session enter one activation-local bounded queue.
//! Provider callbacks run on blocking threads under a shared concurrency
//! semaphore, never on the pump. Outcomes return as commands so the pump
//! remains the sole session mutator.

use std::collections::BTreeMap;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;

use tokio::sync::{Semaphore, mpsc, oneshot};
use yah_compose::ScopeCancellation;
use yah_plugin_host::PluginStartContext;
use yah_plugin_ipc::session::AppError;
use yah_plugin_ipc::types::{CallId, CancelReason, Outcome, WireError, WireErrorKind};

use crate::shared::PumpCommand;

mod capabilities;
mod methods;

pub use capabilities::{TEXT_CAPABILITY_ACQUIRE_METHOD, TEXT_CAPABILITY_INVOKE_METHOD};
pub use methods::{
    WorkerMethod, WorkerMethodCancellation, WorkerMethodFailure, WorkerMethodFailureCode,
    WorkerMethodRegistrationError, WorkerMethodRegistry, WorkerMethodRequest, WorkerMethodResult,
    WorkerMethodResultError,
};

/// One session-admitted worker call handed to the application lane.
pub(crate) struct DispatchRequest {
    pub call_id: CallId,
    pub work: DispatchWork,
    pub cancellation: WorkerMethodCancellation,
}

impl DispatchRequest {
    fn payload(&self) -> serde_json::Value {
        match &self.work {
            DispatchWork::CapabilityAcquire { payload }
            | DispatchWork::Application { payload, .. } => payload.clone(),
            DispatchWork::CapabilityInvoke { .. } => serde_json::Value::Null,
        }
    }
}

pub(crate) enum DispatchWork {
    Application {
        method: String,
        payload: serde_json::Value,
    },
    CapabilityAcquire {
        payload: serde_json::Value,
    },
    CapabilityInvoke {
        capability: capabilities::DispatchedTextCapability,
        input: String,
    },
}

pub(crate) use capabilities::{
    DispatchedTextCapability, decode_invoke, malformed_invoke, unknown_handle,
};

pub(super) fn refusal(kind: WireErrorKind, message: &'static str, retryable: bool) -> Outcome {
    Outcome::Err {
        error: WireError {
            kind,
            message: message.to_owned(),
            retryable,
            reconcile_required: false,
        },
    }
}

fn cancelled() -> Outcome {
    Outcome::Cancelled {
        reason: CancelReason::Requested,
    }
}

#[derive(Clone)]
struct Dispatcher {
    methods: Arc<BTreeMap<String, Arc<dyn WorkerMethod>>>,
    /// Weak so callbacks and queued work cannot keep an ended pump alive.
    commands: mpsc::WeakSender<PumpCommand>,
    context: PluginStartContext,
    providers: Arc<Semaphore>,
}

struct DispatchAnswer {
    outcome: Outcome,
    capability: bool,
}

pub(crate) fn spawn(
    context: PluginStartContext,
    commands: mpsc::WeakSender<PumpCommand>,
    queue_capacity: usize,
    provider_concurrency: usize,
    methods: WorkerMethodRegistry,
) -> mpsc::Sender<DispatchRequest> {
    let scope_cancellation = context.cancellation().clone();
    let (queue, receiver) = mpsc::channel(queue_capacity.max(1));
    let dispatcher = Dispatcher {
        methods: Arc::new(methods.into_methods()),
        commands,
        context,
        providers: Arc::new(Semaphore::new(provider_concurrency.max(1))),
    };
    tokio::spawn(dispatcher.run(receiver, scope_cancellation));
    queue
}

impl Dispatcher {
    async fn command<T>(
        &self,
        make: impl FnOnce(oneshot::Sender<Result<T, AppError>>) -> PumpCommand,
    ) -> Option<Result<T, AppError>> {
        let sender = self.commands.upgrade()?;
        // Keep the reserved slot from reservation through delivery. A reply
        // admitted by the dispatcher must not lose a race to another sender.
        let permit = sender.reserve_owned().await.ok()?;
        let (done_sender, done) = oneshot::channel();
        permit.send(make(done_sender));
        done.await.ok()
    }

    async fn reply(&self, call_id: CallId, answer: DispatchAnswer) {
        let DispatchAnswer {
            outcome,
            capability,
        } = answer;
        let spillable = match &outcome {
            Outcome::Ok { result } => serde_json::to_vec(result)
                .ok()
                .filter(|bytes| bytes.len() > yah_plugin_ipc::MAX_INLINE_RESULT_BYTES),
            _ => None,
        };
        let applied = self
            .command(|done| PumpCommand::Reply {
                call_id,
                outcome,
                done,
            })
            .await;
        match applied {
            Some(Ok(())) | None => {}
            Some(Err(AppError::SpillRequired { .. })) => {
                if capability {
                    let _ = self
                        .command(|done| PumpCommand::Reply {
                            call_id,
                            outcome: capabilities::exhausted_result(),
                            done,
                        })
                        .await;
                } else if let Some(bytes) = spillable {
                    let _ = self
                        .command(|done| PumpCommand::SpillReply {
                            call_id,
                            bytes,
                            done,
                        })
                        .await;
                }
            }
            Some(Err(
                AppError::UnknownCall
                | AppError::AlreadySettled
                | AppError::SessionRetired
                | AppError::NotActive,
            )) => {}
            Some(Err(_)) if capability => {
                let _ = self
                    .command(|done| PumpCommand::Reply {
                        call_id,
                        outcome: capabilities::exhausted_result(),
                        done,
                    })
                    .await;
            }
            Some(Err(_)) => {
                let _ = self
                    .command(|done| PumpCommand::Reply {
                        call_id,
                        outcome: refusal(
                            WireErrorKind::Internal,
                            "the registered method result was rejected by the session",
                            false,
                        ),
                        done,
                    })
                    .await;
            }
        }
    }

    async fn run(
        self,
        mut receiver: mpsc::Receiver<DispatchRequest>,
        scope_cancellation: ScopeCancellation,
    ) {
        loop {
            // Take a provider permit before receiving. Work that cannot start
            // remains in the bounded channel where overflow is observable.
            let Ok(permit) = self.providers.clone().acquire_owned().await else {
                break;
            };
            let request = tokio::select! {
                request = receiver.recv() => request,
                _ = scope_cancellation.cancelled() => break,
            };
            let Some(request) = request else {
                break;
            };
            let dispatcher = self.clone();
            tokio::spawn(async move {
                let answer = dispatcher.dispatch(&request).await;
                dispatcher.reply(request.call_id, answer).await;
                drop(permit);
            });
        }
        // Closing the receiver promptly retires queued requests. The pump's
        // activation-close path settles their protocol calls and reaps the
        // worker independently of callbacks already running.
    }

    async fn dispatch(&self, request: &DispatchRequest) -> DispatchAnswer {
        if request.cancellation.is_cancelled() {
            return DispatchAnswer {
                outcome: cancelled(),
                capability: !matches!(&request.work, DispatchWork::Application { .. }),
            };
        }
        let (outcome, capability) = match &request.work {
            DispatchWork::CapabilityAcquire { .. } => {
                (self.acquire_capability(request).await, true)
            }
            DispatchWork::CapabilityInvoke { capability, input } => (
                self.invoke_capability(request, capability.clone(), input.clone())
                    .await,
                true,
            ),
            DispatchWork::Application { method, payload } => {
                (self.application(request, method, payload).await, false)
            }
        };
        DispatchAnswer {
            outcome,
            capability,
        }
    }

    async fn application(
        &self,
        request: &DispatchRequest,
        method_name: &str,
        payload: &serde_json::Value,
    ) -> Outcome {
        let Some(method) = self.methods.get(method_name).cloned() else {
            return refusal(
                WireErrorKind::UnknownMethod,
                "the receiver does not offer the requested method",
                false,
            );
        };
        let view = WorkerMethodRequest::new(payload.clone(), request.cancellation.clone());
        let cancellation = request.cancellation.clone();
        let result = tokio::task::spawn_blocking(move || {
            std::panic::catch_unwind(AssertUnwindSafe(|| method.invoke(&view)))
        })
        .await;
        if cancellation.is_cancelled() {
            return cancelled();
        }
        match result {
            Ok(Ok(Ok(result))) => Outcome::Ok {
                result: result.into_inner(),
            },
            Ok(Ok(Err(failure))) => Outcome::Err {
                error: WireError {
                    kind: match failure.code() {
                        WorkerMethodFailureCode::InvalidInput => WireErrorKind::InvalidFrame,
                        WorkerMethodFailureCode::Failed => WireErrorKind::Internal,
                    },
                    message: failure.message().to_owned(),
                    retryable: false,
                    reconcile_required: false,
                },
            },
            Ok(Err(_)) | Err(_) => refusal(
                WireErrorKind::Internal,
                "the registered method failed unexpectedly",
                false,
            ),
        }
    }
}
