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
use yah_plugin_ipc::session::AppError;
use yah_plugin_ipc::types::{CallId, CancelReason, Outcome, WireError, WireErrorKind};

use crate::shared::PumpCommand;

mod methods;

pub use methods::{
    WorkerMethod, WorkerMethodCancellation, WorkerMethodFailure, WorkerMethodFailureCode,
    WorkerMethodRegistrationError, WorkerMethodRegistry, WorkerMethodRequest, WorkerMethodResult,
    WorkerMethodResultError,
};

/// One session-admitted worker call handed to the application lane.
pub(crate) struct DispatchRequest {
    pub call_id: CallId,
    pub method: String,
    pub payload: serde_json::Value,
    pub cancellation: WorkerMethodCancellation,
}

fn refusal(kind: WireErrorKind, message: &'static str, retryable: bool) -> Outcome {
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
    providers: Arc<Semaphore>,
}

pub(crate) fn spawn(
    scope_cancellation: ScopeCancellation,
    commands: mpsc::WeakSender<PumpCommand>,
    queue_capacity: usize,
    provider_concurrency: usize,
    methods: WorkerMethodRegistry,
) -> mpsc::Sender<DispatchRequest> {
    let (queue, receiver) = mpsc::channel(queue_capacity.max(1));
    let dispatcher = Dispatcher {
        methods: Arc::new(methods.into_methods()),
        commands,
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

    async fn reply(&self, call_id: CallId, outcome: Outcome) {
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
                if let Some(bytes) = spillable {
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
                let outcome = dispatcher.dispatch(&request).await;
                dispatcher.reply(request.call_id, outcome).await;
                drop(permit);
            });
        }
        // Closing the receiver promptly retires queued requests. The pump's
        // activation-close path settles their protocol calls and reaps the
        // worker independently of callbacks already running.
    }

    async fn dispatch(&self, request: &DispatchRequest) -> Outcome {
        if request.cancellation.is_cancelled() {
            return cancelled();
        }
        let Some(method) = self.methods.get(&request.method).cloned() else {
            // The method is worker-authored text and never crosses back.
            return refusal(
                WireErrorKind::UnknownMethod,
                "the receiver does not offer the requested method",
                false,
            );
        };
        let view = WorkerMethodRequest::new(request.payload.clone(), request.cancellation.clone());
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
