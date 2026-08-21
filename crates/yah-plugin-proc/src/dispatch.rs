//! The bounded worker-to-host application dispatcher.
//!
//! Admitted worker calls — the versioned capability family, and any
//! other method a future application registers — are routed here off the
//! pump task. The pump never runs provider code: a slow or panicking
//! provider can extend a dispatch slot, never IO, ticks, cancellation,
//! endpoint withdrawal, or the kill path.
//!
//! Bounds are structural: the queue is a bounded channel whose overflow
//! answers the worker an observable, retryable refusal before any
//! provider runs; provider concurrency is a semaphore; and every request
//! body is byte-bounded before it occupies a slot. Results ride back
//! through the same bounded command channel the calls came in on, as
//! first-class reply commands — the pump applies them to the session on
//! its own loop, so the session stays single-owner.
//!
//! Refusal mapping is whole-set against the broker and handle gates,
//! with host-authored static messages; worker text never echoes. The
//! one exception follows the Wasm lane's rule: a provider's own refusal
//! text is its caller-facing contract and crosses verbatim, bounded.

use std::collections::HashMap;
use std::panic::AssertUnwindSafe;
use std::sync::{Arc, Mutex};

use tokio::sync::{Semaphore, mpsc, oneshot};
use yah_plugin_host::{
    CapabilityBrokerError, CapabilityDefinition, CapabilityHandle, CapabilityHandleError,
    CapabilityId, PluginStartContext, TextCapability, TextCapabilityFailureCode,
};
use yah_plugin_ipc::session::AppError;
use yah_plugin_ipc::types::{CallId, HandleId, Outcome, WireError, WireErrorKind};

use crate::shared::PumpCommand;

/// One admitted worker call handed to the application lane.
pub(crate) struct DispatchRequest {
    pub call_id: CallId,
    pub method: String,
    pub payload: serde_json::Value,
}

/// The exact activation-local table of host-minted capability handles.
///
/// Wire ids name entries here; they are not bearer authority. Every
/// invoke re-enters the handle's own revocation, admission, and
/// provider gates.
#[derive(Default)]
pub(crate) struct CapabilityTable {
    entries: Mutex<HashMap<HandleId, CapabilityHandle<dyn TextCapability>>>,
}

impl CapabilityTable {
    fn insert(&self, handle: HandleId, capability: CapabilityHandle<dyn TextCapability>) {
        self.entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(handle, capability);
    }

    fn get(&self, handle: HandleId) -> Option<CapabilityHandle<dyn TextCapability>> {
        self.entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&handle)
            .cloned()
    }

    /// Drop one entry: a release (or a reclamation the session named).
    /// The removal is also the double-release guard — a second caller
    /// finds nothing and is refused before anything reaches the session.
    pub(crate) fn remove(&self, handle: HandleId) -> Option<CapabilityHandle<dyn TextCapability>> {
        self.entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&handle)
    }
}

/// A refusal the dispatcher answers with before or instead of any
/// provider running.
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

fn malformed(method: &'static str) -> Outcome {
    refusal(
        WireErrorKind::InvalidFrame,
        match method {
            "capability.acquire" => "capability.acquire expects { capability: string }",
            "capability.invoke" => "capability.invoke expects { handle: number, input: string }",
            _ => "capability.release expects { handle: number }",
        },
        false,
    )
}

/// The dispatcher's shared state, cloned into one task per admitted
/// request. Every field is cheap to clone and activation-scoped.
#[derive(Clone)]
struct Dispatcher {
    table: Arc<CapabilityTable>,
    /// Weak: the task never keeps an ended activation's command channel
    /// alive, so it cannot outlive its activation as authority.
    commands: mpsc::WeakSender<PumpCommand>,
    context: PluginStartContext,
    providers: Arc<Semaphore>,
}

/// Build and spawn the dispatcher for one activation. Returns the bounded
/// queue the pump routes admitted worker calls into.
pub(crate) fn spawn(
    context: PluginStartContext,
    commands: mpsc::WeakSender<PumpCommand>,
    queue_capacity: usize,
    provider_concurrency: usize,
) -> (mpsc::Sender<DispatchRequest>, Arc<CapabilityTable>) {
    let (queue, receiver) = mpsc::channel(queue_capacity.max(1));
    let table = Arc::new(CapabilityTable::default());
    let dispatcher = Dispatcher {
        table: Arc::clone(&table),
        commands,
        context,
        providers: Arc::new(Semaphore::new(provider_concurrency.max(1))),
    };
    tokio::spawn(dispatcher.run(receiver));
    (queue, table)
}

impl Dispatcher {
    /// Upgrade the weak command link, waiting if the channel is at
    /// capacity. Waiting is safe from here: the pump never waits on this
    /// task, so no cycle exists — and an admitted worker call owes a
    /// terminal, so silently dropping one under load would be exactly
    /// the lost-terminal shape the bounded lane exists to prevent.
    async fn commands(&self) -> Option<mpsc::Sender<PumpCommand>> {
        let sender = self.commands.upgrade()?;
        sender.reserve().await.ok()?;
        Some(sender)
    }

    /// Apply one outcome to the session through the pump's command
    /// channel. Fails only when the activation is gone; a worker call
    /// whose session ended needs no answer.
    async fn reply(&self, call_id: CallId, outcome: Outcome) {
        let Some(commands) = self.commands().await else {
            return;
        };
        let (done_sender, done) = oneshot::channel();
        if commands
            .try_send(PumpCommand::Reply {
                call_id,
                outcome,
                done: done_sender,
            })
            .is_err()
        {
            return;
        }
        // Application waits for the pump to have applied the reply; the
        // wire delivery itself stays the session's exactly-once law.
        let _ = done.await;
    }

    /// Mint a wire handle for a freshly acquired capability. The table
    /// insertion happens only after the session mint succeeds, so the
    /// session's live-handle gauge and this table cannot diverge on the
    /// insertion path.
    async fn mint(&self, call_id: CallId) -> Option<Result<HandleId, AppError>> {
        let commands = self.commands().await?;
        let (done_sender, done) = oneshot::channel();
        commands
            .try_send(PumpCommand::MintHandle {
                minted_for: call_id,
                done: done_sender,
            })
            .ok()?;
        done.await.ok()
    }

    async fn release(&self, handle: HandleId) -> Option<Result<(), AppError>> {
        let commands = self.commands().await?;
        let (done_sender, done) = oneshot::channel();
        commands
            .try_send(PumpCommand::RetireWorkerCapability {
                handle,
                done: done_sender,
            })
            .ok()?;
        done.await.ok()
    }

    async fn run(self, mut receiver: mpsc::Receiver<DispatchRequest>) {
        loop {
            // Take a provider slot BEFORE taking a request: a request the
            // concurrency bound cannot start yet stays in the bounded
            // queue, where overflow past the bound is refused on the call
            // itself instead of piling up invisibly inside this task.
            let Ok(permit) = self.providers.clone().acquire_owned().await else {
                break;
            };
            let Some(request) = receiver.recv().await else {
                break;
            };
            let dispatcher = self.clone();
            tokio::spawn(async move {
                let outcome = dispatcher.dispatch(&request).await;
                dispatcher.reply(request.call_id, outcome).await;
                drop(permit);
            });
        }
        // The queue closed — every pump sender is gone, so the
        // activation ended. The task ends with it; no provider state
        // outlives the activation as authority.
    }

    async fn dispatch(&self, request: &DispatchRequest) -> Outcome {
        match request.method.as_str() {
            "capability.acquire" => self.acquire(request).await,
            "capability.invoke" => self.invoke(request).await,
            "capability.release" => self.release_request(request).await,
            // artifact.read is served by the session itself from
            // host-held spill bytes; reaching the dispatcher under that
            // name would mean the interception changed. Answer the
            // closed family's refusal rather than guess.
            "artifact.read" => refusal(
                WireErrorKind::UnknownMethod,
                "artifact.read is served by the host session",
                false,
            ),
            other => refusal(
                WireErrorKind::UnknownMethod,
                match other {
                    // The method name is worker text and must not echo;
                    // the refusal names the family, never the ask.
                    "capability.acquire" | "capability.invoke" | "capability.release" => {
                        unreachable!("the capability family is matched above")
                    }
                    _ => "the receiver does not offer the requested method",
                },
                false,
            ),
        }
    }

    async fn acquire(&self, request: &DispatchRequest) -> Outcome {
        #[derive(serde::Deserialize)]
        struct Acquire {
            capability: String,
        }
        let Ok(acquire) = serde_json::from_value::<Acquire>(request.payload.clone()) else {
            return malformed("capability.acquire");
        };
        // The raw id is worker text and must not echo: the same
        // reflection path the Wasm lane closes stays closed here.
        let Ok(id) = CapabilityId::new(acquire.capability) else {
            return refusal(
                WireErrorKind::InvalidFrame,
                "the requested capability id is not well-formed",
                false,
            );
        };
        if !self.context.is_granted(&id) {
            return refusal(
                WireErrorKind::UnknownHandle,
                "the requested capability is not granted to this activation",
                false,
            );
        }
        let definition = CapabilityDefinition::<dyn TextCapability>::new(id);
        let handle = match self.context.handle(&definition) {
            Ok(handle) => handle,
            Err(error) => return acquire_refusal(error),
        };
        let Some(minted) = self.mint(request.call_id).await else {
            return refusal(
                WireErrorKind::ResourceExhausted,
                "the activation ended before the capability handle was minted",
                true,
            );
        };
        // Whole-set against the session's mint refusals, same discipline
        // as the broker mapping: each kind names its own recovery.
        let wire_handle = match minted {
            Ok(handle) => handle,
            Err(AppError::HandleCeiling) => {
                return refusal(
                    WireErrorKind::ResourceExhausted,
                    "the activation's live-handle ceiling is exhausted",
                    false,
                );
            }
            Err(AppError::SessionRetired) => {
                return refusal(
                    WireErrorKind::ResourceExhausted,
                    "the activation's correlation budget is spent; retire it",
                    false,
                );
            }
            // The call or the session ended under us; the terminal this
            // refusal answers is about to be moot either way.
            Err(_) => {
                return refusal(
                    WireErrorKind::ResourceExhausted,
                    "the activation ended before the capability handle was minted",
                    true,
                );
            }
        };
        self.table.insert(wire_handle, handle);
        Outcome::Ok {
            result: serde_json::json!({ "handle": wire_handle }),
        }
    }

    async fn invoke(&self, request: &DispatchRequest) -> Outcome {
        #[derive(serde::Deserialize)]
        struct Invoke {
            handle: HandleId,
            input: String,
        }
        let Ok(invoke) = serde_json::from_value::<Invoke>(request.payload.clone()) else {
            return malformed("capability.invoke");
        };
        // Lookup by clone, never consumption: a handle stays invocable
        // until its release — the Wasm lane's repeated-invoke semantics.
        // A forged id, a foreign id, and a released id all land in the
        // same bounded refusal, and every invoke still re-enters the
        // handle's own revocation and admission gates below.
        let Some(capability) = self.table.get(invoke.handle) else {
            return refusal(
                WireErrorKind::UnknownHandle,
                "no such capability handle is held by this activation",
                false,
            );
        };
        // Off-pump by construction: this runs on the dispatcher task
        // inside a bounded permit. The synchronous provider may run
        // long; cancelling the worker call cannot interrupt it — the
        // terminal races settle exactly once in the session, and a late
        // reply after a local settle is tolerated there.
        match tokio::task::spawn_blocking(move || {
            std::panic::catch_unwind(AssertUnwindSafe(|| {
                capability.try_with(|provider| provider.invoke(&invoke.input))
            }))
        })
        .await
        {
            Ok(Ok(Ok(Ok(text)))) => Outcome::Ok {
                result: serde_json::Value::String(text),
            },
            Ok(Ok(Ok(Err(failure)))) => Outcome::Err {
                error: WireError {
                    kind: match failure.code {
                        TextCapabilityFailureCode::InvalidInput => WireErrorKind::InvalidFrame,
                        TextCapabilityFailureCode::Failed => WireErrorKind::Internal,
                    },
                    message: {
                        let mut bounded = failure.message;
                        bounded.truncate(yah_plugin_ipc::MAX_ERROR_DETAIL_CHARS);
                        bounded
                    },
                    retryable: false,
                    reconcile_required: false,
                },
            },
            Ok(Ok(Err(CapabilityHandleError::Revoked { .. }))) => refusal(
                WireErrorKind::UnknownHandle,
                "the capability handle has been revoked",
                false,
            ),
            Ok(Ok(Err(CapabilityHandleError::AdmissionExhausted { .. }))) => refusal(
                WireErrorKind::ResourceExhausted,
                "the capability provider is at its admission bound",
                true,
            ),
            // The handle gate's whole error set is matched above, so the
            // residue here is exactly the panic path — contained: the
            // host authors the failure, and no type, path, or backtrace
            // crosses.
            Ok(Err(_)) | Err(_) => refusal(
                WireErrorKind::Internal,
                "the capability provider failed unexpectedly",
                false,
            ),
        }
    }

    async fn release_request(&self, request: &DispatchRequest) -> Outcome {
        #[derive(serde::Deserialize)]
        struct Release {
            handle: HandleId,
        }
        let Ok(release) = serde_json::from_value::<Release>(request.payload.clone()) else {
            return malformed("capability.release");
        };
        // Exact removal first: a double release is a fault, and an
        // unknown id is indistinguishable from a forged one.
        if self.table.remove(release.handle).is_none() {
            return refusal(
                WireErrorKind::UnknownHandle,
                "no such capability handle is held by this activation",
                false,
            );
        }
        match self.release(release.handle).await {
            Some(Ok(())) => Outcome::Ok {
                result: serde_json::json!({ "released": release.handle }),
            },
            // Release crossing a reclaiming terminal: the session has
            // already reclaimed the id, which is the outcome the
            // release wanted. Tolerated once there, acked here.
            Some(Err(AppError::AlreadyReleased)) => Outcome::Ok {
                result: serde_json::json!({ "released": release.handle }),
            },
            Some(Err(_)) => refusal(
                WireErrorKind::UnknownHandle,
                "the release could not be acknowledged",
                false,
            ),
            None => refusal(
                WireErrorKind::ResourceExhausted,
                "the activation ended before the release was acknowledged",
                true,
            ),
        }
    }
}

/// Map a broker refusal onto the bounded refusal surface, whole-set —
/// the same discipline, and the same no-echo rule, as the Wasm lane.
fn acquire_refusal(error: CapabilityBrokerError) -> Outcome {
    use CapabilityBrokerError as E;
    match error {
        E::NotGranted { .. } => refusal(
            WireErrorKind::UnknownHandle,
            "the requested capability is not granted to this activation",
            false,
        ),
        E::ActivationInactive { .. } => refusal(
            WireErrorKind::ResourceExhausted,
            "the activation is closing; capabilities are revoked",
            false,
        ),
        E::ProviderUnavailable { .. } => refusal(
            WireErrorKind::UnknownHandle,
            "the capability's granted provider is withdrawn or replaced",
            false,
        ),
        E::ContractTypeMismatch { .. } => refusal(
            WireErrorKind::UnknownHandle,
            "the capability is not granted under the portable text contract",
            false,
        ),
        E::DuplicateProvider { .. } | E::ForeignRegistration { .. } => refusal(
            WireErrorKind::Internal,
            "the capability registration is inconsistent",
            false,
        ),
        E::BrokerIncarnationExhausted | E::RegistrationIdExhausted => refusal(
            WireErrorKind::ResourceExhausted,
            "the capability broker's identity space is exhausted",
            false,
        ),
    }
}
