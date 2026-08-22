//! The bounded worker-to-host application dispatcher.
//!
//! Admitted worker calls route through an immutable activation-local table
//! here, off the pump task. The table contains protocol-owned capability
//! methods and the application's pre-registered methods; no activation can
//! mutate it after start. The pump never runs provider code: a slow or
//! panicking provider can extend a dispatch slot, never IO, ticks, endpoint
//! withdrawal, or the kill path.
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

use std::collections::{BTreeMap, HashMap};
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

mod methods;

pub use methods::{
    WorkerMethod, WorkerMethodFailure, WorkerMethodFailureCode, WorkerMethodRegistrationError,
    WorkerMethodRegistry, WorkerMethodRequest, WorkerMethodResult, WorkerMethodResultError,
};

/// The capability shape the dispatcher's table holds, named so the pump
/// can carry one through a mint command without reaching into this
/// module's generics.
pub(crate) type DispatchedCapability = CapabilityHandle<dyn TextCapability>;

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
    pub(crate) fn insert(
        &self,
        handle: HandleId,
        capability: CapabilityHandle<dyn TextCapability>,
    ) {
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
    routes: Arc<BTreeMap<String, DispatchRoute>>,
    /// Weak: the task never keeps an ended activation's command channel
    /// alive, so it cannot outlive its activation as authority.
    commands: mpsc::WeakSender<PumpCommand>,
    context: PluginStartContext,
    providers: Arc<Semaphore>,
}

#[derive(Clone)]
enum DispatchRoute {
    Capability(CapabilityMethod),
    Application(Arc<dyn WorkerMethod>),
    ArtifactRead,
}

#[derive(Clone, Copy)]
enum CapabilityMethod {
    Acquire,
    Invoke,
    Release,
}

/// Build and spawn the dispatcher for one activation. Returns the bounded
/// queue the pump routes admitted worker calls into.
pub(crate) fn spawn(
    context: PluginStartContext,
    commands: mpsc::WeakSender<PumpCommand>,
    queue_capacity: usize,
    provider_concurrency: usize,
    methods: WorkerMethodRegistry,
) -> (mpsc::Sender<DispatchRequest>, Arc<CapabilityTable>) {
    let (queue, receiver) = mpsc::channel(queue_capacity.max(1));
    let table = Arc::new(CapabilityTable::default());
    let dispatcher = Dispatcher {
        table: Arc::clone(&table),
        routes: Arc::new(routes(methods)),
        commands,
        context,
        providers: Arc::new(Semaphore::new(provider_concurrency.max(1))),
    };
    tokio::spawn(dispatcher.run(receiver));
    (queue, table)
}

fn routes(methods: WorkerMethodRegistry) -> BTreeMap<String, DispatchRoute> {
    let mut routes = BTreeMap::from([
        (
            "capability.acquire".to_owned(),
            DispatchRoute::Capability(CapabilityMethod::Acquire),
        ),
        (
            "capability.invoke".to_owned(),
            DispatchRoute::Capability(CapabilityMethod::Invoke),
        ),
        (
            "capability.release".to_owned(),
            DispatchRoute::Capability(CapabilityMethod::Release),
        ),
        ("artifact.read".to_owned(), DispatchRoute::ArtifactRead),
    ]);
    routes.extend(
        methods
            .into_methods()
            .into_iter()
            .map(|(name, method)| (name, DispatchRoute::Application(method))),
    );
    routes
}

impl Dispatcher {
    /// Upgrade the weak command link, waiting if the channel is at
    /// capacity. Waiting is safe from here: the pump never waits on this
    /// task, so no cycle exists — and an admitted worker call owes a
    /// terminal, so silently dropping one under load would be exactly
    /// the lost-terminal shape the bounded lane exists to prevent.
    async fn commands<T>(
        &self,
        make: impl FnOnce(oneshot::Sender<Result<T, AppError>>) -> PumpCommand,
    ) -> Option<Result<T, AppError>> {
        let sender = self.commands.upgrade()?;
        // Reserve an owned permit and send through it: the slot is held
        // from reservation to delivery, so no competing sender can slip
        // in and strand a terminal behind a Full rejection.
        let permit = sender.reserve_owned().await.ok()?;
        let (done_sender, done) = oneshot::channel();
        permit.send(make(done_sender));
        done.await.ok()
    }

    /// Apply one outcome to the session through the pump's command
    /// channel. Fails only when the activation is gone; a worker call
    /// whose session ended needs no answer.
    async fn reply(&self, call_id: CallId, outcome: Outcome) {
        // Refusals are static text, so only an Ok result can ever trip
        // the inline bound; keep a copy just in case it does.
        let spillable = match &outcome {
            Outcome::Ok { result } => serde_json::to_vec(result)
                .ok()
                .filter(|bytes| bytes.len() > yah_plugin_ipc::MAX_INLINE_RESULT_BYTES),
            _ => None,
        };
        let applied = self
            .commands(|done| PumpCommand::Reply {
                call_id,
                outcome,
                done,
            })
            .await;
        match applied {
            // Applied, or the activation is gone and no terminal is
            // possible for anyone.
            Some(Ok(())) | None => {}
            Some(Err(AppError::SpillRequired { .. })) => {
                // The inline result was over the bound. Spill it exactly
                // as the protocol demands — the session mints the offer
                // and pins the bytes host-side, and the call still ends
                // with its one terminal.
                if let Some(bytes) = spillable {
                    let _ = self
                        .commands(|done| PumpCommand::SpillReply {
                            call_id,
                            bytes,
                            done,
                        })
                        .await;
                }
            }
            // The call ended before the answer landed: an already-ended
            // race the session's exactly-once law tolerates by design.
            Some(Err(AppError::UnknownCall | AppError::AlreadySettled)) => {}
            // The activation is ending or its budget is spent; the call's
            // terminal is already being settled by the close path.
            Some(Err(AppError::SessionRetired | AppError::NotActive)) => {}
            // The session rejected the result's contents (an unsafe
            // integer, most likely). The call still owes its terminal, so
            // answer a bounded internal error — static text, always
            // admissible — rather than strand the worker until deadline.
            Some(Err(_)) => {
                let _ = self
                    .commands(|done| PumpCommand::Reply {
                        call_id,
                        outcome: Outcome::Err {
                            error: WireError {
                                kind: WireErrorKind::Internal,
                                message: "the provider result was rejected by the session"
                                    .to_owned(),
                                retryable: false,
                                reconcile_required: false,
                            },
                        },
                        done,
                    })
                    .await;
            }
        }
    }

    /// Mint a wire handle for a freshly acquired capability. The table
    /// insertion happens only after the session mint succeeds, so the
    /// session's live-handle gauge and this table cannot diverge on the
    /// insertion path.
    async fn mint(
        &self,
        call_id: CallId,
        capability: CapabilityHandle<dyn TextCapability>,
    ) -> Option<Result<HandleId, AppError>> {
        self.commands(
            |done: oneshot::Sender<Result<HandleId, AppError>>| PumpCommand::MintHandle {
                minted_for: call_id,
                capability,
                done,
            },
        )
        .await
    }

    async fn release(&self, handle: HandleId) -> Option<Result<(), AppError>> {
        self.commands(|done: oneshot::Sender<Result<(), AppError>>| {
            PumpCommand::RetireWorkerCapability { handle, done }
        })
        .await
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
        let Some(route) = self.routes.get(&request.method).cloned() else {
            // The method name is worker text and must not echo; the
            // refusal names the receiver's closed family, never the ask.
            return refusal(
                WireErrorKind::UnknownMethod,
                "the receiver does not offer the requested method",
                false,
            );
        };
        match route {
            DispatchRoute::Capability(CapabilityMethod::Acquire) => self.acquire(request).await,
            DispatchRoute::Capability(CapabilityMethod::Invoke) => self.invoke(request).await,
            DispatchRoute::Capability(CapabilityMethod::Release) => {
                self.release_request(request).await
            }
            // artifact.read is served by the session itself from host-held
            // spill bytes. The registration keeps the reserved method's
            // refusal stable if interception ever changes.
            DispatchRoute::ArtifactRead => refusal(
                WireErrorKind::UnknownMethod,
                "artifact.read is served by the host session",
                false,
            ),
            DispatchRoute::Application(method) => self.application(request, method).await,
        }
    }

    async fn application(
        &self,
        request: &DispatchRequest,
        method: Arc<dyn WorkerMethod>,
    ) -> Outcome {
        let request = WorkerMethodRequest::new(request.payload.clone());
        match tokio::task::spawn_blocking(move || {
            std::panic::catch_unwind(AssertUnwindSafe(|| method.invoke(&request)))
        })
        .await
        {
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

    async fn acquire(&self, request: &DispatchRequest) -> Outcome {
        // Exact members only: an undeclared field is a worker bug, not
        // something to accept silently and guess about.
        #[derive(serde::Deserialize)]
        #[serde(deny_unknown_fields)]
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
        // The capability rides the mint command: the pump inserts the
        // table entry in the same task that processes reclamation
        // events, so a handle reclaimed between mint and insert can
        // never leave a ghost behind.
        let Some(minted) = self.mint(request.call_id, handle).await else {
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
        Outcome::Ok {
            result: serde_json::json!({ "handle": wire_handle }),
        }
    }

    async fn invoke(&self, request: &DispatchRequest) -> Outcome {
        #[derive(serde::Deserialize)]
        #[serde(deny_unknown_fields)]
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
                    message: bound_chars(failure.message),
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
        #[serde(deny_unknown_fields)]
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

/// Bound provider-authored text to the wire's detail budget. The limit
/// is counted in Unicode scalar values — the same unit the session
/// clips worker-authored detail in — and the collect can never split a
/// character, so no byte-offset truncation (which panics off a boundary)
/// is ever needed.
fn bound_chars(text: String) -> String {
    text.chars()
        .take(yah_plugin_ipc::MAX_ERROR_DETAIL_CHARS)
        .collect()
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
