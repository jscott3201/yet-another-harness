//! The route from a guest to a granted capability, carried as a resource.
//!
//! The `capabilities` import is present in every activation; the authority is
//! not the import but the handle `acquire` returns. Each resource-table entry
//! wraps one [`CapabilityHandle`] resolved through the activation's admitted
//! [`PluginStartContext`], so every guest call re-enters the same gates a
//! trusted in-process consumer would: the exact registration's revocation
//! gate, the activation's pre-cleanup activity fence, and its cancellation.
//! The table index the guest holds is store-local bookkeeping, never bearer
//! authority - a forged index traps inside Wasmtime before the host sees it,
//! and the handle inside the entry refuses on its own once revoked.
//!
//! Every failure is an ordinary WIT error result, never a trap: the spec's
//! rule for optional grants is a profile the guest can observe and refuse
//! politely, not a stub that kills the activation for asking.
//!
//! Two ceilings meet here. The host-call byte budget already bounds what one
//! `acquire` or `invoke` lifts out of the guest; [`WasmLimits`]'s handle
//! ceiling bounds how many live entries one activation may hold, because the
//! table grows on the host heap where neither the memory ceiling nor the
//! store limiter can see it.
//!
//! [`WasmLimits`]: crate::limits::WasmLimits

use wasmtime::component::Resource;
use yah_plugin_host::{
    CapabilityBrokerError, CapabilityDefinition, CapabilityHandle, CapabilityHandleError,
    CapabilityId, TextCapability, TextCapabilityFailure, TextCapabilityFailureCode,
};

use crate::{
    bindings::yah::plugin::capabilities::{
        AcquireError, AcquireErrorCode, CallError, CallErrorCode, Host, HostCapability,
    },
    host::HostState,
};

/// One live guest-held grant: the WIT `capability` resource's host half.
///
/// Wasmtime never runs a host destructor for entries still live when the
/// store drops - it drops the store data, which drops the table, which drops
/// each entry. Anything that must happen on release therefore happens in this
/// type's `Drop`, which both release paths share; `HostCapability::drop` runs
/// only when the guest itself calls `resource.drop`.
pub struct GrantedCapability {
    handle: CapabilityHandle<dyn TextCapability>,
    /// Held for its `Drop` alone: releasing the entry is what counts it down.
    _released: LiveHandleGuard,
}

/// Decrements the activation's live-handle count however the entry dies.
struct LiveHandleGuard(std::sync::Arc<std::sync::atomic::AtomicUsize>);

impl Drop for LiveHandleGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, std::sync::atomic::Ordering::AcqRel);
    }
}

impl Host for HostState {
    fn acquire(
        &mut self,
        capability_id: String,
    ) -> Result<Resource<GrantedCapability>, AcquireError> {
        self.observer().count_capability_acquire_attempt();
        let admitted = self.acquire_inner(capability_id);
        if admitted.is_err() {
            self.observer().count_capability_acquire_refusal();
        }
        admitted
    }
}

impl HostState {
    fn acquire_inner(
        &mut self,
        capability_id: String,
    ) -> Result<Resource<GrantedCapability>, AcquireError> {
        // The raw ID is guest text and is not echoed back: the identity
        // error names the violated rule, which is the whole finding.
        let id = CapabilityId::new(capability_id).map_err(|error| AcquireError {
            code: AcquireErrorCode::InvalidId,
            message: error.to_string(),
        })?;
        let Some(context) = self.capability_context() else {
            // Reachable only outside the host lifecycle: a store built without
            // a start permit has no grants, and "no context" must read as the
            // absence of a grant rather than as a distinct, softer state.
            return Err(AcquireError {
                code: AcquireErrorCode::NotGranted,
                message: format!("capability {id} was not granted: no admitted start context"),
            });
        };
        let handle = context
            .handle(&CapabilityDefinition::<dyn TextCapability>::new(id))
            .map_err(acquire_refusal)?;
        let live = self.observer().live_capability_handles_counter();
        if live.load(std::sync::atomic::Ordering::Acquire) >= self.limits().max_capability_handles {
            return Err(AcquireError {
                code: AcquireErrorCode::HandleLimit,
                message: format!(
                    "activation already holds {} live capability handles",
                    self.limits().max_capability_handles
                ),
            });
        }
        live.fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        let entry = GrantedCapability {
            handle,
            _released: LiveHandleGuard(live),
        };
        // `push` fails only when the table itself is full; the ceiling above
        // is the real bound and is far below the table's own capacity.
        self.capability_table()
            .push(entry)
            .map_err(|error| AcquireError {
                code: AcquireErrorCode::HandleLimit,
                message: error.to_string(),
            })
    }
}

/// Map a broker refusal onto the `acquire` error surface, whole-set.
fn acquire_refusal(error: CapabilityBrokerError) -> AcquireError {
    let message = error.to_string();
    let code = match error {
        CapabilityBrokerError::NotGranted { .. } => AcquireErrorCode::NotGranted,
        // The stale-activation fence: the context outlived its activation's
        // scope, so every authority it could name is gone at once.
        CapabilityBrokerError::ActivationInactive { .. } => AcquireErrorCode::Revoked,
        // Withdrawn, dropped, or replaced: the immutable snapshot still names
        // the old exact registration and never follows a replacement, so a
        // fresh acquire after replacement lands here, not on `revoked`.
        CapabilityBrokerError::ProviderUnavailable { .. } => AcquireErrorCode::Unavailable,
        // Granted, but not under the portable text contract this ABI carries.
        CapabilityBrokerError::ContractTypeMismatch { .. } => AcquireErrorCode::Mismatched,
        // None of these four can escape `PluginStartContext::handle`: the two
        // exhaustions arise only in `CapabilityBroker::new`/`register`,
        // `DuplicateProvider` only in registration, and `ForeignRegistration`
        // only in grant validation at host `prepare`, before any start permit
        // exists. Mapped fail-closed rather than panicking, and left explicit
        // so a new variant is a compile error here instead of a silent arm.
        CapabilityBrokerError::BrokerIncarnationExhausted
        | CapabilityBrokerError::RegistrationIdExhausted
        | CapabilityBrokerError::DuplicateProvider { .. }
        | CapabilityBrokerError::ForeignRegistration { .. } => AcquireErrorCode::Unavailable,
    };
    AcquireError { code, message }
}

impl HostCapability for HostState {
    fn invoke(
        &mut self,
        self_: Resource<GrantedCapability>,
        input: String,
    ) -> Result<String, CallError> {
        self.observer().count_capability_call_attempt();
        let answered = self.invoke_inner(&self_, &input);
        if answered.is_err() {
            self.observer().count_capability_call_refusal();
        }
        answered
    }

    fn drop(&mut self, rep: Resource<GrantedCapability>) -> wasmtime::Result<()> {
        // An `Err` here traps the guest and faults the activation, so a
        // missing or mistyped entry is swallowed: there is nothing to release,
        // and a guest must not die for a host bookkeeping gap. The entry's own
        // `Drop` does the release work on success.
        if let Ok(entry) = self.capability_table().delete(rep) {
            drop(entry);
        }
        Ok(())
    }
}

impl HostState {
    fn invoke_inner(
        &mut self,
        handle: &Resource<GrantedCapability>,
        input: &str,
    ) -> Result<String, CallError> {
        // Unreachable in practice: Wasmtime traps a handle index the guest was
        // never given before the host is called. Kept as an error rather than
        // a panic so a host-side bookkeeping bug degrades to a refusal.
        let entry = self
            .capability_table_ref()
            .get(handle)
            .map_err(|error| CallError {
                code: CallErrorCode::Failed,
                message: format!("capability entry is not present: {error}"),
            })?;
        let answered = entry
            .handle
            .try_with(|provider| provider.invoke(input))
            .map_err(call_refusal)?;
        answered.map_err(provider_failure)
    }
}

/// Map a handle refusal onto the `invoke` error surface, whole-set.
fn call_refusal(error: CapabilityHandleError) -> CallError {
    let message = error.to_string();
    let code = match error {
        // Provider withdrawn or replaced, activation closing, or cancellation:
        // the handle folds them into one fail-closed refusal and so does the
        // ABI.
        CapabilityHandleError::Revoked { .. } => CallErrorCode::Revoked,
        // Requires the activity scope's admission space to run out, which has
        // never been observed; mapped so the enum stays whole-set.
        CapabilityHandleError::AdmissionExhausted { .. } => CallErrorCode::Exhausted,
    };
    CallError { code, message }
}

/// Map a provider's own refusal onto the `invoke` error surface.
fn provider_failure(failure: TextCapabilityFailure) -> CallError {
    let code = match failure.code {
        TextCapabilityFailureCode::InvalidInput => CallErrorCode::InvalidInput,
        TextCapabilityFailureCode::Failed => CallErrorCode::Failed,
    };
    CallError {
        code,
        message: failure.message,
    }
}
