//! §2.2 step 3: command validation as a pure function of the pre-read
//! state — every authorization class, fence axis, and rejection-
//! persistence decision lives here; the mutation phase in `submit`
//! executes the returned plan without further reads.

use super::*;

impl Funnel {
    /// §2.2 step 3 as a pure function of the pre-read state.
    pub(super) fn validate(&self, cmd: &Command, pre: &PreRead) -> Result<Accepted, Rejection> {
        let current_epoch = self.store.authority_epoch();
        // One implementation of the §2.1 authority class, shared with the
        // §4 settle/park arms — two copies of an authorization check are
        // two chances to drift apart.
        let authority_ok = || -> Result<(), Rejection> { self.authority_gate(cmd) };
        // I2: a mutation of existing unit state REQUIRES the expectation;
        // a stale one is version_conflict (transient, never persisted).
        let expected_version_ok = |actual: u64| -> Result<(), Rejection> {
            match cmd.expected_version {
                Some(expected) if expected != actual => Err((
                    ErrorKind::VersionConflict,
                    format!("expected version {expected}, actual {actual}"),
                    false,
                )),
                Some(_) => Ok(()),
                None => Err((
                    ErrorKind::InvalidRequest,
                    "mutating method requires expected_version".into(),
                    true,
                )),
            }
        };
        // §5.2 rule 4: a committed ancestor cancellation bars the admission
        // of a new child. The blockers are precomputed in the same
        // transaction (cancel_pre_read); a non-empty set is a shape
        // rejection — the committed cancellation cannot be un-committed, so
        // the retry gets the same answer (persist=true).
        let admission_gate = || -> Result<(), Rejection> {
            let blockers: &[super::cancel_rules::Blocker] = pre
                .cancel
                .as_ref()
                .map(|c| c.admission_blockers.as_slice())
                .unwrap_or(&[]);
            if blockers.is_empty() {
                return Ok(());
            }
            let named = blockers
                .iter()
                .map(|b| format!("{} ({})", b.member, b.reason))
                .collect::<Vec<_>>()
                .join("; ");
            Err((
                ErrorKind::InvalidRequest,
                format!("admission barred by committed cancellation: {named}"),
                true,
            ))
        };
        match &cmd.method {
            Method::RunOpen {
                run_id,
                goal_work_item_id,
            } => {
                authority_ok()?;
                if pre.run.is_some() {
                    return Err((
                        ErrorKind::InvalidRequest,
                        format!("run {run_id} already exists"),
                        true,
                    ));
                }
                Ok(Accepted {
                    plan: Plan::CreateRun {
                        run_id: run_id.clone(),
                        goal_work_item_id: goal_work_item_id.clone(),
                    },
                    events: vec![EventDraft {
                        aggregate_kind: "run",
                        aggregate_id: run_id.clone(),
                        aggregate_version: 1,
                        event_kind: "run.opened",
                        payload: json!({ "goal_work_item_id": goal_work_item_id }),
                    }],
                    result: json!({ "run_id": run_id, "version": 1 }),
                })
            }
            Method::RunClose { run_id, outcome } => {
                authority_ok()?;
                let Some(run) = &pre.run else {
                    return Err((ErrorKind::NotFound, format!("run {run_id}"), false));
                };
                expected_version_ok(run.version)?;
                if run.status != "open" && run.status != "active" {
                    return Err((
                        ErrorKind::InvalidRequest,
                        format!("run {run_id} is already {}", run.status),
                        true,
                    ));
                }
                // I11 (§5.2 rule 7). Only a success close is barred: failure
                // and cancelled are honest terminals for a run with
                // unresolved members, and barring them too would leave a run
                // whose effects never resolve with no lawful close at all.
                if !pre.run_blockers.is_empty() {
                    let named = pre
                        .run_blockers
                        .iter()
                        .map(|b| format!("{} ({})", b.member, b.reason))
                        .collect::<Vec<_>>()
                        .join("; ");
                    return Err((
                        ErrorKind::InvalidRequest,
                        format!(
                            "run {run_id} cannot close success while members are unresolved: {named}"
                        ),
                        true,
                    ));
                }
                let new_version = increment(run.version, "run version")?;
                Ok(Accepted {
                    plan: Plan::CloseRun {
                        run_node: run.node,
                        new_version,
                        status: outcome.wire(),
                    },
                    events: vec![EventDraft {
                        aggregate_kind: "run",
                        aggregate_id: run_id.clone(),
                        aggregate_version: new_version,
                        event_kind: "run.closed",
                        payload: json!({ "outcome": outcome.wire() }),
                    }],
                    result: json!({
                        "run_id": run_id,
                        "version": new_version,
                        "status": outcome.wire(),
                    }),
                })
            }
            Method::WorkItemCreate {
                work_item_id,
                acceptance_contract_digest,
                declared_write_scope,
            } => {
                authority_ok()?;
                if self.store.work_item_node(work_item_id).is_some() {
                    return Err((
                        ErrorKind::InvalidRequest,
                        format!("work item {work_item_id} already exists"),
                        true,
                    ));
                }
                Ok(Accepted {
                    plan: Plan::CreateWorkItem {
                        work_item_id: work_item_id.clone(),
                        acceptance_contract_digest: acceptance_contract_digest.as_str().to_owned(),
                        declared_write_scope: serde_json::to_string(declared_write_scope)
                            .expect("scope serializes"),
                    },
                    events: vec![EventDraft {
                        aggregate_kind: "work_item",
                        aggregate_id: work_item_id.clone(),
                        aggregate_version: 1,
                        event_kind: "work_item.created",
                        payload: json!({
                            "acceptance_contract_digest": acceptance_contract_digest.as_str(),
                            "declared_write_scope": declared_write_scope,
                        }),
                    }],
                    result: json!({ "work_item_id": work_item_id, "version": 1 }),
                })
            }
            Method::UnitAdmit {
                unit_id,
                work_item_id,
                run_id,
            } => {
                authority_ok()?;
                if self.store.run_node(run_id).is_none() {
                    // Same not-persisted shape as the work-item check: the
                    // run may be opened later and a byte-identical retry
                    // must then succeed.
                    return Err((ErrorKind::NotFound, format!("run {run_id}"), false));
                }
                if self.store.work_item_node(work_item_id).is_none() {
                    // Not persisted: the work item may be created later,
                    // and a byte-identical retry must then succeed.
                    return Err((
                        ErrorKind::NotFound,
                        format!("work item {work_item_id}"),
                        false,
                    ));
                }
                if pre.unit.is_some() {
                    return Err((
                        ErrorKind::InvalidRequest,
                        format!("unit {unit_id} already exists"),
                        true,
                    ));
                }
                admission_gate()?;
                Ok(Accepted {
                    plan: Plan::CreateUnit {
                        unit_id: unit_id.clone(),
                        work_item_id: work_item_id.clone(),
                        run_id: run_id.clone(),
                    },
                    events: vec![EventDraft {
                        aggregate_kind: "unit",
                        aggregate_id: unit_id.clone(),
                        aggregate_version: 1,
                        event_kind: "unit.admitted",
                        payload: json!({ "work_item_id": work_item_id, "run_id": run_id }),
                    }],
                    result: json!({ "unit_id": unit_id, "version": 1 }),
                })
            }
            Method::UnitDispatch { unit_id, holder_id } => {
                authority_ok()?;
                let Some(unit) = &pre.unit else {
                    return Err((ErrorKind::NotFound, format!("unit {unit_id}"), false));
                };
                expected_version_ok(unit.version)?;
                admission_gate()?;
                let prior_attempt = if unit.epoch > 0 {
                    let key = format!("{unit_id}/{}", unit.epoch);
                    match self.store.attempt_node(&key) {
                        Some(n) => Some(n),
                        None => {
                            return Err((
                                ErrorKind::Internal,
                                format!("attempt row {key} missing for current epoch"),
                                false,
                            ));
                        }
                    }
                } else {
                    None
                };
                let new_epoch = increment(unit.epoch, "attempt epoch")?;
                let new_version = increment(unit.version, "unit version")?;
                let attempt_id = self.mint_id().to_string();
                let token_nonce = self.mint_id().to_string();
                Ok(Accepted {
                    plan: Plan::Dispatch {
                        unit_node: unit.node,
                        unit_id: unit_id.clone(),
                        new_version,
                        new_epoch,
                        stamp: unit.stamp,
                        authority_epoch: current_epoch.0,
                        holder_id: holder_id.clone(),
                        attempt_id: attempt_id.clone(),
                        token_nonce: token_nonce.clone(),
                        existing_lease: pre.lease.as_ref().map(|l| (l.node, l.version)),
                        prior_attempt,
                    },
                    events: vec![EventDraft {
                        aggregate_kind: "unit",
                        aggregate_id: unit_id.clone(),
                        aggregate_version: new_version,
                        event_kind: "unit.dispatched",
                        payload: json!({
                            "attempt_epoch": new_epoch,
                            "attempt_id": attempt_id,
                            "holder_id": holder_id,
                        }),
                    }],
                    // `attempt_id` rides the result because §5.1 roots a
                    // CancelRequest at an id, not at the (unit_id, epoch)
                    // composite the store addresses attempt rows by — with
                    // no way to learn it, an attempt-rooted cancellation
                    // would be unrepresentable by any caller.
                    result: json!({
                        "unit_id": unit_id,
                        "version": new_version,
                        "attempt_epoch": new_epoch,
                        "attempt_id": attempt_id,
                        "stamp": unit.stamp,
                        "authority_epoch": current_epoch.0,
                        "holder_id": holder_id,
                        "token_nonce": token_nonce,
                    }),
                })
            }
            Method::ProgressReport { unit_id } => {
                self.holder_gate(cmd, pre, unit_id)?;
                let unit = pre.unit.as_ref().expect("fence passed implies unit");
                expected_version_ok(unit.version)?;
                Ok(Accepted {
                    plan: Plan::Nothing,
                    events: vec![],
                    result: json!({ "unit_id": unit_id, "version": unit.version }),
                })
            }
            Method::StampBump { unit_id } => {
                authority_ok()?;
                let Some(unit) = &pre.unit else {
                    return Err((ErrorKind::NotFound, format!("unit {unit_id}"), false));
                };
                expected_version_ok(unit.version)?;
                let new_version = increment(unit.version, "unit version")?;
                let new_stamp = increment(unit.stamp, "unit stamp")?;
                Ok(Accepted {
                    plan: Plan::BumpUnit {
                        unit_node: unit.node,
                        new_version,
                        new_stamp: Some(new_stamp),
                    },
                    events: vec![EventDraft {
                        aggregate_kind: "unit",
                        aggregate_id: unit_id.clone(),
                        aggregate_version: new_version,
                        event_kind: "unit.stamp_bumped",
                        payload: json!({ "stamp": new_stamp }),
                    }],
                    result: json!({
                        "unit_id": unit_id,
                        "version": new_version,
                        "stamp": new_stamp,
                    }),
                })
            }
            Method::TokenReissue { unit_id } => Err((
                ErrorKind::CapabilityUnsupported,
                format!(
                    "token reauthorization for unit {unit_id} requires the policy and approval gate"
                ),
                false,
            )),
            Method::EffectPrepare { .. }
            | Method::EffectDispatch { .. }
            | Method::EffectRecordDispatched { .. }
            | Method::EffectSettle { .. }
            | Method::EffectParkReconciling { .. } => self.validate_effect(cmd, pre),
            Method::CancelRequest { .. } | Method::CancelRecordDelivery { .. } => {
                self.validate_cancel(cmd, pre)
            }
        }
    }

    /// Epoch-mismatch classification, shared by both authorization classes:
    /// Envelope authority is excluded from the request digest, so neither a
    /// stale nor an ahead value may reserve the command's idempotency key.
    pub(super) fn epoch_mismatch(e: AuthorityEpoch, current: AuthorityEpoch) -> Rejection {
        if e.0 < current.0 {
            (
                ErrorKind::FenceRejected,
                format!("authority epoch {} behind current {}", e.0, current.0),
                false,
            )
        } else {
            (
                ErrorKind::FenceRejected,
                format!(
                    "authority epoch {} ahead of current {} (split-brain suspect)",
                    e.0, current.0
                ),
                false,
            )
        }
    }

    /// The token's sealed unit binding: a token for another unit — or a
    /// unit that does not resolve — is an unresolvable token here,
    /// fence_rejected per ADR-002 §10. The foreign holder must not reserve an
    /// idempotency key in the target unit's namespace.
    pub(super) fn token_binds(token: &AttemptTokenClaims, unit_id: &str) -> Result<(), Rejection> {
        if token.unit_id != unit_id {
            return Err((
                ErrorKind::FenceRejected,
                format!(
                    "token sealed for unit {}, method targets {}",
                    token.unit_id, unit_id
                ),
                false,
            ));
        }
        Ok(())
    }

    /// The full holder-class gate: token presence, envelope epoch currency
    /// when declared, the sealed unit binding, and the §3.3 five-axis fence.
    pub(super) fn holder_gate<'c>(
        &self,
        cmd: &'c Command,
        pre: &PreRead,
        unit_id: &str,
    ) -> Result<&'c AttemptTokenClaims, Rejection> {
        if cmd.principal_kind != PrincipalKind::Agent {
            return Err((
                ErrorKind::InvalidRequest,
                format!(
                    "holder method requires principal_kind agent, got {:?}",
                    cmd.principal_kind
                ),
                true,
            ));
        }
        let Some(token) = &cmd.attempt_token else {
            return Err((
                ErrorKind::InvalidRequest,
                "holder method requires an attempt token".into(),
                true,
            ));
        };
        let current = self.store.authority_epoch();
        if let Some(e) = cmd.authority_epoch
            && e != current
        {
            return Err(Self::epoch_mismatch(e, current));
        }
        Self::token_binds(token, unit_id)?;
        if cmd.principal_id != token.holder_id {
            return Err((
                ErrorKind::Unauthorized,
                "holder principal does not match the sealed token".into(),
                true,
            ));
        }
        self.store
            .check_holder_fence(pre, token)
            .map_err(|r| match r {
                StoreRejection::FenceRejected { detail } => {
                    (ErrorKind::FenceRejected, detail, true)
                }
                StoreRejection::NotFound { aggregate } => (ErrorKind::NotFound, aggregate, false),
                other => (ErrorKind::Internal, format!("{other:?}"), false),
            })?;
        Ok(token)
    }
}

fn increment(value: u64, axis: &str) -> Result<u64, Rejection> {
    value.checked_add(1).ok_or_else(|| {
        (
            ErrorKind::ResourceExhausted,
            format!("{axis} space exhausted"),
            true,
        )
    })
}
