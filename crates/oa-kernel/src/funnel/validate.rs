//! §2.2 step 3: command validation as a pure function of the pre-read
//! state — every authorization class, fence axis, and rejection-
//! persistence decision lives here; the mutation phase in `submit`
//! executes the returned plan without further reads.

use super::*;

impl Funnel {
    /// §2.2 step 3 as a pure function of the pre-read state.
    pub(super) fn validate(&self, cmd: &Command, pre: &PreRead) -> Result<Accepted, Rejection> {
        let current_epoch = self.store.authority_epoch();
        let epoch_mismatch =
            |e: AuthorityEpoch| -> Rejection { Self::epoch_mismatch(e, current_epoch) };
        // One implementation of the §2.1 authority class, shared with the
        // §4 settle/park arms — two copies of an authorization check are
        // two chances to drift apart.
        let authority_ok = || -> Result<(), Rejection> { self.authority_gate(cmd) };
        // Holder methods carry the authority axis inside the token, but an
        // envelope authority_epoch, when present, must still be current —
        // ADR-002 §5: a prior epoch is fence_rejected on ANY method.
        let envelope_epoch_ok = || -> Result<(), Rejection> {
            match cmd.authority_epoch {
                Some(e) if e != current_epoch => Err(epoch_mismatch(e)),
                _ => Ok(()),
            }
        };
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
                    result: json!({ "run_id": run_id }),
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
                Ok(Accepted {
                    plan: Plan::CloseRun {
                        run_node: run.node,
                        new_version: run.version + 1,
                        status: outcome.wire(),
                    },
                    events: vec![EventDraft {
                        aggregate_kind: "run",
                        aggregate_id: run_id.clone(),
                        aggregate_version: run.version + 1,
                        event_kind: "run.closed",
                        payload: json!({ "outcome": outcome.wire() }),
                    }],
                    result: json!({ "run_id": run_id, "status": outcome.wire() }),
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
                    result: json!({ "work_item_id": work_item_id }),
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
                    result: json!({ "unit_id": unit_id }),
                })
            }
            Method::UnitDispatch { unit_id, holder_id } => {
                authority_ok()?;
                let Some(unit) = &pre.unit else {
                    return Err((ErrorKind::NotFound, format!("unit {unit_id}"), false));
                };
                expected_version_ok(unit.version)?;
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
                let new_epoch = unit.epoch + 1;
                let attempt_id = self.mint_id().to_string();
                Ok(Accepted {
                    plan: Plan::Dispatch {
                        unit_node: unit.node,
                        unit_id: unit_id.clone(),
                        new_version: unit.version + 1,
                        new_epoch,
                        holder_id: holder_id.clone(),
                        attempt_id: attempt_id.clone(),
                        existing_lease: pre.lease.as_ref().map(|l| (l.node, l.version)),
                        prior_attempt,
                    },
                    events: vec![EventDraft {
                        aggregate_kind: "unit",
                        aggregate_id: unit_id.clone(),
                        aggregate_version: unit.version + 1,
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
                        "attempt_epoch": new_epoch,
                        "attempt_id": attempt_id,
                        "stamp": unit.stamp,
                        "authority_epoch": current_epoch.0,
                        "holder_id": holder_id,
                    }),
                })
            }
            Method::ProgressReport { unit_id, note } => {
                self.holder_gate(cmd, pre, unit_id)?;
                let unit = pre.unit.as_ref().expect("fence passed implies unit");
                expected_version_ok(unit.version)?;
                Ok(Accepted {
                    plan: Plan::BumpUnit {
                        unit_node: unit.node,
                        new_version: unit.version + 1,
                        new_stamp: None,
                    },
                    events: vec![EventDraft {
                        aggregate_kind: "unit",
                        aggregate_id: unit_id.clone(),
                        aggregate_version: unit.version + 1,
                        event_kind: "unit.progress",
                        payload: json!({ "note": note }),
                    }],
                    result: json!({ "unit_id": unit_id, "version": unit.version + 1 }),
                })
            }
            Method::StampBump { unit_id } => {
                authority_ok()?;
                let Some(unit) = &pre.unit else {
                    return Err((ErrorKind::NotFound, format!("unit {unit_id}"), false));
                };
                expected_version_ok(unit.version)?;
                Ok(Accepted {
                    plan: Plan::BumpUnit {
                        unit_node: unit.node,
                        new_version: unit.version + 1,
                        new_stamp: Some(unit.stamp + 1),
                    },
                    events: vec![EventDraft {
                        aggregate_kind: "unit",
                        aggregate_id: unit_id.clone(),
                        aggregate_version: unit.version + 1,
                        event_kind: "unit.stamp_bumped",
                        payload: json!({ "stamp": unit.stamp + 1 }),
                    }],
                    result: json!({ "unit_id": unit_id, "stamp": unit.stamp + 1 }),
                })
            }
            Method::TokenReissue { unit_id } => {
                let Some(token) = &cmd.attempt_token else {
                    return Err((
                        ErrorKind::InvalidRequest,
                        "token.reissue requires the stale token as identity proof".into(),
                        true,
                    ));
                };
                envelope_epoch_ok()?;
                Self::token_binds(token, unit_id)?;
                if token.authority_epoch != current_epoch {
                    return Err((
                        ErrorKind::FenceRejected,
                        "token minted under a prior authority epoch".into(),
                        true,
                    ));
                }
                let Some(unit) = &pre.unit else {
                    return Err((ErrorKind::NotFound, format!("unit {unit_id}"), false));
                };
                if token.attempt_epoch != AttemptEpoch(unit.epoch) {
                    return Err((
                        ErrorKind::FenceRejected,
                        "token attempt epoch superseded".into(),
                        true,
                    ));
                }
                let Some(lease) = &pre.lease else {
                    return Err((
                        ErrorKind::NotFound,
                        format!("lease for unit {unit_id}"),
                        false,
                    ));
                };
                if lease.status != "active" || lease.holder_id != token.holder_id {
                    return Err((
                        ErrorKind::FenceRejected,
                        "holder does not match the active lease".into(),
                        true,
                    ));
                }
                Ok(Accepted {
                    plan: Plan::Nothing,
                    events: vec![],
                    result: json!({
                        "unit_id": unit_id,
                        "attempt_epoch": unit.epoch,
                        "stamp": unit.stamp,
                        "authority_epoch": current_epoch.0,
                        "holder_id": token.holder_id,
                    }),
                })
            }
            Method::EffectPrepare { .. }
            | Method::EffectDispatch { .. }
            | Method::EffectRecordDispatched { .. }
            | Method::EffectSettle { .. }
            | Method::EffectParkReconciling { .. } => self.validate_effect(cmd, pre),
        }
    }

    /// Epoch-mismatch classification, shared by both authorization classes:
    /// behind is permanently stale (epochs are monotonic), so the rejection
    /// persists; ahead is a split-brain signature that a later lawful
    /// takeover could make current, so it must not.
    pub(super) fn epoch_mismatch(e: AuthorityEpoch, current: AuthorityEpoch) -> Rejection {
        if e.0 < current.0 {
            (
                ErrorKind::FenceRejected,
                format!("authority epoch {} behind current {}", e.0, current.0),
                true,
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
    /// fence_rejected per ADR-002 §10. Permanently deterministic (the
    /// token's unit_id never changes), so it persists.
    pub(super) fn token_binds(token: &AttemptTokenClaims, unit_id: &str) -> Result<(), Rejection> {
        if token.unit_id != unit_id {
            return Err((
                ErrorKind::FenceRejected,
                format!(
                    "token sealed for unit {}, method targets {}",
                    token.unit_id, unit_id
                ),
                true,
            ));
        }
        Ok(())
    }

    /// The full holder-class gate every holder method (except the
    /// stamp-forgiving `token.reissue`) runs: token presence, envelope
    /// epoch currency when declared, the sealed unit binding, and the §3.3
    /// five-axis fence. Returns the token so callers can stamp its claims.
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
