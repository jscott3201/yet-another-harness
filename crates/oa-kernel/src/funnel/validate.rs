//! §2.2 step 3: command validation as a pure function of the pre-read
//! state — every authorization class, fence axis, and rejection-
//! persistence decision lives here; the mutation phase in `submit`
//! executes the returned plan without further reads.

use super::*;

impl Funnel {
    /// §2.2 step 3 as a pure function of the pre-read state.
    pub(super) fn validate(&self, cmd: &Command, pre: &PreRead) -> Result<Accepted, Rejection> {
        let current_epoch = self.store.authority_epoch();
        // Epoch-mismatch classification, shared by both authorization
        // classes: behind is permanently stale (epochs are monotonic), so
        // the rejection persists; ahead is a split-brain signature that a
        // later lawful takeover could make current, so it must not.
        let epoch_mismatch = |e: AuthorityEpoch| -> Rejection {
            if e.0 < current_epoch.0 {
                (
                    ErrorKind::FenceRejected,
                    format!("authority epoch {} behind current {}", e.0, current_epoch.0),
                    true,
                )
            } else {
                (
                    ErrorKind::FenceRejected,
                    format!(
                        "authority epoch {} ahead of current {} (split-brain suspect)",
                        e.0, current_epoch.0
                    ),
                    false,
                )
            }
        };
        let authority_ok = || -> Result<(), Rejection> {
            if cmd.attempt_token.is_some() {
                return Err((
                    ErrorKind::InvalidRequest,
                    "authority method must not carry an attempt token".into(),
                    true,
                ));
            }
            match cmd.authority_epoch {
                Some(e) if e == current_epoch => Ok(()),
                Some(e) => Err(epoch_mismatch(e)),
                None => Err((
                    ErrorKind::InvalidRequest,
                    "authority method requires authority_epoch".into(),
                    true,
                )),
            }
        };
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
            match cmd.expected_unit_version {
                Some(expected) if expected != actual => Err((
                    ErrorKind::VersionConflict,
                    format!("expected version {expected}, actual {actual}"),
                    false,
                )),
                Some(_) => Ok(()),
                None => Err((
                    ErrorKind::InvalidRequest,
                    "mutating method requires expected_unit_version".into(),
                    true,
                )),
            }
        };
        // The token's sealed unit binding: a token for another unit — or a
        // unit that does not resolve — is an unresolvable token here,
        // fence_rejected per ADR-002 §10. Permanently deterministic (the
        // token's unit_id never changes), so it persists.
        let token_binds = |token: &AttemptTokenClaims, unit_id: &str| -> Result<(), Rejection> {
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
        };

        match &cmd.method {
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
            } => {
                authority_ok()?;
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
                    },
                    events: vec![EventDraft {
                        aggregate_kind: "unit",
                        aggregate_id: unit_id.clone(),
                        aggregate_version: 1,
                        event_kind: "unit.admitted",
                        payload: json!({ "work_item_id": work_item_id }),
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
                Ok(Accepted {
                    plan: Plan::Dispatch {
                        unit_node: unit.node,
                        unit_id: unit_id.clone(),
                        new_version: unit.version + 1,
                        new_epoch,
                        holder_id: holder_id.clone(),
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
                            "holder_id": holder_id,
                        }),
                    }],
                    result: json!({
                        "unit_id": unit_id,
                        "attempt_epoch": new_epoch,
                        "stamp": unit.stamp,
                        "authority_epoch": current_epoch.0,
                        "holder_id": holder_id,
                    }),
                })
            }
            Method::ProgressReport { unit_id, note } => {
                let Some(token) = &cmd.attempt_token else {
                    return Err((
                        ErrorKind::InvalidRequest,
                        "holder method requires an attempt token".into(),
                        true,
                    ));
                };
                envelope_epoch_ok()?;
                token_binds(token, unit_id)?;
                self.store
                    .check_holder_fence(pre, token)
                    .map_err(|r| match r {
                        StoreRejection::FenceRejected { detail } => {
                            (ErrorKind::FenceRejected, detail, true)
                        }
                        StoreRejection::NotFound { aggregate } => {
                            (ErrorKind::NotFound, aggregate, false)
                        }
                        other => (ErrorKind::Internal, format!("{other:?}"), false),
                    })?;
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
                token_binds(token, unit_id)?;
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
        }
    }
}
