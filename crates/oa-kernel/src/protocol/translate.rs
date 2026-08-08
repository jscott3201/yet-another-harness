use super::types::*;
use crate::funnel::{self, Method, PrincipalKind, RunOutcome};
use crate::ids::{AuthorityEpoch, Digest};
use crate::store::AttemptTokenClaims;
pub(crate) fn into_funnel(
    command: &Command,
    token_claims: Option<AttemptTokenClaims>,
) -> Result<funnel::Command, String> {
    validate_envelope(command)?;
    let method = method(command)?;
    let principal_kind = if matches!(command.body.command_type(), CommandType::ProgressReport) {
        PrincipalKind::Agent
    } else {
        PrincipalKind::Daemon
    };
    Ok(funnel::Command {
        command_id: command.command_id.clone(),
        scope_kind: scope_kind(command.scope.scope_kind),
        scope_id: command.scope.scope_id.clone(),
        request_digest: Digest::try_from(command.request_digest.clone())?,
        expected_version: expected_version(command)?,
        principal_kind,
        principal_id: token_claims
            .as_ref()
            .map(|claims| claims.holder_id.clone())
            .unwrap_or_else(|| "daemon-local".into()),
        authority_epoch: command
            .authority_epoch
            .as_ref()
            .map(|v| AuthorityEpoch(v.get())),
        attempt_token: token_claims,
        causation_id: command.causation_id.clone(),
        correlation_id: command.correlation_id.clone(),
        method,
    })
}

pub(crate) fn validate_envelope(command: &Command) -> Result<(), String> {
    if command.protocol_version != super::PROTOCOL_VERSION {
        return Err("unsupported protocol_version".into());
    }
    if command.payload_schema_version != super::PAYLOAD_SCHEMA_VERSION {
        return Err("unsupported payload_schema_version".into());
    }
    if command.expected_versions.len() > 8 {
        return Err("expected_versions exceeds 8 entries".into());
    }
    for id in [
        command.command_id.as_str(),
        command.scope.scope_id.as_str(),
        command.target.aggregate_id.as_str(),
    ] {
        validate_wire_id(id)?;
    }
    for id in [
        command.causation_id.as_deref(),
        command.correlation_id.as_deref(),
    ]
    .into_iter()
    .flatten()
    {
        validate_wire_id(id)?;
    }
    validate_payload_ids(&command.body)?;
    if command.body.extra_fields().contains_key("principal") {
        return Err("payload must not contain principal".into());
    }
    validate_scope(&command.scope, &command.body)?;
    let holder = matches!(command.body.command_type(), CommandType::ProgressReport);
    if holder != command.attempt_token.is_some() {
        return Err(if holder {
            "holder command requires attempt_token".into()
        } else {
            "authority command must not carry attempt_token".into()
        });
    }
    match (
        command.scope.scope_kind,
        holder,
        command.authority_epoch.is_some(),
    ) {
        (ScopeKind::Global, _, true) => {
            return Err("global command must not carry authority_epoch".into());
        }
        (ScopeKind::Global, _, false) => {}
        (_, _, false) => return Err("control-graph command requires authority_epoch".into()),
        _ => {}
    }
    Ok(())
}

fn validate_scope(scope: &Scope, body: &CommandBody) -> Result<(), String> {
    let (expected_kind, expected_id) = match body {
        CommandBody::RunOpen(p) => (ScopeKind::Run, p.run_id.as_str()),
        CommandBody::RunClose(p) => (ScopeKind::Run, p.run_id.as_str()),
        CommandBody::WorkItemCreate(_) => return Ok(()),
        CommandBody::UnitAdmit(p) => (ScopeKind::Unit, p.unit_id.as_str()),
        CommandBody::UnitDispatch(p) => (ScopeKind::Unit, p.unit_id.as_str()),
        CommandBody::ProgressReport(p) => (ScopeKind::Unit, p.unit_id.as_str()),
        CommandBody::StampBump(p) => (ScopeKind::Unit, p.unit_id.as_str()),
    };
    if scope.scope_kind != expected_kind || scope.scope_id != expected_id {
        return Err("receipt scope does not match the command aggregate".into());
    }
    Ok(())
}

fn validate_payload_ids(payload: &CommandBody) -> Result<(), String> {
    match payload {
        CommandBody::RunOpen(p) => {
            validate_wire_id(&p.run_id)?;
            validate_wire_id(&p.goal_work_item_id)
        }
        CommandBody::RunClose(p) => validate_wire_id(&p.run_id),
        CommandBody::WorkItemCreate(p) => validate_wire_id(&p.work_item_id),
        CommandBody::UnitAdmit(p) => {
            validate_wire_id(&p.unit_id)?;
            validate_wire_id(&p.work_item_id)?;
            validate_wire_id(&p.run_id)
        }
        CommandBody::UnitDispatch(p) => {
            validate_wire_id(&p.unit_id)?;
            validate_wire_id(&p.holder_id)
        }
        CommandBody::ProgressReport(p) => validate_wire_id(&p.unit_id),
        CommandBody::StampBump(p) => validate_wire_id(&p.unit_id),
    }
}

pub(crate) fn validate_wire_id(value: &str) -> Result<(), String> {
    if !crate::ids::valid_wire_identifier(value) {
        return Err("wire identifier must be 1-64 characters from [A-Za-z0-9_.:-]".into());
    }
    Ok(())
}

fn expected_version(command: &Command) -> Result<Option<u64>, String> {
    match command.expected_versions.as_slice() {
        [] => Ok(None),
        [expected]
            if expected.aggregate_kind == command.target.aggregate_kind
                && expected.aggregate_id == command.target.aggregate_id =>
        {
            Ok(Some(expected.version.get()))
        }
        [_] => Err("expected version does not match target".into()),
        _ => Err("current command registry accepts one expected version".into()),
    }
}

fn method(command: &Command) -> Result<Method, String> {
    match &command.body {
        CommandBody::RunOpen(p) => {
            target(command, "run", &p.run_id)?;
            Ok(Method::RunOpen {
                run_id: p.run_id.clone(),
                goal_work_item_id: p.goal_work_item_id.clone(),
            })
        }
        CommandBody::RunClose(p) => {
            target(command, "run", &p.run_id)?;
            let outcome = match p.outcome {
                RunCloseOutcome::ClosedSuccess => RunOutcome::ClosedSuccess,
                RunCloseOutcome::ClosedFailure => RunOutcome::ClosedFailure,
                RunCloseOutcome::Cancelled => RunOutcome::Cancelled,
            };
            Ok(Method::RunClose {
                run_id: p.run_id.clone(),
                outcome,
            })
        }
        CommandBody::WorkItemCreate(p) => {
            target(command, "work_item", &p.work_item_id)?;
            Ok(Method::WorkItemCreate {
                work_item_id: p.work_item_id.clone(),
                acceptance_contract_digest: Digest::try_from(p.acceptance_contract_digest.clone())?,
                declared_write_scope: p.declared_write_scope.clone(),
            })
        }
        CommandBody::UnitAdmit(p) => {
            target(command, "unit", &p.unit_id)?;
            Ok(Method::UnitAdmit {
                unit_id: p.unit_id.clone(),
                work_item_id: p.work_item_id.clone(),
                run_id: p.run_id.clone(),
            })
        }
        CommandBody::UnitDispatch(p) => {
            target(command, "unit", &p.unit_id)?;
            Ok(Method::UnitDispatch {
                unit_id: p.unit_id.clone(),
                holder_id: p.holder_id.clone(),
            })
        }
        CommandBody::ProgressReport(p) => {
            target(command, "unit", &p.unit_id)?;
            Ok(Method::ProgressReport {
                unit_id: p.unit_id.clone(),
            })
        }
        CommandBody::StampBump(p) => {
            target(command, "unit", &p.unit_id)?;
            Ok(Method::StampBump {
                unit_id: p.unit_id.clone(),
            })
        }
    }
}

fn target(command: &Command, kind: &str, id: &str) -> Result<(), String> {
    if command.target.aggregate_kind != kind || command.target.aggregate_id != id {
        return Err("payload identity does not match target".into());
    }
    Ok(())
}

fn scope_kind(kind: ScopeKind) -> funnel::ScopeKind {
    match kind {
        ScopeKind::Global => funnel::ScopeKind::Global,
        ScopeKind::Project => funnel::ScopeKind::Project,
        ScopeKind::Run => funnel::ScopeKind::Run,
        ScopeKind::Unit => funnel::ScopeKind::Unit,
    }
}
