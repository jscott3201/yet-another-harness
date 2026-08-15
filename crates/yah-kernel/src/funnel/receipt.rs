use super::*;

pub(super) fn identifiers_are_valid(command: &Command) -> bool {
    crate::ids::valid_wire_identifier(&command.scope_id)
        && crate::ids::valid_wire_identifier(&command.command_id)
        && crate::ids::valid_wire_identifier(&command.principal_id)
        && [
            command.causation_id.as_deref(),
            command.correlation_id.as_deref(),
        ]
        .into_iter()
        .flatten()
        .all(crate::ids::valid_wire_identifier)
        && method_identifiers_are_valid(&command.method)
}

fn method_identifiers_are_valid(method: &Method) -> bool {
    let valid = crate::ids::valid_wire_identifier;
    match method {
        Method::RunOpen {
            run_id,
            goal_work_item_id,
        } => valid(run_id) && valid(goal_work_item_id),
        Method::RunClose { run_id, .. } => valid(run_id),
        Method::WorkItemCreate { work_item_id, .. } => valid(work_item_id),
        Method::UnitAdmit {
            unit_id,
            work_item_id,
            run_id,
        } => valid(unit_id) && valid(work_item_id) && valid(run_id),
        Method::UnitDispatch { unit_id, holder_id } => valid(unit_id) && valid(holder_id),
        Method::ProgressReport { unit_id }
        | Method::StampBump { unit_id }
        | Method::TokenReissue { unit_id } => valid(unit_id),
        Method::EffectPrepare { unit_id, spec } => valid(unit_id) && valid(&spec.adapter_id),
        Method::EffectDispatch {
            unit_id,
            operation_key,
        }
        | Method::EffectRecordDispatched {
            unit_id,
            operation_key,
            ..
        }
        | Method::EffectSettle {
            unit_id,
            operation_key,
            ..
        }
        | Method::EffectParkReconciling {
            unit_id,
            operation_key,
            ..
        } => valid(unit_id) && crate::ids::Digest::try_from(operation_key.clone()).is_ok(),
        Method::CancelRequest {
            root_id, proposed, ..
        } => valid(root_id) && proposed.iter().all(|member| valid(&member.member_id)),
        Method::CancelRecordDelivery {
            cancel_request_id,
            member_id,
            ..
        } => valid(cancel_request_id) && valid(member_id),
    }
}

pub(super) fn project_scope_is_valid(command: &Command, project_id: &str) -> bool {
    command.scope_kind != ScopeKind::Project || command.scope_id == project_id
}

pub(super) fn address_is_valid(command: &Command, project_id: &str) -> bool {
    if !identifiers_are_valid(command) || !project_scope_is_valid(command, project_id) {
        return false;
    }
    match &command.method {
        Method::RunOpen { run_id, .. } | Method::RunClose { run_id, .. } => {
            command.scope_kind == ScopeKind::Global
                || (command.scope_kind == ScopeKind::Run && command.scope_id == *run_id)
        }
        Method::WorkItemCreate { .. } => {
            matches!(command.scope_kind, ScopeKind::Global | ScopeKind::Project)
        }
        Method::ProgressReport { unit_id }
        | Method::TokenReissue { unit_id }
        | Method::EffectPrepare { unit_id, .. }
        | Method::EffectDispatch { unit_id, .. }
        | Method::EffectRecordDispatched { unit_id, .. } => {
            command.scope_kind == ScopeKind::Unit && command.scope_id == *unit_id
        }
        Method::UnitAdmit { unit_id, .. }
        | Method::UnitDispatch { unit_id, .. }
        | Method::StampBump { unit_id }
        | Method::EffectSettle { unit_id, .. }
        | Method::EffectParkReconciling { unit_id, .. } => {
            command.scope_kind == ScopeKind::Global
                || (command.scope_kind == ScopeKind::Unit && command.scope_id == *unit_id)
        }
        Method::CancelRequest { .. } | Method::CancelRecordDelivery { .. } => {
            command.scope_kind == ScopeKind::Global
        }
    }
}

pub(super) fn properties(
    command: &Command,
    command_type: &str,
    receipt_key: &str,
    status: &str,
    result: &serde_json::Value,
    cursors: Option<(u64, u64)>,
) -> PropertyMap {
    let string = |value: &str| Value::String(db(value));
    let mut pairs = vec![
        (db("receipt_key"), string(receipt_key)),
        (db("command_type"), string(command_type)),
        (
            db("receipt_version"),
            Value::Uint(u64::from(crate::protocol::RECEIPT_VERSION.get())),
        ),
        (
            db("request_digest"),
            string(command.request_digest.as_str()),
        ),
        (db("principal_kind"), string(command.principal_kind.wire())),
        (db("principal_id"), string(&command.principal_id)),
        (db("status"), string(status)),
        (db("result"), string(&result.to_string())),
    ];
    if let Some((first, last)) = cursors {
        pairs.push((db("first_cursor"), Value::Uint(first)));
        pairs.push((db("last_cursor"), Value::Uint(last)));
    }
    PropertyMap::from_pairs(pairs).expect("receipt property map")
}
