use super::*;
use crate::cancel::{CancelDelivery, CancelKind, CancelPolicy, CancelRequest, CancelScope};
use crate::effect::EffectIntent;
use crate::ids::Digest;
use selene_graph::SeleneGraph;
use std::collections::HashSet;

enum HistoricalProof {
    Aggregate {
        aggregate_kind: &'static str,
        aggregate_id: String,
        event_kind: &'static str,
    },
    SupersedingAttempt {
        unit_id: String,
        next_attempt_id: String,
    },
}

pub(super) fn decode_token_key(value: &str) -> Option<[u8; 32]> {
    if value.len() != 64 {
        return None;
    }
    let mut key = [0_u8; 32];
    for (index, byte) in key.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&value[index * 2..index * 2 + 2], 16).ok()?;
    }
    Some(key)
}

pub(crate) fn commit_error(error: GraphError) -> StoreError {
    match error {
        GraphError::Durable { reason } => StoreError::CommitUnknown(reason),
        GraphError::TypeViolation(_) | GraphError::Core(_) | GraphError::Cancelled => {
            StoreError::Graph(error)
        }
        _ => StoreError::CommitUnknown(format!("unclassified Selene commit outcome: {error:?}")),
    }
}

fn required_string(props: &PropertyMap, field: &str, row: &str) -> Result<String, StoreError> {
    props
        .get(&db(field))
        .and_then(value_str)
        .ok_or_else(|| StoreError::Internal(format!("{row} has invalid {field}")))
}

fn wire_name(value: &impl serde::Serialize) -> Result<String, StoreError> {
    serde_json::to_value(value)
        .ok()
        .and_then(|value| value.as_str().map(str::to_owned))
        .ok_or_else(|| {
            StoreError::Internal("cancellation enum does not serialize as a string".into())
        })
}

fn required_u64(props: &PropertyMap, field: &str, row: &str) -> Result<u64, StoreError> {
    props
        .get(&db(field))
        .and_then(value_u64)
        .ok_or_else(|| StoreError::Internal(format!("{row} has invalid {field}")))
}

pub(super) fn validate_attempt(props: &PropertyMap) -> Result<(), StoreError> {
    let row = "Attempt row";
    let unit_id = required_string(props, "unit_id", row)?;
    let epoch = required_u64(props, "attempt_epoch", row)?;
    if required_string(props, "attempt_key", row)? != format!("{unit_id}/{epoch}") {
        return Err(StoreError::Internal(
            "Attempt attempt_key disagrees with its fields".into(),
        ));
    }
    Ok(())
}

pub(super) fn validate_effect(props: &PropertyMap) -> Result<(), StoreError> {
    let row = "Effect row";
    let intent: EffectIntent = serde_json::from_str(&required_string(props, "record", row)?)
        .map_err(|error| StoreError::Internal(format!("{row} has invalid record: {error}")))?;
    let terminal = match props.get(&db("terminal")) {
        Some(value) => value_str(value)
            .map(Some)
            .ok_or_else(|| StoreError::Internal(format!("{row} has invalid terminal")))?,
        None => None,
    };
    if required_string(props, "operation_key", row)? != intent.operation_key
        || required_string(props, "effect_intent_id", row)? != intent.effect_intent_id.to_string()
        || required_string(props, "unit_id", row)? != intent.unit_id
        || required_u64(props, "attempt_epoch", row)? != intent.attempt_epoch.0
        || required_u64(props, "version", row)? != intent.version
        || required_string(props, "state", row)? != wire_name(&intent.state)?
        || terminal != intent.terminal.as_ref().map(wire_name).transpose()?
    {
        return Err(StoreError::Internal(
            "Effect indexed fields disagree with its record".into(),
        ));
    }
    Ok(())
}

pub(super) fn validate_cancel_request(props: &PropertyMap) -> Result<(), StoreError> {
    let row = "CancelRequest row";
    let request_id = required_string(props, "cancel_request_id", row)?;
    let root_kind = required_string(props, "root_kind", row)?;
    let root_id = required_string(props, "root_id", row)?;
    let policy = required_string(props, "policy", row)?;
    let reason = required_string(props, "reason", row)?;
    let status = required_string(props, "status", row)?;
    let scope: CancelScope = serde_json::from_str(&required_string(props, "scope", row)?)
        .map_err(|error| StoreError::Internal(format!("{row} has invalid scope: {error}")))?;
    let request: CancelRequest = serde_json::from_str(&required_string(props, "record", row)?)
        .map_err(|error| StoreError::Internal(format!("{row} has invalid record: {error}")))?;
    if request.cancel_request_id.to_string() != request_id
        || scope != request.scope
        || !scope.validate()
        || scope.root_kind().wire() != root_kind
        || scope.root_id() != root_id
        || wire_name(&request.policy)? != policy
        || wire_name(&request.reason)? != reason
        || wire_name(&request.status)? != status
        || (scope.members().is_empty() && request.policy != CancelPolicy::RootOnly)
    {
        return Err(StoreError::Internal(
            "CancelRequest indexed fields disagree with its record".into(),
        ));
    }
    Ok(())
}

pub(super) fn validate_cancel_delivery(props: &PropertyMap) -> Result<(), StoreError> {
    let row = "CancelDelivery row";
    let request_id = required_string(props, "cancel_request_id", row)?;
    let member_id = required_string(props, "member_id", row)?;
    let member_kind = required_string(props, "member_kind", row)?;
    let order_index = required_u64(props, "order_index", row)?;
    let outcome = required_string(props, "outcome", row)?;
    let delivery_key = required_string(props, "delivery_key", row)?;
    let delivery: CancelDelivery = serde_json::from_str(&required_string(props, "record", row)?)
        .map_err(|error| StoreError::Internal(format!("{row} has invalid record: {error}")))?;
    let observation_is_valid = match (delivery.outcome, delivery.observed_at) {
        (crate::cancel::DeliveryOutcome::Unresponsive, None) => true,
        (crate::cancel::DeliveryOutcome::Unresponsive, Some(_)) | (_, None) => false,
        (_, Some(observed_at)) => observed_at >= delivery.delivered_at,
    };
    if delivery_key != format!("{request_id}|{member_id}")
        || delivery.cancel_request_id.to_string() != request_id
        || delivery.member_id != member_id
        || delivery.member_kind.wire() != member_kind
        || order_index >= crate::cancel::MAX_SCOPE_MEMBERS as u64
        || delivery.outcome.wire() != outcome
        || !observation_is_valid
    {
        return Err(StoreError::Internal(
            "CancelDelivery indexed fields disagree with its record".into(),
        ));
    }
    Ok(())
}

impl Store {
    fn member_creation_proof(
        &self,
        read: &SeleneGraph,
        kind: CancelKind,
        id: &str,
    ) -> Result<HistoricalProof, StoreError> {
        match kind {
            CancelKind::Run => Ok(HistoricalProof::Aggregate {
                aggregate_kind: "run",
                aggregate_id: id.to_owned(),
                event_kind: "run.opened",
            }),
            CancelKind::ExecutionUnit => Ok(HistoricalProof::Aggregate {
                aggregate_kind: "unit",
                aggregate_id: id.to_owned(),
                event_kind: "unit.admitted",
            }),
            CancelKind::Attempt => {
                let node = self.attempt_id_node(id).ok_or_else(|| {
                    StoreError::Internal("cancellation has no attempt root".into())
                })?;
                let unit_id = read
                    .node_properties(node)
                    .and_then(|properties| properties.get(&db("unit_id")).and_then(value_str))
                    .ok_or_else(|| StoreError::Internal("attempt root is unreadable".into()))?;
                Ok(HistoricalProof::SupersedingAttempt {
                    unit_id,
                    next_attempt_id: id.to_owned(),
                })
            }
            CancelKind::EffectIntent => Ok(HistoricalProof::Aggregate {
                aggregate_kind: "effect",
                aggregate_id: id.to_owned(),
                event_kind: "effect.prepared",
            }),
        }
    }

    fn empty_root_terminal_proof(
        &self,
        read: &SeleneGraph,
        request: &CancelRequest,
    ) -> Result<Option<HistoricalProof>, StoreError> {
        if !request.scope.members().is_empty() {
            return Ok(None);
        }
        let root_id = request.scope.root_id();
        match request.scope.root_kind() {
            CancelKind::Run => Ok(Some(HistoricalProof::Aggregate {
                aggregate_kind: "run",
                aggregate_id: root_id.to_owned(),
                event_kind: "run.closed",
            })),
            CancelKind::EffectIntent => Ok(Some(HistoricalProof::Aggregate {
                aggregate_kind: "effect",
                aggregate_id: root_id.to_owned(),
                event_kind: "effect.settled",
            })),
            CancelKind::Attempt => {
                let node = self.attempt_id_node(root_id).ok_or_else(|| {
                    StoreError::Internal("empty cancellation has no attempt root".into())
                })?;
                let properties = read
                    .node_properties(node)
                    .ok_or_else(|| StoreError::Internal("attempt root is unreadable".into()))?;
                let unit_id = required_string(properties, "unit_id", "Attempt row")?;
                let next_epoch = required_u64(properties, "attempt_epoch", "Attempt row")?
                    .checked_add(1)
                    .ok_or_else(|| StoreError::Internal("attempt epoch overflow".into()))?;
                let next_node = self
                    .attempt_node(&format!("{unit_id}/{next_epoch}"))
                    .ok_or_else(|| {
                        StoreError::Internal("superseded attempt has no successor".into())
                    })?;
                let next_attempt_id = read
                    .node_properties(next_node)
                    .and_then(|properties| properties.get(&db("attempt_id")).and_then(value_str))
                    .ok_or_else(|| {
                        StoreError::Internal("successor attempt is unreadable".into())
                    })?;
                Ok(Some(HistoricalProof::SupersedingAttempt {
                    unit_id,
                    next_attempt_id,
                }))
            }
            CancelKind::ExecutionUnit => Err(StoreError::Internal(
                "empty unit cancellation has no supported terminal proof".into(),
            )),
        }
    }

    fn validate_cancel_scope_ownership(
        &self,
        read: &SeleneGraph,
        request: &CancelRequest,
    ) -> Result<(), StoreError> {
        let root_kind = request.scope.root_kind();
        let root_id = request.scope.root_id();
        let root_attempt = if root_kind == CancelKind::Attempt {
            let node = self
                .attempt_id_node(root_id)
                .ok_or_else(|| StoreError::Internal("cancellation has no attempt root".into()))?;
            let properties = read
                .node_properties(node)
                .ok_or_else(|| StoreError::Internal("attempt root is unreadable".into()))?;
            Some((
                required_string(properties, "unit_id", "Attempt row")?,
                required_u64(properties, "attempt_epoch", "Attempt row")?,
            ))
        } else {
            None
        };
        for member in request.scope.members() {
            if member.member_kind == root_kind && member.member_id == root_id {
                continue;
            }
            let (unit_id, run_id, attempt_epoch) = match member.member_kind {
                CancelKind::ExecutionUnit => {
                    let node = self.unit_node(&member.member_id).ok_or_else(|| {
                        StoreError::Internal("cancellation scope member does not exist".into())
                    })?;
                    let properties = read.node_properties(node).ok_or_else(|| {
                        StoreError::Internal("cancellation unit member is unreadable".into())
                    })?;
                    (
                        None,
                        Some(required_string(properties, "run_id", "Unit row")?),
                        None,
                    )
                }
                CancelKind::Attempt => {
                    let node = self.attempt_id_node(&member.member_id).ok_or_else(|| {
                        StoreError::Internal("cancellation scope member does not exist".into())
                    })?;
                    let properties = read.node_properties(node).ok_or_else(|| {
                        StoreError::Internal("cancellation attempt member is unreadable".into())
                    })?;
                    let unit_id = required_string(properties, "unit_id", "Attempt row")?;
                    let unit = self
                        .unit_node(&unit_id)
                        .ok_or_else(|| StoreError::Internal("attempt member has no unit".into()))?;
                    let run_id = read
                        .node_properties(unit)
                        .and_then(|properties| properties.get(&db("run_id")).and_then(value_str))
                        .ok_or_else(|| StoreError::Internal("attempt unit is unreadable".into()))?;
                    (Some(unit_id), Some(run_id), None)
                }
                CancelKind::EffectIntent => {
                    let node = self
                        .effect_intent_id_node(&member.member_id)
                        .ok_or_else(|| {
                            StoreError::Internal("cancellation scope member does not exist".into())
                        })?;
                    let properties = read.node_properties(node).ok_or_else(|| {
                        StoreError::Internal("cancellation effect member is unreadable".into())
                    })?;
                    let unit_id = required_string(properties, "unit_id", "Effect row")?;
                    let unit = self
                        .unit_node(&unit_id)
                        .ok_or_else(|| StoreError::Internal("effect member has no unit".into()))?;
                    let run_id = read
                        .node_properties(unit)
                        .and_then(|properties| properties.get(&db("run_id")).and_then(value_str))
                        .ok_or_else(|| StoreError::Internal("effect unit is unreadable".into()))?;
                    let intent: EffectIntent =
                        serde_json::from_str(&required_string(properties, "record", "Effect row")?)
                            .map_err(|error| {
                                StoreError::Internal(format!("Effect record is invalid: {error}"))
                            })?;
                    (Some(unit_id), Some(run_id), Some(intent.attempt_epoch.0))
                }
                CancelKind::Run => {
                    return Err(StoreError::Internal(
                        "cancellation scope contains a foreign run".into(),
                    ));
                }
            };
            let owned = match (root_kind, member.member_kind) {
                (CancelKind::Run, _) => run_id.as_deref() == Some(root_id),
                (CancelKind::ExecutionUnit, CancelKind::Attempt | CancelKind::EffectIntent) => {
                    unit_id.as_deref() == Some(root_id)
                }
                (CancelKind::Attempt, CancelKind::EffectIntent) => root_attempt
                    .as_ref()
                    .is_some_and(|(root_unit, root_epoch)| {
                        unit_id.as_deref() == Some(root_unit.as_str())
                            && attempt_epoch == Some(*root_epoch)
                    }),
                _ => false,
            };
            if !owned {
                return Err(StoreError::Internal(
                    "cancellation scope member is outside its root tree".into(),
                ));
            }
        }
        Ok(())
    }

    pub(crate) fn validate_all_cancellation_lifecycles(&self) -> Result<(), StoreError> {
        let read = self.shared.read();
        struct RequestRecovery {
            version: u64,
            record: CancelRequest,
            members: HashMap<String, (CancelKind, u32)>,
            deliveries: Vec<crate::funnel::cancel_rules::DeliverySummary>,
            creation_proofs: Vec<HistoricalProof>,
            empty_root_terminal_proof: Option<HistoricalProof>,
        }

        let mut requests = HashMap::new();
        for request_node in self.cancel_request_nodes() {
            let props = read
                .node_properties(request_node)
                .ok_or_else(|| StoreError::Internal("CancelRequest row is unreadable".into()))?;
            let request_id = required_string(props, "cancel_request_id", "CancelRequest row")?;
            let request: CancelRequest =
                serde_json::from_str(&required_string(props, "record", "CancelRequest row")?)
                    .map_err(|error| {
                        StoreError::Internal(format!("CancelRequest record is invalid: {error}"))
                    })?;
            let members = request
                .scope
                .members()
                .iter()
                .map(|member| {
                    (
                        member.member_id.clone(),
                        (member.member_kind, member.order_index),
                    )
                })
                .collect();
            let root_terminal = self.cancel_root_terminal(
                &read,
                request.scope.root_kind(),
                request.scope.root_id(),
            );
            if root_terminal.is_none()
                || (request.scope.members().is_empty()
                    && (request.policy != CancelPolicy::RootOnly || root_terminal != Some(true)))
            {
                return Err(StoreError::Internal(
                    "CancelRequest has an ineligible cancellation root".into(),
                ));
            }
            self.validate_cancel_scope_ownership(&read, &request)?;
            let mut creation_proofs = vec![self.member_creation_proof(
                &read,
                request.scope.root_kind(),
                request.scope.root_id(),
            )?];
            for member in request.scope.members() {
                if member.member_kind != request.scope.root_kind()
                    || member.member_id != request.scope.root_id()
                {
                    creation_proofs.push(self.member_creation_proof(
                        &read,
                        member.member_kind,
                        &member.member_id,
                    )?);
                }
            }
            let empty_root_terminal_proof = self.empty_root_terminal_proof(&read, &request)?;
            requests.insert(
                request_id,
                RequestRecovery {
                    version: required_u64(props, "version", "CancelRequest row")?,
                    record: request,
                    members,
                    deliveries: Vec::new(),
                    creation_proofs,
                    empty_root_terminal_proof,
                },
            );
        }
        let mut delivery_keys = HashSet::new();
        for (delivery_key, node) in self.cancel_delivery_entries() {
            let properties = read
                .node_properties(node)
                .ok_or_else(|| StoreError::Internal("CancelDelivery row is unreadable".into()))?;
            let record = properties
                .get(&db("record"))
                .and_then(value_str)
                .ok_or_else(|| StoreError::Internal("CancelDelivery row is unreadable".into()))?;
            let delivery: CancelDelivery = serde_json::from_str(&record).map_err(|error| {
                StoreError::Internal(format!("CancelDelivery record is invalid: {error}"))
            })?;
            let request_id = delivery.cancel_request_id.to_string();
            let request = requests.get_mut(&request_id).ok_or_else(|| {
                StoreError::Internal("CancelDelivery has no CancelRequest row".into())
            })?;
            let order_index = required_u64(properties, "order_index", "CancelDelivery row")?;
            if request.members.get(delivery.member_id.as_str())
                != Some(&(delivery.member_kind, order_index as u32))
            {
                return Err(StoreError::Internal(
                    "CancelDelivery member is outside its frozen scope".into(),
                ));
            }
            delivery_keys.insert(delivery_key);
            request
                .deliveries
                .push(crate::funnel::cancel_rules::DeliverySummary {
                    member_id: delivery.member_id,
                    outcome: delivery.outcome,
                });
        }
        drop(read);

        let mut delivery_events = HashSet::new();
        let mut request_events = HashMap::new();
        let mut aggregate_events = HashMap::new();
        let mut dispatch_events = HashMap::new();
        let mut first_delivery_cursor = HashMap::new();
        let mut next_delivery_order = HashMap::new();
        let mut cursor = 0;
        loop {
            let events = self.events_after_limit(cursor, crate::protocol::MAX_RESUME_EVENTS)?;
            if events.is_empty() {
                break;
            }
            for event in events {
                cursor = event.cursor;
                match event.event_kind.as_str() {
                    "cancel_request.requested" => {
                        let payload: serde_json::Value = serde_json::from_str(&event.payload)
                            .map_err(|error| {
                                StoreError::Internal(format!(
                                    "cancel request event payload is invalid: {error}"
                                ))
                            })?;
                        let request = requests.get(&event.aggregate_id).ok_or_else(|| {
                            StoreError::Internal(
                                "cancel request event has no CancelRequest row".into(),
                            )
                        })?;
                        let expected_status = if request.members.is_empty() {
                            crate::cancel::CancelStatus::Settled
                        } else {
                            crate::cancel::CancelStatus::Requested
                        };
                        if payload.get("status").and_then(serde_json::Value::as_str)
                            != Some(wire_name(&expected_status)?.as_str())
                            || payload.get("members").and_then(serde_json::Value::as_u64)
                                != Some(request.members.len() as u64)
                            || request_events
                                .insert(event.aggregate_id.clone(), event.cursor)
                                .is_some()
                        {
                            return Err(StoreError::Internal(
                                "cancel request event has an invalid initial lifecycle".into(),
                            ));
                        }
                    }
                    "run.opened" | "unit.admitted" | "effect.prepared" | "run.closed"
                    | "effect.settled" => {
                        aggregate_events
                            .entry((
                                event.aggregate_kind.clone(),
                                event.aggregate_id.clone(),
                                event.event_kind.clone(),
                            ))
                            .or_insert(event.cursor);
                    }
                    "unit.dispatched" => {
                        let payload: serde_json::Value = serde_json::from_str(&event.payload)
                            .map_err(|error| {
                                StoreError::Internal(format!(
                                    "dispatch event payload is invalid: {error}"
                                ))
                            })?;
                        let attempt_id = payload
                            .get("attempt_id")
                            .and_then(serde_json::Value::as_str)
                            .ok_or_else(|| {
                                StoreError::Internal("dispatch event has no attempt_id".into())
                            })?;
                        dispatch_events.insert(
                            (event.aggregate_id.clone(), attempt_id.to_owned()),
                            event.cursor,
                        );
                    }
                    _ => {}
                }
                if event.event_kind != "cancel_request.delivered" {
                    continue;
                }
                let payload: serde_json::Value =
                    serde_json::from_str(&event.payload).map_err(|error| {
                        StoreError::Internal(format!("delivery event payload is invalid: {error}"))
                    })?;
                let member_id = payload
                    .get("member_id")
                    .and_then(serde_json::Value::as_str)
                    .ok_or_else(|| {
                        StoreError::Internal("delivery event has no member_id".into())
                    })?;
                let request = requests.get(&event.aggregate_id).ok_or_else(|| {
                    StoreError::Internal("delivery event has no CancelRequest row".into())
                })?;
                let (_, order_index) = request.members.get(member_id).ok_or_else(|| {
                    StoreError::Internal("delivery event member is outside its frozen scope".into())
                })?;
                let delivery_key = format!("{}|{member_id}", event.aggregate_id);
                let expected_status = if *order_index as usize + 1 == request.members.len() {
                    request.record.status
                } else {
                    crate::cancel::CancelStatus::Delivering
                };
                let payload_status = payload
                    .get("status")
                    .and_then(serde_json::Value::as_str)
                    .ok_or_else(|| StoreError::Internal("delivery event has no status".into()))?;
                first_delivery_cursor
                    .entry(event.aggregate_id.clone())
                    .or_insert(event.cursor);
                let next_order = next_delivery_order
                    .entry(event.aggregate_id.clone())
                    .or_insert(0_u32);
                if event.aggregate_version != u64::from(*order_index) + 2
                    || *order_index != *next_order
                    || payload_status != wire_name(&expected_status)?
                    || !delivery_keys.contains(&delivery_key)
                    || !delivery_events.insert(delivery_key)
                {
                    return Err(StoreError::Internal(
                        "CancelDelivery events violate frozen leaf-first order".into(),
                    ));
                }
                *next_order += 1;
            }
        }
        if delivery_events != delivery_keys {
            return Err(StoreError::Internal(
                "CancelDelivery row has no leaf-first event".into(),
            ));
        }

        for (request_id, request) in requests {
            let requested_cursor = request_events.get(&request_id).ok_or_else(|| {
                StoreError::Internal("CancelRequest row has no requested event".into())
            })?;
            let proof_cursor = |proof: &HistoricalProof| match proof {
                HistoricalProof::Aggregate {
                    aggregate_kind,
                    aggregate_id,
                    event_kind,
                } => aggregate_events.get(&(
                    (*aggregate_kind).to_owned(),
                    aggregate_id.clone(),
                    (*event_kind).to_owned(),
                )),
                HistoricalProof::SupersedingAttempt {
                    unit_id,
                    next_attempt_id,
                } => dispatch_events.get(&(unit_id.clone(), next_attempt_id.clone())),
            };
            if request
                .creation_proofs
                .iter()
                .any(|proof| proof_cursor(proof).is_none_or(|cursor| cursor >= requested_cursor))
                || request
                    .empty_root_terminal_proof
                    .as_ref()
                    .is_some_and(|proof| {
                        proof_cursor(proof).is_none_or(|cursor| cursor >= requested_cursor)
                    })
            {
                return Err(StoreError::Internal(
                    "cancellation root history is invalid at request time".into(),
                ));
            }
            if request.deliveries.is_empty() && first_delivery_cursor.contains_key(&request_id) {
                return Err(StoreError::Internal(
                    "CancelRequest delivery history is inconsistent".into(),
                ));
            }
            if !request.deliveries.is_empty()
                && first_delivery_cursor
                    .get(&request_id)
                    .is_none_or(|cursor| cursor <= requested_cursor)
            {
                return Err(StoreError::Internal(
                    "CancelDelivery precedes its request".into(),
                ));
            }
            let expected_version = u64::try_from(request.deliveries.len())
                .ok()
                .and_then(|count| count.checked_add(1))
                .ok_or_else(|| StoreError::Internal("CancelRequest version overflow".into()))?;
            let expected_status = crate::funnel::cancel_rules::request_status_after(
                &request.record.scope,
                &request.deliveries,
            );
            if request.version != expected_version || request.record.status != expected_status {
                return Err(StoreError::Internal(
                    "CancelRequest lifecycle disagrees with its deliveries".into(),
                ));
            }
        }
        Ok(())
    }
}

pub(super) fn validate_receipt(props: &PropertyMap) -> Result<(), StoreError> {
    let record = super::read::receipt_record(props)?;
    let key = required_string(props, "receipt_key", "Receipt row")?;
    let mut parts = key.split('/');
    let scope_kind = parts.next();
    let scope_id = parts.next();
    let command_id = parts.next();
    if !matches!(scope_kind, Some("global" | "project" | "run" | "unit"))
        || scope_id.is_none_or(str::is_empty)
        || command_id.is_none_or(str::is_empty)
        || scope_id.is_some_and(|value| !crate::ids::valid_wire_identifier(value))
        || command_id.is_some_and(|value| !crate::ids::valid_wire_identifier(value))
        || parts.next().is_some()
        || Digest::try_from(record.request_digest.clone()).is_err()
        || !matches!(
            record.principal_kind.as_str(),
            "owner" | "delegate_human" | "agent" | "daemon"
        )
        || !crate::ids::valid_wire_identifier(&record.principal_id)
        || !super::receipt::valid_command_type(&record.command_type)
        || record.receipt_version != u64::from(crate::protocol::RECEIPT_VERSION.get())
    {
        return Err(StoreError::Internal("Receipt identity is invalid".into()));
    }
    if record.status == "rejected" {
        super::receipt::validate_rejection_result(&record.result)?;
    }
    Ok(())
}
