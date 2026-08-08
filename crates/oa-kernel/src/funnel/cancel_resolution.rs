use super::{EffectRow, Method};
use crate::cancel::{CancelKind, MemberInput};
use crate::store::{Store, db, value_str};
use selene_core::PropertyMap;
use selene_graph::SeleneGraph;

pub(super) struct MemberLink {
    pub parent_unit: Option<String>,
    pub run_id: Option<String>,
    pub attempt_epoch: Option<u64>,
}

fn prop_str(properties: &PropertyMap, name: &str) -> Option<String> {
    properties.get(&db(name)).and_then(value_str)
}

fn unit_run_id(read: &SeleneGraph, store: &Store, unit_id: &str) -> Option<String> {
    let node = store.unit_node(unit_id)?;
    let properties = read.node_properties(node)?;
    prop_str(properties, "run_id")
}

fn attempt_to_unit(
    read: &SeleneGraph,
    store: &Store,
    attempt_id: &str,
) -> Option<(String, String)> {
    let node = store.attempt_id_node(attempt_id)?;
    let properties = read.node_properties(node)?;
    let unit_id = prop_str(properties, "unit_id")?;
    let run_id = unit_run_id(read, store, &unit_id)?;
    Some((unit_id, run_id))
}

fn effect_to_unit(
    read: &SeleneGraph,
    store: &Store,
    effect_intent_id: &str,
) -> Option<(String, String, Option<u64>)> {
    let node = store.effect_intent_id_node(effect_intent_id)?;
    let properties = read.node_properties(node)?;
    let unit_id = prop_str(properties, "unit_id")?;
    let run_id = unit_run_id(read, store, &unit_id)?;
    let attempt_epoch = properties
        .get(&db("attempt_epoch"))
        .and_then(crate::store::value_u64);
    Some((unit_id, run_id, attempt_epoch))
}

pub(super) fn member_link(read: &SeleneGraph, store: &Store, member: &MemberInput) -> MemberLink {
    let (parent_unit, run_id, attempt_epoch) = match member.member_kind {
        CancelKind::Run => (None, Some(member.member_id.clone()), None),
        CancelKind::ExecutionUnit => (None, unit_run_id(read, store, &member.member_id), None),
        CancelKind::Attempt => {
            let (unit, run) = attempt_to_unit(read, store, &member.member_id).unwrap_or_default();
            (Some(unit), Some(run), None)
        }
        CancelKind::EffectIntent => {
            let (unit, run, epoch) =
                effect_to_unit(read, store, &member.member_id).unwrap_or_default();
            (Some(unit), Some(run), epoch)
        }
    };
    MemberLink {
        parent_unit,
        run_id,
        attempt_epoch,
    }
}

pub(super) fn root_is_terminal(
    read: &SeleneGraph,
    store: &Store,
    root_kind: CancelKind,
    root_id: &str,
) -> Option<bool> {
    store.cancel_root_terminal(read, root_kind, root_id)
}

pub(super) fn adoption_lineage(
    method: &Method,
    read: &SeleneGraph,
    store: &Store,
    effect: &Option<EffectRow>,
) -> Option<Vec<(CancelKind, String)>> {
    match method {
        Method::UnitAdmit { run_id, .. } => Some(vec![(CancelKind::Run, run_id.clone())]),
        Method::UnitDispatch { unit_id, .. } => unit_lineage(read, store, unit_id, false),
        Method::EffectPrepare { unit_id, .. } => unit_lineage(read, store, unit_id, true),
        Method::EffectDispatch { unit_id, .. } => {
            let mut lineage = unit_lineage(read, store, unit_id, true)?;
            if let Some(row) = effect
                && let Some(properties) = read.node_properties(row.node)
                && let Some(intent_id) = prop_str(properties, "effect_intent_id")
            {
                lineage.push((CancelKind::EffectIntent, intent_id));
            }
            Some(lineage)
        }
        _ => None,
    }
}

fn unit_lineage(
    read: &SeleneGraph,
    store: &Store,
    unit_id: &str,
    include_attempt: bool,
) -> Option<Vec<(CancelKind, String)>> {
    let unit_node = store.unit_node(unit_id)?;
    let properties = read.node_properties(unit_node)?;
    let run_id = prop_str(properties, "run_id")?;
    let epoch = properties
        .get(&db("current_attempt_epoch"))
        .and_then(crate::store::value_u64)?;
    let current = (epoch > 0)
        .then(|| store.attempt_node(&format!("{unit_id}/{epoch}")))
        .flatten()
        .and_then(|node| read.node_properties(node))
        .and_then(|properties| prop_str(properties, "attempt_id"));
    let mut lineage = vec![
        (CancelKind::Run, run_id),
        (CancelKind::ExecutionUnit, unit_id.to_owned()),
    ];
    if include_attempt && let Some(attempt_id) = current {
        lineage.push((CancelKind::Attempt, attempt_id));
    }
    Some(lineage)
}
