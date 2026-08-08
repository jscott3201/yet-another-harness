use super::*;
use crate::cancel::CancelKind;
use crate::effect::{EffectIntent, EffectState};
use selene_graph::SeleneGraph;

impl Store {
    pub(crate) fn cancel_root_terminal(
        &self,
        read: &SeleneGraph,
        root_kind: CancelKind,
        root_id: &str,
    ) -> Option<bool> {
        let status = |node| {
            read.node_properties(node)
                .and_then(|properties| properties.get(&db("status")).and_then(value_str))
        };
        match root_kind {
            CancelKind::Run => match status(self.run_node(root_id)?)?.as_str() {
                "open" | "active" | "reconciling" => Some(false),
                "closed_success" | "closed_failure" | "cancelled" => Some(true),
                _ => None,
            },
            CancelKind::ExecutionUnit => match status(self.unit_node(root_id)?)?.as_str() {
                "admitted"
                | "dispatched"
                | "running"
                | "awaiting_review"
                | "awaiting_integration" => Some(false),
                "settled_accepted" | "settled_rejected" | "cancelled" | "failed" | "abandoned" => {
                    Some(true)
                }
                _ => None,
            },
            CancelKind::Attempt => match status(self.attempt_id_node(root_id)?)?.as_str() {
                "active" => Some(false),
                "superseded" | "completed" | "cancelled" | "failed" | "unknown" => Some(true),
                _ => None,
            },
            CancelKind::EffectIntent => {
                let node = self.effect_intent_id_node(root_id)?;
                let record = read
                    .node_properties(node)
                    .and_then(|properties| properties.get(&db("record")).and_then(value_str))?;
                let intent: EffectIntent = serde_json::from_str(&record).ok()?;
                Some(intent.state == EffectState::Settled && intent.terminal.is_some())
            }
        }
    }
}
