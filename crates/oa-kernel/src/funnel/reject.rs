use super::*;

impl Funnel {
    pub(crate) fn reject_keyed(
        &self,
        cmd: &Command,
        command_type: &str,
        kind: ErrorKind,
        detail: &str,
    ) -> Submission {
        let mut gate = self.gate.lock().expect("funnel gate");
        if let Some(poison) = gate.as_ref() {
            return Submission::Rejected {
                kind: ErrorKind::Unavailable,
                detail: format!("funnel poisoned by uncertain commit: {poison}"),
                replayed: false,
            };
        }
        if !receipt::identifiers_are_valid(cmd)
            || !receipt::project_scope_is_valid(cmd, self.store.project_id())
        {
            return Submission::Rejected {
                kind: ErrorKind::InvalidRequest,
                detail:
                    "receipt scope and command identifiers must use the wire identifier grammar"
                        .into(),
                replayed: false,
            };
        }
        let receipt_key = format!(
            "{}/{}/{}",
            cmd.scope_kind.wire(),
            cmd.scope_id,
            cmd.command_id
        );
        let mut txn = self.store.shared().begin_write();
        if let Some(node) = self.store.receipt_node(&receipt_key) {
            let stored = txn.read().node_properties(node).map(|properties| {
                (
                    properties.get(&db("command_type")).and_then(value_str),
                    properties.get(&db("receipt_version")).and_then(value_u64),
                    properties.get(&db("request_digest")).and_then(value_str),
                    properties.get(&db("principal_kind")).and_then(value_str),
                    properties.get(&db("principal_id")).and_then(value_str),
                    properties.get(&db("status")).and_then(value_str),
                    properties.get(&db("result")).and_then(value_str),
                )
            });
            txn.rollback();
            return Self::replay_stored(cmd, command_type, stored);
        }
        let detail = wire::bounded_detail(detail);
        let result = json!({ "error_kind": kind, "detail": detail });
        let node = txn.mutator().create_node(
            LabelSet::single(db("Receipt")),
            receipt::properties(cmd, command_type, &receipt_key, "rejected", &result, None),
        );
        let persistence_error = match node {
            Ok(node) => match txn.commit() {
                Ok(_) => {
                    self.store.book_insert(BookKind::Receipt, receipt_key, node);
                    None
                }
                Err(error) => {
                    if let StoreError::CommitUnknown(detail) = crate::store::commit_error(error) {
                        *gate = Some(detail);
                        None
                    } else {
                        Some("protocol rejection commit failed before durability".to_owned())
                    }
                }
            },
            Err(error) => {
                txn.rollback();
                Some(format!("cannot stage protocol rejection: {error:?}"))
            }
        };
        if let Some(detail) = persistence_error {
            return Submission::Rejected {
                kind: ErrorKind::Internal,
                detail,
                replayed: false,
            };
        }
        Submission::Rejected {
            kind,
            detail,
            replayed: false,
        }
    }
}
