use super::*;

impl Funnel {
    pub(crate) fn reject_keyed(&self, cmd: &Command, kind: ErrorKind, detail: &str) -> Submission {
        let mut gate = self.gate.lock().expect("funnel gate");
        if let Some(poison) = gate.as_ref() {
            return Submission::Rejected {
                kind: ErrorKind::Unavailable,
                detail: format!("funnel poisoned by uncertain commit: {poison}"),
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
                    properties.get(&db("request_digest")).and_then(value_str),
                    properties.get(&db("principal_kind")).and_then(value_str),
                    properties.get(&db("principal_id")).and_then(value_str),
                    properties.get(&db("status")).and_then(value_str),
                    properties.get(&db("result")).and_then(value_str),
                )
            });
            txn.rollback();
            return Self::replay_stored(cmd, stored);
        }
        let detail = wire::bounded_detail(detail);
        let result = json!({ "error_kind": kind, "detail": detail });
        let node = txn.mutator().create_node(
            LabelSet::single(db("Receipt")),
            PropertyMap::from_pairs([
                (db("receipt_key"), Value::String(db(&receipt_key))),
                (
                    db("request_digest"),
                    Value::String(db(cmd.request_digest.as_str())),
                ),
                (
                    db("principal_kind"),
                    Value::String(db(cmd.principal_kind.wire())),
                ),
                (db("principal_id"), Value::String(db(&cmd.principal_id))),
                (db("status"), Value::String(db("rejected"))),
                (db("result"), Value::String(db(&result.to_string()))),
            ])
            .expect("protocol rejection receipt property map"),
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
