//! Read-only store projections used by tests, audit, and Adapter 1.

use super::*;

impl Store {
    pub(crate) fn cancel_delivery_nodes(&self, cancel_request_id: &str) -> Vec<NodeId> {
        self.books
            .lock()
            .expect("books")
            .cancel_deliveries_by_request
            .get(cancel_request_id)
            .cloned()
            .unwrap_or_default()
    }

    pub(crate) fn cancel_root_node(&self, root_kind: &str, root_id: &str) -> Option<NodeId> {
        self.books
            .lock()
            .expect("books")
            .cancel_roots
            .get(&format!("{root_kind}/{root_id}"))
            .copied()
    }

    pub(crate) fn cancel_request_nodes(&self) -> Vec<NodeId> {
        self.books
            .lock()
            .expect("books")
            .cancel_requests
            .values()
            .copied()
            .collect()
    }

    pub(crate) fn validate_dispatch_claims(
        &self,
        claims: &AttemptTokenClaims,
        attempt_id: &str,
        unit_version: u64,
    ) -> Result<bool, StoreError> {
        let attempt = self
            .attempt_node(&format!("{}/{}", claims.unit_id, claims.attempt_epoch.0))
            .ok_or_else(|| StoreError::Internal("dispatch receipt has no Attempt row".into()))?;
        let unit = self
            .unit_node(&claims.unit_id)
            .ok_or_else(|| StoreError::Internal("dispatch receipt has no Unit row".into()))?;
        let lease = self
            .lease_node(&claims.unit_id)
            .ok_or_else(|| StoreError::Internal("dispatch receipt has no Lease row".into()))?;
        let txn = self.shared.begin_write();
        let read = txn.read();
        let attempt_props = read
            .node_properties(attempt)
            .ok_or_else(|| StoreError::Internal("dispatch Attempt row is unreadable".into()))?;
        let attempt_u64 = |field: &str| {
            attempt_props
                .get(&db(field))
                .and_then(value_u64)
                .ok_or_else(|| StoreError::Internal(format!("Attempt row has invalid {field}")))
        };
        let attempt_string = |field: &str| {
            attempt_props
                .get(&db(field))
                .and_then(value_str)
                .ok_or_else(|| StoreError::Internal(format!("Attempt row has invalid {field}")))
        };
        if attempt_string("attempt_id")? != attempt_id
            || attempt_string("unit_id")? != claims.unit_id
            || attempt_u64("attempt_epoch")? != claims.attempt_epoch.0
            || attempt_u64("stamp")? != claims.stamp.0
            || attempt_u64("authority_epoch")? != claims.authority_epoch.0
            || attempt_string("holder_id")? != claims.holder_id
            || attempt_string("token_nonce")? != claims.nonce
        {
            return Err(StoreError::Internal(
                "dispatch receipt claims disagree with its Attempt row".into(),
            ));
        }
        let unit_props = read
            .node_properties(unit)
            .ok_or_else(|| StoreError::Internal("dispatch Unit row is unreadable".into()))?;
        let lease_props = read
            .node_properties(lease)
            .ok_or_else(|| StoreError::Internal("dispatch Lease row is unreadable".into()))?;
        let stored_unit_version = unit_props.get(&db("version")).and_then(value_u64);
        if stored_unit_version.is_none_or(|version| version < unit_version) {
            return Err(StoreError::Internal(
                "dispatch receipt version exceeds its Unit row".into(),
            ));
        }
        let current = self.authority_epoch() == claims.authority_epoch
            && unit_props
                .get(&db("current_attempt_epoch"))
                .and_then(value_u64)
                == Some(claims.attempt_epoch.0)
            && unit_props.get(&db("stamp")).and_then(value_u64) == Some(claims.stamp.0)
            && lease_props.get(&db("attempt_epoch")).and_then(value_u64)
                == Some(claims.attempt_epoch.0)
            && lease_props
                .get(&db("holder_id"))
                .and_then(value_str)
                .as_deref()
                == Some(claims.holder_id.as_str())
            && lease_props
                .get(&db("status"))
                .and_then(value_str)
                .as_deref()
                == Some("active");
        txn.rollback();
        Ok(current)
    }

    #[cfg(test)]
    pub(crate) fn insert_test_event(&self, event: &EventRecord) {
        let mut txn = self.shared.begin_write();
        let node = txn
            .mutator()
            .create_node(
                LabelSet::single(db("Event")),
                PropertyMap::from_pairs([
                    (db("event_id"), Value::String(db(&event.event_id))),
                    (db("cursor"), Value::Uint(event.cursor)),
                    (
                        db("agg_ver_ord"),
                        Value::String(db(&format!(
                            "{}/{}/{}/{}",
                            event.aggregate_kind,
                            event.aggregate_id,
                            event.aggregate_version,
                            event.ordinal
                        ))),
                    ),
                    (
                        db("aggregate_kind"),
                        Value::String(db(&event.aggregate_kind)),
                    ),
                    (db("aggregate_id"), Value::String(db(&event.aggregate_id))),
                    (
                        db("aggregate_version"),
                        Value::Uint(event.aggregate_version),
                    ),
                    (db("ordinal"), Value::Uint(event.ordinal)),
                    (db("event_kind"), Value::String(db(&event.event_kind))),
                    (db("payload"), Value::String(db(&event.payload))),
                    (db("receipt_key"), Value::String(db(&event.receipt_key))),
                    (db("command_id"), Value::String(db(&event.command_id))),
                    (db("actor_kind"), Value::String(db(&event.actor_kind))),
                    (db("actor_id"), Value::String(db(&event.actor_id))),
                    (db("occurred_at_ms"), Value::Uint(event.occurred_at_ms)),
                ])
                .unwrap(),
            )
            .unwrap();
        txn.commit().unwrap();
        self.book_insert(BookKind::Event, event.cursor.to_string(), node);
    }

    #[cfg(test)]
    pub(crate) fn insert_test_receipt(&self, key: &str, receipt: &ReceiptRecord) {
        let mut txn = self.shared.begin_write();
        let mut pairs = vec![
            (db("receipt_key"), Value::String(db(key))),
            (db("command_type"), Value::String(db(&receipt.command_type))),
            (db("receipt_version"), Value::Uint(receipt.receipt_version)),
            (
                db("request_digest"),
                Value::String(db(&receipt.request_digest)),
            ),
            (
                db("principal_kind"),
                Value::String(db(&receipt.principal_kind)),
            ),
            (db("principal_id"), Value::String(db(&receipt.principal_id))),
            (db("status"), Value::String(db(&receipt.status))),
            (db("result"), Value::String(db(&receipt.result))),
        ];
        if let Some(first) = receipt.first_cursor {
            pairs.push((db("first_cursor"), Value::Uint(first)));
        }
        if let Some(last) = receipt.last_cursor {
            pairs.push((db("last_cursor"), Value::Uint(last)));
        }
        let node = txn
            .mutator()
            .create_node(
                LabelSet::single(db("Receipt")),
                PropertyMap::from_pairs(pairs).unwrap(),
            )
            .unwrap();
        txn.commit().unwrap();
        self.book_insert(BookKind::Receipt, key.to_owned(), node);
    }

    pub fn attempt_status(&self, unit_id: &str, epoch: u64) -> Option<String> {
        let node = self.attempt_node(&format!("{unit_id}/{epoch}"))?;
        let txn = self.shared.begin_write();
        let status = txn
            .read()
            .node_properties(node)
            .and_then(|p| p.get(&db("status")).and_then(value_str));
        txn.rollback();
        status
    }

    pub fn effect_record(&self, operation_key: &str) -> Option<EffectRecordRow> {
        let node = self.effect_node(operation_key)?;
        let txn = self.shared.begin_write();
        let row = txn.read().node_properties(node).map(|p| EffectRecordRow {
            version: p.get(&db("version")).and_then(value_u64).unwrap_or(0),
            state: p.get(&db("state")).and_then(value_str).unwrap_or_default(),
            terminal: p.get(&db("terminal")).and_then(value_str),
            record: p.get(&db("record")).and_then(value_str).unwrap_or_default(),
        });
        txn.rollback();
        row
    }

    pub fn cancel_request_row(&self, cancel_request_id: &str) -> Option<CancelRequestRow> {
        let node = self.cancel_request_node(cancel_request_id)?;
        let txn = self.shared.begin_write();
        let row = txn.read().node_properties(node).map(|p| CancelRequestRow {
            version: p.get(&db("version")).and_then(value_u64).unwrap_or(0),
            status: p.get(&db("status")).and_then(value_str).unwrap_or_default(),
            root_kind: p
                .get(&db("root_kind"))
                .and_then(value_str)
                .unwrap_or_default(),
            root_id: p
                .get(&db("root_id"))
                .and_then(value_str)
                .unwrap_or_default(),
            scope: p.get(&db("scope")).and_then(value_str).unwrap_or_default(),
            record: p.get(&db("record")).and_then(value_str).unwrap_or_default(),
        });
        txn.rollback();
        row
    }

    pub fn cancel_delivery_rows(&self) -> Vec<CancelDeliveryRow> {
        let txn = self.shared.begin_write();
        let mut out = Vec::new();
        {
            let read = txn.read();
            for (_, node) in self.cancel_delivery_entries() {
                let Some(p) = read.node_properties(node) else {
                    continue;
                };
                let get_s = |k: &str| p.get(&db(k)).and_then(value_str).unwrap_or_default();
                out.push(CancelDeliveryRow {
                    delivery_key: get_s("delivery_key"),
                    cancel_request_id: get_s("cancel_request_id"),
                    member_id: get_s("member_id"),
                    member_kind: get_s("member_kind"),
                    outcome: get_s("outcome"),
                });
            }
        }
        txn.rollback();
        out
    }

    pub fn journal(&self) -> Result<Vec<EventRecord>, StoreError> {
        self.events_after(0)
    }

    pub fn events_after(&self, cursor: u64) -> Result<Vec<EventRecord>, StoreError> {
        self.events_after_limit(cursor, usize::MAX)
    }

    pub fn events_after_limit(
        &self,
        cursor: u64,
        limit: usize,
    ) -> Result<Vec<EventRecord>, StoreError> {
        let nodes: Vec<(u64, NodeId)> = self
            .books
            .lock()
            .expect("books")
            .events
            .range((
                std::ops::Bound::Excluded(cursor),
                std::ops::Bound::Unbounded,
            ))
            .take(limit)
            .map(|(cursor, node)| (*cursor, *node))
            .collect();
        let read = self.shared.read();
        let mut out = Vec::with_capacity(nodes.len());
        for (indexed_cursor, node) in nodes {
            let p = read.node_properties(node).ok_or_else(|| {
                StoreError::Internal(format!("event at cursor {indexed_cursor} is unreadable"))
            })?;
            let get_u = |k: &str| {
                p.get(&db(k)).and_then(value_u64).ok_or_else(|| {
                    StoreError::Internal(format!(
                        "event at cursor {indexed_cursor} has invalid {k}"
                    ))
                })
            };
            let get_s = |k: &str| {
                p.get(&db(k)).and_then(value_str).ok_or_else(|| {
                    StoreError::Internal(format!(
                        "event at cursor {indexed_cursor} has invalid {k}"
                    ))
                })
            };
            let stored_cursor = get_u("cursor")?;
            if stored_cursor != indexed_cursor {
                return Err(StoreError::Internal(format!(
                    "event book cursor {indexed_cursor} disagrees with stored cursor {stored_cursor}"
                )));
            }
            let optional_string = |k: &str| match p.get(&db(k)) {
                Some(value) => value_str(value).map(Some).ok_or_else(|| {
                    StoreError::Internal(format!(
                        "event at cursor {indexed_cursor} has invalid {k}"
                    ))
                }),
                None => Ok(None),
            };
            let aggregate_kind = get_s("aggregate_kind")?;
            let aggregate_id = get_s("aggregate_id")?;
            let aggregate_version = get_u("aggregate_version")?;
            let ordinal = get_u("ordinal")?;
            let composite = get_s("agg_ver_ord")?;
            let expected_composite =
                format!("{aggregate_kind}/{aggregate_id}/{aggregate_version}/{ordinal}");
            if composite != expected_composite {
                return Err(StoreError::Internal(format!(
                    "event at cursor {indexed_cursor} has inconsistent agg_ver_ord"
                )));
            }
            out.push(EventRecord {
                cursor: stored_cursor,
                event_id: get_s("event_id")?,
                aggregate_kind,
                aggregate_id,
                aggregate_version,
                ordinal,
                event_kind: get_s("event_kind")?,
                payload: get_s("payload")?,
                receipt_key: get_s("receipt_key")?,
                command_id: get_s("command_id")?,
                actor_kind: get_s("actor_kind")?,
                actor_id: get_s("actor_id")?,
                occurred_at_ms: get_u("occurred_at_ms")?,
                causation_id: optional_string("causation_id")?,
                correlation_id: optional_string("correlation_id")?,
            });
        }
        Ok(out)
    }

    pub fn latest_cursor(&self) -> u64 {
        self.books
            .lock()
            .expect("books")
            .events
            .last_key_value()
            .map(|(cursor, _)| *cursor)
            .unwrap_or(0)
    }

    pub fn receipt(&self, key: &str) -> Result<Option<ReceiptRecord>, StoreError> {
        let Some(node) = self.receipt_node(key) else {
            return Ok(None);
        };
        let record = {
            let read = self.shared.read();
            let properties = read
                .node_properties(node)
                .ok_or_else(|| StoreError::Internal(format!("receipt {key} is unreadable")))?;
            receipt_record(properties)?
        };
        self.validate_receipt_events(key, &record)?;
        Ok(Some(record))
    }

    pub(crate) fn receipt_keys(&self) -> Vec<String> {
        self.books
            .lock()
            .expect("books")
            .receipts
            .keys()
            .cloned()
            .collect()
    }

    pub(crate) fn validate_event_receipts(&self) -> Result<(), StoreError> {
        let mut cursor = 0;
        loop {
            let events = self.events_after_limit(cursor, crate::protocol::MAX_RESUME_EVENTS)?;
            if events.is_empty() {
                return Ok(());
            }
            for event in events {
                cursor = event.cursor;
                let receipt = self.receipt(&event.receipt_key)?.ok_or_else(|| {
                    StoreError::Internal(format!(
                        "event at cursor {} has no owning receipt",
                        event.cursor
                    ))
                })?;
                if receipt.status != "completed"
                    || !receipt
                        .first_cursor
                        .zip(receipt.last_cursor)
                        .is_some_and(|(first, last)| (first..=last).contains(&event.cursor))
                    || event.actor_kind != receipt.principal_kind
                    || event.actor_id != receipt.principal_id
                {
                    return Err(StoreError::Internal(format!(
                        "event at cursor {} is outside its owning receipt",
                        event.cursor
                    )));
                }
            }
        }
    }

    fn validate_receipt_events(
        &self,
        key: &str,
        receipt: &ReceiptRecord,
    ) -> Result<(), StoreError> {
        let Some((first, last)) = receipt.first_cursor.zip(receipt.last_cursor) else {
            return Ok(());
        };
        let count = last
            .checked_sub(first)
            .and_then(|distance| distance.checked_add(1))
            .and_then(|count| usize::try_from(count).ok())
            .filter(|count| *count <= crate::protocol::MAX_RESUME_EVENTS)
            .ok_or_else(|| StoreError::Internal("receipt event cursor range is invalid".into()))?;
        let command_id = key
            .rsplit('/')
            .next()
            .filter(|command_id| !command_id.is_empty())
            .ok_or_else(|| StoreError::Internal("receipt key has no command id".into()))?;
        let events = self.events_after_limit(first - 1, count)?;
        if events.len() != count
            || events.iter().enumerate().any(|(offset, event)| {
                event.cursor != first + offset as u64
                    || event.receipt_key != key
                    || event.command_id != command_id
            })
        {
            return Err(StoreError::Internal(
                "receipt event cursors do not identify its command events".into(),
            ));
        }
        Ok(())
    }
}

pub(super) fn receipt_record(p: &PropertyMap) -> Result<ReceiptRecord, StoreError> {
    let string = |field: &str| {
        p.get(&db(field))
            .and_then(value_str)
            .ok_or_else(|| StoreError::Internal(format!("receipt has invalid {field}")))
    };
    let optional_u64 = |field: &str| match p.get(&db(field)) {
        Some(value) => value_u64(value)
            .map(Some)
            .ok_or_else(|| StoreError::Internal(format!("receipt has invalid {field}"))),
        None => Ok(None),
    };
    let first_cursor = optional_u64("first_cursor")?;
    let last_cursor = optional_u64("last_cursor")?;
    if first_cursor.is_some() != last_cursor.is_some() {
        return Err(StoreError::Internal(
            "receipt has an incomplete event cursor range".into(),
        ));
    }
    let status = string("status")?;
    let result = string("result")?;
    if status != "completed" && status != "rejected" {
        return Err(StoreError::Internal("receipt has invalid status".into()));
    }
    if status == "rejected" && first_cursor.is_some() {
        return Err(StoreError::Internal(
            "rejected receipt has event cursors".into(),
        ));
    }
    if result.len() > crate::protocol::MAX_RESULT_BYTES
        || !matches!(
            serde_json::from_str(&result),
            Ok(serde_json::Value::Object(_))
        )
    {
        return Err(StoreError::Internal("receipt has invalid result".into()));
    }
    if let Some(first) = first_cursor
        && first == 0
    {
        return Err(StoreError::Internal(
            "receipt event cursor range starts at zero".into(),
        ));
    }
    let command_type = string("command_type")?;
    let receipt_version = p
        .get(&db("receipt_version"))
        .and_then(value_u64)
        .ok_or_else(|| StoreError::Internal("receipt has invalid receipt_version".into()))?;
    if !super::receipt::valid_command_type(&command_type)
        || receipt_version != u64::from(crate::protocol::RECEIPT_VERSION.get())
    {
        return Err(StoreError::Internal(
            "receipt has unsupported command_type or receipt_version".into(),
        ));
    }
    Ok(ReceiptRecord {
        command_type,
        receipt_version,
        request_digest: string("request_digest")?,
        principal_kind: string("principal_kind")?,
        principal_id: string("principal_id")?,
        status,
        result,
        first_cursor,
        last_cursor,
    })
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EffectRecordRow {
    pub version: u64,
    pub state: String,
    pub terminal: Option<String>,
    pub record: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CancelRequestRow {
    pub version: u64,
    pub status: String,
    pub root_kind: String,
    pub root_id: String,
    pub scope: String,
    pub record: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CancelDeliveryRow {
    pub delivery_key: String,
    pub cancel_request_id: String,
    pub member_id: String,
    pub member_kind: String,
    pub outcome: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EventRecord {
    pub cursor: u64,
    pub event_id: String,
    pub aggregate_kind: String,
    pub aggregate_id: String,
    pub aggregate_version: u64,
    pub ordinal: u64,
    pub event_kind: String,
    pub payload: String,
    pub receipt_key: String,
    pub command_id: String,
    pub actor_kind: String,
    pub actor_id: String,
    pub occurred_at_ms: u64,
    pub causation_id: Option<String>,
    pub correlation_id: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReceiptRecord {
    pub command_type: String,
    pub receipt_version: u64,
    pub request_digest: String,
    pub principal_kind: String,
    pub principal_id: String,
    pub status: String,
    pub result: String,
    pub first_cursor: Option<u64>,
    pub last_cursor: Option<u64>,
}
