use super::{DurableSubscription, InProcessAdapter, SubscriptionClosure, protocol_error};
use crate::protocol::{
    AdapterLimits, BoundedU32, ClientMessage, CursorExpired, DecimalU64, ErrorKind, Event,
    Retention, ServerMessage, SlowConsumer, SubscriptionClosed, SubscriptionOpened,
    SubscriptionPending, SubscriptionPoll,
};
use std::collections::VecDeque;
use std::sync::Arc;

impl InProcessAdapter {
    pub fn limits(&self) -> AdapterLimits {
        AdapterLimits {
            durable_event_queue_capacity: BoundedU32::new(
                crate::protocol::DEFAULT_DURABLE_QUEUE_CAPACITY as u32,
            ),
            progress_event_queue_capacity: BoundedU32::new(
                crate::protocol::DEFAULT_PROGRESS_QUEUE_CAPACITY as u32,
            ),
        }
    }

    pub fn retention(&self) -> Retention {
        Retention {
            min_retained_cursor: DecimalU64::new(self.funnel.store().min_retained_cursor()),
            max_age_seconds: DecimalU64::new(crate::protocol::DEFAULT_MAX_AGE_SECONDS),
            max_events_per_project: DecimalU64::new(
                crate::protocol::DEFAULT_MAX_EVENTS_PER_PROJECT,
            ),
        }
    }

    pub fn subscribe(
        &self,
        after_cursor: u64,
    ) -> Result<SubscriptionOpened, crate::protocol::Error> {
        let request = serde_json::to_vec(&ClientMessage::Subscribe {
            project_id: self.project_id.clone(),
            after_cursor: DecimalU64::new(after_cursor),
        })
        .expect("subscribe serializes");
        match serde_json::from_slice(&self.handle_json(&request)).expect("valid response") {
            ServerMessage::SubscriptionOpened(opened) => Ok(opened),
            ServerMessage::CursorExpired(expired) => Err(protocol_error(
                ErrorKind::CursorExpired,
                &format!(
                    "cursor is older than retained floor {}",
                    expired.min_retained_cursor.get()
                ),
            )),
            ServerMessage::Error(error) => Err(error),
            other => Err(protocol_error(
                ErrorKind::Internal,
                &format!("subscribe returned unexpected response: {other:?}"),
            )),
        }
    }

    pub fn poll_subscription(
        &self,
        subscription_id: &str,
    ) -> Result<SubscriptionPoll, crate::protocol::Error> {
        let request = serde_json::to_vec(&ClientMessage::SubscriptionPoll {
            subscription_id: subscription_id.to_owned(),
        })
        .expect("subscription poll serializes");
        match serde_json::from_slice(&self.handle_json(&request)).expect("valid response") {
            ServerMessage::Event(event) => Ok(SubscriptionPoll::Event(Box::new(event))),
            ServerMessage::SubscriptionPending(_) => Ok(SubscriptionPoll::Pending),
            ServerMessage::SlowConsumer(condition) => Ok(SubscriptionPoll::SlowConsumer(condition)),
            ServerMessage::Error(error) => Err(error),
            other => Err(protocol_error(
                ErrorKind::Internal,
                &format!("subscription poll returned unexpected response: {other:?}"),
            )),
        }
    }

    pub fn close_subscription(&self, subscription_id: &str) -> Result<(), crate::protocol::Error> {
        let request = serde_json::to_vec(&ClientMessage::SubscriptionClose {
            subscription_id: subscription_id.to_owned(),
        })
        .expect("subscription close serializes");
        match serde_json::from_slice(&self.handle_json(&request)).expect("valid response") {
            ServerMessage::SubscriptionClosed(_) => Ok(()),
            ServerMessage::Error(error) => Err(error),
            other => Err(protocol_error(
                ErrorKind::Internal,
                &format!("subscription close returned unexpected response: {other:?}"),
            )),
        }
    }

    pub(super) fn handle_subscribe(&self, project_id: &str, after_cursor: u64) -> ServerMessage {
        if project_id != self.project_id {
            return ServerMessage::Error(protocol_error(
                ErrorKind::InvalidRequest,
                "subscription belongs to a different project",
            ));
        }
        let _stream = self.stream_gate.lock().expect("stream gate");
        if let Some(detail) = self.funnel.poison_detail() {
            return ServerMessage::Error(protocol_error(ErrorKind::Unavailable, &detail));
        }
        let mut subscriptions = self.subscriptions.lock().expect("subscription registry");
        if subscriptions.len() >= crate::protocol::MAX_DURABLE_SUBSCRIPTIONS {
            return ServerMessage::Error(protocol_error(
                ErrorKind::ResourceExhausted,
                "project already has 64 open durable subscriptions",
            ));
        }
        let min_retained_cursor = self.funnel.store().min_retained_cursor();
        if min_retained_cursor > 1 && after_cursor < min_retained_cursor {
            return ServerMessage::CursorExpired(CursorExpired {
                min_retained_cursor: DecimalU64::new(min_retained_cursor),
            });
        }
        let available_cursor = self.latest_cursor();
        if after_cursor > available_cursor {
            return ServerMessage::Error(protocol_error(
                ErrorKind::InvalidRequest,
                "cursor is ahead of the project journal",
            ));
        }
        let mut next_id = self
            .next_subscription_id
            .lock()
            .expect("subscription sequence");
        let Some(sequence) = next_id.checked_add(1) else {
            return ServerMessage::Error(protocol_error(
                ErrorKind::ResourceExhausted,
                "subscription identifier space exhausted",
            ));
        };
        *next_id = sequence;
        let subscription_id = format!("subscription-{next_id}");
        let mut subscription = DurableSubscription {
            queue: VecDeque::new(),
            queued_bytes: 0,
            last_queued_cursor: after_cursor,
            last_delivered_cursor: after_cursor,
            available_cursor,
            closed: None,
        };
        let mut total_queued_bytes = queued_bytes(&subscriptions);
        self.fill_retained(&mut subscription, &mut total_queued_bytes);
        subscriptions.insert(subscription_id.clone(), subscription);
        ServerMessage::SubscriptionOpened(SubscriptionOpened {
            subscription_id,
            limits: self.limits(),
            retention: self.retention(),
        })
    }

    pub(super) fn handle_subscription_poll(&self, subscription_id: &str) -> ServerMessage {
        let _stream = self.stream_gate.lock().expect("stream gate");
        if let Some(detail) = self.funnel.poison_detail() {
            return ServerMessage::Error(protocol_error(ErrorKind::Unavailable, &detail));
        }
        let mut subscriptions = self.subscriptions.lock().expect("subscription registry");
        if let Some(closure) = subscriptions
            .get_mut(subscription_id)
            .and_then(|subscription| subscription.closed.take())
        {
            subscriptions.remove(subscription_id);
            return match closure {
                SubscriptionClosure::SlowConsumer(condition) => {
                    ServerMessage::SlowConsumer(condition)
                }
                SubscriptionClosure::Internal(detail) => {
                    ServerMessage::Error(protocol_error(ErrorKind::Internal, &detail))
                }
            };
        }
        let mut total_queued_bytes = queued_bytes(&subscriptions);
        let Some(subscription) = subscriptions.get_mut(subscription_id) else {
            return ServerMessage::Error(protocol_error(
                ErrorKind::NotFound,
                "unknown subscription",
            ));
        };
        let Some((event, event_bytes)) = subscription.queue.pop_front() else {
            return ServerMessage::SubscriptionPending(SubscriptionPending {
                subscription_id: subscription_id.to_owned(),
            });
        };
        subscription.queued_bytes -= event_bytes;
        total_queued_bytes -= event_bytes;
        subscription.last_delivered_cursor = event.cursor.get();
        self.fill_retained(subscription, &mut total_queued_bytes);
        ServerMessage::Event((*event).clone())
    }

    pub(super) fn handle_subscription_close(&self, subscription_id: &str) -> ServerMessage {
        let _stream = self.stream_gate.lock().expect("stream gate");
        if let Some(detail) = self.funnel.poison_detail() {
            return ServerMessage::Error(protocol_error(ErrorKind::Unavailable, &detail));
        }
        if self
            .subscriptions
            .lock()
            .expect("subscription registry")
            .remove(subscription_id)
            .is_none()
        {
            return ServerMessage::Error(protocol_error(
                ErrorKind::NotFound,
                "unknown subscription",
            ));
        }
        ServerMessage::SubscriptionClosed(SubscriptionClosed {
            subscription_id: subscription_id.to_owned(),
        })
    }

    pub(super) fn publish_through(&self, target_cursor: u64) {
        let _stream = self.stream_gate.lock().expect("stream gate");
        while *self.published_cursor.lock().expect("published cursor") < target_cursor {
            let published_cursor = *self.published_cursor.lock().expect("published cursor");
            let events = match self
                .funnel
                .store()
                .events_after_limit(published_cursor, crate::protocol::MAX_RESUME_EVENTS)
            {
                Ok(events) => events,
                Err(error) => {
                    self.close_internal(format!("durable event stream is unreadable: {error:?}"));
                    return;
                }
            };
            if events.is_empty() {
                self.close_internal(format!(
                    "durable event stream ended before committed cursor {target_cursor}"
                ));
                return;
            }
            let min_retained_cursor = self.funnel.store().min_retained_cursor();
            let mut subscriptions = self.subscriptions.lock().expect("subscription registry");
            let mut total_queued_bytes = queued_bytes(&subscriptions);
            for event in events {
                let event = match self.event(event) {
                    Ok(event) => event,
                    Err(detail) => {
                        drop(subscriptions);
                        self.close_internal(detail);
                        return;
                    }
                };
                *self.published_cursor.lock().expect("published cursor") = event.cursor.get();
                let event = Arc::new(event);
                let event_bytes = event_bytes(&event);
                for subscription in subscriptions.values_mut() {
                    subscription.available_cursor = event.cursor.get();
                    self.enqueue_event(
                        subscription,
                        Arc::clone(&event),
                        event_bytes,
                        min_retained_cursor,
                        &mut total_queued_bytes,
                    );
                }
            }
        }
    }

    fn fill_retained(
        &self,
        subscription: &mut DurableSubscription,
        total_queued_bytes: &mut usize,
    ) {
        if subscription.closed.is_some()
            || subscription.queue.len() == crate::protocol::DEFAULT_DURABLE_QUEUE_CAPACITY
            || subscription.last_queued_cursor >= subscription.available_cursor
        {
            return;
        }
        let remaining = crate::protocol::DEFAULT_DURABLE_QUEUE_CAPACITY - subscription.queue.len();
        let events = match self
            .funnel
            .store()
            .events_after_limit(subscription.last_queued_cursor, remaining)
        {
            Ok(events) => events,
            Err(error) => {
                let detail = format!("durable event stream is unreadable: {error:?}");
                self.funnel.poison(detail.clone());
                subscription.closed = Some(SubscriptionClosure::Internal(detail));
                return;
            }
        };
        for record in events {
            let event = match self.event(record) {
                Ok(event) => event,
                Err(detail) => {
                    self.funnel.poison(detail.clone());
                    subscription.closed = Some(SubscriptionClosure::Internal(detail));
                    return;
                }
            };
            if event.cursor.get() > subscription.available_cursor {
                break;
            }
            let event = Arc::new(event);
            let event_bytes = event_bytes(&event);
            self.enqueue_event(
                subscription,
                event,
                event_bytes,
                self.funnel.store().min_retained_cursor(),
                total_queued_bytes,
            );
            if subscription.closed.is_some() {
                return;
            }
        }
    }

    fn enqueue_event(
        &self,
        subscription: &mut DurableSubscription,
        event: Arc<Event>,
        event_bytes: usize,
        min_retained_cursor: u64,
        total_queued_bytes: &mut usize,
    ) {
        if subscription.closed.is_some() || event.cursor.get() <= subscription.last_queued_cursor {
            return;
        }
        if subscription.queue.len() == crate::protocol::DEFAULT_DURABLE_QUEUE_CAPACITY
            || total_queued_bytes.saturating_add(event_bytes)
                > crate::protocol::MAX_DURABLE_SUBSCRIPTION_BYTES
        {
            *total_queued_bytes -= subscription.queued_bytes;
            subscription.queue.clear();
            subscription.queued_bytes = 0;
            subscription.closed = Some(SubscriptionClosure::SlowConsumer(SlowConsumer {
                min_retained_cursor: DecimalU64::new(min_retained_cursor),
                last_delivered_cursor: DecimalU64::new(subscription.last_delivered_cursor),
            }));
            return;
        }
        subscription.last_queued_cursor = event.cursor.get();
        subscription.queued_bytes += event_bytes;
        *total_queued_bytes += event_bytes;
        subscription.queue.push_back((event, event_bytes));
    }

    fn close_internal(&self, detail: String) {
        self.funnel.poison(detail.clone());
        for subscription in self
            .subscriptions
            .lock()
            .expect("subscription registry")
            .values_mut()
        {
            subscription.closed = Some(SubscriptionClosure::Internal(detail.clone()));
        }
    }
}

fn event_bytes(event: &Event) -> usize {
    serde_json::to_vec(event)
        .expect("projected event serializes")
        .len()
}

fn queued_bytes(subscriptions: &std::collections::BTreeMap<String, DurableSubscription>) -> usize {
    subscriptions
        .values()
        .map(|subscription| subscription.queued_bytes)
        .sum()
}
