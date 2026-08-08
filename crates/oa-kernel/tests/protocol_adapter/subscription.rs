use super::*;

#[test]
fn resume_is_bounded_and_subscriptions_are_capped() {
    let (_dir, adapter) = adapter();
    append_events(&adapter, 1_025);
    let resume = serde_json::to_vec(&oa_kernel::protocol::ClientMessage::Resume {
        project_id: "p-1".into(),
        after_cursor: DecimalU64::new(0),
    })
    .unwrap();
    let response: ServerMessage = serde_json::from_slice(&adapter.handle_json(&resume)).unwrap();
    let ServerMessage::Error(error) = response else {
        panic!("unbounded resume must be rejected")
    };
    assert_eq!(error.kind, ErrorKind::ResourceExhausted);

    for _ in 0..oa_kernel::protocol::MAX_DURABLE_SUBSCRIPTIONS {
        adapter.subscribe(1_025).unwrap();
    }
    assert_eq!(
        adapter.subscribe(1_025).unwrap_err().kind,
        ErrorKind::ResourceExhausted
    );
}

#[test]
fn cursor_before_retention_floor_expires_without_partial_stream() {
    let (_dir, adapter) = adapter();
    append_events(&adapter, 12);
    adapter.set_min_retained_cursor(3).unwrap();

    let expired = adapter.subscribe(2).unwrap_err();
    assert_eq!(expired.kind, ErrorKind::CursorExpired);
    assert_eq!(
        adapter.resume(2).unwrap_err().kind,
        ErrorKind::CursorExpired
    );

    let retained = adapter.subscribe(3).unwrap();
    assert_eq!(retained.retention.min_retained_cursor, DecimalU64::new(3));
    assert_eq!(
        retained.retention.max_age_seconds,
        DecimalU64::new(1_209_600)
    );
    assert_eq!(
        retained.retention.max_events_per_project,
        DecimalU64::new(1_000_000)
    );
    for expected_cursor in 4..=12 {
        let SubscriptionPoll::Event(event) = adapter
            .poll_subscription(&retained.subscription_id)
            .unwrap()
        else {
            panic!("retained boundary must deliver cursor {expected_cursor}")
        };
        assert_eq!(event.cursor, DecimalU64::new(expected_cursor));
    }
    assert_eq!(
        adapter
            .poll_subscription(&retained.subscription_id)
            .unwrap(),
        SubscriptionPoll::Pending
    );
    adapter
        .close_subscription(&retained.subscription_id)
        .unwrap();
}

#[test]
fn slow_durable_consumer_closes_without_blocking_producers_and_resumes() {
    let (_dir, adapter) = adapter();
    assert_eq!(
        adapter.limits().durable_event_queue_capacity,
        BoundedU32::new(1024)
    );
    assert_eq!(
        adapter.limits().progress_event_queue_capacity,
        BoundedU32::new(256)
    );
    let subscription = adapter.subscribe(0).unwrap();

    append_events(&adapter, 2);
    let SubscriptionPoll::Event(first) = adapter
        .poll_subscription(&subscription.subscription_id)
        .unwrap()
    else {
        panic!("first queued event must remain deliverable")
    };
    assert_eq!(first.cursor, DecimalU64::new(1));

    let overflow_at = DEFAULT_DURABLE_QUEUE_CAPACITY as u128 + 2;
    for index in 3..=overflow_at {
        let run_id = format!("run-{index}");
        let open = command(
            index,
            CommandType::RunOpen,
            "run",
            &run_id,
            json!({ "run_id": run_id, "goal_work_item_id": "wi-goal" }),
            None,
        );
        assert_eq!(
            adapter.submit(&open).unwrap().outcome,
            ReceiptOutcome::Completed
        );
    }
    let SubscriptionPoll::SlowConsumer(condition) = adapter
        .poll_subscription(&subscription.subscription_id)
        .unwrap()
    else {
        panic!("overflow must close the durable subscription")
    };
    assert_eq!(condition.last_delivered_cursor, DecimalU64::new(1));

    let resumed = adapter
        .subscribe(condition.last_delivered_cursor.get())
        .unwrap();
    for expected_cursor in 2..=overflow_at as u64 {
        let SubscriptionPoll::Event(event) =
            adapter.poll_subscription(&resumed.subscription_id).unwrap()
        else {
            panic!("retained catch-up must stay live at cursor {expected_cursor}")
        };
        assert_eq!(event.cursor, DecimalU64::new(expected_cursor));
    }
    assert_eq!(
        adapter.poll_subscription(&resumed.subscription_id).unwrap(),
        SubscriptionPoll::Pending
    );
}

#[test]
fn typed_subscription_helpers_return_routine_errors() {
    let (_dir, adapter) = adapter();
    assert_eq!(
        adapter.subscribe(1).unwrap_err().kind,
        ErrorKind::InvalidRequest
    );
    assert_eq!(
        adapter.poll_subscription("missing").unwrap_err().kind,
        ErrorKind::NotFound
    );
    assert_eq!(
        adapter.close_subscription("missing").unwrap_err().kind,
        ErrorKind::NotFound
    );
    assert_eq!(adapter.set_min_retained_cursor(1), Ok(()));
}

#[test]
fn retention_floor_survives_store_recovery() {
    let (dir, adapter) = adapter();
    append_events(&adapter, 10);
    adapter.set_min_retained_cursor(10).unwrap();
    drop(adapter);

    let recovered = Store::recover(dir.path(), "adapter-b").unwrap();
    let adapter = InProcessAdapter::new(Funnel::new(recovered, 2_000).unwrap(), "p-1").unwrap();
    assert_eq!(
        adapter.resume(9).unwrap_err().kind,
        ErrorKind::CursorExpired
    );
}
