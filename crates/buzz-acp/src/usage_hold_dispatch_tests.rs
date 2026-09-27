#[tokio::test]
async fn notice_storage_failure_keeps_request_and_loop_alive() {
    let rest = pool::test_prompt_context().rest_client;
    let rest = relay::RestClient { keys: nostr::Keys::generate(), ..rest };
    let path = notice_outbox::directory(&rest).unwrap();
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    // A regular file at the unique outbox path makes create_dir_all fail.
    std::fs::OpenOptions::new().write(true).create_new(true).open(&path).unwrap();
    let channel = Uuid::new_v4();
    let mut queue = EventQueue::new(DedupMode::Queue);
    queue.requeue_preserve_timestamps(one_event_batch(channel, "must survive"));
    let batch = queue.flush_next().unwrap();
    let action = run_error_outcome_with_rest(&mut queue, channel, batch, limit_error(), Some(&rest)).await;
    std::fs::remove_file(&path).unwrap();
    assert!(matches!(action, LoopAction::Continue));
    assert_eq!(queue.queued_event_count(channel), 1);
    assert!(!queue.is_scope_in_flight(scope::SessionScope::Conversation { channel_id: channel }));
    assert!(queue.next_retry_deadline().is_some());
    let scope = scope::SessionScope::Conversation { channel_id: channel };
    assert!(!queue.usage_notice_saved(&scope));
    queue.expire_retry_for_test();
    let retry = queue.flush_next().unwrap();
    let action = run_error_outcome_with_rest(&mut queue, channel, retry, limit_error(), Some(&rest)).await;
    assert!(matches!(action, LoopAction::Continue));
    assert!(queue.usage_notice_saved(&scope));
    let records: Vec<_> = std::fs::read_dir(&path).unwrap().map(|e| e.unwrap().path()).collect();
    assert_eq!(records.len(), 1);
    for record in records { std::fs::remove_file(record).unwrap(); }
    std::fs::remove_dir(path).unwrap();
}

#[tokio::test]
async fn heartbeat_usage_error_holds_account_without_a_batch() {
    let mut queue = EventQueue::new(DedupMode::Queue);
    let action = run_prompt_error(&mut queue, PromptSource::Heartbeat, None, limit_error(), None).await;
    assert!(matches!(action, LoopAction::Continue));
    assert!(queue.is_account_held());
    let mut pool = AgentPool::from_slots(vec![Some(dummy_agent(0).await)]);
    let mut in_flight = false;
    dispatch_heartbeat(&mut pool, &queue, &Arc::new(pool::test_prompt_context()), &mut in_flight);
    assert!(!in_flight);
    assert!(pool.any_idle());
}

#[tokio::test]
async fn quiet_queue_wakes_at_retry_deadline_without_periodic_features() {
    let channel = Uuid::new_v4();
    let mut queue = EventQueue::new(DedupMode::Queue);
    queue.requeue_held(one_event_batch(channel, "retry"), Duration::from_millis(20));
    queue.mark_complete(channel);
    let deadline = queue.next_retry_deadline().map(tokio::time::Instant::from_std);
    tokio::time::timeout(Duration::from_secs(1), pool::AgentPool::wait_for_hold_deadline(deadline)).await.unwrap();
    queue.consume_retry_deadline();
    assert!(queue.next_retry_deadline().is_none());
    assert!(queue.flush_next().is_some());
    assert!(queue.next_retry_deadline().is_none());
}

#[tokio::test]
async fn account_hold_blocks_heartbeat_and_mid_turn_controls() {
    let mut queue = EventQueue::new(DedupMode::Queue);
    let active = Uuid::new_v4();
    let scope = scope::SessionScope::Conversation { channel_id: active };
    let active_batch = one_event_batch(active, "active request");
    queue.requeue_preserve_timestamps(active_batch);
    queue.flush_next().unwrap();
    let limited = one_event_batch(Uuid::new_v4(), "held request");
    queue.requeue_held(limited, Duration::from_secs(3600));
    let mut pool = AgentPool::from_slots(vec![Some(dummy_agent(0).await)]);
    let ctx = Arc::new(pool::test_prompt_context());
    let mut heartbeat_in_flight = false;
    dispatch_heartbeat(&mut pool, &queue, &ctx, &mut heartbeat_in_flight);
    assert!(!heartbeat_in_flight);
    assert!(pool.any_idle());

    for handling in [
        MultipleEventHandling::Steer,
        MultipleEventHandling::Interrupt,
    ] {
        let (control_tx, mut control_rx) = tokio::sync::oneshot::channel();
        let task = pool.join_set.spawn(async {}).id();
        pool.task_map_mut().insert(
            task,
            pool::TaskMeta {
                agent_index: 0,
                channel_id: Some(active),
                scope: Some(scope.clone()),
                turn_id: "active".into(),
                recoverable_batch: None,
                control_tx: Some(control_tx),
                steer_tx: None,
                successful_steer_deliveries: HashSet::new(),
            },
        );
        let event = one_event_batch(active, "steer").events.remove(0).event;
        let (ack_tx, _) = mpsc::unbounded_channel();
        QueuedNormalListenerEvent {
            accepted: true,
            scope: scope.clone(),
            event_id_hex: event.id.to_hex(),
            effective_author: "author".into(),
            event_for_steer: event,
            prompt_tag_for_steer: "test".into(),
        }
        .steer_or_interrupt(handling, None, &mut pool, &mut queue, &ack_tx);
        assert!(matches!(
            control_rx.try_recv(),
            Err(tokio::sync::oneshot::error::TryRecvError::Empty)
        ));
        pool.task_map_mut().clear();
    }
}

#[tokio::test]
async fn exhausted_pool_does_not_complete_a_held_request() {
    let channel = Uuid::new_v4();
    let mut queue = EventQueue::new(DedupMode::Queue);
    queue.requeue_held(one_event_batch(channel, "held"), Duration::ZERO);
    let mut pool = AgentPool::from_slots(vec![None]);
    let ctx = Arc::new(pool::test_prompt_context());
    dispatch_pending(
        &mut pool,
        &mut queue,
        &ctx,
        &mut tokio::time::Instant::now(),
        None,
    );
    assert_eq!(queue.usage_limit_holds(channel), 1);
    assert_eq!(queue.queued_event_count(channel), 1);
    assert!(
        !queue.is_scope_in_flight(&scope::SessionScope::Conversation {
            channel_id: channel
        })
    );
}
