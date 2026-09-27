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
    assert!(queue.usage_notice_scheduled(&scope));
    // No second prompt result: the independent persistence retry must save it.
    tokio::time::timeout(Duration::from_secs(12), async {
        loop {
            if std::fs::read_dir(&path).is_ok_and(|entries| entries.flatten().any(|entry| entry.path().extension().is_some_and(|ext| ext == "json"))) { break; }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }).await.unwrap();
    let records: Vec<_> = std::fs::read_dir(&path).unwrap().map(|e| e.unwrap().path()).collect();
    assert_eq!(records.iter().filter(|record| record.extension().is_some_and(|ext| ext == "json")).count(), 1);
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
async fn terminal_usage_hold_saves_notice_without_extending_account_hold() {
    let rest = pool::test_prompt_context().rest_client;
    let rest = relay::RestClient { keys: nostr::Keys::generate(), ..rest };
    let channel = Uuid::new_v4();
    let mut queue = EventQueue::new(DedupMode::Queue);
    queue.set_usage_limit_holds_for_test(channel, u32::MAX - 1);
    let action = run_error_outcome_with_rest(&mut queue, channel, one_event_batch(channel, "expired"), limit_error(), Some(&rest)).await;
    assert!(matches!(action, LoopAction::Continue));
    assert!(!queue.is_account_held());
    assert_eq!(queue.queued_event_count(channel), 0);
    let path = notice_outbox::directory(&rest).unwrap();
    let records: Vec<_> = std::fs::read_dir(&path).unwrap().map(|entry| entry.unwrap().path()).collect();
    let record = records.iter().find(|path| path.extension().is_some_and(|ext| ext == "json")).unwrap();
    let saved: serde_json::Value = serde_json::from_slice(&std::fs::read(record).unwrap()).unwrap();
    assert!(saved["event"]["content"].as_str().unwrap().contains("Please re-send"));
    for record in records { std::fs::remove_file(record).unwrap(); }
    std::fs::remove_dir(path).unwrap();
}

#[tokio::test]
async fn removed_channel_usage_error_still_holds_other_work() {
    let channel = Uuid::new_v4();
    let scope = scope::SessionScope::Conversation { channel_id: channel };
    let mut queue = EventQueue::new(DedupMode::Queue);
    let action = run_test_outcome(&mut queue, PromptSource::Channel(scope), Some(one_event_batch(channel, "removed")), PromptOutcome::Error(limit_error()), None, HashSet::from([channel])).await;
    assert!(matches!(action, LoopAction::Continue));
    assert!(queue.is_account_held());
    assert_eq!(queue.queued_event_count(channel), 0);
    queue.requeue_preserve_timestamps(one_event_batch(Uuid::new_v4(), "other request"));
    assert!(queue.flush_next().is_none());
}

#[tokio::test]
async fn explicit_cancel_without_batch_clears_previous_hold_metadata() {
    let channel = Uuid::new_v4();
    let scope = scope::SessionScope::Conversation { channel_id: channel };
    let mut queue = EventQueue::new(DedupMode::Queue);
    queue.set_usage_limit_holds_for_test(channel, 5);
    queue.set_retry_count_for_test(channel, 9);
    queue.mark_usage_notice_scheduled(scope.clone());
    let action = run_test_outcome(&mut queue, PromptSource::Channel(scope.clone()), None, PromptOutcome::Cancelled, None, HashSet::new()).await;
    assert!(matches!(action, LoopAction::Continue));
    assert_eq!(queue.usage_limit_holds(channel), 0);
    assert_eq!(queue.retry_count_for_test(channel), 0);
    assert!(!queue.usage_notice_scheduled(&scope));
}

#[test]
fn maximum_cancelled_batch_has_bounded_notice_details() {
    let channel = Uuid::new_v4();
    let mut batch = one_event_batch(channel, "latest");
    for _ in 0..499 {
        batch.cancelled_events.extend(one_event_batch(channel, &"🦀".repeat(80)).events);
    }
    let details = describe_batch_events(&batch, &"turn".repeat(1000), &test_config());
    assert!(details.contains("490 more event(s)"));
    let rest = pool::test_prompt_context().rest_client;
    let notice = pool::build_failure_notice(&rest, channel, &queue::ThreadTags::default(), &details).unwrap();
    assert!(serde_json::to_vec(&notice).unwrap().len() < 16 * 1024);
}

#[tokio::test]
async fn retry_timer_releases_work_for_a_sleeping_lazy_pool() {
    let mut queue = EventQueue::new(DedupMode::Queue);
    queue.hold_account(Duration::from_millis(20));
    queue.requeue_preserve_timestamps(one_event_batch(Uuid::new_v4(), "sleeping pool request"));
    assert!(!queue.has_flushable_work());
    // Same unguarded timer as select!: no ready pool or heartbeat required.
    pool::AgentPool::wait_for_hold_deadline(queue.next_retry_deadline().map(tokio::time::Instant::from_std)).await;
    queue.consume_retry_deadline();
    assert!(queue.has_flushable_work());
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
