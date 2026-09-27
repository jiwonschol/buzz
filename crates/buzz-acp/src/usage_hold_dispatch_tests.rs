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
async fn account_only_hold_notifies_once_then_dispatches_after_release() {
    let rest = pool::test_prompt_context().rest_client;
    let rest = relay::RestClient { keys: nostr::Keys::generate(), ..rest };
    let mut queue = EventQueue::new(DedupMode::Queue);
    run_prompt_error(&mut queue, PromptSource::Heartbeat, None, limit_error(), None).await;
    let channel = Uuid::new_v4();
    let scope = scope::SessionScope::Conversation { channel_id: channel };
    queue.requeue_preserve_timestamps(one_event_batch(channel, "first request"));
    retry_terminal_notices(&mut queue, Some(&rest));
    assert!(queue.usage_notice_scheduled(&scope));
    assert!(queue.flush_next().is_none());
    queue.requeue_preserve_timestamps(one_event_batch(channel, "follow-up"));
    retry_terminal_notices(&mut queue, Some(&rest));
    let path = notice_outbox::directory(&rest).unwrap();
    let files: Vec<_> = std::fs::read_dir(&path).unwrap().map(|entry| entry.unwrap().path()).collect();
    assert_eq!(files.iter().filter(|path| path.extension().is_some_and(|ext| ext == "json")).count(), 1);
    queue.expire_retry_for_test();
    let batch = queue.flush_next().unwrap();
    assert_eq!(batch.events.len(), 2);
    queue.finish_request(scope.clone());
    assert!(!queue.usage_notice_scheduled(&scope));
    for file in files { std::fs::remove_file(file).unwrap(); }
    std::fs::remove_dir(path).unwrap();
}

#[test]
fn saturated_terminal_notice_retries_without_provider_dispatch() {
    const CHILD: &str = "BUZZ_TERMINAL_SATURATION_CHILD";
    if std::env::var_os(CHILD).is_none() {
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "error_outcome_emission_tests::saturated_terminal_notice_retries_without_provider_dispatch", "--nocapture"])
            .env(CHILD, "1").status().unwrap();
        assert!(status.success());
        return;
    }
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(async {
        let rest = pool::test_prompt_context().rest_client;
        let rest = relay::RestClient { keys: nostr::Keys::generate(), ..rest };
        let path = notice_outbox::directory(&rest).unwrap();
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, b"storage unavailable").unwrap();
        let full = notice_outbox::saturate_retries_for_test();
        let mut account_queue = EventQueue::new(DedupMode::Queue);
        account_queue.hold_account(Duration::from_secs(3600));
        let account_channel = Uuid::new_v4();
        let account_scope = scope::SessionScope::Conversation { channel_id: account_channel };
        account_queue.requeue_preserve_timestamps(one_event_batch(account_channel, "account hold"));
        retry_terminal_notices(&mut account_queue, Some(&rest));
        assert!(!account_queue.usage_notice_scheduled(&account_scope));
        let notice_id = account_queue.usage_notice_draft(&account_scope).unwrap().id;
        account_queue.make_account_notice_due_for_test();
        retry_terminal_notices(&mut account_queue, Some(&rest));
        assert_eq!(account_queue.usage_notice_draft(&account_scope).unwrap().id, notice_id);
        assert!(account_queue.next_retry_deadline().unwrap() <= std::time::Instant::now() + Duration::from_secs(5));
        assert_eq!(account_queue.queued_event_count(account_channel), 1);
        let channel = Uuid::new_v4();
        let mut queue = EventQueue::new(DedupMode::Queue);
        let mut batch = one_event_batch(channel, "terminal");
        for _ in 0..499 { batch.cancelled_events.extend(one_event_batch(channel, "earlier").events); }
        save_terminal_notice(&mut queue, Some(&rest), batch, "terminal notice".into());
        assert!(queue.next_retry_deadline().is_some());
        assert!(queue.flush_next().is_none());
        assert!(!queue.push(QueuedEvent {
            channel_id: channel,
            scope: scope::SessionScope::Conversation { channel_id: channel },
            event: one_event_batch(channel, "new").events.pop().unwrap().event,
            received_at: std::time::Instant::now(),
            prompt_tag: "test".into(),
        }));
        queue.make_terminal_notices_due_for_test();
        retry_terminal_notices(&mut queue, Some(&rest));
        assert!(queue.next_retry_deadline().is_some());
        assert!(queue.next_retry_deadline().unwrap() > std::time::Instant::now() + Duration::from_secs(9));
        drop(full);
        // A free memory retry slot is not a durable write. Keep ownership and
        // prevent inactivity shutdown until the actual storage recovers.
        queue.make_terminal_notices_due_for_test();
        retry_terminal_notices(&mut queue, Some(&rest));
        assert!(queue.next_retry_deadline().is_some());
        assert!(queue.has_undispatched_work());
        assert!(queue.next_retry_deadline().unwrap() > std::time::Instant::now() + Duration::from_secs(19));
        let idle_start = tokio::time::Instant::now();
        let idle_end = idle_start + Duration::from_secs(61);
        assert!(!inactivity_exit_due(idle_start, idle_end, Duration::from_secs(60), &queue, false));
        std::fs::remove_file(&path).unwrap();
        tokio::time::sleep(Duration::from_secs(5)).await;
        retry_terminal_notices(&mut account_queue, Some(&rest));
        assert!(account_queue.usage_notice_scheduled(&account_scope));
        assert!(account_queue.flush_next().is_none());
        queue.make_terminal_notices_due_for_test();
        retry_terminal_notices(&mut queue, Some(&rest));
        assert!(queue.next_retry_deadline().is_none());
        assert!(!queue.has_undispatched_work());
        assert!(inactivity_exit_due(idle_start, idle_end, Duration::from_secs(60), &queue, false));
        assert!(queue.flush_next().is_none());
        let files: Vec<_> = std::fs::read_dir(&path).unwrap().map(|entry| entry.unwrap().path()).collect();
        assert_eq!(files.iter().filter(|path| path.extension().is_some_and(|ext| ext == "json")).count(), 2);
        for file in files { std::fs::remove_file(file).unwrap(); }
        std::fs::remove_dir(path).unwrap();
    });
}

#[tokio::test]
async fn account_notice_protects_the_notified_request() {
    let rest = relay::RestClient { keys: nostr::Keys::generate(), ..pool::test_prompt_context().rest_client };
    let mut queue = EventQueue::new(DedupMode::Queue);
    let channel = Uuid::new_v4();
    let scope = scope::SessionScope::Conversation { channel_id: channel };
    queue.hold_account(Duration::from_secs(3600));
    let first = one_event_batch(channel, "notified");
    let id = first.events[0].event.id;
    queue.requeue_preserve_timestamps(first);
    retry_terminal_notices(&mut queue, Some(&rest));
    for _ in 0..500 {
        queue.push(QueuedEvent { channel_id: channel, scope: scope.clone(),
            event: one_event_batch(channel, "later").events.pop().unwrap().event,
            received_at: std::time::Instant::now(), prompt_tag: "test".into() });
    }
    assert!(queue.queued_event_ids_for_test(scope).contains(&id));
}

#[tokio::test]
async fn old_completion_preserves_followup_notice() {
    let rest = relay::RestClient { keys: nostr::Keys::generate(), ..pool::test_prompt_context().rest_client };
    let mut queue = EventQueue::new(DedupMode::Queue);
    let channel = Uuid::new_v4();
    let scope = scope::SessionScope::Conversation { channel_id: channel };
    queue.requeue_preserve_timestamps(one_event_batch(channel, "old"));
    queue.flush_next().unwrap();
    queue.hold_account(Duration::from_secs(3600));
    queue.requeue_preserve_timestamps(one_event_batch(channel, "new"));
    retry_terminal_notices(&mut queue, Some(&rest));
    queue.finish_request(scope.clone());
    assert!(queue.usage_notice_scheduled(&scope));
    queue.expire_retry_for_test();
    queue.flush_next().unwrap();
    queue.finish_request(scope.clone());
    assert!(!queue.usage_notice_scheduled(&scope));
}

#[tokio::test]
async fn maintenance_preserves_account_notice_without_request_hold() {
    let rest = relay::RestClient { keys: nostr::Keys::generate(), ..pool::test_prompt_context().rest_client };
    let mut queue = EventQueue::new(DedupMode::Queue);
    let channel = Uuid::new_v4();
    let scope = scope::SessionScope::Conversation { channel_id: channel };
    queue.hold_account(Duration::from_secs(3600));
    queue.requeue_preserve_timestamps(one_event_batch(channel, "new"));
    retry_terminal_notices(&mut queue, Some(&rest));
    queue.compact_expired_state();
    assert!(queue.usage_notice_scheduled(&scope));
    assert_eq!(queue.usage_limit_holds(channel), 0);
}

#[tokio::test]
async fn new_account_hold_does_not_inherit_previous_notice() {
    let rest = relay::RestClient { keys: nostr::Keys::generate(), ..pool::test_prompt_context().rest_client };
    let mut queue = EventQueue::new(DedupMode::Queue);
    let channel = Uuid::new_v4();
    let scope = scope::SessionScope::Conversation { channel_id: channel };
    queue.hold_account(Duration::from_secs(3600));
    queue.requeue_preserve_timestamps(one_event_batch(channel, "old hold"));
    retry_terminal_notices(&mut queue, Some(&rest));
    queue.expire_retry_for_test();
    queue.flush_next().unwrap();
    queue.hold_account(Duration::from_secs(3600));
    queue.requeue_preserve_timestamps(one_event_batch(channel, "new hold"));
    assert!(!queue.usage_notice_scheduled(&scope));
    retry_terminal_notices(&mut queue, Some(&rest));
    assert!(queue.usage_notice_scheduled(&scope));
    queue.hold_account(Duration::from_secs(7200));
    assert!(queue.usage_notice_scheduled(&scope));
    queue.finish_request(scope.clone());
    assert!(queue.usage_notice_scheduled(&scope));
}

#[tokio::test]
async fn successful_retry_clears_hold_with_new_input_before_timer_consumption() {
    let channel = Uuid::new_v4();
    let scope = scope::SessionScope::Conversation { channel_id: channel };
    let mut queue = EventQueue::new(DedupMode::Queue);
    queue.requeue_held(one_event_batch(channel, "retry"), Duration::ZERO);
    queue.release_in_flight(scope.clone());
    queue.flush_next().unwrap();
    queue.requeue_preserve_timestamps(one_event_batch(channel, "new input"));
    let action = run_test_outcome(&mut queue, PromptSource::Channel(scope), None,
        PromptOutcome::Ok(acp::StopReason::EndTurn), None, HashSet::new()).await;
    assert!(matches!(action, LoopAction::Continue));
    assert_eq!(queue.usage_limit_holds(channel), 0);
    assert_eq!(queue.queued_event_count(channel), 1);
}

#[tokio::test]
async fn cancelled_only_terminal_notice_keeps_original_thread() {
    let rest = relay::RestClient { keys: nostr::Keys::generate(), ..pool::test_prompt_context().rest_client };
    let channel = Uuid::new_v4();
    let root = nostr::EventId::all_zeros().to_hex();
    let event = nostr::EventBuilder::new(nostr::Kind::Custom(9), "expired thread request")
        .tags([nostr::Tag::parse(["e", root.as_str(), "", "reply"]).unwrap()])
        .sign_with_keys(&nostr::Keys::generate()).unwrap();
    let mut batch = one_event_batch(channel, "unused");
    batch.events.clear();
    batch.cancelled_events.push(BatchEvent { event, prompt_tag: "test".into(), received_at: std::time::Instant::now() });
    let mut queue = EventQueue::new(DedupMode::Queue);
    save_terminal_notice(&mut queue, Some(&rest), batch, "expired".into());
    let path = notice_outbox::directory(&rest).unwrap();
    let records: Vec<_> = std::fs::read_dir(&path).unwrap().map(|entry| entry.unwrap().path()).collect();
    let record = records.iter().find(|path| path.extension().is_some_and(|ext| ext == "json")).unwrap();
    let saved: serde_json::Value = serde_json::from_slice(&std::fs::read(record).unwrap()).unwrap();
    let event: nostr::Event = serde_json::from_value(saved["event"].clone()).unwrap();
    assert_eq!(queue::parse_thread_tags(&event).root_event_id, Some(root));
    for record in records { std::fs::remove_file(record).unwrap(); }
    std::fs::remove_dir(path).unwrap();
}

#[tokio::test]
async fn terminal_usage_hold_saves_notice_without_extending_account_hold() {
    let rest = pool::test_prompt_context().rest_client;
    let rest = relay::RestClient { keys: nostr::Keys::generate(), ..rest };
    let channel = Uuid::new_v4();
    let mut queue = EventQueue::new(DedupMode::Queue);
    queue.requeue_preserve_timestamps(one_event_batch(channel, "expired"));
    let batch = queue.flush_next().unwrap();
    queue.set_usage_limit_holds_for_test(channel, u32::MAX - 1);
    let action = run_error_outcome_with_rest(&mut queue, channel, batch, limit_error(), Some(&rest)).await;
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
    // Dispatch is ready, but the queued request still has its absolute expiry.
    assert_eq!(queue.next_retry_deadline(), queue.request_expiry_deadline());
    assert!(queue.request_expiry_deadline().is_some_and(|at| at > std::time::Instant::now()));
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
