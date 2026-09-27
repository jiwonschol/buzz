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
