// ── usage-limit hold ──────────────────────────────────────────────────

#[test]
fn account_hold_starts_backlog_windows_without_extending_them() {
    for state in ["queued", "retry", "cancelled", "steer-release", "steer-recovery", "steer-consumed"] {
        let mut q = EventQueue::new(DedupMode::Queue);
        let limited = Uuid::new_v4();
        q.push(make_queued(limited, "provider limit"));
        let limited_batch = q.flush_next().unwrap();
        let channel = Uuid::new_v4();
        let mut pending = make_queued(channel, state);
        let window = Duration::from_secs(crate::usage_limit::MAX_HOLD_SECS);
        // Ordinary backlog age does not consume the usage-limit window.
        pending.received_at = Instant::now() - window - Duration::from_secs(1);
        let id = pending.event.id;
        q.push(pending);
        if state.starts_with("steer-") {
            assert!(q.mark_native_steer_pending(channel, &id.to_hex()));
        } else if state != "queued" {
            let batch = q.flush_next().unwrap();
            if state == "retry" {
                assert!(q.requeue(batch).is_none());
            } else {
                q.requeue_as_cancelled(batch, CancelReason::Steer);
            }
            q.release_in_flight(channel);
        }
        let earliest = Instant::now() + window;
        assert!(q.requeue_held(limited_batch, window).is_none());
        q.release_in_flight(limited);
        let deadline = q.usage_hold_deadlines.get(&conv(channel)).and_then(|windows| windows.get(&id)).copied();
        assert!(deadline.is_some_and(|at| at >= earliest && at <= Instant::now() + window), "{state}");
        assert!(q.take_expired_requests().is_empty());
        q.compact_expired_state();
        assert_eq!(q.usage_hold_deadlines[&conv(channel)][&id], deadline.unwrap());

        if state == "steer-consumed" {
            q.remove_event(channel, &id.to_hex());
            assert!(!q.usage_hold_deadlines.contains_key(&conv(channel)));
            assert!(q.take_expired_requests().is_empty());
            continue;
        }

        let expired_at = Instant::now();
        q.usage_hold_deadlines.get_mut(&conv(channel)).unwrap().insert(id, expired_at);
        q.hold_account(window);
        assert_eq!(q.usage_hold_deadlines[&conv(channel)][&id], expired_at);
        q.expire_retry_for_test();
        q.hold_account(window);
        assert_eq!(q.usage_hold_deadlines[&conv(channel)][&id], expired_at);
        if state.starts_with("steer-") {
            // Pending delivery remains owned by the steer result, even after expiry.
            assert!(q.take_expired_requests().is_empty());
            if state == "steer-release" {
                q.release_native_steer(channel, &id.to_hex());
            } else {
                q.in_flight_scopes.insert(conv(channel));
                q.in_flight_deadlines.insert(conv(channel), Instant::now() - Duration::from_secs(1));
                assert!(!q.has_flushable_work());
            }
            assert_eq!(q.usage_hold_deadlines[&conv(channel)][&id], expired_at);
            assert!(q.next_retry_deadline().is_some_and(|at| at <= Instant::now()));
        }
        let expired = q.take_expired_requests();
        assert_eq!(expired.len(), 1, "{state}");
        assert_eq!(expired[0].events.iter().chain(&expired[0].cancelled_events).map(|event| event.event.id).collect::<Vec<_>>(), vec![id]);
        assert_eq!(expired[0].cancelled_events.len(), usize::from(state == "cancelled"));
        assert!(q.is_account_held());
        assert!(q.flush_next().is_none());
    }
}

#[test]
fn request_expiry_wakes_before_another_scopes_account_hold() {
    let mut q = EventQueue::new(DedupMode::Queue);
    let channel = Uuid::new_v4();
    q.hold_account(Duration::from_secs(86400));
    let mut old = make_queued(channel, "expired");
    old.received_at = Instant::now() - Duration::from_secs(crate::usage_limit::MAX_HOLD_SECS + 1);
    q.push(old);
    assert!(q.next_retry_deadline().is_some_and(|at| at <= Instant::now()));
    let fresh = make_queued(channel, "fresh");
    let fresh_id = fresh.event.id;
    q.push(fresh);
    let expired = q.take_expired_requests();
    assert_eq!(expired.len(), 1);
    assert_eq!(expired[0].events.len(), 1);
    assert_eq!(q.queued_event_ids_for_test(channel), vec![fresh_id]);
    assert!(q.is_account_held());
    assert!(q.flush_next().is_none());
}

#[test]
fn expiry_batches_leave_a_wakeup_for_remaining_scopes() {
    let mut q = EventQueue::new(DedupMode::Queue);
    q.hold_account(Duration::from_secs(86400));
    for _ in 0..33 {
        let mut event = make_queued(Uuid::new_v4(), "expired");
        event.received_at = Instant::now() - Duration::from_secs(crate::usage_limit::MAX_HOLD_SECS + 1);
        q.push(event);
    }
    assert_eq!(q.take_expired_requests().len(), 32);
    assert!(q.request_expiry_deadline().is_some_and(|at| at <= Instant::now()));
    assert_eq!(q.take_expired_requests().len(), 1);
    assert!(q.request_expiry_deadline().is_none());
}

#[test]
fn account_admission_does_not_restart_absolute_hold_window() {
    let mut q = EventQueue::new(DedupMode::Queue);
    let channel = Uuid::new_v4();
    q.hold_account(Duration::from_secs(3600));
    let mut event = make_queued(channel, "aged admission");
    event.received_at = Instant::now() - Duration::from_secs(crate::usage_limit::MAX_HOLD_SECS + 1);
    q.push(event);
    q.expire_retry_for_test();
    let batch = q.flush_next().unwrap();
    assert!(q.requeue_held(batch, Duration::from_secs(3600)).is_some());
}

#[test]
fn mixed_hold_batch_expires_only_old_requests_including_cancelled() {
    for cancelled in [false, true] {
        let mut q = EventQueue::new(DedupMode::Queue);
        let channel = Uuid::new_v4();
        q.hold_account(Duration::from_secs(3600));
        let mut old = make_queued(channel, "old");
        old.received_at = Instant::now() - Duration::from_secs(crate::usage_limit::MAX_HOLD_SECS + 1);
        let old_id = old.event.id;
        q.push(old);
        let fresh = make_queued(channel, "fresh");
        let fresh_id = fresh.event.id;
        q.push(fresh);
        q.expire_retry_for_test();
        let mut batch = q.flush_next().unwrap();
        if cancelled {
            batch.cancelled_events.push(batch.events.remove(0));
            batch.cancel_reason = Some(CancelReason::Steer);
        }
        let (dead, retained) = q.requeue_held_partitioned(batch, Duration::from_secs(3600));
        assert!(retained);
        let dead = dead.unwrap();
        assert_eq!(dead.events.iter().chain(&dead.cancelled_events).map(|e| e.event.id).collect::<Vec<_>>(), vec![old_id]);
        q.release_in_flight(channel);
        q.compact_expired_state();
        assert!(q.usage_hold_deadlines[&conv(channel)].contains_key(&fresh_id));
        q.expire_retry_for_test();
        let next = q.flush_next().unwrap();
        assert_eq!(next.events[0].event.id, fresh_id);
        assert!(q.requeue_held(next, Duration::from_secs(3600)).is_none());
    }
}

#[test]
fn terminal_retry_pass_has_a_work_limit() {
    let mut q = EventQueue::new(DedupMode::Queue);
    for _ in 0..33 {
        let channel = Uuid::new_v4();
        q.push(make_queued(channel, "terminal"));
        let batch = q.flush_next().unwrap();
        q.release_in_flight(conv(channel));
        q.retain_terminal_notice(batch, "terminal".into(), None);
    }
    q.make_terminal_notices_due_for_test();
    assert!(q.take_due_terminal_notices().len() <= 32);
    assert!(q.has_undispatched_work());
}

#[test]
fn maintenance_preserves_due_retry_wake_until_consumed() {
    let mut q = EventQueue::new(DedupMode::Queue);
    let channel = Uuid::new_v4();
    q.push(make_queued(channel, "retry"));
    q.retry_after.insert(conv(channel), Instant::now());
    q.compact_expired_state();
    assert!(q.next_retry_deadline().is_some());
    q.consume_retry_deadline();
    assert!(q.next_retry_deadline().is_none());
    assert!(q.flush_next().is_some());
}

#[test]
fn cancellation_carryover_keeps_capacity_across_every_return_path() {
    for path in ["held", "retry", "preserve", "cancelled"] {
        let mut q = EventQueue::new(DedupMode::Queue);
        let channel = Uuid::new_v4();
        for i in 0..MAX_PENDING_PER_CHANNEL {
            assert!(q.push(make_queued(channel, &format!("original-{i}"))));
            let batch = q.flush_next().unwrap();
            q.requeue_as_cancelled(batch, CancelReason::Interrupt);
            q.release_in_flight(channel);
            assert_eq!(q.channel_event_total(channel), i + 1);
        }
        assert!(!q.push(make_queued(channel, "overflow")));
        let batch = q.flush_next().unwrap();
        assert_eq!(batch.events.len() + batch.cancelled_events.len(), MAX_PENDING_PER_CHANNEL);
        assert!(!q.push(make_queued(channel, "in-flight overflow")));
        match path {
            "held" => { assert!(q.requeue_held(batch, Duration::from_secs(3600)).is_none()); }
            "retry" => { assert!(q.requeue(batch).is_none()); }
            "preserve" => q.requeue_preserve_timestamps(batch),
            _ => q.requeue_as_cancelled(batch, CancelReason::Interrupt),
        }
        q.release_in_flight(channel);
        assert_eq!(q.channel_event_total(channel), MAX_PENDING_PER_CHANNEL, "{path}");
        let accepted = q.push(make_queued(channel, "post-return overflow"));
        if matches!(path, "held" | "cancelled") {
            assert!(!accepted, "{path}");
        }
        assert_eq!(q.channel_event_total(channel), MAX_PENDING_PER_CHANNEL, "{path}");
    }
}

#[test]
fn transient_failure_preserves_merged_cancelled_requests() {
    let mut q = EventQueue::new(DedupMode::Queue);
    let channel = Uuid::new_v4();
    q.push(make_queued(channel, "original"));
    let first = q.flush_next().unwrap();
    let original = first.events[0].event.id;
    q.requeue_as_cancelled(first, CancelReason::Interrupt);
    q.release_in_flight(channel);
    q.push(make_queued(channel, "followup"));
    let merged = q.flush_next().unwrap();
    assert_eq!(merged.cancelled_events[0].event.id, original);
    assert_eq!(q.in_flight_batch_sizes[&conv(channel)], 2);
    assert!(q.requeue(merged).is_none());
    q.release_in_flight(channel);
    q.expire_retry_for_test();
    let retried = q.flush_next().unwrap();
    assert_eq!(retried.cancelled_events[0].event.id, original);
    assert_eq!(retried.events.len(), 1);
}

#[test]
fn in_flight_batches_reserve_channel_capacity_before_usage_errors() {
    let mut q = EventQueue::new(DedupMode::Queue);
    let channel = Uuid::new_v4();
    let mut batches = Vec::new();
    for thread in 0..10 {
        for item in 0..MAX_BATCH_EVENTS {
            let mut event = make_queued(channel, &format!("{thread}/{item}"));
            event.scope = SessionScope::Thread {
                channel_id: channel,
                root_event_id: format!("{thread:064x}"),
            };
            assert!(q.push(event));
        }
        batches.push(q.flush_next().unwrap());
    }
    assert!(!q.push(make_queued(channel, "eleventh batch")));
    for batch in batches {
        let scope = batch.scope.clone();
        assert!(q.requeue_held(batch, Duration::from_secs(3600)).is_none());
        q.mark_complete(scope);
        assert!(q.channel_event_total(channel) <= MAX_PENDING_PER_CHANNEL);
    }
    assert_eq!(q.channel_event_total(channel), MAX_PENDING_PER_CHANNEL);
}

#[test]
fn held_channel_capacity_rejects_the_new_event() {
    let mut q = EventQueue::new(DedupMode::Queue);
    let channel = Uuid::new_v4();
    let mut batches = Vec::new();
    for thread in 0..10 {
        for item in 0..MAX_BATCH_EVENTS {
            let mut event = make_queued(channel, &format!("{thread}/{item}"));
            event.scope = SessionScope::Thread {
                channel_id: channel,
                root_event_id: format!("{thread:064x}"),
            };
            assert!(q.push(event));
        }
        batches.push(q.flush_next().unwrap());
    }
    for batch in batches {
        let scope = batch.scope.clone();
        assert!(q.requeue_held(batch, Duration::from_secs(3600)).is_none());
        q.mark_complete(scope);
    }
    assert_eq!(q.channel_event_total(channel), MAX_PENDING_PER_CHANNEL);
    let fresh = make_queued(channel, "new request");
    let id = fresh.event.id;
    assert!(!q.push(fresh));
    assert!(!q.queued_event_ids_for_test(channel).contains(&id));
    assert_eq!(q.channel_event_total(channel), MAX_PENDING_PER_CHANNEL);
}

#[test]
fn worker_contention_preserves_expired_hold_protection() {
    let mut q = EventQueue::new(DedupMode::Queue);
    let channel = Uuid::new_v4();
    q.push(make_queued(channel, "held request"));
    let batch = q.flush_next().unwrap();
    let id = batch.events[0].event.id;
    q.requeue_held(batch, Duration::from_secs(3600));
    q.mark_complete(channel);
    q.retry_after.clear();
    q.account_retry_after = None;
    let batch = q.flush_next().unwrap();
    q.requeue_preserve_timestamps(batch);
    q.release_in_flight(channel);
    for i in 0..600 {
        q.push(make_queued(channel, &format!("traffic {i}")));
    }
    assert!(q.queued_event_ids_for_test(channel).contains(&id));
    assert_eq!(q.usage_limit_holds(channel), 1);
}

#[test]
fn account_hold_does_not_disable_in_flight_expiry() {
    for use_flush in [false, true] {
        let mut q = EventQueue::new(DedupMode::Queue);
        let channel = Uuid::new_v4();
        q.push(make_queued(channel, "hung"));
        q.flush_next().unwrap();
        q.in_flight_deadlines.insert(conv(channel), Instant::now());
        q.account_retry_after = Some(Instant::now() + Duration::from_secs(3600));
        if use_flush {
            assert!(q.flush_next().is_none());
        } else {
            assert!(!q.has_flushable_work());
        }
        assert!(!q.is_scope_in_flight(conv(channel)));
        assert!(q.is_account_held());
    }
}

#[test]
fn usage_window_is_absolute_for_every_delay() {
    for delay in [90, 1800, crate::usage_limit::MAX_HOLD_SECS] {
        let mut q = EventQueue::new(DedupMode::Queue);
        let channel = Uuid::new_v4();
        q.push(make_queued(channel, "limited"));
        let batch = q.flush_next().unwrap();
        q.requeue_held(batch, Duration::from_secs(delay));
        q.mark_complete(channel);
        let end = Instant::now() + Duration::from_secs(1);
        q.usage_hold_deadlines.get_mut(&conv(channel)).unwrap().values_mut().for_each(|deadline| *deadline = end);
        q.retry_after.clear();
        q.account_retry_after = None;
        let batch = q.flush_next().unwrap();
        assert!(q.requeue_held(batch, Duration::from_secs(delay)).is_none());
        assert_eq!(q.retry_after[&conv(channel)], end);
        q.mark_complete(channel);
        q.usage_hold_deadlines.get_mut(&conv(channel)).unwrap().values_mut().for_each(|deadline| *deadline = Instant::now());
        q.retry_after.clear();
        q.account_retry_after = None;
        let batch = q.flush_next().unwrap();
        assert!(q.requeue_held(batch, Duration::from_secs(delay)).is_some());
        q.mark_complete(channel);
        assert!(!q.usage_hold_deadlines.contains_key(&conv(channel)));
    }
}

#[test]
fn held_request_survives_scope_and_channel_overflow() {
    let mut q = EventQueue::new(DedupMode::Queue);
    let channel = Uuid::new_v4();
    q.push(make_queued(channel, "original request"));
    let batch = q.flush_next().unwrap();
    let original = batch.events[0].event.id;
    q.requeue_held(batch, Duration::from_secs(3600));
    q.mark_complete(channel);
    for i in 0..600 {
        q.push(make_queued(channel, &format!("fresh {i}")));
    }
    let mut other = make_queued(channel, "other thread");
    other.scope = SessionScope::Thread {
        channel_id: channel,
        root_event_id: nostr::EventId::all_zeros().to_hex(),
    };
    for _ in 0..600 {
        q.push(other.clone());
    }
    assert!(q.queued_event_ids_for_test(channel).contains(&original));
    assert!(q.channel_event_total(channel) <= MAX_PENDING_PER_CHANNEL);
    q.retry_after.clear();
    q.account_retry_after = None;
    let recovered = q.flush_next().unwrap();
    assert!(recovered
        .events
        .iter()
        .any(|event| event.event.id == original));
    q.finish_request(conv(channel));
    assert!(!q.protected_events.get(&conv(channel)).is_some_and(|ids| ids.contains(&original)));
    // Later requests accepted during the hold still own their protection.
    let pending = q.queued_event_ids_for_test(channel);
    assert!(!pending.is_empty());
    assert!(pending.iter().all(|id| q.protected_events.get(&conv(channel)).is_some_and(|ids| ids.contains(id))));
}

#[test]
fn notice_ids_survive_cancel_requeue_and_other_scope_completion() {
    let mut q = EventQueue::new(DedupMode::Queue);
    let a = Uuid::new_v4();
    let b = Uuid::new_v4();
    q.hold_account(Duration::from_secs(3600));
    q.push(make_queued(a, "a"));
    q.push(make_queued(b, "b"));
    q.mark_usage_notice_scheduled(conv(a));
    q.mark_usage_notice_scheduled(conv(b));
    q.expire_retry_for_test();
    let batch = q.flush_next().unwrap();
    let active = batch.scope.clone();
    let other = if active == conv(a) { conv(b) } else { conv(a) };
    q.requeue_as_cancelled(batch, CancelReason::Interrupt);
    q.release_in_flight(&active);
    q.compact_expired_state();
    assert!(q.usage_notice_scheduled(&active));
    assert!(q.usage_notice_scheduled(&other));
    while let Some(batch) = q.flush_next() {
        let finished = batch.scope;
        q.finish_request(finished.clone());
        assert!(!q.usage_notice_scheduled(&finished));
    }
    q.compact_expired_state();
    assert!(q.protected_events.is_empty());
    assert!(q.usage_notices_scheduled.is_empty());
    assert!(q.in_flight_request_ids.is_empty());
}

#[test]
fn usage_hold_gates_fresh_and_cancelled_scopes() {
    let mut q = EventQueue::new(DedupMode::Queue);
    let held = Uuid::new_v4();
    let cancelled = Uuid::new_v4();
    q.push(make_queued(cancelled, "cancelled"));
    let batch = q.flush_next().unwrap();
    q.requeue_as_cancelled(batch, CancelReason::Steer);
    q.mark_complete(cancelled);
    q.push(make_queued(held, "limited"));
    let batch = q.flush_next().unwrap();
    q.requeue_held(batch, Duration::from_secs(3600));
    q.mark_complete(held);
    q.push(make_queued(Uuid::new_v4(), "new channel"));
    assert!(!q.has_flushable_work());
    assert!(q.flush_next().is_none());
    assert!(q.has_undispatched_work());
}

#[test]
fn cancelled_only_fallback_respects_scope_throttle() {
    let mut q = EventQueue::new(DedupMode::Queue);
    let channel = Uuid::new_v4();
    q.push(make_queued(channel, "cancelled"));
    let batch = q.flush_next().unwrap();
    q.requeue_as_cancelled(batch, CancelReason::Steer);
    q.retry_after
        .insert(conv(channel), Instant::now() + Duration::from_secs(60));
    q.mark_complete(channel);
    assert!(!q.has_flushable_work());
    assert!(q.flush_next().is_none());
}

#[test]
fn timezone_fallback_survives_a_week_of_half_hour_retries() {
    let mut q = EventQueue::new(DedupMode::Queue);
    let channel = Uuid::new_v4();
    q.push(make_queued(channel, "weekly request"));
    for _ in 0..=7 * 24 * 2 {
        q.retry_after.clear();
        q.account_retry_after = None;
        let batch = q.flush_next().unwrap();
        assert!(q.requeue_held(batch, Duration::from_secs(1800)).is_none());
        q.mark_complete(channel);
    }
    assert_eq!(q.queued_event_count(channel), 1);
}

/// A hold keeps the batch queued, throttles the scope for the given delay
/// and leaves `retry_counts` untouched — the transient-failure budget must
/// not be spent on a deterministic limit.
#[test]
fn test_requeue_held_keeps_events_without_consuming_retry_budget() {
    let mut queue = EventQueue::new(DedupMode::Queue);
    let channel_id = Uuid::new_v4();
    queue.set_retry_count_for_test(channel_id, 3);
    queue.push(make_queued(channel_id, "held"));
    let batch = queue.flush_next().expect("batch");
    let original_id = batch.events[0].event.id;

    assert!(queue
        .requeue_held(batch, Duration::from_secs(3600))
        .is_none());
    queue.mark_complete(channel_id);

    assert_eq!(
        queue.retry_count_for_test(channel_id),
        3,
        "a hold must not touch retry_counts"
    );
    assert_eq!(queue.usage_limit_holds(channel_id), 1);
    assert_eq!(queue.queued_event_count(channel_id), 1);
    let remaining = queue
        .retry_after_remaining_for_test(channel_id)
        .expect("scope throttled");
    assert!(remaining > Duration::from_secs(3500), "{remaining:?}");
    assert!(
        queue.flush_next().is_none(),
        "held scope must not flush before the delay elapses"
    );
    // Still the same event, at the head.
    assert_eq!(
        queue.queued_event_ids_for_test(channel_id),
        vec![original_id]
    );
}

/// Once the delay elapses the very same events flush again, in order, and
/// the subsequent successful completion clears the hold counter.
#[test]
fn test_requeue_held_releases_the_same_events_after_the_delay() {
    let mut queue = EventQueue::new(DedupMode::Queue);
    let channel_id = Uuid::new_v4();
    queue.push(make_queued(channel_id, "first"));
    queue.push(make_queued(channel_id, "second"));
    let batch = queue.flush_next().expect("batch");
    let ids: Vec<_> = batch.events.iter().map(|e| e.event.id).collect();
    assert_eq!(ids.len(), 2);

    assert!(queue
        .requeue_held(batch, Duration::from_millis(60))
        .is_none());
    queue.mark_complete(channel_id);
    assert!(queue.flush_next().is_none(), "held");

    std::thread::sleep(Duration::from_millis(80));
    let again = queue.flush_next().expect("released after the hold");
    let again_ids: Vec<_> = again.events.iter().map(|e| e.event.id).collect();
    assert_eq!(again_ids, ids, "same events, same order");
    queue.mark_complete(channel_id);
    assert_eq!(
        queue.usage_limit_holds(channel_id),
        0,
        "a successful turn clears the hold counter"
    );
    assert_eq!(queue.retry_count_for_test(channel_id), 0);
}

/// Consecutive holds keep counting while retry_counts stays put; past the
/// cap the batch is dead-lettered and all throttle state is cleared.
#[test]
fn test_requeue_held_dead_letters_past_the_hold_cap() {
    let mut queue = EventQueue::new(DedupMode::Queue);
    let channel_id = Uuid::new_v4();
    queue.push(make_queued(channel_id, "doomed"));
    queue.set_usage_limit_holds_for_test(channel_id, MAX_USAGE_LIMIT_HOLDS - 1);

    // Hold number MAX is still a hold …
    let batch = queue.flush_next().expect("batch");
    assert!(queue
        .requeue_held(batch, Duration::ZERO)
        .is_none());
    queue.mark_complete(channel_id);
    assert_eq!(queue.usage_limit_holds(channel_id), MAX_USAGE_LIMIT_HOLDS);
    assert_eq!(queue.retry_count_for_test(channel_id), 0);

    // … and the next one dead-letters.
    let batch = queue.flush_next().expect("batch after hold");
    let dead = queue
        .requeue_held(batch, Duration::from_secs(60))
        .expect("dead-lettered past the cap");
    assert_eq!(dead.events.len(), 1);
    queue.mark_complete(channel_id);
    assert_eq!(queue.queued_event_count(channel_id), 0);
    assert_eq!(queue.usage_limit_holds(channel_id), 0);
    assert!(queue.retry_after_remaining_for_test(channel_id).is_none());
}

/// `drain_channel` (agent removed from the channel) forgets hold state
/// along with the other per-scope side tables.
#[test]
fn test_drain_channel_clears_usage_limit_holds() {
    let mut queue = EventQueue::new(DedupMode::Queue);
    let channel_id = Uuid::new_v4();
    queue.push(make_queued(channel_id, "x"));
    let batch = queue.flush_next().expect("batch");
    assert!(queue.requeue_held(batch, Duration::from_secs(60)).is_none());
    queue.mark_complete(channel_id);
    assert_eq!(queue.usage_limit_holds(channel_id), 1);
    queue.drain_channel(channel_id);
    assert_eq!(queue.usage_limit_holds(channel_id), 0);
}
