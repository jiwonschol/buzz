use super::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[tokio::test]
async fn notice_retries_the_same_signed_event_after_relay_failure() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let rest = crate::relay::RestClient {
        http: reqwest::Client::new(),
        base_url: format!("http://{}", listener.local_addr().unwrap()),
        keys: nostr::Keys::generate(),
        auth_tag_json: None,
    };
    let directory = std::env::temp_dir().join(format!("buzz-notice-{}", uuid::Uuid::new_v4()));
    let event = crate::pool::build_failure_notice(
        &rest,
        uuid::Uuid::new_v4(),
        &crate::queue::ThreadTags::default(),
        "usage hold pending",
    )
    .unwrap();
    let path = directory.join(format!("{}.json", event.id));
    enqueue_at(&directory, event).unwrap();
    let server = tokio::spawn(async move {
        let mut ids = Vec::new();
        for (status, body) in [
            ("503 Service Unavailable", "{}"),
            ("200 OK", "{\"accepted\":false}"),
            ("200 OK", "{\"accepted\":true}"),
        ] {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            loop {
                let mut chunk = [0; 4096];
                let n = socket.read(&mut chunk).await.unwrap();
                assert!(n > 0);
                request.extend_from_slice(&chunk[..n]);
                if let Some(index) = request.windows(4).position(|w| w == b"\r\n\r\n") {
                    let headers = String::from_utf8_lossy(&request[..index]).to_ascii_lowercase();
                    let length: usize = headers
                        .lines()
                        .find_map(|line| line.strip_prefix("content-length: "))
                        .unwrap()
                        .trim()
                        .parse()
                        .unwrap();
                    if request.len() >= index + 4 + length {
                        let event: nostr::Event =
                            serde_json::from_slice(&request[index + 4..index + 4 + length])
                                .unwrap();
                        event.verify().unwrap();
                        ids.push(event.id);
                        break;
                    }
                }
            }
            socket
                .write_all(
                    format!(
                        "HTTP/1.1 {status}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();
        }
        assert_eq!(ids.len(), 3);
        assert_eq!(ids[0], ids[1]);
        assert_eq!(ids[1], ids[2]);
    });
    tokio::time::timeout(Duration::from_secs(25), async {
        for _ in 0..3 {
            drain(&rest, &directory, &mut None).await.unwrap();
            if path.exists() {
                // Simulate restart: only the durable record survives.
                let mut pending: PendingNotice =
                    serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
                pending.next_attempt_at = 0;
                save(&path, &pending).unwrap();
            }
        }
    })
    .await
    .unwrap();
    tokio::time::timeout(Duration::from_secs(1), server)
        .await
        .unwrap()
        .unwrap();
    assert!(!path.exists());
    std::fs::remove_dir_all(directory).unwrap();
}

async fn respond(listener: &tokio::net::TcpListener, body: &str) -> (String, serde_json::Value) {
    let (mut socket, _) = listener.accept().await.unwrap();
    let mut request = Vec::new();
    let (headers, value) = loop {
        let mut chunk = [0; 4096];
        let n = socket.read(&mut chunk).await.unwrap();
        assert!(n > 0);
        request.extend_from_slice(&chunk[..n]);
        if let Some(index) = request.windows(4).position(|w| w == b"\r\n\r\n") {
            let headers = String::from_utf8_lossy(&request[..index]).to_ascii_lowercase();
            let length: usize = headers
                .lines()
                .find_map(|line| line.strip_prefix("content-length: "))
                .unwrap()
                .trim()
                .parse()
                .unwrap();
            if request.len() >= index + 4 + length {
                break (
                    headers,
                    serde_json::from_slice(&request[index + 4..index + 4 + length]).unwrap(),
                );
            }
        }
    };
    socket
        .write_all(
            format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            )
            .as_bytes(),
        )
        .await
        .unwrap();
    (headers, value)
}

async fn stale_fixture() -> (RestClient, tokio::net::TcpListener, PathBuf, Event) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let rest = RestClient {
        http: reqwest::Client::new(),
        base_url: format!("http://{}", listener.local_addr().unwrap()),
        keys: nostr::Keys::generate(),
        auth_tag_json: None,
    };
    let directory = std::env::temp_dir().join(format!("buzz-notice-{}", uuid::Uuid::new_v4()));
    let original = crate::pool::build_failure_notice(
        &rest,
        uuid::Uuid::new_v4(),
        &crate::queue::ThreadTags::default(),
        "usage hold pending",
    )
    .unwrap();
    let event = EventBuilder::new(original.kind, &original.content)
        .tags(original.tags.iter().cloned())
        .custom_created_at(Timestamp::from(
            Timestamp::now().as_secs() - FRESHNESS_SECS - 60,
        ))
        .sign_with_keys(&rest.keys)
        .unwrap();
    enqueue_at(&directory, event.clone()).unwrap();
    (rest, listener, directory, event)
}

#[tokio::test]
async fn stale_notice_queries_old_id_then_persists_fresh_event_before_post() {
    let (rest, listener, directory, original) = stale_fixture().await;
    let path = directory.join(format!("{}.json", original.id));
    let saved_path = path.clone();
    let server = tokio::spawn(async move {
        let (headers, query) = respond(&listener, "[]").await;
        assert!(headers.starts_with("post /query "));
        assert_eq!(query[0]["ids"][0], original.id.to_hex());
        let (headers, submitted) = respond(&listener, r#"{"accepted":false}"#).await;
        assert!(headers.starts_with("post /events "));
        let fresh: Event = serde_json::from_value(submitted).unwrap();
        fresh.verify().unwrap();
        assert_ne!(fresh.id, original.id);
        assert!(Timestamp::now().as_secs() - fresh.created_at.as_secs() < FRESHNESS_SECS);
        let persisted: PendingNotice =
            serde_json::from_slice(&std::fs::read(saved_path).unwrap()).unwrap();
        assert_eq!(persisted.event.id, fresh.id);
    });
    drain(&rest, &directory, &mut None).await.unwrap();
    server.await.unwrap();
    assert!(path.exists());
    std::fs::remove_dir_all(directory).unwrap();
}

#[tokio::test]
async fn stale_notice_already_accepted_is_not_posted_again() {
    let (rest, listener, directory, original) = stale_fixture().await;
    let path = directory.join(format!("{}.json", original.id));
    let server = tokio::spawn(async move {
        let body = serde_json::to_string(&vec![original]).unwrap();
        respond(&listener, &body).await;
        assert!(
            tokio::time::timeout(Duration::from_millis(100), listener.accept())
                .await
                .is_err()
        );
    });
    drain(&rest, &directory, &mut None).await.unwrap();
    assert!(!path.exists());
    server.await.unwrap();
    std::fs::remove_dir_all(directory).unwrap();
}

#[tokio::test]
async fn invalid_lookup_preserves_old_identity_and_expiry_reclaims_capacity() {
    let (rest, listener, directory, original) = stale_fixture().await;
    let path = directory.join(format!("{}.json", original.id));
    let server = tokio::spawn(async move {
        respond(&listener, "{}").await;
    });
    drain(&rest, &directory, &mut None).await.unwrap();
    server.await.unwrap();
    let mut persisted: PendingNotice =
        serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    assert_eq!(persisted.event.id, original.id);
    persisted.expires_at = 0;
    persisted.next_attempt_at = 0;
    save(&path, &persisted).unwrap();
    drain(&rest, &directory, &mut None).await.unwrap();
    assert!(!path.exists());
    enqueue_at(&directory, original).unwrap();
    assert!(path.exists());
    std::fs::remove_dir_all(directory).unwrap();
}

#[tokio::test]
async fn full_outbox_refuses_new_notice_without_discarding_existing_records() {
    let (rest, _listener, directory, original) = stale_fixture().await;
    for index in 1..MAX_RECORDS {
        std::fs::write(directory.join(format!("retained-{index}.tmp")), b"retained").unwrap();
    }
    let new_event = crate::pool::build_failure_notice(
        &rest,
        uuid::Uuid::new_v4(),
        &crate::queue::ThreadTags::default(),
        "another hold",
    )
    .unwrap();
    assert!(enqueue_at(&directory, new_event).is_err());
    assert!(directory.join(format!("{}.json", original.id)).exists());
    assert_eq!(
        std::fs::read_dir(&directory).unwrap().count(),
        MAX_RECORDS + 1
    );
    std::fs::remove_dir_all(directory).unwrap();
}

#[tokio::test]
async fn failed_cleanup_cannot_starve_notices_beyond_the_pass_limit() {
    let (rest, listener, directory, original) = stale_fixture().await;
    // This fixture's original stale record is replaced with a current event
    // so every attempt exercises POST followed by the failing cleanup seam.
    std::fs::remove_file(directory.join(format!("{}.json", original.id))).unwrap();
    for index in 0..=MAX_PER_PASS {
        let event = crate::pool::build_failure_notice(
            &rest,
            uuid::Uuid::new_v4(),
            &crate::queue::ThreadTags::default(),
            &format!("hold {index}"),
        )
        .unwrap();
        enqueue_at(&directory, event).unwrap();
    }
    let server = tokio::spawn(async move {
        let mut delivered = std::collections::HashSet::new();
        for _ in 0..MAX_PER_PASS * 2 {
            let (headers, event) = respond(&listener, r#"{"accepted":true}"#).await;
            assert!(headers.starts_with("post /events "));
            delivered.insert(event["id"].as_str().unwrap().to_owned());
        }
        delivered
    });
    let mut cursor = None;
    tokio::time::timeout(Duration::from_secs(10), async {
        for _ in 0..2 {
            drain_with_remove(&rest, &directory, &mut cursor, |_| {
                Err(std::io::Error::new(
                    std::io::ErrorKind::PermissionDenied,
                    "outbox directory is read-only",
                ))
            })
            .await
            .unwrap();
        }
    })
    .await
    .unwrap();
    let delivered = server.await.unwrap();
    assert_eq!(delivered.len(), MAX_PER_PASS + 1);
    // Cleanup failed for every record: fairness cannot depend on any on-disk
    // schedule advancing or the accepted records disappearing.
    assert_eq!(
        std::fs::read_dir(&directory).unwrap().count(),
        MAX_PER_PASS + 2
    );
    std::fs::remove_dir_all(directory).unwrap();
}

#[tokio::test]
async fn concurrent_admission_keeps_the_disk_record_bound() {
    let (rest, _listener, directory, _) = stale_fixture().await;
    for index in 1..MAX_RECORDS - 1 {
        std::fs::write(directory.join(format!("retained-{index}.tmp")), b"retained").unwrap();
    }
    let events: Vec<_> = (0..16)
        .map(|_| {
            crate::pool::build_failure_notice(
                &rest,
                uuid::Uuid::new_v4(),
                &crate::queue::ThreadTags::default(),
                "concurrent hold",
            )
            .unwrap()
        })
        .collect();
    let barrier = std::sync::Barrier::new(events.len());
    let accepted = std::thread::scope(|scope| {
        let threads: Vec<_> = events
            .into_iter()
            .map(|event| {
                let directory = &directory;
                let barrier = &barrier;
                scope.spawn(move || {
                    barrier.wait();
                    enqueue_at(directory, event).is_ok()
                })
            })
            .collect();
        threads
            .into_iter()
            .map(|thread| usize::from(thread.join().unwrap()))
            .sum::<usize>()
    });
    assert_eq!(accepted, 1);
    assert_eq!(
        std::fs::read_dir(&directory).unwrap().count(),
        MAX_RECORDS + 1
    );
    std::fs::remove_dir_all(directory).unwrap();
}

#[tokio::test(start_paused = true)]
async fn transient_storage_failure_retries_during_hold_without_another_usage_result() {
    let (_rest, _listener, directory, original) = stale_fixture().await;
    let blocked = directory.join("blocked-outbox");
    std::fs::write(&blocked, b"not a directory").unwrap();
    let destination = blocked.clone();
    let earliest_expiry = Timestamp::now().as_secs() + crate::usage_limit::MAX_HOLD_SECS;
    persist_or_retry_in(original.clone(), move || Ok(destination.clone())).unwrap();
    let latest_expiry = Timestamp::now().as_secs() + crate::usage_limit::MAX_HOLD_SECS;
    tokio::task::yield_now().await;
    // The first independent retry still encounters the blocked directory.
    tokio::time::advance(Duration::from_secs(POLL_SECS)).await;
    tokio::task::yield_now().await;
    assert!(blocked.is_file());
    std::fs::remove_file(&blocked).unwrap();
    std::fs::create_dir(&blocked).unwrap();
    // No new usage outcome, no second persist call: the existing task saves it.
    tokio::time::advance(Duration::from_secs(POLL_SECS * 2)).await;
    tokio::task::yield_now().await;
    let path = blocked.join(format!("{}.json", original.id));
    let stored: PendingNotice = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
    assert_eq!(stored.event.id, original.id);
    assert!((earliest_expiry..=latest_expiry).contains(&stored.expires_at));
    std::fs::remove_dir_all(directory).unwrap();
}
