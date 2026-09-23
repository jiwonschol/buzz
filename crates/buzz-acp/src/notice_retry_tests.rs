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
    tokio::time::timeout(
        Duration::from_secs(25),
        post_failure_notice(
            &rest,
            Uuid::new_v4(),
            &ThreadTags::default(),
            "usage hold pending",
            true,
        ),
    )
    .await
    .unwrap();
    tokio::time::timeout(Duration::from_secs(1), server)
        .await
        .unwrap()
        .unwrap();
}
