//! Durable, bounded delivery of usage-hold notices. The signed event is saved
//! before POST, so a restart or lost acknowledgement retries the same identity.
use crate::relay::RestClient;
use anyhow::{Context, Result};
use nostr::{Event, EventBuilder, Timestamp};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::Duration;

const POLL_SECS: u64 = 5;
const FRESHNESS_SECS: u64 = 900;
const MAX_PER_PASS: usize = 32;
const MAX_RECORDS: usize = 1024;
const MAX_RECORD_BYTES: u64 = 64 * 1024;

#[derive(Serialize, Deserialize)]
struct PendingNotice {
    event: Event,
    expires_at: u64,
    next_attempt_at: u64,
    backoff_secs: u64,
    expired: bool,
}

pub(super) fn directory(rest: &RestClient) -> Result<PathBuf> {
    let home = std::env::var_os("HOME").context("HOME is required for the notice outbox")?;
    let scope = Sha256::digest(format!("{}\n{}", rest.base_url, rest.keys.public_key()));
    Ok(PathBuf::from(home)
        .join(".buzz/notice-outbox")
        .join(hex::encode(scope)))
}

fn save(path: &Path, notice: &PendingNotice) -> Result<()> {
    let parent = path.parent().context("notice path has no parent")?;
    std::fs::create_dir_all(parent)?;
    let bytes = serde_json::to_vec(notice)?;
    anyhow::ensure!(
        bytes.len() as u64 <= MAX_RECORD_BYTES,
        "notice exceeds outbox record limit"
    );
    // Include abandoned temporary files in the bound. Repeated disk failures
    // must not accumulate a new temporary record on every worker pass.
    anyhow::ensure!(
        std::fs::read_dir(parent)?.take(MAX_RECORDS + 1).count() <= MAX_RECORDS,
        "notice outbox temporary-record limit reached"
    );
    let temporary = parent.join(format!(".{}.tmp", uuid::Uuid::new_v4()));
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(&temporary)?;
    let result = (|| -> std::io::Result<()> {
        file.write_all(&bytes)?;
        file.sync_all()?;
        std::fs::rename(&temporary, path)
    })();
    if let Err(error) = result {
        // Only this invocation owns this unique scratch file. The previous
        // committed record remains intact when write/fsync/rename fails.
        if let Err(cleanup) = std::fs::remove_file(&temporary) {
            tracing::error!(%cleanup, path = %temporary.display(), "notice temporary file retained");
        }
        return Err(error.into());
    }
    #[cfg(unix)]
    std::fs::File::open(parent)?.sync_all()?;
    Ok(())
}

fn enqueue_at(directory: &Path, event: Event) -> Result<()> {
    std::fs::create_dir_all(directory)?;
    let path = directory.join(format!("{}.json", event.id));
    if path.exists() {
        return Ok(());
    }
    anyhow::ensure!(
        std::fs::read_dir(directory)?.take(MAX_RECORDS).count() < MAX_RECORDS,
        "notice outbox is full; retained records require operator attention"
    );
    let now = Timestamp::now().as_secs();
    save(
        &path,
        &PendingNotice {
            event,
            expires_at: now.saturating_add(crate::usage_limit::MAX_HOLD_SECS),
            next_attempt_at: now,
            backoff_secs: POLL_SECS,
            expired: false,
        },
    )
}

pub(crate) fn enqueue(rest: &RestClient, event: Event) -> Result<()> {
    enqueue_at(&directory(rest)?, event)
}

// An old ID might already have been accepted before the connection failed.
// Query it before refreshing its timestamp; an unavailable/invalid query is
// never evidence of absence. Keep the old event until a fresh one is durable.
async fn deliver(rest: &RestClient, path: &Path, notice: &mut PendingNotice) -> Result<bool> {
    let now = Timestamp::now().as_secs();
    if now.saturating_sub(notice.event.created_at.as_secs()) >= FRESHNESS_SECS {
        let channel = notice
            .event
            .tags
            .iter()
            .find_map(|tag| {
                let values = tag.as_slice();
                (values.first().map(String::as_str) == Some("h"))
                    .then(|| values.get(1).map(String::as_str))
                    .flatten()
            })
            .context("notice event lacks channel scope")?;
        let filter = serde_json::json!({
            "ids": [notice.event.id.to_hex()], "kinds": [9], "#h": [channel], "limit": 1
        });
        let response = rest.query_raw(&[filter]).await?;
        let events = response
            .as_array()
            .context("notice lookup was not an array")?;
        if events.iter().any(|event| {
            event.get("id").and_then(serde_json::Value::as_str)
                == Some(notice.event.id.to_hex().as_str())
        }) {
            return Ok(true);
        }
        anyhow::ensure!(
            events.is_empty(),
            "notice lookup returned an unexpected event"
        );
        notice.event = EventBuilder::new(notice.event.kind, &notice.event.content)
            .tags(notice.event.tags.iter().cloned())
            .custom_created_at(Timestamp::from(now))
            .sign_with_keys(&rest.keys)?;
        save(path, notice)?;
    }
    let response = rest.submit_event(&notice.event).await?;
    Ok(response
        .get("accepted")
        .and_then(serde_json::Value::as_bool)
        == Some(true))
}

async fn drain(rest: &RestClient, directory: &Path, cursor: &mut Option<PathBuf>) -> Result<()> {
    drain_with_remove(rest, directory, cursor, |path| std::fs::remove_file(path)).await
}

async fn drain_with_remove(
    rest: &RestClient,
    directory: &Path,
    cursor: &mut Option<PathBuf>,
    mut remove: impl FnMut(&Path) -> std::io::Result<()>,
) -> Result<()> {
    std::fs::create_dir_all(directory)?;
    let mut pending = Vec::new();
    for entry in std::fs::read_dir(directory)?.take(MAX_RECORDS + 1) {
        let path = entry?.path();
        if path.extension().and_then(|s| s.to_str()) != Some("json") {
            continue;
        }
        let loaded = (|| -> Result<PendingNotice> {
            anyhow::ensure!(
                std::fs::metadata(&path)?.len() <= MAX_RECORD_BYTES,
                "notice record exceeds size limit"
            );
            Ok(serde_json::from_slice(&std::fs::read(&path)?)?)
        })();
        match loaded {
            Ok(notice) if !notice.expired => pending.push((path, notice)),
            Ok(_) => {}
            Err(error) => {
                tracing::error!(path = %path.display(), %error, "unreadable notice record retained")
            }
        }
    }
    // Disk cleanup/schedule writes can fail even when reads and relay POSTs
    // succeed. Advance a process-local cursor independently of those writes,
    // so a permanently due record cannot monopolize each bounded pass.
    pending.sort_by(|(left, _), (right, _)| left.cmp(right));
    if let Some(previous) = cursor.as_ref() {
        let next = pending.partition_point(|(path, _)| path <= previous);
        pending.rotate_left(next);
    }
    let mut attempted = 0;
    for (path, mut notice) in pending {
        let result = async {
            let now = Timestamp::now().as_secs();
            if notice.expired || notice.next_attempt_at > now {
                return Ok::<_, anyhow::Error>(());
            }
            if now >= notice.expires_at {
                notice.expired = true;
                save(&path, &notice)?;
                tracing::error!(path = %path.display(), "notice delivery expired; durable record retained");
                return Ok(());
            }
            *cursor = Some(path.clone());
            attempted += 1;
            match tokio::time::timeout(Duration::from_secs(10), deliver(rest, &path, &mut notice)).await {
                Ok(Ok(true)) => {
                    remove(&path)?;
                    #[cfg(unix)]
                    std::fs::File::open(directory)?.sync_all()?;
                }
                result => {
                    tracing::warn!(event_id = %notice.event.id, ?result, "notice delivery pending");
                    notice.next_attempt_at = now.saturating_add(notice.backoff_secs);
                    notice.backoff_secs = (notice.backoff_secs * 2).min(300);
                    save(&path, &notice)?;
                }
            }
            Ok(())
        }.await;
        if let Err(error) = result {
            tracing::error!(path = %path.display(), %error, "notice outbox record retained after error");
        }
        if attempted >= MAX_PER_PASS {
            break;
        }
    }
    Ok(())
}

pub(crate) async fn run(rest: RestClient) {
    let mut cursor = None;
    loop {
        match directory(&rest) {
            Ok(directory) => {
                if let Err(error) = drain(&rest, &directory, &mut cursor).await {
                    tracing::error!(%error, "notice outbox scan failed; retrying");
                }
            }
            Err(error) => {
                tracing::error!(%error, "cannot open durable notice outbox; retrying");
            }
        }
        tokio::time::sleep(Duration::from_secs(POLL_SECS)).await;
    }
}

#[cfg(test)]
#[path = "notice_retry_tests.rs"]
mod tests;
