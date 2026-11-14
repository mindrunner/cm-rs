//! Probe external snapshot sources following the agave RPC snapshot
//! convention: `{base}/snapshot.tar.bz2` and `{base}/incremental-snapshot.tar.bz2`
//! redirect to the concrete `snapshot-SLOT-HASH.tar.zst`, so a no-follow GET
//! reveals the latest slot, filename, and download URL without downloading.
//! The redirect target may be a short-lived presigned object-store URL (e.g.
//! avorio's R2 mirror), so resolved URLs are only valid briefly.

use std::time::{Duration, Instant};

use futures_util::StreamExt;
use tracing::debug;

use crate::snapshot::{SnapshotInfo, SnapshotType};

/// Alias endpoint for the latest full snapshot.
pub const FULL_ALIAS: &str = "snapshot.tar.bz2";
/// Alias endpoint for the latest incremental snapshot.
pub const INCREMENTAL_ALIAS: &str = "incremental-snapshot.tar.bz2";

/// An external source that answered the probe: what it serves and how fast it
/// answered. The `*_url` fields are the resolved redirect targets — use them
/// promptly, presigned URLs expire.
#[derive(Debug, Clone)]
pub struct ProbedSource {
    pub base_url: String,
    pub full: SnapshotInfo,
    pub full_url: String,
    pub incremental: Option<SnapshotInfo>,
    pub latency_ms: f64,
}

impl ProbedSource {
    /// The bootstrap slot this source can bring a node to.
    pub fn resulting_slot(&self) -> u64 {
        self.incremental
            .as_ref()
            .map(|i| i.slot)
            .unwrap_or(self.full.slot)
    }
}

/// Resolve an alias endpoint without following the redirect: returns the
/// absolute download URL and the snapshot parsed from its filename.
/// `client` must have redirects disabled.
pub async fn resolve_alias(
    client: &reqwest::Client,
    base_url: &str,
    alias: &str,
) -> Option<(String, SnapshotInfo)> {
    let alias_url = format!("{}/{}", base_url.trim_end_matches('/'), alias);
    let resp = client.get(&alias_url).send().await.ok()?;
    if !resp.status().is_redirection() {
        return None;
    }
    let location = resp
        .headers()
        .get(reqwest::header::LOCATION)?
        .to_str()
        .ok()?;
    // Location may be absolute or relative, and may carry a query string
    // (presigned URLs); Url::join and .path_segments() handle both.
    let resolved = reqwest::Url::parse(&alias_url).ok()?.join(location).ok()?;
    let name = resolved.path_segments()?.next_back()?.to_string();
    let info = SnapshotInfo::from_filename(&name, 0, 0)?;
    Some((resolved.to_string(), info))
}

/// Probe one source. `client` must have redirects disabled. Returns `None`
/// when the source doesn't serve the redirect convention or advertises no
/// parsable full snapshot.
pub async fn probe_source(client: &reqwest::Client, base_url: &str) -> Option<ProbedSource> {
    let base = base_url.trim_end_matches('/');

    let start = Instant::now();
    let (full_url, full) = resolve_alias(client, base, FULL_ALIAS).await?;
    let latency_ms = start.elapsed().as_secs_f64() * 1000.0;
    if full.snapshot_type != SnapshotType::Full {
        return None;
    }

    let incremental = resolve_alias(client, base, INCREMENTAL_ALIAS)
        .await
        .map(|(_, info)| info)
        .filter(|s| s.snapshot_type == SnapshotType::Incremental)
        .filter(|s| s.base_slot == Some(full.slot));

    debug!(
        source = base,
        full_slot = full.slot,
        incremental_slot = incremental.as_ref().map(|s| s.slot),
        latency_ms,
        "probed external source"
    );

    Some(ProbedSource {
        base_url: base.to_string(),
        full,
        full_url,
        incremental,
        latency_ms,
    })
}

/// Measured download speed in bytes/sec: stream up to `max_bytes` from `url`
/// for at most `timeout`. Hitting the deadline mid-stream still measures the
/// bytes that arrived; `None` when the source served nothing at all — a hung
/// or failing source must be dropped, not ranked.
pub async fn speed_test(
    client: &reqwest::Client,
    url: &str,
    max_bytes: u64,
    timeout: Duration,
) -> Option<f64> {
    let start = Instant::now();
    let deadline = start + timeout;
    let range = format!("bytes=0-{}", max_bytes.saturating_sub(1));

    let resp = tokio::time::timeout(
        timeout,
        client
            .get(url)
            .header(reqwest::header::RANGE, &range)
            .send(),
    )
    .await
    .ok()?
    .ok()?;
    if !resp.status().is_success() {
        return None;
    }

    let mut stream = resp.bytes_stream();
    let mut total = 0u64;
    // Servers ignoring Range answer 200 with the full body; stop reading at
    // max_bytes either way. The per-chunk timeout keeps the partial count
    // when the deadline passes mid-stream.
    while total < max_bytes {
        let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
            break;
        };
        match tokio::time::timeout(remaining, stream.next()).await {
            Err(_) | Ok(None) => break,
            Ok(Some(Ok(chunk))) => total += chunk.len() as u64,
            Ok(Some(Err(_))) => return None,
        }
    }

    let secs = start.elapsed().as_secs_f64();
    (total > 0 && secs > 0.0).then(|| total as f64 / secs)
}
