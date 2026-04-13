use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use futures_util::StreamExt;
use sha2::{Digest, Sha256};
use tokio::io::AsyncWriteExt;
use tracing::{debug, info, warn};

const MIN_SPEED_WINDOW: Duration = Duration::from_secs(60);

/// Result of a download: bytes written and SHA-256 hex digest.
pub struct DownloadResult {
    pub bytes: u64,
    pub sha256: String,
}

/// Download a file using parallel HTTP range requests.
///
/// Returns bytes written and SHA-256 digest.
pub async fn download(
    url: &str,
    dest: &Path,
    parallel_chunks: u16,
    min_speed: u64,
) -> Result<DownloadResult> {
    let client = reqwest::Client::new();

    let head = client
        .head(url)
        .send()
        .await
        .context("HEAD request failed")?
        .error_for_status()
        .context("HEAD returned error status")?;

    let total_size = head
        .headers()
        .get(reqwest::header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<u64>().ok());

    let accepts_ranges = head
        .headers()
        .get(reqwest::header::ACCEPT_RANGES)
        .and_then(|v| v.to_str().ok())
        .map(|v| v.contains("bytes"))
        .unwrap_or(false);

    match (total_size, accepts_ranges) {
        (Some(size), true) if size > 0 && parallel_chunks > 1 => {
            info!(size, chunks = parallel_chunks, "parallel range download");
            parallel_range_download(&client, url, dest, size, parallel_chunks, min_speed).await
        }
        _ => {
            info!("falling back to sequential download");
            sequential_download(&client, url, dest, total_size, min_speed).await
        }
    }
}

fn format_bytes(bytes: u64) -> String {
    const GB: u64 = 1_073_741_824;
    const MB: u64 = 1_048_576;
    if bytes >= GB {
        format!("{:.2} GB", bytes as f64 / GB as f64)
    } else {
        format!("{:.1} MB", bytes as f64 / MB as f64)
    }
}

fn format_eta(secs: f64) -> String {
    if secs.is_infinite() || secs.is_nan() || secs < 0.0 {
        return "--".into();
    }
    let s = secs as u64;
    if s >= 3600 {
        format!("{}h{:02}m", s / 3600, (s % 3600) / 60)
    } else if s >= 60 {
        format!("{}m{:02}s", s / 60, s % 60)
    } else {
        format!("{}s", s)
    }
}

fn spawn_progress_watchdog(
    downloaded: Arc<AtomicU64>,
    total_size: u64,
    file_name: String,
    min_speed: u64,
    abort: Arc<AtomicBool>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let start = Instant::now();
        let mut interval = tokio::time::interval(Duration::from_secs(5));
        interval.tick().await; // first tick is immediate, skip it

        let mut prev_bytes = 0u64;
        let mut prev_tick = Instant::now();
        let mut slow_since: Option<Instant> = None;

        loop {
            interval.tick().await;
            let bytes = downloaded.load(Ordering::Relaxed);
            let elapsed = start.elapsed().as_secs_f64();
            let pct = if total_size > 0 {
                (bytes as f64 / total_size as f64) * 100.0
            } else {
                0.0
            };
            let avg_speed = if elapsed > 0.0 {
                bytes as f64 / elapsed
            } else {
                0.0
            };
            let remaining = if avg_speed > 0.0 && total_size > bytes {
                (total_size - bytes) as f64 / avg_speed
            } else {
                0.0
            };

            let now = Instant::now();
            let dt = now.duration_since(prev_tick).as_secs_f64();
            let instant_speed = if dt > 0.0 {
                (bytes - prev_bytes) as f64 / dt
            } else {
                0.0
            };
            prev_bytes = bytes;
            prev_tick = now;

            info!(
                file = file_name,
                progress = format!("{:.1}%", pct),
                downloaded = format_bytes(bytes),
                total = format_bytes(total_size),
                speed = format!("{:.0} MB/s", instant_speed / 1_048_576.0),
                eta = format_eta(remaining),
                "downloading"
            );

            if min_speed > 0 {
                if (instant_speed as u64) < min_speed {
                    let since = *slow_since.get_or_insert(now);
                    let slow_dur = now.duration_since(since);
                    if slow_dur >= MIN_SPEED_WINDOW {
                        warn!(
                            file = file_name,
                            instant_speed_mbps = format!("{:.1}", instant_speed / 1_048_576.0),
                            min_speed_mbps = format!("{:.1}", min_speed as f64 / 1_048_576.0),
                            slow_for_secs = slow_dur.as_secs(),
                            "aborting download: speed below minimum for too long"
                        );
                        abort.store(true, Ordering::Relaxed);
                        break;
                    }
                } else {
                    slow_since = None;
                }
            }

            if bytes >= total_size {
                break;
            }
        }
    })
}

async fn parallel_range_download(
    client: &reqwest::Client,
    url: &str,
    dest: &Path,
    total_size: u64,
    chunks: u16,
    min_speed: u64,
) -> Result<DownloadResult> {
    let chunk_size = total_size / chunks as u64;
    let tmp_dir = dest.parent().unwrap_or(Path::new("."));

    let downloaded = Arc::new(AtomicU64::new(0));
    let abort = Arc::new(AtomicBool::new(false));
    let file_name = dest
        .file_name()
        .unwrap_or_default()
        .to_string_lossy()
        .to_string();

    let progress_handle = spawn_progress_watchdog(
        Arc::clone(&downloaded),
        total_size,
        file_name.clone(),
        min_speed,
        Arc::clone(&abort),
    );

    let mut handles = Vec::new();

    for i in 0..chunks {
        let start = i as u64 * chunk_size;
        let end = if i == chunks - 1 {
            total_size - 1
        } else {
            (i as u64 + 1) * chunk_size - 1
        };

        let client = client.clone();
        let url = url.to_string();
        let chunk_path = tmp_dir.join(format!(".cm-chunk-{}-{}", file_name, i));
        let downloaded = Arc::clone(&downloaded);
        let abort = Arc::clone(&abort);

        let handle = tokio::spawn(async move {
            download_range_streaming(&client, &url, &chunk_path, start, end, downloaded, abort)
                .await
        });
        handles.push((handle, i));
    }

    let mut file = tokio::fs::File::create(dest).await?;
    let mut total_written = 0u64;
    let mut hasher = Sha256::new();

    for (handle, i) in handles {
        let chunk_bytes = handle.await.context("chunk task panicked")??;
        let chunk_path = tmp_dir.join(format!(".cm-chunk-{}-{}", file_name, i));
        let data = tokio::fs::read(&chunk_path).await?;
        hasher.update(&data);
        file.write_all(&data).await?;
        total_written += chunk_bytes;
        tokio::fs::remove_file(&chunk_path).await.ok();
    }

    file.flush().await?;
    progress_handle.abort();

    let hash = crate::snapshot::hex_encode_sha256(&hasher.finalize());
    debug!(total_written, sha256 = hash, "parallel download complete");
    Ok(DownloadResult {
        bytes: total_written,
        sha256: hash,
    })
}

async fn download_range_streaming(
    client: &reqwest::Client,
    url: &str,
    dest: &Path,
    start: u64,
    end: u64,
    downloaded: Arc<AtomicU64>,
    abort: Arc<AtomicBool>,
) -> Result<u64> {
    let range = format!("bytes={}-{}", start, end);
    let resp = client
        .get(url)
        .header(reqwest::header::RANGE, &range)
        .send()
        .await
        .context("range request failed")?;

    if !resp.status().is_success() {
        anyhow::bail!("range request returned {}", resp.status());
    }

    let mut file = tokio::fs::File::create(dest).await?;
    let mut stream = resp.bytes_stream();
    let mut written = 0u64;

    while let Some(chunk) = stream.next().await {
        if abort.load(Ordering::Relaxed) {
            anyhow::bail!("download aborted: speed below minimum threshold");
        }
        let chunk = chunk?;
        file.write_all(&chunk).await?;
        let len = chunk.len() as u64;
        written += len;
        downloaded.fetch_add(len, Ordering::Relaxed);
    }

    file.flush().await?;
    Ok(written)
}

async fn sequential_download(
    client: &reqwest::Client,
    url: &str,
    dest: &Path,
    total_size: Option<u64>,
    min_speed: u64,
) -> Result<DownloadResult> {
    let resp = client
        .get(url)
        .send()
        .await
        .context("download request failed")?;

    if !resp.status().is_success() {
        anyhow::bail!("download returned {}", resp.status());
    }

    let downloaded = Arc::new(AtomicU64::new(0));
    let abort = Arc::new(AtomicBool::new(false));
    let file_name = dest
        .file_name()
        .unwrap_or_default()
        .to_string_lossy()
        .to_string();

    let progress_handle = if let Some(size) = total_size {
        Some(spawn_progress_watchdog(
            Arc::clone(&downloaded),
            size,
            file_name,
            min_speed,
            Arc::clone(&abort),
        ))
    } else {
        None
    };

    let mut file = tokio::fs::File::create(dest).await?;
    let mut stream = resp.bytes_stream();
    let mut total = 0u64;
    let mut hasher = Sha256::new();

    while let Some(chunk) = stream.next().await {
        if abort.load(Ordering::Relaxed) {
            anyhow::bail!("download aborted: speed below minimum threshold");
        }
        let chunk = chunk?;
        hasher.update(&chunk);
        file.write_all(&chunk).await?;
        let len = chunk.len() as u64;
        total += len;
        downloaded.fetch_add(len, Ordering::Relaxed);
    }

    file.flush().await?;
    if let Some(h) = progress_handle {
        h.abort();
    }
    let hash = crate::snapshot::hex_encode_sha256(&hasher.finalize());
    Ok(DownloadResult {
        bytes: total,
        sha256: hash,
    })
}
