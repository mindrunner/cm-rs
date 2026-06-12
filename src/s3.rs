use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};
use futures_util::stream::{self, StreamExt};
use tracing::{debug, info, warn};

use crate::config::S3Config;
use crate::index::SnapshotIndex;
use crate::metrics::Metrics;
use crate::snapshot::SnapshotInfo;

/// Files above this threshold use multipart upload (S3 single PUT limit is 5 GB).
const MULTIPART_THRESHOLD: u64 = 256 * 1024 * 1024;
const MULTIPART_PART_SIZE: u64 = 256 * 1024 * 1024;
const MULTIPART_CONCURRENCY: usize = 32;

pub struct S3Uploader {
    config: S3Config,
    client: aws_sdk_s3::Client,
    index: SnapshotIndex,
    metrics: Metrics,
}

impl S3Uploader {
    pub async fn new(config: S3Config, index: SnapshotIndex, metrics: Metrics) -> Result<Self> {
        let mut aws_config = aws_config::defaults(aws_config::BehaviorVersion::latest());

        if let Some(ref region) = config.region {
            aws_config = aws_config.region(aws_config::Region::new(region.clone()));
        }

        let aws_config = aws_config.load().await;
        let mut s3_config = aws_sdk_s3::config::Builder::from(&aws_config);

        if let Some(ref endpoint) = config.endpoint
            && !endpoint.is_empty()
        {
            s3_config = s3_config.endpoint_url(endpoint);
        }

        if config.path_style {
            s3_config = s3_config.force_path_style(true);
        }

        let client = aws_sdk_s3::Client::from_conf(s3_config.build());

        Ok(Self {
            config,
            client,
            index,
            metrics,
        })
    }

    /// Upload the best available full snapshot and its latest incremental to S3.
    pub async fn upload_best(&self) -> Result<bool> {
        let snapshots = self.index.list().await;
        let best_full = match snapshots.iter().find(|s| s.is_full()) {
            Some(s) => s,
            None => {
                debug!("no full snapshot to upload");
                return Ok(false);
            }
        };

        let mut uploaded = false;

        let full_key = self.s3_key(&best_full.file_name);
        if !self.object_exists(&full_key).await? {
            self.upload_snapshot(best_full).await?;
            uploaded = true;
        }

        let full_slot = best_full.slot;
        let fresh = self.index.list().await;
        if let Some(incremental) = fresh
            .iter()
            .find(|s| !s.is_full() && s.base_slot == Some(full_slot))
        {
            let inc_key = self.s3_key(&incremental.file_name);
            if !self.object_exists(&inc_key).await? {
                match self.upload_snapshot(incremental).await {
                    Ok(()) => uploaded = true,
                    Err(e) => {
                        warn!(
                            file = incremental.file_name,
                            error = format!("{e:#}"),
                            "incremental upload failed, skipping"
                        );
                    }
                }
            }
        }

        Ok(uploaded)
    }

    async fn upload_snapshot(&self, snap: &SnapshotInfo) -> Result<()> {
        let path = self.index.dir().join(&snap.file_name);
        let key = self.s3_key(&snap.file_name);
        info!(
            file = snap.file_name,
            key,
            size = snap.size,
            "uploading to S3"
        );

        let start = Instant::now();

        if snap.size >= MULTIPART_THRESHOLD {
            self.upload_multipart(&path, &key, snap.size).await?;
        } else {
            self.upload_single(&path, &key).await?;
        }

        let duration = start.elapsed();
        self.metrics
            .s3_upload_duration
            .observe(duration.as_secs_f64());
        info!(
            key,
            duration_secs = duration.as_secs_f64(),
            "S3 upload complete"
        );
        Ok(())
    }

    async fn upload_single(&self, path: &std::path::Path, key: &str) -> Result<()> {
        let body = ByteStream::from_path(path)
            .await
            .context("open snapshot for S3 upload")?;

        self.client
            .put_object()
            .bucket(&self.config.bucket)
            .key(key)
            .body(body)
            .content_type("application/zstd")
            .send()
            .await
            .map_err(|e| anyhow::anyhow!("S3 put_object: {e:#}"))?;

        Ok(())
    }

    async fn upload_multipart(
        &self,
        path: &std::path::Path,
        key: &str,
        file_size: u64,
    ) -> Result<()> {
        let total_parts = file_size.div_ceil(MULTIPART_PART_SIZE);
        info!(
            key,
            file_size,
            total_parts,
            part_size = MULTIPART_PART_SIZE,
            "starting multipart upload"
        );

        let create = self
            .client
            .create_multipart_upload()
            .bucket(&self.config.bucket)
            .key(key)
            .content_type("application/zstd")
            .send()
            .await
            .map_err(|e| anyhow::anyhow!("S3 create_multipart_upload: {e:#}"))?;

        let upload_id = create
            .upload_id()
            .context("missing upload_id from create_multipart_upload")?
            .to_string();

        let result = self.upload_parts(path, key, &upload_id, file_size).await;

        match result {
            Ok(parts) => {
                let completed = CompletedMultipartUpload::builder()
                    .set_parts(Some(parts))
                    .build();

                self.client
                    .complete_multipart_upload()
                    .bucket(&self.config.bucket)
                    .key(key)
                    .upload_id(&upload_id)
                    .multipart_upload(completed)
                    .send()
                    .await
                    .map_err(|e| anyhow::anyhow!("S3 complete_multipart_upload: {e:#}"))?;

                Ok(())
            }
            Err(e) => {
                warn!(key, upload_id, error = %e, "aborting multipart upload");
                let _ = self
                    .client
                    .abort_multipart_upload()
                    .bucket(&self.config.bucket)
                    .key(key)
                    .upload_id(&upload_id)
                    .send()
                    .await;
                Err(e)
            }
        }
    }

    async fn upload_parts(
        &self,
        path: &std::path::Path,
        key: &str,
        upload_id: &str,
        file_size: u64,
    ) -> Result<Vec<CompletedPart>> {
        let total_parts = file_size.div_ceil(MULTIPART_PART_SIZE) as i32;

        let part_specs: Vec<(i32, u64, u64)> = (0..total_parts)
            .map(|i| {
                let pn = i + 1;
                let offset = i as u64 * MULTIPART_PART_SIZE;
                let length = std::cmp::min(MULTIPART_PART_SIZE, file_size - offset);
                (pn, offset, length)
            })
            .collect();

        let uploaded = Arc::new(AtomicU64::new(0));
        let progress_handle =
            spawn_upload_progress(Arc::clone(&uploaded), file_size, key.to_string());

        let path: Arc<std::path::Path> = Arc::from(path);
        let key: Arc<str> = Arc::from(key);
        let upload_id: Arc<str> = Arc::from(upload_id);

        let results: Vec<Result<CompletedPart>> = stream::iter(part_specs)
            .map(|(part_number, offset, length)| {
                let client = &self.client;
                let bucket = &self.config.bucket;
                let path = Arc::clone(&path);
                let key = Arc::clone(&key);
                let upload_id = Arc::clone(&upload_id);
                let uploaded = Arc::clone(&uploaded);

                async move {
                    let body = ByteStream::read_from()
                        .path(path.as_ref())
                        .offset(offset)
                        .length(aws_sdk_s3::primitives::Length::Exact(length))
                        .build()
                        .await
                        .context("open snapshot part for S3 upload")?;

                    let resp = client
                        .upload_part()
                        .bucket(bucket)
                        .key(key.as_ref())
                        .upload_id(upload_id.as_ref())
                        .part_number(part_number)
                        .content_length(length as i64)
                        .body(body)
                        .send()
                        .await
                        .map_err(|e| anyhow::anyhow!("S3 upload_part {part_number}: {e:#}"))?;

                    let etag = resp
                        .e_tag()
                        .context("missing ETag from upload_part")?
                        .to_string();

                    uploaded.fetch_add(length, Ordering::Relaxed);

                    Ok(CompletedPart::builder()
                        .part_number(part_number)
                        .e_tag(etag)
                        .build())
                }
            })
            .buffer_unordered(MULTIPART_CONCURRENCY)
            .collect()
            .await;

        progress_handle.abort();

        let mut parts = Vec::with_capacity(results.len());
        for r in results {
            parts.push(r?);
        }
        parts.sort_by_key(|p| p.part_number());
        Ok(parts)
    }

    async fn object_exists(&self, key: &str) -> Result<bool> {
        match self
            .client
            .head_object()
            .bucket(&self.config.bucket)
            .key(key)
            .send()
            .await
        {
            Ok(_) => Ok(true),
            Err(e) => {
                let svc_err = e.into_service_error();
                if svc_err.is_not_found() {
                    Ok(false)
                } else {
                    Err(anyhow::anyhow!("S3 head_object error: {}", svc_err))
                }
            }
        }
    }

    fn s3_key(&self, filename: &str) -> String {
        if self.config.prefix.is_empty() {
            filename.to_string()
        } else {
            format!("{}/{}", self.config.prefix.trim_end_matches('/'), filename)
        }
    }

    /// Periodic upload loop.
    pub async fn upload_loop(&self) {
        let interval = self.config.upload_interval;
        info!(
            interval_secs = interval.as_secs(),
            "starting S3 upload loop"
        );
        loop {
            match self.upload_best().await {
                Ok(uploaded) => {
                    if uploaded {
                        info!("S3 upload cycle: uploaded new snapshot");
                    }
                }
                Err(e) => {
                    warn!(error = format!("{e:#}"), "S3 upload cycle failed");
                }
            }
            tokio::time::sleep(interval).await;
        }
    }
}

fn format_bytes(bytes: u64) -> String {
    const GB: u64 = 1_073_741_824;
    const MB: u64 = 1_048_576;
    if bytes >= GB {
        format!("{:.1} GB", bytes as f64 / GB as f64)
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
        format!("{}h{}m", s / 3600, (s % 3600) / 60)
    } else if s >= 60 {
        format!("{}m{}s", s / 60, s % 60)
    } else {
        format!("{}s", s)
    }
}

const SPEED_WINDOW: usize = 4; // 4 x 5s = 20-second rolling window

fn spawn_upload_progress(
    uploaded: Arc<AtomicU64>,
    total_size: u64,
    key: String,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let start = Instant::now();
        let mut interval = tokio::time::interval(Duration::from_secs(5));
        interval.tick().await;
        let mut prev_bytes = 0u64;
        let mut prev_tick = Instant::now();
        let mut speed_samples = std::collections::VecDeque::with_capacity(SPEED_WINDOW);

        loop {
            interval.tick().await;
            let bytes = uploaded.load(Ordering::Relaxed);
            let elapsed = start.elapsed().as_secs_f64();
            let pct = if total_size > 0 {
                (bytes as f64 / total_size as f64) * 100.0
            } else {
                0.0
            };

            let now = Instant::now();
            let dt = now.duration_since(prev_tick).as_secs_f64();
            let sample_speed = if dt > 0.0 {
                (bytes - prev_bytes) as f64 / dt
            } else {
                0.0
            };
            prev_bytes = bytes;
            prev_tick = now;

            if speed_samples.len() >= SPEED_WINDOW {
                speed_samples.pop_front();
            }
            speed_samples.push_back(sample_speed);

            let smooth_speed = speed_samples.iter().sum::<f64>() / speed_samples.len() as f64;
            let remaining = if smooth_speed > 0.0 && total_size > bytes {
                (total_size - bytes) as f64 / smooth_speed
            } else if elapsed > 0.0 && total_size > bytes {
                let avg = bytes as f64 / elapsed;
                (total_size - bytes) as f64 / avg
            } else {
                0.0
            };

            info!(
                key,
                progress = format!("{:.1}%", pct),
                uploaded = format_bytes(bytes),
                total = format_bytes(total_size),
                speed = format!("{:.0} MB/s", smooth_speed / 1_048_576.0),
                eta = format_eta(remaining),
                "uploading"
            );

            if bytes >= total_size {
                break;
            }
        }
    })
}
