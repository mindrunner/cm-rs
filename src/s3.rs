use std::time::Instant;

use anyhow::{Context, Result};
use aws_sdk_s3::primitives::ByteStream;
use tracing::{info, warn};

use crate::config::S3Config;
use crate::index::SnapshotIndex;
use crate::metrics::Metrics;
use crate::snapshot::SnapshotInfo;

pub struct S3Uploader {
    config: S3Config,
    client: aws_sdk_s3::Client,
    index: SnapshotIndex,
    metrics: Metrics,
}

impl S3Uploader {
    pub async fn new(
        config: S3Config,
        index: SnapshotIndex,
        metrics: Metrics,
    ) -> Result<Self> {
        let mut aws_config = aws_config::defaults(aws_config::BehaviorVersion::latest());

        if let Some(ref region) = config.region {
            aws_config = aws_config.region(aws_config::Region::new(region.clone()));
        }

        let aws_config = aws_config.load().await;
        let mut s3_config = aws_sdk_s3::config::Builder::from(&aws_config);

        if let Some(ref endpoint) = config.endpoint {
            if !endpoint.is_empty() {
                s3_config = s3_config.endpoint_url(endpoint);
            }
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

    /// Upload the best available snapshot to S3 if it hasn't been uploaded yet.
    pub async fn upload_best(&self) -> Result<bool> {
        let snapshots = self.index.list().await;
        let best_full = match snapshots.iter().find(|s| s.is_full()) {
            Some(s) => s,
            None => {
                info!("no full snapshot to upload");
                return Ok(false);
            }
        };

        // Check if already uploaded
        let key = self.s3_key(&best_full.file_name);
        if self.object_exists(&key).await? {
            info!(key, "snapshot already uploaded");
            return Ok(false);
        }

        self.upload_snapshot(best_full).await?;

        // Also upload matching incremental if available
        if let Some(incremental) = snapshots.iter().find(|s| {
            !s.is_full() && s.base_slot == Some(best_full.slot)
        }) {
            let inc_key = self.s3_key(&incremental.file_name);
            if !self.object_exists(&inc_key).await? {
                self.upload_snapshot(incremental).await?;
            }
        }

        Ok(true)
    }

    async fn upload_snapshot(&self, snap: &SnapshotInfo) -> Result<()> {
        let path = self.index.dir().join(&snap.file_name);
        let key = self.s3_key(&snap.file_name);
        info!(file = snap.file_name, key, size = snap.size, "uploading to S3");

        let start = Instant::now();
        let body = ByteStream::from_path(&path)
            .await
            .context("open snapshot for S3 upload")?;

        self.client
            .put_object()
            .bucket(&self.config.bucket)
            .key(&key)
            .body(body)
            .content_type("application/zstd")
            .send()
            .await
            .context("S3 put_object failed")?;

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
                    warn!(error = %e, "S3 upload cycle failed");
                }
            }
            tokio::time::sleep(interval).await;
        }
    }
}
