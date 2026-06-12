use std::path::Path;

use anyhow::{Context, Result};
use tracing::info;

use super::DownloadResult;
use crate::config::Aria2cConfig;

/// Download a file using aria2c subprocess.
///
/// Returns bytes downloaded and SHA-256 digest of the output file.
pub async fn download(
    config: &Aria2cConfig,
    url: &str,
    dir: &Path,
    filename: &str,
    min_speed: u64,
) -> Result<DownloadResult> {
    info!(url, filename, "downloading via aria2c");

    let mut cmd = tokio::process::Command::new(&config.binary);
    cmd.arg("--dir")
        .arg(dir)
        .arg("--out")
        .arg(filename)
        .arg("--continue=true")
        .arg("--max-connection-per-server=16")
        .arg("--split=16")
        .arg("--min-split-size=1M")
        .arg("--console-log-level=notice")
        .arg("--summary-interval=5");

    if min_speed > 0 {
        cmd.arg(format!("--lowest-speed-limit={}", min_speed));
    }

    for arg in &config.extra_args {
        cmd.arg(arg);
    }

    cmd.arg(url);

    let status = cmd.status().await.context("failed to spawn aria2c")?;

    if !status.success() {
        anyhow::bail!("aria2c exited with {}", status);
    }

    let dest = dir.join(filename);
    let meta = tokio::fs::metadata(&dest)
        .await
        .context("aria2c output file missing")?;
    let bytes = meta.len();

    let sha256 = tokio::task::spawn_blocking(move || crate::snapshot::sha256_file(&dest))
        .await
        .context("hash task panicked")??;

    Ok(DownloadResult { bytes, sha256 })
}
