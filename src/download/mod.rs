pub mod aria2c;
pub mod builtin;

use std::path::{Path, PathBuf};
use std::time::Instant;

use anyhow::{Context, Result};
use tracing::{info, warn};

use crate::config::DownloadConfig;
use crate::index::SnapshotIndex;
use crate::metrics::Metrics;
use crate::peer::PeerManager;
use crate::snapshot::{SnapshotInfo, SnapshotType};

/// A downloadable snapshot set: the full snapshot and an optional incremental.
#[derive(Debug, Clone)]
pub struct DownloadPlan {
    pub peer_addr: String,
    pub full: SnapshotInfo,
    pub incremental: Option<SnapshotInfo>,
}

/// A successfully downloaded file in the tmp directory.
struct DownloadedFile {
    file_name: String,
    sha256: String,
    bytes: u64,
}

/// Download a single snapshot file to `tmp_dir`, verify its checksum, and
/// return metadata about the downloaded file.
async fn download_one_file(
    config: &DownloadConfig,
    peer_addr: &str,
    file: &SnapshotInfo,
    tmp_dir: &Path,
) -> Result<DownloadedFile> {
    let url = format!("http://{}/api/v1/snapshot/{}", peer_addr, file.file_name);
    let dest = tmp_dir.join(&file.file_name);

    let result = match config.backend {
        crate::config::DownloadBackend::Builtin => {
            builtin::download(&url, &dest, config.parallel_chunks, config.min_speed).await?
        }
        crate::config::DownloadBackend::Aria2c => {
            let bytes =
                aria2c::download(&config.aria2c, &url, tmp_dir, &file.file_name, config.min_speed)
                    .await?;
            let path = dest.clone();
            let hash =
                tokio::task::spawn_blocking(move || crate::snapshot::sha256_file(&path))
                    .await??;
            builtin::DownloadResult {
                bytes,
                sha256: hash,
            }
        }
    };

    if let Some(ref expected) = file.sha256 {
        if result.sha256 != *expected {
            tokio::fs::remove_file(&dest).await.ok();
            anyhow::bail!(
                "checksum mismatch for {}: expected {} got {}",
                file.file_name,
                expected,
                result.sha256
            );
        }
        info!(
            file = file.file_name,
            sha256 = result.sha256,
            "checksum verified"
        );
    }

    Ok(DownloadedFile {
        file_name: file.file_name.clone(),
        sha256: result.sha256,
        bytes: result.bytes,
    })
}

/// Move a downloaded file from `tmp_dir` to `snap_dir` and write a `.sha256`
/// sidecar file next to it.
async fn finalize_file(dl: &DownloadedFile, tmp_dir: &Path, snap_dir: &Path) -> Result<()> {
    let src = tmp_dir.join(&dl.file_name);
    let dst = snap_dir.join(&dl.file_name);
    tokio::fs::rename(&src, &dst)
        .await
        .with_context(|| format!("rename {} -> {}", src.display(), dst.display()))?;
    let mut sidecar = dst.as_os_str().to_owned();
    sidecar.push(".sha256");
    tokio::fs::write(PathBuf::from(sidecar), format!("{}\n", dl.sha256))
        .await
        .ok();
    Ok(())
}

/// Orchestrates finding the best snapshot and downloading it.
pub struct DownloadOrchestrator {
    config: DownloadConfig,
    index: SnapshotIndex,
    peer_mgr: PeerManager,
    metrics: Metrics,
}

impl DownloadOrchestrator {
    pub fn new(
        config: DownloadConfig,
        index: SnapshotIndex,
        peer_mgr: PeerManager,
        metrics: Metrics,
    ) -> Self {
        Self {
            config,
            index,
            peer_mgr,
            metrics,
        }
    }

    /// Check if we need a new snapshot and download it if so.
    /// Returns true if a download was performed.
    pub async fn fetch_if_needed(&self) -> Result<bool> {
        let local_slot = self.index.highest_slot().await;

        let ranked = self.peer_mgr.ranked_peers().await;
        if ranked.is_empty() {
            info!("no peers available, skipping fetch");
            return Ok(false);
        }

        let best_remote_slot = ranked.iter().map(|p| p.highest_slot()).max().unwrap_or(0);

        if best_remote_slot <= local_slot {
            info!(local_slot, best_remote_slot, "local is up to date");
            return Ok(false);
        }

        let diff = best_remote_slot - local_slot;
        if diff < self.config.min_slots_behind {
            info!(
                local_slot,
                best_remote_slot,
                diff,
                min = self.config.min_slots_behind,
                "not far enough behind, skipping"
            );
            return Ok(false);
        }

        info!(
            local_slot,
            best_remote_slot,
            diff,
            "snapshot is stale, fetching"
        );

        for peer in &ranked {
            if peer.highest_slot() < best_remote_slot {
                continue;
            }
            let plan = match build_download_plan(peer, best_remote_slot) {
                Some(p) => p,
                None => continue,
            };
            info!(
                peer = peer.addr,
                full = plan.full.file_name,
                incremental = plan.incremental.as_ref().map(|s| &s.file_name),
                "attempting download"
            );

            let start = Instant::now();
            match self.download_plan(&plan, best_remote_slot).await {
                Ok(bytes) => {
                    let duration = start.elapsed();
                    let bps = bytes as f64 / duration.as_secs_f64();
                    self.peer_mgr.record_bandwidth(&peer.addr, bps).await;
                    self.metrics
                        .download_duration
                        .observe(duration.as_secs_f64());
                    self.metrics.download_bytes.inc_by(bytes);
                    info!(
                        peer = peer.addr,
                        bytes,
                        duration_secs = duration.as_secs_f64(),
                        mbps = bps / 1_000_000.0,
                        "download complete"
                    );
                    self.index.scan().await?;
                    self.cleanup().await?;
                    return Ok(true);
                }
                Err(e) => {
                    warn!(peer = peer.addr, error = %e, "download failed, trying next peer");
                    continue;
                }
            }
        }

        anyhow::bail!("all peers failed");
    }

    async fn download_plan(
        &self,
        plan: &DownloadPlan,
        best_remote_slot: u64,
    ) -> Result<u64> {
        let snap_dir = self.index.dir();
        let tmp_dir = snap_dir.join(".cm-tmp");
        tokio::fs::create_dir_all(&tmp_dir).await?;

        let full_local = self.exists_locally(&plan.full).await;
        let inc_local = match &plan.incremental {
            Some(inc) => self.exists_locally(inc).await,
            None => false,
        };

        if full_local {
            info!(file = plan.full.file_name, "already exists locally, skipping download");
        }
        if inc_local {
            info!(
                file = plan.incremental.as_ref().unwrap().file_name,
                "already exists locally, skipping download"
            );
        }

        let need_full = !full_local;
        let need_inc = plan.incremental.is_some() && !inc_local;

        let (full_dl, inc_dl) = match (need_full, need_inc) {
            (true, true) => {
                let full_fut =
                    download_one_file(&self.config, &plan.peer_addr, &plan.full, &tmp_dir);
                let inc_fut = download_one_file(
                    &self.config,
                    &plan.peer_addr,
                    plan.incremental.as_ref().unwrap(),
                    &tmp_dir,
                );
                let (full_result, inc_result) = tokio::join!(full_fut, inc_fut);
                let full_dl = full_result?;
                let inc_dl = match inc_result {
                    Ok(dl) => Some(dl),
                    Err(e) => {
                        warn!(error = %e, "concurrent incremental download failed, will retry");
                        None
                    }
                };
                (Some(full_dl), inc_dl)
            }
            (true, false) => {
                let full_dl =
                    download_one_file(&self.config, &plan.peer_addr, &plan.full, &tmp_dir).await?;
                (Some(full_dl), None)
            }
            (false, true) => {
                let inc_dl = match download_one_file(
                    &self.config,
                    &plan.peer_addr,
                    plan.incremental.as_ref().unwrap(),
                    &tmp_dir,
                )
                .await
                {
                    Ok(dl) => Some(dl),
                    Err(e) => {
                        warn!(error = %e, "incremental download failed, will retry");
                        None
                    }
                };
                (None, inc_dl)
            }
            (false, false) => (None, None),
        };

        // If we expected an incremental but didn't get one (and don't have it locally), retry
        let inc_dl = match inc_dl {
            Some(dl) => Some(dl),
            None if plan.incremental.is_some() && !inc_local => {
                self.try_fetch_incremental(plan.full.slot, &tmp_dir, &plan.peer_addr)
                    .await
            }
            None => None,
        };

        let got_incremental = inc_dl.is_some() || inc_local;

        // Staleness gate: if no incremental, check whether the full alone is fresh enough
        if !got_incremental {
            let staleness = best_remote_slot.saturating_sub(plan.full.slot);
            if staleness > self.config.min_slots_behind {
                if !full_local {
                    tokio::fs::remove_file(tmp_dir.join(&plan.full.file_name))
                        .await
                        .ok();
                }
                let _ = tokio::fs::remove_dir(&tmp_dir).await;
                anyhow::bail!(
                    "full snapshot at slot {} is {} slots behind tip {}, \
                     exceeds min_slots_behind={}, need incremental",
                    plan.full.slot,
                    staleness,
                    best_remote_slot,
                    self.config.min_slots_behind,
                );
            }
            warn!(
                full_slot = plan.full.slot,
                staleness,
                "accepting full-only snapshot (within min_slots_behind)"
            );
        }

        // Move downloaded files to snapshot dir
        let mut total_bytes = 0u64;
        if let Some(ref dl) = full_dl {
            total_bytes += dl.bytes;
            finalize_file(dl, &tmp_dir, snap_dir).await?;
        }
        if let Some(ref dl) = inc_dl {
            total_bytes += dl.bytes;
            finalize_file(dl, &tmp_dir, snap_dir).await?;
        }

        let _ = tokio::fs::remove_dir(&tmp_dir).await;
        Ok(total_bytes)
    }

    /// Check whether a snapshot already exists locally with a matching hash.
    async fn exists_locally(&self, remote: &SnapshotInfo) -> bool {
        let local = match self.index.get(&remote.file_name).await {
            Some(s) => s,
            None => return false,
        };
        match (&local.sha256, &remote.sha256) {
            (Some(local_hash), Some(remote_hash)) => local_hash == remote_hash,
            (Some(_), None) => true,
            _ => false,
        }
    }

    /// Re-poll peers live to find and download an incremental snapshot
    /// compatible with `full_slot`. Tries the original peer first, then others.
    async fn try_fetch_incremental(
        &self,
        full_slot: u64,
        tmp_dir: &Path,
        original_peer: &str,
    ) -> Option<DownloadedFile> {
        let all_peers = self.peer_mgr.list_peers().await;
        let mut addrs: Vec<String> = Vec::with_capacity(all_peers.len());
        addrs.push(original_peer.to_string());
        for p in &all_peers {
            if p.addr != original_peer {
                addrs.push(p.addr.clone());
            }
        }

        for addr in &addrs {
            let snapshots = match self.peer_mgr.poll_peer(addr).await {
                Ok((snaps, _)) => snaps,
                Err(e) => {
                    warn!(peer = %addr, error = %e, "failed to re-poll peer for incremental");
                    continue;
                }
            };

            let best_inc = snapshots
                .iter()
                .filter(|s| {
                    s.snapshot_type == SnapshotType::Incremental
                        && s.base_slot == Some(full_slot)
                })
                .max_by_key(|s| s.slot);

            let inc = match best_inc {
                Some(s) => s.clone(),
                None => continue,
            };

            info!(
                peer = %addr,
                file = inc.file_name,
                "retrying incremental download"
            );

            match download_one_file(&self.config, addr, &inc, tmp_dir).await {
                Ok(dl) => return Some(dl),
                Err(e) => {
                    warn!(peer = %addr, error = %e, "incremental retry failed");
                    continue;
                }
            }
        }

        warn!(full_slot, "no peer could serve an incremental snapshot");
        None
    }

    async fn cleanup(&self) -> Result<()> {
        let snap_dir = self.index.dir();
        cleanup_old_snapshots(
            snap_dir,
            self.config.max_full_snapshots,
            self.config.max_incremental_snapshots,
        )
        .await
    }
}

pub fn build_download_plan(peer: &crate::peer::PeerInfo, _target_slot: u64) -> Option<DownloadPlan> {
    // Find the best full snapshot at or near the target
    let full = peer
        .snapshots
        .iter()
        .filter(|s| s.snapshot_type == SnapshotType::Full)
        .max_by_key(|s| s.slot)?;

    // Find the best incremental for this full snapshot's base
    let incremental = peer
        .snapshots
        .iter()
        .filter(|s| s.snapshot_type == SnapshotType::Incremental && s.base_slot == Some(full.slot))
        .max_by_key(|s| s.slot);

    Some(DownloadPlan {
        peer_addr: peer.addr.clone(),
        full: full.clone(),
        incremental: incremental.cloned(),
    })
}

pub async fn cleanup_old_snapshots(
    dir: &Path,
    max_full: u32,
    max_incremental: u32,
) -> Result<()> {
    let mut fulls = list_snapshots_by_type(dir, "snapshot-").await?;
    let mut incrementals = list_snapshots_by_type(dir, "incremental-snapshot-").await?;

    // Sort newest first by slot extracted from filename
    fulls.sort_by(|a, b| b.1.cmp(&a.1));
    incrementals.sort_by(|a, b| b.1.cmp(&a.1));

    let valid_full_slots: std::collections::HashSet<u64> = fulls
        .iter()
        .take(max_full as usize)
        .map(|(_, slot)| *slot)
        .collect();

    // Remove excess full snapshots
    for (path, _) in fulls.iter().skip(max_full as usize) {
        info!(file = %path.display(), "removing old full snapshot");
        tokio::fs::remove_file(path).await.ok();
        remove_sidecar(path).await;
    }

    // Remove orphaned incrementals (base slot not in kept fulls)
    let mut kept_incrementals = 0u32;
    for (path, _) in &incrementals {
        let fname = path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("");
        if let Some(base_slot) = parse_incremental_base_slot(fname) {
            if !valid_full_slots.contains(&base_slot) {
                info!(file = %path.display(), "removing orphaned incremental snapshot");
                tokio::fs::remove_file(path).await.ok();
                remove_sidecar(path).await;
                continue;
            }
        }
        kept_incrementals += 1;
        if kept_incrementals > max_incremental {
            info!(file = %path.display(), "removing old incremental snapshot");
            tokio::fs::remove_file(path).await.ok();
            remove_sidecar(path).await;
        }
    }

    Ok(())
}

async fn list_snapshots_by_type(dir: &Path, prefix: &str) -> Result<Vec<(PathBuf, u64)>> {
    let mut result = Vec::new();
    let mut entries = tokio::fs::read_dir(dir).await?;
    while let Some(entry) = entries.next_entry().await? {
        let name = entry.file_name();
        let name_str = match name.to_str() {
            Some(n) => n,
            None => continue,
        };
        if !name_str.starts_with(prefix) || !name_str.ends_with(".tar.zst") {
            continue;
        }
        // Skip .aria2 companions
        if entry.path().with_extension("zst.aria2").exists() {
            continue;
        }
        let slot = extract_primary_slot(name_str).unwrap_or(0);
        result.push((entry.path(), slot));
    }
    Ok(result)
}

async fn remove_sidecar(snapshot_path: &Path) {
    let mut p = snapshot_path.as_os_str().to_owned();
    p.push(".sha256");
    tokio::fs::remove_file(PathBuf::from(p)).await.ok();
}

pub fn extract_primary_slot(name: &str) -> Option<u64> {
    if let Some(rest) = name.strip_prefix("incremental-snapshot-") {
        let rest = rest.strip_suffix(".tar.zst")?;
        let mut parts = rest.splitn(3, '-');
        let _base: u64 = parts.next()?.parse().ok()?;
        let slot: u64 = parts.next()?.parse().ok()?;
        Some(slot)
    } else if let Some(rest) = name.strip_prefix("snapshot-") {
        let rest = rest.strip_suffix(".tar.zst")?;
        let slot: u64 = rest.splitn(2, '-').next()?.parse().ok()?;
        Some(slot)
    } else {
        None
    }
}

pub fn parse_incremental_base_slot(name: &str) -> Option<u64> {
    let rest = name.strip_prefix("incremental-snapshot-")?;
    let rest = rest.strip_suffix(".tar.zst")?;
    rest.splitn(3, '-').next()?.parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::peer::{PeerInfo, PeerStats};
    use crate::snapshot::{SnapshotInfo, SnapshotType};

    fn make_snap(slot: u64, full: bool, base: Option<u64>) -> SnapshotInfo {
        SnapshotInfo {
            file_name: if full {
                format!("snapshot-{}-hash.tar.zst", slot)
            } else {
                format!(
                    "incremental-snapshot-{}-{}-hash.tar.zst",
                    base.unwrap(),
                    slot
                )
            },
            snapshot_type: if full {
                SnapshotType::Full
            } else {
                SnapshotType::Incremental
            },
            slot,
            base_slot: base,
            size: 1000,
            modified_at: 0,
            sha256: None,
        }
    }

    #[test]
    fn extract_primary_slot_full() {
        assert_eq!(
            extract_primary_slot("snapshot-12345-hash.tar.zst"),
            Some(12345)
        );
    }

    #[test]
    fn extract_primary_slot_incremental() {
        assert_eq!(
            extract_primary_slot("incremental-snapshot-100-200-hash.tar.zst"),
            Some(200)
        );
    }

    #[test]
    fn extract_primary_slot_garbage() {
        assert_eq!(extract_primary_slot("random.tar.zst"), None);
    }

    #[test]
    fn parse_base_slot() {
        assert_eq!(
            parse_incremental_base_slot("incremental-snapshot-100-200-hash.tar.zst"),
            Some(100)
        );
    }

    #[test]
    fn parse_base_slot_not_incremental() {
        assert_eq!(
            parse_incremental_base_slot("snapshot-100-hash.tar.zst"),
            None
        );
    }

    #[test]
    fn build_plan_selects_best() {
        let peer = PeerInfo {
            addr: "host:1".into(),
            snapshots: vec![
                make_snap(100, true, None),
                make_snap(200, true, None),
                make_snap(250, false, Some(200)),
                make_snap(150, false, Some(100)),
            ],
            stats: PeerStats::default(),
        };
        let plan = build_download_plan(&peer, 250).unwrap();
        assert_eq!(plan.full.slot, 200);
        assert_eq!(plan.incremental.as_ref().unwrap().slot, 250);
    }

    #[test]
    fn build_plan_no_matching_incremental() {
        let peer = PeerInfo {
            addr: "host:1".into(),
            snapshots: vec![
                make_snap(100, true, None),
                make_snap(300, false, Some(999)), // orphan
            ],
            stats: PeerStats::default(),
        };
        let plan = build_download_plan(&peer, 100).unwrap();
        assert_eq!(plan.full.slot, 100);
        assert!(plan.incremental.is_none());
    }

    #[test]
    fn build_plan_no_full_snapshot() {
        let peer = PeerInfo {
            addr: "host:1".into(),
            snapshots: vec![make_snap(200, false, Some(100))],
            stats: PeerStats::default(),
        };
        assert!(build_download_plan(&peer, 200).is_none());
    }

    fn touch(dir: &std::path::Path, name: &str) {
        std::fs::write(dir.join(name), &[0u8; 64]).unwrap();
    }

    #[tokio::test]
    async fn cleanup_removes_excess_full() {
        let dir = tempfile::tempdir().unwrap();
        touch(dir.path(), "snapshot-100-a.tar.zst");
        touch(dir.path(), "snapshot-200-b.tar.zst");
        touch(dir.path(), "snapshot-300-c.tar.zst");

        cleanup_old_snapshots(dir.path(), 2, 4).await.unwrap();

        assert!(dir.path().join("snapshot-300-c.tar.zst").exists());
        assert!(dir.path().join("snapshot-200-b.tar.zst").exists());
        assert!(!dir.path().join("snapshot-100-a.tar.zst").exists());
    }

    #[tokio::test]
    async fn cleanup_removes_orphaned_incrementals() {
        let dir = tempfile::tempdir().unwrap();
        touch(dir.path(), "snapshot-200-a.tar.zst");
        touch(
            dir.path(),
            "incremental-snapshot-200-250-hash.tar.zst",
        );
        // Orphaned: base slot 100 has no full snapshot
        touch(
            dir.path(),
            "incremental-snapshot-100-150-hash.tar.zst",
        );

        cleanup_old_snapshots(dir.path(), 2, 4).await.unwrap();

        assert!(dir
            .path()
            .join("incremental-snapshot-200-250-hash.tar.zst")
            .exists());
        assert!(!dir
            .path()
            .join("incremental-snapshot-100-150-hash.tar.zst")
            .exists());
    }

    #[tokio::test]
    async fn cleanup_removes_excess_incrementals() {
        let dir = tempfile::tempdir().unwrap();
        touch(dir.path(), "snapshot-100-a.tar.zst");
        touch(
            dir.path(),
            "incremental-snapshot-100-200-a.tar.zst",
        );
        touch(
            dir.path(),
            "incremental-snapshot-100-300-b.tar.zst",
        );
        touch(
            dir.path(),
            "incremental-snapshot-100-400-c.tar.zst",
        );

        cleanup_old_snapshots(dir.path(), 2, 2).await.unwrap();

        // Should keep 2 newest incrementals (400, 300) and remove 200
        assert!(dir
            .path()
            .join("incremental-snapshot-100-400-c.tar.zst")
            .exists());
        assert!(dir
            .path()
            .join("incremental-snapshot-100-300-b.tar.zst")
            .exists());
        assert!(!dir
            .path()
            .join("incremental-snapshot-100-200-a.tar.zst")
            .exists());
    }

    fn make_orchestrator(dir: &std::path::Path) -> (DownloadOrchestrator, SnapshotIndex) {
        let index = crate::index::SnapshotIndex::new(dir.to_path_buf());
        let config = crate::config::DownloadConfig::default();
        let metrics = crate::metrics::Metrics::new(None);
        let peer_mgr = crate::peer::PeerManager::new(
            crate::config::PeersConfig::default(),
            Vec::new(),
            metrics.clone(),
        );
        let orch = DownloadOrchestrator::new(config, index.clone(), peer_mgr, metrics);
        (orch, index)
    }

    #[tokio::test]
    async fn exists_locally_matching_hash() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("snapshot-100-hash.tar.zst"), b"data").unwrap();

        let (orch, index) = make_orchestrator(dir.path());
        index.scan().await.unwrap();

        let local = index.get("snapshot-100-hash.tar.zst").await.unwrap();
        let local_hash = local.sha256.clone().unwrap();

        let remote = SnapshotInfo {
            sha256: Some(local_hash),
            ..make_snap(100, true, None)
        };
        assert!(orch.exists_locally(&remote).await);
    }

    #[tokio::test]
    async fn exists_locally_no_remote_hash() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("snapshot-100-hash.tar.zst"), b"data").unwrap();

        let (orch, index) = make_orchestrator(dir.path());
        index.scan().await.unwrap();

        let remote = make_snap(100, true, None);
        assert!(remote.sha256.is_none());
        assert!(orch.exists_locally(&remote).await);
    }

    #[tokio::test]
    async fn exists_locally_hash_mismatch() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("snapshot-100-hash.tar.zst"), b"data").unwrap();

        let (orch, index) = make_orchestrator(dir.path());
        index.scan().await.unwrap();

        let remote = SnapshotInfo {
            sha256: Some("0000000000000000000000000000000000000000000000000000000000000000".into()),
            ..make_snap(100, true, None)
        };
        assert!(!orch.exists_locally(&remote).await);
    }

    #[tokio::test]
    async fn exists_locally_not_present() {
        let dir = tempfile::tempdir().unwrap();

        let (orch, index) = make_orchestrator(dir.path());
        index.scan().await.unwrap();

        let remote = make_snap(100, true, None);
        assert!(!orch.exists_locally(&remote).await);
    }

    #[tokio::test]
    async fn exists_locally_incremental() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join("incremental-snapshot-100-200-hash.tar.zst"),
            b"inc-data",
        )
        .unwrap();

        let (orch, index) = make_orchestrator(dir.path());
        index.scan().await.unwrap();

        let local = index
            .get("incremental-snapshot-100-200-hash.tar.zst")
            .await
            .unwrap();
        let local_hash = local.sha256.clone().unwrap();

        let remote = SnapshotInfo {
            sha256: Some(local_hash),
            ..make_snap(200, false, Some(100))
        };
        assert!(orch.exists_locally(&remote).await);
    }
}
