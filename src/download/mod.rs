pub mod aria2c;
pub mod builtin;
pub mod policy;

use std::path::Path;
use std::time::Instant;

use anyhow::Result;
use tracing::{info, warn};

use crate::config::DownloadConfig;
use crate::index::SnapshotIndex;
use crate::metrics::Metrics;
use crate::peer::PeerManager;
use crate::snapshot::{self, SnapshotInfo};

pub use policy::{
    DownloadPlan, FetchDecision, FetchReason, FullFetchGate, agave_bootstrap_slot,
    build_download_plan, decide, plan_resulting_bootstrap, rank_incremental_plans,
    rank_newer_full_plans,
};

/// Result of a backend download: bytes written and SHA-256 hex digest.
pub struct DownloadResult {
    pub bytes: u64,
    pub sha256: String,
}

/// A successfully downloaded file in the staging directory.
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

    // Reuse a previously staged file whose contents already match the
    // peer-reported hash (e.g. a verified full kept after a staleness bail) —
    // hashing is minutes, re-downloading is hours.
    if let Some(ref expected) = file.sha256
        && tokio::fs::try_exists(&dest).await.unwrap_or(false)
    {
        let path = dest.clone();
        if let Ok(Ok(hash)) =
            tokio::task::spawn_blocking(move || crate::snapshot::sha256_file(&path)).await
            && hash == *expected
        {
            let bytes = tokio::fs::metadata(&dest).await?.len();
            info!(
                file = file.file_name,
                "reusing staged file with matching checksum"
            );
            return Ok(DownloadedFile {
                file_name: file.file_name.clone(),
                sha256: hash,
                bytes,
            });
        }
    }

    let result = match config.backend {
        crate::config::DownloadBackend::Builtin => {
            builtin::download(&url, &dest, config.parallel_chunks, config.min_speed).await?
        }
        crate::config::DownloadBackend::Aria2c => {
            aria2c::download(
                &config.aria2c,
                &url,
                tmp_dir,
                &file.file_name,
                config.min_speed,
            )
            .await?
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
        let local_snaps = self.index.list().await;
        let ranked = self.peer_mgr.ranked_peers().await;
        if ranked.is_empty() {
            info!("no peers available, skipping fetch");
            return Ok(false);
        }

        let decision = policy::decide(&local_snaps, &ranked, self.config.min_slots_behind);

        let Some(reason) = decision.reason else {
            match decision.gate {
                FullFetchGate::UpToDate => info!(
                    local_bootstrap = decision.local_bootstrap,
                    best_remote_slot = decision.best_remote_slot,
                    "local bootstrap is up to date with peers"
                ),
                FullFetchGate::NotFarEnoughBehind { diff } => info!(
                    local_bootstrap = decision.local_bootstrap,
                    best_remote_slot = decision.best_remote_slot,
                    diff,
                    min = self.config.min_slots_behind,
                    "not far enough behind for new full snapshot"
                ),
                FullFetchGate::Seek { .. } => info!(
                    local_bootstrap = decision.local_bootstrap,
                    best_remote_slot = decision.best_remote_slot,
                    "behind, but no worthwhile plan from peers; waiting"
                ),
            }
            return Ok(false);
        };

        info!(
            reason = reason.as_str(),
            local_bootstrap = decision.local_bootstrap,
            best_remote_slot = decision.best_remote_slot,
            plans = decision.plans.len(),
            "fetch cycle planned"
        );

        for plan in &decision.plans {
            if plan.incremental_only {
                info!(
                    peer = plan.peer_addr,
                    base_full = plan.full.slot,
                    incremental = plan.incremental.as_ref().map(|s| s.slot),
                    "attempting incremental on local full"
                );
            } else {
                info!(
                    peer = plan.peer_addr,
                    full = plan.full.file_name,
                    incremental = plan.incremental.as_ref().map(|s| &s.file_name),
                    "attempting newer full snapshot plan"
                );
            }
            match self
                .execute_plan(
                    plan,
                    decision.best_remote_slot,
                    decision.local_bootstrap,
                    reason,
                )
                .await
            {
                Ok(true) => return Ok(true),
                Ok(false) => continue,
                Err(e) => {
                    warn!(peer = plan.peer_addr, error = %e, "download failed, trying next plan");
                }
            }
        }

        info!(
            reason = reason.as_str(),
            "no download plan succeeded this cycle"
        );
        Ok(false)
    }

    async fn plan_needs_download(&self, plan: &DownloadPlan) -> bool {
        let dir = self.index.dir();
        if plan.incremental_only {
            if let Some(inc) = &plan.incremental {
                return !snapshot::file_complete(dir, &inc.file_name).await;
            }
            return false;
        }
        let need_full = !snapshot::file_complete(dir, &plan.full.file_name).await;
        let need_inc = match &plan.incremental {
            Some(inc) => !snapshot::file_complete(dir, &inc.file_name).await,
            None => false,
        };
        need_full || need_inc
    }

    async fn execute_plan(
        &self,
        plan: &DownloadPlan,
        best_remote_slot: u64,
        local_bootstrap: u64,
        reason: FetchReason,
    ) -> Result<bool> {
        if !self.plan_needs_download(plan).await {
            info!("snapshot plan already satisfied locally");
            return Ok(false);
        }

        let start = Instant::now();
        match self
            .download_plan(plan, best_remote_slot, local_bootstrap)
            .await
        {
            Ok(bytes) if bytes > 0 => {
                let duration = start.elapsed();
                let bps = bytes as f64 / duration.as_secs_f64();
                self.peer_mgr.record_bandwidth(&plan.peer_addr, bps).await;
                self.metrics
                    .download_duration
                    .observe(duration.as_secs_f64());
                self.metrics.download_bytes.inc_by(bytes);
                self.metrics
                    .downloads_by_reason
                    .get_or_create(&crate::metrics::ReasonLabel::new(reason.as_str()))
                    .inc();
                info!(
                    peer = plan.peer_addr,
                    bytes,
                    duration_secs = duration.as_secs_f64(),
                    mbps = bps / 1_000_000.0,
                    "download complete"
                );
                self.index
                    .retain(
                        self.config.max_full_snapshots,
                        self.config.max_incremental_snapshots,
                    )
                    .await?;
                Ok(true)
            }
            Ok(_) => Ok(false),
            Err(e) => Err(e),
        }
    }

    async fn download_plan(
        &self,
        plan: &DownloadPlan,
        best_remote_slot: u64,
        local_bootstrap: u64,
    ) -> Result<u64> {
        let snap_dir = self.index.dir();
        if plan.incremental_only && !snapshot::file_complete(snap_dir, &plan.full.file_name).await {
            anyhow::bail!(
                "incremental-only plan requires base full on disk: {}",
                plan.full.file_name
            );
        }

        let tmp_dir = self.index.staging_dir();
        tokio::fs::create_dir_all(&tmp_dir).await?;

        let full_local = self.exists_locally(&plan.full).await;
        let inc_local = match &plan.incremental {
            Some(inc) => self.exists_locally(inc).await,
            None => false,
        };

        if full_local {
            info!(
                file = plan.full.file_name,
                "already exists locally, skipping download"
            );
        }
        if inc_local {
            info!(
                file = plan.incremental.as_ref().unwrap().file_name,
                "already exists locally, skipping download"
            );
        }

        let need_full = !plan.incremental_only && !full_local;
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
                self.try_fetch_incremental(
                    plan.full.slot,
                    local_bootstrap,
                    &tmp_dir,
                    &plan.peer_addr,
                )
                .await
            }
            None => None,
        };

        let got_incremental = inc_dl.is_some() || inc_local;

        // Staleness gate: full-only downloads must be fresh enough relative to cluster tip.
        if !got_incremental && !plan.incremental_only {
            if let Some(staleness) = policy::full_only_too_stale(
                plan.full.slot,
                best_remote_slot,
                self.config.min_slots_behind,
            ) {
                // Keep the verified full in staging: next cycle reuses it via
                // the checksum match in download_one_file instead of pulling
                // the same 100 GB again.
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
                staleness = best_remote_slot.saturating_sub(plan.full.slot),
                "accepting full-only snapshot (within min_slots_behind)"
            );
        }

        // Admit downloaded files into the snapshot store
        let mut total_bytes = 0u64;
        if let Some(ref dl) = full_dl {
            total_bytes += dl.bytes;
            self.index.admit(&dl.file_name, &dl.sha256).await?;
        }
        if let Some(ref dl) = inc_dl {
            total_bytes += dl.bytes;
            self.index.admit(&dl.file_name, &dl.sha256).await?;
        }

        let _ = tokio::fs::remove_dir(&tmp_dir).await;
        Ok(total_bytes)
    }

    /// Check whether a completed snapshot file already exists on disk.
    async fn exists_locally(&self, remote: &SnapshotInfo) -> bool {
        snapshot::file_complete(self.index.dir(), &remote.file_name).await
    }

    /// Re-poll peers live to find and download an incremental snapshot
    /// compatible with `full_slot` and strictly above `min_slot` (pass the
    /// local bootstrap so we never fetch one we already surpass). Tries the
    /// original peer first, then others.
    async fn try_fetch_incremental(
        &self,
        full_slot: u64,
        min_slot: u64,
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

            let inc = match policy::best_incremental_for(&snapshots, full_slot, min_slot) {
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
}

#[cfg(test)]
mod tests {
    use super::*;
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

    fn make_orchestrator(dir: &std::path::Path) -> (DownloadOrchestrator, SnapshotIndex) {
        let index = crate::index::SnapshotIndex::new(dir.to_path_buf());
        let config = crate::config::DownloadConfig::default();
        let metrics = crate::metrics::Metrics::new(None);
        let peer_mgr =
            crate::peer::PeerManager::new(crate::config::PeersConfig::default(), metrics.clone());
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
    async fn exists_locally_hash_mismatch_still_skips() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("snapshot-100-hash.tar.zst"), b"data").unwrap();

        let (orch, index) = make_orchestrator(dir.path());
        index.scan().await.unwrap();

        let remote = SnapshotInfo {
            sha256: Some("0000000000000000000000000000000000000000000000000000000000000000".into()),
            ..make_snap(100, true, None)
        };
        assert!(orch.exists_locally(&remote).await);
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
