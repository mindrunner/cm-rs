pub mod aria2c;
pub mod builtin;
pub mod policy;

use std::collections::HashSet;
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
    DownloadPlan, FallbackTrigger, FetchDecision, FetchReason, FullFetchGate, PlanAttempt,
    SnapshotSource, SourceView, agave_bootstrap_slot, build_download_plan, decide,
    plan_resulting_bootstrap, rank_incremental_plans, rank_newer_full_plans,
};

use crate::fallback::FallbackManager;

/// Result of a backend download: bytes written and SHA-256 hex digest.
pub struct DownloadResult {
    pub bytes: u64,
    pub sha256: String,
}

/// The progress watchdog gave up on a transfer: below `min_speed`, or no
/// bytes at all, for a whole window. `measured_bps` is the last measured
/// speed (builtin backend) or the floor it fell below (aria2c, which reports
/// no speed), so callers can mark the source as slow.
#[derive(Debug)]
pub struct SlowTransfer {
    pub measured_bps: f64,
}

impl std::fmt::Display for SlowTransfer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("download aborted: speed below minimum threshold")
    }
}

impl std::error::Error for SlowTransfer {}

/// What one fetch cycle achieved.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FetchOutcome {
    /// A snapshot was downloaded and admitted.
    Downloaded,
    /// No source is far enough ahead of the local bootstrap to justify a download.
    NotNeeded,
    /// No ranked peer (and no usable external source) to plan against.
    NoPeers,
    /// The cycle wanted a snapshot for this reason and none landed.
    Unresolved(FetchReason),
}

/// Result of attempting one tier's plans.
enum PlansResult {
    /// A plan from this source landed.
    Downloaded(SnapshotSource),
    /// Nothing landed; `any_err` is true when at least one plan failed
    /// (rather than every plan being already satisfied locally).
    Failed { any_err: bool },
}

/// A successfully downloaded file in the staging directory.
struct DownloadedFile {
    file_name: String,
    sha256: String,
    bytes: u64,
}

/// Resolve where to actually download `planned` from an external source.
///
/// Mirrors like avorio only serve files through the alias redirect (a
/// short-lived presigned URL) and 404 the concrete `{base}/{file}` path, so
/// re-resolve the alias at download time. A full must resolve to exactly the
/// planned file — substituting a rotated full would desync the plan's
/// staleness gate and incremental pairing. A rotated *incremental* on the
/// same base is accepted: it pairs with the same full and only improves the
/// resulting bootstrap. Anything else falls back to the direct path (agave
/// RPC nodes serve concrete filenames).
async fn resolve_external_download(
    fallback: Option<&FallbackManager>,
    source: &SnapshotSource,
    planned: &SnapshotInfo,
) -> (String, SnapshotInfo) {
    let direct = (source.url_for(&planned.file_name), planned.clone());
    let (Some(fallback), SnapshotSource::External { base_url }) = (fallback, source) else {
        return direct;
    };

    let alias = if planned.is_full() {
        crate::fallback::probe::FULL_ALIAS
    } else {
        crate::fallback::probe::INCREMENTAL_ALIAS
    };

    if let Some((url, actual)) =
        crate::fallback::probe::resolve_alias(fallback.probe_client(), base_url, alias).await
    {
        let acceptable = if planned.is_full() {
            actual.file_name == planned.file_name
        } else {
            !actual.is_full()
                && actual.base_slot == planned.base_slot
                && actual.slot >= planned.slot
        };
        if acceptable {
            if actual.file_name != planned.file_name {
                info!(
                    planned = planned.file_name,
                    actual = actual.file_name,
                    "external source rotated its incremental; downloading current one"
                );
            }
            return (url, actual);
        }
    }

    direct
}

/// Download a single snapshot file to `tmp_dir`, verify its checksum, and
/// return metadata about the downloaded file. `min_speed` overrides the
/// configured floor (0 = abort only on a stall).
async fn download_one_file(
    config: &DownloadConfig,
    fallback: Option<&FallbackManager>,
    source: &SnapshotSource,
    file: &SnapshotInfo,
    tmp_dir: &Path,
    min_speed: u64,
) -> Result<DownloadedFile> {
    let (url, file) = match source {
        SnapshotSource::Peer { .. } => (source.url_for(&file.file_name), file.clone()),
        SnapshotSource::External { .. } => resolve_external_download(fallback, source, file).await,
    };
    let file = &file;
    let dest = tmp_dir.join(&file.file_name);

    // Reuse a previously staged file whose contents already match a known
    // hash (e.g. a verified full kept after a staleness bail) — hashing is
    // minutes, re-downloading is hours. External sources report no hash, so
    // fall back to the sidecar written into staging after a completed
    // download (see the staleness bail in `download_plan`).
    let expected_hash = file
        .sha256
        .clone()
        .or_else(|| crate::snapshot::load_sidecar_hash(&dest));
    if let Some(ref expected) = expected_hash
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
            builtin::download(&url, &dest, config.parallel_chunks, min_speed).await?
        }
        crate::config::DownloadBackend::Aria2c => {
            aria2c::download(&config.aria2c, &url, tmp_dir, &file.file_name, min_speed).await?
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
    fallback: Option<FallbackManager>,
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
            fallback: None,
        }
    }

    /// Attach an external-source fallback tier.
    pub fn with_fallback(mut self, fallback: Option<FallbackManager>) -> Self {
        self.fallback = fallback;
        self
    }

    /// Check if we need a new snapshot and download it if so.
    pub async fn fetch_if_needed(&self) -> Result<FetchOutcome> {
        let local_snaps = self.index.list().await;
        let ranked = self.peer_mgr.ranked_peers().await;

        // No peers / only slow peers: try the external tier first, keeping
        // whatever peers exist as last resort below.
        let now_unix = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let pre_trigger = self.fallback.as_ref().and_then(|fb| {
            policy::pre_fetch_fallback_trigger(&ranked, fb.config().min_peer_bandwidth, now_unix)
        });
        let mut fallback_outcome = None;
        if let Some(trigger) = pre_trigger {
            let outcome = self.try_fallback(trigger, &local_snaps).await?;
            if outcome == FetchOutcome::Downloaded {
                return Ok(outcome);
            }
            fallback_outcome = Some(outcome);
        }

        if ranked.is_empty() {
            info!("no peers available, skipping fetch");
            return Ok(fallback_outcome.unwrap_or(FetchOutcome::NoPeers));
        }

        let sources: Vec<SourceView> = ranked.iter().map(SourceView::from_peer).collect();
        let decision = policy::decide(&local_snaps, &sources, self.config.min_slots_behind);
        let Some(reason) = decision.reason else {
            return Ok(self.no_plan_outcome(&decision));
        };

        info!(
            reason = reason.as_str(),
            local_bootstrap = decision.local_bootstrap,
            best_remote_slot = decision.best_remote_slot,
            plans = decision.plans.len(),
            "fetch cycle planned"
        );

        // While the external tier hasn't run this cycle, it is a remaining
        // candidate: the speed floor stays in force for the last peer.
        let backup = if pre_trigger.is_none() {
            self.fallback.as_ref()
        } else {
            None
        };
        match self.run_plans(&decision, reason, backup).await? {
            PlansResult::Downloaded(_) => return Ok(FetchOutcome::Downloaded),
            PlansResult::Failed { any_err } => {
                if any_err
                    && backup.is_some()
                    && self
                        .try_fallback(FallbackTrigger::PeerPlansFailed, &local_snaps)
                        .await?
                        == FetchOutcome::Downloaded
                {
                    return Ok(FetchOutcome::Downloaded);
                }
            }
        }

        warn!(
            reason = reason.as_str(),
            local_bootstrap = decision.local_bootstrap,
            best_remote_slot = decision.best_remote_slot,
            "no download plan succeeded this cycle"
        );
        Ok(FetchOutcome::Unresolved(reason))
    }

    /// Outcome of a cycle whose decision produced no plan.
    fn no_plan_outcome(&self, decision: &FetchDecision) -> FetchOutcome {
        if decision.local_bootstrap == 0 {
            warn!(
                best_remote_slot = decision.best_remote_slot,
                "no local snapshot and no source offers one"
            );
            return FetchOutcome::Unresolved(FetchReason::Bootstrap);
        }
        match decision.gate {
            FullFetchGate::UpToDate => {
                info!(
                    local_bootstrap = decision.local_bootstrap,
                    best_remote_slot = decision.best_remote_slot,
                    "local bootstrap is up to date with sources"
                );
                FetchOutcome::NotNeeded
            }
            FullFetchGate::NotFarEnoughBehind { diff } => {
                info!(
                    local_bootstrap = decision.local_bootstrap,
                    best_remote_slot = decision.best_remote_slot,
                    diff,
                    min = self.config.min_slots_behind,
                    "not far enough behind for new full snapshot"
                );
                FetchOutcome::NotNeeded
            }
            FullFetchGate::Seek { diff } => {
                warn!(
                    local_bootstrap = decision.local_bootstrap,
                    best_remote_slot = decision.best_remote_slot,
                    "behind, but no worthwhile plan from sources; waiting"
                );
                FetchOutcome::Unresolved(FetchReason::Behind { diff })
            }
        }
    }

    /// Attempt `decision.plans` in order, applying the per-cycle speed-floor
    /// rule. `backup` is a tier still to run after these plans: when it has
    /// usable sources, the floor stays in force for the last plan here.
    async fn run_plans(
        &self,
        decision: &FetchDecision,
        reason: FetchReason,
        backup: Option<&FallbackManager>,
    ) -> Result<PlansResult> {
        let mut slow_sources: HashSet<&str> = HashSet::new();
        let mut backup_usable: Option<bool> = None;
        let mut any_err = false;
        for (i, plan) in decision.plans.iter().enumerate() {
            let mut attempt = policy::plan_attempt(
                &decision.plans,
                i,
                &slow_sources,
                self.config.min_speed,
                false,
            );
            // Dropping the floor (or keeping a slow source) only makes sense
            // when nothing else is left; probe the backup tier lazily, at the
            // first moment that question comes up.
            let floor_dropped =
                self.config.min_speed > 0 && attempt == PlanAttempt::Run { min_speed: 0 };
            if floor_dropped && let Some(fb) = backup {
                if backup_usable.is_none() {
                    backup_usable = Some(!fb.discover_sources().await.is_empty());
                }
                if backup_usable == Some(true) {
                    attempt = policy::plan_attempt(
                        &decision.plans,
                        i,
                        &slow_sources,
                        self.config.min_speed,
                        true,
                    );
                }
            }
            let min_speed = match attempt {
                PlanAttempt::Skip => {
                    info!(
                        source = plan.source.label(),
                        "skipping plan: source was below min_speed this cycle"
                    );
                    continue;
                }
                PlanAttempt::Run { min_speed } => {
                    if min_speed < self.config.min_speed {
                        info!(
                            source = plan.source.label(),
                            "last candidate source: min_speed not enforced"
                        );
                    }
                    min_speed
                }
            };
            if plan.incremental_only {
                info!(
                    source = plan.source.label(),
                    base_full = plan.full.slot,
                    incremental = plan.incremental.as_ref().map(|s| s.slot),
                    "attempting incremental on local full"
                );
            } else {
                info!(
                    source = plan.source.label(),
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
                    min_speed,
                )
                .await
            {
                Ok(true) => return Ok(PlansResult::Downloaded(plan.source.clone())),
                Ok(false) => continue,
                Err(e) => {
                    any_err = true;
                    if e.downcast_ref::<SlowTransfer>().is_some() {
                        slow_sources.insert(plan.source.label());
                    }
                    warn!(source = plan.source.label(), error = %e, "download failed, trying next plan");
                }
            }
        }
        Ok(PlansResult::Failed { any_err })
    }

    /// Discover external sources and run the regular fetch policy over them.
    async fn try_fallback(
        &self,
        trigger: FallbackTrigger,
        local_snaps: &[SnapshotInfo],
    ) -> Result<FetchOutcome> {
        let Some(fallback) = &self.fallback else {
            return Ok(FetchOutcome::NoPeers);
        };

        info!(
            trigger = trigger.as_str(),
            "peer sources insufficient, trying external snapshot sources"
        );
        self.metrics
            .fallback_attempts
            .get_or_create(&crate::metrics::TriggerLabel::new(trigger.as_str()))
            .inc();

        let sources = fallback.discover_sources().await;
        if sources.is_empty() {
            info!("no usable external snapshot sources");
            return Ok(FetchOutcome::NoPeers);
        }

        let decision = policy::decide(local_snaps, &sources, self.config.min_slots_behind);
        let Some(reason) = decision.reason else {
            return Ok(self.no_plan_outcome(&decision));
        };

        match self.run_plans(&decision, reason, None).await? {
            PlansResult::Downloaded(source) => {
                self.metrics
                    .fallback_downloads
                    .get_or_create(&crate::metrics::SourceLabel::new(source.label()))
                    .inc();
                Ok(FetchOutcome::Downloaded)
            }
            PlansResult::Failed { .. } => Ok(FetchOutcome::Unresolved(reason)),
        }
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
        min_speed: u64,
    ) -> Result<bool> {
        if !self.plan_needs_download(plan).await {
            info!("snapshot plan already satisfied locally");
            return Ok(false);
        }

        let start = Instant::now();
        match self
            .download_plan(plan, best_remote_slot, local_bootstrap, min_speed)
            .await
        {
            Ok(bytes) if bytes > 0 => {
                let duration = start.elapsed();
                let bps = bytes as f64 / duration.as_secs_f64();
                if let Some(peer) = plan.source.peer_addr() {
                    self.peer_mgr.record_bandwidth(peer, bps).await;
                }
                self.metrics
                    .download_duration
                    .observe(duration.as_secs_f64());
                self.metrics.download_bytes.inc_by(bytes);
                self.metrics
                    .downloads_by_reason
                    .get_or_create(&crate::metrics::ReasonLabel::new(reason.as_str()))
                    .inc();
                info!(
                    source = plan.source.label(),
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
            Err(e) => {
                // A watchdog abort measured this peer as slow: remember that,
                // so the next cycle's slow-peer trigger can see it.
                if let Some(peer) = plan.source.peer_addr()
                    && let Some(slow) = e.downcast_ref::<SlowTransfer>()
                {
                    self.peer_mgr
                        .record_bandwidth(peer, slow.measured_bps)
                        .await;
                }
                Err(e)
            }
        }
    }

    async fn download_plan(
        &self,
        plan: &DownloadPlan,
        best_remote_slot: u64,
        local_bootstrap: u64,
        min_speed: u64,
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

        let fallback = self.fallback.as_ref();
        let (full_dl, inc_dl) = match (need_full, need_inc) {
            (true, true) => {
                let full_fut = download_one_file(
                    &self.config,
                    fallback,
                    &plan.source,
                    &plan.full,
                    &tmp_dir,
                    min_speed,
                );
                let inc_fut = download_one_file(
                    &self.config,
                    fallback,
                    &plan.source,
                    plan.incremental.as_ref().unwrap(),
                    &tmp_dir,
                    min_speed,
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
                let full_dl = download_one_file(
                    &self.config,
                    fallback,
                    &plan.source,
                    &plan.full,
                    &tmp_dir,
                    min_speed,
                )
                .await?;
                (Some(full_dl), None)
            }
            (false, true) => {
                let inc_dl = match download_one_file(
                    &self.config,
                    fallback,
                    &plan.source,
                    plan.incremental.as_ref().unwrap(),
                    &tmp_dir,
                    min_speed,
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
                    plan.source.peer_addr(),
                    min_speed,
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
                // the same 100 GB again. Record the computed hash as a staging
                // sidecar so the reuse also works for external sources, which
                // report no hash of their own.
                if let Some(ref dl) = full_dl {
                    crate::snapshot::save_sidecar_hash(&tmp_dir.join(&dl.file_name), &dl.sha256);
                }
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
            let sidecar = snapshot::sidecar_path(&tmp_dir.join(&dl.file_name));
            tokio::fs::remove_file(sidecar).await.ok();
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
    /// original peer first, then others. External sources can't be re-polled,
    /// so for an external plan (`original_peer` = None) only cm peers are tried.
    async fn try_fetch_incremental(
        &self,
        full_slot: u64,
        min_slot: u64,
        tmp_dir: &Path,
        original_peer: Option<&str>,
        min_speed: u64,
    ) -> Option<DownloadedFile> {
        let all_peers = self.peer_mgr.list_peers().await;
        let mut addrs: Vec<String> = Vec::with_capacity(all_peers.len() + 1);
        addrs.extend(original_peer.map(String::from));
        for p in &all_peers {
            if Some(p.addr.as_str()) != original_peer {
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

            match download_one_file(
                &self.config,
                None,
                &SnapshotSource::peer(addr.clone()),
                &inc,
                tmp_dir,
                min_speed,
            )
            .await
            {
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
