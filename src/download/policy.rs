//! Pure fetch policy: local snapshots + peer snapshots + config in, ordered
//! download plans out. No I/O — every branch is testable without disk or HTTP.

use std::collections::HashSet;

use crate::peer::PeerInfo;
use crate::snapshot::{SnapshotInfo, SnapshotType};

/// Where a snapshot can be downloaded from: a cm peer (served through the cm
/// API) or an external source following the agave snapshot URL convention.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SnapshotSource {
    Peer { addr: String },
    External { base_url: String },
}

impl SnapshotSource {
    pub fn peer(addr: impl Into<String>) -> Self {
        Self::Peer { addr: addr.into() }
    }

    pub fn external(base_url: impl Into<String>) -> Self {
        Self::External {
            base_url: base_url.into(),
        }
    }

    /// Download URL for a snapshot file served by this source.
    pub fn url_for(&self, file_name: &str) -> String {
        match self {
            Self::Peer { addr } => format!("http://{}/api/v1/snapshot/{}", addr, file_name),
            Self::External { base_url } => {
                format!("{}/{}", base_url.trim_end_matches('/'), file_name)
            }
        }
    }

    /// The cm peer address, when this source is a peer.
    pub fn peer_addr(&self) -> Option<&str> {
        match self {
            Self::Peer { addr } => Some(addr),
            Self::External { .. } => None,
        }
    }

    /// Human-readable identity for logs and metrics.
    pub fn label(&self) -> &str {
        match self {
            Self::Peer { addr } => addr,
            Self::External { base_url } => base_url,
        }
    }
}

/// A source together with the snapshots it advertises — the unit the fetch
/// policy ranks over, regardless of whether the source is a peer or external.
#[derive(Debug, Clone)]
pub struct SourceView {
    pub source: SnapshotSource,
    pub snapshots: Vec<SnapshotInfo>,
}

impl SourceView {
    pub fn from_peer(peer: &PeerInfo) -> Self {
        Self {
            source: SnapshotSource::peer(peer.addr.clone()),
            snapshots: peer.snapshots.clone(),
        }
    }

    pub fn highest_slot(&self) -> u64 {
        crate::snapshot::highest_usable_slot(&self.snapshots)
    }
}

/// A downloadable snapshot set: the full snapshot and an optional incremental.
#[derive(Debug, Clone)]
pub struct DownloadPlan {
    pub source: SnapshotSource,
    pub full: SnapshotInfo,
    pub incremental: Option<SnapshotInfo>,
    /// Local node already has `full`; only download the incremental.
    pub incremental_only: bool,
}

/// Agave bootstrap slot: highest full, then highest incremental matching that full.
pub fn agave_bootstrap_slot(snapshots: &[SnapshotInfo]) -> u64 {
    let full_slot = snapshots
        .iter()
        .filter(|s| s.is_full())
        .map(|s| s.slot)
        .max()
        .unwrap_or(0);
    if full_slot == 0 {
        return 0;
    }
    snapshots
        .iter()
        .filter(|s| !s.is_full() && s.base_slot == Some(full_slot) && s.slot > full_slot)
        .map(|s| s.slot)
        .max()
        .unwrap_or(full_slot)
}

/// Highest-slot incremental in `snapshots` based on the full at `full_slot`,
/// strictly above `min_slot`. Pass the local bootstrap as `min_slot` to skip
/// incrementals that wouldn't improve it; pass 0 for no floor.
pub fn best_incremental_for(
    snapshots: &[SnapshotInfo],
    full_slot: u64,
    min_slot: u64,
) -> Option<&SnapshotInfo> {
    snapshots
        .iter()
        .filter(|s| {
            s.snapshot_type == SnapshotType::Incremental
                && s.base_slot == Some(full_slot)
                && s.slot > min_slot
        })
        .max_by_key(|s| s.slot)
}

/// Bootstrap slot agave reaches after admitting this plan.
pub fn plan_resulting_bootstrap(plan: &DownloadPlan) -> u64 {
    plan.incremental
        .as_ref()
        .map(|i| i.slot)
        .unwrap_or(plan.full.slot)
}

pub fn build_download_plan(source: &SourceView, _target_slot: u64) -> Option<DownloadPlan> {
    // Find the best full snapshot at or near the target
    let full = source
        .snapshots
        .iter()
        .filter(|s| s.snapshot_type == SnapshotType::Full)
        .max_by_key(|s| s.slot)?;

    // Find the best incremental for this full snapshot's base
    let incremental = best_incremental_for(&source.snapshots, full.slot, 0);

    Some(DownloadPlan {
        source: source.source.clone(),
        full: full.clone(),
        incremental: incremental.cloned(),
        incremental_only: false,
    })
}

/// Ranked incremental plans on `local_full` from all sources, ahead of `local_bootstrap`.
pub fn rank_incremental_plans(
    sources: &[SourceView],
    local_full: &SnapshotInfo,
    local_bootstrap: u64,
) -> Vec<DownloadPlan> {
    let mut plans: Vec<(u64, DownloadPlan)> = Vec::new();

    for source in sources {
        let Some(inc) = source
            .snapshots
            .iter()
            .filter(|s| {
                s.snapshot_type == SnapshotType::Incremental
                    && s.base_slot == Some(local_full.slot)
                    && s.slot > local_bootstrap
            })
            .max_by_key(|s| s.slot)
        else {
            continue;
        };

        plans.push((
            inc.slot,
            DownloadPlan {
                source: source.source.clone(),
                full: local_full.clone(),
                incremental: Some(inc.clone()),
                incremental_only: true,
            },
        ));
    }

    plans.sort_by_key(|(slot, _)| std::cmp::Reverse(*slot));
    plans.into_iter().map(|(_, plan)| plan).collect()
}

/// Ranked newer-full plans from all sources, best bootstrap slot first.
pub fn rank_newer_full_plans(
    sources: &[SourceView],
    local_full_slot: Option<u64>,
    local_bootstrap: u64,
) -> Vec<DownloadPlan> {
    let floor = local_full_slot.unwrap_or(0);
    let mut plans: Vec<(u64, DownloadPlan)> = Vec::new();

    for source in sources {
        for full in source
            .snapshots
            .iter()
            .filter(|s| s.is_full() && s.slot > floor)
        {
            let incremental = best_incremental_for(&source.snapshots, full.slot, 0).cloned();

            let resulting_bootstrap = incremental.as_ref().map(|i| i.slot).unwrap_or(full.slot);
            if resulting_bootstrap <= local_bootstrap {
                continue;
            }

            plans.push((
                resulting_bootstrap,
                DownloadPlan {
                    source: source.source.clone(),
                    full: full.clone(),
                    incremental,
                    incremental_only: false,
                },
            ));
        }
    }

    plans.sort_by_key(|(slot, _)| std::cmp::Reverse(*slot));
    plans.into_iter().map(|(_, plan)| plan).collect()
}

/// Whether peers justify fetching a newer full snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FullFetchGate {
    /// Local bootstrap is at or ahead of every peer.
    UpToDate,
    /// Behind, but by less than `min_slots_behind`.
    NotFarEnoughBehind { diff: u64 },
    /// Far enough behind to seek a newer full snapshot.
    Seek { diff: u64 },
}

pub fn full_fetch_gate(
    local_bootstrap: u64,
    best_remote_slot: u64,
    min_slots_behind: u64,
) -> FullFetchGate {
    if best_remote_slot <= local_bootstrap {
        return FullFetchGate::UpToDate;
    }
    let diff = best_remote_slot - local_bootstrap;
    if diff < min_slots_behind {
        FullFetchGate::NotFarEnoughBehind { diff }
    } else {
        FullFetchGate::Seek { diff }
    }
}

/// Why a fetch cycle wants to download anything.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FetchReason {
    /// No usable local full snapshot; gate and improvement floor are bypassed.
    Bootstrap,
    /// Local bootstrap is at least `min_slots_behind` behind the cluster tip.
    Behind { diff: u64 },
    /// Within tolerance, but a peer incremental still improves the local bootstrap.
    TopUp,
}

impl FetchReason {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Bootstrap => "bootstrap",
            Self::Behind { .. } => "behind",
            Self::TopUp => "topup",
        }
    }
}

/// The complete fetch decision for one cycle.
pub struct FetchDecision {
    /// Plans to attempt, in order. Sufficient incrementals (reaching within
    /// `min_slots_behind` of the tip) come first; the rest are ordered by the
    /// bootstrap slot they produce, cheaper plan on ties.
    pub plans: Vec<DownloadPlan>,
    pub local_bootstrap: u64,
    pub best_remote_slot: u64,
    pub gate: FullFetchGate,
    /// `None` means nothing worth downloading this cycle.
    pub reason: Option<FetchReason>,
}

/// Decide what to fetch. Pure: derives everything from the given snapshots.
///
/// Full-snapshot plans exist only on bootstrap (no local full) or when the
/// gate opens (`min_slots_behind` behind the tip), and each must individually
/// improve the local bootstrap by at least `min_slots_behind` — a failed best
/// plan never falls through to a marginal-gain full download. Incremental
/// plans on the local full stay ungated: they are cheap top-ups.
pub fn decide(
    local_snaps: &[SnapshotInfo],
    sources: &[SourceView],
    min_slots_behind: u64,
) -> FetchDecision {
    let local_bootstrap = agave_bootstrap_slot(local_snaps);
    let local_full = local_snaps
        .iter()
        .filter(|s| s.is_full())
        .max_by_key(|s| s.slot);
    let local_full_slot = local_full.map(|s| s.slot);
    let best_remote_slot = sources.iter().map(|s| s.highest_slot()).max().unwrap_or(0);

    let gate = full_fetch_gate(local_bootstrap, best_remote_slot, min_slots_behind);
    let is_bootstrap = local_full.is_none() && best_remote_slot > 0;

    let mut plans = Vec::new();

    // Incremental top-ups on the local full: ungated, always cheap.
    if let Some(local_full) = local_full {
        plans.extend(rank_incremental_plans(sources, local_full, local_bootstrap));
    }

    // Full-snapshot plans: only on bootstrap or when the gate opens, and each
    // plan must clear the improvement floor (bypassed on bootstrap).
    if is_bootstrap || matches!(gate, FullFetchGate::Seek { .. }) {
        plans.extend(
            rank_newer_full_plans(sources, local_full_slot, local_bootstrap)
                .into_iter()
                .filter(|p| {
                    is_bootstrap
                        || plan_resulting_bootstrap(p).saturating_sub(local_bootstrap)
                            >= min_slots_behind
                }),
        );
    }

    // Global order: sufficient incrementals (reach within min_slots_behind of
    // the tip) as a class, then by resulting bootstrap, cheaper plan on ties.
    // The sort is stable, so equal keys keep the incoming peer ranking.
    let sufficient = |p: &DownloadPlan| {
        p.incremental_only
            && best_remote_slot.saturating_sub(plan_resulting_bootstrap(p)) <= min_slots_behind
    };
    plans.sort_by_key(|p| {
        (
            std::cmp::Reverse(sufficient(p)),
            std::cmp::Reverse(plan_resulting_bootstrap(p)),
            std::cmp::Reverse(p.incremental_only),
        )
    });

    let reason = if plans.is_empty() {
        None
    } else if is_bootstrap {
        Some(FetchReason::Bootstrap)
    } else if let FullFetchGate::Seek { diff } = gate {
        Some(FetchReason::Behind { diff })
    } else {
        Some(FetchReason::TopUp)
    };

    FetchDecision {
        plans,
        local_bootstrap,
        best_remote_slot,
        gate,
        reason,
    }
}

/// How to attempt one plan of a cycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlanAttempt {
    /// The source tripped the speed floor earlier this cycle and another usable
    /// candidate remains.
    Skip,
    /// Attempt with this speed floor; 0 aborts only on a stall.
    Run { min_speed: u64 },
}

/// Decide how to attempt `plans[i]` given the sources that already tripped the
/// speed floor this cycle. The floor only applies while a plan from another
/// usable source remains, so the last candidate is never abandoned while it is
/// still making progress. `backup_remains` counts a later tier (external
/// sources still to try this cycle) as such a candidate.
pub fn plan_attempt(
    plans: &[DownloadPlan],
    i: usize,
    slow_sources: &HashSet<&str>,
    min_speed: u64,
    backup_remains: bool,
) -> PlanAttempt {
    let source = plans[i].source.label();
    let fallback_left = backup_remains
        || plans[i + 1..].iter().any(|p| {
            let other = p.source.label();
            other != source && !slow_sources.contains(other)
        });
    match (slow_sources.contains(source), fallback_left) {
        (true, true) => PlanAttempt::Skip,
        (_, true) => PlanAttempt::Run { min_speed },
        (_, false) => PlanAttempt::Run { min_speed: 0 },
    }
}

/// Why the external-source fallback tier is being engaged.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FallbackTrigger {
    /// No cm peers are known at all.
    NoPeers,
    /// Every peer with a measured bandwidth is below the configured minimum.
    OnlySlowPeers,
    /// Peers offered plans this cycle, but every one of them failed.
    PeerPlansFailed,
}

impl FallbackTrigger {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::NoPeers => "no_peers",
            Self::OnlySlowPeers => "only_slow_peers",
            Self::PeerPlansFailed => "peer_plans_failed",
        }
    }
}

/// How long a slow-bandwidth measurement keeps counting against a peer.
/// Without an expiry, one bad cycle would route around peers forever: a
/// successful fallback returns before any peer plan runs, so peer bandwidth
/// would never be remeasured. After this window the mark lapses, the peer
/// counts as unmeasured (= not slow), and the peer tier gets retried.
pub const SLOW_MARK_TTL_SECS: u64 = 900;

/// A peer counts as slow only while a *recent* measurement says so; an
/// unmeasured peer (or one whose slow mark has expired) deserves a real
/// download attempt first.
pub fn peer_is_slow(stats: &crate::peer::PeerStats, min_bandwidth_bps: u64, now_unix: u64) -> bool {
    let Some(bw) = stats.bandwidth_bps else {
        return false;
    };
    let Some(at) = stats.bandwidth_at else {
        return false;
    };
    now_unix.saturating_sub(at) <= SLOW_MARK_TTL_SECS && bw < min_bandwidth_bps as f64
}

/// Trigger evaluated before the peer download loop: engage the fallback tier
/// when there are no peers at all, or when every peer is measurably slow
/// (`min_peer_bandwidth` > 0). `PeerPlansFailed` is decided by the caller
/// after the peer loop, not here.
pub fn pre_fetch_fallback_trigger(
    peers: &[PeerInfo],
    min_peer_bandwidth: u64,
    now_unix: u64,
) -> Option<FallbackTrigger> {
    if peers.is_empty() {
        return Some(FallbackTrigger::NoPeers);
    }
    if min_peer_bandwidth > 0
        && peers
            .iter()
            .all(|p| peer_is_slow(&p.stats, min_peer_bandwidth, now_unix))
    {
        return Some(FallbackTrigger::OnlySlowPeers);
    }
    None
}

/// Staleness of a full-only download (no incremental obtained) relative to the
/// cluster tip. Returns `Some(staleness)` when it exceeds `min_slots_behind`
/// and the download should be rejected.
pub fn full_only_too_stale(
    full_slot: u64,
    best_remote_slot: u64,
    min_slots_behind: u64,
) -> Option<u64> {
    let staleness = best_remote_slot.saturating_sub(full_slot);
    (staleness > min_slots_behind).then_some(staleness)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::peer::PeerStats;

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

    fn make_peer(addr: &str, snapshots: Vec<SnapshotInfo>) -> SourceView {
        SourceView {
            source: SnapshotSource::peer(addr),
            snapshots,
        }
    }

    fn make_external(base_url: &str, snapshots: Vec<SnapshotInfo>) -> SourceView {
        SourceView {
            source: SnapshotSource::external(base_url),
            snapshots,
        }
    }

    /// Fixed "now" for trigger tests; fresh measurements are stamped with it.
    const NOW: u64 = 1_000_000;

    fn peer_with_stats(addr: &str, bandwidth_bps: Option<f64>) -> PeerInfo {
        PeerInfo {
            addr: addr.into(),
            snapshots: Vec::new(),
            stats: PeerStats {
                bandwidth_bps,
                bandwidth_at: bandwidth_bps.map(|_| NOW),
                ..Default::default()
            },
        }
    }

    #[test]
    fn build_plan_selects_best() {
        let peer = make_peer(
            "host:1",
            vec![
                make_snap(100, true, None),
                make_snap(200, true, None),
                make_snap(250, false, Some(200)),
                make_snap(150, false, Some(100)),
            ],
        );
        let plan = build_download_plan(&peer, 250).unwrap();
        assert_eq!(plan.full.slot, 200);
        assert_eq!(plan.incremental.as_ref().unwrap().slot, 250);
    }

    #[test]
    fn build_plan_no_matching_incremental() {
        let peer = make_peer(
            "host:1",
            vec![
                make_snap(100, true, None),
                make_snap(300, false, Some(999)), // orphan
            ],
        );
        let plan = build_download_plan(&peer, 100).unwrap();
        assert_eq!(plan.full.slot, 100);
        assert!(plan.incremental.is_none());
    }

    #[test]
    fn build_plan_no_full_snapshot() {
        let peer = make_peer("host:1", vec![make_snap(200, false, Some(100))]);
        assert!(build_download_plan(&peer, 200).is_none());
    }

    #[test]
    fn agave_bootstrap_slot_picks_matching_incremental() {
        let snaps = vec![
            make_snap(200, true, None),
            make_snap(250, false, Some(200)),
            make_snap(300, false, Some(100)), // orphan base
        ];
        assert_eq!(agave_bootstrap_slot(&snaps), 250);
    }

    #[test]
    fn agave_bootstrap_slot_ignores_orphan_incremental_on_old_full() {
        let snaps = vec![
            make_snap(200, true, None),
            make_snap(300, true, None),
            make_snap(350, false, Some(300)),
            make_snap(400, false, Some(200)), // orphan: not Agave-relevant
        ];
        assert_eq!(agave_bootstrap_slot(&snaps), 350);
    }

    #[test]
    fn rank_incremental_skips_peer_without_match() {
        let local_full = make_snap(200, true, None);
        let peers = vec![
            make_peer("host:1", vec![make_snap(613204, true, None)]),
            make_peer("host:2", vec![make_snap(280, false, Some(200))]),
        ];
        let plans = rank_incremental_plans(&peers, &local_full, 250);
        assert_eq!(plans.len(), 1);
        assert_eq!(plans[0].source.peer_addr(), Some("host:2"));
        assert_eq!(plans[0].incremental.as_ref().unwrap().slot, 280);
    }

    #[test]
    fn rank_incremental_plans_returns_all_peers() {
        let local_full = make_snap(200, true, None);
        let peers = vec![
            make_peer("host:1", vec![make_snap(280, false, Some(200))]),
            make_peer("host:2", vec![make_snap(290, false, Some(200))]),
        ];
        let plans = rank_incremental_plans(&peers, &local_full, 250);
        assert_eq!(plans.len(), 2);
        assert_eq!(plans[0].incremental.as_ref().unwrap().slot, 290);
        assert_eq!(plans[1].incremental.as_ref().unwrap().slot, 280);
    }

    #[test]
    fn rank_incremental_on_local_full() {
        let local_full = make_snap(200, true, None);
        let peers = vec![make_peer(
            "host:1",
            vec![
                make_snap(613204, true, None),
                make_snap(688421, false, Some(613204)),
                make_snap(280, false, Some(200)),
            ],
        )];
        let plans = rank_incremental_plans(&peers, &local_full, 250);
        assert!(plans[0].incremental_only);
        assert_eq!(plans[0].full.slot, 200);
        assert_eq!(plans[0].incremental.as_ref().unwrap().slot, 280);
    }

    #[test]
    fn rank_incremental_skips_older_peer_chain() {
        let local_full = make_snap(663293, true, None);
        let peers = vec![make_peer(
            "host:1",
            vec![
                make_snap(613204, true, None),
                make_snap(688421, false, Some(613204)),
            ],
        )];
        assert!(rank_incremental_plans(&peers, &local_full, 683917).is_empty());
    }

    #[test]
    fn rank_newer_full_rejects_older_full() {
        let peers = vec![make_peer(
            "host:1",
            vec![make_snap(100, true, None), make_snap(250, false, Some(100))],
        )];
        assert!(rank_newer_full_plans(&peers, Some(200), 150).is_empty());
    }

    #[test]
    fn rank_newer_full_accepts_newer_full() {
        let peers = vec![make_peer(
            "host:1",
            vec![make_snap(300, true, None), make_snap(350, false, Some(300))],
        )];
        let plans = rank_newer_full_plans(&peers, Some(200), 150);
        assert_eq!(plans.len(), 1);
        assert_eq!(plans[0].full.slot, 300);
        assert_eq!(plans[0].incremental.as_ref().unwrap().slot, 350);
        assert!(!plans[0].incremental_only);
    }

    #[test]
    fn gate_up_to_date() {
        assert_eq!(full_fetch_gate(500, 500, 100), FullFetchGate::UpToDate);
        assert_eq!(full_fetch_gate(500, 400, 100), FullFetchGate::UpToDate);
    }

    #[test]
    fn gate_not_far_enough() {
        assert_eq!(
            full_fetch_gate(500, 550, 100),
            FullFetchGate::NotFarEnoughBehind { diff: 50 }
        );
    }

    #[test]
    fn gate_seek() {
        assert_eq!(
            full_fetch_gate(500, 700, 100),
            FullFetchGate::Seek { diff: 200 }
        );
    }

    #[test]
    fn decide_full_chain_outranks_insufficient_incremental() {
        // Local has full 200; peer offers a stale incremental on it AND a much
        // newer chain. The incremental (280) is nowhere near tip (1100), so
        // the full chain is the real fix and goes first.
        let local = vec![make_snap(200, true, None)];
        let peers = vec![make_peer(
            "host:1",
            vec![
                make_snap(280, false, Some(200)),
                make_snap(1000, true, None),
                make_snap(1100, false, Some(1000)),
            ],
        )];
        let decision = decide(&local, &peers, 0);
        assert_eq!(decision.plans.len(), 2);
        assert!(!decision.plans[0].incremental_only);
        assert_eq!(decision.plans[0].full.slot, 1000);
        // Stale incremental kept as cheap fallback
        assert!(decision.plans[1].incremental_only);
        assert_eq!(decision.plans[1].incremental.as_ref().unwrap().slot, 280);
        assert!(matches!(decision.reason, Some(FetchReason::Behind { .. })));
    }

    #[test]
    fn decide_sufficient_incremental_first() {
        // Gate open (1040 behind, min 100). The incremental on the local full
        // reaches within min_slots_behind of tip → it wins over the full chain.
        let local = vec![make_snap(1000, true, None)];
        let peers = vec![make_peer(
            "host:1",
            vec![
                make_snap(1980, false, Some(1000)),
                make_snap(2000, true, None),
                make_snap(2040, false, Some(2000)),
            ],
        )];
        let decision = decide(&local, &peers, 100);
        assert!(matches!(
            decision.reason,
            Some(FetchReason::Behind { diff: 1040 })
        ));
        assert!(decision.plans[0].incremental_only);
        assert_eq!(decision.plans[0].incremental.as_ref().unwrap().slot, 1980);
        assert!(!decision.plans[1].incremental_only);
        assert_eq!(decision.plans[1].full.slot, 2000);
    }

    #[test]
    fn decide_sufficient_incremental_class_promoted() {
        // Two peers serve sufficient incrementals; both must precede the full
        // chain so a 404 on the best peer falls to the next incremental, not
        // to a 100 GB full download.
        let local = vec![make_snap(1000, true, None)];
        let peers = vec![
            make_peer("host:1", vec![make_snap(1980, false, Some(1000))]),
            make_peer(
                "host:2",
                vec![
                    make_snap(1960, false, Some(1000)),
                    make_snap(2000, true, None),
                    make_snap(2040, false, Some(2000)),
                ],
            ),
        ];
        let decision = decide(&local, &peers, 100);
        assert_eq!(decision.plans.len(), 3);
        assert!(decision.plans[0].incremental_only);
        assert_eq!(decision.plans[0].incremental.as_ref().unwrap().slot, 1980);
        assert!(decision.plans[1].incremental_only);
        assert_eq!(decision.plans[1].incremental.as_ref().unwrap().slot, 1960);
        assert!(!decision.plans[2].incremental_only);
    }

    #[test]
    fn decide_healthy_validator_tops_up_incremental_only() {
        // Healthy node: own incremental at 101_000, peer marginally ahead.
        // Gate closed → no full plans, but the cheap top-up is planned.
        let local = vec![
            make_snap(100_000, true, None),
            make_snap(101_000, false, Some(100_000)),
        ];
        let peers = vec![make_peer(
            "host:1",
            vec![
                make_snap(100_000, true, None),
                make_snap(101_100, false, Some(100_000)),
            ],
        )];
        let decision = decide(&local, &peers, 2500);
        assert!(matches!(
            decision.gate,
            FullFetchGate::NotFarEnoughBehind { diff: 100 }
        ));
        assert_eq!(decision.reason, Some(FetchReason::TopUp));
        assert_eq!(decision.plans.len(), 1);
        assert!(decision.plans[0].incremental_only);
    }

    #[test]
    fn decide_healthy_validator_nothing_newer_no_plans() {
        // Peer offers nothing beyond our bootstrap → no plans, no reason.
        let local = vec![
            make_snap(100_000, true, None),
            make_snap(101_000, false, Some(100_000)),
        ];
        let peers = vec![make_peer(
            "host:1",
            vec![
                make_snap(100_000, true, None),
                make_snap(101_000, false, Some(100_000)),
            ],
        )];
        let decision = decide(&local, &peers, 2500);
        assert!(decision.plans.is_empty());
        assert_eq!(decision.reason, None);
    }

    #[test]
    fn decide_gate_closed_yields_no_full_plans() {
        // Local bootstrap 200, peer tip 250, min_slots_behind 100 → gate closed.
        let local = vec![make_snap(200, true, None)];
        let peers = vec![make_peer("host:1", vec![make_snap(250, true, None)])];
        let decision = decide(&local, &peers, 100);
        assert_eq!(
            decision.gate,
            FullFetchGate::NotFarEnoughBehind { diff: 50 }
        );
        assert!(decision.plans.is_empty());
        assert_eq!(decision.reason, None);
    }

    #[test]
    fn decide_incremental_bypasses_gate() {
        // Gate closed, but an incremental on the local full is still planned.
        let local = vec![make_snap(200, true, None)];
        let peers = vec![make_peer(
            "host:1",
            vec![make_snap(200, true, None), make_snap(250, false, Some(200))],
        )];
        let decision = decide(&local, &peers, 1000);
        assert!(matches!(
            decision.gate,
            FullFetchGate::NotFarEnoughBehind { .. }
        ));
        assert_eq!(decision.reason, Some(FetchReason::TopUp));
        assert_eq!(decision.plans.len(), 1);
        assert!(decision.plans[0].incremental_only);
    }

    #[test]
    fn decide_empty_local_bootstraps() {
        let local = vec![];
        let peers = vec![make_peer("host:1", vec![make_snap(5000, true, None)])];
        let decision = decide(&local, &peers, 0);
        assert_eq!(decision.reason, Some(FetchReason::Bootstrap));
        assert_eq!(decision.plans.len(), 1);
        assert_eq!(decision.plans[0].full.slot, 5000);
    }

    #[test]
    fn decide_bootstrap_bypasses_gate_on_young_cluster() {
        // Cluster tip (1000) below min_slots_behind (2500): the gate never
        // opens, but an empty node must still bootstrap.
        let local = vec![];
        let peers = vec![make_peer("host:1", vec![make_snap(1000, true, None)])];
        let decision = decide(&local, &peers, 2500);
        assert!(matches!(
            decision.gate,
            FullFetchGate::NotFarEnoughBehind { diff: 1000 }
        ));
        assert_eq!(decision.reason, Some(FetchReason::Bootstrap));
        assert_eq!(decision.plans.len(), 1);
        assert_eq!(decision.plans[0].full.slot, 1000);
    }

    #[test]
    fn decide_no_peers_no_reason() {
        let decision = decide(&[], &[], 0);
        assert_eq!(decision.gate, FullFetchGate::UpToDate);
        assert!(decision.plans.is_empty());
        assert_eq!(decision.reason, None);
    }

    #[test]
    fn decide_floor_drops_marginal_full_fallbacks() {
        // Gate open via peer1's chain. Peer2's full would improve bootstrap by
        // only 90 slots (< min 100) — never worth a full download, dropped.
        let local = vec![make_snap(1000, true, None)];
        let peers = vec![
            make_peer(
                "host:1",
                vec![
                    make_snap(5000, true, None),
                    make_snap(5050, false, Some(5000)),
                ],
            ),
            make_peer("host:2", vec![make_snap(1090, true, None)]),
        ];
        let decision = decide(&local, &peers, 100);
        let fulls: Vec<_> = decision
            .plans
            .iter()
            .filter(|p| !p.incremental_only)
            .collect();
        assert_eq!(fulls.len(), 1);
        assert_eq!(fulls[0].full.slot, 5000);
    }

    #[test]
    fn decide_ties_preserve_peer_rank() {
        // Identical chains on two peers: the incoming (ranked) order is kept.
        let local = vec![];
        let peers = vec![
            make_peer("host:1", vec![make_snap(3000, true, None)]),
            make_peer("host:2", vec![make_snap(3000, true, None)]),
        ];
        let decision = decide(&local, &peers, 0);
        assert_eq!(decision.plans.len(), 2);
        assert_eq!(decision.plans[0].source.peer_addr(), Some("host:1"));
        assert_eq!(decision.plans[1].source.peer_addr(), Some("host:2"));
    }

    #[test]
    fn full_only_staleness_check() {
        assert_eq!(full_only_too_stale(900, 1000, 50), Some(100));
        assert_eq!(full_only_too_stale(990, 1000, 50), None);
        assert_eq!(full_only_too_stale(1000, 900, 50), None);
    }

    #[test]
    fn best_incremental_for_picks_highest() {
        let snaps = vec![
            make_snap(250, false, Some(200)),
            make_snap(280, false, Some(200)),
            make_snap(300, false, Some(999)),
        ];
        assert_eq!(best_incremental_for(&snaps, 200, 0).unwrap().slot, 280);
        assert!(best_incremental_for(&snaps, 123, 0).is_none());
    }

    #[test]
    fn best_incremental_for_respects_min_slot() {
        let snaps = vec![
            make_snap(250, false, Some(200)),
            make_snap(280, false, Some(200)),
        ];
        // Floor below both → highest wins; floor at 280 → nothing improves it.
        assert_eq!(best_incremental_for(&snaps, 200, 250).unwrap().slot, 280);
        assert!(best_incremental_for(&snaps, 200, 280).is_none());
    }

    #[test]
    fn plan_resulting_bootstrap_uses_incremental_when_present() {
        let with_inc = DownloadPlan {
            source: SnapshotSource::peer("host:1"),
            full: make_snap(1000, true, None),
            incremental: Some(make_snap(1500, false, Some(1000))),
            incremental_only: false,
        };
        let full_only = DownloadPlan {
            incremental: None,
            ..with_inc.clone()
        };
        assert_eq!(plan_resulting_bootstrap(&with_inc), 1500);
        assert_eq!(plan_resulting_bootstrap(&full_only), 1000);
    }

    #[test]
    fn url_for_peer_uses_cm_api_path() {
        let src = SnapshotSource::peer("host:13080");
        assert_eq!(
            src.url_for("snapshot-100-hash.tar.zst"),
            "http://host:13080/api/v1/snapshot/snapshot-100-hash.tar.zst"
        );
    }

    #[test]
    fn url_for_external_joins_base_url() {
        let src = SnapshotSource::external("https://mirror.example.com/snapshots/");
        assert_eq!(
            src.url_for("snapshot-100-hash.tar.zst"),
            "https://mirror.example.com/snapshots/snapshot-100-hash.tar.zst"
        );
        let no_slash = SnapshotSource::external("https://mirror.example.com/snapshots");
        assert_eq!(
            no_slash.url_for("f.tar.zst"),
            "https://mirror.example.com/snapshots/f.tar.zst"
        );
    }

    #[test]
    fn decide_over_external_sources() {
        // Bootstrap purely from an external mirror: same gating as peers.
        let sources = vec![make_external(
            "https://mirror.example.com/snapshots",
            vec![
                make_snap(5000, true, None),
                make_snap(5100, false, Some(5000)),
            ],
        )];
        let decision = decide(&[], &sources, 0);
        assert_eq!(decision.reason, Some(FetchReason::Bootstrap));
        assert_eq!(decision.plans.len(), 1);
        assert_eq!(decision.plans[0].source.peer_addr(), None);
        assert_eq!(decision.plans[0].full.slot, 5000);
        assert_eq!(decision.plans[0].incremental.as_ref().unwrap().slot, 5100);
    }

    #[test]
    fn decide_mixed_sources_ranked_by_slot() {
        // A fresher external chain outranks a staler peer chain and vice versa.
        let sources = vec![
            make_peer("host:1", vec![make_snap(3000, true, None)]),
            make_external("https://m.example.com", vec![make_snap(4000, true, None)]),
        ];
        let decision = decide(&[], &sources, 0);
        assert_eq!(decision.plans.len(), 2);
        assert_eq!(decision.plans[0].full.slot, 4000);
        assert_eq!(decision.plans[0].source.peer_addr(), None);
        assert_eq!(decision.plans[1].source.peer_addr(), Some("host:1"));
    }

    #[test]
    fn trigger_no_peers() {
        assert_eq!(
            pre_fetch_fallback_trigger(&[], 10_000_000, NOW),
            Some(FallbackTrigger::NoPeers)
        );
        // Threshold disabled still detects the empty peer set.
        assert_eq!(
            pre_fetch_fallback_trigger(&[], 0, NOW),
            Some(FallbackTrigger::NoPeers)
        );
    }

    #[test]
    fn trigger_all_measured_slow() {
        let peers = vec![
            peer_with_stats("a:1", Some(1_000_000.0)),
            peer_with_stats("b:1", Some(5_000_000.0)),
        ];
        assert_eq!(
            pre_fetch_fallback_trigger(&peers, 10_000_000, NOW),
            Some(FallbackTrigger::OnlySlowPeers)
        );
    }

    #[test]
    fn trigger_one_fast_peer_disarms() {
        let peers = vec![
            peer_with_stats("a:1", Some(1_000_000.0)),
            peer_with_stats("b:1", Some(50_000_000.0)),
        ];
        assert_eq!(pre_fetch_fallback_trigger(&peers, 10_000_000, NOW), None);
    }

    #[test]
    fn trigger_unmeasured_peer_counts_as_not_slow() {
        // A fresh peer without bandwidth history deserves a real attempt
        // before we route around it.
        let peers = vec![
            peer_with_stats("a:1", Some(1_000_000.0)),
            peer_with_stats("b:1", None),
        ];
        assert_eq!(pre_fetch_fallback_trigger(&peers, 10_000_000, NOW), None);
    }

    #[test]
    fn trigger_threshold_zero_disables_slow_detection() {
        let peers = vec![peer_with_stats("a:1", Some(1.0))];
        assert_eq!(pre_fetch_fallback_trigger(&peers, 0, NOW), None);
    }

    fn full_plan(peer: &str, slot: u64) -> DownloadPlan {
        DownloadPlan {
            source: SnapshotSource::peer(peer),
            full: make_snap(slot, true, None),
            incremental: None,
            incremental_only: false,
        }
    }

    #[test]
    fn plan_attempt_enforces_floor_while_another_peer_remains() {
        let plans = vec![full_plan("a:1", 300), full_plan("b:1", 200)];
        let slow = HashSet::new();
        assert_eq!(
            plan_attempt(&plans, 0, &slow, 50, false),
            PlanAttempt::Run { min_speed: 50 }
        );
        assert_eq!(
            plan_attempt(&plans, 1, &slow, 50, false),
            PlanAttempt::Run { min_speed: 0 }
        );
    }

    #[test]
    fn plan_attempt_drops_floor_when_only_slow_or_same_peer_remain() {
        // wt-2 2026-10-06: every peer below the floor, nothing ever landed.
        let plans = vec![
            full_plan("a:1", 400),
            full_plan("b:1", 300),
            full_plan("a:1", 200),
        ];
        let slow: HashSet<&str> = ["a:1"].into();
        assert_eq!(
            plan_attempt(&plans, 1, &slow, 50, false),
            PlanAttempt::Run { min_speed: 0 }
        );
        let same_peer = vec![full_plan("a:1", 400), full_plan("a:1", 300)];
        assert_eq!(
            plan_attempt(&same_peer, 0, &HashSet::new(), 50, false),
            PlanAttempt::Run { min_speed: 0 }
        );
    }

    #[test]
    fn plan_attempt_skips_slow_peer_only_while_fallback_remains() {
        let plans = vec![
            full_plan("a:1", 400),
            full_plan("a:1", 300),
            full_plan("b:1", 200),
            full_plan("a:1", 100),
        ];
        let slow: HashSet<&str> = ["a:1"].into();
        assert_eq!(plan_attempt(&plans, 1, &slow, 50, false), PlanAttempt::Skip);
        // b:1 failed for another reason; a:1 is all that is left.
        assert_eq!(
            plan_attempt(&plans, 3, &slow, 50, false),
            PlanAttempt::Run { min_speed: 0 }
        );
    }

    #[test]
    fn plan_attempt_keeps_floor_when_backup_tier_remains() {
        // External sources still to try: the last peer keeps the floor, and
        // a slow peer is skipped in favour of them.
        let plans = vec![full_plan("a:1", 300)];
        assert_eq!(
            plan_attempt(&plans, 0, &HashSet::new(), 50, true),
            PlanAttempt::Run { min_speed: 50 }
        );
        let slow: HashSet<&str> = ["a:1"].into();
        assert_eq!(plan_attempt(&plans, 0, &slow, 50, true), PlanAttempt::Skip);
    }

    #[test]
    fn trigger_slow_mark_expires() {
        // A slow measurement older than the TTL no longer counts: the peer
        // reverts to "unmeasured" and the peer tier gets retried.
        let peers = vec![peer_with_stats("a:1", Some(1_000_000.0))];
        let later = NOW + SLOW_MARK_TTL_SECS + 1;
        assert_eq!(
            pre_fetch_fallback_trigger(&peers, 10_000_000, NOW),
            Some(FallbackTrigger::OnlySlowPeers)
        );
        assert_eq!(pre_fetch_fallback_trigger(&peers, 10_000_000, later), None);
    }

    #[test]
    fn trigger_missing_timestamp_counts_as_not_slow() {
        // Hydrated stats from an older daemon may lack bandwidth_at; treat
        // them as unmeasured rather than permanently slow.
        let mut peer = peer_with_stats("a:1", Some(1_000_000.0));
        peer.stats.bandwidth_at = None;
        assert_eq!(pre_fetch_fallback_trigger(&[peer], 10_000_000, NOW), None);
    }
}
