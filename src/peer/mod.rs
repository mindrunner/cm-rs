pub mod discovery;
pub mod static_discovery;
pub mod tailscale;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

use crate::config::PeersConfig;
use crate::metrics::{self, Metrics};
use crate::snapshot::SnapshotInfo;

use self::discovery::PeerDiscoverySource;
use self::static_discovery::StaticDiscovery;
use self::tailscale::TailscaleDiscovery;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerInfo {
    pub addr: String,
    pub snapshots: Vec<SnapshotInfo>,
    #[serde(default)]
    pub stats: PeerStats,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PeerStats {
    pub last_seen: Option<u64>,
    pub latency_ms: Option<f64>,
    pub bandwidth_bps: Option<f64>,
    pub errors: u64,
    pub score: f64,
}

impl PeerInfo {
    pub fn highest_slot(&self) -> u64 {
        let full_slots: std::collections::HashSet<u64> = self
            .snapshots
            .iter()
            .filter(|s| s.is_full())
            .map(|s| s.slot)
            .collect();
        self.snapshots
            .iter()
            .filter(|s| match s.base_slot {
                Some(base) => full_slots.contains(&base),
                None => true,
            })
            .map(|s| s.slot)
            .max()
            .unwrap_or(0)
    }
}

#[derive(Clone)]
pub struct PeerManager {
    config: PeersConfig,
    dc_affinity: Vec<String>,
    peers: Arc<RwLock<HashMap<String, PeerInfo>>>,
    client: reqwest::Client,
    metrics: Metrics,
}

impl PeerManager {
    pub fn new(config: PeersConfig, dc_affinity: Vec<String>, metrics: Metrics) -> Self {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(5))
            .build()
            .expect("build http client");
        Self {
            config,
            dc_affinity,
            peers: Arc::new(RwLock::new(HashMap::new())),
            client,
            metrics,
        }
    }

    fn build_discovery_sources(&self) -> Vec<PeerDiscoverySource> {
        let mut sources = Vec::new();
        if let Some(ref static_cfg) = self.config.static_peers {
            sources.push(PeerDiscoverySource::Static(StaticDiscovery::new(
                static_cfg.hosts.clone(),
            )));
        }
        if let Some(ref ts_cfg) = self.config.tailscale {
            if ts_cfg.enabled {
                sources.push(PeerDiscoverySource::Tailscale(TailscaleDiscovery::new(
                    ts_cfg.tag.clone(),
                    ts_cfg.port,
                )));
            }
        }
        sources
    }

    async fn discover_peers(&self) -> Vec<String> {
        let sources = self.build_discovery_sources();
        let mut addrs = Vec::new();
        for source in &sources {
            match source.discover().await {
                Ok(found) => addrs.extend(found),
                Err(e) => warn!(error = %e, "peer discovery failed"),
            }
        }
        addrs.sort();
        addrs.dedup();
        addrs
    }

    pub async fn poll_peer(&self, addr: &str) -> anyhow::Result<(Vec<SnapshotInfo>, f64)> {
        let url = format!("http://{}/api/v1/snapshots", addr);
        let start = Instant::now();
        let resp = self.client.get(&url).send().await?;
        let latency = start.elapsed();
        let snapshots: Vec<SnapshotInfo> = resp.json().await?;
        let latency_ms = latency.as_secs_f64() * 1000.0;

        self.metrics
            .peer_latency
            .get_or_create(&metrics::peer_label(addr))
            .observe(latency.as_secs_f64());

        Ok((snapshots, latency_ms))
    }

    pub async fn poll_loop(&self) {
        let interval = self.config.poll_interval;
        info!(interval_secs = interval.as_secs(), "starting peer poll loop");

        loop {
            let addrs = self.discover_peers().await;
            debug!(peer_count = addrs.len(), "discovered peers");

            for addr in &addrs {
                match self.poll_peer(addr).await {
                    Ok((snapshots, latency_ms)) => {
                        let mut peers = self.peers.write().await;
                        let entry = peers.entry(addr.clone()).or_insert_with(|| PeerInfo {
                            addr: addr.clone(),
                            snapshots: Vec::new(),
                            stats: PeerStats::default(),
                        });
                        entry.snapshots = snapshots;
                        entry.stats.last_seen = Some(
                            std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .unwrap_or_default()
                                .as_secs(),
                        );
                        entry.stats.latency_ms = Some(latency_ms);
                    }
                    Err(e) => {
                        debug!(peer = addr, error = %e, "poll failed");
                        let mut peers = self.peers.write().await;
                        let entry = peers.entry(addr.clone()).or_insert_with(|| PeerInfo {
                            addr: addr.clone(),
                            snapshots: Vec::new(),
                            stats: PeerStats::default(),
                        });
                        entry.stats.errors += 1;
                        self.metrics
                            .peer_errors
                            .get_or_create(&metrics::peer_label(addr))
                            .inc();
                    }
                }
            }

            let peer_count = self.peers.read().await.len();
            self.metrics.peers_total.set(peer_count as i64);

            tokio::time::sleep(interval).await;
        }
    }

    /// Get peers ranked by composite score (lower is better).
    pub async fn ranked_peers(&self) -> Vec<PeerInfo> {
        let peers = self.peers.read().await;
        let mut list: Vec<PeerInfo> = peers.values().cloned().collect();

        let affinity = &self.dc_affinity;
        for peer in &mut list {
            peer.stats.score = compute_score(&peer.addr, &peer.stats, affinity);
        }

        list.sort_by(|a, b| a.stats.score.partial_cmp(&b.stats.score).unwrap());
        list
    }

    pub async fn peer_count(&self) -> usize {
        self.peers.read().await.len()
    }

    pub async fn list_peers(&self) -> Vec<PeerInfo> {
        self.peers.read().await.values().cloned().collect()
    }

    /// Record download bandwidth for a peer.
    pub async fn record_bandwidth(&self, addr: &str, bytes_per_sec: f64) {
        self.metrics
            .peer_bandwidth
            .get_or_create(&metrics::peer_label(addr))
            .observe(bytes_per_sec);

        let mut peers = self.peers.write().await;
        if let Some(peer) = peers.get_mut(addr) {
            peer.stats.bandwidth_bps = Some(bytes_per_sec);
        }
    }
}

pub fn compute_score(addr: &str, stats: &PeerStats, dc_affinity: &[String]) -> f64 {
    let latency_score = stats.latency_ms.unwrap_or(1000.0) / 1000.0;
    let error_score = stats.errors as f64 * 0.5;
    let bandwidth_score = match stats.bandwidth_bps {
        Some(bw) if bw > 0.0 => 1.0 / (bw / 1_000_000_000.0),
        _ => 10.0,
    };

    let dc_score = extract_dc(addr)
        .and_then(|dc| dc_affinity.iter().position(|a| a == &dc))
        .map(|pos| pos as f64 * 0.1)
        .unwrap_or(100.0);

    latency_score + error_score + bandwidth_score + dc_score
}

pub fn extract_dc(addr: &str) -> Option<String> {
    let host = addr.split(':').next()?;
    let parts: Vec<&str> = host.split('.').next()?.split('-').collect();
    if parts.len() >= 2 {
        Some(parts[parts.len() - 2].to_string())
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::snapshot::{SnapshotInfo, SnapshotType};

    #[test]
    fn extract_dc_standard() {
        assert_eq!(
            extract_dc("rpc-ams-0.tail848bb2.ts.net:13080"),
            Some("ams".into())
        );
        assert_eq!(
            extract_dc("validator-fra-1.tail.ts.net:13080"),
            Some("fra".into())
        );
    }

    #[test]
    fn extract_dc_short_hostname() {
        assert_eq!(extract_dc("ams-0:13080"), Some("ams".into()));
    }

    #[test]
    fn extract_dc_single_part() {
        assert_eq!(extract_dc("localhost:13080"), None);
    }

    #[test]
    fn extract_dc_no_port() {
        assert_eq!(extract_dc("rpc-lax-0"), Some("lax".into()));
    }

    #[test]
    fn score_prefers_lower_latency() {
        let fast = PeerStats {
            latency_ms: Some(5.0),
            ..Default::default()
        };
        let slow = PeerStats {
            latency_ms: Some(500.0),
            ..Default::default()
        };
        let affinity: Vec<String> = vec![];
        assert!(compute_score("a:1", &fast, &affinity) < compute_score("a:1", &slow, &affinity));
    }

    #[test]
    fn score_penalizes_errors() {
        let clean = PeerStats {
            latency_ms: Some(10.0),
            errors: 0,
            ..Default::default()
        };
        let errored = PeerStats {
            latency_ms: Some(10.0),
            errors: 10,
            ..Default::default()
        };
        let affinity: Vec<String> = vec![];
        assert!(
            compute_score("a:1", &clean, &affinity) < compute_score("a:1", &errored, &affinity)
        );
    }

    #[test]
    fn score_prefers_higher_bandwidth() {
        let fast = PeerStats {
            latency_ms: Some(10.0),
            bandwidth_bps: Some(10_000_000_000.0),
            ..Default::default()
        };
        let slow = PeerStats {
            latency_ms: Some(10.0),
            bandwidth_bps: Some(100_000_000.0),
            ..Default::default()
        };
        let affinity: Vec<String> = vec![];
        assert!(compute_score("a:1", &fast, &affinity) < compute_score("a:1", &slow, &affinity));
    }

    #[test]
    fn score_dc_affinity_ordering() {
        let stats = PeerStats {
            latency_ms: Some(10.0),
            ..Default::default()
        };
        let affinity = vec!["ams".to_string(), "fra".to_string(), "lax".to_string()];
        let ams_score = compute_score("rpc-ams-0.ts:13080", &stats, &affinity);
        let fra_score = compute_score("rpc-fra-0.ts:13080", &stats, &affinity);
        let lax_score = compute_score("rpc-lax-0.ts:13080", &stats, &affinity);
        let unknown_score = compute_score("rpc-tyo-0.ts:13080", &stats, &affinity);
        assert!(ams_score < fra_score);
        assert!(fra_score < lax_score);
        assert!(lax_score < unknown_score);
    }

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
    fn peer_highest_slot_with_valid_incremental() {
        let peer = PeerInfo {
            addr: "test:1".into(),
            snapshots: vec![
                make_snap(100, true, None),
                make_snap(200, false, Some(100)),
            ],
            stats: PeerStats::default(),
        };
        assert_eq!(peer.highest_slot(), 200);
    }

    #[test]
    fn peer_highest_slot_ignores_orphaned_incremental() {
        let peer = PeerInfo {
            addr: "test:1".into(),
            snapshots: vec![
                make_snap(100, true, None),
                make_snap(500, false, Some(999)), // orphan
            ],
            stats: PeerStats::default(),
        };
        assert_eq!(peer.highest_slot(), 100);
    }

    #[test]
    fn peer_highest_slot_empty() {
        let peer = PeerInfo {
            addr: "test:1".into(),
            snapshots: vec![],
            stats: PeerStats::default(),
        };
        assert_eq!(peer.highest_slot(), 0);
    }

    #[test]
    fn peer_highest_slot_only_full() {
        let peer = PeerInfo {
            addr: "test:1".into(),
            snapshots: vec![make_snap(300, true, None), make_snap(100, true, None)],
            stats: PeerStats::default(),
        };
        assert_eq!(peer.highest_slot(), 300);
    }
}
