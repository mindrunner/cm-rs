//! External snapshot-source fallback: when no (or only slow) cm peers can
//! serve, discover mirrors and gossip-advertised RPC nodes that follow the
//! agave snapshot convention, rank them by measured speed, and hand them to
//! the regular fetch policy as [`SourceView`]s.

pub mod probe;
pub mod rpc;

use std::sync::Arc;
use std::time::{Duration, Instant};

use futures_util::StreamExt;
use tokio::sync::Mutex;
use tracing::{debug, info, warn};

use crate::config::FallbackConfig;
use crate::download::{SnapshotSource, SourceView};

use self::probe::ProbedSource;

/// The last probe round's result and when it ran.
type ProbeCache = Arc<Mutex<Option<(Instant, Vec<SourceView>)>>>;

/// Discovers, probes, and speed-ranks external snapshot sources. Probe rounds
/// are expensive for third parties (public mirrors, public RPCs), so results
/// are cached and reused within `cooldown`.
#[derive(Clone)]
pub struct FallbackManager {
    config: FallbackConfig,
    /// No-redirect client for probes and RPC calls; global `probe_timeout`.
    probe_client: reqwest::Client,
    /// Untimed client for speed tests (bounded manually by `speed_test_timeout`).
    speed_client: reqwest::Client,
    cache: ProbeCache,
}

impl FallbackManager {
    pub fn new(config: FallbackConfig) -> Self {
        let probe_client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(config.probe_timeout)
            .build()
            .expect("build probe http client");
        let speed_client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .expect("build speed-test http client");
        Self {
            config,
            probe_client,
            speed_client,
            cache: Arc::new(Mutex::new(None)),
        }
    }

    pub fn config(&self) -> &FallbackConfig {
        &self.config
    }

    /// Shared no-redirect client (honors `probe_timeout`) for alias resolution.
    pub fn probe_client(&self) -> &reqwest::Client {
        &self.probe_client
    }

    /// Speed-ranked external sources, best first. Reuses the previous round's
    /// result within `cooldown`; otherwise probes mirrors and RPC-discovered
    /// nodes, filters by slot lag, and speed-tests the lowest-latency candidates.
    pub async fn discover_sources(&self) -> Vec<SourceView> {
        // An empty round is usually a transient failure (DNS blip, dead RPC):
        // retry it much sooner than a successful round's cooldown, so a
        // bootstrap isn't stalled for the whole window.
        const EMPTY_ROUND_TTL: Duration = Duration::from_secs(60);

        {
            let cache = self.cache.lock().await;
            if let Some((at, sources)) = cache.as_ref() {
                let ttl = if sources.is_empty() {
                    EMPTY_ROUND_TTL.min(self.config.cooldown)
                } else {
                    self.config.cooldown
                };
                if at.elapsed() < ttl {
                    debug!(
                        age_secs = at.elapsed().as_secs(),
                        sources = sources.len(),
                        "reusing cached external sources within cooldown"
                    );
                    return sources.clone();
                }
            }
            // Lock released here: a probe round takes seconds to minutes and
            // must not block other users of the manager.
        }

        let sources = self.probe_round().await;
        *self.cache.lock().await = Some((Instant::now(), sources.clone()));
        sources
    }

    async fn probe_round(&self) -> Vec<SourceView> {
        let mut candidates: Vec<String> = self
            .config
            .mirrors
            .iter()
            .map(|m| m.trim_end_matches('/').to_string())
            .collect();
        // Bound the probe fan-out: getClusterNodes on mainnet can return
        // thousands of RPC-advertising nodes. Probe only an oversample of
        // what we'll speed-test; configured mirrors are always included.
        let mut rpc_nodes = rpc::cluster_rpc_nodes(&self.probe_client, &self.config.rpc_urls).await;
        rpc_nodes.truncate(self.config.max_candidates.max(1) * 10);
        candidates.extend(rpc_nodes);
        candidates.sort();
        candidates.dedup();

        if candidates.is_empty() {
            warn!("fallback enabled but no external candidates configured or discovered");
            return Vec::new();
        }
        info!(
            candidates = candidates.len(),
            "probing external snapshot sources"
        );

        let mut probed: Vec<ProbedSource> = futures_util::stream::iter(candidates)
            .map(|base| {
                let client = self.probe_client.clone();
                async move { probe::probe_source(&client, &base).await }
            })
            .buffer_unordered(self.config.probe_concurrency.max(1))
            .filter_map(|p| async { p })
            .collect()
            .await;

        // Slot-lag gate against the cluster tip; without an RPC answer, the
        // freshest probed source stands in for the tip.
        let tip = rpc::current_slot(&self.probe_client, &self.config.rpc_urls)
            .await
            .or_else(|| probed.iter().map(|p| p.resulting_slot()).max());
        if let Some(tip) = tip {
            probed.retain(|p| {
                let lag = tip.saturating_sub(p.resulting_slot());
                if lag > self.config.max_slot_lag {
                    debug!(
                        source = p.base_url,
                        lag, "dropping external source: too far behind tip"
                    );
                    return false;
                }
                true
            });
        }

        // Speed-test only the lowest-latency candidates.
        probed.sort_by(|a, b| a.latency_ms.total_cmp(&b.latency_ms));
        probed.truncate(self.config.max_candidates.max(1));

        let mut measured: Vec<(f64, ProbedSource)> = Vec::with_capacity(probed.len());
        for p in probed {
            // Speed-test the resolved redirect target: sources like avorio
            // only serve files through it (presigned URLs), never at
            // `{base}/{file}`.
            let speed = probe::speed_test(
                &self.speed_client,
                &p.full_url,
                self.config.speed_test_bytes,
                self.config.speed_test_timeout,
            )
            .await;
            match speed {
                Some(bps) if bps >= self.config.min_source_speed as f64 => {
                    info!(
                        source = p.base_url,
                        slot = p.resulting_slot(),
                        mbps = bps / 1_000_000.0,
                        latency_ms = p.latency_ms,
                        "external source measured"
                    );
                    measured.push((bps, p));
                }
                Some(bps) => {
                    info!(
                        source = p.base_url,
                        mbps = bps / 1_000_000.0,
                        min_mbps = self.config.min_source_speed as f64 / 1_000_000.0,
                        "dropping external source: below minimum speed"
                    );
                }
                None => {
                    warn!(source = p.base_url, "external source failed speed test");
                }
            }
        }

        measured.sort_by(|a, b| b.0.total_cmp(&a.0));
        measured
            .into_iter()
            .map(|(_, p)| {
                let mut snapshots = vec![p.full];
                snapshots.extend(p.incremental);
                SourceView {
                    source: SnapshotSource::external(p.base_url),
                    snapshots,
                }
            })
            .collect()
    }
}
