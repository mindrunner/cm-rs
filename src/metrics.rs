use std::sync::Arc;

use parking_lot::RwLock;
use prometheus_client::encoding::EncodeLabelSet;
use prometheus_client::encoding::text::encode;
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::gauge::Gauge;
use prometheus_client::metrics::histogram::{Histogram, exponential_buckets};
use prometheus_client::registry::Registry;

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct PeerLabel {
    pub peer: String,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct TypeLabel {
    pub r#type: String,
}

impl TypeLabel {
    pub fn new(t: &str) -> Self {
        Self {
            r#type: t.to_string(),
        }
    }
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct ReasonLabel {
    pub reason: String,
}

impl ReasonLabel {
    pub fn new(reason: &str) -> Self {
        Self {
            reason: reason.to_string(),
        }
    }
}

#[derive(Clone)]
pub struct Metrics {
    pub registry: Arc<RwLock<Registry>>,

    pub local_snapshot_slot: Family<TypeLabel, Gauge>,
    pub local_snapshot_count: Family<TypeLabel, Gauge>,
    pub peer_latency: Family<PeerLabel, Histogram>,
    pub peer_bandwidth: Family<PeerLabel, Histogram>,
    pub peer_errors: Family<PeerLabel, Counter>,
    pub peers_total: Gauge,
    pub download_duration: Histogram,
    pub download_bytes: Counter,
    pub downloads_by_reason: Family<ReasonLabel, Counter>,
    pub s3_upload_duration: Histogram,
}

impl Metrics {
    pub fn new(cluster_name: Option<&str>) -> Self {
        let mut registry = Registry::default();

        let reg = match cluster_name {
            Some(name) => registry.sub_registry_with_label((
                std::borrow::Cow::Borrowed("cluster"),
                std::borrow::Cow::Owned(name.to_string()),
            )),
            None => &mut registry,
        };

        let local_snapshot_slot = Family::<TypeLabel, Gauge>::default();
        reg.register(
            "cm_local_snapshot_slot",
            "Highest local snapshot slot",
            local_snapshot_slot.clone(),
        );

        let local_snapshot_count = Family::<TypeLabel, Gauge>::default();
        reg.register(
            "cm_snapshots_local_total",
            "Number of local snapshots",
            local_snapshot_count.clone(),
        );

        let peer_latency = Family::<PeerLabel, Histogram>::new_with_constructor(|| {
            Histogram::new(exponential_buckets(0.001, 2.0, 15))
        });
        reg.register(
            "cm_peer_latency_seconds",
            "Peer poll RTT",
            peer_latency.clone(),
        );

        let peer_bandwidth = Family::<PeerLabel, Histogram>::new_with_constructor(|| {
            Histogram::new(exponential_buckets(1_000_000.0, 2.0, 20))
        });
        reg.register(
            "cm_peer_download_bytes_per_second",
            "Peer download bandwidth",
            peer_bandwidth.clone(),
        );

        let peer_errors = Family::<PeerLabel, Counter>::default();
        reg.register(
            "cm_peer_errors_total",
            "Peer error count",
            peer_errors.clone(),
        );

        let peers_total = Gauge::default();
        reg.register(
            "cm_peers_total",
            "Number of known peers",
            peers_total.clone(),
        );

        let download_duration = Histogram::new(exponential_buckets(1.0, 2.0, 15));
        reg.register(
            "cm_download_duration_seconds",
            "Last fetch duration",
            download_duration.clone(),
        );

        let download_bytes = Counter::default();
        reg.register(
            "cm_download_bytes_total",
            "Total bytes downloaded",
            download_bytes.clone(),
        );

        let downloads_by_reason = Family::<ReasonLabel, Counter>::default();
        reg.register(
            "cm_downloads",
            "Completed downloads by reason (bootstrap, behind, topup)",
            downloads_by_reason.clone(),
        );

        let s3_upload_duration = Histogram::new(exponential_buckets(1.0, 2.0, 15));
        reg.register(
            "cm_s3_upload_duration_seconds",
            "S3 upload duration",
            s3_upload_duration.clone(),
        );

        Self {
            registry: Arc::new(RwLock::new(registry)),
            local_snapshot_slot,
            local_snapshot_count,
            peer_latency,
            peer_bandwidth,
            peer_errors,
            peers_total,
            download_duration,
            download_bytes,
            downloads_by_reason,
            s3_upload_duration,
        }
    }

    pub fn encode(&self) -> String {
        let mut buf = String::new();
        let reg = self.registry.read();
        encode(&mut buf, &reg).unwrap_or_default();
        buf
    }
}

pub fn peer_label(peer: &str) -> PeerLabel {
    PeerLabel {
        peer: peer.to_string(),
    }
}
