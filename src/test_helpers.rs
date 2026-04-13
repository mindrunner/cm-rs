use std::path::Path;

use tokio::task::JoinHandle;

use crate::config::{
    AutoConfig, Config, DownloadConfig, NodeConfig, PeersConfig, StaticPeersConfig,
};
use crate::index::SnapshotIndex;
use crate::metrics::Metrics;
use crate::peer::PeerManager;
use crate::server::{self, AppState};

/// Create a fake snapshot file (empty but valid filename).
pub fn create_fake_snapshot(dir: &Path, name: &str, size: usize) {
    let path = dir.join(name);
    let data = vec![0u8; size];
    std::fs::write(path, data).unwrap();
}

/// Start an API server on a random port. Returns (addr, join_handle).
pub async fn start_test_server(snap_dir: &Path) -> (String, JoinHandle<()>) {
    let index = SnapshotIndex::new(snap_dir.to_path_buf());
    index.scan().await.unwrap();
    start_test_server_with_index(index).await
}

/// Start an API server with a pre-built index.
pub async fn start_test_server_with_index(index: SnapshotIndex) -> (String, JoinHandle<()>) {
    let metrics = Metrics::new(None);
    let peer_mgr = PeerManager::new(PeersConfig::default(), vec![], metrics.clone());
    let state = AppState {
        index,
        peer_mgr,
        metrics,
    };

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();

    let router = server::api_router(state);
    let handle = tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });

    (addr, handle)
}

/// Build a minimal Config for testing.
pub fn test_config(snap_dir: &Path, peer_addrs: Vec<String>) -> Config {
    Config {
        node: NodeConfig {
            snapshot_dir: snap_dir.to_path_buf(),
            listen_addr: "127.0.0.1:0".into(),
            metrics_addr: "127.0.0.1:0".into(),
            cluster_name: None,
        },
        peers: PeersConfig {
            poll_interval: std::time::Duration::from_secs(1),
            static_peers: if peer_addrs.is_empty() {
                None
            } else {
                Some(StaticPeersConfig { hosts: peer_addrs })
            },
            tailscale: None,
        },
        download: DownloadConfig {
            min_slots_behind: 0,
            ..Default::default()
        },
        s3: Default::default(),
        auto: AutoConfig {
            enabled: false,
            check_interval: std::time::Duration::from_secs(30),
        },
    }
}
