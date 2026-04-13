use cm::config::DownloadConfig;
use cm::download::DownloadOrchestrator;
use cm::index::SnapshotIndex;
use cm::metrics::Metrics;
use cm::peer::PeerManager;
use cm::config::{PeersConfig, StaticPeersConfig};
use cm::server::{self, AppState};
use cm::snapshot::SnapshotInfo;

fn touch(dir: &std::path::Path, name: &str, data: &[u8]) {
    std::fs::write(dir.join(name), data).unwrap();
}

async fn start_server(snap_dir: &std::path::Path) -> String {
    let index = SnapshotIndex::new(snap_dir.to_path_buf());
    index.scan().await.unwrap();
    let metrics = Metrics::new(None);
    let peer_mgr = PeerManager::new(Default::default(), vec![], metrics.clone());
    let state = AppState {
        index,
        peer_mgr,
        metrics,
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let router = server::api_router(state);
    tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    addr
}

#[tokio::test]
async fn full_download_cycle() {
    // Peer node with a snapshot
    let peer_dir = tempfile::tempdir().unwrap();
    let snap_data = vec![42u8; 1024];
    touch(
        peer_dir.path(),
        "snapshot-5000-abcdef.tar.zst",
        &snap_data,
    );
    let peer_addr = start_server(peer_dir.path()).await;

    // Local node with empty snapshot dir
    let local_dir = tempfile::tempdir().unwrap();

    let metrics = Metrics::new(None);
    let config = PeersConfig {
        poll_interval: std::time::Duration::from_secs(1),
        static_peers: Some(StaticPeersConfig {
            hosts: vec![peer_addr.clone()],
        }),
        tailscale: None,
    };
    let peer_mgr = PeerManager::new(config, vec![], metrics.clone());

    // Poll once to discover the peer
    let mgr = peer_mgr.clone();
    let poll = tokio::spawn(async move { mgr.poll_loop().await });
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    poll.abort();

    assert_eq!(peer_mgr.peer_count().await, 1);

    let local_index = SnapshotIndex::new(local_dir.path().to_path_buf());
    local_index.scan().await.unwrap();

    let download_config = DownloadConfig {
        min_slots_behind: 0,
        ..Default::default()
    };

    let orchestrator = DownloadOrchestrator::new(
        download_config,
        local_index.clone(),
        peer_mgr,
        metrics,
    );

    let downloaded = orchestrator.fetch_if_needed().await.unwrap();
    assert!(downloaded);

    // Verify file landed in local dir
    let local_snap = local_dir.path().join("snapshot-5000-abcdef.tar.zst");
    assert!(local_snap.exists());
    let content = std::fs::read(&local_snap).unwrap();
    assert_eq!(content, snap_data);

    // Verify index was updated
    assert_eq!(local_index.highest_slot().await, 5000);
}

#[tokio::test]
async fn download_with_incremental() {
    let peer_dir = tempfile::tempdir().unwrap();
    touch(
        peer_dir.path(),
        "snapshot-1000-full.tar.zst",
        &[1u8; 512],
    );
    touch(
        peer_dir.path(),
        "incremental-snapshot-1000-2000-inc.tar.zst",
        &[2u8; 256],
    );
    let peer_addr = start_server(peer_dir.path()).await;

    let local_dir = tempfile::tempdir().unwrap();

    let metrics = Metrics::new(None);
    let config = PeersConfig {
        poll_interval: std::time::Duration::from_secs(1),
        static_peers: Some(StaticPeersConfig {
            hosts: vec![peer_addr.clone()],
        }),
        tailscale: None,
    };
    let peer_mgr = PeerManager::new(config, vec![], metrics.clone());

    let mgr = peer_mgr.clone();
    let poll = tokio::spawn(async move { mgr.poll_loop().await });
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    poll.abort();

    let local_index = SnapshotIndex::new(local_dir.path().to_path_buf());
    local_index.scan().await.unwrap();

    let download_config = DownloadConfig {
        min_slots_behind: 0,
        ..Default::default()
    };

    let orchestrator = DownloadOrchestrator::new(
        download_config,
        local_index.clone(),
        peer_mgr,
        metrics,
    );

    let downloaded = orchestrator.fetch_if_needed().await.unwrap();
    assert!(downloaded);

    // Both full and incremental should be present
    assert!(local_dir
        .path()
        .join("snapshot-1000-full.tar.zst")
        .exists());
    assert!(local_dir
        .path()
        .join("incremental-snapshot-1000-2000-inc.tar.zst")
        .exists());
    assert_eq!(local_index.highest_slot().await, 2000);
}

#[tokio::test]
async fn no_download_when_up_to_date() {
    let peer_dir = tempfile::tempdir().unwrap();
    touch(
        peer_dir.path(),
        "snapshot-100-hash.tar.zst",
        &[0u8; 64],
    );
    let peer_addr = start_server(peer_dir.path()).await;

    // Local already has the same slot
    let local_dir = tempfile::tempdir().unwrap();
    touch(
        local_dir.path(),
        "snapshot-100-hash.tar.zst",
        &[0u8; 64],
    );

    let metrics = Metrics::new(None);
    let config = PeersConfig {
        poll_interval: std::time::Duration::from_secs(1),
        static_peers: Some(StaticPeersConfig {
            hosts: vec![peer_addr.clone()],
        }),
        tailscale: None,
    };
    let peer_mgr = PeerManager::new(config, vec![], metrics.clone());

    let mgr = peer_mgr.clone();
    let poll = tokio::spawn(async move { mgr.poll_loop().await });
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    poll.abort();

    let local_index = SnapshotIndex::new(local_dir.path().to_path_buf());
    local_index.scan().await.unwrap();

    let download_config = DownloadConfig {
        min_slots_behind: 0,
        ..Default::default()
    };

    let orchestrator = DownloadOrchestrator::new(
        download_config,
        local_index,
        peer_mgr,
        metrics,
    );

    let downloaded = orchestrator.fetch_if_needed().await.unwrap();
    assert!(!downloaded);
}

#[tokio::test]
async fn download_verifies_checksum() {
    let peer_dir = tempfile::tempdir().unwrap();
    let snap_data = vec![7u8; 2048];
    touch(
        peer_dir.path(),
        "snapshot-8000-chk.tar.zst",
        &snap_data,
    );
    let peer_addr = start_server(peer_dir.path()).await;

    // Verify the peer's API returns sha256
    let client = reqwest::Client::new();
    let snaps: Vec<SnapshotInfo> = client
        .get(format!("http://{}/api/v1/snapshots", peer_addr))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(snaps.len(), 1);
    assert!(snaps[0].sha256.is_some(), "peer should report sha256");

    // Download and verify
    let local_dir = tempfile::tempdir().unwrap();
    let metrics = Metrics::new(None);
    let config = PeersConfig {
        poll_interval: std::time::Duration::from_secs(1),
        static_peers: Some(StaticPeersConfig {
            hosts: vec![peer_addr.clone()],
        }),
        tailscale: None,
    };
    let peer_mgr = PeerManager::new(config, vec![], metrics.clone());

    let mgr = peer_mgr.clone();
    let poll = tokio::spawn(async move { mgr.poll_loop().await });
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    poll.abort();

    let local_index = SnapshotIndex::new(local_dir.path().to_path_buf());
    local_index.scan().await.unwrap();

    let download_config = DownloadConfig {
        min_slots_behind: 0,
        ..Default::default()
    };

    let orchestrator = DownloadOrchestrator::new(
        download_config,
        local_index.clone(),
        peer_mgr,
        metrics,
    );

    let downloaded = orchestrator.fetch_if_needed().await.unwrap();
    assert!(downloaded);

    // After download, local index should also have the same hash
    let local_snap = local_index.get("snapshot-8000-chk.tar.zst").await.unwrap();
    assert_eq!(local_snap.sha256, snaps[0].sha256);
}
