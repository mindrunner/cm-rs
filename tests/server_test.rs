use cm::index::SnapshotIndex;
use cm::metrics::Metrics;
use cm::peer::PeerManager;
use cm::server::{self, AppState, CompatSnapshotSource, StatusResponse};
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
async fn list_snapshots_returns_json() {
    let dir = tempfile::tempdir().unwrap();
    touch(dir.path(), "snapshot-100-hash.tar.zst", &[0; 64]);
    touch(dir.path(), "snapshot-200-hash.tar.zst", &[0; 128]);
    touch(
        dir.path(),
        "incremental-snapshot-200-300-hash.tar.zst",
        &[0; 32],
    );

    let addr = start_server(dir.path()).await;
    let client = reqwest::Client::new();
    let resp = client
        .get(format!("http://{}/api/v1/snapshots", addr))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200);
    let snaps: Vec<SnapshotInfo> = resp.json().await.unwrap();
    assert_eq!(snaps.len(), 3);
    // Sorted: full desc, then incremental desc
    assert!(snaps[0].is_full());
    assert_eq!(snaps[0].slot, 200);
    assert!(snaps[1].is_full());
    assert_eq!(snaps[1].slot, 100);
    assert!(!snaps[2].is_full());
    assert_eq!(snaps[2].slot, 300);
}

#[tokio::test]
async fn serve_snapshot_returns_file_content() {
    let dir = tempfile::tempdir().unwrap();
    let data = b"hello snapshot world";
    touch(dir.path(), "snapshot-42-hash.tar.zst", data);

    let addr = start_server(dir.path()).await;
    let client = reqwest::Client::new();
    let resp = client
        .get(format!(
            "http://{}/api/v1/snapshot/snapshot-42-hash.tar.zst",
            addr
        ))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200);
    assert_eq!(
        resp.headers()
            .get("accept-ranges")
            .unwrap()
            .to_str()
            .unwrap(),
        "bytes"
    );
    let body = resp.bytes().await.unwrap();
    assert_eq!(&body[..], data);
}

#[tokio::test]
async fn serve_snapshot_range_request() {
    let dir = tempfile::tempdir().unwrap();
    let data: Vec<u8> = (0..100u8).collect();
    touch(dir.path(), "snapshot-99-hash.tar.zst", &data);

    let addr = start_server(dir.path()).await;
    let client = reqwest::Client::new();
    let resp = client
        .get(format!(
            "http://{}/api/v1/snapshot/snapshot-99-hash.tar.zst",
            addr
        ))
        .header("Range", "bytes=10-19")
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 206);
    let content_range = resp
        .headers()
        .get("content-range")
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    assert_eq!(content_range, "bytes 10-19/100");
    let body = resp.bytes().await.unwrap();
    assert_eq!(body.len(), 10);
    assert_eq!(&body[..], &data[10..20]);
}

#[tokio::test]
async fn serve_snapshot_path_traversal_rejected() {
    let dir = tempfile::tempdir().unwrap();
    touch(dir.path(), "snapshot-1-h.tar.zst", &[0; 1]);

    let addr = start_server(dir.path()).await;
    let client = reqwest::Client::new();

    // Path traversal with ".." - note: axum may normalize this, so test the raw path
    let resp = client
        .get(format!(
            "http://{}/api/v1/snapshot/..%2F..%2Fetc%2Fpasswd",
            addr
        ))
        .send()
        .await
        .unwrap();
    // Should be 400 or 404, not 200
    assert!(resp.status().is_client_error());
}

#[tokio::test]
async fn serve_snapshot_not_found() {
    let dir = tempfile::tempdir().unwrap();
    touch(dir.path(), "snapshot-1-h.tar.zst", &[0; 1]);

    let addr = start_server(dir.path()).await;
    let client = reqwest::Client::new();
    let resp = client
        .get(format!(
            "http://{}/api/v1/snapshot/nonexistent.tar.zst",
            addr
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 404);
}

#[tokio::test]
async fn status_endpoint() {
    let dir = tempfile::tempdir().unwrap();
    touch(dir.path(), "snapshot-500-hash.tar.zst", &[0; 64]);
    touch(
        dir.path(),
        "incremental-snapshot-500-600-hash.tar.zst",
        &[0; 32],
    );

    let addr = start_server(dir.path()).await;
    let client = reqwest::Client::new();
    let resp: StatusResponse = client
        .get(format!("http://{}/api/v1/status", addr))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();

    assert_eq!(resp.highest_slot, 600);
    assert_eq!(resp.local_snapshots.len(), 2);
    assert_eq!(resp.peer_count, 0);
}

#[tokio::test]
async fn peers_endpoint_empty() {
    let dir = tempfile::tempdir().unwrap();

    let addr = start_server(dir.path()).await;
    let client = reqwest::Client::new();
    let resp: Vec<cm::peer::PeerInfo> = client
        .get(format!("http://{}/api/v1/peers", addr))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();

    assert!(resp.is_empty());
}

#[tokio::test]
async fn best_snapshots_compat_endpoint() {
    let dir = tempfile::tempdir().unwrap();
    touch(dir.path(), "snapshot-500-abcdef.tar.zst", &[0; 1024]);
    touch(
        dir.path(),
        "incremental-snapshot-500-600-ghijkl.tar.zst",
        &[0; 512],
    );
    touch(dir.path(), "snapshot-300-mnopqr.tar.zst", &[0; 2048]);

    let addr = start_server(dir.path()).await;
    let client = reqwest::Client::new();

    // Default (no ?max)
    let resp: Vec<CompatSnapshotSource> = client
        .get(format!("http://{}/v1/best_snapshots", addr))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();

    assert_eq!(resp.len(), 2);
    // Best full first (slot 500)
    assert_eq!(resp[0].slot, 500);
    assert_eq!(resp[0].hash, "abcdef");
    assert_eq!(resp[0].files.len(), 2); // full + matching incremental
    assert_eq!(resp[0].files[0].file_name, "snapshot-500-abcdef.tar.zst");
    assert_eq!(resp[0].files[1].slot, 600);
    assert_eq!(resp[0].files[1].base_slot, Some(500));

    assert_eq!(resp[1].slot, 300);
    assert_eq!(resp[1].files.len(), 1); // full only, no incremental

    // With ?max=1
    let resp: Vec<CompatSnapshotSource> = client
        .get(format!("http://{}/v1/best_snapshots?max=1", addr))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();

    assert_eq!(resp.len(), 1);
    assert_eq!(resp[0].slot, 500);
}
