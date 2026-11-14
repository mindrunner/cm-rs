//! Integration tests for the external snapshot-source fallback tier, against
//! a local server mimicking the agave RPC snapshot convention:
//! `/snapshot.tar.bz2` 303-redirects to the concrete snapshot filename, which
//! is then served at `/{file_name}`.

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use axum::Router;
use axum::extract::{Path, State};
use axum::http::{StatusCode, header};
use axum::response::IntoResponse;
use axum::routing::{get, post};

use cm::config::{DownloadConfig, FallbackConfig};
use cm::download::{DownloadOrchestrator, FetchOutcome};
use cm::fallback::FallbackManager;
use cm::index::SnapshotIndex;
use cm::metrics::Metrics;
use cm::peer::PeerManager;

#[derive(Clone)]
struct MockSource {
    dir: PathBuf,
    full_name: Option<String>,
    inc_name: Option<String>,
    /// getSlot answer for the mock JSON-RPC endpoint.
    tip: u64,
    /// Mimic presigned mirrors (avorio-style): redirect Location carries a
    /// query string and points under /signed/, the concrete `/{file}` path
    /// 404s, and the signed route rejects HEAD.
    presigned: bool,
    probe_hits: Arc<AtomicUsize>,
}

impl MockSource {
    fn location_for(&self, name: &str) -> String {
        if self.presigned {
            format!("/signed/{}?X-Amz-Expires=600&sig=mock", name)
        } else {
            format!("/{}", name)
        }
    }
}

async fn full_redirect(State(s): State<MockSource>) -> impl IntoResponse {
    s.probe_hits.fetch_add(1, Ordering::SeqCst);
    match &s.full_name {
        Some(name) => (
            StatusCode::SEE_OTHER,
            [(header::LOCATION, s.location_for(name))],
        )
            .into_response(),
        None => StatusCode::NOT_FOUND.into_response(),
    }
}

async fn inc_redirect(State(s): State<MockSource>) -> impl IntoResponse {
    match &s.inc_name {
        Some(name) => (
            StatusCode::SEE_OTHER,
            [(header::LOCATION, s.location_for(name))],
        )
            .into_response(),
        None => StatusCode::NOT_FOUND.into_response(),
    }
}

async fn serve_file(State(s): State<MockSource>, Path(name): Path<String>) -> impl IntoResponse {
    if s.presigned {
        // Presigned mirrors don't serve concrete paths directly.
        return StatusCode::NOT_FOUND.into_response();
    }
    match tokio::fs::read(s.dir.join(&name)).await {
        Ok(data) => (StatusCode::OK, data).into_response(),
        Err(_) => StatusCode::NOT_FOUND.into_response(),
    }
}

async fn serve_signed(
    method: axum::http::Method,
    State(s): State<MockSource>,
    Path(name): Path<String>,
) -> impl IntoResponse {
    if method == axum::http::Method::HEAD {
        // Presigned object-store URLs are method-bound to GET.
        return StatusCode::FORBIDDEN.into_response();
    }
    match tokio::fs::read(s.dir.join(&name)).await {
        Ok(data) => (StatusCode::OK, data).into_response(),
        Err(_) => StatusCode::NOT_FOUND.into_response(),
    }
}

async fn json_rpc(
    State(s): State<MockSource>,
    body: axum::Json<serde_json::Value>,
) -> impl IntoResponse {
    let result = match body.0["method"].as_str() {
        Some("getSlot") => serde_json::json!(s.tip),
        // No extra cluster nodes in these tests.
        Some("getClusterNodes") => serde_json::json!([]),
        _ => serde_json::Value::Null,
    };
    axum::Json(serde_json::json!({"jsonrpc": "2.0", "id": 1, "result": result}))
}

/// Serve `state` on a random port; returns the base URL.
async fn start_mock_source(state: MockSource) -> String {
    let router = Router::new()
        .route("/snapshot.tar.bz2", get(full_redirect))
        .route("/incremental-snapshot.tar.bz2", get(inc_redirect))
        .route("/", post(json_rpc))
        .route("/signed/{name}", get(serve_signed))
        .route("/{name}", get(serve_file))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    tokio::time::sleep(Duration::from_millis(10)).await;
    format!("http://{}", addr)
}

fn touch(dir: &std::path::Path, name: &str, data: &[u8]) {
    std::fs::write(dir.join(name), data).unwrap();
}

fn fallback_config(base_url: &str, rpc: bool) -> FallbackConfig {
    FallbackConfig {
        enabled: true,
        mirrors: vec![base_url.to_string()],
        rpc_urls: if rpc {
            vec![base_url.to_string()]
        } else {
            vec![]
        },
        speed_test_bytes: 1024,
        speed_test_timeout: Duration::from_secs(2),
        probe_timeout: Duration::from_secs(2),
        ..Default::default()
    }
}

async fn make_orchestrator(
    local_dir: &std::path::Path,
    fallback: FallbackConfig,
) -> (DownloadOrchestrator, SnapshotIndex) {
    let metrics = Metrics::new(None);
    let peer_mgr = PeerManager::new(Default::default(), metrics.clone());
    let index = SnapshotIndex::new(local_dir.to_path_buf());
    index.scan().await.unwrap();
    let download_config = DownloadConfig {
        min_slots_behind: 0,
        ..Default::default()
    };
    let orch = DownloadOrchestrator::new(download_config, index.clone(), peer_mgr, metrics)
        .with_fallback(Some(FallbackManager::new(fallback)));
    (orch, index)
}

#[tokio::test]
async fn bootstraps_from_mirror_when_no_peers() {
    let mirror_dir = tempfile::tempdir().unwrap();
    let full_data = vec![7u8; 2048];
    let inc_data = vec![8u8; 512];
    touch(mirror_dir.path(), "snapshot-5000-mock.tar.zst", &full_data);
    touch(
        mirror_dir.path(),
        "incremental-snapshot-5000-5100-mock.tar.zst",
        &inc_data,
    );

    let base = start_mock_source(MockSource {
        dir: mirror_dir.path().to_path_buf(),
        full_name: Some("snapshot-5000-mock.tar.zst".into()),
        inc_name: Some("incremental-snapshot-5000-5100-mock.tar.zst".into()),
        tip: 5100,
        presigned: false,
        probe_hits: Arc::new(AtomicUsize::new(0)),
    })
    .await;

    let local_dir = tempfile::tempdir().unwrap();
    let (orch, index) = make_orchestrator(local_dir.path(), fallback_config(&base, false)).await;

    let downloaded = orch.fetch_if_needed().await.unwrap();
    assert_eq!(
        downloaded,
        FetchOutcome::Downloaded,
        "expected a fallback download"
    );

    let full_path = local_dir.path().join("snapshot-5000-mock.tar.zst");
    assert!(full_path.exists());
    assert_eq!(std::fs::read(&full_path).unwrap(), full_data);
    let inc_path = local_dir
        .path()
        .join("incremental-snapshot-5000-5100-mock.tar.zst");
    assert!(inc_path.exists());
    assert_eq!(std::fs::read(&inc_path).unwrap(), inc_data);

    // Admit wrote locally computed sha256 sidecars.
    assert!(
        local_dir
            .path()
            .join("snapshot-5000-mock.tar.zst.sha256")
            .exists()
    );

    assert_eq!(index.highest_slot().await, 5100);
}

#[tokio::test]
async fn downloads_from_presigned_mirror() {
    // Avorio-style mirror: files only reachable through the redirect target
    // (query-string presigned URL, GET-only), concrete paths 404.
    let mirror_dir = tempfile::tempdir().unwrap();
    let full_data = vec![9u8; 4096];
    touch(mirror_dir.path(), "snapshot-7000-mock.tar.zst", &full_data);

    let base = start_mock_source(MockSource {
        dir: mirror_dir.path().to_path_buf(),
        full_name: Some("snapshot-7000-mock.tar.zst".into()),
        inc_name: None,
        tip: 7000,
        presigned: true,
        probe_hits: Arc::new(AtomicUsize::new(0)),
    })
    .await;

    let local_dir = tempfile::tempdir().unwrap();
    let (orch, index) = make_orchestrator(local_dir.path(), fallback_config(&base, false)).await;

    let downloaded = orch.fetch_if_needed().await.unwrap();
    assert_eq!(
        downloaded,
        FetchOutcome::Downloaded,
        "expected download via presigned redirect"
    );

    let full_path = local_dir.path().join("snapshot-7000-mock.tar.zst");
    assert!(full_path.exists());
    assert_eq!(std::fs::read(&full_path).unwrap(), full_data);
    assert_eq!(index.highest_slot().await, 7000);
}

#[tokio::test]
async fn no_external_requests_without_fallback() {
    let mirror_dir = tempfile::tempdir().unwrap();
    touch(mirror_dir.path(), "snapshot-5000-mock.tar.zst", &[1u8; 128]);
    let probe_hits = Arc::new(AtomicUsize::new(0));

    let _base = start_mock_source(MockSource {
        dir: mirror_dir.path().to_path_buf(),
        full_name: Some("snapshot-5000-mock.tar.zst".into()),
        inc_name: None,
        tip: 5000,
        presigned: false,
        probe_hits: probe_hits.clone(),
    })
    .await;

    let local_dir = tempfile::tempdir().unwrap();
    let metrics = Metrics::new(None);
    let peer_mgr = PeerManager::new(Default::default(), metrics.clone());
    let index = SnapshotIndex::new(local_dir.path().to_path_buf());
    index.scan().await.unwrap();
    // No fallback attached: identical to today's behavior.
    let orch = DownloadOrchestrator::new(Default::default(), index, peer_mgr, metrics);

    let downloaded = orch.fetch_if_needed().await.unwrap();
    assert_eq!(downloaded, FetchOutcome::NoPeers);
    assert_eq!(probe_hits.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn cooldown_reuses_probe_results() {
    let mirror_dir = tempfile::tempdir().unwrap();
    touch(mirror_dir.path(), "snapshot-5000-mock.tar.zst", &[3u8; 256]);
    let probe_hits = Arc::new(AtomicUsize::new(0));

    let base = start_mock_source(MockSource {
        dir: mirror_dir.path().to_path_buf(),
        full_name: Some("snapshot-5000-mock.tar.zst".into()),
        inc_name: None,
        tip: 5000,
        presigned: false,
        probe_hits: probe_hits.clone(),
    })
    .await;

    let local_dir = tempfile::tempdir().unwrap();
    let (orch, _index) = make_orchestrator(local_dir.path(), fallback_config(&base, false)).await;

    assert_eq!(
        orch.fetch_if_needed().await.unwrap(),
        FetchOutcome::Downloaded
    );
    let hits_after_first = probe_hits.load(Ordering::SeqCst);
    assert!(hits_after_first >= 1);

    // Second cycle within the cooldown: cached sources, no new probes.
    assert_eq!(
        orch.fetch_if_needed().await.unwrap(),
        FetchOutcome::NotNeeded
    );
    assert_eq!(probe_hits.load(Ordering::SeqCst), hits_after_first);
}

#[tokio::test]
async fn stale_mirror_rejected_by_slot_lag() {
    let mirror_dir = tempfile::tempdir().unwrap();
    touch(mirror_dir.path(), "snapshot-1000-mock.tar.zst", &[4u8; 256]);
    let probe_hits = Arc::new(AtomicUsize::new(0));

    // Mock RPC reports the cluster tip miles ahead of the mirror's snapshot.
    let base = start_mock_source(MockSource {
        dir: mirror_dir.path().to_path_buf(),
        full_name: Some("snapshot-1000-mock.tar.zst".into()),
        inc_name: None,
        tip: 1_000_000,
        presigned: false,
        probe_hits: probe_hits.clone(),
    })
    .await;

    let local_dir = tempfile::tempdir().unwrap();
    let mut cfg = fallback_config(&base, true);
    cfg.max_slot_lag = 500;
    let (orch, index) = make_orchestrator(local_dir.path(), cfg).await;

    let downloaded = orch.fetch_if_needed().await.unwrap();
    assert_eq!(
        downloaded,
        FetchOutcome::NoPeers,
        "stale mirror must not be used"
    );
    assert_eq!(index.highest_slot().await, 0);
}

#[tokio::test]
async fn orphan_incremental_from_mirror_ignored() {
    // Mirror advertises an incremental whose base doesn't match its full:
    // only the full may be planned.
    let mirror_dir = tempfile::tempdir().unwrap();
    touch(mirror_dir.path(), "snapshot-5000-mock.tar.zst", &[5u8; 256]);
    touch(
        mirror_dir.path(),
        "incremental-snapshot-4000-4200-mock.tar.zst",
        &[6u8; 128],
    );

    let base = start_mock_source(MockSource {
        dir: mirror_dir.path().to_path_buf(),
        full_name: Some("snapshot-5000-mock.tar.zst".into()),
        inc_name: Some("incremental-snapshot-4000-4200-mock.tar.zst".into()),
        tip: 5000,
        presigned: false,
        probe_hits: Arc::new(AtomicUsize::new(0)),
    })
    .await;

    let local_dir = tempfile::tempdir().unwrap();
    let (orch, index) = make_orchestrator(local_dir.path(), fallback_config(&base, false)).await;

    assert_eq!(
        orch.fetch_if_needed().await.unwrap(),
        FetchOutcome::Downloaded
    );
    assert!(local_dir.path().join("snapshot-5000-mock.tar.zst").exists());
    assert!(
        !local_dir
            .path()
            .join("incremental-snapshot-4000-4200-mock.tar.zst")
            .exists()
    );
    assert_eq!(index.highest_slot().await, 5000);
}
