use std::sync::Arc;

use axum::body::Body;
use axum::extract::{Path as AxumPath, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::{IntoResponse, Json, Response};
use axum::routing::get;
use axum::Router;
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tokio_util::io::ReaderStream;
use tracing::info;

use crate::index::SnapshotIndex;
use crate::metrics::Metrics;
use crate::peer::PeerManager;

#[derive(Clone)]
pub struct AppState {
    pub index: SnapshotIndex,
    pub peer_mgr: PeerManager,
    pub metrics: Metrics,
}

pub fn api_router(state: AppState) -> Router {
    Router::new()
        .route("/api/v1/snapshots", get(list_snapshots))
        .route("/api/v1/snapshot/{filename}", get(serve_snapshot))
        .route("/api/v1/peers", get(list_peers))
        .route("/api/v1/status", get(status))
        .route("/v1/best_snapshots", get(best_snapshots_compat))
        .with_state(Arc::new(state))
}

pub fn metrics_router(metrics: Metrics) -> Router {
    Router::new()
        .route("/metrics", get(metrics_handler))
        .with_state(Arc::new(metrics))
}

async fn list_snapshots(
    State(state): State<Arc<AppState>>,
) -> Json<Vec<crate::snapshot::SnapshotInfo>> {
    Json(state.index.list().await)
}

async fn serve_snapshot(
    State(state): State<Arc<AppState>>,
    AxumPath(filename): AxumPath<String>,
    headers: HeaderMap,
) -> Result<Response, StatusCode> {
    // Validate filename to prevent path traversal
    if filename.contains('/') || filename.contains("..") {
        return Err(StatusCode::BAD_REQUEST);
    }

    let snap = state
        .index
        .get(&filename)
        .await
        .ok_or(StatusCode::NOT_FOUND)?;

    let path = state.index.dir().join(&snap.file_name);
    let file = tokio::fs::File::open(&path)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let metadata = file
        .metadata()
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let total_size = metadata.len();

    // Handle range requests
    if let Some(range_header) = headers.get(header::RANGE) {
        let range_str = range_header
            .to_str()
            .map_err(|_| StatusCode::BAD_REQUEST)?;

        if let Some((start, end)) = parse_range(range_str, total_size) {
            let length = end - start + 1;
            let mut file = file;
            file.seek(std::io::SeekFrom::Start(start))
                .await
                .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

            let stream = ReaderStream::new(file.take(length));
            let body = Body::from_stream(stream);

            return Ok(Response::builder()
                .status(StatusCode::PARTIAL_CONTENT)
                .header(header::CONTENT_TYPE, "application/octet-stream")
                .header(header::CONTENT_LENGTH, length.to_string())
                .header(
                    header::CONTENT_RANGE,
                    format!("bytes {}-{}/{}", start, end, total_size),
                )
                .header(header::ACCEPT_RANGES, "bytes")
                .body(body)
                .unwrap());
        }
    }

    // Full file response
    let stream = ReaderStream::new(file);
    let body = Body::from_stream(stream);

    Ok(Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/octet-stream")
        .header(header::CONTENT_LENGTH, total_size.to_string())
        .header(header::ACCEPT_RANGES, "bytes")
        .header(
            header::CONTENT_DISPOSITION,
            format!("attachment; filename=\"{}\"", filename),
        )
        .body(body)
        .unwrap())
}

pub fn parse_range(range: &str, total: u64) -> Option<(u64, u64)> {
    if total == 0 {
        return None;
    }
    let range = range.strip_prefix("bytes=")?;
    let mut parts = range.splitn(2, '-');
    let start_str = parts.next()?;
    let end_str = parts.next()?;

    if start_str.is_empty() {
        // Suffix range: bytes=-N means last N bytes
        let suffix: u64 = end_str.parse().ok()?;
        let start = total.saturating_sub(suffix);
        return Some((start, total - 1));
    }

    let start: u64 = start_str.parse().ok()?;
    let end: u64 = if end_str.is_empty() {
        total - 1
    } else {
        end_str.parse().ok()?
    };

    if start <= end && end < total {
        Some((start, end))
    } else {
        None
    }
}

async fn list_peers(
    State(state): State<Arc<AppState>>,
) -> Json<Vec<crate::peer::PeerInfo>> {
    Json(state.peer_mgr.list_peers().await)
}

#[derive(serde::Serialize, serde::Deserialize)]
pub struct StatusResponse {
    pub local_snapshots: Vec<crate::snapshot::SnapshotInfo>,
    pub highest_slot: u64,
    pub peer_count: usize,
}

async fn status(
    State(state): State<Arc<AppState>>,
) -> Json<StatusResponse> {
    let local_snapshots = state.index.list().await;
    let highest_slot = state.index.highest_slot().await;
    let peer_count = state.peer_mgr.peer_count().await;
    Json(StatusResponse {
        local_snapshots,
        highest_slot,
        peer_count,
    })
}

async fn metrics_handler(
    State(metrics): State<Arc<Metrics>>,
) -> impl IntoResponse {
    let body = metrics.encode();
    (
        [(header::CONTENT_TYPE, "text/plain; version=0.0.4")],
        body,
    )
}

/// Blockdaemon-compatible `/v1/best_snapshots` response types.
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct CompatSnapshotSource {
    pub slot: u64,
    pub hash: String,
    pub files: Vec<CompatSnapshotFile>,
    pub size: u64,
    pub target: String,
    pub updated_at: String,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct CompatSnapshotFile {
    pub file_name: String,
    pub slot: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub base_slot: Option<u64>,
    pub hash: String,
    pub ext: String,
    pub size: u64,
}

#[derive(serde::Deserialize)]
struct BestSnapshotsQuery {
    #[serde(default = "default_max")]
    max: usize,
}

fn default_max() -> usize {
    25
}

/// Compatibility endpoint matching old Blockdaemon tracker GET /v1/best_snapshots?max=N
async fn best_snapshots_compat(
    State(state): State<Arc<AppState>>,
    axum::extract::Query(query): axum::extract::Query<BestSnapshotsQuery>,
) -> Json<Vec<CompatSnapshotSource>> {
    let max = query.max.min(25);
    let snaps = state.index.list().await;

    // Group: find best full snapshot(s) + their incrementals
    let full_snaps: Vec<&crate::snapshot::SnapshotInfo> =
        snaps.iter().filter(|s| s.is_full()).take(max).collect();

    let mut sources = Vec::new();
    for full in &full_snaps {
        let mut files = vec![snap_to_compat_file(full)];
        // Attach matching incrementals
        for inc in snaps.iter().filter(|s| !s.is_full() && s.base_slot == Some(full.slot)) {
            files.push(snap_to_compat_file(inc));
        }
        let total_size: u64 = files.iter().map(|f| f.size).sum();
        let hash = full.hash_component();
        sources.push(CompatSnapshotSource {
            slot: full.slot,
            hash,
            files,
            size: total_size,
            target: format!("http://localhost/api/v1/snapshot"),
            updated_at: chrono_or_epoch(full.modified_at),
        });
    }

    Json(sources)
}

fn snap_to_compat_file(s: &crate::snapshot::SnapshotInfo) -> CompatSnapshotFile {
    CompatSnapshotFile {
        file_name: s.file_name.clone(),
        slot: s.slot,
        base_slot: s.base_slot,
        hash: s.hash_component(),
        ext: "tar.zst".into(),
        size: s.size,
    }
}

fn chrono_or_epoch(ts: u64) -> String {
    use std::time::{Duration, UNIX_EPOCH};
    let dt = UNIX_EPOCH + Duration::from_secs(ts);
    let secs = dt
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    format!(
        "{}-{:02}-{:02}T{:02}:{:02}:{:02}Z",
        1970 + secs / 31_557_600,
        (secs % 31_557_600) / 2_629_800 + 1,
        (secs % 2_629_800) / 86400 + 1,
        (secs % 86400) / 3600,
        (secs % 3600) / 60,
        secs % 60
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn range_full_range() {
        assert_eq!(parse_range("bytes=0-499", 1000), Some((0, 499)));
    }

    #[test]
    fn range_open_end() {
        assert_eq!(parse_range("bytes=500-", 1000), Some((500, 999)));
    }

    #[test]
    fn range_suffix() {
        assert_eq!(parse_range("bytes=-200", 1000), Some((800, 999)));
    }

    #[test]
    fn range_exact_last_byte() {
        assert_eq!(parse_range("bytes=999-999", 1000), Some((999, 999)));
    }

    #[test]
    fn range_entire_file() {
        assert_eq!(parse_range("bytes=0-999", 1000), Some((0, 999)));
    }

    #[test]
    fn range_out_of_bounds() {
        assert_eq!(parse_range("bytes=0-1000", 1000), None);
    }

    #[test]
    fn range_start_after_end() {
        assert_eq!(parse_range("bytes=500-100", 1000), None);
    }

    #[test]
    fn range_no_prefix() {
        assert_eq!(parse_range("0-100", 1000), None);
    }

    #[test]
    fn range_zero_size_file() {
        assert_eq!(parse_range("bytes=0-0", 0), None);
    }

    #[test]
    fn range_suffix_larger_than_file() {
        // bytes=-2000 on a 1000-byte file should start at 0
        assert_eq!(parse_range("bytes=-2000", 1000), Some((0, 999)));
    }
}

/// Start the main API server.
pub async fn serve_api(state: AppState, addr: &str) -> anyhow::Result<()> {
    let router = api_router(state);
    let listener = tokio::net::TcpListener::bind(addr).await?;
    info!(addr, "API server listening");
    axum::serve(listener, router).await?;
    Ok(())
}

/// Start the metrics server on a separate port.
pub async fn serve_metrics(metrics: Metrics, addr: &str) -> anyhow::Result<()> {
    let router = metrics_router(metrics);
    let listener = tokio::net::TcpListener::bind(addr).await?;
    info!(addr, "metrics server listening");
    axum::serve(listener, router).await?;
    Ok(())
}
