use tracing::{info, warn};

use crate::config::Config;
use crate::download::DownloadOrchestrator;
use crate::index::SnapshotIndex;
use crate::metrics::{Metrics, TypeLabel};
use crate::peer::PeerManager;
use crate::s3::S3Uploader;
use crate::server::{self, AppState};

/// Run the full daemon: indexer + server + peer manager + auto-fetch + S3.
pub async fn run(config: Config) -> anyhow::Result<()> {
    info!("starting cm daemon");

    let metrics = Metrics::new(config.node.cluster_name.as_deref());
    let index = SnapshotIndex::new(config.node.snapshot_dir.clone());
    index.scan().await?;
    update_local_metrics(&index, &metrics).await;

    let peer_mgr = PeerManager::new(
        config.peers.clone(),
        config.download.dc_affinity.clone(),
        metrics.clone(),
    );

    let orchestrator = DownloadOrchestrator::new(
        config.download.clone(),
        index.clone(),
        peer_mgr.clone(),
        metrics.clone(),
    );

    let state = AppState {
        index: index.clone(),
        peer_mgr: peer_mgr.clone(),
        metrics: metrics.clone(),
    };

    let api_addr = config.node.listen_addr.clone();
    tokio::spawn(async move {
        if let Err(e) = server::serve_api(state, &api_addr).await {
            tracing::error!(error = %e, "API server failed");
        }
    });

    let metrics_addr = config.node.metrics_addr.clone();
    let metrics_clone = metrics.clone();
    tokio::spawn(async move {
        if let Err(e) = server::serve_metrics(metrics_clone, &metrics_addr).await {
            tracing::error!(error = %e, "metrics server failed");
        }
    });

    let index_watcher = index.clone();
    let metrics_watcher = metrics.clone();
    tokio::spawn(async move {
        loop {
            if let Err(e) = index_watcher.watch_loop().await {
                warn!(error = %e, "inotify watcher restarting");
            }
            update_local_metrics(&index_watcher, &metrics_watcher).await;
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        }
    });

    let peer_mgr_poll = peer_mgr.clone();
    tokio::spawn(async move {
        peer_mgr_poll.poll_loop().await;
    });

    if config.s3.enabled {
        let s3_uploader =
            S3Uploader::new(config.s3.clone(), index.clone(), metrics.clone()).await?;
        tokio::spawn(async move {
            s3_uploader.upload_loop().await;
        });
    }

    if config.auto.enabled {
        let interval = config.auto.check_interval;
        info!(interval_secs = interval.as_secs(), "auto-fetch enabled");
        loop {
            tokio::time::sleep(interval).await;
            match orchestrator.fetch_if_needed().await {
                Ok(true) => {
                    update_local_metrics(&index, &metrics).await;
                    info!("auto-fetch: downloaded new snapshot");
                }
                Ok(false) => {}
                Err(e) => {
                    warn!(error = %e, "auto-fetch failed");
                }
            }
            update_local_metrics(&index, &metrics).await;
        }
    } else {
        info!("auto-fetch disabled, running in serve-only mode");
        std::future::pending::<()>().await;
    }

    Ok(())
}

/// One-shot fetch: discover peers, poll them once, find best snapshot, download, exit.
pub async fn fetch_once(config: Config) -> anyhow::Result<()> {
    info!("one-shot snapshot fetch");

    let metrics = Metrics::new(config.node.cluster_name.as_deref());
    let index = SnapshotIndex::new(config.node.snapshot_dir.clone());
    index.scan().await?;

    let peer_mgr = PeerManager::new(
        config.peers.clone(),
        config.download.dc_affinity.clone(),
        metrics.clone(),
    );

    // Run one poll cycle with a timeout to populate the peer list
    info!("discovering and polling peers...");
    let poll_fut = peer_mgr.poll_loop();
    let timeout = tokio::time::sleep(std::time::Duration::from_secs(10));
    tokio::select! {
        _ = poll_fut => {}
        _ = timeout => {}
    }

    let peer_count = peer_mgr.peer_count().await;
    info!(peer_count, "poll cycle complete");

    let orchestrator = DownloadOrchestrator::new(
        config.download.clone(),
        index.clone(),
        peer_mgr,
        metrics,
    );

    match orchestrator.fetch_if_needed().await {
        Ok(true) => info!("fetch complete: downloaded new snapshot"),
        Ok(false) => info!("fetch complete: no download needed"),
        Err(e) => return Err(e),
    }
    Ok(())
}

/// One-shot S3 upload.
pub async fn upload_once(config: Config) -> anyhow::Result<()> {
    if !config.s3.enabled {
        anyhow::bail!("S3 is not enabled in config");
    }

    let metrics = Metrics::new(config.node.cluster_name.as_deref());
    let index = SnapshotIndex::new(config.node.snapshot_dir.clone());
    index.scan().await?;

    let uploader = S3Uploader::new(config.s3, index, metrics).await?;
    match uploader.upload_best().await? {
        true => info!("upload complete"),
        false => info!("nothing new to upload"),
    }
    Ok(())
}

/// Print daemon status by querying its API.
pub async fn print_status(url: &str) -> anyhow::Result<()> {
    let client = reqwest::Client::new();
    let resp: serde_json::Value = client
        .get(format!("{}/api/v1/status", url))
        .send()
        .await?
        .json()
        .await?;

    println!("{}", serde_json::to_string_pretty(&resp)?);
    Ok(())
}

/// Print peer list by querying the daemon API.
pub async fn print_peers(url: &str) -> anyhow::Result<()> {
    let client = reqwest::Client::new();
    let resp: Vec<crate::peer::PeerInfo> = client
        .get(format!("{}/api/v1/peers", url))
        .send()
        .await?
        .json()
        .await?;

    if resp.is_empty() {
        println!("No peers discovered.");
        return Ok(());
    }

    println!(
        "{:<40} {:>10} {:>10} {:>12} {:>8} {:>8}",
        "PEER", "SLOT", "FILES", "LATENCY", "BW(MB/s)", "ERRORS"
    );
    println!("{}", "-".repeat(90));

    for peer in &resp {
        let slot = peer.highest_slot();
        let files = peer.snapshots.len();
        let latency = peer
            .stats
            .latency_ms
            .map(|l| format!("{:.1}ms", l))
            .unwrap_or_else(|| "-".into());
        let bw = peer
            .stats
            .bandwidth_bps
            .map(|b| format!("{:.1}", b / 1_000_000.0))
            .unwrap_or_else(|| "-".into());

        println!(
            "{:<40} {:>10} {:>10} {:>12} {:>8} {:>8}",
            peer.addr, slot, files, latency, bw, peer.stats.errors
        );
    }

    Ok(())
}

async fn update_local_metrics(index: &SnapshotIndex, metrics: &Metrics) {
    let snaps = index.list().await;
    let full_count = snaps.iter().filter(|s| s.is_full()).count();
    let inc_count = snaps.iter().filter(|s| !s.is_full()).count();

    metrics
        .local_snapshot_count
        .get_or_create(&TypeLabel::new("full"))
        .set(full_count as i64);
    metrics
        .local_snapshot_count
        .get_or_create(&TypeLabel::new("incremental"))
        .set(inc_count as i64);

    if let Some(slot) = snaps.iter().filter(|s| s.is_full()).map(|s| s.slot).max() {
        metrics
            .local_snapshot_slot
            .get_or_create(&TypeLabel::new("full"))
            .set(slot as i64);
    }
    if let Some(slot) = snaps.iter().filter(|s| !s.is_full()).map(|s| s.slot).max() {
        metrics
            .local_snapshot_slot
            .get_or_create(&TypeLabel::new("incremental"))
            .set(slot as i64);
    }
}
