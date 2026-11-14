//! Solana JSON-RPC helpers for external source discovery: enumerate cluster
//! nodes with open RPC ports (`getClusterNodes`) and read the cluster tip
//! (`getSlot`). Never fatal — a failing endpoint degrades to the next one.

use serde_json::json;
use tracing::{debug, warn};

/// Base URLs (`http://host:port`) of cluster nodes advertising an RPC port,
/// from the first RPC endpoint that answers.
pub async fn cluster_rpc_nodes(client: &reqwest::Client, rpc_urls: &[String]) -> Vec<String> {
    for url in rpc_urls {
        match fetch_cluster_nodes(client, url).await {
            Ok(nodes) if !nodes.is_empty() => {
                debug!(rpc = %url, nodes = nodes.len(), "discovered cluster RPC nodes");
                return nodes;
            }
            Ok(_) => debug!(rpc = %url, "getClusterNodes returned no RPC nodes"),
            Err(e) => warn!(rpc = %url, error = %e, "getClusterNodes failed"),
        }
    }
    Vec::new()
}

async fn fetch_cluster_nodes(client: &reqwest::Client, url: &str) -> anyhow::Result<Vec<String>> {
    let resp: serde_json::Value = client
        .post(url)
        .json(&json!({"jsonrpc": "2.0", "id": 1, "method": "getClusterNodes"}))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;

    let nodes = resp["result"]
        .as_array()
        .ok_or_else(|| anyhow::anyhow!("getClusterNodes: no result array"))?
        .iter()
        .filter_map(|n| n["rpc"].as_str())
        .map(|rpc| format!("http://{}", rpc))
        .collect();
    Ok(nodes)
}

/// Cluster tip slot from the first RPC endpoint that answers.
pub async fn current_slot(client: &reqwest::Client, rpc_urls: &[String]) -> Option<u64> {
    for url in rpc_urls {
        match fetch_slot(client, url).await {
            Ok(slot) => return Some(slot),
            Err(e) => warn!(rpc = %url, error = %e, "getSlot failed"),
        }
    }
    None
}

async fn fetch_slot(client: &reqwest::Client, url: &str) -> anyhow::Result<u64> {
    let resp: serde_json::Value = client
        .post(url)
        .json(&json!({"jsonrpc": "2.0", "id": 1, "method": "getSlot"}))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;

    resp["result"]
        .as_u64()
        .ok_or_else(|| anyhow::anyhow!("getSlot: no numeric result"))
}
