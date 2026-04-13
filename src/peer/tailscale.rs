use std::collections::HashMap;

use anyhow::{Context, Result};
use serde::Deserialize;

pub struct TailscaleDiscovery {
    tag: Option<String>,
    port: u16,
}

impl TailscaleDiscovery {
    pub fn new(tag: Option<String>, port: u16) -> Self {
        Self { tag, port }
    }

    pub async fn discover(&self) -> Result<Vec<String>> {
        let output = tokio::process::Command::new("tailscale")
            .args(["status", "--json"])
            .output()
            .await
            .context("failed to run tailscale status")?;

        if !output.status.success() {
            anyhow::bail!("tailscale status exited with {}", output.status);
        }

        let status: TailscaleStatus =
            serde_json::from_slice(&output.stdout).context("parse tailscale status")?;

        let peers = status.peer.unwrap_or_default();
        let mut addrs = Vec::new();

        for peer in peers.values() {
            if !peer.online {
                continue;
            }
            if let Some(ref required_tag) = self.tag {
                let has_tag = peer
                    .tags
                    .as_ref()
                    .is_some_and(|tags| tags.iter().any(|t| t == required_tag));
                if !has_tag {
                    continue;
                }
            }
            let host = peer.dns_name.trim_end_matches('.');
            if !host.is_empty() {
                addrs.push(format!("{}:{}", host, self.port));
            }
        }

        Ok(addrs)
    }
}

#[derive(Deserialize)]
struct TailscaleStatus {
    #[serde(rename = "Peer")]
    peer: Option<HashMap<String, TailscalePeer>>,
}

#[derive(Deserialize)]
struct TailscalePeer {
    #[serde(rename = "DNSName")]
    dns_name: String,
    #[serde(rename = "Online")]
    online: bool,
    #[serde(rename = "Tags")]
    tags: Option<Vec<String>>,
}
