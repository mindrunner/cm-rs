use anyhow::Result;

use super::static_discovery::StaticDiscovery;
use super::tailscale::TailscaleDiscovery;

/// Enum-based peer discovery to avoid dyn-incompatible async traits.
pub enum PeerDiscoverySource {
    Static(StaticDiscovery),
    Tailscale(TailscaleDiscovery),
}

impl PeerDiscoverySource {
    /// Returns a list of peer addresses (host:port).
    pub async fn discover(&self) -> Result<Vec<String>> {
        match self {
            Self::Static(s) => s.discover().await,
            Self::Tailscale(s) => s.discover().await,
        }
    }
}
