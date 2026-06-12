use std::path::PathBuf;
use std::time::Duration;

use serde::Deserialize;

fn default_listen_addr() -> String {
    "0.0.0.0:13080".into()
}
fn default_metrics_addr() -> String {
    "0.0.0.0:9184".into()
}
fn default_poll_interval() -> Duration {
    Duration::from_secs(15)
}
fn default_parallel_chunks() -> u16 {
    16
}
fn default_min_slots_behind() -> u64 {
    2500
}
fn default_max_full() -> u32 {
    2
}
fn default_max_incremental() -> u32 {
    4
}
fn default_check_interval() -> Duration {
    Duration::from_secs(30)
}
fn default_upload_interval() -> Duration {
    Duration::from_secs(300)
}
fn default_aria2c_binary() -> String {
    "/usr/bin/aria2c".into()
}

#[derive(Debug, Deserialize, Clone)]
pub struct Config {
    pub node: NodeConfig,
    #[serde(default)]
    pub peers: PeersConfig,
    #[serde(default)]
    pub download: DownloadConfig,
    #[serde(default)]
    pub s3: S3Config,
    #[serde(default)]
    pub auto: AutoConfig,
}

#[derive(Debug, Deserialize, Clone)]
pub struct NodeConfig {
    pub snapshot_dir: PathBuf,
    #[serde(default = "default_listen_addr")]
    pub listen_addr: String,
    #[serde(default = "default_metrics_addr")]
    pub metrics_addr: String,
    #[serde(default)]
    pub cluster_name: Option<String>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct PeersConfig {
    #[serde(default = "default_poll_interval", with = "humantime_serde")]
    pub poll_interval: Duration,
    #[serde(default, rename = "static")]
    pub static_peers: Option<StaticPeersConfig>,
    #[serde(default)]
    pub tailscale: Option<TailscaleConfig>,
    /// Preferred datacenters, best first. Used to rank peers.
    #[serde(default)]
    pub dc_affinity: Vec<String>,
}

impl Default for PeersConfig {
    fn default() -> Self {
        Self {
            poll_interval: default_poll_interval(),
            static_peers: None,
            tailscale: None,
            dc_affinity: Vec::new(),
        }
    }
}

#[derive(Debug, Deserialize, Clone)]
pub struct StaticPeersConfig {
    pub hosts: Vec<String>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct TailscaleConfig {
    #[serde(default)]
    pub enabled: bool,
    pub tag: Option<String>,
    #[serde(default = "default_tailscale_port")]
    pub port: u16,
}

fn default_tailscale_port() -> u16 {
    13080
}

#[derive(Debug, Deserialize, Clone)]
pub struct DownloadConfig {
    #[serde(default)]
    pub backend: DownloadBackend,
    #[serde(default = "default_parallel_chunks")]
    pub parallel_chunks: u16,
    #[serde(default = "default_min_slots_behind")]
    pub min_slots_behind: u64,
    #[serde(default = "default_max_full")]
    pub max_full_snapshots: u32,
    #[serde(default = "default_max_incremental")]
    pub max_incremental_snapshots: u32,
    /// Deprecated: use `peers.dc_affinity`. Still honored when the new key is unset.
    #[serde(default)]
    pub dc_affinity: Vec<String>,
    /// Minimum download speed in bytes/sec. Abort if speed stays below this
    /// for 60 continuous seconds. 0 = disabled.
    #[serde(default)]
    pub min_speed: u64,
    #[serde(default)]
    pub aria2c: Aria2cConfig,
}

impl Default for DownloadConfig {
    fn default() -> Self {
        Self {
            backend: DownloadBackend::default(),
            parallel_chunks: default_parallel_chunks(),
            min_slots_behind: default_min_slots_behind(),
            max_full_snapshots: default_max_full(),
            max_incremental_snapshots: default_max_incremental(),
            dc_affinity: Vec::new(),
            min_speed: 0,
            aria2c: Aria2cConfig::default(),
        }
    }
}

#[derive(Debug, Deserialize, Clone, Default)]
#[serde(rename_all = "lowercase")]
pub enum DownloadBackend {
    #[default]
    Builtin,
    Aria2c,
}

#[derive(Debug, Deserialize, Clone)]
pub struct Aria2cConfig {
    #[serde(default = "default_aria2c_binary")]
    pub binary: String,
    #[serde(default)]
    pub extra_args: Vec<String>,
}

impl Default for Aria2cConfig {
    fn default() -> Self {
        Self {
            binary: default_aria2c_binary(),
            extra_args: Vec::new(),
        }
    }
}

#[derive(Debug, Deserialize, Clone, Default)]
pub struct S3Config {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub bucket: String,
    #[serde(default)]
    pub region: Option<String>,
    #[serde(default)]
    pub endpoint: Option<String>,
    #[serde(default)]
    pub prefix: String,
    #[serde(default)]
    pub path_style: bool,
    #[serde(default = "default_upload_interval", with = "humantime_serde")]
    pub upload_interval: Duration,
}

#[derive(Debug, Deserialize, Clone)]
pub struct AutoConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_check_interval", with = "humantime_serde")]
    pub check_interval: Duration,
}

impl Default for AutoConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            check_interval: default_check_interval(),
        }
    }
}

impl Config {
    pub fn load(path: &std::path::Path) -> anyhow::Result<Self> {
        let contents = std::fs::read_to_string(path)?;
        contents.parse()
    }

    /// Migrate deprecated keys to their current location.
    fn normalize(mut self) -> Self {
        if self.peers.dc_affinity.is_empty() && !self.download.dc_affinity.is_empty() {
            self.peers.dc_affinity = std::mem::take(&mut self.download.dc_affinity);
        }
        self
    }
}

impl std::str::FromStr for Config {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> anyhow::Result<Self> {
        let config: Config = toml::from_str(s)?;
        Ok(config.normalize())
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;

    #[test]
    fn parse_minimal_config() {
        let cfg = Config::from_str(
            r#"
            [node]
            snapshot_dir = "/mnt/snapshots"
            "#,
        )
        .unwrap();
        assert_eq!(cfg.node.snapshot_dir, PathBuf::from("/mnt/snapshots"));
        assert_eq!(cfg.node.listen_addr, "0.0.0.0:13080");
        assert_eq!(cfg.node.metrics_addr, "0.0.0.0:9184");
        assert_eq!(cfg.peers.poll_interval, Duration::from_secs(15));
        assert!(cfg.peers.static_peers.is_none());
        assert!(cfg.peers.tailscale.is_none());
        assert_eq!(cfg.download.parallel_chunks, 16);
        assert_eq!(cfg.download.min_slots_behind, 2500);
        assert_eq!(cfg.download.max_full_snapshots, 2);
        assert_eq!(cfg.download.max_incremental_snapshots, 4);
        assert!(cfg.auto.enabled);
        assert_eq!(cfg.auto.check_interval, Duration::from_secs(30));
        assert!(!cfg.s3.enabled);
    }

    #[test]
    fn parse_full_config() {
        let cfg = Config::from_str(
            r#"
            [node]
            snapshot_dir = "/data/snapshots"
            listen_addr = "10.0.0.1:9000"
            metrics_addr = "10.0.0.1:9001"

            [peers]
            poll_interval = "30s"

            [peers.static]
            hosts = ["peer1:13080", "peer2:13080"]

            [peers.tailscale]
            enabled = true
            tag = "tag:solana-rpc"
            port = 14000

            [download]
            backend = "aria2c"
            parallel_chunks = 8
            min_slots_behind = 1000
            max_full_snapshots = 3
            max_incremental_snapshots = 6
            dc_affinity = ["ams", "fra"]
            min_speed = 50_000_000

            [download.aria2c]
            binary = "/usr/local/bin/aria2c"
            extra_args = ["--max-concurrent-downloads=5"]

            [s3]
            enabled = true
            bucket = "my-bucket"
            region = "us-east-1"
            endpoint = "https://s3.custom.com"
            prefix = "snapshots/"
            path_style = true
            upload_interval = "10m"

            [auto]
            enabled = false
            check_interval = "1m"
            "#,
        )
        .unwrap();
        assert_eq!(cfg.node.snapshot_dir, PathBuf::from("/data/snapshots"));
        assert_eq!(cfg.node.listen_addr, "10.0.0.1:9000");
        assert_eq!(cfg.peers.poll_interval, Duration::from_secs(30));
        let static_peers = cfg.peers.static_peers.unwrap();
        assert_eq!(static_peers.hosts, vec!["peer1:13080", "peer2:13080"]);
        let ts = cfg.peers.tailscale.unwrap();
        assert!(ts.enabled);
        assert_eq!(ts.tag.as_deref(), Some("tag:solana-rpc"));
        assert_eq!(ts.port, 14000);
        assert!(matches!(cfg.download.backend, DownloadBackend::Aria2c));
        assert_eq!(cfg.download.parallel_chunks, 8);
        // Deprecated [download].dc_affinity migrates to peers
        assert_eq!(cfg.peers.dc_affinity, vec!["ams", "fra"]);
        assert!(cfg.download.dc_affinity.is_empty());
        assert_eq!(cfg.download.min_speed, 50_000_000);
        assert_eq!(cfg.download.aria2c.binary, "/usr/local/bin/aria2c");
        assert_eq!(
            cfg.download.aria2c.extra_args,
            vec!["--max-concurrent-downloads=5"]
        );
        assert!(cfg.s3.enabled);
        assert_eq!(cfg.s3.bucket, "my-bucket");
        assert_eq!(cfg.s3.region.as_deref(), Some("us-east-1"));
        assert!(cfg.s3.path_style);
        assert_eq!(cfg.s3.upload_interval, Duration::from_secs(600));
        assert!(!cfg.auto.enabled);
        assert_eq!(cfg.auto.check_interval, Duration::from_secs(60));
    }

    #[test]
    fn dc_affinity_under_peers() {
        let cfg = Config::from_str(
            r#"
            [node]
            snapshot_dir = "/tmp"

            [peers]
            dc_affinity = ["fra", "ams"]
            "#,
        )
        .unwrap();
        assert_eq!(cfg.peers.dc_affinity, vec!["fra", "ams"]);
    }

    #[test]
    fn peers_dc_affinity_wins_over_deprecated_key() {
        let cfg = Config::from_str(
            r#"
            [node]
            snapshot_dir = "/tmp"

            [peers]
            dc_affinity = ["fra"]

            [download]
            dc_affinity = ["ams"]
            "#,
        )
        .unwrap();
        assert_eq!(cfg.peers.dc_affinity, vec!["fra"]);
    }

    #[test]
    fn missing_snapshot_dir_fails() {
        let result = Config::from_str(
            r#"
            [node]
            listen_addr = "0.0.0.0:8080"
            "#,
        );
        assert!(result.is_err());
    }

    #[test]
    fn invalid_backend_fails() {
        let result = Config::from_str(
            r#"
            [node]
            snapshot_dir = "/tmp"

            [download]
            backend = "wget"
            "#,
        );
        assert!(result.is_err());
    }

    #[test]
    fn humantime_durations() {
        let cfg = Config::from_str(
            r#"
            [node]
            snapshot_dir = "/tmp"

            [peers]
            poll_interval = "5s"

            [auto]
            check_interval = "2m"

            [s3]
            upload_interval = "1h"
            "#,
        )
        .unwrap();
        assert_eq!(cfg.peers.poll_interval, Duration::from_secs(5));
        assert_eq!(cfg.auto.check_interval, Duration::from_secs(120));
        assert_eq!(cfg.s3.upload_interval, Duration::from_secs(3600));
    }

    #[test]
    fn load_from_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");
        std::fs::write(
            &path,
            r#"
            [node]
            snapshot_dir = "/mnt/snap"
            "#,
        )
        .unwrap();
        let cfg = Config::load(&path).unwrap();
        assert_eq!(cfg.node.snapshot_dir, PathBuf::from("/mnt/snap"));
    }
}
