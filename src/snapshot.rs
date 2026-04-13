use std::fmt::Write as _;
use std::path::Path;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SnapshotType {
    Full,
    Incremental,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotInfo {
    pub file_name: String,
    pub snapshot_type: SnapshotType,
    pub slot: u64,
    /// For incremental snapshots, the base full snapshot slot.
    pub base_slot: Option<u64>,
    pub size: u64,
    pub modified_at: u64,
    /// SHA-256 hex digest of the file contents. Computed lazily during index scan.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sha256: Option<String>,
}

impl SnapshotInfo {
    /// Parse a snapshot filename like `snapshot-123456789-HASH.tar.zst`
    /// or `incremental-snapshot-100000000-123456789-HASH.tar.zst`.
    pub fn from_filename(file_name: &str, size: u64, modified_at: u64) -> Option<Self> {
        if let Some(rest) = file_name.strip_prefix("incremental-snapshot-") {
            let rest = rest.strip_suffix(".tar.zst")?;
            let mut parts = rest.splitn(3, '-');
            let base_slot: u64 = parts.next()?.parse().ok()?;
            let slot: u64 = parts.next()?.parse().ok()?;
            Some(Self {
                file_name: file_name.to_string(),
                snapshot_type: SnapshotType::Incremental,
                slot,
                base_slot: Some(base_slot),
                size,
                modified_at,
                sha256: None,
            })
        } else if let Some(rest) = file_name.strip_prefix("snapshot-") {
            let rest = rest.strip_suffix(".tar.zst")?;
            let mut parts = rest.splitn(2, '-');
            let slot: u64 = parts.next()?.parse().ok()?;
            Some(Self {
                file_name: file_name.to_string(),
                snapshot_type: SnapshotType::Full,
                slot,
                base_slot: None,
                size,
                modified_at,
                sha256: None,
            })
        } else {
            None
        }
    }

    pub fn is_full(&self) -> bool {
        self.snapshot_type == SnapshotType::Full
    }
}

/// Compute SHA-256 of a file, streaming in 8 MB chunks. Suitable for multi-GB files.
pub fn sha256_file(path: &Path) -> std::io::Result<String> {
    use std::io::Read;
    let mut file = std::fs::File::open(path)?;
    let mut hasher = Sha256::new();
    let mut buf = vec![0u8; 8 * 1024 * 1024];
    loop {
        let n = file.read(&mut buf)?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    Ok(hex_encode(&hasher.finalize()))
}

/// Streaming SHA-256 hasher for computing digest during downloads.
pub struct StreamingHasher {
    hasher: Sha256,
}

impl StreamingHasher {
    pub fn new() -> Self {
        Self {
            hasher: Sha256::new(),
        }
    }

    pub fn update(&mut self, data: &[u8]) {
        self.hasher.update(data);
    }

    pub fn finalize(self) -> String {
        hex_encode(&self.hasher.finalize())
    }
}

fn hex_encode(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        write!(s, "{b:02x}").unwrap();
    }
    s
}

/// Public hex encoder for SHA-256 digest bytes (used by download module).
pub fn hex_encode_sha256(bytes: &[u8]) -> String {
    hex_encode(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_full_snapshot() {
        let info = SnapshotInfo::from_filename(
            "snapshot-312902937-4kRecSQVGmEYRnsH2pEcGySuzGMiVEhXaHK3LCs7TmD2.tar.zst",
            1_000_000,
            1700000000,
        )
        .unwrap();
        assert_eq!(info.slot, 312902937);
        assert!(info.is_full());
        assert!(info.base_slot.is_none());
    }

    #[test]
    fn parse_incremental_snapshot() {
        let info = SnapshotInfo::from_filename(
            "incremental-snapshot-312902937-312912345-5xB2pCF.tar.zst",
            500_000,
            1700000000,
        )
        .unwrap();
        assert_eq!(info.slot, 312912345);
        assert!(!info.is_full());
        assert_eq!(info.base_slot, Some(312902937));
    }

    #[test]
    fn reject_garbage() {
        assert!(SnapshotInfo::from_filename("garbage.txt", 0, 0).is_none());
    }

    #[test]
    fn reject_no_extension() {
        assert!(SnapshotInfo::from_filename("snapshot-100-abc", 0, 0).is_none());
    }

    #[test]
    fn reject_wrong_prefix() {
        assert!(SnapshotInfo::from_filename("backup-100-abc.tar.zst", 0, 0).is_none());
    }

    #[test]
    fn slot_zero() {
        let info = SnapshotInfo::from_filename("snapshot-0-hash.tar.zst", 100, 0).unwrap();
        assert_eq!(info.slot, 0);
        assert!(info.is_full());
    }

    #[test]
    fn large_slot() {
        let info =
            SnapshotInfo::from_filename("snapshot-999999999999-hash.tar.zst", 100, 0).unwrap();
        assert_eq!(info.slot, 999999999999);
    }

    #[test]
    fn multi_hyphen_hash() {
        let info = SnapshotInfo::from_filename(
            "snapshot-12345-abc-def-ghi.tar.zst",
            100,
            0,
        )
        .unwrap();
        assert_eq!(info.slot, 12345);
        assert!(info.is_full());
    }

    #[test]
    fn incremental_is_not_full() {
        let info = SnapshotInfo::from_filename(
            "incremental-snapshot-100-200-hash.tar.zst",
            100,
            0,
        )
        .unwrap();
        assert!(!info.is_full());
        assert_eq!(info.snapshot_type, SnapshotType::Incremental);
    }

    #[test]
    fn preserves_size_and_modified() {
        let info = SnapshotInfo::from_filename("snapshot-42-h.tar.zst", 999, 1234567890).unwrap();
        assert_eq!(info.size, 999);
        assert_eq!(info.modified_at, 1234567890);
    }

    #[test]
    fn sha256_file_computes_correct_hash() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("testfile");
        std::fs::write(&path, b"hello world").unwrap();
        let hash = sha256_file(&path).unwrap();
        // SHA-256 of "hello world"
        assert_eq!(
            hash,
            "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"
        );
    }

    #[test]
    fn sha256_file_empty() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("empty");
        std::fs::write(&path, b"").unwrap();
        let hash = sha256_file(&path).unwrap();
        // SHA-256 of empty input
        assert_eq!(
            hash,
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
    }

    #[test]
    fn streaming_hasher_matches_file_hash() {
        let data = b"the quick brown fox jumps over the lazy dog";
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("fox");
        std::fs::write(&path, data).unwrap();
        let file_hash = sha256_file(&path).unwrap();

        let mut hasher = StreamingHasher::new();
        hasher.update(&data[..10]);
        hasher.update(&data[10..]);
        let stream_hash = hasher.finalize();

        assert_eq!(file_hash, stream_hash);
    }

    #[test]
    fn from_filename_sha256_is_none() {
        let info = SnapshotInfo::from_filename("snapshot-42-h.tar.zst", 100, 0).unwrap();
        assert!(info.sha256.is_none());
    }
}
