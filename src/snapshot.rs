use std::fmt::Write as _;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tracing::warn;

/// File extension of a snapshot archive.
pub const ARCHIVE_SUFFIX: &str = ".tar.zst";

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

    /// Hash component of the filename (`HASH` in `snapshot-SLOT-HASH.tar.zst`).
    pub fn hash_component(&self) -> String {
        hash_component(&self.file_name)
    }
}

/// True when `name` looks like a snapshot archive (by extension only).
pub fn is_archive_name(name: &str) -> bool {
    name.ends_with(ARCHIVE_SUFFIX)
}

/// The slot a snapshot filename boots to (the incremental slot for incrementals).
pub fn primary_slot(file_name: &str) -> Option<u64> {
    SnapshotInfo::from_filename(file_name, 0, 0).map(|i| i.slot)
}

/// Base full slot of an incremental snapshot filename. `None` for full snapshots.
pub fn incremental_base_slot(file_name: &str) -> Option<u64> {
    SnapshotInfo::from_filename(file_name, 0, 0)?.base_slot
}

/// Hash component of a snapshot filename, or `"unknown"` when unparsable.
pub fn hash_component(file_name: &str) -> String {
    let hash = if let Some(rest) = file_name.strip_prefix("incremental-snapshot-") {
        rest.strip_suffix(ARCHIVE_SUFFIX)
            .and_then(|r| r.splitn(3, '-').nth(2))
    } else if let Some(rest) = file_name.strip_prefix("snapshot-") {
        rest.strip_suffix(ARCHIVE_SUFFIX)
            .and_then(|r| r.split_once('-').map(|(_, h)| h))
    } else {
        None
    };
    hash.unwrap_or("unknown").to_string()
}

/// Path of the `.sha256` sidecar next to a snapshot file.
pub fn sidecar_path(snapshot_path: &Path) -> PathBuf {
    let mut p = snapshot_path.as_os_str().to_owned();
    p.push(".sha256");
    PathBuf::from(p)
}

/// Read a `.sha256` sidecar file. Returns `Some(hex_hash)` if valid.
pub fn load_sidecar_hash(snapshot_path: &Path) -> Option<String> {
    let contents = std::fs::read_to_string(sidecar_path(snapshot_path)).ok()?;
    let hash = contents.trim().to_string();
    if hash.len() == 64 && hash.chars().all(|c| c.is_ascii_hexdigit()) {
        Some(hash)
    } else {
        None
    }
}

/// Write a `.sha256` sidecar file next to the snapshot.
pub fn save_sidecar_hash(snapshot_path: &Path, hash: &str) {
    let hash_path = sidecar_path(snapshot_path);
    if let Err(e) = std::fs::write(&hash_path, format!("{hash}\n")) {
        warn!(path = %hash_path.display(), error = %e, "failed to write .sha256 sidecar");
    }
}

/// Remove the `.sha256` sidecar of a snapshot, if present.
pub fn remove_sidecar(snapshot_path: &Path) {
    std::fs::remove_file(sidecar_path(snapshot_path)).ok();
}

/// Path of the marker a partial download leaves next to a snapshot file.
pub fn partial_marker_path(snapshot_path: &Path) -> PathBuf {
    snapshot_path.with_extension("zst.aria2")
}

/// True when `dir/file_name` is a completed snapshot (present, non-empty, no partial marker).
pub async fn file_complete(dir: &Path, file_name: &str) -> bool {
    let path = dir.join(file_name);
    match tokio::fs::metadata(&path).await {
        Ok(m) if m.is_file() && m.len() > 0 => {}
        _ => return false,
    }
    !tokio::fs::try_exists(partial_marker_path(&path))
        .await
        .unwrap_or(false)
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

impl Default for StreamingHasher {
    fn default() -> Self {
        Self::new()
    }
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
        let info =
            SnapshotInfo::from_filename("snapshot-12345-abc-def-ghi.tar.zst", 100, 0).unwrap();
        assert_eq!(info.slot, 12345);
        assert!(info.is_full());
    }

    #[test]
    fn incremental_is_not_full() {
        let info = SnapshotInfo::from_filename("incremental-snapshot-100-200-hash.tar.zst", 100, 0)
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

    #[test]
    fn primary_slot_full() {
        assert_eq!(primary_slot("snapshot-12345-hash.tar.zst"), Some(12345));
    }

    #[test]
    fn primary_slot_incremental() {
        assert_eq!(
            primary_slot("incremental-snapshot-100-200-hash.tar.zst"),
            Some(200)
        );
    }

    #[test]
    fn primary_slot_garbage() {
        assert_eq!(primary_slot("random.tar.zst"), None);
    }

    #[test]
    fn base_slot_incremental() {
        assert_eq!(
            incremental_base_slot("incremental-snapshot-100-200-hash.tar.zst"),
            Some(100)
        );
    }

    #[test]
    fn base_slot_full_is_none() {
        assert_eq!(incremental_base_slot("snapshot-100-hash.tar.zst"), None);
    }

    #[test]
    fn hash_component_full() {
        assert_eq!(hash_component("snapshot-100-abc123.tar.zst"), "abc123");
    }

    #[test]
    fn hash_component_incremental() {
        assert_eq!(
            hash_component("incremental-snapshot-100-200-xyz.tar.zst"),
            "xyz"
        );
    }

    #[test]
    fn hash_component_multi_hyphen() {
        assert_eq!(hash_component("snapshot-100-abc-def.tar.zst"), "abc-def");
    }

    #[test]
    fn hash_component_garbage() {
        assert_eq!(hash_component("garbage.txt"), "unknown");
    }

    #[test]
    fn sidecar_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let snap = dir.path().join("snapshot-1-h.tar.zst");
        std::fs::write(&snap, b"data").unwrap();

        let hash = "a".repeat(64);
        save_sidecar_hash(&snap, &hash);
        assert!(sidecar_path(&snap).exists());
        assert_eq!(load_sidecar_hash(&snap), Some(hash));

        remove_sidecar(&snap);
        assert!(!sidecar_path(&snap).exists());
        assert_eq!(load_sidecar_hash(&snap), None);
    }

    #[test]
    fn sidecar_rejects_invalid_contents() {
        let dir = tempfile::tempdir().unwrap();
        let snap = dir.path().join("snapshot-1-h.tar.zst");
        std::fs::write(sidecar_path(&snap), "not a hash\n").unwrap();
        assert_eq!(load_sidecar_hash(&snap), None);
    }

    #[tokio::test]
    async fn file_complete_states() {
        let dir = tempfile::tempdir().unwrap();
        let name = "snapshot-1-h.tar.zst";

        assert!(!file_complete(dir.path(), name).await);

        std::fs::write(dir.path().join(name), b"").unwrap();
        assert!(!file_complete(dir.path(), name).await, "empty file");

        std::fs::write(dir.path().join(name), b"data").unwrap();
        assert!(file_complete(dir.path(), name).await);

        std::fs::write(partial_marker_path(&dir.path().join(name)), b"x").unwrap();
        assert!(!file_complete(dir.path(), name).await, "partial marker");
    }
}
