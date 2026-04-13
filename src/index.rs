use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::SystemTime;

use anyhow::Context;
use inotify::{Inotify, WatchMask};
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

use crate::snapshot::{self, SnapshotInfo, SnapshotType};

/// Thread-safe snapshot index for the local node.
#[derive(Clone)]
pub struct SnapshotIndex {
    dir: PathBuf,
    inner: Arc<RwLock<HashMap<String, SnapshotInfo>>>,
}

impl SnapshotIndex {
    pub fn new(dir: PathBuf) -> Self {
        Self {
            dir,
            inner: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Full scan of the snapshot directory, computing SHA-256 for new/changed files.
    pub async fn scan(&self) -> anyhow::Result<()> {
        let dir = self.dir.clone();
        let previous = self.inner.read().await.clone();
        let entries =
            tokio::task::spawn_blocking(move || scan_directory_with_hashes(&dir, &previous))
                .await??;
        let count = entries.len();
        let mut map = self.inner.write().await;
        *map = entries;
        info!(count, "snapshot index scan complete");
        Ok(())
    }

    /// Return a sorted vec of all local snapshots (full first, then incremental, by slot desc).
    pub async fn list(&self) -> Vec<SnapshotInfo> {
        let map = self.inner.read().await;
        let mut v: Vec<SnapshotInfo> = map.values().cloned().collect();
        v.sort_by(|a, b| {
            a.snapshot_type
                .cmp_full_first(&b.snapshot_type)
                .then(b.slot.cmp(&a.slot))
        });
        v
    }

    /// Highest full snapshot slot, if any.
    pub async fn highest_full_slot(&self) -> Option<u64> {
        let map = self.inner.read().await;
        map.values()
            .filter(|s| s.is_full())
            .map(|s| s.slot)
            .max()
    }

    /// Highest slot of any type (full or incremental with valid base).
    pub async fn highest_slot(&self) -> u64 {
        let map = self.inner.read().await;
        let full_slots: std::collections::HashSet<u64> = map
            .values()
            .filter(|s| s.is_full())
            .map(|s| s.slot)
            .collect();

        map.values()
            .filter(|s| match s.base_slot {
                Some(base) => full_slots.contains(&base),
                None => true,
            })
            .map(|s| s.slot)
            .max()
            .unwrap_or(0)
    }

    pub async fn get(&self, file_name: &str) -> Option<SnapshotInfo> {
        self.inner.read().await.get(file_name).cloned()
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// Watch for filesystem changes and update the index.
    pub async fn watch_loop(&self) -> anyhow::Result<()> {
        let mut ino = Inotify::init().context("inotify init")?;
        ino.watches()
            .add(
                &self.dir,
                WatchMask::CREATE
                    | WatchMask::DELETE
                    | WatchMask::MOVED_TO
                    | WatchMask::MOVED_FROM
                    | WatchMask::CLOSE_WRITE,
            )
            .context("inotify watch")?;

        let mut buf = vec![0u8; 4096];
        loop {
            let events = tokio::task::block_in_place(|| ino.read_events_blocking(&mut buf))?;
            let mut changed = false;
            for event in events {
                if let Some(name) = event.name.and_then(|n| n.to_str()) {
                    if name.ends_with(".tar.zst") && !name.ends_with(".tar.zst.aria2") {
                        debug!(file = name, "inotify event");
                        changed = true;
                    }
                }
            }
            if changed {
                if let Err(e) = self.scan().await {
                    warn!(error = %e, "rescan after inotify event failed");
                }
            }
        }
    }
}

trait SnapshotTypeOrd {
    fn cmp_full_first(&self, other: &Self) -> std::cmp::Ordering;
}

impl SnapshotTypeOrd for SnapshotType {
    fn cmp_full_first(&self, other: &Self) -> std::cmp::Ordering {
        let rank = |t: &SnapshotType| match t {
            SnapshotType::Full => 0,
            SnapshotType::Incremental => 1,
        };
        rank(self).cmp(&rank(other))
    }
}

pub fn scan_directory(dir: &Path) -> anyhow::Result<HashMap<String, SnapshotInfo>> {
    scan_directory_with_hashes(dir, &HashMap::new())
}

fn scan_directory_with_hashes(
    dir: &Path,
    previous: &HashMap<String, SnapshotInfo>,
) -> anyhow::Result<HashMap<String, SnapshotInfo>> {
    let mut map = HashMap::new();
    let entries = std::fs::read_dir(dir).context("read snapshot dir")?;
    for entry in entries {
        let entry = entry?;
        let name = entry.file_name();
        let name_str = match name.to_str() {
            Some(n) => n,
            None => continue,
        };
        if !name_str.ends_with(".tar.zst") {
            continue;
        }
        let aria2_path = entry.path().with_extension("zst.aria2");
        if aria2_path.exists() {
            continue;
        }
        let meta = entry.metadata()?;
        let modified = meta
            .modified()
            .unwrap_or(SystemTime::UNIX_EPOCH)
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        if let Some(mut info) = SnapshotInfo::from_filename(name_str, meta.len(), modified) {
            // Reuse in-memory cached hash if file hasn't changed
            if let Some(prev) = previous.get(name_str) {
                if prev.size == info.size && prev.modified_at == info.modified_at {
                    info.sha256 = prev.sha256.clone();
                }
            }
            // Try loading from sidecar .sha256 file
            if info.sha256.is_none() {
                info.sha256 = load_sidecar_hash(&entry.path());
            }
            // Compute hash if we still don't have one
            if info.sha256.is_none() {
                match snapshot::sha256_file(&entry.path()) {
                    Ok(hash) => {
                        debug!(file = name_str, hash = hash, "computed sha256");
                        save_sidecar_hash(&entry.path(), &hash);
                        info.sha256 = Some(hash);
                    }
                    Err(e) => {
                        warn!(file = name_str, error = %e, "failed to compute sha256");
                    }
                }
            }
            map.insert(name_str.to_string(), info);
        }
    }

    cleanup_orphan_sidecars(dir, &map);

    Ok(map)
}

/// Remove `.sha256` sidecar files whose corresponding snapshot no longer exists.
fn cleanup_orphan_sidecars(dir: &Path, live: &HashMap<String, SnapshotInfo>) {
    let entries = match std::fs::read_dir(dir) {
        Ok(e) => e,
        Err(_) => return,
    };
    for entry in entries.flatten() {
        let name = entry.file_name();
        let name_str = match name.to_str() {
            Some(n) => n,
            None => continue,
        };
        if let Some(snapshot_name) = name_str.strip_suffix(".sha256") {
            if !live.contains_key(snapshot_name) {
                debug!(file = name_str, "removing orphan sidecar");
                std::fs::remove_file(entry.path()).ok();
            }
        }
    }
}

/// Read a `.sha256` sidecar file. Returns `Some(hex_hash)` if valid.
fn load_sidecar_hash(snapshot_path: &Path) -> Option<String> {
    let hash_path = sidecar_path(snapshot_path);
    let contents = std::fs::read_to_string(&hash_path).ok()?;
    let hash = contents.trim().to_string();
    if hash.len() == 64 && hash.chars().all(|c| c.is_ascii_hexdigit()) {
        Some(hash)
    } else {
        None
    }
}

/// Write a `.sha256` sidecar file next to the snapshot.
fn save_sidecar_hash(snapshot_path: &Path, hash: &str) {
    let hash_path = sidecar_path(snapshot_path);
    if let Err(e) = std::fs::write(&hash_path, format!("{hash}\n")) {
        warn!(path = %hash_path.display(), error = %e, "failed to write .sha256 sidecar");
    }
}

fn sidecar_path(snapshot_path: &Path) -> PathBuf {
    let mut p = snapshot_path.as_os_str().to_owned();
    p.push(".sha256");
    PathBuf::from(p)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn touch(dir: &Path, name: &str) {
        std::fs::write(dir.join(name), &[0u8; 64]).unwrap();
    }

    #[tokio::test]
    async fn scan_finds_snapshots() {
        let dir = tempfile::tempdir().unwrap();
        touch(dir.path(), "snapshot-100-hash.tar.zst");
        touch(dir.path(), "snapshot-200-hash.tar.zst");
        touch(
            dir.path(),
            "incremental-snapshot-200-300-hash.tar.zst",
        );

        let idx = SnapshotIndex::new(dir.path().to_path_buf());
        idx.scan().await.unwrap();

        let list = idx.list().await;
        assert_eq!(list.len(), 3);
        // Full snapshots come first, sorted by slot desc
        assert!(list[0].is_full());
        assert_eq!(list[0].slot, 200);
        assert!(list[1].is_full());
        assert_eq!(list[1].slot, 100);
        assert!(!list[2].is_full());
        assert_eq!(list[2].slot, 300);
    }

    #[tokio::test]
    async fn highest_slot_considers_base_validity() {
        let dir = tempfile::tempdir().unwrap();
        touch(dir.path(), "snapshot-100-hash.tar.zst");
        // Incremental with valid base
        touch(
            dir.path(),
            "incremental-snapshot-100-200-hash.tar.zst",
        );
        // Orphaned incremental (base 999 doesn't exist)
        touch(
            dir.path(),
            "incremental-snapshot-999-1000-hash.tar.zst",
        );

        let idx = SnapshotIndex::new(dir.path().to_path_buf());
        idx.scan().await.unwrap();

        // Highest valid slot is 200 (the valid incremental), not 1000 (orphan)
        assert_eq!(idx.highest_slot().await, 200);
    }

    #[tokio::test]
    async fn highest_full_slot() {
        let dir = tempfile::tempdir().unwrap();
        touch(dir.path(), "snapshot-100-hash.tar.zst");
        touch(dir.path(), "snapshot-300-hash.tar.zst");
        touch(
            dir.path(),
            "incremental-snapshot-300-400-hash.tar.zst",
        );

        let idx = SnapshotIndex::new(dir.path().to_path_buf());
        idx.scan().await.unwrap();

        assert_eq!(idx.highest_full_slot().await, Some(300));
    }

    #[tokio::test]
    async fn aria2_files_excluded() {
        let dir = tempfile::tempdir().unwrap();
        touch(dir.path(), "snapshot-100-hash.tar.zst");
        touch(dir.path(), "snapshot-200-hash.tar.zst");
        // This has an aria2 control file -> should be excluded
        touch(dir.path(), "snapshot-300-hash.tar.zst");
        touch(dir.path(), "snapshot-300-hash.tar.zst.aria2");

        let idx = SnapshotIndex::new(dir.path().to_path_buf());
        idx.scan().await.unwrap();

        let list = idx.list().await;
        assert_eq!(list.len(), 2);
        assert!(list.iter().all(|s| s.slot != 300));
    }

    #[tokio::test]
    async fn non_snapshot_files_ignored() {
        let dir = tempfile::tempdir().unwrap();
        touch(dir.path(), "snapshot-100-hash.tar.zst");
        touch(dir.path(), "README.md");
        touch(dir.path(), "data.json");
        touch(dir.path(), "random.tar.gz");

        let idx = SnapshotIndex::new(dir.path().to_path_buf());
        idx.scan().await.unwrap();

        assert_eq!(idx.list().await.len(), 1);
    }

    #[tokio::test]
    async fn get_by_filename() {
        let dir = tempfile::tempdir().unwrap();
        let name = "snapshot-42-hash.tar.zst";
        touch(dir.path(), name);

        let idx = SnapshotIndex::new(dir.path().to_path_buf());
        idx.scan().await.unwrap();

        let info = idx.get(name).await.unwrap();
        assert_eq!(info.slot, 42);
        assert!(idx.get("nonexistent.tar.zst").await.is_none());
    }

    #[tokio::test]
    async fn empty_dir() {
        let dir = tempfile::tempdir().unwrap();
        let idx = SnapshotIndex::new(dir.path().to_path_buf());
        idx.scan().await.unwrap();

        assert_eq!(idx.list().await.len(), 0);
        assert_eq!(idx.highest_slot().await, 0);
        assert_eq!(idx.highest_full_slot().await, None);
    }

    #[tokio::test]
    async fn scan_computes_sha256() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("snapshot-100-hash.tar.zst"), b"test data").unwrap();

        let idx = SnapshotIndex::new(dir.path().to_path_buf());
        idx.scan().await.unwrap();

        let snap = idx.get("snapshot-100-hash.tar.zst").await.unwrap();
        assert!(snap.sha256.is_some());
        let hash = snap.sha256.unwrap();
        assert_eq!(hash.len(), 64); // hex-encoded SHA-256
    }

    #[tokio::test]
    async fn scan_caches_sha256_on_rescan() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("snapshot-100-hash.tar.zst"), b"content").unwrap();

        let idx = SnapshotIndex::new(dir.path().to_path_buf());
        idx.scan().await.unwrap();
        let hash1 = idx
            .get("snapshot-100-hash.tar.zst")
            .await
            .unwrap()
            .sha256
            .unwrap();

        // Rescan without changing the file — hash should be reused
        idx.scan().await.unwrap();
        let hash2 = idx
            .get("snapshot-100-hash.tar.zst")
            .await
            .unwrap()
            .sha256
            .unwrap();

        assert_eq!(hash1, hash2);
    }

    #[tokio::test]
    async fn hash_sidecar_survives_restart() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("snapshot-100-hash.tar.zst"), b"persistent").unwrap();

        // First "run" — scan and compute hash, writes .sha256 sidecar
        let idx1 = SnapshotIndex::new(dir.path().to_path_buf());
        idx1.scan().await.unwrap();
        let hash1 = idx1
            .get("snapshot-100-hash.tar.zst")
            .await
            .unwrap()
            .sha256
            .unwrap();

        // Verify sidecar file exists
        assert!(dir
            .path()
            .join("snapshot-100-hash.tar.zst.sha256")
            .exists());
        let sidecar_content = std::fs::read_to_string(
            dir.path().join("snapshot-100-hash.tar.zst.sha256"),
        )
        .unwrap();
        assert_eq!(sidecar_content.trim(), hash1);

        // Second "run" — fresh index, should load hash from sidecar
        let idx2 = SnapshotIndex::new(dir.path().to_path_buf());
        idx2.scan().await.unwrap();
        let hash2 = idx2
            .get("snapshot-100-hash.tar.zst")
            .await
            .unwrap()
            .sha256
            .unwrap();

        assert_eq!(hash1, hash2);
    }

    #[tokio::test]
    async fn orphan_sidecars_cleaned_up() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("snapshot-100-hash.tar.zst"), b"data").unwrap();
        std::fs::write(dir.path().join("snapshot-200-hash.tar.zst"), b"data2").unwrap();

        let idx = SnapshotIndex::new(dir.path().to_path_buf());
        idx.scan().await.unwrap();

        assert!(dir.path().join("snapshot-100-hash.tar.zst.sha256").exists());
        assert!(dir.path().join("snapshot-200-hash.tar.zst.sha256").exists());

        // Simulate the validator removing a snapshot (but leaving the sidecar)
        std::fs::remove_file(dir.path().join("snapshot-200-hash.tar.zst")).unwrap();

        idx.scan().await.unwrap();

        // Sidecar for the removed snapshot should be gone
        assert!(dir.path().join("snapshot-100-hash.tar.zst.sha256").exists());
        assert!(!dir.path().join("snapshot-200-hash.tar.zst.sha256").exists());
    }
}
