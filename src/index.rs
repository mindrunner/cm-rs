use std::collections::HashMap;
use std::future::Future;
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
        debug!(count, "snapshot index scan complete");
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
        map.values().filter(|s| s.is_full()).map(|s| s.slot).max()
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

    /// Staging directory for in-flight downloads, inside the snapshot dir.
    /// Files placed here are invisible to the index until [`Self::admit`]ed.
    pub fn staging_dir(&self) -> PathBuf {
        self.dir.join(".cm-tmp")
    }

    /// Move a staged file into the snapshot dir, write its `.sha256` sidecar,
    /// and add it to the index. The file must already be in [`Self::staging_dir`].
    pub async fn admit(&self, file_name: &str, sha256: &str) -> anyhow::Result<SnapshotInfo> {
        let src = self.staging_dir().join(file_name);
        let dst = self.dir.join(file_name);
        tokio::fs::rename(&src, &dst)
            .await
            .with_context(|| format!("rename {} -> {}", src.display(), dst.display()))?;
        snapshot::save_sidecar_hash(&dst, sha256);

        let meta = tokio::fs::metadata(&dst).await?;
        let modified = meta
            .modified()
            .unwrap_or(SystemTime::UNIX_EPOCH)
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let mut info = SnapshotInfo::from_filename(file_name, meta.len(), modified)
            .with_context(|| format!("admitted file is not a snapshot: {file_name}"))?;
        info.sha256 = Some(sha256.to_string());
        self.inner
            .write()
            .await
            .insert(file_name.to_string(), info.clone());
        Ok(info)
    }

    /// Remove staging entries that can no longer improve the local bootstrap:
    /// snapshot files (and their partial-download markers) at or below
    /// `keep_min_slot`, plus anything unrecognized (chunk files, junk) and
    /// orphaned markers. Fresher partials are kept — together with their
    /// markers — so the download backend can resume them after a restart.
    pub async fn clean_staging(&self, keep_min_slot: u64) -> anyhow::Result<()> {
        let staging = self.staging_dir();
        let mut entries = match tokio::fs::read_dir(&staging).await {
            Ok(e) => e,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(e) => return Err(e.into()),
        };

        let mut names = Vec::new();
        while let Some(entry) = entries.next_entry().await? {
            match entry.file_name().to_str() {
                Some(n) => names.push(n.to_string()),
                None => {
                    tokio::fs::remove_file(entry.path()).await.ok();
                }
            }
        }

        let name_set: std::collections::HashSet<&str> = names.iter().map(|n| n.as_str()).collect();
        for name in &names {
            let is_marker = name.ends_with(".aria2");
            let payload = name.strip_suffix(".aria2").unwrap_or(name);
            let fresh = snapshot::primary_slot(payload).is_some_and(|s| s > keep_min_slot);
            let keep = fresh && (!is_marker || name_set.contains(payload));
            if !keep {
                debug!(file = name, "removing stale staging entry");
                tokio::fs::remove_file(staging.join(name)).await.ok();
            }
        }

        // Only succeeds when nothing was kept.
        let _ = tokio::fs::remove_dir(&staging).await;
        Ok(())
    }

    /// Retention: keep the newest `max_full` full snapshots and the newest
    /// `max_incremental` incrementals whose base full survives; delete the
    /// rest from disk (with their sidecars) and from the index.
    pub async fn retain(&self, max_full: u32, max_incremental: u32) -> anyhow::Result<()> {
        let removed = cleanup_old_snapshots(&self.dir, max_full, max_incremental).await?;
        if !removed.is_empty() {
            let mut map = self.inner.write().await;
            for name in &removed {
                map.remove(name);
            }
        }
        Ok(())
    }

    /// Watch for filesystem changes and update the index.
    pub async fn watch_loop(&self) -> anyhow::Result<()> {
        self.watch_loop_with_rescan(|| async {}).await
    }

    /// Watch until the next snapshot file event, update the index, and run a callback.
    pub async fn watch_once_with_rescan<F, Fut>(&self, mut on_rescan: F) -> anyhow::Result<()>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = ()>,
    {
        let mut ino = self.create_watcher()?;
        self.process_next_snapshot_event(&mut ino, &mut on_rescan)
            .await
    }

    /// Watch for filesystem changes, update the index, and run a callback after successful rescans.
    pub async fn watch_loop_with_rescan<F, Fut>(&self, mut on_rescan: F) -> anyhow::Result<()>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = ()>,
    {
        let mut ino = self.create_watcher()?;

        loop {
            self.process_next_snapshot_event(&mut ino, &mut on_rescan)
                .await?;
        }
    }

    fn create_watcher(&self) -> anyhow::Result<Inotify> {
        let ino = Inotify::init().context("inotify init")?;
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
        Ok(ino)
    }

    async fn process_next_snapshot_event<F, Fut>(
        &self,
        ino: &mut Inotify,
        on_rescan: &mut F,
    ) -> anyhow::Result<()>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = ()>,
    {
        let mut buf = vec![0u8; 4096];
        let events = tokio::task::block_in_place(|| ino.read_events_blocking(&mut buf))?;
        let mut changed = false;
        for event in events {
            if let Some(name) = event.name.and_then(|n| n.to_str())
                && snapshot::is_archive_name(name)
            {
                debug!(file = name, "inotify event");
                changed = true;
            }
        }
        if changed {
            if let Err(e) = self.scan().await {
                warn!(error = %e, "rescan after inotify event failed");
            } else {
                on_rescan().await;
            }
        }
        Ok(())
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
        let entry = match entry {
            Ok(e) => e,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
            Err(e) => return Err(e.into()),
        };
        let name = entry.file_name();
        let name_str = match name.to_str() {
            Some(n) => n,
            None => continue,
        };
        if !snapshot::is_archive_name(name_str) {
            continue;
        }
        if snapshot::partial_marker_path(&entry.path()).exists() {
            continue;
        }
        let meta = match entry.metadata() {
            Ok(m) => m,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                debug!(file = name_str, "file vanished during scan");
                continue;
            }
            Err(e) => return Err(e.into()),
        };
        let modified = meta
            .modified()
            .unwrap_or(SystemTime::UNIX_EPOCH)
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        if let Some(mut info) = SnapshotInfo::from_filename(name_str, meta.len(), modified) {
            // Reuse in-memory cached hash if file hasn't changed
            if let Some(prev) = previous.get(name_str)
                && prev.size == info.size
                && prev.modified_at == info.modified_at
            {
                info.sha256 = prev.sha256.clone();
            }
            // Try loading from sidecar .sha256 file
            if info.sha256.is_none() {
                info.sha256 = snapshot::load_sidecar_hash(&entry.path());
            }
            // Compute hash if we still don't have one
            if info.sha256.is_none() {
                match snapshot::sha256_file(&entry.path()) {
                    Ok(hash) => {
                        debug!(file = name_str, hash = hash, "computed sha256");
                        snapshot::save_sidecar_hash(&entry.path(), &hash);
                        info.sha256 = Some(hash);
                    }
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                        debug!(file = name_str, "file vanished during hash");
                        continue;
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

/// Delete snapshots beyond the retention counts. Returns removed file names.
async fn cleanup_old_snapshots(
    dir: &Path,
    max_full: u32,
    max_incremental: u32,
) -> anyhow::Result<Vec<String>> {
    let mut fulls = list_snapshots_by_type(dir, "snapshot-").await?;
    let mut incrementals = list_snapshots_by_type(dir, "incremental-snapshot-").await?;

    // Sort newest first by slot extracted from filename
    fulls.sort_by_key(|(_, slot)| std::cmp::Reverse(*slot));
    incrementals.sort_by_key(|(_, slot)| std::cmp::Reverse(*slot));

    let valid_full_slots: std::collections::HashSet<u64> = fulls
        .iter()
        .take(max_full as usize)
        .map(|(_, slot)| *slot)
        .collect();

    let mut removed = Vec::new();
    let mut remove = |path: &Path, reason: &str| {
        info!(file = %path.display(), "removing {reason}");
        std::fs::remove_file(path).ok();
        snapshot::remove_sidecar(path);
        if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
            removed.push(name.to_string());
        }
    };

    // Remove excess full snapshots
    for (path, _) in fulls.iter().skip(max_full as usize) {
        remove(path, "old full snapshot");
    }

    // Remove orphaned incrementals (base slot not in kept fulls)
    let mut kept_incrementals = 0u32;
    for (path, _) in &incrementals {
        let fname = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
        if let Some(base_slot) = snapshot::incremental_base_slot(fname)
            && !valid_full_slots.contains(&base_slot)
        {
            remove(path, "orphaned incremental snapshot");
            continue;
        }
        kept_incrementals += 1;
        if kept_incrementals > max_incremental {
            remove(path, "old incremental snapshot");
        }
    }

    Ok(removed)
}

async fn list_snapshots_by_type(dir: &Path, prefix: &str) -> anyhow::Result<Vec<(PathBuf, u64)>> {
    let mut result = Vec::new();
    let mut entries = tokio::fs::read_dir(dir).await?;
    while let Some(entry) = entries.next_entry().await? {
        let name = entry.file_name();
        let name_str = match name.to_str() {
            Some(n) => n,
            None => continue,
        };
        if !name_str.starts_with(prefix) || !snapshot::is_archive_name(name_str) {
            continue;
        }
        // Skip incomplete downloads
        if snapshot::partial_marker_path(&entry.path()).exists() {
            continue;
        }
        let slot = snapshot::primary_slot(name_str).unwrap_or(0);
        result.push((entry.path(), slot));
    }
    Ok(result)
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
        if let Some(snapshot_name) = name_str.strip_suffix(".sha256")
            && !live.contains_key(snapshot_name)
        {
            debug!(file = name_str, "removing orphan sidecar");
            std::fs::remove_file(entry.path()).ok();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn touch(dir: &Path, name: &str) {
        std::fs::write(dir.join(name), [0u8; 64]).unwrap();
    }

    #[tokio::test]
    async fn scan_finds_snapshots() {
        let dir = tempfile::tempdir().unwrap();
        touch(dir.path(), "snapshot-100-hash.tar.zst");
        touch(dir.path(), "snapshot-200-hash.tar.zst");
        touch(dir.path(), "incremental-snapshot-200-300-hash.tar.zst");

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
        touch(dir.path(), "incremental-snapshot-100-200-hash.tar.zst");
        // Orphaned incremental (base 999 doesn't exist)
        touch(dir.path(), "incremental-snapshot-999-1000-hash.tar.zst");

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
        touch(dir.path(), "incremental-snapshot-300-400-hash.tar.zst");

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
        assert!(dir.path().join("snapshot-100-hash.tar.zst.sha256").exists());
        let sidecar_content =
            std::fs::read_to_string(dir.path().join("snapshot-100-hash.tar.zst.sha256")).unwrap();
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
    async fn admit_moves_staged_file_and_indexes_it() {
        let dir = tempfile::tempdir().unwrap();
        let idx = SnapshotIndex::new(dir.path().to_path_buf());
        idx.scan().await.unwrap();

        std::fs::create_dir_all(idx.staging_dir()).unwrap();
        std::fs::write(idx.staging_dir().join("snapshot-100-h.tar.zst"), b"data").unwrap();

        let hash = "b".repeat(64);
        let info = idx.admit("snapshot-100-h.tar.zst", &hash).await.unwrap();
        assert_eq!(info.slot, 100);
        assert_eq!(info.sha256.as_deref(), Some(hash.as_str()));

        // File moved out of staging, sidecar written, index updated without a scan
        assert!(!idx.staging_dir().join("snapshot-100-h.tar.zst").exists());
        assert!(dir.path().join("snapshot-100-h.tar.zst").exists());
        assert!(dir.path().join("snapshot-100-h.tar.zst.sha256").exists());
        assert_eq!(idx.highest_full_slot().await, Some(100));
    }

    #[tokio::test]
    async fn admit_rejects_non_snapshot_names() {
        let dir = tempfile::tempdir().unwrap();
        let idx = SnapshotIndex::new(dir.path().to_path_buf());
        std::fs::create_dir_all(idx.staging_dir()).unwrap();
        std::fs::write(idx.staging_dir().join("garbage.tar.zst"), b"data").unwrap();

        assert!(idx.admit("garbage.tar.zst", &"c".repeat(64)).await.is_err());
    }

    #[tokio::test]
    async fn retain_removes_excess_full() {
        let dir = tempfile::tempdir().unwrap();
        touch(dir.path(), "snapshot-100-a.tar.zst");
        touch(dir.path(), "snapshot-200-b.tar.zst");
        touch(dir.path(), "snapshot-300-c.tar.zst");

        let idx = SnapshotIndex::new(dir.path().to_path_buf());
        idx.scan().await.unwrap();
        idx.retain(2, 4).await.unwrap();

        assert!(dir.path().join("snapshot-300-c.tar.zst").exists());
        assert!(dir.path().join("snapshot-200-b.tar.zst").exists());
        assert!(!dir.path().join("snapshot-100-a.tar.zst").exists());
        // Index pruned without rescan
        assert_eq!(idx.list().await.len(), 2);
    }

    #[tokio::test]
    async fn retain_removes_orphaned_incrementals() {
        let dir = tempfile::tempdir().unwrap();
        touch(dir.path(), "snapshot-200-a.tar.zst");
        touch(dir.path(), "incremental-snapshot-200-250-hash.tar.zst");
        // Orphaned: base slot 100 has no full snapshot
        touch(dir.path(), "incremental-snapshot-100-150-hash.tar.zst");

        let idx = SnapshotIndex::new(dir.path().to_path_buf());
        idx.scan().await.unwrap();
        idx.retain(2, 4).await.unwrap();

        assert!(
            dir.path()
                .join("incremental-snapshot-200-250-hash.tar.zst")
                .exists()
        );
        assert!(
            !dir.path()
                .join("incremental-snapshot-100-150-hash.tar.zst")
                .exists()
        );
    }

    #[tokio::test]
    async fn retain_removes_excess_incrementals() {
        let dir = tempfile::tempdir().unwrap();
        touch(dir.path(), "snapshot-100-a.tar.zst");
        touch(dir.path(), "incremental-snapshot-100-200-a.tar.zst");
        touch(dir.path(), "incremental-snapshot-100-300-b.tar.zst");
        touch(dir.path(), "incremental-snapshot-100-400-c.tar.zst");

        let idx = SnapshotIndex::new(dir.path().to_path_buf());
        idx.scan().await.unwrap();
        idx.retain(2, 2).await.unwrap();

        // Should keep 2 newest incrementals (400, 300) and remove 200
        assert!(
            dir.path()
                .join("incremental-snapshot-100-400-c.tar.zst")
                .exists()
        );
        assert!(
            dir.path()
                .join("incremental-snapshot-100-300-b.tar.zst")
                .exists()
        );
        assert!(
            !dir.path()
                .join("incremental-snapshot-100-200-a.tar.zst")
                .exists()
        );
    }

    #[tokio::test]
    async fn clean_staging_removes_stale_and_junk() {
        let dir = tempfile::tempdir().unwrap();
        let idx = SnapshotIndex::new(dir.path().to_path_buf());
        let staging = idx.staging_dir();
        std::fs::create_dir_all(&staging).unwrap();
        std::fs::write(staging.join("snapshot-100-h.tar.zst"), b"stale").unwrap();
        std::fs::write(staging.join("garbage.bin"), b"junk").unwrap();
        std::fs::write(staging.join(".cm-chunk-snapshot-300-h.tar.zst-0"), b"chunk").unwrap();

        idx.clean_staging(200).await.unwrap();

        // Everything removed, empty staging dir removed too
        assert!(!staging.exists());
    }

    #[tokio::test]
    async fn clean_staging_keeps_fresh_resumable_partials() {
        let dir = tempfile::tempdir().unwrap();
        let idx = SnapshotIndex::new(dir.path().to_path_buf());
        let staging = idx.staging_dir();
        std::fs::create_dir_all(&staging).unwrap();
        std::fs::write(staging.join("snapshot-300-h.tar.zst"), b"partial").unwrap();
        std::fs::write(staging.join("snapshot-300-h.tar.zst.aria2"), b"ctrl").unwrap();
        std::fs::write(staging.join("snapshot-100-h.tar.zst"), b"stale").unwrap();

        idx.clean_staging(200).await.unwrap();

        // Fresh payload and its resume marker survive; stale one is gone
        assert!(staging.join("snapshot-300-h.tar.zst").exists());
        assert!(staging.join("snapshot-300-h.tar.zst.aria2").exists());
        assert!(!staging.join("snapshot-100-h.tar.zst").exists());
    }

    #[tokio::test]
    async fn clean_staging_removes_orphan_markers() {
        let dir = tempfile::tempdir().unwrap();
        let idx = SnapshotIndex::new(dir.path().to_path_buf());
        let staging = idx.staging_dir();
        std::fs::create_dir_all(&staging).unwrap();
        // Marker without its payload — even though the slot is fresh
        std::fs::write(staging.join("snapshot-300-h.tar.zst.aria2"), b"ctrl").unwrap();

        idx.clean_staging(200).await.unwrap();
        assert!(!staging.exists());
    }

    #[tokio::test]
    async fn clean_staging_without_staging_dir_is_ok() {
        let dir = tempfile::tempdir().unwrap();
        let idx = SnapshotIndex::new(dir.path().to_path_buf());
        idx.clean_staging(0).await.unwrap();
    }

    #[tokio::test]
    async fn retain_removes_sidecars_of_deleted_snapshots() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("snapshot-100-a.tar.zst"), b"x").unwrap();
        std::fs::write(dir.path().join("snapshot-200-b.tar.zst"), b"y").unwrap();
        std::fs::write(dir.path().join("snapshot-300-c.tar.zst"), b"z").unwrap();

        let idx = SnapshotIndex::new(dir.path().to_path_buf());
        idx.scan().await.unwrap();
        assert!(dir.path().join("snapshot-100-a.tar.zst.sha256").exists());

        idx.retain(2, 4).await.unwrap();
        assert!(!dir.path().join("snapshot-100-a.tar.zst.sha256").exists());
        assert!(dir.path().join("snapshot-300-c.tar.zst.sha256").exists());
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
