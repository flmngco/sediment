//! Read-only replicas: a local copy restored from the current manifest
//! without taking the writer lease or writing anything to S3.

use std::path::{Path, PathBuf};

use super::config::S3Config;
use super::error::{Result, S3Error};
use super::layout::{Manifest, SnapshotLink, MANIFEST_KEY};
use super::restore;

/// How often a restore is retried when the writer garbage collects the
/// epoch being downloaded.
const ATTEMPTS: usize = 3;

/// What a replica was restored from.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReplicaState {
    pub epoch: String,
    pub snapshot: String,
    pub log_bytes: u64,
    pub writer: String,
    pub restored_at_ms: u64,
}

/// A restore downloaded next to the database, ready to be moved in place.
pub struct Staged {
    db: PathBuf,
    pub state: ReplicaState,
}

impl Staged {
    /// Moves the staged files over `db_path`. The database at `db_path`
    /// must not be open in this process anymore.
    pub fn install(self, db_path: &Path) -> Result<ReplicaState> {
        restore::remove_if_exists(&restore::wal_path(db_path))?;
        std::fs::rename(restore::log_path(&self.db), restore::log_path(db_path))?;
        std::fs::rename(&self.db, db_path)?;
        Ok(self.state.clone())
    }
}

impl Drop for Staged {
    fn drop(&mut self) {
        let _ = restore::remove_if_exists(&restore::log_path(&self.db));
        let _ = restore::remove_if_exists(&self.db);
    }
}

/// A path next to `db_path` that no other restore, refresh or connection in
/// this process uses: `.<stem>.<tag>-<pid>-<n>.db`.
pub fn unique_path(db_path: &Path, tag: &str) -> PathBuf {
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let n = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let name = db_path
        .file_stem()
        .map(|s| s.to_string_lossy().into_owned())
        .unwrap_or_default();
    db_path.with_file_name(format!(".{name}.{tag}-{}-{n}.db", std::process::id()))
}

/// The rebuilt snapshot a refresh restored, kept next to the replica so the
/// next refresh downloads only newer deltas (plus the log):
/// `.<file name>.s3-cache-<hash>`, the hash covering the store, the prefix
/// and the chain's last key (which names the whole chain). The rebuilt file
/// is checked against the manifest anyway.
fn cache_path(db_path: &Path, scope: &str, last_key: &str) -> PathBuf {
    let name = db_path
        .file_name()
        .map(|s| s.to_string_lossy().into_owned())
        .unwrap_or_default();
    let hash = twox_hash::XxHash3_128::oneshot(format!("{scope}|{last_key}").as_bytes());
    db_path.with_file_name(format!(".{name}.s3-cache-{hash:032x}"))
}

/// The longest prefix of `chain` a cache holds: `(links, file)`.
fn cached_start(db_path: &Path, scope: &str, chain: &[SnapshotLink]) -> Option<(usize, PathBuf)> {
    (1..=chain.len())
        .rev()
        .map(|n| (n, cache_path(db_path, scope, &chain[n - 1].key)))
        .find(|(_, path)| path.exists())
}

/// Keeps `staged` (the rebuilt snapshot, before the log is replayed into it)
/// as the cache for `chain`, and drops older caches of this replica.
fn store_cache(db_path: &Path, scope: &str, chain: &[SnapshotLink], staged: &Path) -> Result<()> {
    let last = chain.last().expect("chain is not empty");
    let target = cache_path(db_path, scope, &last.key);
    if !target.exists() {
        let tmp = unique_path(db_path, "s3-cache-tmp");
        let copied = std::fs::copy(staged, &tmp).and_then(|_| std::fs::rename(&tmp, &target));
        if copied.is_err() {
            let _ = std::fs::remove_file(&tmp);
        }
        copied?;
    }
    let prefix = target
        .file_name()
        .and_then(|n| n.to_str())
        .and_then(|n| n.split_once(".s3-cache-"))
        .map(|(stem, _)| format!("{stem}.s3-cache-"))
        .unwrap_or_default();
    if let Some(dir) = target.parent() {
        for entry in std::fs::read_dir(dir)?.flatten() {
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if name.starts_with(&prefix) && entry.path() != target {
                let _ = std::fs::remove_file(entry.path());
            }
        }
    }
    Ok(())
}

fn staging_path(db_path: &Path) -> PathBuf {
    unique_path(db_path, "s3-staging")
}

/// Downloads the current state of `cfg`'s database into a staging file next
/// to `db_path`, verified like a writer's restore.
pub fn stage(cfg: &S3Config, db_path: &Path) -> Result<Staged> {
    let remote = cfg.remote()?;
    let staging = staging_path(db_path);
    let mut last_error = None;
    for _ in 0..ATTEMPTS {
        let object = remote.get(MANIFEST_KEY)?.ok_or_else(|| {
            S3Error::Config(format!("no database at s3://{}/{}", cfg.bucket, cfg.prefix))
        })?;
        let manifest = Manifest::decode(&object.bytes)?;
        super::require_encryption_choice_for(cfg, Some(&manifest))?;
        super::check_encryption(&manifest, cfg)?;
        let record = manifest.current();
        let chain = record.chain();
        let cacheable = !record.snapshot_base.is_empty() || record.snapshot_zstd;
        let scope = format!("{}|{}", cfg.store_identity(), cfg.prefix);
        let start = cacheable
            .then(|| cached_start(db_path, &scope, &chain))
            .flatten();
        let restored = match start {
            Some((n, ref cache)) => restore::restore_from(
                &remote,
                &manifest,
                &staging,
                cfg.download_concurrency,
                Some((n, cache)),
            )
            // Whatever went wrong with the cache, a full download can't be worse.
            .or_else(|_| restore::restore(&remote, &manifest, &staging, cfg.download_concurrency)),
            None => restore::restore(&remote, &manifest, &staging, cfg.download_concurrency),
        };
        match restored.map_err(|err| super::missing_key_hint(err, cfg)) {
            Ok(log) => {
                if cacheable {
                    if let Err(err) = store_cache(db_path, &scope, &chain, &staging) {
                        tracing::warn!("s3 replica: keeping the snapshot cache failed: {err}");
                    }
                }
                let state = ReplicaState {
                    epoch: manifest.epoch.to_string(),
                    snapshot: manifest.snapshot.clone(),
                    log_bytes: log.len,
                    writer: manifest.writer.clone(),
                    restored_at_ms: super::layout::now_ms(),
                };
                return Ok(Staged { db: staging, state });
            }
            // The writer moved to a new epoch and collected the old one
            // while we were downloading it: start over from the new manifest.
            Err(err @ (S3Error::Corrupt(_) | S3Error::Store(_))) => last_error = Some(err),
            Err(err) => return Err(err),
        }
    }
    Err(last_error.unwrap_or_else(|| S3Error::Corrupt("replica restore failed".into())))
}
