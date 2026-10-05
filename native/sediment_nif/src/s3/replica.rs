//! Read-only replicas: a local copy restored from the current manifest
//! without taking the writer lease or writing anything to S3.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use super::config::S3Config;
use super::error::{Result, S3Error};
use super::layout::{parse_segment_key, Epoch, Manifest, SnapshotLink};
use super::logfmt::{verify_segments_from, LogState};
use super::remote::Remote;
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

/// Whether a generation another connection of this process restored may
/// still be handed out: the manifest is a database's (not destroyed, and not
/// a new one in its place) that still retains that epoch.
pub fn still_usable(cfg: &S3Config, state: &ReplicaState) -> Result<bool> {
    let Some(epoch) = super::layout::Epoch::parse(&state.epoch) else {
        return Ok(false);
    };
    restore::still_retained(&cfg.remote()?, epoch)
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

/// The current epoch's log as a replica's last refresh verified it, kept so
/// the next refresh of the same epoch downloads only the log objects added
/// since (instead of the whole epoch's log again). Per replica path, in this
/// process; the bytes are in `file`, `state` is where verification stopped.
struct LogCache {
    scope: String,
    epoch: Epoch,
    snapshot: String,
    state: LogState,
    file: PathBuf,
}

static LOG_CACHES: Mutex<BTreeMap<PathBuf, LogCache>> = Mutex::new(BTreeMap::new());

fn log_cache_path(db_path: &Path) -> PathBuf {
    let name = db_path
        .file_name()
        .map(|s| s.to_string_lossy().into_owned())
        .unwrap_or_default();
    db_path.with_file_name(format!(".{name}.s3-logcache"))
}

fn forget_log_cache(db_path: &Path) {
    LOG_CACHES
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .remove(db_path);
    let _ = restore::remove_if_exists(&log_cache_path(db_path));
}

/// Keeps the log just restored into `staging` as `db_path`'s log cache.
fn store_log_cache(
    db_path: &Path,
    scope: &str,
    manifest: &Manifest,
    staging: &Path,
    state: LogState,
) {
    let file = log_cache_path(db_path);
    let tmp = unique_path(db_path, "s3-logcache-tmp");
    let copied =
        std::fs::copy(restore::log_path(staging), &tmp).and_then(|_| std::fs::rename(&tmp, &file));
    if copied.is_err() {
        let _ = std::fs::remove_file(&tmp);
        forget_log_cache(db_path);
        return;
    }
    LOG_CACHES.lock().unwrap_or_else(|e| e.into_inner()).insert(
        db_path.to_path_buf(),
        LogCache {
            scope: scope.to_string(),
            epoch: manifest.epoch,
            snapshot: manifest.snapshot.clone(),
            state,
            file,
        },
    );
}

/// What a refresh in the same epoch adds to the log cache, once its
/// manifest proved current (see [`restore::still_retained`]).
struct LogCacheUpdate {
    segments: Vec<(u64, bytes::Bytes)>,
    state: LogState,
}

/// A refresh of the epoch the last one restored: the snapshot from the
/// cache, the log from the log cache plus only the log objects added since,
/// verified as a continuation of what was verified before. `None` when that
/// doesn't apply (another epoch or snapshot, no cache); then the caller
/// does a full restore.
fn refresh_incrementally(
    remote: &Remote,
    manifest: &Manifest,
    db_path: &Path,
    scope: &str,
    chain: &[SnapshotLink],
    staging: &Path,
    concurrency: usize,
) -> Result<Option<LogCacheUpdate>> {
    let (start, file) = {
        let caches = LOG_CACHES.lock().unwrap_or_else(|e| e.into_inner());
        match caches.get(db_path) {
            Some(c)
                if c.scope == scope
                    && c.epoch == manifest.epoch
                    && c.snapshot == manifest.snapshot =>
            {
                (c.state, c.file.clone())
            }
            _ => return Ok(None),
        }
    };
    let Some((links, snapshot)) =
        cached_start(db_path, scope, chain).filter(|(n, _)| *n == chain.len())
    else {
        return Ok(None);
    };
    if start.len == 0 || start.sealed {
        return Ok(None);
    }
    // The cached log must still be exactly what was verified: a damaged
    // frame would otherwise end the replayed log early, behind `start`.
    let cached = bytes::Bytes::from(std::fs::read(&file)?);
    let verified = verify_segments_from(
        LogState::default(),
        &[(0, cached.clone())],
        remote.log_encryption(),
    )?;
    if verified != start {
        return Err(S3Error::Corrupt("the replica's log cache changed".into()));
    }
    // Every segment before `start.len` starts below it, so its key sorts
    // before this one; the new segments start at or after `start.len`.
    let epoch = manifest.epoch;
    let mut listed: Vec<(u64, String)> = remote
        .list_after(&epoch.log_dir(), &epoch.segment_key(start.len - 1))?
        .into_iter()
        .map(|object| match parse_segment_key(&object.key) {
            Some((found, offset)) if found == epoch => Ok((offset, object.key)),
            _ => Err(S3Error::Corrupt(format!(
                "unexpected object {}",
                object.key
            ))),
        })
        .collect::<Result<_>>()?;
    listed.retain(|(offset, _)| *offset >= start.len);
    listed.sort();
    let bodies = remote.get_many(
        listed.iter().map(|(_, key)| key.clone()).collect(),
        concurrency,
    )?;
    let segments: Vec<(u64, bytes::Bytes)> = listed
        .into_iter()
        .map(|(offset, _)| offset)
        .zip(bodies)
        .collect();
    let state = verify_segments_from(start, &segments, remote.log_encryption())?;
    restore::remove_if_exists(&restore::wal_path(staging))?;
    remote.download_chain_from(chain, staging, Some((links, &snapshot)))?;
    let log = restore::log_path(staging);
    restore::write_log(&log, &[(0, cached)], false)?;
    restore::write_log(&log, &segments, true)?;
    Ok(Some(LogCacheUpdate { segments, state }))
}

/// Appends what a refresh added to the log cache.
fn update_log_cache(db_path: &Path, update: &LogCacheUpdate) {
    let mut caches = LOG_CACHES.lock().unwrap_or_else(|e| e.into_inner());
    let Some(cache) = caches.get_mut(db_path) else {
        return;
    };
    match restore::write_log(&cache.file, &update.segments, true) {
        Ok(()) => cache.state = update.state,
        // A partial append leaves the file longer than `state`: the next
        // refresh finds that and restores in full.
        Err(err) => tracing::warn!("s3 replica: updating the log cache failed: {err}"),
    }
}

fn moved_on() -> S3Error {
    S3Error::Timeout(
        "s3: the writer moved to a new epoch, or another writer took over, during every \
         attempt; try again"
            .into(),
    )
}

fn staging_path(db_path: &Path) -> PathBuf {
    unique_path(db_path, "s3-staging")
}

fn replica_state(manifest: &Manifest, log: LogState) -> ReplicaState {
    ReplicaState {
        epoch: manifest.epoch.to_string(),
        snapshot: manifest.snapshot.clone(),
        log_bytes: log.len,
        writer: manifest.writer.clone(),
        restored_at_ms: super::layout::now_ms(),
    }
}

/// Downloads the current state of `cfg`'s database into a staging file next
/// to `db_path`, verified like a writer's restore.
///
/// A replica only shows a log that is part of the database: during a
/// takeover the old writer's late upload can land in the old epoch after the
/// new writer listed it, and that commit never becomes part of the database.
/// So the log must come from a manifest its epoch's writer wrote, or end at
/// the takeover's seal ([`restore::usable`]), and no takeover may have begun
/// while it was read ([`restore::still_retained`]).
pub fn stage(cfg: &S3Config, db_path: &Path) -> Result<Staged> {
    stage_at(cfg, db_path).map_err(|err| err.at(db_path))
}

fn stage_at(cfg: &S3Config, db_path: &Path) -> Result<Staged> {
    let remote = cfg.remote()?;
    let staging = staging_path(db_path);
    let scope = format!("{}|{}", cfg.store_identity(), cfg.prefix);
    let mut last_error = None;
    for _ in 0..ATTEMPTS {
        let manifest = restore::read_manifest(&remote)?.ok_or_else(|| {
            S3Error::Config(format!("no database at s3://{}/{}", cfg.bucket, cfg.prefix))
        })?;
        super::require_encryption_choice_for(cfg, Some(&manifest))?;
        super::check_encryption(&manifest, cfg)?;
        let record = manifest.current();
        let chain = record.chain();
        let cacheable = !record.snapshot_base.is_empty() || record.snapshot_zstd;
        if cacheable {
            match refresh_incrementally(
                &remote,
                &manifest,
                db_path,
                &scope,
                &chain,
                &staging,
                cfg.download_concurrency,
            ) {
                Ok(Some(update)) => {
                    let staged = Staged {
                        db: staging.clone(),
                        state: replica_state(&manifest, update.state),
                    };
                    if !restore::usable(&manifest, &update.state) {
                        return Err(restore::taking_over(&manifest));
                    }
                    if !restore::still_retained(&remote, manifest.epoch)? {
                        last_error = Some(moved_on());
                        continue;
                    }
                    update_log_cache(db_path, &update);
                    return Ok(staged);
                }
                Ok(None) => {}
                // Anything unexpected (an object collected meanwhile, a gap,
                // a broken chain, a damaged cache): drop the cache and
                // restore in full.
                Err(err) => {
                    tracing::debug!(
                        "s3 replica: incremental refresh fell back to a full restore: {err}"
                    );
                    forget_log_cache(db_path);
                }
            }
        }
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
                let staged = Staged {
                    db: staging.clone(),
                    state: replica_state(&manifest, log),
                };
                if !restore::usable(&manifest, &log) {
                    return Err(restore::taking_over(&manifest));
                }
                if !restore::still_retained(&remote, manifest.epoch)? {
                    last_error = Some(moved_on());
                    continue;
                }
                if cacheable {
                    match store_cache(db_path, &scope, &chain, &staging) {
                        Ok(()) => store_log_cache(db_path, &scope, &manifest, &staging, log),
                        Err(err) => {
                            tracing::warn!("s3 replica: keeping the snapshot cache failed: {err}");
                            forget_log_cache(db_path);
                        }
                    }
                }
                return Ok(staged);
            }
            // The writer moved to a new epoch and collected the old one
            // while we were downloading it: start over from the new manifest.
            Err(err @ (S3Error::Corrupt(_) | S3Error::Store(_))) => last_error = Some(err),
            Err(err) => return Err(err),
        }
    }
    Err(last_error.unwrap_or_else(|| S3Error::Corrupt("replica restore failed".into())))
}
