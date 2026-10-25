//! S3-backed durability for MVCC databases.
//!
//! See `docs/s3-durability.md` for the design. Entry point: [`prepare`],
//! which restores (or creates) the database at `local_path` from S3, takes
//! the writer lease, and returns the storage to attach with
//! `OpenOptions::durable_storage`.

mod census;
mod config;
mod destroy;
mod error;
pub mod hook;
pub(crate) mod import;
pub mod keys;
mod layout;
mod lease;
mod logfmt;
pub mod meter;
mod probe;
mod rekey;
pub mod remote;
pub mod replica;
pub mod restore;
mod snapshot;
mod storage;
pub(crate) mod warm;

#[cfg(test)]
pub(crate) mod tests;

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, Weak};

use turso_core::mvcc::persistent_storage::Storage;
use turso_core::{Database, OpenFlags, OpenOptions, PlatformIO, SqliteDialect, IO};

pub use census::{key_census, Census};
pub use config::S3Config;
pub use destroy::{destroy, Destroyed};
pub use error::{Result, S3Error};
pub use import::{import, ImportOptions, Imported, Verify};
pub use keys::{short_fingerprint, shown, shown_value, Key, KeyRecord, KeyRing};
pub use rekey::{rekey, release_key, Rekeyed, Released};
pub use restore::Target;
use snapshot::Digests;
pub use storage::Loss;
pub use storage::{take_last_enqueued, Attached, S3DurableStorage, S3Info};

use layout::{now_ms, Epoch, Manifest, ManifestState, Tombstone, MANIFEST_KEY, MANIFEST_VERSION};
use lease::Lease;
use remote::Put;

/// Restores `local_path` from S3 (or creates a new database there and in S3),
/// takes the writer lease, and returns the storage to open it with.
///
/// When the prefix holds a database, any local files at `local_path` are
/// replaced by the S3 state. When it doesn't, a local database with tables
/// is refused rather than replaced (a new database starts empty). The caller
/// must open the database with `OpenOptions::durable_storage` using the
/// returned storage; it is in MVCC journal mode.
pub fn prepare(cfg: &S3Config, local_path: &Path) -> Result<Arc<S3DurableStorage>> {
    let io: Arc<dyn IO> = Arc::new(PlatformIO::new()?);
    prepare_with_io(cfg, local_path, io)
}

/// Like [`prepare`], using `io` for the local logical-log file.
pub fn prepare_with_io(
    cfg: &S3Config,
    local_path: &Path,
    io: Arc<dyn IO>,
) -> Result<Arc<S3DurableStorage>> {
    prepare_attached(cfg, local_path, io).map(Attached::into_storage)
}

/// Like [`prepare_with_io`], counting the open as one of the storage's
/// connections from here on (a connection keeps the guard until it starts
/// closing; an open that fails drops it).
///
/// Within one process, opening the same path again while its storage is
/// alive returns that storage (turso shares one `Database` per path too), so
/// pooled connections don't restore over each other. Concurrent calls for
/// one file must be serialized by the caller (the NIF's per-file open lock):
/// the restore runs without any process-wide lock.
pub fn prepare_attached(cfg: &S3Config, local_path: &Path, io: Arc<dyn IO>) -> Result<Attached> {
    prepare_at(cfg, local_path, io).map_err(|err| err.at(local_path))
}

fn prepare_at(cfg: &S3Config, local_path: &Path, io: Arc<dyn IO>) -> Result<Attached> {
    refuse_symlink(local_path)?;
    cfg.validate()?;
    if let Some(attached) = attach_live(local_path)? {
        let storage = attached.storage();
        // Before the settings: they name the cipher only, and this open
        // would share the storage's decrypted state.
        check_same_encryption(storage.encryption(), cfg)?;
        if storage.settings() != cfg.settings() {
            return Err(S3Error::Config(format!(
                "{} is already open in this VM with other S3 settings ({}); \
                 every connection to a path must use the same :s3 options (got {})",
                local_path.display(),
                storage.settings(),
                cfg.settings()
            )));
        }
        if let Some(reason) = storage.info().poisoned {
            return Err(S3Error::Fenced(format!(
                "{reason}; close every connection to {} and open it again",
                local_path.display()
            )));
        }
        return Ok(attached);
    }
    // Not holding LIVE across the restore (network I/O, possibly minutes):
    // opens of other files look there too. The caller serializes opens of
    // one file (the NIF's per-file open lock), so no other restore of it
    // runs now.
    let Fresh::Opened { storage, .. } = prepare_fresh(cfg, local_path, io, &Op::Open)? else {
        unreachable!("an open always opens")
    };
    let attached = Attached::try_new(&storage).expect("a new storage isn't closing");
    // The restore put a new file in place: register its identity.
    LIVE.lock()
        .unwrap_or_else(|e| e.into_inner())
        .insert(canonical_key(local_path)?, Live::of(&storage));
    Ok(attached)
}

/// A storage in this process, from its open until its Drop has finished.
struct Live {
    storage: Weak<S3DurableStorage>,
    gone: Arc<AtomicBool>,
    place: String,
    close_timeout: std::time::Duration,
}

impl Live {
    fn of(storage: &Arc<S3DurableStorage>) -> Self {
        Live {
            storage: Arc::downgrade(storage),
            gone: storage.gone(),
            place: storage.place().to_string(),
            close_timeout: storage.close_timeout(),
        }
    }

    /// Its state. The storage is handed out, never dropped here: the caller
    /// drops it after the LIVE lock, since the last reference would run the
    /// whole Drop (final uploads, lease release, sidecar) under it.
    fn state(&self) -> LiveState {
        match self.storage.upgrade() {
            Some(storage) if storage.is_closing() => LiveState::Closing(storage),
            Some(storage) => LiveState::Open(storage),
            None if self.gone.load(Ordering::SeqCst) => LiveState::Finished,
            None => LiveState::Dropping,
        }
    }

    fn finished(&self) -> bool {
        self.storage.strong_count() == 0 && self.gone.load(Ordering::SeqCst)
    }

    /// How long an open (or destroy) waits for it to finish closing: the
    /// last close waits at most its close timeout for the snapshot its
    /// checkpoint queued (the background pass may hold the storage longer),
    /// Drop uploads what is left for at most another, then releases the
    /// lease and hashes the files for the sidecar.
    fn wait(&self) -> std::time::Duration {
        2 * self.close_timeout + CLOSE_MARGIN
    }
}

/// Beyond twice the close timeout: the lease release and the sidecar.
const CLOSE_MARGIN: std::time::Duration =
    std::time::Duration::from_millis(if cfg!(test) { 2_000 } else { 5_000 });

enum LiveState {
    Open(Arc<S3DurableStorage>),
    /// Every connection has started closing; something still holds it (a
    /// close still running, the background pass publishing the snapshot
    /// the last close's checkpoint queued).
    Closing(Arc<S3DurableStorage>),
    /// Its Drop runs: it uploads what is left, releases the lease and
    /// leaves the sidecar.
    Dropping,
    Finished,
}

static LIVE: Mutex<BTreeMap<PathBuf, Live>> = Mutex::new(BTreeMap::new());

#[cfg(test)]
thread_local! {
    /// Test hook: runs in `attach_live` right after it read an entry's
    /// state, with LIVE held.
    pub(crate) static ON_STATE: std::cell::RefCell<Option<Box<dyn FnMut()>>> =
        const { std::cell::RefCell::new(None) };
}

/// Attaches an open to the storage of `local_path` in this process, if one
/// is open (`None`: none is, or the last one has finished closing). A
/// storage that is closing is never handed to a new open (its file may have
/// been replaced since: inodes are reused at once, and the registry keys by
/// inode), so this waits for it to finish first: a storage still referenced
/// at most its close timeout and a second (then the open fails, to be
/// retried), a dropping one longer, and after that the open goes ahead
/// without it (it restores in full; the dropping storage's late sidecar
/// names an older generation, so no open reuses it).
pub fn attach_live(local_path: &Path) -> Result<Option<Attached>> {
    let start = std::time::Instant::now();
    loop {
        let key = canonical_key(local_path)?;
        // Dropped after the lock (see `Live::state`).
        let held;
        {
            let mut live = LIVE.lock().unwrap_or_else(|e| e.into_inner());
            live.retain(|_, entry| !entry.finished());
            let Some(entry) = live.get(&key) else {
                return Ok(None);
            };
            let waited_out = start.elapsed() > entry.wait();
            let state = entry.state();
            #[cfg(test)]
            ON_STATE.with(|hook| {
                if let Some(hook) = hook.borrow_mut().as_mut() {
                    hook()
                }
            });
            held = match state {
                LiveState::Finished => return Ok(None),
                LiveState::Open(storage) => match Attached::try_new(&storage) {
                    Some(attached) => return Ok(Some(attached)),
                    // Its last connection closed just now.
                    None => Some(storage),
                },
                LiveState::Closing(_) | LiveState::Dropping if waited_out => {
                    // Safe: the new open's takeover fences the old storage
                    // (its manifest PUT and uploads fail, so it leaves no
                    // sidecar), and a sidecar it already left names an older
                    // generation.
                    tracing::warn!(
                        "s3: {} is still finishing its close; opening without waiting for it",
                        local_path.display()
                    );
                    return Ok(None);
                }
                LiveState::Closing(storage) => Some(storage),
                LiveState::Dropping => None,
            };
        }
        drop(held);
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
}

/// Flushes every S3 database open in this VM (the VM is about to exit
/// without closing them), waiting at most `timeout` in all. Returns what
/// could not be made durable, one message per database.
pub fn flush_all(timeout: std::time::Duration) -> Vec<String> {
    let storages: Vec<(PathBuf, Arc<S3DurableStorage>)> = LIVE
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .iter()
        .filter_map(|(key, entry)| entry.storage.upgrade().map(|s| (key.clone(), s)))
        .collect();
    let deadline = std::time::Instant::now() + timeout;
    storages
        .into_iter()
        .filter_map(|(key, storage)| {
            let left = deadline.saturating_duration_since(std::time::Instant::now());
            storage
                .flush(left)
                .err()
                .map(|err| format!("{}: {err}", key.display()))
        })
        .collect()
}

/// One key per file however it is reached (`..`, directory or file
/// symlinks, hard links): its device and inode once it exists, like turso's
/// own registry; before that, the canonical directory plus the file name.
pub fn canonical_key(path: &Path) -> std::io::Result<PathBuf> {
    #[cfg(unix)]
    if let Ok(meta) = std::fs::metadata(path) {
        use std::os::unix::fs::MetadataExt;
        return Ok(PathBuf::from(format!(
            "inode:{}:{}",
            meta.dev(),
            meta.ino()
        )));
    }
    let absolute = std::path::absolute(path)?;
    let dir = absolute.parent().unwrap_or(Path::new("/"));
    let name = absolute
        .file_name()
        .ok_or_else(|| std::io::Error::other("database path has no file name"))?;
    Ok(std::fs::canonicalize(dir)?.join(name))
}

/// The S3 storage of the database at `local_path`, if one is open in this
/// process. Plain opens of that file share turso's `Database`, so they must
/// follow the same rules (commit through S3, stay in MVCC, group-commit sync).
pub fn live_storage(local_path: &Path) -> Option<Arc<S3DurableStorage>> {
    let key = canonical_key(local_path).ok()?;
    let live = LIVE.lock().unwrap_or_else(|e| e.into_inner());
    live.get(&key).and_then(|entry| entry.storage.upgrade())
}

/// The database at `place` (see `S3Config::place`) in this process: whether
/// a connection has it open (not only closing, or still finishing its
/// Drop), and how long to wait for its close (see `Live::wait`). `None`: not
/// open here.
fn open_at(place: &str) -> Option<(bool, std::time::Duration)> {
    open_where(|_, entry| entry.place == place)
}

/// Like [`open_at`], for the S3 database (of any prefix) whose local file
/// is `local_path`.
fn file_open(local_path: &Path) -> Option<(bool, std::time::Duration)> {
    let key = canonical_key(local_path).ok()?;
    open_where(|at, _| *at == key)
}

fn open_where(matches: impl Fn(&PathBuf, &Live) -> bool) -> Option<(bool, std::time::Duration)> {
    // Dropped after the lock (see `Live::state`).
    let mut held = Vec::new();
    let live = LIVE.lock().unwrap_or_else(|e| e.into_inner());
    let found = live
        .iter()
        .filter(|(key, entry)| matches(key, entry) && !entry.finished())
        .map(|(_, entry)| match entry.state() {
            LiveState::Open(storage) => {
                let open = storage.attached() > 0;
                held.push(storage);
                (open, entry.wait())
            }
            LiveState::Closing(storage) => {
                held.push(storage);
                (false, entry.wait())
            }
            _ => (false, entry.wait()),
        })
        .max_by_key(|(open, _)| *open);
    drop(live);
    drop(held);
    found
}

/// What a writer open does besides opening the database.
#[derive(Clone, Debug)]
pub(crate) enum Op {
    Open,
    /// `Sediment.S3.rekey/3`: re-encrypt under the key `to` (the ring's
    /// active one without it). A retired key only with `allow_reuse`.
    Rekey {
        to: Option<String>,
        force: bool,
        allow_reuse: bool,
    },
    /// `Sediment.S3.release_key/4`: drop the retained epochs under `key`.
    Release {
        key: String,
        force: bool,
    },
}

impl Op {
    /// Like a destroy, a maintenance operation refuses any unexpired lease,
    /// this owner's too, unless forced: it may be a writer still running.
    fn exclusive(&self) -> Option<bool> {
        match self {
            Op::Open => None,
            Op::Rekey { force, .. } | Op::Release { force, .. } => Some(*force),
        }
    }
}

/// A writer open's result.
pub(crate) enum Fresh {
    Opened {
        storage: Arc<S3DurableStorage>,
        /// The key ids it moved the database from and to, if it rekeyed.
        rekeyed: Option<(String, String)>,
        /// The epochs it dropped from the history (`Op::Release`).
        released: Vec<Epoch>,
    },
    /// Nothing to do: the database is under the rekey's key already, or no
    /// retained epoch is under the released key. Nothing was written but
    /// the lease, which is released.
    Unchanged,
}

fn prepare_fresh(cfg: &S3Config, local_path: &Path, io: Arc<dyn IO>, op: &Op) -> Result<Fresh> {
    // Read only: it is discarded once the open goes ahead (see
    // prepare_under_lease), so a run that stops before touching the local
    // files (KeyNeeded, a no-op or refused rekey) keeps the copy warm.
    let sidecar = warm::read(local_path);
    let remote = cfg.remote()?;
    let must_exist = cfg.must_exist || !matches!(op, Op::Open);
    // Before anything is written (the probe, the lease): a mistyped bucket
    // or prefix gets nothing. Checked again under the lease below.
    if must_exist && restore::read_manifest(&remote)?.is_none() {
        return Err(no_database(cfg));
    }
    require_encryption_choice(cfg, &remote)?;
    remove_snapshot_images(local_path);
    if cfg.verify_conditional_writes {
        probe::verify(&remote, &cfg.store_identity())?;
    }
    let owner = cfg.owner.clone().unwrap_or_else(default_owner);
    let lease = match op.exclusive() {
        None => Lease::acquire(remote.clone(), owner.clone(), cfg.lease_ttl)?,
        Some(force) => {
            Lease::acquire_exclusive(remote.clone(), owner.clone(), cfg.lease_ttl, force)?
        }
    };
    // The renewal thread holds the lease now and then, so dropping it may
    // release it only later: give it up at once when this open ends
    // without a storage (a maintenance operation's re-run, or another
    // owner's open, need it now). A crash (test hook) gave it up already.
    let fresh = prepare_under_lease(cfg, local_path, io, op, &remote, sidecar, owner, &lease);
    if !matches!(fresh, Ok(Fresh::Opened { .. })) {
        lease.release();
    }
    fresh
}

#[allow(clippy::too_many_arguments)]
fn prepare_under_lease(
    cfg: &S3Config,
    local_path: &Path,
    io: Arc<dyn IO>,
    op: &Op,
    remote: &remote::Remote,
    sidecar: Option<warm::Sidecar>,
    owner: String,
    lease: &Arc<Lease>,
) -> Result<Fresh> {
    let must_exist = cfg.must_exist || !matches!(op, Op::Open);
    let generation = lease.generation();

    let current = match remote.get(MANIFEST_KEY)? {
        Some(object) => Some((ManifestState::decode(&object.bytes)?, object.version)),
        None => None,
    };
    if let Some((state, _)) = &current {
        refuse_newer(state.generation(), generation)?;
    }
    if must_exist && !matches!(current, Some((ManifestState::Database(_), _))) {
        return Err(no_database(cfg));
    }
    let mut rekeyed = None;
    let mut released = Vec::new();
    let (manifest, version, published, key) = match current {
        Some((ManifestState::Database(decoded), object_version)) => {
            // The key comes from the manifest just read under the lease,
            // before anything is written: an unknown id, or another key under
            // the id, is refused by name, and a key_for ring asks for the key
            // it lacks (`S3Error::KeyNeeded`).
            let key = resolve_key(cfg, decoded.key.as_ref())?;
            let target = rekey_target(cfg, op, &decoded, key.as_ref())?;
            if matches!(op, Op::Rekey { .. }) && target.is_none() {
                return Ok(Fresh::Unchanged);
            }
            if let Op::Release { key: id, .. } = op {
                released = released_epochs(&decoded, id)?;
                if released.is_empty() {
                    return Ok(Fresh::Unchanged);
                }
            }
            // The open goes ahead: the sidecar (and a temp file a cut-short
            // write left) goes before anything touches the local files.
            let discarded = warm::discard(local_path);
            let sidecar = sidecar.filter(|_| discarded);
            let remote = remote.clone().for_key(key.as_ref())?;
            // Take the manifest over before reading the log: from here on no
            // stale writer can publish, or acknowledge a commit (see
            // `S3DurableStorage::confirm_ownership`).
            // Checked against the manifest as the previous writer left it.
            let warm = sidecar.as_ref().and_then(|sidecar| {
                warm::check(sidecar, &decoded, key.as_ref(), &remote, local_path)
                    .map_err(|why| {
                        tracing::info!("s3: full restore of {}: {why}", local_path.display())
                    })
                    .ok()
            });
            let manifest = Manifest {
                generation,
                writer: owner,
                updated_at_ms: now_ms(),
                ..decoded
            };
            let version = remote
                .put(MANIFEST_KEY, manifest.encode(), Put::Update(object_version))
                .map_err(|err| overtaken_at_takeover(&remote, err))?;
            let reused = warm.and_then(|local| {
                restore::restore_and_seal(
                    &remote,
                    &manifest,
                    local_path,
                    cfg.download_concurrency,
                    generation,
                    Some(local),
                )
                .map_err(|err| tracing::warn!("s3: reusing {} failed: {err}", local_path.display()))
                .ok()
            });
            let log = match reused {
                Some(log) => log,
                None => restore::restore_and_seal(
                    &remote,
                    &manifest,
                    local_path,
                    cfg.download_concurrency,
                    generation,
                    None,
                )?,
            };
            // The new epoch starts at the store's time of the seal that
            // closed the old one: every frame of the old epoch landed
            // before it, every frame of the new one lands after the
            // manifest below.
            let boundary = remote.boundary(&manifest.epoch.segment_key(log.len))?;
            // The restored snapshot, before the log is replayed into it: the
            // new epoch's snapshot can be a delta on top of it. Never across
            // a rekey: a chain is under one key.
            let restored = (cfg.incremental_snapshots && target.is_none())
                .then(|| Digests::of_file(local_path))
                .transpose()?;
            // Every open moves to an epoch of its own: an earlier incarnation's
            // or a stale writer's late PUTs can only target the old one.
            checkpoint_local(local_path, key.as_ref())?;
            if let (Some(from), Some(to)) = (&key, &target) {
                rekey::reencrypt(local_path, from, to)?;
                rekeyed = Some((from.id().to_string(), to.id().to_string()));
            }
            let epoch = manifest.epoch.next(generation);
            let record = manifest.current();
            let published = if target.is_some() {
                // A delta never spans two keys: the new key's epoch starts
                // a new chain, whatever the delta heuristic would pick.
                snapshot::publish_full(&remote, epoch, local_path)?
            } else {
                snapshot::publish(
                    &remote,
                    epoch,
                    local_path,
                    restored.as_ref().map(|d| (&record, d)),
                )?
            };
            let key = target.or(key);
            let mut fresh = manifest.advance(
                epoch,
                &published,
                generation,
                &manifest.writer,
                cfg.retain_epochs,
                key.as_ref().map(Key::record),
                boundary,
            );
            fresh
                .history
                .retain(|record| !released.contains(&record.epoch));
            if !matches!(op, Op::Open) {
                hook::at(hook::PUBLISH, &cfg.place(), lease)?;
            }
            // A rekey's switch: If-Match on the takeover, so a writer that
            // took over since (during the copy, say) gets this one fenced
            // with nothing switched.
            let version = publish_manifest(&remote, &fresh, version)?;
            (fresh, version, published, key)
        }
        // A destroyed database: finish its purge (a destroy may have been
        // interrupted), then start a new one in its place.
        Some((ManifestState::Destroyed(tombstone), object_version)) => {
            warm::discard(local_path);
            let key = new_database_key(cfg)?;
            purge(remote, &tombstone)?;
            check_no_old_objects(remote, Some(&tombstone))?;
            refuse_to_replace_local_data(local_path, key.as_ref())?;
            bootstrap_local(local_path, key.as_ref())?;
            let over = Some((&tombstone, object_version));
            let (manifest, version, published) =
                create_database(remote, key.as_ref(), generation, owner, local_path, over)
                    .map_err(fenced_on_conflict)?;
            (manifest, version, published, key)
        }
        None => {
            warm::discard(local_path);
            let key = new_database_key(cfg)?;
            check_no_old_objects(remote, None)?;
            refuse_to_replace_local_data(local_path, key.as_ref())?;
            bootstrap_local(local_path, key.as_ref())?;
            let (manifest, version, published) =
                create_database(remote, key.as_ref(), generation, owner, local_path, None)
                    .map_err(fenced_on_conflict)?;
            (manifest, version, published, key)
        }
    };

    let log_path = restore::log_path(local_path);
    let log_path = log_path
        .to_str()
        .ok_or_else(|| S3Error::Config("database path must be valid UTF-8".into()))?;
    let file = io.open_file(log_path, OpenFlags::default(), false)?;
    let inner = Storage::new(file, io, key.as_ref().map(Key::context).transpose()?);
    if let Some(threshold) = cfg.checkpoint_threshold {
        turso_core::mvcc::persistent_storage::DurableStorage::set_checkpoint_threshold(
            &inner, threshold,
        );
    }
    let storage = Arc::new(S3DurableStorage::new(
        inner,
        remote.clone().for_key(key.as_ref())?,
        lease.clone(),
        cfg,
        local_path.to_path_buf(),
        (manifest, version),
        cfg.incremental_snapshots
            .then_some((published.key, published.digests)),
        key,
    ));
    storage.collect_garbage_now();
    storage.start_uploader();
    Ok(Fresh::Opened {
        storage,
        rekeyed,
        released,
    })
}

/// The key `Op::Rekey` moves the database to (the ring's active one unless
/// it names one), or `None` when the database is under it already. A key
/// the database was rotated away from is refused unless reuse is allowed.
fn rekey_target(
    cfg: &S3Config,
    op: &Op,
    manifest: &Manifest,
    current: Option<&Key>,
) -> Result<Option<Key>> {
    let Op::Rekey {
        to, allow_reuse, ..
    } = op
    else {
        return Ok(None);
    };
    let Some(ring) = &cfg.encryption else {
        return Err(S3Error::Config(
            "a rekey needs the :encryption key ring with the key to move to".into(),
        ));
    };
    let target = match to {
        Some(id) => ring.need(id, |ring| {
            format!("rekey: key {} is not in the ring (ring: {ring})", shown(id))
        })?,
        None => ring.active()?,
    };
    let current = current.ok_or_else(|| S3Error::Config(UNENCRYPTED_WITH_KEY.into()))?;
    if current.id() == target.id() {
        // The fingerprint was checked: the ring's key under that id is the
        // database's.
        return Ok(None);
    }
    if target.cipher() != current.cipher() {
        return Err(S3Error::Config(format!(
            "rekey: key {} is for {}, the database's key {} for {}: a rekey keeps the cipher",
            shown(target.id()),
            target.cipher(),
            shown(current.id()),
            current.cipher()
        )));
    }
    if target.fingerprint() == current.fingerprint() {
        return Err(S3Error::Config(format!(
            "rekey: key {} is the database's key {} under another id; a rekey moves to \
             another key",
            shown(target.id()),
            shown(current.id())
        )));
    }
    // Retired by id or by key material: a key comes back, under any id,
    // only with allow_reuse.
    let same =
        |id: &str, fingerprint: &str| id == target.id() || fingerprint == target.fingerprint();
    let retired: Vec<_> = manifest
        .retired_keys
        .iter()
        .filter(|r| same(&r.id, &r.fingerprint))
        .collect();
    if let Some(first) = retired.first() {
        if !allow_reuse {
            return Err(S3Error::Config(format!(
                "rekey: key {} (fingerprint {}) was retired from this database at {} (unix ms) \
                 as key {}; a rekey moves to a new key (allow_reuse: true to move back, once \
                 its retained epochs are released)",
                shown(target.id()),
                keys::short_fingerprint(target.fingerprint()),
                first.retired_at_ms,
                shown(&first.id)
            )));
        }
    }
    // Each key has at most one pinned run, and an id names one key in the
    // history: no retained epoch may be under the target's id or material,
    // or under a retired key it reuses.
    let clashing = manifest
        .history
        .iter()
        .filter_map(|record| record.key.as_ref())
        .find(|key| {
            same(&key.id, &key.fingerprint)
                || retired
                    .iter()
                    .any(|r| r.id == key.id && r.fingerprint == key.fingerprint)
        });
    if let Some(key) = clashing {
        return Err(S3Error::Config(format!(
            "rekey: key {} still has retained epochs from when it was current; release them \
             first (Sediment.S3.release_key/4), then reuse {}",
            shown(&key.id),
            shown(target.id())
        )));
    }
    Ok(Some(target.clone()))
}

/// The epoch-advance manifest PUT (`If-Match` on the takeover). A failure is
/// never reported while this PUT can still land: after any error the
/// manifest is read back. Ours: adopted. Still the version we hold: the PUT
/// may be in flight, so the same body is sent again on that version.
/// Foreign: another writer moved the manifest, so ours can't land anymore
/// (fenced). Still ours-held or unreadable after the retries: the outcome
/// is unknown.
fn publish_manifest(
    remote: &remote::Remote,
    manifest: &Manifest,
    held: object_store::UpdateVersion,
) -> Result<object_store::UpdateVersion> {
    const ATTEMPTS: usize = 4;
    let body = manifest.encode();
    let mut last = String::new();
    for _ in 0..ATTEMPTS {
        let err = match remote.put(MANIFEST_KEY, body.clone(), Put::Update(held.clone())) {
            Ok(version) => return Ok(version),
            Err(err) => err,
        };
        match remote.get(MANIFEST_KEY) {
            Ok(Some(object)) if object.bytes == body => return Ok(object.version),
            Ok(Some(object))
                if object.version.e_tag.is_some() && object.version.e_tag == held.e_tag =>
            {
                last = format!("{err}, and the manifest is still the one this open holds");
            }
            // Another writer moved the manifest: ours can't land anymore, but
            // it may have landed before (and that writer taken over after
            // it). Say which key the manifest names now.
            Ok(foreign) => {
                let key = foreign
                    .and_then(|object| Manifest::decode(&object.bytes).ok())
                    .and_then(|current| current.key);
                return Err(S3Error::Overtaken {
                    message: format!(
                        "{err}; another writer has the database now (its manifest names key \
                         {}), and this open's write of epoch {} may have landed before it",
                        key.as_ref().map_or("none".into(), |key| shown(&key.id)),
                        manifest.epoch
                    ),
                    key,
                });
            }
            Err(read) => last = format!("{err}, and reading it back failed too ({read})"),
        }
    }
    Err(S3Error::Indeterminate(format!(
        "the manifest PUT of epoch {} failed ({last}): whether it landed is unknown",
        manifest.epoch
    )))
}

/// The retained epochs `release_key` drops: those under `id`. The current
/// key is refused: its epoch is the database.
fn released_epochs(manifest: &Manifest, id: &str) -> Result<Vec<Epoch>> {
    if manifest.key.as_ref().is_some_and(|key| key.id == id) {
        return Err(S3Error::Config(format!(
            "key {} is the current key of this database: rekey it to another key first",
            shown(id)
        )));
    }
    Ok(manifest
        .history
        .iter()
        .filter(|record| record.key.as_ref().is_some_and(|key| key.id == id))
        .map(|record| record.epoch)
        .collect())
}

/// Checkpoint images a previous writer of this file left behind (a crash
/// while its snapshot was pending); this open publishes a snapshot of its own.
fn remove_snapshot_images(local_path: &Path) {
    let prefix = storage::snapshot_image_prefix(local_path);
    let Some(dir) = local_path.parent() else {
        return;
    };
    let dir = if dir.as_os_str().is_empty() {
        Path::new(".")
    } else {
        dir
    };
    if let Ok(entries) = std::fs::read_dir(dir) {
        for entry in entries.flatten() {
            if entry.file_name().to_string_lossy().starts_with(&prefix) {
                let _ = std::fs::remove_file(entry.path());
            }
        }
    }
}

/// Result of [`restore_to`].
#[derive(Debug, Clone)]
pub struct Restored {
    pub epoch: String,
    pub epoch_started_at_ms: u64,
    pub frames: usize,
    /// The key of the restored epoch, which the restored file is encrypted
    /// with.
    pub key: Option<Key>,
}

/// A restore renames a new file over the local path. Over a symlink, that
/// replaces the link and leaves the file it pointed to behind, with the MVCC
/// log next to it that the restored database would use too.
fn refuse_symlink(path: &Path) -> Result<()> {
    if path
        .symlink_metadata()
        .is_ok_and(|meta| meta.file_type().is_symlink())
    {
        return Err(S3Error::Config(format!(
            "{} is a symlink: an S3 database's local path must be the file itself, since \
             a restore replaces it. Use the path the link points to",
            path.display()
        )));
    }
    Ok(())
}

/// Restores the database into a standalone local file at `local_path`,
/// without the lease and without writing to S3. With [`Target::Time`] or
/// [`Target::Epoch`] this is point-in-time restore over the epochs the
/// manifest retains (`retain_epochs`). The result is folded into one file
/// that plain turso (or `Sediment.Engine.open/2` without `:s3`) opens.
pub fn restore_to(cfg: &S3Config, local_path: &Path, target: Target) -> Result<Restored> {
    restore_into(cfg, local_path, target).map_err(|err| err.at(local_path))
}

fn restore_into(cfg: &S3Config, local_path: &Path, target: Target) -> Result<Restored> {
    refuse_symlink(local_path)?;
    cfg.validate()?;
    let _log =
        crate::log_guard::claim(local_path, crate::log_guard::Use::New).map_err(S3Error::Config)?;
    let remote = cfg.remote()?;
    // Like a replica's refresh (see replica::stage): only a log the writer
    // still owned after it was listed.
    let mut attempts = 0;
    let (point, log, key) = loop {
        let manifest = restore::read_manifest(&remote)?
            .ok_or_else(|| S3Error::Config("no database at this S3 prefix".into()))?;
        require_encryption_choice_for(cfg, Some(&manifest))?;
        let point = restore::choose(&manifest, target)?;
        // The key recorded for that epoch, whatever the current one is.
        let key = restore_key(cfg, point.record.key.as_ref())?;
        let remote = remote.clone().for_key(key.as_ref())?;
        let log = restore::restore_point(&remote, &point, local_path, cfg.download_concurrency)?;
        // An epoch before the current one was sealed by whoever replaced it
        // (and its snapshot alone needs no seal).
        if point.record.epoch == manifest.epoch
            && !point.snapshot_only
            && !restore::usable(&manifest, &log)
        {
            return Err(restore::taking_over(&manifest));
        }
        attempts += 1;
        if restore::still_retained(&remote, point.record.epoch)? {
            break (point, log, key);
        }
        if attempts == 3 {
            return Err(S3Error::Timeout(
                "s3: the writer moved to a new epoch, or another writer took over, during every \
         attempt; try again"
                    .into(),
            ));
        }
    };
    checkpoint_local(local_path, key.as_ref())?;
    Ok(Restored {
        epoch: point.record.epoch.to_string(),
        epoch_started_at_ms: point.record.started_at_ms,
        frames: log.frames,
        key,
    })
}

/// An encrypted database opened without `:encryption` would otherwise fail
/// its log verification (frames are sized for plaintext) as "corrupt", and
/// the advice for corrupt objects is to restore them from backups.
/// S3 databases are encrypted unless `encryption: false` says otherwise:
/// without a key or that opt-out, refuse, saying what the prefix holds.
pub(crate) fn require_encryption_choice(cfg: &S3Config, remote: &remote::Remote) -> Result<()> {
    if cfg.encryption.is_some() || cfg.unencrypted {
        return Ok(());
    }
    let existing = remote
        .get(MANIFEST_KEY)
        .ok()
        .flatten()
        .and_then(|object| Manifest::decode(&object.bytes).ok());
    require_encryption_choice_for(cfg, existing.as_ref())
}

/// [`require_encryption_choice`] with the manifest already read.
pub(crate) fn require_encryption_choice_for(
    cfg: &S3Config,
    existing: Option<&Manifest>,
) -> Result<()> {
    if cfg.encryption.is_some() || cfg.unencrypted {
        return Ok(());
    }
    Err(S3Error::Config(match existing {
        Some(manifest) if manifest.key.is_some() => ENCRYPTED_NEEDS_KEY.into(),
        Some(_) => UNENCRYPTED_NEEDS_OPT_OUT.into(),
        None => "S3 databases are encrypted by default: set encryption: [cipher: \"aegis256\", \
                 key: \"<64 hex chars>\"] (generate a key with Sediment.S3.generate_key() or \
                 `openssl rand -hex 32`, and keep it safe: the data can't be read without it), \
                 or encryption: false to store this database unencrypted"
            .into(),
    }))
}

const UNENCRYPTED_NEEDS_OPT_OUT: &str = "the database at this S3 prefix is unencrypted: open \
     it with encryption: false (S3 databases are encrypted by default, so storing one \
     unencrypted is an explicit choice), or import its data into a new, encrypted prefix";

const UNENCRYPTED_WITH_KEY: &str = "the database at this S3 prefix is unencrypted: open it \
     with encryption: false (it was created without a key; to encrypt it, import its data \
     into a new prefix with Sediment.S3.import/3 and a key)";

const ENCRYPTED_NEEDS_KEY: &str = "the database at this S3 prefix is encrypted: open or \
                                   restore it with an :encryption key ring holding its key";

/// The key `recorded` (a manifest's current key) names, from the open's
/// ring; `None` for an unencrypted database. Refuses a ring on an
/// unencrypted database and no ring on an encrypted one, an id the ring
/// lacks, and a ring key under the id that isn't the recorded key.
pub(crate) fn resolve_key(cfg: &S3Config, recorded: Option<&KeyRecord>) -> Result<Option<Key>> {
    resolve_with(cfg, recorded, KeyRing::resolve)
}

/// [`resolve_key`] for a restore of an epoch whose key is `recorded`.
pub(crate) fn restore_key(cfg: &S3Config, recorded: Option<&KeyRecord>) -> Result<Option<Key>> {
    resolve_with(cfg, recorded, KeyRing::resolve_for_restore)
}

fn resolve_with(
    cfg: &S3Config,
    recorded: Option<&KeyRecord>,
    resolve: impl for<'a> Fn(&'a KeyRing, &KeyRecord) -> Result<&'a Key>,
) -> Result<Option<Key>> {
    match (recorded, &cfg.encryption) {
        (Some(record), Some(ring)) => resolve(ring, record).cloned().map(Some),
        (Some(_), None) => Err(S3Error::Config(ENCRYPTED_NEEDS_KEY.into())),
        // Refused before anything is written: a key can't read a plaintext
        // database.
        (None, Some(_)) => Err(S3Error::Config(UNENCRYPTED_WITH_KEY.into())),
        (None, None) if cfg.unencrypted => Ok(None),
        (None, None) => Err(S3Error::Config(UNENCRYPTED_NEEDS_OPT_OUT.into())),
    }
}

/// The key a new database is created under: the ring's active one.
pub(crate) fn new_database_key(cfg: &S3Config) -> Result<Option<Key>> {
    match &cfg.encryption {
        Some(ring) => ring.active().cloned().map(Some),
        None if cfg.unencrypted => Ok(None),
        None => require_encryption_choice_for(cfg, None).map(|()| None),
    }
}

/// A database already open in this VM (a writer's storage, a replica's
/// generation) was opened with `existing`. Another open shares its decrypted
/// state, so its ring must hold that same key (or it must make the same
/// choice of no key, with `encryption: false`).
pub fn check_same_encryption(existing: Option<&Key>, cfg: &S3Config) -> Result<()> {
    let refuse = |message: &str| Err(S3Error::Config(message.into()));
    match (existing, &cfg.encryption) {
        (Some(open), Some(ring)) => {
            let key = ring.need(open.id(), |ring| {
                format!(
                    "the database at this S3 prefix is open in this VM under key {}, which is \
                     not in the ring (ring: {ring}); every open needs the key it is under",
                    shown(open.id())
                )
            })?;
            keys::check_matches(key, &open.record())
        }
        (Some(_), None) => refuse(ENCRYPTED_NEEDS_KEY),
        (None, Some(_)) => refuse(UNENCRYPTED_WITH_KEY),
        (None, None) if cfg.unencrypted => Ok(()),
        (None, None) => refuse(UNENCRYPTED_NEEDS_OPT_OUT),
    }
}

/// The takeover PUT lost to another writer: fenced before anything was
/// switched, naming the key the manifest has now.
fn overtaken_at_takeover(remote: &remote::Remote, err: S3Error) -> S3Error {
    let S3Error::Conflict(message) = err else {
        return err;
    };
    let key = remote
        .get(MANIFEST_KEY)
        .ok()
        .flatten()
        .and_then(|object| Manifest::decode(&object.bytes).ok())
        .and_then(|current| current.key);
    S3Error::Overtaken {
        message: format!(
            "manifest changed concurrently: {message}; another writer has the database (its \
             manifest names key {}), and this open switched nothing",
            key.as_ref().map_or("none".into(), |key| shown(&key.id))
        ),
        key,
    }
}

fn fenced_on_conflict(err: S3Error) -> S3Error {
    match err {
        S3Error::Conflict(msg) => S3Error::Fenced(format!("manifest changed concurrently: {msg}")),
        other => other,
    }
}

/// Creates an empty MVCC database at `path`, replacing whatever is there.
/// Opening an encrypted snapshot with the wrong key (or none) fails to parse
/// it; say what that most likely means.
fn wrong_key(err: turso_core::LimboError) -> S3Error {
    let message = err.to_string();
    if message.contains("ecrypt")
        || message.contains("not a database")
        || message.contains("encrypted")
    {
        S3Error::Config(format!(
            "the restored database can't be read with the given encryption options \
             (wrong key, or missing :encryption for an encrypted database): {message}"
        ))
    } else {
        S3Error::Turso(message)
    }
}

/// A prefix without a manifest must not hold anything a database could have
/// written, except what a first open that failed before its manifest leaves
/// (a full snapshot of the first epoch), and what a destroy ended
/// (`tombstone`).
fn check_no_old_objects(remote: &remote::Remote, tombstone: Option<&Tombstone>) -> Result<()> {
    // Objects without a manifest: someone deleted it (and maybe the
    // lease, restarting generations). Don't build over them, except
    // what a first open that failed before its manifest leaves: its
    // epoch-0 full snapshot (only bootstrap writes epoch 0 before a
    // manifest exists, and without a manifest no commit was ever
    // acknowledged). This bootstrap uses an epoch of its own (its
    // lease generation), and garbage collection removes the leftover
    // once the database moves past epoch 0.
    let first = tombstone.map_or(0, Tombstone::next_seq);
    let leftover = |key: &str| {
        tombstone.is_some_and(|tombstone| tombstone.covers(key))
            || key.ends_with(".db")
                && layout::parse_snapshot_key(key).is_some_and(|epoch| epoch.seq == first)
    };
    let mut objects = remote.list(layout::LOG_DIR)?;
    objects.extend(remote.list(layout::SNAPSHOT_DIR)?);
    if let Some(stray) = objects.iter().find(|object| !leftover(&object.key)) {
        return Err(S3Error::Corrupt(format!(
            "no manifest.json but {} exists; refusing to create a new database \
             over the old one's objects (restore the manifest, or clear the prefix)",
            stray.key
        )));
    }
    Ok(())
}

/// Whether the prefix holds a database (not none yet, not destroyed),
/// without taking the lease or writing anything.
pub fn exists(cfg: &S3Config) -> Result<bool> {
    cfg.validate()?;
    Ok(restore::read_manifest(&cfg.remote()?)?.is_some())
}

fn no_database(cfg: &S3Config) -> S3Error {
    S3Error::Config(format!(
        "no database at s3://{}/{} (must_exist: true)",
        cfg.bucket, cfg.prefix
    ))
}

/// A manifest or tombstone written under a lease generation at least ours:
/// someone took the lease after us, so this lease is stale. Building on it
/// could write keys a destroy considers dead (see `Tombstone::covers`).
pub(crate) fn refuse_newer(written_by: u64, generation: u64) -> Result<()> {
    if written_by >= generation {
        return Err(S3Error::Fenced(format!(
            "the prefix was written under lease generation {written_by} after this one \
             took the lease (generation {generation}); try again"
        )));
    }
    Ok(())
}

/// Deletes every object of the database `tombstone` ended. Returns how many.
pub(crate) fn purge(remote: &remote::Remote, tombstone: &Tombstone) -> Result<usize> {
    let mut objects = remote.list(layout::LOG_DIR)?;
    objects.extend(remote.list(layout::SNAPSHOT_DIR)?);
    let dead: Vec<String> = objects
        .into_iter()
        .map(|object| object.key)
        .filter(|key| tombstone.covers(key))
        .collect();
    let count = dead.len();
    remote.delete_many(dead)?;
    Ok(count)
}

/// Publishes the file at `local_path` as the first epoch of a new database
/// and creates the manifest: create-only (epoch 0), or `over` a destroy's
/// tombstone with `If-Match`, after its epochs (`S3Error::Conflict` if
/// another writer got there first).
fn create_database(
    remote: &remote::Remote,
    key: Option<&Key>,
    generation: u64,
    owner: String,
    local_path: &Path,
    over: Option<(&Tombstone, object_store::UpdateVersion)>,
) -> Result<(Manifest, object_store::UpdateVersion, snapshot::Published)> {
    let (seq, mode) = match over {
        Some((tombstone, version)) => (tombstone.next_seq(), Put::Update(version)),
        None => (0, Put::Create),
    };
    let epoch = Epoch { seq, generation };
    let published = snapshot::publish(remote, epoch, local_path, None)?;
    // The first epoch starts at the store's time of its snapshot.
    let started_at_ms = remote.boundary(&published.key)?;
    let snapshot = &published.snapshot;
    let manifest = Manifest {
        version: MANIFEST_VERSION,
        generation,
        epoch,
        snapshot: published.key.clone(),
        snapshot_size: snapshot.size,
        snapshot_crc32c: snapshot.crc32c,
        snapshot_zstd: snapshot.zstd,
        snapshot_stored_size: snapshot.stored_size,
        snapshot_base: Vec::new(),
        writer: owner,
        updated_at_ms: now_ms(),
        started_at_ms,
        history: Vec::new(),
        key: key.map(Key::record),
        retired_keys: Vec::new(),
        database_id: Some(format!("{:016x}{:016x}", rand_u64(), rand_u64())),
    };
    let version = remote.put(MANIFEST_KEY, manifest.encode(), mode)?;
    Ok((manifest, version, published))
}

/// A new database replaces whatever lies at `path`: refuse when that is a
/// database with anything in it (an existing SQLite file the user meant to
/// keep, or the local copy of a prefix someone emptied). A database with
/// nothing but turso's internal tables (e.g. what an earlier bootstrap that
/// failed before its manifest left) is replaced. A file that can't be read isn't assumed empty.
fn refuse_to_replace_local_data(path: &Path, key: Option<&Key>) -> Result<()> {
    let has_bytes = |p: &Path| std::fs::metadata(p).is_ok_and(|meta| meta.len() > 0);
    if ![
        path.to_path_buf(),
        restore::wal_path(path),
        restore::log_path(path),
    ]
    .iter()
    .any(|p| has_bytes(p))
    {
        return Ok(());
    }
    let mut entries = 0;
    let read = with_local_connection(path, key.map(Key::opts).as_ref(), |conn| {
        let rows = conn
            .query(
                "SELECT count(*) FROM sqlite_schema \
                 WHERE substr(name, 1, 17) != '__turso_internal_'",
            )?
            .map(|mut stmt| stmt.run_collect_rows())
            .transpose()?
            .unwrap_or_default();
        if let Some(count) = rows.first().and_then(|row| row.first()) {
            entries = count.to_string().parse().unwrap_or(1);
        }
        Ok(())
    });
    let what = match read {
        Ok(()) if entries == 0 => return Ok(()),
        Ok(()) => format!("a database with {entries} schema objects (tables, indexes, ...)"),
        Err(err) => format!("a file that can't be read as a database ({err})"),
    };
    Err(S3Error::Config(format!(
        "{} holds {what}, but the S3 prefix has no database; refusing to replace \
         the local file with a new, empty database. To start empty, move or delete it \
         (and its -wal and .db-log files). To upload it as this prefix's database, \
         import it (Sediment.S3.import/3). If the prefix should hold this database's \
         S3 copy, check the bucket and prefix",
        path.display(),
    )))
}

fn bootstrap_local(path: &Path, key: Option<&Key>) -> Result<()> {
    for stale in [
        path.to_path_buf(),
        restore::wal_path(path),
        restore::log_path(path),
    ] {
        restore::remove_if_exists(&stale)?;
    }
    with_local_connection(path, key.map(Key::opts).as_ref(), |conn| {
        conn.execute("PRAGMA journal_mode = 'mvcc'")?;
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    })
}

/// Replays the local log into the DB file and empties the log, using turso's
/// own local storage.
fn checkpoint_local(path: &Path, key: Option<&Key>) -> Result<()> {
    with_local_connection(path, key.map(Key::opts).as_ref(), |conn| {
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    })
}

fn with_local_connection(
    path: &Path,
    encryption: Option<&turso_core::EncryptionOpts>,
    f: impl FnOnce(&Arc<turso_core::Connection>) -> turso_core::Result<()>,
) -> Result<()> {
    let (_db, conn) = open_local(path, encryption)?;
    f(&conn)?;
    conn.close()?;
    Ok(())
}

/// A connection to the local file at `path` without S3 (keep the `Database`
/// alive while it is used).
pub(crate) fn open_local(
    path: &Path,
    encryption: Option<&turso_core::EncryptionOpts>,
) -> Result<(Arc<Database>, Arc<turso_core::Connection>)> {
    open_local_with(path, encryption, turso_core::DatabaseOpts::new())
}

/// [`open_local`] with these database options (experimental features).
pub(crate) fn open_local_with(
    path: &Path,
    encryption: Option<&turso_core::EncryptionOpts>,
    db_opts: turso_core::DatabaseOpts,
) -> Result<(Arc<Database>, Arc<turso_core::Connection>)> {
    let path_str = path
        .to_str()
        .ok_or_else(|| S3Error::Config("database path must be valid UTF-8".into()))?;
    let io: Arc<dyn IO> = Arc::new(PlatformIO::new()?);
    let options = OpenOptions::new(Arc::new(SqliteDialect))
        .db_opts(db_opts.with_encryption(encryption.is_some()))
        .encryption(encryption.cloned());
    let db = Database::open(io, path_str, options).map_err(wrong_key)?;
    let key = encryption
        .map(|opts| turso_core::EncryptionKey::from_hex_string(&opts.hexkey))
        .transpose()?;
    let conn = db.connect_with_encryption(key).map_err(wrong_key)?;
    Ok((db, conn))
}

fn default_owner() -> String {
    let host = std::env::var("HOSTNAME").unwrap_or_else(|_| "host".into());
    format!("{host}-{}-{:016x}", std::process::id(), rand_u64())
}

fn rand_u64() -> u64 {
    use std::hash::{BuildHasher, Hasher};
    let mut hasher = std::collections::hash_map::RandomState::new().build_hasher();
    hasher.write_u64(now_ms());
    hasher.finish()
}
