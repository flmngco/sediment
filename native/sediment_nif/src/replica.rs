//! Read-only S3 replicas (`s3: [mode: :replica]`) and `Sediment.S3.refresh/1`.
//!
//! The connections to one replica (same path, store and prefix) in this OS
//! process share a *generation*: one working copy restored from S3
//! (`.<stem>.replica-<pid>-<n>.db` next to the path) and one turso `Database`
//! opened on it. A refresh stages the latest state once and publishes it as
//! the new generation; the other connections switch to it at their own next
//! refresh (a pool-wide refresh asks them to), without touching S3. A
//! generation's copy is removed when its last connection has moved on.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, LazyLock, Mutex, Weak};

use rustler::{Encoder, Env, ResourceArc, Term};
use turso_core::{Database, DatabaseOpts, OpenFlags, OpenOptions, PlatformIO, SqliteDialect, IO};

use crate::conn::{closed, error_tuple, lock, ok_tuple, ConnRes, Handle};
use crate::open::Opened;
use crate::s3::replica::{stage, still_usable, unique_path, ReplicaState};
use crate::s3::restore;
use crate::s3::S3Config;

/// One restored state of a replica, shared by the connections reading it.
pub struct Generation {
    working: PathBuf,
    io: Arc<dyn IO>,
    db: Arc<Database>,
    pub state: ReplicaState,
    /// The key it was opened with: a connection reusing it must give the
    /// same (it reads the decrypted state).
    encryption: Option<turso_core::EncryptionOpts>,
}

impl Drop for Generation {
    fn drop(&mut self) {
        let _ = restore::remove_if_exists(&restore::wal_path(&self.working));
        let _ = restore::remove_if_exists(&restore::log_path(&self.working));
        let _ = restore::remove_if_exists(&self.working);
    }
}

/// The current generation of one replica. Locked while a connection stages
/// a new one, so the connections of a pool download a state once.
#[derive(Default)]
struct Slot {
    current: Weak<Generation>,
}

type Key = (PathBuf, String);

static SLOTS: LazyLock<Mutex<HashMap<Key, Arc<Mutex<Slot>>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

fn slot(key: &Key) -> Arc<Mutex<Slot>> {
    let mut slots = lock(&SLOTS);
    // Forget replicas nobody reads anymore. A slot locked by a staging
    // connection is in use: never wait for it here (a download can take long).
    slots.retain(|k, slot| {
        k == key
            || Arc::strong_count(slot) > 1
            || slot
                .try_lock()
                .map_or(true, |slot| slot.current.strong_count() > 0)
    });
    slots.entry(key.clone()).or_default().clone()
}

/// A replica connection: the generation it reads, and what it needs to
/// stage the next one.
pub struct Replica {
    pub cfg: S3Config,
    pub path: PathBuf,
    pub db_opts: DatabaseOpts,
    pub generation: Arc<Generation>,
}

impl Replica {
    fn key(cfg: &S3Config, path: &std::path::Path) -> Key {
        (
            path.to_path_buf(),
            format!("{}|{}", cfg.store_identity(), cfg.prefix),
        )
    }
}

/// Opens a replica connection on the current generation of `path`, restoring
/// one from S3 first if no connection of this process reads it yet.
pub fn open(cfg: S3Config, path: &str, db_opts: DatabaseOpts) -> Result<Opened, String> {
    let path = PathBuf::from(path);
    let slot = slot(&Replica::key(&cfg, &path));
    let mut slot = lock(&slot);
    let generation = match usable(&mut slot, &cfg)? {
        Some(generation) => {
            crate::s3::check_same_encryption(generation.encryption.as_ref(), &cfg)
                .map_err(|e| e.to_string())?;
            generation
        }
        None => {
            let generation = new_generation(&cfg, &path, db_opts)?;
            slot.current = Arc::downgrade(&generation);
            generation
        }
    };
    drop(slot);
    connect(Replica {
        cfg,
        path,
        db_opts,
        generation,
    })
}

/// The slot's current generation, if S3 still has it: a destroy (and maybe a
/// new database in its place) since it was restored must not be served from
/// the cache. Forgets it otherwise.
fn usable(slot: &mut Slot, cfg: &S3Config) -> Result<Option<Arc<Generation>>, String> {
    let Some(generation) = slot.current.upgrade() else {
        return Ok(None);
    };
    if still_usable(cfg, &generation.state).map_err(|e| e.to_string())? {
        Ok(Some(generation))
    } else {
        slot.current = Weak::new();
        Ok(None)
    }
}

/// Stages the latest state from S3 and opens it as a new generation.
fn new_generation(
    cfg: &S3Config,
    path: &std::path::Path,
    db_opts: DatabaseOpts,
) -> Result<Arc<Generation>, String> {
    let staged = stage(cfg, path).map_err(|e| e.to_string())?;
    let working = unique_path(path, "replica");
    let state = staged.install(&working).map_err(|e| e.to_string())?;
    open_generation(cfg, working, db_opts, state)
}

fn open_generation(
    cfg: &S3Config,
    working: PathBuf,
    db_opts: DatabaseOpts,
    state: ReplicaState,
) -> Result<Arc<Generation>, String> {
    let removal = Removal(working.clone());
    let path = working
        .to_str()
        .ok_or("database path must be valid UTF-8")?
        .to_owned();
    let io: Arc<dyn IO> = Arc::new(PlatformIO::new().map_err(|e| e.to_string())?);
    let options = OpenOptions::new(Arc::new(SqliteDialect))
        .flags(OpenFlags::Create)
        .db_opts(db_opts)
        .encryption(cfg.encryption.clone());
    let db = Database::open(io.clone(), &path, options).map_err(|e| e.to_string())?;
    std::mem::forget(removal);
    Ok(Arc::new(Generation {
        working,
        io,
        db,
        state,
        encryption: cfg.encryption.clone(),
    }))
}

/// Removes an installed copy that never became a generation.
struct Removal(PathBuf);

impl Drop for Removal {
    fn drop(&mut self) {
        let _ = restore::remove_if_exists(&restore::wal_path(&self.0));
        let _ = restore::remove_if_exists(&restore::log_path(&self.0));
        let _ = restore::remove_if_exists(&self.0);
    }
}

fn connect(replica: Replica) -> Result<Opened, String> {
    let generation = replica.generation.clone();
    let key = replica
        .cfg
        .encryption
        .as_ref()
        .map(|opts| turso_core::EncryptionKey::from_hex_string(&opts.hexkey))
        .transpose()
        .map_err(|e| e.to_string())?;
    let conn = generation
        .db
        .connect_with_encryption(key)
        .map_err(|e| e.to_string())?;
    conn.set_query_only(true);
    Ok(Opened {
        io: generation.io.clone(),
        db: generation.db.clone(),
        conn,
        s3: None,
        replica: Some(replica),
    })
}

fn state_map<'a>(env: Env<'a>, state: &ReplicaState) -> Term<'a> {
    let pairs = [
        (
            "mode",
            rustler::Atom::from_str(env, "replica").unwrap().encode(env),
        ),
        ("epoch", state.epoch.encode(env)),
        ("snapshot", state.snapshot.encode(env)),
        ("log_bytes", state.log_bytes.encode(env)),
        ("writer", state.writer.encode(env)),
        ("restored_at_ms", state.restored_at_ms.encode(env)),
    ]
    .map(|(k, v)| (rustler::Atom::from_str(env, k).unwrap().encode(env), v));
    match Term::map_from_pairs(env, &pairs) {
        Ok(map) => ok_tuple(env, map),
        Err(_) => error_tuple(env, "could not build info map"),
    }
}

#[rustler::nif(schedule = "DirtyIo")]
fn s3_replica_info(env: Env<'_>, res: ResourceArc<ConnRes>) -> Term<'_> {
    match res.handle().as_ref() {
        None => closed(env),
        Some(Handle {
            replica: Some(replica),
            ..
        }) => state_map(env, &replica.generation.state),
        Some(_) => error_tuple(env, "not an s3 database"),
    }
}

/// Brings the connection to the latest state: to the replica's current
/// generation if another connection already published a newer one (no S3
/// request), otherwise to a new generation staged from S3 if S3 changed.
/// Statements prepared before a switch are finalized.
#[rustler::nif(schedule = "DirtyIo")]
fn s3_refresh(env: Env<'_>, res: ResourceArc<ConnRes>) -> Term<'_> {
    let mut guard = res.handle();
    let Some(handle) = guard.as_ref() else {
        return closed(env);
    };
    let Some(replica) = handle.replica.as_ref() else {
        return error_tuple(env, "not an s3 replica");
    };

    let slot = slot(&Replica::key(&replica.cfg, &replica.path));
    let mut slot = lock(&slot);
    let current = match usable(&mut slot, &replica.cfg) {
        Ok(current) => current,
        Err(msg) => return error_tuple(env, msg),
    };
    let target = match current {
        Some(current) if !Arc::ptr_eq(&current, &replica.generation) => {
            if let Err(err) =
                crate::s3::check_same_encryption(current.encryption.as_ref(), &replica.cfg)
            {
                return error_tuple(env, err.to_string());
            }
            current
        }
        _ => {
            let staged = match stage(&replica.cfg, &replica.path) {
                Ok(staged) => staged,
                Err(err) => return error_tuple(env, err.to_string()),
            };
            let state = &replica.generation.state;
            if staged.state.epoch == state.epoch && staged.state.log_bytes == state.log_bytes {
                return state_map(env, state);
            }
            let working = unique_path(&replica.path, "replica");
            let generation = staged
                .install(&working)
                .map_err(|e| e.to_string())
                .and_then(|state| open_generation(&replica.cfg, working, replica.db_opts, state));
            match generation {
                Ok(generation) => {
                    slot.current = Arc::downgrade(&generation);
                    generation
                }
                Err(msg) => return error_tuple(env, msg),
            }
        }
    };
    drop(slot);

    res.finalize_all();
    let old = guard.take().expect("handle checked above");
    let _ = old.conn.close();
    let previous = old.replica.as_ref().expect("replica checked above");
    let reopened = connect(Replica {
        cfg: previous.cfg.clone(),
        path: previous.path.clone(),
        db_opts: previous.db_opts,
        generation: target,
    });
    // The previous generation's copy goes once no connection reads it.
    drop(old);
    match reopened {
        Ok(opened) => {
            *lock(&res.interrupt) = Some(opened.conn.clone());
            let handle = Handle::from(opened);
            let info = match &handle.replica {
                Some(replica) => state_map(env, &replica.generation.state),
                None => error_tuple(env, "not an s3 replica"),
            };
            *guard = Some(handle);
            info
        }
        Err(msg) => {
            lock(&res.interrupt).take();
            error_tuple(env, msg)
        }
    }
}
