//! NIF glue for S3 durability: decoding the `s3:` option and the
//! `Sediment.S3` helpers.

use std::path::Path;
use std::sync::Arc;

use rustler::types::map::MapIterator;
use rustler::{Atom, Encoder, Env, ResourceArc, Term, TermType};
use turso_core::IO;

use crate::conn::{closed, error_tuple, ok_tuple, ConnRes};
use crate::s3::{self, S3Config, S3DurableStorage, S3Error, Target};
use crate::stmt::on_turso_stack;

/// Turns the `s3:` keyword list or map into a config. Values may be strings,
/// atoms, integers or booleans.
pub fn decode_config(term: Term<'_>) -> Result<S3Config, String> {
    let pairs: Vec<(Term, Term)> = match term.get_type() {
        TermType::Map => MapIterator::new(term)
            .ok_or("invalid s3 options")?
            .collect(),
        TermType::List => term
            .decode::<Vec<(Term, Term)>>()
            .map_err(|_| "s3 options must be a keyword list or a map".to_string())?,
        _ => return Err("s3 options must be a keyword list or a map".into()),
    };
    // `nil` means unset (runtime.exs often reads options from the environment).
    let pairs = pairs
        .into_iter()
        .filter(|(_, value)| value.atom_to_string().ok().as_deref() != Some("nil"))
        .map(|(key, value)| {
            let key =
                scalar(key).map_err(|_| "s3 option names must be atoms or strings".to_string())?;
            // Values can be credentials: name the option, never echo the value.
            let value = scalar(value).map_err(|_| {
                format!("s3 option {key}: expected a string, atom, integer or boolean")
            })?;
            Ok((key, value))
        })
        .collect::<Result<Vec<_>, String>>()?;
    S3Config::from_pairs(pairs).map_err(|e| e.to_string())
}

fn scalar(term: Term<'_>) -> Result<String, ()> {
    if let Ok(s) = term.decode::<String>() {
        return Ok(s);
    }
    if let Ok(i) = term.decode::<i64>() {
        return Ok(i.to_string());
    }
    if let Ok(b) = term.decode::<bool>() {
        return Ok(b.to_string());
    }
    term.atom_to_string().map_err(|_| ())
}

/// Decodes the `:encryption` key ring Elixir builds
/// (`Sediment.S3.Keys.native/1`): `%{keys: [{id, cipher, hex}], active: id |
/// nil, resolvable: bool}`, or nil. Errors name ids, never key material.
pub fn decode_ring(term: Term<'_>) -> Result<Option<s3::KeyRing>, String> {
    if term.atom_to_string().ok().as_deref() == Some("nil") {
        return Ok(None);
    }
    let field = |key: Atom| term.map_get(key).ok();
    let entries: Vec<(String, String, String)> = field(crate::atoms::keys())
        .and_then(|t| t.decode().ok())
        .ok_or("encryption: expected a key ring (see Sediment.S3.Keys)")?;
    let active = field(crate::atoms::active()).and_then(|t| t.decode::<String>().ok());
    let resolvable = field(crate::atoms::resolvable())
        .and_then(|t| t.decode::<bool>().ok())
        .unwrap_or(false);
    let invalid = |err: S3Error| match err {
        S3Error::Config(message) => format!("encryption: {message}"),
        other => other.to_string(),
    };
    // An entry is named by its position: its id isn't checked yet.
    let keys = entries
        .iter()
        .enumerate()
        .map(|(i, (id, cipher, hex))| {
            s3::Key::new(id, cipher, hex).map_err(|err| match err {
                S3Error::Config(message) => {
                    S3Error::Config(format!("ring entry {}: {message}", i + 1))
                }
                other => other,
            })
        })
        .collect::<s3::Result<Vec<_>>>()
        .map_err(invalid)?;
    s3::KeyRing::new(keys, active, resolvable)
        .map(Some)
        .map_err(invalid)
}

/// Restores `path` from S3 (or creates it) and returns the storage to open
/// it with, counting this open as one of its connections.
pub fn prepare(
    term: Term<'_>,
    path: &str,
    encryption: Option<s3::KeyRing>,
    io: Arc<dyn IO>,
) -> Result<s3::Attached, crate::open::OpenError> {
    let mut config = decode_config(term)?;
    config.encryption = encryption;
    Ok(s3::prepare_attached(&config, Path::new(path), io)?)
}

fn describe(err: &S3Error) -> String {
    err.to_string()
}

/// `{:error, message}`, `{:error, {:key_needed, id}}` for a key a `key_for`
/// ring must add, `{:error, {:indeterminate, message}}`, or `{:error,
/// {:fenced, %{current_key:, fingerprint:, message:}}}` when another
/// writer's manifest replaced the one an epoch advance held.
pub fn error_term<'a>(env: Env<'a>, err: &S3Error) -> Term<'a> {
    match err {
        S3Error::Overtaken { key, .. } => {
            let pairs = [
                (
                    "current_key",
                    key.as_ref()
                        .map(|k| s3::shown(&k.id).into_owned())
                        .encode(env),
                ),
                (
                    "fingerprint",
                    key.as_ref().map(|k| k.fingerprint.clone()).encode(env),
                ),
                ("message", err.to_string().encode(env)),
            ]
            .map(|(k, v)| (Atom::from_str(env, k).unwrap().encode(env), v));
            match Term::map_from_pairs(env, &pairs) {
                Ok(map) => (
                    crate::atoms::error(),
                    (Atom::from_str(env, "fenced").unwrap(), map),
                )
                    .encode(env),
                Err(_) => error_tuple(env, describe(err)),
            }
        }
        S3Error::KeyNeeded(id) => (
            crate::atoms::error(),
            (crate::atoms::key_needed(), id.as_str()),
        )
            .encode(env),
        S3Error::Indeterminate(_) => (
            crate::atoms::error(),
            (crate::atoms::indeterminate(), err.to_string()),
        )
            .encode(env),
        other => error_tuple(env, describe(other)),
    }
}

/// Decodes the S3 options and the key ring of a `Sediment.S3` call.
fn config_with_ring(opts: Term<'_>, ring: Term<'_>) -> Result<S3Config, String> {
    let mut config = decode_config(opts)?;
    config.encryption = decode_ring(ring)?;
    Ok(config)
}

fn with_storage<'a>(
    env: Env<'a>,
    res: &ConnRes,
    f: impl FnOnce(&S3DurableStorage) -> Term<'a>,
) -> Term<'a> {
    let guard = res.handle();
    match guard.as_ref() {
        None => closed(env),
        Some(handle) => match &handle.s3 {
            Some(storage) => f(storage),
            None => error_tuple(env, "not an s3 database"),
        },
    }
}

#[rustler::nif(schedule = "DirtyIo")]
fn s3_info<'a>(env: Env<'a>, res: ResourceArc<ConnRes>) -> Term<'a> {
    with_storage(env, &res, |storage| {
        let info = storage.info();
        let map = rustler::Term::map_from_pairs(
            env,
            &[
                ("epoch", info.epoch.encode(env)),
                ("snapshot", info.snapshot.encode(env)),
                ("generation", info.generation.encode(env)),
                ("owner", info.owner.encode(env)),
                ("lease_expires_at_ms", info.lease_expires_at_ms.encode(env)),
                ("log_offset", info.log_offset.encode(env)),
                ("uploaded_frames", info.uploaded_frames.encode(env)),
                ("uploaded_objects", info.uploaded_objects.encode(env)),
                ("uploaded_bytes", info.uploaded_bytes.encode(env)),
                ("snapshot_pending", info.snapshot_pending.encode(env)),
                ("poisoned", info.poisoned.encode(env)),
                (
                    "durability",
                    if info.asynchronous { "async" } else { "sync" }.encode(env),
                ),
                ("pending_bytes", info.pending_bytes.encode(env)),
                ("pending_frames", info.pending_frames.encode(env)),
                ("lag_ms", info.lag_ms.encode(env)),
                ("durable_epoch", info.durable_epoch.encode(env)),
                ("durable_offset", info.durable_offset.encode(env)),
                ("committed_offset", info.log_offset.encode(env)),
                ("lost", encode_loss(env, info.lost.as_ref())),
            ]
            .map(|(k, v)| (rustler::Atom::from_str(env, k).unwrap().encode(env), v)),
        );
        match map {
            Ok(map) => ok_tuple(env, map),
            Err(_) => error_tuple(env, "could not build info map"),
        }
    })
}

fn encode_loss<'a>(env: Env<'a>, loss: Option<&s3::Loss>) -> Term<'a> {
    let Some(loss) = loss else {
        return rustler::types::atom::nil().encode(env);
    };
    rustler::Term::map_from_pairs(
        env,
        &[
            ("durable_epoch", loss.durable.0.encode(env)),
            ("durable_offset", loss.durable.1.encode(env)),
            ("committed_epoch", loss.committed.0.encode(env)),
            ("committed_offset", loss.committed.1.encode(env)),
            ("frames", loss.frames.encode(env)),
        ]
        .map(|(k, v)| (rustler::Atom::from_str(env, k).unwrap().encode(env), v)),
    )
    .unwrap_or_else(|_| rustler::types::atom::nil().encode(env))
}

/// The application knows that commits were lost (see `s3_info`'s `lost`):
/// flushes work again. Returns what was lost, or nil.
#[rustler::nif(schedule = "DirtyIo")]
fn s3_acknowledge_loss<'a>(env: Env<'a>, res: ResourceArc<ConnRes>) -> Term<'a> {
    with_storage(env, &res, |storage| {
        ok_tuple(env, encode_loss(env, storage.acknowledge_loss().as_ref()))
    })
}

/// Flushes every S3 database open in this VM (before it exits without
/// closing them). `:ok`, or `{:error, messages}` for what isn't durable.
#[rustler::nif(schedule = "DirtyIo")]
fn s3_flush_all<'a>(env: Env<'a>, timeout_ms: u64) -> Term<'a> {
    match s3::flush_all(std::time::Duration::from_millis(timeout_ms))[..] {
        [] => rustler::types::atom::ok().encode(env),
        ref failed => error_tuple(env, failed.to_vec()),
    }
}

/// S3 requests this OS process has sent: `[{operation, class, count}]`.
#[rustler::nif]
fn s3_request_counts(env: Env<'_>) -> Term<'_> {
    s3::meter::counts()
        .into_iter()
        .map(|(op, class, n)| (op, class.name(), n))
        .collect::<Vec<_>>()
        .encode(env)
}

/// Waits until everything committed through this database so far (all
/// connections) is in S3. `cancel/1` interrupts the wait.
#[rustler::nif(schedule = "DirtyIo")]
fn s3_flush<'a>(
    env: Env<'a>,
    res: ResourceArc<ConnRes>,
    timeout_ms: u64,
    admission: u64,
) -> Term<'a> {
    let storage = {
        let guard = res.handle();
        match guard.as_ref() {
            None => return closed(env),
            Some(handle) => match &handle.s3 {
                Some(storage) => storage.clone(),
                None => return error_tuple(env, "not an s3 database"),
            },
        }
    };
    let flushed = s3::remote::with_cancel(&|| res.is_cancelled(admission), || {
        storage.flush(std::time::Duration::from_millis(timeout_ms))
    });
    match flushed {
        Ok((epoch, offset)) => {
            let map = rustler::Term::map_from_pairs(
                env,
                &[
                    ("durable_epoch", epoch.encode(env)),
                    ("durable_offset", offset.encode(env)),
                ]
                .map(|(k, v)| (rustler::Atom::from_str(env, k).unwrap().encode(env), v)),
            );
            match map {
                Ok(map) => ok_tuple(env, map),
                Err(_) => error_tuple(env, "could not build flush result"),
            }
        }
        Err(err) => error_tuple(env, describe(&err)),
    }
}

/// For `sync: true`: waits until what this connection's last autocommit
/// statement or transaction committed is in S3, and no longer. `{:ok, nil}`
/// at once when it committed nothing (a read), so reads never wait for S3.
#[rustler::nif(schedule = "DirtyIo")]
fn s3_flush_commit<'a>(
    env: Env<'a>,
    res: ResourceArc<ConnRes>,
    timeout_ms: u64,
    admission: u64,
) -> Term<'a> {
    let mark = res.take_commit_mark();
    if !mark.wrote && mark.seq.is_none() {
        return ok_tuple(env, rustler::types::atom::nil());
    }
    let storage = {
        let guard = res.handle();
        match guard.as_ref() {
            None => return closed(env),
            Some(handle) => match &handle.s3 {
                Some(storage) => storage.clone(),
                None => return ok_tuple(env, rustler::types::atom::nil()),
            },
        }
    };
    let timeout = std::time::Duration::from_millis(timeout_ms);
    // No sequence although it wrote: nothing queued (sync durability), or
    // turso's group commit queued this commit on another connection's
    // thread; waiting for everything queued covers it.
    let flushed = s3::remote::with_cancel(&|| res.is_cancelled(admission), || match mark.seq {
        Some(seq) => storage.flush_through(seq, timeout),
        None => storage.flush(timeout),
    });
    match flushed {
        Ok(_) => ok_tuple(env, rustler::types::atom::ok()),
        Err(err) => error_tuple(env, describe(&err)),
    }
}

#[rustler::nif(schedule = "DirtyIo")]
fn s3_flush_snapshot<'a>(env: Env<'a>, res: ResourceArc<ConnRes>) -> Term<'a> {
    with_storage(env, &res, |storage| match storage.flush_snapshot() {
        Ok(uploaded) => ok_tuple(env, uploaded),
        Err(err) => error_tuple(env, describe(&err)),
    })
}

/// Restores into a standalone local file without the lease (point-in-time
/// restore with a `{:epoch, n}` or `{:time, unix_ms}` target).
#[rustler::nif(schedule = "DirtyIo")]
fn s3_restore<'a>(
    env: Env<'a>,
    path: String,
    opts: Term<'a>,
    target: Term<'a>,
    encryption: Term<'a>,
) -> Term<'a> {
    let config = match config_with_ring(opts, encryption) {
        Ok(config) => config,
        Err(reason) => return error_tuple(env, reason),
    };
    let target = match target.decode::<(Atom, u64)>() {
        Ok((tag, n)) => match tag.to_term(env).atom_to_string().as_deref() {
            Ok("epoch") => Target::Epoch(n),
            Ok("time") => Target::Time(n),
            _ => return error_tuple(env, "target must be :latest, {:epoch, n} or {:time, ms}"),
        },
        Err(_) => Target::Latest,
    };
    let restored = match on_turso_stack(|| s3::restore_to(&config, Path::new(&path), target)) {
        Ok(restored) => restored,
        Err(reason) => return error_tuple(env, reason),
    };
    match restored {
        Ok(restored) => {
            let map = Term::map_from_pairs(
                env,
                &[
                    ("epoch", restored.epoch.encode(env)),
                    (
                        "epoch_started_at_ms",
                        restored.epoch_started_at_ms.encode(env),
                    ),
                    ("frames", restored.frames.encode(env)),
                    (
                        "key",
                        restored
                            .key
                            .as_ref()
                            .map(|key| key.id().to_string())
                            .encode(env),
                    ),
                ]
                .map(|(k, v)| (Atom::from_str(env, k).unwrap().encode(env), v)),
            );
            match map {
                Ok(map) => ok_tuple(env, map),
                Err(_) => error_tuple(env, "could not build result"),
            }
        }
        Err(err) => error_term(env, &err),
    }
}

/// Imports an existing database file into an empty prefix (see
/// `s3::import`): `verify` is `:checksum` or `:restore`, `encryption` the new
/// database's key, `source_encryption` the source's (if it is encrypted).
#[rustler::nif(schedule = "DirtyIo")]
fn s3_import<'a>(
    env: Env<'a>,
    path: String,
    opts: Term<'a>,
    verify: Atom,
    encryption: Term<'a>,
    source_encryption: Term<'a>,
) -> Term<'a> {
    let config = match config_with_ring(opts, encryption) {
        Ok(config) => config,
        Err(reason) => return error_tuple(env, reason),
    };
    // The source's ring: its active key reads it.
    let source_key = match decode_ring(source_encryption).and_then(|ring| {
        ring.map(|r| r.active().map(s3::Key::opts))
            .transpose()
            .map_err(|e| e.to_string())
    }) {
        Ok(key) => key,
        Err(reason) => return error_tuple(env, reason),
    };
    let verify = match verify.to_term(env).atom_to_string().as_deref() {
        Ok("checksum") => s3::Verify::Checksum,
        Ok("restore") => s3::Verify::Restore,
        _ => return error_tuple(env, "verify must be :checksum or :restore"),
    };
    let import_opts = s3::ImportOptions {
        verify,
        source_encryption: source_key,
    };
    let imported = match on_turso_stack(|| s3::import(&config, Path::new(&path), &import_opts)) {
        Ok(imported) => imported,
        Err(reason) => return error_tuple(env, reason),
    };
    match imported {
        Ok(imported) => {
            let map = Term::map_from_pairs(
                env,
                &[
                    ("epoch", imported.epoch.encode(env)),
                    ("size", imported.size.encode(env)),
                    ("stored_size", imported.stored_size.encode(env)),
                    ("objects", imported.objects.encode(env)),
                    ("rows", imported.rows.encode(env)),
                    (
                        "sequences_not_advanced",
                        imported.sequences_not_advanced.encode(env),
                    ),
                ]
                .map(|(k, v)| (Atom::from_str(env, k).unwrap().encode(env), v)),
            );
            match map {
                Ok(map) => ok_tuple(env, map),
                Err(_) => error_tuple(env, "could not build result"),
            }
        }
        Err(err) => error_term(env, &err),
    }
}

/// Destroys the database at the prefix (see `s3::destroy`).
#[rustler::nif(schedule = "DirtyIo")]
fn s3_destroy<'a>(env: Env<'a>, opts: Term<'a>, force: bool) -> Term<'a> {
    let config = match decode_config(opts) {
        Ok(config) => config,
        Err(reason) => return error_tuple(env, reason),
    };
    match s3::destroy(&config, force) {
        Ok(destroyed) => {
            let map = Term::map_from_pairs(
                env,
                &[(
                    Atom::from_str(env, "objects").unwrap().encode(env),
                    destroyed.objects.encode(env),
                )],
            );
            match map {
                Ok(map) => ok_tuple(env, map),
                Err(_) => error_tuple(env, "could not build result"),
            }
        }
        Err(err) => error_tuple(env, describe(&err)),
    }
}

/// Whether the prefix holds a database (see `s3::exists`).
#[rustler::nif(schedule = "DirtyIo")]
fn s3_exists<'a>(env: Env<'a>, opts: Term<'a>) -> Term<'a> {
    let config = match decode_config(opts) {
        Ok(config) => config,
        Err(reason) => return error_tuple(env, reason),
    };
    match s3::exists(&config) {
        Ok(exists) => ok_tuple(env, exists),
        Err(err) => error_tuple(env, describe(&err)),
    }
}

/// Moves the database to another key of the ring (see `s3::rekey`). Opens of
/// `path` are serialized with it, as with `open`.
#[rustler::nif(schedule = "DirtyIo")]
fn s3_rekey<'a>(
    env: Env<'a>,
    path: String,
    opts: Term<'a>,
    encryption: Term<'a>,
    to: Option<String>,
    force: bool,
    allow_reuse: bool,
) -> Term<'a> {
    let config = match config_with_ring(opts, encryption) {
        Ok(config) => config,
        Err(reason) => return error_tuple(env, reason),
    };
    let lock = crate::conn::path_lock(&path);
    let _opening = lock.as_deref().map(crate::conn::lock);
    let rekeyed =
        on_turso_stack(|| s3::rekey(&config, Path::new(&path), to.as_deref(), force, allow_reuse));
    let rekeyed = match rekeyed {
        Ok(rekeyed) => rekeyed,
        Err(reason) => return error_tuple(env, reason),
    };
    match rekeyed {
        Ok(s3::Rekeyed::Current) => ok_tuple(env, Atom::from_str(env, "current").unwrap()),
        Ok(s3::Rekeyed::Done { from, to, epoch }) => {
            let map = Term::map_from_pairs(
                env,
                &[("from", from), ("to", to), ("epoch", epoch)]
                    .map(|(k, v)| (Atom::from_str(env, k).unwrap().encode(env), v.encode(env))),
            );
            match map {
                Ok(map) => ok_tuple(env, map),
                Err(_) => error_tuple(env, "could not build result"),
            }
        }
        Err(err) => error_term(env, &err),
    }
}

/// Drops the retained epochs under a key (see `s3::release_key`).
#[rustler::nif(schedule = "DirtyIo")]
fn s3_release_key<'a>(
    env: Env<'a>,
    path: String,
    opts: Term<'a>,
    encryption: Term<'a>,
    key: String,
    force: bool,
) -> Term<'a> {
    let config = match config_with_ring(opts, encryption) {
        Ok(config) => config,
        Err(reason) => return error_tuple(env, reason),
    };
    let lock = crate::conn::path_lock(&path);
    let _opening = lock.as_deref().map(crate::conn::lock);
    let released = match on_turso_stack(|| s3::release_key(&config, Path::new(&path), &key, force))
    {
        Ok(released) => released,
        Err(reason) => return error_tuple(env, reason),
    };
    match released {
        Ok(released) => {
            let map = Term::map_from_pairs(
                env,
                &[
                    (
                        Atom::from_str(env, "key").unwrap().encode(env),
                        s3::shown(&key).encode(env),
                    ),
                    (
                        Atom::from_str(env, "epochs").unwrap().encode(env),
                        released.epochs.encode(env),
                    ),
                ],
            );
            match map {
                Ok(map) => ok_tuple(env, map),
                Err(_) => error_tuple(env, "could not build result"),
            }
        }
        Err(err) => error_term(env, &err),
    }
}

/// Which keys the database depends on (see `s3::key_census`).
#[rustler::nif(schedule = "DirtyIo")]
fn s3_key_census<'a>(env: Env<'a>, opts: Term<'a>) -> Term<'a> {
    let config = match decode_config(opts) {
        Ok(config) => config,
        Err(reason) => return error_tuple(env, reason),
    };
    match s3::key_census(&config) {
        Ok(None) => ok_tuple(env, Atom::from_str(env, "no_database").unwrap()),
        Ok(Some(census)) => {
            let retained: Vec<(String, Vec<String>)> = census.retained.into_iter().collect();
            let retained = match Term::map_from_pairs(
                env,
                &retained
                    .iter()
                    .map(|(id, epochs)| (id.encode(env), epochs.encode(env)))
                    .collect::<Vec<_>>(),
            ) {
                Ok(map) => map,
                Err(_) => return error_tuple(env, "could not build result"),
            };
            let map = Term::map_from_pairs(
                env,
                &[
                    (
                        Atom::from_str(env, "current").unwrap().encode(env),
                        census.current.encode(env),
                    ),
                    (
                        Atom::from_str(env, "retained").unwrap().encode(env),
                        retained,
                    ),
                ],
            );
            match map {
                Ok(map) => ok_tuple(env, map),
                Err(_) => error_tuple(env, "could not build result"),
            }
        }
        Err(err) => error_term(env, &err),
    }
}

/// Test hook (see `s3::hook`): `action` is `:pause`, `:fail` or `:crash`
/// to arm it at `stage` for the database in `opts`, `:release` to let a
/// paused operation go on, `:state` to read where it is. Returns the state:
/// `:armed`, `:paused`, `:done` or nil.
#[rustler::nif]
fn s3_test_hook<'a>(env: Env<'a>, opts: Term<'a>, stage: String, action: Atom) -> Term<'a> {
    let config = match decode_config(opts) {
        Ok(config) => config,
        Err(reason) => return error_tuple(env, reason),
    };
    let place = config.place();
    let action = action.to_term(env).atom_to_string().unwrap_or_default();
    match action.as_str() {
        "pause" => s3::hook::arm(&place, &stage, s3::hook::Action::Pause),
        "fail" => s3::hook::arm(&place, &stage, s3::hook::Action::Fail),
        "crash" => s3::hook::arm(&place, &stage, s3::hook::Action::Crash),
        "release" => s3::hook::release(&place),
        "state" => {}
        _ => {
            return error_tuple(
                env,
                "action must be :pause, :fail, :crash, :release or :state",
            )
        }
    }
    let state = match s3::hook::state(&place) {
        Some(s3::hook::State::Armed) => "armed",
        Some(s3::hook::State::Paused) => "paused",
        Some(s3::hook::State::Done) => "done",
        None => "nil",
    };
    Atom::from_str(env, state).unwrap().encode(env)
}
