//! NIF glue for S3 durability: decoding the `s3:` option and the
//! `Sediment.S3` helpers.

use std::path::Path;
use std::sync::Arc;

use rustler::types::map::MapIterator;
use rustler::{Atom, Encoder, Env, ResourceArc, Term, TermType};
use turso_core::IO;

use crate::conn::{closed, error_tuple, ok_tuple, ConnRes};
use crate::s3::{self, S3Config, S3DurableStorage, S3Error, Target};

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

/// Restores `path` from S3 (or creates it) and returns the storage to open
/// it with.
pub fn prepare(
    term: Term<'_>,
    path: &str,
    encryption: Option<turso_core::EncryptionOpts>,
    io: Arc<dyn IO>,
) -> Result<Arc<S3DurableStorage>, String> {
    let mut config = decode_config(term)?;
    config.encryption = encryption;
    s3::prepare_with_io(&config, Path::new(path), io).map_err(|e| describe(&e))
}

fn describe(err: &S3Error) -> String {
    err.to_string()
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
fn s3_flush<'a>(env: Env<'a>, res: ResourceArc<ConnRes>, timeout_ms: u64) -> Term<'a> {
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
    let flushed = s3::remote::with_cancel(&res.cancelled, || {
        storage.flush(std::time::Duration::from_millis(timeout_ms))
    });
    res.cancelled
        .store(false, std::sync::atomic::Ordering::SeqCst);
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
fn s3_flush_commit<'a>(env: Env<'a>, res: ResourceArc<ConnRes>, timeout_ms: u64) -> Term<'a> {
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
    let flushed = s3::remote::with_cancel(&res.cancelled, || match mark.seq {
        Some(seq) => storage.flush_through(seq, timeout),
        None => storage.flush(timeout),
    });
    res.cancelled
        .store(false, std::sync::atomic::Ordering::SeqCst);
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
    encryption: Option<(String, String)>,
) -> Term<'a> {
    let mut config = match decode_config(opts) {
        Ok(config) => config,
        Err(reason) => return error_tuple(env, reason),
    };
    if let Some((_, hexkey)) = &encryption {
        if let Err(reason) = crate::open::check_hex_key(hexkey) {
            return error_tuple(env, reason);
        }
    }
    config.encryption =
        encryption.map(|(cipher, hexkey)| turso_core::EncryptionOpts { cipher, hexkey });
    let target = match target.decode::<(Atom, u64)>() {
        Ok((tag, n)) => match tag.to_term(env).atom_to_string().as_deref() {
            Ok("epoch") => Target::Epoch(n),
            Ok("time") => Target::Time(n),
            _ => return error_tuple(env, "target must be :latest, {:epoch, n} or {:time, ms}"),
        },
        Err(_) => Target::Latest,
    };
    match s3::restore_to(&config, Path::new(&path), target) {
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
                ]
                .map(|(k, v)| (Atom::from_str(env, k).unwrap().encode(env), v)),
            );
            match map {
                Ok(map) => ok_tuple(env, map),
                Err(_) => error_tuple(env, "could not build result"),
            }
        }
        Err(err) => error_tuple(env, describe(&err)),
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
    encryption: Option<(String, String)>,
    source_encryption: Option<(String, String)>,
) -> Term<'a> {
    let mut config = match decode_config(opts) {
        Ok(config) => config,
        Err(reason) => return error_tuple(env, reason),
    };
    for (_, hexkey) in encryption.iter().chain(source_encryption.iter()) {
        if let Err(reason) = crate::open::check_hex_key(hexkey) {
            return error_tuple(env, reason);
        }
    }
    let key = |(cipher, hexkey): (String, String)| turso_core::EncryptionOpts { cipher, hexkey };
    config.encryption = encryption.map(key);
    let verify = match verify.to_term(env).atom_to_string().as_deref() {
        Ok("checksum") => s3::Verify::Checksum,
        Ok("restore") => s3::Verify::Restore,
        _ => return error_tuple(env, "verify must be :checksum or :restore"),
    };
    let import_opts = s3::ImportOptions {
        verify,
        source_encryption: source_encryption.map(key),
    };
    match s3::import(&config, Path::new(&path), &import_opts) {
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
        Err(err) => error_tuple(env, describe(&err)),
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
