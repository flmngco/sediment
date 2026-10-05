//! Ends a database: `Sediment.S3.destroy/2`.
//!
//! The manifest is replaced by a tombstone (`If-Match`, so a destroy whose
//! lease lapsed can't end a database another writer took over), then every
//! object of the ended database is deleted. Once the tombstone is in place
//! no writer acknowledges a commit and no reader uses the old objects, so an
//! interrupted destroy never looks like an older database: the next destroy,
//! or an open, which then creates a new empty database, finishes the purge.
//! The tombstone and `lease.json` stay; the lease keeps generations growing,
//! so no later database reuses a key a late write of this one could recreate.

use super::layout::{ManifestState, Tombstone, MANIFEST_KEY};
use super::lease::Lease;
use super::remote::{Put, Remote};
use super::{default_owner, open_at, probe, purge, refuse_newer, Result, S3Config, S3Error};

/// What a destroy deleted.
#[derive(Debug)]
pub struct Destroyed {
    pub objects: usize,
}

/// Destroys the database at `cfg`'s prefix. Refuses while a writer holds the
/// lease (or while this VM has the database open), unless `force`: then a
/// running writer is fenced and its unacknowledged commits are lost too.
pub fn destroy(cfg: &S3Config, force: bool) -> Result<Destroyed> {
    cfg.validate()?;
    if cfg.replica {
        return Err(S3Error::Config(
            "destroy can't run with mode: :replica".into(),
        ));
    }
    if open_at(&cfg.place()) {
        return Err(S3Error::Config(format!(
            "the database at s3://{}/{} is open in this VM; close every connection to it first",
            cfg.bucket, cfg.prefix
        )));
    }
    let remote = cfg.remote()?;
    if cfg.verify_conditional_writes {
        probe::verify(&remote, &cfg.store_identity())?;
    }
    let owner = cfg.owner.clone().unwrap_or_else(default_owner);
    let lease = Lease::acquire_to_destroy(remote.clone(), owner.clone(), cfg.lease_ttl, force)?;
    let ended = end(&remote, &lease, &owner);
    // Not left to Drop: the renewal thread may hold the lease a while longer.
    lease.release();
    Ok(Destroyed { objects: ended? })
}

/// Replaces the manifest with a tombstone, then purges what it ended.
fn end(remote: &Remote, lease: &Lease, owner: &str) -> Result<usize> {
    let generation = lease.generation();
    let (seq, mode) = match remote.get(MANIFEST_KEY)? {
        Some(object) => match ManifestState::decode(&object.bytes) {
            Ok(state) => {
                refuse_newer(state.generation(), generation)?;
                (state.seq(), Put::Update(object.version))
            }
            // An unreadable manifest is no database anyone opens: end it too.
            Err(_) => (0, Put::Update(object.version)),
        },
        None => (0, Put::Create),
    };
    let tombstone = Tombstone::new(generation, seq, owner);
    lease.ensure()?;
    let body = tombstone.encode();
    match remote.put(MANIFEST_KEY, body.clone(), mode) {
        Ok(_) => {}
        Err(S3Error::Conflict(_)) => {
            return Err(S3Error::Fenced(
                "another writer opened the database during the destroy; nothing was deleted".into(),
            ))
        }
        // The answer may be what got lost.
        Err(err) => match remote.get(MANIFEST_KEY) {
            Ok(Some(object)) if object.bytes == body => {}
            _ => return Err(err),
        },
    }
    purge(remote, &tombstone)
}
