//! `Sediment.S3.key_census/1`: which keys a database still depends on.

use std::collections::BTreeMap;

use super::keys::short_fingerprint;
use super::layout::Manifest;
use super::{restore, storage, Result, S3Config};

/// The key of the current epoch, and the retained epochs (point-in-time
/// restore) by key. A key can leave the ring once it is neither.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Census {
    /// `None` for an unencrypted database.
    pub current: Option<String>,
    /// Key id -> the ids of the retained epochs under it, newest first.
    pub retained: BTreeMap<String, Vec<String>>,
    /// Key id -> its fingerprint (16 hex digits), for every key listed.
    pub fingerprints: BTreeMap<String, String>,
}

/// Reads the manifest (no lease, no writes). `None`: no database (none yet,
/// or destroyed).
pub fn key_census(cfg: &S3Config) -> Result<Option<Census>> {
    cfg.validate()?;
    let Some(manifest) = restore::read_manifest(&cfg.remote()?)? else {
        return Ok(None);
    };
    let mut retained: BTreeMap<String, Vec<String>> = BTreeMap::new();
    let mut fingerprints = BTreeMap::new();
    for key in manifest.key.iter() {
        fingerprints.insert(
            key.id.clone(),
            short_fingerprint(&key.fingerprint).to_string(),
        );
    }
    for record in &manifest.history {
        if let Some(key) = &record.key {
            retained
                .entry(key.id.clone())
                .or_default()
                .push(record.epoch.to_string());
            fingerprints
                .entry(key.id.clone())
                .or_insert_with(|| short_fingerprint(&key.fingerprint).to_string());
        }
    }
    Ok(Some(Census {
        current: manifest.key.map(|key| key.id),
        retained,
        fingerprints,
    }))
}

/// What a writer's garbage collection would delete (see
/// [`storage::garbage`]), counted: objects of epochs the manifest no longer
/// retains, left by a failed delete or a writer that stopped before
/// collecting them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Orphans {
    pub objects: usize,
    pub bytes: u64,
    /// Key id (`None`: not known) -> objects.
    pub by_key: BTreeMap<Option<String>, usize>,
}

/// Lists the orphans of the database (no lease, no writes), with the GC's
/// own rule and the manifest's generation, which is conservative: it never
/// lists what a writer's collection would keep. `None`: no database.
pub fn orphans(cfg: &S3Config) -> Result<Option<Orphans>> {
    cfg.validate()?;
    let remote = cfg.remote()?;
    let Some(manifest) = restore::read_manifest(&remote)? else {
        return Ok(None);
    };
    let garbage = storage::garbage(&remote, &manifest, manifest.generation)?;
    let mut by_key = BTreeMap::new();
    for object in &garbage {
        *by_key.entry(key_of(&manifest, object.epoch)).or_insert(0) += 1;
    }
    Ok(Some(Orphans {
        objects: garbage.len(),
        bytes: garbage.iter().map(|object| object.size).sum(),
        by_key,
    }))
}

/// The key of an epoch the manifest no longer names, where certain: objects
/// don't record their key, and the manifest forgets a released epoch. An
/// epoch after the current key's earliest retained one is under the current
/// key if no key was retired since that one started (a key changes only by
/// a rekey, which retires the old one); any other is `None`.
fn key_of(manifest: &Manifest, epoch: super::layout::Epoch) -> Option<String> {
    let current = manifest.key.as_ref()?;
    let earliest = manifest
        .retained()
        .filter(|record| record.key.as_ref() == Some(current))
        .min_by_key(|record| record.epoch.seq)?;
    let since = manifest
        .retired_keys
        .iter()
        .all(|retired| retired.retired_at_ms <= earliest.started_at_ms);
    (since && epoch.seq > earliest.epoch.seq).then(|| current.id.clone())
}
