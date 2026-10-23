//! `Sediment.S3.key_census/1`: which keys a database still depends on.

use std::collections::BTreeMap;

use super::{restore, Result, S3Config};

/// The key of the current epoch, and the retained epochs (point-in-time
/// restore) by key. A key can leave the ring once it is neither.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Census {
    /// `None` for an unencrypted database.
    pub current: Option<String>,
    /// Key id -> the ids of the retained epochs under it, newest first.
    pub retained: BTreeMap<String, Vec<String>>,
}

/// Reads the manifest (no lease, no writes). `None`: no database (none yet,
/// or destroyed).
pub fn key_census(cfg: &S3Config) -> Result<Option<Census>> {
    cfg.validate()?;
    let Some(manifest) = restore::read_manifest(&cfg.remote()?)? else {
        return Ok(None);
    };
    let mut retained: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for record in &manifest.history {
        if let Some(key) = &record.key {
            retained
                .entry(key.id.clone())
                .or_default()
                .push(record.epoch.to_string());
        }
    }
    Ok(Some(Census {
        current: manifest.key.map(|key| key.id),
        retained,
    }))
}
