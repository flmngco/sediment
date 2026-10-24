//! Object keys and the JSON documents stored next to the data.

use std::fmt;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use super::error::{Result, S3Error};
use super::keys::KeyRecord;

pub const MANIFEST_KEY: &str = "manifest.json";
pub const LEASE_KEY: &str = "lease.json";
pub const LOG_DIR: &str = "log/";
pub const SNAPSHOT_DIR: &str = "snapshots/";
/// 4: the key of every epoch (`key`), and when retained epochs ended.
/// Versions 1 and 2 (0.1.0-beta.6 and older) are refused.
pub const MANIFEST_VERSION: u32 = 4;
/// Version of the manifest a destroy leaves: no database. Older drivers
/// refuse it rather than read it as one.
pub const MANIFEST_VERSION_DESTROYED: u32 = 3;
/// Prefix of a seal object: written create-only at an epoch's end offset
/// when the epoch is closed, so no stale writer can append after it.
/// A format identifier persisted in S3, not a product name: never change it.
pub const SEAL_MAGIC: &[u8] = b"TURSO-S3-SEAL\n";

pub fn seal_body(closed_by: u64) -> bytes::Bytes {
    let mut body = SEAL_MAGIC.to_vec();
    body.extend_from_slice(format!("{{\"generation\":{closed_by}}}").as_bytes());
    body.into()
}

/// One log epoch: the frames written between two truncating checkpoints.
///
/// `generation` is the lease generation of the writer that started the
/// epoch, so keys written by a stale writer never collide with a newer one.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Epoch {
    pub seq: u64,
    pub generation: u64,
}

impl Epoch {
    pub fn next(self, generation: u64) -> Self {
        Self {
            seq: self.seq + 1,
            generation,
        }
    }

    pub fn log_dir(self) -> String {
        format!("{LOG_DIR}{self}/")
    }

    pub fn segment_key(self, offset: u64) -> String {
        format!("{LOG_DIR}{self}/{offset:020}")
    }

    pub fn snapshot_key(self) -> String {
        format!("{SNAPSHOT_DIR}{self}.db")
    }

    /// An incremental snapshot: the segments changed since the previous one.
    pub fn delta_key(self) -> String {
        format!("{SNAPSHOT_DIR}{self}.delta")
    }

    pub fn parse(id: &str) -> Option<Self> {
        let (seq, generation) = id.split_once('-')?;
        Some(Self {
            seq: seq.parse().ok()?,
            generation: generation.parse().ok()?,
        })
    }
}

impl fmt::Display for Epoch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:020}-{:010}", self.seq, self.generation)
    }
}

/// Parses `log/<epoch>/<offset>` into its parts.
pub fn parse_segment_key(key: &str) -> Option<(Epoch, u64)> {
    let rest = key.strip_prefix(LOG_DIR)?;
    let (epoch, offset) = rest.split_once('/')?;
    Some((Epoch::parse(epoch)?, offset.parse().ok()?))
}

/// Parses `snapshots/<epoch>.db` and `snapshots/<epoch>.delta`.
pub fn parse_snapshot_key(key: &str) -> Option<Epoch> {
    let name = key.strip_prefix(SNAPSHOT_DIR)?;
    Epoch::parse(
        name.strip_suffix(".db")
            .or_else(|| name.strip_suffix(".delta"))?,
    )
}

/// One object of a snapshot chain: a full snapshot, or a delta applied on
/// top of the links before it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotLink {
    pub key: String,
    /// Size of the (compressed) object.
    pub stored_size: u64,
    /// Size and CRC32C of the database file once this link is applied.
    pub size: u64,
    pub crc32c: u32,
}

/// Root of the restore chain: which snapshot to load and which log epoch to
/// replay on top of it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Manifest {
    pub version: u32,
    /// Lease generation of the writer that last wrote the manifest.
    pub generation: u64,
    pub epoch: Epoch,
    pub snapshot: String,
    pub snapshot_size: u64,
    pub snapshot_crc32c: u32,
    /// Stored form of the snapshot (absent in manifests from before zstd).
    #[serde(default)]
    pub snapshot_zstd: bool,
    #[serde(default)]
    pub snapshot_stored_size: u64,
    /// When `snapshot` is a delta: the chain below it, a full snapshot first.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub snapshot_base: Vec<SnapshotLink>,
    pub writer: String,
    pub updated_at_ms: u64,
    /// When the current epoch was published.
    #[serde(default)]
    pub started_at_ms: u64,
    /// Retained earlier epochs, newest first (point-in-time restore).
    #[serde(default)]
    pub history: Vec<EpochRecord>,
    /// The key the current epoch (its snapshot and every frame of its log)
    /// is encrypted with, `None` for an unencrypted database. An open
    /// resolves it from its ring before writing anything.
    pub key: Option<KeyRecord>,
    /// Every key a rekey moved the database away from, released or not: a
    /// rekey back to one is refused unless explicitly allowed.
    #[serde(default)]
    pub retired_keys: Vec<RetiredKey>,
    /// Random, chosen when the database is created and kept by every later
    /// manifest: which database this is, whatever its epochs. `None` in
    /// manifests written by older versions (or a takeover by one).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub database_id: Option<String>,
}

impl EpochRecord {
    /// Every snapshot object this epoch's restore reads, a full snapshot first.
    pub fn chain(&self) -> Vec<SnapshotLink> {
        let mut chain = self.snapshot_base.clone();
        chain.push(SnapshotLink {
            key: self.snapshot.clone(),
            stored_size: self.snapshot_info().stored_size,
            size: self.snapshot_size,
            crc32c: self.snapshot_crc32c,
        });
        chain
    }

    /// What the restore must find in the snapshot object.
    pub fn snapshot_info(&self) -> super::remote::Snapshot {
        super::remote::Snapshot {
            size: self.snapshot_size,
            crc32c: self.snapshot_crc32c,
            stored_size: if self.snapshot_zstd {
                self.snapshot_stored_size
            } else {
                self.snapshot_size
            },
            zstd: self.snapshot_zstd,
        }
    }
}

/// A restorable epoch: its snapshot and when it started.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EpochRecord {
    pub epoch: Epoch,
    pub snapshot: String,
    pub snapshot_size: u64,
    pub snapshot_crc32c: u32,
    #[serde(default)]
    pub snapshot_zstd: bool,
    #[serde(default)]
    pub snapshot_stored_size: u64,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub snapshot_base: Vec<SnapshotLink>,
    pub started_at_ms: u64,
    /// When the next epoch started: the epoch holds the database's history
    /// over `[started_at_ms, ended_at_ms)`. `None` for the current epoch.
    pub ended_at_ms: Option<u64>,
    /// The key the epoch is encrypted with.
    pub key: Option<KeyRecord>,
    /// Kept by a rekey (the epoch it ended and everything retained then):
    /// retention never trims it, only `release_key` of its key drops it.
    #[serde(default)]
    pub pinned: bool,
}

/// A key a rekey moved away from.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetiredKey {
    pub id: String,
    pub fingerprint: String,
    pub retired_at_ms: u64,
}

impl EpochRecord {
    /// Whether the epoch holds the database's state at `at` (unix ms).
    pub fn covers(&self, at: u64) -> bool {
        self.started_at_ms <= at && self.ended_at_ms.is_none_or(|end| at < end)
    }

    /// Its key's id, for messages.
    pub fn key_id(&self) -> &str {
        self.key.as_ref().map_or("none", |key| key.id.as_str())
    }
}

impl Manifest {
    /// Whether the writer that started the current epoch wrote this manifest.
    /// A writer taking over first writes the manifest with its own generation
    /// and the old epoch, then reads that epoch's log and seals it. Until the
    /// seal, the old epoch's log may still grow by a late upload of the old
    /// writer that the new one doesn't restore.
    pub fn settled(&self) -> bool {
        self.generation == self.epoch.generation
    }

    /// The current epoch as a record.
    pub fn current(&self) -> EpochRecord {
        EpochRecord {
            epoch: self.epoch,
            snapshot: self.snapshot.clone(),
            snapshot_size: self.snapshot_size,
            snapshot_crc32c: self.snapshot_crc32c,
            snapshot_zstd: self.snapshot_zstd,
            snapshot_stored_size: self.snapshot_stored_size,
            snapshot_base: self.snapshot_base.clone(),
            started_at_ms: self.started_at_ms,
            ended_at_ms: None,
            key: self.key.clone(),
            pinned: false,
        }
    }

    /// The manifest for a new epoch with the snapshot `published`,
    /// encrypted with `key`, starting at `boundary_ms`: the store's time of
    /// the seal that closed the current epoch (clamped so that epochs never
    /// start before their predecessor).
    ///
    /// The history keeps the `retain` newest earlier epochs that aren't
    /// pinned. A rekey (`key` differs from the current one) pins the epoch it
    /// ends and every epoch retained, and retires the old key.
    #[allow(clippy::too_many_arguments)]
    pub fn advance(
        &self,
        epoch: Epoch,
        published: &super::snapshot::Published,
        generation: u64,
        writer: &str,
        retain: usize,
        key: Option<KeyRecord>,
        boundary_ms: u64,
    ) -> Manifest {
        let started = boundary_ms.max(self.started_at_ms);
        let rekey = key != self.key;
        let mut ended = self.current();
        ended.ended_at_ms = Some(started);
        let mut history = vec![ended];
        history.extend(self.history.iter().cloned());
        let mut retired_keys = self.retired_keys.clone();
        if rekey {
            for record in &mut history {
                record.pinned = true;
            }
            // A reused key (`allow_reuse`) is current again, no more retired.
            // Only that key, by id and fingerprint: another key retired under
            // its id, or its material retired under another id, stays retired.
            if let Some(new) = &key {
                retired_keys.retain(|r| !(r.id == new.id && r.fingerprint == new.fingerprint));
            }
            if let Some(old) = &self.key {
                retired_keys.retain(|r| !(r.id == old.id && r.fingerprint == old.fingerprint));
                retired_keys.push(RetiredKey {
                    id: old.id.clone(),
                    fingerprint: old.fingerprint.clone(),
                    retired_at_ms: started,
                });
            }
        }
        let mut unpinned = 0;
        history.retain(|record| {
            record.pinned || {
                unpinned += 1;
                unpinned <= retain
            }
        });
        let snapshot = &published.snapshot;
        Manifest {
            version: MANIFEST_VERSION,
            generation,
            epoch,
            snapshot: published.key.clone(),
            snapshot_size: snapshot.size,
            snapshot_crc32c: snapshot.crc32c,
            snapshot_zstd: snapshot.zstd,
            snapshot_stored_size: snapshot.stored_size,
            snapshot_base: published.base.clone(),
            writer: writer.to_string(),
            updated_at_ms: now_ms(),
            started_at_ms: started,
            history,
            key,
            retired_keys,
            database_id: self.database_id.clone(),
        }
    }

    /// Every epoch a restore may still use.
    pub fn retained(&self) -> impl Iterator<Item = EpochRecord> + '_ {
        std::iter::once(self.current()).chain(self.history.iter().cloned())
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        #[derive(Deserialize)]
        struct Version {
            version: u32,
        }
        let version = serde_json::from_slice::<Version>(bytes)?.version;
        if version < MANIFEST_VERSION {
            return Err(S3Error::Corrupt(format!(
                "manifest version {version} was written by sediment 0.1.0-beta.6 or older, \
                 whose format this version doesn't read (version {MANIFEST_VERSION}: key \
                 rotation). Read the data with that version (export it), or clear the prefix \
                 with Sediment.S3.destroy/2"
            )));
        }
        if version != MANIFEST_VERSION {
            return Err(S3Error::Corrupt(format!(
                "unsupported manifest version {version}: written by a newer sediment"
            )));
        }
        Ok(serde_json::from_slice(bytes)?)
    }

    pub fn encode(&self) -> bytes::Bytes {
        serde_json::to_vec_pretty(self)
            .expect("manifest serializes")
            .into()
    }
}

/// What `Sediment.S3.destroy/2` leaves in place of the manifest: the
/// database is gone, and every object of an epoch whose generation is at
/// most `generation` is garbage (any later database uses a newer one).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Tombstone {
    pub version: u32,
    pub destroyed: bool,
    pub generation: u64,
    /// The epoch sequence number of the manifest it replaced. The next
    /// database starts after it, so epochs never go back: a stale writer of
    /// the destroyed database, collecting epochs older than its own, never
    /// collects the new database's.
    pub seq: u64,
    pub writer: String,
    pub updated_at_ms: u64,
}

impl Tombstone {
    pub fn new(generation: u64, seq: u64, writer: &str) -> Self {
        Self {
            version: MANIFEST_VERSION_DESTROYED,
            destroyed: true,
            generation,
            seq,
            writer: writer.to_string(),
            updated_at_ms: now_ms(),
        }
    }

    pub fn encode(&self) -> bytes::Bytes {
        serde_json::to_vec_pretty(self)
            .expect("tombstone serializes")
            .into()
    }

    /// The first epoch's sequence number of a database created over it.
    pub fn next_seq(&self) -> u64 {
        self.seq + 1
    }

    /// Whether an object key belongs to a database the tombstone ended.
    pub fn covers(&self, key: &str) -> bool {
        parse_segment_key(key)
            .map(|(epoch, _)| epoch)
            .or_else(|| parse_snapshot_key(key))
            .is_some_and(|epoch| epoch.generation <= self.generation)
    }
}

/// What `manifest.json` holds.
#[derive(Debug, Clone)]
#[allow(clippy::large_enum_variant)]
pub enum ManifestState {
    Database(Manifest),
    Destroyed(Tombstone),
}

impl ManifestState {
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        #[derive(Deserialize)]
        struct Probe {
            #[serde(default)]
            destroyed: bool,
        }
        if serde_json::from_slice::<Probe>(bytes)?.destroyed {
            Ok(Self::Destroyed(serde_json::from_slice(bytes)?))
        } else {
            Ok(Self::Database(Manifest::decode(bytes)?))
        }
    }

    /// The epoch sequence number it ends at.
    pub fn seq(&self) -> u64 {
        match self {
            Self::Database(manifest) => manifest.epoch.seq,
            Self::Destroyed(tombstone) => tombstone.seq,
        }
    }

    /// The lease generation it was written under.
    pub fn generation(&self) -> u64 {
        match self {
            Self::Database(manifest) => manifest.generation,
            Self::Destroyed(tombstone) => tombstone.generation,
        }
    }

    /// The database, or `None` after a destroy.
    pub fn database(self) -> Option<Manifest> {
        match self {
            Self::Database(manifest) => Some(manifest),
            Self::Destroyed(_) => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LeaseRecord {
    pub owner: String,
    pub generation: u64,
    pub expires_at_ms: u64,
}

/// The lease generation a manifest or tombstone records, read leniently (a
/// damaged or newer-format one too); 0 when there is none to read.
pub fn written_generation(bytes: &[u8]) -> u64 {
    serde_json::from_slice::<serde_json::Value>(bytes)
        .ok()
        .and_then(|value| value.get("generation")?.as_u64())
        .unwrap_or(0)
}

pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keys_round_trip_and_sort_by_offset() {
        let epoch = Epoch {
            seq: 3,
            generation: 7,
        };
        let key = epoch.segment_key(1234);
        assert_eq!(
            key,
            "log/00000000000000000003-0000000007/00000000000000001234"
        );
        assert_eq!(parse_segment_key(&key), Some((epoch, 1234)));
        assert_eq!(parse_snapshot_key(&epoch.snapshot_key()), Some(epoch));
        assert_eq!(parse_snapshot_key(&epoch.delta_key()), Some(epoch));
        assert!(epoch.segment_key(99) < epoch.segment_key(100));
        assert!(parse_segment_key("log/garbage/12").is_none());
    }
}
