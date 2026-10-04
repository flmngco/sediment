//! Incremental snapshots.
//!
//! A checkpoint uploads only the 64 KiB segments of the database file that
//! changed since the previous snapshot, as a delta object on top of it. The
//! manifest names the whole chain (`snapshot_base`, a full snapshot first),
//! and the last link's size and CRC32C are those of the rebuilt file, so a
//! restore checks the result exactly like a full snapshot. A snapshot is
//! full again when the chain gets long or its deltas add up to more than the
//! full snapshot, so restores stay bounded.
//!
//! Delta format (zstd-compressed): `DELTA_MAGIC`, the file size (u64 LE),
//! the segment size (u32 LE), then for each changed segment its index
//! (u32 LE) and its bytes (a whole segment, or less for the last one), then
//! `DELTA_END` (u32 LE).

use std::io::{BufReader, Read, Seek, SeekFrom, Write};
use std::path::Path;

use twox_hash::XxHash3_128;

use super::error::{Result, S3Error};
use super::layout::{Epoch, EpochRecord, SnapshotLink};
use super::remote::{Remote, Snapshot};

pub const SEGMENT: usize = 64 * 1024;
/// A format identifier persisted in S3, not a product name: never change it.
pub const DELTA_MAGIC: &[u8] = b"TURSO-S3-DELTA1\n";
pub const DELTA_END: u32 = u32::MAX;
/// Most links (a full snapshot and its deltas) a restore has to apply.
pub const MAX_CHAIN: usize = 16;

/// Size, CRC32C and per-segment hashes of a database file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Digests {
    pub size: u64,
    pub crc32c: u32,
    pub segments: Vec<u128>,
}

impl Digests {
    pub fn of_file(path: &Path) -> Result<Self> {
        let mut input = BufReader::with_capacity(8 * SEGMENT, std::fs::File::open(path)?);
        let mut buf = vec![0u8; SEGMENT];
        let (mut size, mut crc32c, mut segments) = (0u64, 0u32, Vec::new());
        loop {
            let n = read_full(&mut input, &mut buf)?;
            if n == 0 {
                break;
            }
            size += n as u64;
            crc32c = crc32c::crc32c_append(crc32c, &buf[..n]);
            segments.push(XxHash3_128::oneshot(&buf[..n]));
            if n < SEGMENT {
                break;
            }
        }
        Ok(Self {
            size,
            crc32c,
            segments,
        })
    }

    /// Indexes of the segments that differ from `previous` (or are new).
    pub fn changed_since<'a>(&'a self, previous: &'a Digests) -> impl Iterator<Item = usize> + 'a {
        self.segments
            .iter()
            .enumerate()
            .filter(|(i, digest)| previous.segments.get(*i) != Some(*digest))
            .map(|(i, _)| i)
    }

    fn changed_bytes(&self, previous: &Digests) -> u64 {
        self.changed_since(previous)
            .map(|i| (self.size - i as u64 * SEGMENT as u64).min(SEGMENT as u64))
            .sum()
    }
}

/// Fills `buf` unless the input ends first; returns how much it read.
fn read_full(input: &mut impl Read, buf: &mut [u8]) -> Result<usize> {
    let mut filled = 0;
    while filled < buf.len() {
        match input.read(&mut buf[filled..])? {
            0 => break,
            n => filled += n,
        }
    }
    Ok(filled)
}

pub fn delta_header(size: u64) -> Vec<u8> {
    let mut header = DELTA_MAGIC.to_vec();
    header.extend_from_slice(&size.to_le_bytes());
    header.extend_from_slice(&(SEGMENT as u32).to_le_bytes());
    header
}

/// Applies a decoded delta (`delta`, a local file) to `target`.
pub fn apply_delta(key: &str, delta: &Path, target: &Path) -> Result<()> {
    let corrupt = |what: &str| S3Error::Corrupt(format!("{key}: {what}"));
    let mut input = BufReader::new(std::fs::File::open(delta)?);
    let mut magic = vec![0u8; DELTA_MAGIC.len()];
    let mut word = [0u8; 8];
    if read_full(&mut input, &mut magic)? != magic.len() || magic != DELTA_MAGIC {
        return Err(corrupt("not a snapshot delta"));
    }
    if read_full(&mut input, &mut word)? != 8 {
        return Err(corrupt("truncated header"));
    }
    let size = u64::from_le_bytes(word);
    if read_full(&mut input, &mut word[..4])? != 4
        || u32::from_le_bytes(word[..4].try_into().expect("4 bytes")) as usize != SEGMENT
    {
        return Err(corrupt("unexpected segment size"));
    }
    let mut out = std::fs::OpenOptions::new().write(true).open(target)?;
    out.set_len(size)?;
    let mut buf = vec![0u8; SEGMENT];
    loop {
        if read_full(&mut input, &mut word[..4])? != 4 {
            return Err(corrupt("truncated (no end marker)"));
        }
        let index = u32::from_le_bytes(word[..4].try_into().expect("4 bytes"));
        if index == DELTA_END {
            break;
        }
        let start = index as u64 * SEGMENT as u64;
        if start >= size {
            return Err(corrupt("segment beyond the end of the file"));
        }
        let len = (size - start).min(SEGMENT as u64) as usize;
        if read_full(&mut input, &mut buf[..len])? != len {
            return Err(corrupt("truncated segment"));
        }
        out.seek(SeekFrom::Start(start))?;
        out.write_all(&buf[..len])?;
    }
    if read_full(&mut input, &mut word[..1])? != 0 {
        return Err(corrupt("data after the end marker"));
    }
    out.sync_all()?;
    Ok(())
}

pub fn size_and_crc(path: &Path) -> Result<(u64, u32)> {
    let mut input = std::fs::File::open(path)?;
    let mut buf = vec![0u8; 8 * SEGMENT];
    let (mut size, mut crc) = (0u64, 0u32);
    loop {
        match input.read(&mut buf)? {
            0 => return Ok((size, crc)),
            n => {
                size += n as u64;
                crc = crc32c::crc32c_append(crc, &buf[..n]);
            }
        }
    }
}

/// A snapshot uploaded for a new epoch.
#[derive(Debug, Clone)]
pub struct Published {
    pub key: String,
    pub snapshot: Snapshot,
    /// The chain below `key` when it is a delta, else empty.
    pub base: Vec<SnapshotLink>,
    /// Digests of the file the snapshot holds.
    pub digests: Digests,
}

/// Uploads the database file at `path` as `epoch`'s snapshot: a delta on
/// top of `previous` (the current snapshot and the digests of the file it
/// holds) when that is worth it, else a full snapshot.
pub fn publish(
    remote: &Remote,
    epoch: Epoch,
    path: &Path,
    previous: Option<(&EpochRecord, &Digests)>,
) -> Result<Published> {
    let current = Digests::of_file(path)?;
    if let Some((record, digests)) = previous {
        let chain = record.chain();
        if worth_a_delta(record, &chain, &current, digests) {
            let key = epoch.delta_key();
            let snapshot = remote.upload_delta(&key, path, &current, digests)?;
            return Ok(Published {
                key,
                snapshot,
                base: chain,
                digests: current,
            });
        }
    }
    let key = epoch.snapshot_key();
    let snapshot = remote.upload_snapshot(&key, path)?;
    if (snapshot.size, snapshot.crc32c) != (current.size, current.crc32c) {
        return Err(S3Error::Corrupt(format!(
            "{} changed while it was uploaded",
            path.display()
        )));
    }
    Ok(Published {
        key,
        snapshot,
        base: Vec::new(),
        digests: current,
    })
}

fn worth_a_delta(
    record: &EpochRecord,
    chain: &[SnapshotLink],
    current: &Digests,
    previous: &Digests,
) -> bool {
    let full = &chain[0];
    let deltas: u64 = chain[1..].iter().map(|link| link.stored_size).sum();
    // A chain starts on a zstd snapshot (older ones may be raw).
    let base_is_zstd = !record.snapshot_base.is_empty() || record.snapshot_zstd;
    base_is_zstd
        && chain.len() < MAX_CHAIN
        && deltas < full.stored_size
        && current.changed_bytes(previous) * 2 <= current.size
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write(path: &Path, bytes: &[u8]) {
        std::fs::write(path, bytes).unwrap();
    }

    #[test]
    fn deltas_rebuild_the_file_through_growth_and_shrinking() {
        let dir = super::super::tests::TempDir::new();
        let (old, new, delta) = (dir.db("old"), dir.db("new"), dir.db("delta"));
        let original: Vec<u8> = (0..(3 * SEGMENT + 100)).map(|i| (i % 251) as u8).collect();
        for edit in [
            |v: &mut Vec<u8>| v[SEGMENT + 5] ^= 1,
            |v: &mut Vec<u8>| v.extend(std::iter::repeat_n(7u8, 2 * SEGMENT + 3)),
            |v: &mut Vec<u8>| v.truncate(SEGMENT + 17),
            |v: &mut Vec<u8>| v.clear(),
        ] {
            let mut edited = original.clone();
            edit(&mut edited);
            write(&old, &original);
            write(&new, &edited);
            let (before, after) = (
                Digests::of_file(&old).unwrap(),
                Digests::of_file(&new).unwrap(),
            );
            let mut body = delta_header(after.size);
            for i in after.changed_since(&before) {
                let start = i * SEGMENT;
                body.extend_from_slice(&(i as u32).to_le_bytes());
                body.extend_from_slice(&edited[start..edited.len().min(start + SEGMENT)]);
            }
            body.extend_from_slice(&DELTA_END.to_le_bytes());
            write(&delta, &body);
            apply_delta("k", &delta, &old).unwrap();
            assert_eq!(std::fs::read(&old).unwrap(), edited);
            assert_eq!(size_and_crc(&old).unwrap(), (after.size, after.crc32c));
        }
    }

    #[test]
    fn malformed_deltas_are_corrupt() {
        let dir = super::super::tests::TempDir::new();
        let (target, delta) = (dir.db("t"), dir.db("d"));
        let mut truncated = delta_header(10);
        truncated.extend_from_slice(&0u32.to_le_bytes());
        truncated.extend_from_slice(&[1, 2, 3]);
        let mut beyond = delta_header(10);
        beyond.extend_from_slice(&5u32.to_le_bytes());
        for body in [b"garbage".to_vec(), delta_header(10), truncated, beyond] {
            write(&target, b"0123456789");
            write(&delta, &body);
            assert!(matches!(
                apply_delta("k", &delta, &target),
                Err(S3Error::Corrupt(_))
            ));
        }
    }
}
