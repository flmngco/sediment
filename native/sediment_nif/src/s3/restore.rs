//! Rebuilding the local database files from S3.

use std::io::Write;
use std::path::{Path, PathBuf};

use bytes::Bytes;

use super::error::{Result, S3Error};
use super::layout::{parse_segment_key, Epoch, EpochRecord, Manifest, SEAL_MAGIC};
use super::logfmt::{verify_segments_from, LogState};
use super::remote::Remote;

/// Local path of turso's logical log for `db_path` (`app.db` -> `app.db-log`).
pub fn log_path(db_path: &Path) -> PathBuf {
    db_path.with_extension("db-log")
}

pub fn wal_path(db_path: &Path) -> PathBuf {
    let mut path = db_path.as_os_str().to_owned();
    path.push("-wal");
    PathBuf::from(path)
}

/// Which state a restore should produce.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Target {
    /// The current state (the manifest's epoch and all of its log).
    Latest,
    /// The start of the given epoch (its snapshot), plus its whole log.
    Epoch(u64),
    /// The state as of this unix time in ms: the newest retained epoch that
    /// started by then, plus its frames that had landed by then.
    Time(u64),
}

/// A retained epoch to restore, and an optional cutoff for its log.
#[derive(Debug, Clone)]
pub struct Point {
    pub record: EpochRecord,
    pub cutoff_ms: Option<u64>,
}

pub fn choose(manifest: &Manifest, target: Target) -> Result<Point> {
    let pick = |ok: &dyn Fn(&EpochRecord) -> bool| {
        manifest
            .retained()
            .filter(|r| ok(r))
            .max_by_key(|r| r.epoch.seq)
    };
    let (record, cutoff_ms) = match target {
        Target::Latest => (Some(manifest.current()), None),
        Target::Epoch(seq) => (pick(&|r| r.epoch.seq == seq), None),
        Target::Time(at) => (pick(&|r| r.started_at_ms <= at), Some(at)),
    };
    let record = record.ok_or_else(|| {
        S3Error::Config(format!(
            "{target:?} is not retained (retained epochs: {:?}); raise retain_epochs",
            manifest.retained().map(|r| r.epoch.seq).collect::<Vec<_>>()
        ))
    })?;
    Ok(Point { record, cutoff_ms })
}

/// Downloads and verifies the segments of `epoch` past `start`. With a cutoff,
/// only the contiguous run of segments that landed by then is used.
pub fn fetch_log_from(
    remote: &Remote,
    epoch: Epoch,
    start: LogState,
    cutoff_ms: Option<u64>,
    concurrency: usize,
) -> Result<(Vec<(u64, Bytes)>, LogState)> {
    let mut listed: Vec<(u64, String, u64, u64)> = remote
        .list(&epoch.log_dir())?
        .into_iter()
        .map(|object| match parse_segment_key(&object.key) {
            Some((found, offset)) if found == epoch => {
                Ok((offset, object.key, object.size, object.modified_ms))
            }
            _ => Err(S3Error::Corrupt(format!(
                "unexpected object {}",
                object.key
            ))),
        })
        .collect::<Result<_>>()?;
    listed.sort();
    listed.retain(|(offset, ..)| *offset >= start.len);
    if let Some(cutoff) = cutoff_ms {
        let mut next = start.len;
        let mut prefix = Vec::new();
        for item in listed {
            // Segments land in offset order, so the first one newer than the
            // cutoff ends the restore point.
            if item.3 > cutoff {
                break;
            }
            // A gap before the cutoff is missing data, not a time boundary.
            if item.0 != next {
                return Err(S3Error::Corrupt(format!(
                    "log gap before the restore point: expected segment at offset {next}, found {}",
                    item.0
                )));
            }
            next += item.2;
            prefix.push(item);
        }
        listed = prefix;
    }
    let keys = listed.iter().map(|(_, key, ..)| key.clone()).collect();
    let bodies = remote.get_many(keys, concurrency)?;
    let segments: Vec<(u64, Bytes)> = listed
        .into_iter()
        .map(|(offset, ..)| offset)
        .zip(bodies)
        .collect();
    let state = verify_segments_from(start, &segments, remote.log_encryption())?;
    Ok((segments, state))
}

/// Replaces the local database with the manifest's snapshot plus its log.
pub fn restore(
    remote: &Remote,
    manifest: &Manifest,
    db_path: &Path,
    concurrency: usize,
) -> Result<LogState> {
    let point = Point {
        record: manifest.current(),
        cutoff_ms: None,
    };
    restore_point(remote, &point, db_path, concurrency)
}

/// Like [`restore`], rebuilding the snapshot from `start` (a local file
/// holding the database as the first `n` links of its chain rebuild it)
/// instead of downloading those links.
pub fn restore_from(
    remote: &Remote,
    manifest: &Manifest,
    db_path: &Path,
    concurrency: usize,
    start: Option<(usize, &Path)>,
) -> Result<LogState> {
    let point = Point {
        record: manifest.current(),
        cutoff_ms: None,
    };
    restore_point_from(remote, &point, db_path, concurrency, start)
}

/// Replaces the local database with `point`'s snapshot plus its log.
pub fn restore_point(
    remote: &Remote,
    point: &Point,
    db_path: &Path,
    concurrency: usize,
) -> Result<LogState> {
    restore_point_from(remote, point, db_path, concurrency, None)
}

fn restore_point_from(
    remote: &Remote,
    point: &Point,
    db_path: &Path,
    concurrency: usize,
    start: Option<(usize, &Path)>,
) -> Result<LogState> {
    let (segments, state) = fetch_log_from(
        remote,
        point.record.epoch,
        LogState::default(),
        point.cutoff_ms,
        concurrency,
    )?;
    remove_if_exists(&wal_path(db_path))?;
    if start.is_some() {
        remote.download_chain_from(&point.record.chain(), db_path, start)?;
    } else if point.record.snapshot_base.is_empty() {
        let expected = point.record.snapshot_info();
        remote.download_snapshot(&point.record.snapshot, expected, db_path)?;
    } else {
        remote.download_chain(&point.record.chain(), db_path)?;
    }
    write_log(&log_path(db_path), &segments, false)?;
    Ok(state)
}

/// Writes (or appends) log segments, leaving out a seal.
pub fn write_log(path: &Path, segments: &[(u64, Bytes)], append: bool) -> Result<()> {
    let mut log = std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .append(append)
        .truncate(!append)
        .open(path)?;
    for (_, bytes) in segments.iter().filter(|(_, b)| !b.starts_with(SEAL_MAGIC)) {
        log.write_all(bytes)?;
    }
    log.sync_all()?;
    Ok(())
}

pub fn remove_if_exists(path: &Path) -> Result<()> {
    match std::fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(err.into()),
    }
}
