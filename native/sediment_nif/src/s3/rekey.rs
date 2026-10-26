//! Key rotation: `Sediment.S3.rekey/3` and `Sediment.S3.release_key/4`.
//!
//! Both are a writer open (`super::prepare_fresh`) that changes what the
//! new epoch is, followed by a clean close. A rekey re-encrypts the folded
//! database page by page under the new key (checked against the source)
//! and publishes it as a full snapshot: the new epoch, and every later one,
//! is under the new key, while the retained epochs keep theirs. A release
//! drops the retained epochs under a key from the history, and GC deletes
//! their objects. Either switch is the epoch-advance manifest PUT,
//! `If-Match` on this open's takeover, and a crash before it leaves the
//! database as it was. Any result but success (fenced, indeterminate, a
//! store error) is resolved only by running the operation again: its
//! takeover ends a PUT still in flight.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use turso_core::{PlatformIO, IO};

use super::keys::{check_id, Key};
use super::{
    destroy, layout::Epoch, prepare_fresh, refuse_symlink, restore, Fresh, Op, Result, S3Config,
    S3Error,
};

/// Result of [`rekey`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Rekeyed {
    /// The database was under the key already: nothing changed.
    Current,
    Done {
        from: String,
        to: String,
        /// The first epoch under the new key.
        epoch: String,
    },
}

/// Result of [`release_key`]: the epochs dropped from the history.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Released {
    pub epochs: Vec<String>,
}

/// Moves the database at `cfg`'s prefix to the key `to` of `cfg`'s ring (the
/// active key without it), through the local working copy at `local_path`,
/// which it leaves warm under the new key.
pub fn rekey(
    cfg: &S3Config,
    local_path: &Path,
    to: Option<&str>,
    force: bool,
    allow_reuse: bool,
) -> Result<Rekeyed> {
    rekey_with(cfg, local_path, to, force, allow_reuse, true)
}

/// [`rekey`], removing the working copy once done unless
/// `keep_working_copy` (under the lease, see [`discard`]).
pub fn rekey_with(
    cfg: &S3Config,
    local_path: &Path,
    to: Option<&str>,
    force: bool,
    allow_reuse: bool,
    keep_working_copy: bool,
) -> Result<Rekeyed> {
    if let Some(to) = to {
        check_id(to)?;
    }
    let op = Op::Rekey {
        to: to.map(str::to_string),
        force,
        allow_reuse,
        keep_working_copy,
    };
    match maintain(cfg, local_path, &op)? {
        Fresh::Unchanged => Ok(Rekeyed::Current),
        Fresh::Opened {
            storage, rekeyed, ..
        } => {
            let epoch = storage.info().epoch;
            let discarded = discard(&storage, cfg, keep_working_copy);
            close(storage, cfg);
            discarded?;
            let (from, to) = rekeyed.expect("a rekey that opened switched keys");
            Ok(Rekeyed::Done { from, to, epoch })
        }
    }
}

/// Drops the retained epochs encrypted under `key` from the database's
/// history (point-in-time restore no longer reaches them), so the key can
/// leave the ring. Refuses the current key.
pub fn release_key(cfg: &S3Config, local_path: &Path, key: &str, force: bool) -> Result<Released> {
    release_key_with(cfg, local_path, key, force, true)
}

/// [`release_key`], removing the working copy once done unless
/// `keep_working_copy` (under the lease, see [`discard`]).
pub fn release_key_with(
    cfg: &S3Config,
    local_path: &Path,
    key: &str,
    force: bool,
    keep_working_copy: bool,
) -> Result<Released> {
    check_id(key)?;
    let op = Op::Release {
        key: key.to_string(),
        force,
        keep_working_copy,
    };
    match maintain(cfg, local_path, &op)? {
        Fresh::Unchanged => Ok(Released { epochs: Vec::new() }),
        Fresh::Opened {
            storage, released, ..
        } => {
            // Under the lease: the released epochs' objects (and any earlier
            // leftovers) go now, or the release reports what is left.
            let left = storage.sweep(&cfg.place());
            let discarded = match left {
                Ok(0) => discard(&storage, cfg, keep_working_copy),
                _ => Ok(()),
            };
            close(storage, cfg);
            let epochs = released.iter().map(Epoch::to_string).collect();
            match left? {
                0 => discarded.map(|()| Released { epochs }),
                objects => Err(S3Error::Leftover { epochs, objects }),
            }
        }
    }
}

fn maintain(cfg: &S3Config, local_path: &Path, op: &Op) -> Result<Fresh> {
    (|| {
        refuse_symlink(local_path)?;
        cfg.validate()?;
        if cfg.replica {
            return Err(S3Error::Config(
                "rekey and release_key can't run with mode: :replica".into(),
            ));
        }
        destroy::wait_until_closed(cfg)?;
        destroy::wait_until_file_closed(local_path)?;
        let _log = crate::log_guard::claim(local_path, crate::log_guard::Use::New)
            .map_err(S3Error::Config)?;
        let io: Arc<dyn IO> = Arc::new(PlatformIO::new()?);
        prepare_fresh(cfg, local_path, io, op)
    })()
    .map_err(|err| err.at(local_path))
}

/// A successful run asked not to keep the working copy removes it here,
/// before [`close`] gives the lease up (another VM may take the lease once
/// it is free, and open the same path), its background work done first.
fn discard(storage: &super::S3DurableStorage, cfg: &S3Config, keep: bool) -> Result<()> {
    if keep {
        return Ok(());
    }
    storage.wait_background(cfg.close_timeout);
    storage.discard_working_copy(&cfg.place())
}

/// A clean close: the garbage collection the open started (the old key's
/// objects after a release), then the storage's Drop, which releases the
/// lease and leaves the warm sidecar.
fn close(storage: Arc<super::S3DurableStorage>, cfg: &S3Config) {
    storage.wait_background(cfg.close_timeout);
    drop(storage);
}

/// Replaces the folded database at `path` (log checkpointed, WAL empty)
/// under `from` with the same pages under `to`: each page is decrypted with
/// the old key and encrypted with the new one into a file beside it. Before
/// it replaces the database, the new file must open under the new key and
/// pass `PRAGMA integrity_check`, and every page's plaintext must be the
/// old file's. Both keys use the same cipher, so the pages' layout (the
/// reserved bytes) is unchanged.
pub(crate) fn reencrypt(path: &Path, from: &Key, to: &Key) -> Result<()> {
    if from.cipher() != to.cipher() {
        return Err(S3Error::Config("rekey: a rekey keeps the cipher".into()));
    }
    // Folded: the file alone is the database. Never drop a log or WAL
    // with something in it.
    for leftover in [restore::wal_path(path), restore::log_path(path)] {
        if std::fs::metadata(&leftover).is_ok_and(|meta| meta.len() > 0) {
            return Err(failed(format!(
                "{} wasn't folded into the database",
                leftover.display()
            )));
        }
    }
    let copy = Copy(sibling(path));
    copy.remove();
    // The copy beside it (the database's size again), as an import checks.
    super::import::check_free_space(&copy.0, std::fs::metadata(path)?.len())?;
    let page_size = page_size(path)?;
    let (from_ctx, to_ctx) = (from.page_context(page_size)?, to.page_context(page_size)?);
    let source = pages(
        path,
        page_size,
        |id, page| {
            let plain = from_ctx.decrypt_page(page, id).map_err(|e| {
                failed(format!(
                    "page {id} doesn't decrypt with key {}: {e}",
                    super::shown(from.id())
                ))
            })?;
            let encrypted = to_ctx
                .encrypt_page(&plain, id)
                .map_err(|e| failed(format!("page {id}: {e}")))?;
            Ok((hash(&plain), Some(encrypted)))
        },
        Some(&copy.0),
    )?;
    // turso reads it under the new key; the checkpoint leaves anything its
    // open wrote in the file, so the comparison below sees the final pages.
    let mut check = Vec::new();
    super::with_local_connection(&copy.0, Some(&to.opts()), |conn| {
        if let Some(mut stmt) = conn.query("PRAGMA integrity_check")? {
            check = stmt.run_collect_rows()?;
        }
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    })?;
    let check: Vec<String> = check.iter().map(|row| row[0].to_string()).collect();
    if check != ["ok"] {
        return Err(failed(format!(
            "integrity_check of the re-encrypted database reports {}",
            check.join("; ")
        )));
    }
    if std::fs::metadata(restore::log_path(&copy.0)).is_ok_and(|meta| meta.len() > 0) {
        return Err(failed(
            "the re-encrypted database's log wasn't checkpointed".into(),
        ));
    }
    let copied = pages(
        &copy.0,
        page_size,
        |id, page| {
            let plain = to_ctx.decrypt_page(page, id).map_err(|e| {
                failed(format!(
                    "page {id} doesn't decrypt with key {}: {e}",
                    super::shown(to.id())
                ))
            })?;
            Ok((hash(&plain), None))
        },
        None,
    )?;
    if copied != source {
        let first = copied
            .iter()
            .zip(&source)
            .position(|(a, b)| a != b)
            .unwrap_or(copied.len().min(source.len()));
        return Err(failed(format!(
            "the re-encrypted database differs from the database at page {} ({} pages, {} \
             before)",
            first + 1,
            copied.len(),
            source.len()
        )));
    }
    restore::remove_if_exists(&restore::wal_path(path))?;
    restore::remove_if_exists(&restore::log_path(path))?;
    std::fs::rename(&copy.0, path)?;
    if let Some(dir) = path.parent().filter(|dir| !dir.as_os_str().is_empty()) {
        std::fs::File::open(dir)?.sync_all()?;
    }
    Ok(())
}

fn failed(why: String) -> S3Error {
    S3Error::Config(format!("rekey: {why}"))
}

fn hash(page: &[u8]) -> u128 {
    twox_hash::XxHash3_128::oneshot(page)
}

/// The page size in the database header (bytes 16-17 of page 1, outside
/// the encrypted part; 1 means 65536).
fn page_size(path: &Path) -> Result<usize> {
    use std::io::Read;
    let mut header = [0u8; 100];
    std::fs::File::open(path)?.read_exact(&mut header)?;
    let size = match u16::from_be_bytes([header[16], header[17]]) {
        1 => 65536,
        n => n as usize,
    };
    if !(512..=65536).contains(&size) || !size.is_power_of_two() {
        return Err(failed(format!(
            "invalid page size {size} in the database header"
        )));
    }
    Ok(size)
}

/// Runs `each` on every page of the file at `path` (ids from 1), writing
/// what it returns to `out`, and returns the hashes it gave.
fn pages(
    path: &Path,
    page_size: usize,
    mut each: impl FnMut(usize, &[u8]) -> Result<(u128, Option<Vec<u8>>)>,
    out: Option<&Path>,
) -> Result<Vec<u128>> {
    use std::io::{Read, Write};
    let mut input = std::io::BufReader::new(std::fs::File::open(path)?);
    let len = std::fs::metadata(path)?.len();
    if len % page_size as u64 != 0 {
        return Err(failed(format!(
            "the database file's size {len} isn't a multiple of its page size {page_size}"
        )));
    }
    let mut output = out
        .map(|out| -> Result<_> {
            let mut options = std::fs::OpenOptions::new();
            options.write(true).create_new(true);
            #[cfg(unix)]
            std::os::unix::fs::OpenOptionsExt::mode(&mut options, 0o600);
            Ok(std::io::BufWriter::new(options.open(out)?))
        })
        .transpose()?;
    let mut page = vec![0u8; page_size];
    let mut hashes = Vec::with_capacity((len / page_size as u64) as usize);
    for id in 1..=(len / page_size as u64) as usize {
        input.read_exact(&mut page)?;
        let (hash, written) = each(id, &page)?;
        hashes.push(hash);
        if let (Some(output), Some(written)) = (output.as_mut(), written) {
            output.write_all(&written)?;
        }
    }
    if let Some(output) = output {
        output
            .into_inner()
            .map_err(|err| S3Error::Io(err.into_error()))?
            .sync_all()?;
    }
    Ok(hashes)
}

/// The bytes a rekey (`rekey`) or a release needs free beside `local_path`,
/// margin included: the restore (its peak, unless the working copy is warm,
/// plus the epoch's log), and for a rekey its copy (the snapshot plus the
/// log it may grow by). Warm, here, is a sidecar naming the manifest's
/// database, generation, epoch and key, beside a file of the snapshot's
/// size (the open checks more: a copy it then refuses downloads anyway, and
/// the copy's own check after the restore catches what that costs).
pub(crate) fn space_needed(
    remote: &super::remote::Remote,
    manifest: &super::layout::Manifest,
    local_path: &Path,
    rekey: bool,
) -> Result<u64> {
    let log: u64 = remote
        .list(&manifest.epoch.log_dir())?
        .iter()
        .map(|object| object.size)
        .sum();
    let warm = super::warm::read(local_path).is_some_and(|sidecar| {
        manifest.database_id.as_deref() == Some(sidecar.database_id.as_str())
            && manifest.generation == sidecar.generation
            && manifest.epoch.to_string() == sidecar.epoch
            && manifest.key.as_ref().map(|key| &key.fingerprint) == sidecar.key_fingerprint.as_ref()
            && std::fs::metadata(local_path).is_ok_and(|meta| meta.len() == manifest.snapshot_size)
    });
    let restore = if warm {
        log
    } else {
        chain_peak(&manifest.current().chain()) + log
    };
    let copy = if rekey {
        manifest.snapshot_size + log
    } else {
        0
    };
    Ok(super::import::with_margin(restore + copy))
}

/// The most a cold restore of `chain` holds on disk at once: a full
/// snapshot alone is its size; for a delta chain, at each delta, the image
/// rebuilt so far (as large as before or after it) and the decoded delta
/// staged beside it. A delta's decoded size isn't recorded: it is bounded
/// by the database's size after it (its pages, page count times page size,
/// are what a delta can change).
pub(crate) fn chain_peak(chain: &[super::layout::SnapshotLink]) -> u64 {
    let base = chain.first().map_or(0, |link| link.size);
    chain
        .windows(2)
        .map(|pair| pair[0].size.max(pair[1].size) + pair[1].size)
        .fold(base, u64::max)
}

/// The bytes reserved by the rekeys and releases of this VM between their
/// space check and the end of their restore and copy: each check counts
/// the others' (bytes they wrote already count twice: conservative).
static RESERVED: std::sync::Mutex<u64> = std::sync::Mutex::new(0);

/// A reservation of [`RESERVED`], given back when dropped.
pub(crate) struct Reservation(u64);

impl Drop for Reservation {
    fn drop(&mut self) {
        let mut reserved = RESERVED.lock().unwrap_or_else(|e| e.into_inner());
        *reserved = reserved.saturating_sub(self.0);
    }
}

/// Refuses a rekey or release at `local_path` before it downloads anything
/// when [`space_needed`], plus what the others running in this VM reserved,
/// is more than is free there; else reserves it until the guard drops.
pub(crate) fn check_space(needed: u64, local_path: &Path, what: &str) -> Result<Reservation> {
    let mut reserved = RESERVED.lock().unwrap_or_else(|e| e.into_inner());
    admit(
        &mut reserved,
        super::import::free_space(local_path)?,
        needed,
        what,
    )?;
    Ok(Reservation(needed))
}

/// Adds `needed` to `reserved` if `free` (the directory, and its free bytes;
/// `None`: unknown) holds it besides what is reserved already.
fn admit(reserved: &mut u64, free: Option<(PathBuf, u64)>, needed: u64, what: &str) -> Result<()> {
    if let Some((dir, free)) = free {
        if free < *reserved + needed {
            let others = if *reserved > 0 {
                format!(
                    ", and {} reserved by the others running in this VM",
                    *reserved
                )
            } else {
                String::new()
            };
            return Err(S3Error::Config(format!(
                "{what} needs about {needed} bytes free in {} (the restore, the copy and a \
                 margin; see Sediment.S3.space_needed/3{others}), only {free} are",
                dir.display()
            )));
        }
    }
    *reserved += needed;
    Ok(())
}

/// [`space_needed`] for `Sediment.S3.space_needed/3`: the manifest read
/// without the lease. `None`: no database.
pub fn space_needed_at(cfg: &S3Config, local_path: &Path, rekey: bool) -> Result<Option<u64>> {
    cfg.validate()?;
    let remote = cfg.remote()?;
    match super::restore::read_manifest(&remote)? {
        Some(manifest) => space_needed(&remote, &manifest, local_path, rekey).map(Some),
        None => Ok(None),
    }
}

/// Removes the working copy at `path` after a rekey or release that was
/// asked not to keep it (`keep_working_copy: false`): the file, its WAL and
/// log, and the warm sidecar. Only under the file's lock.
pub(crate) fn drop_working_copy(path: &Path) -> Result<()> {
    super::warm::discard(path);
    for file in [
        restore::wal_path(path),
        restore::log_path(path),
        path.to_path_buf(),
    ] {
        restore::remove_if_exists(&file).map_err(|err| err.at(&file))?;
    }
    Ok(())
}

/// Removes the copy a rekey killed while running left beside `path` (a
/// database's size). Only under the file's lock, which a running rekey of
/// it holds, and the lease, which a rekey in another process holds.
pub(crate) fn remove_stale_copy(path: &Path) {
    Copy(sibling(path)).remove();
}

/// `.<name>.rekey.db` next to the database.
fn sibling(path: &Path) -> PathBuf {
    let name = path
        .file_name()
        .map(|s| s.to_string_lossy().into_owned())
        .unwrap_or_default();
    path.with_file_name(format!(".{name}.rekey.db"))
}

/// The copy's files, removed when dropped (after the rename only its empty
/// WAL and log are left).
struct Copy(PathBuf);

impl Copy {
    fn remove(&self) {
        for path in [
            self.0.clone(),
            restore::wal_path(&self.0),
            restore::log_path(&self.0),
        ] {
            let _ = std::fs::remove_file(path);
        }
    }
}

impl Drop for Copy {
    fn drop(&mut self) {
        self.remove();
    }
}

#[cfg(test)]
mod space_tests {
    use super::{admit, chain_peak};
    use crate::s3::layout::SnapshotLink;
    use std::path::PathBuf;

    fn link(size: u64) -> SnapshotLink {
        SnapshotLink {
            key: String::new(),
            stored_size: 0,
            size,
            crc32c: 0,
        }
    }

    #[test]
    fn a_chain_peaks_at_an_image_and_its_staged_delta() {
        const GIB: u64 = 1 << 30;
        assert_eq!(chain_peak(&[link(GIB)]), GIB);
        assert_eq!(chain_peak(&[link(GIB), link(GIB)]), 2 * GIB);
        // An earlier, larger image: it is there while the delta is staged.
        assert_eq!(chain_peak(&[link(2 * GIB), link(GIB), link(GIB)]), 3 * GIB);
        assert_eq!(chain_peak(&[link(GIB), link(3 * GIB)]), 6 * GIB);
    }

    #[test]
    fn admission_counts_what_others_reserved() {
        let dir = || Some((PathBuf::from("/data"), 100));
        let mut reserved = 0;
        admit(&mut reserved, dir(), 60, "rekey").unwrap();
        let err = admit(&mut reserved, dir(), 60, "rekey")
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("rekey needs about 60 bytes free in /data")
                && err.contains("and 60 reserved by the others running in this VM")
                && err.contains("only 100 are"),
            "{err}"
        );
        assert_eq!(reserved, 60);
        admit(&mut reserved, dir(), 40, "release_key").unwrap();
        assert_eq!(reserved, 100);
        admit(&mut reserved, None, 1 << 40, "rekey").unwrap();
    }
}
