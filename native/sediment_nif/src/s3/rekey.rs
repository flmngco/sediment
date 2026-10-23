//! Key rotation: `Sediment.S3.rekey/3` and `Sediment.S3.release_key/4`.
//!
//! Both are a writer open (`super::prepare_fresh`) that changes what the
//! new epoch is, followed by a clean close. A rekey re-encrypts the folded
//! database under the new key (the import's logical copy, checked against
//! the source) and publishes it as a full snapshot: the new epoch, and every
//! later one, is under the new key, while the retained epochs keep theirs.
//! A release drops the retained epochs under a key from the history, and GC
//! deletes their objects. Either switch is the epoch-advance manifest PUT,
//! `If-Match` on this open's takeover: a writer that took over since gets
//! the operation fenced with nothing switched, and a crash before it leaves
//! the database as it was (a re-run starts over).

use std::path::{Path, PathBuf};
use std::sync::Arc;

use turso_core::{PlatformIO, IO};

use super::keys::Key;
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
    let op = Op::Rekey {
        to: to.map(str::to_string),
        force,
        allow_reuse,
    };
    match maintain(cfg, local_path, &op)? {
        Fresh::Unchanged => Ok(Rekeyed::Current),
        Fresh::Opened {
            storage, rekeyed, ..
        } => {
            let epoch = storage.info().epoch;
            close(storage, cfg);
            let (from, to) = rekeyed.expect("a rekey that opened switched keys");
            Ok(Rekeyed::Done { from, to, epoch })
        }
    }
}

/// Drops the retained epochs encrypted under `key` from the database's
/// history (point-in-time restore no longer reaches them), so the key can
/// leave the ring. Refuses the current key.
pub fn release_key(cfg: &S3Config, local_path: &Path, key: &str, force: bool) -> Result<Released> {
    let op = Op::Release {
        key: key.to_string(),
        force,
    };
    match maintain(cfg, local_path, &op)? {
        Fresh::Unchanged => Ok(Released { epochs: Vec::new() }),
        Fresh::Opened {
            storage, released, ..
        } => {
            close(storage, cfg);
            Ok(Released {
                epochs: released.iter().map(Epoch::to_string).collect(),
            })
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
        let _log = crate::log_guard::claim(local_path, crate::log_guard::Use::New)
            .map_err(S3Error::Config)?;
        let io: Arc<dyn IO> = Arc::new(PlatformIO::new()?);
        prepare_fresh(cfg, local_path, io, op)
    })()
    .map_err(|err| err.at(local_path))
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
    let copy = Copy(sibling(path));
    copy.remove();
    let page_size = page_size(path)?;
    let (from_ctx, to_ctx) = (from.page_context(page_size)?, to.page_context(page_size)?);
    let source = pages(
        path,
        page_size,
        |id, page| {
            let plain = from_ctx.decrypt_page(page, id).map_err(|e| {
                failed(format!(
                    "page {id} doesn't decrypt with key {}: {e}",
                    from.id()
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
                    to.id()
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
