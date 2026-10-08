//! Warm reopen: an open whose local working copy provably matches S3 skips
//! the snapshot download and fetches only the log past the copy.
//!
//! A clean close leaves a sidecar (`.<name>.s3-warm`) describing the copy:
//! the database and epoch it belongs to, the writer's lease generation, how
//! much of the epoch's log it holds (length and the CRC chain's end there),
//! and the database file's size and CRC32C. The next open takes the sidecar
//! (reads and deletes it) before anything touches the local files, so an
//! open that crashes leaves none behind. After its takeover it reuses the
//! copy only if the manifest still names the same database at the same epoch
//! the same writer left, the local files are what the sidecar says (their
//! SHA-256), the file is the epoch's snapshot (its size and CRC32C, which the
//! manifest records), and S3's log holds the local log as a prefix: objects
//! cover it from offset 0 without a gap, and the last one ends where it does
//! with the same bytes (so the copy is never ahead of S3, as after async
//! commits that never uploaded). Anything else, including any error while
//! reusing, falls back to the full restore.

use std::io::Write;
use std::path::{Path, PathBuf};

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use super::config::S3Config;
use super::error::Result;
use super::layout::{parse_segment_key, Manifest};
use super::logfmt::{verify_segments_from, LogState};
use super::remote::Remote;
use super::restore;

/// 2: the files' SHA-256. A sidecar of another version is never reused.
const VERSION: u32 = 2;

/// What a clean close records about the local working copy.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Sidecar {
    pub version: u32,
    pub database_id: String,
    pub generation: u64,
    pub epoch: String,
    pub log_len: u64,
    pub log_end_crc: Option<u32>,
    pub db_size: u64,
    pub db_crc32c: u32,
    /// SHA-256 (hex) of the database file and of the local log: CRC32C
    /// catches damage, not a change made to keep it.
    pub db_sha256: String,
    pub log_sha256: String,
    pub encrypted: bool,
    pub cipher: Option<String>,
}

pub fn sidecar_path(db_path: &Path) -> PathBuf {
    let name = db_path
        .file_name()
        .map(|s| s.to_string_lossy().into_owned())
        .unwrap_or_default();
    db_path.with_file_name(format!(".{name}.s3-warm"))
}

fn dir_of(path: &Path) -> &Path {
    match path.parent() {
        Some(dir) if !dir.as_os_str().is_empty() => dir,
        _ => Path::new("."),
    }
}

fn sync_dir(path: &Path) {
    if let Ok(dir) = std::fs::File::open(dir_of(path)) {
        let _ = dir.sync_all();
    }
}

/// Reads and deletes the sidecar of `db_path`. Whatever it held, it is gone
/// once this returns (or the removal failed and nothing may be reused).
pub fn take(db_path: &Path) -> Option<Sidecar> {
    let path = sidecar_path(db_path);
    let bytes = std::fs::read(&path).ok();
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(tmp_path(&path));
    if path.exists() {
        return None;
    }
    sync_dir(&path);
    bytes.and_then(|bytes| serde_json::from_slice::<Sidecar>(&bytes).ok())
}

fn tmp_path(sidecar: &Path) -> PathBuf {
    let mut name = sidecar.as_os_str().to_owned();
    name.push(".tmp");
    PathBuf::from(name)
}

/// The local log of `db_path`, verified from its header: its bytes and the
/// chain's state at its end.
fn local_log(db_path: &Path, encryption: Option<(usize, usize)>) -> Result<(Bytes, LogState)> {
    let bytes: Bytes = match std::fs::read(restore::log_path(db_path)) {
        Ok(bytes) => bytes.into(),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Bytes::new(),
        Err(err) => return Err(err.into()),
    };
    let state = if bytes.is_empty() {
        LogState::default()
    } else {
        verify_segments_from(LogState::default(), &[(0, bytes.clone())], encryption)?
    };
    Ok((bytes, state))
}

fn sha256(bytes: &[u8]) -> String {
    hex(aws_lc_rs::digest::digest(&aws_lc_rs::digest::SHA256, bytes).as_ref())
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// The file's size, CRC32C and SHA-256.
fn file_digest(path: &Path) -> Result<(u64, u32, String)> {
    use std::io::Read;
    let mut file = std::fs::File::open(path)?;
    let mut crc = 0u32;
    let mut sha = aws_lc_rs::digest::Context::new(&aws_lc_rs::digest::SHA256);
    let mut size = 0u64;
    let mut buf = vec![0u8; 1 << 20];
    loop {
        let n = file.read(&mut buf)?;
        if n == 0 {
            return Ok((size, crc, hex(sha.finish().as_ref())));
        }
        crc = crc32c::crc32c_append(crc, &buf[..n]);
        sha.update(&buf[..n]);
        size += n as u64;
    }
}

/// What a clean close knows about the copy it leaves.
pub struct Closing<'a> {
    pub db_path: &'a Path,
    pub manifest: &'a Manifest,
    pub generation: u64,
    pub log_offset: u64,
    pub encryption: Option<&'a turso_core::EncryptionOpts>,
    pub log_encryption: Option<(usize, usize)>,
}

/// Writes the sidecar after a clean close: the database file and its log are
/// synced first, and the sidecar lands atomically (temp file, sync, rename,
/// directory sync). Writes nothing when the copy isn't the manifest's
/// current state (the file isn't the epoch's snapshot, or the log isn't
/// what the writer uploaded).
pub fn write(closing: &Closing) -> Result<bool> {
    let Some(database_id) = closing.manifest.database_id.clone() else {
        return Ok(false);
    };
    for path in [
        closing.db_path.to_path_buf(),
        restore::log_path(closing.db_path),
    ] {
        if path.exists() {
            std::fs::OpenOptions::new()
                .write(true)
                .open(&path)?
                .sync_all()?;
        }
    }
    let (db_size, db_crc32c, db_sha256) = file_digest(closing.db_path)?;
    let manifest = closing.manifest;
    if (db_size, db_crc32c) != (manifest.snapshot_size, manifest.snapshot_crc32c) {
        return Ok(false);
    }
    let (log_bytes, log) = local_log(closing.db_path, closing.log_encryption)?;
    if log.len != closing.log_offset || log.sealed {
        return Ok(false);
    }
    let sidecar = Sidecar {
        version: VERSION,
        database_id,
        generation: closing.generation,
        epoch: manifest.epoch.to_string(),
        log_len: log.len,
        log_end_crc: log.end_crc,
        db_size,
        db_crc32c,
        db_sha256,
        log_sha256: sha256(&log_bytes),
        encrypted: closing.encryption.is_some(),
        cipher: closing.encryption.map(|e| e.cipher.clone()),
    };
    let path = sidecar_path(closing.db_path);
    let tmp = tmp_path(&path);
    let mut file = std::fs::File::create(&tmp)?;
    file.write_all(&serde_json::to_vec(&sidecar)?)?;
    file.sync_all()?;
    drop(file);
    std::fs::rename(&tmp, &path)?;
    sync_dir(&path);
    Ok(true)
}

/// Whether the copy at `db_path` may be reused for `manifest` (as it was
/// before this open's takeover): returns the local log's state to continue
/// from, or why not.
pub fn check(
    sidecar: &Sidecar,
    manifest: &Manifest,
    cfg: &S3Config,
    remote: &Remote,
    db_path: &Path,
) -> std::result::Result<LogState, String> {
    if sidecar.version != VERSION {
        return Err(format!("sidecar version {}", sidecar.version));
    }
    if manifest.database_id.as_deref() != Some(sidecar.database_id.as_str()) {
        return Err("another database".into());
    }
    if manifest.generation != sidecar.generation || manifest.epoch.to_string() != sidecar.epoch {
        return Err("S3 moved on since the copy was closed".into());
    }
    let cipher = cfg.encryption.as_ref().map(|e| e.cipher.clone());
    if (sidecar.encrypted, &sidecar.cipher) != (cfg.encryption.is_some(), &cipher) {
        return Err("another encryption choice".into());
    }
    let (size, crc, sha) = file_digest(db_path).map_err(|e| e.to_string())?;
    if (size, crc, &sha) != (sidecar.db_size, sidecar.db_crc32c, &sidecar.db_sha256)
        || (size, crc) != (manifest.snapshot_size, manifest.snapshot_crc32c)
    {
        return Err("the database file is not the epoch's snapshot".into());
    }
    let (bytes, log) = local_log(db_path, remote.log_encryption()).map_err(|e| e.to_string())?;
    if log.len != sidecar.log_len
        || log.end_crc != sidecar.log_end_crc
        || log.sealed
        || sha256(&bytes) != sidecar.log_sha256
    {
        return Err("the local log is not the one the copy was closed with".into());
    }
    if log.len > 0 {
        s3_holds(remote, manifest, &bytes)?;
    }
    Ok(log)
}

/// S3's log of the epoch holds `local` as a prefix: its objects cover the
/// log from offset 0 without a gap or an overlap, and the last of them ends
/// exactly where the local log does, with the same bytes. Those carry the
/// chain's CRCs, which a different history before them wouldn't produce.
fn s3_holds(
    remote: &Remote,
    manifest: &Manifest,
    local: &Bytes,
) -> std::result::Result<(), String> {
    let end = local.len() as u64;
    let mut below = Vec::new();
    for object in remote
        .list(&manifest.epoch.log_dir())
        .map_err(|e| e.to_string())?
    {
        match parse_segment_key(&object.key) {
            Some((epoch, offset)) if epoch == manifest.epoch => {
                if offset < end {
                    below.push((offset, object.size, object.key));
                }
            }
            // The full restore refuses it too.
            _ => return Err(format!("unexpected object {}", object.key)),
        }
    }
    below.sort();
    let mut next = 0;
    for (offset, size, _) in &below {
        if *offset != next {
            return Err(format!(
                "S3's log has a gap or an overlap at offset {next} (found {offset})"
            ));
        }
        next = offset + size;
    }
    let Some((last, _, key)) = below.last().filter(|_| next == end) else {
        return Err("S3's log has no object ending where the local log does".into());
    };
    let object = remote
        .get(key)
        .map_err(|e| e.to_string())?
        .ok_or("the object ending where the local log does is gone")?;
    if object.bytes[..] != local[*last as usize..] {
        return Err("S3's log differs from the local log".into());
    }
    Ok(())
}
