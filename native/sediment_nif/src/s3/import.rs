//! Importing an existing database file into an empty prefix: its schema and
//! rows are copied into a new MVCC database (encrypted with the target key,
//! if any), which becomes epoch 0 of a new S3 database. The source is never
//! written to.
//!
//! A logical copy rather than switching a copy of the file to MVCC: in
//! turso_core 0.8.1 that switch loses AUTOINCREMENT state (the next insert
//! reuses id 1 and overwrites the row; files written by SQLite can't insert
//! at all). Rows written in MVCC keep the sequences right.

use std::collections::BTreeMap;
use std::num::NonZero;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use turso_core::{Connection, EncryptionOpts, Value};

use super::layout::{ManifestState, MANIFEST_KEY};
use super::lease::Lease;
use super::{
    check_no_old_objects, create_database, default_owner, live_storage, open_local, probe, purge,
    refuse_newer, require_encryption_choice, restore, snapshot::Digests, with_local_connection,
    Result, S3Config, S3Error,
};

/// What [`import`] checks once the database is in S3.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verify {
    /// The uploaded snapshot's size and CRC32C match the new database file
    /// (every part also carries a SHA-256 the store checks).
    Checksum,
    /// Also restore the database from S3 into a temporary file and compare.
    Restore,
}

/// Options of [`import`] besides the S3 configuration (whose `encryption`
/// is the new database's key).
#[derive(Debug, Clone)]
pub struct ImportOptions {
    pub verify: Verify,
    /// The source's key, if it is encrypted (a plaintext source is read
    /// without it).
    pub source_encryption: Option<EncryptionOpts>,
}

/// Result of [`import`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Imported {
    pub epoch: String,
    /// Size of the new database file.
    pub size: u64,
    /// Bytes stored in S3 (compressed).
    pub stored_size: u64,
    /// Tables, indexes, views and triggers imported.
    pub objects: usize,
    /// Rows copied, over all tables.
    pub rows: u64,
    /// AUTOINCREMENT tables whose sequence stayed below the source's
    /// `sqlite_sequence` value: empty tables whose constraints refuse the
    /// placeholder row (ids up to that value could be handed out again).
    pub sequences_not_advanced: Vec<String>,
}

/// S3's largest object: the snapshot (compressed) must fit.
pub(crate) const MAX_SNAPSHOT_BYTES: u64 = super::remote::MAX_OBJECT;

/// Rows per transaction of the copy.
const BATCH_ROWS: u64 = 10_000;
/// Rows per transaction for AUTOINCREMENT tables: in turso_core 0.8.1 MVCC
/// each such insert walks the transaction's own sqlite_sequence updates, so
/// a transaction costs O(rows^2) (measured by `tests::import::import_throughput`).
const AUTOINCREMENT_BATCH_ROWS: u64 = 500;

#[cfg(test)]
thread_local! {
    /// Tests and measurements: the AUTOINCREMENT batch to use instead.
    pub static AUTOINCREMENT_BATCH_OVERRIDE: std::cell::Cell<Option<u64>> = const { std::cell::Cell::new(None) };
}

/// Rows between checkpoints while copying an AUTOINCREMENT table: each such
/// insert also adds a version of the table's sqlite_sequence row, collected
/// only at a checkpoint, and every insert walks them. The automatic
/// checkpoint (at ~4 MB of log) comes too rarely for small rows.
const AUTOINCREMENT_CHECKPOINT_ROWS: u64 = 1_000;

#[cfg(test)]
thread_local! {
    /// Measurements: the checkpoint interval to use instead (0: none).
    pub static AUTOINCREMENT_CHECKPOINT_OVERRIDE: std::cell::Cell<Option<u64>> = const { std::cell::Cell::new(None) };
}

fn autoincrement_checkpoint_rows() -> u64 {
    #[cfg(test)]
    if let Some(rows) = AUTOINCREMENT_CHECKPOINT_OVERRIDE.with(|o| o.get()) {
        return rows;
    }
    AUTOINCREMENT_CHECKPOINT_ROWS
}

fn autoincrement_batch_rows() -> u64 {
    #[cfg(test)]
    if let Some(rows) = AUTOINCREMENT_BATCH_OVERRIDE.with(|o| o.get()) {
        return rows;
    }
    AUTOINCREMENT_BATCH_ROWS
}

/// Imports the database file at `source` as a new database at the (empty)
/// S3 location of `cfg`, under the writer lease. The source (with its WAL)
/// is copied next to it and read from that copy; its schema and rows are
/// copied into a new MVCC database, which is uploaded as the first snapshot,
/// and the create-only manifest PUT makes it visible. Every failure before
/// that PUT leaves nothing an open would use; a re-run then works. The
/// temporary files are deleted on every path.
pub fn import(cfg: &S3Config, source: &Path, opts: &ImportOptions) -> Result<Imported> {
    import_from(cfg, source, opts).map_err(|err| err.at(source))
}

fn import_from(cfg: &S3Config, source: &Path, opts: &ImportOptions) -> Result<Imported> {
    cfg.validate()?;
    if cfg.replica {
        return Err(S3Error::Config(
            "import creates a new database: it can't run with mode: :replica".into(),
        ));
    }
    let remote = cfg.remote()?;
    require_encryption_choice(cfg, &remote)?;
    // turso keeps a symlinked database's log next to the file it points to.
    let source = &std::fs::canonicalize(source).unwrap_or_else(|_| source.to_path_buf());
    // The source's log is copied with it: it must be the source's own.
    let _log = crate::log_guard::is_mvcc_file(source)
        .then(|| crate::log_guard::claim(source, crate::log_guard::Use::Existing))
        .transpose()
        .map_err(S3Error::Config)?;
    let size = check_source(source)?;
    let work = Work::new(source)?;
    // The source's copy, and the new database at most as large again.
    check_free_space(&work.db, 2 * size)?;
    work.copy_source(source)?;
    let copied = copy_database(
        &work,
        opts.source_encryption.as_ref(),
        cfg.encryption.as_ref(),
    )?;
    let converted = Digests::of_file(&work.db)?;
    if converted.size > MAX_SNAPSHOT_BYTES {
        return Err(too_large(source, converted.size));
    }

    if cfg.verify_conditional_writes {
        probe::verify(&remote, &cfg.store_identity())?;
    }
    let owner = cfg.owner.clone().unwrap_or_else(default_owner);
    let lease = Lease::acquire(remote.clone(), owner.clone(), cfg.lease_ttl)?;
    let generation = lease.generation();
    // An empty prefix, or one a destroy emptied (its objects purged first).
    let over = match remote.get(MANIFEST_KEY)? {
        None => None,
        Some(object) => match ManifestState::decode(&object.bytes)? {
            ManifestState::Destroyed(tombstone) => {
                refuse_newer(tombstone.generation, generation)?;
                purge(&remote, &tombstone)?;
                Some((tombstone, object.version))
            }
            ManifestState::Database(_) => return Err(already_holds(cfg)),
        },
    };
    let tombstone = over.as_ref().map(|(tombstone, _)| tombstone);
    check_no_old_objects(&remote, tombstone)?;
    let first = tombstone.map_or(0, |tombstone| tombstone.next_seq());
    let over = over
        .as_ref()
        .map(|(tombstone, version)| (tombstone, version.clone()));
    let manifest = match create_database(&remote, cfg, generation, owner, &work.db, over) {
        Ok((manifest, _, _)) => manifest,
        Err(S3Error::Conflict(_)) => return Err(already_holds(cfg)),
        // The answer to the manifest PUT may be what got lost: the manifest
        // carries this lease's generation if the PUT landed.
        Err(err) => match remote.get(MANIFEST_KEY) {
            Ok(Some(object)) => match ManifestState::decode(&object.bytes).map(|s| s.database()) {
                Ok(Some(manifest))
                    if manifest.generation == generation && manifest.epoch.seq == first =>
                {
                    manifest
                }
                _ => return Err(already_holds(cfg)),
            },
            _ => return Err(err),
        },
    };
    if (manifest.snapshot_size, manifest.snapshot_crc32c) != (converted.size, converted.crc32c) {
        return Err(S3Error::Corrupt(format!(
            "the imported snapshot {} doesn't match the new database file",
            manifest.snapshot
        )));
    }
    if opts.verify == Verify::Restore {
        restore::restore(&remote, &manifest, &work.restored, cfg.download_concurrency)?;
        let restored = Digests::of_file(&work.restored)?;
        if (restored.size, restored.crc32c) != (converted.size, converted.crc32c) {
            return Err(S3Error::Corrupt(format!(
                "the database restored from {} after the import doesn't match the imported \
                 file; don't use this prefix: clear it and import again",
                cfg.prefix
            )));
        }
    }
    lease.release();
    Ok(Imported {
        epoch: manifest.epoch.to_string(),
        size: manifest.snapshot_size,
        stored_size: manifest.snapshot_stored_size,
        objects: copied.objects,
        rows: copied.rows,
        sequences_not_advanced: copied.sequences_not_advanced,
    })
}

fn already_holds(cfg: &S3Config) -> S3Error {
    S3Error::Config(format!(
        "the S3 prefix {:?} already holds a database; import only into an empty prefix",
        cfg.prefix
    ))
}

fn too_large(source: &Path, size: u64) -> S3Error {
    S3Error::Config(format!(
        "{} is {size} bytes: a snapshot must fit in one S3 object (at most 5 TiB), so \
         a database this large can't be imported",
        source.display()
    ))
}

/// The source's size (with its WAL), if it can be imported.
fn check_source(source: &Path) -> Result<u64> {
    let meta = std::fs::metadata(source)
        .map_err(|err| S3Error::Config(format!("can't import {}: {err}", source.display())))?;
    if !meta.is_file() {
        return Err(S3Error::Config(format!(
            "can't import {}: not a file",
            source.display()
        )));
    }
    if live_storage(source).is_some() {
        return Err(S3Error::Config(format!(
            "{} is open with :s3 in this VM; it is in S3 already",
            source.display()
        )));
    }
    let mut journal = source.as_os_str().to_owned();
    journal.push("-journal");
    if std::fs::metadata(&journal).is_ok_and(|meta| meta.len() > 0) {
        return Err(S3Error::Config(format!(
            "{} has a hot rollback journal ({}): open the database once with SQLite to \
             recover it, then import",
            source.display(),
            Path::new(&journal).display()
        )));
    }
    let wal = std::fs::metadata(restore::wal_path(source)).map_or(0, |meta| meta.len());
    let log = std::fs::metadata(restore::log_path(source)).map_or(0, |meta| meta.len());
    let size = meta.len() + wal + log;
    if size > MAX_SNAPSHOT_BYTES {
        return Err(too_large(source, size));
    }
    Ok(size)
}

#[cfg(unix)]
fn check_free_space(path: &Path, needed: u64) -> Result<()> {
    use std::os::unix::ffi::OsStrExt;
    let dir = path.parent().unwrap_or(Path::new("."));
    let dir = if dir.as_os_str().is_empty() {
        Path::new(".")
    } else {
        dir
    };
    let c_dir = std::ffi::CString::new(dir.as_os_str().as_bytes())
        .map_err(|_| S3Error::Config("database path contains a NUL byte".into()))?;
    let mut stat: libc::statvfs = unsafe { std::mem::zeroed() };
    // SAFETY: c_dir is a valid C string, stat a valid out-pointer.
    if unsafe { libc::statvfs(c_dir.as_ptr(), &mut stat) } != 0 {
        return Ok(());
    }
    #[allow(clippy::unnecessary_cast)]
    let free = stat.f_bavail as u64 * stat.f_frsize as u64;
    let wanted = needed + needed / 10 + 64 * 1024 * 1024;
    if free < wanted {
        return Err(S3Error::Config(format!(
            "import needs about {wanted} bytes free in {} for its temporary copies, only \
             {free} are",
            dir.display()
        )));
    }
    Ok(())
}

#[cfg(not(unix))]
fn check_free_space(_path: &Path, _needed: u64) -> Result<()> {
    Ok(())
}

fn refuse(why: impl std::fmt::Display) -> S3Error {
    S3Error::Config(format!("can't import this database: {why}"))
}

/// A quoted SQL identifier.
fn ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

fn rows(conn: &Arc<Connection>, sql: &str) -> turso_core::Result<Vec<Vec<Value>>> {
    match conn.query(sql)? {
        Some(mut stmt) => stmt.run_collect_rows(),
        None => Ok(Vec::new()),
    }
}

fn text(value: &Value) -> String {
    match value {
        Value::Null => String::new(),
        other => other.to_string(),
    }
}

fn int(value: &Value) -> Option<i64> {
    value.to_string().parse().ok()
}

struct Copied {
    objects: usize,
    rows: u64,
    sequences_not_advanced: Vec<String>,
}

/// One schema object of the source, in `sqlite_schema` order.
struct Object {
    kind: String,
    name: String,
    sql: String,
}

/// Reads the source copy (plaintext, or with `source_key`), refusing what
/// MVCC doesn't support, and copies it into a new MVCC database at
/// `work.db`: tables, their rows (rowids kept), AUTOINCREMENT sequences,
/// then indexes, views and triggers (so no trigger fires and no index slows
/// the copy). Checks row counts and integrity at the end.
fn copy_database(
    work: &Work,
    source_key: Option<&EncryptionOpts>,
    target_key: Option<&EncryptionOpts>,
) -> Result<Copied> {
    let (src_db, src) = open_source(&work.src, source_key)?;
    let schema = rows(
        &src,
        "SELECT type, name, coalesce(sql, '') FROM sqlite_schema ORDER BY rowid",
    )
    .map_err(refuse)?;
    let objects: Vec<Object> = schema
        .iter()
        .map(|row| Object {
            kind: text(&row[0]),
            name: text(&row[1]),
            sql: text(&row[2]),
        })
        // sqlite_sequence, sqlite_stat*, autoindexes (no SQL) and turso's own
        // tables (sequences, MVCC metadata) are recreated by the target.
        .filter(|o| {
            !o.sql.is_empty()
                && !o.name.starts_with("sqlite_")
                && !o.name.starts_with("__turso_internal_")
        })
        .collect();
    let mut problems = Vec::new();
    for o in &objects {
        let sql = o.sql.to_ascii_uppercase();
        if sql.starts_with("CREATE VIRTUAL TABLE") {
            problems.push(format!(
                "{} is a virtual table (MVCC doesn't support them; for fts5, drop it and \
                 create a Turso FTS index after the import)",
                o.name
            ));
        } else if o.kind == "table" && sql.contains("WITHOUT ROWID") {
            problems.push(format!(
                "{} is a WITHOUT ROWID table (MVCC doesn't support them)",
                o.name
            ));
        }
    }
    if !problems.is_empty() {
        return Err(refuse(problems.join("; ")));
    }
    let sequences: BTreeMap<String, i64> =
        if schema.iter().any(|row| text(&row[1]) == "sqlite_sequence") {
            rows(&src, "SELECT name, seq FROM sqlite_sequence")
                .map_err(refuse)?
                .iter()
                .filter_map(|row| Some((text(&row[0]), int(&row[1])?)))
                .collect()
        } else {
            BTreeMap::new()
        };
    let user_version = rows(&src, "PRAGMA user_version")
        .ok()
        .and_then(|r| r.first().and_then(|row| int(&row[0])))
        .unwrap_or(0);

    work.create_target()?;
    with_local_connection(&work.db, target_key, |conn| {
        conn.execute("PRAGMA journal_mode = 'mvcc'")
    })?;
    let (dst_db, dst) = open_local(&work.db, target_key)?;
    // Rows go in table by table, and advance_sequence moves a row around.
    dst.execute("PRAGMA foreign_keys = OFF")
        .map_err(|err| S3Error::Config(format!("import copy: {err}")))?;
    let copy_err = |err: turso_core::LimboError| S3Error::Config(format!("import copy: {err}"));
    let mut copied = Copied {
        objects: objects.len(),
        rows: 0,
        sequences_not_advanced: Vec::new(),
    };
    let tables: Vec<&Object> = objects.iter().filter(|o| o.kind == "table").collect();
    for table in &tables {
        dst.execute(&table.sql).map_err(copy_err)?;
    }
    for table in &tables {
        let columns =
            rows(&src, &format!("PRAGMA table_info({})", ident(&table.name))).map_err(refuse)?;
        let names: Vec<String> = columns.iter().map(|c| text(&c[1])).collect();
        // A single INTEGER PRIMARY KEY column is the rowid; otherwise the
        // rowid is copied explicitly.
        let pks: Vec<&Vec<Value>> = columns
            .iter()
            .filter(|c| int(&c[5]).unwrap_or(0) > 0)
            .collect();
        let alias = match pks.as_slice() {
            [pk] if text(&pk[2]).eq_ignore_ascii_case("INTEGER") => Some(text(&pk[1])),
            _ => None,
        };
        let mut select_cols: Vec<String> = names.iter().map(|n| ident(n)).collect();
        if alias.is_none() {
            select_cols.insert(0, "rowid".into());
        }
        let list = select_cols.join(", ");
        let marks = vec!["?"; select_cols.len()].join(", ");
        let (batch, checkpoint) = if table.sql.to_ascii_uppercase().contains("AUTOINCREMENT") {
            (autoincrement_batch_rows(), autoincrement_checkpoint_rows())
        } else {
            (BATCH_ROWS, 0)
        };
        let n = copy_rows(
            &src,
            &dst,
            batch,
            checkpoint,
            &format!("SELECT {list} FROM {}", ident(&table.name)),
            &format!(
                "INSERT INTO {} ({list}) VALUES ({marks})",
                ident(&table.name)
            ),
        )
        .map_err(copy_err)?;
        copied.rows += n;
        let src_count = rows(
            &src,
            &format!("SELECT count(*) FROM {}", ident(&table.name)),
        )
        .map_err(refuse)?;
        if src_count.first().and_then(|r| int(&r[0])) != Some(n as i64) {
            return Err(refuse(format!(
                "copied {n} rows of {}, but it has {:?}",
                table.name, src_count
            )));
        }
        if let (Some(&seq), Some(alias)) = (sequences.get(&table.name), &alias) {
            if !advance_sequence(&dst, &table.name, alias, &columns, seq).map_err(copy_err)? {
                copied.sequences_not_advanced.push(table.name.clone());
            }
        }
    }
    for kind in ["index", "view", "trigger"] {
        for o in objects.iter().filter(|o| o.kind == kind) {
            dst.execute(&o.sql).map_err(copy_err)?;
        }
    }
    if user_version != 0 {
        dst.execute(format!("PRAGMA user_version = {user_version}"))
            .map_err(copy_err)?;
    }
    dst.close().map_err(copy_err)?;
    src.close().map_err(refuse)?;
    drop((dst_db, src_db));

    // Fold the log into the file (the snapshot is the file alone), then check.
    with_local_connection(&work.db, target_key, |conn| {
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    })?;
    if std::fs::metadata(restore::log_path(&work.db)).is_ok_and(|meta| meta.len() > 0) {
        return Err(refuse("the new database's log wasn't checkpointed"));
    }
    let mut check = Vec::new();
    with_local_connection(&work.db, target_key, |conn| {
        check = rows(conn, "PRAGMA integrity_check")?;
        Ok(())
    })?;
    let check: Vec<String> = check.iter().map(|row| text(&row[0])).collect();
    if check != ["ok"] {
        return Err(refuse(format!(
            "integrity_check of the new database reports {}",
            check.join("; ")
        )));
    }
    Ok(copied)
}

/// Opens the source copy: with `key` when it is encrypted, plaintext
/// otherwise (a key given for a plaintext file is ignored).
fn open_source(
    path: &Path,
    key: Option<&EncryptionOpts>,
) -> Result<(Arc<turso_core::Database>, Arc<Connection>)> {
    let readable = |opened: Result<(Arc<turso_core::Database>, Arc<Connection>)>| {
        opened.and_then(|(db, conn)| {
            rows(&conn, "SELECT count(*) FROM sqlite_schema")
                .map_err(|err| S3Error::Turso(err.to_string()))?;
            Ok((db, conn))
        })
    };
    match readable(open_local(path, None)) {
        Ok(opened) => Ok(opened),
        Err(plain) => match key {
            Some(key) => readable(open_local(path, Some(key))).map_err(|err| {
                refuse(format!(
                    "it can't be read with the given source key ({err}) nor as a plaintext \
                     database ({plain})"
                ))
            }),
            None => Err(refuse(format!(
                "it can't be read as a database ({plain}); for an encrypted database give \
                 its key"
            ))),
        },
    }
}

/// Copies the rows of `select` into `insert`, in transactions of
/// `batch` rows, checkpointing every `checkpoint` rows (0: never).
/// Returns the number of rows.
fn copy_rows(
    src: &Arc<Connection>,
    dst: &Arc<Connection>,
    batch: u64,
    checkpoint: u64,
    select: &str,
    insert: &str,
) -> turso_core::Result<u64> {
    let mut reader = src.prepare(select)?;
    let mut writer = dst.prepare(insert)?;
    let mut count = 0u64;
    dst.execute("BEGIN")?;
    reader.run_with_row_callback(|row| {
        for (i, value) in row.get_values().enumerate() {
            writer.bind_at(NonZero::new(i + 1).expect("1-based"), value.clone())?;
        }
        writer.run_ignore_rows()?;
        writer.reset()?;
        count += 1;
        if count.is_multiple_of(batch) {
            dst.execute("COMMIT")?;
            if checkpoint > 0 && count.is_multiple_of(checkpoint) {
                dst.execute("PRAGMA wal_checkpoint(TRUNCATE)")?;
            }
            dst.execute("BEGIN")?;
        }
        Ok(())
    })?;
    dst.execute("COMMIT")?;
    Ok(count)
}

/// Moves an AUTOINCREMENT table's sequence up to the source's
/// `sqlite_sequence` value (above the largest id when the newest rows were
/// deleted) by inserting a row with that id and deleting it again, in one
/// transaction. No index (but the table's own UNIQUE constraints) and no
/// trigger exists yet, and foreign keys are off.
///
/// With rows: an existing row R is deleted, inserted with id = seq, deleted,
/// and inserted back with its own id, so no UNIQUE constraint ever sees two
/// copies of it. Empty: a row with the id, typed placeholders (0, '', x'')
/// for NOT NULL columns without a default, and NULL or the default for the
/// rest. Returns false when that row is refused (a CHECK constraint, say).
fn advance_sequence(
    dst: &Arc<Connection>,
    table: &str,
    alias: &str,
    columns: &[Vec<Value>],
    seq: i64,
) -> turso_core::Result<bool> {
    let current = rows(
        dst,
        &format!(
            "SELECT seq FROM sqlite_sequence WHERE name = '{}'",
            table.replace('\'', "''")
        ),
    )?
    .first()
    .and_then(|row| int(&row[0]))
    .unwrap_or(0);
    if current >= seq {
        return Ok(true);
    }
    let t = ident(table);
    let names: Vec<String> = columns.iter().map(|c| text(&c[1])).collect();
    let list = names
        .iter()
        .map(|c| ident(c))
        .collect::<Vec<_>>()
        .join(", ");
    let marks = vec!["?"; names.len()].join(", ");
    let at = names
        .iter()
        .position(|n| n == alias)
        .expect("the alias is a column");
    let insert = |values: &[Value]| -> turso_core::Result<()> {
        let mut stmt = dst.prepare(format!("INSERT INTO {t} ({list}) VALUES ({marks})"))?;
        for (i, value) in values.iter().enumerate() {
            stmt.bind_at(NonZero::new(i + 1).expect("1-based"), value.clone())?;
        }
        stmt.run_ignore_rows()
    };
    let delete = |id: &Value| -> turso_core::Result<()> {
        let mut stmt = dst.prepare(format!("DELETE FROM {t} WHERE {} = ?", ident(alias)))?;
        stmt.bind_at(NonZero::new(1).expect("1-based"), id.clone())?;
        stmt.run_ignore_rows()
    };
    let existing = rows(dst, &format!("SELECT {list} FROM {t} LIMIT 1"))?;
    let empty = existing.is_empty();
    dst.execute("BEGIN")?;
    let advanced = match existing.into_iter().next() {
        Some(row) => {
            let mut moved = row.clone();
            moved[at] = Value::from_i64(seq);
            delete(&row[at])
                .and_then(|()| insert(&moved))
                .and_then(|()| delete(&moved[at]))
                .and_then(|()| insert(&row))
        }
        None => {
            let placeholder: Vec<Value> = columns
                .iter()
                .enumerate()
                .map(|(i, c)| {
                    let required = int(&c[3]).unwrap_or(0) != 0 && matches!(c[4], Value::Null);
                    if i == at {
                        Value::from_i64(seq)
                    } else if required {
                        placeholder_for(&text(&c[2]))
                    } else {
                        Value::Null
                    }
                })
                .collect();
            // Columns with a default get it rather than NULL.
            let defaults: Vec<usize> = columns
                .iter()
                .enumerate()
                .filter(|(i, c)| *i != at && !matches!(c[4], Value::Null))
                .map(|(i, _)| i)
                .collect();
            let kept: Vec<usize> = (0..names.len()).filter(|i| !defaults.contains(i)).collect();
            let cols = kept
                .iter()
                .map(|&i| ident(&names[i]))
                .collect::<Vec<_>>()
                .join(", ");
            let qs = vec!["?"; kept.len()].join(", ");
            dst.prepare(format!("INSERT INTO {t} ({cols}) VALUES ({qs})"))
                .and_then(|mut stmt| {
                    for (n, &i) in kept.iter().enumerate() {
                        stmt.bind_at(
                            NonZero::new(n + 1).expect("1-based"),
                            placeholder[i].clone(),
                        )?;
                    }
                    stmt.run_ignore_rows()
                })
                .and_then(|()| delete(&placeholder[at]))
        }
    };
    match advanced {
        Ok(()) => {
            dst.execute("COMMIT")?;
            Ok(true)
        }
        Err(_) if empty => {
            dst.execute("ROLLBACK")?;
            Ok(false)
        }
        Err(err) => {
            let _ = dst.execute("ROLLBACK");
            Err(err)
        }
    }
}

/// A value of the column's type affinity (SQLite's rules) for a NOT NULL
/// column of a placeholder row.
fn placeholder_for(declared: &str) -> Value {
    let t = declared.to_ascii_uppercase();
    if t.contains("INT") {
        Value::from_i64(0)
    } else if t.contains("CHAR") || t.contains("CLOB") || t.contains("TEXT") {
        Value::build_text("")
    } else if t.contains("BLOB") || t.is_empty() {
        Value::from_blob(Vec::new())
    } else {
        // REAL, NUMERIC and anything else
        Value::from_i64(0)
    }
}

/// The temporary files of one import, next to the source (the same file
/// system, readable only by this user); removed when dropped.
struct Work {
    src: PathBuf,
    db: PathBuf,
    restored: PathBuf,
}

impl Work {
    fn new(source: &Path) -> Result<Self> {
        let dir = source.parent().unwrap_or(Path::new(""));
        let name = source
            .file_name()
            .ok_or_else(|| S3Error::Config("database path has no file name".into()))?
            .to_string_lossy()
            .into_owned();
        let tag = format!(
            "{:016x}",
            std::hash::BuildHasher::hash_one(
                &std::collections::hash_map::RandomState::new(),
                std::time::SystemTime::now()
            )
        );
        let path = |what: &str| dir.join(format!(".{name}.import-{tag}-{what}.db"));
        Ok(Self {
            src: path("source"),
            db: path("new"),
            restored: path("check"),
        })
    }

    fn create(path: &Path) -> Result<std::fs::File> {
        let mut options = std::fs::OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        std::os::unix::fs::OpenOptionsExt::mode(&mut options, 0o600);
        Ok(options.open(path)?)
    }

    /// The source, its WAL and MVCC log, copied as they are: the source is
    /// only ever read (opening it with turso could checkpoint its WAL).
    fn copy_source(&self, source: &Path) -> Result<()> {
        let pairs = [
            (source.to_path_buf(), self.src.clone()),
            (restore::wal_path(source), restore::wal_path(&self.src)),
            (restore::log_path(source), restore::log_path(&self.src)),
        ];
        for (from, to) in pairs {
            let mut input = match std::fs::File::open(&from) {
                Ok(file) => file,
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => continue,
                Err(err) => return Err(err.into()),
            };
            let mut output = Self::create(&to)?;
            std::io::copy(&mut input, &mut output)?;
            output.sync_all()?;
        }
        // turso would create the WAL itself, with the default mode.
        if !restore::wal_path(&self.src).exists() {
            Self::create(&restore::wal_path(&self.src))?;
        }
        Ok(())
    }

    /// The new database's (empty) file and log, created mode 0600 before
    /// turso opens them.
    fn create_target(&self) -> Result<()> {
        Self::create(&self.db)?;
        Self::create(&restore::wal_path(&self.db))?;
        Self::create(&restore::log_path(&self.db))?;
        Ok(())
    }
}

impl Drop for Work {
    fn drop(&mut self) {
        for db in [&self.src, &self.db, &self.restored] {
            for path in [db.clone(), restore::wal_path(db), restore::log_path(db)] {
                let _ = std::fs::remove_file(path);
            }
        }
    }
}
