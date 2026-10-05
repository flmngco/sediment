//! Exporting a database to a plain SQLite file (`Sediment.export_sqlite/3`).
//!
//! `VACUUM INTO` alone isn't enough in turso_core 0.8.1: from an MVCC database it
//! writes an MVCC header and resets `sqlite_sequence` to max(id), and copies
//! every row in one transaction, which is quadratic for AUTOINCREMENT tables;
//! a Turso FTS index makes the whole file unreadable by SQLite. So the private
//! copy of the source is switched to WAL first, FTS indexes are dropped from
//! it (only when asked), the output's `sqlite_sequence` is checked against
//! the source's, and the result is verified before it is published.
//!
//! Everything is generated inside a private (0700) staging directory next to
//! `dest`, so plaintext from an encrypted source is never readable by other
//! users, whatever the umask; only the finished file (0600) is linked out.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use turso_core::{Connection, DatabaseOpts, EncryptionOpts, Value};

use crate::s3::{self, S3Config, S3Error, Target};

/// Where the database to export comes from.
pub enum Source {
    /// A local database file (plaintext, or encrypted with the key).
    Local(PathBuf),
    /// The latest state of an S3 database, restored into a temporary file
    /// (the prefix is only read).
    S3(Box<S3Config>),
}

pub struct ExportOptions {
    /// The source's key, if it is encrypted.
    pub encryption: Option<EncryptionOpts>,
    /// Drop Turso FTS indexes from the copy instead of refusing.
    pub drop_fts: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Exported {
    /// Tables, indexes, views and triggers in the copy (turso's own tables
    /// not counted).
    pub objects: usize,
    pub rows: u64,
    /// AUTOINCREMENT sequences, as in the source and now in the copy.
    pub sequences: Vec<(String, i64)>,
    /// FTS indexes dropped from the copy (`drop_fts: true`).
    pub dropped_fts: Vec<String>,
}

type Result<T> = std::result::Result<T, String>;

fn err(context: &str) -> impl Fn(turso_core::LimboError) -> String + '_ {
    move |e| format!("export: {context}: {e}")
}

fn s3_err(e: S3Error) -> String {
    format!("export: {e}")
}

fn io_err(e: std::io::Error) -> String {
    format!("export: {e}")
}

/// Exports `source` to a new SQLite file at `dest` (mode 0600), which must
/// not exist. The source is never written to: it is copied (or restored
/// from S3) into a private staging directory next to `dest` first. The
/// output is checked (integrity_check, row counts and sqlite_sequence
/// against the source's copy) before it appears at `dest`; on any failure
/// nothing is left there and the staging directory is removed.
pub fn export(source: Source, dest: &Path, opts: &ExportOptions) -> Result<Exported> {
    for path in [
        dest.to_path_buf(),
        suffixed(dest, "-wal"),
        suffixed(dest, "-journal"),
        suffixed(dest, "-shm"),
    ] {
        if path.symlink_metadata().is_ok() {
            return Err(format!("export target exists: {}", path.display()));
        }
    }
    let work = Work::new(dest)?;
    // The source's log is copied with it: it must be the source's own.
    let _log = match &source {
        Source::Local(path) if crate::log_guard::is_mvcc_file(path) => {
            Some(crate::log_guard::claim(path)?)
        }
        _ => None,
    };
    let key = match &source {
        Source::Local(path) => {
            let files = source_files(path)?;
            if s3::live_storage(&files.db).is_some() {
                return Err(format!(
                    "{} is open with :s3 in this VM; export its S3 database (from_s3) or \
                     close it first",
                    path.display()
                ));
            }
            work.copy_source(&files)?;
            opts.encryption.clone()
        }
        Source::S3(cfg) => {
            let mut cfg = (**cfg).clone();
            cfg.encryption = opts.encryption.clone();
            s3::restore_to(&cfg, &work.src, Target::Latest).map_err(s3_err)?;
            cfg.encryption
        }
    };

    let (src_db, src) = open(&work.src, key.as_ref())?;
    src.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        .map_err(err("checkpointing the copy of the source"))?;
    // VACUUM INTO from MVCC writes an MVCC file, resets sqlite_sequence and
    // copies in one quadratic transaction; from WAL none of that.
    let mode = rows(&src, "PRAGMA journal_mode").map_err(err("reading the journal mode"))?;
    if mode.first().map(|r| text(&r[0])).as_deref() == Some("mvcc") {
        src.execute("PRAGMA journal_mode = wal")
            .map_err(err("switching the copy of the source to WAL"))?;
    }
    let objects_before = schema(&src)?;
    let methods: Vec<(String, String)> = objects_before
        .iter()
        .filter(|o| o.kind == "index" && !o.name.starts_with("__turso_internal_"))
        .filter_map(|o| index_method(&o.sql).map(|m| (o.name.clone(), m)))
        .collect();
    let other: Vec<String> = methods
        .iter()
        .filter(|(_, m)| !m.eq_ignore_ascii_case("fts"))
        .map(|(name, m)| format!("{name} (USING {m})"))
        .collect();
    if !other.is_empty() {
        return Err(format!(
            "export: indexes of Turso index methods SQLite can't read: {}",
            other.join(", ")
        ));
    }
    let fts: Vec<String> = methods.into_iter().map(|(name, _)| name).collect();
    if !fts.is_empty() && !opts.drop_fts {
        return Err(format!(
            "export: the database has Turso FTS indexes ({}), which SQLite can't read; \
             export with drop_fts: true to leave them out (the indexed text stays)",
            fts.join(", ")
        ));
    }
    // Dropped from the private copy of the source (VACUUM INTO doesn't carry
    // an FTS index's backing store); that drops the backing store too.
    for index in &fts {
        src.execute(format!("DROP INDEX {}", ident(index)))
            .map_err(err("dropping an FTS index"))?;
    }
    let left: Vec<String> = schema_unreadable_by_sqlite(&schema(&src)?);
    if !left.is_empty() {
        return Err(format!(
            "export: schema entries SQLite can't read remain: {}",
            left.join(", ")
        ));
    }
    let tables: Vec<String> = objects_before
        .iter()
        .filter(|o| o.kind == "table" && o.name != "sqlite_sequence")
        .map(|o| o.name.clone())
        .filter(|name| !name.starts_with("__turso_internal_"))
        .collect();
    let counts = count_rows(&src, &tables)?;
    let sequences = sequences(&src)?;
    let out = work.out.to_str().ok_or("dest must be valid UTF-8")?;
    src.execute(format!("VACUUM INTO '{}'", out.replace('\'', "''")))
        .map_err(err("VACUUM INTO"))?;
    src.close().map_err(err("closing the source copy"))?;
    drop(src_db);

    // The output is plaintext (still inside the private directory): fix it
    // up for SQLite, then check it.
    let (out_db, dst) = open(&work.out, None)?;
    let mode = rows(&dst, "PRAGMA journal_mode").map_err(err("reading the journal mode"))?;
    if mode.first().map(|r| text(&r[0])).as_deref() != Some("wal") {
        dst.execute("PRAGMA journal_mode = wal")
            .map_err(err("switching the copy to WAL"))?;
    }
    for (name, seq) in &sequences {
        dst.execute(format!(
            "UPDATE sqlite_sequence SET seq = {seq} WHERE name = '{}'",
            name.replace('\'', "''")
        ))
        .map_err(err("restoring sqlite_sequence"))?;
    }
    let check = rows(&dst, "PRAGMA integrity_check").map_err(err("integrity_check"))?;
    let check: Vec<String> = check.iter().map(|r| text(&r[0])).collect();
    if check != ["ok"] {
        return Err(format!(
            "export: integrity_check of the copy reports {}",
            check.join("; ")
        ));
    }
    let copied = count_rows(&dst, &tables)?;
    if copied != counts {
        return Err(format!(
            "export: the copy's row counts {copied:?} differ from the source's {counts:?}"
        ));
    }
    check_sequences(&dst, &sequences)?;
    let objects = rows(
        &dst,
        "SELECT count(*) FROM sqlite_schema WHERE sql IS NOT NULL \
         AND substr(name, 1, 17) != '__turso_internal_' AND name != 'sqlite_sequence'",
    )
    .ok()
    .and_then(|r| r.first().and_then(|row| int(&row[0])))
    .unwrap_or(0) as usize;
    dst.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        .map_err(err("checkpointing the copy"))?;
    dst.close().map_err(err("closing the copy"))?;
    drop(out_db);
    set_private(&work.out)?;
    // `dest` appears complete or not at all, and a file created there
    // meanwhile is never replaced.
    std::fs::hard_link(&work.out, dest).map_err(|e| {
        if e.kind() == std::io::ErrorKind::AlreadyExists {
            format!("export target exists: {}", dest.display())
        } else {
            io_err(e)
        }
    })?;
    Ok(Exported {
        objects,
        rows: counts.values().sum(),
        sequences,
        dropped_fts: fts,
    })
}

/// A database file and its sidecars, as its writers see them.
struct SourceFiles {
    db: PathBuf,
    wal: PathBuf,
    log: PathBuf,
}

/// The source's files. turso derives the WAL and MVCC log from the path a
/// writer opened, which for a symlink is the link's name, not the target's:
/// look in both places, and refuse when both hold data or when hard links
/// make other names possible.
fn source_files(path: &Path) -> Result<SourceFiles> {
    let meta = std::fs::metadata(path)
        .map_err(|e| format!("export: {} doesn't exist ({e})", path.display()))?;
    if !meta.is_file() {
        return Err(format!("export: {} is not a file", path.display()));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if meta.nlink() > 1 {
            return Err(format!(
                "export: {} has {} hard links, so its WAL and MVCC log may be next to \
                 another name; export a copy made with the database closed",
                path.display(),
                meta.nlink()
            ));
        }
    }
    let real = std::fs::canonicalize(path).map_err(io_err)?;
    let has_data = |p: &Path| std::fs::metadata(p).is_ok_and(|m| m.len() > 0);
    let at = |db: &Path| SourceFiles {
        db: real.clone(),
        wal: s3::restore::wal_path(db),
        log: s3::restore::log_path(db),
    };
    // Where a writer that opened `path` keeps its sidecars: next to that
    // name, in its directory (canonicalized, so a relative or `..` path
    // compares equal to the target's when it isn't a symlink).
    let dir = match path.parent() {
        Some(dir) if !dir.as_os_str().is_empty() => dir,
        _ => Path::new("."),
    };
    let name = path
        .file_name()
        .ok_or("export: the source has no file name")?;
    let alias = std::fs::canonicalize(dir).map_err(io_err)?.join(name);
    let given = at(&alias);
    let canonical = at(&real);
    let given_data = has_data(&given.wal) || has_data(&given.log);
    let canonical_data = has_data(&canonical.wal) || has_data(&canonical.log);
    if given.wal == canonical.wal {
        return Ok(canonical);
    }
    match (given_data, canonical_data) {
        (true, true) => Err(format!(
            "export: {} is a link to {}, and both have a WAL or MVCC log; which one \
             belongs to the database is ambiguous",
            path.display(),
            real.display()
        )),
        (true, false) => Ok(given),
        _ => Ok(canonical),
    }
}

/// One `sqlite_schema` entry.
struct Object {
    kind: String,
    name: String,
    sql: String,
}

fn schema(conn: &Arc<Connection>) -> Result<Vec<Object>> {
    Ok(rows(
        conn,
        "SELECT type, name, coalesce(sql, '') FROM sqlite_schema ORDER BY rowid",
    )
    .map_err(err("reading the schema"))?
    .iter()
    .map(|row| Object {
        kind: text(&row[0]),
        name: text(&row[1]),
        sql: text(&row[2]),
    })
    .collect())
}

/// Entries SQLite can't parse: indexes of an index method, virtual tables.
fn schema_unreadable_by_sqlite(schema: &[Object]) -> Vec<String> {
    schema
        .iter()
        .filter(|o| {
            (o.kind == "index" && index_method(&o.sql).is_some())
                || (o.kind == "table" && is_virtual_table(&o.sql))
        })
        .map(|o| o.name.clone())
        .collect()
}

/// The index method of a `CREATE [UNIQUE] INDEX [IF NOT EXISTS] name ON
/// table USING method(...)`, read token by token: `USING` right after the
/// table, where a plain index has its column list. Literals, comments and
/// partial-index conditions never count.
pub(crate) fn index_method(sql: &str) -> Option<String> {
    let tokens = tokens(sql);
    let mut t = tokens.iter().map(|t| t.as_str());
    let word = |tok: Option<&str>, w: &str| tok.is_some_and(|t| t.eq_ignore_ascii_case(w));
    if !word(t.next(), "CREATE") {
        return None;
    }
    let mut next = t.next();
    if word(next, "UNIQUE") {
        next = t.next();
    }
    if !word(next, "INDEX") {
        return None;
    }
    let mut next = t.next();
    if word(next, "IF") {
        t.next(); // NOT
        t.next(); // EXISTS
        next = t.next();
    }
    // the index name (schema.name), then ON
    loop {
        match next {
            Some(tok) if tok.eq_ignore_ascii_case("ON") => break,
            Some(_) => next = t.next(),
            None => return None,
        }
    }
    // the table name, possibly schema.table
    t.next()?;
    let mut next = t.next();
    while next == Some(".") {
        t.next();
        next = t.next();
    }
    if word(next, "USING") {
        t.next().map(str::to_string)
    } else {
        None
    }
}

fn is_virtual_table(sql: &str) -> bool {
    let tokens = tokens(sql);
    tokens.len() >= 3
        && tokens[0].eq_ignore_ascii_case("CREATE")
        && tokens[1].eq_ignore_ascii_case("VIRTUAL")
        && tokens[2].eq_ignore_ascii_case("TABLE")
}

/// SQL tokens: words, quoted names (as one token), punctuation; string
/// literals and comments skipped.
fn tokens(sql: &str) -> Vec<String> {
    let chars: Vec<char> = sql.chars().collect();
    let mut out = Vec::new();
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        if c.is_whitespace() {
            i += 1;
        } else if c == '-' && chars.get(i + 1) == Some(&'-') {
            while i < chars.len() && chars[i] != '\n' {
                i += 1;
            }
        } else if c == '/' && chars.get(i + 1) == Some(&'*') {
            i += 2;
            while i + 1 < chars.len() && !(chars[i] == '*' && chars[i + 1] == '/') {
                i += 1;
            }
            i += 2;
        } else if c == '\'' || c == '"' || c == '`' || c == '[' {
            let close = if c == '[' { ']' } else { c };
            let start = i;
            i += 1;
            loop {
                match chars.get(i) {
                    None => break,
                    Some(&d) if d == close => {
                        if close != ']' && chars.get(i + 1) == Some(&close) {
                            i += 2;
                        } else {
                            i += 1;
                            break;
                        }
                    }
                    Some(_) => i += 1,
                }
            }
            if c != '\'' {
                out.push(chars[start..i].iter().collect());
            }
        } else if c.is_alphanumeric() || c == '_' || c == '$' {
            let start = i;
            while i < chars.len()
                && (chars[i].is_alphanumeric() || chars[i] == '_' || chars[i] == '$')
            {
                i += 1;
            }
            out.push(chars[start..i].iter().collect());
        } else {
            out.push(c.to_string());
            i += 1;
        }
    }
    out
}

fn sequences(conn: &Arc<Connection>) -> Result<Vec<(String, i64)>> {
    let has = rows(
        conn,
        "SELECT 1 FROM sqlite_schema WHERE type = 'table' AND name = 'sqlite_sequence'",
    )
    .map_err(err("reading the schema"))?;
    if has.is_empty() {
        return Ok(Vec::new());
    }
    Ok(
        rows(conn, "SELECT name, seq FROM sqlite_sequence ORDER BY name")
            .map_err(err("reading sqlite_sequence"))?
            .iter()
            .filter_map(|row| Some((text(&row[0]), int(&row[1])?)))
            .collect(),
    )
}

/// The copy's sequences: the source's values, and any others (turso can add
/// rows for its own AUTOINCREMENT tables, CDC's) at least the table's
/// largest rowid, so SQLite never hands out an id that is in use.
fn check_sequences(conn: &Arc<Connection>, expected: &[(String, i64)]) -> Result<()> {
    let copied: BTreeMap<String, i64> = sequences(conn)?.into_iter().collect();
    for (name, seq) in expected {
        if copied.get(name) != Some(seq) {
            return Err(format!(
                "export: the copy's sqlite_sequence {copied:?} differs from the source's \
                 {expected:?}"
            ));
        }
    }
    for (name, seq) in &copied {
        if expected.iter().any(|(n, _)| n == name) {
            continue;
        }
        let max = rows(conn, &format!("SELECT max(rowid) FROM {}", ident(name)))
            .ok()
            .and_then(|r| r.first().and_then(|row| int(&row[0])))
            .unwrap_or(0);
        if *seq < max {
            return Err(format!(
                "export: the copy's sqlite_sequence for {name} ({seq}) is below its largest \
                 id ({max})"
            ));
        }
    }
    Ok(())
}

fn open(
    path: &Path,
    key: Option<&EncryptionOpts>,
) -> Result<(Arc<turso_core::Database>, Arc<Connection>)> {
    // Whatever the source used: views, FTS (index_method) indexes.
    let db_opts = DatabaseOpts::new().with_views(true).with_index_method(true);
    s3::open_local_with(path, key, db_opts).map_err(|e| match key {
        Some(_) => format!("export: can't open the source with the given :encryption: {e}"),
        None => format!(
            "export: can't open {} ({e}); for an encrypted database give its :encryption",
            path.display()
        ),
    })
}

fn count_rows(conn: &Arc<Connection>, tables: &[String]) -> Result<BTreeMap<String, u64>> {
    tables
        .iter()
        .map(|t| {
            let n = rows(conn, &format!("SELECT count(*) FROM {}", ident(t)))
                .map_err(err("counting rows"))?
                .first()
                .and_then(|r| int(&r[0]))
                .unwrap_or(0);
            Ok((t.clone(), n as u64))
        })
        .collect()
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

fn ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

fn suffixed(path: &Path, suffix: &str) -> PathBuf {
    let mut name = path.as_os_str().to_owned();
    name.push(suffix);
    name.into()
}

fn set_private(path: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)).map_err(io_err)?;
    }
    Ok(())
}

/// The export's private staging directory, next to `dest` (the same file
/// system, so the result can be linked into place), mode 0700 from its
/// creation: every file in it, whatever mode turso gives it, is reachable
/// only by this user. Removed when dropped.
struct Work {
    dir: PathBuf,
    src: PathBuf,
    out: PathBuf,
}

impl Work {
    fn new(dest: &Path) -> Result<Self> {
        let parent = dest.parent().unwrap_or(Path::new(""));
        let parent = if parent.as_os_str().is_empty() {
            Path::new(".")
        } else {
            parent
        };
        let name = dest
            .file_name()
            .ok_or("dest has no file name")?
            .to_string_lossy()
            .into_owned();
        let tag = format!(
            "{:016x}",
            std::hash::BuildHasher::hash_one(
                &std::collections::hash_map::RandomState::new(),
                std::time::SystemTime::now()
            )
        );
        let dir = parent.join(format!(".{name}.export-{tag}"));
        let mut builder = std::fs::DirBuilder::new();
        #[cfg(unix)]
        std::os::unix::fs::DirBuilderExt::mode(&mut builder, 0o700);
        builder.create(&dir).map_err(io_err)?;
        let work = Self {
            src: dir.join("source.db"),
            out: dir.join("export.db"),
            dir,
        };
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(&work.dir)
                .map_err(io_err)?
                .permissions()
                .mode();
            if mode & 0o077 != 0 {
                return Err(format!(
                    "export: the staging directory {} isn't private ({:o})",
                    work.dir.display(),
                    mode & 0o777
                ));
            }
        }
        Ok(work)
    }

    fn create(path: &Path) -> Result<std::fs::File> {
        let mut options = std::fs::OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        std::os::unix::fs::OpenOptionsExt::mode(&mut options, 0o600);
        options.open(path).map_err(io_err)
    }

    /// The source, its WAL and MVCC log, copied as they are (opening the
    /// source itself could checkpoint it).
    fn copy_source(&self, files: &SourceFiles) -> Result<()> {
        let pairs = [
            (&files.db, self.src.clone()),
            (&files.wal, s3::restore::wal_path(&self.src)),
            (&files.log, s3::restore::log_path(&self.src)),
        ];
        for (from, to) in pairs {
            let mut input = match std::fs::File::open(from) {
                Ok(file) => file,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
                Err(e) => return Err(io_err(e)),
            };
            let mut output = Self::create(&to)?;
            std::io::copy(&mut input, &mut output).map_err(io_err)?;
        }
        Ok(())
    }
}

impl Drop for Work {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

/// `Sediment.Native.export_sqlite/5`: `source` is a path, or nil with
/// `from_s3` (the S3 options).
#[rustler::nif(schedule = "DirtyIo")]
fn export_sqlite<'a>(
    env: rustler::Env<'a>,
    source: Option<String>,
    dest: String,
    from_s3: Option<rustler::Term<'a>>,
    encryption: Option<(String, String)>,
    drop_fts: bool,
) -> rustler::Term<'a> {
    use rustler::{Atom, Encoder};
    // `nil` arrives as a term, not as `None`.
    let from_s3 = from_s3.filter(|term| term.atom_to_string().ok().as_deref() != Some("nil"));
    let source = match (source, from_s3) {
        (Some(path), None) => Source::Local(PathBuf::from(path)),
        (None, Some(term)) => match crate::s3_nif::decode_config(term) {
            Ok(cfg) => Source::S3(Box::new(cfg)),
            Err(reason) => return crate::conn::error_tuple(env, reason),
        },
        _ => return crate::conn::error_tuple(env, "give a source path or from_s3, not both"),
    };
    if let Some((_, hexkey)) = &encryption {
        if let Err(reason) = crate::open::check_hex_key(hexkey) {
            return crate::conn::error_tuple(env, reason);
        }
    }
    let opts = ExportOptions {
        encryption: encryption.map(|(cipher, hexkey)| EncryptionOpts { cipher, hexkey }),
        drop_fts,
    };
    match export(source, Path::new(&dest), &opts) {
        Ok(exported) => {
            let pairs = [
                ("objects", exported.objects.encode(env)),
                ("rows", exported.rows.encode(env)),
                (
                    "sequences",
                    exported
                        .sequences
                        .iter()
                        .map(|(name, seq)| (name.as_str(), *seq))
                        .collect::<Vec<_>>()
                        .encode(env),
                ),
                ("dropped_fts", exported.dropped_fts.encode(env)),
            ]
            .map(|(k, v)| (Atom::from_str(env, k).unwrap().encode(env), v));
            match rustler::Term::map_from_pairs(env, &pairs) {
                Ok(map) => crate::conn::ok_tuple(env, map),
                Err(_) => crate::conn::error_tuple(env, "could not build result"),
            }
        }
        Err(reason) => crate::conn::error_tuple(env, reason),
    }
}
