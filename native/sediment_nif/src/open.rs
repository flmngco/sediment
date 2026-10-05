use std::path::Path;
use std::sync::Arc;

use rustler::{Atom, Term};
use turso_core::{
    Connection, Database, DatabaseOpts, EncryptionKey, EncryptionOpts, MemoryIO, OpenFlags,
    OpenOptions, PlatformIO, SqliteDialect, IO,
};

use crate::atoms;
use crate::s3::S3DurableStorage;
use turso_core::mvcc::persistent_storage::DurableStorage;

/// Everything needed to open a database, decoded from the options map built
/// by `Sediment.Engine.open/2`. All turso `OpenOptions` are assembled in
/// `OpenConfig::open`, so new open-time features only touch this file.
pub struct OpenConfig<'a> {
    pub path: String,
    pub readonly: bool,
    pub create: bool,
    pub journal_mode: Option<String>,
    pub encryption: Option<EncryptionOpts>,
    pub experimental: Vec<String>,
    /// Raw `s3:` option term, handed to the s3 module untouched.
    pub s3: Option<Term<'a>>,
}

const JOURNAL_MODES: [&str; 8] = [
    "delete",
    "truncate",
    "persist",
    "memory",
    "wal",
    "off",
    "mvcc",
    "experimental_mvcc",
];

pub struct Opened {
    pub io: Arc<dyn IO>,
    pub db: Arc<Database>,
    pub conn: Arc<Connection>,
    pub s3: Option<Arc<S3DurableStorage>>,
    pub replica: Option<crate::replica::Replica>,
}

fn get<'a>(opts: Term<'a>, key: Atom) -> Option<Term<'a>> {
    opts.map_get(key)
        .ok()
        .filter(|t| !(t.is_atom() && t.decode::<Atom>().ok() == Some(atoms::nil())))
}

impl<'a> OpenConfig<'a> {
    pub fn decode(path: String, opts: Term<'a>) -> Result<Self, String> {
        let bool_opt = |key: Atom, default: bool| -> Result<bool, String> {
            match get(opts, key) {
                Some(t) => t
                    .decode::<bool>()
                    .map_err(|_| format!("invalid boolean for {key:?}")),
                None => Ok(default),
            }
        };

        let journal_mode = match get(opts, atoms::journal_mode()) {
            Some(t) => {
                let mode = t
                    .decode::<String>()
                    .map_err(|_| "journal_mode must be a string".to_string())?;
                if !JOURNAL_MODES.contains(&mode.as_str()) {
                    return Err(format!("unknown journal mode: {mode}"));
                }
                Some(mode)
            }
            None => None,
        };

        let encryption = match get(opts, atoms::encryption()) {
            Some(t) => {
                let cipher = get(t, atoms::cipher())
                    .and_then(|c| c.decode::<String>().ok())
                    .ok_or("encryption requires a :cipher")?;
                let hexkey = get(t, atoms::key())
                    .and_then(|k| k.decode::<String>().ok())
                    .ok_or("encryption requires a hex :key")?;
                check_hex_key(&hexkey)?;
                Some(EncryptionOpts { cipher, hexkey })
            }
            None => None,
        };

        let experimental = match get(opts, atoms::experimental()) {
            Some(t) => t
                .decode::<Vec<String>>()
                .map_err(|_| "experimental must be a list of strings".to_string())?,
            None => Vec::new(),
        };

        Ok(Self {
            path,
            readonly: bool_opt(atoms::readonly(), false)?,
            create: bool_opt(atoms::create(), true)?,
            journal_mode,
            encryption,
            experimental,
            s3: get(opts, atoms::s3()),
        })
    }

    pub fn is_memory(&self) -> bool {
        let p = self.path.trim();
        p.is_empty() || p.starts_with(":memory:") || p.starts_with("file::memory:")
    }

    fn flags(&self) -> OpenFlags {
        let mut flags = OpenFlags::None;
        if self.readonly {
            flags |= OpenFlags::ReadOnly;
        } else if self.create {
            flags |= OpenFlags::Create;
        }
        flags
    }

    fn db_opts(&self) -> Result<DatabaseOpts, String> {
        let mut opts = DatabaseOpts::new().with_encryption(self.encryption.is_some());
        for feature in &self.experimental {
            opts = match feature.as_str() {
                "views" => opts.with_views(true),
                "custom_types" => opts.with_custom_types(true),
                "encryption" => opts.with_encryption(true),
                "index_method" => opts.with_index_method(true),
                "autovacuum" => opts.with_autovacuum(true),
                "vacuum" => opts.with_vacuum(true),
                "attach" => opts.with_attach(true),
                "generated_columns" => opts.with_generated_columns(true),
                "without_rowid" => opts.with_without_rowid(true),
                "mvcc_passive_checkpoint" => opts.with_experimental_mvcc_passive_checkpoint(true),
                other => return Err(format!("unknown experimental feature: {other}")),
            };
        }
        Ok(opts)
    }

    pub fn open(&self) -> Result<Opened, String> {
        // See log_guard: a database that can ATTACH is never MVCC, and S3
        // databases (replicas too) always are.
        let attach = self.experimental.iter().any(|feature| feature == "attach");
        let mvcc = || {
            self.s3.is_some()
                || self
                    .journal_mode
                    .as_ref()
                    .is_some_and(|m| m.contains("mvcc"))
                || crate::log_guard::is_mvcc_file(Path::new(&self.path))
        };
        if attach && mvcc() {
            return Err(crate::log_guard::ATTACH_WITHOUT_MVCC.into());
        }

        if let Some(term) = self.s3 {
            let mut cfg = crate::s3_nif::decode_config(term)?;
            if cfg.replica {
                self.check_s3()?;
                cfg.encryption = self.encryption.clone();
                return crate::replica::open(cfg, &self.path, self.db_opts()?);
            }
        }

        let io: Arc<dyn IO> = if self.is_memory() {
            Arc::new(MemoryIO::new())
        } else {
            Arc::new(PlatformIO::new().map_err(|e| e.to_string())?)
        };
        let io: Arc<dyn IO> = if attach {
            Arc::new(crate::log_guard::NoMvccLogs(io))
        } else {
            io
        };

        let mut options = OpenOptions::new(Arc::new(SqliteDialect))
            .flags(self.flags())
            .db_opts(self.db_opts()?)
            .encryption(self.encryption.clone());

        // Before an S3 restore writes the MVCC log or turso replays it.
        let path = Path::new(&self.path);
        let log_claim = if self.is_memory() {
            None
        } else if crate::log_guard::is_mvcc_file(path) {
            Some(crate::log_guard::claim(
                path,
                crate::log_guard::Use::Existing,
            )?)
        } else if self.s3.is_some() {
            Some(crate::log_guard::claim(path, crate::log_guard::Use::New)?)
        } else {
            None
        };

        // s3 hook: restore from S3 (or create), take the writer lease, and
        // attach the storage that uploads every commit.
        let s3 = match self.s3 {
            Some(term) => Some(self.prepare_s3(term, io.clone())?),
            None if self.is_memory() => None,
            // A plain open of a file whose S3 database is open here shares it.
            None => crate::s3::live_storage(std::path::Path::new(&self.path)),
        };
        if self.s3.is_none() && s3.is_some() {
            if let Some(mode) = self.journal_mode.as_ref().filter(|m| !m.contains("mvcc")) {
                return Err(format!(
                    "{} is open with S3 durability here; journal_mode must stay mvcc, got {mode}",
                    self.path
                ));
            }
        }
        if let (None, Some(storage)) = (self.s3, &s3) {
            // A plain open shares the S3 database's decrypted state: it needs
            // the same key (or none for an unencrypted one).
            if !same_encryption(storage.encryption(), self.encryption.as_ref()) {
                return Err(format!(
                    "{} is open with S3 durability here with another :encryption choice; \
                     open it with the same :encryption",
                    self.path
                ));
            }
        }
        if s3.is_none() && !self.is_memory() {
            // turso hands this open the Database already open for the path,
            // with its decrypted pages: only an open with the same key may
            // share it (or none, for an unencrypted file).
            if let Some(open) = plain_encryption(&self.path) {
                if !same_encryption(open.as_ref(), self.encryption.as_ref()) {
                    return Err(format!(
                        "{} is already open in this VM with another :encryption choice; \
                         every open of a file needs the same :encryption (the key it was \
                         created with, or none for an unencrypted file)",
                        self.path
                    ));
                }
            }
        }
        options = options.durable_storage(s3.clone().map(|s| s as Arc<dyn DurableStorage>));

        let db = Database::open(io.clone(), &self.path, options).map_err(|e| {
            let message = crate::error::locked_elsewhere(&e).unwrap_or_else(|| e.to_string());
            crate::log_guard::explain_open_error(Path::new(&self.path), message)
        })?;
        if let Some(claim) = log_claim {
            claim.keep_while(&db);
        }
        if s3.is_none() && !self.is_memory() {
            register_plain(&self.path, &db, self.encryption.clone());
        }

        let key = match &self.encryption {
            Some(enc) => {
                Some(EncryptionKey::from_hex_string(&enc.hexkey).map_err(|e| e.to_string())?)
            }
            None => None,
        };
        let conn = db.connect_with_encryption(key).map_err(|e| e.to_string())?;

        // Turso shares one Database per file within the process, so a
        // read-only open of a file that is already open read-write gets the
        // read-write instance. query_only enforces read-only per connection.
        if self.readonly {
            conn.set_query_only(true);
        }

        Ok(Opened {
            io,
            db,
            conn,
            s3,
            replica: None,
        })
    }

    fn prepare_s3(&self, term: Term<'a>, io: Arc<dyn IO>) -> Result<Arc<S3DurableStorage>, String> {
        if self.readonly {
            return Err(
                "s3 durability does not support read-only opens; use s3: [mode: :replica]".into(),
            );
        }
        self.check_s3()?;
        // Restoring would replace the file under those connections.
        if plain_open(&self.path) {
            return Err(format!(
                "{} is open without :s3 in this VM; close those connections before opening it with :s3",
                self.path
            ));
        }
        crate::s3_nif::prepare(term, &self.path, self.encryption.clone(), io)
    }

    fn check_s3(&self) -> Result<(), String> {
        if self.is_memory() {
            return Err("s3 durability needs a file path, not an in-memory database".into());
        }
        if let Some(mode) = &self.journal_mode {
            if !mode.contains("mvcc") {
                return Err(format!(
                    "s3 durability requires journal_mode mvcc, got {mode}"
                ));
            }
        }
        Ok(())
    }
}

/// Files open without S3 in this VM, so an S3 open (which restores over the
/// file) can refuse instead of replacing it under them.
/// Each with the encryption it was opened with.
type PlainOpen = (
    std::path::PathBuf,
    std::sync::Weak<Database>,
    Option<EncryptionOpts>,
);

static PLAIN: std::sync::Mutex<Vec<PlainOpen>> = std::sync::Mutex::new(Vec::new());

/// Whether two opens make the same encryption choice: the same cipher and
/// key, or no encryption.
pub fn same_encryption(a: Option<&EncryptionOpts>, b: Option<&EncryptionOpts>) -> bool {
    match (a, b) {
        (Some(a), Some(b)) => a.cipher == b.cipher && a.hexkey.eq_ignore_ascii_case(&b.hexkey),
        (None, None) => true,
        _ => false,
    }
}

fn register_plain(path: &str, db: &Arc<Database>, encryption: Option<EncryptionOpts>) {
    let Ok(key) = crate::s3::canonical_key(std::path::Path::new(path)) else {
        return;
    };
    let mut plain = crate::conn::lock(&PLAIN);
    plain.retain(|(_, db, _)| db.strong_count() > 0);
    plain.push((key, Arc::downgrade(db), encryption));
}

/// The encryption a file open without S3 in this VM was opened with, if it is.
fn plain_encryption(path: &str) -> Option<Option<EncryptionOpts>> {
    let key = crate::s3::canonical_key(std::path::Path::new(path)).ok()?;
    let mut plain = crate::conn::lock(&PLAIN);
    plain.retain(|(_, db, _)| db.strong_count() > 0);
    plain
        .iter()
        .find(|(open, _, _)| *open == key)
        .map(|(_, _, encryption)| encryption.clone())
}

fn plain_open(path: &str) -> bool {
    let Ok(key) = crate::s3::canonical_key(std::path::Path::new(path)) else {
        return false;
    };
    let mut plain = crate::conn::lock(&PLAIN);
    plain.retain(|(_, db, _)| db.strong_count() > 0);
    plain.iter().any(|(open, _, _)| *open == key)
}

/// turso's own error quotes the offending character of the key; this one
/// doesn't. The key's length is checked by turso against the cipher.
pub fn check_hex_key(hexkey: &str) -> Result<(), String> {
    if hexkey.len().is_multiple_of(2) && hexkey.bytes().all(|b| b.is_ascii_hexdigit()) {
        Ok(())
    } else {
        Err("encryption :key must be hex encoded (an even number of 0-9a-f digits)".into())
    }
}
