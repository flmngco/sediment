//! Keeps distinct database files from sharing one MVCC log.
//!
//! turso_core names a database's MVCC log after the file without its last
//! extension (`app.db` -> `app.db-log`), so `app`, `app.db`, `app.sqlite`,
//! `app.1` and `app.2` would all use `app.db-log`: each would replay,
//! checkpoint and truncate the others' commits. Sediment keeps turso's name,
//! which other turso tools look for, and refuses to use a log for a database
//! when another database file maps to it, or when a database open in this VM
//! under another name already uses it.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, Weak};

use turso_core::Database;

use crate::s3::restore::log_path;

enum Holder {
    /// An operation in progress (an S3 restore, an import, an open).
    Claim(Weak<()>),
    /// A database open in this VM.
    Database(Weak<Database>),
}

struct Entry {
    db: PathBuf,
    log: PathBuf,
    holder: Holder,
}

impl Entry {
    fn live(&self) -> bool {
        match &self.holder {
            Holder::Claim(token) => token.strong_count() > 0,
            Holder::Database(db) => db.strong_count() > 0,
        }
    }
}

static IN_USE: Mutex<Vec<Entry>> = Mutex::new(Vec::new());

/// The right to use `db`'s MVCC log while it is held (or, after
/// [`Claim::keep_while`], while that database is open).
pub struct Claim {
    keys: Option<(PathBuf, PathBuf)>,
    _token: Arc<()>,
}

impl Claim {
    /// Keeps the log reserved for as long as `database` is open.
    pub fn keep_while(self, database: &Arc<Database>) {
        if let Some((db, log)) = self.keys.clone() {
            crate::conn::lock(&IN_USE).push(Entry {
                db,
                log,
                holder: Holder::Database(Arc::downgrade(database)),
            });
        }
    }
}

/// Claims the MVCC log of the database file `db` for this database, or says
/// which other database uses it.
pub fn claim(db: &Path) -> Result<Claim, String> {
    let token = Arc::new(());
    // Without a resolvable directory there is nothing to share; the open or
    // restore reports the path itself.
    let Some((db_key, log_key)) = keys(db) else {
        return Ok(Claim {
            keys: None,
            _token: token,
        });
    };
    let mut in_use = crate::conn::lock(&IN_USE);
    in_use.retain(Entry::live);
    if let Some(other) = in_use.iter().find(|e| e.log == log_key && e.db != db_key) {
        return Err(conflict(db, &other.db, &log_key, "is open in this VM"));
    }
    // A database open here was checked when it opened: a database file added
    // next to it since is refused when it claims the log.
    let open_here = in_use.iter().any(|e| e.log == log_key);
    if !open_here {
        if let Some(other) = sharer(&db_key) {
            return Err(conflict(db, &other, &log_key, "exists"));
        }
    }
    in_use.push(Entry {
        db: db_key.clone(),
        log: log_key.clone(),
        holder: Holder::Claim(Arc::downgrade(&token)),
    });
    Ok(Claim {
        keys: Some((db_key, log_key)),
        _token: token,
    })
}

/// turso's error for a WAL database next to another database's MVCC log
/// suggests corruption; say whose log it is instead.
pub fn explain_open_error(db: &Path, error: String) -> String {
    if !error.contains("MVCC logical log file exists") {
        return error;
    }
    match keys(db).and_then(|(db_key, log_key)| Some((sharer(&db_key)?, log_key))) {
        Some((other, log)) => format!("{error} ({})", conflict(db, &other, &log, "exists")),
        None => error,
    }
}

fn conflict(db: &Path, other: &Path, log: &Path, how: &str) -> String {
    format!(
        "{} would share its MVCC log {} with {}, which {how}: turso names the log after \
         the database file without its extension. Give each database file its own name \
         before the last dot (app-1.db and app-2.db, not app.1 and app.2)",
        db.display(),
        log.display(),
        other.display()
    )
}

/// `db`'s path and its log's, with the directory canonicalized.
fn keys(db: &Path) -> Option<(PathBuf, PathBuf)> {
    let absolute = std::path::absolute(db).ok()?;
    let dir = std::fs::canonicalize(absolute.parent()?).ok()?;
    let name = absolute.file_name()?;
    let log = log_path(Path::new(name));
    Some((dir.join(name), dir.join(log)))
}

/// Another database file next to `db` whose log is `db`'s.
fn sharer(db: &Path) -> Option<PathBuf> {
    let dir = db.parent()?;
    let name = db.file_name()?;
    let log = log_path(Path::new(name));
    std::fs::read_dir(dir)
        .ok()?
        .filter_map(|entry| entry.ok())
        .filter(|entry| entry.file_name() != name)
        .filter(|entry| log_path(Path::new(&entry.file_name())) == log)
        .map(|entry| entry.path())
        .find(|path| is_database(path))
}

/// Whether `path` is a database file (SQLite's header, or turso's encrypted
/// one), not a WAL, log or anything else with the same stem.
fn is_database(path: &Path) -> bool {
    use std::io::Read;
    let mut magic = [0u8; 16];
    std::fs::File::open(path)
        .and_then(|mut file| file.read_exact(&mut magic))
        .is_ok_and(|()| magic == *b"SQLite format 3\0" || magic.starts_with(b"Turso\0"))
}

/// Whether the database file at `path` is in MVCC mode (its header's read
/// version is 255). A missing or unreadable file isn't.
pub fn is_mvcc_file(path: &Path) -> bool {
    use std::io::Read;
    let mut header = [0u8; 20];
    std::fs::File::open(path)
        .and_then(|mut file| file.read_exact(&mut header))
        .is_ok_and(|()| header[18] == 255)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dir() -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "sediment-log-guard-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn database(path: &Path) {
        let mut bytes = b"SQLite format 3\0".to_vec();
        bytes.resize(100, 0);
        std::fs::write(path, bytes).unwrap();
    }

    #[test]
    fn refuses_a_log_another_database_file_maps_to() {
        let dir = dir();
        database(&dir.join("app.1"));
        database(&dir.join("app.2"));
        let err = claim(&dir.join("app.2")).err().expect("refused");
        assert!(err.contains("app.db-log"), "{err}");
        assert!(err.contains("app.1"), "{err}");
        assert!(claim(&dir.join("other.db")).is_ok());
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn ignores_sidecars_and_other_files_with_the_stem() {
        let dir = dir();
        database(&dir.join("app.db"));
        for sidecar in ["app.db-wal", "app.db-log", "app.txt", "app.db-shm"] {
            std::fs::write(dir.join(sidecar), b"not a database file header").unwrap();
        }
        std::fs::write(dir.join("app.empty"), b"").unwrap();
        assert!(claim(&dir.join("app.db")).is_ok());
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn refuses_a_log_in_use_in_this_vm_until_released() {
        let dir = dir();
        let first = claim(&dir.join("app.1")).expect("first");
        // The same database may claim it again (a pool's connections).
        let again = claim(&dir.join("app.1")).expect("same database");
        let err = claim(&dir.join("app.2")).err().expect("refused");
        assert!(err.contains("is open in this VM"), "{err}");
        drop((first, again));
        assert!(claim(&dir.join("app.2")).is_ok());
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn reads_the_mvcc_header() {
        let dir = dir();
        let path = dir.join("m.db");
        let mut bytes = b"SQLite format 3\0".to_vec();
        bytes.resize(100, 0);
        bytes[18] = 255;
        bytes[19] = 255;
        std::fs::write(&path, &bytes).unwrap();
        assert!(is_mvcc_file(&path));
        bytes[18] = 2;
        std::fs::write(&path, &bytes).unwrap();
        assert!(!is_mvcc_file(&path));
        assert!(!is_mvcc_file(&dir.join("missing.db")));
        std::fs::remove_dir_all(dir).unwrap();
    }
}
