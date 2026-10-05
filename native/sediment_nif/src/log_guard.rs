//! Keeps distinct database files from sharing one MVCC log.
//!
//! turso_core names a database's MVCC log after the file without its last
//! extension (`app.db` -> `app.db-log`), so `app`, `app.db`, `app.sqlite`,
//! `app.1` and `app.2` would all use `app.db-log`: each would replay,
//! checkpoint and truncate the others' commits. Sediment keeps turso's name,
//! which other turso tools look for, and refuses to use a log for a database
//! when another database file maps to it, or when a database open in this VM
//! under another name already uses it.
//!
//! turso puts the log next to the file a symlink points to (S3 databases
//! refuse symlinked paths), so that is the log claimed. Names are compared
//! ignoring case, as on macOS and Windows volumes.
//!
//! ATTACH opens the attached file through the main database's IO, and names
//! its log the same way. A database opened with `experimental: [:attach]` is
//! never MVCC, and its IO ([`NoMvccLogs`]) opens no MVCC log, so no attached
//! file uses one, whatever SQL attached it.

use std::path::{Path, PathBuf};
use std::ptr::NonNull;
use std::sync::{Arc, Mutex, Weak};

use turso_core::io::FileId;
use turso_core::{
    Clock, Completion, Database, File, LimboError, MemoryIO, MonotonicInstant, OpenFlags,
    WallClockInstant, IO,
};

use crate::s3::restore::log_path;

/// How a database is about to use its MVCC log.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Use {
    /// An existing MVCC database: only another MVCC database file can share
    /// its log (a WAL or legacy file never reads it, and turso refuses to
    /// open one next to a log with data in it).
    Existing,
    /// A database that becomes MVCC (created, restored, switched): any other
    /// database file mapping to the log is refused, since that file couldn't
    /// be opened next to the new log, or switched to MVCC, anymore.
    New,
}

/// A log file: its directory and its file name, lowercased.
#[derive(Clone, PartialEq, Eq)]
struct Log {
    dir: PathBuf,
    name: String,
}

impl Log {
    fn of(db: &Path) -> Option<Log> {
        Some(Log {
            dir: db.parent()?.to_path_buf(),
            name: fold(&log_path(Path::new(db.file_name()?))),
        })
    }
}

fn fold(name: &Path) -> String {
    name.to_string_lossy().to_lowercase()
}

/// The database file `db` names (the file a symlink points to, once it
/// exists; otherwise the path with its directory canonicalized) and its log.
fn keys(db: &Path) -> Option<(PathBuf, Log)> {
    let absolute = std::path::absolute(db).ok()?;
    let target = match std::fs::canonicalize(&absolute) {
        Ok(target) => target,
        Err(_) => std::fs::canonicalize(absolute.parent()?)
            .ok()?
            .join(absolute.file_name()?),
    };
    let log = Log::of(&target)?;
    Some((target, log))
}

enum Holder {
    /// An operation in progress (an S3 restore, an import, an open).
    Claim(Weak<()>),
    /// A database open in this VM.
    Database(Weak<Database>),
}

struct Entry {
    db: PathBuf,
    log: Log,
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
    entry: Option<(PathBuf, Log)>,
    _token: Arc<()>,
}

impl Claim {
    /// Keeps the log reserved for as long as `database` is open.
    pub fn keep_while(self, database: &Arc<Database>) {
        if let Some((db, log)) = self.entry.clone() {
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
pub fn claim(db: &Path, purpose: Use) -> Result<Claim, String> {
    let token = Arc::new(());
    // Without a resolvable directory there is nothing to share; the open or
    // restore reports the path itself.
    let Some((target, log)) = keys(db) else {
        return Ok(Claim {
            entry: None,
            _token: token,
        });
    };
    let mut in_use = crate::conn::lock(&IN_USE);
    in_use.retain(Entry::live);
    if let Some(other) = in_use.iter().find(|e| e.db != target && e.log == log) {
        return Err(conflict(db, &other.db, &target, "is open in this VM"));
    }
    // A database open here was checked when it opened: a database file added
    // next to it since is refused when it claims the log.
    if !in_use.iter().any(|e| e.db == target) {
        if let Some(other) = sharer(&target, &log, purpose) {
            return Err(conflict(db, &other, &target, "exists"));
        }
    }
    in_use.push(Entry {
        db: target.clone(),
        log: log.clone(),
        holder: Holder::Claim(Arc::downgrade(&token)),
    });
    Ok(Claim {
        entry: Some((target, log)),
        _token: token,
    })
}

/// turso's error for a WAL database next to another database's MVCC log
/// suggests corruption; say whose log it is instead.
pub fn explain_open_error(db: &Path, error: String) -> String {
    if !error.contains("MVCC logical log file exists") {
        return error;
    }
    let found =
        keys(db).and_then(|(target, log)| Some((sharer(&target, &log, Use::Existing)?, target)));
    match found {
        Some((other, target)) => format!("{error} ({})", conflict(db, &other, &target, "exists")),
        None => error,
    }
}

/// `db` (the file `target`) would share its log with `other`.
fn conflict(db: &Path, other: &Path, target: &Path, how: &str) -> String {
    format!(
        "{} would share its MVCC log {} with {}, which {how}: turso names the log after \
         the database file without its extension. Give each database file its own name \
         before the last dot (app-1.db and app-2.db, not app.1 and app.2)",
        db.display(),
        log_path(target).display(),
        other.display()
    )
}

/// Another database file in `log`'s directory whose log is `log` (for
/// [`Use::Existing`], only an MVCC one). `db` itself, under any name, isn't,
/// and neither is a symlink: its file's log is next to that file.
fn sharer(db: &Path, log: &Log, purpose: Use) -> Option<PathBuf> {
    std::fs::read_dir(&log.dir)
        .ok()?
        .filter_map(|entry| entry.ok())
        .filter(|entry| entry.file_type().is_ok_and(|kind| !kind.is_symlink()))
        .filter(|entry| fold(&log_path(Path::new(&entry.file_name()))) == log.name)
        .map(|entry| entry.path())
        .filter(|path| std::fs::canonicalize(path).map_or(true, |real| real != db))
        .find(|path| match purpose {
            Use::Existing => is_mvcc_file(path),
            Use::New => is_database(path),
        })
}

/// Why a database opened with `experimental: [:attach]` uses no MVCC.
pub const ATTACH_WITHOUT_MVCC: &str =
    "experimental :attach can't be combined with MVCC: a database opened with :attach \
     can't be in MVCC mode (no journal_mode mvcc, no :s3), and can't attach an MVCC \
     database, because turso would name the attached database's MVCC log after its file \
     without the extension, which may be another database's log";

/// The IO of a database opened with `experimental: [:attach]`: it opens no
/// MVCC log (see the module docs).
pub struct NoMvccLogs(pub Arc<dyn IO>);

impl NoMvccLogs {
    fn check(path: &str) -> turso_core::Result<()> {
        if fold(Path::new(path)).ends_with(".db-log") {
            return Err(LimboError::InvalidArgument(ATTACH_WITHOUT_MVCC.into()));
        }
        Ok(())
    }
}

impl Clock for NoMvccLogs {
    fn current_time_monotonic(&self) -> MonotonicInstant {
        self.0.current_time_monotonic()
    }

    fn current_time_wall_clock(&self) -> WallClockInstant {
        self.0.current_time_wall_clock()
    }
}

impl IO for NoMvccLogs {
    fn open_file(
        &self,
        path: &str,
        flags: OpenFlags,
        direct: bool,
    ) -> turso_core::Result<Arc<dyn File>> {
        Self::check(path)?;
        self.0.open_file(path, flags, direct)
    }

    fn open_shared_wal_file(&self, path: &str) -> turso_core::Result<Arc<dyn File>> {
        Self::check(path)?;
        self.0.open_shared_wal_file(path)
    }

    fn remove_file(&self, path: &str) -> turso_core::Result<()> {
        self.0.remove_file(path)
    }

    fn supports_shared_wal_coordination(&self) -> bool {
        self.0.supports_shared_wal_coordination()
    }

    fn step(&self) -> turso_core::Result<()> {
        self.0.step()
    }

    fn cancel(&self, c: &[Completion]) -> turso_core::Result<()> {
        self.0.cancel(c)
    }

    fn drain_completions(&self, completions: &[Completion]) -> turso_core::Result<()> {
        self.0.drain_completions(completions)
    }

    fn wait_for_completion(&self, c: Completion) -> turso_core::Result<()> {
        self.0.wait_for_completion(c)
    }

    fn generate_random_number(&self) -> i64 {
        self.0.generate_random_number()
    }

    fn fill_bytes(&self, dest: &mut [u8]) {
        self.0.fill_bytes(dest)
    }

    fn get_memory_io(&self) -> Arc<MemoryIO> {
        self.0.get_memory_io()
    }

    fn register_fixed_buffer(&self, ptr: NonNull<u8>, len: usize) -> turso_core::Result<u32> {
        self.0.register_fixed_buffer(ptr, len)
    }

    fn yield_now(&self) {
        self.0.yield_now()
    }

    fn sleep(&self, duration: std::time::Duration) {
        self.0.sleep(duration)
    }

    fn file_id(&self, path: &str) -> turso_core::Result<FileId> {
        self.0.file_id(path)
    }
}

/// Whether `path` is a database file (SQLite's header, or turso's encrypted
/// one, which keeps the rest of the header readable), not a WAL, log or
/// anything else with the same stem.
fn is_database(path: &Path) -> bool {
    use std::io::Read;
    let mut magic = [0u8; 16];
    std::fs::File::open(path)
        .and_then(|mut file| file.read_exact(&mut magic))
        .is_ok_and(|()| magic == *b"SQLite format 3\0" || magic.starts_with(b"Turso\0"))
}

/// Whether `path` is a database file in MVCC mode (its header's read version
/// is 255). A missing or unreadable file isn't.
pub fn is_mvcc_file(path: &Path) -> bool {
    use std::io::Read;
    let mut header = [0u8; 20];
    std::fs::File::open(path)
        .and_then(|mut file| file.read_exact(&mut header))
        .is_ok_and(|()| {
            (header.starts_with(b"SQLite format 3\0") || header.starts_with(b"Turso\0"))
                && header[18] == 255
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dir() -> PathBuf {
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let dir = std::env::temp_dir().join(format!(
            "sediment-log-guard-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn database(path: &Path, version: u8) {
        let mut bytes = b"SQLite format 3\0".to_vec();
        bytes.resize(100, 0);
        bytes[18] = version;
        bytes[19] = version;
        std::fs::write(path, bytes).unwrap();
    }

    fn mvcc(path: &Path) {
        database(path, 255);
    }

    fn wal(path: &Path) {
        database(path, 2);
    }

    #[test]
    fn refuses_a_log_another_mvcc_database_file_maps_to() {
        let dir = dir();
        mvcc(&dir.join("app.1"));
        mvcc(&dir.join("app.2"));
        for purpose in [Use::Existing, Use::New] {
            let err = claim(&dir.join("app.2"), purpose).err().expect("refused");
            assert!(err.contains("app.db-log"), "{err}");
            assert!(err.contains("app.1"), "{err}");
        }
        assert!(claim(&dir.join("other.db"), Use::New).is_ok());
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn other_database_files_only_block_a_new_mvcc_database() {
        // An export or backup (WAL, legacy) next to an MVCC database never
        // reads its log; a database becoming MVCC there would lock it out.
        let dir = dir();
        mvcc(&dir.join("app.db"));
        wal(&dir.join("app.sqlite"));
        database(&dir.join("app.bak"), 1);
        assert!(claim(&dir.join("app.db"), Use::Existing).is_ok());
        let err = claim(&dir.join("app.db"), Use::New).err().expect("refused");
        assert!(
            err.contains("app.bak") || err.contains("app.sqlite"),
            "{err}"
        );
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn ignores_sidecars_and_other_files_with_the_stem() {
        let dir = dir();
        mvcc(&dir.join("app.db"));
        for sidecar in ["app.db-wal", "app.db-log", "app.txt", "app.db-shm"] {
            std::fs::write(dir.join(sidecar), b"not a database file header").unwrap();
        }
        std::fs::write(dir.join("app.empty"), b"").unwrap();
        assert!(claim(&dir.join("app.db"), Use::New).is_ok());
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn refuses_a_log_in_use_in_this_vm_until_released() {
        let dir = dir();
        let first = claim(&dir.join("app.1"), Use::New).expect("first");
        // The same database may claim it again (a pool's connections).
        let again = claim(&dir.join("app.1"), Use::Existing).expect("same database");
        let err = claim(&dir.join("app.2"), Use::New).err().expect("refused");
        assert!(err.contains("is open in this VM"), "{err}");
        drop((first, again));
        assert!(claim(&dir.join("app.2"), Use::New).is_ok());
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn log_names_differing_in_case_collide() {
        // One file on macOS and Windows volumes.
        let dir = dir();
        mvcc(&dir.join("App.1"));
        mvcc(&dir.join("app.2"));
        let err = claim(&dir.join("app.2"), Use::Existing)
            .err()
            .expect("refused");
        assert!(err.contains("App.1"), "{err}");
        let held = claim(&dir.join("Other.db"), Use::New).expect("other");
        let err = claim(&dir.join("other.x"), Use::New)
            .err()
            .expect("refused");
        assert!(err.contains("is open in this VM"), "{err}");
        drop(held);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn a_symlink_claims_the_log_next_to_its_target() {
        // turso keeps the log next to the file a symlink points to.
        let dir = dir();
        let (v, d) = (dir.join("v"), dir.join("d"));
        std::fs::create_dir_all(&v).unwrap();
        std::fs::create_dir_all(&d).unwrap();
        mvcc(&v.join("app.1"));
        std::os::unix::fs::symlink(v.join("app.1"), d.join("one.db")).unwrap();
        std::os::unix::fs::symlink(v.join("app.1"), d.join("same.db")).unwrap();
        let one = claim(&d.join("one.db"), Use::Existing).expect("one");
        // Two links to one file are one database.
        let same = claim(&d.join("same.db"), Use::Existing).expect("same file");

        mvcc(&v.join("app.2"));
        std::os::unix::fs::symlink(v.join("app.2"), d.join("two.db")).unwrap();
        let err = claim(&d.join("two.db"), Use::Existing)
            .err()
            .expect("refused");
        assert!(
            err.contains(&v.join("app.db-log").display().to_string()),
            "{err}"
        );
        drop((one, same));
        // Without the other open, the target's directory still shows app.1.
        let err = claim(&d.join("two.db"), Use::Existing)
            .err()
            .expect("refused");
        assert!(
            err.contains(&v.join("app.1").display().to_string()),
            "{err}"
        );
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn a_symlink_doesnt_share_the_log_its_own_name_maps_to() {
        // app.1 -> v/one.db uses v/one.db-log; app.2 uses app.db-log.
        let dir = dir();
        let v = dir.join("v");
        std::fs::create_dir_all(&v).unwrap();
        mvcc(&v.join("one.db"));
        mvcc(&dir.join("app.2"));
        std::os::unix::fs::symlink(v.join("one.db"), dir.join("app.1")).unwrap();
        let one = claim(&dir.join("app.1"), Use::Existing).expect("app.1");
        let two = claim(&dir.join("app.2"), Use::Existing).expect("app.2");
        drop((one, two));
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn the_attach_io_opens_no_mvcc_log() {
        let dir = dir();
        let io = NoMvccLogs(Arc::new(MemoryIO::new()));
        for log in ["app.db-log", "APP.DB-LOG"] {
            let path = dir.join(log).display().to_string();
            assert!(
                io.open_file(&path, OpenFlags::Create, false).is_err(),
                "{log}"
            );
        }
        let path = dir.join("app.db").display().to_string();
        assert!(io.open_file(&path, OpenFlags::Create, false).is_ok());
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn reads_the_mvcc_header() {
        let dir = dir();
        let path = dir.join("m.db");
        mvcc(&path);
        assert!(is_mvcc_file(&path));
        wal(&path);
        assert!(!is_mvcc_file(&path));
        let mut bytes = vec![0u8; 100];
        bytes[18] = 255;
        std::fs::write(&path, &bytes).unwrap();
        assert!(!is_mvcc_file(&path), "not a database header");
        assert!(!is_mvcc_file(&dir.join("missing.db")));
        std::fs::remove_dir_all(dir).unwrap();
    }
}
