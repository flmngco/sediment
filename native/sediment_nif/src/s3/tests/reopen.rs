use std::sync::Arc;
use std::time::{Duration, Instant};

use turso_core::{Database, OpenFlags, OpenOptions, PlatformIO, SqliteDialect, IO};

use super::faulty::FaultyStore;
use super::{config, open_db, open_with, TempDir};
use crate::s3::{prepare, restore, Attached};

fn wipe(path: &std::path::Path) {
    for file in [
        path.to_path_buf(),
        restore::wal_path(path),
        restore::log_path(path),
    ] {
        let _ = std::fs::remove_file(file);
    }
}

#[test]
fn an_s3_open_never_creates_the_file_it_restored() {
    let store = FaultyStore::new();
    let dir = TempDir::new();
    let path = dir.db("a.db");
    drop(open_db(&config(&store, "a"), &path).unwrap());
    // The S3 prepare restores the file; a cleanup removes it before the turso
    // open (a pool stopped while one of its connections was still opening).
    let storage = prepare(&config(&store, "a"), &path).unwrap();
    wipe(&path);
    let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
    let opened = Database::open(
        io,
        path.to_str().unwrap(),
        OpenOptions::new(Arc::new(SqliteDialect))
            .flags(crate::open::s3_open_flags(OpenFlags::Create))
            .durable_storage(
                storage.clone() as Arc<dyn turso_core::mvcc::persistent_storage::DurableStorage>
            ),
    );
    // Before: turso created an empty database that isn't MVCC, and every
    // statement on it failed "the database left MVCC journal mode".
    assert!(opened.is_err());
    assert!(!path.exists());
}

#[test]
fn a_storage_whose_connections_all_closed_is_not_reused() {
    let store = FaultyStore::new();
    let dir = TempDir::new();
    let path = dir.db("a.db");
    let db = open_db(&config(&store, "a"), &path).unwrap();
    db.exec("CREATE TABLE t(id INTEGER PRIMARY KEY)").unwrap();
    db.exec("INSERT INTO t VALUES (1)").unwrap();
    // A connection that had it open closes; the storage lives on a moment
    // (a snapshot its close queued, held here).
    drop(Attached::new(&db.storage));
    let lingering = db.storage.clone();
    let old = lingering.info().generation;
    drop(db);
    let released = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(200));
        drop(lingering);
    });

    let started = Instant::now();
    let reopened = open_db(&config(&store, "a"), &path).unwrap();
    released.join().unwrap();
    // A storage of its own: it took the lease again.
    assert!(reopened.storage.info().generation > old);
    assert!(started.elapsed() >= Duration::from_millis(150));
    assert_eq!(reopened.int("SELECT count(*) FROM t"), 1);
}

#[test]
fn an_open_storage_is_still_shared() {
    let store = FaultyStore::new();
    let dir = TempDir::new();
    let path = dir.db("a.db");
    let db = open_db(&config(&store, "a"), &path).unwrap();
    let attached = Attached::new(&db.storage);
    let second = open_with(prepare(&config(&store, "a"), &path).unwrap(), &path);
    assert!(Arc::ptr_eq(&db.storage, &second.storage));
    drop(attached);
}
