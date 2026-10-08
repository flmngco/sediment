use std::sync::Arc;
use std::time::{Duration, Instant};

use turso_core::{Database, OpenFlags, OpenOptions, PlatformIO, SqliteDialect, IO};

use super::faulty::FaultyStore;
use super::{config, open_db, open_with, TempDir};
use crate::s3::{attach_live, prepare, prepare_attached, restore, Attached};

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
    drop(Attached::try_new(&db.storage).unwrap());
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
    let attached = Attached::try_new(&db.storage).unwrap();
    let second = open_with(prepare(&config(&store, "a"), &path).unwrap(), &path);
    assert!(Arc::ptr_eq(&db.storage, &second.storage));
    drop(attached);
}

fn platform_io() -> Arc<dyn IO> {
    Arc::new(PlatformIO::new().unwrap())
}

#[test]
fn an_open_overlapping_the_last_close_keeps_the_storage_open() {
    let store = FaultyStore::new();
    let dir = TempDir::new();
    let path = dir.db("a.db");
    let cfg = config(&store, "a");
    let db = open_db(&cfg, &path).unwrap();
    let conn_a = Attached::try_new(&db.storage).unwrap();
    // conn_b's open reuses the storage; conn_a closes before conn_b's handle
    // exists (its turso open and connect still run).
    let conn_b = prepare_attached(&cfg, &path, platform_io()).unwrap();
    assert!(Arc::ptr_eq(conn_b.storage(), &db.storage));
    drop(conn_a);
    // Before: conn_b attached only after this, to a storage already marked
    // closing for good, and every later open waited 11 s and was refused.
    assert!(!db.storage.is_closing());
    assert_eq!(db.storage.attached(), 1);

    let started = Instant::now();
    let third = prepare_attached(&cfg, &path, platform_io()).unwrap();
    assert!(Arc::ptr_eq(third.storage(), &db.storage));
    assert!(started.elapsed() < Duration::from_millis(500));
    db.exec("CREATE TABLE t(id INTEGER PRIMARY KEY)").unwrap();

    drop(third);
    drop(conn_b);
    assert!(db.storage.is_closing());
    assert!(Attached::try_new(&db.storage).is_none());
}

#[test]
fn closing_is_set_only_when_no_open_can_attach() {
    let store = FaultyStore::new();
    let dir = TempDir::new();
    let db = open_db(&config(&store, "a"), &dir.db("a.db")).unwrap();
    let base = Attached::try_new(&db.storage).unwrap();
    // Connections come and go while one stays open: the count never passes
    // through 0, so the storage never closes.
    std::thread::scope(|scope| {
        for _ in 0..8 {
            scope.spawn(|| {
                for _ in 0..10_000 {
                    drop(Attached::try_new(&db.storage).unwrap());
                }
            });
        }
    });
    assert!(!db.storage.is_closing());
    drop(base);
    drop(db);

    // An open racing the last close either attached first (and the storage
    // stays open while it lives) or finds it closing, never both.
    for i in 0..20 {
        let store = FaultyStore::new();
        let db = open_db(&config(&store, "a"), &dir.db(&format!("r{i}.db"))).unwrap();
        let last = Attached::try_new(&db.storage).unwrap();
        let racing = std::thread::spawn({
            let storage = db.storage.clone();
            move || Attached::try_new(&storage)
        });
        drop(last);
        let won = racing.join().unwrap();
        assert_eq!(won.is_some(), !db.storage.is_closing());
        assert_eq!(db.storage.attached(), usize::from(won.is_some()));
    }
}

#[test]
fn a_plain_open_never_attaches_to_a_closing_storage() {
    let store = FaultyStore::new();
    let dir = TempDir::new();
    let path = dir.db("a.db");
    let db = open_db(&config(&store, "a"), &path).unwrap();
    drop(Attached::try_new(&db.storage).unwrap());
    assert!(db.storage.is_closing());
    let lingering = db.storage.clone();
    drop(db);
    let released = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(200));
        drop(lingering);
    });
    let started = Instant::now();
    // It waits for the storage to go, then opens the file on its own.
    assert!(attach_live(&path).unwrap().is_none());
    assert!(started.elapsed() >= Duration::from_millis(150));
    released.join().unwrap();
}
