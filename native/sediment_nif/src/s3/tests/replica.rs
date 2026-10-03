use std::path::Path;
use std::sync::Arc;

use turso_core::{Database, OpenOptions, PlatformIO, SqliteDialect, IO};

use super::faulty::{Fault, FaultyStore, Op};
use super::{config, open_db, TempDir};
use crate::s3::replica::stage;
use crate::s3::S3Error;

fn count(path: &Path) -> i64 {
    let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
    let db = Database::open(
        io,
        path.to_str().unwrap(),
        OpenOptions::new(Arc::new(SqliteDialect)),
    )
    .unwrap();
    let conn = db.connect().unwrap();
    let mut stmt = conn.query("SELECT count(*) FROM t").unwrap().unwrap();
    let rows = stmt.run_collect_rows().unwrap();
    conn.close().unwrap();
    rows[0][0].to_string().parse().unwrap()
}

#[test]
fn replica_restores_committed_state_without_writing_to_s3() {
    let store = FaultyStore::new();
    let dir = TempDir::new();
    let writer = open_db(&config(&store, "writer"), &dir.db("w.db")).unwrap();
    writer.exec("CREATE TABLE t (x INTEGER)").unwrap();
    writer.exec("INSERT INTO t VALUES (1), (2)").unwrap();

    let puts_before = store.put_log().len();
    let replica_path = dir.db("r.db");
    let state = stage(&config(&store, "reader"), &replica_path)
        .unwrap()
        .install(&replica_path)
        .unwrap();

    assert_eq!(store.put_log().len(), puts_before, "a replica never writes");
    assert_eq!(state.writer, "writer");
    assert_eq!(count(&replica_path), 2);

    // The writer is unaffected and keeps committing.
    writer.exec("INSERT INTO t VALUES (3)").unwrap();
}

#[test]
fn replica_follows_new_commits_and_new_epochs() {
    let store = FaultyStore::new();
    let dir = TempDir::new();
    let writer = open_db(&config(&store, "writer"), &dir.db("w.db")).unwrap();
    writer.exec("CREATE TABLE t (x INTEGER)").unwrap();
    writer.exec("INSERT INTO t VALUES (1)").unwrap();

    let replica_path = dir.db("r.db");
    let cfg = config(&store, "reader");
    let first = stage(&cfg, &replica_path)
        .unwrap()
        .install(&replica_path)
        .unwrap();
    assert_eq!(count(&replica_path), 1);

    writer.exec("INSERT INTO t VALUES (2)").unwrap();
    let same_epoch = stage(&cfg, &replica_path)
        .unwrap()
        .install(&replica_path)
        .unwrap();
    assert_eq!(same_epoch.epoch, first.epoch);
    assert!(same_epoch.log_bytes > first.log_bytes);
    assert_eq!(count(&replica_path), 2);

    writer.checkpoint();
    writer.exec("INSERT INTO t VALUES (3)").unwrap();
    let new_epoch = stage(&cfg, &replica_path)
        .unwrap()
        .install(&replica_path)
        .unwrap();
    assert_ne!(new_epoch.epoch, first.epoch);
    assert_eq!(count(&replica_path), 3);
}

#[test]
fn replica_retries_when_the_epoch_disappears_mid_restore() {
    let store = FaultyStore::new();
    let dir = TempDir::new();
    let writer = open_db(&config(&store, "writer"), &dir.db("w.db")).unwrap();
    writer.exec("CREATE TABLE t (x INTEGER)").unwrap();
    writer.exec("INSERT INTO t VALUES (1)").unwrap();

    store.inject(Op::Get, "snapshots/", Fault::Fail, 1);
    let replica_path = dir.db("r.db");
    stage(&config(&store, "reader"), &replica_path)
        .unwrap()
        .install(&replica_path)
        .unwrap();
    assert_eq!(count(&replica_path), 1);
}

#[test]
fn replica_of_a_missing_database_is_an_error() {
    let store = FaultyStore::new();
    let dir = TempDir::new();
    match stage(&config(&store, "reader"), &dir.db("r.db")) {
        Err(S3Error::Config(msg)) => assert!(msg.contains("no database"), "{msg}"),
        Err(other) => panic!("unexpected error {other}"),
        Ok(_) => panic!("restored a database that does not exist"),
    }
}

#[test]
fn a_failed_stage_leaves_no_files_behind() {
    let store = FaultyStore::new();
    let dir = TempDir::new();
    let writer = open_db(&config(&store, "writer"), &dir.db("w.db")).unwrap();
    writer.exec("CREATE TABLE t (x INTEGER)").unwrap();

    store.inject(Op::Get, "snapshots/", Fault::Fail, 10);
    assert!(stage(&config(&store, "reader"), &dir.db("r.db")).is_err());
    let leftovers: Vec<_> = std::fs::read_dir(&dir.0)
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .filter(|name| name.contains("s3-replica") || name.starts_with("r."))
        .collect();
    assert!(leftovers.is_empty(), "{leftovers:?}");
}

#[test]
fn replicas_keep_up_with_a_writer_that_checkpoints_continuously() {
    let store = FaultyStore::new();
    let dir = TempDir::new();
    let cfg = config(&store, "a");
    let db = std::sync::Arc::new(open_db(&cfg, &dir.db("w.db")).unwrap());
    db.exec("CREATE TABLE t(id INTEGER PRIMARY KEY)").unwrap();
    // Uploads slow enough that restores overlap checkpoints and GC.
    store.inject(
        Op::Put,
        "",
        Fault::Delay(std::time::Duration::from_millis(2)),
        100_000,
    );
    let done = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let writer = {
        let (db, done) = (db.clone(), done.clone());
        std::thread::spawn(move || {
            for id in 0..120 {
                db.exec(&format!("INSERT INTO t VALUES ({id})")).unwrap();
                if id % 6 == 5 {
                    db.checkpoint();
                }
            }
            done.store(true, std::sync::atomic::Ordering::SeqCst);
        })
    };
    let mut seen = 0;
    let mut restores = 0;
    let mut round = 0;
    while !done.load(std::sync::atomic::Ordering::SeqCst) || round < 3 {
        round += 1;
        let path = dir.db(&format!("r{round}.db"));
        let staged = stage(&cfg, &path).unwrap_or_else(|err| panic!("round {round}: {err}"));
        staged.install(&path).unwrap();
        let n = count(&path);
        assert!(n >= seen, "round {round}: went back from {seen} to {n}");
        seen = n;
        restores += 1;
    }
    writer.join().unwrap();
    store.clear_faults();
    assert!(restores >= 3);
    let path = dir.db("final.db");
    stage(&cfg, &path).unwrap().install(&path).unwrap();
    assert_eq!(count(&path), 120);
}

#[test]
fn replica_follows_a_snapshot_chain() {
    let store = FaultyStore::new();
    let dir = TempDir::new();
    let writer = open_db(&config(&store, "writer"), &dir.db("w.db")).unwrap();
    writer.exec("CREATE TABLE t (x INTEGER, pad BLOB)").unwrap();
    for i in 0..32 {
        writer
            .exec(&format!("INSERT INTO t VALUES ({i}, randomblob(65536))"))
            .unwrap();
    }
    writer.checkpoint();
    let cfg = config(&store, "reader");
    for round in 1..=3 {
        writer
            .exec(&format!("INSERT INTO t VALUES ({}, NULL)", 100 + round))
            .unwrap();
        writer.checkpoint();
        let manifest = super::manifest(&store);
        assert!(manifest.snapshot.ends_with(".delta"), "{manifest:?}");
        let path = dir.db(&format!("r{round}.db"));
        stage(&cfg, &path).unwrap().install(&path).unwrap();
        assert_eq!(count(&path), 32 + round);
    }
}

fn caches(dir: &Path) -> Vec<std::path::PathBuf> {
    std::fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p.to_string_lossy().contains(".s3-cache-"))
        .collect()
}

/// A writer publishing deltas on a big database, and a replica config.
fn chained(store: &Arc<FaultyStore>, dir: &TempDir) -> (super::Db, crate::s3::S3Config) {
    let writer = open_db(&config(store, "writer"), &dir.db("w.db")).unwrap();
    writer.exec("CREATE TABLE t (x INTEGER, pad BLOB)").unwrap();
    for i in 0..32 {
        writer
            .exec(&format!("INSERT INTO t VALUES ({i}, randomblob(65536))"))
            .unwrap();
    }
    writer.checkpoint();
    writer.exec("INSERT INTO t VALUES (100, NULL)").unwrap();
    writer.checkpoint();
    assert!(!super::manifest(store).snapshot_base.is_empty());
    (writer, config(store, "reader"))
}

#[test]
fn a_refresh_downloads_only_the_newer_deltas() {
    let store = FaultyStore::new();
    let dir = TempDir::new();
    let (writer, cfg) = chained(&store, &dir);
    let path = dir.db("r.db");
    stage(&cfg, &path).unwrap().install(&path).unwrap();
    assert_eq!(count(&path), 33);
    assert_eq!(caches(path.parent().unwrap()).len(), 1);

    writer.exec("INSERT INTO t VALUES (101, NULL)").unwrap();
    writer.checkpoint();
    let base = super::manifest(&store).snapshot_base[0].key.clone();
    // The base snapshot can't be downloaded now; the cache has it.
    store.inject(Op::Get, &base, Fault::Fail, 100);
    let fresh = dir.db("r2.db");
    let staged = stage(&cfg, &path).unwrap();
    staged.install(&fresh).unwrap();
    assert_eq!(count(&fresh), 34);
    assert_eq!(caches(path.parent().unwrap()).len(), 1, "older caches go");
    store.clear_faults();
}

#[test]
fn a_damaged_cache_falls_back_to_a_full_download() {
    let store = FaultyStore::new();
    let dir = TempDir::new();
    let (writer, cfg) = chained(&store, &dir);
    let path = dir.db("r.db");
    stage(&cfg, &path).unwrap().install(&path).unwrap();
    let [cache] = &caches(path.parent().unwrap())[..] else {
        panic!("one cache")
    };
    let mut bytes = std::fs::read(cache).unwrap();
    bytes[5000] ^= 1;
    std::fs::write(cache, bytes).unwrap();

    writer.exec("INSERT INTO t VALUES (101, NULL)").unwrap();
    writer.checkpoint();
    let fresh = dir.db("r2.db");
    stage(&cfg, &path).unwrap().install(&fresh).unwrap();
    assert_eq!(count(&fresh), 34);
}
