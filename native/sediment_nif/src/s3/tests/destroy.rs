use std::sync::Arc;
use std::time::Duration;

use object_store::ObjectStore;

use super::faulty::{Fault, FaultyStore, Op};
use super::{assert_err_contains, config, keys, open_db, segments, TempDir, PREFIX};
use crate::s3::layout::{now_ms, LeaseRecord, ManifestState, Tombstone, LEASE_KEY, MANIFEST_KEY};
use crate::s3::remote::{Put, Remote};
use crate::s3::{destroy, replica, restore_to, S3Error, Target};

fn state(store: &Arc<FaultyStore>) -> ManifestState {
    let remote = Remote::new(store.clone(), PREFIX);
    ManifestState::decode(&remote.get(MANIFEST_KEY).unwrap().unwrap().bytes).unwrap()
}

fn assert_destroyed(store: &Arc<FaultyStore>) {
    assert!(matches!(state(store), ManifestState::Destroyed(_)));
    assert!(segments(store).is_empty(), "{:?}", segments(store));
    assert!(keys(store, "snapshots/").is_empty());
}

/// Another process's writer: the same objects through another client, so
/// this VM doesn't know it has them open.
fn elsewhere(store: &Arc<FaultyStore>, owner: &str) -> crate::s3::S3Config {
    let mut cfg = config(store, owner);
    cfg.store = Some(Arc::new(object_store::prefix::PrefixStore::new(
        store.clone() as Arc<dyn ObjectStore>,
        "",
    )));
    cfg
}

/// A writer of another process that crashed: its lease is unexpired.
fn crashed_elsewhere(store: &Arc<FaultyStore>, dir: &TempDir) {
    let db = open_db(&elsewhere(store, "a"), &dir.db("a.db")).unwrap();
    db.exec("CREATE TABLE t(id INTEGER PRIMARY KEY)").unwrap();
    db.exec("INSERT INTO t VALUES (1)").unwrap();
    db.crash();
}

/// Tables a user made (a new database has turso's internal ones).
const USER_TABLES: &str =
    "SELECT count(*) FROM sqlite_schema WHERE substr(name, 1, 17) != '__turso_internal_'";

fn a_database_with_history(store: &Arc<FaultyStore>, dir: &TempDir) {
    let mut cfg = config(store, "a");
    cfg.retain_epochs = 2;
    let db = open_db(&cfg, &dir.db("a.db")).unwrap();
    db.exec("CREATE TABLE t(id INTEGER PRIMARY KEY)").unwrap();
    for i in 0..3 {
        db.exec(&format!("INSERT INTO t VALUES ({i})")).unwrap();
        db.checkpoint();
    }
    db.exec("INSERT INTO t VALUES (99)").unwrap();
    drop(db);
}

#[test]
fn destroy_deletes_the_database_and_an_open_starts_empty() {
    let store = FaultyStore::new();
    let dir = TempDir::new();
    a_database_with_history(&store, &dir);
    assert!(!segments(&store).is_empty());

    let destroyed = destroy(&config(&store, "d"), false).unwrap();
    assert!(destroyed.objects >= 4, "{destroyed:?}");
    assert_destroyed(&store);
    // The lease stays, released, so generations keep growing.
    let lease: LeaseRecord = serde_json::from_slice(
        &Remote::new(store.clone(), PREFIX)
            .get(LEASE_KEY)
            .unwrap()
            .unwrap()
            .bytes,
    )
    .unwrap();
    assert_eq!(lease.expires_at_ms, 0);

    // Readers find no database.
    let err = restore_to(&config(&store, "r"), &dir.db("r.db"), Target::Latest).unwrap_err();
    assert!(err.to_string().contains("no database"), "{err}");
    let mut replica_cfg = config(&store, "r");
    replica_cfg.replica = true;
    let err = replica::stage(&replica_cfg, &dir.db("rep.db"))
        .err()
        .unwrap();
    assert!(err.to_string().contains("no database"), "{err}");

    // A writer gets a new, empty database, in a generation of its own.
    let fresh = open_db(&config(&store, "b"), &dir.db("b.db")).unwrap();
    assert_eq!(fresh.int(USER_TABLES), 0);
    fresh
        .exec("CREATE TABLE u(id INTEGER PRIMARY KEY)")
        .unwrap();
    fresh.exec("INSERT INTO u VALUES (1)").unwrap();
    let ManifestState::Database(manifest) = state(&store) else {
        panic!("expected a database");
    };
    assert!(manifest.epoch.generation > lease.generation);
    fresh.crash();
    let again = open_db(&config(&store, "b"), &dir.db("c.db")).unwrap();
    assert_eq!(again.int("SELECT count(*) FROM u"), 1);
}

#[test]
fn destroy_refuses_a_held_lease_unless_forced() {
    let store = FaultyStore::new();
    let dir = TempDir::new();
    crashed_elsewhere(&store, &dir);
    // The crashed writer's lease is still unexpired: its own owner name
    // doesn't get past it either.
    for owner in ["a", "d"] {
        match destroy(&config(&store, owner), false) {
            Err(S3Error::LeaseHeld { .. }) => {}
            other => panic!("expected LeaseHeld, got {other:?}"),
        }
    }
    assert!(matches!(state(&store), ManifestState::Database(_)));
    destroy(&config(&store, "d"), true).unwrap();
    assert_destroyed(&store);
}

#[test]
fn destroy_refuses_a_database_open_in_this_vm() {
    let store = FaultyStore::new();
    let dir = TempDir::new();
    let db = open_db(&config(&store, "a"), &dir.db("a.db")).unwrap();
    for force in [false, true] {
        let err = destroy(&config(&store, "d"), force).unwrap_err();
        assert!(err.to_string().contains("open in this VM"), "{err}");
    }
    db.exec("CREATE TABLE t(id INTEGER PRIMARY KEY)").unwrap();
}

#[test]
fn a_forced_destroy_fences_a_running_writer() {
    let store = FaultyStore::new();
    let dir = TempDir::new();
    let a = open_db(&elsewhere(&store, "a"), &dir.db("a.db")).unwrap();
    a.exec("CREATE TABLE t(id INTEGER PRIMARY KEY)").unwrap();
    a.exec("INSERT INTO t VALUES (1)").unwrap();

    match destroy(&config(&store, "d"), false) {
        Err(S3Error::LeaseHeld { .. }) => {}
        other => panic!("expected LeaseHeld, got {other:?}"),
    }
    a.checkpoint();
    destroy(&config(&store, "d"), true).unwrap();
    assert_err_contains(a.exec("INSERT INTO t VALUES (2)"), "fenced");
    assert!(a.storage.info().poisoned.is_some());
    // Its failed commit's frame may have landed; it is never a database.
    assert!(matches!(state(&store), ManifestState::Destroyed(_)));

    // A new database starts after the destroyed one's epochs, so the fenced
    // writer's garbage collection (epochs older than its own) leaves it be.
    let b = open_db(&config(&store, "b"), &dir.db("b.db")).unwrap();
    b.exec("CREATE TABLE u(id INTEGER PRIMARY KEY)").unwrap();
    let ManifestState::Database(manifest) = state(&store) else {
        panic!("expected a database");
    };
    assert_eq!(manifest.epoch.seq, 2);
    a.storage.collect_garbage_now();
    drop(a);
    b.crash();
    let again = open_db(&config(&store, "b"), &dir.db("c.db")).unwrap();
    assert_eq!(again.int("SELECT count(*) FROM u"), 0);
}

#[test]
fn an_interrupted_destroy_is_never_an_older_database() {
    let store = FaultyStore::new();
    let dir = TempDir::new();
    a_database_with_history(&store, &dir);
    // The tombstone lands, then deletes fail: the destroy stops half way.
    store.inject(Op::Delete, "log/", Fault::Fail, 1000);
    assert!(destroy(&config(&store, "d"), false).is_err());
    store.clear_faults();
    assert!(matches!(state(&store), ManifestState::Destroyed(_)));
    assert!(!segments(&store).is_empty());

    let err = restore_to(&config(&store, "r"), &dir.db("r.db"), Target::Latest).unwrap_err();
    assert!(err.to_string().contains("no database"), "{err}");
    // An open finishes the purge and starts empty.
    let fresh = open_db(&config(&store, "b"), &dir.db("b.db")).unwrap();
    assert_eq!(fresh.int(USER_TABLES), 0);
    let ManifestState::Database(manifest) = state(&store) else {
        panic!("expected a database");
    };
    assert_eq!(keys(&store, "snapshots/"), vec![manifest.snapshot.clone()]);
    assert!(segments(&store).is_empty());
}

#[test]
fn a_destroy_whose_tombstone_lost_its_answer_goes_on() {
    let store = FaultyStore::new();
    let dir = TempDir::new();
    a_database_with_history(&store, &dir);
    store.inject(Op::Put, MANIFEST_KEY, Fault::FailAfter, 1);
    destroy(&config(&store, "d"), false).unwrap();
    assert_destroyed(&store);
}

#[test]
fn a_takeover_during_destroy_deletes_nothing() {
    let store = FaultyStore::new();
    let dir = TempDir::new();
    a_database_with_history(&store, &dir);
    let before = segments(&store);
    // Another writer rewrites the manifest between destroy's read and its
    // tombstone PUT (held in flight here).
    store.inject(
        Op::Put,
        MANIFEST_KEY,
        Fault::Delay(Duration::from_millis(300)),
        1,
    );
    let racing = {
        let store = store.clone();
        std::thread::spawn(move || destroy(&config(&store, "d"), false))
    };
    std::thread::sleep(Duration::from_millis(100));
    let writer = Remote::new(store.clone(), PREFIX);
    let current = writer.get(MANIFEST_KEY).unwrap().unwrap();
    writer
        .put(
            MANIFEST_KEY,
            current.bytes.clone(),
            Put::Update(current.version),
        )
        .unwrap();
    let result = racing.join().unwrap();
    match result {
        Err(S3Error::Fenced(msg)) => assert!(msg.contains("nothing was deleted"), "{msg}"),
        other => panic!("expected Fenced, got {other:?}"),
    }
    assert_eq!(segments(&store), before);
    let db = open_db(&config(&store, "b"), &dir.db("b.db")).unwrap();
    assert_eq!(db.int("SELECT count(*) FROM t"), 4);
}

#[test]
fn destroy_clears_a_prefix_whose_manifest_was_deleted() {
    let store = FaultyStore::new();
    let dir = TempDir::new();
    crashed_elsewhere(&store, &dir);
    std::thread::sleep(Duration::from_millis(10));
    let remote = Remote::new(store.clone(), PREFIX);
    remote.delete(MANIFEST_KEY).unwrap();
    let expired = LeaseRecord {
        owner: "a".into(),
        generation: 1,
        expires_at_ms: now_ms() - 1,
    };
    remote
        .put(
            LEASE_KEY,
            serde_json::to_vec(&expired).unwrap().into(),
            Put::Overwrite,
        )
        .unwrap();
    // Objects without a manifest: an open refuses to build over them (and
    // holds the lease a while longer, hence the force).
    assert!(open_db(&config(&store, "b"), &dir.db("b.db")).is_err());
    destroy(&config(&store, "d"), true).unwrap();
    assert_destroyed(&store);
    open_db(&config(&store, "b"), &dir.db("c.db")).unwrap();
}

#[test]
fn a_tombstone_covers_only_older_generations() {
    let tombstone = Tombstone::new(5, 0, "d");
    assert!(tombstone.covers("log/00000000000000000003-0000000005/00000000000000000000"));
    assert!(tombstone.covers("snapshots/00000000000000000000-0000000002.db"));
    assert!(tombstone.covers("snapshots/00000000000000000001-0000000004.delta"));
    assert!(!tombstone.covers("snapshots/00000000000000000000-0000000006.db"));
    assert!(!tombstone.covers("log/00000000000000000000-0000000006/00000000000000000000"));
    assert!(!tombstone.covers("log/unrelated"));
}

#[test]
fn an_open_with_an_older_lease_than_the_tombstone_refuses() {
    let store = FaultyStore::new();
    let dir = TempDir::new();
    // A destroy whose lease generation is ahead of the next open's: the
    // open took its lease before that destroy, so it is stale.
    let remote = Remote::new(store.clone(), PREFIX);
    remote
        .put(
            MANIFEST_KEY,
            Tombstone::new(100, 0, "d").encode(),
            Put::Create,
        )
        .unwrap();
    match open_db(&config(&store, "a"), &dir.db("a.db")) {
        Err(S3Error::Fenced(msg)) => assert!(msg.contains("lease generation 100"), "{msg}"),
        Err(other) => panic!("expected Fenced, got {other}"),
        Ok(_) => panic!("a stale open must not build over a newer tombstone"),
    }
    assert!(matches!(state(&store), ManifestState::Destroyed(_)));
}

#[test]
fn exists_and_must_exist_tell_a_database_from_none() {
    let store = FaultyStore::new();
    let dir = TempDir::new();
    let mut strict = config(&store, "a");
    strict.must_exist = true;
    assert!(!crate::s3::exists(&config(&store, "x")).unwrap());
    match open_db(&strict, &dir.db("a.db")) {
        Err(S3Error::Config(msg)) => assert!(msg.contains("must_exist"), "{msg}"),
        Err(other) => panic!("expected a config error, got {other}"),
        Ok(_) => panic!("must_exist must not create a database"),
    }
    // Nothing written: no lease, no probe leftovers.
    assert!(store.put_log().is_empty(), "{:?}", store.put_log());

    drop(open_db(&config(&store, "a"), &dir.db("a.db")).unwrap());
    assert!(crate::s3::exists(&config(&store, "x")).unwrap());
    drop(open_db(&strict, &dir.db("b.db")).unwrap());

    destroy(&config(&store, "d"), false).unwrap();
    assert!(!crate::s3::exists(&config(&store, "x")).unwrap());
    assert!(open_db(&strict, &dir.db("c.db")).is_err());
}
