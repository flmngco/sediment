use std::path::{Path, PathBuf};
use std::sync::Arc;

use super::faulty::{Fault, FaultyStore, Op};
use super::{
    config, encryption, open_db, open_encrypted, open_with, segments, Db, TempDir, PREFIX,
};
use crate::s3::layout::MANIFEST_KEY;
use crate::s3::remote::Remote;
use crate::s3::{destroy, prepare, restore, warm, S3Config};

fn sidecar(path: &Path) -> PathBuf {
    warm::sidecar_path(path)
}

/// A database with rows, closed cleanly: its copy has a sidecar.
fn closed_with_rows(store: &Arc<FaultyStore>, cfg: &S3Config, path: &Path, rows: i64) {
    let db = open_db(cfg, path).unwrap();
    db.exec("CREATE TABLE t(id INTEGER PRIMARY KEY, v BLOB)")
        .unwrap();
    for i in 0..rows {
        db.exec(&format!("INSERT INTO t VALUES ({i}, randomblob(1000))"))
            .unwrap();
    }
    drop(db);
    assert!(sidecar(path).exists(), "a clean close leaves a sidecar");
    let _ = store;
}

/// Runs `open` and counts its downloads of the snapshot chain the database
/// was at (the open's own upload of its fresh epoch's snapshot is read back
/// to verify it, in both paths, and doesn't count).
fn counting<T>(store: &Arc<FaultyStore>, open: impl FnOnce() -> T) -> (T, usize) {
    let chain: Vec<String> = super::manifest(store)
        .current()
        .chain()
        .into_iter()
        .map(|link| format!("{PREFIX}/{}", link.key))
        .collect();
    let before = store.get_log().len();
    let opened = open();
    let gets = store.get_log()[before..]
        .iter()
        .filter(|key| chain.contains(key))
        .count();
    (opened, gets)
}

fn reopen(store: &Arc<FaultyStore>, cfg: &S3Config, path: &Path) -> (Db, usize) {
    counting(store, || open_db(cfg, path).unwrap())
}

#[test]
fn a_clean_copy_is_reused_without_downloading_the_snapshot() {
    let store = FaultyStore::new();
    let dir = TempDir::new();
    let path = dir.db("a.db");
    let cfg = config(&store, "a");
    closed_with_rows(&store, &cfg, &path, 20);

    let (db, gets) = reopen(&store, &cfg, &path);
    assert_eq!(gets, 0, "warm: no snapshot download");
    // Taken at the open: a crash now leaves nothing to trust.
    assert!(!sidecar(&path).exists());
    assert_eq!(db.int("SELECT count(*) FROM t"), 20);
    db.exec("INSERT INTO t VALUES (100, x'00')").unwrap();
    drop(db);

    // And again, from what the warm open left.
    let (db, gets) = reopen(&store, &cfg, &path);
    assert_eq!(gets, 0);
    assert_eq!(db.int("SELECT count(*) FROM t"), 21);
    db.crash();
    // A cold restore on another path sees the same.
    let cold = open_db(&cfg, &dir.db("b.db")).unwrap();
    assert_eq!(cold.int("SELECT count(*) FROM t"), 21);
}

#[test]
fn the_log_past_the_copy_is_fetched_and_appended() {
    let store = FaultyStore::new();
    let dir = TempDir::new();
    let path = dir.db("a.db");
    let cfg = config(&store, "a");
    let db = open_db(&cfg, &path).unwrap();
    db.exec("CREATE TABLE t(id INTEGER PRIMARY KEY)").unwrap();
    db.exec("INSERT INTO t VALUES (1)").unwrap();
    // The copy as it was here, with its sidecar: what a clean close at this
    // point would have left.
    let saved = save_copy(&store, &db, &path);
    db.exec("INSERT INTO t VALUES (2)").unwrap();
    db.exec("INSERT INTO t VALUES (3)").unwrap();
    db.crash();
    put_back(&saved, &path);

    let (db, gets) = reopen(&store, &cfg, &path);
    assert_eq!(gets, 0);
    assert_eq!(db.int("SELECT count(*) FROM t"), 3);
}

/// The local files plus a sidecar describing them, as a clean close would
/// leave them now (the writer is still open: its state is final so far).
fn save_copy(store: &Arc<FaultyStore>, db: &Db, path: &Path) -> Vec<(PathBuf, Vec<u8>)> {
    let manifest = super::manifest(store);
    let closing = warm::Closing {
        db_path: path,
        manifest: &manifest,
        generation: db.storage.info().generation,
        log_offset: db.storage.info().log_offset,
        encryption: db.storage.encryption(),
        log_encryption: None,
    };
    assert!(warm::write(&closing).unwrap());
    [path.to_path_buf(), restore::log_path(path), sidecar(path)]
        .into_iter()
        .map(|file| {
            let bytes = std::fs::read(&file).unwrap_or_default();
            (file, bytes)
        })
        .collect()
}

fn put_back(saved: &[(PathBuf, Vec<u8>)], _path: &Path) {
    for (file, bytes) in saved {
        std::fs::write(file, bytes).unwrap();
    }
}

#[test]
fn a_stale_copy_is_restored_in_full() {
    let store = FaultyStore::new();
    let dir = TempDir::new();
    let path = dir.db("a.db");
    let cfg = config(&store, "a");
    closed_with_rows(&store, &cfg, &path, 5);
    let saved: Vec<_> = [path.clone(), restore::log_path(&path), sidecar(&path)]
        .into_iter()
        .map(|file| {
            let bytes = std::fs::read(&file).unwrap();
            (file, bytes)
        })
        .collect();
    // Another writer (elsewhere) commits, and checkpoints.
    let other = open_db(&cfg, &dir.db("b.db")).unwrap();
    other.exec("INSERT INTO t VALUES (100, x'00')").unwrap();
    other.checkpoint();
    drop(other);
    put_back(&saved, &path);

    let (db, gets) = reopen(&store, &cfg, &path);
    assert!(gets > 0, "full restore");
    assert_eq!(db.int("SELECT count(*) FROM t"), 6);
}

#[test]
fn a_changed_local_copy_is_restored_in_full() {
    for damage in ["file", "log-shorter", "log-longer", "sidecar-garbage"] {
        let store = FaultyStore::new();
        let dir = TempDir::new();
        let path = dir.db("a.db");
        let cfg = config(&store, "a");
        closed_with_rows(&store, &cfg, &path, 5);
        match damage {
            "file" => {
                let mut bytes = std::fs::read(&path).unwrap();
                let last = bytes.len() - 1;
                bytes[last] ^= 0xff;
                std::fs::write(&path, bytes).unwrap();
            }
            "log-shorter" => {
                let log = restore::log_path(&path);
                let bytes = std::fs::read(&log).unwrap();
                std::fs::write(&log, &bytes[..bytes.len() - 10]).unwrap();
            }
            "log-longer" => {
                let log = restore::log_path(&path);
                let mut bytes = std::fs::read(&log).unwrap();
                bytes.extend_from_slice(&[0u8; 64]);
                std::fs::write(&log, bytes).unwrap();
            }
            _ => std::fs::write(sidecar(&path), b"{not json").unwrap(),
        }
        let (db, gets) = reopen(&store, &cfg, &path);
        assert!(gets > 0, "{damage}: full restore");
        assert_eq!(db.int("SELECT count(*) FROM t"), 5, "{damage}");
    }
}

#[test]
fn a_copy_ahead_of_s3_is_never_reused() {
    let store = FaultyStore::new();
    let dir = TempDir::new();
    let path = dir.db("a.db");
    let cfg = config(&store, "a");
    closed_with_rows(&store, &cfg, &path, 5);
    // S3 lost the copy's last commit (or the copy has commits that never
    // reached S3): its log ends earlier than the local one.
    let last = segments(&store).pop().unwrap();
    Remote::new(store.clone(), PREFIX).delete(&last).unwrap();

    let (db, gets) = reopen(&store, &cfg, &path);
    assert!(gets > 0, "full restore");
    assert_eq!(
        db.int("SELECT count(*) FROM t"),
        4,
        "S3's state, not the copy's"
    );
}

#[test]
fn an_async_writer_whose_uploads_failed_leaves_no_sidecar() {
    let store = FaultyStore::new();
    let dir = TempDir::new();
    let path = dir.db("a.db");
    let mut cfg = config(&store, "a");
    cfg.async_durability = true;
    cfg.close_timeout = std::time::Duration::from_millis(300);
    let db = open_db(&cfg, &path).unwrap();
    db.exec("CREATE TABLE t(id INTEGER PRIMARY KEY)").unwrap();
    db.storage.flush(std::time::Duration::from_secs(5)).unwrap();
    store.inject(Op::Put, "/log/", Fault::Fail, 1000);
    db.exec("INSERT INTO t VALUES (1)").unwrap();
    drop(db);
    store.clear_faults();
    assert!(!sidecar(&path).exists());
}

#[test]
fn a_destroyed_and_recreated_database_is_restored_in_full() {
    let store = FaultyStore::new();
    let dir = TempDir::new();
    let path = dir.db("a.db");
    let cfg = config(&store, "a");
    closed_with_rows(&store, &cfg, &path, 5);
    let saved: Vec<_> = [path.clone(), restore::log_path(&path), sidecar(&path)]
        .into_iter()
        .map(|file| {
            let bytes = std::fs::read(&file).unwrap();
            (file, bytes)
        })
        .collect();
    destroy(&config(&store, "d"), false).unwrap();
    let recreated = open_db(&cfg, &dir.db("b.db")).unwrap();
    recreated
        .exec("CREATE TABLE u(id INTEGER PRIMARY KEY)")
        .unwrap();
    drop(recreated);
    put_back(&saved, &path);

    let (db, gets) = reopen(&store, &cfg, &path);
    assert!(gets > 0, "full restore");
    assert_eq!(
        db.int("SELECT count(*) FROM sqlite_schema WHERE name = 't'"),
        0,
        "the new database, not the destroyed one"
    );
}

#[test]
fn an_encrypted_copy_is_reused_with_its_key_only() {
    let store = FaultyStore::new();
    let dir = TempDir::new();
    let path = dir.db("a.db");
    let mut cfg = config(&store, "a");
    cfg.encryption = Some(encryption("ab"));
    let db = open_encrypted(&cfg, &path).unwrap();
    db.exec("CREATE TABLE t(id INTEGER PRIMARY KEY)").unwrap();
    db.exec("INSERT INTO t VALUES (1)").unwrap();
    drop(db);
    assert!(sidecar(&path).exists());

    let mut wrong = cfg.clone();
    wrong.encryption = Some(encryption("cd"));
    assert!(
        open_encrypted(&wrong, &path).is_err(),
        "a wrong key still fails"
    );

    // The failed open took the sidecar: a full restore, with the right key.
    let (db, gets) = counting(&store, || open_encrypted(&cfg, &path).unwrap());
    assert!(gets > 0);
    assert_eq!(db.int("SELECT count(*) FROM t"), 1);
    drop(db);
    let (db, gets) = counting(&store, || open_encrypted(&cfg, &path).unwrap());
    assert_eq!(gets, 0, "warm");
    assert_eq!(db.int("SELECT count(*) FROM t"), 1);
}

#[test]
fn an_open_that_fails_after_taking_the_sidecar_leaves_none() {
    let store = FaultyStore::new();
    let dir = TempDir::new();
    let path = dir.db("a.db");
    let cfg = config(&store, "a");
    closed_with_rows(&store, &cfg, &path, 3);
    store.inject(Op::Put, MANIFEST_KEY, Fault::Fail, 1);
    assert!(open_db(&cfg, &path).is_err());
    store.clear_faults();
    assert!(!sidecar(&path).exists());
    let (db, gets) = reopen(&store, &cfg, &path);
    assert!(gets > 0, "full restore");
    assert_eq!(db.int("SELECT count(*) FROM t"), 3);
}

#[test]
fn a_sidecar_write_cut_short_is_no_sidecar() {
    let store = FaultyStore::new();
    let dir = TempDir::new();
    let path = dir.db("a.db");
    let cfg = config(&store, "a");
    closed_with_rows(&store, &cfg, &path, 3);
    // A crash after the temp file, before the rename.
    let side = sidecar(&path);
    let mut tmp = side.as_os_str().to_owned();
    tmp.push(".tmp");
    std::fs::rename(&side, PathBuf::from(&tmp)).unwrap();
    let (db, gets) = reopen(&store, &cfg, &path);
    assert!(gets > 0, "full restore");
    assert!(!PathBuf::from(&tmp).exists());
    assert_eq!(db.int("SELECT count(*) FROM t"), 3);
}

#[test]
fn a_reuse_that_fails_midway_falls_back_to_the_full_restore() {
    let store = FaultyStore::new();
    let dir = TempDir::new();
    let path = dir.db("a.db");
    let cfg = config(&store, "a");
    closed_with_rows(&store, &cfg, &path, 3);
    // The takeover's seal fails once: the reuse gives up, the full restore
    // (which seals again) goes on.
    store.inject(Op::Put, "/log/", Fault::Fail, 1);
    let (db, gets) = reopen(&store, &cfg, &path);
    store.clear_faults();
    assert!(gets > 0);
    assert_eq!(db.int("SELECT count(*) FROM t"), 3);
}

#[test]
fn a_warm_reopen_shares_the_storage_with_a_second_connection() {
    let store = FaultyStore::new();
    let dir = TempDir::new();
    let path = dir.db("a.db");
    let cfg = config(&store, "a");
    closed_with_rows(&store, &cfg, &path, 3);
    let (db, _) = reopen(&store, &cfg, &path);
    let second = open_with(prepare(&cfg, &path).unwrap(), &path);
    assert_eq!(second.int("SELECT count(*) FROM t"), 3);
    drop(second);
    drop(db);
}

#[test]
fn every_check_refuses_on_its_own() {
    let store = FaultyStore::new();
    let dir = TempDir::new();
    let path = dir.db("a.db");
    let cfg = config(&store, "a");
    closed_with_rows(&store, &cfg, &path, 3);
    let side: warm::Sidecar =
        serde_json::from_slice(&std::fs::read(sidecar(&path)).unwrap()).unwrap();
    let remote = Remote::new(store.clone(), PREFIX);
    let manifest = super::manifest(&store);
    assert!(warm::check(&side, &manifest, &cfg, &remote, &path).is_ok());

    type Change = Box<dyn Fn(&mut warm::Sidecar)>;
    let variants: Vec<(&str, Change)> = vec![
        ("version", Box::new(|s| s.version = 2)),
        (
            "database_id",
            Box::new(|s| s.database_id = "another".into()),
        ),
        ("generation", Box::new(|s| s.generation += 1)),
        (
            "epoch",
            Box::new(|s| s.epoch = "00000000000000000009-0000000001".into()),
        ),
        ("encryption", Box::new(|s| s.encrypted = true)),
        ("db_size", Box::new(|s| s.db_size += 1)),
        ("db_crc32c", Box::new(|s| s.db_crc32c ^= 1)),
        ("log_len", Box::new(|s| s.log_len += 1)),
        (
            "log_end_crc",
            Box::new(|s| s.log_end_crc = s.log_end_crc.map(|c| c ^ 1)),
        ),
    ];
    for (field, change) in variants {
        let mut changed = side.clone();
        change(&mut changed);
        assert!(
            warm::check(&changed, &manifest, &cfg, &remote, &path).is_err(),
            "{field}"
        );
    }
}

#[test]
fn a_failing_read_of_the_log_while_checking_falls_back() {
    let store = FaultyStore::new();
    let dir = TempDir::new();
    let path = dir.db("a.db");
    let cfg = config(&store, "a");
    closed_with_rows(&store, &cfg, &path, 3);
    store.inject(Op::Get, "/log/", Fault::Fail, 1);
    let (db, gets) = reopen(&store, &cfg, &path);
    store.clear_faults();
    assert!(gets > 0, "full restore");
    assert_eq!(db.int("SELECT count(*) FROM t"), 3);
}
