use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

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
        ("version", Box::new(|s| s.version = 1)),
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
        (
            "db_sha256",
            Box::new(|s| s.db_sha256.replace_range(0..1, "x")),
        ),
        (
            "log_sha256",
            Box::new(|s| s.log_sha256.replace_range(0..1, "x")),
        ),
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

#[test]
fn an_open_waits_for_the_last_close_to_leave_its_sidecar() {
    let store = FaultyStore::new();
    let dir = TempDir::new();
    let path = dir.db("a.db");
    let cfg = config(&store, "a");
    let db = open_db(&cfg, &path).unwrap();
    db.exec("CREATE TABLE t(id INTEGER PRIMARY KEY, v BLOB)")
        .unwrap();
    for i in 0..20 {
        db.exec(&format!("INSERT INTO t VALUES ({i}, randomblob(1000))"))
            .unwrap();
    }
    let generation = db.storage.info().generation;
    drop(crate::s3::Attached::try_new(&db.storage).unwrap());
    // The close's lease release is slow: its Drop runs on another thread (a
    // pool's deferred close) while the database is opened again.
    store.inject(
        Op::Put,
        "lease",
        Fault::Delay(Duration::from_millis(400)),
        1,
    );
    let closing = std::thread::spawn(move || drop(db));
    std::thread::sleep(Duration::from_millis(50));

    let started = Instant::now();
    let (reopened, gets) = reopen(&store, &cfg, &path);
    closing.join().unwrap();
    // Before: the open went ahead at once, took the lease and restored in
    // full, and the old Drop could leave a sidecar under the newer writer.
    assert!(started.elapsed() >= Duration::from_millis(250));
    assert_eq!(gets, 0, "reused the copy the close left");
    assert!(reopened.storage.info().generation > generation);
    assert_eq!(reopened.int("SELECT count(*) FROM t"), 20);
    drop(reopened);
    assert!(sidecar(&path).exists());
}

/// Bytes to XOR into `file[at..at + 8]` that leave its CRC32C as it is: the
/// change of the CRC is linear in the change of the bytes, so flip the first
/// byte and solve for the other 7 bytes' bits over GF(2).
fn crc_preserving_delta(file: &[u8], at: usize) -> [u8; 8] {
    let crc = crc32c::crc32c(file);
    let effect = |bit: usize| {
        let mut changed = file.to_vec();
        changed[at + bit / 8] ^= 1 << (bit % 8);
        crc32c::crc32c(&changed) ^ crc
    };
    let target = effect(0);
    // Gaussian elimination: rows (effect, which bits of bytes 1..8 make it).
    let mut rows: Vec<(u32, u64)> = (8..64).map(|bit| (effect(bit), 1u64 << bit)).collect();
    let mut pivots: Vec<(u32, u64)> = Vec::new();
    for _ in 0..32 {
        let Some(i) = rows.iter().position(|(e, _)| *e != 0) else {
            break;
        };
        let (e, m) = rows.swap_remove(i);
        let top = 31 - e.leading_zeros();
        for row in rows.iter_mut().chain(pivots.iter_mut()) {
            if row.0 >> top & 1 == 1 {
                row.0 ^= e;
                row.1 ^= m;
            }
        }
        pivots.push((e, m));
    }
    let (mut left, mut mask) = (target, 1u64);
    for (e, m) in &pivots {
        let top = 31 - e.leading_zeros();
        if left >> top & 1 == 1 {
            left ^= e;
            mask ^= m;
        }
    }
    assert_eq!(left, 0, "solvable");
    mask.to_le_bytes()
}

#[test]
fn a_change_that_keeps_the_crc32c_is_restored_in_full() {
    let store = FaultyStore::new();
    let dir = TempDir::new();
    let path = dir.db("a.db");
    let cfg = config(&store, "a");
    let db = open_db(&cfg, &path).unwrap();
    db.exec("CREATE TABLE t(id INTEGER PRIMARY KEY, v BLOB)")
        .unwrap();
    db.exec("INSERT INTO t VALUES (1, X'5741524D424C4F42')")
        .unwrap();
    db.checkpoint();
    drop(db);
    assert!(sidecar(&path).exists());

    // Another blob of the same length, and the file's size and CRC32C as
    // the sidecar and the manifest say.
    let mut file = std::fs::read(&path).unwrap();
    let at = file
        .windows(8)
        .position(|w| w == b"WARMBLOB")
        .expect("the blob is in the file");
    let delta = crc_preserving_delta(&file, at);
    let crc = crc32c::crc32c(&file);
    for (i, d) in delta.iter().enumerate() {
        file[at + i] ^= d;
    }
    assert_eq!(crc32c::crc32c(&file), crc);
    assert_ne!(&file[at..at + 8], b"WARMBLOB");
    std::fs::write(&path, &file).unwrap();

    let (db, gets) = reopen(&store, &cfg, &path);
    assert!(gets > 0, "full restore");
    assert_eq!(
        db.int("SELECT count(*) FROM t WHERE v = X'5741524D424C4F42'"),
        1
    );
}

#[test]
fn a_missing_earlier_log_object_is_never_skipped() {
    let store = FaultyStore::new();
    let dir = TempDir::new();
    let path = dir.db("a.db");
    let cfg = config(&store, "a");
    closed_with_rows(&store, &cfg, &path, 4);
    let side: warm::Sidecar =
        serde_json::from_slice(&std::fs::read(sidecar(&path)).unwrap()).unwrap();
    let manifest = super::manifest(&store);
    let remote = Remote::new(store.clone(), PREFIX);
    let mut log: Vec<String> = remote
        .list(&manifest.epoch.log_dir())
        .unwrap()
        .into_iter()
        .map(|o| o.key)
        .collect();
    log.sort();
    assert!(log.len() >= 3, "{log:?}");
    assert!(warm::check(&side, &manifest, &cfg, &remote, &path).is_ok());
    remote.delete(&log[0]).unwrap();

    let why = warm::check(&side, &manifest, &cfg, &remote, &path).unwrap_err();
    assert!(why.contains("gap"), "{why}");
    // Before: the warm open succeeded where the full restore fails.
    match open_db(&cfg, &path) {
        Err(crate::s3::S3Error::Corrupt(msg)) => assert!(msg.contains("gap"), "{msg}"),
        Err(other) => panic!("unexpected error {other}"),
        Ok(_) => panic!("the full restore must fail"),
    }
}

/// A database closed with its Drop on another thread, its lease release
/// held up by `delay`; then reopened 100 ms into that Drop.
fn reopen_during_a_slow_drop(delay: Duration) -> (Db, usize, Duration, u64) {
    let store = FaultyStore::new();
    let dir = TempDir::new();
    let path = dir.db("a.db");
    let mut cfg = config(&store, "a");
    cfg.close_timeout = Duration::from_millis(200);
    let db = open_db(&cfg, &path).unwrap();
    db.exec("CREATE TABLE t(id INTEGER PRIMARY KEY)").unwrap();
    db.exec("INSERT INTO t VALUES (1)").unwrap();
    let generation = db.storage.info().generation;
    drop(crate::s3::Attached::try_new(&db.storage).unwrap());
    store.inject(Op::Put, "lease", Fault::Delay(delay), 1);
    let closing = std::thread::spawn(move || drop(db));
    std::thread::sleep(Duration::from_millis(100));
    let started = Instant::now();
    let (reopened, gets) = reopen(&store, &cfg, &path);
    let took = started.elapsed();
    closing.join().unwrap();
    assert_eq!(reopened.int("SELECT count(*) FROM t"), 1);
    (reopened, gets, took, generation)
}

#[test]
fn a_reopen_waits_out_a_drop_slower_than_the_close_timeout() {
    // Well past the close timeout and a second (1.2 s), within the dropping
    // budget (twice the close timeout and the margin, 2.4 s in tests).
    // Before: the reopen failed "still closing from an earlier open".
    let (db, gets, took, generation) = reopen_during_a_slow_drop(Duration::from_millis(1_800));
    assert!(took >= Duration::from_millis(1_500), "{took:?}");
    assert_eq!(gets, 0, "reused the copy the close left");
    assert!(db.storage.info().generation > generation);
}

#[test]
fn a_reopen_goes_ahead_cold_when_a_drop_takes_too_long() {
    let (db, gets, took, generation) = reopen_during_a_slow_drop(Duration::from_millis(4_000));
    // Opened once the dropping budget ran out, not when the Drop ended.
    assert!(took < Duration::from_millis(3_700), "{took:?}");
    assert!(gets > 0, "full restore");
    assert!(db.storage.info().generation > generation);
}
