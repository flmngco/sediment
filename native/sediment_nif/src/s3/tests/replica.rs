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

fn ids(path: &Path) -> Vec<i64> {
    let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
    let db = Database::open(
        io,
        path.to_str().unwrap(),
        OpenOptions::new(Arc::new(SqliteDialect)),
    )
    .unwrap();
    let conn = db.connect().unwrap();
    let mut stmt = conn.query("SELECT x FROM t ORDER BY x").unwrap().unwrap();
    let rows = stmt.run_collect_rows().unwrap();
    conn.close().unwrap();
    rows.iter()
        .map(|r| r[0].to_string().parse().unwrap())
        .collect()
}

/// Log objects GET since `from` (an index into the store's GET log).
fn log_gets(store: &Arc<FaultyStore>, from: usize) -> usize {
    store.get_log()[from..]
        .iter()
        .filter(|key| key.contains("/log/"))
        .count()
}

/// Stages a refresh of `path` into a fresh file and checks it against a
/// full restore of the same state. Returns the ids and how many log objects
/// the refresh itself downloaded.
fn refresh_and_compare(
    store: &Arc<FaultyStore>,
    cfg: &crate::s3::S3Config,
    dir: &TempDir,
    path: &Path,
    round: usize,
) -> (Vec<i64>, usize) {
    let before = store.get_log().len();
    let staged = stage(cfg, path).unwrap();
    let fetched = log_gets(store, before);
    let out = dir.db(&format!("round{round}.db"));
    staged.install(&out).unwrap();
    let full = dir.db(&format!("full{round}.db"));
    crate::s3::restore_to(cfg, &full, crate::s3::Target::Latest).unwrap();
    let got = ids(&out);
    assert_eq!(
        got,
        ids(&full),
        "round {round}: an incremental refresh equals a full restore"
    );
    (got, fetched)
}

#[test]
fn a_refresh_of_the_same_epoch_downloads_only_the_new_log_objects() {
    let store = FaultyStore::new();
    let dir = TempDir::new();
    let writer = open_db(&config(&store, "writer"), &dir.db("w.db")).unwrap();
    writer.exec("CREATE TABLE t (x INTEGER)").unwrap();
    writer.exec("INSERT INTO t VALUES (0)").unwrap();
    let cfg = config(&store, "reader");
    let path = dir.db("r.db");
    stage(&cfg, &path).unwrap().install(&path).unwrap();

    let mut next = 0;
    for round in 1..=6 {
        let k = round * 3;
        if round == 4 {
            // A new epoch: this refresh restores in full.
            writer.checkpoint();
        }
        for _ in 0..k {
            next += 1;
            writer
                .exec(&format!("INSERT INTO t VALUES ({next})"))
                .unwrap();
        }
        let (got, fetched) = refresh_and_compare(&store, &cfg, &dir, &path, round);
        assert_eq!(got, (0..=next).collect::<Vec<_>>());
        // Each refresh downloads the round's k new log objects, never the
        // earlier ones (round 4 restores the new epoch in full, which holds
        // just its k objects; rounds 5 and 6 would read 27 and 45 in full).
        assert_eq!(fetched, k, "round {round}");
    }
}

#[test]
fn a_failed_incremental_refresh_falls_back_to_a_full_restore() {
    let store = FaultyStore::new();
    let dir = TempDir::new();
    let writer = open_db(&config(&store, "writer"), &dir.db("w.db")).unwrap();
    writer.exec("CREATE TABLE t (x INTEGER)").unwrap();
    writer.exec("INSERT INTO t VALUES (1)").unwrap();
    let cfg = config(&store, "reader");
    let path = dir.db("r.db");
    stage(&cfg, &path).unwrap().install(&path).unwrap();
    writer.exec("INSERT INTO t VALUES (2)").unwrap();
    // The next GET of a log object fails once: the incremental refresh gives
    // up, the full restore that follows succeeds.
    store.inject(Op::Get, "/log/", Fault::Fail, 1);
    assert_eq!(refresh_and_compare(&store, &cfg, &dir, &path, 1).0, [1, 2]);

    // A damaged log cache (another length) is not used.
    writer.exec("INSERT INTO t VALUES (3)").unwrap();
    let cache = path.with_file_name(".r.db.s3-logcache");
    let len = std::fs::metadata(&cache).unwrap().len();
    std::fs::OpenOptions::new()
        .write(true)
        .open(&cache)
        .unwrap()
        .set_len(len - 1)
        .unwrap();
    assert_eq!(
        refresh_and_compare(&store, &cfg, &dir, &path, 2).0,
        [1, 2, 3]
    );

    // A takeover by another writer (a new epoch and generation).
    drop(writer);
    let other = open_db(&config(&store, "other"), &dir.db("w2.db")).unwrap();
    other.exec("INSERT INTO t VALUES (4)").unwrap();
    assert_eq!(
        refresh_and_compare(&store, &cfg, &dir, &path, 3).0,
        [1, 2, 3, 4]
    );
    other.exec("INSERT INTO t VALUES (5)").unwrap();
    assert_eq!(
        refresh_and_compare(&store, &cfg, &dir, &path, 4).0,
        [1, 2, 3, 4, 5]
    );
}

/// Writer "a" commits row 1 and a replica restores it; then a's upload of
/// row 2 times out but stays in flight, and a stops. Returns the writer
/// (keep it alive), the replica config and its path.
fn late_upload_in_flight(
    store: &Arc<FaultyStore>,
    dir: &TempDir,
    retain: usize,
    land_after: std::time::Duration,
) -> (super::Db, crate::s3::S3Config, std::path::PathBuf) {
    let mut cfg = config(store, "a");
    cfg.retain_epochs = retain;
    let writer = open_db(&cfg, &dir.db("a.db")).unwrap();
    writer.exec("CREATE TABLE t (x INTEGER)").unwrap();
    writer.exec("INSERT INTO t VALUES (1)").unwrap();
    let replica = config(store, "reader");
    let path = dir.db("r.db");
    stage(&replica, &path).unwrap().install(&path).unwrap();
    assert_eq!(ids(&path), [1]);
    store.inject(Op::Put, "/log/", Fault::LandLater(land_after), 1);
    assert!(writer.exec("INSERT INTO t VALUES (2)").is_err());
    writer.storage.release();
    (writer, replica, path)
}

/// A refresh reads a's manifest and is held at its LIST while "b" takes
/// over: b restores row 1 only, seals a's epoch there, publishes its own and
/// commits row 3. a's upload of row 2 then finds the seal (a's epoch kept,
/// retain 1), or lands in a's collected epoch (retain 0). Row 2 is not part
/// of the database, so neither the refresh nor a restore of a's epoch may
/// show it.
fn refresh_across_a_takeover(log_cache: bool, retain: usize) {
    use std::time::Duration;
    let store = FaultyStore::new();
    let dir = TempDir::new();
    let (_a, cfg, path) = late_upload_in_flight(&store, &dir, retain, Duration::from_secs(2));
    let old_epoch = super::manifest(&store).epoch;
    if !log_cache {
        std::fs::remove_file(path.with_file_name(".r.db.s3-logcache")).unwrap();
    }

    let (started, release) = store.hold_next_list();
    let refresh = {
        let (cfg, path) = (cfg.clone(), path.clone());
        std::thread::spawn(move || stage(&cfg, &path))
    };
    started.recv_timeout(Duration::from_secs(5)).unwrap();
    let mut b_cfg = config(&store, "b");
    b_cfg.retain_epochs = retain;
    let b = open_db(&b_cfg, &dir.db("b.db")).unwrap();
    b.storage.wait_background(Duration::from_secs(5));
    assert_eq!(b.int("SELECT count(*) FROM t"), 1);
    b.exec("INSERT INTO t VALUES (3)").unwrap();
    assert_ne!(super::manifest(&store).epoch, old_epoch);
    let before = super::keys(&store, &old_epoch.log_dir()).len();
    if retain == 0 {
        let landed = std::time::Instant::now();
        while super::keys(&store, &old_epoch.log_dir()).len() == before {
            assert!(
                landed.elapsed() < Duration::from_secs(5),
                "a's upload lands"
            );
            std::thread::sleep(Duration::from_millis(20));
        }
    } else {
        std::thread::sleep(Duration::from_millis(2500));
        assert_eq!(
            super::keys(&store, &old_epoch.log_dir()).len(),
            before,
            "b's seal took a's upload's key"
        );
        let past = dir.db("past.db");
        crate::s3::restore_to(&cfg, &past, crate::s3::Target::Epoch(old_epoch.seq)).unwrap();
        assert_eq!(ids(&past), [1], "a's epoch ends where b took over");
    }
    release.send(()).unwrap();

    let out = dir.db("out.db");
    refresh.join().unwrap().unwrap().install(&out).unwrap();
    let full = dir.db("full.db");
    crate::s3::restore_to(&cfg, &full, crate::s3::Target::Latest).unwrap();
    assert_eq!(ids(&full), [1, 3]);
    // a's epoch while it is retained (sealed where b took over), else b's.
    let expected: &[i64] = if retain == 0 { &[1, 3] } else { &[1] };
    assert_eq!(ids(&out), expected, "no row 2: a prefix of the database");
    assert_eq!(refresh_and_compare(&store, &cfg, &dir, &path, 1).0, [1, 3]);
}

#[test]
fn a_refresh_across_a_takeover_never_shows_the_old_writers_late_upload() {
    // With the log cache (an incremental refresh) and without (a full one);
    // b collects a's epoch (retain 0) or keeps it (retain 1).
    for (log_cache, retain) in [(true, 0), (false, 0), (true, 1), (false, 1)] {
        refresh_across_a_takeover(log_cache, retain);
    }
}

/// a's upload of row 2 lands after b listed a's log but before b's seal
/// (held for 2 s): the seal finds the key taken, b lists again and keeps
/// row 2. While the seal is held, the manifest names a's epoch under b's
/// generation: a refresh or restore then fails at once, and the replica
/// keeps its state.
#[test]
fn a_takeover_keeps_an_upload_that_landed_before_its_seal() {
    use std::time::{Duration, Instant};
    let store = FaultyStore::new();
    let dir = TempDir::new();
    let (_a, cfg, path) = late_upload_in_flight(&store, &dir, 0, Duration::from_secs(1));
    let epoch = super::manifest(&store).epoch;
    let objects = super::keys(&store, &epoch.log_dir()).len();
    // The next log PUT is b's seal.
    store.inject(Op::Put, "/log/", Fault::Delay(Duration::from_secs(2)), 1);
    let b = {
        let (b_cfg, b_path) = (config(&store, "b"), dir.db("b.db"));
        std::thread::spawn(move || open_db(&b_cfg, &b_path).unwrap())
    };
    let wait = Instant::now();
    while super::keys(&store, &epoch.log_dir()).len() == objects {
        assert!(wait.elapsed() < Duration::from_secs(5), "a's upload lands");
        std::thread::sleep(Duration::from_millis(20));
    }
    let manifest = super::manifest(&store);
    assert!(!manifest.settled(), "b's takeover is under way");
    let refused = |err: S3Error| {
        assert!(
            err.to_string().contains("is taking the database over"),
            "{err}"
        );
    };
    refused(stage(&cfg, &path).err().expect("not sealed yet"));
    refused(crate::s3::restore_to(&cfg, &dir.db("x.db"), crate::s3::Target::Latest).unwrap_err());
    assert_eq!(ids(&path), [1], "the replica keeps its state");

    let b = b.join().unwrap();
    assert_eq!(b.int("SELECT count(*) FROM t"), 2, "b restored row 2");
    b.exec("INSERT INTO t VALUES (3)").unwrap();
    assert_eq!(
        refresh_and_compare(&store, &cfg, &dir, &path, 1).0,
        [1, 2, 3]
    );
}

/// Once b sealed a's epoch, its takeover is readable before it finishes
/// (b's snapshot download is held): what a reader gets is where b goes on.
#[test]
fn a_sealed_takeover_is_readable_before_it_finishes() {
    use std::time::{Duration, Instant};
    let store = FaultyStore::new();
    let dir = TempDir::new();
    let a = open_db(&config(&store, "a"), &dir.db("a.db")).unwrap();
    a.exec("CREATE TABLE t (x INTEGER)").unwrap();
    a.exec("INSERT INTO t VALUES (1)").unwrap();
    let cfg = config(&store, "reader");
    let path = dir.db("r.db");
    stage(&cfg, &path).unwrap().install(&path).unwrap();
    a.exec("INSERT INTO t VALUES (2)").unwrap();
    a.storage.release();
    let epoch = super::manifest(&store).epoch;
    let objects = super::keys(&store, &epoch.log_dir()).len();
    store.inject(
        Op::Get,
        "snapshots/",
        Fault::Delay(Duration::from_secs(2)),
        1,
    );
    let b = {
        let (b_cfg, b_path) = (config(&store, "b"), dir.db("b.db"));
        std::thread::spawn(move || open_db(&b_cfg, &b_path).unwrap())
    };
    let wait = Instant::now();
    while super::keys(&store, &epoch.log_dir()).len() == objects {
        assert!(wait.elapsed() < Duration::from_secs(5), "b seals a's epoch");
        std::thread::sleep(Duration::from_millis(10));
    }
    assert!(
        !super::manifest(&store).settled(),
        "b's takeover is under way"
    );
    let out = dir.db("out.db");
    stage(&cfg, &path).unwrap().install(&out).unwrap();
    assert_eq!(ids(&out), [1, 2]);
    let restored = dir.db("restored.db");
    crate::s3::restore_to(&cfg, &restored, crate::s3::Target::Latest).unwrap();
    assert_eq!(ids(&restored), [1, 2]);

    let b = b.join().unwrap();
    b.exec("INSERT INTO t VALUES (3)").unwrap();
    assert_eq!(
        refresh_and_compare(&store, &cfg, &dir, &path, 1).0,
        [1, 2, 3]
    );
}

/// A writer that died right after writing its takeover manifest leaves the
/// old epoch unsealed: readers fail at once (no waiting) until a writer
/// opens. One that died after its seal leaves a readable database.
#[test]
fn an_unfinished_takeover_is_readable_once_sealed() {
    let store = FaultyStore::new();
    let dir = TempDir::new();
    let writer = open_db(&config(&store, "a"), &dir.db("a.db")).unwrap();
    writer.exec("CREATE TABLE t (x INTEGER)").unwrap();
    writer.exec("INSERT INTO t VALUES (1)").unwrap();
    let cfg = config(&store, "reader");
    let path = dir.db("r.db");
    let log_bytes = stage(&cfg, &path)
        .unwrap()
        .install(&path)
        .unwrap()
        .log_bytes;
    let mut manifest = super::manifest(&store);
    manifest.generation += 1;
    manifest.writer = "b".into();
    super::put_manifest(&store, &manifest);
    let started = std::time::Instant::now();
    let err = stage(&cfg, &path).err().expect("not sealed");
    assert!(
        err.to_string().contains("is taking the database over"),
        "{err}"
    );
    assert!(
        started.elapsed() < std::time::Duration::from_secs(2),
        "no waiting"
    );

    let remote = crate::s3::remote::Remote::new(store.clone(), super::PREFIX);
    remote
        .put(
            &manifest.epoch.segment_key(log_bytes),
            crate::s3::layout::seal_body(manifest.generation),
            crate::s3::remote::Put::Create,
        )
        .unwrap();
    let out = dir.db("out.db");
    stage(&cfg, &path).unwrap().install(&out).unwrap();
    assert_eq!(ids(&out), [1]);
}

/// No takeover: a's commit of row 2 fails while its upload is still in
/// flight; a checkpoint seals the epoch at that offset and collects it, and
/// then the upload lands in the collected epoch (followed by a seal, with
/// `seal`, as a stale writer's checkpoint would write it). A refresh that
/// read the manifest before the checkpoint and cached that epoch must not
/// take the upload for a new commit, sealed or not.
fn late_upload_into_a_collected_epoch(seal: bool) {
    use std::time::Duration;
    let store = FaultyStore::new();
    let dir = TempDir::new();
    let a = open_db(&config(&store, "a"), &dir.db("a.db")).unwrap();
    a.exec("CREATE TABLE t (x INTEGER)").unwrap();
    a.exec("INSERT INTO t VALUES (1)").unwrap();
    let cfg = config(&store, "reader");
    let path = dir.db("r.db");
    stage(&cfg, &path).unwrap().install(&path).unwrap();
    let old_epoch = super::manifest(&store).epoch;
    store.inject(
        Op::Put,
        "/log/",
        Fault::LandLater(Duration::from_secs(2)),
        1,
    );
    assert!(a.exec("INSERT INTO t VALUES (2)").is_err());

    let (started, release) = store.hold_next_list();
    let refresh = {
        let (cfg, path) = (cfg.clone(), path.clone());
        std::thread::spawn(move || stage(&cfg, &path))
    };
    started.recv_timeout(Duration::from_secs(5)).unwrap();
    a.checkpoint();
    assert!(
        super::keys(&store, &old_epoch.log_dir()).is_empty(),
        "collected"
    );
    let landed = std::time::Instant::now();
    while super::keys(&store, &old_epoch.log_dir()).is_empty() {
        assert!(
            landed.elapsed() < Duration::from_secs(5),
            "a's upload lands"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
    if seal {
        let remote = crate::s3::remote::Remote::new(store.clone(), super::PREFIX);
        let [key] = &super::keys(&store, &old_epoch.log_dir())[..] else {
            panic!("one object")
        };
        let (_, offset) = crate::s3::layout::parse_segment_key(key).unwrap();
        let size = remote.get(key).unwrap().unwrap().bytes.len() as u64;
        remote
            .put(
                &old_epoch.segment_key(offset + size),
                crate::s3::layout::seal_body(old_epoch.generation),
                crate::s3::remote::Put::Create,
            )
            .unwrap();
    }
    release.send(()).unwrap();

    let out = dir.db("out.db");
    refresh.join().unwrap().unwrap().install(&out).unwrap();
    assert_eq!(ids(&out), [1]);
    let full = dir.db("full.db");
    crate::s3::restore_to(&cfg, &full, crate::s3::Target::Latest).unwrap();
    assert_eq!(ids(&full), [1]);
}

#[test]
fn a_refresh_ignores_a_late_upload_into_a_collected_epoch() {
    late_upload_into_a_collected_epoch(false);
    late_upload_into_a_collected_epoch(true);
}

#[test]
fn a_log_cache_damaged_in_place_is_not_used() {
    let store = FaultyStore::new();
    let dir = TempDir::new();
    let writer = open_db(&config(&store, "writer"), &dir.db("w.db")).unwrap();
    writer.exec("CREATE TABLE t (x INTEGER)").unwrap();
    for x in 1..=3 {
        writer.exec(&format!("INSERT INTO t VALUES ({x})")).unwrap();
    }
    let cfg = config(&store, "reader");
    let path = dir.db("r.db");
    stage(&cfg, &path).unwrap().install(&path).unwrap();
    // One bit of the last frame's trailer, the length unchanged.
    let cache = path.with_file_name(".r.db.s3-logcache");
    let mut bytes = std::fs::read(&cache).unwrap();
    *bytes.last_mut().unwrap() ^= 1;
    std::fs::write(&cache, &bytes).unwrap();

    writer.exec("INSERT INTO t VALUES (4)").unwrap();
    assert_eq!(
        refresh_and_compare(&store, &cfg, &dir, &path, 1).0,
        [1, 2, 3, 4]
    );
    writer.exec("INSERT INTO t VALUES (5)").unwrap();
    let (got, fetched) = refresh_and_compare(&store, &cfg, &dir, &path, 2);
    assert_eq!(got, [1, 2, 3, 4, 5]);
    assert_eq!(fetched, 1, "incremental again after the full restore");
}

/// A point-in-time restore of a retained past epoch, and a replica's first
/// refresh, while the writer keeps checkpointing and every snapshot
/// download takes 3 s: each attempt sees the writer move to a new epoch,
/// yet the epoch read stays retained, so the read stands.
#[test]
fn restores_finish_while_the_writer_checkpoints() {
    use std::time::{Duration, Instant};
    let store = FaultyStore::new();
    let dir = TempDir::new();
    let mut wcfg = config(&store, "writer");
    wcfg.retain_epochs = 40;
    let writer = open_db(&wcfg, &dir.db("w.db")).unwrap();
    writer.exec("CREATE TABLE t (x INTEGER)").unwrap();
    writer.exec("INSERT INTO t VALUES (1)").unwrap();
    writer.checkpoint();
    let past = super::manifest(&store).epoch;
    writer.exec("INSERT INTO t VALUES (2)").unwrap();
    writer.checkpoint();

    store.inject(
        Op::Get,
        "snapshots/",
        Fault::Delay(Duration::from_secs(3)),
        40,
    );
    let cfg = config(&store, "reader");
    let readers = {
        let (cfg, dir_path) = (cfg.clone(), dir.db("x").parent().unwrap().to_path_buf());
        std::thread::spawn(move || {
            let restored = dir_path.join("past.db");
            let pitr = crate::s3::restore_to(&cfg, &restored, crate::s3::Target::Epoch(past.seq));
            let replica = dir_path.join("r.db");
            let staged = stage(&cfg, &replica).map(|s| s.install(&replica));
            (pitr.map(|_| restored), staged.map(|_| replica))
        })
    };
    let started = Instant::now();
    let mut x = 2;
    let mut epochs = 0;
    while !readers.is_finished() {
        assert!(started.elapsed() < Duration::from_secs(30));
        x += 1;
        writer.exec(&format!("INSERT INTO t VALUES ({x})")).unwrap();
        writer.checkpoint();
        epochs += 1;
        std::thread::sleep(Duration::from_millis(100));
    }
    let (pitr, replica) = readers.join().unwrap();
    store.clear_faults();
    assert!(
        epochs >= 2,
        "the writer moved on during the reads ({epochs} epochs)"
    );
    assert_eq!(ids(&pitr.expect("point-in-time restore")), [1, 2]);
    let shown = ids(&replica.expect("replica bootstrap"));
    let all: Vec<i64> = (1..=x).collect();
    assert!(all.starts_with(&shown) && shown.len() >= 2, "{shown:?}");
}
