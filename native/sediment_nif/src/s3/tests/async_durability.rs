//! `durability: async`: commits are acknowledged once written locally and
//! uploaded in log order in the background.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use super::faulty::{Fault, FaultyStore, Op};
use super::{assert_err_contains, config, open_db, segments, Db, TempDir};
use crate::s3::{restore_to, S3Config, S3Error, Target};

fn async_config(store: &Arc<FaultyStore>, owner: &str) -> S3Config {
    let mut cfg = config(store, owner);
    cfg.async_durability = true;
    cfg
}

fn setup(cfg: &S3Config, dir: &TempDir) -> Db {
    let db = open_db(cfg, &dir.db("a.db")).unwrap();
    db.exec("CREATE TABLE t(id INTEGER PRIMARY KEY)").unwrap();
    db.storage.flush(Duration::from_secs(5)).unwrap();
    db
}

/// Ids restored from S3 alone.
fn restored_ids(cfg: &S3Config, dir: &TempDir, name: &str) -> Vec<i64> {
    let path = dir.db(name);
    restore_to(cfg, &path, Target::Latest).unwrap();
    let db = super::open_plain(&path);
    db.ids()
}

const SLOW: Fault = Fault::Delay(Duration::from_millis(300));

#[test]
fn commits_return_before_their_upload_and_flush_waits_for_it() {
    let store = FaultyStore::new();
    let dir = TempDir::new();
    let cfg = async_config(&store, "a");
    let db = setup(&cfg, &dir);
    store.inject(Op::Put, "log/", SLOW, 1_000);
    let started = Instant::now();
    for i in 1..=5 {
        db.exec(&format!("INSERT INTO t VALUES ({i})")).unwrap();
    }
    assert!(
        started.elapsed() < Duration::from_millis(250),
        "{:?}",
        started.elapsed()
    );
    assert!(db.storage.pending().0 > 0);
    assert!(db.storage.info().asynchronous);

    let (epoch, offset) = db.storage.flush(Duration::from_secs(10)).unwrap();
    let info = db.storage.info();
    assert_eq!((info.pending_frames, info.pending_bytes), (0, 0));
    assert_eq!((epoch, offset), (info.durable_epoch, info.durable_offset));
    assert_eq!(offset, info.log_offset);
    store.clear_faults();
    db.crash();
    assert_eq!(restored_ids(&cfg, &dir, "r.db"), vec![1, 2, 3, 4, 5]);
}

#[test]
fn queued_frames_are_coalesced_into_contiguous_segments() {
    let store = FaultyStore::new();
    let dir = TempDir::new();
    let cfg = async_config(&store, "a");
    let db = setup(&cfg, &dir);
    let before = segments(&store).len();
    // Every upload takes a while, so commits pile up behind each one.
    store.inject(
        Op::Put,
        "log/",
        Fault::Delay(Duration::from_millis(50)),
        1_000,
    );
    for i in 1..=40 {
        db.exec(&format!("INSERT INTO t VALUES ({i})")).unwrap();
    }
    db.storage.flush(Duration::from_secs(10)).unwrap();
    let objects = segments(&store).len() - before;
    assert!(objects < 20, "40 commits in {objects} objects");
    assert_eq!(db.storage.info().uploaded_frames, 41);
    db.crash();
    assert_eq!(
        restored_ids(&cfg, &dir, "r.db"),
        (1..=40).collect::<Vec<_>>()
    );
}

#[test]
fn commits_wait_once_too_much_is_pending() {
    let store = FaultyStore::new();
    let dir = TempDir::new();
    let mut cfg = async_config(&store, "a");
    cfg.max_pending_bytes = 1;
    let db = setup(&cfg, &dir);
    store.inject(
        Op::Put,
        "log/",
        Fault::Delay(Duration::from_millis(150)),
        1_000,
    );
    let started = Instant::now();
    for i in 1..=4 {
        db.exec(&format!("INSERT INTO t VALUES ({i})")).unwrap();
    }
    // Each commit after the first waits for the one before to upload.
    assert!(
        started.elapsed() >= Duration::from_millis(400),
        "{:?}",
        started.elapsed()
    );
    assert!(db.storage.pending().0 <= 1);
}

#[test]
fn commits_wait_once_the_oldest_pending_one_is_too_old() {
    let store = FaultyStore::new();
    let dir = TempDir::new();
    let mut cfg = async_config(&store, "a");
    cfg.max_lag = Duration::from_millis(50);
    let db = setup(&cfg, &dir);
    store.inject(
        Op::Put,
        "log/",
        Fault::Delay(Duration::from_millis(300)),
        1_000,
    );
    db.exec("INSERT INTO t VALUES (1)").unwrap();
    std::thread::sleep(Duration::from_millis(80));
    let started = Instant::now();
    db.exec("INSERT INTO t VALUES (2)").unwrap();
    assert!(
        started.elapsed() >= Duration::from_millis(150),
        "{:?}",
        started.elapsed()
    );
}

#[test]
fn a_store_that_keeps_failing_poisons_the_writer_and_names_the_durable_prefix() {
    let store = FaultyStore::new();
    let dir = TempDir::new();
    let cfg = async_config(&store, "a");
    let db = setup(&cfg, &dir);
    db.exec("INSERT INTO t VALUES (1)").unwrap();
    db.storage.flush(Duration::from_secs(5)).unwrap();
    let durable = db.storage.info().durable_offset;
    store.inject(Op::Put, "log/", Fault::Fail, 1_000);
    db.exec("INSERT INTO t VALUES (2)").unwrap();
    let err = db.storage.flush(Duration::from_secs(10)).unwrap_err();
    assert!(matches!(err, S3Error::Fenced(_)), "{err:?}");
    let message = err.to_string();
    assert!(message.contains(&format!("/{durable})")), "{message}");
    assert_err_contains(db.exec("INSERT INTO t VALUES (3)"), "fenced");
    store.clear_faults();
    db.crash();
    assert_eq!(restored_ids(&cfg, &dir, "r.db"), vec![1]);
}

#[test]
fn a_takeover_poisons_a_writer_with_pending_commits() {
    let store = FaultyStore::new();
    let dir = TempDir::new();
    let cfg = async_config(&store, "a");
    let db = setup(&cfg, &dir);
    db.exec("INSERT INTO t VALUES (1)").unwrap();
    db.storage.flush(Duration::from_secs(5)).unwrap();
    store.inject(Op::Put, "log/", Fault::Delay(Duration::from_millis(400)), 1);
    db.exec("INSERT INTO t VALUES (2)").unwrap();
    // The same owner reopens elsewhere (a restart) while 2 may be queued.
    let successor = open_db(&cfg, &dir.db("b.db")).unwrap();
    // Nothing A commits from now on becomes durable: the first flush after
    // the takeover fails, and later commits fail outright.
    let _ = db.exec("INSERT INTO t VALUES (3)");
    let err = db.storage.flush(Duration::from_secs(10)).unwrap_err();
    assert!(matches!(err, S3Error::Fenced(_)), "{err:?}");
    assert_err_contains(db.exec("INSERT INTO t VALUES (4)"), "fenced");
    let rows = successor.int("SELECT count(*) FROM t");
    assert!(rows == 1 || rows == 2, "{rows}");
    assert_eq!(successor.int("SELECT count(*) FROM t WHERE id > 2"), 0);
    db.crash();
}

#[test]
fn a_checkpoint_uploads_the_pending_commits_first() {
    let store = FaultyStore::new();
    let dir = TempDir::new();
    let cfg = async_config(&store, "a");
    let db = setup(&cfg, &dir);
    store.inject(
        Op::Put,
        "log/",
        Fault::Delay(Duration::from_millis(100)),
        1_000,
    );
    for i in 1..=10 {
        db.exec(&format!("INSERT INTO t VALUES ({i})")).unwrap();
    }
    let epoch = db.storage.info().epoch;
    db.checkpoint();
    let info = db.storage.info();
    assert_ne!(info.epoch, epoch);
    assert_eq!(info.pending_frames, 0);
    store.clear_faults();
    db.crash();
    assert_eq!(
        restored_ids(&cfg, &dir, "r.db"),
        (1..=10).collect::<Vec<_>>()
    );
}

#[test]
fn closing_uploads_the_pending_commits() {
    let store = FaultyStore::new();
    let dir = TempDir::new();
    let cfg = async_config(&store, "a");
    let db = setup(&cfg, &dir);
    store.inject(
        Op::Put,
        "log/",
        Fault::Delay(Duration::from_millis(100)),
        1_000,
    );
    for i in 1..=5 {
        db.exec(&format!("INSERT INTO t VALUES ({i})")).unwrap();
    }
    assert!(db.storage.close(Duration::from_secs(10)));
    db.crash();
    store.clear_faults();
    assert_eq!(restored_ids(&cfg, &dir, "r.db"), vec![1, 2, 3, 4, 5]);
}

#[test]
fn a_crash_leaves_a_prefix_of_the_commits() {
    for attempt in 0..5 {
        let store = FaultyStore::new();
        let dir = TempDir::new();
        let cfg = async_config(&store, "a");
        let db = setup(&cfg, &dir);
        store.inject(
            Op::Put,
            "log/",
            Fault::Delay(Duration::from_millis(200)),
            1_000,
        );
        for i in 1..=30 {
            db.exec(&format!("INSERT INTO t VALUES ({i})")).unwrap();
        }
        std::thread::sleep(Duration::from_millis(10 * attempt));
        db.crash();
        std::thread::sleep(Duration::from_millis(100));
        store.clear_faults();
        let ids = restored_ids(&cfg, &dir, "r.db");
        let prefix: Vec<i64> = (1..=ids.len() as i64).collect();
        assert_eq!(ids, prefix, "attempt {attempt}");
        assert!(ids.len() < 30, "attempt {attempt}: nothing was pending");
    }
}

#[test]
fn flush_is_immediate_with_sync_durability() {
    let store = FaultyStore::new();
    let dir = TempDir::new();
    let db = setup(&config(&store, "a"), &dir);
    db.exec("INSERT INTO t VALUES (1)").unwrap();
    let (_, offset) = db.storage.flush(Duration::ZERO).unwrap();
    assert_eq!(offset, db.storage.info().log_offset);
    assert!(!db.storage.info().asynchronous);
}

#[test]
fn a_flush_gives_up_when_cancelled_or_after_its_timeout() {
    let store = FaultyStore::new();
    let dir = TempDir::new();
    let cfg = async_config(&store, "a");
    let db = setup(&cfg, &dir);
    store.inject(Op::Put, "log/", Fault::Delay(Duration::from_secs(2)), 1);
    db.exec("INSERT INTO t VALUES (1)").unwrap();
    let err = db.storage.flush(Duration::from_millis(100)).unwrap_err();
    assert!(err.to_string().contains("timed out after"), "{err}");
    let flag = AtomicBool::new(true);
    let err =
        crate::s3::remote::with_cancel(&|| flag.load(std::sync::atomic::Ordering::SeqCst), || {
            db.storage.flush(Duration::from_secs(10))
        })
        .unwrap_err();
    assert!(err.to_string().contains("cancelled"), "{err}");
    flag.store(false, Ordering::SeqCst);
    db.storage.flush(Duration::from_secs(10)).unwrap();
}

#[test]
fn info_does_not_wait_for_an_upload_in_flight() {
    let store = FaultyStore::new();
    let dir = TempDir::new();
    let cfg = async_config(&store, "a");
    let db = setup(&cfg, &dir);
    store.inject(
        Op::Put,
        "log/",
        Fault::Delay(Duration::from_millis(600)),
        1_000,
    );
    db.exec("INSERT INTO t VALUES (1)").unwrap();
    std::thread::sleep(Duration::from_millis(50));
    db.exec("INSERT INTO t VALUES (2)").unwrap();
    let started = Instant::now();
    let info = db.storage.info();
    assert!(
        started.elapsed() < Duration::from_millis(100),
        "{:?}",
        started.elapsed()
    );
    assert!(info.pending_frames >= 1, "{info:?}");
    assert!(info.lag_ms > 0 || info.pending_frames > 0);
    db.storage.flush(Duration::from_secs(10)).unwrap();
    let info = db.storage.info();
    assert_eq!(info.pending_frames, 0);
    assert_eq!(info.uploaded_frames, 3);
}

/// A snapshot retry streams while the next checkpoint backfills the
/// same DB file; what gets published must still restore.
#[test]
fn a_snapshot_retry_is_isolated_from_the_next_checkpoint() {
    let store = FaultyStore::new();
    let dir = TempDir::new();
    let mut cfg = async_config(&store, "a");
    cfg.checkpoint_threshold = Some(-1);
    cfg.lease_ttl = Duration::from_secs(60);
    cfg.max_pending_bytes = 100 * 1024 * 1024;
    let db = setup(&cfg, &dir);
    db.exec("CREATE TABLE big(id INTEGER PRIMARY KEY, v BLOB)")
        .unwrap();
    db.exec("BEGIN").unwrap();
    for i in 1..=240 {
        db.exec(&format!("INSERT INTO big VALUES ({i}, randomblob(128000))"))
            .unwrap();
    }
    db.exec("COMMIT").unwrap();
    db.checkpoint();
    // Ten MB of changes: an incremental snapshot large enough to stream.
    db.exec("UPDATE big SET v = randomblob(128000) WHERE id <= 80")
        .unwrap();
    db.storage.flush(Duration::from_secs(10)).unwrap();
    let retry_epoch = super::manifest(&store).epoch.next(1);
    store.inject(Op::Put, &retry_epoch.delta_key(), Fault::Fail, 1);
    db.checkpoint();
    assert!(db.storage.info().snapshot_pending);
    // The uploader retries the snapshot with a commit waiting behind it.
    store.inject(
        Op::Put,
        &retry_epoch.delta_key(),
        Fault::Delay(Duration::from_millis(50)),
        1,
    );
    let next_epoch = retry_epoch.next(1);
    store.inject(Op::Put, &next_epoch.delta_key(), Fault::Fail, 100);
    store.inject(Op::Put, &next_epoch.snapshot_key(), Fault::Fail, 100);
    db.exec("UPDATE big SET v = randomblob(128000) WHERE id = 80")
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    while !store
        .put_log()
        .iter()
        .any(|k| k.starts_with("MULTIPART:") && k.contains(&retry_epoch.delta_key()))
    {
        assert!(
            Instant::now() < deadline,
            "uploader never started a multipart delta: {:?}",
            store.put_log()
        );
        std::thread::sleep(Duration::from_millis(1));
    }
    // Its first compressed part is in flight while the next checkpoint
    // backfills the same DB file, before truncate() drains the queue.
    db.checkpoint();
    db.storage.flush(Duration::from_secs(10)).unwrap();
    eprintln!(
        "manifest epoch {:?}, pending snapshot {}",
        super::manifest(&store).epoch,
        db.storage.info().snapshot_pending
    );
    let result = restore_to(&cfg, &dir.db("restored.db"), Target::Latest);
    eprintln!("RESTORE RESULT: {result:?}");
    assert!(
        result.is_ok(),
        "a successful flush must leave a restorable database: {result:?}"
    );
    db.crash();
}

#[test]
fn checkpoint_images_live_only_while_their_snapshot_is_pending() {
    let images = |dir: &TempDir| -> usize {
        std::fs::read_dir(dir.db("a.db").parent().unwrap())
            .unwrap()
            .filter(|e| {
                e.as_ref()
                    .unwrap()
                    .file_name()
                    .to_string_lossy()
                    .contains(".s3-snapshot-")
            })
            .count()
    };
    let store = FaultyStore::new();
    let dir = TempDir::new();
    let cfg = async_config(&store, "a");
    let db = setup(&cfg, &dir);
    db.exec("INSERT INTO t VALUES (1)").unwrap();
    db.checkpoint();
    assert_eq!(images(&dir), 0, "published");

    // Fails at the checkpoint and once more; the uploader's next try works.
    store.inject(Op::Put, "snapshots/", Fault::Fail, 2);
    db.exec("INSERT INTO t VALUES (2)").unwrap();
    db.checkpoint();
    assert!(db.storage.info().snapshot_pending);
    assert_eq!(images(&dir), 1, "pending");
    db.exec("INSERT INTO t VALUES (3)").unwrap();
    db.storage.flush(Duration::from_secs(10)).unwrap();
    assert!(!db.storage.info().snapshot_pending);
    assert_eq!(images(&dir), 0, "published by a retry");

    // An image left by a crash is removed by the next open of that file.
    // (A crashed storage stays registered in this VM, so open another path.)
    db.crash();
    let path = dir.db("b.db");
    let stray = path.with_file_name(".b.db.s3-snapshot-1-1");
    std::fs::write(&stray, b"left by a crash").unwrap();
    let reopened = open_db(&cfg, &path).unwrap();
    assert!(!stray.exists(), "the next open cleans up");
    assert_eq!(reopened.int("SELECT count(*) FROM t"), 3);
}

/// Closes a test database the way a pool closes a connection.
fn close(db: Db) {
    let conn = db.conn.clone();
    db.storage.close(Duration::from_secs(1));
    drop(db);
    conn.close().unwrap();
}

/// Commits acknowledged before a fence are lost; a later writer of
/// the same file (a pool that reconnected) must not certify them with a
/// successful flush until the loss is acknowledged.
#[test]
fn a_loss_is_reported_by_the_next_writer_of_the_file_until_acknowledged() {
    let store = FaultyStore::new();
    let dir = TempDir::new();
    let cfg = async_config(&store, "a");
    let db = setup(&cfg, &dir);
    db.exec("INSERT INTO t VALUES (1)").unwrap();
    db.storage.flush(Duration::from_secs(5)).unwrap();
    // Long enough that the successor below has taken over before the upload
    // lands, even on a loaded machine: an upload landing first leaves the old
    // writer nothing to be fenced on.
    store.inject(Op::Put, "log/", Fault::Delay(Duration::from_secs(3)), 1);
    db.exec("INSERT INTO t VALUES (2)").unwrap();
    db.exec("INSERT INTO t VALUES (3)").unwrap();
    // A restart elsewhere takes over while 2 and 3 are queued.
    let successor = open_db(&cfg, &dir.db("b.db")).unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    while db.storage.poisoned().is_none() {
        assert!(Instant::now() < deadline, "the old writer was never fenced");
        std::thread::sleep(Duration::from_millis(20));
    }
    close(db);
    close(successor);

    // The pool reconnects to the same file: a new writer, restored from S3.
    let again = open_db(&cfg, &dir.db("a.db")).unwrap();
    assert_eq!(again.int("SELECT count(*) FROM t WHERE id > 1"), 0);
    let err = again.storage.flush(Duration::from_secs(10)).unwrap_err();
    assert!(matches!(err, S3Error::Lost(_)), "{err:?}");
    let message = err.to_string();
    assert!(
        message.contains("acknowledge_loss") && message.contains("durable up to"),
        "{message}"
    );
    assert!(again.storage.info().lost.is_some());
    again.exec("INSERT INTO t VALUES (4)").unwrap();
    assert!(
        again.storage.flush(Duration::from_secs(10)).is_err(),
        "sticky"
    );

    assert!(again.storage.acknowledge_loss().is_some());
    again.storage.flush(Duration::from_secs(10)).unwrap();
    assert!(again.storage.info().lost.is_none());
    close(again);
}

#[test]
fn flush_through_waits_only_for_the_given_commit() {
    let store = FaultyStore::new();
    let dir = TempDir::new();
    let cfg = async_config(&store, "a");
    let db = setup(&cfg, &dir);
    crate::s3::take_last_enqueued();
    db.exec("INSERT INTO t VALUES (1)").unwrap();
    let first = crate::s3::take_last_enqueued().expect("this thread queued commit 1");
    db.storage
        .flush_through(first, Duration::from_secs(5))
        .unwrap();
    store.inject(
        Op::Put,
        "log/",
        Fault::Delay(Duration::from_millis(1500)),
        1,
    );
    db.exec("INSERT INTO t VALUES (2)").unwrap();
    let started = Instant::now();
    db.storage
        .flush_through(first, Duration::from_secs(5))
        .unwrap();
    assert!(
        started.elapsed() < Duration::from_millis(200),
        "{:?}",
        started.elapsed()
    );
    let err = db.storage.flush(Duration::from_millis(100)).unwrap_err();
    assert!(err.to_string().contains("timed out"), "{err}");
}

fn interval_config(store: &Arc<FaultyStore>, interval_ms: u64) -> S3Config {
    let mut cfg = async_config(store, "a");
    cfg.upload_interval = Duration::from_millis(interval_ms);
    cfg
}

#[test]
fn commits_within_the_upload_interval_share_one_segment() {
    let store = FaultyStore::new();
    let dir = TempDir::new();
    // Long enough that the inserts fit in it on a loaded machine.
    let mut cfg = interval_config(&store, 2_000);
    cfg.max_lag = Duration::from_secs(10);
    let db = setup(&cfg, &dir);
    let before = segments(&store).len();
    for i in 1..=20 {
        db.exec(&format!("INSERT INTO t VALUES ({i})")).unwrap();
    }
    std::thread::sleep(Duration::from_millis(100));
    assert_eq!(
        segments(&store).len(),
        before,
        "nothing uploaded before the interval"
    );
    std::thread::sleep(Duration::from_millis(2_500));
    assert_eq!(
        segments(&store).len(),
        before + 1,
        "one segment for 20 commits"
    );
    assert_eq!(db.storage.info().pending_frames, 0);
    db.crash();
    assert_eq!(
        restored_ids(&cfg, &dir, "r.db"),
        (1..=20).collect::<Vec<_>>()
    );
}

#[test]
fn flushes_sync_commits_and_checkpoints_do_not_wait_for_the_upload_interval() {
    let store = FaultyStore::new();
    let dir = TempDir::new();
    let cfg = interval_config(&store, 900);
    let db = setup(&cfg, &dir);
    db.exec("INSERT INTO t VALUES (1)").unwrap();
    let started = Instant::now();
    db.storage.flush(Duration::from_secs(5)).unwrap();
    assert!(
        started.elapsed() < Duration::from_millis(300),
        "flush {:?}",
        started.elapsed()
    );

    crate::s3::take_last_enqueued();
    db.exec("INSERT INTO t VALUES (2)").unwrap();
    let seq = crate::s3::take_last_enqueued().expect("this thread queued commit 2");
    let started = Instant::now();
    db.storage
        .flush_through(seq, Duration::from_secs(5))
        .unwrap();
    assert!(
        started.elapsed() < Duration::from_millis(300),
        "sync {:?}",
        started.elapsed()
    );

    db.exec("INSERT INTO t VALUES (3)").unwrap();
    let started = Instant::now();
    db.checkpoint();
    assert!(
        started.elapsed() < Duration::from_millis(600),
        "checkpoint {:?}",
        started.elapsed()
    );
    db.crash();
    assert_eq!(restored_ids(&cfg, &dir, "r.db"), [1, 2, 3]);
}

#[test]
fn max_pending_bytes_uploads_before_the_upload_interval() {
    let store = FaultyStore::new();
    let dir = TempDir::new();
    let mut cfg = interval_config(&store, 900);
    cfg.max_pending_bytes = 4096;
    let db = open_db(&cfg, &dir.db("a.db")).unwrap();
    db.exec("CREATE TABLE b(id INTEGER PRIMARY KEY, v BLOB)")
        .unwrap();
    db.storage.flush(Duration::from_secs(5)).unwrap();
    let before = segments(&store).len();
    db.exec("INSERT INTO b VALUES (1, randomblob(8000))")
        .unwrap();
    std::thread::sleep(Duration::from_millis(300));
    assert!(
        segments(&store).len() > before,
        "uploaded before the interval"
    );
}

#[test]
fn a_crash_within_the_upload_interval_leaves_a_prefix() {
    let store = FaultyStore::new();
    let dir = TempDir::new();
    let cfg = interval_config(&store, 500);
    let db = setup(&cfg, &dir);
    for i in 1..=10 {
        db.exec(&format!("INSERT INTO t VALUES ({i})")).unwrap();
    }
    db.crash();
    std::thread::sleep(Duration::from_millis(700));
    let ids = restored_ids(&cfg, &dir, "r.db");
    assert_eq!(ids, (1..=ids.len() as i64).collect::<Vec<_>>());
    assert!(
        ids.len() < 10,
        "the interval's commits were not uploaded: {ids:?}"
    );
}

#[test]
fn the_upload_interval_is_async_only_and_below_max_lag() {
    let pairs = |extra: &[(&str, &str)]| {
        let mut pairs = vec![("bucket", "b"), ("encryption", "false")];
        pairs.extend_from_slice(extra);
        S3Config::from_pairs(pairs).and_then(|cfg| cfg.validate().map(|()| cfg))
    };
    let cfg = pairs(&[("upload_interval_ms", "250")]).unwrap();
    assert_eq!(cfg.upload_interval, Duration::from_millis(250));
    let err = pairs(&[("upload_interval_ms", "250"), ("durability", "sync")]).unwrap_err();
    assert!(err.to_string().contains("async only"), "{err}");
    let err = pairs(&[("upload_interval_ms", "1000")]).unwrap_err();
    assert!(err.to_string().contains("below max_lag_ms"), "{err}");
    pairs(&[("upload_interval_ms", "1000"), ("max_lag_ms", "2000")]).unwrap();
}
