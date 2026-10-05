mod async_durability;
mod destroy;
mod export;
mod faulty;
mod group_commit;
mod import;
mod incremental;
mod replica;
mod seaweed;

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use turso_core::mvcc::persistent_storage::DurableStorage;
use turso_core::{Connection, Database, OpenOptions, PlatformIO, SqliteDialect, Value, IO};

use super::layout::{Epoch, Manifest, MANIFEST_KEY};
use super::remote::{Put, Remote};
use super::{prepare, S3Config, S3DurableStorage, S3Error, Target};
use faulty::{Fault, FaultyStore, Op};

const PREFIX: &str = "dbs/test";

pub(crate) struct TempDir(PathBuf);

impl TempDir {
    pub(crate) fn new() -> Self {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        if COUNTER.load(Ordering::Relaxed) == 0 {
            prune_dead_runs();
        }
        let dir = std::env::temp_dir().join(format!(
            "sediment-s3-{}-{}",
            std::process::id(),
            COUNTER.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        Self(dir)
    }

    pub(crate) fn db(&self, name: &str) -> PathBuf {
        self.0.join(name)
    }
}

/// Removes directories left by test processes that were killed before their
/// `TempDir`s were dropped (Linux: a pid without /proc entry is gone).
fn prune_dead_runs() {
    let Ok(entries) = std::fs::read_dir(std::env::temp_dir()) else {
        return;
    };
    for entry in entries.flatten() {
        let name = entry.file_name().to_string_lossy().into_owned();
        let Some(pid) = name
            .strip_prefix("sediment-s3-")
            .and_then(|rest| rest.split('-').next())
        else {
            continue;
        };
        if !Path::new("/proc").join(pid).exists() && Path::new("/proc/self").exists() {
            let _ = std::fs::remove_dir_all(entry.path());
        }
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn config(store: &Arc<FaultyStore>, owner: &str) -> S3Config {
    let mut cfg = S3Config::new("", PREFIX);
    cfg.store = Some(store.clone());
    cfg.owner = Some(owner.to_string());
    cfg.lease_ttl = Duration::from_secs(10);
    // Encryption is the default; tests that use a key set it.
    cfg.unencrypted = true;
    cfg
}

pub(crate) struct Db {
    pub _db: Arc<Database>,
    pub conn: Arc<Connection>,
    pub storage: Arc<S3DurableStorage>,
}

impl Db {
    pub(crate) fn exec(&self, sql: &str) -> turso_core::Result<()> {
        self.conn.execute(sql)
    }

    pub(crate) fn rows(&self, sql: &str) -> Vec<Vec<Value>> {
        let mut stmt = self.conn.query(sql).unwrap().unwrap();
        stmt.run_collect_rows().unwrap()
    }

    pub(crate) fn int(&self, sql: &str) -> i64 {
        let value = &self.rows(sql)[0][0];
        value
            .to_string()
            .parse()
            .unwrap_or_else(|_| panic!("expected an integer, got {value:?}"))
    }

    /// Simulates a crash: nothing is closed, checkpointed, or released.
    /// A crash: none of the writer's threads (uploader, lease renewal,
    /// background publication) goes on, and nothing is closed or released.
    pub(crate) fn crash(self) {
        self.storage.stop_threads_for_test();
        std::mem::forget(self);
    }

    /// Checkpoints, then waits for the background publication and GC.
    pub(crate) fn checkpoint(&self) {
        self.exec("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
        self.storage.wait_background(Duration::MAX);
    }
}

pub(crate) fn open_db(cfg: &S3Config, path: &Path) -> Result<Db, S3Error> {
    let storage = prepare(cfg, path)?;
    Ok(open_with(storage, path))
}

pub(crate) fn open_with(storage: Arc<S3DurableStorage>, path: &Path) -> Db {
    open_with_encryption(storage, path, None)
}

pub(crate) fn open_with_encryption(
    storage: Arc<S3DurableStorage>,
    path: &Path,
    encryption: Option<&turso_core::EncryptionOpts>,
) -> Db {
    let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
    let db = Database::open(
        io,
        path.to_str().unwrap(),
        OpenOptions::new(Arc::new(SqliteDialect))
            .db_opts(
                turso_core::DatabaseOpts::new()
                    .with_encryption(encryption.is_some())
                    .with_views(true)
                    .with_index_method(true),
            )
            .encryption(encryption.cloned())
            .durable_storage(storage.clone() as Arc<dyn DurableStorage>),
    )
    .unwrap();
    let key = encryption.map(|e| turso_core::EncryptionKey::from_hex_string(&e.hexkey).unwrap());
    let conn = db.connect_with_encryption(key).unwrap();
    Db {
        _db: db,
        conn,
        storage,
    }
}

fn manifest(store: &Arc<FaultyStore>) -> Manifest {
    let remote = Remote::new(store.clone(), PREFIX);
    Manifest::decode(&remote.get(MANIFEST_KEY).unwrap().unwrap().bytes).unwrap()
}

fn put_manifest(store: &Arc<FaultyStore>, manifest: &Manifest) {
    let remote = Remote::new(store.clone(), PREFIX);
    remote
        .put(MANIFEST_KEY, manifest.encode(), Put::Overwrite)
        .unwrap();
}

fn keys(store: &Arc<FaultyStore>, dir: &str) -> Vec<String> {
    let remote = Remote::new(store.clone(), PREFIX);
    let mut keys: Vec<String> = remote
        .list(dir)
        .unwrap()
        .into_iter()
        .map(|o| o.key)
        .collect();
    keys.sort();
    keys
}

fn segments(store: &Arc<FaultyStore>) -> Vec<String> {
    keys(store, "log/")
}

pub(crate) fn assert_err_contains<T>(result: turso_core::Result<T>, needle: &str) {
    match result {
        Ok(_) => panic!("expected an error containing {needle:?}"),
        Err(err) => assert!(
            err.to_string().contains(needle),
            "error {err} does not contain {needle:?}"
        ),
    }
}

#[test]
fn local_errors_name_the_database() {
    let store = FaultyStore::new();
    let dir = TempDir::new();
    let path = dir.db("gone/a.db");
    let err = open_db(&config(&store, "a"), &path).err().unwrap();
    assert!(err.to_string().contains(path.to_str().unwrap()), "{err}");
}

#[test]
fn new_database_is_bootstrapped_in_s3() {
    let store = FaultyStore::new();
    let dir = TempDir::new();
    let db = open_db(&config(&store, "a"), &dir.db("a.db")).unwrap();
    let m = manifest(&store);
    assert_eq!(m.epoch.seq, 0);
    assert_eq!(keys(&store, "snapshots/"), vec![m.snapshot.clone()]);
    assert!(segments(&store).is_empty());
    assert_eq!(db.rows("PRAGMA journal_mode")[0][0].to_string(), "mvcc");
}

#[test]
fn every_commit_is_one_contiguous_segment_and_survives_a_crash() {
    let store = FaultyStore::new();
    let dir = TempDir::new();
    let db = open_db(&config(&store, "a"), &dir.db("a.db")).unwrap();
    db.exec("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    for i in 0..20 {
        db.exec(&format!("INSERT INTO t VALUES ({i}, 'v{i}')"))
            .unwrap();
    }
    db.exec("BEGIN").unwrap();
    db.exec("INSERT INTO t VALUES (100, 'rolled back')")
        .unwrap();
    db.exec("ROLLBACK").unwrap();
    let segs = segments(&store);
    assert_eq!(segs.len(), 21);
    assert_eq!(db.storage.info().uploaded_frames, 21);
    db.crash();

    let restored = open_db(&config(&store, "a"), &dir.db("b.db")).unwrap();
    assert_eq!(restored.int("SELECT count(*) FROM t"), 20);
    assert_eq!(restored.int("SELECT max(id) FROM t"), 19);
    restored
        .exec("INSERT INTO t VALUES (20, 'after restore')")
        .unwrap();
    // The restore folded the log into a new epoch's snapshot; the old epoch
    // is gone.
    let epoch = manifest(&store).epoch;
    assert_eq!(epoch.seq, 1);
    assert_eq!(segments(&store), vec![epoch.segment_key(0)]);
    restored.crash();

    let again = open_db(&config(&store, "a"), &dir.db("c.db")).unwrap();
    assert_eq!(again.int("SELECT count(*) FROM t"), 21);
}

#[test]
fn checkpoint_uploads_snapshot_then_manifest_then_collects_garbage() {
    let store = FaultyStore::new();
    let dir = TempDir::new();
    let db = open_db(&config(&store, "a"), &dir.db("a.db")).unwrap();
    db.exec("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    db.exec("INSERT INTO t VALUES (1, 'a')").unwrap();
    let old = manifest(&store);
    let before = store.put_log().len();
    db.checkpoint();

    let m = manifest(&store);
    assert_eq!(m.epoch.seq, old.epoch.seq + 1);
    assert_ne!(m.snapshot, old.snapshot);
    let puts: Vec<String> = store.put_log()[before..].to_vec();
    let snap = puts.iter().position(|k| k.ends_with(&m.snapshot)).unwrap();
    let man = puts.iter().position(|k| k.ends_with(MANIFEST_KEY)).unwrap();
    assert!(
        snap < man,
        "snapshot must be durable before the manifest: {puts:?}"
    );
    assert!(segments(&store).is_empty(), "old epoch is collected");
    assert_eq!(keys(&store, "snapshots/"), vec![m.snapshot.clone()]);

    db.exec("INSERT INTO t VALUES (2, 'b')").unwrap();
    assert_eq!(segments(&store), vec![m.epoch.segment_key(0)]);
    db.crash();

    let restored = open_db(&config(&store, "a"), &dir.db("b.db")).unwrap();
    assert_eq!(restored.int("SELECT count(*) FROM t"), 2);
}

#[test]
fn failed_put_fails_the_commit_and_rolls_it_back() {
    let store = FaultyStore::new();
    let dir = TempDir::new();
    let db = open_db(&config(&store, "a"), &dir.db("a.db")).unwrap();
    db.exec("CREATE TABLE t(id INTEGER PRIMARY KEY)").unwrap();
    store.inject(Op::Put, "log/", Fault::Fail, 1);
    assert_err_contains(db.exec("INSERT INTO t VALUES (1)"), "injected fault");
    assert_eq!(db.int("SELECT count(*) FROM t"), 0);

    db.exec("INSERT INTO t VALUES (2)").unwrap();
    assert_eq!(db.int("SELECT count(*) FROM t"), 1);
    db.crash();

    let restored = open_db(&config(&store, "a"), &dir.db("b.db")).unwrap();
    assert_eq!(restored.rows("SELECT id FROM t")[0][0], Value::from_i64(2));
}

#[test]
fn ambiguous_put_that_landed_is_acknowledged() {
    let store = FaultyStore::new();
    let dir = TempDir::new();
    let db = open_db(&config(&store, "a"), &dir.db("a.db")).unwrap();
    db.exec("CREATE TABLE t(id INTEGER PRIMARY KEY)").unwrap();
    store.inject(Op::Put, "log/", Fault::FailAfter, 1);
    db.exec("INSERT INTO t VALUES (1)").unwrap();
    db.exec("INSERT INTO t VALUES (2)").unwrap();
    db.crash();
    let restored = open_db(&config(&store, "a"), &dir.db("b.db")).unwrap();
    assert_eq!(restored.int("SELECT count(*) FROM t"), 2);
}

#[test]
fn a_failed_commit_landing_later_is_never_overwritten() {
    // Audit finding A: rewriting it could clobber what a new owner restored.
    let store = FaultyStore::new();
    let dir = TempDir::new();
    let db = open_db(&config(&store, "a"), &dir.db("a.db")).unwrap();
    db.exec("CREATE TABLE t(id INTEGER PRIMARY KEY)").unwrap();
    store.inject(
        Op::Put,
        "log/",
        Fault::LandLater(Duration::from_millis(300)),
        1,
    );
    assert!(db.exec("INSERT INTO t VALUES (1)").is_err());
    std::thread::sleep(Duration::from_millis(500));
    assert_err_contains(
        db.exec("INSERT INTO t VALUES (2)"),
        "reported failure reached S3",
    );
    let path = dir.db("a.db");
    assert_err_contains_s3(
        open_db(&config(&store, "a"), &path),
        "close every connection",
    );
    drop(db);

    // The indeterminate commit is durable; nothing was rewritten.
    let restored = open_db(&config(&store, "a"), &dir.db("b.db")).unwrap();
    assert_eq!(
        restored.rows("SELECT id FROM t"),
        vec![vec![Value::from_i64(1)]]
    );
}

fn assert_err_contains_s3<T>(result: Result<T, S3Error>, needle: &str) {
    match result {
        Ok(_) => panic!("expected an error containing {needle:?}"),
        Err(err) => assert!(err.to_string().contains(needle), "{err}"),
    }
}

#[test]
fn stale_commit_landing_in_a_collected_epoch_is_not_acknowledged() {
    // Audit finding B: GC frees interior keys of old epochs.
    let store = FaultyStore::new();
    let dir = TempDir::new();
    let (a, stalled) = stall_writer_a(&store, &dir, "log/", |a| a.exec("INSERT INTO t VALUES (2)"));
    let b = open_db(&config(&store, "b"), &dir.db("b.db")).unwrap();
    b.exec("INSERT INTO t VALUES (10)").unwrap();
    b.exec("INSERT INTO t VALUES (11)").unwrap();
    b.checkpoint();
    assert_err_contains(stalled.join().unwrap(), "fenced");
    assert!(a.storage.info().poisoned.is_some());
    b.storage.release();
    let fresh = open_db(&config(&store, "c"), &dir.db("c.db")).unwrap();
    assert_eq!(fresh.int("SELECT count(*) FROM t"), 3);
}

#[test]
fn lost_answer_on_lease_renewal_keeps_the_lease() {
    let store = FaultyStore::new();
    let dir = TempDir::new();
    let mut cfg = config(&store, "a");
    cfg.lease_ttl = Duration::from_secs(1);
    let a = open_db(&cfg, &dir.db("a.db")).unwrap();
    a.exec("CREATE TABLE t(id INTEGER PRIMARY KEY)").unwrap();
    store.inject(Op::Put, "lease.json", Fault::FailAfter, 1);
    std::thread::sleep(Duration::from_millis(1500));
    a.exec("INSERT INTO t VALUES (1)").unwrap();
    assert!(a.storage.info().poisoned.is_none());
}

#[test]
fn lost_answer_on_manifest_is_adopted() {
    let store = FaultyStore::new();
    let dir = TempDir::new();
    let db = open_db(&config(&store, "a"), &dir.db("a.db")).unwrap();
    db.exec("CREATE TABLE t(id INTEGER PRIMARY KEY)").unwrap();
    store.inject(Op::Put, MANIFEST_KEY, Fault::FailAfter, 1);
    db.checkpoint();
    db.exec("INSERT INTO t VALUES (1)").unwrap();
    let info = db.storage.info();
    assert!(info.poisoned.is_none() && !info.snapshot_pending);
    assert_eq!(manifest(&store).epoch.seq, 1);
}

#[test]
fn late_landing_manifest_is_adopted() {
    // ReplicaFence's late rotation: our own manifest lands after we gave up.
    let store = FaultyStore::new();
    let dir = TempDir::new();
    let db = open_db(&config(&store, "a"), &dir.db("a.db")).unwrap();
    db.exec("CREATE TABLE t(id INTEGER PRIMARY KEY)").unwrap();
    store.inject(
        Op::Put,
        MANIFEST_KEY,
        Fault::LandLater(Duration::from_millis(300)),
        1,
    );
    db.checkpoint();
    std::thread::sleep(Duration::from_millis(500));
    db.exec("INSERT INTO t VALUES (1)").unwrap();
    assert!(db.storage.info().poisoned.is_none());
    db.crash();
    let restored = open_db(&config(&store, "a"), &dir.db("b.db")).unwrap();
    assert_eq!(restored.int("SELECT count(*) FROM t"), 1);
}

#[test]
fn restore_rejects_a_corrupt_snapshot() {
    let store = FaultyStore::new();
    let dir = TempDir::new();
    crashed_db_with_rows(&store, &dir, 1);
    let remote = Remote::new(store.clone(), PREFIX);
    let key = manifest(&store).snapshot;
    let mut bytes = remote.get(&key).unwrap().unwrap().bytes.to_vec();
    bytes[100] ^= 1;
    remote.put(&key, bytes.into(), Put::Overwrite).unwrap();
    assert!(matches!(
        open_db(&config(&store, "a"), &dir.db("b.db")),
        Err(S3Error::Corrupt(_))
    ));
}

#[test]
fn restore_rejects_an_undecodable_snapshot_and_cleans_up() {
    let store = FaultyStore::new();
    let dir = TempDir::new();
    crashed_db_with_rows(&store, &dir, 1);
    let remote = Remote::new(store.clone(), PREFIX);
    let key = manifest(&store).snapshot;
    assert!(manifest(&store).snapshot_zstd);
    let mut bytes = remote.get(&key).unwrap().unwrap().bytes.to_vec();
    // Past the frame header: the zstd decoder fails, not just the CRC.
    for b in &mut bytes[12..] {
        *b ^= 0x5a;
    }
    remote.put(&key, bytes.into(), Put::Overwrite).unwrap();
    let path = dir.db("b.db");
    match open_db(&config(&store, "a"), &path) {
        Err(S3Error::Corrupt(msg)) => assert!(msg.contains(&key), "{msg}"),
        Err(other) => panic!("expected Corrupt, got {other:?}"),
        Ok(_) => panic!("expected Corrupt, the restore succeeded"),
    }
    let leftovers: Vec<_> = std::fs::read_dir(path.parent().unwrap())
        .unwrap()
        .map(|e| e.unwrap().file_name().into_string().unwrap())
        .filter(|name| name.contains("s3-restore"))
        .collect();
    assert!(leftovers.is_empty(), "{leftovers:?}");
}

#[test]
fn restore_interrupted_mid_download_cleans_up_and_retries() {
    let store = FaultyStore::new();
    let dir = TempDir::new();
    crashed_db_with_rows(&store, &dir, 3);
    let path = dir.db("b.db");
    store.inject(Op::Get, "snapshots/", Fault::FailMidStream, 1);
    match open_db(&config(&store, "a"), &path) {
        Err(S3Error::Store(err)) => assert!(err.to_string().contains("injected"), "{err}"),
        Err(other) => panic!("expected a store error, got {other:?}"),
        Ok(_) => panic!("expected a store error, the restore succeeded"),
    }
    let leftovers: Vec<_> = std::fs::read_dir(path.parent().unwrap())
        .unwrap()
        .map(|e| e.unwrap().file_name().into_string().unwrap())
        .filter(|name| name.contains("s3-restore"))
        .collect();
    assert!(leftovers.is_empty(), "{leftovers:?}");
    let db = open_db(&config(&store, "a"), &path).unwrap();
    assert_eq!(db.int("SELECT count(*) FROM t"), 3);
}

#[test]
fn prefixes_with_unusual_characters_restore() {
    for prefix in [
        "tenant#1/db",
        "x//y",
        "100%/db",
        "space here/ü",
        "a+b=c&d",
        "[x]{y}",
    ] {
        let store = FaultyStore::new();
        let dir = TempDir::new();
        let mut cfg = config(&store, "a");
        cfg.prefix = prefix.into();
        let db = open_db(&cfg, &dir.db("a.db")).unwrap();
        db.exec("CREATE TABLE t(id INTEGER PRIMARY KEY)").unwrap();
        db.exec("INSERT INTO t VALUES (1)").unwrap();
        db.checkpoint();
        db.exec("INSERT INTO t VALUES (2)").unwrap();
        db.crash();
        let restored =
            open_db(&cfg, &dir.db("b.db")).unwrap_or_else(|e| panic!("prefix {prefix:?}: {e:?}"));
        assert_eq!(
            restored.int("SELECT count(*) FROM t"),
            2,
            "prefix {prefix:?}"
        );
    }
}

#[test]
fn snapshot_failure_blocks_commits_until_it_succeeds() {
    let store = FaultyStore::new();
    let dir = TempDir::new();
    let db = open_db(&config(&store, "a"), &dir.db("a.db")).unwrap();
    db.exec("CREATE TABLE t(id INTEGER PRIMARY KEY)").unwrap();
    db.exec("INSERT INTO t VALUES (1)").unwrap();
    let old = manifest(&store);
    let old_segments = segments(&store);

    store.inject(Op::Put, "snapshots/", Fault::Fail, 2);
    db.checkpoint();
    assert_eq!(manifest(&store), old, "manifest untouched");
    assert_eq!(
        segments(&store)[..old_segments.len()],
        old_segments[..],
        "no GC before the manifest"
    );
    assert!(db.storage.info().snapshot_pending);

    // The retry inside the next commit fails too, so the commit fails.
    assert_err_contains(db.exec("INSERT INTO t VALUES (2)"), "injected fault");
    // Now it succeeds: snapshot, manifest, then the frame.
    db.exec("INSERT INTO t VALUES (3)").unwrap();
    let m = manifest(&store);
    assert_eq!(m.epoch.seq, old.epoch.seq + 1);
    assert!(!db.storage.info().snapshot_pending);
    assert_eq!(segments(&store), vec![m.epoch.segment_key(0)]);
    db.crash();

    let restored = open_db(&config(&store, "a"), &dir.db("b.db")).unwrap();
    assert_eq!(
        restored.rows("SELECT id FROM t ORDER BY id"),
        vec![vec![Value::from_i64(1)], vec![Value::from_i64(3)]]
    );
}

#[test]
fn manifest_failure_keeps_old_epoch_restorable() {
    let store = FaultyStore::new();
    let dir = TempDir::new();
    let db = open_db(&config(&store, "a"), &dir.db("a.db")).unwrap();
    db.exec("CREATE TABLE t(id INTEGER PRIMARY KEY)").unwrap();
    db.exec("INSERT INTO t VALUES (1)").unwrap();
    let old = manifest(&store);
    store.inject(Op::Put, MANIFEST_KEY, Fault::Fail, 1);
    db.checkpoint();
    assert_eq!(manifest(&store), old);
    assert!(!segments(&store).is_empty(), "old epoch not collected");
    db.crash();

    // Crash before the retry: the old manifest still restores everything.
    let restored = open_db(&config(&store, "a"), &dir.db("b.db")).unwrap();
    assert_eq!(restored.int("SELECT count(*) FROM t"), 1);
}

#[test]
fn second_writer_is_refused_while_the_lease_is_held() {
    let store = FaultyStore::new();
    let dir = TempDir::new();
    let _a = open_db(&config(&store, "a"), &dir.db("a.db")).unwrap();
    match open_db(&config(&store, "b"), &dir.db("b.db")) {
        Err(S3Error::LeaseHeld { owner, .. }) => assert_eq!(owner, "a"),
        Err(other) => panic!("unexpected error {other}"),
        Ok(_) => panic!("second writer must be refused"),
    }
}

#[test]
fn lease_is_released_on_close() {
    let store = FaultyStore::new();
    let dir = TempDir::new();
    let a = open_db(&config(&store, "a"), &dir.db("a.db")).unwrap();
    a.exec("CREATE TABLE t(id INTEGER PRIMARY KEY)").unwrap();
    a.exec("INSERT INTO t VALUES (1)").unwrap();
    a.storage.release();
    assert_err_contains(a.exec("INSERT INTO t VALUES (2)"), "released");
    drop(a);
    let b = open_db(&config(&store, "b"), &dir.db("b.db")).unwrap();
    assert_eq!(b.int("SELECT count(*) FROM t"), 1);
}

#[test]
fn stale_writer_is_fenced_after_takeover() {
    let store = FaultyStore::new();
    let dir = TempDir::new();
    let mut cfg = config(&store, "a");
    cfg.lease_ttl = Duration::from_secs(1);
    let a = open_db(&cfg, &dir.db("a.db")).unwrap();
    a.exec("CREATE TABLE t(id INTEGER PRIMARY KEY)").unwrap();
    // "a" stalls: its lease can't be renewed and expires.
    store.inject(Op::Put, "lease.json", Fault::Fail, 1000);
    std::thread::sleep(Duration::from_millis(1200));
    store.clear_faults();

    let b = open_db(&config(&store, "b"), &dir.db("b.db")).unwrap();
    b.exec("INSERT INTO t VALUES (1)").unwrap();

    assert_err_contains(a.exec("INSERT INTO t VALUES (2)"), "fenced");
    assert!(a.storage.info().poisoned.is_some());
    assert_err_contains(a.exec("INSERT INTO t VALUES (3)"), "fenced");
    assert_eq!(b.int("SELECT count(*) FROM t"), 1);
}

#[test]
fn log_collision_with_another_writer_fences() {
    let store = FaultyStore::new();
    let dir = TempDir::new();
    let a = open_db(&config(&store, "a"), &dir.db("a.db")).unwrap();
    a.exec("CREATE TABLE t(id INTEGER PRIMARY KEY)").unwrap();
    // Someone else wrote the next offset behind our back.
    let epoch = manifest(&store).epoch;
    let next = a.storage.info().log_offset;
    Remote::new(store.clone(), PREFIX)
        .put(&epoch.segment_key(next), "intruder".into(), Put::Create)
        .unwrap();
    assert_err_contains(a.exec("INSERT INTO t VALUES (1)"), "another writer");
    assert_err_contains(a.exec("INSERT INTO t VALUES (2)"), "fenced");
}

fn crashed_db_with_rows(store: &Arc<FaultyStore>, dir: &TempDir, rows: usize) -> Epoch {
    let db = open_db(&config(store, "a"), &dir.db("a.db")).unwrap();
    db.exec("CREATE TABLE t(id INTEGER PRIMARY KEY)").unwrap();
    for i in 0..rows {
        db.exec(&format!("INSERT INTO t VALUES ({i})")).unwrap();
    }
    db.crash();
    manifest(store).epoch
}

#[test]
fn restore_rejects_a_gap_in_the_log() {
    let store = FaultyStore::new();
    let dir = TempDir::new();
    crashed_db_with_rows(&store, &dir, 3);
    let segs = segments(&store);
    Remote::new(store.clone(), PREFIX).delete(&segs[2]).unwrap();
    match open_db(&config(&store, "a"), &dir.db("b.db")) {
        Err(S3Error::Corrupt(msg)) => assert!(msg.contains("gap"), "{msg}"),
        Err(other) => panic!("unexpected error {other}"),
        Ok(_) => panic!("restore must fail"),
    }
}

#[test]
fn restore_rejects_a_broken_crc_chain() {
    let store = FaultyStore::new();
    let dir = TempDir::new();
    crashed_db_with_rows(&store, &dir, 3);
    let remote = Remote::new(store.clone(), PREFIX);
    let segs = segments(&store);
    let mut bytes = remote.get(&segs[2]).unwrap().unwrap().bytes.to_vec();
    bytes[30] ^= 0xff;
    remote.put(&segs[2], bytes.into(), Put::Overwrite).unwrap();
    match open_db(&config(&store, "a"), &dir.db("b.db")) {
        Err(S3Error::Corrupt(msg)) => assert!(msg.contains("crc"), "{msg}"),
        Err(other) => panic!("unexpected error {other}"),
        Ok(_) => panic!("restore must fail"),
    }
}

#[test]
fn restore_rejects_a_frame_spliced_from_another_chain() {
    // Same offsets, different history: a valid frame whose CRC chain starts
    // from a different predecessor.
    let store = FaultyStore::new();
    let other = FaultyStore::new();
    let dir = TempDir::new();
    crashed_db_with_rows(&store, &dir, 3);
    let other_dir = TempDir::new();
    let db = open_db(&config(&other, "a"), &other_dir.db("x.db")).unwrap();
    db.exec("CREATE TABLE t(id INTEGER PRIMARY KEY)").unwrap();
    for i in 10..13 {
        db.exec(&format!("INSERT INTO t VALUES ({i})")).unwrap();
    }
    let theirs = segments(&other);
    let ours = segments(&store);
    let foreign = Remote::new(other.clone(), PREFIX)
        .get(&theirs[3])
        .unwrap()
        .unwrap();
    assert_eq!(
        theirs[3].rsplit('/').next(),
        ours[3].rsplit('/').next(),
        "same offset"
    );
    Remote::new(store.clone(), PREFIX)
        .put(&ours[3], foreign.bytes, Put::Overwrite)
        .unwrap();
    assert!(matches!(
        open_db(&config(&store, "a"), &dir.db("b.db")),
        Err(S3Error::Corrupt(_))
    ));
}

#[test]
fn transient_list_failure_fails_open_cleanly() {
    let store = FaultyStore::new();
    let dir = TempDir::new();
    crashed_db_with_rows(&store, &dir, 2);
    store.inject(Op::List, "log/", Fault::Fail, 1);
    assert!(matches!(
        open_db(&config(&store, "a"), &dir.db("b.db")),
        Err(S3Error::Store(_))
    ));
    let db = open_db(&config(&store, "a"), &dir.db("c.db")).unwrap();
    assert_eq!(db.int("SELECT count(*) FROM t"), 2);
}

#[test]
fn many_checkpoints_and_restores_keep_all_data() {
    let store = FaultyStore::new();
    let dir = TempDir::new();
    let mut expected = 0;
    for round in 0..4 {
        let db = open_db(&config(&store, "a"), &dir.db(&format!("r{round}.db"))).unwrap();
        if round == 0 {
            db.exec("CREATE TABLE t(id INTEGER PRIMARY KEY, v BLOB)")
                .unwrap();
        }
        assert_eq!(db.int("SELECT count(*) FROM t"), expected);
        for i in 0..25 {
            db.exec(&format!(
                "INSERT INTO t VALUES ({}, randomblob(200))",
                round * 100 + i
            ))
            .unwrap();
            expected += 1;
            if i % 10 == 9 {
                db.checkpoint();
            }
        }
        db.crash();
    }
    let db = open_db(&config(&store, "a"), &dir.db("final.db")).unwrap();
    assert_eq!(db.int("SELECT count(*) FROM t"), expected);
    assert_eq!(keys(&store, "snapshots/").len(), 1);
}

#[test]
fn automatic_checkpoint_threshold_rolls_epochs() {
    let store = FaultyStore::new();
    let dir = TempDir::new();
    let mut cfg = config(&store, "a");
    cfg.checkpoint_threshold = Some(4096);
    let db = open_db(&cfg, &dir.db("a.db")).unwrap();
    db.exec("CREATE TABLE t(id INTEGER PRIMARY KEY, v BLOB)")
        .unwrap();
    for i in 0..40 {
        db.exec(&format!("INSERT INTO t VALUES ({i}, randomblob(500))"))
            .unwrap();
    }
    assert!(manifest(&store).epoch.seq >= 2);
    db.crash();
    let restored = open_db(&cfg, &dir.db("b.db")).unwrap();
    assert_eq!(restored.int("SELECT count(*) FROM t"), 40);
}

/// One writer never fences itself: commits from several connections and
/// checkpoints every few ms (each publishing a snapshot and a manifest in
/// the background) all write the manifest through the writer state, one at
/// a time, against the one version it holds.
#[test]
fn a_busy_writer_with_constant_checkpoints_never_fences_itself() {
    for async_durability in [false, true] {
        let store = FaultyStore::new();
        let dir = TempDir::new();
        let mut cfg = config(&store, "a");
        cfg.async_durability = async_durability;
        cfg.checkpoint_threshold = Some(4096);
        let db = open_db(&cfg, &dir.db("a.db")).unwrap();
        db.exec("CREATE TABLE t(id INTEGER PRIMARY KEY, v BLOB)")
            .unwrap();
        let first = manifest(&store).epoch.seq;
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let checkpointer = {
            let conn = db._db.connect().unwrap();
            let stop = stop.clone();
            std::thread::spawn(move || {
                while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                    let _ = conn.execute("PRAGMA wal_checkpoint(TRUNCATE)");
                    std::thread::sleep(Duration::from_millis(5));
                }
            })
        };
        let writers: Vec<_> = (0..3)
            .map(|w| {
                let conn = db._db.connect().unwrap();
                std::thread::spawn(move || {
                    let mut done = 0;
                    for i in 0..60 {
                        loop {
                            let sql =
                                format!("INSERT INTO t VALUES ({}, randomblob(300))", w * 1000 + i);
                            match conn.execute(&sql) {
                                Ok(_) => break done += 1,
                                Err(err) => {
                                    let msg = err.to_string();
                                    assert!(
                                        msg.contains("usy") || msg.contains("onflict"),
                                        "{msg}"
                                    );
                                    std::thread::sleep(Duration::from_millis(1));
                                }
                            }
                        }
                    }
                    done
                })
            })
            .collect();
        let rows: i64 = writers.into_iter().map(|h| h.join().unwrap()).sum();
        stop.store(true, std::sync::atomic::Ordering::Relaxed);
        checkpointer.join().unwrap();
        assert_eq!(db.storage.info().poisoned, None);
        db.storage.flush(Duration::from_secs(10)).unwrap();
        db.storage.wait_background(Duration::MAX);
        assert!(
            manifest(&store).epoch.seq >= first + 3,
            "checkpoints rolled epochs"
        );
        db.crash();
        let restored = open_db(&cfg, &dir.db("b.db")).unwrap();
        assert_eq!(restored.int("SELECT count(*) FROM t"), rows);
    }
}

#[test]
fn background_renewal_keeps_the_lease_past_its_ttl() {
    let store = FaultyStore::new();
    let dir = TempDir::new();
    let mut cfg = config(&store, "a");
    cfg.lease_ttl = Duration::from_secs(1);
    let a = open_db(&cfg, &dir.db("a.db")).unwrap();
    a.exec("CREATE TABLE t(id INTEGER PRIMARY KEY)").unwrap();
    let first_expiry = a.storage.info().lease_expires_at_ms;
    std::thread::sleep(Duration::from_millis(2500));
    assert!(a.storage.info().lease_expires_at_ms > first_expiry + 1000);
    assert!(matches!(
        open_db(&config(&store, "b"), &dir.db("b.db")),
        Err(S3Error::LeaseHeld { .. })
    ));
    a.exec("INSERT INTO t VALUES (1)").unwrap();
}

/// Opens "a" with a 1s lease, lets `stall` start on another thread with
/// `delayed` PUTs held in flight for 2.5s, and makes the lease lapse
/// meanwhile. Returns once "a" has certainly lost the lease.
fn stall_writer_a<T: Send + 'static>(
    store: &Arc<FaultyStore>,
    dir: &TempDir,
    delayed: &str,
    stall: impl FnOnce(&Db) -> T + Send + 'static,
) -> (Arc<Db>, std::thread::JoinHandle<T>) {
    let mut cfg = config(store, "a");
    cfg.lease_ttl = Duration::from_secs(2);
    let a = Arc::new(open_db(&cfg, &dir.db("a.db")).unwrap());
    a.exec("CREATE TABLE t(id INTEGER PRIMARY KEY)").unwrap();
    a.exec("INSERT INTO t VALUES (1)").unwrap();
    // A fresh lease, so the stalled operation passes its lease check and
    // really reaches the delayed PUT.
    a.storage.renew_lease_for_test();
    store.inject(Op::Put, "lease.json", Fault::Fail, 1000);
    store.inject(
        Op::Put,
        delayed,
        Fault::Delay(Duration::from_millis(4000)),
        1,
    );
    let handle = {
        let a = a.clone();
        std::thread::spawn(move || stall(&a))
    };
    std::thread::sleep(Duration::from_millis(2300));
    store.clear_faults();
    (a, handle)
}

#[test]
fn delayed_commit_of_a_stale_writer_cannot_land_after_takeover() {
    let store = FaultyStore::new();
    let dir = TempDir::new();
    let (a, stalled) = stall_writer_a(&store, &dir, "log/", |a| a.exec("INSERT INTO t VALUES (2)"));
    let b = open_db(&config(&store, "b"), &dir.db("b.db")).unwrap();
    // B already writes to an epoch of its own; the stale PUT lands in a dead one.
    b.checkpoint();
    for id in 10..15 {
        b.exec(&format!("INSERT INTO t VALUES ({id})")).unwrap();
    }
    assert_err_contains(stalled.join().unwrap(), "fenced");
    assert!(a.storage.info().poisoned.is_some());
    b.storage.release();

    let fresh = open_db(&config(&store, "c"), &dir.db("c.db")).unwrap();
    let ids: Vec<String> = fresh
        .rows("SELECT id FROM t ORDER BY id")
        .into_iter()
        .map(|row| row[0].to_string())
        .collect();
    assert_eq!(ids, ["1", "10", "11", "12", "13", "14"]);
}

#[test]
fn delayed_manifest_of_a_stale_writer_is_rejected() {
    let store = FaultyStore::new();
    let dir = TempDir::new();
    let (a, stalled) = stall_writer_a(&store, &dir, MANIFEST_KEY, |a| a.checkpoint());
    // "a"'s manifest is in flight (or "a" is still stalled on its lease):
    // "b" takes the manifest over and moves to an epoch of its own.
    let b = open_db(&config(&store, "b"), &dir.db("b.db")).unwrap();
    for id in 10..15 {
        b.exec(&format!("INSERT INTO t VALUES ({id})")).unwrap();
    }
    stalled.join().unwrap();
    assert_err_contains(a.exec("INSERT INTO t VALUES (3)"), "fenced");
    assert_eq!(manifest(&store).writer, "b");
    b.storage.release();

    let fresh = open_db(&config(&store, "c"), &dir.db("c.db")).unwrap();
    assert_eq!(fresh.int("SELECT count(*) FROM t"), 6);
}

#[test]
fn restore_compacts_a_sealed_epoch_into_a_new_one() {
    let store = FaultyStore::new();
    let dir = TempDir::new();
    let db = open_db(&config(&store, "a"), &dir.db("a.db")).unwrap();
    db.exec("CREATE TABLE t(id INTEGER PRIMARY KEY)").unwrap();
    db.exec("INSERT INTO t VALUES (1)").unwrap();
    let old = manifest(&store);
    store.inject(Op::Put, MANIFEST_KEY, Fault::Fail, 1);
    db.checkpoint();
    db.crash();

    let restored = open_db(&config(&store, "a"), &dir.db("b.db")).unwrap();
    let m = manifest(&store);
    assert_eq!(m.epoch.seq, old.epoch.seq + 1);
    restored.exec("INSERT INTO t VALUES (2)").unwrap();
    assert_eq!(restored.int("SELECT count(*) FROM t"), 2);
    restored.crash();
    let again = open_db(&config(&store, "a"), &dir.db("c.db")).unwrap();
    assert_eq!(again.int("SELECT count(*) FROM t"), 2);
}

#[test]
fn an_expired_lease_is_never_renewed() {
    let store = FaultyStore::new();
    let dir = TempDir::new();
    let mut cfg = config(&store, "a");
    cfg.lease_ttl = Duration::from_secs(1);
    let a = open_db(&cfg, &dir.db("a.db")).unwrap();
    a.exec("CREATE TABLE t(id INTEGER PRIMARY KEY)").unwrap();
    store.inject(Op::Put, "lease.json", Fault::Fail, 1000);
    std::thread::sleep(Duration::from_millis(1200));
    store.clear_faults();
    // Give the renewer several chances to (wrongly) resurrect the lease.
    std::thread::sleep(Duration::from_millis(800));
    assert!(open_db(&config(&store, "b"), &dir.db("b.db")).is_ok());
    assert_err_contains(a.exec("INSERT INTO t VALUES (1)"), "fenced");
}

#[test]
fn orphan_is_not_rewritten_after_a_new_owner_restored_it() {
    // A's failed commit lands late; B restores and extends it;
    // A's retry at that offset must not replace it.
    let store = FaultyStore::new();
    let dir = TempDir::new();
    let mut cfg = config(&store, "a");
    cfg.lease_ttl = Duration::from_secs(2);
    let a = Arc::new(open_db(&cfg, &dir.db("a.db")).unwrap());
    a.exec("CREATE TABLE t(id INTEGER PRIMARY KEY)").unwrap();
    store.inject(
        Op::Put,
        "log/",
        Fault::LandLater(Duration::from_millis(100)),
        1,
    );
    assert!(a.exec("INSERT INTO t VALUES (1)").is_err());
    std::thread::sleep(Duration::from_millis(200));
    a.storage.renew_lease_for_test();
    store.inject(Op::Put, "lease.json", Fault::Fail, 1000);
    store.inject(
        Op::Put,
        "log/",
        Fault::Delay(Duration::from_millis(4000)),
        1,
    );
    let retry = {
        let a = a.clone();
        std::thread::spawn(move || a.exec("INSERT INTO t VALUES (2)"))
    };
    std::thread::sleep(Duration::from_millis(2300));
    store.clear_faults();
    let b = open_db(&config(&store, "b"), &dir.db("b.db")).unwrap();
    b.exec("INSERT INTO t VALUES (3)").unwrap();
    assert!(retry.join().unwrap().is_err());
    b.storage.release();

    let fresh = open_db(&config(&store, "c"), &dir.db("c.db")).unwrap();
    let ids: Vec<String> = fresh
        .rows("SELECT id FROM t ORDER BY id")
        .into_iter()
        .map(|row| row[0].to_string())
        .collect();
    assert_eq!(ids, ["1", "3"]);
}

#[test]
fn every_failed_attempt_at_an_offset_is_remembered() {
    // Found by S3Fence.tla (Sole): a landed frame whose confirm failed, then a
    // failed retry, must not make the writer take itself for another one.
    let store = FaultyStore::new();
    let dir = TempDir::new();
    let db = open_db(&config(&store, "a"), &dir.db("a.db")).unwrap();
    db.exec("CREATE TABLE t(id INTEGER PRIMARY KEY)").unwrap();
    store.inject(Op::Get, MANIFEST_KEY, Fault::Fail, 1);
    assert!(db.exec("INSERT INTO t VALUES (1)").is_err());
    store.inject(Op::Put, "log/", Fault::Fail, 1);
    assert!(db.exec("INSERT INTO t VALUES (2)").is_err());
    assert_err_contains(
        db.exec("INSERT INTO t VALUES (3)"),
        "reported failure reached S3",
    );
}

fn open_plain(path: &Path) -> Db2 {
    let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
    let db = Database::open(
        io,
        path.to_str().unwrap(),
        OpenOptions::new(Arc::new(SqliteDialect)),
    )
    .unwrap();
    let conn = db.connect().unwrap();
    Db2 { _db: db, conn }
}

/// A local database without S3.
struct Db2 {
    _db: Arc<Database>,
    conn: Arc<Connection>,
}

impl Db2 {
    fn ids(&self) -> Vec<i64> {
        let mut stmt = self
            .conn
            .query("SELECT id FROM t ORDER BY id")
            .unwrap()
            .unwrap();
        stmt.run_collect_rows()
            .unwrap()
            .into_iter()
            .map(|row| row[0].to_string().parse().unwrap())
            .collect()
    }
}

fn restored_ids(cfg: &S3Config, dir: &TempDir, name: &str, target: Target) -> Vec<i64> {
    let path = dir.db(name);
    super::restore_to(cfg, &path, target).unwrap();
    open_plain(&path).ids()
}

#[test]
fn point_in_time_restore_over_retained_epochs() {
    let store = FaultyStore::new();
    let dir = TempDir::new();
    let mut cfg = config(&store, "a");
    cfg.retain_epochs = 3;
    let db = open_db(&cfg, &dir.db("a.db")).unwrap();
    let pause = || std::thread::sleep(Duration::from_millis(30));
    db.exec("CREATE TABLE t(id INTEGER PRIMARY KEY)").unwrap();
    db.exec("INSERT INTO t VALUES (1)").unwrap();
    db.exec("INSERT INTO t VALUES (2)").unwrap();
    pause();
    let t1 = super::layout::now_ms();
    pause();
    db.exec("INSERT INTO t VALUES (3)").unwrap();
    db.checkpoint();
    db.exec("INSERT INTO t VALUES (4)").unwrap();
    pause();
    let t2 = super::layout::now_ms();
    pause();
    db.exec("INSERT INTO t VALUES (5)").unwrap();
    let first_epoch = manifest(&store).history.last().unwrap().epoch.seq;

    assert_eq!(restored_ids(&cfg, &dir, "t1.db", Target::Time(t1)), [1, 2]);
    assert_eq!(
        restored_ids(&cfg, &dir, "t2.db", Target::Time(t2)),
        [1, 2, 3, 4]
    );
    assert_eq!(
        restored_ids(&cfg, &dir, "now.db", Target::Latest),
        [1, 2, 3, 4, 5]
    );
    assert_eq!(
        restored_ids(&cfg, &dir, "e.db", Target::Epoch(first_epoch)),
        [1, 2, 3]
    );
    // Restoring never touches the writer.
    db.exec("INSERT INTO t VALUES (6)").unwrap();
}

#[test]
fn point_in_time_restore_rejects_a_gap_before_the_cutoff() {
    let store = FaultyStore::new();
    let dir = TempDir::new();
    let cfg = config(&store, "a");
    let db = open_db(&cfg, &dir.db("a.db")).unwrap();
    db.exec("CREATE TABLE t(id INTEGER PRIMARY KEY)").unwrap();
    for id in 1..=3 {
        db.exec(&format!("INSERT INTO t VALUES ({id})")).unwrap();
    }
    std::thread::sleep(Duration::from_millis(30));
    let cutoff = super::layout::now_ms();

    // Lose the segment of INSERT 2 but keep the later one.
    let epoch = manifest(&store).epoch;
    let log = keys(&store, &epoch.log_dir());
    let remote = Remote::new(store.clone(), PREFIX);
    remote.delete(&log[log.len() - 2]).unwrap();

    match super::restore_to(&cfg, &dir.db("pitr.db"), Target::Time(cutoff)) {
        Err(S3Error::Corrupt(msg)) => assert!(msg.contains("gap"), "{msg}"),
        Err(other) => panic!("unexpected error {other}"),
        Ok(_) => panic!("a restore across a missing segment succeeded"),
    }
    db.crash();
}

#[test]
fn point_in_time_restore_needs_retention() {
    let store = FaultyStore::new();
    let dir = TempDir::new();
    let cfg = config(&store, "a");
    let db = open_db(&cfg, &dir.db("a.db")).unwrap();
    db.exec("CREATE TABLE t(id INTEGER PRIMARY KEY)").unwrap();
    db.checkpoint();
    match super::restore_to(&cfg, &dir.db("old.db"), Target::Epoch(0)) {
        Err(S3Error::Config(msg)) => assert!(msg.contains("retain_epochs"), "{msg}"),
        other => panic!("unexpected {other:?}"),
    }
}

#[test]
fn retention_keeps_the_configured_number_of_epochs() {
    let store = FaultyStore::new();
    let dir = TempDir::new();
    let mut cfg = config(&store, "a");
    cfg.retain_epochs = 2;
    let db = open_db(&cfg, &dir.db("a.db")).unwrap();
    db.exec("CREATE TABLE t(id INTEGER PRIMARY KEY)").unwrap();
    for i in 0..5 {
        db.exec(&format!("INSERT INTO t VALUES ({i})")).unwrap();
        db.checkpoint();
    }
    let m = manifest(&store);
    assert_eq!(m.history.len(), 2);
    let mut expected: Vec<String> = m.retained().map(|r| r.snapshot).collect();
    expected.sort();
    assert_eq!(keys(&store, "snapshots/"), expected);
}

#[test]
fn a_prefix_without_manifest_but_with_objects_is_not_bootstrapped() {
    let store = FaultyStore::new();
    let dir = TempDir::new();
    crashed_db_with_rows(&store, &dir, 1);
    let remote = Remote::new(store.clone(), PREFIX);
    remote.delete(MANIFEST_KEY).unwrap();
    remote.delete("lease.json").unwrap();
    match open_db(&config(&store, "a"), &dir.db("b.db")) {
        Err(S3Error::Corrupt(msg)) => assert!(msg.contains("no manifest.json"), "{msg}"),
        Err(other) => panic!("unexpected error {other}"),
        Ok(_) => panic!("must not bootstrap over old objects"),
    }
}

#[test]
fn a_first_open_that_failed_before_its_manifest_does_not_lock_the_prefix() {
    let store = FaultyStore::new();
    let dir = TempDir::new();
    store.inject(Op::Put, MANIFEST_KEY, Fault::Fail, 1);
    assert!(open_db(&config(&store, "a"), &dir.db("a.db")).is_err());
    let leftover = keys(&store, "snapshots/");
    assert_eq!(leftover.len(), 1, "the bootstrap snapshot stayed behind");

    // The next open (a pool reconnecting) creates the database anyway.
    let db = open_db(&config(&store, "a"), &dir.db("b.db")).unwrap();
    db.exec("CREATE TABLE t(id INTEGER PRIMARY KEY)").unwrap();
    db.exec("INSERT INTO t VALUES (1)").unwrap();
    db.crash();
    let reopened = open_db(&config(&store, "a"), &dir.db("c.db")).unwrap();
    assert_eq!(reopened.int("SELECT count(*) FROM t"), 1);
    // Once the database moved past epoch 0, the leftover is collected.
    assert!(!keys(&store, "snapshots/").contains(&leftover[0]));
}

#[test]
fn a_retried_first_open_replaces_the_empty_database_its_failed_attempt_left() {
    let store = FaultyStore::new();
    let dir = TempDir::new();
    store.inject(Op::Put, MANIFEST_KEY, Fault::Fail, 1);
    assert!(open_db(&config(&store, "a"), &dir.db("a.db")).is_err());
    assert!(dir.db("a.db").exists());
    let db = open_db(&config(&store, "a"), &dir.db("a.db")).unwrap();
    db.exec("CREATE TABLE t(id INTEGER PRIMARY KEY)").unwrap();
}

fn assert_refused(store: &Arc<FaultyStore>, path: &Path) {
    match open_db(&config(store, "a"), path) {
        Err(S3Error::Config(msg)) => {
            assert!(msg.contains("refusing to replace"), "{msg}");
            assert!(msg.contains(&path.display().to_string()), "{msg}");
        }
        Err(other) => panic!("unexpected error {other}"),
        Ok(_) => panic!("must not replace the local database"),
    }
    let remote = Remote::new(store.clone(), PREFIX);
    assert!(remote.get(MANIFEST_KEY).unwrap().is_none());
    assert!(keys(store, "snapshots/").is_empty());
}

#[test]
fn an_empty_prefix_does_not_replace_an_existing_local_database() {
    let store = FaultyStore::new();
    let dir = TempDir::new();
    let path = dir.db("a.db");
    {
        let plain = open_plain(&path);
        plain
            .conn
            .execute("CREATE TABLE t(id INTEGER PRIMARY KEY)")
            .unwrap();
        plain.conn.execute("INSERT INTO t VALUES (1), (2)").unwrap();
        plain.conn.close().unwrap();
    }
    assert_refused(&store, &path);
    assert_eq!(open_plain(&path).ids(), vec![1, 2]);
}

#[test]
fn an_emptied_prefix_does_not_replace_the_local_copy() {
    let dir = TempDir::new();
    let path = dir.db("a.db");
    let db = open_db(&config(&FaultyStore::new(), "a"), &path).unwrap();
    db.exec("CREATE TABLE t(id INTEGER PRIMARY KEY)").unwrap();
    db.exec("INSERT INTO t VALUES (1)").unwrap();
    drop(db);
    // Another bucket, or the prefix cleared: no manifest.
    assert_refused(&FaultyStore::new(), &path);
}

#[test]
fn an_empty_prefix_does_not_replace_a_file_that_is_not_a_database() {
    let store = FaultyStore::new();
    let dir = TempDir::new();
    let path = dir.db("a.db");
    let bytes = vec![7u8; 8192];
    std::fs::write(&path, &bytes).unwrap();
    assert_refused(&store, &path);
    assert_eq!(std::fs::read(&path).unwrap(), bytes);
}

#[test]
fn an_empty_local_file_is_replaced() {
    let store = FaultyStore::new();
    let dir = TempDir::new();
    std::fs::write(dir.db("a.db"), b"").unwrap();
    let db = open_db(&config(&store, "a"), &dir.db("a.db")).unwrap();
    db.exec("CREATE TABLE t(id INTEGER PRIMARY KEY)").unwrap();
}

#[test]
fn a_store_that_stops_answering_fails_the_commit() {
    let store = FaultyStore::new();
    let dir = TempDir::new();
    let mut cfg = config(&store, "a");
    cfg.request_timeout = Duration::from_millis(300);
    cfg.max_retries = 0;
    let db = open_db(&cfg, &dir.db("a.db")).unwrap();
    db.exec("CREATE TABLE t(id INTEGER PRIMARY KEY)").unwrap();
    store.inject(Op::Put, "log/", Fault::Delay(Duration::from_secs(30)), 1);
    let started = std::time::Instant::now();
    assert_err_contains(db.exec("INSERT INTO t VALUES (1)"), "no answer within");
    assert!(started.elapsed() < Duration::from_secs(5));
    assert_eq!(db.int("SELECT count(*) FROM t"), 0);
}

#[test]
fn large_snapshots_use_multipart_upload_and_restore() {
    let store = FaultyStore::new();
    let dir = TempDir::new();
    let db = open_db(&config(&store, "a"), &dir.db("a.db")).unwrap();
    db.exec("CREATE TABLE t(id INTEGER PRIMARY KEY, v BLOB)")
        .unwrap();
    // About 20 MB: above the 16 MB multipart threshold.
    for i in 0..20 {
        db.exec(&format!("INSERT INTO t VALUES ({i}, randomblob(1000000))"))
            .unwrap();
    }
    db.checkpoint();
    let m = manifest(&store);
    assert!(m.snapshot_size > 16 * 1024 * 1024, "{}", m.snapshot_size);
    db.exec("INSERT INTO t VALUES (100, x'00')").unwrap();
    db.crash();

    let restored = open_db(&config(&store, "a"), &dir.db("b.db")).unwrap();
    assert_eq!(restored.int("SELECT count(*) FROM t"), 21);
    assert_eq!(restored.int("SELECT sum(length(v)) FROM t"), 20_000_001);
}

#[test]
fn a_stale_local_file_is_replaced_by_the_s3_state() {
    let store = FaultyStore::new();
    let dir = TempDir::new();
    crashed_db_with_rows(&store, &dir, 3);
    let path = dir.db("stale.db");
    {
        let local = open_plain(&path);
        local
            .conn
            .execute("CREATE TABLE t(id INTEGER PRIMARY KEY)")
            .unwrap();
        local.conn.execute("INSERT INTO t VALUES (999)").unwrap();
    }
    let db = open_db(&config(&store, "a"), &path).unwrap();
    assert_eq!(db.int("SELECT count(*) FROM t"), 3);
    assert_eq!(db.int("SELECT count(*) FROM t WHERE id = 999"), 0);
}

#[test]
fn restoring_before_the_database_existed_is_an_error() {
    let store = FaultyStore::new();
    let dir = TempDir::new();
    let mut cfg = config(&store, "a");
    cfg.retain_epochs = 5;
    let before = super::layout::now_ms() - 60_000;
    let db = open_db(&cfg, &dir.db("a.db")).unwrap();
    db.exec("CREATE TABLE t(id INTEGER PRIMARY KEY)").unwrap();
    match super::restore_to(&cfg, &dir.db("old.db"), Target::Time(before)) {
        Err(S3Error::Config(msg)) => assert!(msg.contains("not retained"), "{msg}"),
        other => panic!("unexpected {other:?}"),
    }
    assert!(!dir.db("old.db").exists());
}

#[test]
fn restore_and_replica_of_an_empty_prefix_fail_clearly() {
    let store = FaultyStore::new();
    let dir = TempDir::new();
    let mut cfg = config(&store, "r");
    match super::restore_to(&cfg, &dir.db("x.db"), Target::Latest) {
        Err(S3Error::Config(msg)) => assert!(msg.contains("no database"), "{msg}"),
        other => panic!("unexpected {other:?}"),
    }
    cfg.replica = true;
    match super::replica::stage(&cfg, &dir.db("y.db")) {
        Err(S3Error::Config(msg)) => assert!(msg.contains("no database"), "{msg}"),
        Err(other) => panic!("unexpected {other}"),
        Ok(_) => panic!("a replica of nothing must fail"),
    }
    // Nothing was created in S3 by either.
    assert!(store.put_log().is_empty());
}

/// Small deterministic RNG (xorshift64*), so a failing seed reproduces.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    fn chance(&mut self, percent: u64) -> bool {
        self.next() % 100 < percent
    }

    fn pick<T: Copy>(&mut self, items: &[T]) -> T {
        items[(self.next() % items.len() as u64) as usize]
    }
}

fn soak(seed: u64) {
    let mut rng = Rng(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1);
    let store = FaultyStore::new();
    let dir = TempDir::new();
    let cfg = config(&store, "soak");
    let mut incarnation = 0;
    let open = |incarnation: &mut u32| {
        for _ in 0..5 {
            *incarnation += 1;
            match open_db(&cfg, &dir.db(&format!("i{incarnation}.db"))) {
                Ok(db) => return db,
                Err(err @ S3Error::Corrupt(_)) => panic!("seed {seed}: restore failed: {err}"),
                // Opening is subject to the same faults; try again.
                Err(_) => store.clear_faults(),
            }
        }
        panic!("seed {seed}: could not reopen");
    };
    let mut db = open(&mut incarnation);
    db.exec("CREATE TABLE IF NOT EXISTS t(id INTEGER PRIMARY KEY)")
        .unwrap();
    let mut acked = Vec::new();
    let mut attempted = Vec::new();
    let faults = [
        Fault::Fail,
        Fault::FailAfter,
        Fault::LandLater(Duration::from_millis(15)),
        Fault::Delay(Duration::from_millis(5)),
    ];
    for id in 1..=60i64 {
        if rng.chance(20) {
            let key = rng.pick(&["log/", "log/", "snapshots/", MANIFEST_KEY]);
            store.inject(Op::Put, key, rng.pick(&faults), 1);
        }
        if rng.chance(5) {
            store.inject(Op::Get, MANIFEST_KEY, Fault::Fail, 1);
        }
        attempted.push(id);
        if db.exec(&format!("INSERT INTO t VALUES ({id})")).is_ok() {
            acked.push(id);
        }
        if rng.chance(10) {
            let _ = db.exec("PRAGMA wal_checkpoint(TRUNCATE)");
        }
        let poisoned = db.storage.info().poisoned.is_some();
        if poisoned || rng.chance(7) {
            db.crash();
            std::thread::sleep(Duration::from_millis(20));
            db = open(&mut incarnation);
        }
    }
    db.crash();
    store.clear_faults();
    std::thread::sleep(Duration::from_millis(50));
    let restored = open(&mut incarnation);
    let ids: Vec<i64> = restored
        .rows("SELECT id FROM t ORDER BY id")
        .into_iter()
        .map(|row| row[0].to_string().parse().unwrap())
        .collect();
    for id in &acked {
        assert!(
            ids.contains(id),
            "seed {seed}: acknowledged {id} lost ({ids:?})"
        );
    }
    for id in &ids {
        assert!(attempted.contains(id), "seed {seed}: phantom row {id}");
    }
}

#[test]
fn randomized_faults_never_lose_acknowledged_commits() {
    let seeds: u64 = std::env::var("S3_SOAK_SEEDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(4);
    for seed in 1..=seeds {
        soak(seed);
    }
}

#[test]
fn a_provider_ignoring_conditional_writes_is_refused_at_open() {
    let store = FaultyStore::new();
    store.ignore_conditions();
    let dir = TempDir::new();
    let mut cfg = config(&store, "a");
    cfg.bucket = "ignores-conditions".into();
    match open_db(&cfg, &dir.db("a.db")) {
        Err(S3Error::Config(msg)) => assert!(msg.contains("If-None-Match"), "{msg}"),
        Err(other) => panic!("unexpected error {other}"),
        Ok(_) => panic!("must refuse a store that ignores conditions"),
    }
    assert!(
        keys(&store, "").is_empty(),
        "the probe cleans up and writes nothing else"
    );

    // Explicitly trusted: opens (at the user's risk).
    cfg.verify_conditional_writes = false;
    assert!(open_db(&cfg, &dir.db("b.db")).is_ok());
}

#[test]
fn a_compliant_provider_passes_the_probe() {
    let store = FaultyStore::new();
    let dir = TempDir::new();
    let mut cfg = config(&store, "a");
    cfg.bucket = "compliant".into();
    open_db(&cfg, &dir.db("a.db")).unwrap();
    assert!(store.put_log().iter().any(|k| k.contains("/probe/")));
    assert!(keys(&store, "probe/").is_empty());
}

/// Diagnosis of slow AUTOINCREMENT inserts in MVCC (not an S3 test): per-batch insert time into an
/// AUTOINCREMENT table vs a plain one within one MVCC transaction.
#[test]
#[ignore]
fn autoincrement_mvcc_probe() {
    let dir = TempDir::new();
    for (name, ddl, insert) in [
        ("rowid", "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)", "INSERT INTO t(v) VALUES ('x')"),
        ("autoinc", "CREATE TABLE t(id INTEGER PRIMARY KEY AUTOINCREMENT, v TEXT)", "INSERT INTO t(v) VALUES ('x')"),
        (
            "counter-row",
            "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); CREATE TABLE c(k INTEGER PRIMARY KEY, n INTEGER); INSERT INTO c VALUES (1, 0)",
            "UPDATE c SET n = n + 1 WHERE k = 1",
        ),
    ] {
        let db = open_plain(&dir.db(&format!("{name}.db")));
        db.conn.execute("PRAGMA journal_mode = 'mvcc'").unwrap();
        db.conn.execute(ddl).unwrap();
        db.conn.execute("BEGIN").unwrap();
        let mut line = String::new();
        for _ in 0..6 {
            let t = std::time::Instant::now();
            for _ in 0..500 {
                db.conn.execute(insert).unwrap();
            }
            line.push_str(&format!(" {:?}", t.elapsed()));
        }
        db.conn.execute("COMMIT").unwrap();
        eprintln!("probe {name}:{line}");
    }
}

#[test]
fn the_probe_runs_once_per_store_per_vm() {
    let store = FaultyStore::new();
    let dir = TempDir::new();
    let mut cfg = config(&store, "a");
    cfg.bucket = "probe-once".into();
    let probes = || {
        store
            .put_log()
            .iter()
            .filter(|k| k.contains("/probe/"))
            .count()
    };
    open_db(&cfg, &dir.db("a.db")).unwrap().storage.release();
    let first = probes();
    assert_eq!(first, 4);
    open_db(&cfg, &dir.db("b.db")).unwrap();
    assert_eq!(
        probes(),
        first,
        "a second open of the same store doesn't probe again"
    );
}

#[test]
fn a_commit_waiting_on_s3_gives_up_when_cancelled() {
    use std::sync::atomic::{AtomicBool, Ordering};
    let store = FaultyStore::new();
    let dir = TempDir::new();
    let db = Arc::new(open_db(&config(&store, "a"), &dir.db("a.db")).unwrap());
    db.exec("CREATE TABLE t(id INTEGER PRIMARY KEY)").unwrap();
    store.inject(Op::Put, "log/", Fault::Delay(Duration::from_secs(5)), 1);
    let flag = Arc::new(AtomicBool::new(false));
    let started = std::time::Instant::now();
    let commit = {
        let (db, flag) = (db.clone(), flag.clone());
        std::thread::spawn(move || {
            super::remote::with_cancel(&flag, || db.exec("INSERT INTO t VALUES (1)"))
        })
    };
    std::thread::sleep(Duration::from_millis(200));
    flag.store(true, Ordering::SeqCst);
    assert_err_contains(commit.join().unwrap(), "cancelled");
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "{:?}",
        started.elapsed()
    );
    // The writer goes on (the delayed PUT may still land: an orphan).
    db.exec("INSERT INTO t VALUES (2)").unwrap();
}

fn encryption(key_byte: &str) -> turso_core::EncryptionOpts {
    turso_core::EncryptionOpts {
        cipher: "aegis256".into(),
        hexkey: key_byte.repeat(32),
    }
}

fn open_encrypted(cfg: &S3Config, path: &Path) -> Result<Db, S3Error> {
    let storage = prepare(cfg, path)?;
    Ok(open_with_encryption(storage, path, cfg.encryption.as_ref()))
}

#[test]
fn encrypted_databases_are_encrypted_at_rest_and_need_the_key() {
    let store = FaultyStore::new();
    let dir = TempDir::new();
    let mut cfg = config(&store, "a");
    cfg.encryption = Some(encryption("ab"));
    let db = open_encrypted(&cfg, &dir.db("a.db")).unwrap();
    db.exec("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    db.exec("INSERT INTO t VALUES (1, 'SECRET-MARKER-one')")
        .unwrap();
    // More than one 32 KiB encryption chunk in a single frame.
    db.exec("INSERT INTO t VALUES (2, 'SECRET-MARKER-' || hex(randomblob(40000)))")
        .unwrap();
    db.checkpoint();
    db.exec("INSERT INTO t VALUES (3, 'SECRET-MARKER-three')")
        .unwrap();
    db.crash();

    // Nothing in the bucket is readable without the key.
    let remote = Remote::new(store.clone(), PREFIX);
    let all = keys(&store, "");
    assert!(all.iter().any(|k| k.starts_with("snapshots/")));
    assert!(all.iter().any(|k| k.starts_with("log/")));
    for key in all
        .iter()
        .filter(|k| k.starts_with("snapshots/") || k.starts_with("log/"))
    {
        let bytes = remote.get(key).unwrap().unwrap().bytes;
        let text = String::from_utf8_lossy(&bytes);
        assert!(!text.contains("SECRET-MARKER"), "{key} holds plaintext");
        if key.starts_with("snapshots/") {
            assert!(
                !bytes.starts_with(b"SQLite format 3"),
                "{key} is a plain database"
            );
        }
    }

    let restored = open_encrypted(&cfg, &dir.db("b.db")).unwrap();
    assert_eq!(restored.int("SELECT count(*) FROM t"), 3);
    assert_eq!(restored.int("SELECT length(v) FROM t WHERE id = 2"), 80_014);
    restored.crash();

    let mut wrong = cfg.clone();
    wrong.encryption = Some(encryption("cd"));
    match open_encrypted(&wrong, &dir.db("c.db")) {
        Err(S3Error::Config(msg)) => assert!(msg.contains("wrong key"), "{msg}"),
        Err(other) => panic!("unexpected error {other}"),
        Ok(_) => panic!("a wrong key must not open"),
    }
    let mut none = cfg.clone();
    none.encryption = None;
    assert!(open_encrypted(&none, &dir.db("d.db")).is_err());

    // The failed opens began taking the database over; a restore waits for
    // a writer to finish that, which a successful open does.
    open_encrypted(&cfg, &dir.db("e.db")).unwrap().crash();
    // Point-in-time restore needs the key too.
    super::restore_to(&cfg, &dir.db("copy.db"), Target::Latest).unwrap();
    assert!(super::restore_to(&wrong, &dir.db("bad.db"), Target::Latest).is_err());
}

#[test]
fn an_s3_database_needs_a_key_or_an_explicit_opt_out() {
    let store = FaultyStore::new();
    let dir = TempDir::new();
    let mut unset = config(&store, "a");
    unset.unencrypted = false;
    let refused = |result: Result<(), S3Error>, needle: &str| match result {
        Err(S3Error::Config(msg)) => assert!(msg.contains(needle), "{msg}"),
        other => panic!("expected a config error containing {needle:?}, got {other:?}"),
    };
    let open = |cfg: &S3Config, name: &str| open_encrypted(cfg, &dir.db(name)).map(drop);

    // An empty prefix: how to set a key or opt out; nothing written
    refused(open(&unset, "a.db"), "encrypted by default");
    refused(open(&unset, "a.db"), "Sediment.S3.generate_key()");
    refused(open(&unset, "a.db"), "encryption: false");
    assert!(store.put_log().is_empty(), "{:?}", store.put_log());
    let mut replica = unset.clone();
    replica.replica = true;
    refused(
        super::restore_to(&unset, &dir.db("r.db"), Target::Latest).map(drop),
        "no database",
    );
    let import = crate::s3::ImportOptions {
        verify: crate::s3::Verify::Checksum,
        source_encryption: None,
    };
    refused(
        crate::s3::import(&unset, &dir.db("missing.db"), &import).map(drop),
        "encrypted by default",
    );

    // An unencrypted database (opened with encryption: false)
    let plain = config(&store, "a");
    let db = open_db(&plain, &dir.db("plain.db")).unwrap();
    db.exec("CREATE TABLE t(x)").unwrap();
    drop(db);
    refused(open(&unset, "b.db"), "is unencrypted");
    refused(
        super::restore_to(&unset, &dir.db("r.db"), Target::Latest).map(drop),
        "is unencrypted",
    );
    refused(
        crate::s3::replica::stage(&replica, &dir.db("replica.db")).map(drop),
        "is unencrypted",
    );
    open(&plain, "c.db").unwrap();

    // An encrypted database: no key, or encryption: false, isn't enough
    let store = FaultyStore::new();
    let mut keyed = config(&store, "a");
    keyed.unencrypted = false;
    keyed.encryption = Some(encryption("ab"));
    open(&keyed, "d.db").unwrap();
    let mut unset = keyed.clone();
    unset.encryption = None;
    refused(open(&unset, "e.db"), "is encrypted:");
    let mut opted_out = unset.clone();
    opted_out.unencrypted = true;
    refused(open(&opted_out, "f.db"), "is encrypted:");
}

#[test]
fn a_key_on_an_unencrypted_prefix_is_refused_and_writes_nothing() {
    let store = FaultyStore::new();
    let dir = TempDir::new();
    let plain = config(&store, "a");
    let db = open_db(&plain, &dir.db("a.db")).unwrap();
    db.exec("CREATE TABLE t(x)").unwrap();
    db.exec("INSERT INTO t VALUES (1)").unwrap();
    drop(db);
    assert_eq!(manifest(&store).encrypted, Some(false));
    let remote = Remote::new(store.clone(), PREFIX);
    let before = remote.get(MANIFEST_KEY).unwrap().unwrap().bytes;

    let mut keyed = plain.clone();
    keyed.unencrypted = false;
    keyed.encryption = Some(encryption("ab"));
    match open_encrypted(&keyed, &dir.db("b.db")) {
        Err(S3Error::Config(msg)) => assert!(msg.contains("is unencrypted"), "{msg}"),
        Err(other) => panic!("unexpected error {other}"),
        Ok(_) => panic!("a key must not open an unencrypted database"),
    }
    assert!(super::restore_to(&keyed, &dir.db("r.db"), Target::Latest).is_err());
    assert_eq!(remote.get(MANIFEST_KEY).unwrap().unwrap().bytes, before);

    let db = open_db(&plain, &dir.db("c.db")).unwrap();
    assert_eq!(db.int("SELECT count(*) FROM t"), 1);
}

/// Strips `encrypted` from the manifest, as manifests from before it was
/// always written are.
fn forget_encryption(store: &Arc<FaultyStore>) {
    let remote = Remote::new(store.clone(), PREFIX);
    let mut m = manifest(store);
    m.encrypted = None;
    remote
        .put(MANIFEST_KEY, m.encode(), Put::Overwrite)
        .unwrap();
}

#[test]
fn an_unrecorded_encryption_is_recorded_only_by_an_open_that_reads_the_database() {
    // Unencrypted, not recorded: a key fails without marking it encrypted.
    let store = FaultyStore::new();
    let dir = TempDir::new();
    let plain = config(&store, "a");
    let db = open_db(&plain, &dir.db("a.db")).unwrap();
    db.exec("CREATE TABLE t(x)").unwrap();
    db.exec("INSERT INTO t VALUES (1)").unwrap();
    drop(db);
    forget_encryption(&store);
    let mut keyed = plain.clone();
    keyed.unencrypted = false;
    keyed.encryption = Some(encryption("ab"));
    assert!(open_encrypted(&keyed, &dir.db("b.db")).is_err());
    assert_eq!(manifest(&store).encrypted, None);
    let db = open_db(&plain, &dir.db("c.db")).unwrap();
    assert_eq!(db.int("SELECT count(*) FROM t"), 1);
    drop(db);
    assert_eq!(manifest(&store).encrypted, Some(false));

    // Encrypted, not recorded: no key fails without marking it unencrypted;
    // the key opens it and records it.
    let store = FaultyStore::new();
    let mut keyed = config(&store, "a");
    keyed.unencrypted = false;
    keyed.encryption = Some(encryption("ab"));
    let db = open_encrypted(&keyed, &dir.db("d.db")).unwrap();
    db.exec("CREATE TABLE t(x)").unwrap();
    db.exec("INSERT INTO t VALUES (1)").unwrap();
    drop(db);
    forget_encryption(&store);
    let plain = config(&store, "a");
    assert!(open_db(&plain, &dir.db("e.db")).is_err());
    assert_eq!(manifest(&store).encrypted, None);
    let db = open_encrypted(&keyed, &dir.db("f.db")).unwrap();
    assert_eq!(db.int("SELECT count(*) FROM t"), 1);
    drop(db);
    assert_eq!(manifest(&store).encrypted, Some(true));
}

#[test]
fn a_second_open_of_a_database_open_here_needs_the_same_encryption() {
    use crate::s3::check_same_encryption;
    let store = FaultyStore::new();
    let plain = config(&store, "a");
    let mut unset = plain.clone();
    unset.unencrypted = false;
    let mut keyed = unset.clone();
    keyed.encryption = Some(encryption("ab"));
    let mut upper = unset.clone();
    upper.encryption = Some(encryption("AB"));
    let mut wrong = unset.clone();
    wrong.encryption = Some(encryption("cd"));
    let ab = encryption("ab");
    assert!(check_same_encryption(Some(&ab), &keyed).is_ok());
    assert!(check_same_encryption(Some(&ab), &upper).is_ok());
    for (cfg, needle) in [
        (&wrong, "another :encryption key"),
        (&unset, "is encrypted"),
        (&plain, "is encrypted"),
    ] {
        let err = check_same_encryption(Some(&ab), cfg)
            .unwrap_err()
            .to_string();
        assert!(err.contains(needle), "{err}");
    }
    assert!(check_same_encryption(None, &plain).is_ok());
    for cfg in [&unset, &keyed] {
        let err = check_same_encryption(None, cfg).unwrap_err().to_string();
        assert!(err.contains("is unencrypted"), "{err}");
    }
}

#[test]
fn encryption_inside_the_s3_options_only_opts_out() {
    let cfg = S3Config::from_pairs([("bucket", "b"), ("encryption", "false")]).unwrap();
    assert!(cfg.unencrypted);
    let err = S3Config::from_pairs([("bucket", "b"), ("encryption", "true")]).unwrap_err();
    assert!(err.to_string().contains("not inside :s3"), "{err}");
}

#[test]
fn the_part_size_grows_so_any_snapshot_fits_in_the_part_limit() {
    use crate::s3::remote::{part_size, MAX_OBJECT};
    const MIB: u64 = 1024 * 1024;
    assert_eq!(part_size(0), 8 * MIB as usize);
    assert_eq!(part_size(60 * 1024 * MIB), 8 * MIB as usize);
    for size in [
        78 * 1024 * MIB,
        100 * 1024 * MIB,
        1024 * 1024 * MIB,
        MAX_OBJECT,
    ] {
        let part = part_size(size) as u64;
        assert_eq!(part % MIB, 0);
        // incompressible input, zstd's worst case included
        let bound = size + size / 128 + 64 * 1024;
        assert!(bound.div_ceil(part) <= 9_000, "{size}: {part}");
    }
    assert_eq!(part_size(100 * 1024 * MIB), 12 * MIB as usize);
}

#[test]
fn a_snapshot_larger_than_its_parts_allow_at_8_mib_still_uploads() {
    // Two parts at most: 8 MiB parts would need three for 20 MiB.
    crate::s3::remote::TARGET_PARTS_OVERRIDE.with(|o| o.set(Some(2)));
    let store = FaultyStore::new();
    let dir = TempDir::new();
    let file = dir.db("big.bin");
    let mut x = 0x9E37_79B9_7F4A_7C15u64;
    let bytes: Vec<u8> = (0..20 * 1024 * 1024)
        .map(|_| {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x as u8
        })
        .collect();
    std::fs::write(&file, &bytes).unwrap();
    let remote = Remote::new(store.clone(), PREFIX);
    let snapshot = remote.upload_snapshot("snapshots/big", &file).unwrap();
    crate::s3::remote::TARGET_PARTS_OVERRIDE.with(|o| o.set(None));

    let parts = store.part_sizes("snapshots/big");
    assert_eq!(parts.len(), 2, "{parts:?}");
    assert!(
        parts[0] > 8 * 1024 * 1024 && parts[0].is_multiple_of(1024 * 1024),
        "{parts:?}"
    );
    assert!(parts[1] <= parts[0]);
    assert_eq!(parts.iter().sum::<usize>() as u64, snapshot.stored_size);
    let back = dir.db("back.bin");
    remote
        .download_snapshot("snapshots/big", snapshot, &back)
        .unwrap();
    assert_eq!(std::fs::read(&back).unwrap(), bytes);
}

#[test]
fn every_part_but_the_last_has_the_same_size() {
    let store = FaultyStore::new();
    let dir = TempDir::new();
    let file = dir.db("big.bin");
    let mut x = 0x2545_F491_4F6C_DD1Du64;
    let bytes: Vec<u8> = (0..30 * 1024 * 1024)
        .map(|_| {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x as u8
        })
        .collect();
    std::fs::write(&file, &bytes).unwrap();
    let remote = Remote::new(store.clone(), PREFIX);
    remote.upload_snapshot("snapshots/even", &file).unwrap();
    let parts = store.part_sizes("snapshots/even");
    assert_eq!(parts.len(), 4, "{parts:?}");
    assert!(
        parts[..3].iter().all(|&p| p == 8 * 1024 * 1024),
        "{parts:?}"
    );
}

#[test]
fn snapshots_are_zstd_compressed() {
    let store = FaultyStore::new();
    let dir = TempDir::new();
    let db = open_db(&config(&store, "a"), &dir.db("a.db")).unwrap();
    db.exec("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    for i in 0..200 {
        db.exec(&format!(
            "INSERT INTO t VALUES ({i}, '{}')",
            "compressible ".repeat(300)
        ))
        .unwrap();
    }
    db.checkpoint();
    let m = manifest(&store);
    assert!(m.snapshot_zstd);
    assert!(
        m.snapshot_stored_size * 5 < m.snapshot_size,
        "{} stored for {}",
        m.snapshot_stored_size,
        m.snapshot_size
    );
    db.crash();
    let restored = open_db(&config(&store, "a"), &dir.db("b.db")).unwrap();
    assert_eq!(restored.int("SELECT count(*) FROM t"), 200);
}

#[test]
fn manifests_with_raw_snapshots_still_restore() {
    // Written before snapshots were compressed: no zstd fields, raw file.
    let store = FaultyStore::new();
    let dir = TempDir::new();
    crashed_db_with_rows(&store, &dir, 3);
    let remote = Remote::new(store.clone(), PREFIX);
    let mut m = manifest(&store);
    let raw_path = dir.db("raw.db");
    remote
        .download_snapshot(&m.snapshot, m.current().snapshot_info(), &raw_path)
        .unwrap();
    let raw = std::fs::read(&raw_path).unwrap();
    remote.put(&m.snapshot, raw.into(), Put::Overwrite).unwrap();
    m.snapshot_zstd = false;
    m.snapshot_stored_size = 0;
    let mut json: serde_json::Value = serde_json::from_slice(&m.encode()).unwrap();
    json.as_object_mut().unwrap().remove("snapshot_zstd");
    json.as_object_mut().unwrap().remove("snapshot_stored_size");
    remote
        .put(
            MANIFEST_KEY,
            serde_json::to_vec(&json).unwrap().into(),
            Put::Overwrite,
        )
        .unwrap();
    let restored = open_db(&config(&store, "a"), &dir.db("b.db")).unwrap();
    assert_eq!(restored.int("SELECT count(*) FROM t"), 3);
}

/// Inside BEGIN CONCURRENT, turso allocates each AUTOINCREMENT id in an
/// inner transaction that commits on its own, so every insert uploads a log
/// frame of its own (guides/s3.md, "Costs").
#[test]
fn autoincrement_in_concurrent_transactions_uploads_per_insert() {
    let mut uploads = Vec::new();
    for ddl in [
        "CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER)",
        "CREATE TABLE t(id INTEGER PRIMARY KEY AUTOINCREMENT, v INTEGER)",
    ] {
        for begin in ["BEGIN", "BEGIN CONCURRENT"] {
            let store = FaultyStore::new();
            let dir = TempDir::new();
            let db = open_db(&config(&store, "a"), &dir.db("a.db")).unwrap();
            db.exec(ddl).unwrap();
            let frames = || {
                store
                    .put_log()
                    .iter()
                    .filter(|k| k.contains("/log/"))
                    .count()
            };
            let before = frames();
            db.exec(begin).unwrap();
            for i in 0..3 {
                db.exec(&format!("INSERT INTO t(v) VALUES ({i})")).unwrap();
            }
            db.exec("COMMIT").unwrap();
            uploads.push(frames() - before);
        }
    }
    // rowid: BEGIN, BEGIN CONCURRENT; AUTOINCREMENT: BEGIN, BEGIN CONCURRENT
    assert_eq!(uploads, vec![1, 1, 1, 4]);
}

/// Outcome mix of concurrent BEGIN CONCURRENT writers on S3 with
/// delayed uploads, with and without AUTOINCREMENT and group commit (the
/// repro of docs/upstream/turso-sequence-inner-tx-busy.md). PROBE_PLAIN runs
/// it without S3; PROBE_DELAY_MS sets the delay of each log upload (default 5 ms).
#[test]
#[ignore]
fn concurrent_autoincrement_busy_probe() {
    use crate::conn::ConnRes;
    use crate::stmt::{advance_blocking as advance, Step};
    for autoinc in [false, true] {
        for group in [false, true] {
            let store = FaultyStore::new();
            let dir = TempDir::new();
            let mut cfg = config(&store, "a");
            cfg.group_commit = group;
            let plain = std::env::var("PROBE_PLAIN").is_ok();
            let (database, setup): (Arc<Database>, Arc<Connection>) = if plain {
                let db = open_plain(&dir.db("a.db"));
                db.conn.execute("PRAGMA journal_mode = 'mvcc'").unwrap();
                (db._db, db.conn)
            } else {
                let db = open_db(&cfg, &dir.db("a.db")).unwrap();
                (db._db, db.conn)
            };
            let key = if autoinc {
                "INTEGER PRIMARY KEY AUTOINCREMENT"
            } else {
                "INTEGER PRIMARY KEY"
            };
            setup
                .execute(format!("CREATE TABLE t(id {key}, w INTEGER)"))
                .unwrap();
            let delay: u64 = std::env::var("PROBE_DELAY_MS")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(5);
            if delay > 0 {
                store.inject(
                    Op::Put,
                    "/log/",
                    Fault::Delay(Duration::from_millis(delay)),
                    1_000_000,
                );
            }
            let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
            let handles: Vec<_> = (0..4)
                .map(|w| {
                    let (conn, stop) = (database.connect().unwrap(), stop.clone());
                    std::thread::spawn(move || {
                        conn.set_busy_timeout(Duration::from_secs(15));
                        if group {
                            conn.execute("PRAGMA synchronous = FULL").unwrap();
                            conn.execute("PRAGMA mvcc_group_commit = ON").unwrap();
                        }
                        let res = ConnRes::detached();
                        let mut counts = std::collections::BTreeMap::<String, u32>::new();
                        while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                            let mut outcome = "ok".to_string();
                            for sql in [
                                "BEGIN CONCURRENT".to_string(),
                                format!("INSERT INTO t(w) VALUES ({w})"),
                                format!("INSERT INTO t(w) VALUES ({w})"),
                                "COMMIT".to_string(),
                            ] {
                                let mut stmt = conn.prepare(&sql).unwrap();
                                let r = loop {
                                    match advance(&res, &mut stmt) {
                                        Ok(Step::Row) => continue,
                                        Ok(Step::Done) => break None,
                                        Ok(Step::Busy) => break Some("busy".to_string()),
                                        Ok(Step::Sleep(_)) => {
                                            unreachable!("advance_blocking sleeps")
                                        }
                                        Ok(Step::Error(m)) | Err(Step::Error(m)) => break Some(m),
                                        Err(_) => break Some("busy".to_string()),
                                    }
                                };
                                if let Some(e) = r {
                                    let e: String = e.chars().take(40).collect();
                                    outcome = format!("{}: {e}", &sql[..sql.len().min(12)]);
                                    drop(stmt);
                                    let _ = conn.execute("ROLLBACK");
                                    break;
                                }
                            }
                            *counts.entry(outcome).or_default() += 1;
                        }
                        counts
                    })
                })
                .collect();
            std::thread::sleep(Duration::from_secs(4));
            stop.store(true, std::sync::atomic::Ordering::Relaxed);
            let mut total = std::collections::BTreeMap::<String, u32>::new();
            for h in handles {
                for (k, v) in h.join().unwrap() {
                    *total.entry(k).or_default() += v;
                }
            }
            eprintln!("probe autoinc={autoinc} group={group}: {total:?}");
        }
    }
}

/// Turso holds its checkpoint lock (every read and write waits)
/// until the storage's checkpoint hooks return, so S3 work must not happen
/// there: a slow snapshot upload doesn't hold up the checkpoint or reads.
#[test]
fn a_checkpoint_does_not_wait_for_the_snapshot_upload() {
    let store = FaultyStore::new();
    let dir = TempDir::new();
    let db = open_db(&config(&store, "a"), &dir.db("a.db")).unwrap();
    db.exec("CREATE TABLE t(id INTEGER PRIMARY KEY)").unwrap();
    for i in 0..50 {
        db.exec(&format!("INSERT INTO t VALUES ({i})")).unwrap();
    }
    let before = manifest(&store).epoch;
    store.inject(
        Op::Put,
        "snapshots/",
        Fault::Delay(Duration::from_millis(1500)),
        1,
    );
    let started = std::time::Instant::now();
    db.exec("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    assert!(
        started.elapsed() < Duration::from_millis(700),
        "{:?}",
        started.elapsed()
    );
    // Another connection reads while the snapshot uploads.
    let reader = db._db.connect().unwrap();
    let started = std::time::Instant::now();
    let mut stmt = reader.query("SELECT count(*) FROM t").unwrap().unwrap();
    assert_eq!(stmt.run_collect_rows().unwrap()[0][0], Value::from_i64(50));
    assert!(
        started.elapsed() < Duration::from_millis(500),
        "{:?}",
        started.elapsed()
    );
    db.storage.wait_background(Duration::MAX);
    assert!(!db.storage.info().snapshot_pending);
    assert!(manifest(&store).epoch.seq > before.seq);
}

#[test]
fn close_waits_for_a_slow_snapshot_upload_only_within_its_timeout() {
    let store = FaultyStore::new();
    let dir = TempDir::new();
    let db = open_db(&config(&store, "a"), &dir.db("a.db")).unwrap();
    db.exec("CREATE TABLE t(id INTEGER PRIMARY KEY)").unwrap();
    db.exec("INSERT INTO t VALUES (1)").unwrap();
    store.inject(
        Op::Put,
        "snapshots/",
        Fault::Delay(Duration::from_secs(3)),
        1,
    );
    db.exec("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    let started = std::time::Instant::now();
    assert!(db.storage.close(Duration::from_millis(300)));
    assert!(
        started.elapsed() < Duration::from_secs(1),
        "{:?}",
        started.elapsed()
    );
    // The pass goes on and publishes the snapshot.
    assert!(db.storage.wait_background(Duration::from_secs(10)));
    assert!(!db.storage.info().snapshot_pending);
}
