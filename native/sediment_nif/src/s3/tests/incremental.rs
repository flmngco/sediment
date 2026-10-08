//! Incremental snapshots: deltas on top of a full snapshot.

use std::sync::Arc;

use super::faulty::FaultyStore;
use super::{config, keys, manifest, open_db, Db, TempDir, PREFIX};
use crate::s3::layout::{Manifest, MANIFEST_VERSION, MANIFEST_VERSION_CHAIN};
use crate::s3::remote::{Put, Remote};
use crate::s3::restore::restore_point;
use crate::s3::snapshot::MAX_CHAIN;
use crate::s3::{restore_to, S3Config, S3Error, Target};

/// A database of about 4 MB that compresses poorly, so a snapshot is big
/// next to a delta.
fn big_db(cfg: &S3Config, dir: &TempDir) -> Db {
    let db = open_db(cfg, &dir.db("a.db")).unwrap();
    db.exec("CREATE TABLE t(id INTEGER PRIMARY KEY, v BLOB)")
        .unwrap();
    for i in 0..64 {
        db.exec(&format!("INSERT INTO t VALUES ({i}, randomblob(65536))"))
            .unwrap();
    }
    db.checkpoint();
    db
}

fn restored_rows(cfg: &S3Config, dir: &TempDir, name: &str) -> i64 {
    let path = dir.db(name);
    restore_to(cfg, &path, Target::Latest).unwrap();
    let db = super::open_plain(&path);
    let mut stmt = db.conn.query("SELECT count(*) FROM t").unwrap().unwrap();
    match stmt.run_collect_rows().unwrap()[0][0] {
        turso_core::Value::Numeric(turso_core::Numeric::Integer(n)) => n,
        ref other => panic!("{other:?}"),
    }
}

#[test]
fn small_changes_upload_deltas_until_the_chain_is_long() {
    let store = FaultyStore::new();
    let dir = TempDir::new();
    let cfg = config(&store, "a");
    let db = big_db(&cfg, &dir);
    let start = manifest(&store).snapshot_base.len();
    let full = manifest(&store).current().chain()[0].clone();

    for i in 1..MAX_CHAIN - start {
        db.exec(&format!("UPDATE t SET v = zeroblob(10) WHERE id = {i}"))
            .unwrap();
        db.checkpoint();
        let m = manifest(&store);
        assert!(m.snapshot.ends_with(".delta"), "{}", m.snapshot);
        assert_eq!(m.snapshot_base.len(), start + i);
        assert_eq!(m.snapshot_base[0], full);
        assert_eq!(m.version, MANIFEST_VERSION_CHAIN);
        assert!(
            m.snapshot_stored_size * 10 < full.stored_size,
            "delta {} vs full {}",
            m.snapshot_stored_size,
            full.stored_size
        );
        assert_eq!(restored_rows(&cfg, &dir, &format!("r{i}.db")), 64);
    }
    // The chain is full: the next snapshot starts a new one.
    db.exec("DELETE FROM t WHERE id = 0").unwrap();
    db.checkpoint();
    let m = manifest(&store);
    assert!(
        m.snapshot_base.is_empty() && m.snapshot.ends_with(".db"),
        "{m:?}"
    );
    assert_eq!(m.version, MANIFEST_VERSION);
    assert_eq!(restored_rows(&cfg, &dir, "after.db"), 63);
    // Nothing references the old chain anymore.
    assert_eq!(keys(&store, "snapshots/"), vec![m.snapshot.clone()]);
}

#[test]
fn deltas_adding_up_to_the_full_snapshot_start_a_new_chain() {
    let store = FaultyStore::new();
    let dir = TempDir::new();
    let cfg = config(&store, "a");
    let db = big_db(&cfg, &dir);
    let mut lengths = Vec::new();
    for _ in 0..4 {
        db.exec("UPDATE t SET v = randomblob(65536) WHERE id % 2 = 0")
            .unwrap();
        db.checkpoint();
        let m = manifest(&store);
        let chain = m.current().chain();
        let deltas: u64 = chain[1..].iter().map(|l| l.stored_size).sum();
        // Deltas never add up to more than a full snapshot plus the last one.
        assert!(deltas <= chain[0].stored_size + m.snapshot_stored_size);
        lengths.push(m.snapshot_base.len());
    }
    assert!(
        lengths.contains(&0),
        "the chain never restarted: {lengths:?}"
    );
    assert_eq!(restored_rows(&cfg, &dir, "r.db"), 64);
}

#[test]
fn a_reopened_writer_publishes_a_delta_on_the_restored_chain() {
    let store = FaultyStore::new();
    let dir = TempDir::new();
    let cfg = config(&store, "a");
    let db = big_db(&cfg, &dir);
    db.exec("UPDATE t SET v = zeroblob(1) WHERE id = 1")
        .unwrap();
    db.checkpoint();
    let before = manifest(&store);
    db.exec("UPDATE t SET v = zeroblob(1) WHERE id = 2")
        .unwrap();
    db.crash();
    // The open restores the chain, replays the log and snapshots a delta.
    let db = open_db(&cfg, &dir.db("b.db")).unwrap();
    let m = manifest(&store);
    assert_eq!(m.snapshot_base, before.current().chain(), "{m:?}");
    assert_eq!(db.int("SELECT count(*) FROM t WHERE length(v) = 1"), 2);
    assert_eq!(restored_rows(&cfg, &dir, "r.db"), 64);
}

#[test]
fn retained_epochs_keep_their_chains_for_point_in_time_restore() {
    let store = FaultyStore::new();
    let dir = TempDir::new();
    let mut cfg = config(&store, "a");
    cfg.retain_epochs = 3;
    let db = big_db(&cfg, &dir);
    // Rows at the end of each epoch: what restoring that epoch gives.
    let mut rows_at_end = std::collections::BTreeMap::new();
    for i in 1..=5 {
        db.exec(&format!("DELETE FROM t WHERE id = {i}")).unwrap();
        let seq = manifest(&store).epoch.seq;
        rows_at_end.insert(seq, db.int("SELECT count(*) FROM t"));
        db.checkpoint();
    }
    let m = manifest(&store);
    let remote = Remote::new(store.clone(), PREFIX);
    assert_eq!(m.history.len(), 3);
    for (n, record) in m.retained().enumerate() {
        assert!(!record.snapshot_base.is_empty());
        let path = dir.db(&format!("pitr{n}.db"));
        let point = crate::s3::restore::Point {
            record,
            cutoff_ms: None,
        };
        restore_point(&remote, &point, &path, 4).unwrap();
    }
    for record in &m.history {
        let path = dir.db(&format!("epoch{}.db", record.epoch.seq));
        restore_to(&cfg, &path, Target::Epoch(record.epoch.seq)).unwrap();
        let old = super::open_plain(&path);
        let mut stmt = old.conn.query("SELECT count(*) FROM t").unwrap().unwrap();
        assert_eq!(
            stmt.run_collect_rows().unwrap()[0][0],
            turso_core::Value::from_i64(rows_at_end[&record.epoch.seq]),
            "epoch {}",
            record.epoch.seq
        );
    }
}

#[test]
fn a_damaged_or_missing_link_fails_the_restore() {
    let store = FaultyStore::new();
    let dir = TempDir::new();
    let cfg = config(&store, "a");
    let db = big_db(&cfg, &dir);
    db.exec("UPDATE t SET v = zeroblob(1) WHERE id = 1")
        .unwrap();
    db.checkpoint();
    drop(db);
    let m: Manifest = manifest(&store);
    let remote = Remote::new(store.clone(), PREFIX);
    let delta = m.snapshot.clone();
    let good = remote.get(&delta).unwrap().unwrap().bytes;
    let mut bad = good.to_vec();
    let middle = bad.len() / 2;
    bad[middle] ^= 0x40;
    remote.put(&delta, bad.into(), Put::Overwrite).unwrap();
    let err = restore_to(&cfg, &dir.db("x.db"), Target::Latest).unwrap_err();
    assert!(matches!(err, S3Error::Corrupt(_)), "{err:?}");

    remote.put(&delta, good, Put::Overwrite).unwrap();
    remote.delete(&m.snapshot_base[0].key).unwrap();
    let err = restore_to(&cfg, &dir.db("y.db"), Target::Latest).unwrap_err();
    assert!(matches!(err, S3Error::Corrupt(_)), "{err:?}");
    let leftovers: Vec<_> = std::fs::read_dir(dir.db("y.db").parent().unwrap())
        .unwrap()
        .map(|e| e.unwrap().file_name().into_string().unwrap())
        // The failed restore's files (the writer's sidecar is a.db's).
        .filter(|name| name.contains(".s3-") && name.contains("y.db"))
        .collect();
    assert!(leftovers.is_empty(), "{leftovers:?}");
}

#[test]
fn incremental_snapshots_can_be_turned_off() {
    let store: Arc<FaultyStore> = FaultyStore::new();
    let dir = TempDir::new();
    let mut cfg = config(&store, "a");
    cfg.incremental_snapshots = false;
    let db = big_db(&cfg, &dir);
    db.exec("UPDATE t SET v = zeroblob(1) WHERE id = 1")
        .unwrap();
    db.checkpoint();
    let m = manifest(&store);
    assert!(m.snapshot_base.is_empty() && m.snapshot.ends_with(".db"));
}

#[test]
fn deltas_of_encrypted_databases_stay_encrypted_and_restore() {
    let store = FaultyStore::new();
    let dir = TempDir::new();
    let mut cfg = config(&store, "a");
    cfg.encryption = Some(super::encryption("cd"));
    let db = super::open_encrypted(&cfg, &dir.db("a.db")).unwrap();
    db.exec("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    for i in 0..200 {
        db.exec(&format!(
            "INSERT INTO t VALUES ({i}, 'SECRET-MARKER-' || hex(randomblob(2000)))"
        ))
        .unwrap();
    }
    db.checkpoint();
    db.exec("UPDATE t SET v = 'SECRET-MARKER-changed' WHERE id = 7")
        .unwrap();
    db.checkpoint();
    let m = manifest(&store);
    assert!(m.snapshot.ends_with(".delta"), "{m:?}");
    let remote = Remote::new(store.clone(), PREFIX);
    let delta = remote.get(&m.snapshot).unwrap().unwrap().bytes;
    let plain = zstd::decode_all(&delta[..]).unwrap();
    assert!(!plain.windows(13).any(|w| w == b"SECRET-MARKER"));
    let path = dir.db("r.db");
    restore_to(&cfg, &path, Target::Latest).unwrap();
}

/// Numbers for guides/s3.md: snapshot upload size and time after a small
/// change to a ~40 MB database, full vs incremental.
#[test]
#[ignore]
fn incremental_snapshot_sizes_probe() {
    for incremental in [false, true] {
        let store = FaultyStore::new();
        let dir = TempDir::new();
        let mut cfg = config(&store, "a");
        cfg.incremental_snapshots = incremental;
        cfg.checkpoint_threshold = Some(1 << 40);
        let db = open_db(&cfg, &dir.db("a.db")).unwrap();
        db.exec("CREATE TABLE t(id INTEGER PRIMARY KEY, v BLOB)")
            .unwrap();
        db.exec("BEGIN").unwrap();
        for i in 0..640 {
            db.exec(&format!("INSERT INTO t VALUES ({i}, randomblob(65536))"))
                .unwrap();
        }
        db.exec("COMMIT").unwrap();
        db.checkpoint();
        for rows in [1, 10, 100] {
            db.exec(&format!(
                "UPDATE t SET v = randomblob(65536) WHERE id < {rows}"
            ))
            .unwrap();
            let started = std::time::Instant::now();
            db.checkpoint();
            let m = manifest(&store);
            eprintln!(
                "probe incremental={incremental} rows={rows}: db {} MB, uploaded {} KB, {:?}",
                m.snapshot_size >> 20,
                m.snapshot_stored_size >> 10,
                started.elapsed()
            );
        }
    }
}

/// Older drivers refuse version 2 manifests; a manifest stays version 2 as
/// long as any retained epoch is a chain, or an older driver could rewrite
/// the history without it.
#[test]
fn manifests_stay_version_2_while_any_retained_epoch_is_a_chain() {
    for turn_off in [false, true] {
        let store = FaultyStore::new();
        let dir = TempDir::new();
        let mut cfg = config(&store, "a");
        cfg.retain_epochs = 2;
        let mut db = big_db(&cfg, &dir);
        let mut saw_full_with_chain_history = false;
        for step in 0..(MAX_CHAIN as i64 + 6) {
            if turn_off && step == 3 {
                // A writer without incremental snapshots takes over.
                db.storage.release();
                drop(db);
                cfg.incremental_snapshots = false;
                db = open_db(&cfg, &dir.db(&format!("b{step}.db"))).unwrap();
            } else {
                db.exec(&format!("UPDATE t SET v = zeroblob(1) WHERE id = {step}"))
                    .unwrap();
                db.checkpoint();
            }
            let m = manifest(&store);
            let chains = m.retained().any(|r| !r.snapshot_base.is_empty());
            saw_full_with_chain_history |= m.snapshot_base.is_empty() && chains;
            let expected = if chains {
                MANIFEST_VERSION_CHAIN
            } else {
                MANIFEST_VERSION
            };
            assert_eq!(
                m.version, expected,
                "turn_off={turn_off} step {step}: {m:?}"
            );
        }
        assert!(saw_full_with_chain_history, "turn_off={turn_off}");
    }
}

#[test]
fn a_delta_whose_manifest_never_landed_is_ignored_and_collected() {
    use super::faulty::{Fault, Op};
    let store = FaultyStore::new();
    let dir = TempDir::new();
    let cfg = config(&store, "a");
    let db = big_db(&cfg, &dir);
    let published = manifest(&store);
    store.inject(Op::Put, "manifest.json", Fault::Fail, 1_000);
    db.exec("UPDATE t SET v = zeroblob(1) WHERE id = 5")
        .unwrap();
    db.checkpoint();
    let orphan = keys(&store, "snapshots/")
        .into_iter()
        .find(|k| {
            !published
                .retained()
                .any(|r| r.chain().iter().any(|l| &l.key == k))
        })
        .expect("the new delta was uploaded");
    assert!(orphan.ends_with(".delta"), "{orphan}");
    assert_eq!(manifest(&store), published);
    db.crash();
    store.clear_faults();

    let db = open_db(&cfg, &dir.db("b.db")).unwrap();
    // The commit before the checkpoint is in the log; the orphan isn't used.
    assert_eq!(db.int("SELECT count(*) FROM t WHERE length(v) = 1"), 1);
    assert_eq!(restored_rows(&cfg, &dir, "r.db"), 64);
    // GC deletes unreferenced snapshots of older epochs; the orphan's epoch
    // number is the reopened writer's, so it goes at the next checkpoint.
    db.exec("UPDATE t SET v = zeroblob(1) WHERE id = 6")
        .unwrap();
    db.checkpoint();
    assert!(!keys(&store, "snapshots/").contains(&orphan));
    let m = manifest(&store);
    let referenced: Vec<String> = m.current().chain().into_iter().map(|l| l.key).collect();
    assert_eq!(keys(&store, "snapshots/"), {
        let mut r = referenced.clone();
        r.sort();
        r
    });
}

#[test]
fn lowering_retain_epochs_collects_what_is_no_longer_retained() {
    let store = FaultyStore::new();
    let dir = TempDir::new();
    let mut cfg = config(&store, "a");
    cfg.retain_epochs = 4;
    let db = big_db(&cfg, &dir);
    for i in 1..=5 {
        db.exec(&format!("UPDATE t SET v = zeroblob(1) WHERE id = {i}"))
            .unwrap();
        db.checkpoint();
    }
    assert_eq!(manifest(&store).history.len(), 4);
    db.storage.release();
    drop(db);

    cfg.retain_epochs = 1;
    let db = open_db(&cfg, &dir.db("b.db")).unwrap();
    let m = manifest(&store);
    assert_eq!(m.history.len(), 1);
    let referenced: std::collections::BTreeSet<String> = m
        .retained()
        .flat_map(|r| r.chain())
        .map(|l| l.key)
        .collect();
    let stored: std::collections::BTreeSet<String> =
        keys(&store, "snapshots/").into_iter().collect();
    assert_eq!(stored, referenced);
    let oldest = m.history[0].epoch.seq;
    let logs: Vec<String> = keys(&store, "log/");
    assert!(logs.iter().all(|k| {
        crate::s3::layout::parse_segment_key(k).is_some_and(|(e, _)| e.seq >= oldest)
    }));
    for record in m.retained() {
        let path = dir.db(&format!("e{}.db", record.epoch.seq));
        restore_to(&cfg, &path, Target::Epoch(record.epoch.seq)).unwrap();
    }
    assert_eq!(db.int("SELECT count(*) FROM t"), 64);
}
