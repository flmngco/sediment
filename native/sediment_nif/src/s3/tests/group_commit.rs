use std::sync::Arc;
use std::time::Duration;

use super::faulty::{Fault, FaultyStore, Op};
use super::{assert_err_contains, config, open_db, segments, Db, TempDir};
use crate::s3::S3Config;

fn group_config(store: &Arc<FaultyStore>, owner: &str) -> S3Config {
    let mut cfg = config(store, owner);
    cfg.group_commit = true;
    cfg
}

fn setup(store: &Arc<FaultyStore>, dir: &TempDir) -> Db {
    let db = open_db(&group_config(store, "a"), &dir.db("a.db")).unwrap();
    db.exec("CREATE TABLE t(id INTEGER PRIMARY KEY, writer INTEGER)")
        .unwrap();
    db
}

#[test]
fn sequential_commits_upload_one_segment_each_and_survive_a_crash() {
    let store = FaultyStore::new();
    let dir = TempDir::new();
    let db = setup(&store, &dir);
    let before = segments(&store).len();
    for i in 0..10 {
        db.exec(&format!("INSERT INTO t VALUES ({i}, 0)")).unwrap();
    }
    assert_eq!(segments(&store).len(), before + 10);
    db.crash();

    let restored = open_db(&config(&store, "a"), &dir.db("b.db")).unwrap();
    assert_eq!(restored.int("SELECT count(*) FROM t"), 10);
}

#[test]
fn concurrent_commits_are_batched_into_fewer_objects() {
    let store = FaultyStore::new();
    let dir = TempDir::new();
    let db = Arc::new(setup(&store, &dir));
    // Slow segment uploads let commits queue up behind the leader.
    store.inject(
        Op::Put,
        "log/",
        Fault::Delay(Duration::from_millis(20)),
        1_000,
    );
    let before = segments(&store).len();

    let writers = 8;
    let commits = 20;
    let handles: Vec<_> = (0..writers)
        .map(|w| {
            let db = db.clone();
            std::thread::spawn(move || {
                let conn = db._db.connect().unwrap();
                for i in 0..commits {
                    let id = w * 1000 + i;
                    conn.execute(format!(
                        "BEGIN CONCURRENT; INSERT INTO t VALUES ({id}, {w}); COMMIT"
                    ))
                    .unwrap();
                }
                conn.close().unwrap();
            })
        })
        .collect();
    for handle in handles {
        handle.join().unwrap();
    }
    store.clear_faults();

    let total = (writers * commits) as i64;
    assert_eq!(db.int("SELECT count(*) FROM t"), total);
    let objects = segments(&store).len() - before;
    assert!(
        objects < total as usize,
        "{objects} objects for {total} commits: nothing was batched"
    );
    let info = db.storage.info();
    assert_eq!(info.uploaded_frames, total as u64 + 1);

    Arc::into_inner(db).unwrap().crash();
    let restored = open_db(&config(&store, "a"), &dir.db("b.db")).unwrap();
    assert_eq!(restored.int("SELECT count(*) FROM t"), total);
}

#[test]
fn a_failed_batch_upload_fences_the_writer_and_reopen_is_consistent() {
    let store = FaultyStore::new();
    let dir = TempDir::new();
    let db = setup(&store, &dir);
    db.exec("INSERT INTO t VALUES (1, 0)").unwrap();

    store.inject(Op::Put, "log/", Fault::Fail, 100);
    assert_err_contains(
        db.exec("INSERT INTO t VALUES (2, 0)"),
        "reopen the database",
    );
    store.clear_faults();
    assert_err_contains(
        db.exec("INSERT INTO t VALUES (3, 0)"),
        "reopen the database",
    );
    assert!(db.storage.info().poisoned.is_some());
    db.crash();

    let restored = open_db(&config(&store, "a"), &dir.db("b.db")).unwrap();
    assert_eq!(restored.int("SELECT count(*) FROM t WHERE id = 1"), 1);
    assert_eq!(restored.int("SELECT count(*) FROM t WHERE id = 3"), 0);
    restored.exec("INSERT INTO t VALUES (4, 0)").unwrap();
}

#[test]
fn checkpoints_flush_the_batch_first() {
    let store = FaultyStore::new();
    let dir = TempDir::new();
    let db = setup(&store, &dir);
    for i in 0..5 {
        db.exec(&format!("INSERT INTO t VALUES ({i}, 0)")).unwrap();
    }
    db.checkpoint();
    db.exec("INSERT INTO t VALUES (99, 0)").unwrap();
    db.crash();

    let restored = open_db(&config(&store, "a"), &dir.db("b.db")).unwrap();
    assert_eq!(restored.int("SELECT count(*) FROM t"), 6);
}

/// Each of `writers` threads commits `per` rows (BEGIN CONCURRENT, retrying
/// while busy); returns the ids whose COMMIT returned Ok.
fn acked_inserts(db: &Db, writers: i64, per: i64) -> Vec<i64> {
    acked_inserts_from(db, 0, writers, per)
}

/// Like `acked_inserts`, with ids offset by `base`.
fn acked_inserts_from(db: &Db, base: i64, writers: i64, per: i64) -> Vec<i64> {
    let handles: Vec<_> = (0..writers)
        .map(|w| {
            let conn = db._db.connect().unwrap();
            std::thread::spawn(move || {
                conn.execute("PRAGMA synchronous = FULL").unwrap();
                conn.execute("PRAGMA mvcc_group_commit = ON").unwrap();
                let mut acked = Vec::new();
                for i in 0..per {
                    let id = base + w * 1000 + i;
                    let tx = format!("BEGIN CONCURRENT; INSERT INTO t VALUES ({id}, {w}); COMMIT");
                    for _ in 0..2000 {
                        match conn.execute(&tx) {
                            Ok(()) => acked.push(id),
                            Err(turso_core::LimboError::Busy) => {
                                let _ = conn.execute("ROLLBACK");
                                std::thread::sleep(Duration::from_millis(1));
                                continue;
                            }
                            Err(_) => {
                                let _ = conn.execute("ROLLBACK");
                            }
                        }
                        break;
                    }
                }
                acked
            })
        })
        .collect();
    handles
        .into_iter()
        .flat_map(|h| h.join().unwrap())
        .collect()
}

#[test]
fn failed_batch_upload_acknowledges_nothing_it_lost() {
    // After a leader's failed upload turso lets a waiting commit sync the
    // written prefix and acknowledge it; that sync must fail as well.
    let store = FaultyStore::new();
    let dir = TempDir::new();
    let db = setup(&store, &dir);
    store.inject(Op::Put, "log/", Fault::Delay(Duration::from_millis(10)), 5);
    store.inject(Op::Put, "log/", Fault::Fail, 1);
    let acked = acked_inserts(&db, 4, 10);
    assert!(acked.len() < 40, "the failed batch's commits must fail");
    assert!(db.storage.info().poisoned.is_some());
    db.crash();

    let restored = open_db(&config(&store, "a"), &dir.db("b.db")).unwrap();
    let ids: Vec<i64> = restored
        .rows("SELECT id FROM t ORDER BY id")
        .into_iter()
        .map(|row| row[0].to_string().parse().unwrap())
        .collect();
    for id in &acked {
        assert!(ids.contains(id), "acknowledged {id} lost");
    }
}

/// Seeded soak: rounds of concurrent group commits under random upload
/// faults, reopening whenever the writer is fenced. Every acknowledged
/// commit must survive; nothing never attempted may appear.
#[test]
fn randomized_group_commit_faults_never_lose_acknowledged_commits() {
    let seeds: u64 = std::env::var("S3_SOAK_SEEDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(3);
    for seed in 1..=seeds {
        let mut rng = super::Rng(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1);
        let store = FaultyStore::new();
        let dir = TempDir::new();
        let mut incarnation = 0;
        let reopen = |n: &mut u32| {
            *n += 1;
            open_db(&group_config(&store, "a"), &dir.db(&format!("i{n}.db")))
                .unwrap_or_else(|err| panic!("seed {seed}: reopen failed: {err}"))
        };
        let mut db = setup(&store, &dir);
        let mut acked = Vec::new();
        let mut attempted = std::collections::BTreeSet::new();
        for round in 0..6i64 {
            let faults = [
                Fault::Fail,
                Fault::FailAfter,
                Fault::LandLater(Duration::from_millis(10)),
                Fault::Delay(Duration::from_millis(5)),
            ];
            for _ in 0..(rng.next() % 3) {
                store.inject(Op::Put, "log/", rng.pick(&faults), 1);
            }
            let base = (round + 1) * 100_000;
            for w in 0..3 {
                for i in 0..8 {
                    attempted.insert(base + w * 1000 + i);
                }
            }
            acked.extend(acked_inserts_from(&db, base, 3, 8));
            store.clear_faults();
            if db.storage.info().poisoned.is_some() || rng.chance(30) {
                db.crash();
                std::thread::sleep(Duration::from_millis(20));
                db = reopen(&mut incarnation);
            }
        }
        db.crash();
        std::thread::sleep(Duration::from_millis(30));
        let restored = reopen(&mut incarnation);
        let ids: std::collections::BTreeSet<i64> = restored
            .rows("SELECT id FROM t")
            .into_iter()
            .map(|row| row[0].to_string().parse().unwrap())
            .collect();
        for id in &acked {
            assert!(ids.contains(id), "seed {seed}: acknowledged {id} lost");
        }
        for id in &ids {
            assert!(attempted.contains(id), "seed {seed}: phantom row {id}");
        }
    }
}

#[test]
fn multi_frame_objects_restore_point_in_time_and_to_replicas() {
    let store = FaultyStore::new();
    let dir = TempDir::new();
    let mut cfg = group_config(&store, "a");
    cfg.retain_epochs = 2;
    let db = open_db(&cfg, &dir.db("a.db")).unwrap();
    db.exec("CREATE TABLE t(id INTEGER PRIMARY KEY, writer INTEGER)")
        .unwrap();
    store.inject(
        Op::Put,
        "log/",
        Fault::Delay(Duration::from_millis(15)),
        1_000,
    );
    let acked = acked_inserts(&db, 6, 8);
    store.clear_faults();
    let info = db.storage.info();
    assert!(
        info.uploaded_objects < info.uploaded_frames,
        "batches must have formed: {info:?}"
    );

    let latest = dir.db("latest.db");
    crate::s3::restore_to(&cfg, &latest, crate::s3::Target::Latest).unwrap();
    let copy = super::open_plain(&latest);
    let mut stmt = copy.conn.query("SELECT count(*) FROM t").unwrap().unwrap();
    let rows = stmt.run_collect_rows().unwrap();
    assert_eq!(rows[0][0].to_string(), acked.len().to_string());

    let now = crate::s3::layout::now_ms() + 2_000;
    let timed = dir.db("timed.db");
    crate::s3::restore_to(&cfg, &timed, crate::s3::Target::Time(now)).unwrap();
    let copy = super::open_plain(&timed);
    let mut stmt = copy.conn.query("SELECT count(*) FROM t").unwrap().unwrap();
    let rows = stmt.run_collect_rows().unwrap();
    assert_eq!(rows[0][0].to_string(), acked.len().to_string());

    let staged = crate::s3::replica::stage(&cfg, &dir.db("r.db")).unwrap();
    assert_eq!(staged.state.log_bytes, info.log_offset);
}
