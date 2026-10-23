//! Key rotation: rings, rekey, retention of the old key's epochs, PITR
//! across a rekey, release_key and the census.

use std::path::Path;
use std::sync::Arc;

use super::faulty::{Fault, FaultyStore, Op};
use super::{config, key_as, manifest, open_encrypted, ring, segments, Db, TempDir, PREFIX};
use crate::s3::hook::{self, Action, State};
use crate::s3::layout::MANIFEST_KEY;
use crate::s3::remote::Remote;
use crate::s3::{key_census, rekey, release_key, Rekeyed, S3Config, S3Error, Target};

/// A writer config with the ring `keys` (`(id, key byte)`, the first active).
fn keyed(store: &Arc<FaultyStore>, owner: &str, keys: &[(&str, &str)]) -> S3Config {
    let mut cfg = config(store, owner);
    cfg.unencrypted = false;
    cfg.encryption = Some(ring(keys));
    cfg
}

fn rows(db: &Db) -> Vec<String> {
    db.rows("SELECT v FROM t ORDER BY id")
        .iter()
        .map(|row| row[0].to_string())
        .collect()
}

/// A database under r1 with rows `values`, closed cleanly.
fn seeded(store: &Arc<FaultyStore>, dir: &TempDir, values: &[&str]) -> S3Config {
    let cfg = keyed(store, "a", &[("r1", "11")]);
    let db = open_encrypted(&cfg, &dir.db("seed.db")).unwrap();
    db.exec("CREATE TABLE t(id INTEGER PRIMARY KEY AUTOINCREMENT, v TEXT)")
        .unwrap();
    for v in values {
        db.exec(&format!("INSERT INTO t (v) VALUES ('{v}')"))
            .unwrap();
    }
    drop(db);
    cfg
}

fn census(cfg: &S3Config) -> (Option<String>, Vec<(String, usize)>) {
    let census = key_census(cfg).unwrap().unwrap();
    (
        census.current,
        census
            .retained
            .into_iter()
            .map(|(id, epochs)| (id, epochs.len()))
            .collect(),
    )
}

fn err_text<T: std::fmt::Debug>(result: Result<T, S3Error>) -> String {
    let message = result.unwrap_err().to_string();
    for byte in ["11", "22", "33"] {
        assert!(
            !message.contains(&byte.repeat(8)),
            "key material in {message}"
        );
    }
    message
}

fn restore_rows(
    cfg: &S3Config,
    path: &Path,
    target: Target,
) -> Result<(Vec<String>, String), S3Error> {
    let restored = crate::s3::restore_to(cfg, path, target)?;
    let key = restored.key.as_ref().map(|key| key.opts());
    let (_db, conn) = crate::s3::open_local(path, key.as_ref())?;
    let mut stmt = conn.query("SELECT v FROM t ORDER BY id").unwrap().unwrap();
    let rows = stmt
        .run_collect_rows()
        .unwrap()
        .iter()
        .map(|row| row[0].to_string())
        .collect();
    Ok((
        rows,
        restored
            .key
            .map(|key| key.id().to_string())
            .unwrap_or_default(),
    ))
}

#[test]
fn a_rekey_moves_the_database_to_the_active_key_and_keeps_its_source() {
    let store = FaultyStore::new();
    let dir = TempDir::new();
    let old = seeded(&store, &dir, &["a", "b"]);
    let before = manifest(&store);
    let both = keyed(&store, "a", &[("r2", "22"), ("r1", "11")]);

    let rekeyed = rekey(&both, &dir.db("w.db"), None, false, false).unwrap();
    let Rekeyed::Done { from, to, epoch } = rekeyed else {
        panic!("{rekeyed:?}")
    };
    assert_eq!((from.as_str(), to.as_str()), ("r1", "r2"));
    let m = manifest(&store);
    assert_eq!(m.epoch.to_string(), epoch);
    assert_eq!(m.key, Some(key_as("r2", "22").record()));
    assert!(m.snapshot_base.is_empty(), "a delta never spans two keys");
    // retain_epochs is 0, yet the epoch the rekey ended stays, pinned.
    assert_eq!(m.history.len(), 1);
    let pinned = &m.history[0];
    assert!(pinned.pinned);
    assert_eq!(pinned.key, Some(key_as("r1", "11").record()));
    assert_eq!(pinned.ended_at_ms, Some(m.started_at_ms));
    assert!(pinned.started_at_ms <= m.started_at_ms);
    assert_eq!(pinned.epoch, before.epoch);
    assert_eq!(m.retired_keys.len(), 1);
    assert_eq!(m.retired_keys[0].id, "r1");
    // The boundary is the store's time of the seal that closed the epoch.
    let remote = Remote::new(store.clone(), PREFIX);
    let sealed = segments(&store)
        .into_iter()
        .filter(|key| key.contains(&pinned.epoch.to_string()))
        .max()
        .unwrap();
    assert_eq!(
        remote.last_modified(&sealed).unwrap(),
        Some(m.started_at_ms)
    );

    // The local copy was left warm under the new key: the next open there
    // downloads no snapshot.
    let gets = store.get_log().len();
    let db = open_encrypted(&keyed(&store, "a", &[("r2", "22")]), &dir.db("w.db")).unwrap();
    assert!(
        store.get_log()[gets..]
            .iter()
            .all(|key| !key.ends_with(&m.snapshot)),
        "{:?}",
        &store.get_log()[gets..]
    );
    assert_eq!(rows(&db), ["a", "b"]);
    db.exec("INSERT INTO t (v) VALUES ('c')").unwrap();
    assert_eq!(
        db.int("SELECT max(id) FROM t"),
        3,
        "the sequence carried over"
    );
    drop(db);

    // The old key alone no longer opens it; again, it is current.
    let message = err_text(open_encrypted(&old, &dir.db("old.db")).map(drop));
    assert!(
        message.contains("unknown key id r2 (ring: r1)"),
        "{message}"
    );
    assert_eq!(
        rekey(&both, &dir.db("w.db"), None, false, false).unwrap(),
        Rekeyed::Current
    );
    assert_eq!(manifest(&store).epoch, manifest(&store).epoch);
    assert_eq!(census(&both), (Some("r2".into()), vec![("r1".into(), 1)]));
}

#[test]
fn ring_errors_name_the_key_and_write_nothing() {
    let store = FaultyStore::new();
    let dir = TempDir::new();
    seeded(&store, &dir, &["a"]);
    let remote = Remote::new(store.clone(), PREFIX);
    let before = remote.get(MANIFEST_KEY).unwrap().unwrap().bytes;

    let unknown = keyed(&store, "b", &[("r2", "22"), ("r3", "33")]);
    let message = err_text(open_encrypted(&unknown, &dir.db("x.db")).map(drop));
    assert!(
        message.contains("unknown key id r1 (ring: r2, r3)"),
        "{message}"
    );
    // Two ids swapped: the ring's r1 is another key.
    let swapped = keyed(&store, "b", &[("r1", "22"), ("r2", "11")]);
    let message = err_text(open_encrypted(&swapped, &dir.db("y.db")).map(drop));
    assert!(
        message.contains("key r1 in the ring is not the key that encrypted this database"),
        "{message}"
    );
    // Fingerprints show by their first 16 hex digits, never 32 (the net).
    let fingerprint = key_as("r1", "11").fingerprint().to_string();
    assert!(
        message.contains(&format!("recorded {})", &fingerprint[..16])),
        "{message}"
    );
    assert!(!message.contains(&fingerprint), "{message}");
    let message = err_text(rekey(&swapped, &dir.db("z.db"), Some("r2"), false, false));
    assert!(message.contains("not the key that encrypted"), "{message}");
    assert_eq!(remote.get(MANIFEST_KEY).unwrap().unwrap().bytes, before);

    // A key_for ring asks for the key it lacks, by id.
    let mut lazy = config(&store, "b");
    lazy.unencrypted = false;
    lazy.encryption = Some(crate::s3::KeyRing::new(vec![], Some("r2".into()), true).unwrap());
    match open_encrypted(&lazy, &dir.db("lazy.db")) {
        Err(S3Error::KeyNeeded(id)) => assert_eq!(id, "r1"),
        other => panic!("{:?}", other.map(drop)),
    }
    assert_eq!(remote.get(MANIFEST_KEY).unwrap().unwrap().bytes, before);
}

#[test]
fn a_shared_storage_refuses_an_open_whose_ring_lacks_its_key() {
    let store = FaultyStore::new();
    let dir = TempDir::new();
    let cfg = seeded(&store, &dir, &["a"]);
    let path = dir.db("shared.db");
    let db = open_encrypted(&cfg, &path).unwrap();
    let lacking = keyed(&store, "a", &[("r2", "22")]);
    let message = err_text(crate::s3::prepare(&lacking, &path).map(drop));
    assert!(
        message.contains("open in this VM under key r1"),
        "{message}"
    );
    let other = keyed(&store, "a", &[("r1", "22")]);
    let message = err_text(crate::s3::prepare(&other, &path).map(drop));
    assert!(message.contains("not the key that encrypted"), "{message}");
    // A ring that holds it (as another id's neighbour) shares it.
    let holding = keyed(&store, "a", &[("r2", "22"), ("r1", "11")]);
    crate::s3::prepare(&holding, &path).unwrap();
    drop(db);
}

#[test]
fn point_in_time_restore_crosses_a_rekey_with_each_epochs_key() {
    let store = FaultyStore::new();
    let dir = TempDir::new();
    let mut cfg = keyed(&store, "a", &[("r1", "11")]);
    cfg.retain_epochs = 4;
    let db = open_encrypted(&cfg, &dir.db("a.db")).unwrap();
    db.exec("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    db.exec("INSERT INTO t VALUES (1, 'under r1')").unwrap();
    drop(db);
    std::thread::sleep(std::time::Duration::from_millis(20));
    let during_r1 = crate::s3::layout::now_ms();
    std::thread::sleep(std::time::Duration::from_millis(20));

    let mut both = keyed(&store, "a", &[("r2", "22"), ("r1", "11")]);
    both.retain_epochs = 4;
    rekey(&both, &dir.db("a.db"), None, false, false).unwrap();
    let db = open_encrypted(&both, &dir.db("a.db")).unwrap();
    db.exec("INSERT INTO t VALUES (2, 'under r2')").unwrap();
    drop(db);
    std::thread::sleep(std::time::Duration::from_millis(20));
    let during_r2 = crate::s3::layout::now_ms();

    let (before, key) = restore_rows(&both, &dir.db("p1.db"), Target::Time(during_r1)).unwrap();
    assert_eq!((before, key.as_str()), (vec!["under r1".to_string()], "r1"));
    let (after, key) = restore_rows(&both, &dir.db("p2.db"), Target::Time(during_r2)).unwrap();
    assert_eq!(
        (after, key.as_str()),
        (vec!["under r1".to_string(), "under r2".to_string()], "r2")
    );
    // The old epoch needs the old key.
    let new_only = keyed(&store, "a", &[("r2", "22")]);
    let message = err_text(restore_rows(
        &new_only,
        &dir.db("p3.db"),
        Target::Time(during_r1),
    ));
    assert!(
        message.contains("needs key r1, which is not in the ring"),
        "{message}"
    );
    // Epoch targets too.
    let pinned = manifest(&store)
        .history
        .iter()
        .find(|record| record.key.as_ref().is_some_and(|key| key.id == "r1"))
        .unwrap()
        .epoch
        .seq;
    let (rows, key) = restore_rows(&both, &dir.db("p4.db"), Target::Epoch(pinned)).unwrap();
    assert_eq!((rows.len(), key.as_str()), (1, "r1"));
}

#[test]
fn a_released_key_leaves_a_gap_that_is_refused_never_filled() {
    let store = FaultyStore::new();
    let dir = TempDir::new();
    let path = dir.db("a.db");
    let mut cfg = seeded(&store, &dir, &["under r1"]);
    cfg.retain_epochs = 2;
    std::thread::sleep(std::time::Duration::from_millis(20));
    let during_r1 = crate::s3::layout::now_ms();
    std::thread::sleep(std::time::Duration::from_millis(20));

    let mut r2 = keyed(&store, "a", &[("r2", "22"), ("r1", "11")]);
    r2.retain_epochs = 2;
    rekey(&r2, &path, None, false, false).unwrap();
    let db = open_encrypted(&r2, &path).unwrap();
    db.exec("INSERT INTO t (v) VALUES ('under r2')").unwrap();
    drop(db);
    std::thread::sleep(std::time::Duration::from_millis(20));
    let during_r2 = crate::s3::layout::now_ms();
    std::thread::sleep(std::time::Duration::from_millis(20));

    let mut r3 = keyed(&store, "a", &[("r3", "33"), ("r2", "22"), ("r1", "11")]);
    r3.retain_epochs = 2;
    rekey(&r3, &path, None, false, false).unwrap();
    let (current, retained) = census(&r3);
    assert_eq!(current.as_deref(), Some("r3"));
    assert_eq!(
        retained
            .iter()
            .map(|(id, _)| id.as_str())
            .collect::<Vec<_>>(),
        ["r1", "r2"]
    );

    // The current key can't be released.
    let message = err_text(release_key(&r3, &path, "r3", false));
    assert!(message.contains("key r3 is the current key"), "{message}");
    let r2_epochs: Vec<String> = manifest(&store)
        .history
        .iter()
        .filter(|record| record.key.as_ref().is_some_and(|key| key.id == "r2"))
        .map(|record| record.epoch.to_string())
        .collect();
    let released = release_key(&r3, &path, "r2", false).unwrap();
    assert_eq!(released.epochs, r2_epochs);
    // The release's own open retains the r3 epoch it ended (retain_epochs 2).
    let (_, retained) = census(&r3);
    assert_eq!(
        retained
            .iter()
            .map(|(id, _)| id.as_str())
            .collect::<Vec<_>>(),
        ["r1", "r3"]
    );
    // Their objects are gone.
    let remote = Remote::new(store.clone(), PREFIX);
    let objects: Vec<String> = remote
        .list("")
        .unwrap()
        .into_iter()
        .map(|object| object.key)
        .collect();
    for epoch in &r2_epochs {
        assert!(
            objects.iter().all(|key| !key.contains(epoch.as_str())),
            "{epoch} left in {objects:?}"
        );
    }
    // Nothing to release under r2 anymore.
    assert!(release_key(&r3, &path, "r2", false)
        .unwrap()
        .epochs
        .is_empty());

    // A moment under r2 is a gap now: refused, never r1's final state.
    let message = err_text(restore_rows(
        &r3,
        &dir.db("gap.db"),
        Target::Time(during_r2),
    ));
    assert!(
        message.contains("is not covered by a retained epoch"),
        "{message}"
    );
    let (rows, key) = restore_rows(&r3, &dir.db("r1.db"), Target::Time(during_r1)).unwrap();
    assert_eq!((rows, key.as_str()), (vec!["under r1".to_string()], "r1"));

    // r2 can't come back without allow_reuse; r1 not even with it while its
    // pinned epochs remain; r2 can, its epochs being released.
    let back = keyed(&store, "a", &[("r2", "22"), ("r3", "33"), ("r1", "11")]);
    let message = err_text(rekey(&back, &path, None, false, false));
    assert!(message.contains("key r2 (fingerprint"), "{message}");
    assert!(message.contains("was retired"), "{message}");
    let to_r1 = keyed(&store, "a", &[("r1", "11"), ("r3", "33")]);
    let message = err_text(rekey(&to_r1, &path, None, false, true));
    assert!(
        message.contains("key r1 still has retained epochs"),
        "{message}"
    );
    assert!(matches!(
        rekey(&back, &path, None, false, true).unwrap(),
        Rekeyed::Done { .. }
    ));
    let m = manifest(&store);
    assert_eq!(m.key.as_ref().unwrap().id, "r2");
    assert!(m.retired_keys.iter().all(|r| r.id != "r2"));
    assert!(m.retired_keys.iter().any(|r| r.id == "r3"));
}

#[test]
fn a_retired_key_stays_retired_by_id_and_by_material() {
    let store = FaultyStore::new();
    let dir = TempDir::new();
    let path = dir.db("a.db");
    seeded(&store, &dir, &["a"]);
    // r1 (11) -> r2 (22), r1 released: retired (r1, 11) and (r2, 22).
    let r2 = keyed(&store, "a", &[("r2", "22"), ("r1", "11")]);
    rekey(&r2, &path, None, false, false).unwrap();
    assert_eq!(
        release_key(&r2, &path, "r1", false).unwrap().epochs.len(),
        1
    );

    // The id r1 again, with new material: a reuse of the id.
    let fresh = keyed(&store, "a", &[("r1", "44"), ("r2", "22")]);
    let message = err_text(rekey(&fresh, &path, None, false, false));
    assert!(message.contains("was retired"), "{message}");
    assert!(matches!(
        rekey(&fresh, &path, None, false, true).unwrap(),
        Rekeyed::Done { .. }
    ));
    let m = manifest(&store);
    assert_eq!(m.key.as_ref().unwrap().id, "r1");
    // The old r1 key stays retired: its material can't come back under
    // another id without allow_reuse.
    assert_eq!(m.retired_keys.len(), 2, "{:?}", m.retired_keys);
    let old_as_r4 = keyed(&store, "a", &[("r4", "11"), ("r1", "44")]);
    let message = err_text(rekey(&old_as_r4, &path, None, false, false));
    assert!(message.contains("was retired"), "{message}");
    assert!(message.contains("as key r1"), "{message}");

    // r1 (44) -> r3 (33): retired (r1, 11), (r2, 22), (r1, 44). The
    // material of the released r1 under the id of the retained r2: refused,
    // with allow_reuse too (the id r2 still names r2's epochs).
    let r3 = keyed(&store, "a", &[("r3", "33"), ("r1", "44")]);
    rekey(&r3, &path, None, false, false).unwrap();
    let r2_with_old = keyed(&store, "a", &[("r2", "11"), ("r3", "33")]);
    let message = err_text(rekey(&r2_with_old, &path, None, false, true));
    assert!(
        message.contains("key r2 still has retained epochs"),
        "{message}"
    );
    assert_eq!(manifest(&store).key.unwrap().id, "r3");
}

#[test]
fn retain_epochs_zero_keeps_exactly_the_rekeyed_epoch_until_release() {
    let store = FaultyStore::new();
    let dir = TempDir::new();
    let path = dir.db("a.db");
    seeded(&store, &dir, &["a"]);
    let both = keyed(&store, "a", &[("r2", "22"), ("r1", "11")]);
    rekey(&both, &path, None, false, false).unwrap();
    // Checkpoints under r2 trim r2's epochs to retain_epochs (0), never the
    // pinned r1 one.
    let db = open_encrypted(&both, &path).unwrap();
    for i in 0..3 {
        db.exec(&format!("INSERT INTO t (v) VALUES ('{i}')"))
            .unwrap();
        db.checkpoint();
    }
    drop(db);
    let m = manifest(&store);
    assert_eq!(m.history.len(), 1);
    assert!(m.history[0].pinned);
    assert_eq!(census(&both), (Some("r2".into()), vec![("r1".into(), 1)]));
    release_key(&both, &path, "r1", false).unwrap();
    assert_eq!(census(&both), (Some("r2".into()), vec![]));
    assert!(manifest(&store).history.is_empty());
    // The database is whole.
    let db = open_encrypted(&keyed(&store, "a", &[("r2", "22")]), &path).unwrap();
    assert_eq!(db.int("SELECT count(*) FROM t"), 4);
}

/// Runs a rekey on its own thread, paused right before its publication.
fn paused_rekey(cfg: &S3Config, path: &Path) -> std::thread::JoinHandle<Result<Rekeyed, S3Error>> {
    hook::arm(&cfg.place(), hook::PUBLISH, Action::Pause);
    let (cfg2, path2) = (cfg.clone(), path.to_path_buf());
    let run = std::thread::spawn(move || rekey(&cfg2, &path2, None, false, false));
    while hook::state(&cfg.place()) != Some(State::Paused) {
        assert!(!run.is_finished(), "the rekey ended before its publication");
        std::thread::sleep(std::time::Duration::from_millis(5));
    }
    run
}

#[test]
fn a_takeover_during_a_rekey_fences_it_with_nothing_switched_and_no_commit_lost() {
    let store = FaultyStore::new();
    let dir = TempDir::new();
    let cfg = seeded(&store, &dir, &["before"]);
    let both = keyed(&store, "a", &[("r2", "22"), ("r1", "11")]);
    let run = paused_rekey(&both, &dir.db("r.db"));

    // Another writer (the same owner takes over at once) commits meanwhile.
    let other = open_encrypted(&cfg, &dir.db("other.db")).unwrap();
    other.exec("INSERT INTO t (v) VALUES ('during')").unwrap();
    drop(other);

    hook::release(&both.place());
    // Fenced, naming the key the manifest has now (this rekey's write could
    // have landed first: only a re-run tells).
    match run.join().unwrap() {
        Err(err @ S3Error::Overtaken { .. }) => {
            let S3Error::Overtaken { key, .. } = &err else {
                unreachable!()
            };
            assert_eq!(key.as_ref().unwrap(), &key_as("r1", "11").record());
            let message = err_text(Err::<(), _>(err));
            assert!(message.contains("names key r1"), "{message}");
        }
        other => panic!("{other:?}"),
    }
    let m = manifest(&store);
    assert_eq!(m.key.as_ref().unwrap().id, "r1");
    assert!(m.retired_keys.is_empty());
    let db = open_encrypted(&cfg, &dir.db("check.db")).unwrap();
    assert_eq!(rows(&db), ["before", "during"]);
    drop(db);
    // Its new-key snapshot was left unreferenced, and collected.
    assert_eq!(
        super::keys(&store, "snapshots/"),
        vec![manifest(&store).snapshot]
    );
}

#[test]
fn a_rekey_crashing_before_its_publication_changes_nothing_and_a_rerun_succeeds() {
    let store = FaultyStore::new();
    let dir = TempDir::new();
    let cfg = seeded(&store, &dir, &["a"]);
    let both = keyed(&store, "a", &[("r2", "22"), ("r1", "11")]);
    hook::arm(&both.place(), hook::PUBLISH, Action::Crash);
    let message = err_text(rekey(&both, &dir.db("r.db"), None, false, false));
    assert!(message.contains("crashed"), "{message}");
    assert_eq!(manifest(&store).key.as_ref().unwrap().id, "r1");

    // The crashed run still holds the lease: a re-run is refused, unless
    // forced.
    let message = err_text(rekey(&both, &dir.db("r.db"), None, false, false));
    assert!(message.contains("lease held"), "{message}");
    // Under the old key the database works (its owner takes over at once).
    let db = open_encrypted(&cfg, &dir.db("w.db")).unwrap();
    db.exec("INSERT INTO t (v) VALUES ('b')").unwrap();
    drop(db);
    assert!(matches!(
        rekey(&both, &dir.db("r.db"), None, false, false).unwrap(),
        Rekeyed::Done { .. }
    ));
    let db = open_encrypted(&both, &dir.db("r.db")).unwrap();
    assert_eq!(rows(&db), ["a", "b"]);
}

#[test]
fn a_lost_answer_on_the_publication_is_adopted_when_it_landed() {
    // Landed with its answer lost, or failed while the manifest is still the
    // one the open holds (resent).
    for fault in [Fault::FailAfter, Fault::LandThenPrecondition, Fault::Fail] {
        let store = FaultyStore::new();
        let dir = TempDir::new();
        seeded(&store, &dir, &["a"]);
        let both = keyed(&store, "a", &[("r2", "22"), ("r1", "11")]);
        let run = paused_rekey(&both, &dir.db("r.db"));
        store.inject(Op::Put, MANIFEST_KEY, fault, 1);
        hook::release(&both.place());
        assert!(
            matches!(run.join().unwrap(), Ok(Rekeyed::Done { .. })),
            "{fault:?}"
        );
        assert_eq!(manifest(&store).key.as_ref().unwrap().id, "r2");
    }

    // Unreadable afterwards: the outcome is unknown, and said so.
    let store = FaultyStore::new();
    let dir = TempDir::new();
    seeded(&store, &dir, &["a"]);
    let both = keyed(&store, "a", &[("r2", "22"), ("r1", "11")]);
    let run = paused_rekey(&both, &dir.db("r.db"));
    store.inject(Op::Put, MANIFEST_KEY, Fault::FailAfter, 1);
    store.inject(Op::Get, MANIFEST_KEY, Fault::Fail, 4);
    hook::release(&both.place());
    match run.join().unwrap() {
        Err(S3Error::Indeterminate(message)) => {
            assert!(message.contains("unknown"), "{message}")
        }
        other => panic!("{other:?}"),
    }
}

#[test]
fn a_replica_follows_a_rekey_at_its_next_refresh() {
    let store = FaultyStore::new();
    let dir = TempDir::new();
    seeded(&store, &dir, &["a"]);
    let mut replica = keyed(&store, "r", &[("r2", "22"), ("r1", "11")]);
    replica.replica = true;
    let staged = crate::s3::replica::stage(&replica, &dir.db("rep.db")).unwrap();
    assert_eq!(staged.key.as_ref().unwrap().id(), "r1");
    drop(staged);
    let both = keyed(&store, "a", &[("r2", "22"), ("r1", "11")]);
    rekey(&both, &dir.db("w.db"), None, false, false).unwrap();
    let staged = crate::s3::replica::stage(&replica, &dir.db("rep.db")).unwrap();
    assert_eq!(staged.key.as_ref().unwrap().id(), "r2");
    let key = staged.key.as_ref().map(|key| key.opts());
    let installed = dir.db("installed.db");
    staged.install(&installed).unwrap();
    let (_db, conn) = crate::s3::open_local(&installed, key.as_ref()).unwrap();
    let mut stmt = conn.query("SELECT count(*) FROM t").unwrap().unwrap();
    assert_eq!(stmt.run_collect_rows().unwrap()[0][0].to_string(), "1");
    // A replica whose ring lacks the new key is told which one.
    let mut old_only = keyed(&store, "r", &[("r1", "11")]);
    old_only.replica = true;
    let message = err_text(crate::s3::replica::stage(&old_only, &dir.db("rep2.db")).map(drop));
    assert!(
        message.contains("needs key r2, which is not in the ring"),
        "{message}"
    );
}

#[test]
fn a_rekey_keeps_the_cipher_and_needs_both_keys() {
    let store = FaultyStore::new();
    let dir = TempDir::new();
    seeded(&store, &dir, &["a"]);
    let mut other_cipher = keyed(&store, "a", &[("r1", "11")]);
    let aes = crate::s3::Key::new("r2", "aes256gcm", &"22".repeat(32)).unwrap();
    other_cipher.encryption =
        Some(crate::s3::KeyRing::new(vec![aes, key_as("r1", "11")], None, false).unwrap());
    let message = err_text(rekey(&other_cipher, &dir.db("r.db"), None, false, false));
    assert!(message.contains("a rekey keeps the cipher"), "{message}");
    let missing = keyed(&store, "a", &[("r1", "11")]);
    let message = err_text(rekey(&missing, &dir.db("r.db"), Some("r9"), false, false));
    assert!(
        message.contains("rekey: key r9 is not in the ring (ring: r1)"),
        "{message}"
    );
    assert_eq!(manifest(&store).key.as_ref().unwrap().id, "r1");
}

#[test]
fn manifests_of_older_versions_are_refused_clearly() {
    let store = FaultyStore::new();
    let dir = TempDir::new();
    let cfg = seeded(&store, &dir, &["a"]);
    let remote = Remote::new(store.clone(), PREFIX);
    let mut value: serde_json::Value =
        serde_json::from_slice(&remote.get(MANIFEST_KEY).unwrap().unwrap().bytes).unwrap();
    value["version"] = 2.into();
    remote
        .put(
            MANIFEST_KEY,
            serde_json::to_vec(&value).unwrap().into(),
            crate::s3::remote::Put::Overwrite,
        )
        .unwrap();
    let message = err_text(open_encrypted(&cfg, &dir.db("old.db")).map(drop));
    assert!(message.contains("manifest version 2 was written by sediment 0.1.0-beta.6 or older"));
}

#[test]
fn a_rekey_publishes_a_full_snapshot_and_later_deltas_chain_on_it_only() {
    let store = FaultyStore::new();
    let dir = TempDir::new();
    let path = dir.db("a.db");
    let mut cfg = seeded(&store, &dir, &["a"]);
    cfg.retain_epochs = 3;
    // A chain under r1 first: a file large enough for small changes to
    // be deltas.
    let db = open_encrypted(&cfg, &path).unwrap();
    db.exec("CREATE TABLE pad(x BLOB)").unwrap();
    for _ in 0..8 {
        db.exec("INSERT INTO pad SELECT randomblob(1000) FROM generate_series(1, 100)")
            .unwrap();
    }
    db.checkpoint();
    for i in 0..3 {
        db.exec(&format!("INSERT INTO t (v) VALUES ('{i}')"))
            .unwrap();
        db.checkpoint();
    }
    drop(db);
    assert!(!manifest(&store).snapshot_base.is_empty(), "r1 has a chain");

    let mut both = keyed(&store, "a", &[("r2", "22"), ("r1", "11")]);
    both.retain_epochs = 3;
    rekey(&both, &path, None, false, false).unwrap();
    let rekeyed = manifest(&store);
    assert!(rekeyed.snapshot.ends_with(".db"), "{}", rekeyed.snapshot);
    assert!(rekeyed.snapshot_base.is_empty());

    let db = open_encrypted(&both, &path).unwrap();
    db.exec("INSERT INTO t (v) VALUES ('after')").unwrap();
    db.checkpoint();
    drop(db);
    let m = manifest(&store);
    let chain = m.current().chain();
    assert!(m.snapshot.ends_with(".delta"), "{}", m.snapshot);
    assert_eq!(
        chain[0].key, rekeyed.snapshot,
        "the chain starts at the rekey"
    );
    assert_eq!(m.key.as_ref().unwrap().id, "r2");
    let db = open_encrypted(&both, &dir.db("b.db")).unwrap();
    assert_eq!(db.int("SELECT count(*) FROM t"), 5);
}

#[test]
fn a_rekey_fenced_at_its_takeover_names_the_current_key() {
    let store = FaultyStore::new();
    let dir = TempDir::new();
    seeded(&store, &dir, &["a"]);
    let remote = Remote::new(store.clone(), PREFIX);
    let err = crate::s3::overtaken_at_takeover(&remote, S3Error::Conflict("manifest.json".into()));
    match &err {
        S3Error::Overtaken { key, .. } => {
            assert_eq!(key.as_ref().unwrap(), &key_as("r1", "11").record())
        }
        other => panic!("{other:?}"),
    }
    let message = err_text(Err::<(), _>(err));
    assert!(message.contains("switched nothing"), "{message}");
}

/// A key_for ring holding `keys` (`(id, key byte)`), `active` resolved on
/// demand.
fn lazy(store: &Arc<FaultyStore>, keys: &[(&str, &str)], active: &str) -> S3Config {
    let mut cfg = config(store, "a");
    cfg.unencrypted = false;
    let keys = keys.iter().map(|(id, byte)| key_as(id, byte)).collect();
    cfg.encryption = Some(crate::s3::KeyRing::new(keys, Some(active.into()), true).unwrap());
    cfg
}

/// Runs `run` and asserts it downloaded no snapshot of the current epoch.
fn downloads_no_snapshot<T>(store: &Arc<FaultyStore>, run: impl FnOnce() -> T) -> T {
    let snapshot = manifest(store).snapshot;
    let gets = store.get_log().len();
    let result = run();
    let downloaded = &store.get_log()[gets..];
    assert!(
        downloaded.iter().all(|key| !key.ends_with(&snapshot)),
        "{downloaded:?}"
    );
    result
}

#[test]
fn a_key_for_ring_and_a_no_op_rekey_keep_the_copy_warm() {
    let store = FaultyStore::new();
    let dir = TempDir::new();
    let seed = seeded(&store, &dir, &["a"]);
    let path = dir.db("seed.db");
    let sidecar = crate::s3::warm::sidecar_path(&path);
    assert!(sidecar.exists(), "the clean close left the copy warm");

    // A key_for ring's first round needs the key and stops before it
    // touches the local files: the copy stays warm for the second.
    match open_encrypted(&lazy(&store, &[], "r1"), &path) {
        Err(S3Error::KeyNeeded(id)) => assert_eq!(id, "r1"),
        other => panic!("{:?}", other.map(drop)),
    }
    assert!(sidecar.exists());
    let db = downloads_no_snapshot(&store, || {
        open_encrypted(&lazy(&store, &[("r1", "11")], "r1"), &path).unwrap()
    });
    assert_eq!(rows(&db), ["a"]);
    drop(db);
    assert!(sidecar.exists());

    // A rekey with nothing to do changes nothing local either.
    assert_eq!(
        rekey(&seed, &path, None, false, false).unwrap(),
        Rekeyed::Current
    );
    assert!(sidecar.exists());

    // A key_for rekey asks for the current key, then the target, and
    // rekeys the warm copy without downloading it.
    for (keys, needed) in [(&[][..], "r1"), (&[("r1", "11")][..], "r2")] {
        match rekey(&lazy(&store, keys, "r2"), &path, None, false, false) {
            Err(S3Error::KeyNeeded(id)) => assert_eq!(id, needed),
            other => panic!("{other:?}"),
        }
        assert!(sidecar.exists());
    }
    let both = lazy(&store, &[("r1", "11"), ("r2", "22")], "r2");
    let rekeyed = downloads_no_snapshot(&store, || rekey(&both, &path, None, false, false));
    assert!(matches!(rekeyed, Ok(Rekeyed::Done { .. })), "{rekeyed:?}");
    let db = downloads_no_snapshot(&store, || open_encrypted(&both, &path).unwrap());
    assert_eq!(rows(&db), ["a"]);
}
