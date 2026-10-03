use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use super::faulty::{Fault, FaultyStore, Op};
use super::{
    config, encryption, keys, manifest, open_db, open_encrypted, open_plain, TempDir, PREFIX,
};
use crate::s3::layout::MANIFEST_KEY;
use crate::s3::remote::{Put, Remote};
use crate::s3::{import, ImportOptions, Imported, S3Error, Verify};

fn opts(verify: Verify) -> ImportOptions {
    ImportOptions {
        verify,
        source_encryption: None,
    }
}

fn checksum() -> ImportOptions {
    opts(Verify::Checksum)
}

/// A plain (WAL) database written by turso, as an Ecto app makes it: an
/// AUTOINCREMENT table whose top rows were deleted, an index, and rows only
/// in its WAL.
fn source(dir: &TempDir) -> PathBuf {
    let path = dir.db("app.db");
    let plain = open_plain(&path);
    for sql in [
        "CREATE TABLE t(id INTEGER PRIMARY KEY AUTOINCREMENT, v TEXT NOT NULL)",
        "CREATE INDEX t_v ON t(v)",
        "BEGIN",
    ] {
        plain.conn.execute(sql).unwrap();
    }
    for id in 1..=500 {
        plain
            .conn
            .execute(format!("INSERT INTO t (v) VALUES ('v{id}')"))
            .unwrap();
    }
    plain.conn.execute("COMMIT").unwrap();
    plain
        .conn
        .execute("PRAGMA wal_checkpoint(TRUNCATE)")
        .unwrap();
    plain
        .conn
        .execute("INSERT INTO t (v) VALUES ('in the wal')")
        .unwrap();
    plain.conn.execute("DELETE FROM t WHERE id > 501").unwrap();
    plain
        .conn
        .execute("INSERT INTO t (v) VALUES ('deleted'), ('deleted')")
        .unwrap();
    plain.conn.execute("DELETE FROM t WHERE id > 501").unwrap();
    // Dropped without close: the last rows stay in the WAL.
    path
}

fn files(dir: &Path) -> Vec<(String, Vec<u8>)> {
    let mut files: Vec<_> = std::fs::read_dir(dir)
        .unwrap()
        .flatten()
        .map(|entry| {
            (
                entry.file_name().to_string_lossy().into_owned(),
                std::fs::read(entry.path()).unwrap(),
            )
        })
        .collect();
    files.sort();
    files
}

fn assert_config_err(result: Result<Imported, S3Error>, needle: &str) {
    match result {
        Err(S3Error::Config(msg)) => assert!(msg.contains(needle), "{msg}"),
        Err(other) => panic!("unexpected error {other}"),
        Ok(imported) => panic!("expected an error containing {needle:?}, got {imported:?}"),
    }
}

/// Opens the imported database from S3 and checks the source's rows and
/// that the next AUTOINCREMENT id continues after the source's sequence.
fn check_restored(store: &Arc<FaultyStore>, dir: &TempDir, name: &str) {
    let db = open_db(&config(store, "app"), &dir.db(name)).unwrap();
    assert_eq!(db.int("SELECT count(*) FROM t"), 501);
    assert_eq!(
        db.rows("SELECT v FROM t WHERE id = 501")[0][0].to_string(),
        "in the wal"
    );
    db.exec("INSERT INTO t (v) VALUES ('after')").unwrap();
    assert_eq!(db.int("SELECT max(id) FROM t"), 504);
    assert_eq!(db.int("SELECT count(*) FROM t"), 502);
    assert_eq!(db.int("SELECT count(*) FROM t WHERE v = 'v1'"), 1);
}

#[test]
fn an_imported_database_opens_from_s3_and_the_source_is_unchanged() {
    for verify in [Verify::Checksum, Verify::Restore] {
        let store = FaultyStore::new();
        let dir = TempDir::new();
        let path = source(&dir);
        let before = files(&dir.0);
        assert!(before.iter().any(|(name, _)| name == "app.db-wal"));

        let imported = import(&config(&store, "importer"), &path, &opts(verify)).unwrap();
        assert_eq!((imported.objects, imported.rows), (2, 501), "{imported:?}");
        assert!(imported.sequences_not_advanced.is_empty());
        assert!(imported.epoch.starts_with("00000000000000000000-"));
        // Byte for byte, and no temporary files left.
        assert_eq!(files(&dir.0), before);
        assert_eq!(manifest(&store).epoch.seq, 0);
        // The importer released the lease: the open doesn't wait for it.
        check_restored(&store, &dir, "restored.db");
    }
}

#[test]
fn a_file_written_by_sqlite_keeps_its_schema_rows_and_sequences() {
    let store = FaultyStore::new();
    let dir = TempDir::new();
    let path = dir.db("sqlite_app.db");
    std::fs::write(&path, include_bytes!("fixtures/sqlite_app.db")).unwrap();
    let imported = import(&config(&store, "importer"), &path, &checksum()).unwrap();
    assert_eq!(imported.sequences_not_advanced, ["empty_checked"]);
    // 9 tables, 1 index (the UNIQUE ones are automatic), 1 view, 1 trigger;
    // authors 2, posts 3, plain 2, audit 5 (the trigger fired in SQLite),
    // users 2
    assert_eq!((imported.objects, imported.rows), (12, 14), "{imported:?}");
    assert_eq!(
        std::fs::read(&path).unwrap(),
        include_bytes!("fixtures/sqlite_app.db")
    );

    let db = open_db(&config(&store, "app"), &dir.db("restored.db")).unwrap();
    let ids = |sql: &str| -> Vec<String> {
        db.rows(sql)
            .iter()
            .map(|row| {
                row.iter()
                    .map(|v| v.to_string())
                    .collect::<Vec<_>>()
                    .join(":")
            })
            .collect()
    };
    assert_eq!(
        ids("SELECT id, title, score FROM posts"),
        ["1:one:1.5", "2:two:2.5", "3:three:3.0"]
    );
    assert_eq!(ids("SELECT hex(body) FROM posts WHERE id = 1"), ["00FF"]);
    assert_eq!(ids("SELECT rowid, k FROM plain"), ["2:b", "3:c"]);
    assert_eq!(db.int("PRAGMA user_version"), 7);
    // Sequences continue after the source's: posts was at 5 (4 and 5 deleted).
    db.exec("INSERT INTO posts (author_id, title) VALUES (2, 'six')")
        .unwrap();
    assert_eq!(db.int("SELECT max(id) FROM posts"), 6);
    db.exec("INSERT INTO empty_nullable (v) VALUES ('w')")
        .unwrap();
    assert_eq!(db.int("SELECT max(id) FROM empty_nullable"), 4);
    db.exec("INSERT INTO empty_seq (v) VALUES ('w')").unwrap();
    assert_eq!(db.int("SELECT max(id) FROM empty_seq"), 3);
    db.exec("INSERT INTO sessions (token, user_id, inserted_at) VALUES (x'01', 1, 't')")
        .unwrap();
    assert_eq!(db.int("SELECT max(id) FROM sessions"), 3);
    db.exec("INSERT INTO empty_checked (v) VALUES ('long enough')")
        .unwrap();
    assert_eq!(
        db.int("SELECT max(id) FROM empty_checked"),
        1,
        "reported, not advanced"
    );
    // users: UNIQUE email, newest row deleted
    assert_eq!(ids("SELECT id, email FROM users"), ["1:a@x", "2:b@x"]);
    db.exec("INSERT INTO users (email, inserted_at, updated_at) VALUES ('d@x', 't', 't')")
        .unwrap();
    assert_eq!(db.int("SELECT max(id) FROM users"), 4);
    assert!(db
        .exec("INSERT INTO users (email, inserted_at, updated_at) VALUES ('a@x', 't', 't')")
        .is_err());
    // The trigger, the view, the constraints and the indexes came along.
    assert_eq!(ids("SELECT msg FROM audit WHERE rowid > 5"), ["post six"]);
    assert_eq!(
        ids("SELECT name, title FROM post_titles WHERE id = 6"),
        ["bob:six"]
    );
    assert!(db
        .exec("INSERT INTO posts (author_id, title) VALUES (1, '')")
        .is_err());
    assert!(db
        .exec("INSERT INTO authors (name) VALUES ('ann')")
        .is_err());
    assert_eq!(
        db.int("SELECT count(*) FROM sqlite_schema WHERE name = 'posts_author'"),
        1
    );
    db.exec("PRAGMA foreign_keys = ON").unwrap();
    db.exec("DELETE FROM authors WHERE id = 1").unwrap();
    assert_eq!(ids("SELECT id FROM posts"), ["2", "6"]);
}

#[test]
fn an_encrypted_target_needs_its_key_and_the_source_may_be_encrypted_too() {
    let dir = TempDir::new();
    let plain_source = source(&dir);

    // A plaintext source into an encrypted database
    let store = FaultyStore::new();
    let mut cfg = config(&store, "importer");
    cfg.encryption = Some(encryption("ab"));
    import(&cfg, &plain_source, &opts(Verify::Restore)).unwrap();
    assert_eq!(manifest(&store).encrypted, Some(true));
    let db = open_encrypted(&cfg, &dir.db("restored.db")).unwrap();
    assert_eq!(db.int("SELECT count(*) FROM t"), 501);
    db.exec("INSERT INTO t (v) VALUES ('after')").unwrap();
    assert_eq!(db.int("SELECT max(id) FROM t"), 504);
    drop(db);
    let mut wrong = cfg.clone();
    wrong.encryption = Some(encryption("cd"));
    assert!(open_encrypted(&wrong, &dir.db("wrong.db")).is_err());
    let mut none = cfg.clone();
    none.encryption = None;
    assert!(open_encrypted(&none, &dir.db("none.db")).is_err());

    // An encrypted source (turso, key "cd") into an encrypted (key "ab") and
    // a plaintext database
    let encrypted_source = dir.db("enc.db");
    {
        let key = encryption("cd");
        let (_db, conn) = crate::s3::open_local(&encrypted_source, Some(&key)).unwrap();
        conn.execute("CREATE TABLE e(id INTEGER PRIMARY KEY AUTOINCREMENT, v TEXT)")
            .unwrap();
        conn.execute("INSERT INTO e (v) VALUES ('a'), ('b')")
            .unwrap();
        conn.close().unwrap();
    }
    for target in [Some(encryption("ab")), None] {
        let store = FaultyStore::new();
        let mut cfg = config(&store, "importer");
        cfg.encryption = target.clone();
        assert_config_err(
            import(&cfg, &encrypted_source, &checksum()),
            "for an encrypted database give its key",
        );
        let mut bad = checksum();
        bad.source_encryption = Some(encryption("ab"));
        assert_config_err(
            import(&cfg, &encrypted_source, &bad),
            "the given source key",
        );
        let mut good = checksum();
        good.source_encryption = Some(encryption("cd"));
        import(&cfg, &encrypted_source, &good).unwrap();
        let db = open_encrypted(&cfg, &dir.db("from-enc.db")).unwrap();
        db.exec("INSERT INTO e (v) VALUES ('c')").unwrap();
        assert_eq!(db.int("SELECT max(id) FROM e"), 3);
        drop(db);
        std::fs::remove_file(dir.db("from-enc.db")).unwrap();
        let _ = std::fs::remove_file(dir.db("from-enc.db-log"));
    }
}

#[test]
fn a_prefix_that_holds_a_database_is_refused() {
    let store = FaultyStore::new();
    let dir = TempDir::new();
    let path = source(&dir);
    drop(open_db(&config(&store, "a"), &dir.db("other.db")).unwrap());
    let before = files(&dir.0);
    assert_config_err(
        import(&config(&store, "importer"), &path, &checksum()),
        "already holds a database",
    );
    assert_eq!(files(&dir.0), before);
}

#[test]
fn a_failed_upload_leaves_nothing_an_open_uses_and_a_rerun_works() {
    let store = FaultyStore::new();
    let dir = TempDir::new();
    let path = source(&dir);
    let before = files(&dir.0);
    let cfg = config(&store, "importer");

    // The snapshot upload fails, then the manifest PUT does (the snapshot is
    // left behind, like an importer that died before its manifest).
    store.inject(Op::Put, "snapshots/", Fault::Fail, 1);
    assert!(import(&cfg, &path, &checksum()).is_err());
    store.inject(Op::Put, MANIFEST_KEY, Fault::Fail, 1);
    assert!(import(&cfg, &path, &checksum()).is_err());
    assert_eq!(keys(&store, "snapshots/").len(), 1);
    assert_eq!(
        files(&dir.0),
        before,
        "temporary files removed, source intact"
    );

    import(&cfg, &path, &checksum()).unwrap();
    assert_eq!(files(&dir.0), before);
    check_restored(&store, &dir, "restored.db");
}

#[test]
fn a_lost_answer_to_the_manifest_put_is_still_an_import() {
    let store = FaultyStore::new();
    let dir = TempDir::new();
    let path = source(&dir);
    store.inject(Op::Put, MANIFEST_KEY, Fault::FailAfter, 1);
    import(&config(&store, "importer"), &path, &checksum()).unwrap();
    check_restored(&store, &dir, "restored.db");
}

#[test]
fn losing_the_manifest_race_is_refused() {
    let store = FaultyStore::new();
    let dir = TempDir::new();
    let path = source(&dir);
    let before = files(&dir.0);
    // Another creator (one that ignored the lease) gets its manifest in
    // while the import's create-only PUT is in flight.
    store.inject(
        Op::Put,
        MANIFEST_KEY,
        Fault::Delay(Duration::from_millis(1500)),
        1,
    );
    let racer = {
        let store = store.clone();
        std::thread::spawn(move || {
            while !store
                .put_log()
                .iter()
                .any(|key| key.ends_with(MANIFEST_KEY))
            {
                std::thread::sleep(Duration::from_millis(10));
            }
            let remote = Remote::new(store.clone(), PREFIX);
            remote
                .put(MANIFEST_KEY, b"{}".to_vec().into(), Put::Create)
                .unwrap();
        })
    };
    let result = import(&config(&store, "importer"), &path, &checksum());
    racer.join().unwrap();
    assert_config_err(result, "already holds a database");
    assert_eq!(files(&dir.0), before);
}

#[test]
fn files_that_cant_be_imported_are_refused_and_left_alone() {
    let store = FaultyStore::new();
    let dir = TempDir::new();
    let cfg = config(&store, "importer");

    let missing = dir.db("missing.db");
    assert_config_err(import(&cfg, &missing, &checksum()), "can't import");

    let garbage = dir.db("garbage.db");
    std::fs::write(&garbage, vec![7u8; 8192]).unwrap();
    assert_config_err(
        import(&cfg, &garbage, &checksum()),
        "can't be read as a database",
    );

    let journal = source(&dir);
    std::fs::write(dir.db("app.db-journal"), b"hot").unwrap();
    assert_config_err(import(&cfg, &journal, &checksum()), "hot rollback journal");
    std::fs::remove_file(dir.db("app.db-journal")).unwrap();

    // Too large for the multipart limit (a sparse file: checked before copying).
    let huge = dir.db("huge.db");
    std::fs::File::create(&huge)
        .unwrap()
        .set_len(crate::s3::import::MAX_SNAPSHOT_BYTES + 1)
        .unwrap();
    assert_config_err(import(&cfg, &huge, &checksum()), "5 TiB");
    std::fs::remove_file(&huge).unwrap();

    assert!(
        files(&dir.0)
            .iter()
            .all(|(name, _)| !name.contains(".import-")),
        "temporary files removed"
    );
    assert!(store.put_log().is_empty(), "{:?}", store.put_log());
}

#[test]
fn a_database_open_with_s3_in_this_vm_is_refused() {
    let store = FaultyStore::new();
    let dir = TempDir::new();
    let db = open_db(&config(&store, "a"), &dir.db("live.db")).unwrap();
    assert_config_err(
        import(
            &config(&FaultyStore::new(), "importer"),
            &dir.db("live.db"),
            &checksum(),
        ),
        "open with :s3",
    );
    drop(db);
}

#[test]
fn temporary_files_are_private_while_they_exist() {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let store = FaultyStore::new();
        let dir = TempDir::new();
        let path = source(&dir);
        // Hold the snapshot upload so the temporary files can be looked at.
        store.inject(
            Op::Put,
            "snapshots/",
            Fault::Delay(Duration::from_millis(800)),
            1,
        );
        let seen = {
            let dir = dir.0.clone();
            std::thread::spawn(move || {
                std::thread::sleep(Duration::from_millis(400));
                std::fs::read_dir(&dir)
                    .unwrap()
                    .flatten()
                    .filter(|e| e.file_name().to_string_lossy().contains(".import-"))
                    .map(|e| e.metadata().unwrap().permissions().mode() & 0o777)
                    .collect::<Vec<_>>()
            })
        };
        import(&config(&store, "importer"), &path, &checksum()).unwrap();
        let modes = seen.join().unwrap();
        assert!(!modes.is_empty());
        assert!(modes.iter().all(|&mode| mode == 0o600), "{modes:?}");
    }
}

/// Import throughput by AUTOINCREMENT batch size, against an
/// in-memory store: `cargo test --release --lib import_throughput -- --ignored --nocapture`
/// (IMPORT_BENCH_ROWS, default 40,000; IMPORT_BENCH_ROW_BYTES, default 1,000;
/// IMPORT_BENCH_BATCHES; IMPORT_BENCH_CHECKPOINTS, rows between checkpoints,
/// 0 for none).
#[test]
#[ignore]
fn import_throughput() {
    let rows: u64 = std::env::var("IMPORT_BENCH_ROWS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(40_000);
    let row_bytes: u64 = std::env::var("IMPORT_BENCH_ROW_BYTES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1_000);
    let checkpoints: Vec<Option<u64>> = match std::env::var("IMPORT_BENCH_CHECKPOINTS") {
        Ok(list) => list.split(',').map(|c| c.trim().parse().ok()).collect(),
        Err(_) => vec![None],
    };
    let dir = TempDir::new();
    for (name, ddl) in [
        (
            "plain",
            "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT NOT NULL)",
        ),
        (
            "autoincrement",
            "CREATE TABLE t(id INTEGER PRIMARY KEY AUTOINCREMENT, v TEXT NOT NULL)",
        ),
    ] {
        let path = dir.db(&format!("{name}.db"));
        {
            let plain = open_plain(&path);
            plain.conn.execute(ddl).unwrap();
            plain.conn.execute("BEGIN").unwrap();
            for _ in 0..rows {
                plain
                    .conn
                    .execute(format!(
                        "INSERT INTO t (v) VALUES (hex(randomblob({})))",
                        (row_bytes / 2).max(1)
                    ))
                    .unwrap();
            }
            plain.conn.execute("COMMIT").unwrap();
            plain.conn.close().unwrap();
        }
        let mb = std::fs::metadata(&path).unwrap().len() as f64 / 1e6;
        // IMPORT_BENCH_BATCHES=10000,500,200 compares AUTOINCREMENT batches
        let batches: Vec<Option<u64>> = match std::env::var("IMPORT_BENCH_BATCHES") {
            _ if name == "plain" => vec![None],
            Ok(list) => list.split(',').map(|b| b.trim().parse().ok()).collect(),
            Err(_) => vec![None],
        };
        for batch in batches {
            for &checkpoint in &checkpoints {
                crate::s3::import::AUTOINCREMENT_BATCH_OVERRIDE.with(|o| o.set(batch));
                crate::s3::import::AUTOINCREMENT_CHECKPOINT_OVERRIDE.with(|o| o.set(checkpoint));
                let store = FaultyStore::new();
                let started = std::time::Instant::now();
                import(&config(&store, "importer"), &path, &checksum()).unwrap();
                let secs = started.elapsed().as_secs_f64();
                eprintln!(
                    "IMPORT_BENCH {name} batch={batch:?} checkpoint={checkpoint:?} rows={rows} \
                     row_bytes={row_bytes} {mb:.1} MB {secs:.1} s {:.2} MB/s {:.0} rows/s",
                    mb / secs,
                    rows as f64 / secs
                );
            }
        }
        crate::s3::import::AUTOINCREMENT_CHECKPOINT_OVERRIDE.with(|o| o.set(None));
        crate::s3::import::AUTOINCREMENT_BATCH_OVERRIDE.with(|o| o.set(None));
    }
}

#[test]
fn small_autoincrement_rows_import_in_linear_time() {
    // Without checkpoints during the copy, each insert walks every
    // sqlite_sequence version since the last one: 20,000 rows of ~20 bytes
    // took 72 s instead of 3. Release builds only.
    if cfg!(debug_assertions) {
        return;
    }
    let dir = TempDir::new();
    let path = dir.db("small.db");
    {
        let plain = open_plain(&path);
        plain
            .conn
            .execute("CREATE TABLE t(id INTEGER PRIMARY KEY AUTOINCREMENT, v TEXT NOT NULL)")
            .unwrap();
        plain.conn.execute("BEGIN").unwrap();
        for i in 0..20_000 {
            plain
                .conn
                .execute(format!("INSERT INTO t (v) VALUES ('row {i}')"))
                .unwrap();
        }
        plain.conn.execute("COMMIT").unwrap();
        plain.conn.close().unwrap();
    }
    let store = FaultyStore::new();
    let started = std::time::Instant::now();
    import(&config(&store, "importer"), &path, &checksum()).unwrap();
    let secs = started.elapsed().as_secs_f64();
    assert!(
        secs < 30.0,
        "20,000 small AUTOINCREMENT rows took {secs:.1} s"
    );
    let db = open_db(&config(&store, "app"), &dir.db("restored.db")).unwrap();
    db.exec("INSERT INTO t (v) VALUES ('next')").unwrap();
    assert_eq!(db.int("SELECT max(id) FROM t"), 20_001);
}
