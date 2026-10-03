//! End-to-end tests against a real S3 server: SeaweedFS on 127.0.0.1:8333 by
//! default, or S3_TEST_ENDPOINT / S3_TEST_ACCESS_KEY_ID /
//! S3_TEST_SECRET_ACCESS_KEY / S3_TEST_BUCKET (for example MinIO). Skipped
//! when nothing listens there.
//!
//! Every SeaweedFS bucket is a collection that reserves 7 of the server's few
//! volume slots, so all tests share one bucket and use unique prefixes.

use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::Duration;

use super::super::{S3Config, S3Error};
use super::{open_db, TempDir};

fn env(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

fn endpoint() -> String {
    env("S3_TEST_ENDPOINT", "http://127.0.0.1:8333")
}

fn bucket() -> String {
    env("S3_TEST_BUCKET", "sediment-tests")
}

/// Ensures the shared bucket exists and returns a fresh prefix for `test`
/// (under S3_TEST_PREFIX_ROOT), or None when the server isn't running.
/// Servers that require signed requests (403), and remote ones (HTTPS), must
/// have the bucket already.
fn prefix(test: &str) -> Option<String> {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let root = env("S3_TEST_PREFIX_ROOT", "");
    let prefix = format!("{}/{test}/{nanos:x}/", root.trim_end_matches('/'));
    let endpoint = endpoint();
    if endpoint.starts_with("https://") {
        return Some(prefix);
    }
    let addr = endpoint.trim_start_matches("http://").trim_end_matches('/');
    let socket = std::net::ToSocketAddrs::to_socket_addrs(addr)
        .ok()?
        .next()?;
    let mut stream = TcpStream::connect_timeout(&socket, Duration::from_secs(1))
        .map_err(|_| eprintln!("no S3 server on {addr}; skipping {test}"))
        .ok()?;
    let bucket = bucket();
    write!(
        stream,
        "PUT /{bucket} HTTP/1.1\r\nHost: {addr}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
    )
    .unwrap();
    let mut response = String::new();
    stream.read_to_string(&mut response).unwrap();
    assert!(
        ["200", "403", "409"]
            .iter()
            .any(|code| response.starts_with(&format!("HTTP/1.1 {code}"))),
        "{response}"
    );
    Some(prefix)
}

fn config(prefix: &str, owner: &str) -> S3Config {
    S3Config::from_pairs([
        ("bucket", bucket().as_str()),
        ("prefix", prefix),
        ("endpoint", endpoint().as_str()),
        ("region", env("S3_TEST_REGION", "us-east-1").as_str()),
        (
            "access_key_id",
            env("S3_TEST_ACCESS_KEY_ID", "any").as_str(),
        ),
        (
            "secret_access_key",
            env("S3_TEST_SECRET_ACCESS_KEY", "any").as_str(),
        ),
        ("owner", owner),
        ("lease_ttl_ms", "5000"),
        // These tests check that every acknowledged commit is in S3, which
        // is what sync durability promises (from_pairs defaults to async).
        ("durability", "sync"),
        ("encryption", "false"),
    ])
    .unwrap()
}

#[test]
fn seaweed_round_trip_with_checkpoint() {
    let Some(prefix) = prefix("roundtrip") else {
        return;
    };
    let dir = TempDir::new();
    let db = open_db(&config(&prefix, "a"), &dir.db("a.db")).unwrap();
    db.exec("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    for i in 0..10 {
        db.exec(&format!("INSERT INTO t VALUES ({i}, 'x')"))
            .unwrap();
    }
    db.checkpoint();
    for i in 10..15 {
        db.exec(&format!("INSERT INTO t VALUES ({i}, 'y')"))
            .unwrap();
    }
    assert_eq!(
        db.storage
            .info()
            .epoch
            .split('-')
            .next()
            .unwrap()
            .parse::<u64>()
            .unwrap(),
        1
    );
    // The old epoch was collected through the server's DeleteObjects.
    let epoch = db.storage.info().epoch;
    let logs = config(&prefix, "a").remote().unwrap().list("log/").unwrap();
    assert!(!logs.is_empty());
    for listed in logs {
        assert!(
            listed.key.starts_with(&format!("log/{epoch}/")),
            "{} left behind",
            listed.key
        );
    }
    db.crash();

    let restored = open_db(&config(&prefix, "a"), &dir.db("b.db")).unwrap();
    assert_eq!(restored.int("SELECT count(*) FROM t"), 15);
    assert_eq!(restored.int("SELECT count(*) FROM t WHERE v = 'y'"), 5);
}

/// Prefixes object_store percent-encodes or normalizes, through a real
/// server's URL handling.
#[test]
fn seaweed_prefixes_with_unusual_characters() {
    let Some(base) = prefix("unusual") else {
        return;
    };
    for (i, odd) in [
        "tenant#1",
        "100%",
        "space here/ü",
        "a+b=c&d",
        "x//y",
        "[x]{y}",
    ]
    .iter()
    .enumerate()
    {
        let prefix = format!("{base}{i}/{odd}");
        let dir = TempDir::new();
        let db = open_db(&config(&prefix, "a"), &dir.db("a.db")).unwrap();
        db.exec("CREATE TABLE t(id INTEGER PRIMARY KEY)").unwrap();
        db.exec("INSERT INTO t VALUES (1)").unwrap();
        db.checkpoint();
        db.exec("INSERT INTO t VALUES (2)").unwrap();
        db.crash();
        let restored = open_db(&config(&prefix, "a"), &dir.db("b.db"))
            .unwrap_or_else(|e| panic!("prefix {prefix:?}: {e:?}"));
        assert_eq!(
            restored.int("SELECT count(*) FROM t"),
            2,
            "prefix {prefix:?}"
        );
        restored.storage.release();
    }
}

#[test]
fn seaweed_lease_conflict() {
    let Some(prefix) = prefix("lease") else {
        return;
    };
    let dir = TempDir::new();
    let _a = open_db(&config(&prefix, "a"), &dir.db("a.db")).unwrap();
    assert!(matches!(
        open_db(&config(&prefix, "b"), &dir.db("b.db")),
        Err(S3Error::LeaseHeld { .. })
    ));
}

const CRASH_ENV: &str = "SEDIMENT_S3_CRASH_CHILD_PREFIX";

/// The child side of `seaweed_kill9_loses_no_acknowledged_commit`: commits
/// forever, printing each id once its commit returned. Runs only when the
/// parent sets the environment variable.
#[test]
#[ignore]
fn crash_child() {
    let Ok(prefix) = std::env::var(CRASH_ENV) else {
        return;
    };
    use std::io::Write as _;
    let path = std::path::PathBuf::from(std::env::var("SEDIMENT_S3_CRASH_CHILD_DB").unwrap());
    let db = open_db(&config(&prefix, "crash-writer"), &path).unwrap();
    db.exec("CREATE TABLE IF NOT EXISTS t(id INTEGER PRIMARY KEY)")
        .unwrap();
    let mut next = db.int("SELECT coalesce(max(id), 0) FROM t") + 1;
    let mut out = std::io::stdout();
    loop {
        if db.exec(&format!("INSERT INTO t VALUES ({next})")).is_ok() {
            writeln!(out, "acked {next}").unwrap();
            out.flush().unwrap();
            next += 1;
        }
    }
}

#[test]
fn seaweed_kill9_loses_no_acknowledged_commit() {
    use std::io::BufRead;
    use std::process::{Command, Stdio};

    let Some(prefix) = prefix("kill9") else {
        return;
    };
    let dir = TempDir::new();
    let mut acked = Vec::new();
    for round in 0..3u64 {
        let mut child = Command::new(std::env::current_exe().unwrap())
            .args([
                "--ignored",
                "--exact",
                "s3::tests::seaweed::crash_child",
                "--nocapture",
            ])
            .env(CRASH_ENV, &prefix)
            .env(
                "SEDIMENT_S3_CRASH_CHILD_DB",
                dir.db(&format!("child{round}.db")),
            )
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let stdout = child.stdout.take().unwrap();
        let (tx, rx) = std::sync::mpsc::channel();
        let reader = std::thread::spawn(move || {
            let mut ids = Vec::new();
            for line in std::io::BufReader::new(stdout)
                .lines()
                .map_while(|l| l.ok())
            {
                if let Some(id) = line
                    .strip_prefix("acked ")
                    .and_then(|n| n.parse::<i64>().ok())
                {
                    ids.push(id);
                    let _ = tx.send(());
                }
            }
            ids
        });
        // Once it commits, kill -9 at an arbitrary point, most likely mid-commit.
        rx.recv_timeout(Duration::from_secs(60))
            .expect("the child never committed");
        std::thread::sleep(Duration::from_millis(150 + 97 * round));
        child.kill().unwrap();
        child.wait().unwrap();
        let ids = reader.join().unwrap();
        acked.extend(ids);
    }

    let restored = open_db(&config(&prefix, "crash-writer"), &dir.db("restored.db")).unwrap();
    let ids: Vec<i64> = restored
        .rows("SELECT id FROM t ORDER BY id")
        .into_iter()
        .map(|row| row[0].to_string().parse().unwrap())
        .collect();
    for id in &acked {
        assert!(ids.binary_search(id).is_ok(), "acknowledged {id} lost");
    }
    // Contiguous: every commit continues from the restored maximum.
    assert_eq!(ids, (1..=ids.len() as i64).collect::<Vec<_>>());
    eprintln!(
        "kill9: {} acknowledged, {} restored",
        acked.len(),
        ids.len()
    );
}

/// Timings for the guide: `cargo test seaweed_timings -- --ignored --nocapture`.
#[test]
#[ignore]
fn seaweed_timings() {
    let Some(prefix) = prefix("timings") else {
        return;
    };
    let dir = TempDir::new();
    let mut cfg = config(&prefix, "timer");
    cfg.checkpoint_threshold = Some(-1);
    let db = open_db(&cfg, &dir.db("a.db")).unwrap();
    db.exec("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    for (round, n) in [(0, 500), (1, 1500)] {
        let started = std::time::Instant::now();
        for i in 0..n {
            db.exec(&format!(
                "INSERT INTO t VALUES ({}, 'row')",
                round * 10_000 + i
            ))
            .unwrap();
        }
        let per_commit = started.elapsed() / n as u32;
        let segments = db.storage.info().uploaded_frames;
        let started = std::time::Instant::now();
        let restored = super::super::restore_to(
            &cfg,
            &dir.db(&format!("r{round}.db")),
            super::super::Target::Latest,
        )
        .unwrap();
        let restore = started.elapsed();
        eprintln!(
            "timings: {segments} log objects, {per_commit:?}/commit, restore {restore:?} ({} frames)",
            restored.frames
        );
    }
}

fn percentile(sorted: &[Duration], p: f64) -> Duration {
    sorted[((sorted.len() as f64 - 1.0) * p).round() as usize]
}

/// Throughput numbers for the guide (release build):
/// `cargo test --release seaweed_throughput -- --ignored --nocapture`.
#[test]
#[ignore]
fn seaweed_throughput() {
    use std::time::Instant;
    if prefix("throughput").is_none() {
        return;
    }
    for sync in ["FULL", "NORMAL"] {
        let prefix = prefix("throughput").unwrap();
        let dir = TempDir::new();
        let db = open_db(&config(&prefix, "bench"), &dir.db("a.db")).unwrap();
        db.exec(&format!("PRAGMA synchronous = {sync}")).unwrap();
        db.exec("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
            .unwrap();
        let mut latencies = Vec::new();
        let started = Instant::now();
        for i in 0..300 {
            let t = Instant::now();
            db.exec(&format!("INSERT INTO t VALUES ({i}, 'hello')"))
                .unwrap();
            latencies.push(t.elapsed());
        }
        let total = started.elapsed();
        latencies.sort();
        eprintln!(
            "bench single sync={sync}: p50 {:?} p99 {:?}, {:.0} commits/s",
            percentile(&latencies, 0.5),
            percentile(&latencies, 0.99),
            300.0 / total.as_secs_f64()
        );
    }
    for group in [false, true] {
        for writers in [1i64, 4, 16] {
            let prefix = prefix("throughput").unwrap();
            let dir = TempDir::new();
            let mut cfg = config(&prefix, "bench");
            cfg.group_commit = group;
            let db = open_db(&cfg, &dir.db("a.db")).unwrap();
            db.exec("CREATE TABLE t(id INTEGER PRIMARY KEY, w INTEGER)")
                .unwrap();
            let frames_before = db.storage.info().uploaded_frames;
            let objects_before = segment_objects(&db);
            let per = 400 / writers;
            let started = Instant::now();
            let handles: Vec<_> = (0..writers)
                .map(|w| {
                    let conn = db._db.connect().unwrap();
                    std::thread::spawn(move || {
                        conn.execute("PRAGMA synchronous = FULL").unwrap();
                        let mut done = 0;
                        for i in 0..per {
                            let tx = format!(
                                "BEGIN CONCURRENT; INSERT INTO t VALUES ({}, {w}); COMMIT",
                                w * 100_000 + i
                            );
                            loop {
                                match conn.execute(&tx) {
                                    Ok(()) => break,
                                    Err(_) => {
                                        let _ = conn.execute("ROLLBACK");
                                        std::thread::sleep(Duration::from_micros(200));
                                    }
                                }
                            }
                            done += 1;
                        }
                        done
                    })
                })
                .collect();
            let commits: i64 = handles.into_iter().map(|h| h.join().unwrap()).sum();
            let total = started.elapsed();
            let frames = db.storage.info().uploaded_frames - frames_before;
            let objects = segment_objects(&db) - objects_before;
            eprintln!(
                "bench concurrent group_commit={group} writers={writers}: {:.0} commits/s, \
                 {commits} commits in {objects} log objects ({frames} frames)",
                commits as f64 / total.as_secs_f64()
            );
        }
    }
}

/// Log objects uploaded so far (a group-commit batch is one object).
fn segment_objects(db: &super::Db) -> u64 {
    db.storage.info().uploaded_objects
}

/// A >100 MB snapshot through real multipart upload:
/// `cargo test --release seaweed_huge_snapshot -- --ignored --nocapture`.
#[test]
#[ignore]
fn seaweed_huge_snapshot() {
    let Some(prefix) = prefix("huge") else {
        return;
    };
    let dir = TempDir::new();
    let mut cfg = config(&prefix, "huge");
    cfg.checkpoint_threshold = Some(-1);
    let db = open_db(&cfg, &dir.db("a.db")).unwrap();
    db.exec("CREATE TABLE t(id INTEGER PRIMARY KEY, v BLOB)")
        .unwrap();
    for i in 0..110 {
        db.exec(&format!("INSERT INTO t VALUES ({i}, randomblob(1000000))"))
            .unwrap();
    }
    let started = std::time::Instant::now();
    db.checkpoint();
    let upload = started.elapsed();
    db.crash();
    let started = std::time::Instant::now();
    let restored = open_db(&cfg, &dir.db("b.db")).unwrap();
    let restore = started.elapsed();
    assert_eq!(restored.int("SELECT sum(length(v)) FROM t"), 110_000_000);
    eprintln!("huge on server: checkpoint+upload {upload:?}, open+restore {restore:?}");
}

/// The soak harness (scripts/s3_soak.exs) only; ignored by default. The
/// root the soak writes under: S3_TEST_PREFIX_ROOT, or the whole bucket.
fn soak_remote() -> (String, super::super::remote::Remote) {
    let root = env("S3_TEST_PREFIX_ROOT", "");
    let store = config("soak", "soak").build_store().unwrap();
    (
        root.clone(),
        super::super::remote::Remote::new(store, &root),
    )
}

/// Soak harness: what the bucket (or root) holds, by first path segment.
#[test]
#[ignore]
fn soak_list() {
    let (root, remote) = soak_remote();
    let listed = remote.list("").unwrap();
    let mut tops = std::collections::BTreeMap::<String, u64>::new();
    for l in &listed {
        *tops
            .entry(l.key.split('/').next().unwrap_or("").to_string())
            .or_default() += 1;
    }
    println!(
        "SOAK_LIST root={root:?} objects={} tops={tops:?}",
        listed.len()
    );
}

/// Soak harness: deletes everything under S3_TEST_PREFIX_ROOT, which must be a soak-* or
/// boat-* root (so it can never empty a whole bucket or another prefix). Deletes the listed locations as they
/// are (keys under unusual prefixes aren't re-encoded), in a few rounds: a
/// listing may lag deletes, and a killed writer's last upload may land late.
#[test]
#[ignore]
fn soak_cleanup() {
    use futures::{StreamExt, TryStreamExt};
    use object_store::path::Path;

    let root = env("S3_TEST_PREFIX_ROOT", "");
    let name = root.trim_matches('/');
    // Only the soaks' own roots (scripts/s3_soak.exs: soak-*, the multi-node soak: boat-*),
    // never a whole bucket or anything else.
    assert!(
        (name.starts_with("soak-") || name.starts_with("boat-")) && !name.contains('/'),
        "refusing to clean {root:?}: not a soak root"
    );
    let store = config("soak", "soak").build_store().unwrap();
    let prefix = Path::from(root.trim_matches('/'));
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let list = || -> Vec<Path> {
        runtime.block_on(async {
            store
                .list(Some(&prefix))
                .map_ok(|meta| meta.location)
                .try_collect()
                .await
                .unwrap()
        })
    };
    let mut deleted = 0;
    for round in 0..5 {
        let locations = list();
        if locations.is_empty() {
            println!("SOAK_CLEANUP root={root:?} deleted={deleted} left=0 rounds={round}");
            return;
        }
        deleted += locations.len();
        runtime.block_on(async {
            let stream = futures::stream::iter(locations.into_iter().map(Ok)).boxed();
            let results: Vec<_> = store.delete_stream(stream).collect().await;
            for result in results {
                match result {
                    Ok(_) | Err(object_store::Error::NotFound { .. }) => {}
                    Err(err) => panic!("delete failed: {err}"),
                }
            }
        });
        std::thread::sleep(Duration::from_secs(2));
    }
    let left = list();
    println!(
        "SOAK_CLEANUP root={root:?} deleted={deleted} left={}",
        left.len()
    );
    panic!(
        "objects left under {root:?}: {:?}",
        &left[..left.len().min(20)]
    );
}
