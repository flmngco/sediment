//! `Sediment.export_sqlite`: every export is opened with real SQLite
//! (rusqlite, bundled) and checked against the source.

use std::path::Path;
use std::sync::Arc;

use super::faulty::FaultyStore;
use super::{config, encryption, keys, open_encrypted, TempDir, PREFIX};
use crate::export::{export, ExportOptions, Exported, Source};
use crate::s3::layout::MANIFEST_KEY;
use crate::s3::remote::Remote;
use crate::s3::S3Config;

const SCHEMA: &[&str] = &[
    "PRAGMA foreign_keys = ON",
    "CREATE TABLE authors (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL UNIQUE)",
    "CREATE TABLE posts (id INTEGER PRIMARY KEY AUTOINCREMENT, \
     author_id INTEGER NOT NULL REFERENCES authors(id) ON DELETE CASCADE, \
     title TEXT NOT NULL CHECK (title != ''), body BLOB, score REAL DEFAULT 0)",
    "CREATE UNIQUE INDEX posts_title ON posts (title)",
    "CREATE INDEX posts_author ON posts (author_id)",
    "CREATE TABLE audit (id INTEGER PRIMARY KEY AUTOINCREMENT, msg TEXT NOT NULL)",
    "CREATE TRIGGER posts_audit AFTER INSERT ON posts BEGIN \
     INSERT INTO audit (msg) VALUES ('post ' || new.title); END",
    "CREATE VIEW post_titles AS SELECT p.id, a.name, p.title FROM posts p \
     JOIN authors a ON a.id = p.author_id",
    "CREATE TABLE plain_rowid (k TEXT, v BLOB)",
    "PRAGMA user_version = 42",
];

/// Fills a database: 20 authors and 1,000 posts, then (`after`) the rest:
/// 50 more posts, the 10 newest deleted, an author deleted with its posts
/// (cascade), a rowid gap.
fn fill(exec: &dyn Fn(&str), after: &dyn Fn()) {
    for sql in SCHEMA {
        exec(sql);
    }
    exec("BEGIN");
    for a in 1..=20 {
        exec(&format!("INSERT INTO authors (name) VALUES ('author-{a}')"));
    }
    for i in 1..=1000 {
        exec(&format!(
            "INSERT INTO posts (author_id, title, body, score) \
             VALUES ({}, 'title-{i}', randomblob({}), {i}.5)",
            i % 20 + 1,
            i % 50
        ));
    }
    for i in 1..=100 {
        exec(&format!(
            "INSERT INTO plain_rowid VALUES ('k{i}', randomblob(8))"
        ));
    }
    exec("COMMIT");
    after();
    exec("BEGIN");
    for i in 1001..=1050 {
        exec(&format!(
            "INSERT INTO posts (author_id, title) VALUES (1, 'late-{i}')"
        ));
    }
    exec("COMMIT");
    exec("DELETE FROM posts WHERE id > 1040");
    exec("DELETE FROM authors WHERE id = 20");
    exec("DELETE FROM plain_rowid WHERE k = 'k100'");
}

/// Row count and a checksum (rowid and every column, quoted) per table, and
/// sqlite_sequence; the same SQL runs on turso and on SQLite.
type Fingerprint = (Vec<(String, usize, String)>, Vec<(String, i64)>);

fn fingerprint_sql(table: &str, columns: &[String]) -> String {
    let expr = columns
        .iter()
        .map(|c| format!("quote(\"{c}\")"))
        .collect::<Vec<_>>()
        .join(" || ',' || ");
    format!("SELECT rowid || ':' || {expr} FROM \"{table}\" ORDER BY rowid")
}

const TABLES: &[&str] = &["audit", "authors", "plain_rowid", "posts"];

fn hash(lines: &[String]) -> String {
    use std::hash::{BuildHasher, Hasher};
    // Deterministic: a fixed-key hasher over the joined text.
    let mut h =
        std::hash::BuildHasherDefault::<std::collections::hash_map::DefaultHasher>::default()
            .build_hasher();
    h.write(lines.join("\n").as_bytes());
    format!("{:016x}", h.finish())
}

fn turso_fingerprint(path: &Path, key: Option<&turso_core::EncryptionOpts>) -> Fingerprint {
    let (_db, conn) =
        crate::s3::open_local_with(path, key, turso_core::DatabaseOpts::new().with_views(true))
            .unwrap();
    let all = |sql: &str| -> Vec<Vec<turso_core::Value>> {
        conn.query(sql)
            .unwrap()
            .unwrap()
            .run_collect_rows()
            .unwrap()
    };
    let tables = TABLES
        .iter()
        .map(|t| {
            let columns: Vec<String> = all(&format!("SELECT name FROM pragma_table_info('{t}')"))
                .iter()
                .map(|r| r[0].to_string())
                .collect();
            let lines: Vec<String> = all(&fingerprint_sql(t, &columns))
                .iter()
                .map(|r| r[0].to_string())
                .collect();
            (t.to_string(), lines.len(), hash(&lines))
        })
        .collect();
    let seq = all("SELECT name, seq FROM sqlite_sequence ORDER BY name")
        .iter()
        .map(|r| (r[0].to_string(), r[1].to_string().parse().unwrap()))
        .collect();
    (tables, seq)
}

fn sqlite_fingerprint(db: &rusqlite::Connection) -> Fingerprint {
    let tables = TABLES
        .iter()
        .map(|t| {
            let columns: Vec<String> = db
                .prepare(&format!("SELECT name FROM pragma_table_info('{t}')"))
                .unwrap()
                .query_map([], |r| r.get(0))
                .unwrap()
                .map(Result::unwrap)
                .collect();
            let lines: Vec<String> = db
                .prepare(&fingerprint_sql(t, &columns))
                .unwrap()
                .query_map([], |r| r.get(0))
                .unwrap()
                .map(Result::unwrap)
                .collect();
            (t.to_string(), lines.len(), hash(&lines))
        })
        .collect();
    let seq = db
        .prepare("SELECT name, seq FROM sqlite_sequence ORDER BY name")
        .unwrap()
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
        .unwrap()
        .map(Result::unwrap)
        .collect();
    (tables, seq)
}

/// Opens `dest` with real SQLite: the same data and sequences as `source`,
/// a sound file, and the schema working (AUTOINCREMENT continuing).
fn check_in_sqlite(dest: &Path, source: &Fingerprint) {
    let header = std::fs::read(dest).unwrap();
    assert_eq!(&header[..16], b"SQLite format 3\0");
    assert_eq!((header[18], header[19]), (2, 2), "WAL, not MVCC");
    let db = rusqlite::Connection::open(dest).unwrap();
    let check: String = db
        .query_row("PRAGMA integrity_check", [], |r| r.get(0))
        .unwrap();
    assert_eq!(check, "ok");
    let fk: Vec<String> = db
        .prepare("PRAGMA foreign_key_check")
        .unwrap()
        .query_map([], |r| r.get(0))
        .unwrap()
        .map(Result::unwrap)
        .collect();
    assert!(fk.is_empty(), "{fk:?}");
    let version: i64 = db
        .query_row("PRAGMA user_version", [], |r| r.get(0))
        .unwrap();
    assert_eq!(version, 42);
    assert_eq!(&sqlite_fingerprint(&db), source);
    let posts_seq = source.1.iter().find(|(n, _)| n == "posts").unwrap().1;
    assert_eq!(posts_seq, 1050, "the deleted newest posts count");

    db.execute_batch("PRAGMA foreign_keys = ON").unwrap();
    db.execute(
        "INSERT INTO posts (author_id, title) VALUES (1, 'in-sqlite')",
        [],
    )
    .unwrap();
    assert_eq!(
        db.last_insert_rowid(),
        posts_seq + 1,
        "no deleted id is reused"
    );
    let audit: i64 = db
        .query_row(
            "SELECT count(*) FROM audit WHERE msg = 'post in-sqlite'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(audit, 1, "the trigger fires");
    let name: String = db
        .query_row(
            "SELECT name FROM post_titles WHERE title = 'in-sqlite'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(name, "author-1", "the view works");
    for bad in [
        "INSERT INTO posts (author_id, title) VALUES (1, '')",
        "INSERT INTO posts (author_id, title) VALUES (1, 'title-1')",
        "INSERT INTO posts (author_id, title) VALUES (999, 'orphan')",
        "INSERT INTO authors (name) VALUES ('author-1')",
    ] {
        assert!(db.execute(bad, []).is_err(), "{bad}");
    }
    db.execute("DELETE FROM authors WHERE id = 2", []).unwrap();
    let left: i64 = db
        .query_row("SELECT count(*) FROM posts WHERE author_id = 2", [], |r| {
            r.get(0)
        })
        .unwrap();
    assert_eq!(left, 0, "ON DELETE CASCADE");
}

fn files(dir: &Path) -> Vec<String> {
    let mut names: Vec<String> = std::fs::read_dir(dir)
        .unwrap()
        .flatten()
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .collect();
    names.sort();
    names
}

fn opts(key: Option<turso_core::EncryptionOpts>) -> ExportOptions {
    ExportOptions {
        encryption: key,
        drop_fts: false,
    }
}

/// An S3 database filled through a writer (rows in the snapshot and in the
/// log); returns its config.
fn s3_source(store: &Arc<FaultyStore>, dir: &TempDir, key: Option<&str>) -> S3Config {
    let mut cfg = config(store, "writer");
    if let Some(k) = key {
        cfg.unencrypted = false;
        cfg.encryption = Some(encryption(k));
    }
    let db = open_encrypted(&cfg, &dir.db("writer.db")).unwrap();
    fill(&|sql| db.exec(sql).unwrap(), &|| db.checkpoint());
    drop(db);
    cfg
}

#[test]
fn an_encrypted_s3_database_exports_to_a_working_sqlite_file() {
    let store = FaultyStore::new();
    let dir = TempDir::new();
    let cfg = s3_source(&store, &dir, Some("ab"));
    let remote = Remote::new(store.clone(), PREFIX);
    let manifest = remote.get(MANIFEST_KEY).unwrap().unwrap().bytes;
    let objects = keys(&store, "");
    let out = TempDir::new();
    let dest = out.db("export.db");

    let exported = export(
        Source::S3(Box::new(cfg.clone())),
        &dest,
        &opts(Some(encryption("ab"))),
    )
    .unwrap();
    assert_eq!(exported.rows, 19 + 990 + 1050 + 99, "{exported:?}");
    assert!(exported.dropped_fts.is_empty());

    // The source: a fingerprint of the S3 database (restored separately).
    crate::s3::restore_to(&cfg, &dir.db("restored.db"), crate::s3::Target::Latest).unwrap();
    let source = turso_fingerprint(&dir.db("restored.db"), cfg.encryption.as_ref());
    check_in_sqlite(&dest, &source);
    // The prefix only read; no temporary files left.
    assert_eq!(remote.get(MANIFEST_KEY).unwrap().unwrap().bytes, manifest);
    assert_eq!(keys(&store, ""), objects);
    assert_eq!(files(&out.0), ["export.db"]);
}

#[test]
fn a_local_mvcc_database_with_its_log_and_a_wal_database_export() {
    // MVCC: an S3 writer's working copy, rows still in its log
    let store = FaultyStore::new();
    let dir = TempDir::new();
    let cfg = s3_source(&store, &dir, None);
    let writer = dir.db("writer.db");
    assert!(std::fs::metadata(crate::s3::restore::log_path(&writer)).is_ok());
    // WAL: a plain database
    let wal = dir.db("wal.db");
    {
        let (_db, conn) = crate::s3::open_local_with(
            &wal,
            None,
            turso_core::DatabaseOpts::new().with_views(true),
        )
        .unwrap();
        fill(&|sql| conn.execute(sql).unwrap(), &|| ());
        conn.close().unwrap();
    }
    let _ = cfg;
    for source in [writer, wal] {
        let before: Vec<(String, Vec<u8>)> = files(&dir.0)
            .into_iter()
            .map(|n| (n.clone(), std::fs::read(dir.0.join(&n)).unwrap()))
            .collect();
        let out = TempDir::new();
        let dest = out.db("export.db");
        export(Source::Local(source.clone()), &dest, &opts(None)).unwrap();
        let after: Vec<(String, Vec<u8>)> = files(&dir.0)
            .into_iter()
            .map(|n| (n.clone(), std::fs::read(dir.0.join(&n)).unwrap()))
            .collect();
        assert_eq!(after, before, "the source is never written to");
        // The source's fingerprint, from a copy of it.
        let probe = TempDir::new();
        for (from, to) in [
            (source.clone(), probe.db("p.db")),
            (
                crate::s3::restore::log_path(&source),
                crate::s3::restore::log_path(&probe.db("p.db")),
            ),
            (
                crate::s3::restore::wal_path(&source),
                crate::s3::restore::wal_path(&probe.db("p.db")),
            ),
        ] {
            let _ = std::fs::copy(from, to);
        }
        check_in_sqlite(&dest, &turso_fingerprint(&probe.db("p.db"), None));
        assert_eq!(files(&out.0), ["export.db"]);
    }
}

#[test]
fn an_export_refuses_an_existing_dest_a_wrong_key_and_fts_indexes_unless_asked() {
    let store = FaultyStore::new();
    let dir = TempDir::new();
    let cfg = s3_source(&store, &dir, Some("ab"));
    let out = TempDir::new();
    let dest = out.db("export.db");
    let refused = |result: Result<Exported, String>, needle: &str| match result {
        Err(msg) => assert!(msg.contains(needle), "{msg}"),
        Ok(exported) => panic!("expected an error containing {needle:?}, got {exported:?}"),
    };

    std::fs::write(&dest, b"keep me").unwrap();
    refused(
        export(
            Source::S3(Box::new(cfg.clone())),
            &dest,
            &opts(Some(encryption("ab"))),
        ),
        "export target exists",
    );
    assert_eq!(std::fs::read(&dest).unwrap(), b"keep me");
    std::fs::remove_file(&dest).unwrap();

    refused(
        export(
            Source::S3(Box::new(cfg.clone())),
            &dest,
            &opts(Some(encryption("cd"))),
        ),
        "",
    );
    refused(
        export(Source::S3(Box::new(cfg)), &dest, &opts(None)),
        "encrypted",
    );
    refused(
        export(Source::Local(dir.db("missing.db")), &dest, &opts(None)),
        "doesn't exist",
    );
    assert_eq!(files(&out.0), Vec::<String>::new(), "nothing left behind");

    // FTS: refused with the index names, dropped when asked
    let fts = dir.db("fts.db");
    {
        let (_db, conn) = crate::s3::open_local_with(
            &fts,
            None,
            turso_core::DatabaseOpts::new().with_index_method(true),
        )
        .unwrap();
        for sql in [
            "CREATE TABLE docs (id INTEGER PRIMARY KEY, body TEXT, emb BLOB)",
            "INSERT INTO docs VALUES (1, 'hello world', vector32('[1,2,3]'))",
            "CREATE INDEX docs_fts ON docs USING fts(body)",
        ] {
            conn.execute(sql).unwrap();
        }
        conn.close().unwrap();
    }
    refused(
        export(Source::Local(fts.clone()), &dest, &opts(None)),
        "docs_fts",
    );
    assert!(!dest.exists());
    let exported = export(
        Source::Local(fts),
        &dest,
        &ExportOptions {
            encryption: None,
            drop_fts: true,
        },
    )
    .unwrap();
    assert_eq!(exported.dropped_fts, ["docs_fts"]);
    let db = rusqlite::Connection::open(&dest).unwrap();
    let (body, emb): (String, Vec<u8>) = db
        .query_row("SELECT body, emb FROM docs", [], |r| {
            Ok((r.get(0)?, r.get(1)?))
        })
        .unwrap();
    assert_eq!(body, "hello world");
    assert_eq!(emb.len(), 12, "a vector is a blob of floats");
    drop(db);
    assert_eq!(files(&out.0), ["export.db"]);
}

fn local(path: &Path, sqls: &[&str]) {
    let (_db, conn) = crate::s3::open_local_with(
        path,
        None,
        turso_core::DatabaseOpts::new()
            .with_views(true)
            .with_index_method(true),
    )
    .unwrap();
    for sql in sqls {
        conn.execute(sql).unwrap();
    }
    conn.close().unwrap();
}

/// A WAL database whose last rows are only in its WAL (the connection is
/// dropped without the checkpoint a close does).
fn with_wal_tail(path: &Path) {
    local(
        path,
        &[
            "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)",
            "INSERT INTO t VALUES (1, 'base')",
        ],
    );
    let (db, conn) = crate::s3::open_local(path, None).unwrap();
    conn.execute("INSERT INTO t VALUES (2, 'durable tail')")
        .unwrap();
    std::mem::forget((db, conn));
}

fn sqlite_ids(dest: &Path) -> Vec<i64> {
    let db = rusqlite::Connection::open(dest).unwrap();
    let mut stmt = db.prepare("SELECT id FROM t ORDER BY id").unwrap();
    let ids = stmt
        .query_map([], |r| r.get(0))
        .unwrap()
        .map(Result::unwrap)
        .collect();
    ids
}

#[cfg(unix)]
#[test]
fn an_export_through_a_symlink_finds_the_sidecars_and_refuses_ambiguity() {
    let dir = TempDir::new();
    let links = TempDir::new();
    let out = TempDir::new();
    // The WAL next to the target (the writer used the real path)
    let real = dir.db("real.db");
    with_wal_tail(&real);
    assert!(
        std::fs::metadata(crate::s3::restore::wal_path(&real))
            .unwrap()
            .len()
            > 0
    );
    let link = links.db("link.db");
    std::os::unix::fs::symlink(&real, &link).unwrap();
    export(Source::Local(link.clone()), &out.db("a.db"), &opts(None)).unwrap();
    assert_eq!(sqlite_ids(&out.db("a.db")), [1, 2]);

    // The WAL next to the link (the writer used the link)
    let real2 = dir.db("real2.db");
    let link2 = links.db("link2.db");
    local(&real2, &[]);
    std::fs::remove_file(&real2).unwrap();
    std::fs::File::create(&real2).unwrap();
    std::os::unix::fs::symlink(&real2, &link2).unwrap();
    with_wal_tail(&link2);
    assert!(
        std::fs::metadata(crate::s3::restore::wal_path(&link2))
            .unwrap()
            .len()
            > 0
    );
    export(Source::Local(link2.clone()), &out.db("b.db"), &opts(None)).unwrap();
    assert_eq!(sqlite_ids(&out.db("b.db")), [1, 2]);

    // Both: ambiguous
    std::fs::copy(
        crate::s3::restore::wal_path(&link2),
        crate::s3::restore::wal_path(&real2),
    )
    .unwrap();
    let refused = export(Source::Local(link2), &out.db("c.db"), &opts(None)).unwrap_err();
    assert!(refused.contains("ambiguous"), "{refused}");

    // A hard link: other names possible
    let hard = links.db("hard.db");
    std::fs::hard_link(&real, &hard).unwrap();
    let refused = export(Source::Local(hard), &out.db("d.db"), &opts(None)).unwrap_err();
    assert!(refused.contains("hard links"), "{refused}");
    assert!(!out.db("c.db").exists() && !out.db("d.db").exists());
}

#[test]
fn fts_indexes_are_found_by_structure_not_by_a_word() {
    let dir = TempDir::new();
    let out = TempDir::new();
    let source = dir.db("s.db");
    local(
        &source,
        &[
            "CREATE TABLE a (id INTEGER PRIMARY KEY, note TEXT DEFAULT 'made using defaults')",
            "CREATE TABLE b (id INTEGER PRIMARY KEY, email TEXT, note TEXT)",
            "CREATE UNIQUE INDEX b_email ON b (email) WHERE note != 'created using api'",
            "CREATE VIEW ab AS SELECT * FROM a JOIN b USING (id)",
            "CREATE TABLE docs (id INTEGER PRIMARY KEY, body TEXT)",
            "INSERT INTO a (id) VALUES (1)",
            "INSERT INTO b VALUES (1, 'x@y', 'n')",
            "INSERT INTO docs VALUES (1, 'hello')",
        ],
    );
    // No FTS: everything exports, the view and the partial index included.
    export(
        Source::Local(source.clone()),
        &out.db("plain.db"),
        &opts(None),
    )
    .unwrap();
    let check = |dest: &Path| {
        let db = rusqlite::Connection::open(dest).unwrap();
        let names: Vec<String> = db
            .prepare("SELECT name FROM sqlite_schema ORDER BY name")
            .unwrap()
            .query_map([], |r| r.get(0))
            .unwrap()
            .map(Result::unwrap)
            .collect();
        assert!(names.contains(&"b_email".to_string()), "{names:?}");
        assert!(names.contains(&"ab".to_string()), "{names:?}");
        let joined: i64 = db
            .query_row("SELECT count(*) FROM ab", [], |r| r.get(0))
            .unwrap();
        assert_eq!(joined, 1);
        assert!(
            db.execute("INSERT INTO b VALUES (2, 'x@y', 'm')", [])
                .is_err(),
            "UNIQUE kept"
        );
        db.execute("INSERT INTO b VALUES (3, 'x@y', 'created using api')", [])
            .unwrap();
    };
    check(&out.db("plain.db"));

    // A real FTS index: named alone, dropped alone.
    local(&source, &["CREATE INDEX docs_fts ON docs USING fts(body)"]);
    let refused = export(Source::Local(source.clone()), &out.db("x.db"), &opts(None)).unwrap_err();
    assert!(refused.contains("(docs_fts)"), "{refused}");
    let exported = export(
        Source::Local(source),
        &out.db("fts.db"),
        &ExportOptions {
            encryption: None,
            drop_fts: true,
        },
    )
    .unwrap();
    assert_eq!(exported.dropped_fts, ["docs_fts"]);
    check(&out.db("fts.db"));
}

#[test]
fn index_methods_are_read_token_by_token() {
    use crate::export::index_method;
    assert_eq!(
        index_method("CREATE INDEX f ON docs USING fts(body)").as_deref(),
        Some("fts")
    );
    assert_eq!(
        index_method("create index if not exists \"a b\" on main.\"my docs\" using fts (body)")
            .as_deref(),
        Some("fts")
    );
    for plain in [
        "CREATE UNIQUE INDEX u ON t (email) WHERE note != 'created using api'",
        "CREATE INDEX \"using\" ON \"using\" (x)",
        "CREATE INDEX i ON t (x) -- using fts",
        "CREATE VIEW v AS SELECT * FROM a JOIN b USING (id)",
    ] {
        assert_eq!(index_method(plain), None, "{plain}");
    }
    // Unterminated literals, quoted names and comments end the scan.
    for broken in [
        "CREATE INDEX i ON t USING 'fts",
        "CREATE INDEX \"i ON t USING fts(body)",
        "CREATE INDEX i ON [t USING fts(body)",
        "CREATE INDEX i ON t /* USING fts(body)",
        "CREATE INDEX i ON t -- USING",
        "'",
        "/*",
    ] {
        let _ = index_method(broken);
    }
}

#[test]
fn an_mvcc_database_with_many_autoincrement_rows_exports_in_linear_time() {
    // VACUUM INTO from MVCC copies in one transaction, quadratic for
    // AUTOINCREMENT tables (20,000 rows: 66 s); the copy is switched to WAL
    // first. Release builds only.
    if cfg!(debug_assertions) {
        return;
    }
    let dir = TempDir::new();
    let out = TempDir::new();
    let source = dir.db("mvcc.db");
    std::fs::write(&source, b"").unwrap();
    local(&source, &["PRAGMA journal_mode = 'mvcc'"]);
    let (_db, conn) = crate::s3::open_local(&source, None).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY AUTOINCREMENT, v TEXT)")
        .unwrap();
    for chunk in 0..40 {
        conn.execute("BEGIN").unwrap();
        for i in 0..500 {
            conn.execute(format!(
                "INSERT INTO t (v) VALUES ('row {}')",
                chunk * 500 + i
            ))
            .unwrap();
        }
        conn.execute("COMMIT").unwrap();
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    }
    conn.execute("DELETE FROM t WHERE id > 19990").unwrap();
    conn.close().unwrap();
    drop(_db);
    let started = std::time::Instant::now();
    let exported = export(Source::Local(source), &out.db("e.db"), &opts(None)).unwrap();
    let secs = started.elapsed().as_secs_f64();
    assert!(secs < 15.0, "20,000 AUTOINCREMENT rows took {secs:.1} s");
    assert_eq!(exported.sequences, [("t".to_string(), 20_000)]);
    let db = rusqlite::Connection::open(out.db("e.db")).unwrap();
    db.execute("INSERT INTO t (v) VALUES ('in sqlite')", [])
        .unwrap();
    assert_eq!(db.last_insert_rowid(), 20_001);
}

#[cfg(unix)]
#[test]
fn the_export_is_generated_in_a_private_directory() {
    use std::os::unix::fs::PermissionsExt;
    let store = FaultyStore::new();
    let dir = TempDir::new();
    let cfg = s3_source(&store, &dir, Some("ab"));
    let out = TempDir::new();
    // Hold the restore so the destination directory can be looked at.
    store.inject(
        super::faulty::Op::Get,
        "snapshots/",
        super::faulty::Fault::Delay(std::time::Duration::from_millis(800)),
        1,
    );
    let seen = {
        let out = out.0.clone();
        std::thread::spawn(move || {
            std::thread::sleep(std::time::Duration::from_millis(300));
            std::fs::read_dir(&out)
                .unwrap()
                .flatten()
                .map(|e| {
                    let meta = e.metadata().unwrap();
                    (meta.is_dir(), meta.permissions().mode() & 0o777)
                })
                .collect::<Vec<_>>()
        })
    };
    export(
        Source::S3(Box::new(cfg)),
        &out.db("e.db"),
        &opts(Some(encryption("ab"))),
    )
    .unwrap();
    assert_eq!(
        seen.join().unwrap(),
        [(true, 0o700)],
        "only the private directory"
    );
    let mode = std::fs::metadata(out.db("e.db"))
        .unwrap()
        .permissions()
        .mode()
        & 0o777;
    assert_eq!(mode, 0o600);
    assert_eq!(files(&out.0), ["e.db"]);
}

#[test]
fn a_wal_database_with_cdc_exports() {
    let dir = TempDir::new();
    let out = TempDir::new();
    let source = dir.db("cdc.db");
    let (_db, conn) = crate::s3::open_local(&source, None).unwrap();
    for sql in [
        "CREATE TABLE t (id INTEGER PRIMARY KEY AUTOINCREMENT, v TEXT)",
        "PRAGMA unstable_capture_data_changes_conn('full')",
        "INSERT INTO t (v) VALUES ('a'), ('b')",
    ] {
        conn.execute(sql).unwrap();
    }
    conn.close().unwrap();
    drop(_db);
    export(Source::Local(source), &out.db("e.db"), &opts(None)).unwrap();
    let db = rusqlite::Connection::open(out.db("e.db")).unwrap();
    let check: String = db
        .query_row("PRAGMA integrity_check", [], |r| r.get(0))
        .unwrap();
    assert_eq!(check, "ok");
    let changes: i64 = db
        .query_row("SELECT count(*) FROM turso_cdc", [], |r| r.get(0))
        .unwrap();
    assert!(changes >= 2);
}

#[test]
fn a_relative_source_path_with_a_wal_tail_exports() {
    // Ecto's :database is often relative (priv/repo/app.db); the sidecars
    // were compared textually and refused as ambiguous.
    let dir = TempDir::new();
    let out = TempDir::new();
    let real = dir.db("app.db");
    with_wal_tail(&real);
    let cwd = std::env::current_dir().unwrap();
    let relative = pathdiff(&real, &cwd);
    assert!(relative.is_relative());
    export(
        Source::Local(relative.clone()),
        &out.db("rel.db"),
        &opts(None),
    )
    .unwrap();
    assert_eq!(sqlite_ids(&out.db("rel.db")), [1, 2]);
    // A `..` spelling of the same file, too.
    let dotted = dir
        .0
        .join("..")
        .join(dir.0.file_name().unwrap())
        .join("app.db");
    export(Source::Local(dotted), &out.db("dots.db"), &opts(None)).unwrap();
    assert_eq!(sqlite_ids(&out.db("dots.db")), [1, 2]);
}

/// `path` relative to `base` (both absolute), with `..` as needed.
fn pathdiff(path: &Path, base: &Path) -> std::path::PathBuf {
    let path: Vec<_> = path.components().collect();
    let base: Vec<_> = base.components().collect();
    let common = path.iter().zip(&base).take_while(|(a, b)| a == b).count();
    let mut out = std::path::PathBuf::new();
    for _ in common..base.len() {
        out.push("..");
    }
    for part in &path[common..] {
        out.push(part.as_os_str());
    }
    out
}
