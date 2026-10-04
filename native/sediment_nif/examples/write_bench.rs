//! The write workloads of bench/run.exs, straight on turso_core: the same
//! schema, 10,000 seed rows, `synchronous = 1`, and per insert the calls the
//! NIF makes (reset, clear_bindings, bind_at, then step and drive the IO).
//! It measures turso_core's own cost, so the difference to the Elixir numbers
//! is the NIF's.
//!
//!   cargo run --release --example write_bench -- [wal|mvcc] [seconds]

use std::num::NonZero;
use std::sync::Arc;
use std::time::{Duration, Instant};

use turso_core::{
    Connection, Database, OpenOptions, PlatformIO, SqliteDialect, Statement, StepResult, Value,
};

fn run(stmt: &mut Statement) {
    loop {
        match stmt.step().unwrap() {
            StepResult::Done => return,
            StepResult::Row => {}
            StepResult::IO | StepResult::Yield => stmt._io().step().unwrap(),
            StepResult::Sleep { duration } => std::thread::sleep(duration),
            other => panic!("unexpected {other:?}"),
        }
    }
}

fn exec(conn: &Arc<Connection>, sql: &str) {
    let mut stmt = conn.prepare(sql).unwrap();
    run(&mut stmt);
}

fn insert(stmt: &mut Statement, i: usize) {
    let state = stmt.execution_state();
    if state.is_running() || state.is_terminal() {
        let _ = stmt.reset();
    }
    stmt.clear_bindings();
    let values = [
        Value::build_text(format!("new {i}")),
        Value::build_text(format!("new{i}@example.com")),
        Value::from_i64(42),
    ];
    for (n, value) in values.into_iter().enumerate() {
        stmt.bind_at(NonZero::new(n + 1).unwrap(), value).unwrap();
    }
    run(stmt);
}

/// Runs `op` for `time`, printing ops/s, median and 99th percentile.
fn measure(label: &str, time: Duration, mut op: impl FnMut(usize)) {
    for i in 0..200 {
        op(i);
    }
    let mut samples = Vec::new();
    let start = Instant::now();
    let mut i = 0;
    while start.elapsed() < time {
        let t = Instant::now();
        op(i);
        samples.push(t.elapsed());
        i += 1;
    }
    let total = start.elapsed();
    samples.sort();
    let pct = |p: f64| samples[((samples.len() as f64 * p) as usize).min(samples.len() - 1)];
    println!(
        "{label:<40} {:>10.1} /s   median {:>9.1?}   p99 {:>9.1?}   ({} ops)",
        samples.len() as f64 / total.as_secs_f64(),
        pct(0.5),
        pct(0.99),
        samples.len()
    );
}

fn main() {
    let mut args = std::env::args().skip(1);
    let mode = args.next().unwrap_or_else(|| "wal".into());
    let secs: u64 = args.next().map(|s| s.parse().unwrap()).unwrap_or(3);
    let time = Duration::from_secs(secs);

    let dir = std::env::temp_dir().join(format!("sediment_write_bench_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join(format!("{mode}.db"));
    // Knobs: IO=uring (needs --features io_uring), SYNC=0|1|2,
    // WAL_AUTOCHECKPOINT=pages, MVCC_THRESHOLD=bytes, AUTOINC=1 (an
    // AUTOINCREMENT key, as Ecto's default primary keys).
    let knob = |name: &str| std::env::var(name).ok();
    let io: Arc<dyn turso_core::IO> = match knob("IO").as_deref() {
        #[cfg(feature = "io_uring")]
        Some("uring") => Arc::new(turso_core::UringIO::new().unwrap()),
        Some(other) if other != "syscall" => panic!("unknown IO {other}"),
        _ => Arc::new(PlatformIO::new().unwrap()),
    };
    let db = Database::open(
        io,
        path.to_str().unwrap(),
        OpenOptions::new(Arc::new(SqliteDialect)),
    )
    .unwrap();
    let conn = db.connect().unwrap();
    exec(&conn, &format!("PRAGMA journal_mode = '{mode}'"));
    let sync = knob("SYNC").unwrap_or_else(|| "1".into());
    exec(&conn, &format!("PRAGMA synchronous = {sync}"));
    if let Some(pages) = knob("WAL_AUTOCHECKPOINT") {
        exec(&conn, &format!("PRAGMA wal_autocheckpoint = {pages}"));
    }
    if let Some(bytes) = knob("MVCC_THRESHOLD") {
        exec(
            &conn,
            &format!("PRAGMA mvcc_checkpoint_threshold = {bytes}"),
        );
    }
    exec(
        &conn,
        &format!(
            "CREATE TABLE users (id INTEGER PRIMARY KEY{}, name TEXT, email TEXT, age INTEGER)",
            if knob("AUTOINC").is_some() {
                " AUTOINCREMENT"
            } else {
                ""
            }
        ),
    );
    exec(&conn, "BEGIN");
    let mut stmt = conn
        .prepare("INSERT INTO users (name, email, age) VALUES (?, ?, ?)")
        .unwrap();
    for i in 0..10_000 {
        insert(&mut stmt, i);
    }
    exec(&conn, "COMMIT");

    // Best of 15 batches (robust against other load on the machine).
    let best = |label: &str, n: usize, op: &mut dyn FnMut(usize)| {
        let per_op = (0..15)
            .map(|b| {
                let t = Instant::now();
                for i in 0..n {
                    op(b * n + i);
                }
                t.elapsed().as_secs_f64() * 1e6 / n as f64
            })
            .fold(f64::MAX, f64::min);
        println!("{label:<40} {per_op:.2} us");
    };
    exec(&conn, "BEGIN");
    best("insert in a tx", 2000, &mut |i| insert(&mut stmt, i));
    exec(&conn, "COMMIT");
    best("insert autocommit", 300, &mut |i| insert(&mut stmt, i));

    measure(
        &format!("turso_core ({mode}) insert, autocommit"),
        time,
        |i| insert(&mut stmt, i),
    );
    measure(
        &format!("turso_core ({mode}) 1000 inserts in a tx"),
        time,
        |_| {
            exec(&conn, "BEGIN");
            for i in 0..1000 {
                insert(&mut stmt, i);
            }
            exec(&conn, "COMMIT");
        },
    );

    drop(stmt);
    conn.close().unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}
