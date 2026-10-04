use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use rustler::types::binary::NewBinary;
use rustler::{Binary, Encoder, Env, ResourceArc, Term};
use turso_core::{
    Buffer, CheckpointMode, Completion, Database, MemoryIO, OpenFlags, OpenOptions, PlatformIO,
    SqliteDialect, IO,
};

use crate::atoms;
use crate::conn::{closed, error_tuple, lock, ok_tuple, run_script, ConnRes, Handle};

static NEXT_TMP: AtomicU64 = AtomicU64::new(0);

const MEMORY_PATH: &str = ":memory:";
const HEADER_SIZE: usize = 100;

fn quote(ident: &str) -> String {
    format!("\"{}\"", ident.replace('"', "\"\""))
}

fn tmp_path() -> String {
    let n = NEXT_TMP.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir()
        .join(format!("sediment_serialize_{}_{n}.db", std::process::id()))
        .to_string_lossy()
        .into_owned()
}

/// Folds the WAL that `VACUUM INTO` leaves next to its output back into the
/// database file, so the file alone is a complete image.
fn checkpoint_copy(path: &str) -> Result<(), String> {
    let io: Arc<dyn IO> = Arc::new(PlatformIO::new().map_err(|e| e.to_string())?);
    let options = OpenOptions::new(Arc::new(SqliteDialect)).flags(OpenFlags::None);
    let db = Database::open(io, path, options).map_err(|e| e.to_string())?;
    let conn = db.connect().map_err(|e| e.to_string())?;
    conn.checkpoint(CheckpointMode::Truncate {
        upper_bound_inclusive: None,
    })
    .map_err(|e| e.to_string())?;
    conn.close().map_err(|e| e.to_string())
}

fn read_copy(path: &str) -> Result<Vec<u8>, String> {
    if !Path::new(path).exists() {
        // A database without any pages produces no file, like an empty
        // sqlite3_serialize image.
        return Ok(Vec::new());
    }
    checkpoint_copy(path)?;
    std::fs::read(path).map_err(|e| e.to_string())
}

fn serialize_handle(res: &ConnRes, handle: &Handle, database: &str) -> Result<Vec<u8>, String> {
    let path = tmp_path();
    let sql = format!(
        "VACUUM {} INTO '{}'",
        quote(database),
        path.replace('\'', "''")
    );
    let result = run_script(res, &handle.conn, &sql)
        .map_err(|step| match step {
            crate::stmt::Step::Error(msg) => msg,
            _ => "database is locked".to_string(),
        })
        .and_then(|()| read_copy(&path));
    for suffix in ["", "-wal", "-shm"] {
        let _ = std::fs::remove_file(format!("{path}{suffix}"));
    }
    result
}

#[rustler::nif(schedule = "DirtyIo")]
fn serialize<'a>(env: Env<'a>, res: ResourceArc<ConnRes>, database: String) -> Term<'a> {
    let guard = res.handle();
    let Some(handle) = guard.as_ref() else {
        return closed(env);
    };
    match serialize_handle(&res, handle, &database) {
        Ok(bytes) => {
            let mut bin = NewBinary::new(env, bytes.len());
            bin.as_mut_slice().copy_from_slice(&bytes);
            ok_tuple(env, Term::from(bin))
        }
        Err(msg) => error_tuple(env, msg),
    }
}

fn open_image(bytes: &[u8]) -> Result<Handle, String> {
    if bytes.len() < HEADER_SIZE || !bytes.starts_with(b"SQLite format 3\0") {
        return Err("file is not a database".to_string());
    }
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let file = io
        .open_file(MEMORY_PATH, OpenFlags::Create, false)
        .map_err(|e| e.to_string())?;
    let c = Completion::new_write(|_| {});
    let c = file
        .pwrite(0, Arc::new(Buffer::new(bytes.to_vec())), c)
        .map_err(|e| e.to_string())?;
    io.wait_for_completion(c).map_err(|e| e.to_string())?;

    let options = OpenOptions::new(Arc::new(SqliteDialect));
    let db = Database::open(io.clone(), MEMORY_PATH, options).map_err(|e| e.to_string())?;
    let conn = db.connect().map_err(|e| e.to_string())?;
    Ok(Handle {
        conn,
        _io: io,
        _db: db,
        s3: None,
        replica: None,
    })
}

/// Replaces the connection's database with an in-memory copy of `image`.
#[rustler::nif(schedule = "DirtyIo")]
fn deserialize<'a>(
    env: Env<'a>,
    res: ResourceArc<ConnRes>,
    database: String,
    image: Binary<'a>,
) -> Term<'a> {
    let mut guard = res.handle();
    if guard.is_none() {
        return closed(env);
    }
    if database != "main" {
        return error_tuple(env, "only the main database can be deserialized");
    }
    match open_image(image.as_slice()) {
        Ok(handle) => {
            *lock(&res.interrupt) = Some(handle.conn.clone());
            // Statements of the replaced database can't run against the new one.
            res.finalize_all();
            res.clear_s3_guard();
            if let Some(old) = guard.replace(handle) {
                let _ = old.conn.close();
            }
            atoms::ok().encode(env)
        }
        Err(msg) => error_tuple(env, msg),
    }
}
