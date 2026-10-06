use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, Weak};
use std::time::Duration;

use rustler::{Atom, Encoder, Env, LocalPid, Monitor, NifResult, ResourceArc, Term};
use turso_core::{Connection, Database, Statement, SyncMode, IO};

use crate::atoms;
use crate::error;
use crate::open::OpenConfig;
use crate::s3::S3DurableStorage;
use crate::stmt::{self, Step};

static NEXT_ID: AtomicU64 = AtomicU64::new(1);

/// Live connection and statement resources, for leak checks in tests.
pub static LIVE_CONNS: AtomicI64 = AtomicI64::new(0);
pub static LIVE_STMTS: AtomicI64 = AtomicI64::new(0);

pub struct Handle {
    pub conn: Arc<Connection>,
    // Held only to keep the database and its IO alive as long as the handle.
    pub _io: Arc<dyn IO>,
    pub _db: Arc<Database>,
    pub s3: Option<Arc<S3DurableStorage>>,
    pub replica: Option<crate::replica::Replica>,
    /// Counts this connection among the S3 storage's open ones until it
    /// starts closing (see `S3DurableStorage::attach`).
    pub attached: Option<crate::s3::Attached>,
}

impl From<crate::open::Opened> for Handle {
    fn from(opened: crate::open::Opened) -> Self {
        Handle {
            conn: opened.conn,
            _db: opened.db,
            _io: opened.io,
            attached: opened.s3.as_ref().map(crate::s3::Attached::new),
            s3: opened.s3,
            replica: opened.replica,
        }
    }
}

/// A connection resource. `handle` is locked for the whole duration of an
/// operation, which serializes use of the underlying turso connection.
/// `interrupt` holds a second reference that interrupt/cancel can reach
/// while an operation owns `handle`.
pub struct ConnRes {
    pub id: u64,
    handle: Mutex<Option<Handle>>,
    pub interrupt: Mutex<Option<Arc<Connection>>>,
    pub cancelled: AtomicBool,
    /// Set when turso panicked on this connection: its state is unknown, so
    /// the next operation closes it.
    broken: AtomicBool,
    /// Test hook: make the next guarded call into turso panic.
    pub panic_next_step: AtomicBool,
    /// S3 group commit uploads in `sync`, which turso only calls for
    /// `synchronous = FULL` commits, so the connection is held at FULL.
    force_full_sync: AtomicBool,
    /// Statements garbage collected while an operation held `handle`. They
    /// are finalized by the next operation instead of blocking the
    /// scheduler thread that ran the destructor.
    graveyard: Mutex<Vec<Statement>>,
    /// For S3 databases: the database and its storage (weak, so they don't
    /// outlive `close`), to check MVCC is still on around every step.
    s3_guard: Mutex<Option<(Weak<Database>, Weak<S3DurableStorage>)>>,
    /// Set by `close` (under the graveyard lock) once nothing will empty the
    /// graveyard anymore.
    closed: AtomicBool,
    /// Every statement prepared on this connection. Closing finalizes them,
    /// because a live statement keeps the database (and its S3 lease) open.
    statements: Mutex<Vec<Weak<StatementSlot>>>,
    /// While monitored (`monitor_owner`): a reference to this resource, so
    /// the owner's DOWN can hand the close to a thread of its own. Cleared
    /// by `close` and by the DOWN; only ever held for a take or a set.
    self_ref: Mutex<Option<ResourceArc<ConnRes>>>,
    /// An `execute` that returned `{:sleep, ms}`: the statement waiting out
    /// a busy backoff and the rest of the script, for `execute_resume`.
    pending_script: Mutex<Option<PendingScript>>,
    /// What this connection's current autocommit statement or transaction
    /// committed, for `sync: true` (`s3_flush_commit`).
    commit_mark: Mutex<CommitMark>,
    /// A file database (not in memory, not a replica): its path as opened
    /// and the database, so a switch to MVCC can claim the log.
    pub file: Option<(String, Weak<Database>)>,
    /// The database was opened with `experimental: [:attach]`, so it may not
    /// switch to MVCC (see `log_guard`).
    pub attach: bool,
}

/// See `ConnRes::commit_mark`.
#[derive(Default)]
pub struct CommitMark {
    /// A statement that writes ran (so something may have been committed).
    pub wrote: bool,
    /// The upload queue sequence of the last commit this connection's
    /// thread queued (`durability: async`).
    pub seq: Option<u64>,
}

/// See `ConnRes::pending_script`.
pub struct PendingScript {
    statement: Statement,
    rest: String,
    baseline: Option<u64>,
    /// The statement switches to MVCC: checked again when it resumes.
    switches_to_mvcc: bool,
}

pub type StatementSlot = Mutex<Option<Statement>>;

#[rustler::resource_impl]
impl rustler::Resource for ConnRes {
    const IMPLEMENTS_DOWN: bool = true;

    /// The owner (see `monitor_owner`) died without closing, for example a
    /// DBConnection process killed by its supervisor. Statements cached in
    /// other processes would otherwise keep the database, and an S3 lease,
    /// alive. This runs on a normal scheduler, so it only takes what it can
    /// without waiting and closes on the cleanup thread.
    fn down<'a>(&'a self, _env: Env<'a>, _pid: LocalPid, _monitor: Monitor) {
        // Never wait here (normal scheduler): closing may wait for a running
        // statement or an S3 request, so it gets a thread of its own.
        let Some(me) = lock(&self.self_ref).take() else {
            return;
        };
        // An operation still running (say, a commit waiting on S3) gives up.
        me.cancelled.store(true, Ordering::SeqCst);
        let spawned = std::thread::Builder::new()
            .name("sediment_owner_down".into())
            .spawn(move || me.close_now());
        if let Err(err) = spawned {
            eprintln!("sediment: could not close a connection after its owner died: {err}");
        }
    }
}

impl Drop for ConnRes {
    /// A connection that was never closed is closed on the cleanup thread:
    /// closing can checkpoint, and S3 storage releases its lease over the
    /// network.
    fn drop(&mut self) {
        LIVE_CONNS.fetch_sub(1, Ordering::Relaxed);
        let graveyard = std::mem::take(self.graveyard.get_mut().unwrap_or_else(|e| e.into_inner()));
        let handle = self
            .handle
            .get_mut()
            .unwrap_or_else(|e| e.into_inner())
            .take();
        if graveyard.is_empty() && handle.is_none() {
            return;
        }
        crate::cleanup::defer(move || {
            drop(graveyard);
            if let Some(handle) = handle {
                let _ = handle.conn.close();
            }
        });
    }
}

/// The locked handle, held for the whole of an operation.
pub type HandleGuard<'a> = MutexGuard<'a, Option<Handle>>;

pub fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

impl ConnRes {
    /// A resource without a handle, to drive statements in unit tests.
    #[cfg(test)]
    pub fn detached() -> ConnRes {
        LIVE_CONNS.fetch_add(1, Ordering::Relaxed);
        ConnRes {
            id: NEXT_ID.fetch_add(1, Ordering::Relaxed),
            handle: Mutex::new(None),
            interrupt: Mutex::new(None),
            cancelled: AtomicBool::new(false),
            broken: AtomicBool::new(false),
            panic_next_step: AtomicBool::new(false),
            self_ref: Mutex::new(None),
            s3_guard: Mutex::new(None),
            force_full_sync: AtomicBool::new(false),
            graveyard: Mutex::new(Vec::new()),
            closed: AtomicBool::new(false),
            statements: Mutex::new(Vec::new()),
            pending_script: Mutex::new(None),
            commit_mark: Mutex::new(CommitMark::default()),
            file: None,
            attach: false,
        }
    }

    /// S3 durability only hooks MVCC commits. If SQL switched the database to
    /// another journal mode, commits would be acknowledged without reaching
    /// S3: poison the storage and refuse to go on.
    pub fn check_s3_mvcc(&self) -> Result<(), String> {
        let guard = lock(&self.s3_guard);
        let Some((db, storage)) = guard.as_ref() else {
            return Ok(());
        };
        let (Some(db), Some(storage)) = (db.upgrade(), storage.upgrade()) else {
            return Ok(());
        };
        if let Some(reason) = storage.fenced() {
            // Reads too: this connection's view may miss what another writer
            // committed, and only a reconnect restores from S3.
            return Err(format!("s3 writer fenced: {reason}"));
        }
        if db.mvcc_enabled() {
            return Ok(());
        }
        let reason = "the database left MVCC journal mode, which S3 durability requires; \
                      commits since are not in S3; reopen the database";
        storage.poison(reason);
        Err(format!("s3 writer fenced: {reason}"))
    }

    pub fn clear_s3_guard(&self) {
        lock(&self.s3_guard).take();
    }

    /// Takes the handle and the statements out and closes them on the
    /// cleanup thread. Call with `handle` held.
    fn close_detached(&self, guard: &mut Option<Handle>) {
        lock(&self.interrupt).take();
        self.clear_s3_guard();
        let pending = lock(&self.pending_script).take();
        let slots: Vec<_> = std::mem::take(&mut *lock(&self.statements))
            .iter()
            .filter_map(Weak::upgrade)
            .collect();
        let graveyard = std::mem::take(&mut *lock(&self.graveyard));
        let mut handle = guard.take();
        if let Some(handle) = &mut handle {
            handle.attached.take();
        }
        crate::cleanup::defer(move || {
            drop(pending);
            for slot in slots {
                drop(lock(&slot).take());
            }
            drop(graveyard);
            if let Some(handle) = handle {
                let _ = handle.conn.close();
            }
        });
    }

    /// Closes the connection, waiting for a running operation first (until
    /// then interrupt/1 must still be able to reach it).
    pub fn close_now(&self) {
        let mut guard = self.handle();
        lock(&self.interrupt).take();
        self.finalize_all();
        self.clear_s3_guard();
        if let Some(mut handle) = guard.take() {
            // Closing from here on: not counted as open (a destroy waits for
            // the close instead of refusing).
            handle.attached.take();
            // `durability: async`: upload what this database committed so far.
            if let Some(storage) = &handle.s3 {
                storage.close(storage.close_timeout());
            }
            // A panicking close (after an earlier turso panic) must not escape.
            let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| handle.conn.close()));
        }
        // Statements collected while this held the lock were parked; from now
        // on finalize drops them directly.
        let parked = {
            let mut graveyard = lock(&self.graveyard);
            self.closed.store(true, Ordering::SeqCst);
            std::mem::take(&mut *graveyard)
        };
        drop(guard);
        drop(parked);
    }

    pub fn handle(&self) -> HandleGuard<'_> {
        let mut guard = lock(&self.handle);
        if self.broken.swap(false, Ordering::SeqCst) {
            self.close_detached(&mut guard);
        }
        self.bury();
        if let Some(handle) = guard.as_ref() {
            self.enforce_sync_mode(&handle.conn);
        }
        guard
    }

    pub fn mark_broken(&self) {
        self.broken.store(true, Ordering::SeqCst);
    }

    pub fn enforce_sync_mode(&self, conn: &Connection) {
        if self.force_full_sync.load(Ordering::Relaxed) && conn.get_sync_mode() != SyncMode::Full {
            conn.set_sync_mode(SyncMode::Full);
        }
    }

    fn bury(&self) {
        let dead = std::mem::take(&mut *lock(&self.graveyard));
        drop(dead);
    }

    pub fn track(&self, slot: &Arc<StatementSlot>) {
        let mut statements = lock(&self.statements);
        statements.retain(|weak| weak.strong_count() > 0);
        statements.push(Arc::downgrade(slot));
    }

    /// Finalizes every statement of the connection. Call with `handle` held.
    /// A statement starts executing: outside a transaction a new unit of
    /// work begins, and a statement that writes marks it.
    pub fn note_start(&self, conn: &Connection, writes: bool) {
        let mut mark = lock(&self.commit_mark);
        if conn.get_auto_commit() {
            *mark = CommitMark::default();
        }
        mark.wrote |= writes;
    }

    /// Records a commit the current thread just queued for upload.
    pub fn note_queued(&self, seq: u64) {
        let mut mark = lock(&self.commit_mark);
        mark.seq = Some(mark.seq.map_or(seq, |s| s.max(seq)));
    }

    /// What `sync: true` has to wait for, and a fresh mark.
    pub fn take_commit_mark(&self) -> CommitMark {
        std::mem::take(&mut *lock(&self.commit_mark))
    }

    pub fn finalize_all(&self) {
        drop(lock(&self.pending_script).take());
        let statements = std::mem::take(&mut *lock(&self.statements));
        for slot in statements.iter().filter_map(Weak::upgrade) {
            drop(lock(&slot).take());
        }
        self.bury();
    }

    /// Finalizes a statement under the connection lock, or defers it to the
    /// next operation when the lock is busy.
    pub fn finalize(&self, statement: Statement) {
        match self.handle.try_lock() {
            Ok(_guard) => {
                drop(statement);
                self.bury();
            }
            Err(_) => {
                let mut graveyard = lock(&self.graveyard);
                if self.closed.load(Ordering::SeqCst) {
                    // The connection is closed: nothing can race this drop.
                    drop(graveyard);
                    drop(statement);
                } else {
                    graveyard.push(statement);
                }
            }
        }
    }
}

pub fn ok_tuple<'a, T: Encoder>(env: Env<'a>, value: T) -> Term<'a> {
    (atoms::ok(), value).encode(env)
}

pub fn error_tuple<'a, T: Encoder>(env: Env<'a>, reason: T) -> Term<'a> {
    (atoms::error(), reason).encode(env)
}

pub fn closed(env: Env<'_>) -> Term<'_> {
    error_tuple(env, atoms::connection_closed())
}

/// Runs `sql` (one or more statements) to completion, discarding rows,
/// sleeping out busy backoffs here: for internal statements only (see
/// `stmt::advance_blocking`); `execute` hands the waits to its caller.
pub fn run_script(res: &ConnRes, conn: &Arc<Connection>, sql: &str) -> Result<(), Step> {
    let mut remaining = sql;
    loop {
        let parsed = stmt::guarded(res, || conn.consume_stmt(remaining))?;
        let (mut statement, consumed) = match parsed {
            Ok(Some(parsed)) => parsed,
            Ok(None) => return Ok(()),
            Err(e) => return Err(Step::Error(error::message(&e))),
        };
        crate::mvcc_guard::check_statement(res, conn, &remaining[..consumed])?;
        loop {
            match stmt::advance_blocking(res, &mut statement)? {
                Step::Row => continue,
                Step::Done => break,
                other => return Err(other),
            }
        }
        res.enforce_sync_mode(conn);
        remaining = &remaining[consumed..];
    }
}

/// A script `execute` parked while waiting out a busy backoff.
type ScriptWait = Option<(Duration, PendingScript)>;

/// Runs a script for `execute`, starting with `current` (a statement that
/// was waiting) if any, then the statements of `rest`. Returns `Ok(None)`
/// when done, or the wait to hand back to the caller with the script
/// parked to resume.
fn run_script_resumable(
    res: &ConnRes,
    conn: &Arc<Connection>,
    mut current: Option<(Statement, Option<u64>, bool)>,
    rest: &str,
) -> Result<ScriptWait, Step> {
    let mut remaining = rest;
    loop {
        let (mut statement, mut baseline, switches_to_mvcc) = match current.take() {
            Some(waiting) => waiting,
            None => {
                let parsed = stmt::guarded(res, || conn.consume_stmt(remaining))?;
                match parsed {
                    Ok(Some((statement, consumed))) => {
                        let switches = crate::mvcc_guard::requests_mvcc(&remaining[..consumed]);
                        remaining = &remaining[consumed..];
                        res.note_start(conn, stmt::writes(&statement));
                        (statement, None, switches)
                    }
                    Ok(None) => return Ok(None),
                    Err(e) => return Err(Step::Error(error::message(&e))),
                }
            }
        };
        // Checked right before it runs, and again on every resume after a
        // busy wait: an earlier statement of the script, or another
        // connection during the wait, may have created an AUTOINCREMENT table.
        if switches_to_mvcc {
            crate::mvcc_guard::check_switch(res, conn)?;
        }
        loop {
            match stmt::advance_with(res, &mut statement, &mut baseline)? {
                Step::Row => continue,
                Step::Done => break,
                Step::Sleep(duration) => {
                    let pending = PendingScript {
                        statement,
                        rest: remaining.to_string(),
                        baseline,
                        switches_to_mvcc,
                    };
                    return Ok(Some((duration, pending)));
                }
                other => return Err(other),
            }
        }
        res.enforce_sync_mode(conn);
    }
}

/// The outcome of `run_script_resumable` as `execute`'s result.
fn script_result<'a>(env: Env<'a>, res: &ConnRes, outcome: Result<ScriptWait, Step>) -> Term<'a> {
    match outcome {
        Ok(None) => atoms::ok().encode(env),
        Ok(Some((duration, pending))) => {
            *lock(&res.pending_script) = Some(pending);
            (atoms::sleep(), stmt::millis(duration)).encode(env)
        }
        Err(step) => step.into_error(env),
    }
}

type OpenLocks = HashMap<PathBuf, Weak<Mutex<()>>>;

static OPEN_LOCKS: OnceLock<Mutex<OpenLocks>> = OnceLock::new();

/// The per-file lock serializing opens, or `None` for in-memory databases.
fn path_lock(path: &str) -> Option<Arc<Mutex<()>>> {
    let trimmed = path.trim();
    if trimmed.is_empty() || trimmed.starts_with(":memory:") || trimmed.starts_with("file::memory:")
    {
        return None;
    }
    let path = Path::new(trimmed);
    // A file reached through a symlink is locked under its target's name, so
    // an open through the link waits for an S3 restore of the file (which
    // replaces it). A hard link to an S3 database is left behind by any
    // restore (it keeps naming the old file): not supported.
    let key = match (path.parent(), path.file_name()) {
        _ if path.exists() => std::fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf()),
        (Some(dir), Some(name)) => std::fs::canonicalize(if dir.as_os_str().is_empty() {
            Path::new(".")
        } else {
            dir
        })
        .map(|dir| dir.join(name))
        .unwrap_or_else(|_| path.to_path_buf()),
        _ => path.to_path_buf(),
    };

    let mut locks = lock(OPEN_LOCKS.get_or_init(Default::default));
    locks.retain(|_, weak| weak.strong_count() > 0);
    if let Some(existing) = locks.get(&key).and_then(Weak::upgrade) {
        return Some(existing);
    }
    let fresh = Arc::new(Mutex::new(()));
    locks.insert(key, Arc::downgrade(&fresh));
    Some(fresh)
}

/// How long `open` retries switching the journal mode while another
/// connection to the same file holds the lock, e.g. a pool whose
/// connections all open a new database in MVCC mode at once.
const JOURNAL_MODE_RETRY: Duration = Duration::from_secs(5);

fn current_journal_mode(res: &ConnRes, conn: &Arc<Connection>) -> Result<String, Step> {
    let mut statement = conn
        .prepare("PRAGMA journal_mode")
        .map_err(|e| Step::Error(error::message(&e)))?;
    match stmt::advance_blocking(res, &mut statement)? {
        Step::Row => Ok(statement
            .row()
            .and_then(|row| {
                row.get_values()
                    .next()
                    .and_then(|v| v.to_text())
                    .map(str::to_owned)
            })
            .unwrap_or_default()),
        Step::Done => Ok(String::new()),
        other => Err(other),
    }
}

fn set_journal_mode(res: &ConnRes, conn: &Arc<Connection>, mode: &str) -> Result<(), Step> {
    let wanted = if mode == "experimental_mvcc" {
        "mvcc"
    } else {
        mode
    };
    let pragma = format!("PRAGMA journal_mode = '{}'", mode.replace('\'', "''"));
    let deadline = std::time::Instant::now() + JOURNAL_MODE_RETRY;
    loop {
        let result = match current_journal_mode(res, conn) {
            Ok(current) if current == wanted => return Ok(()),
            // run_script refuses an MVCC switch the database can't take.
            Ok(_) => run_script(res, conn, &pragma),
            Err(step) => Err(step),
        };
        match result {
            Err(Step::Busy) if std::time::Instant::now() < deadline => {
                std::thread::sleep(Duration::from_millis(10));
            }
            Err(Step::Error(msg))
                if msg == "database is locked" && std::time::Instant::now() < deadline =>
            {
                std::thread::sleep(Duration::from_millis(10));
            }
            other => return other,
        }
    }
}

#[rustler::nif(schedule = "DirtyIo")]
fn open<'a>(env: Env<'a>, path: String, opts: Term<'a>) -> NifResult<Term<'a>> {
    let config = match OpenConfig::decode(path, opts) {
        Ok(config) => config,
        Err(reason) => return Ok(error_tuple(env, reason)),
    };
    // Opens of one file are serialized (per file, never VM-wide: an S3 open
    // may wait on the network for minutes), so concurrent opens don't race
    // turso's switch into MVCC mode, or an S3 restore replacing the file.
    let open_lock = path_lock(&config.path);
    let _opening = open_lock.as_deref().map(lock);

    // Opening parses the schema (and may restore from S3): on the turso
    // stack, with panics reported as errors like any other turso call.
    let opened = stmt::with_turso_stack(|| {
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| config.open()))
    });
    let opened = match opened {
        Ok(Ok(opened)) => opened,
        Ok(Err(reason)) => return Ok(error_tuple(env, reason)),
        Err(panic) => {
            let reason = stmt::panic_reason(&*panic);
            return Ok(error_tuple(env, format!("internal turso error: {reason}")));
        }
    };

    LIVE_CONNS.fetch_add(1, Ordering::Relaxed);
    let res = ConnRes {
        id: NEXT_ID.fetch_add(1, Ordering::Relaxed),
        handle: Mutex::new(None),
        interrupt: Mutex::new(Some(opened.conn.clone())),
        cancelled: AtomicBool::new(false),
        broken: AtomicBool::new(false),
        panic_next_step: AtomicBool::new(false),
        self_ref: Mutex::new(None),
        s3_guard: Mutex::new(
            opened
                .s3
                .as_ref()
                .map(|storage| (Arc::downgrade(&opened.db), Arc::downgrade(storage))),
        ),
        force_full_sync: AtomicBool::new(
            opened
                .s3
                .as_ref()
                .is_some_and(|storage| storage.group_commit()),
        ),
        graveyard: Mutex::new(Vec::new()),
        closed: AtomicBool::new(false),
        statements: Mutex::new(Vec::new()),
        pending_script: Mutex::new(None),
        commit_mark: Mutex::new(CommitMark::default()),
        file: (!config.is_memory() && opened.replica.is_none())
            .then(|| (config.path.clone(), Arc::downgrade(&opened.db))),
        attach: opened.db.experimental_attach_enabled(),
    };

    if let Some(mode) = &config.journal_mode {
        if let Err(step) = set_journal_mode(&res, &opened.conn, mode) {
            let _ = opened.conn.close();
            return Ok(step.into_error(env));
        }
    }

    *res.handle() = Some(Handle::from(opened));
    Ok(ok_tuple(env, ResourceArc::new(res)))
}

#[rustler::nif(schedule = "DirtyIo")]
fn close(res: ResourceArc<ConnRes>) -> Atom {
    lock(&res.self_ref).take();
    res.close_now();
    atoms::ok()
}

#[rustler::nif(schedule = "DirtyIo")]
fn execute<'a>(env: Env<'a>, res: ResourceArc<ConnRes>, sql: String) -> Term<'a> {
    let guard = res.handle();
    let Some(handle) = guard.as_ref() else {
        return closed(env);
    };
    res.cancelled.store(false, Ordering::SeqCst);
    // A script abandoned mid-wait (its caller gave up) is dropped.
    drop(lock(&res.pending_script).take());
    if crate::mvcc_guard::requests_mvcc(&sql) {
        if let Err(step) = crate::mvcc_guard::check_switch(&res, &handle.conn) {
            return step.into_error(env);
        }
    }
    let outcome = run_script_resumable(&res, &handle.conn, None, &sql);
    script_result(env, &res, outcome)
}

/// Continues an `execute` that returned `{:sleep, ms}` after the caller
/// waited; a `cancel/1` issued during the wait still applies.
#[rustler::nif(schedule = "DirtyIo")]
fn execute_resume(env: Env<'_>, res: ResourceArc<ConnRes>) -> Term<'_> {
    let guard = res.handle();
    let Some(handle) = guard.as_ref() else {
        return closed(env);
    };
    let Some(pending) = lock(&res.pending_script).take() else {
        return error_tuple(env, "no execute to resume");
    };
    let outcome = run_script_resumable(
        &res,
        &handle.conn,
        Some((
            pending.statement,
            pending.baseline,
            pending.switches_to_mvcc,
        )),
        &pending.rest,
    );
    script_result(env, &res, outcome)
}

#[rustler::nif]
fn interrupt(res: ResourceArc<ConnRes>) -> Atom {
    if let Some(conn) = lock(&res.interrupt).as_ref() {
        conn.interrupt();
    }
    atoms::ok()
}

#[rustler::nif]
fn cancel(res: ResourceArc<ConnRes>) -> Atom {
    if let Some(conn) = lock(&res.interrupt).as_ref() {
        res.cancelled.store(true, Ordering::SeqCst);
        conn.interrupt();
    }
    atoms::ok()
}

fn with_conn<'a>(
    env: Env<'a>,
    res: &ConnRes,
    f: impl FnOnce(&Arc<Connection>) -> Term<'a>,
) -> Term<'a> {
    match res.handle().as_ref() {
        Some(handle) => f(&handle.conn),
        None => closed(env),
    }
}

#[rustler::nif(schedule = "DirtyIo")]
fn set_busy_timeout(env: Env<'_>, res: ResourceArc<ConnRes>, timeout_ms: i64) -> Term<'_> {
    with_conn(env, &res, |conn| {
        conn.set_busy_timeout(Duration::from_millis(timeout_ms.max(0) as u64));
        atoms::ok().encode(env)
    })
}

#[rustler::nif(schedule = "DirtyIo")]
fn set_progress_handler_steps(env: Env<'_>, res: ResourceArc<ConnRes>, _steps: i64) -> Term<'_> {
    // Interrupt and cancel reach turso's VDBE directly, so no progress
    // handler is needed; the call only validates the connection.
    with_conn(env, &res, |_conn| atoms::ok().encode(env))
}

#[rustler::nif(schedule = "DirtyIo")]
fn changes(env: Env<'_>, res: ResourceArc<ConnRes>) -> Term<'_> {
    with_conn(env, &res, |conn| ok_tuple(env, conn.changes()))
}

#[rustler::nif(schedule = "DirtyIo")]
fn total_changes(env: Env<'_>, res: ResourceArc<ConnRes>) -> Term<'_> {
    with_conn(env, &res, |conn| ok_tuple(env, conn.total_changes()))
}

#[rustler::nif(schedule = "DirtyIo")]
fn last_insert_rowid(env: Env<'_>, res: ResourceArc<ConnRes>) -> Term<'_> {
    with_conn(env, &res, |conn| ok_tuple(env, conn.last_insert_rowid()))
}

#[rustler::nif(schedule = "DirtyIo")]
fn transaction_status(env: Env<'_>, res: ResourceArc<ConnRes>) -> Term<'_> {
    with_conn(env, &res, |conn| {
        let status = if conn.get_auto_commit() {
            atoms::idle()
        } else {
            atoms::transaction()
        };
        ok_tuple(env, status)
    })
}

/// Closes the connection when the calling process exits, however it exits.
/// `Sediment.Connection` calls this from its connection process.
#[rustler::nif]
fn monitor_owner(env: Env<'_>, res: ResourceArc<ConnRes>) -> Atom {
    *lock(&res.self_ref) = Some(res.clone());
    res.monitor(Some(env), &env.pid());
    atoms::ok()
}

/// `:ok`, or the fenced error of an S3 database that must be reopened
/// (DBConnection's ping uses this to disconnect idle connections). Dirty:
/// the storage mutex is held across S3 requests while a commit uploads.
#[rustler::nif(schedule = "DirtyIo")]
fn s3_check<'a>(env: Env<'a>, res: ResourceArc<ConnRes>) -> Term<'a> {
    match res.check_s3_mvcc() {
        Ok(()) => atoms::ok().encode(env),
        Err(reason) => error_tuple(env, reason),
    }
}

#[rustler::nif]
fn resource_counts() -> (i64, i64) {
    (
        LIVE_CONNS.load(Ordering::Relaxed),
        LIVE_STMTS.load(Ordering::Relaxed),
    )
}

/// Test hook: the next step on this connection panics inside turso's guard.
#[rustler::nif]
fn debug_panic_next_step(res: ResourceArc<ConnRes>) -> Atom {
    res.panic_next_step.store(true, Ordering::SeqCst);
    atoms::ok()
}
