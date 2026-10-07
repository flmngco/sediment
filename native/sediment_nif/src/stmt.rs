use std::num::NonZero;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use rustler::{Atom, Encoder, Env, Error, NifResult, ResourceArc, Term};
use turso_core::{LimboError, Statement, StepResult, Value};

use crate::atoms;
use crate::conn::{closed, error_tuple, lock, ok_tuple, ConnRes};
use crate::error;
use crate::value;

/// Longest uninterrupted sleep while waiting out a busy handler backoff, so
/// that `cancel/1` is noticed promptly.
const CANCEL_POLL: Duration = Duration::from_millis(5);

#[cfg(test)]
thread_local! {
    /// Test hook: runs whenever `advance` is about to wait out a busy backoff.
    static ON_BUSY_SLEEP: std::cell::RefCell<Option<Box<dyn FnMut()>>> =
        const { std::cell::RefCell::new(None) };
    /// Test hook: runs right before each call into turso's `step`, after the
    /// cancellation checks.
    static BEFORE_STEP: std::cell::RefCell<Option<Box<dyn FnMut()>>> =
        const { std::cell::RefCell::new(None) };
}

pub struct StmtRes {
    conn: ResourceArc<ConnRes>,
    stmt: Arc<Mutex<Option<Statement>>>,
    /// Rows written when the current execution started, kept across the
    /// calls that resume it (see `busy_inner_commit`).
    baseline: Mutex<Option<u64>>,
    /// `writes(statement)`, computed once.
    writes: std::sync::OnceLock<bool>,
    /// The statement switches the journal mode to MVCC: checked by
    /// `mvcc_guard` before every run, since the database may have gained
    /// AUTOINCREMENT tables since it was prepared.
    switches_to_mvcc: bool,
}

#[rustler::resource_impl]
impl rustler::Resource for StmtRes {}

impl Drop for StmtRes {
    fn drop(&mut self) {
        crate::conn::LIVE_STMTS.fetch_sub(1, Ordering::Relaxed);
        // Finalizing touches connection state, so it must happen under the
        // connection lock like every other statement operation.
        if let Some(statement) = lock(&self.stmt).take() {
            let conn = self.conn.clone();
            crate::cleanup::defer(move || conn.finalize(statement));
        }
    }
}

pub enum Step {
    Row,
    Done,
    Busy,
    /// turso's busy handler asks to wait this long, then step again. Not
    /// slept here: a dirty scheduler thread sleeping on a lock is one the
    /// lock holder may need to commit, so the wait goes back to
    /// the caller, which sleeps in its own process.
    Sleep(Duration),
    Error(String),
}

impl Step {
    pub fn into_error(self, env: Env<'_>) -> Term<'_> {
        match self {
            Step::Error(msg) => error_tuple(env, msg),
            _ => error_tuple(env, "database is locked"),
        }
    }
}

fn from_err(err: LimboError) -> Step {
    if error::is_busy(&err) {
        Step::Busy
    } else {
        Step::Error(error::message(&err))
    }
}

fn cancelled(res: &ConnRes, stmt: &mut Statement) -> Step {
    res.consume_cancel();
    let _ = guarded(res, || stmt.reset());
    Step::Error("interrupted".to_string())
}

/// Stack that calls into turso_core run on. turso compiles expressions and
/// runs trigger programs recursively, and a dirty scheduler's stack (about
/// 320 KB by default) overflowed on `SELECT 1 + 1 + ...` with 50 terms
/// (under turso's expression depth limit of 100) and on an INSERT firing a
/// chain of 100 triggers: a stack overflow on a scheduler thread kills the
/// VM. So turso runs on a stack of this size, allocated once per scheduler
/// thread (lazily committed) and reused; switching to it is cheap.
const TURSO_STACK: usize = 16 * 1024 * 1024;

/// Runs `f` (a call into turso_core) on this thread's turso stack; in place
/// when already on it.
pub fn with_turso_stack<T>(f: impl FnOnce() -> T) -> T {
    turso_stack::run(f)
}

/// Elsewhere (Windows): a stack grown per call, slower but portable.
#[cfg(not(unix))]
mod turso_stack {
    pub fn run<T>(f: impl FnOnce() -> T) -> T {
        stacker::maybe_grow(super::TURSO_STACK / 2, super::TURSO_STACK, f)
    }
}

/// On Unix: an mmap'ed stack per thread, reused.
#[cfg(unix)]
mod turso_stack {
    use std::cell::{Cell, RefCell};
    use std::panic::{self, AssertUnwindSafe};

    use super::TURSO_STACK;

    /// An mmap'ed stack with a guard page below it.
    struct Stack {
        base: *mut libc::c_void,
        len: usize,
        page: usize,
    }

    impl Stack {
        fn new() -> Option<Self> {
            // SAFETY: plain anonymous mapping; checked for MAP_FAILED.
            let page = unsafe { libc::sysconf(libc::_SC_PAGESIZE) } as usize;
            let len = TURSO_STACK + page;
            let base = unsafe {
                libc::mmap(
                    std::ptr::null_mut(),
                    len,
                    libc::PROT_READ | libc::PROT_WRITE,
                    libc::MAP_PRIVATE | libc::MAP_ANON | libc::MAP_NORESERVE,
                    -1,
                    0,
                )
            };
            if base == libc::MAP_FAILED {
                return None;
            }
            // SAFETY: the first page of the mapping just created.
            if unsafe { libc::mprotect(base, page, libc::PROT_NONE) } != 0 {
                unsafe { libc::munmap(base, len) };
                return None;
            }
            Some(Self { base, len, page })
        }

        fn usable(&self) -> (*mut u8, usize) {
            // SAFETY: stays inside the mapping.
            (
                unsafe { (self.base as *mut u8).add(self.page) },
                self.len - self.page,
            )
        }
    }

    impl Drop for Stack {
        fn drop(&mut self) {
            // SAFETY: the mapping from Stack::new, no longer in use.
            unsafe { libc::munmap(self.base, self.len) };
        }
    }

    thread_local! {
        static STACK: RefCell<Option<Stack>> = const { RefCell::new(None) };
        static ACTIVE: Cell<bool> = const { Cell::new(false) };
    }

    pub fn run<T>(f: impl FnOnce() -> T) -> T {
        if ACTIVE.with(Cell::get) {
            return f();
        }
        let usable = STACK.with(|slot| {
            let mut slot = slot.borrow_mut();
            if slot.is_none() {
                *slot = Stack::new();
            }
            slot.as_ref().map(Stack::usable)
        });
        let Some((base, size)) = usable else {
            // No stack to switch to: grow one per call instead.
            return stacker::grow(TURSO_STACK, f);
        };
        ACTIVE.with(|active| active.set(true));
        let mut outcome = None;
        // SAFETY: `base..base + size` is this thread's stack mapping, not in
        // use (ACTIVE guards reentry) and alive until the thread exits.
        // Panics are caught on that stack and resumed after switching back.
        unsafe {
            psm::on_stack(base, size, || {
                outcome = Some(panic::catch_unwind(AssertUnwindSafe(f)));
            });
        }
        ACTIVE.with(|active| active.set(false));
        match outcome.expect("turso stack callback ran") {
            Ok(value) => value,
            Err(payload) => panic::resume_unwind(payload),
        }
    }
}

/// Like [`guarded`], for NIFs that report errors as plain messages.
fn guarded_msg<T>(res: &ConnRes, f: impl FnOnce() -> T) -> Result<T, String> {
    guarded(res, f).map_err(|step| match step {
        Step::Error(msg) => msg,
        _ => "internal turso error".to_string(),
    })
}

/// The message a panic carried.
pub fn panic_reason(panic: &(dyn std::any::Any + Send)) -> String {
    panic
        .downcast_ref::<&str>()
        .map(|s| s.to_string())
        .or_else(|| panic.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "unknown panic".to_string())
}

/// Runs a call into turso_core, turning a panic into an error. The
/// connection's state is unknown after a panic, so it is closed.
pub fn guarded<T>(res: &ConnRes, f: impl FnOnce() -> T) -> Result<T, Step> {
    with_turso_stack(|| {
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            if res.panic_next_step.swap(false, Ordering::SeqCst) {
                panic!("panic requested by Sediment.Native.debug_panic_next_step/1");
            }
            f()
        }))
    })
    .map_err(|panic| {
        res.mark_broken();
        let reason = panic_reason(&*panic);
        Step::Error(format!(
            "internal turso error: {reason}; the connection was closed"
        ))
    })
}

/// Steps until the statement produces a row, finishes, or has to wait out a
/// busy backoff (`Step::Sleep`), driving turso's IO loop in between.
/// `baseline` carries the rows written at the start of this execution
/// across calls that resume it.
pub fn advance_with(
    res: &ConnRes,
    stmt: &mut Statement,
    baseline: &mut Option<u64>,
) -> Result<Step, Step> {
    // A commit this step queues for upload is this connection's (for
    // `sync: true`); a value left by other work on this thread is not.
    let _ = crate::s3::take_last_enqueued();
    let _running = res.running();
    // cancel/1 also gives up an S3 upload this step is waiting on.
    let step =
        crate::s3::remote::with_cancel(&res.cancelled, || advance_inner(res, stmt, baseline));
    if let Some(seq) = crate::s3::take_last_enqueued() {
        res.note_queued(seq);
    }
    step
}

/// Whether a statement may commit something: it writes, and is not
/// transaction control (`BEGIN IMMEDIATE` is not read-only, but only a
/// statement after it writes).
pub fn writes(stmt: &Statement) -> bool {
    let program = stmt.get_program();
    !program.is_readonly()
        && !program
            .prepared()
            .insns
            .iter()
            .any(|(insn, _)| format!("{insn:?}").starts_with("AutoCommit"))
}

/// `advance_with` for callers that can't hand a wait back: internal
/// statements (journal mode at open, `VACUUM INTO`) sleep here, still
/// giving up on `cancel/1`.
pub fn advance_blocking(res: &ConnRes, stmt: &mut Statement) -> Result<Step, Step> {
    let mut baseline = None;
    loop {
        match advance_with(res, stmt, &mut baseline)? {
            Step::Sleep(duration) => {
                let deadline = Instant::now() + duration;
                loop {
                    if res.cancelled.load(Ordering::SeqCst) {
                        return Err(cancelled(res, stmt));
                    }
                    let now = Instant::now();
                    if now >= deadline {
                        break;
                    }
                    std::thread::sleep((deadline - now).min(CANCEL_POLL));
                }
            }
            other => return Ok(other),
        }
    }
}

/// Whether the statement allocates AUTOINCREMENT values in an inner MVCC
/// transaction of its own (inside `BEGIN CONCURRENT`), itself or in a
/// trigger (a subprogram, printed as part of its `Program` instruction).
fn commits_inner_tx(stmt: &Statement) -> bool {
    stmt.get_program()
        .prepared()
        .insns
        .iter()
        .any(|(insn, _)| format!("{insn:?}").contains("SequenceCommitInnerTx"))
}

/// A busy statement is retried at the instruction that got busy. turso_core
/// 0.8.1 gets that wrong for the commit of an AUTOINCREMENT inner
/// transaction: the failed commit already switched the connection back to
/// the surrounding transaction, and the retry commits that one instead, with
/// whatever it wrote so far (docs/upstream/turso-sequence-inner-tx-busy.md).
/// Such a statement is reset instead and reported busy; the transaction then
/// has to be rolled back, like after a write-write conflict. Its writes come
/// before that commit, while a statement busy at its start wrote nothing.
fn busy_inner_commit(stmt: &Statement, written_before: Option<u64>) -> bool {
    written_before.is_some_and(|before| stmt.metrics().rows_written > before)
        && commits_inner_tx(stmt)
}

fn advance_inner(
    res: &ConnRes,
    stmt: &mut Statement,
    baseline: &mut Option<u64>,
) -> Result<Step, Step> {
    res.check_s3_mvcc().map_err(Step::Error)?;
    // A cancel/1 issued since `Sediment.Engine` started this operation:
    // while its call waited for a scheduler, or during a busy backoff.
    if res.cancelled.load(Ordering::SeqCst) {
        return Err(cancelled(res, stmt));
    }
    let state = stmt.execution_state();
    if !state.is_running() && !state.is_terminal() {
        *baseline = Some(stmt.metrics().rows_written);
    }
    let written_before = *baseline;
    loop {
        #[cfg(test)]
        BEFORE_STEP.with(|hook| {
            if let Some(hook) = hook.borrow_mut().as_mut() {
                hook()
            }
        });
        match guarded(res, || stmt.step())? {
            Ok(StepResult::Row) => return Ok(Step::Row),
            Ok(StepResult::Done) => {
                // A write that completed after the database left MVCC (this
                // statement, or a switch on another connection) is not in S3.
                res.check_s3_mvcc().map_err(Step::Error)?;
                return Ok(Step::Done);
            }
            Ok(StepResult::Busy) => {
                if busy_inner_commit(stmt, written_before) {
                    let _ = guarded(res, || stmt.reset());
                }
                return Ok(Step::Busy);
            }
            Ok(StepResult::Interrupt) => {
                res.consume_cancel();
                let _ = guarded(res, || stmt.reset());
                return Err(Step::Error("interrupted".to_string()));
            }
            Ok(StepResult::IO) | Ok(StepResult::Yield) => {
                if res.cancelled.load(Ordering::SeqCst) {
                    return Err(cancelled(res, stmt));
                }
                guarded(res, || stmt._io().step())?.map_err(from_err)?;
            }
            Ok(StepResult::Sleep { duration }) => {
                #[cfg(test)]
                ON_BUSY_SLEEP.with(|hook| {
                    if let Some(hook) = hook.borrow_mut().as_mut() {
                        hook()
                    }
                });
                if busy_inner_commit(stmt, written_before) {
                    let _ = guarded(res, || stmt.reset());
                    return Ok(Step::Busy);
                }
                if res.cancelled.load(Ordering::SeqCst) {
                    return Err(cancelled(res, stmt));
                }
                return Ok(Step::Sleep(duration));
            }
            Err(err) => {
                let _ = guarded(res, || stmt.reset());
                return match from_err(err) {
                    Step::Busy => Ok(Step::Busy),
                    other => Err(other),
                };
            }
        }
    }
}

impl StmtRes {
    /// Refuses to run a statement that would switch a database with
    /// AUTOINCREMENT tables to MVCC (see `mvcc_guard`).
    fn check_mvcc_switch(
        &self,
        res: &ConnRes,
        conn: &Arc<turso_core::Connection>,
    ) -> Result<(), Step> {
        if self.switches_to_mvcc {
            crate::mvcc_guard::check_switch(res, conn)
        } else {
            Ok(())
        }
    }

    /// A fresh execution of this statement starts (see `ConnRes::note_start`).
    fn note_start(&self, res: &ConnRes, conn: &turso_core::Connection, statement: &Statement) {
        res.note_start(conn, *self.writes.get_or_init(|| writes(statement)));
    }
}

/// A busy backoff in whole milliseconds for `Process.sleep/1` (at least 1).
pub fn millis(duration: Duration) -> u64 {
    (duration.as_micros().div_ceil(1000) as u64).max(1)
}

/// Rejects statements used with a connection other than their own, which
/// `Sediment.Engine` turns into an ArgumentError like exqlite does.
fn check_owner(res: &ConnRes, stmt: &StmtRes) -> NifResult<()> {
    if res.id == stmt.conn.id {
        Ok(())
    } else {
        Err(Error::RaiseAtom("cross_connection_call"))
    }
}

/// Restarts a statement that already ran to completion, matching sqlite's
/// auto-reset on the next `sqlite3_step`.
fn restart_if_finished(stmt: &mut Statement) {
    if stmt.execution_state().is_terminal() {
        let _ = stmt.reset();
    }
}

#[rustler::nif(schedule = "DirtyIo")]
fn prepare<'a>(env: Env<'a>, res: ResourceArc<ConnRes>, sql: String) -> Term<'a> {
    let guard = res.handle();
    let Some(handle) = guard.as_ref() else {
        return closed(env);
    };
    if crate::mvcc_guard::requests_mvcc(&sql) {
        if let Err(step) = crate::mvcc_guard::check_switch(&res, &handle.conn) {
            return step.into_error(env);
        }
    }
    let switches_to_mvcc = crate::mvcc_guard::requests_mvcc(&sql);
    let prepared = match guarded_msg(&res, || handle.conn.prepare(&sql)) {
        Ok(prepared) => prepared,
        Err(msg) => return error_tuple(env, msg),
    };
    match prepared {
        Ok(statement) => {
            let slot = Arc::new(Mutex::new(Some(statement)));
            // Registered while the handle is still held, so a concurrent
            // close/deserialize/refresh either sees it or ran before prepare.
            res.track(&slot);
            drop(guard);
            crate::conn::LIVE_STMTS.fetch_add(1, Ordering::Relaxed);
            let stmt = StmtRes {
                conn: res.clone(),
                stmt: slot,
                baseline: Mutex::new(None),
                writes: std::sync::OnceLock::new(),
                switches_to_mvcc,
            };
            ok_tuple(env, ResourceArc::new(stmt))
        }
        Err(err) => error_tuple(env, error::message(&err)),
    }
}

#[rustler::nif(schedule = "DirtyIo")]
fn step<'a>(
    env: Env<'a>,
    res: ResourceArc<ConnRes>,
    stmt: ResourceArc<StmtRes>,
) -> NifResult<Term<'a>> {
    check_owner(&res, &stmt)?;
    let guard = res.handle();
    if guard.is_none() {
        return Ok(closed(env));
    }
    let mut stmt_guard = lock(&stmt.stmt);
    let Some(statement) = stmt_guard.as_mut() else {
        return Ok(error_tuple(env, atoms::invalid_statement()));
    };
    restart_if_finished(statement);
    if let Err(step) = stmt.check_mvcc_switch(&res, &guard.as_ref().expect("checked").conn) {
        return Ok(step.into_error(env));
    }
    if !statement.execution_state().is_running() {
        stmt.note_start(&res, &guard.as_ref().expect("checked").conn, statement);
    }
    let mut baseline = lock(&stmt.baseline);
    Ok(match advance_with(&res, statement, &mut baseline) {
        Ok(Step::Row) => {
            let row = statement.row().expect("row after StepResult::Row");
            (atoms::row(), value::encode_row(env, row)).encode(env)
        }
        Ok(Step::Done) => atoms::done().encode(env),
        Ok(Step::Busy) => atoms::busy().encode(env),
        Ok(Step::Sleep(duration)) => (atoms::sleep(), millis(duration)).encode(env),
        Ok(Step::Error(msg)) | Err(Step::Error(msg)) => error_tuple(env, msg),
        Err(other) => other.into_error(env),
    })
}

#[rustler::nif(schedule = "DirtyIo")]
fn multi_step<'a>(
    env: Env<'a>,
    res: ResourceArc<ConnRes>,
    stmt: ResourceArc<StmtRes>,
    chunk_size: i64,
) -> NifResult<Term<'a>> {
    check_owner(&res, &stmt)?;
    let guard = res.handle();
    if guard.is_none() {
        return Ok(closed(env));
    }
    let mut stmt_guard = lock(&stmt.stmt);
    let Some(statement) = stmt_guard.as_mut() else {
        return Ok(error_tuple(env, atoms::invalid_statement()));
    };
    restart_if_finished(statement);
    if let Err(step) = stmt.check_mvcc_switch(&res, &guard.as_ref().expect("checked").conn) {
        return Ok(step.into_error(env));
    }
    if !statement.execution_state().is_running() {
        stmt.note_start(&res, &guard.as_ref().expect("checked").conn, statement);
    }
    let mut baseline = lock(&stmt.baseline);

    let mut rows: Vec<Term<'a>> = Vec::new();
    let chunk_size = chunk_size.max(1) as usize;
    while rows.len() < chunk_size {
        match advance_with(&res, statement, &mut baseline) {
            Ok(Step::Row) => {
                let row = statement.row().expect("row after StepResult::Row");
                rows.push(value::encode_row(env, row));
            }
            Ok(Step::Done) => return Ok((atoms::done(), rows).encode(env)),
            Ok(Step::Busy) => return Ok(atoms::busy().encode(env)),
            Ok(Step::Sleep(duration)) => {
                return Ok((atoms::sleep(), millis(duration), rows).encode(env));
            }
            Ok(Step::Error(msg)) | Err(Step::Error(msg)) => return Ok(error_tuple(env, msg)),
            Err(other) => return Ok(other.into_error(env)),
        }
    }
    Ok((atoms::rows(), rows).encode(env))
}

/// Binds `params`, runs the statement to completion and returns
/// `{:ok, columns, rows, changes, transaction_status}`: what
/// `Sediment.Connection` needs for a query, in one dirty scheduler call
/// instead of six. `{:error, :parameter_count}` when the number of values
/// doesn't match the statement's, which the caller handles (constant-folded
/// parameters).
#[rustler::nif(schedule = "DirtyIo")]
fn run_prepared<'a>(
    env: Env<'a>,
    res: ResourceArc<ConnRes>,
    stmt: ResourceArc<StmtRes>,
    params: Vec<Term<'a>>,
) -> NifResult<Term<'a>> {
    check_owner(&res, &stmt)?;
    let guard = res.handle();
    let Some(handle) = guard.as_ref() else {
        return Ok(closed(env));
    };
    let mut stmt_guard = lock(&stmt.stmt);
    let Some(statement) = stmt_guard.as_mut() else {
        return Ok(error_tuple(env, atoms::invalid_statement()));
    };

    let bound = guarded_msg(&res, || -> Result<bool, String> {
        let state = statement.execution_state();
        if state.is_running() || state.is_terminal() {
            let _ = statement.reset();
        }
        if params.len() != statement.parameters_count() {
            return Ok(false);
        }
        statement.clear_bindings();
        for (i, term) in params.iter().enumerate() {
            bind_value(statement, i + 1, value::decode(*term)?)?;
        }
        Ok(true)
    });
    match bound {
        Ok(Ok(true)) => {}
        Ok(Ok(false)) => return Ok(error_tuple(env, atoms::parameter_count())),
        Ok(Err(msg)) | Err(msg) => return Ok(error_tuple(env, msg)),
    }

    *lock(&stmt.baseline) = None;
    if let Err(step) = stmt.check_mvcc_switch(&res, &handle.conn) {
        return Ok(step.into_error(env));
    }
    stmt.note_start(&res, &handle.conn, statement);
    Ok(collect_prepared(env, &res, &stmt, handle, statement))
}

/// Continues a `run_prepared` that returned `{:sleep, ms, rows}` after the
/// caller waited: no binding, and a `cancel/1` issued during the wait
/// still applies. Returns what `run_prepared` does, with the rows since.
#[rustler::nif(schedule = "DirtyIo")]
fn resume_prepared<'a>(
    env: Env<'a>,
    res: ResourceArc<ConnRes>,
    stmt: ResourceArc<StmtRes>,
) -> NifResult<Term<'a>> {
    check_owner(&res, &stmt)?;
    let guard = res.handle();
    let Some(handle) = guard.as_ref() else {
        return Ok(closed(env));
    };
    let mut stmt_guard = lock(&stmt.stmt);
    let Some(statement) = stmt_guard.as_mut() else {
        return Ok(error_tuple(env, atoms::invalid_statement()));
    };
    if let Err(step) = stmt.check_mvcc_switch(&res, &handle.conn) {
        return Ok(step.into_error(env));
    }
    Ok(collect_prepared(env, &res, &stmt, handle, statement))
}

/// Steps a bound statement to completion for `run_prepared`, or until it
/// has to wait out a busy backoff: `{:sleep, ms, rows_so_far}`.
fn collect_prepared<'a>(
    env: Env<'a>,
    res: &ConnRes,
    stmt: &StmtRes,
    handle: &crate::conn::Handle,
    statement: &mut Statement,
) -> Term<'a> {
    let mut baseline = lock(&stmt.baseline);
    let mut rows: Vec<Term<'a>> = Vec::new();
    loop {
        match advance_with(res, statement, &mut baseline) {
            Ok(Step::Row) => {
                let row = statement.row().expect("row after StepResult::Row");
                rows.push(value::encode_row(env, row));
            }
            Ok(Step::Done) => break,
            Ok(Step::Busy) => return error_tuple(env, "Database busy"),
            Ok(Step::Sleep(duration)) => {
                return (atoms::sleep(), millis(duration), rows).encode(env);
            }
            Ok(Step::Error(msg)) | Err(Step::Error(msg)) => return error_tuple(env, msg),
            Err(other) => return other.into_error(env),
        }
    }
    // After stepping: a statement re-prepared for a schema change has its
    // new columns only now.
    let columns: Vec<String> = (0..statement.num_columns())
        .map(|i| statement.get_column_name(i).into_owned())
        .collect();
    let status = if handle.conn.get_auto_commit() {
        atoms::idle()
    } else {
        atoms::transaction()
    };
    (atoms::ok(), columns, rows, handle.conn.changes(), status).encode(env)
}

#[rustler::nif(schedule = "DirtyIo")]
fn columns<'a>(
    env: Env<'a>,
    res: ResourceArc<ConnRes>,
    stmt: ResourceArc<StmtRes>,
) -> NifResult<Term<'a>> {
    check_owner(&res, &stmt)?;
    let stmt_guard = lock(&stmt.stmt);
    let Some(statement) = stmt_guard.as_ref() else {
        return Ok(error_tuple(env, atoms::invalid_statement()));
    };
    let names = guarded_msg(&stmt.conn, || {
        (0..statement.num_columns())
            .map(|i| statement.get_column_name(i).into_owned())
            .collect::<Vec<String>>()
    });
    Ok(match names {
        Ok(names) => ok_tuple(env, names),
        Err(msg) => error_tuple(env, msg),
    })
}

#[rustler::nif(schedule = "DirtyIo")]
fn reset(env: Env<'_>, stmt: ResourceArc<StmtRes>) -> Term<'_> {
    let _guard = stmt.conn.handle();
    match lock(&stmt.stmt).as_mut() {
        Some(statement) => match guarded_msg(&stmt.conn, || statement.reset()) {
            Ok(Ok(())) => atoms::ok().encode(env),
            Ok(Err(err)) => error_tuple(env, error::message(&err)),
            Err(msg) => error_tuple(env, msg),
        },
        None => error_tuple(env, atoms::invalid_statement()),
    }
}

#[rustler::nif(schedule = "DirtyIo")]
fn release(res: ResourceArc<ConnRes>, stmt: ResourceArc<StmtRes>) -> NifResult<Atom> {
    check_owner(&res, &stmt)?;
    let _guard = stmt.conn.handle();
    lock(&stmt.stmt).take();
    Ok(atoms::ok())
}

#[rustler::nif(schedule = "DirtyIo")]
fn bind_parameter_count(env: Env<'_>, stmt: ResourceArc<StmtRes>) -> Term<'_> {
    match lock(&stmt.stmt).as_ref() {
        Some(statement) => statement.parameters_count().encode(env),
        None => error_tuple(env, atoms::invalid_statement()),
    }
}

/// Number of parameters written in the SQL text, counted like SQLite does:
/// `?` takes the next index, `?NNN` its own, and each distinct name one.
///
/// turso_core 0.8.1 drops trailing parameters that constant folding
/// eliminated (`WHERE 0 AND x = ?`) from `parameters_count`, so this is the
/// count callers actually have to supply.
fn declared_parameter_count(sql: &str) -> usize {
    use turso_parser::lexer::Lexer;
    use turso_parser::token::TokenType;

    let mut max_index = 0usize;
    let mut names = std::collections::HashSet::new();
    // Up to the first lexer error: it repeats forever without advancing.
    for token in Lexer::new(sql.as_bytes()).map_while(Result::ok) {
        if token.token_type != TokenType::TK_VARIABLE {
            continue;
        }
        let text = String::from_utf8_lossy(token.value);
        match text.strip_prefix('?') {
            Some("") => max_index += 1,
            Some(digits) => max_index = max_index.max(digits.parse().unwrap_or(0)),
            None => {
                if names.insert(text.into_owned()) {
                    max_index += 1;
                }
            }
        }
    }
    max_index
}

#[rustler::nif(schedule = "DirtyIo")]
fn sql_parameter_count(env: Env<'_>, stmt: ResourceArc<StmtRes>) -> Term<'_> {
    match lock(&stmt.stmt).as_ref() {
        Some(statement) => declared_parameter_count(statement.get_sql()).encode(env),
        None => error_tuple(env, atoms::invalid_statement()),
    }
}

#[rustler::nif(schedule = "DirtyIo")]
fn bind_parameter_index(stmt: ResourceArc<StmtRes>, name: String) -> usize {
    lock(&stmt.stmt)
        .as_ref()
        .and_then(|statement| statement.parameter_index(&name))
        .map_or(0, NonZero::get)
}

fn bind_value(statement: &mut Statement, index: usize, value: Value) -> Result<(), String> {
    let index = NonZero::new(index)
        .filter(|i| i.get() <= statement.parameters_count())
        .ok_or("column index out of range")?;
    statement
        .bind_at(index, value)
        .map_err(|e| error::message(&e))
}

fn with_stmt<'a>(
    env: Env<'a>,
    stmt: &StmtRes,
    f: impl FnOnce(&mut Statement) -> Result<(), String>,
) -> Term<'a> {
    let _guard = stmt.conn.handle();
    let mut stmt_guard = lock(&stmt.stmt);
    let Some(statement) = stmt_guard.as_mut() else {
        return error_tuple(env, atoms::invalid_statement());
    };
    let bound = guarded_msg(&stmt.conn, || {
        let state = statement.execution_state();
        if state.is_running() || state.is_terminal() {
            let _ = statement.reset();
        }
        f(statement)
    });
    match bound {
        Ok(Ok(())) => atoms::ok().encode(env),
        Ok(Err(msg)) | Err(msg) => error_tuple(env, msg),
    }
}

/// Binds a single value already normalized by `Sediment.Engine`.
#[rustler::nif(schedule = "DirtyIo")]
fn bind_value_at<'a>(
    env: Env<'a>,
    stmt: ResourceArc<StmtRes>,
    index: usize,
    value: Term<'a>,
) -> Term<'a> {
    with_stmt(env, &stmt, |statement| {
        bind_value(statement, index, value::decode(value)?)
    })
}

/// Resets the statement, clears its bindings and binds all `values`
/// positionally in one call.
#[rustler::nif(schedule = "DirtyIo")]
fn bind_all<'a>(env: Env<'a>, stmt: ResourceArc<StmtRes>, values: Vec<Term<'a>>) -> Term<'a> {
    // Checked here so Sediment.Engine.bind/2 needs no separate call for
    // the count; it handles a mismatch (constant-folded parameters). No
    // values only clears the bindings (named parameters are bound after).
    let count_matches = values.is_empty()
        || lock(&stmt.stmt)
            .as_ref()
            .is_none_or(|statement| statement.parameters_count() == values.len());
    if !count_matches {
        return error_tuple(env, atoms::parameter_count());
    }
    with_stmt(env, &stmt, |statement| {
        statement.clear_bindings();
        for (i, term) in values.into_iter().enumerate() {
            bind_value(statement, i + 1, value::decode(term)?)?;
        }
        Ok(())
    })
}

#[cfg(test)]
mod tests {
    use super::declared_parameter_count;
    use super::with_turso_stack;
    use super::{advance_blocking as advance, Step, BEFORE_STEP, ON_BUSY_SLEEP};
    use crate::conn::ConnRes;
    use crate::s3::tests::TempDir;
    use std::sync::Arc;
    use std::time::Duration;
    use turso_core::{Connection, Database, OpenOptions, PlatformIO, SqliteDialect, Value};

    /// An MVCC database where `b` holds the write lock (an IMMEDIATE
    /// transaction) until `a` waits out a busy backoff for the first time.
    fn locked_by_b(dir: &TempDir, schema: &[&str]) -> (Arc<Database>, Arc<Connection>) {
        let io = Arc::new(PlatformIO::new().unwrap());
        let path = dir.db("j72.db");
        let db = Database::open(
            io,
            path.to_str().unwrap(),
            OpenOptions::new(Arc::new(SqliteDialect)),
        )
        .unwrap();
        let a = db.connect().unwrap();
        a.execute("PRAGMA journal_mode = 'mvcc'").unwrap();
        a.execute("CREATE TABLE s(id INTEGER PRIMARY KEY AUTOINCREMENT, v INTEGER)")
            .unwrap();
        a.execute("CREATE TABLE p(v TEXT)").unwrap();
        for sql in schema {
            a.execute(sql).unwrap();
        }
        a.set_busy_timeout(Duration::from_secs(5));
        let b = db.connect().unwrap();
        b.execute("BEGIN IMMEDIATE").unwrap();
        b.execute("INSERT INTO p VALUES ('b')").unwrap();
        let mut b = Some(b);
        ON_BUSY_SLEEP.with(|hook| {
            *hook.borrow_mut() = Some(Box::new(move || {
                if let Some(b) = b.take() {
                    b.execute("COMMIT").unwrap();
                }
            }))
        });
        (db, a)
    }

    fn rows(conn: &Arc<Connection>, sql: &str) -> Vec<Vec<Value>> {
        conn.prepare(sql).unwrap().run_collect_rows().unwrap()
    }

    const ENDLESS: &str =
        "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c) SELECT count(*) FROM c";

    /// Runs `ENDLESS` on `res`'s connection through the driver's step loop on
    /// a thread of its own (the hooks are per thread), with `hook` run right
    /// before each call into turso; the result, unless it ran for a minute.
    fn endless_with(
        res: Arc<ConnRes>,
        conn: Arc<Connection>,
        hook: impl FnMut() + Send + 'static,
    ) -> Option<Result<Step, Step>> {
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            BEFORE_STEP.with(|before| *before.borrow_mut() = Some(Box::new(hook)));
            let mut stmt = conn.prepare(ENDLESS).unwrap();
            let _ = tx.send(advance(&res, &mut stmt));
        });
        rx.recv_timeout(Duration::from_secs(60)).ok()
    }

    fn memory_conn() -> Arc<Connection> {
        let db = Database::open(
            Arc::new(turso_core::MemoryIO::new()),
            ":memory:",
            OpenOptions::new(Arc::new(SqliteDialect)),
        )
        .unwrap();
        db.connect().unwrap()
    }

    #[test]
    fn a_cancel_landing_just_before_turso_starts_the_statement_stops_it() {
        // turso's interrupt only reaches a statement that is executing; this
        // cancel lands after the driver's last look at the flag, before that.
        let conn = memory_conn();
        let res = Arc::new(ConnRes::detached_with(conn.clone()));
        let canceller = res.clone();
        let mut once = true;
        let step = endless_with(res.clone(), conn, move || {
            if std::mem::take(&mut once) {
                ConnRes::request_cancel(canceller.clone());
            }
        });
        let step = step.expect("the cancel stopped the statement");
        assert!(matches!(step, Err(Step::Error(ref msg)) if msg == "interrupted"));
        assert!(!res.cancelled.load(std::sync::atomic::Ordering::SeqCst));
    }

    #[test]
    fn a_cancel_before_the_call_applies_and_is_used_up() {
        let conn = memory_conn();
        let res = Arc::new(ConnRes::detached_with(conn.clone()));
        ConnRes::request_cancel(res.clone());
        let step = endless_with(res.clone(), conn.clone(), || {}).expect("stopped");
        assert!(matches!(step, Err(Step::Error(ref msg)) if msg == "interrupted"));
        let mut stmt = conn.prepare("SELECT 1").unwrap();
        assert!(matches!(advance(&res, &mut stmt), Ok(Step::Row)));
    }

    #[test]
    fn a_busy_autoincrement_insert_never_commits_part_of_its_transaction() {
        let dir = TempDir::new();
        let (_db, a) = locked_by_b(&dir, &[]);
        a.execute("BEGIN CONCURRENT").unwrap();
        a.execute("INSERT INTO p VALUES ('a')").unwrap();
        let res = ConnRes::detached();
        let mut stmt = a
            .prepare("INSERT INTO s(v) VALUES (1) RETURNING id")
            .unwrap();
        assert!(matches!(advance(&res, &mut stmt), Ok(Step::Busy)));
        drop(stmt);
        a.execute("ROLLBACK").unwrap();
        assert_eq!(
            rows(&a, "SELECT v FROM p"),
            vec![vec![Value::from_text("b")]]
        );
        assert!(rows(&a, "SELECT id FROM s").is_empty());
    }

    #[test]
    fn a_busy_autoincrement_insert_in_a_trigger_never_commits_part_of_its_transaction() {
        let dir = TempDir::new();
        let (_db, a) = locked_by_b(
            &dir,
            &[
                "CREATE TABLE q(v INTEGER)",
                "CREATE TRIGGER tq AFTER INSERT ON q BEGIN INSERT INTO s(v) VALUES (new.v); END",
            ],
        );
        a.execute("BEGIN CONCURRENT").unwrap();
        a.execute("INSERT INTO p VALUES ('a')").unwrap();
        let res = ConnRes::detached();
        let mut stmt = a.prepare("INSERT INTO q VALUES (1)").unwrap();
        let step = advance(&res, &mut stmt);
        drop(stmt);
        let rollback = a.execute("ROLLBACK");
        assert_eq!(
            rows(&a, "SELECT v FROM p"),
            vec![vec![Value::from_text("b")]]
        );
        assert!(rows(&a, "SELECT v FROM q").is_empty());
        assert!(
            matches!(step, Ok(Step::Busy)),
            "{:?}",
            step.err().map(|_| "error")
        );
        rollback.unwrap();
    }

    /// Runs `sql` through the driver's step loop.
    fn run(res: &ConnRes, conn: &Arc<Connection>, sql: &str) -> Result<Vec<Value>, String> {
        let mut stmt = conn.prepare(sql).map_err(|e| e.to_string())?;
        let mut first = Vec::new();
        loop {
            match advance(res, &mut stmt) {
                Ok(Step::Row) if first.is_empty() => {
                    first = stmt.row().unwrap().get_values().cloned().collect();
                }
                Ok(Step::Row) => {}
                Ok(Step::Done) => return Ok(first),
                Ok(Step::Busy) => return Err("busy".into()),
                Ok(Step::Sleep(_)) => unreachable!("advance_blocking sleeps"),
                Ok(Step::Error(msg)) | Err(Step::Error(msg)) => return Err(msg),
                Err(_) => return Err("busy".into()),
            }
        }
    }

    /// Transactions writing one row to each of two tables (one of them
    /// AUTOINCREMENT) under contention from exclusive writers and
    /// checkpoints: each is all or nothing, and exactly the committed ones
    /// are visible. `MVCC_SOAK_SECS` runs it longer.
    #[test]
    fn mvcc_transactions_stay_atomic_under_contention() {
        use std::collections::BTreeSet;
        use std::sync::atomic::{AtomicBool, Ordering};
        let secs: u64 = std::env::var("MVCC_SOAK_SECS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(3);
        let dir = TempDir::new();
        let io = Arc::new(PlatformIO::new().unwrap());
        let path = dir.db("soak.db");
        let db = Database::open(
            io,
            path.to_str().unwrap(),
            OpenOptions::new(Arc::new(SqliteDialect)),
        )
        .unwrap();
        let setup = db.connect().unwrap();
        setup.execute("PRAGMA journal_mode = 'mvcc'").unwrap();
        setup.execute("CREATE TABLE p(tag INTEGER)").unwrap();
        setup
            .execute("CREATE TABLE s(id INTEGER PRIMARY KEY AUTOINCREMENT, tag INTEGER)")
            .unwrap();
        setup.execute("CREATE TABLE d(v INTEGER)").unwrap();
        let stop = Arc::new(AtomicBool::new(false));
        let disruptor = {
            let (conn, stop) = (db.connect().unwrap(), stop.clone());
            conn.set_busy_timeout(Duration::from_millis(200));
            std::thread::spawn(move || {
                let mut i = 0u64;
                while !stop.load(Ordering::Relaxed) {
                    i += 1;
                    if conn.execute("BEGIN IMMEDIATE").is_ok() {
                        let _ = conn.execute("INSERT INTO d VALUES (1)");
                        std::thread::sleep(Duration::from_millis(2));
                        if conn.execute("COMMIT").is_err() {
                            let _ = conn.execute("ROLLBACK");
                        }
                    }
                    if i.is_multiple_of(5) {
                        let _ = conn.execute("PRAGMA wal_checkpoint(TRUNCATE)");
                    }
                    std::thread::sleep(Duration::from_millis(1));
                }
            })
        };
        let writers: Vec<_> = (0..4u64)
            .map(|w| {
                let (conn, stop) = (db.connect().unwrap(), stop.clone());
                conn.set_busy_timeout(Duration::from_millis(200));
                std::thread::spawn(move || {
                    let res = ConnRes::detached();
                    let mut rng = 0x9e37_79b9_7f4a_7c15u64 ^ (w + 1);
                    let (mut committed, mut failures) = (BTreeSet::new(), Vec::new());
                    let mut i = 0;
                    while !stop.load(Ordering::Relaxed) {
                        i += 1;
                        rng ^= rng << 13;
                        rng ^= rng >> 7;
                        rng ^= rng << 17;
                        let tag = (w * 1_000_000 + i) as i64;
                        let body = run(&res, &conn, "BEGIN CONCURRENT")
                            .and_then(|_| {
                                run(&res, &conn, &format!("INSERT INTO p VALUES ({tag})"))
                            })
                            .and_then(|_| {
                                run(
                                    &res,
                                    &conn,
                                    &format!("INSERT INTO s(tag) VALUES ({tag}) RETURNING id"),
                                )
                            });
                        let outcome = match body {
                            Ok(_) if !rng.is_multiple_of(4) => {
                                run(&res, &conn, "COMMIT").map(|_| true)
                            }
                            Ok(_) => Err("chose to roll back".into()),
                            Err(e) => Err(e),
                        };
                        match outcome {
                            Ok(_) => {
                                committed.insert(tag);
                            }
                            Err(e) => {
                                if e.contains("internal turso error")
                                    || e.contains("No such transaction")
                                {
                                    failures.push(e);
                                }
                                let _ = run(&res, &conn, "ROLLBACK");
                            }
                        }
                    }
                    (committed, failures)
                })
            })
            .collect();
        std::thread::sleep(Duration::from_secs(secs));
        stop.store(true, Ordering::Relaxed);
        disruptor.join().unwrap();
        let (mut committed, mut failures) = (BTreeSet::new(), Vec::new());
        for writer in writers {
            let (c, f) = writer.join().unwrap();
            committed.extend(c);
            failures.extend(f);
        }
        let tags = |sql: &str| -> BTreeSet<i64> {
            rows(&setup, sql)
                .into_iter()
                .map(|row| match &row[0] {
                    Value::Numeric(turso_core::Numeric::Integer(v)) => *v,
                    other => panic!("{other:?}"),
                })
                .collect()
        };
        let (in_p, in_s) = (tags("SELECT tag FROM p"), tags("SELECT tag FROM s"));
        assert!(failures.is_empty(), "{failures:?}");
        assert!(committed.len() > 10, "only {} commits", committed.len());
        assert_eq!(in_p, in_s, "a transaction was split");
        assert_eq!(
            in_p, committed,
            "visible rows differ from the committed transactions"
        );
    }

    #[test]
    fn a_busy_autoincrement_insert_waits_in_a_deferred_transaction() {
        let dir = TempDir::new();
        let (_db, a) = locked_by_b(&dir, &[]);
        a.execute("BEGIN").unwrap();
        a.execute("SELECT 1").unwrap();
        let res = ConnRes::detached();
        let mut stmt = a
            .prepare("INSERT INTO s(v) VALUES (1) RETURNING id")
            .unwrap();
        assert!(matches!(advance(&res, &mut stmt), Ok(Step::Row)));
        assert!(matches!(advance(&res, &mut stmt), Ok(Step::Done)));
        drop(stmt);
        a.execute("COMMIT").unwrap();
        assert_eq!(rows(&a, "SELECT id FROM s"), vec![vec![Value::from_i64(1)]]);
        assert_eq!(
            rows(&a, "SELECT v FROM p"),
            vec![vec![Value::from_text("b")]]
        );
    }

    #[test]
    fn a_busy_autoincrement_insert_waits_outside_a_transaction() {
        let dir = TempDir::new();
        let (_db, a) = locked_by_b(&dir, &[]);
        let res = ConnRes::detached();
        let mut stmt = a
            .prepare("INSERT INTO s(v) VALUES (1) RETURNING id")
            .unwrap();
        assert!(matches!(advance(&res, &mut stmt), Ok(Step::Row)));
        assert_eq!(stmt.row().unwrap().get_value(0), &Value::from_i64(1));
        assert!(matches!(advance(&res, &mut stmt), Ok(Step::Done)));
        assert_eq!(
            rows(&a, "SELECT id, v FROM s"),
            vec![vec![Value::from_i64(1), Value::from_i64(1)]]
        );
        assert_eq!(
            rows(&a, "SELECT v FROM p"),
            vec![vec![Value::from_text("b")]]
        );
    }

    #[test]
    fn counts_parameters_like_sqlite() {
        assert_eq!(declared_parameter_count("select 1"), 0);
        assert_eq!(declared_parameter_count("select ?, ?"), 2);
        assert_eq!(declared_parameter_count("select ?3, ?"), 4);
        assert_eq!(declared_parameter_count("select :a, :b, :a"), 2);
        assert_eq!(declared_parameter_count("select '?', \"?\" -- ?\n, ?"), 1);
    }

    /// Recurses `depth` times with a 4 KB frame each.
    fn deep(depth: usize) -> usize {
        let frame = std::hint::black_box([0u8; 4096]);
        if depth == 0 {
            frame[0] as usize
        } else {
            deep(depth - 1) + frame[1] as usize + 1
        }
    }

    #[test]
    fn compile_stack_runs_deep_recursion_on_a_small_thread() {
        // A thread with a dirty scheduler's default stack size.
        let handle = std::thread::Builder::new()
            .stack_size(320 * 1024)
            .spawn(|| {
                (
                    with_turso_stack(|| deep(2000)),
                    with_turso_stack(|| deep(10)),
                )
            })
            .unwrap();
        assert_eq!(handle.join().unwrap(), (2000, 10));
    }

    #[test]
    fn compile_stack_resumes_panics_and_stays_usable() {
        let caught = std::panic::catch_unwind(|| {
            with_turso_stack(|| -> usize { panic!("inside the compile stack") })
        });
        let payload = caught.unwrap_err();
        assert_eq!(
            payload.downcast_ref::<&str>(),
            Some(&"inside the compile stack")
        );
        // Reusable after a panic, and nested calls run in place.
        assert_eq!(with_turso_stack(|| with_turso_stack(|| deep(100))), 100);
    }
}
