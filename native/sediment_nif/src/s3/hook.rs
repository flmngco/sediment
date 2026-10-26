//! Test hooks at fixed points of a rekey or key release
//! (`Sediment.Native.s3_test_hook/3`): hold the operation there until
//! released, fail it (as a refusal, or as an S3 request would), or crash it
//! (the lease stays taken, as after a kill).
//! Armed for one database (its store and prefix) and one use.

use std::sync::{Condvar, Mutex};

use super::error::{Result, S3Error};
use super::lease::Lease;

/// Right before the manifest PUT that publishes a rekey or a release: the
/// copy is done and the new snapshot uploaded.
pub const PUBLISH: &str = "publish";

/// Right before a release deletes the objects of epochs no longer retained
/// (`:store_fail` there fails the delete).
pub const SWEEP: &str = "sweep";

/// Right before a rekey or release asked not to keep the working copy
/// removes it (`keep_working_copy: false`), still holding the lease.
pub const DISCARD: &str = "discard";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Action {
    Pause,
    Fail,
    /// Fails as an S3 request would (a store error): a transient fault,
    /// the lease released as after any error.
    StoreFail,
    Crash,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum State {
    Armed,
    /// Paused at the point, until [`release`].
    Paused,
    /// Released, or it failed or crashed there.
    Done,
}

struct Armed {
    place: String,
    stage: String,
    action: Action,
    state: State,
}

static HOOK: Mutex<Vec<Armed>> = Mutex::new(Vec::new());

/// A test's cap on the free space the space checks see (0: none).
static FREE_SPACE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Caps the free space the space checks see at `bytes` (`None`: the real
/// free space), for every database of the VM.
pub fn limit_free_space(bytes: Option<u64>) {
    FREE_SPACE.store(bytes.unwrap_or(0), std::sync::atomic::Ordering::Relaxed);
}

/// The free space a space check uses: `real`, unless a test capped it.
pub(crate) fn free_space(real: u64) -> u64 {
    match FREE_SPACE.load(std::sync::atomic::Ordering::Relaxed) {
        0 => real,
        cap => real.min(cap),
    }
}
static CHANGED: Condvar = Condvar::new();

/// Arms the hook at `stage` for the database at `place` (`S3Config::place`).
pub fn arm(place: &str, stage: &str, action: Action) {
    let mut hooks = HOOK.lock().unwrap_or_else(|e| e.into_inner());
    hooks.retain(|armed| armed.place != place);
    hooks.push(Armed {
        place: place.to_string(),
        stage: stage.to_string(),
        action,
        state: State::Armed,
    });
}

/// Where the hook for `place` is (`None`: none armed).
pub fn state(place: &str) -> Option<State> {
    let hooks = HOOK.lock().unwrap_or_else(|e| e.into_inner());
    hooks
        .iter()
        .find(|armed| armed.place == place)
        .map(|armed| armed.state)
}

/// Lets a paused operation go on, and disarms the hook.
pub fn release(place: &str) {
    let mut hooks = HOOK.lock().unwrap_or_else(|e| e.into_inner());
    hooks.retain(|armed| armed.place != place);
    CHANGED.notify_all();
}

/// Called by the operation at `stage`.
pub(crate) fn at(stage: &str, place: &str, lease: &Lease) -> Result<()> {
    let mut hooks = HOOK.lock().unwrap_or_else(|e| e.into_inner());
    let Some(armed) = hooks
        .iter_mut()
        .find(|armed| armed.place == place && armed.stage == stage && armed.state == State::Armed)
    else {
        return Ok(());
    };
    match armed.action {
        Action::Pause => {
            armed.state = State::Paused;
            CHANGED.notify_all();
            while hooks.iter().any(|armed| armed.place == place) {
                hooks = CHANGED.wait(hooks).unwrap_or_else(|e| e.into_inner());
            }
            Ok(())
        }
        Action::Fail => {
            armed.state = State::Done;
            Err(S3Error::Config(format!("test hook: failed at {stage}")))
        }
        Action::StoreFail => {
            armed.state = State::Done;
            Err(S3Error::Store(object_store::Error::Generic {
                store: "test hook",
                source: format!("failed at {stage}").into(),
            }))
        }
        Action::Crash => {
            armed.state = State::Done;
            lease.abandon();
            Err(S3Error::Fenced(format!("test hook: crashed at {stage}")))
        }
    }
}
