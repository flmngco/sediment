//! `DurableStorage` that mirrors every committed logical-log frame to S3,
//! before the commit is acknowledged (`durability: sync`) or in log order in
//! the background (`durability: async`, see [`Uploads`]).

use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, Weak};
use std::time::{Duration, Instant};

use bytes::Bytes;
use object_store::UpdateVersion;
use turso_core::io::{FileSyncType, SharedBufferData};
use turso_core::mvcc::database::{LogRecord, RowVersion};
use turso_core::mvcc::persistent_storage::logical_log::{
    LogHeader, LogTxFrameInfo, OnSerializationComplete,
};
use turso_core::mvcc::persistent_storage::{DurableStorage, LogicalLogTruncateOutcome, Storage};
use turso_core::storage::sqlite3_ondisk::DatabaseHeader;
use turso_core::EncryptionContext;
use turso_core::{CheckpointResult, Completion, File, LimboError};

use super::config::S3Config;
use super::error::{Result, S3Error};
use super::layout::{parse_segment_key, parse_snapshot_key, seal_body, Epoch, Manifest};
use super::layout::{LOG_DIR, MANIFEST_KEY, SNAPSHOT_DIR};
use super::lease::Lease;
use super::remote::{Put, Remote};
use super::snapshot::{self, Digests};

/// A frame serialized by `log_tx`, waiting for `on_log_write_complete`.
#[derive(Debug, Clone)]
struct Frame {
    offset: u64,
    bytes: Bytes,
}

#[derive(Debug)]
struct WriterState {
    epoch: Epoch,
    manifest: Manifest,
    manifest_version: UpdateVersion,
    /// A truncating checkpoint started `epoch`, but the manifest still points
    /// at the previous one.
    snapshot_pending: bool,
    /// Frames whose PUT failed (or that were uploaded and then discarded),
    /// with their epoch: any of them may be in S3 although turso rolled them
    /// back. Cleared once a frame of ours holds the offset.
    orphans: Vec<(Epoch, Frame)>,
    /// Epoch to seal, and its end offset, before the manifest may move on.
    seal: Option<(Epoch, u64)>,
    /// Manifests sent without an answer; any of them may land later.
    unconfirmed_manifests: Vec<Manifest>,
    /// Why this writer stopped. Shared with the storage, whose commit path
    /// reads it without this state's lock (held during uploads).
    poisoned: PoisonSlot,
    /// A copy of the DB file as the checkpoint that started `epoch` left it:
    /// what its snapshot is uploaded from (every retry too), since a later
    /// checkpoint rewrites the DB file itself. Removed once published.
    snapshot_image: Option<PathBuf>,
    /// Digests of the file a snapshot object holds, keyed by that object:
    /// the next snapshot can be a delta while the manifest points at it.
    snapshot_digests: Option<(String, Digests)>,
    uploaded_frames: u64,
    uploaded_objects: u64,
    uploaded_bytes: u64,
}

/// Point-in-time view of the writer, for `Sediment.S3.info/1`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct S3Info {
    pub epoch: String,
    pub snapshot: String,
    pub generation: u64,
    pub owner: String,
    pub lease_expires_at_ms: u64,
    pub log_offset: u64,
    pub uploaded_frames: u64,
    /// Log objects PUT (a group-commit batch is one object).
    pub uploaded_objects: u64,
    pub uploaded_bytes: u64,
    pub snapshot_pending: bool,
    pub poisoned: Option<String>,
    /// `durability: async`.
    pub asynchronous: bool,
    /// Committed but not yet in S3 (async).
    pub pending_bytes: u64,
    pub pending_frames: u64,
    /// Age of the oldest pending commit.
    pub lag_ms: u64,
    /// The log is in S3 up to here.
    pub durable_epoch: String,
    pub durable_offset: u64,
    /// Commits an earlier writer of this file lost (see [`Loss`]).
    pub lost: Option<Loss>,
}

/// Set in `S3DurableStorage::attached` once every connection has started
/// closing: from then on no open attaches to it.
const CLOSING: usize = 1 << (usize::BITS - 1);

/// One connection counted in `S3DurableStorage::attached`, from the open's
/// prepare until dropped (when the connection starts closing).
pub struct Attached(Arc<S3DurableStorage>);

impl Attached {
    /// Counts one more connection, unless every connection has started
    /// closing: then the storage is on its way out and `None`.
    pub fn try_new(storage: &Arc<S3DurableStorage>) -> Option<Self> {
        // `fetch_update` is deprecated as `try_update` in newer Rust, which
        // the 1.91 minimum doesn't have.
        #[allow(deprecated)]
        storage
            .attached
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |state| {
                (state & CLOSING == 0).then_some(state + 1)
            })
            .ok()
            .map(|_| Self(storage.clone()))
    }

    pub fn storage(&self) -> &Arc<S3DurableStorage> {
        &self.0
    }

    /// Gives the count back without closing the storage, for callers that
    /// attach no connection to it (tests open turso on it directly).
    pub fn into_storage(self) -> Arc<S3DurableStorage> {
        let this = std::mem::ManuallyDrop::new(self);
        // SAFETY: `this` is never dropped, so the Arc is moved out once.
        let storage = unsafe { std::ptr::read(&this.0) };
        storage.attached.fetch_sub(1, Ordering::SeqCst);
        storage
    }
}

impl Drop for Attached {
    fn drop(&mut self) {
        // The last one closes it, unless an open attached in between (then
        // the count isn't 0 anymore and the exchange fails).
        if self.0.attached.fetch_sub(1, Ordering::SeqCst) == 1 {
            let _ =
                self.0
                    .attached
                    .compare_exchange(0, CLOSING, Ordering::SeqCst, Ordering::SeqCst);
        }
    }
}

pub struct S3DurableStorage {
    inner: Storage,
    remote: Remote,
    lease: Arc<Lease>,
    db_path: PathBuf,
    location: (String, String),
    settings: String,
    place: String,
    /// Connections that have it open (or are opening it) and haven't
    /// started closing, plus `CLOSING` once all of them have: never handed
    /// to a new open from then on.
    attached: std::sync::atomic::AtomicUsize,
    /// Set when Drop has finished (lease released, sidecar written): until
    /// then a new open of the file waits (see `super::wait_while_closing`).
    gone: Arc<AtomicBool>,
    /// The key it was opened with: another open must give the same.
    encryption: Option<turso_core::EncryptionOpts>,
    retain_epochs: usize,
    group_commit: bool,
    incremental_snapshots: bool,
    /// `durability: async`: committed frames waiting for the uploader.
    uploads: Option<Arc<(Mutex<Uploads>, Condvar)>>,
    max_lag: Duration,
    max_pending_bytes: u64,
    upload_interval: Duration,
    close_timeout: Duration,
    pending: Mutex<Option<Frame>>,
    poison_slot: PoisonSlot,
    /// Last view of the writer state (see [`Self::view`]).
    view: Mutex<StateView>,
    /// Work for the background thread (see [`background_loop`]).
    background: Arc<(Mutex<Background>, Condvar)>,
    /// With group commit: frames written since the last upload, contiguous.
    batch: Mutex<Vec<Frame>>,
    state: Mutex<WriterState>,
}

impl std::fmt::Debug for S3DurableStorage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3DurableStorage")
            .field("db_path", &self.db_path)
            .finish_non_exhaustive()
    }
}

impl S3DurableStorage {
    pub(crate) fn new(
        inner: Storage,
        remote: Remote,
        lease: Arc<Lease>,
        cfg: &S3Config,
        db_path: PathBuf,
        (manifest, manifest_version): (Manifest, UpdateVersion),
        snapshot_digests: Option<(String, Digests)>,
    ) -> Self {
        let poison_slot = PoisonSlot::default();
        let view = StateView {
            epoch: manifest.epoch,
            snapshot: manifest.snapshot.clone(),
            uploaded_frames: 0,
            uploaded_objects: 0,
            uploaded_bytes: 0,
            snapshot_pending: false,
        };
        Self {
            poison_slot: poison_slot.clone(),
            view: Mutex::new(view),
            background: Arc::default(),
            inner,
            remote,
            lease,
            db_path,
            location: (cfg.bucket.clone(), cfg.prefix.clone()),
            settings: cfg.settings(),
            place: cfg.place(),
            attached: std::sync::atomic::AtomicUsize::new(0),
            gone: Arc::default(),
            encryption: cfg.encryption.clone(),
            retain_epochs: cfg.retain_epochs,
            group_commit: cfg.group_commit,
            incremental_snapshots: cfg.incremental_snapshots,
            uploads: cfg
                .async_durability
                .then(|| Arc::new((Mutex::new(Uploads::default()), Condvar::new()))),
            max_lag: cfg.max_lag,
            max_pending_bytes: cfg.max_pending_bytes,
            upload_interval: cfg.upload_interval,
            close_timeout: cfg.close_timeout,
            pending: Mutex::new(None),
            batch: Mutex::new(Vec::new()),
            state: Mutex::new(WriterState {
                epoch: manifest.epoch,
                manifest,
                manifest_version,
                snapshot_pending: false,
                orphans: Vec::new(),
                seal: None,
                unconfirmed_manifests: Vec::new(),
                poisoned: poison_slot.clone(),
                snapshot_digests,
                snapshot_image: None,
                uploaded_frames: 0,
                uploaded_objects: 0,
                uploaded_bytes: 0,
            }),
        }
    }

    /// Best-effort cleanup of what the current manifest no longer references
    /// (at open: no checkpoint lock is held).
    pub(crate) fn collect_garbage_now(&self) {
        let manifest = self.state.lock().unwrap().manifest.clone();
        if let Err(err) = self.collect_garbage(&manifest) {
            tracing::warn!("s3 garbage collection failed: {err}");
        }
    }

    /// Hands work to the background thread, off turso's checkpoint lock and
    /// the writer state's lock.
    fn background_work(&self, f: impl FnOnce(&mut Background)) {
        let (lock, cvar) = &*self.background;
        f(&mut lock.lock().unwrap());
        cvar.notify_all();
    }

    /// The background thread's pass: publish a pending snapshot (one
    /// attempt; after a failure the next commit publishes it first), then
    /// collect garbage.
    fn background_pass(&self, publish: bool, gc: Option<Manifest>) {
        if publish {
            let mut state = self.state.lock().unwrap();
            if state.snapshot_pending && state.poisoned.get().is_none() {
                if let Err(err) = self.publish_snapshot(&mut state) {
                    tracing::warn!("s3 snapshot upload failed, will retry: {err}");
                }
            }
        }
        if let Some(manifest) = gc {
            if let Err(err) = self.collect_garbage(&manifest) {
                tracing::warn!("s3 garbage collection failed: {err}");
            }
        }
    }

    /// Waits until the background thread has done what it was handed, or
    /// `timeout` passed. Returns whether it is done.
    pub(crate) fn wait_background(&self, timeout: Duration) -> bool {
        let deadline = Instant::now().checked_add(timeout);
        let (lock, cvar) = &*self.background;
        let mut work = lock.lock().unwrap();
        while work.publish || work.gc.is_some() || work.busy {
            if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
                return false;
            }
            work = cvar.wait_timeout(work, WAIT_POLL).unwrap().0;
        }
        true
    }

    /// `(bucket, prefix)` this storage writes to.
    /// Every connection that had it open has started closing.
    pub fn is_closing(&self) -> bool {
        self.attached.load(Ordering::SeqCst) & CLOSING != 0
    }

    /// Set once this storage's Drop has finished.
    pub fn gone(&self) -> Arc<AtomicBool> {
        self.gone.clone()
    }

    /// Connections that have the database open and haven't started closing.
    pub fn attached(&self) -> usize {
        self.attached.load(Ordering::SeqCst) & !CLOSING
    }

    /// See `S3Config::place`.
    pub fn place(&self) -> &str {
        &self.place
    }

    pub fn settings(&self) -> &str {
        &self.settings
    }

    /// The encryption it was opened with.
    pub fn encryption(&self) -> Option<&turso_core::EncryptionOpts> {
        self.encryption.as_ref()
    }

    pub fn location(&self) -> (&str, &str) {
        (&self.location.0, &self.location.1)
    }

    /// The writer state's fields `info` shows. The uploader holds the state's
    /// lock during uploads: then this is the view recorded last (at most one
    /// upload old) instead of a wait for the upload.
    fn view(&self) -> StateView {
        match self.state.try_lock() {
            Ok(state) => self.record_view(&state),
            Err(_) => self.view.lock().unwrap().clone(),
        }
    }

    fn record_view(&self, state: &WriterState) -> StateView {
        let view = StateView {
            epoch: state.epoch,
            snapshot: state.manifest.snapshot.clone(),
            uploaded_frames: state.uploaded_frames,
            uploaded_objects: state.uploaded_objects,
            uploaded_bytes: state.uploaded_bytes,
            snapshot_pending: state.snapshot_pending,
        };
        *self.view.lock().unwrap() = view.clone();
        view
    }

    pub fn info(&self) -> S3Info {
        let state = self.view();
        let mut info = S3Info {
            epoch: state.epoch.to_string(),
            snapshot: state.snapshot.clone(),
            generation: self.lease.generation(),
            owner: self.lease.owner().to_string(),
            lease_expires_at_ms: self.lease.expires_at_ms(),
            log_offset: self.inner.logical_log_offset(),
            uploaded_frames: state.uploaded_frames,
            uploaded_objects: state.uploaded_objects,
            uploaded_bytes: state.uploaded_bytes,
            snapshot_pending: state.snapshot_pending,
            poisoned: self.poison_slot.get(),
            asynchronous: self.uploads.is_some(),
            pending_bytes: 0,
            pending_frames: 0,
            lag_ms: 0,
            durable_epoch: state.epoch.to_string(),
            durable_offset: self.inner.logical_log_offset(),
            lost: recorded_loss(&self.db_path),
        };
        drop(state);
        if let Some(uploads) = &self.uploads {
            let up = uploads.0.lock().unwrap();
            info.pending_bytes = up.bytes;
            info.pending_frames = up.queue.len() as u64;
            info.lag_ms = up
                .queue
                .front()
                .map_or(0, |q| q.at.elapsed().as_millis() as u64);
            if let Some((epoch, offset)) = up.durable_at {
                info.durable_epoch = epoch.to_string();
                info.durable_offset = offset;
            } else if !up.queue.is_empty() {
                info.durable_offset = up.queue.front().map_or(0, |q| q.frame.offset);
            }
        }
        info
    }

    /// Uploads a pending snapshot now instead of at the next commit. Returns
    /// whether anything was uploaded.
    pub fn flush_snapshot(&self) -> Result<bool> {
        self.wait_background(Duration::MAX);
        let mut state = self.state.lock().unwrap();
        check_poisoned(&state)?;
        if !state.snapshot_pending {
            return Ok(false);
        }
        self.publish_snapshot(&mut state)?;
        drop(state);
        self.wait_background(Duration::MAX);
        Ok(true)
    }

    /// Test hook for a crash: the uploader stops (a batch in flight may
    /// still land) and nothing drains.
    #[cfg(test)]
    pub(crate) fn stop_uploads_for_test(&self) {
        if let Some(uploads) = &self.uploads {
            uploads.0.lock().unwrap().stop = true;
            uploads.1.notify_all();
        }
    }

    /// Test hook for a crash: none of the writer's threads works on. A
    /// background pass in flight completes (the process died right after).
    #[cfg(test)]
    pub(crate) fn stop_threads_for_test(&self) {
        self.stop_uploads_for_test();
        self.lease.stop_renewer_for_test();
        let (lock, cvar) = &*self.background;
        let mut work = lock.lock().unwrap();
        work.stopped = true;
        cvar.notify_all();
        while work.busy {
            work = cvar.wait_timeout(work, WAIT_POLL).unwrap().0;
        }
    }

    #[cfg(test)]
    pub(crate) fn renew_lease_for_test(&self) {
        self.lease.renew().unwrap();
    }

    /// Releases the writer lease. Further commits fail.
    pub fn release(&self) {
        self.lease.release();
    }

    /// Pending uploads (`durability: async`): frames, bytes and the age of
    /// the oldest, without waiting for an upload in flight.
    pub fn pending(&self) -> (u64, u64, Duration) {
        match &self.uploads {
            None => (0, 0, Duration::ZERO),
            Some(uploads) => {
                let up = uploads.0.lock().unwrap();
                let age = up.queue.front().map_or(Duration::ZERO, |q| q.at.elapsed());
                (up.queue.len() as u64, up.bytes, age)
            }
        }
    }

    /// Copies the DB file as a checkpoint just left it (see
    /// `WriterState::snapshot_image`).
    fn checkpoint_image(&self) -> turso_core::Result<PathBuf> {
        let image = snapshot_image_path(&self.db_path);
        std::fs::copy(&self.db_path, &image).map_err(|err| {
            let _ = std::fs::remove_file(&image);
            LimboError::InternalError(format!(
                "s3: copying the checkpoint of {} for its snapshot: {err}",
                self.db_path.display()
            ))
        })?;
        Ok(image)
    }

    /// Starts the background threads: the maintenance one, and the uploader
    /// with `durability: async`.
    pub(crate) fn start_uploader(self: &Arc<Self>) {
        let (storage, background) = (Arc::downgrade(self), self.background.clone());
        std::thread::Builder::new()
            .name("sediment-s3-background".into())
            .spawn(move || background_loop(storage, background))
            .expect("spawn the S3 background thread");
        if let Some(uploads) = &self.uploads {
            let (storage, uploads) = (Arc::downgrade(self), uploads.clone());
            let (interval, max_pending) = (self.upload_interval, self.max_pending_bytes);
            std::thread::Builder::new()
                .name("sediment-s3-uploader".into())
                .spawn(move || upload_loop(storage, uploads, interval, max_pending))
                .expect("spawn the S3 uploader");
        }
    }

    /// How long closing waits for pending uploads (`close_timeout_ms`).
    pub fn close_timeout(&self) -> Duration {
        self.close_timeout
    }

    /// Closing a connection: uploads what is pending, waiting at most
    /// `timeout`. Returns whether everything committed is in S3. A poisoned
    /// writer's pending commits are lost: recorded for later flushes.
    pub fn close(&self, timeout: Duration) -> bool {
        // The background thread holds the storage while it works; let it
        // finish, so the storage (and its lease) go when the last
        // connection does. Within `timeout`: an S3 outage can stretch a pass
        // to minutes, and then the storage goes when the pass ends.
        let deadline = Instant::now() + timeout;
        self.wait_background(timeout);
        match self.flush(deadline.saturating_duration_since(Instant::now())) {
            Ok(_) => true,
            Err(err) => {
                tracing::error!("s3: closing before everything was uploaded: {err}");
                if self.poisoned().is_some() {
                    self.record_loss();
                }
                false
            }
        }
    }

    /// Records that commits this writer acknowledged will never reach S3, if
    /// any are pending.
    fn record_loss(&self) {
        let Some(uploads) = &self.uploads else {
            return;
        };
        let up = uploads.0.lock().unwrap();
        let Some(last) = up.queue.back() else {
            return;
        };
        let epoch = self.view().epoch;
        let durable = match up.durable_at {
            Some((epoch, offset)) => (epoch.to_string(), offset),
            None => (
                epoch.to_string(),
                up.queue.front().map_or(0, |q| q.frame.offset),
            ),
        };
        let committed = (
            epoch.to_string(),
            last.frame.offset + last.frame.bytes.len() as u64,
        );
        let frames = up.queue.len() as u64;
        drop(up);
        let loss = Loss {
            durable,
            committed,
            frames,
        };
        tracing::error!("s3: {}", loss.message());
        losses().insert(loss_key(&self.db_path), loss);
    }

    /// The commits an earlier writer of this file lost, if not acknowledged.
    pub fn lost(&self) -> Option<Loss> {
        recorded_loss(&self.db_path)
    }

    /// The application knows about the lost commits: flushes work again.
    pub fn acknowledge_loss(&self) -> Option<Loss> {
        losses().remove(&loss_key(&self.db_path))
    }

    /// Waits until everything committed so far is in S3 (all connections).
    /// Returns where the log is durable. Immediate with `durability: sync`.
    /// Fails while an earlier writer of this file lost commits it had
    /// acknowledged (see [`Loss`]).
    pub fn flush(&self, timeout: Duration) -> Result<(String, u64)> {
        if let Some(loss) = recorded_loss(&self.db_path) {
            return Err(S3Error::Lost(loss.message()));
        }
        match &self.uploads {
            None => {
                self.poison_slot.check()?;
                Ok(self.durable_now())
            }
            Some(uploads) => {
                let target = uploads.0.lock().unwrap().enqueued;
                self.wait_durable(uploads, target, Instant::now() + timeout)
            }
        }
    }

    /// Like [`Self::flush`], waiting only through frame `seq` (from
    /// [`take_last_enqueued`]), not for commits queued after it.
    pub fn flush_through(&self, seq: u64, timeout: Duration) -> Result<(String, u64)> {
        if let Some(loss) = recorded_loss(&self.db_path) {
            return Err(S3Error::Lost(loss.message()));
        }
        match &self.uploads {
            None => {
                self.poison_slot.check()?;
                Ok(self.durable_now())
            }
            Some(uploads) => self.wait_durable(uploads, seq, Instant::now() + timeout),
        }
    }

    fn durable_now(&self) -> (String, u64) {
        let epoch = self.view().epoch;
        (epoch.to_string(), self.inner.logical_log_offset())
    }

    fn durable_label(up: &Uploads, fallback: &(String, u64)) -> String {
        match up.durable_at {
            Some((epoch, offset)) => format!("{epoch}/{offset}"),
            None => format!(
                "{}/{}",
                fallback.0,
                up.queue.front().map_or(fallback.1, |q| q.frame.offset)
            ),
        }
    }

    /// Waits until frame `target` (a sequence number) is in S3.
    fn wait_durable(
        &self,
        uploads: &(Mutex<Uploads>, Condvar),
        target: u64,
        deadline: Instant,
    ) -> Result<(String, u64)> {
        let (lock, cvar) = uploads;
        let started = Instant::now();
        let mut up = lock.lock().unwrap();
        if up.urgent < target {
            up.urgent = target;
            cvar.notify_all();
        }
        loop {
            if up.durable >= target {
                return Ok(match up.durable_at {
                    Some((epoch, offset)) => (epoch.to_string(), offset),
                    None => {
                        drop(up);
                        self.durable_now()
                    }
                });
            }
            // The state's lock is held during uploads: don't wait for it here.
            let fallback = match self.state.try_lock() {
                Ok(state) => (state.epoch.to_string(), 0),
                Err(_) => ("(current epoch)".to_string(), 0),
            };
            if let Some(reason) = self.poisoned() {
                return Err(S3Error::Fenced(format!(
                    "{reason} (durable up to {})",
                    Self::durable_label(&up, &fallback)
                )));
            }
            if super::remote::cancelled() {
                return Err(S3Error::Timeout(format!(
                    "s3 flush cancelled; durable up to {}",
                    Self::durable_label(&up, &fallback)
                )));
            }
            let now = Instant::now();
            if now >= deadline {
                return Err(S3Error::Timeout(format!(
                    "s3 flush timed out after {} ms; durable up to {}",
                    (now - started).as_millis(),
                    Self::durable_label(&up, &fallback)
                )));
            }
            up = cvar
                .wait_timeout(up, (deadline - now).min(WAIT_POLL))
                .unwrap()
                .0;
        }
    }

    /// Waits while too much is pending (or for too long): a store that
    /// stopped answering makes commits wait, not the loss window grow. Runs
    /// before the commit's frame is written locally, so a commit that gives
    /// up here (cancel/1, a poisoned writer) leaves nothing behind: once in
    /// the local log, a frame must reach the queue.
    fn wait_for_room(&self, uploads: &(Mutex<Uploads>, Condvar)) -> Result<()> {
        let (lock, cvar) = uploads;
        let mut up = lock.lock().unwrap();
        loop {
            self.poison_slot.check()?;
            let behind = up.bytes >= self.max_pending_bytes
                || up
                    .queue
                    .front()
                    .is_some_and(|q| q.at.elapsed() >= self.max_lag);
            if !behind {
                return Ok(());
            }
            if super::remote::cancelled() {
                return Err(S3Error::Timeout(
                    "s3 commit cancelled while waiting for pending uploads".into(),
                ));
            }
            up = cvar.wait_timeout(up, WAIT_POLL).unwrap().0;
        }
    }

    /// Queues a frame written to the local log for the uploader.
    fn enqueue(&self, uploads: &(Mutex<Uploads>, Condvar), frame: Frame) {
        let (lock, cvar) = uploads;
        let mut up = lock.lock().unwrap();
        up.enqueued += 1;
        let seq = up.enqueued;
        LAST_ENQUEUED.with(|last| last.set(Some(seq)));
        up.bytes += frame.bytes.len() as u64;
        up.queue.push_back(Queued {
            seq,
            frame,
            at: Instant::now(),
        });
        cvar.notify_all();
    }

    /// Waits until the queue is empty: before the epoch changes (frames are
    /// uploaded under the current epoch) and when the writer goes away.
    fn drain(&self, timeout: Duration) -> Result<()> {
        if let Some(uploads) = &self.uploads {
            let target = uploads.0.lock().unwrap().enqueued;
            self.wait_durable(uploads, target, Instant::now() + timeout)?;
        }
        Ok(())
    }

    /// Uploads a batch of queued frames as one segment (the uploader). The
    /// identical batch is retried after a failure, so an attempt that landed
    /// without an answer reads as ours.
    fn upload_batch(&self, batch: &[Queued]) -> Result<()> {
        let first = batch.first().expect("batch is not empty");
        let mut bytes = Vec::with_capacity(batch.iter().map(|q| q.frame.bytes.len()).sum());
        for queued in batch {
            bytes.extend_from_slice(&queued.frame.bytes);
        }
        let segment = Frame {
            offset: first.frame.offset,
            bytes: Bytes::from(bytes),
        };
        self.upload_frame(&segment)?;
        let mut state = self.state.lock().unwrap();
        state.uploaded_frames += batch.len() as u64 - 1;
        Ok(())
    }

    fn capture(&self, bytes: SharedBufferData, info: LogTxFrameInfo) -> turso_core::Result<()> {
        *self.pending.lock().unwrap() = Some(Frame {
            offset: info.logical_start_offset,
            bytes: Bytes::copy_from_slice(bytes.as_slice()),
        });
        Ok(())
    }

    fn upload_frame(&self, frame: &Frame) -> Result<()> {
        let mut state = self.state.lock().unwrap();
        check_poisoned(&state)?;
        self.lease.ensure().map_err(|err| poison(&mut state, err))?;
        if state.snapshot_pending {
            self.publish_snapshot(&mut state)?;
        }
        let key = state.epoch.segment_key(frame.offset);
        let result = match self.remote.put(&key, frame.bytes.clone(), Put::Create) {
            Ok(_) => Ok(()),
            Err(S3Error::Conflict(_)) => self.resolve_conflict(&mut state, &key, frame),
            Err(err) => self.resolve_failed_put(&key, frame, err),
        };
        let result = result.and_then(|()| self.confirm_ownership(&mut state));
        match result {
            Ok(()) => {
                // A frame of ours now holds this offset, so none of the
                // earlier attempts at it can land anymore.
                state.orphans.clear();
                state.uploaded_frames += 1;
                state.uploaded_objects += 1;
                state.uploaded_bytes += frame.bytes.len() as u64;
                self.record_view(&state);
                Ok(())
            }
            Err(err @ S3Error::Fenced(_)) => Err(poison(&mut state, err)),
            Err(err) => {
                let epoch = state.epoch;
                remember(&mut state, epoch, frame.clone());
                Err(err)
            }
        }
    }

    /// A PUT failed without an answer. It may still have landed: look.
    fn resolve_failed_put(&self, key: &str, frame: &Frame, err: S3Error) -> Result<()> {
        match self.remote.get(key) {
            Ok(Some(existing)) if existing.bytes == frame.bytes => Ok(()),
            _ => Err(err),
        }
    }

    /// `key` already exists. That's fine only if it holds this very frame (a
    /// retried PUT that had landed). Landed objects are never rewritten: a
    /// new owner may already have restored them.
    fn resolve_conflict(&self, state: &mut WriterState, key: &str, frame: &Frame) -> Result<()> {
        let existing = self
            .remote
            .get(key)?
            .ok_or_else(|| S3Error::Fenced(format!("{key} changed under us")))?;
        if existing.bytes == frame.bytes {
            return Ok(());
        }
        Err(foreign_object(
            state,
            state.epoch,
            frame.offset,
            key,
            &existing.bytes,
        ))
    }

    /// The commit may be acknowledged only while the manifest is still the
    /// one this writer last wrote. A new owner rewrites the manifest before
    /// it reads the log, so a frame that lands after a takeover (even at a
    /// key GC had freed) is never acknowledged.
    fn confirm_ownership(&self, state: &mut WriterState) -> Result<()> {
        match self.remote.head(MANIFEST_KEY)? {
            Some((version, _)) if version == state.manifest_version => Ok(()),
            _ => self.adopt_manifest_if_ours(state),
        }
    }

    /// The manifest isn't at the version we hold. If it holds a body we sent
    /// whose answer was lost (possibly one that landed late), adopt it;
    /// otherwise another writer took over.
    fn adopt_manifest_if_ours(&self, state: &mut WriterState) -> Result<()> {
        let fenced = || S3Error::Fenced("manifest was updated by another writer".into());
        let Some(current) = self.remote.get(MANIFEST_KEY)? else {
            return Err(poison(state, fenced()));
        };
        if current.version == state.manifest_version {
            return Ok(());
        }
        let sent = state
            .unconfirmed_manifests
            .iter()
            .position(|sent| sent.encode() == current.bytes);
        match sent {
            Some(index) => {
                let manifest = state.unconfirmed_manifests.remove(index);
                self.manifest_published(state, manifest, current.version);
                Ok(())
            }
            None => Err(poison(state, fenced())),
        }
    }

    fn manifest_published(
        &self,
        state: &mut WriterState,
        manifest: Manifest,
        version: UpdateVersion,
    ) {
        // An adopted late manifest can name an older, empty epoch than the
        // one we're in; then it still has to be sealed and moved past.
        let behind = manifest.epoch != state.epoch;
        state.seal = behind.then_some((manifest.epoch, 0));
        state.snapshot_pending = behind;
        state.manifest = manifest;
        state.manifest_version = version;
        state.orphans.clear();
        if !behind {
            if let Some(image) = state.snapshot_image.take() {
                let _ = std::fs::remove_file(image);
            }
        }
        self.record_view(state);
        // Every other manifest we sent was conditional on the version this
        // one replaced, so none of them can land anymore.
        state.unconfirmed_manifests.clear();
        self.background_work(|work| work.gc = Some(state.manifest.clone()));
    }

    /// Uploads the DB file as the new epoch's snapshot, points the manifest at
    /// it, then deletes what the manifest no longer references.
    fn publish_snapshot(&self, state: &mut WriterState) -> Result<()> {
        let generation = self.lease.ensure().map_err(|err| poison(state, err))?;
        if let Some((epoch, end)) = state.seal {
            self.seal_epoch(state, epoch, end, generation)?;
        }
        // A previous attempt's manifest may have landed without an answer.
        if !state.unconfirmed_manifests.is_empty() {
            self.adopt_manifest_if_ours(state)?;
            if !state.snapshot_pending {
                return Ok(());
            }
        }
        let record = state.manifest.current();
        let previous = state
            .snapshot_digests
            .as_ref()
            .filter(|(key, _)| *key == record.snapshot)
            .map(|(_, digests)| (&record, digests));
        let source = state
            .snapshot_image
            .clone()
            .unwrap_or_else(|| self.db_path.clone());
        let published = snapshot::publish(&self.remote, state.epoch, &source, previous)?;
        if self.incremental_snapshots {
            state.snapshot_digests = Some((published.key.clone(), published.digests.clone()));
        }
        let manifest = state.manifest.advance(
            state.epoch,
            &published,
            generation,
            self.lease.owner(),
            self.retain_epochs,
        );
        state.unconfirmed_manifests.push(manifest.clone());
        match self.remote.put(
            MANIFEST_KEY,
            manifest.encode(),
            Put::Update(state.manifest_version.clone()),
        ) {
            Ok(version) => {
                self.manifest_published(state, manifest, version);
                Ok(())
            }
            Err(S3Error::Conflict(_)) => self.adopt_manifest_if_ours(state),
            Err(err) => Err(err),
        }
    }

    /// Deletes epochs and snapshots older than the manifest's that it no
    /// longer retains. A stale writer's late PUT may recreate a key there,
    /// but can't be acknowledged: its ownership confirm sees this manifest.
    fn collect_garbage(&self, manifest: &Manifest) -> Result<()> {
        let current = manifest.epoch;
        let retained: Vec<Epoch> = manifest.retained().map(|r| r.epoch).collect();
        // Never an epoch of a newer lease generation: a writer that took the
        // lease after this one wrote it (a takeover, or a new database after
        // a destroy), and a stale writer collecting it would delete another
        // database's data.
        let generation = self.lease.generation();
        let doomed = |epoch: Epoch| {
            epoch.seq < current.seq && epoch.generation <= generation && !retained.contains(&epoch)
        };
        let mut keys: Vec<String> = self
            .remote
            .list(LOG_DIR)?
            .into_iter()
            .filter(|o| parse_segment_key(&o.key).is_some_and(|(epoch, _)| doomed(epoch)))
            .map(|o| o.key)
            .collect();
        // A retained epoch's snapshot may be a delta on older snapshots.
        let referenced: std::collections::HashSet<String> = manifest
            .retained()
            .flat_map(|record| record.chain())
            .map(|link| link.key)
            .collect();
        keys.extend(
            self.remote
                .list(SNAPSHOT_DIR)?
                .into_iter()
                .filter(|o| {
                    parse_snapshot_key(&o.key).is_some_and(|epoch| {
                        epoch.seq < current.seq && epoch.generation <= generation
                    }) && !referenced.contains(&o.key)
                })
                .map(|o| o.key),
        );
        self.remote.delete_many(keys)
    }

    /// Closes `epoch` with a create-only seal at its end offset, so a restore
    /// can tell a closed epoch from a live one; an append that lands there
    /// later is refused (and would not be acknowledged anyway: its writer's
    /// ownership check sees the newer manifest).
    fn seal_epoch(
        &self,
        state: &mut WriterState,
        epoch: Epoch,
        end: u64,
        generation: u64,
    ) -> Result<()> {
        let key = epoch.segment_key(end);
        let body = seal_body(generation);
        let tag = Some(super::meter::TraceTag("seal"));
        match self.remote.put_tagged(&key, body.clone(), Put::Create, tag) {
            Ok(_) => Ok(()),
            Err(S3Error::Conflict(_)) => {
                let existing = self.remote.get(&key)?.ok_or_else(|| {
                    poison(state, S3Error::Fenced(format!("{key} changed under us")))
                })?;
                if existing.bytes == body {
                    return Ok(());
                }
                Err(foreign_object(state, epoch, end, &key, &existing.bytes))
            }
            Err(err) => Err(err),
        }
    }

    /// Refuses every further commit (reopening restores the S3 state).
    pub fn poison(&self, reason: &str) {
        self.poison_slot.set(reason.to_string());
    }

    /// Whether commits are batched (see [`Self::flush_batch`]).
    pub fn group_commit(&self) -> bool {
        self.group_commit
    }

    /// Why this writer was fenced, if it was.
    pub fn poisoned(&self) -> Option<String> {
        self.poison_slot.get()
    }

    /// Like [`Self::poisoned`], but also when the lease was lost or lapsed
    /// with no write since to notice it: this writer's view may then miss
    /// another writer's commits. Poisons the storage in that case.
    pub fn fenced(&self) -> Option<String> {
        if let Some(reason) = self.lease.lapsed() {
            self.poison(reason);
        }
        self.poisoned()
    }

    /// Uploads the frames of a group commit as one object.
    ///
    /// turso acknowledges the transactions of a group commit only after the
    /// batch's `sync`, which calls this, so none is acknowledged before its
    /// frame is in S3. By then turso has advanced the log past the frames, so
    /// a failure can't be rolled back: it fences the writer, and reopening
    /// restores whatever reached S3 (the commits are indeterminate).
    fn flush_batch(&self) -> Result<()> {
        let frames = std::mem::take(&mut *self.batch.lock().unwrap());
        let Some(first) = frames.first() else {
            return Ok(());
        };
        let mut bytes = Vec::with_capacity(frames.iter().map(|f| f.bytes.len()).sum());
        for frame in &frames {
            bytes.extend_from_slice(&frame.bytes);
        }
        let segment = Frame {
            offset: first.offset,
            bytes: Bytes::from(bytes),
        };
        self.upload_frame(&segment).map_err(|err| {
            let state = self.state.lock().unwrap();
            let reason =
                format!("a group commit could not be stored in S3 ({err}); reopen the database");
            state.poisoned.set(reason.clone());
            S3Error::Fenced(reason)
        })?;
        let mut state = self.state.lock().unwrap();
        // upload_frame counted the batch as one frame.
        state.uploaded_frames += frames.len() as u64 - 1;
        Ok(())
    }

    /// The log was truncated at `end`: frames from now on go to a new epoch,
    /// and the old one must be sealed and snapshotted.
    fn start_new_epoch(&self, end: u64, image: PathBuf) {
        let mut state = self.state.lock().unwrap();
        // A newer checkpoint supersedes a snapshot that was never published.
        if let Some(old) = state.snapshot_image.replace(image) {
            let _ = std::fs::remove_file(old);
        }
        // If an earlier roll was never published, the manifest still points
        // at the epoch recorded then; later epochs never reached S3.
        if state.seal.is_none() {
            state.seal = Some((state.epoch, end));
        }
        state.epoch = state.epoch.next(self.lease.generation());
        state.snapshot_pending = true;
        self.record_view(&state);
    }
}

/// `key` holds something this writer didn't just send. Either an earlier
/// commit that reported failure landed after all (its outcome is
/// indeterminate, and the local state no longer matches S3), or another
/// writer wrote it. Both end this writer; reopening restores the S3 state.
fn foreign_object(
    state: &mut WriterState,
    epoch: Epoch,
    offset: u64,
    key: &str,
    bytes: &Bytes,
) -> S3Error {
    let reason = if is_orphan(state, epoch, offset, bytes) {
        format!("{key}: a commit that reported failure reached S3; reopen the database")
    } else {
        format!("{key} was written by another writer")
    };
    poison(state, S3Error::Fenced(reason))
}

fn is_orphan(state: &WriterState, epoch: Epoch, offset: u64, bytes: &Bytes) -> bool {
    state.orphans.iter().any(|(orphan_epoch, orphan)| {
        *orphan_epoch == epoch && orphan.offset == offset && orphan.bytes == *bytes
    })
}

/// Every attempt at an offset may still land, not only the last one.
fn remember(state: &mut WriterState, epoch: Epoch, frame: Frame) {
    if !state
        .orphans
        .iter()
        .any(|(e, f)| *e == epoch && f.offset == frame.offset && f.bytes == frame.bytes)
    {
        state.orphans.push((epoch, frame));
    }
}

fn check_poisoned(state: &WriterState) -> Result<()> {
    state.poisoned.check()
}

/// A fresh path for a checkpoint image of `db_path`:
/// `.<file name>.s3-snapshot-<pid>-<n>` next to it.
fn snapshot_image_path(db_path: &std::path::Path) -> PathBuf {
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let n = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    db_path.with_file_name(format!(
        "{}{}-{n}",
        snapshot_image_prefix(db_path),
        std::process::id()
    ))
}

pub(crate) fn snapshot_image_prefix(db_path: &std::path::Path) -> String {
    let name = db_path
        .file_name()
        .map(|s| s.to_string_lossy().into_owned())
        .unwrap_or_default();
    format!(".{name}.s3-snapshot-")
}

/// Commits a writer acknowledged (`durability: async`) that never reached
/// S3: it was fenced, or went away, with them queued. Kept per database file
/// for this VM, so that a flush through a later writer of the file (a pool
/// that reconnected) reports them instead of succeeding, until acknowledged.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Loss {
    /// The log is in S3 up to here.
    pub durable: (String, u64),
    /// Commits were acknowledged up to here.
    pub committed: (String, u64),
    /// Log frames lost.
    pub frames: u64,
}

impl Loss {
    fn message(&self) -> String {
        format!(
            "s3 commits were lost: {} acknowledged log frames never reached S3 \
             (durable up to {}/{}, committed up to {}/{}); \
             call Sediment.S3.acknowledge_loss/1 to continue",
            self.frames, self.durable.0, self.durable.1, self.committed.0, self.committed.1
        )
    }
}

fn losses() -> std::sync::MutexGuard<'static, std::collections::HashMap<PathBuf, Loss>> {
    static LOSSES: std::sync::OnceLock<Mutex<std::collections::HashMap<PathBuf, Loss>>> =
        std::sync::OnceLock::new();
    LOSSES
        .get_or_init(Default::default)
        .lock()
        .unwrap_or_else(|e| e.into_inner())
}

/// The file's canonical directory and name: stable across restores, which
/// replace the file.
fn loss_key(db_path: &std::path::Path) -> PathBuf {
    let dir = db_path
        .parent()
        .filter(|d| !d.as_os_str().is_empty())
        .unwrap_or(std::path::Path::new("."));
    let dir = std::fs::canonicalize(dir).unwrap_or_else(|_| dir.to_path_buf());
    dir.join(db_path.file_name().unwrap_or_default())
}

fn recorded_loss(db_path: &std::path::Path) -> Option<Loss> {
    losses().get(&loss_key(db_path)).cloned()
}

thread_local! {
    static LAST_ENQUEUED: std::cell::Cell<Option<u64>> = const { std::cell::Cell::new(None) };
}

/// The sequence number of the last frame this thread queued since the last
/// call (`durability: async`), for [`S3DurableStorage::flush_through`].
///
/// With turso's group commit one connection's thread (the leader) writes
/// the records of others, in order: a waiter then finds `None` here (flush
/// everything queued instead, which includes its commit), and the leader a
/// number at or after its own commit's. Either way the commit is covered.
pub fn take_last_enqueued() -> Option<u64> {
    LAST_ENQUEUED.with(|last| last.take())
}

/// Work for the background thread: snapshot publication after a checkpoint
/// and garbage collection after a manifest update, both off turso's
/// checkpoint lock (which stops every read and write) and the writer
/// state's lock.
#[derive(Default)]
struct Background {
    publish: bool,
    gc: Option<Manifest>,
    busy: bool,
    /// No more passes: the storage was dropped, or the crash test hook.
    stopped: bool,
}

fn background_loop(storage: Weak<S3DurableStorage>, background: Arc<(Mutex<Background>, Condvar)>) {
    let (lock, cvar) = &*background;
    loop {
        let (publish, gc) = {
            let mut work = lock.lock().unwrap();
            // Woken by new work, the crash hook, or the storage's Drop (no
            // timed polling: an idle database costs no wakeups).
            while !work.publish && work.gc.is_none() || work.stopped {
                if work.stopped || storage.strong_count() == 0 {
                    return;
                }
                work = cvar.wait(work).unwrap();
            }
            work.busy = true;
            (std::mem::take(&mut work.publish), work.gc.take())
        };
        if let Some(storage) = storage.upgrade() {
            storage.background_pass(publish, gc);
        }
        lock.lock().unwrap().busy = false;
        cvar.notify_all();
        if storage.strong_count() == 0 {
            return;
        }
    }
}

/// What `info` shows of the writer state.
#[derive(Clone, Debug)]
struct StateView {
    epoch: Epoch,
    snapshot: String,
    uploaded_frames: u64,
    uploaded_objects: u64,
    uploaded_bytes: u64,
    snapshot_pending: bool,
}

/// Why a writer stopped, readable without the writer state's lock.
#[derive(Clone, Default, Debug)]
struct PoisonSlot(Arc<Mutex<Option<String>>>);

impl PoisonSlot {
    fn get(&self) -> Option<String> {
        self.0.lock().unwrap().clone()
    }

    /// Records `reason` unless the writer already stopped for another one.
    fn set(&self, reason: String) {
        self.0.lock().unwrap().get_or_insert(reason);
    }

    fn check(&self) -> Result<()> {
        match self.get() {
            Some(reason) => Err(S3Error::Fenced(reason)),
            None => Ok(()),
        }
    }
}

fn poison(state: &mut WriterState, err: S3Error) -> S3Error {
    if let S3Error::Fenced(reason) = &err {
        state.poisoned.set(reason.clone());
    }
    err
}

impl Drop for S3DurableStorage {
    fn drop(&mut self) {
        // Set when this returns (or unwinds): see `gone`.
        struct Gone(Arc<AtomicBool>);
        impl Drop for Gone {
            fn drop(&mut self) {
                self.0.store(true, Ordering::SeqCst);
            }
        }
        let _gone = Gone(self.gone.clone());
        // The uploader only holds a weak reference: upload what is left here.
        if let Some(uploads) = self.uploads.clone() {
            let deadline = Instant::now() + self.close_timeout;
            while Instant::now() < deadline && self.poisoned().is_none() {
                let batch = next_batch(&uploads.0.lock().unwrap());
                if batch.is_empty() {
                    break;
                }
                match self.upload_batch(&batch) {
                    Ok(()) => uploaded(&uploads, &batch, self.state.lock().unwrap().epoch),
                    Err(_) => std::thread::sleep(Duration::from_millis(200)),
                }
            }
            let pending = !uploads.0.lock().unwrap().queue.is_empty();
            if pending {
                self.record_loss();
            }
            uploads.0.lock().unwrap().stop = true;
            uploads.1.notify_all();
        }
        // The background thread waits without a timeout: tell it to end.
        {
            let (lock, cvar) = &*self.background;
            lock.lock().unwrap().stopped = true;
            cvar.notify_all();
        }
        if let Ok(state) = self.state.get_mut() {
            if let Some(image) = state.snapshot_image.take() {
                let _ = std::fs::remove_file(image);
            }
        }
        // An open or a destroy of this database in this VM waits for `gone`,
        // so it finds the lease free and the sidecar written. The sidecar is
        // only a hint (the next open checks it against S3 and the files).
        let lapsed = self.lease.lapsed().is_some();
        self.lease.release();
        if !lapsed {
            self.leave_sidecar();
        }
    }
}

impl S3DurableStorage {
    /// After a clean close, describes the local copy for the next open to
    /// reuse (see `super::warm`): only when everything committed is in S3
    /// and the manifest names this epoch. The caller checked that the lease
    /// never lapsed.
    fn leave_sidecar(&mut self) {
        let pending = self
            .uploads
            .as_ref()
            .is_some_and(|uploads| !uploads.0.lock().unwrap().queue.is_empty());
        if pending || self.poisoned().is_some() || recorded_loss(&self.db_path).is_some() {
            return;
        }
        let generation = self.lease.generation();
        let log_offset = self.inner.logical_log_offset();
        let encryption = self.encryption.clone();
        let log_encryption = self.remote.log_encryption();
        let Ok(state) = self.state.get_mut() else {
            return;
        };
        if state.snapshot_pending || state.epoch != state.manifest.epoch {
            return;
        }
        let closing = super::warm::Closing {
            db_path: &self.db_path,
            manifest: &state.manifest,
            generation,
            log_offset,
            encryption: encryption.as_ref(),
            log_encryption,
        };
        if let Err(err) = super::warm::write(&closing) {
            tracing::warn!("s3: no warm reopen for {}: {err}", self.db_path.display());
        }
    }
}

const WAIT_POLL: Duration = Duration::from_millis(20);
/// How long a checkpoint waits for pending uploads before the epoch changes.
const DRAIN_TIMEOUT: Duration = Duration::from_secs(10);
/// Largest segment the uploader builds from queued frames.
const MAX_SEGMENT: u64 = 8 * 1024 * 1024;
/// Failed attempts at one segment before the writer gives up (fenced),
/// with exponential backoff from `RETRY_BASE_MS` between them.
const UPLOAD_ATTEMPTS: u32 = 6;
const RETRY_BASE_MS: u64 = if cfg!(test) { 2 } else { 100 };

/// `durability: async`: frames committed locally, in log order, and how far
/// they are in S3. Sequence numbers count frames across epochs.
#[derive(Default)]
struct Uploads {
    queue: VecDeque<Queued>,
    bytes: u64,
    /// Last queued frame.
    enqueued: u64,
    /// Last frame in S3.
    durable: u64,
    durable_at: Option<(Epoch, u64)>,
    /// Frames someone waits for (a flush, a `sync: true` commit, a
    /// checkpoint, close): uploaded without waiting for `upload_interval`.
    urgent: u64,
    stop: bool,
}

struct Queued {
    seq: u64,
    frame: Frame,
    at: Instant,
}

impl Clone for Queued {
    fn clone(&self) -> Self {
        Self {
            seq: self.seq,
            frame: self.frame.clone(),
            at: self.at,
        }
    }
}

/// The frames at the head of the queue, up to `MAX_SEGMENT` bytes.
fn next_batch(up: &Uploads) -> Vec<Queued> {
    let mut size = 0u64;
    let mut batch = Vec::new();
    for queued in &up.queue {
        if !batch.is_empty() && size + queued.frame.bytes.len() as u64 > MAX_SEGMENT {
            break;
        }
        size += queued.frame.bytes.len() as u64;
        batch.push(queued.clone());
    }
    batch
}

/// `batch` is in S3: drop it from the queue and wake whoever waits.
fn uploaded(uploads: &(Mutex<Uploads>, Condvar), batch: &[Queued], epoch: Epoch) {
    let mut up = uploads.0.lock().unwrap();
    for queued in batch {
        if up.queue.front().is_some_and(|q| q.seq == queued.seq) {
            let done = up.queue.pop_front().expect("front exists");
            up.bytes -= done.frame.bytes.len() as u64;
        }
    }
    let last = batch.last().expect("batch is not empty");
    up.durable = up.durable.max(last.seq);
    up.durable_at = Some((epoch, last.frame.offset + last.frame.bytes.len() as u64));
    uploads.1.notify_all();
}

/// The background uploader: uploads queued frames in order, one segment at
/// a time, retrying a failed segment unchanged. A fence, or a segment that
/// keeps failing, poisons the writer: the frames after the durable prefix
/// are then lost, and commits fail until the database is reopened.
fn upload_loop(
    storage: Weak<S3DurableStorage>,
    uploads: Arc<(Mutex<Uploads>, Condvar)>,
    interval: Duration,
    max_pending: u64,
) {
    let (lock, cvar) = &*uploads;
    let mut batch: Vec<Queued> = Vec::new();
    let mut attempts = 0u32;
    loop {
        if batch.is_empty() {
            let mut up = lock.lock().unwrap();
            // Woken by a commit, or by stop (the storage's Drop sets it).
            while up.queue.is_empty() && !up.stop {
                if storage.strong_count() == 0 {
                    return;
                }
                up = cvar.wait(up).unwrap();
            }
            // upload_interval_ms: let more commits join the segment, unless
            // someone waits for these frames, a segment is full, or the
            // pending bytes reach max_pending_bytes.
            while !up.stop {
                let age = up.queue.front().map_or(interval, |q| q.at.elapsed());
                let ready = age >= interval
                    || up.urgent > up.durable
                    || up.bytes >= MAX_SEGMENT
                    || up.bytes >= max_pending;
                if ready {
                    break;
                }
                up = cvar.wait_timeout(up, interval - age).unwrap().0;
            }
            if up.stop {
                return;
            }
            batch = next_batch(&up);
            attempts = 0;
        }
        if lock.lock().unwrap().stop {
            return;
        }
        let Some(storage) = storage.upgrade() else {
            return;
        };
        match storage.upload_batch(&batch) {
            Ok(()) => {
                let epoch = storage.state.lock().unwrap().epoch;
                uploaded(&uploads, &batch, epoch);
                batch.clear();
            }
            Err(err) => {
                attempts += 1;
                if storage.poisoned().is_none() && attempts >= UPLOAD_ATTEMPTS {
                    storage.poison(&format!(
                        "uploading commits to S3 failed {attempts} times ({err}); \
                         reopen the database"
                    ));
                }
                if let Some(reason) = storage.poisoned() {
                    tracing::error!(
                        "s3: {reason}; {} commits ({} bytes) are not durable",
                        lock.lock().unwrap().queue.len(),
                        lock.lock().unwrap().bytes
                    );
                    cvar.notify_all();
                    return;
                }
                tracing::warn!("s3 upload failed (attempt {attempts}), retrying: {err}");
                drop(storage);
                std::thread::sleep(Duration::from_millis(RETRY_BASE_MS << attempts.min(5)));
            }
        }
    }
}

impl DurableStorage for S3DurableStorage {
    fn serialize_row_version(
        &self,
        log_record: &mut LogRecord,
        row_version: &RowVersion,
        portable_extension: Option<&[u8]>,
    ) -> turso_core::Result<()> {
        self.inner
            .serialize_row_version(log_record, row_version, portable_extension)
    }

    fn serialize_database_header(
        &self,
        log_record: &mut LogRecord,
        header: &DatabaseHeader,
    ) -> turso_core::Result<()> {
        self.inner.serialize_database_header(log_record, header)
    }

    fn log_tx(
        &self,
        m: LogRecord,
        on_serialization_complete: OnSerializationComplete<'_>,
    ) -> turso_core::Result<(Completion, u64)> {
        self.poison_slot.check()?;
        if let Some(uploads) = &self.uploads {
            self.wait_for_room(uploads)?;
        }
        let capture = |bytes: SharedBufferData, info: LogTxFrameInfo| {
            if let Some(callback) = on_serialization_complete {
                callback(bytes.clone(), info)?;
            }
            self.capture(bytes, info)
        };
        self.inner.log_tx(m, Some(&capture))
    }

    fn upgrade_header_for_log_tx(&self, m: &LogRecord) -> turso_core::Result<Option<Completion>> {
        match self.inner.upgrade_header_for_log_tx(m)? {
            None => Ok(None),
            Some(_) => Err(LimboError::InternalError(
                "s3: logical log header upgrades (portable changes) are not supported".into(),
            )),
        }
    }

    fn sync(&self, sync_type: FileSyncType) -> turso_core::Result<Completion> {
        if self.group_commit && self.uploads.is_none() {
            // After a leader's failed upload, turso lets a waiter sync the
            // written prefix and then acknowledges it: that must fail too.
            check_poisoned(&self.state.lock().unwrap())?;
            self.flush_batch()?;
        }
        self.inner.sync(sync_type)
    }

    fn on_log_write_complete(&self) -> turso_core::Result<Completion> {
        let frame = self.pending.lock().unwrap().clone().ok_or_else(|| {
            LimboError::InternalError("s3: log write completed without a captured frame".into())
        })?;
        if let Some(uploads) = &self.uploads {
            self.enqueue(uploads, frame);
        } else if self.group_commit {
            check_poisoned(&self.state.lock().unwrap())?;
            self.lease
                .ensure()
                .map_err(|err| poison(&mut self.state.lock().unwrap(), err))?;
            self.batch.lock().unwrap().push(frame);
        } else {
            self.upload_frame(&frame)?;
        }
        Ok(Completion::new_yield())
    }

    fn update_header(&self) -> turso_core::Result<Completion> {
        self.inner.update_header()
    }

    fn truncate(
        &self,
        checkpointed_through_ts: u64,
    ) -> turso_core::Result<(Completion, LogicalLogTruncateOutcome)> {
        self.drain(DRAIN_TIMEOUT)?;
        if self.group_commit {
            self.flush_batch()?;
        }
        let end = self.inner.logical_log_offset();
        // turso has backfilled and synced the DB file; the next checkpoint
        // can't touch it before this returns.
        let image = self.checkpoint_image()?;
        let (completion, outcome) = self.inner.truncate(checkpointed_through_ts)?;
        if outcome == LogicalLogTruncateOutcome::Truncated {
            self.start_new_epoch(end, image);
        } else {
            let _ = std::fs::remove_file(image);
        }
        Ok((completion, outcome))
    }

    fn reset_to_fresh_header(&self) -> turso_core::Result<Completion> {
        self.drain(DRAIN_TIMEOUT)?;
        if self.group_commit {
            self.flush_batch()?;
        }
        let end = self.inner.logical_log_offset();
        let image = self.checkpoint_image()?;
        let completion = self.inner.reset_to_fresh_header()?;
        self.start_new_epoch(end, image);
        Ok(completion)
    }

    fn get_logical_log_file(&self) -> Arc<dyn File> {
        self.inner.get_logical_log_file()
    }

    fn logical_log_offset(&self) -> u64 {
        self.inner.logical_log_offset()
    }

    fn should_checkpoint(&self) -> bool {
        self.inner.should_checkpoint()
    }

    fn set_checkpoint_threshold(&self, threshold: i64) {
        self.inner.set_checkpoint_threshold(threshold)
    }

    fn checkpoint_threshold(&self) -> i64 {
        self.inner.checkpoint_threshold()
    }

    fn advance_logical_log_offset_after_success(&self, bytes: u64) -> turso_core::Result<()> {
        self.pending.lock().unwrap().take();
        self.inner.advance_logical_log_offset_after_success(bytes)
    }

    fn discard_pending_log_write(&self) -> turso_core::Result<()> {
        let pending = self.pending.lock().unwrap().take();
        if self.uploads.is_some() {
            // Frames are queued once their write completed: this one never was.
            return self.inner.discard_pending_log_write();
        }
        if let Some(frame) = &pending {
            let mut batch = self.batch.lock().unwrap();
            if batch.last().is_some_and(|last| last.offset == frame.offset) {
                // Buffered for a group upload, never sent: just drop it.
                batch.pop();
                return self.inner.discard_pending_log_write();
            }
        }
        if let Some(frame) = pending {
            // The frame may already be in S3; remember it so that finding it
            // there later reads as "our failed commit landed", not as another
            // writer (it is never replaced).
            let mut state = self.state.lock().unwrap();
            let epoch = state.epoch;
            remember(&mut state, epoch, frame);
        }
        self.inner.discard_pending_log_write()
    }

    fn restore_logical_log_state_after_recovery(&self, offset: u64, running_crc: u32) {
        self.inner
            .restore_logical_log_state_after_recovery(offset, running_crc)
    }

    fn set_header(&self, header: LogHeader) {
        self.inner.set_header(header)
    }

    fn on_checkpoint_end(
        &self,
        result: turso_core::Result<&CheckpointResult>,
    ) -> turso_core::Result<()> {
        if result.is_err() {
            return Ok(());
        }
        // turso holds its checkpoint lock (every read and write waits) until
        // this returns: publish in the background.
        self.background_work(|work| work.publish = true);
        Ok(())
    }

    fn encryption_ctx(&self) -> Option<EncryptionContext> {
        self.inner.encryption_ctx()
    }
}
