//! Single-writer lease on `lease.json`, taken and renewed with conditional PUTs.

use std::sync::{Arc, Condvar, Mutex, Weak};
use std::time::Duration;

use object_store::UpdateVersion;

use super::error::{Result, S3Error};
use super::layout::{now_ms, LeaseRecord, LEASE_KEY};
use super::remote::{Put, Remote};

#[derive(Debug)]
struct LeaseState {
    generation: u64,
    version: UpdateVersion,
    expires_at_ms: u64,
    lost: Option<String>,
    released: bool,
}

#[derive(Debug)]
pub struct Lease {
    remote: Remote,
    owner: String,
    ttl: Duration,
    state: Mutex<LeaseState>,
    stop: (Mutex<bool>, Condvar),
    /// Mirrors of `state` readable without waiting for a renewal in flight.
    lost_flag: std::sync::atomic::AtomicBool,
    expires_mirror: std::sync::atomic::AtomicU64,
}

impl Lease {
    /// Takes the lease, or fails with `LeaseHeld` while another owner's lease
    /// is unexpired. Starts a background renewal thread.
    pub fn acquire(remote: Remote, owner: String, ttl: Duration) -> Result<Arc<Lease>> {
        let ours = owner.clone();
        Self::acquire_unless_held(remote, owner, ttl, move |current| current.owner != ours)
    }

    /// Takes the lease for a destroy: unlike an open, it refuses any
    /// unexpired lease, this owner's too (that may be a writer of this
    /// database still running), unless `force`.
    pub fn acquire_to_destroy(
        remote: Remote,
        owner: String,
        ttl: Duration,
        force: bool,
    ) -> Result<Arc<Lease>> {
        Self::acquire_unless_held(remote, owner, ttl, move |_| !force)
    }

    /// `held`: whether an unexpired lease keeps this one out.
    fn acquire_unless_held(
        remote: Remote,
        owner: String,
        ttl: Duration,
        held: impl Fn(&LeaseRecord) -> bool,
    ) -> Result<Arc<Lease>> {
        let ttl_ms = ttl.as_millis() as u64;
        // Generations only grow, even if lease.json was lost or rolled back
        // (a restored backup): never below what wrote the manifest.
        let floor = manifest_generation(&remote)?;
        let mut attempts = 0;
        let (generation, version, expires_at_ms) = loop {
            attempts += 1;
            let (generation, mode) = match remote.get(LEASE_KEY)? {
                None => (floor + 1, Put::Create),
                Some(object) => {
                    let current: LeaseRecord = serde_json::from_slice(&object.bytes)?;
                    if current.expires_at_ms > now_ms() && held(&current) {
                        return Err(S3Error::LeaseHeld {
                            owner: current.owner,
                            expires_at_ms: current.expires_at_ms,
                        });
                    }
                    (
                        current.generation.max(floor) + 1,
                        Put::Update(object.version),
                    )
                }
            };
            let record = LeaseRecord {
                owner: owner.clone(),
                generation,
                expires_at_ms: now_ms() + ttl_ms,
            };
            let body: bytes::Bytes = serde_json::to_vec(&record)?.into();
            match remote.put(LEASE_KEY, body.clone(), mode) {
                Ok(version) => break (generation, version, record.expires_at_ms),
                // The PUT may have landed with its answer lost (a retry of it
                // then gets 412): the lease holding exactly what we sent is
                // ours, not someone else's to wait for.
                Err(err) => match remote.get(LEASE_KEY) {
                    Ok(Some(object)) if object.bytes == body => {
                        break (generation, object.version, record.expires_at_ms)
                    }
                    _ if matches!(err, S3Error::Conflict(_)) && attempts < 3 => continue,
                    _ => return Err(err),
                },
            }
        };
        let lease = Arc::new(Lease {
            remote,
            owner,
            ttl,
            state: Mutex::new(LeaseState {
                generation,
                version,
                expires_at_ms,
                lost: None,
                released: false,
            }),
            stop: (Mutex::new(false), Condvar::new()),
            lost_flag: std::sync::atomic::AtomicBool::new(false),
            expires_mirror: std::sync::atomic::AtomicU64::new(expires_at_ms),
        });
        spawn_renewer(Arc::downgrade(&lease), ttl / 3);
        Ok(lease)
    }

    /// Why this writer can't count on the lease anymore (taken over, or
    /// expired without renewal), without blocking on a renewal in flight.
    pub fn lapsed(&self) -> Option<&'static str> {
        use std::sync::atomic::Ordering;
        if self.lost_flag.load(Ordering::SeqCst) {
            Some("lease was taken over by another writer")
        } else if now_ms() >= self.expires_mirror.load(Ordering::SeqCst) {
            Some("lease expired (not renewed in time)")
        } else {
            None
        }
    }

    pub fn owner(&self) -> &str {
        &self.owner
    }

    pub fn generation(&self) -> u64 {
        self.state.lock().unwrap().generation
    }

    pub fn expires_at_ms(&self) -> u64 {
        self.state.lock().unwrap().expires_at_ms
    }

    fn margin_ms(&self) -> u64 {
        (self.ttl.as_millis() as u64 / 10).max(500)
    }

    /// Returns the generation if the lease is safely held, renewing it first
    /// when less than two thirds of the TTL is left.
    pub fn ensure(&self) -> Result<u64> {
        let mut state = self.state.lock().unwrap();
        if let Some(reason) = &state.lost {
            return Err(S3Error::Fenced(reason.clone()));
        }
        if state.released {
            return Err(S3Error::Fenced("lease released".into()));
        }
        let ttl_ms = self.ttl.as_millis() as u64;
        if now_ms() + ttl_ms * 2 / 3 >= state.expires_at_ms {
            let renewed = self.renew_locked(&mut state);
            if now_ms() + self.margin_ms() >= state.expires_at_ms {
                renewed?;
                return Err(S3Error::Fenced("lease expired".into()));
            }
        }
        Ok(state.generation)
    }

    /// Extends the lease by one TTL.
    pub fn renew(&self) -> Result<()> {
        let mut state = self.state.lock().unwrap();
        if state.lost.is_some() || state.released {
            return Ok(());
        }
        self.renew_locked(&mut state)
    }

    fn renew_locked(&self, state: &mut LeaseState) -> Result<()> {
        // Once expired, another writer may have taken over and released it
        // again; renewing would resurrect a lease we can no longer vouch for.
        if now_ms() >= state.expires_at_ms {
            let reason = "lease expired before it could be renewed".to_string();
            state.lost = Some(reason.clone());
            self.lost_flag
                .store(true, std::sync::atomic::Ordering::SeqCst);
            return Err(S3Error::Fenced(reason));
        }
        let record = LeaseRecord {
            owner: self.owner.clone(),
            generation: state.generation,
            expires_at_ms: now_ms() + self.ttl.as_millis() as u64,
        };
        let body: bytes::Bytes = serde_json::to_vec(&record)?.into();
        let sent = self
            .remote
            .put(LEASE_KEY, body.clone(), Put::Update(state.version.clone()));
        let sent = match sent {
            Err(S3Error::Conflict(_)) => match self.adopt_if_ours(state.generation) {
                Some(version) => self.remote.put(LEASE_KEY, body, Put::Update(version)),
                None => sent,
            },
            other => other,
        };
        match sent {
            Ok(version) => {
                state.version = version;
                state.expires_at_ms = record.expires_at_ms;
                self.expires_mirror
                    .store(record.expires_at_ms, std::sync::atomic::Ordering::SeqCst);
                Ok(())
            }
            Err(S3Error::Conflict(_)) => {
                let reason = "lease was taken over by another writer".to_string();
                state.lost = Some(reason.clone());
                self.lost_flag
                    .store(true, std::sync::atomic::Ordering::SeqCst);
                Err(S3Error::Fenced(reason))
            }
            Err(err) => Err(err),
        }
    }

    /// An earlier renewal may have landed with its answer lost, leaving our
    /// etag stale. Any takeover bumps the generation, so a lease still
    /// carrying our owner and generation was written by us.
    fn adopt_if_ours(&self, generation: u64) -> Option<UpdateVersion> {
        let current = self.remote.get(LEASE_KEY).ok()??;
        let record: LeaseRecord = serde_json::from_slice(&current.bytes).ok()?;
        (record.owner == self.owner
            && record.generation == generation
            && record.expires_at_ms > now_ms())
        .then_some(current.version)
    }

    /// Gives the lease up so another writer can take it immediately.
    pub fn release(&self) {
        self.stop_renewer();
        let mut state = self.state.lock().unwrap();
        if state.lost.is_some() || state.released {
            return;
        }
        state.released = true;
        // Given up on purpose: not a lapse.
        self.expires_mirror
            .store(u64::MAX, std::sync::atomic::Ordering::SeqCst);
        let record = LeaseRecord {
            owner: self.owner.clone(),
            generation: state.generation,
            expires_at_ms: 0,
        };
        if let Ok(body) = serde_json::to_vec(&record) {
            let _ = self
                .remote
                .put(LEASE_KEY, body.into(), Put::Update(state.version.clone()));
        }
    }

    /// Test hook for a crash: the renewer stops, the lease stays taken.
    #[cfg(test)]
    pub(crate) fn stop_renewer_for_test(&self) {
        self.stop_renewer();
    }

    fn stop_renewer(&self) {
        let (lock, cvar) = &self.stop;
        *lock.lock().unwrap() = true;
        cvar.notify_all();
    }

    /// Waits up to `timeout`; returns true if the renewer should stop.
    fn wait_stop(&self, timeout: Duration) -> bool {
        let (lock, cvar) = &self.stop;
        let guard = lock.lock().unwrap();
        let (guard, _) = cvar
            .wait_timeout_while(guard, timeout, |stop| !*stop)
            .unwrap();
        *guard
    }
}

impl Drop for Lease {
    fn drop(&mut self) {
        self.release();
    }
}

/// The lease generation the manifest (or a destroy's tombstone) was written
/// under; 0 without one. Read leniently: only the number matters here.
fn manifest_generation(remote: &Remote) -> Result<u64> {
    Ok(remote
        .get(super::layout::MANIFEST_KEY)?
        .map_or(0, |object| super::layout::written_generation(&object.bytes)))
}

fn spawn_renewer(lease: Weak<Lease>, every: Duration) {
    let spawned = std::thread::Builder::new()
        .name("sediment-s3-lease".into())
        .spawn(move || loop {
            let Some(strong) = lease.upgrade() else {
                return;
            };
            if strong.wait_stop(every) {
                return;
            }
            if let Err(err) = strong.renew() {
                tracing::warn!("s3 lease renewal failed: {err}");
            }
        });
    if let Err(err) = spawned {
        tracing::warn!("could not start s3 lease renewer: {err}");
    }
}
