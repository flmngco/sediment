//! Blocking facade over an `ObjectStore`, scoped to one database prefix.

use std::future::Future;
use std::path::Path as FsPath;
use std::sync::{mpsc, Arc, OnceLock};
use std::time::Duration;

use bytes::Bytes;
use futures::{StreamExt, TryStreamExt};
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload, UpdateVersion};
use tokio::runtime::Runtime;

use super::error::{Result, S3Error};
use super::layout::SnapshotLink;
use super::snapshot::{Digests, SEGMENT};

/// Snapshots are read, compressed and uploaded in parts of this size.
/// The smallest part of a multipart upload (and the read buffer).
const SNAPSHOT_PART: usize = 8 * 1024 * 1024;
/// Parts per upload the part size aims at: S3 allows 10,000, and the bound
/// on the compressed size below leaves some slack too.
const TARGET_PARTS: u64 = 9_000;
/// S3's largest part.
const MAX_PART: u64 = 5 * 1024 * 1024 * 1024;
/// S3's largest object; snapshots must fit (compressed, but zstd can grow
/// incompressible data a little: see [`part_size`]).
pub const MAX_OBJECT: u64 = 5 * 1024 * 1024 * 1024 * 1024;

#[cfg(test)]
thread_local! {
    /// Tests: the parts per upload to aim at instead of [`TARGET_PARTS`].
    pub static TARGET_PARTS_OVERRIDE: std::cell::Cell<Option<u64>> = const { std::cell::Cell::new(None) };
}

/// The part size for an object of at most `input` uncompressed bytes:
/// 8 MiB, or more so the object fits in [`TARGET_PARTS`] parts, rounded up to
/// a MiB (every part but the last has exactly this size, as R2 requires).
pub fn part_size(input: u64) -> usize {
    #[cfg(test)]
    let target = TARGET_PARTS_OVERRIDE
        .with(|o| o.get())
        .unwrap_or(TARGET_PARTS);
    #[cfg(not(test))]
    let target = TARGET_PARTS;
    // zstd's worst case for incompressible input, with room for headers.
    let bound = input + input / 128 + 64 * 1024;
    let mib = 1024 * 1024;
    let size = bound.div_ceil(target).div_ceil(mib) * mib;
    size.clamp(SNAPSHOT_PART as u64, MAX_PART) as usize
}
const SNAPSHOT_ZSTD_LEVEL: i32 = 3;

fn runtime() -> &'static Runtime {
    static RT: OnceLock<Runtime> = OnceLock::new();
    RT.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .thread_name("sediment-s3")
            .enable_all()
            .build()
            .expect("failed to start the S3 runtime")
    })
}

/// Runs `fut` on the S3 runtime and blocks the calling thread until it's done.
///
/// Must not be called from a runtime worker thread.
pub(crate) fn block_on<F, T>(fut: F) -> Result<T>
where
    F: Future<Output = Result<T>> + Send + 'static,
    T: Send + 'static,
{
    let (tx, rx) = mpsc::sync_channel(1);
    runtime().spawn(async move {
        let _ = tx.send(fut.await);
    });
    loop {
        match rx.recv_timeout(Duration::from_millis(20)) {
            Ok(result) => return result,
            Err(mpsc::RecvTimeoutError::Timeout) if cancelled() => {
                // The request goes on in the background and may still land;
                // callers treat this like any lost answer.
                return Err(S3Error::Store(object_store::Error::Generic {
                    store: "S3",
                    source: "cancelled while waiting for S3".into(),
                }));
            }
            Err(mpsc::RecvTimeoutError::Timeout) => continue,
            // The task only drops the sender without sending if it panicked
            // (tokio catches that); report it instead of taking the NIF down.
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                return Err(S3Error::Store(object_store::Error::Generic {
                    store: "S3",
                    source: "the S3 client failed unexpectedly (panicked)".into(),
                }))
            }
        }
    }
}

thread_local! {
    static CANCEL: std::cell::Cell<*const std::sync::atomic::AtomicBool> =
        const { std::cell::Cell::new(std::ptr::null()) };
}

/// Runs `f` with `flag` as this thread's cancellation flag: S3 waits inside
/// it (a commit's upload) give up when the flag is set, e.g. by cancel/1.
pub fn with_cancel<T>(flag: &std::sync::atomic::AtomicBool, f: impl FnOnce() -> T) -> T {
    struct Reset(*const std::sync::atomic::AtomicBool);
    impl Drop for Reset {
        fn drop(&mut self) {
            CANCEL.with(|c| c.set(self.0));
        }
    }
    let _reset = Reset(CANCEL.with(|c| c.replace(flag)));
    f()
}

pub(crate) fn cancelled() -> bool {
    CANCEL.with(|c| {
        let flag = c.get();
        // Only set for the duration of `with_cancel`, which borrows the flag.
        !flag.is_null() && unsafe { &*flag }.load(std::sync::atomic::Ordering::SeqCst)
    })
}

/// Precondition for a PUT.
#[derive(Debug, Clone)]
pub enum Put {
    Overwrite,
    /// Fails with `S3Error::Conflict` if the object exists.
    Create,
    /// Fails with `S3Error::Conflict` if the object changed.
    Update(UpdateVersion),
}

#[derive(Debug, Clone)]
pub struct Object {
    pub bytes: Bytes,
    pub version: UpdateVersion,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Listed {
    /// Key relative to the database prefix.
    pub key: String,
    pub size: u64,
    /// When the object landed (unix ms).
    pub modified_ms: u64,
}

#[derive(Debug, Clone)]
pub struct Remote {
    store: Arc<dyn ObjectStore>,
    prefix: String,
    /// `prefix` as object_store spells it in listings: percent-encoded
    /// where needed (`#` is `%23`), empty segments dropped.
    listed_prefix: String,
    /// Upper bound for any single operation, retries included, so a store
    /// that stops answering fails commits instead of hanging them.
    deadline: Option<Duration>,
    /// `(tag, nonce)` sizes when the logical log is encrypted.
    log_encryption: Option<(usize, usize)>,
}

impl Remote {
    pub fn new(store: Arc<dyn ObjectStore>, prefix: &str) -> Self {
        let prefix = prefix.trim_matches('/').to_string();
        Self {
            store,
            listed_prefix: Path::from(prefix.as_str()).as_ref().to_string(),
            prefix,
            deadline: None,
            log_encryption: None,
        }
    }

    pub fn with_log_encryption(mut self, overhead: Option<(usize, usize)>) -> Self {
        self.log_encryption = overhead;
        self
    }

    pub fn log_encryption(&self) -> Option<(usize, usize)> {
        self.log_encryption
    }

    pub fn with_deadline(mut self, deadline: Duration) -> Self {
        self.deadline = Some(deadline);
        self
    }

    /// Runs a store operation on the S3 runtime, bounded by the deadline.
    fn run<F, T>(&self, fut: F) -> Result<T>
    where
        F: Future<Output = Result<T>> + Send + 'static,
        T: Send + 'static,
    {
        match self.deadline {
            None => block_on(fut),
            Some(deadline) => block_on(async move {
                tokio::time::timeout(deadline, fut)
                    .await
                    .unwrap_or_else(|_| {
                        Err(S3Error::Store(object_store::Error::Generic {
                            store: "S3",
                            source: format!("no answer within {deadline:?}").into(),
                        }))
                    })
            }),
        }
    }

    pub fn path(&self, key: &str) -> Path {
        if self.prefix.is_empty() {
            Path::from(key)
        } else {
            Path::from(format!("{}/{}", self.prefix, key))
        }
    }

    fn relative(&self, path: &Path) -> String {
        let full = path.as_ref();
        if self.listed_prefix.is_empty() {
            full.to_string()
        } else {
            full.strip_prefix(&self.listed_prefix)
                .and_then(|rest| rest.strip_prefix('/'))
                .unwrap_or(full)
                .to_string()
        }
    }

    pub fn get(&self, key: &str) -> Result<Option<Object>> {
        let store = self.store.clone();
        let path = self.path(key);
        self.run(async move { get_object(store.as_ref(), &path).await })
    }

    /// Fetches many keys concurrently; results keep the input order.
    pub fn get_many(&self, keys: Vec<String>, concurrency: usize) -> Result<Vec<Bytes>> {
        let store = self.store.clone();
        let paths: Vec<Path> = keys.iter().map(|key| self.path(key)).collect();
        self.run(async move {
            futures::stream::iter(paths)
                .map(|path| {
                    let store = store.clone();
                    async move {
                        match get_object(store.as_ref(), &path).await? {
                            Some(object) => Ok(object.bytes),
                            None => Err(S3Error::Corrupt(format!("{path} vanished during read"))),
                        }
                    }
                })
                .buffered(concurrency)
                .try_collect()
                .await
        })
    }

    pub fn put(&self, key: &str, bytes: Bytes, mode: Put) -> Result<UpdateVersion> {
        self.put_tagged(key, bytes, mode, None)
    }

    /// Like `put`, naming the request's role in the S3 request trace (the meter's
    /// trace mode, for the TLA+ trace validation).
    pub fn put_tagged(
        &self,
        key: &str,
        bytes: Bytes,
        mode: Put,
        tag: Option<super::meter::TraceTag>,
    ) -> Result<UpdateVersion> {
        let store = self.store.clone();
        let path = self.path(key);
        self.run(async move {
            let mut extensions = object_store::Extensions::new();
            if let Some(tag) = tag {
                extensions.insert(tag);
            }
            let opts = PutOptions {
                mode: match mode {
                    Put::Overwrite => PutMode::Overwrite,
                    Put::Create => PutMode::Create,
                    Put::Update(version) => PutMode::Update(version),
                },
                extensions,
                ..PutOptions::default()
            };
            match store.put_opts(&path, PutPayload::from(bytes), opts).await {
                Ok(result) => Ok(result.into()),
                Err(object_store::Error::AlreadyExists { .. }) => {
                    Err(S3Error::Conflict(format!("{path} already exists")))
                }
                Err(object_store::Error::Precondition { .. }) => {
                    Err(S3Error::Conflict(format!("{path} was modified")))
                }
                Err(err) => Err(S3Error::Store(err)),
            }
        })
    }

    /// Version and size of `key`, if it exists.
    pub fn head(&self, key: &str) -> Result<Option<(UpdateVersion, u64)>> {
        let store = self.store.clone();
        let path = self.path(key);
        self.run(async move {
            match store.head(&path).await {
                Ok(meta) => Ok(Some((
                    UpdateVersion {
                        e_tag: meta.e_tag,
                        version: meta.version,
                    },
                    meta.size,
                ))),
                Err(object_store::Error::NotFound { .. }) => Ok(None),
                Err(err) => Err(S3Error::Store(err)),
            }
        })
    }

    /// Uploads a database file as a zstd-compressed snapshot, streaming it
    /// in parts (multipart once it exceeds one part), then checks the store
    /// holds all of it before it may be referenced.
    pub fn upload_snapshot(&self, key: &str, file: &FsPath) -> Result<Snapshot> {
        use std::io::Read;
        let mut input = std::fs::File::open(file)?;
        let mut buf = vec![0u8; SNAPSHOT_PART];
        let (mut size, mut crc) = (0u64, 0u32);
        let expected = input.metadata()?.len();
        let stored = ObjectWriter::new(self, key, expected)?.run(|writer| loop {
            let n = input.read(&mut buf)?;
            if n == 0 {
                return Ok(());
            }
            size += n as u64;
            crc = crc32c::crc32c_append(crc, &buf[..n]);
            writer.write(&buf[..n])?;
        })?;
        Ok(Snapshot {
            size,
            crc32c: crc,
            stored_size: stored,
            zstd: true,
        })
    }

    /// Uploads the segments of `file` that differ from `previous` as a
    /// delta object (see `super::snapshot`). `current` are the file's
    /// digests, taken just before.
    pub fn upload_delta(
        &self,
        key: &str,
        file: &FsPath,
        current: &Digests,
        previous: &Digests,
    ) -> Result<Snapshot> {
        use std::io::{Read, Seek, SeekFrom};
        let mut input = std::fs::File::open(file)?;
        let mut buf = vec![0u8; SEGMENT];
        let stored = ObjectWriter::new(self, key, current.size)?.run(|writer| {
            writer.write(&super::snapshot::delta_header(current.size))?;
            for index in current.changed_since(previous) {
                let start = index as u64 * SEGMENT as u64;
                let len = (current.size - start).min(SEGMENT as u64) as usize;
                input.seek(SeekFrom::Start(start))?;
                input.read_exact(&mut buf[..len])?;
                writer.write(&(index as u32).to_le_bytes())?;
                writer.write(&buf[..len])?;
            }
            writer.write(&super::snapshot::DELTA_END.to_le_bytes())
        })?;
        Ok(Snapshot {
            size: current.size,
            crc32c: current.crc32c,
            stored_size: stored,
            zstd: true,
        })
    }

    fn put_part(
        &self,
        key: &str,
        upload: &mut Option<Box<dyn object_store::MultipartUpload>>,
        part: Vec<u8>,
    ) -> Result<()> {
        if upload.is_none() {
            let store = self.store.clone();
            let path = self.path(key);
            *upload =
                Some(self.run(async move {
                    store.put_multipart(&path).await.map_err(S3Error::Store)
                })?);
        }
        let request = upload
            .as_mut()
            .expect("multipart upload started")
            .put_part(PutPayload::from(part));
        self.run(async move { request.await.map_err(S3Error::Store) })
    }

    /// Downloads a snapshot into `dest` (streamed, decompressed if needed),
    /// verifying the stored size and the database's size and checksum.
    pub fn download_snapshot(&self, key: &str, expected: Snapshot, dest: &FsPath) -> Result<()> {
        let tmp = suffixed(dest, ".s3-restore");
        let result =
            self.fetch_into(key, expected.zstd, &tmp)
                .and_then(|(size, crc32c, stored_size)| {
                    let found = Snapshot {
                        size,
                        crc32c,
                        stored_size,
                        zstd: expected.zstd,
                    };
                    if found != expected {
                        return Err(S3Error::Corrupt(format!(
                            "{key}: expected {expected:?}, found {found:?}"
                        )));
                    }
                    Ok(std::fs::rename(&tmp, dest)?)
                });
        if result.is_err() {
            let _ = std::fs::remove_file(&tmp);
        }
        result
    }

    /// Rebuilds a database file from a snapshot chain (a full snapshot, then
    /// deltas) and checks it ends up as the last link says.
    pub fn download_chain(&self, chain: &[SnapshotLink], dest: &FsPath) -> Result<()> {
        self.download_chain_from(chain, dest, None)
    }

    /// Like [`Self::download_chain`], starting from `start`: a local file
    /// holding the database as the first `n` links rebuild it.
    pub fn download_chain_from(
        &self,
        chain: &[SnapshotLink],
        dest: &FsPath,
        start: Option<(usize, &FsPath)>,
    ) -> Result<()> {
        let (base, _) = chain
            .split_first()
            .ok_or_else(|| S3Error::Corrupt("empty snapshot chain".into()))?;
        let work = suffixed(dest, ".s3-chain");
        let staging = suffixed(dest, ".s3-delta");
        let result = (|| {
            let applied = match start {
                Some((n, file)) if (1..=chain.len()).contains(&n) => {
                    std::fs::copy(file, &work)?;
                    n
                }
                _ => {
                    self.download_snapshot(
                        &base.key,
                        Snapshot {
                            size: base.size,
                            crc32c: base.crc32c,
                            stored_size: base.stored_size,
                            zstd: true,
                        },
                        &work,
                    )?;
                    1
                }
            };
            for link in &chain[applied..] {
                let (_, _, stored) = self.fetch_into(&link.key, true, &staging)?;
                if stored != link.stored_size {
                    return Err(S3Error::Corrupt(format!(
                        "{}: expected {} stored bytes, found {stored}",
                        link.key, link.stored_size
                    )));
                }
                super::snapshot::apply_delta(&link.key, &staging, &work)?;
            }
            let last = chain.last().expect("chain is not empty");
            let (size, crc32c) = super::snapshot::size_and_crc(&work)?;
            if (size, crc32c) != (last.size, last.crc32c) {
                return Err(S3Error::Corrupt(format!(
                    "snapshot chain ending in {}: expected {} bytes with CRC32C {:08x}, \
                     rebuilt {size} bytes with {crc32c:08x}",
                    last.key, last.size, last.crc32c
                )));
            }
            Ok(std::fs::rename(&work, dest)?)
        })();
        let _ = std::fs::remove_file(&staging);
        if result.is_err() {
            let _ = std::fs::remove_file(&work);
        }
        result
    }

    /// Streams an object into `out`, decoding it if compressed. Returns the
    /// decoded size and CRC32C, and the stored size.
    fn fetch_into(&self, key: &str, zstd: bool, out: &FsPath) -> Result<(u64, u32, u64)> {
        use std::io::Write;
        let store = self.store.clone();
        let path = self.path(key);
        let stream = self.run(async move {
            match store.get(&path).await {
                Ok(result) => Ok(Some(result.into_stream())),
                Err(object_store::Error::NotFound { .. }) => Ok(None),
                Err(err) => Err(S3Error::Store(err)),
            }
        })?;
        let mut stream = stream.ok_or_else(|| S3Error::Corrupt(format!("{key} is missing")))?;
        let file = CountingWriter::new(std::fs::File::create(out)?);
        // A write error the file didn't cause comes from the zstd decoder.
        let failed = |err: std::io::Error| {
            if file.file_failed() {
                S3Error::from(err)
            } else {
                S3Error::Corrupt(format!("{key}: {err}"))
            }
        };
        let mut sink: Box<dyn Write> = if zstd {
            Box::new(zstd::stream::write::Decoder::new(file.clone())?)
        } else {
            Box::new(file.clone())
        };
        let mut stored = 0u64;
        loop {
            let next = self.run({
                let mut stream = std::mem::replace(&mut stream, Box::pin(futures::stream::empty()));
                async move {
                    let item = stream.next().await;
                    Ok((stream, item))
                }
            })?;
            stream = next.0;
            match next.1 {
                None => break,
                Some(Ok(chunk)) => {
                    stored += chunk.len() as u64;
                    sink.write_all(&chunk).map_err(failed)?;
                }
                Some(Err(err)) => return Err(S3Error::Store(err)),
            }
        }
        sink.flush().map_err(failed)?;
        drop(sink);
        let (size, crc) = file.finish()?;
        Ok((size, crc, stored))
    }

    /// Lists all objects under `dir` (relative keys, unordered).
    pub fn list(&self, dir: &str) -> Result<Vec<Listed>> {
        let store = self.store.clone();
        let prefix = self.path(dir);
        let metas: Vec<object_store::ObjectMeta> = self.run(async move {
            store
                .list(Some(&prefix))
                .try_collect()
                .await
                .map_err(S3Error::Store)
        })?;
        Ok(metas
            .into_iter()
            .map(|meta| Listed {
                key: self.relative(&meta.location),
                size: meta.size,
                modified_ms: meta.last_modified.timestamp_millis().max(0) as u64,
            })
            .collect())
    }

    /// Like [`Self::list`], only the keys after `after` (in key order; S3's
    /// `start-after`), so a long listing isn't paged through again.
    pub fn list_after(&self, dir: &str, after: &str) -> Result<Vec<Listed>> {
        let store = self.store.clone();
        let prefix = self.path(dir);
        let offset = self.path(after);
        let metas: Vec<object_store::ObjectMeta> = self.run(async move {
            store
                .list_with_offset(Some(&prefix), &offset)
                .try_collect()
                .await
                .map_err(S3Error::Store)
        })?;
        Ok(metas
            .into_iter()
            .map(|meta| Listed {
                key: self.relative(&meta.location),
                size: meta.size,
                modified_ms: meta.last_modified.timestamp_millis().max(0) as u64,
            })
            .collect())
    }

    /// Deletes `keys` (batched where the store supports it, as S3's
    /// DeleteObjects does); keys already gone are fine.
    pub fn delete_many(&self, keys: Vec<String>) -> Result<()> {
        if keys.is_empty() {
            return Ok(());
        }
        let store = self.store.clone();
        let paths: Vec<object_store::Result<Path>> =
            keys.iter().map(|k| Ok(self.path(k))).collect();
        self.run(async move {
            let mut results = store.delete_stream(futures::stream::iter(paths).boxed());
            while let Some(result) = results.next().await {
                match result {
                    Ok(_) | Err(object_store::Error::NotFound { .. }) => {}
                    Err(err) => return Err(S3Error::Store(err)),
                }
            }
            Ok(())
        })
    }

    pub fn delete(&self, key: &str) -> Result<()> {
        let store = self.store.clone();
        let path = self.path(key);
        self.run(async move {
            match store.delete(&path).await {
                Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()),
                Err(err) => Err(S3Error::Store(err)),
            }
        })
    }
}

/// A snapshot as stored: the database's size and CRC32C, and the stored
/// object's size and encoding.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Snapshot {
    pub size: u64,
    pub crc32c: u32,
    pub stored_size: u64,
    pub zstd: bool,
}

/// `path` with `suffix` appended to its file name (`app.db` ->
/// `app.db.s3-restore`), so databases differing only in their extension
/// don't share scratch files.
fn suffixed(path: &FsPath, suffix: &str) -> std::path::PathBuf {
    let mut name = path.as_os_str().to_owned();
    name.push(suffix);
    name.into()
}

/// A zstd-compressed object written in parts: one PUT if it fits in one
/// part, a multipart upload otherwise. Every part but the last has exactly
/// `part_size` bytes, sized from the input so any object up to S3's limit
/// fits in its 10,000 parts; one part is buffered at a time.
struct ObjectWriter<'a> {
    remote: &'a Remote,
    key: &'a str,
    encoder: zstd::stream::Encoder<'static, Vec<u8>>,
    upload: Option<Box<dyn object_store::MultipartUpload>>,
    part_size: usize,
    stored: u64,
}

impl<'a> ObjectWriter<'a> {
    /// `input`: at most how many (uncompressed) bytes will be written.
    fn new(remote: &'a Remote, key: &'a str, input: u64) -> Result<Self> {
        Ok(Self {
            remote,
            key,
            encoder: zstd::stream::Encoder::new(Vec::new(), SNAPSHOT_ZSTD_LEVEL)?,
            upload: None,
            part_size: part_size(input),
            stored: 0,
        })
    }

    fn write(&mut self, data: &[u8]) -> Result<()> {
        use std::io::Write;
        self.encoder.write_all(data)?;
        while self.encoder.get_ref().len() >= self.part_size {
            let buffered = self.encoder.get_mut();
            let rest = buffered.split_off(self.part_size);
            let part = std::mem::replace(buffered, rest);
            self.stored += part.len() as u64;
            self.remote.put_part(self.key, &mut self.upload, part)?;
        }
        Ok(())
    }

    /// Writes the object with `fill`, then checks the store holds all of it
    /// before it may be referenced. Returns the stored size.
    fn run(mut self, fill: impl FnOnce(&mut Self) -> Result<()>) -> Result<u64> {
        let result = fill(&mut self).and_then(|()| self.finish());
        if result.is_err() {
            if let Some(mut failed) = self.upload.take() {
                let _ = self
                    .remote
                    .run(async move { failed.abort().await.map_err(S3Error::Store) });
            }
        }
        let stored = result?;
        let key = self.key;
        if !matches!(self.remote.head(key)?, Some((_, found)) if found == stored) {
            return Err(S3Error::Corrupt(format!(
                "{key}: store does not hold the {stored} bytes it acknowledged"
            )));
        }
        Ok(stored)
    }

    fn finish(&mut self) -> Result<u64> {
        let encoder = std::mem::replace(
            &mut self.encoder,
            zstd::stream::Encoder::new(Vec::new(), 0)?,
        );
        let mut last = encoder.finish()?;
        self.stored += last.len() as u64;
        if self.upload.is_none() && last.len() <= self.part_size {
            self.remote
                .put(self.key, Bytes::from(last), Put::Overwrite)?;
        } else {
            while last.len() > self.part_size {
                let rest = last.split_off(self.part_size);
                let part = std::mem::replace(&mut last, rest);
                self.remote.put_part(self.key, &mut self.upload, part)?;
            }
            self.remote.put_part(self.key, &mut self.upload, last)?;
            let mut done = self.upload.take().expect("multipart upload started");
            self.remote
                .run(async move { done.complete().await.map_err(S3Error::Store) })?;
        }
        Ok(self.stored)
    }
}

/// Writes to a file while counting bytes and computing their CRC32C; shared
/// so the decoder can own one handle while the caller reads the totals.
#[derive(Clone)]
struct CountingWriter(Arc<std::sync::Mutex<(std::fs::File, u64, u32, bool)>>);

impl CountingWriter {
    fn new(file: std::fs::File) -> Self {
        Self(Arc::new(std::sync::Mutex::new((file, 0, 0, false))))
    }

    /// Whether a write to the file itself failed.
    fn file_failed(&self) -> bool {
        self.0.lock().unwrap().3
    }

    fn finish(&self) -> Result<(u64, u32)> {
        let guard = self.0.lock().unwrap();
        guard.0.sync_all()?;
        Ok((guard.1, guard.2))
    }
}

impl std::io::Write for CountingWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let mut guard = self.0.lock().unwrap();
        let n = match guard.0.write(buf) {
            Ok(n) => n,
            Err(err) => {
                guard.3 = true;
                return Err(err);
            }
        };
        guard.1 += n as u64;
        guard.2 = crc32c::crc32c_append(guard.2, &buf[..n]);
        Ok(n)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        let mut guard = self.0.lock().unwrap();
        let result = guard.0.flush();
        guard.3 |= result.is_err();
        result
    }
}

async fn get_object(store: &dyn ObjectStore, path: &Path) -> Result<Option<Object>> {
    match store.get(path).await {
        Ok(result) => {
            let version = UpdateVersion {
                e_tag: result.meta.e_tag.clone(),
                version: result.meta.version.clone(),
            };
            let bytes = result.bytes().await.map_err(S3Error::Store)?;
            Ok(Some(Object { bytes, version }))
        }
        Err(object_store::Error::NotFound { .. }) => Ok(None),
        Err(err) => Err(S3Error::Store(err)),
    }
}
