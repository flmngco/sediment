//! An `ObjectStore` decorator that injects failures, for fault tests.

use std::fmt;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use futures::stream::BoxStream;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, GetResultPayload, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult, Result,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Op {
    Put,
    Get,
    List,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Fault {
    /// Fail without touching the store.
    Fail,
    /// Perform the operation, then report failure (an ambiguous PUT).
    FailAfter,
    /// Hold the request "in flight" before it reaches the store.
    Delay(std::time::Duration),
    /// Report failure now; the request still lands after the delay.
    LandLater(std::time::Duration),
    /// GET only: deliver the first chunk of the body, then fail the stream.
    FailMidStream,
}

#[derive(Debug)]
struct Rule {
    op: Op,
    key_contains: String,
    fault: Fault,
    remaining: usize,
}

#[derive(Debug, Default)]
pub struct FaultyStore {
    inner: Arc<InMemory>,
    rules: Mutex<Vec<Rule>>,
    puts: Mutex<Vec<String>>,
    /// Sizes of the multipart parts uploaded, per key, in order.
    parts: Arc<Mutex<Vec<(String, usize)>>>,
    /// Behave like a provider that silently ignores If-None-Match/If-Match.
    ignore_conditions: std::sync::atomic::AtomicBool,
}

impl fmt::Display for FaultyStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "FaultyStore")
    }
}

impl FaultyStore {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Fails the next `times` `op`s whose key contains `key_contains`.
    pub fn inject(&self, op: Op, key_contains: &str, fault: Fault, times: usize) {
        self.rules.lock().unwrap().push(Rule {
            op,
            key_contains: key_contains.to_string(),
            fault,
            remaining: times,
        });
    }

    pub fn ignore_conditions(&self) {
        self.ignore_conditions
            .store(true, std::sync::atomic::Ordering::SeqCst);
    }

    pub fn clear_faults(&self) {
        self.rules.lock().unwrap().clear();
    }

    /// The sizes of the parts uploaded to `key` (multipart only).
    pub fn part_sizes(&self, key: &str) -> Vec<usize> {
        self.parts
            .lock()
            .unwrap()
            .iter()
            .filter(|(k, _)| k.ends_with(key))
            .map(|(_, size)| *size)
            .collect()
    }

    /// Keys of all attempted PUTs, in order.
    pub fn put_log(&self) -> Vec<String> {
        self.puts.lock().unwrap().clone()
    }

    fn fault(&self, op: Op, path: &Path) -> Option<Fault> {
        let mut rules = self.rules.lock().unwrap();
        let rule = rules
            .iter_mut()
            .find(|r| r.op == op && r.remaining > 0 && path.as_ref().contains(&r.key_contains))?;
        rule.remaining -= 1;
        Some(rule.fault)
    }
}

fn injected(path: &Path) -> object_store::Error {
    object_store::Error::Generic {
        store: "FaultyStore",
        source: format!("injected fault at {path}").into(),
    }
}

#[async_trait]
impl ObjectStore for FaultyStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        mut opts: PutOptions,
    ) -> Result<PutResult> {
        self.puts.lock().unwrap().push(location.to_string());
        if self
            .ignore_conditions
            .load(std::sync::atomic::Ordering::SeqCst)
        {
            opts.mode = object_store::PutMode::Overwrite;
        }
        match self.fault(Op::Put, location) {
            Some(Fault::Fail | Fault::FailMidStream) => Err(injected(location)),
            Some(Fault::FailAfter) => {
                self.inner.put_opts(location, payload, opts).await?;
                Err(injected(location))
            }
            Some(Fault::LandLater(delay)) => {
                let inner = self.inner.clone();
                let landing = location.clone();
                tokio::spawn(async move {
                    tokio::time::sleep(delay).await;
                    let _ = inner.put_opts(&landing, payload, opts).await;
                });
                Err(injected(location))
            }
            Some(Fault::Delay(delay)) => {
                tokio::time::sleep(delay).await;
                self.inner.put_opts(location, payload, opts).await
            }
            None => self.inner.put_opts(location, payload, opts).await,
        }
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        self.puts
            .lock()
            .unwrap()
            .push(format!("MULTIPART:{location}"));
        match self.fault(Op::Put, location) {
            Some(Fault::Delay(delay)) => tokio::time::sleep(delay).await,
            Some(_) => return Err(injected(location)),
            None => (),
        }
        let inner = self.inner.put_multipart_opts(location, opts).await?;
        Ok(Box::new(RecordedUpload {
            inner,
            key: location.to_string(),
            parts: self.parts.clone(),
        }))
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        match self.fault(Op::Get, location) {
            None => self.inner.get_opts(location, options).await,
            Some(Fault::Delay(delay)) => {
                tokio::time::sleep(delay).await;
                self.inner.get_opts(location, options).await
            }
            Some(Fault::FailMidStream) => {
                use futures::StreamExt;
                let mut result = self.inner.get_opts(location, options).await?;
                let empty = GetResultPayload::Stream(Box::pin(futures::stream::empty()));
                let GetResultPayload::Stream(body) = std::mem::replace(&mut result.payload, empty);
                let err = injected(location);
                result.payload = GetResultPayload::Stream(Box::pin(
                    body.take(1)
                        .chain(futures::stream::once(async move { Err(err) })),
                ));
                Ok(result)
            }
            Some(_) => Err(injected(location)),
        }
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        if let Some(path) = prefix {
            if self.fault(Op::List, path).is_some() {
                let err = injected(path);
                return Box::pin(futures::stream::once(async move { Err(err) }));
            }
        }
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

/// A multipart upload that records its parts' sizes.
#[derive(Debug)]
struct RecordedUpload {
    inner: Box<dyn MultipartUpload>,
    key: String,
    parts: Arc<Mutex<Vec<(String, usize)>>>,
}

#[async_trait]
impl MultipartUpload for RecordedUpload {
    fn put_part(&mut self, data: PutPayload) -> object_store::UploadPart {
        self.parts
            .lock()
            .unwrap()
            .push((self.key.clone(), data.content_length()));
        self.inner.put_part(data)
    }

    async fn complete(&mut self) -> Result<PutResult> {
        self.inner.complete().await
    }

    async fn abort(&mut self) -> Result<()> {
        self.inner.abort().await
    }
}
