//! Counts every HTTP request the S3 client sends (retries and multipart
//! parts included: this sits below object_store's retry loop), by S3
//! operation and price class, so a run against a paid provider can be
//! budgeted.
//!
//! With `SEDIMENT_S3_METER_DIR` set, each request is also appended to
//! `<dir>/requests-<pid>.log` before it is sent (so the log survives a
//! `kill -9`), a failed one again with its status, and no request is sent
//! while `<dir>/STOP` exists.
//!
//! With `SEDIMENT_S3_TRACE=1` as well (the TLA+ trace validation), every request
//! is logged with its key, condition and role (`T <ms> <id> <op> <path?query> <cond> <tag>`)
//! and every answer with its status and ETag (`A <ms> <id> <status> <etag>`).

use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{LazyLock, Mutex};

use async_trait::async_trait;
use object_store::client::{
    HttpClient, HttpConnector, HttpError, HttpErrorKind, HttpRequest, HttpResponse, HttpService,
    ReqwestConnector,
};
use object_store::ClientOptions;

/// Price classes as S3 providers bill them: A (writes, lists), B (reads),
/// free (deletes).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Class {
    A,
    B,
    Free,
}

impl Class {
    pub fn name(self) -> &'static str {
        match self {
            Class::A => "A",
            Class::B => "B",
            Class::Free => "free",
        }
    }
}

/// Every operation the client can send; unrecognized requests count as
/// `Other` (class A, to err on the expensive side).
pub const OPERATIONS: [(&str, Class); 14] = [
    ("PutObject", Class::A),
    ("CopyObject", Class::A),
    ("CreateMultipartUpload", Class::A),
    ("UploadPart", Class::A),
    ("CompleteMultipartUpload", Class::A),
    ("ListObjectsV2", Class::A),
    ("ListMultipartUploads", Class::A),
    ("ListParts", Class::A),
    ("Other", Class::A),
    ("GetObject", Class::B),
    ("HeadObject", Class::B),
    ("DeleteObject", Class::Free),
    ("DeleteObjects", Class::Free),
    ("AbortMultipartUpload", Class::Free),
];

static COUNTS: [AtomicU64; OPERATIONS.len()] = [const { AtomicU64::new(0) }; OPERATIONS.len()];

/// Requests sent by this OS process so far: `(operation, class, count)`.
pub fn counts() -> Vec<(&'static str, Class, u64)> {
    OPERATIONS
        .iter()
        .zip(COUNTS.iter())
        .map(|((op, class), n)| (*op, *class, n.load(Ordering::Relaxed)))
        .collect()
}

/// The operation of a request, from its method, query and headers.
pub fn classify(method: &str, query: Option<&str>, copy: bool) -> usize {
    let has = |key: &str| {
        query
            .unwrap_or("")
            .split('&')
            .any(|pair| pair.split('=').next() == Some(key))
    };
    let op = match method {
        "GET" if has("list-type") => "ListObjectsV2",
        "GET" if has("uploads") => "ListMultipartUploads",
        "GET" if has("uploadId") => "ListParts",
        "GET" => "GetObject",
        "HEAD" => "HeadObject",
        "PUT" if has("uploadId") => "UploadPart",
        "PUT" if copy => "CopyObject",
        "PUT" => "PutObject",
        "POST" if has("uploads") => "CreateMultipartUpload",
        "POST" if has("uploadId") => "CompleteMultipartUpload",
        "POST" if has("delete") => "DeleteObjects",
        "DELETE" if has("uploadId") => "AbortMultipartUpload",
        "DELETE" => "DeleteObject",
        _ => "Other",
    };
    OPERATIONS
        .iter()
        .position(|(name, _)| *name == op)
        .expect("every operation is listed")
}

struct Log {
    file: Mutex<File>,
    stop: PathBuf,
    trace: bool,
}

static NEXT_ID: AtomicU64 = AtomicU64::new(1);

/// Names a request's role for the trace (e.g. a seal, which is otherwise just a
/// create-only PUT of a log key). Travels in the request's extensions, never on the
/// wire, and is logged only in trace mode.
#[derive(Clone, Copy, Debug)]
pub struct TraceTag(pub &'static str);

static LOG: LazyLock<Option<Log>> = LazyLock::new(|| {
    let dir = PathBuf::from(std::env::var_os("SEDIMENT_S3_METER_DIR")?);
    let _ = std::fs::create_dir_all(&dir);
    let path = dir.join(format!("requests-{}.log", std::process::id()));
    match OpenOptions::new().create(true).append(true).open(&path) {
        Ok(file) => Some(Log {
            file: Mutex::new(file),
            stop: dir.join("STOP"),
            trace: std::env::var("SEDIMENT_S3_TRACE").is_ok_and(|v| v == "1"),
        }),
        // Metering was asked for: better no requests than uncounted ones.
        Err(err) => panic!(
            "SEDIMENT_S3_METER_DIR: can't open {}: {err}",
            path.display()
        ),
    }
});

fn now_ms() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0)
}

fn log_line(log: &Log, line: String) -> std::io::Result<()> {
    // One write per line on an O_APPEND file: lines of concurrent requests
    // (and processes) don't interleave.
    log.file
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .write_all(line.as_bytes())
}

/// Counts a request about to be sent, or refuses it once the budget's
/// stop file exists.
fn record(op: usize) -> Result<(), HttpError> {
    let (name, class) = OPERATIONS[op];
    if let Some(log) = LOG.as_ref() {
        if log.stop.exists() {
            return Err(HttpError::new(
                HttpErrorKind::Unknown,
                std::io::Error::other(format!(
                    "S3 request budget exhausted ({} exists)",
                    log.stop.display()
                )),
            ));
        }
        log_line(log, format!("R {} {name} {}\n", now_ms(), class.name())).map_err(|err| {
            HttpError::new(
                HttpErrorKind::Unknown,
                std::io::Error::other(err.to_string()),
            )
        })?;
    }
    COUNTS[op].fetch_add(1, Ordering::Relaxed);
    Ok(())
}

/// Logs a request that failed: a transport error, a throttle or a server
/// error (not the 404/412 answers the protocol expects).
fn record_failure(op: usize, status: String) {
    if let Some(log) = LOG.as_ref() {
        let _ = log_line(
            log,
            format!("E {} {} {status}\n", now_ms(), OPERATIONS[op].0),
        );
    }
}

/// The client every S3 store is built with: reqwest, metered.
#[derive(Debug, Default)]
pub struct MeteredConnector;

impl HttpConnector for MeteredConnector {
    fn connect(&self, options: &ClientOptions) -> object_store::Result<HttpClient> {
        let inner = ReqwestConnector::default().connect(options)?;
        Ok(HttpClient::new(Metered(inner)))
    }
}

#[derive(Debug)]
struct Metered(HttpClient);

#[async_trait]
impl HttpService for Metered {
    async fn call(&self, req: HttpRequest) -> Result<HttpResponse, HttpError> {
        let op = classify(
            req.method().as_str(),
            req.uri().query(),
            req.headers().contains_key("x-amz-copy-source"),
        );
        record(op)?;
        let traced = LOG.as_ref().filter(|log| log.trace).map(|log| {
            let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
            let header = |name: &str| {
                req.headers()
                    .get(name)
                    .and_then(|v| v.to_str().ok())
                    .map(|v| v.replace(' ', ""))
            };
            let cond = match (header("if-none-match"), header("if-match")) {
                (Some(_), _) => "if-none-match".to_string(),
                (None, Some(etag)) => format!("if-match={etag}"),
                (None, None) => "-".to_string(),
            };
            let tag = req.extensions().get::<TraceTag>().map_or("-", |t| t.0);
            let line = format!(
                "T {} {id} {} {} {cond} {tag}\n",
                now_ms(),
                OPERATIONS[op].0,
                req.uri()
                    .path_and_query()
                    .map(|p| p.as_str())
                    .unwrap_or("-")
            );
            let _ = log_line(log, line);
            (log, id)
        });
        let result = self.0.execute(req).await;
        if let Some((log, id)) = traced {
            let (status, etag) = match &result {
                Ok(response) => (
                    response.status().as_u16().to_string(),
                    response
                        .headers()
                        .get("etag")
                        .and_then(|v| v.to_str().ok())
                        .unwrap_or("-")
                        .replace(' ', ""),
                ),
                Err(err) => (format!("{:?}", err.kind()), "-".to_string()),
            };
            let _ = log_line(log, format!("A {} {id} {status} {etag}\n", now_ms()));
        }
        match &result {
            Ok(response) if response.status().is_server_error() || response.status() == 429 => {
                record_failure(op, response.status().as_u16().to_string())
            }
            Ok(_) => {}
            Err(err) => record_failure(op, format!("{:?}", err.kind())),
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn op(method: &str, query: Option<&str>, copy: bool) -> &'static str {
        OPERATIONS[classify(method, query, copy)].0
    }

    #[test]
    fn requests_are_classified_by_operation() {
        assert_eq!(op("PUT", None, false), "PutObject");
        assert_eq!(op("PUT", None, true), "CopyObject");
        assert_eq!(
            op("PUT", Some("partNumber=2&uploadId=x"), false),
            "UploadPart"
        );
        assert_eq!(op("POST", Some("uploads"), false), "CreateMultipartUpload");
        assert_eq!(
            op("POST", Some("uploadId=x"), false),
            "CompleteMultipartUpload"
        );
        assert_eq!(op("POST", Some("delete"), false), "DeleteObjects");
        assert_eq!(
            op("DELETE", Some("uploadId=x"), false),
            "AbortMultipartUpload"
        );
        assert_eq!(op("DELETE", None, false), "DeleteObject");
        assert_eq!(
            op("GET", Some("list-type=2&prefix=a%2F"), false),
            "ListObjectsV2"
        );
        assert_eq!(
            op("GET", Some("uploads&prefix=a"), false),
            "ListMultipartUploads"
        );
        assert_eq!(op("GET", Some("uploadId=x"), false), "ListParts");
        assert_eq!(op("GET", None, false), "GetObject");
        // A key containing "list-type" is in the path, not the query.
        assert_eq!(op("GET", Some("x-id=GetObject"), false), "GetObject");
        assert_eq!(op("HEAD", None, false), "HeadObject");
        assert_eq!(op("PATCH", None, false), "Other");
        assert_eq!(
            OPERATIONS[classify("PATCH", None, false)].1,
            Class::A,
            "unknown requests count as the expensive class"
        );
    }
}
