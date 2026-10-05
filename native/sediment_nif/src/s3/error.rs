use std::fmt;

/// Errors produced by the S3 durability layer.
#[derive(Debug)]
pub enum S3Error {
    /// Invalid or incomplete configuration.
    Config(String),
    /// Transport or server error from the object store.
    Store(object_store::Error),
    /// A conditional write lost (object exists / etag changed).
    Conflict(String),
    /// Another writer holds an unexpired lease.
    LeaseHeld { owner: String, expires_at_ms: u64 },
    /// This writer lost ownership; all further writes are refused.
    Fenced(String),
    /// Remote state is inconsistent (gap in the log, CRC mismatch, bad manifest).
    Corrupt(String),
    /// Local filesystem error.
    Io(std::io::Error),
    /// Error from turso_core while bootstrapping the local database.
    Turso(String),
    /// A wait for uploads gave up (timeout or cancel); the message says
    /// what is durable.
    Timeout(String),
    /// Commits acknowledged earlier (async) never reached S3; reported until
    /// acknowledged (`S3DurableStorage::acknowledge_loss`).
    Lost(String),
}

pub type Result<T> = std::result::Result<T, S3Error>;

impl fmt::Display for S3Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            S3Error::Config(msg) => write!(f, "s3 config: {msg}"),
            S3Error::Store(err) => write!(f, "s3 store: {err}"),
            S3Error::Conflict(msg) => write!(f, "s3 conflict: {msg}"),
            S3Error::LeaseHeld {
                owner,
                expires_at_ms,
            } => write!(
                f,
                "s3 lease held by {owner} until {expires_at_ms} (unix ms)"
            ),
            S3Error::Fenced(msg) => write!(f, "s3 writer fenced: {msg}"),
            S3Error::Corrupt(msg) => write!(f, "s3 corrupt: {msg}"),
            S3Error::Io(err) => write!(f, "s3 local io: {err}"),
            S3Error::Turso(msg) => write!(f, "s3 bootstrap: {msg}"),
            S3Error::Timeout(msg) => write!(f, "{msg}"),
            S3Error::Lost(msg) => write!(f, "{msg}"),
        }
    }
}

impl std::error::Error for S3Error {}

impl From<std::io::Error> for S3Error {
    fn from(err: std::io::Error) -> Self {
        S3Error::Io(err)
    }
}

impl From<turso_core::LimboError> for S3Error {
    fn from(err: turso_core::LimboError) -> Self {
        S3Error::Turso(crate::error::locked_elsewhere(&err).unwrap_or_else(|| err.to_string()))
    }
}

impl From<serde_json::Error> for S3Error {
    fn from(err: serde_json::Error) -> Self {
        S3Error::Corrupt(format!("invalid json: {err}"))
    }
}

impl From<S3Error> for turso_core::LimboError {
    fn from(err: S3Error) -> Self {
        turso_core::LimboError::InternalError(err.to_string())
    }
}
