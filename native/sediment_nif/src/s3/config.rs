use std::sync::Arc;
use std::time::Duration;

use object_store::aws::{AmazonS3Builder, Checksum, S3ConditionalPut};
use object_store::{ClientOptions, ObjectStore, RetryConfig};

use super::error::{Result, S3Error};

/// Where and how a database is stored in S3.
///
/// Built by the NIF from the Elixir `s3:` option. Only `bucket` is required;
/// everything else has defaults suitable for AWS, and `endpoint` switches to
/// an S3-compatible server (MinIO, SeaweedFS, R2, ...) with path-style URLs.
#[derive(Clone)]
pub struct S3Config {
    pub bucket: String,
    /// Key prefix for this database, without leading or trailing `/`.
    pub prefix: String,
    pub endpoint: Option<String>,
    pub region: String,
    pub access_key_id: Option<String>,
    pub secret_access_key: Option<String>,
    pub session_token: Option<String>,
    /// Use virtual-hosted-style URLs. Defaults to `false` when an endpoint is
    /// given (path style), `true` otherwise.
    pub virtual_hosted_style: Option<bool>,
    /// Writer lease duration.
    pub lease_ttl: Duration,
    /// Unique identity of this writer (defaults to host/pid/random).
    pub owner: Option<String>,
    /// Logical-log bytes between automatic checkpoints (each one uploads a
    /// snapshot). `None` keeps turso's default.
    pub checkpoint_threshold: Option<i64>,
    /// Per-request timeout.
    pub request_timeout: Duration,
    /// Retries for transient errors, per request.
    pub max_retries: usize,
    /// Parallel GETs when downloading log segments.
    pub download_concurrency: usize,
    /// Earlier epochs (snapshot plus full log) kept for point-in-time restore.
    pub retain_epochs: usize,
    /// Upload each batch of concurrently committed transactions as one
    /// object instead of one object per commit.
    pub group_commit: bool,
    /// Snapshots upload only the segments changed since the previous one
    /// (a delta), with a full snapshot again when the chain gets long.
    pub incremental_snapshots: bool,
    /// Acknowledge commits once written locally and upload them in the
    /// background (`durability: async`), instead of after the upload.
    pub async_durability: bool,
    /// Async durability: commits wait once the oldest pending one is this old.
    pub max_lag: Duration,
    /// Async durability: how long the uploader lets committed frames
    /// collect before uploading them as one segment (0: upload as soon as
    /// the previous upload is done). Below `max_lag`. Flushes, `sync: true`
    /// commits, checkpoints and close don't wait for it.
    pub upload_interval: Duration,
    /// Async durability: commits wait once this much is pending.
    pub max_pending_bytes: u64,
    /// Async durability: how long closing waits for pending uploads (twice:
    /// closing the connection, then dropping the storage).
    pub close_timeout: Duration,
    /// Check at open that the store enforces If-None-Match / If-Match (a
    /// store that ignores them can't fence writers).
    pub verify_conditional_writes: bool,
    /// Open a read-only replica: restore without the lease, never write to S3.
    pub replica: bool,
    /// The database's encryption (cipher and hex key), from the `:encryption`
    /// open option: snapshots and log frames are then encrypted at rest, and
    /// restoring needs the key.
    pub encryption: Option<turso_core::EncryptionOpts>,
    /// `encryption: false`: the database is stored unencrypted on purpose.
    /// Without it (or a key) S3 databases are refused: encryption is the
    /// default, opting out explicit.
    pub unencrypted: bool,
    /// Use this store instead of building an S3 client (tests, custom stores).
    pub store: Option<Arc<dyn ObjectStore>>,
}

impl std::fmt::Debug for S3Config {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3Config")
            .field("bucket", &self.bucket)
            .field("prefix", &self.prefix)
            .field("endpoint", &self.endpoint)
            .field("region", &self.region)
            .field("lease_ttl", &self.lease_ttl)
            .field("owner", &self.owner)
            .field("checkpoint_threshold", &self.checkpoint_threshold)
            .finish_non_exhaustive()
    }
}

impl S3Config {
    pub fn new(bucket: impl Into<String>, prefix: impl Into<String>) -> Self {
        Self {
            bucket: bucket.into(),
            prefix: normalize_prefix(&prefix.into()),
            endpoint: None,
            region: "us-east-1".to_string(),
            access_key_id: None,
            secret_access_key: None,
            session_token: None,
            virtual_hosted_style: None,
            lease_ttl: Duration::from_secs(30),
            owner: None,
            checkpoint_threshold: None,
            request_timeout: Duration::from_secs(30),
            max_retries: 3,
            download_concurrency: 32,
            retain_epochs: 0,
            group_commit: false,
            incremental_snapshots: true,
            async_durability: false,
            max_lag: Duration::from_millis(1000),
            upload_interval: Duration::ZERO,
            max_pending_bytes: 16 * 1024 * 1024,
            close_timeout: Duration::from_secs(10),
            verify_conditional_writes: true,
            replica: false,
            encryption: None,
            unencrypted: false,
            store: None,
        }
    }

    /// Build from string key/value pairs, as they arrive from Elixir.
    ///
    /// Durations are given in milliseconds (`lease_ttl_ms`, `request_timeout_ms`).
    pub fn from_pairs<K, V>(pairs: impl IntoIterator<Item = (K, V)>) -> Result<Self>
    where
        K: AsRef<str>,
        V: Into<String>,
    {
        let mut cfg = Self::new(String::new(), String::new());
        // The `s3:` option defaults to async durability; Rust code
        // building an S3Config directly (the storage tests) keeps sync.
        cfg.async_durability = true;
        for (key, value) in pairs {
            let value: String = value.into();
            match key.as_ref() {
                "bucket" => cfg.bucket = value,
                "prefix" => cfg.prefix = normalize_prefix(&value),
                "endpoint" => cfg.endpoint = Some(value),
                "region" => cfg.region = value,
                "access_key_id" => cfg.access_key_id = Some(value),
                "secret_access_key" => cfg.secret_access_key = Some(value),
                "session_token" => cfg.session_token = Some(value),
                "virtual_hosted_style" => cfg.virtual_hosted_style = Some(parse_bool(&value)?),
                "lease_ttl_ms" => cfg.lease_ttl = Duration::from_millis(parse_num(&key, &value)?),
                "owner" => cfg.owner = Some(value),
                "checkpoint_threshold" => cfg.checkpoint_threshold = Some(parse_num(&key, &value)?),
                "request_timeout_ms" => {
                    cfg.request_timeout = Duration::from_millis(parse_num(&key, &value)?)
                }
                "max_retries" => cfg.max_retries = parse_num(&key, &value)?,
                "download_concurrency" => cfg.download_concurrency = parse_num(&key, &value)?,
                "retain_epochs" => cfg.retain_epochs = parse_num(&key, &value)?,
                "group_commit" => cfg.group_commit = parse_bool(&value)?,
                "incremental_snapshots" => cfg.incremental_snapshots = parse_bool(&value)?,
                "durability" => {
                    cfg.async_durability = match value.as_str() {
                        "async" => true,
                        "sync" => false,
                        other => {
                            return Err(S3Error::Config(format!(
                                "durability must be async or sync, got {other:?}"
                            )))
                        }
                    }
                }
                "max_lag_ms" => cfg.max_lag = Duration::from_millis(parse_num(&key, &value)?),
                "upload_interval_ms" => {
                    cfg.upload_interval = Duration::from_millis(parse_num(&key, &value)?)
                }
                "max_pending_bytes" => cfg.max_pending_bytes = parse_num(&key, &value)?,
                "close_timeout_ms" => {
                    cfg.close_timeout = Duration::from_millis(parse_num(&key, &value)?)
                }
                "verify_conditional_writes" => cfg.verify_conditional_writes = parse_bool(&value)?,
                // Forwarded from the top-level `encryption: false` open option.
                "encryption" => {
                    if parse_bool(&value)? {
                        return Err(S3Error::Config(
                            "set the key with the :encryption option \
                             ([cipher: \"aegis256\", key: \"<64 hex chars>\"]), not inside :s3"
                                .into(),
                        ));
                    }
                    cfg.unencrypted = true;
                }
                "mode" => {
                    cfg.replica = match value.as_str() {
                        "writer" => false,
                        "replica" => true,
                        other => {
                            return Err(S3Error::Config(format!(
                                "mode must be writer or replica, got {other:?}"
                            )))
                        }
                    }
                }
                other => return Err(S3Error::Config(format!("unknown option {other:?}"))),
            }
        }
        cfg.validate()?;
        Ok(cfg)
    }

    pub fn validate(&self) -> Result<()> {
        if let Some(endpoint) = &self.endpoint {
            let rest = endpoint
                .strip_prefix("http://")
                .or_else(|| endpoint.strip_prefix("https://"));
            let host = rest.map(|r| r.split('/').next().unwrap_or(""));
            if !host.is_some_and(|h| !h.is_empty() && !h.contains(char::is_whitespace)) {
                return Err(S3Error::Config(format!(
                    "invalid endpoint {endpoint:?}: expected http(s)://host[:port]"
                )));
            }
        }
        if self.store.is_none() && self.bucket.is_empty() {
            return Err(S3Error::Config("bucket is required".into()));
        }
        if self.lease_ttl < Duration::from_secs(1) {
            return Err(S3Error::Config("lease_ttl must be at least 1s".into()));
        }
        if !self.upload_interval.is_zero() {
            if !self.async_durability {
                return Err(S3Error::Config(
                    "upload_interval_ms applies to durability: :async only".into(),
                ));
            }
            if self.upload_interval >= self.max_lag {
                return Err(S3Error::Config(format!(
                    "upload_interval_ms ({}) must be below max_lag_ms ({}): commits wait once \
                     the oldest pending one is max_lag_ms old",
                    self.upload_interval.as_millis(),
                    self.max_lag.as_millis()
                )));
            }
        }
        if self.download_concurrency == 0 {
            return Err(S3Error::Config("download_concurrency must be > 0".into()));
        }
        Ok(())
    }

    /// The settings that change how a database behaves; every connection to
    /// one path in a VM shares one storage, so they must agree.
    pub(crate) fn settings(&self) -> String {
        format!(
            "bucket={} prefix={} endpoint={} region={} owner={} lease_ttl_ms={} \
             group_commit={} incremental_snapshots={} durability={} max_lag_ms={} \
             upload_interval_ms={} \
             max_pending_bytes={} retain_epochs={} checkpoint_threshold={} encryption={}",
            self.bucket,
            self.prefix,
            self.endpoint.as_deref().unwrap_or("aws"),
            self.region,
            self.owner.as_deref().unwrap_or("(default)"),
            self.lease_ttl.as_millis(),
            self.group_commit,
            self.incremental_snapshots,
            if self.async_durability {
                "async"
            } else {
                "sync"
            },
            self.max_lag.as_millis(),
            self.upload_interval.as_millis(),
            self.max_pending_bytes,
            self.retain_epochs,
            self.checkpoint_threshold
                .map_or("(default)".to_string(), |t| t.to_string()),
            self.encryption
                .as_ref()
                .map_or("none", |e| e.cipher.as_str()),
        )
    }

    /// Identifies the store for caching the conditional-write probe.
    /// Where the database is: the store and the prefix.
    pub(crate) fn place(&self) -> String {
        match &self.store {
            Some(store) => format!("{:p}|{}", Arc::as_ptr(store) as *const (), self.prefix),
            None => format!("{}|{}", self.store_identity(), self.prefix),
        }
    }

    pub(crate) fn store_identity(&self) -> String {
        format!(
            "{}|{}|{}",
            self.endpoint.as_deref().unwrap_or("aws"),
            self.region,
            self.bucket
        )
    }

    /// The store scoped to this database's prefix. Every operation is bounded
    /// by the time its retries may take, even with a custom `store`.
    pub(crate) fn remote(&self) -> Result<super::remote::Remote> {
        let attempts = self.max_retries as u32 + 1;
        let log_encryption = self
            .encryption_ctx()?
            .map(|ctx| (ctx.tag_size(), ctx.nonce_size()));
        Ok(
            super::remote::Remote::new(self.build_store()?, &self.prefix)
                .with_deadline(self.request_timeout * attempts)
                .with_log_encryption(log_encryption),
        )
    }

    /// The logical log's encryption context, if the database is encrypted.
    pub(crate) fn encryption_ctx(&self) -> Result<Option<turso_core::EncryptionContext>> {
        let Some(opts) = &self.encryption else {
            return Ok(None);
        };
        let cipher = turso_core::CipherMode::try_from(opts.cipher.as_str())
            .map_err(|e| S3Error::Config(format!("encryption: {e}")))?;
        let key = turso_core::EncryptionKey::from_hex_string(&opts.hexkey)
            .map_err(|e| S3Error::Config(format!("encryption: {e}")))?;
        // The page size only matters for page encryption; the log is
        // encrypted in fixed-size chunks.
        turso_core::EncryptionContext::new(cipher, &key, 4096)
            .map(Some)
            .map_err(|e| S3Error::Config(format!("encryption: {e}")))
    }

    pub(crate) fn build_store(&self) -> Result<Arc<dyn ObjectStore>> {
        if let Some(store) = &self.store {
            return Ok(store.clone());
        }
        let mut builder = AmazonS3Builder::from_env()
            .with_bucket_name(&self.bucket)
            .with_region(&self.region)
            .with_conditional_put(S3ConditionalPut::ETagMatch)
            // Every upload (each multipart part too) carries the SHA-256 of
            // its body, so a server can't store a PUT whose connection broke
            // after the headers as an empty or truncated object (SeaweedFS
            // does without it, and an empty manifest bricks the database).
            .with_checksum_algorithm(Checksum::SHA256)
            .with_retry(RetryConfig {
                max_retries: self.max_retries,
                retry_timeout: self.request_timeout * (self.max_retries as u32 + 1),
                ..RetryConfig::default()
            })
            .with_client_options(ClientOptions::new().with_timeout(self.request_timeout))
            .with_http_connector(super::meter::MeteredConnector);
        if let Some(endpoint) = &self.endpoint {
            builder = builder
                .with_endpoint(endpoint)
                .with_allow_http(endpoint.starts_with("http://"))
                .with_virtual_hosted_style_request(self.virtual_hosted_style.unwrap_or(false));
        } else if let Some(vhost) = self.virtual_hosted_style {
            builder = builder.with_virtual_hosted_style_request(vhost);
        }
        if let Some(key) = &self.access_key_id {
            builder = builder.with_access_key_id(key);
        }
        if let Some(secret) = &self.secret_access_key {
            builder = builder.with_secret_access_key(secret);
        }
        if let Some(token) = &self.session_token {
            builder = builder.with_token(token);
        }
        let store = builder
            .build()
            .map_err(|err| S3Error::Config(err.to_string()))?;
        Ok(Arc::new(store))
    }
}

fn normalize_prefix(prefix: &str) -> String {
    prefix.trim_matches('/').to_string()
}

fn parse_bool(value: &str) -> Result<bool> {
    match value {
        "true" | "1" => Ok(true),
        "false" | "0" => Ok(false),
        _ => Err(S3Error::Config(format!("expected boolean, got {value:?}"))),
    }
}

fn parse_num<T: std::str::FromStr>(key: &impl AsRef<str>, value: &str) -> Result<T> {
    value.parse().map_err(|_| {
        S3Error::Config(format!(
            "{}: expected a number, got {value:?}",
            key.as_ref()
        ))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn err(pairs: &[(&str, &str)]) -> String {
        S3Config::from_pairs(pairs.iter().copied())
            .unwrap_err()
            .to_string()
    }

    #[test]
    fn parses_every_option() {
        let cfg = S3Config::from_pairs([
            ("bucket", "b"),
            ("prefix", "/a/b/"),
            ("endpoint", "http://localhost:9000"),
            ("region", "eu-west-1"),
            ("access_key_id", "k"),
            ("secret_access_key", "s"),
            ("session_token", "t"),
            ("virtual_hosted_style", "false"),
            ("lease_ttl_ms", "5000"),
            ("owner", "me"),
            ("checkpoint_threshold", "-1"),
            ("request_timeout_ms", "1500"),
            ("max_retries", "0"),
            ("download_concurrency", "4"),
            ("retain_epochs", "3"),
            ("group_commit", "true"),
            ("mode", "replica"),
        ])
        .unwrap();
        assert_eq!(cfg.prefix, "a/b");
        assert_eq!(cfg.lease_ttl, Duration::from_secs(5));
        assert_eq!(cfg.checkpoint_threshold, Some(-1));
        assert_eq!(cfg.request_timeout, Duration::from_millis(1500));
        assert_eq!((cfg.max_retries, cfg.download_concurrency), (0, 4));
        assert_eq!(cfg.retain_epochs, 3);
        assert!(cfg.group_commit && cfg.replica);
        assert_eq!(cfg.owner.as_deref(), Some("me"));
    }

    #[test]
    fn rejects_bad_values() {
        assert!(err(&[("prefix", "x")]).contains("bucket is required"));
        assert!(err(&[("bucket", "b"), ("nope", "1")]).contains("unknown option"));
        assert!(err(&[("bucket", "b"), ("lease_ttl_ms", "soon")]).contains("expected a number"));
        assert!(err(&[("bucket", "b"), ("lease_ttl_ms", "10")]).contains("at least 1s"));
        assert!(err(&[("bucket", "b"), ("group_commit", "maybe")]).contains("boolean"));
        assert!(err(&[("bucket", "b"), ("mode", "primary")]).contains("writer or replica"));
        assert!(err(&[("bucket", "b"), ("download_concurrency", "0")]).contains("> 0"));
    }
}
