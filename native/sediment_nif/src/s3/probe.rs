//! Checks that the object store enforces the conditional writes fencing
//! relies on. A store that silently ignores `If-None-Match: *` or `If-Match`
//! would let a stale writer overwrite acknowledged commits.

use std::collections::BTreeSet;
use std::sync::Mutex;

use object_store::UpdateVersion;

use super::error::{Result, S3Error};
use super::layout::now_ms;
use super::remote::{Put, Remote};

static VERIFIED: Mutex<BTreeSet<String>> = Mutex::new(BTreeSet::new());

fn unsupported(what: &str) -> S3Error {
    S3Error::Config(format!(
        "this S3 provider does not enforce {what}, which S3 durability needs to fence \
         writers; refusing to open (see guides/s3_providers.md)"
    ))
}

/// Runs the probe once per `identity` per process.
pub fn verify(remote: &Remote, identity: &str) -> Result<()> {
    if VERIFIED
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .contains(identity)
    {
        return Ok(());
    }
    let key = format!("probe/{:016x}-{}", now_ms(), std::process::id());
    let result = probe(remote, &key);
    let _ = remote.delete(&key);
    result?;
    VERIFIED
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .insert(identity.to_string());
    Ok(())
}

/// The version of `key` if it holds `content`: a PUT whose answer was lost
/// landed, and the client's retry then conflicted with it.
fn landed(remote: &Remote, key: &str, content: &str) -> Result<Option<UpdateVersion>> {
    Ok(remote
        .get(key)?
        .filter(|object| object.bytes.as_ref() == content.as_bytes())
        .map(|object| object.version))
}

fn probe(remote: &Remote, key: &str) -> Result<()> {
    // Every upload carries x-amz-checksum-sha256 (config.rs), these too: a
    // provider that refuses checksummed conditional PUTs fails here, and the
    // open with it.
    let created = match remote.put(key, "probe 1".into(), Put::Create) {
        Ok(version) => version,
        Err(err @ S3Error::Conflict(_)) => landed(remote, key, "probe 1")?.ok_or(err)?,
        Err(err) => return Err(err),
    };
    match remote.put(key, "probe 2".into(), Put::Create) {
        Err(S3Error::Conflict(_)) => {}
        Ok(_) => return Err(unsupported("If-None-Match: * (create-only PUT)")),
        Err(err) => return Err(err),
    }
    let stale = UpdateVersion {
        e_tag: Some("\"00000000000000000000000000000000\"".to_string()),
        version: None,
    };
    match remote.put(key, "probe 3".into(), Put::Update(stale)) {
        Err(S3Error::Conflict(_)) => {}
        Ok(_) => return Err(unsupported("If-Match (compare-and-swap PUT)")),
        Err(err) => return Err(err),
    }
    match remote.put(key, "probe 4".into(), Put::Update(created)) {
        Ok(_) => Ok(()),
        Err(S3Error::Conflict(_)) if landed(remote, key, "probe 4")?.is_some() => Ok(()),
        Err(S3Error::Conflict(_)) => Err(unsupported("If-Match with the current ETag")),
        Err(err) => Err(err),
    }
}
