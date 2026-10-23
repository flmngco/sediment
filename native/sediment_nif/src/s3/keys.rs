//! Encryption keys: the ring an open is given (`:encryption`), and what a
//! manifest records about the key of each epoch.
//!
//! A key is known by an id the caller chooses and by its fingerprint, the
//! first 16 bytes of HMAC-SHA256(key, "sediment/key-fingerprint/v1") in hex,
//! which tells two keys apart without revealing anything usable about
//! either. Key material never appears in an error, a log line or S3; ids and
//! fingerprints do.

use serde::{Deserialize, Serialize};

use super::error::{Result, S3Error};

const FINGERPRINT_CONTEXT: &[u8] = b"sediment/key-fingerprint/v1";

/// The ciphers turso takes, for messages.
const CIPHERS: &str =
    "aes128gcm, aes256gcm, aegis128l, aegis128x2, aegis128x4, aegis256, aegis256x2, aegis256x4";

/// What the manifest records about a key: enough to pick it from a ring and
/// to tell whether a ring's key under that id is the same key.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KeyRecord {
    pub id: String,
    pub cipher: String,
    pub fingerprint: String,
}

/// One key of a ring.
#[derive(Clone)]
pub struct Key {
    id: String,
    cipher: String,
    hexkey: String,
    fingerprint: String,
}

impl std::fmt::Debug for Key {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Key")
            .field("id", &self.id)
            .field("cipher", &self.cipher)
            .field("fingerprint", &self.fingerprint)
            .finish_non_exhaustive()
    }
}

impl Key {
    /// Checks the id, the cipher and the key (hex, of the cipher's size).
    /// Errors never show the key, nor an id that could be one (see
    /// [`named`]).
    pub fn new(id: &str, cipher: &str, hexkey: &str) -> Result<Self> {
        check_id(id)?;
        // Never echo the cipher: a key given in its place would show.
        let mode = turso_core::CipherMode::try_from(cipher).map_err(|_| {
            S3Error::Config(format!(
                "{}: unknown cipher (supported: {CIPHERS})",
                named(id)
            ))
        })?;
        let size = mode.required_key_size();
        let bytes = decode_hex(hexkey).ok_or_else(|| {
            S3Error::Config(format!(
                "{}: must be hex encoded (an even number of 0-9a-f digits)",
                named(id)
            ))
        })?;
        if bytes.len() != size {
            return Err(S3Error::Config(format!(
                "{}: {mode} needs a {size}-byte key ({} hex characters), got {} bytes",
                named(id),
                2 * size,
                bytes.len()
            )));
        }
        Ok(Self {
            id: id.to_string(),
            cipher: mode.to_string(),
            hexkey: hexkey.to_ascii_lowercase(),
            fingerprint: fingerprint(&bytes),
        })
    }

    pub fn id(&self) -> &str {
        &self.id
    }

    pub fn cipher(&self) -> &str {
        &self.cipher
    }

    pub fn fingerprint(&self) -> &str {
        &self.fingerprint
    }

    /// The options turso opens the database (and its log) with.
    pub fn opts(&self) -> turso_core::EncryptionOpts {
        turso_core::EncryptionOpts {
            cipher: self.cipher.clone(),
            hexkey: self.hexkey.clone(),
        }
    }

    pub fn record(&self) -> KeyRecord {
        KeyRecord {
            id: self.id.clone(),
            cipher: self.cipher.clone(),
            fingerprint: self.fingerprint.clone(),
        }
    }

    /// The same key: the same cipher and key material.
    pub fn same(&self, other: &Key) -> bool {
        self.cipher == other.cipher && self.fingerprint == other.fingerprint
    }

    /// The log's encryption overhead per chunk: tag and nonce sizes.
    pub fn log_encryption(&self) -> Result<(usize, usize)> {
        let ctx = self.context()?;
        Ok((ctx.tag_size(), ctx.nonce_size()))
    }

    /// The logical log's encryption context.
    pub fn context(&self) -> Result<turso_core::EncryptionContext> {
        // The page size only matters for page encryption; the log is
        // encrypted in fixed-size chunks.
        self.page_context(4096)
    }

    /// The encryption context of a database file with pages of `page_size`.
    pub fn page_context(&self, page_size: usize) -> Result<turso_core::EncryptionContext> {
        let cipher = turso_core::CipherMode::try_from(self.cipher.as_str())
            .map_err(|e| S3Error::Config(format!("key {}: {e}", shown(&self.id))))?;
        let key = turso_core::EncryptionKey::from_hex_string(&self.hexkey)
            .map_err(|_| S3Error::Config(format!("key {}: not a valid key", shown(&self.id))))?;
        turso_core::EncryptionContext::new(cipher, &key, page_size)
            .map_err(|e| S3Error::Config(format!("key {}: {e}", shown(&self.id))))
    }
}

/// Whether `id` could be key material: hex of a key's length (32 hex
/// digits or more; the smallest keys are 16 bytes). An entry given as
/// `{key, id}` has its key as the id.
fn looks_like_a_key(id: &str) -> bool {
    id.len() >= 32 && id.bytes().all(|b| b.is_ascii_hexdigit())
}

/// An id as every message (and result) shows it: as is, unless it could be
/// key material. [`check_id`] refuses such ids, so only an id that wasn't
/// checked is ever redacted.
pub fn shown(id: &str) -> std::borrow::Cow<'_, str> {
    if looks_like_a_key(id) {
        "<an id that looks like a key>".into()
    } else {
        id.into()
    }
}

/// Any other value a message names (an option, a mode) as it shows it: as
/// is, unless it could be a key given in the wrong place.
pub fn shown_value(value: &str) -> std::borrow::Cow<'_, str> {
    if looks_like_a_key(value) {
        "<a value that looks like a key>".into()
    } else {
        value.into()
    }
}

/// `key <id>` for a message about a key that was refused.
fn named(id: &str) -> String {
    if looks_like_a_key(id) {
        "a key whose id looks like a key (an entry given as {key, id}?)".into()
    } else {
        format!("key {id}")
    }
}

/// Key ids: short, lowercase, safe in messages, manifests and file names,
/// and never hex of a key's length (32 hex digits), which is refused: the
/// id is recorded in S3 and shown in messages, and an AES-128 key would
/// pass as one. The error never shows the id refused: it may be a key.
pub(crate) fn check_id(id: &str) -> Result<()> {
    let mut bytes = id.bytes();
    let first_ok = bytes
        .next()
        .is_some_and(|b| b.is_ascii_lowercase() || b.is_ascii_digit());
    let rest_ok =
        bytes.all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_' || b == b'-');
    if looks_like_a_key(id) {
        Err(S3Error::Config(format!(
            "invalid key id ({} hex digits): it looks like a key (an entry given as {{key, id}}?); \
             an id of 32 hex digits is refused",
            id.len()
        )))
    } else if first_ok && rest_ok && id.len() <= 32 {
        Ok(())
    } else {
        Err(S3Error::Config(format!(
            "invalid key id ({} characters): ids match [a-z0-9][a-z0-9_-]{{0,31}}",
            id.chars().count()
        )))
    }
}

fn decode_hex(hex: &str) -> Option<Vec<u8>> {
    if !hex.len().is_multiple_of(2) {
        return None;
    }
    (0..hex.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(hex.get(i..i + 2)?, 16).ok())
        .collect()
}

fn fingerprint(key: &[u8]) -> String {
    let mac = aws_lc_rs::hmac::sign(
        &aws_lc_rs::hmac::Key::new(aws_lc_rs::hmac::HMAC_SHA256, key),
        FINGERPRINT_CONTEXT,
    );
    mac.as_ref()[..16]
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

/// The keys an open may use. The active key is the one new databases are
/// created under and a rekey moves to. A ring with a `key_for` callback
/// (`resolvable`) may lack keys, the active one too: asking for one it lacks
/// gives [`S3Error::KeyNeeded`], and the caller adds it and tries again.
#[derive(Clone, Debug)]
pub struct KeyRing {
    keys: Vec<Key>,
    active: String,
    resolvable: bool,
}

impl KeyRing {
    /// Ids are unique, and so is key material. Without `active`, the first
    /// key is. An active key the ring lacks is refused when it is needed
    /// ([`Self::active`]): a ring whose `key_for` had no key for an id still
    /// gets the error that names that id.
    pub fn new(keys: Vec<Key>, active: Option<String>, resolvable: bool) -> Result<Self> {
        for (i, key) in keys.iter().enumerate() {
            for other in &keys[..i] {
                if other.id == key.id {
                    return Err(S3Error::Config(format!(
                        "key id {} appears twice in the ring",
                        shown(&key.id)
                    )));
                }
                if other.fingerprint == key.fingerprint {
                    return Err(S3Error::Config(format!(
                        "keys {} and {} in the ring are the same key",
                        shown(&other.id),
                        shown(&key.id)
                    )));
                }
            }
        }
        let active = match active {
            Some(id) => {
                check_id(&id)?;
                id
            }
            None => keys
                .first()
                .map(|key| key.id.clone())
                .ok_or_else(|| S3Error::Config("the key ring is empty".into()))?,
        };
        Ok(Self {
            keys,
            active,
            resolvable,
        })
    }

    /// A ring of one key.
    pub fn single(key: Key) -> Self {
        Self {
            active: key.id.clone(),
            keys: vec![key],
            resolvable: false,
        }
    }

    pub fn active_id(&self) -> &str {
        &self.active
    }

    /// The active key.
    pub fn active(&self) -> Result<&Key> {
        self.need(&self.active.clone(), |ring| {
            format!(
                "the active key {} is not in the ring (ring: {ring})",
                shown(&self.active)
            )
        })
    }

    pub fn get(&self, id: &str) -> Option<&Key> {
        self.keys.iter().find(|key| key.id == id)
    }

    /// The ring's ids, for messages: `r2, r1`.
    pub fn ids(&self) -> String {
        if self.keys.is_empty() {
            return "empty".into();
        }
        self.keys
            .iter()
            .map(|key| shown(&key.id))
            .collect::<Vec<_>>()
            .join(", ")
    }

    /// The key `id`, or `KeyNeeded` for a resolvable ring, or the error
    /// `missing` describes (given the ring's ids).
    pub fn need(&self, id: &str, missing: impl FnOnce(&str) -> String) -> Result<&Key> {
        match self.get(id) {
            Some(key) => Ok(key),
            None if self.resolvable => Err(S3Error::KeyNeeded(id.to_string())),
            None => Err(S3Error::Config(missing(&self.ids()))),
        }
    }

    /// The key `record` names, checked against its fingerprint.
    pub fn resolve(&self, record: &KeyRecord) -> Result<&Key> {
        let key = self.need(&record.id, |ring| {
            format!("unknown key id {} (ring: {ring})", shown(&record.id))
        })?;
        check_matches(key, record)?;
        Ok(key)
    }

    /// Like [`Self::resolve`], for a restore of an epoch encrypted with
    /// `record`'s key.
    pub fn resolve_for_restore(&self, record: &KeyRecord) -> Result<&Key> {
        let key = self.need(&record.id, |_| {
            format!("needs key {}, which is not in the ring", shown(&record.id))
        })?;
        check_matches(key, record)?;
        Ok(key)
    }

    /// Adds keys (resolved by the caller), keeping the ids and the material
    /// unique.
    pub fn with(mut self, more: Vec<Key>) -> Result<Self> {
        self.keys.extend(more);
        Self::new(self.keys, Some(self.active), self.resolvable)
    }
}

/// A fingerprint as messages and results show it: its first 16 hex digits
/// (64 bits, enough to tell keys apart), so no message holds a run of 32
/// hex digits, which the net ([`crate::redact`]) would take for a key.
pub fn short_fingerprint(fingerprint: &str) -> &str {
    fingerprint.get(..16).unwrap_or(fingerprint)
}

/// The ring's key under the recorded id must be the recorded key.
pub fn check_matches(key: &Key, record: &KeyRecord) -> Result<()> {
    if key.fingerprint != record.fingerprint {
        return Err(S3Error::Config(format!(
            "key {} in the ring is not the key that encrypted this database (fingerprint {}, \
             recorded {})",
            shown(&record.id),
            short_fingerprint(&key.fingerprint),
            short_fingerprint(&record.fingerprint)
        )));
    }
    if key.cipher != record.cipher {
        return Err(S3Error::Config(format!(
            "key {} in the ring is for {}, but this database was encrypted with {}",
            shown(&record.id),
            key.cipher,
            record.cipher
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    const A: &str = "abababababababababababababababababababababababababababababababab";
    const B: &str = "cdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcd";

    fn err(result: Result<impl std::fmt::Debug>) -> String {
        let message = result.unwrap_err().to_string();
        for secret in [A, B, &A[..16], &B[..16]] {
            assert!(!message.contains(secret), "{message}");
        }
        message
    }

    /// Every way a mistyped ring has put a key in a message (Opus and Codex
    /// reviews): the message never holds the key, or any 16 hex digits of it.
    #[test]
    fn a_mistyped_ring_never_shows_a_key() {
        let k128 = &A[..32];
        let uuid = &B[..32];
        let ring = |keys: Vec<Key>, active: Option<&str>| {
            KeyRing::new(keys, active.map(str::to_string), false).map(|_| ())
        };
        let cases: Vec<(&str, &str, Result<()>)> = vec![
            (
                "{key, r2}, aegis256",
                A,
                Key::new(A, "aegis256", "r2").map(|_| ()),
            ),
            (
                "{key, r2}, aes128gcm",
                k128,
                Key::new(k128, "aes128gcm", "r2").map(|_| ()),
            ),
            // Both fields are 32 hex digits: the key is a valid-looking id,
            // the uuid a valid-looking 16-byte key.
            (
                "{key, uuid}, aes128gcm",
                k128,
                Key::new(k128, "aes128gcm", uuid).map(|_| ()),
            ),
            (
                "{key, uuid}, aegis256",
                A,
                Key::new(A, "aegis256", uuid).map(|_| ()),
            ),
            (
                "cipher: key, key: cipher",
                A,
                Key::new("default", A, "aegis256").map(|_| ()),
            ),
            (
                "cipher: key128, key: cipher",
                k128,
                Key::new("default", k128, "aes128gcm").map(|_| ()),
            ),
            (
                "active: key",
                k128,
                ring(vec![Key::new("r1", "aes128gcm", k128).unwrap()], Some(k128)),
            ),
            (
                "active: key256",
                A,
                ring(vec![Key::new("r1", "aegis256", A).unwrap()], Some(A)),
            ),
            ("to:/release id = key128", k128, check_id(k128)),
            ("to:/release id = key256", A, check_id(A)),
        ];
        for (case, key, result) in cases {
            let message = result.expect_err(case).to_string();
            for i in 0..=key.len() - 16 {
                assert!(!message.contains(&key[i..i + 16]), "{case}: {message}");
            }
        }
        // A ring whose ids could be keys (not built by Key::new, which
        // refuses them) shows none of them.
        assert_eq!(shown(k128), "<an id that looks like a key>");
        assert_eq!(shown("r1"), "r1");
        // Refused ids are never echoed.
        for id in ["R1", "-r", &A[..40], &"r".repeat(33)] {
            let message = err(Key::new(id, "aegis256", B));
            assert!(!message.contains(id), "{message}");
        }
    }

    #[test]
    fn fingerprints_are_hmac_sha256_truncated_and_reveal_no_key() {
        let key = Key::new("r1", "aegis256", A).unwrap();
        assert_eq!(key.fingerprint().len(), 32);
        assert_eq!(
            key.fingerprint(),
            Key::new("r9", "aegis-256", &A.to_uppercase())
                .unwrap()
                .fingerprint()
        );
        assert_ne!(
            key.fingerprint(),
            Key::new("r1", "aegis256", B).unwrap().fingerprint()
        );
        assert!(!key.fingerprint().contains("abab"));
        assert_eq!(key.cipher(), "aegis256");
        assert!(!format!("{key:?}").contains("abab"));
    }

    #[test]
    fn rings_are_validated_strictly() {
        let a = || Key::new("r1", "aegis256", A).unwrap();
        let b = || Key::new("r2", "aegis256", B).unwrap();
        assert!(err(Key::new("R1", "aegis256", A)).contains("invalid key id"));
        assert!(err(Key::new("-r", "aegis256", A)).contains("invalid key id"));
        assert!(err(Key::new(&"r".repeat(33), "aegis256", A)).contains("invalid key id"));
        assert!(Key::new(&"r".repeat(32), "aegis256", A).is_ok());
        assert!(err(Key::new("r1", "rot13", A)).contains("unknown cipher"));
        assert!(err(Key::new("r1", "aegis256", &A[..62])).contains("32-byte key"));
        assert!(err(Key::new("r1", "aegis256", &format!("{}zz", &A[..62]))).contains("hex"));
        assert!(err(Key::new("r1", "aes128gcm", A)).contains("16-byte key"));
        assert!(err(KeyRing::new(vec![a(), a()], None, false)).contains("appears twice"));
        let same = Key::new("r3", "aegis256", A).unwrap();
        assert!(err(KeyRing::new(vec![a(), same], None, false)).contains("are the same key"));
        assert!(err(KeyRing::new(vec![], None, false)).contains("empty"));
        let lacking = KeyRing::new(vec![a()], Some("r2".into()), false).unwrap();
        assert!(err(lacking.active()).contains("the active key r2 is not in the ring (ring: r1)"));
        let ring = KeyRing::new(vec![b(), a()], None, false).unwrap();
        assert_eq!(ring.active().unwrap().id(), "r2");
        assert_eq!(ring.ids(), "r2, r1");
    }

    #[test]
    fn resolving_names_the_key() {
        let a = Key::new("r1", "aegis256", A).unwrap();
        let b = Key::new("r2", "aegis256", B).unwrap();
        let ring = KeyRing::new(vec![b.clone()], None, false).unwrap();
        assert_eq!(
            err(ring.resolve(&a.record())),
            "s3 config: unknown key id r1 (ring: r2)"
        );
        assert_eq!(
            err(ring.resolve_for_restore(&a.record())),
            "s3 config: needs key r1, which is not in the ring"
        );
        // Two ids swapped: the ring's r1 is the database's r2.
        let swapped = KeyRing::new(
            vec![
                Key::new("r1", "aegis256", B).unwrap(),
                Key::new("r2", "aegis256", A).unwrap(),
            ],
            None,
            false,
        )
        .unwrap();
        let message = err(swapped.resolve(&a.record()));
        assert!(message.contains("key r1 in the ring is not the key that encrypted this database"));
        assert!(
            message.contains(short_fingerprint(a.fingerprint()))
                && message.contains(short_fingerprint(b.fingerprint()))
                && !message.contains(a.fingerprint()),
            "{message}"
        );
        let lazy = KeyRing::new(vec![], Some("r2".into()), true).unwrap();
        assert!(matches!(lazy.active(), Err(S3Error::KeyNeeded(id)) if id == "r2"));
        assert!(matches!(lazy.resolve(&a.record()), Err(S3Error::KeyNeeded(id)) if id == "r1"));
        let lazy = lazy.with(vec![a.clone()]).unwrap();
        assert!(lazy.resolve(&a.record()).unwrap().same(&a));
        assert!(err(lazy.with(vec![Key::new("r5", "aegis256", A).unwrap()])).contains("same key"));
    }
}
