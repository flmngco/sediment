//! The net under every error string the NIFs return: a run of 32 or more
//! hex digits is replaced by `<redacted>`. Keys are hex (in the
//! `:encryption` option and from `key_for`), at least 32 digits long, so a
//! key echoed by any message, now or in a future one, never reaches Elixir.
//! Nothing Sediment shows legitimately is such a run: epoch ids and object
//! names have runs of at most 20 digits, and fingerprints are shown by
//! their first 16 hex digits ([`crate::s3::keys::short_fingerprint`]).

use std::borrow::Cow;

use rustler::{Atom, Encoder, Env, Term};

/// The shortest run of hex digits redacted: the hex of a 16-byte key.
const KEY_DIGITS: usize = 32;

/// `text` with every run of 32 or more hex digits replaced by `<redacted>`.
pub fn hex_runs(text: &str) -> Cow<'_, str> {
    let bytes = text.as_bytes();
    let mut out = String::new();
    let mut copied = 0;
    let mut i = 0;
    while i < bytes.len() {
        if !bytes[i].is_ascii_hexdigit() {
            i += 1;
            continue;
        }
        let start = i;
        while i < bytes.len() && bytes[i].is_ascii_hexdigit() {
            i += 1;
        }
        if i - start >= KEY_DIGITS {
            out.push_str(&text[copied..start]);
            out.push_str("<redacted>");
            copied = i;
        }
    }
    if copied == 0 {
        return Cow::Borrowed(text);
    }
    out.push_str(&text[copied..]);
    Cow::Owned(out)
}

/// What an `{:error, reason}` holds: text goes through [`hex_runs`].
pub trait Reason {
    fn encode_reason<'a>(&self, env: Env<'a>) -> Term<'a>;
}

impl Reason for str {
    fn encode_reason<'a>(&self, env: Env<'a>) -> Term<'a> {
        hex_runs(self).as_ref().encode(env)
    }
}

impl Reason for String {
    fn encode_reason<'a>(&self, env: Env<'a>) -> Term<'a> {
        self.as_str().encode_reason(env)
    }
}

impl Reason for Atom {
    fn encode_reason<'a>(&self, env: Env<'a>) -> Term<'a> {
        self.encode(env)
    }
}

impl<T: Reason> Reason for [T] {
    fn encode_reason<'a>(&self, env: Env<'a>) -> Term<'a> {
        self.iter()
            .map(|item| item.encode_reason(env))
            .collect::<Vec<_>>()
            .encode(env)
    }
}

impl<T: Reason + ?Sized> Reason for &T {
    fn encode_reason<'a>(&self, env: Env<'a>) -> Term<'a> {
        (**self).encode_reason(env)
    }
}

impl<T: Reason> Reason for Vec<T> {
    fn encode_reason<'a>(&self, env: Env<'a>) -> Term<'a> {
        self.as_slice().encode_reason(env)
    }
}

#[cfg(test)]
mod tests {
    use super::hex_runs;

    #[test]
    fn runs_of_32_hex_digits_or_more_are_redacted_and_nothing_else() {
        let k128 = "4d".repeat(16);
        let k256 = "1A".repeat(32);
        assert_eq!(hex_runs(&format!("key {k128}")), "key <redacted>");
        assert_eq!(
            hex_runs(&format!("{k256}, x{k128}y and {k256}")),
            "<redacted>, x<redacted>y and <redacted>"
        );
        // What messages show legitimately stays: epoch ids, object names,
        // owners, short fingerprints, 31 digits.
        for kept in [
            "log/00000000000000000003-0000000002/00000000000000000288",
            "lease held by host-1234-0123456789abcdef",
            "fingerprint 0123456789abcdef",
            &"a".repeat(31),
        ] {
            assert_eq!(hex_runs(kept), kept);
        }
    }
}
