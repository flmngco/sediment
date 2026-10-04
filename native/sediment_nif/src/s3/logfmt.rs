//! Verification of turso's MVCC logical-log format (LML2), as rebuilt from S3
//! segments. Mirrors `turso_core::mvcc::persistent_storage::logical_log`.

use bytes::Bytes;

use super::error::{Result, S3Error};
use super::layout::SEAL_MAGIC;

pub const LOG_HDR_SIZE: usize = 56;
const LOG_MAGIC: u32 = 0x4C4D_4C32;
const LOG_HDR_CRC_START: usize = 52;
const FRAME_MAGIC: u32 = 0x5854_564D;
const EXT_FRAME_MAGIC: u32 = 0x5845_564D;
const END_MAGIC: u32 = 0x4554_564D;
const TX_HEADER_SIZE: usize = 24;
const TX_TRAILER_SIZE: usize = 8;

/// End state of a verified log.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct LogState {
    pub len: u64,
    pub frames: usize,
    pub end_crc: Option<u32>,
    pub last_commit_ts: u64,
    /// The epoch ends with a seal object (it was closed by a checkpoint).
    pub sealed: bool,
}

fn u32_at(buf: &[u8], at: usize) -> u32 {
    u32::from_le_bytes(buf[at..at + 4].try_into().unwrap())
}

fn u64_at(buf: &[u8], at: usize) -> u64 {
    u64::from_le_bytes(buf[at..at + 8].try_into().unwrap())
}

const ENCRYPTED_CHUNK: usize = 32 * 1024;

/// On-disk size of an encrypted frame payload (turso's
/// `encrypted_payload_blob_size`): every chunk carries a tag and a nonce.
fn encrypted_size(plaintext: usize, tag: usize, nonce: usize) -> Result<usize> {
    let chunks = plaintext.div_ceil(ENCRYPTED_CHUNK);
    chunks
        .checked_mul(tag + nonce)
        .and_then(|overhead| overhead.checked_add(plaintext))
        .ok_or_else(|| S3Error::Corrupt("encrypted frame size overflow".into()))
}

/// Returns the CRC seed derived from a log header's salt.
pub fn header_seed(header: &[u8]) -> Result<u32> {
    if header.len() < LOG_HDR_SIZE {
        return Err(S3Error::Corrupt("log header truncated".into()));
    }
    if u32_at(header, 0) != LOG_MAGIC {
        return Err(S3Error::Corrupt("bad log header magic".into()));
    }
    if !matches!(header[4], 2 | 3) {
        return Err(S3Error::Corrupt(format!(
            "unsupported log version {}",
            header[4]
        )));
    }
    let mut copy = [0u8; LOG_HDR_SIZE];
    copy.copy_from_slice(&header[..LOG_HDR_SIZE]);
    copy[LOG_HDR_CRC_START..].fill(0);
    if crc32c::crc32c(&copy) != u32_at(header, LOG_HDR_CRC_START) {
        return Err(S3Error::Corrupt("log header checksum mismatch".into()));
    }
    Ok(crc32c::crc32c(&header[8..16]))
}

/// Checks that `segments` (sorted by offset) form one contiguous log starting
/// at offset 0, that every segment holds whole frames, and that the CRC chain
/// is unbroken from the header's salt to the last frame. A seal object may
/// only come last; it is not part of the log.
///
/// `start` is the state of a log already verified up to `start.len`
/// (`LogState::default()` for a fresh one).
///
/// `encryption` is the cipher's `(tag, nonce)` sizes for an encrypted log:
/// each frame's payload is then stored in 32 KiB chunks, each followed by a
/// tag and nonce, while the frame header keeps the plaintext size.
pub fn verify_segments_from(
    start: LogState,
    segments: &[(u64, Bytes)],
    encryption: Option<(usize, usize)>,
) -> Result<LogState> {
    let mut state = start;
    let mut running: Option<u32> = start.end_crc;
    for (offset, bytes) in segments {
        if state.sealed {
            return Err(S3Error::Corrupt(format!(
                "segment at {offset} follows the epoch seal"
            )));
        }
        if *offset != state.len {
            return Err(S3Error::Corrupt(format!(
                "log gap: expected segment at offset {}, found {}",
                state.len, offset
            )));
        }
        if bytes.starts_with(SEAL_MAGIC) {
            state.sealed = true;
            continue;
        }
        let mut pos = 0usize;
        if *offset == 0 {
            running = Some(header_seed(bytes)?);
            pos = LOG_HDR_SIZE;
        }
        let Some(mut crc) = running else {
            return Err(S3Error::Corrupt("log has no header".into()));
        };
        if pos == bytes.len() {
            return Err(S3Error::Corrupt(format!("empty segment at {offset}")));
        }
        while pos < bytes.len() {
            let rest = &bytes[pos..];
            if rest.len() < TX_HEADER_SIZE + TX_TRAILER_SIZE {
                return Err(S3Error::Corrupt(format!(
                    "truncated frame at {}",
                    offset + pos as u64
                )));
            }
            match u32_at(rest, 0) {
                FRAME_MAGIC => {}
                EXT_FRAME_MAGIC => {
                    return Err(S3Error::Corrupt(
                        "extension frames (portable changes) are not supported".into(),
                    ))
                }
                magic => {
                    return Err(S3Error::Corrupt(format!(
                        "bad frame magic {magic:#x} at {}",
                        offset + pos as u64
                    )))
                }
            }
            let plaintext = usize::try_from(u64_at(rest, 4))
                .map_err(|_| S3Error::Corrupt("frame payload too large".into()))?;
            let payload = match encryption {
                None => plaintext,
                Some((tag, nonce)) => encrypted_size(plaintext, tag, nonce)?,
            };
            let body = TX_HEADER_SIZE
                .checked_add(payload)
                .filter(|body| body + TX_TRAILER_SIZE <= rest.len())
                .ok_or_else(|| {
                    S3Error::Corrupt(format!(
                        "frame at {} overruns its segment",
                        offset + pos as u64
                    ))
                })?;
            crc = crc32c::crc32c_append(crc, &rest[..body]);
            if u32_at(rest, body) != crc || u32_at(rest, body + 4) != END_MAGIC {
                return Err(S3Error::Corrupt(format!(
                    "crc chain broken at frame {}",
                    offset + pos as u64
                )));
            }
            state.last_commit_ts = u64_at(rest, 16);
            state.frames += 1;
            pos += body + TX_TRAILER_SIZE;
        }
        running = Some(crc);
        state.len += bytes.len() as u64;
        state.end_crc = running;
    }
    Ok(state)
}
