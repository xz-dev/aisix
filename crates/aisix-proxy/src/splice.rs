//! large-request-capacity D2 egress: each attempt's serialized skeleton is
//! sent with every marker replaced by its blob, read from the spool with
//! positional reads and escaped the way serde writes strings. The bytes
//! and `Content-Length` equal what serializing the full body would give.

use std::sync::Arc;

use bytes::Bytes;
use serde_json::Value;

use crate::body_spool::SpoolFile;
use crate::capacity::CapacityError;
use crate::skeleton::{escape_into, Blob};
use crate::stream_timeout::ProgressBody;

const MARKER_PREFIX: &str = "aisix-spool-";
/// `aisix-spool-` + 32 hex nonce + `-` + 8 hex index.
const MARKER_LEN: usize = 12 + 32 + 1 + 8;
/// Bytes per spool read and per inline piece handed to the transport.
const PIECE: usize = 256 << 10;

/// One request's spooled strings, shared by every attempt.
#[derive(Debug)]
pub(crate) struct BlobStore {
    pub nonce: String,
    pub blobs: Vec<Blob>,
    pub spool: SpoolFile,
    /// The ticket lease, held as long as the spool is (D3).
    pub _lease: Option<Arc<crate::capacity::Lease>>,
}

/// One attempt's outbound body: inline pieces of the serialized skeleton,
/// each followed by the blob that replaces the next marker.
pub(crate) struct Plan {
    wire: Bytes,
    segments: Vec<(std::ops::Range<usize>, Option<Blob>)>,
    pub len: u64,
}

impl BlobStore {
    fn blob_of(&self, marker: &[u8]) -> Option<Blob> {
        let rest = marker.strip_prefix(MARKER_PREFIX.as_bytes())?;
        let rest = rest
            .strip_prefix(self.nonce.as_bytes())?
            .strip_prefix(b"-")?;
        let idx = std::str::from_utf8(rest).ok()?;
        if idx.len() != 8 {
            return None;
        }
        self.blobs
            .get(usize::from_str_radix(idx, 16).ok()?)
            .copied()
    }

    /// Plan the splice of `wire`. Fails closed, sending nothing, when a
    /// marker does not resolve or the nonce appears outside a whole marker
    /// string (an edit cut or merged it).
    pub(crate) fn plan(&self, wire: Bytes) -> Result<Plan, CapacityError> {
        let needle = format!("\"{MARKER_PREFIX}{}-", self.nonce);
        let needle = needle.as_bytes();
        let mut segments = Vec::new();
        let mut last = 0;
        let mut len = 0u64;
        let mut at = 0;
        while let Some(p) = find(&wire[at..], needle).map(|p| p + at) {
            let inner = p + 1..p + 1 + MARKER_LEN;
            let blob = wire
                .get(inner.clone())
                .filter(|_| wire.get(inner.end) == Some(&b'"'))
                .and_then(|m| self.blob_of(m))
                .ok_or_else(|| CapacityError::marker("left unresolved at egress"))?;
            segments.push((last..inner.start, Some(blob)));
            len += (inner.start - last) as u64 + blob.escaped;
            last = inner.end;
            at = inner.end;
        }
        segments.push((last..wire.len(), None));
        len += (wire.len() - last) as u64;
        let nonces = wire
            .windows(32)
            .filter(|w| *w == self.nonce.as_bytes())
            .count();
        if nonces != segments.len() - 1 {
            return Err(CapacityError::marker("left unresolved at egress"));
        }
        Ok(Plan {
            wire,
            segments,
            len,
        })
    }

    fn read(self: &Arc<Self>, blob: Blob, off: u64, n: usize) -> std::io::Result<Vec<u8>> {
        let mut buf = vec![0u8; n];
        self.spool.read_at(&mut buf, blob.off + off)?;
        Ok(buf)
    }

    /// Call `f` on each string of `body` that is one of this store's
    /// markers, in document order.
    fn each_marker(&self, v: &mut Value, f: &mut dyn FnMut(&mut String, Blob)) {
        match v {
            Value::String(s) if s.len() == MARKER_LEN => {
                if let Some(blob) = self.blob_of(s.as_bytes()) {
                    f(s, blob);
                }
            }
            Value::Array(items) => items.iter_mut().for_each(|v| self.each_marker(v, f)),
            Value::Object(map) => map.values_mut().for_each(|v| self.each_marker(v, f)),
            _ => {}
        }
    }

    /// Put every blob back into `body`, for a fallback path that needs the
    /// whole request in memory. The spool reads run on the blocking pool.
    pub(crate) async fn rehydrate(self: &Arc<Self>, body: &mut Value) -> Result<(), CapacityError> {
        let mut blobs = Vec::new();
        self.each_marker(body, &mut |_, blob| blobs.push(blob));
        if blobs.is_empty() {
            return Ok(());
        }
        let store = self.clone();
        let read = tokio::task::spawn_blocking(move || {
            blobs
                .into_iter()
                .map(|blob| store.read(blob, 0, blob.len as usize))
                .collect::<std::io::Result<Vec<_>>>()
        })
        .await
        .map_err(|e| CapacityError::spool_io(&std::io::Error::other(e)))?
        .map_err(|e| CapacityError::spool_io(&e))?;
        let mut read = read.into_iter();
        let mut bad = false;
        // Validated as UTF-8 when it was spooled.
        self.each_marker(body, &mut |s, _| match read.next().map(String::from_utf8) {
            Some(Ok(text)) => *s = text,
            _ => bad = true,
        });
        if bad {
            return Err(CapacityError::marker("blob is not UTF-8"));
        }
        Ok(())
    }
}

/// `body` serialized for content capture and logs: every marker-shaped
/// string becomes `[spooled <n> bytes]` (`[spooled]` when `store` does not
/// resolve it), so no nonce reaches a log sink (D2).
pub(crate) fn capture_json(body: &Value, store: Option<&BlobStore>) -> String {
    let s = serde_json::to_string(body).unwrap_or_default();
    if !s.contains(MARKER_PREFIX) {
        return s;
    }
    let hex = |b: &[u8]| b.iter().all(|c| matches!(c, b'0'..=b'9' | b'a'..=b'f'));
    let mut out = String::with_capacity(s.len());
    let mut rest = s.as_str();
    while let Some(p) = rest.find(MARKER_PREFIX) {
        out.push_str(&rest[..p]);
        let marker = rest
            .as_bytes()
            .get(p..p + MARKER_LEN)
            .filter(|m| hex(&m[12..44]) && m[44] == b'-' && hex(&m[45..]));
        let Some(marker) = marker else {
            out.push_str(MARKER_PREFIX);
            rest = &rest[p + MARKER_PREFIX.len()..];
            continue;
        };
        match store.and_then(|st| st.blob_of(marker)) {
            Some(blob) => out.push_str(&format!("[spooled {} bytes]", blob.len)),
            None => out.push_str("[spooled]"),
        }
        rest = &rest[p + MARKER_LEN..];
    }
    out.push_str(rest);
    out
}

fn find(hay: &[u8], needle: &[u8]) -> Option<usize> {
    hay.windows(needle.len()).position(|w| w == needle)
}

impl Plan {
    /// The attempt's body: inline pieces as slices of the skeleton, blobs
    /// read from the spool piece by piece off the runtime's threads.
    pub(crate) fn body(self, store: Arc<BlobStore>) -> ProgressBody {
        let Plan {
            wire,
            segments,
            len,
        } = self;
        let chunks = async_stream::stream! {
            for (range, blob) in segments {
                let mut i = range.start;
                while i < range.end {
                    let j = (i + PIECE).min(range.end);
                    yield Ok(wire.slice(i..j));
                    i = j;
                }
                let Some(blob) = blob else { continue };
                let mut off = 0u64;
                while off < blob.len {
                    let n = PIECE.min((blob.len - off) as usize);
                    let store = store.clone();
                    let read = tokio::task::spawn_blocking(move || store.read(blob, off, n)).await;
                    let raw = match read {
                        Ok(Ok(raw)) => raw,
                        Ok(Err(e)) => { yield Err(e); return; }
                        Err(e) => { yield Err(std::io::Error::other(e)); return; }
                    };
                    let mut out = Vec::with_capacity(n + n / 16);
                    escape_into(&mut out, &raw);
                    yield Ok(Bytes::from(out));
                    off += n as u64;
                }
            }
        };
        ProgressBody::new(Box::pin(chunks), len)
    }
}
