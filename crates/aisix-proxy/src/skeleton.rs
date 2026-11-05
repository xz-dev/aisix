//! large-request-capacity D2 ingest: a streaming tokenizer that moves the
//! large media strings of a JSON body into a spool and leaves the rest —
//! the skeleton — in memory for serde.
//!
//! A string value is spooled when it reaches the threshold and either sits
//! at an allow-listed key (`image_url`, `file_data`, `image_url.url`,
//! `source.data`) or is a `data:` URL. Its decoded bytes go to the spool,
//! validated exactly as serde would validate them, and the skeleton gets a
//! fixed-length marker string instead. Every other byte is copied
//! verbatim, so serde's parse of the skeleton decides everything else
//! (structure, numbers, depth, duplicate keys) as it does today.

use crate::body_spool::SpoolFile;
use crate::capacity::CapacityError;

/// Marker prefix; the full marker is `aisix-spool-<32 hex nonce>-<8 hex index>`.
const MARKER_PREFIX: &str = "aisix-spool-";
/// Nesting beyond this is far past serde's own limit: stop tracking and
/// copy the rest verbatim, for serde to reject.
const MAX_TRACKED_DEPTH: usize = 512;

/// One spooled string: its decoded bytes at `off..off + len` in the spool,
/// and the length serde's escaping gives them.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Blob {
    pub off: u64,
    pub len: u64,
    pub escaped: u64,
}

/// Why ingest stopped. `Invalid` is the same 400 serde's rejection gives.
#[derive(Debug)]
pub(crate) enum IngestError {
    Invalid,
    Capacity(CapacityError),
}

impl From<CapacityError> for IngestError {
    fn from(e: CapacityError) -> Self {
        Self::Capacity(e)
    }
}

/// Bytes serde's string escaping turns `b` into.
pub(crate) fn escaped_len(b: u8) -> u64 {
    match b {
        b'"' | b'\\' | 0x08 | 0x09 | 0x0a | 0x0c | 0x0d => 2,
        0..=0x1f => 6,
        _ => 1,
    }
}

/// serde_json's string escaping.
pub(crate) fn escape_into(out: &mut Vec<u8>, bytes: &[u8]) {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    for &b in bytes {
        match b {
            b'"' => out.extend_from_slice(b"\\\""),
            b'\\' => out.extend_from_slice(b"\\\\"),
            0x08 => out.extend_from_slice(b"\\b"),
            0x09 => out.extend_from_slice(b"\\t"),
            0x0a => out.extend_from_slice(b"\\n"),
            0x0c => out.extend_from_slice(b"\\f"),
            0x0d => out.extend_from_slice(b"\\r"),
            0..=0x1f => out.extend_from_slice(&[
                b'\\',
                b'u',
                b'0',
                b'0',
                HEX[(b >> 4) as usize],
                HEX[(b & 15) as usize],
            ]),
            _ => out.push(b),
        }
    }
}

#[derive(Debug)]
struct Frame {
    object: bool,
    expect_key: bool,
    /// The key under which this container sits in its parent object.
    name: Vec<u8>,
    /// The current key, while its value is read.
    key: Vec<u8>,
}

/// Escape-decoder state inside a spooled string.
#[derive(Debug, Clone, Copy, PartialEq)]
enum Esc {
    None,
    Backslash,
    /// `\u` read; `n` hex digits so far; `hi` a pending high surrogate.
    Hex {
        n: u8,
        val: u16,
        hi: Option<u16>,
    },
    /// A high surrogate was decoded and must be followed by `\u`.
    WantBackslash(u16),
    WantU(u16),
}

#[derive(Debug)]
enum Str {
    /// Outside any string.
    Out,
    /// In a key or an inline value: copied verbatim. `esc` = last byte was `\`.
    Copy { key: bool, esc: bool },
    /// In a value still under the threshold: its raw bytes are held.
    Held { esc: bool },
    /// In a spooled value.
    Spooled { esc: Esc, start: u64, escaped: u64 },
}

/// The streaming tokenizer. Feed the body with [`Tokenizer::feed`] and
/// close it with [`Tokenizer::finish`].
pub(crate) struct Tokenizer {
    threshold: usize,
    nonce: String,
    pub skeleton: Vec<u8>,
    pub blobs: Vec<Blob>,
    stack: Vec<Frame>,
    str: Str,
    held: Vec<u8>,
    decoded: Vec<u8>,
    utf8_carry: Vec<u8>,
    verbatim: bool,
    spool: Option<SpoolFile>,
    new_spool: Box<dyn FnMut() -> Result<SpoolFile, CapacityError> + Send>,
}

impl Tokenizer {
    /// `new_spool` opens the spool on the first spooled string, so a body
    /// without one never touches the disk.
    pub(crate) fn new(
        threshold: usize,
        new_spool: Box<dyn FnMut() -> Result<SpoolFile, CapacityError> + Send>,
    ) -> Self {
        Self {
            // At least `data:`, which the decision reads.
            threshold: threshold.max(5),
            nonce: format!("{:032x}", rand::random::<u128>()),
            skeleton: Vec::new(),
            blobs: Vec::new(),
            stack: Vec::new(),
            str: Str::Out,
            held: Vec::new(),
            decoded: Vec::new(),
            utf8_carry: Vec::new(),
            verbatim: false,
            spool: None,
            new_spool,
        }
    }

    pub(crate) fn nonce(&self) -> &str {
        &self.nonce
    }

    /// The spool while ingest runs, for the caller to drain between pieces.
    pub(crate) fn spool_mut(&mut self) -> Option<&mut SpoolFile> {
        self.spool.as_mut()
    }

    /// The spool, once ingest is finished; `None` when nothing was spooled.
    pub(crate) fn take_spool(&mut self) -> Option<SpoolFile> {
        self.spool.take()
    }

    fn marker(&self, idx: usize) -> String {
        format!("{MARKER_PREFIX}{}-{idx:08x}", self.nonce)
    }

    /// Whether the value now being read may be spooled by its key alone.
    fn allow_listed(&self) -> bool {
        let Some(f) = self.stack.last() else {
            return false;
        };
        match f.key.as_slice() {
            b"image_url" | b"file_data" => true,
            b"url" => f.name == b"image_url",
            b"data" => f.name == b"source",
            _ => false,
        }
    }
}

fn hex(b: u8) -> Option<u16> {
    (b as char).to_digit(16).map(|d| d as u16)
}

fn push_char(out: &mut Vec<u8>, c: u32) -> Result<(), ()> {
    let c = char::from_u32(c).ok_or(())?;
    let mut buf = [0u8; 4];
    out.extend_from_slice(c.encode_utf8(&mut buf).as_bytes());
    Ok(())
}

/// Decode the raw bytes of a JSON string as serde does, appending to
/// `out`. Returns the index of the closing quote, if `bytes` holds it.
fn decode(esc: &mut Esc, out: &mut Vec<u8>, bytes: &[u8]) -> Result<Option<usize>, ()> {
    let mut k = 0;
    while k < bytes.len() {
        let b = bytes[k];
        match *esc {
            Esc::None => {
                let run = bytes[k..]
                    .iter()
                    .position(|&b| b == b'"' || b == b'\\' || b < 0x20)
                    .unwrap_or(bytes.len() - k);
                out.extend_from_slice(&bytes[k..k + run]);
                k += run;
                match bytes.get(k) {
                    Some(b'"') => return Ok(Some(k)),
                    Some(b'\\') => *esc = Esc::Backslash,
                    Some(_) => return Err(()),
                    None => return Ok(None),
                }
            }
            Esc::Backslash => {
                *esc = Esc::None;
                match b {
                    b'"' | b'\\' | b'/' => out.push(b),
                    b'b' => out.push(0x08),
                    b'f' => out.push(0x0c),
                    b'n' => out.push(0x0a),
                    b'r' => out.push(0x0d),
                    b't' => out.push(0x09),
                    b'u' => {
                        *esc = Esc::Hex {
                            n: 0,
                            val: 0,
                            hi: None,
                        }
                    }
                    _ => return Err(()),
                }
            }
            Esc::Hex { n, val, hi } => {
                let val = val * 16 + hex(b).ok_or(())?;
                if n < 3 {
                    *esc = Esc::Hex { n: n + 1, val, hi };
                } else {
                    *esc = Esc::None;
                    match (hi, val) {
                        (None, 0xD800..=0xDBFF) => *esc = Esc::WantBackslash(val),
                        (None, 0xDC00..=0xDFFF) => return Err(()),
                        (None, _) => push_char(out, u32::from(val))?,
                        (Some(h), 0xDC00..=0xDFFF) => {
                            let c = 0x10000
                                + ((u32::from(h) - 0xD800) << 10)
                                + (u32::from(val) - 0xDC00);
                            push_char(out, c)?;
                        }
                        (Some(_), _) => return Err(()),
                    }
                }
            }
            Esc::WantBackslash(h) if b == b'\\' => *esc = Esc::WantU(h),
            Esc::WantU(h) if b == b'u' => {
                *esc = Esc::Hex {
                    n: 0,
                    val: 0,
                    hi: Some(h),
                }
            }
            Esc::WantBackslash(_) | Esc::WantU(_) => return Err(()),
        }
        k += 1;
    }
    Ok(None)
}

/// Index of the closing quote of a verbatim string, tracking `\` escapes.
fn string_end(esc: &mut bool, bytes: &[u8]) -> Option<usize> {
    for (k, &b) in bytes.iter().enumerate() {
        if *esc {
            *esc = false;
        } else if b == b'\\' {
            *esc = true;
        } else if b == b'"' {
            return Some(k);
        }
    }
    None
}

impl Tokenizer {
    /// Feed the next body bytes.
    pub(crate) fn feed(&mut self, chunk: &[u8]) -> Result<(), IngestError> {
        let mut i = 0;
        while i < chunk.len() {
            if self.verbatim {
                self.skeleton.extend_from_slice(&chunk[i..]);
                return Ok(());
            }
            let rest = &chunk[i..];
            match std::mem::replace(&mut self.str, Str::Out) {
                Str::Out => {
                    i += 1;
                    self.structural(rest[0]);
                }
                Str::Copy { key, mut esc } => match string_end(&mut esc, rest) {
                    Some(end) => {
                        self.copy_string_bytes(key, &rest[..end]);
                        self.skeleton.push(b'"');
                        i += end + 1;
                    }
                    None => {
                        self.copy_string_bytes(key, rest);
                        self.str = Str::Copy { key, esc };
                        i = chunk.len();
                    }
                },
                Str::Held { mut esc } => {
                    let room = self.threshold - self.held.len();
                    let window = &rest[..rest.len().min(room)];
                    match string_end(&mut esc, window) {
                        Some(end) => {
                            self.skeleton.push(b'"');
                            self.skeleton.extend_from_slice(&self.held);
                            self.skeleton.extend_from_slice(&window[..=end]);
                            self.held.clear();
                            i += end + 1;
                        }
                        None => {
                            self.held.extend_from_slice(window);
                            i += window.len();
                            if self.held.len() < self.threshold {
                                self.str = Str::Held { esc };
                            } else if self.allow_listed() || self.held.starts_with(b"data:") {
                                self.start_spooled()?;
                            } else {
                                self.skeleton.push(b'"');
                                self.skeleton.append(&mut self.held);
                                self.str = Str::Copy { key: false, esc };
                            }
                        }
                    }
                }
                Str::Spooled {
                    mut esc,
                    start,
                    escaped,
                } => {
                    let end = decode(&mut esc, &mut self.decoded, rest)
                        .map_err(|()| IngestError::Invalid)?;
                    self.str = Str::Spooled {
                        esc,
                        start,
                        escaped,
                    };
                    match end {
                        Some(end) => {
                            self.end_spooled()?;
                            i += end + 1;
                        }
                        None => {
                            self.flush_decoded(false)?;
                            i = chunk.len();
                        }
                    }
                }
            }
        }
        Ok(())
    }

    fn copy_string_bytes(&mut self, key: bool, bytes: &[u8]) {
        self.skeleton.extend_from_slice(bytes);
        if key {
            if let Some(f) = self.stack.last_mut() {
                // Allow-listed keys are short; a longer one never matches.
                let room = 33usize.saturating_sub(f.key.len());
                f.key.extend_from_slice(&bytes[..bytes.len().min(room)]);
            }
        }
    }

    fn structural(&mut self, b: u8) {
        match b {
            b'"' => {
                if self.stack.last().is_some_and(|f| f.object && f.expect_key) {
                    self.stack.last_mut().unwrap().key.clear();
                    self.skeleton.push(b);
                    self.str = Str::Copy {
                        key: true,
                        esc: false,
                    };
                } else {
                    self.str = Str::Held { esc: false };
                }
                return;
            }
            b'{' | b'[' => {
                if self.stack.len() >= MAX_TRACKED_DEPTH {
                    self.verbatim = true;
                } else {
                    let name = self.stack.last().map(|f| f.key.clone()).unwrap_or_default();
                    self.stack.push(Frame {
                        object: b == b'{',
                        expect_key: true,
                        name,
                        key: Vec::new(),
                    });
                }
            }
            b'}' | b']' => {
                self.stack.pop();
            }
            b':' | b',' => {
                if let Some(f) = self.stack.last_mut().filter(|f| f.object) {
                    f.expect_key = b == b',';
                }
            }
            _ => {}
        }
        self.skeleton.push(b);
    }

    fn start_spooled(&mut self) -> Result<(), IngestError> {
        if self.spool.is_none() {
            self.spool = Some((self.new_spool)()?);
        }
        let start = self.spool.as_ref().map_or(0, SpoolFile::len);
        let mut esc = Esc::None;
        let held = std::mem::take(&mut self.held);
        // `held` holds no closing quote: the threshold came first.
        decode(&mut esc, &mut self.decoded, &held).map_err(|()| IngestError::Invalid)?;
        self.str = Str::Spooled {
            esc,
            start,
            escaped: 0,
        };
        self.flush_decoded(false)
    }

    /// Validate the decoded bytes as UTF-8, count their escaped length and
    /// write them to the spool; an incomplete trailing character waits.
    fn flush_decoded(&mut self, last: bool) -> Result<(), IngestError> {
        let mut data = std::mem::take(&mut self.utf8_carry);
        data.append(&mut self.decoded);
        let valid = match std::str::from_utf8(&data) {
            Ok(_) => data.len(),
            Err(e) if e.error_len().is_none() && !last => e.valid_up_to(),
            Err(_) => return Err(IngestError::Invalid),
        };
        let add: u64 = data[..valid].iter().map(|&b| escaped_len(b)).sum();
        if let Str::Spooled { escaped, .. } = &mut self.str {
            *escaped += add;
        }
        let spool = self.spool.as_mut().expect("spooled string without a spool");
        spool.append(&data[..valid]);
        self.utf8_carry = data.split_off(valid);
        self.decoded = data;
        self.decoded.clear();
        Ok(())
    }

    fn end_spooled(&mut self) -> Result<(), IngestError> {
        self.flush_decoded(true)?;
        let Str::Spooled { start, escaped, .. } = std::mem::replace(&mut self.str, Str::Out) else {
            unreachable!("end_spooled outside a spooled string");
        };
        let len = self.spool.as_ref().map_or(0, SpoolFile::len) - start;
        let marker = self.marker(self.blobs.len());
        self.blobs.push(Blob {
            off: start,
            len,
            escaped,
        });
        self.skeleton.push(b'"');
        self.skeleton.extend_from_slice(marker.as_bytes());
        self.skeleton.push(b'"');
        Ok(())
    }

    /// The body ended. A string left open is serde's to reject, as is any
    /// unclosed structure; a body that already held this request's nonce
    /// fails closed.
    pub(crate) fn finish(&mut self) -> Result<(), IngestError> {
        match std::mem::replace(&mut self.str, Str::Out) {
            Str::Held { .. } => {
                self.skeleton.push(b'"');
                self.skeleton.append(&mut self.held);
            }
            Str::Spooled { .. } => return Err(IngestError::Invalid),
            Str::Out | Str::Copy { .. } => {}
        }
        let nonce = self.nonce.as_bytes();
        let seen = self
            .skeleton
            .windows(nonce.len())
            .filter(|w| *w == nonce)
            .count();
        if seen != self.blobs.len() {
            return Err(CapacityError::marker("nonce already present in the request body").into());
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "skeleton_tests.rs"]
mod tests;
