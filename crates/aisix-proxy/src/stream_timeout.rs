//! Per-chunk read-timeout combinators for streaming upstreams (#554).
//!
//! Per-chunk streaming read-timeout: the deadline bounds the
//! wait for EACH chunk — the first one and every inter-chunk gap — and
//! resets after each successful read. A *first-chunk* timeout lets the
//! caller fail over before any bytes reach the client (issue AC2); a
//! *mid-stream* timeout terminates the stream like any other upstream
//! error, because once the `200` is committed a clean fallback is no
//! longer possible.
//!
//! Two flavours:
//! - [`with_read_timeout`] for the typed [`ChatChunkStream`] path
//!   (`/v1/chat/completions`, cross-provider `/v1/messages`): a read
//!   timeout surfaces as [`BridgeError::Timeout`], which the SSE pump
//!   already renders as an error frame.
//! - [`with_read_timeout_bytes`] for the raw byte passthroughs
//!   (`/v1/responses`, native-Anthropic `/v1/messages`): a read timeout
//!   simply ends the forwarded byte stream (the client sees a truncated
//!   response); there is no in-band error frame to inject into an opaque
//!   passthrough.
//!
//! [`send_with_deadline`] bounds the connect phase of a raw passthrough so
//! a slow upstream that never returns response headers also fails over.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use aisix_gateway::{BridgeError, ChatChunkStream};
use bytes::Bytes;
use futures::{Stream, StreamExt};

/// Wrap a [`ChatChunkStream`] so each `next()` is bounded by `per_chunk`.
/// On elapse, yield a single [`BridgeError::Timeout`] and end the stream.
/// `None` returns the stream unchanged (zero overhead on the hot path).
pub(crate) fn with_read_timeout(
    upstream: ChatChunkStream,
    per_chunk: Option<Duration>,
) -> ChatChunkStream {
    let Some(d) = per_chunk else {
        return upstream;
    };
    Box::pin(async_stream::stream! {
        // `ChatChunkStream` is a `Pin<Box<..>>`, hence `Unpin`; a plain
        // `mut` binding is enough to poll it via `StreamExt::next`.
        let mut upstream = upstream;
        loop {
            match tokio::time::timeout(d, upstream.next()).await {
                Ok(Some(item)) => yield item,
                Ok(None) => break,
                Err(_) => {
                    yield Err(BridgeError::Timeout {
                        elapsed_ms: d.as_millis() as u64,
                        cause: String::new(),
                    });
                    break;
                }
            }
        }
    })
}

/// Wrap a raw byte stream (`reqwest::Response::bytes_stream()`) so each
/// `next()` is bounded by `per_chunk`. On elapse, end the stream (the
/// forwarded client response is truncated). `None` returns a pass-through.
pub(crate) fn with_read_timeout_bytes<S>(
    upstream: S,
    per_chunk: Option<Duration>,
) -> impl Stream<Item = reqwest::Result<Bytes>> + Send
where
    S: Stream<Item = reqwest::Result<Bytes>> + Send + 'static,
{
    with_read_timeout_bytes_signalled(upstream, per_chunk, ReadTimeoutSignal::default())
}

/// Whether [`with_read_timeout_bytes_signalled`] ended its stream on a read
/// timeout. The truncated byte stream looks like a clean end to whoever
/// relays it, so this is how the relay's usage record learns the upstream
/// stalled.
#[derive(Clone, Default)]
pub(crate) struct ReadTimeoutSignal(Arc<AtomicU64>);

impl ReadTimeoutSignal {
    /// The elapsed read timeout, as the error it would have been on the
    /// typed path; `None` if the stream did not end on one.
    pub(crate) fn fired(&self) -> Option<BridgeError> {
        match self.0.load(Ordering::Relaxed) {
            0 => None,
            elapsed_ms => Some(BridgeError::Timeout {
                elapsed_ms,
                cause: String::new(),
            }),
        }
    }
}

/// [`with_read_timeout_bytes`], recording an elapsed read timeout on
/// `signal`.
pub(crate) fn with_read_timeout_bytes_signalled<S>(
    upstream: S,
    per_chunk: Option<Duration>,
    signal: ReadTimeoutSignal,
) -> impl Stream<Item = reqwest::Result<Bytes>> + Send
where
    S: Stream<Item = reqwest::Result<Bytes>> + Send + 'static,
{
    async_stream::stream! {
        let mut upstream = std::pin::pin!(upstream);
        loop {
            match per_chunk {
                Some(d) => match tokio::time::timeout(d, upstream.next()).await {
                    Ok(Some(item)) => yield item,
                    Ok(None) => break,
                    // Read timeout mid-passthrough: end the stream and record
                    // it on `signal`; the consumer decides how to close it.
                    Err(_) => {
                        signal.0.store((d.as_millis() as u64).max(1), Ordering::Relaxed);
                        break;
                    }
                },
                None => match upstream.next().await {
                    Some(item) => yield item,
                    None => break,
                },
            }
        }
    }
}

/// Send a raw-passthrough request, optionally bounding the connect phase
/// (everything up to and including response headers) by `deadline`. Maps
/// both reqwest's own timeout and the outer deadline to
/// [`BridgeError::Timeout`] so a slow connect fails over like the
/// Bridge-trait path. `started` anchors the reported elapsed time.
pub(crate) async fn send_with_deadline(
    req: reqwest::RequestBuilder,
    deadline: Option<Duration>,
    started: Instant,
) -> Result<reqwest::Response, BridgeError> {
    let sent = match deadline {
        Some(d) => match tokio::time::timeout(d, req.send()).await {
            Ok(res) => res.map_err(|e| crate::dispatch::reqwest_error_to_bridge(&e, started)),
            Err(_) => Err(BridgeError::Timeout {
                elapsed_ms: started.elapsed().as_millis() as u64,
                cause: String::new(),
            }),
        },
        None => req
            .send()
            .await
            .map_err(|e| crate::dispatch::reqwest_error_to_bridge(&e, started)),
    };
    if let Ok(resp) = &sent {
        aisix_gateway::note_upstream_response(resp.headers());
    }
    sent
}

/// Upload progress of one request body, shared between the body the
/// transport polls and the deadline that watches it. Times are ms since
/// `start`; `done_ms` is 0 until the last byte is handed to the transport
/// and then holds that time plus one.
struct UploadProgress {
    start: tokio::time::Instant,
    last_ms: AtomicU64,
    done_ms: AtomicU64,
    sent: AtomicU64,
}

impl UploadProgress {
    fn now_ms(&self) -> u64 {
        self.start.elapsed().as_millis() as u64
    }
}

/// Upload chunk size: the transport pulls the next chunk only after it has
/// written the previous ones, so a small chunk makes "pulled" track
/// "accepted by the upstream" closely.
const UPLOAD_CHUNK: usize = 64 * 1024;

/// The chunks of a [`ProgressBody`]; `Sync` because the transport needs it.
pub(crate) type BodyChunks =
    std::pin::Pin<Box<dyn Stream<Item = std::io::Result<Bytes>> + Send + Sync>>;

/// A request body of known length that records each chunk the transport
/// takes. Its exact size hint keeps `Content-Length` framing.
pub(crate) struct ProgressBody {
    chunks: BodyChunks,
    remaining: u64,
    progress: Arc<UploadProgress>,
}

impl ProgressBody {
    /// The body, as chunks of a buffer already in memory.
    pub(crate) fn from_bytes(body: Bytes) -> Self {
        let len = body.len() as u64;
        let chunks = (0..body.len())
            .step_by(UPLOAD_CHUNK)
            .map(move |i| Ok(body.slice(i..(i + UPLOAD_CHUNK).min(body.len()))))
            .collect::<Vec<_>>();
        Self::new(Box::pin(futures::stream::iter(chunks)), len)
    }

    /// A body of exactly `len` bytes read from `chunks`.
    pub(crate) fn new(chunks: BodyChunks, len: u64) -> Self {
        let progress = Arc::new(UploadProgress {
            start: tokio::time::Instant::now(),
            last_ms: AtomicU64::new(0),
            done_ms: AtomicU64::new(if len == 0 { 1 } else { 0 }),
            sent: AtomicU64::new(0),
        });
        Self {
            chunks,
            remaining: len,
            progress,
        }
    }
}

impl http_body::Body for ProgressBody {
    type Data = Bytes;
    type Error = std::io::Error;

    fn poll_frame(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Result<http_body::Frame<Bytes>, std::io::Error>>> {
        if self.remaining == 0 {
            return std::task::Poll::Ready(None);
        }
        let item = std::task::ready!(self.chunks.poll_next_unpin(cx));
        let chunk = match item {
            Some(Ok(c)) => c,
            Some(Err(e)) => return std::task::Poll::Ready(Some(Err(e))),
            None => {
                return std::task::Poll::Ready(Some(Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    format!("request body ended {} bytes short", self.remaining),
                ))))
            }
        };
        let n = chunk.len() as u64;
        if n > self.remaining {
            return std::task::Poll::Ready(Some(Err(std::io::Error::other(
                "request body longer than its Content-Length",
            ))));
        }
        self.remaining -= n;
        let p = &self.progress;
        let now = p.now_ms();
        p.sent.fetch_add(n, Ordering::Relaxed);
        p.last_ms.store(now, Ordering::Relaxed);
        if self.remaining == 0 {
            p.done_ms.store(now + 1, Ordering::Relaxed);
        }
        std::task::Poll::Ready(Some(Ok(http_body::Frame::data(chunk))))
    }

    fn is_end_stream(&self) -> bool {
        self.remaining == 0
    }

    fn size_hint(&self) -> http_body::SizeHint {
        http_body::SizeHint::with_exact(self.remaining)
    }
}

/// The D8 deadline of one send. While the body uploads it fires only when
/// the transport takes no body byte for `d`; once the last byte is taken,
/// `d` bounds the wait for the response headers.
async fn upload_deadline(
    p: Arc<UploadProgress>,
    d: Duration,
    total: u64,
    started: Instant,
) -> BridgeError {
    let d_ms = d.as_millis() as u64;
    loop {
        let done = p.done_ms.load(Ordering::Relaxed);
        if done > 0 {
            tokio::time::sleep_until(p.start + Duration::from_millis(done - 1) + d).await;
            return BridgeError::Timeout {
                elapsed_ms: started.elapsed().as_millis() as u64,
                cause: String::new(),
            };
        }
        let last = p.last_ms.load(Ordering::Relaxed);
        if p.now_ms() >= last + d_ms {
            return BridgeError::Timeout {
                elapsed_ms: started.elapsed().as_millis() as u64,
                cause: format!(
                    "request body upload made no progress for {d_ms}ms ({} of {total} bytes sent)",
                    p.sent.load(Ordering::Relaxed)
                ),
            };
        }
        tokio::time::sleep_until(p.start + Duration::from_millis(last) + d).await;
    }
}

/// Send a request whose body may take long to upload (large `/v1/responses`
/// bodies, D8). `deadline` (the stream budget) starts only after the last
/// body byte is handed to the transport and bounds the wait for response
/// headers; until then the same value bounds each gap with no upload
/// progress. `None` sends with no deadline. `Content-Length` is set from
/// the body.
pub(crate) async fn send_with_upload_deadline(
    req: reqwest::RequestBuilder,
    body: ProgressBody,
    deadline: Option<Duration>,
    started: Instant,
) -> Result<reqwest::Response, BridgeError> {
    let total = body.remaining;
    let progress = body.progress.clone();
    let req = req
        .header(reqwest::header::CONTENT_LENGTH, total)
        .body(reqwest::Body::wrap(body));
    let sent = match deadline {
        Some(d) => tokio::select! {
            res = req.send() => res.map_err(|e| crate::dispatch::reqwest_error_to_bridge(&e, started)),
            e = upload_deadline(progress, d, total, started) => Err(e),
        },
        None => req
            .send()
            .await
            .map_err(|e| crate::dispatch::reqwest_error_to_bridge(&e, started)),
    };
    if let Ok(resp) = &sent {
        aisix_gateway::note_upstream_response(resp.headers());
    }
    sent
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A stall past the per-chunk budget ends the byte stream exactly as it
    /// always did, and now also says so, carrying the budget it hit.
    #[tokio::test]
    async fn a_read_timeout_is_signalled() {
        let signal = ReadTimeoutSignal::default();
        let stalled = futures::stream::once(async { Ok::<_, reqwest::Error>(Bytes::from("a")) })
            .chain(futures::stream::pending());
        let out: Vec<_> = with_read_timeout_bytes_signalled(
            stalled,
            Some(Duration::from_millis(20)),
            signal.clone(),
        )
        .collect()
        .await;
        assert_eq!(out.len(), 1);
        match signal.fired() {
            Some(BridgeError::Timeout { elapsed_ms, .. }) => assert_eq!(elapsed_ms, 20),
            other => panic!("expected a timeout, got {other:?}"),
        }
    }

    /// A stream that ends on its own leaves the signal unset.
    #[tokio::test]
    async fn a_clean_end_is_not_a_read_timeout() {
        let signal = ReadTimeoutSignal::default();
        let done = futures::stream::iter([Ok::<_, reqwest::Error>(Bytes::from("a"))]);
        let _: Vec<_> =
            with_read_timeout_bytes_signalled(done, Some(Duration::from_secs(5)), signal.clone())
                .collect()
                .await;
        assert!(signal.fired().is_none());
    }

    /// A raw upstream with a 4 KiB receive buffer. It reads the request
    /// head, then the body in `step`-byte reads `gap` apart (`step` 0: it
    /// reads nothing), then waits `head_delay` and answers 200. Returns the
    /// address and the request head it saw.
    async fn upstream(
        step: usize,
        gap: Duration,
        head_delay: Duration,
    ) -> (std::net::SocketAddr, tokio::sync::oneshot::Receiver<String>) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let sock = tokio::net::TcpSocket::new_v4().unwrap();
        sock.set_recv_buffer_size(4096).unwrap();
        sock.bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let listener = sock.listen(1).unwrap();
        let addr = listener.local_addr().unwrap();
        let (tx, rx) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            let (mut s, _) = listener.accept().await.unwrap();
            let mut head = Vec::new();
            let mut byte = [0u8; 1];
            while !head.ends_with(b"\r\n\r\n") {
                s.read_exact(&mut byte).await.unwrap();
                head.push(byte[0]);
            }
            let head = String::from_utf8(head).unwrap();
            let len: usize = head
                .lines()
                .find_map(|l| {
                    l.to_ascii_lowercase()
                        .strip_prefix("content-length: ")
                        .map(str::to_string)
                })
                .map_or(0, |v| v.trim().parse().unwrap());
            let _ = tx.send(head);
            if step == 0 {
                tokio::time::sleep(Duration::from_secs(30)).await;
                return;
            }
            let mut buf = vec![0u8; step];
            let mut got = 0;
            while got < len {
                let n = s.read(&mut buf[..step.min(len - got)]).await.unwrap();
                assert!(n > 0, "body cut at {got} of {len}");
                got += n;
                tokio::time::sleep(gap).await;
            }
            tokio::time::sleep(head_delay).await;
            let _ = s
                .write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 2\r\n\r\nok")
                .await;
            tokio::time::sleep(Duration::from_secs(1)).await;
        });
        (addr, rx)
    }

    async fn send(
        addr: std::net::SocketAddr,
        len: usize,
        d: Duration,
    ) -> Result<reqwest::Response, BridgeError> {
        let req = reqwest::Client::new().post(format!("http://{addr}/"));
        let body = ProgressBody::from_bytes(Bytes::from(vec![b'a'; len]));
        send_with_upload_deadline(req, body, Some(d), Instant::now()).await
    }

    /// D8: a transport that keeps taking body bytes, slower than the
    /// deadline in total but faster than it per chunk, is not cut; the
    /// deadline starts when the last byte is taken.
    #[tokio::test(start_paused = true)]
    async fn slow_upload_is_not_cut_at_the_deadline() {
        use http_body_util::BodyExt;
        let d = Duration::from_millis(200);
        let mut body = ProgressBody::from_bytes(Bytes::from(vec![b'a'; 16 * UPLOAD_CHUNK]));
        let started = Instant::now();
        let deadline = tokio::spawn(upload_deadline(
            body.progress.clone(),
            d,
            body.remaining,
            started,
        ));
        for _ in 0..16 {
            tokio::time::sleep(Duration::from_millis(150)).await;
            assert!(!deadline.is_finished(), "cut mid-upload");
            body.frame().await.unwrap().unwrap();
        }
        let done = tokio::time::Instant::now();
        assert!(body.frame().await.is_none());
        match deadline.await.unwrap() {
            BridgeError::Timeout { cause, .. } => assert!(cause.is_empty(), "{cause}"),
            other => panic!("{other:?}"),
        }
        assert_eq!(done.elapsed(), d, "armed when the last byte was taken");
    }

    /// The body goes out whole, framed by `Content-Length`.
    #[tokio::test]
    async fn upload_is_framed_by_content_length() {
        let len = 4 << 20;
        let (addr, head) = upstream(256 << 10, Duration::ZERO, Duration::ZERO).await;
        let resp = send(addr, len, Duration::from_secs(5)).await.expect("sent");
        assert_eq!(resp.status(), 200);
        let head = head.await.unwrap().to_ascii_lowercase();
        assert!(
            head.contains(&format!("content-length: {len}\r\n")),
            "{head}"
        );
        assert!(!head.contains("transfer-encoding"), "{head}");
    }

    /// D8: an upstream that reads nothing gets an explicit timeout that
    /// names the stalled upload.
    #[tokio::test]
    async fn stalled_upload_times_out_explicitly() {
        let len = 16 << 20;
        let (addr, _head) = upstream(0, Duration::ZERO, Duration::ZERO).await;
        let started = Instant::now();
        match send(addr, len, Duration::from_millis(200)).await {
            Err(BridgeError::Timeout { cause, .. }) => {
                assert!(cause.contains("made no progress for 200ms"), "{cause}");
                assert!(!cause.contains(&format!("{len} of {len}")), "{cause}");
            }
            other => panic!("expected a timeout, got {other:?}"),
        }
        assert!(started.elapsed() < Duration::from_secs(5));
    }

    /// D8: once the body is written, the deadline bounds the wait for the
    /// response headers as before, with the unchanged message.
    #[tokio::test]
    async fn deadline_after_the_upload_bounds_the_headers() {
        let (addr, _head) = upstream(4096, Duration::ZERO, Duration::from_secs(5)).await;
        match send(addr, 1024, Duration::from_millis(200)).await {
            Err(e @ BridgeError::Timeout { .. }) => {
                assert!(
                    e.to_string()
                        .starts_with("upstream request timed out after"),
                    "{e}"
                );
                assert!(matches!(&e, BridgeError::Timeout { cause, .. } if cause.is_empty()));
            }
            other => panic!("expected a timeout, got {other:?}"),
        }
    }
}
