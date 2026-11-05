//! large-request-capacity task 6.2 through the real router: the spooled
//! `/v1/responses` path sends the upstream the bytes the buffered path
//! sends, and refuses explicitly where the spec says it must.

use std::os::unix::fs::PermissionsExt;
use std::sync::{Arc, Mutex};

use aisix_core::config::BodySpoolConfig;
use aisix_core::resource::ResourceEntry;
use aisix_core::snapshot::SnapshotHandle;
use aisix_core::{AisixSnapshot, ApiKey, Model, ProviderKey, ProxyConfig};
use aisix_gateway::Hub;
use aisix_provider_anthropic::AnthropicBridge;
use aisix_provider_openai::OpenAiBridge;
use aisix_proxy::spooled_json::BodySpool;
use aisix_proxy::{build_router, ProxyState};
use axum::body::{to_bytes, Body};
use axum::http::{Request, StatusCode};
use futures::StreamExt;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use tower::ServiceExt;

const MIB: usize = 1 << 20;
pub const TICKET: &str = "0123456789abcdef0123456789abcdef";

/// What the upstream saw of one request.
#[derive(Clone, Debug)]
pub struct Seen {
    pub path: String,
    pub len: usize,
    pub content_length: Option<String>,
    pub ticket: Option<String>,
    pub sha: Vec<u8>,
    /// The body, when under 8 MiB.
    pub body: Option<Vec<u8>>,
}

/// An upstream that records each request and answers it; the first
/// `fail_first` calls get a 500.
pub async fn upstream(fail_first: usize) -> (String, Arc<Mutex<Vec<Seen>>>) {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let fails = Arc::new(std::sync::atomic::AtomicUsize::new(fail_first));
    let app = axum::Router::new().fallback({
        let seen = seen.clone();
        move |req: Request<Body>| {
            let seen = seen.clone();
            let fails = fails.clone();
            async move {
                let path = req.uri().path().to_string();
                let (content_length, ticket) = {
                    let header = |n: &str| {
                        req.headers()
                            .get(n)
                            .map(|v| v.to_str().unwrap().to_string())
                    };
                    (header("content-length"), header("x-ai-gateway-capacity-ticket"))
                };
                let mut body = req.into_body().into_data_stream();
                let (mut sha, mut len, mut kept) = (Sha256::new(), 0, Vec::new());
                while let Some(c) = body.next().await {
                    let c = c.unwrap();
                    sha.update(&c);
                    len += c.len();
                    if len < 8 * MIB {
                        kept.extend_from_slice(&c);
                    }
                }
                seen.lock().unwrap().push(Seen {
                    path: path.clone(),
                    len,
                    content_length,
                    ticket,
                    sha: sha.finalize().to_vec(),
                    body: (len < 8 * MIB).then_some(kept),
                });
                use axum::response::IntoResponse;
                if fails
                    .fetch_update(std::sync::atomic::Ordering::SeqCst, std::sync::atomic::Ordering::SeqCst, |n| n.checked_sub(1))
                    .is_ok()
                {
                    return (StatusCode::INTERNAL_SERVER_ERROR, "boom").into_response();
                }
                if path.ends_with("/messages") {
                    return axum::Json(json!({
                        "id": "msg_1", "type": "message", "role": "assistant",
                        "model": "claude-sonnet-4-5", "content": [{"type": "text", "text": "ok"}],
                        "stop_reason": "end_turn", "usage": {"input_tokens": 2, "output_tokens": 1}
                    }))
                    .into_response();
                }
                axum::Json(json!({
                    "id": "resp_1", "object": "response", "status": "completed", "model": "gpt-4o",
                    "output": [], "usage": {"input_tokens": 2, "output_tokens": 1, "total_tokens": 3}
                }))
                .into_response()
            }
        }
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    (format!("http://{addr}"), seen)
}

pub fn private_dir() -> tempfile::TempDir {
    let d = tempfile::tempdir_in("/tmp").unwrap();
    std::fs::set_permissions(d.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    d
}

pub fn spool_config(dir: &std::path::Path) -> BodySpoolConfig {
    BodySpoolConfig {
        threshold_bytes: 64 * 1024,
        dir: Some(dir.display().to_string()),
        ..Default::default()
    }
}

/// A router on `api_base` with an openai model `m` (native) and an
/// anthropic model `c` (bridged). `spool` turns the body spool on, with
/// the memory charge reading `cgroup`.
pub fn router(
    api_base: &str,
    spool: Option<(BodySpoolConfig, &std::path::Path)>,
    retries: u32,
) -> axum::Router {
    router_with(api_base, spool, retries, |_| {})
}

/// [`router`], with `extra` adding resources to the snapshot.
pub fn router_with(
    api_base: &str,
    spool: Option<(BodySpoolConfig, &std::path::Path)>,
    retries: u32,
    extra: impl FnOnce(&AisixSnapshot),
) -> axum::Router {
    let snap = AisixSnapshot::new();
    for (id, provider) in [("pk-openai", "openai"), ("pk-anthropic", "anthropic")] {
        let pk: ProviderKey = serde_json::from_value(json!({
            "display_name": id, "secret": "sk-upstream", "api_base": api_base,
            "provider": provider, "adapter": provider
        }))
        .unwrap();
        snap.provider_keys.insert(ResourceEntry::new(id, pk, 1));
    }
    for (id, name, provider, upstream, pk) in [
        ("model-m", "m", "openai", "gpt-4o", "pk-openai"),
        (
            "model-c",
            "c",
            "anthropic",
            "claude-sonnet-4-5",
            "pk-anthropic",
        ),
    ] {
        let model: Model = serde_json::from_value(json!({
            "display_name": name, "provider": provider, "model_name": upstream, "provider_key_id": pk
        }))
        .unwrap();
        snap.models.insert(ResourceEntry::new(id, model, 1));
    }
    let key: ApiKey = serde_json::from_value(json!({
        "key_hash": ApiKey::hash_bearer("sk-caller"), "allowed_models": ["m", "c"]
    }))
    .unwrap();
    snap.apikeys.insert(ResourceEntry::new("caller", key, 1));
    extra(&snap);
    let hub = Arc::new(Hub::new());
    hub.register_specialized("openai", Arc::new(OpenAiBridge::new()));
    hub.register_specialized("anthropic", Arc::new(AnthropicBridge::new()));
    let cfg = ProxyConfig {
        addr: "127.0.0.1:0".into(),
        request_body_limit_bytes: 256 * MIB,
        real_ip: Default::default(),
        request_id: Default::default(),
        url_rewrites: Vec::new(),
        expose_debug_headers: false,
        tls: None,
        listeners: Vec::new(),
        thread_per_core: None,
        workers: None,
    };
    let spool = spool.map(|(c, cgroup)| {
        BodySpool::from_config(&c)
            .unwrap()
            .expect("spool on")
            .with_cgroup_dir(cgroup)
    });
    build_router(
        ProxyState::new(SnapshotHandle::new(snap), hub, &cfg)
            .without_cache()
            .with_default_retries(retries)
            .with_body_spool(spool),
    )
}

pub async fn send(
    app: &axum::Router,
    body: Body,
    headers: &[(&str, String)],
) -> (StatusCode, Value) {
    let mut req = Request::builder()
        .method("POST")
        .uri("/v1/responses")
        .header("authorization", "Bearer sk-caller")
        .header("content-type", "application/json");
    for (k, v) in headers {
        req = req.header(*k, v);
    }
    let resp = app.clone().oneshot(req.body(body).unwrap()).await.unwrap();
    let status = resp.status();
    let bytes = to_bytes(resp.into_body(), 16 * MIB).await.unwrap();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}

pub fn image_body(model: &str, image_bytes: usize, text: &str) -> String {
    let image = "QUJD".repeat(image_bytes / 4);
    json!({
        "model": model,
        "input": [{"role": "user", "content": [
            {"type": "input_text", "text": text},
            {"type": "input_image", "image_url": format!("data:image/png;base64,{image}")},
            {"type": "input_file", "file_data": format!("{image}\u{1}\"\\é😀")}
        ]}],
        "metadata": {"big": "BIGINT", "e": "EXP"}
    })
    .to_string()
    .replace("\"BIGINT\"", "123456789012345678901234567890")
    .replace("\"EXP\"", "1e3")
}

fn no_limit_cgroup() -> tempfile::TempDir {
    tempfile::tempdir().unwrap()
}

fn cgroup_with_headroom(headroom: u64) -> tempfile::TempDir {
    let d = tempfile::tempdir().unwrap();
    std::fs::write(d.path().join("memory.max"), (1u64 << 30).to_string()).unwrap();
    std::fs::write(
        d.path().join("memory.current"),
        ((1u64 << 30) - headroom).to_string(),
    )
    .unwrap();
    d
}

/// Differential: the buffered and the spooled path send identical bytes
/// and `Content-Length`, and every retry re-reads the spool.
#[tokio::test]
async fn spooled_path_sends_the_buffered_bytes_on_every_attempt() {
    let body = image_body(
        "m",
        3 * MIB,
        "text \" esc \\ \u{7} é 😀 data:not-media-at-text-key",
    );
    let (base, seen) = upstream(0).await;
    let (status, _) = send(&router(&base, None, 0), Body::from(body.clone()), &[]).await;
    assert_eq!(status, StatusCode::OK);
    let dir = private_dir();
    let cg = no_limit_cgroup();
    let (base2, seen2) = upstream(1).await;
    let app = router(&base2, Some((spool_config(dir.path()), cg.path())), 1);
    let (status, v) = send(&app, Body::from(body), &[]).await;
    assert_eq!(status, StatusCode::OK, "{v}");
    let legacy = seen.lock().unwrap()[0].clone();
    let attempts = seen2.lock().unwrap().clone();
    assert_eq!(attempts.len(), 2, "the 500 is retried");
    for a in &attempts {
        assert_eq!(a.content_length, legacy.content_length);
        assert_eq!(a.len, legacy.len);
        assert!(
            a.sha == legacy.sha,
            "spliced body differs from the buffered one"
        );
        assert!(a.ticket.is_none());
    }
    spools_closed(dir.path()).await;
}

#[tokio::test]
async fn text_above_the_inline_limit_gets_a_named_413() {
    let dir = private_dir();
    let cg = no_limit_cgroup();
    let (base, seen) = upstream(0).await;
    let app = router(&base, Some((spool_config(dir.path()), cg.path())), 0);
    let text = "t".repeat(17 * MIB);
    let body = json!({"model": "m", "input": text}).to_string();
    let (status, v) = send(&app, Body::from(body), &[]).await;
    assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE, "{v}");
    assert_eq!(v["error"]["code"], "request_too_large");
    assert_eq!(v["error"]["limit_bytes"], 16 * MIB);
    assert_eq!(v["error"]["hop"], "aisix");
    assert!(
        v["error"]["message"]
            .as_str()
            .unwrap()
            .contains("inline-bytes limit"),
        "{v}"
    );
    assert!(seen.lock().unwrap().is_empty());
}

#[tokio::test]
async fn bridged_target_is_a_fallback_path() {
    let dir = private_dir();
    let cg = no_limit_cgroup();
    let (base, seen) = upstream(0).await;
    let app = router(&base, Some((spool_config(dir.path()), cg.path())), 0);
    let (status, v) = send(&app, Body::from(image_body("c", 9 * MIB, "hi")), &[]).await;
    assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE, "{v}");
    assert_eq!(v["error"]["limit_bytes"], 16 * MIB);
    assert!(
        v["error"]["message"]
            .as_str()
            .unwrap()
            .contains("fallback limit of bridged target c"),
        "{v}"
    );
    assert!(seen.lock().unwrap().is_empty());
    // Under the limit the bridged target gets the whole request back.
    let image = format!("data:image/png;base64,{}", "QUJD".repeat(50_000));
    let small = json!({"model": "c", "max_output_tokens": 16, "input": [{"role": "user", "content": [
        {"type": "input_image", "image_url": image}]}]});
    let (status, v) = send(&app, Body::from(small.to_string()), &[]).await;
    assert_eq!(status, StatusCode::OK, "{v}");
    let sent = seen.lock().unwrap()[0].body.clone().unwrap();
    let sent = String::from_utf8_lossy(&sent);
    assert!(!sent.contains("aisix-spool-"), "no marker leaves the hop");
    assert!(
        sent.contains(&"QUJD".repeat(50_000)),
        "the image is back in the bridged body"
    );
}

#[tokio::test]
async fn route_ceiling_and_memory_charge_refuse_explicitly() {
    let dir = private_dir();
    let cg = no_limit_cgroup();
    let (base, seen) = upstream(0).await;
    let mut cfg = spool_config(dir.path());
    cfg.route_ceilings.insert("model-m".into(), 1_000_000);
    let app = router(&base, Some((cfg, cg.path())), 0);
    let (status, v) = send(&app, Body::from(image_body("m", 2 * MIB, "hi")), &[]).await;
    assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE, "{v}");
    assert_eq!(v["error"]["limit_bytes"], 1_000_000);
    assert!(
        v["error"]["message"]
            .as_str()
            .unwrap()
            .contains("route model-m"),
        "{v}"
    );

    let tight = cgroup_with_headroom(36 << 20);
    let app = router(&base, Some((spool_config(dir.path()), tight.path())), 0);
    let (status, v) = send(&app, Body::from(image_body("m", 2 * MIB, "hi")), &[]).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{v}");
    assert_eq!(v["error"]["code"], "insufficient_memory");
    assert_eq!(v["error"]["hop"], "aisix");
    assert!(seen.lock().unwrap().is_empty());
}

fn big_image_body() -> String {
    json!({"model": "m", "stream": false, "input": [{"role": "user", "content": [
        {"type": "input_image", "image_url": format!("data:image/png;base64,{}", "A".repeat(65 * MIB))}
    ]}]})
    .to_string()
}

/// The same bytes, as a chunked body with no `Content-Length`.
fn chunked(body: String) -> Body {
    let bytes = bytes::Bytes::from(body);
    let chunks: Vec<_> = (0..bytes.len())
        .step_by(MIB)
        .map(|i| Ok::<_, std::io::Error>(bytes.slice(i..(i + MIB).min(bytes.len()))))
        .collect();
    Body::from_stream(futures::stream::iter(chunks))
}

#[tokio::test]
async fn a_large_body_without_a_ticket_or_ledger_is_refused() {
    let dir = private_dir();
    let cg = no_limit_cgroup();
    let (base, seen) = upstream(0).await;
    let app = router(&base, Some((spool_config(dir.path()), cg.path())), 0);
    let body = big_image_body();
    let cl = ("content-length", body.len().to_string());
    for (b, headers) in [
        (Body::from(body.clone()), vec![cl.clone()]),
        (chunked(body.clone()), vec![]),
        (
            Body::from(body.clone()),
            vec![
                cl.clone(),
                ("x-ai-gateway-capacity-ticket", TICKET.to_string()),
            ],
        ),
    ] {
        let (status, v) = send(&app, b, &headers).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{v}");
        assert_eq!(v["error"]["code"], "capacity_ticket_missing");
        assert_eq!(v["error"]["hop"], "aisix");
        assert!(
            v["error"]["message"]
                .as_str()
                .unwrap()
                .contains("no capacity ledger"),
            "{v}"
        );
    }
    assert!(seen.lock().unwrap().is_empty());
}

#[tokio::test]
async fn a_ticketed_large_body_is_leased_and_forwarded_with_its_ticket() {
    let dir = private_dir();
    let ledger = tempfile::tempdir().unwrap();
    let cg = no_limit_cgroup();
    let (base, seen) = upstream(0).await;
    let mut cfg = spool_config(dir.path());
    cfg.ledger_dir = Some(ledger.path().display().to_string());
    cfg.ticket_upstream_hosts = vec!["127.0.0.1".into()];
    let app = router(&base, Some((cfg, cg.path())), 0);
    let body = big_image_body();
    let cl = ("content-length", body.len().to_string());

    // No ticket, and a ticket the ledger does not hold.
    let (status, v) = send(&app, Body::from(body.clone()), std::slice::from_ref(&cl)).await;
    assert_eq!(
        (status.as_u16(), v["error"]["code"].as_str()),
        (503, Some("capacity_ticket_missing")),
        "{v}"
    );
    let ticket = ("x-ai-gateway-capacity-ticket", TICKET.to_string());
    let (status, v) = send(&app, chunked(body.clone()), std::slice::from_ref(&ticket)).await;
    assert_eq!(
        (status.as_u16(), v["error"]["code"].as_str()),
        (503, Some("capacity_ticket_invalid")),
        "{v}"
    );
    assert!(seen.lock().unwrap().is_empty());

    std::fs::write(ledger.path().join(format!("{TICKET}.ticket")), b"ticket").unwrap();
    let (status, v) = send(&app, chunked(body.clone()), std::slice::from_ref(&ticket)).await;
    assert_eq!(status, StatusCode::OK, "{v}");
    let got = seen.lock().unwrap()[0].clone();
    let mut want: Value = serde_json::from_str(&body).unwrap();
    want["model"] = json!("gpt-4o");
    let want = serde_json::to_vec(&want).unwrap();
    assert_eq!(got.len, want.len());
    assert_eq!(got.content_length, Some(want.len().to_string()));
    assert_eq!(
        got.ticket.as_deref(),
        Some(TICKET),
        "forwarded to the internal upstream"
    );
    assert!(got.sha == Sha256::digest(&want).to_vec());
    // The lease ended with the handler: keeper's waiter can take the ticket.
    let keeper = std::fs::File::open(ledger.path().join(format!("{TICKET}.ticket"))).unwrap();
    rustix::fs::flock(
        &keeper,
        rustix::fs::FlockOperation::NonBlockingLockExclusive,
    )
    .unwrap();
    drop(keeper);

    // An upstream host that is not listed never sees the ticket.
    let mut cfg = spool_config(dir.path());
    cfg.ledger_dir = Some(ledger.path().display().to_string());
    let app = router(&base, Some((cfg, cg.path())), 0);
    let (status, _) = send(&app, Body::from(body), &[cl, ticket]).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(seen.lock().unwrap()[1].ticket, None);
}

/// An OTLP collector that records every export body.
async fn collector() -> (String, Arc<Mutex<Vec<String>>>) {
    let got = Arc::new(Mutex::new(Vec::new()));
    let app = axum::Router::new().fallback({
        let got = got.clone();
        move |body: String| {
            got.lock().unwrap().push(body);
            async { StatusCode::OK }
        }
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    (format!("http://{addr}/v1/traces"), got)
}

/// B1: content capture, on success and on failure, gets the skeleton with
/// every marker replaced by a placeholder that carries no nonce.
#[tokio::test]
async fn content_capture_never_sees_a_marker() {
    let (endpoint, got) = collector().await;
    let exporter: aisix_core::models::ObservabilityExporter = serde_json::from_value(json!({
        "name": "capture", "enabled": true, "kind": "otlp_http", "endpoint": endpoint,
        "content_mode": "full", "content_max_bytes": 1 << 20
    }))
    .unwrap();
    let dir = private_dir();
    let cg = no_limit_cgroup();
    for fail in [0, 1] {
        let (base, _) = upstream(fail).await;
        let exporter = exporter.clone();
        let app = router_with(&base, Some((spool_config(dir.path()), cg.path())), 0, |s| {
            s.observability_exporters
                .insert(ResourceEntry::new("exporter-1", exporter, 1));
        });
        let (status, v) = send(&app, Body::from(image_body("m", 3 * MIB, "hi")), &[]).await;
        assert_eq!(status.is_success(), fail == 0, "{v}");
    }
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        let all = got.lock().unwrap().join("\n");
        if all.matches("gen_ai.input.messages").count() >= 2 {
            assert!(!all.contains("aisix-spool-"), "a marker reached capture");
            assert_eq!(
                all.matches("[spooled 31457").count(),
                4,
                "two blobs per request"
            );
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "no capture: {}",
            all.replace("QUJD", "")
        );
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
}

/// Open fds of this process on a spool file in `dir`. Spools are
/// `O_TMPFILE`, so they never show in `read_dir`; in /proc/self/fd they
/// link to `<dir>/#<inode> (deleted)`.
pub fn open_spools(dir: &std::path::Path) -> usize {
    std::fs::read_dir("/proc/self/fd")
        .unwrap()
        .filter_map(|e| std::fs::read_link(e.ok()?.path()).ok())
        .filter(|l| l.starts_with(dir))
        .count()
}

/// Wait until `cond` holds, up to 10 s.
async fn eventually(what: &str, mut cond: impl FnMut() -> bool) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    while !cond() {
        assert!(std::time::Instant::now() < deadline, "{what}");
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
}

async fn spools_closed(dir: &std::path::Path) {
    eventually("a spool fd stayed open", || open_spools(dir) == 0).await;
}

#[test]
fn open_spools_counts_an_anonymous_file() {
    let dir = private_dir();
    let fd = rustix::fs::open(
        dir.path(),
        rustix::fs::OFlags::TMPFILE | rustix::fs::OFlags::RDWR,
        rustix::fs::Mode::from_raw_mode(0o600),
    )
    .unwrap();
    assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 0);
    assert_eq!(open_spools(dir.path()), 1);
    drop(fd);
    assert_eq!(open_spools(dir.path()), 0);
}

/// A body whose first 2 MiB arrive at once; the rest is up to the sender.
fn held_body(
    body: &str,
) -> (
    Body,
    tokio::sync::mpsc::Sender<std::io::Result<bytes::Bytes>>,
) {
    let (tx, mut rx) = tokio::sync::mpsc::channel(4);
    let stream = async_stream::stream! {
        while let Some(c) = rx.recv().await { yield c; }
    };
    let head = bytes::Bytes::copy_from_slice(&body.as_bytes()[..2 * MIB]);
    tx.try_send(Ok(head)).unwrap();
    (Body::from_stream(stream), tx)
}

/// D2: the spool is closed when the client aborts mid-body, by a read
/// error or by dropping the request.
#[tokio::test]
async fn client_abort_closes_the_spool() {
    let dir = private_dir();
    let cg = no_limit_cgroup();
    let (base, seen) = upstream(0).await;
    let app = router(&base, Some((spool_config(dir.path()), cg.path())), 0);
    let body = image_body("m", 4 * MIB, "hi");
    for drop_it in [false, true] {
        let (b, tx) = held_body(&body);
        let task = tokio::spawn({
            let app = app.clone();
            async move { send(&app, b, &[]).await }
        });
        eventually("the spool never opened", || open_spools(dir.path()) == 1).await;
        if drop_it {
            task.abort();
            assert!(task.await.unwrap_err().is_cancelled());
        } else {
            tx.send(Err(std::io::Error::other("reset"))).await.unwrap();
            assert_eq!(task.await.unwrap().0, StatusCode::BAD_REQUEST);
        }
        spools_closed(dir.path()).await;
    }
    assert!(seen.lock().unwrap().is_empty());
}

/// An upstream on raw TCP: reads one request head, writes `answer` (if
/// any) and holds the socket for `hold` without reading, then closes it.
async fn raw_upstream(answer: Option<&'static str>, hold: std::time::Duration) -> String {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();
        let mut head = Vec::new();
        let mut buf = [0u8; 4096];
        while !head.windows(4).any(|w| w == b"\r\n\r\n") {
            let n = sock.read(&mut buf).await.unwrap();
            head.extend_from_slice(&buf[..n]);
        }
        if let Some(json) = answer {
            let resp = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{json}",
                json.len()
            );
            sock.write_all(resp.as_bytes()).await.unwrap();
        }
        tokio::time::sleep(hold).await;
    });
    format!("http://{addr}")
}

/// D2: the spool is closed when the upstream closes the connection before
/// it has read the body.
#[tokio::test]
async fn upstream_early_close_closes_the_spool() {
    let dir = private_dir();
    let cg = no_limit_cgroup();
    let base = raw_upstream(None, std::time::Duration::ZERO).await;
    let app = router(&base, Some((spool_config(dir.path()), cg.path())), 0);
    let (status, v) = send(&app, Body::from(image_body("m", 9 * MIB, "hi")), &[]).await;
    assert!(status.is_server_error(), "{status} {v}");
    spools_closed(dir.path()).await;
}

/// The ticket lease lives as long as the spool: an upstream that answers
/// before it has read the body leaves the transport holding the spool
/// after the handler returned, and the ticket stays leased until then.
#[tokio::test]
async fn the_lease_lives_as_long_as_the_spool() {
    let dir = private_dir();
    let ledger = tempfile::tempdir().unwrap();
    let ticket_file = ledger.path().join(format!("{TICKET}.ticket"));
    std::fs::write(&ticket_file, b"ticket").unwrap();
    let cg = no_limit_cgroup();
    const ANSWER: &str = r#"{"id":"resp_1","object":"response","status":"completed","model":"gpt-4o","output":[],"usage":{"input_tokens":2,"output_tokens":1,"total_tokens":3}}"#;
    let base = raw_upstream(Some(ANSWER), std::time::Duration::from_secs(2)).await;
    let mut cfg = spool_config(dir.path());
    cfg.ledger_dir = Some(ledger.path().display().to_string());
    let app = router(&base, Some((cfg, cg.path())), 0);
    let ticket = ("x-ai-gateway-capacity-ticket", TICKET.to_string());
    let (status, v) = send(&app, Body::from(big_image_body()), &[ticket]).await;
    assert_eq!(status, StatusCode::OK, "{v}");
    let keeper_can_lock = || {
        let keeper = std::fs::File::open(&ticket_file).unwrap();
        rustix::fs::flock(
            &keeper,
            rustix::fs::FlockOperation::NonBlockingLockExclusive,
        )
        .is_ok()
    };
    assert_eq!(
        open_spools(dir.path()),
        1,
        "the transport still holds the spool"
    );
    assert!(
        !keeper_can_lock(),
        "the lease ended while the spool was open"
    );
    eventually("the lease outlived the spool", keeper_can_lock).await;
    spools_closed(dir.path()).await;
}
