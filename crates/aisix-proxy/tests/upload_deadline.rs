//! D8 (large-request-capacity task 4.5): the deadline of a `/v1/responses`
//! send (the stream budget, or the request timeout when not streaming)
//! starts after the last body byte is written; before that it bounds each
//! gap with no upload progress. An upstream that reads nothing gets an
//! explicit timeout relayed to the client.

use std::sync::Arc;
use std::time::{Duration, Instant};

use aisix_core::resource::ResourceEntry;
use aisix_core::snapshot::SnapshotHandle;
use aisix_core::{AisixSnapshot, ApiKey, Model, ProviderKey, ProxyConfig};
use aisix_gateway::Hub;
use aisix_provider_openai::OpenAiBridge;
use aisix_proxy::{build_router, ProxyState};
use axum::body::{to_bytes, Body};
use axum::http::{Request, StatusCode};
use serde_json::json;
use tower::ServiceExt;

const PK_ID: &str = "11111111-1111-1111-1111-111111111111";

/// A raw upstream that accepts connections and never reads from them. Its
/// small receive buffer keeps the kernel from absorbing the body for it.
async fn deaf_upstream() -> String {
    let sock = tokio::net::TcpSocket::new_v4().unwrap();
    sock.set_recv_buffer_size(4096).unwrap();
    sock.bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let listener = sock.listen(8).unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let mut held = Vec::new();
        while let Ok((s, _)) = listener.accept().await {
            held.push(s);
        }
    });
    format!("http://{addr}/v1")
}

/// A raw upstream that reads the body in small reads `gap` apart, so its
/// upload takes several times the deadline, then answers: a completed
/// stream, or for `Answer::Json` a JSON response (for `Answer::Stall` the
/// head and half of it, then nothing).
#[derive(Clone, Copy, PartialEq)]
enum Answer {
    Sse,
    Json,
    Stall,
}

async fn slow_upstream(gap: Duration) -> String {
    reading_upstream(gap, Answer::Sse).await
}

async fn reading_upstream(gap: Duration, answer: Answer) -> String {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let (mut s, _) = listener.accept().await.unwrap();
        let mut head = Vec::new();
        let mut byte = [0u8; 1];
        while !head.ends_with(b"\r\n\r\n") {
            s.read_exact(&mut byte).await.unwrap();
            head.push(byte[0]);
        }
        let head = String::from_utf8(head).unwrap().to_ascii_lowercase();
        let len: usize = head
            .lines()
            .find_map(|l| l.strip_prefix("content-length: "))
            .expect("content-length framing")
            .trim()
            .parse()
            .unwrap();
        let mut buf = vec![0u8; 64 << 10];
        let mut got = 0;
        while got < len {
            let n = s.read(&mut buf).await.unwrap();
            assert!(n > 0);
            got += n;
            tokio::time::sleep(gap).await;
        }
        let resp_json = r#"{"id":"resp_slow","object":"response","status":"completed","model":"gpt-4o","output":[],"usage":{"input_tokens":1,"output_tokens":1,"total_tokens":2}}"#;
        let (ct, body) = match answer {
            Answer::Sse => {
                let done = format!(
                    r#"{{"type":"response.completed","sequence_number":0,"response":{resp_json}}}"#
                );
                (
                    "text/event-stream",
                    format!("event: response.completed\ndata: {done}\n\n"),
                )
            }
            Answer::Json | Answer::Stall => ("application/json", resp_json.to_string()),
        };
        let mut resp = format!(
            "HTTP/1.1 200 OK\r\ncontent-type: {ct}\r\ncontent-length: {}\r\n\r\n{body}",
            body.len()
        );
        if answer == Answer::Stall {
            resp.truncate(resp.len() - body.len() / 2);
        }
        s.write_all(resp.as_bytes()).await.unwrap();
        tokio::time::sleep(Duration::from_secs(5)).await;
    });
    format!("http://{addr}/v1")
}

fn app(api_base: &str, stream_timeout_ms: u64) -> axum::Router {
    app_with(api_base, 0, stream_timeout_ms)
}

fn app_with(api_base: &str, timeout_ms: u64, stream_timeout_ms: u64) -> axum::Router {
    let snapshot = AisixSnapshot::new();
    let pk: ProviderKey = serde_json::from_value(json!({
        "display_name": "deaf", "secret": "test-upstream-credential",
        "api_base": api_base, "provider": "openai", "adapter": "openai"
    }))
    .unwrap();
    snapshot
        .provider_keys
        .insert(ResourceEntry::new(PK_ID, pk, 1));
    let model: Model = serde_json::from_value(json!({
        "display_name": "m", "provider": "openai",
        "model_name": "gpt-4o", "provider_key_id": PK_ID
    }))
    .unwrap();
    snapshot
        .models
        .insert(ResourceEntry::new("model-m", model, 1));
    let key: ApiKey = serde_json::from_value(json!({
        "key_hash": ApiKey::hash_bearer("test-caller"), "allowed_models": ["m"]
    }))
    .unwrap();
    snapshot
        .apikeys
        .insert(ResourceEntry::new("caller", key, 1));
    let hub = Arc::new(Hub::new());
    hub.register_specialized("openai", Arc::new(OpenAiBridge::new()));
    let config = ProxyConfig {
        addr: "127.0.0.1:0".into(),
        request_body_limit_bytes: 64 << 20,
        real_ip: Default::default(),
        request_id: Default::default(),
        url_rewrites: Vec::new(),
        expose_debug_headers: false,
        tls: None,
        listeners: Vec::new(),
        thread_per_core: None,
        workers: None,
    };
    build_router(
        ProxyState::new(SnapshotHandle::new(snapshot), hub, &config)
            .without_cache()
            .with_default_timeouts(timeout_ms, stream_timeout_ms),
    )
}

#[tokio::test]
async fn upstream_that_reads_nothing_gets_an_explicit_timeout() {
    let app = app(&deaf_upstream().await, 300);
    let body = json!({"model": "m", "stream": true, "input": "a".repeat(32 << 20)}).to_string();
    let request = Request::builder()
        .method("POST")
        .uri("/v1/responses")
        .header("authorization", "Bearer test-caller")
        .header("content-type", "application/json")
        .body(Body::from(body))
        .unwrap();
    let started = Instant::now();
    let response = tokio::time::timeout(Duration::from_secs(30), app.oneshot(request))
        .await
        .expect("no explicit timeout")
        .unwrap();
    let status = response.status();
    let text = to_bytes(response.into_body(), 1 << 20).await.unwrap();
    let text = String::from_utf8_lossy(&text);
    assert_eq!(status, StatusCode::GATEWAY_TIMEOUT, "{text}");
    // The cause (the stalled upload, bytes sent) goes to the logs; the
    // client envelope keeps its fixed timeout sentence.
    assert!(text.contains("upstream request timed out after"), "{text}");
    assert!(text.contains("\"type\":\"timeout\""), "{text}");
    assert!(started.elapsed() >= Duration::from_millis(300));
    assert!(started.elapsed() < Duration::from_secs(20));
}

#[tokio::test]
async fn upstream_that_keeps_reading_slowly_is_not_cut_at_the_deadline() {
    let app = app(&slow_upstream(Duration::from_millis(4)).await, 1000);
    let body = json!({"model": "m", "stream": true, "input": "a".repeat(32 << 20)}).to_string();
    let request = Request::builder()
        .method("POST")
        .uri("/v1/responses")
        .header("authorization", "Bearer test-caller")
        .header("content-type", "application/json")
        .body(Body::from(body))
        .unwrap();
    let started = Instant::now();
    let response = tokio::time::timeout(Duration::from_secs(60), app.oneshot(request))
        .await
        .expect("hung")
        .unwrap();
    let status = response.status();
    let text = to_bytes(response.into_body(), 1 << 20).await.unwrap();
    let text = String::from_utf8_lossy(&text);
    let took = started.elapsed();
    assert_eq!(status, StatusCode::OK, "{text}");
    assert!(text.contains("response.completed"), "{text}");
    assert!(
        took > Duration::from_millis(900),
        "the upload must outlast the deadline: {took:?}"
    );
}

async fn post(app: axum::Router, stream: bool) -> (StatusCode, String, Duration) {
    let body = json!({"model": "m", "stream": stream, "input": "a".repeat(32 << 20)}).to_string();
    let request = Request::builder()
        .method("POST")
        .uri("/v1/responses")
        .header("authorization", "Bearer test-caller")
        .header("content-type", "application/json")
        .body(Body::from(body))
        .unwrap();
    let started = Instant::now();
    let response = tokio::time::timeout(Duration::from_secs(60), app.oneshot(request))
        .await
        .expect("hung")
        .unwrap();
    let status = response.status();
    let text = to_bytes(response.into_body(), 1 << 20).await.unwrap();
    (
        status,
        String::from_utf8_lossy(&text).into_owned(),
        started.elapsed(),
    )
}

/// 4.5 non-streaming: the request timeout starts after the last body byte
/// too, so an upstream that keeps reading for longer than it still answers.
#[tokio::test]
async fn non_streaming_slow_reader_is_not_cut_at_the_request_timeout() {
    let up = reading_upstream(Duration::from_millis(4), Answer::Json).await;
    let (status, text, took) = post(app_with(&up, 1000, 0), false).await;
    assert_eq!(status, StatusCode::OK, "{text}");
    assert!(text.contains("resp_slow"), "{text}");
    assert!(
        took > Duration::from_millis(900),
        "the upload must outlast the deadline: {took:?}"
    );
}

/// 4.5 non-streaming: a deaf upstream and a stalled response body each get
/// the explicit 504 timeout.
#[tokio::test]
async fn non_streaming_stalls_get_an_explicit_timeout() {
    for up in [
        deaf_upstream().await,
        reading_upstream(Duration::ZERO, Answer::Stall).await,
    ] {
        let (status, text, took) = post(app_with(&up, 300, 0), false).await;
        assert_eq!(status, StatusCode::GATEWAY_TIMEOUT, "{text}");
        assert!(text.contains("\"type\":\"timeout\""), "{text}");
        assert!(took >= Duration::from_millis(300) && took < Duration::from_secs(20));
    }
}
