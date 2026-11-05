//! Differential and rejection-parity corpus: for every body, serde on the
//! whole body and serde on the skeleton with the blobs spliced back must
//! accept the same bodies and produce the same bytes.

use std::os::unix::fs::PermissionsExt;
use std::sync::Arc;

use serde_json::Value;

use super::*;
use crate::splice::BlobStore;

fn spool_dir() -> std::path::PathBuf {
    static DIR: std::sync::LazyLock<tempfile::TempDir> = std::sync::LazyLock::new(|| {
        let d = tempfile::tempdir_in("/tmp").unwrap();
        std::fs::set_permissions(d.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        d
    });
    DIR.path().to_path_buf()
}

type Ingested = (Vec<u8>, Option<Arc<BlobStore>>);

fn ingest_with(t: &mut Tokenizer, body: &[u8], chunk: usize) -> Result<Ingested, IngestError> {
    for c in body.chunks(chunk.max(1)) {
        t.feed(c)?;
    }
    t.finish()?;
    let store = t.take_spool().map(|mut spool| {
        spool.finish_blocking().unwrap();
        Arc::new(BlobStore {
            nonce: t.nonce().to_string(),
            blobs: t.blobs.clone(),
            spool,
        })
    });
    Ok((std::mem::take(&mut t.skeleton), store))
}

fn tokenizer(threshold: usize) -> Tokenizer {
    let dir = spool_dir();
    Tokenizer::new(threshold, Box::new(move || SpoolFile::create(&dir, 0)))
}

fn ingest(body: &[u8], threshold: usize, chunk: usize) -> Result<Ingested, IngestError> {
    ingest_with(&mut tokenizer(threshold), body, chunk)
}

fn block_on<F: std::future::Future>(f: F) -> F::Output {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(f)
}

fn spliced(store: &Option<Arc<BlobStore>>, wire: Vec<u8>) -> Vec<u8> {
    use http_body_util::BodyExt;
    let Some(store) = store else { return wire };
    let plan = store.plan(bytes::Bytes::from(wire)).unwrap();
    let len = plan.len;
    let body = plan.body(store.clone());
    // The blob reads run on spawn_blocking: give them a runtime.
    let out = block_on(body.collect()).unwrap().to_bytes().to_vec();
    assert_eq!(out.len() as u64, len, "Content-Length matches the bytes");
    out
}

/// The body through both paths; `Some(spooled blob count)` when accepted.
fn differential(body: &[u8]) -> Option<usize> {
    let legacy = serde_json::from_slice::<Value>(body);
    let mut blobs = None;
    for chunk in [1, 3, 7, 64, body.len()] {
        let ours = ingest(body, 4, chunk).ok().and_then(|(skel, store)| {
            let v = serde_json::from_slice::<Value>(&skel).ok()?;
            Some((v, store))
        });
        let shown = String::from_utf8_lossy(&body[..body.len().min(120)]);
        assert_eq!(
            legacy.is_ok(),
            ours.is_some(),
            "accept/reject differs (chunk {chunk}): {shown}"
        );
        if let (Ok(legacy), Some((mut v, store))) = (&legacy, ours) {
            let want = serde_json::to_vec(legacy).unwrap();
            let got = spliced(&store, serde_json::to_vec(&v).unwrap());
            assert_eq!(
                String::from_utf8_lossy(&got),
                String::from_utf8_lossy(&want),
                "{shown}"
            );
            if let Some(store) = &store {
                block_on(store.rehydrate(&mut v)).unwrap();
            }
            assert_eq!(&v, legacy, "rehydrated value: {shown}");
            blobs = Some(store.map_or(0, |s| s.blobs.len()));
        }
    }
    blobs
}

#[test]
fn corpus_accepts_and_rejects_like_serde_with_identical_bytes() {
    let accepted: &[(&str, usize)] = &[
        (
            r#"{"input":[{"type":"input_image","image_url":"data:image/png;base64,AAAA"}]}"#,
            1,
        ),
        (
            r#"{"image_url":"esc \" \\ \/ \b\f\n\r\t \u0000 \u001f \u00e9 \ud83d\ude00 end"}"#,
            1,
        ),
        ("{\"image_url\":\"raw utf8 é 😀 中文 \u{7f}\"}", 1),
        (
            r#"{"a":{"image_url":{"url":"https://x/y.png?q=1"}},"source":{"data":"zzzzzz"}}"#,
            2,
        ),
        (
            r#"{"source":{"type":"base64","data":"QUJDREVGRw=="},"data":"not-allow-listed"}"#,
            1,
        ),
        (r#"{"image_url":"first-dup","image_url":"second-dup"}"#, 2),
        (
            r#"{"n":123456789012345678901234567890,"e":1e3,"f":-0.0,"g":1.5E-7}"#,
            0,
        ),
        (
            r#"{"text":"data:text/plain,a large text that is a data URL"}"#,
            1,
        ),
        (
            r#"{"text":"plain large text stays inline in the skeleton"}"#,
            0,
        ),
        (
            r#"{"file_data":"x","file_data2":"yyyyyyyy","url":"zzzzzzzz"}"#,
            0,
        ),
        (
            r#"  [ {"image_url" : "spaced   value" } , "data:x,yyyy" ]  "#,
            2,
        ),
        (
            r#"{"input_file":{"type":"input_file","file_data":"data:application/pdf;base64,JVBE"}}"#,
            1,
        ),
        (
            r#"{"ai_aff":"tag","encrypted_content":"gAAAAABlong","id":"rs_1","sig":"abc=="}"#,
            0,
        ),
        (r#""data:top-level string""#, 1),
    ];
    for (body, blobs) in accepted {
        assert_eq!(differential(body.as_bytes()), Some(*blobs), "{body}");
    }
    let rejected: &[&[u8]] = &[
        br#"{"image_url":"lone high \ud83d end"}"#,
        br#"{"image_url":"lone low \ude00 end"}"#,
        br#"{"image_url":"high then bmp \ud83d\u0041"}"#,
        br#"{"image_url":"bad escape \x41 here"}"#,
        br#"{"image_url":"bad hex \u12G4 here"}"#,
        b"{\"image_url\":\"raw control \x01 char\"}",
        b"{\"image_url\":\"bad utf8 \xff\xfe bytes\"}",
        b"{\"image_url\":\"truncated utf8 \xe4\xb8\"}",
        br#"{"image_url":"unterminated"#,
        br#"{"image_url":"ok"} trailing"#,
        br#"{"n":1e400}"#,
        br#"{"a":01}"#,
        br#"{"a" "b"}"#,
        br#"{'image_url':'single quotes'}"#,
        b"",
    ];
    for body in rejected {
        assert_eq!(
            differential(body),
            None,
            "{}",
            String::from_utf8_lossy(body)
        );
    }
}

#[test]
fn nesting_limit_matches_serde() {
    for depth in [126, 127, 128, 600] {
        let body = format!(
            "{}{{\"image_url\":\"deep-value\"}}{}",
            "[".repeat(depth),
            "]".repeat(depth)
        );
        differential(body.as_bytes());
    }
}

#[test]
fn large_text_stays_inline_and_media_goes_to_the_spool() {
    let big = "A".repeat(200_000);
    let body = format!(
        r#"{{"input":[{{"type":"input_text","text":"{big}"}},{{"type":"input_image","image_url":"data:image/png;base64,{big}"}}]}}"#
    );
    let (skel, store) = ingest(body.as_bytes(), 65536, 8192).unwrap();
    let store = store.expect("the image is spooled");
    assert_eq!(store.blobs.len(), 1);
    assert!(
        skel.len() < 200_000 + 200,
        "text inline, image out: {}",
        skel.len()
    );
    assert_eq!(
        differential(body.as_bytes()),
        Some(1),
        "text is never spooled"
    );
}

#[test]
fn a_body_holding_the_nonce_fails_closed() {
    let mut t = tokenizer(4);
    let body = format!(r#"{{"text":"{}","image_url":"aaaaaaaa"}}"#, t.nonce());
    match ingest_with(&mut t, body.as_bytes(), 5) {
        Err(IngestError::Capacity(e)) => assert_eq!((e.status, e.code), (500, "marker_collision")),
        other => panic!("{:?}", other.map(|r| r.0)),
    }
}

#[test]
fn an_unresolved_marker_at_egress_fails_closed() {
    let (skel, store) = ingest(br#"{"image_url":"aaaaaaaa"}"#, 4, 64).unwrap();
    let store = store.unwrap();
    let marker = std::str::from_utf8(&skel)
        .unwrap()
        .split('"')
        .nth(3)
        .unwrap()
        .to_string();
    for wire in [
        format!(r#"{{"x":"{marker}tail"}}"#),
        format!(r#"{{"x":"{}"}}"#, marker.replace("-00000000", "-00000001")),
        format!(r#"{{"x":"{}"}}"#, &marker[..45]),
        format!(r#"{{"x":"abc{}"}}"#, &marker[12..44]),
    ] {
        let err = store
            .plan(bytes::Bytes::from(wire.clone()))
            .err()
            .expect(&wire);
        assert_eq!(err.code, "marker_collision", "{wire}");
    }
}

/// Random byte mutations of escape-heavy bodies, split at every chunk
/// size the differential uses: serde and the splice still agree.
#[test]
fn mutated_bodies_keep_parity() {
    use rand::{Rng, SeedableRng};
    let seeds: &[&[u8]] = &[
        br#"{"image_url":"a\"b\\c\/d\u00e9\ud83d\ude00\n\t e","x":[1,2.5,"data:q,rr"]}"#,
        "{\"source\":{\"data\":\"é😀中\\u0041\\u0000zz\"},\"image_url\":{\"url\":\"data:x\"}}"
            .as_bytes(),
    ];
    let alphabet = b"\\\"u0dD8eFa:{}[],\x01\xc3\xa9\xff ";
    let mut rng = rand::rngs::StdRng::seed_from_u64(0x5eed);
    for _ in 0..1500 {
        let mut body = seeds[rng.gen_range(0..seeds.len())].to_vec();
        for _ in 0..rng.gen_range(1..4) {
            let at = rng.gen_range(0..body.len());
            let b = alphabet[rng.gen_range(0..alphabet.len())];
            match rng.gen_range(0..3) {
                0 => body[at] = b,
                1 => body.insert(at, b),
                _ => {
                    body.remove(at);
                }
            }
        }
        differential(&body);
    }
}

#[test]
fn capture_redacts_every_marker_shape_and_nothing_else() {
    let foreign = format!("aisix-spool-{}-{:08x}", "ab".repeat(16), 3);
    let v = serde_json::json!({"a": "aisix-spool-short", "b": foreign, "c": "x"});
    let out = crate::splice::capture_json(&v, None);
    assert_eq!(out, r#"{"a":"aisix-spool-short","b":"[spooled]","c":"x"}"#);
}
