use std::os::unix::fs::PermissionsExt;
use std::time::{Duration, Instant};

use axum::body::Body;

use super::*;

fn private_dir() -> tempfile::TempDir {
    let dir = tempfile::tempdir_in("/tmp").unwrap();
    std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    dir
}

fn spool_cfg(dir: &std::path::Path) -> Arc<BodySpool> {
    Arc::new(BodySpool {
        threshold: 64 << 10,
        dir: dir.to_path_buf(),
        ledger: None,
        ticket_hosts: Vec::new(),
        reserve: 0,
        ceilings: BTreeMap::new(),
        memory: MemoryBudget::new("/nonexistent-cgroup"),
    })
}

fn json_req(body: String) -> Request {
    Request::builder()
        .header("content-type", "application/json")
        .body(Body::from(body))
        .unwrap()
}

fn image_body(bytes: usize) -> String {
    let image = "QUJD".repeat(bytes / 4);
    serde_json::json!({"model": "m", "input": [{"role": "user", "content": [
        {"type": "input_image", "image_url": format!("data:image/png;base64,{image}")}
    ]}]})
    .to_string()
}

/// M1: thread-per-core runs one current-thread runtime per core. While a
/// large upload spools to a slow disk, a small request on the same runtime
/// still completes at once: the disk I/O is not on the worker thread.
#[test]
fn spool_disk_io_does_not_stall_the_runtime() {
    // Each write sleeps this long: on the worker, every small request
    // would wait at least that; the bound leaves room for a loaded host.
    const DISK_DELAY: Duration = Duration::from_secs(1);
    let dir = private_dir();
    *crate::body_spool::SLOW_DISK.lock().unwrap() = Some((dir.path().to_path_buf(), DISK_DELAY));
    let cfg = spool_cfg(dir.path());
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        let upload = tokio::spawn({
            let cfg = cfg.clone();
            async move {
                read(&cfg, json_req(image_body(3 << 19)), 0)
                    .await
                    .map(|_| ())
            }
        });
        let (mut worst, mut small) = (Duration::ZERO, 0);
        while !upload.is_finished() {
            let t = Instant::now();
            let (v, sp) = read(&cfg, json_req(r#"{"model":"m","input":"hi"}"#.into()), 0)
                .await
                .unwrap();
            assert!(sp.store.is_none() && v["input"] == "hi");
            tokio::time::sleep(Duration::from_millis(5)).await;
            worst = worst.max(t.elapsed());
            small += 1;
        }
        upload.await.unwrap().unwrap();
        assert!(small >= 10, "the upload took {small} small requests' time");
        assert!(
            worst < DISK_DELAY * 9 / 10,
            "a small request waited {worst:?} behind the spool's disk I/O"
        );
    });
}

/// Open fds of this process on a spool file in `dir` (`O_TMPFILE` files
/// never show in `read_dir`).
fn open_spools(dir: &std::path::Path) -> usize {
    std::fs::read_dir("/proc/self/fd")
        .unwrap()
        .filter_map(|e| std::fs::read_link(e.ok()?.path()).ok())
        .filter(|l| l.starts_with(dir))
        .count()
}

/// D2: a handler that panics while it holds the spooled request closes
/// the spool as it unwinds.
#[tokio::test]
async fn a_panic_closes_the_spool() {
    let dir = private_dir();
    let cfg = spool_cfg(dir.path());
    let task = tokio::spawn({
        let (cfg, dir) = (cfg.clone(), dir.path().to_path_buf());
        async move {
            let (_v, sp) = read(&cfg, json_req(image_body(1 << 20)), 0).await.unwrap();
            assert!(sp.store.is_some());
            assert_eq!(open_spools(&dir), 1, "the spool is open");
            panic!("handler panic");
        }
    });
    assert!(task.await.unwrap_err().is_panic());
    assert_eq!(open_spools(dir.path()), 0);
}

/// A declared length over the body cap is a 413, not the 503 for a
/// missing ticket (the router's own Content-Length check runs first in
/// production; this is the extractor's).
#[tokio::test]
async fn a_declared_length_over_the_cap_is_a_413() {
    let dir = private_dir();
    let mut req = json_req("{}".into());
    req.headers_mut().insert(
        axum::http::header::CONTENT_LENGTH,
        (300u64 << 20).to_string().parse().unwrap(),
    );
    match read(&spool_cfg(dir.path()), req, 256 << 20).await {
        Err(ProxyError::RequestTooLarge { limit_bytes }) => assert_eq!(limit_bytes, 256 << 20),
        other => panic!("{:?}", other.map(|(v, _)| v)),
    }
}
