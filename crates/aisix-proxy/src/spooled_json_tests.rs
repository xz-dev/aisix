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
