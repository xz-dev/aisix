use super::*;

fn private_dir() -> tempfile::TempDir {
    let dir = tempfile::tempdir_in("/tmp").unwrap();
    std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    dir
}

#[test]
fn startup_check_accepts_a_private_disk_dir() {
    let dir = private_dir();
    check_dir(dir.path()).unwrap();
}

#[test]
fn startup_check_refuses_tmpfs_wrong_mode_and_leftover_files() {
    if Path::new("/dev/shm").is_dir() {
        let shm = tempfile::tempdir_in("/dev/shm").unwrap();
        std::fs::set_permissions(shm.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        let err = check_dir(shm.path()).unwrap_err();
        assert!(err.contains("tmpfs"), "{err}");
    }
    let dir = private_dir();
    std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o755)).unwrap();
    let err = check_dir(dir.path()).unwrap_err();
    assert!(err.contains("0700"), "{err}");

    let dir = private_dir();
    std::fs::write(dir.path().join("left-over"), b"x").unwrap();
    let err = check_dir(dir.path()).unwrap_err();
    assert!(err.contains("left-over"), "{err}");

    let err = check_dir(Path::new("/nonexistent-spool")).unwrap_err();
    assert!(err.contains("/nonexistent-spool"), "{err}");
}

#[tokio::test]
async fn spool_round_trips_without_a_name() {
    let dir = private_dir();
    let mut spool = SpoolFile::create(dir.path(), 0).unwrap();
    let data: Vec<u8> = (0..(20 << 20)).map(|i| (i % 251) as u8).collect();
    for chunk in data.chunks(100_000) {
        spool.append(chunk);
        spool.drain().await.unwrap();
        assert!(
            spool.pending.len() < WRITE_BATCH,
            "drained to under a batch"
        );
    }
    spool.finish().await.unwrap();
    assert_eq!(spool.len(), data.len() as u64);
    assert_eq!(
        std::fs::read_dir(dir.path()).unwrap().count(),
        0,
        "anonymous"
    );
    let mut buf = vec![0u8; 4096];
    spool.read_at(&mut buf, 12_345_678).unwrap();
    assert_eq!(buf, data[12_345_678..12_345_678 + 4096]);
    let mode = std::fs::metadata(format!("/proc/self/fd/{}", {
        use std::os::fd::AsRawFd;
        spool.file.as_raw_fd()
    }))
    .unwrap()
    .permissions()
    .mode();
    assert_eq!(mode & 0o777, 0o600);
}

#[tokio::test]
async fn a_write_past_the_reserve_is_refused_with_507() {
    let dir = private_dir();
    let mut spool = SpoolFile::create(dir.path(), u64::MAX / 2).unwrap();
    spool.append(b"small");
    let err = spool.finish().await.unwrap_err();
    assert_eq!((err.status, err.code), (507, "insufficient_storage"));
}
