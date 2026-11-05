use super::*;

const MIB: u64 = 1 << 20;

fn cgroup(max: &str, current: u64) -> tempfile::TempDir {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("memory.max"), format!("{max}\n")).unwrap();
    std::fs::write(dir.path().join("memory.current"), format!("{current}\n")).unwrap();
    dir
}

#[test]
fn charge_refuses_instead_of_crossing_the_margin() {
    let dir = cgroup(&(200 * MIB).to_string(), 100 * MIB);
    let budget = MemoryBudget::new(dir.path());
    let mut a = Charge::new(budget.clone());
    a.add(50 * MIB).unwrap();
    let mut b = Charge::new(budget.clone());
    // 100 MiB headroom − 50 outstanding < 30 + 32 margin.
    let err = b.add(30 * MIB).unwrap_err();
    assert_eq!((err.status, err.code), (503, "insufficient_memory"));
    assert!(err.message.starts_with("aisix:"), "{err}");
    assert_eq!(budget.outstanding(), 50 * MIB, "a refused step is not kept");
    b.add(18 * MIB).unwrap();
    drop(a);
    drop(b);
    assert_eq!(budget.outstanding(), 0, "released when the request ends");
}

#[test]
fn no_cgroup_limit_charges_without_refusing() {
    let dir = cgroup("max", 100 * MIB);
    let mut c = Charge::new(MemoryBudget::new(dir.path()));
    c.add(1 << 40).unwrap();
    let mut c = Charge::new(MemoryBudget::new("/nonexistent-cgroup"));
    c.add(1 << 40).unwrap();
}

/// Concurrent 1 MiB steps never take the outstanding total past
/// `memory.max − memory.current − 32 MiB`; the rest are refused.
#[test]
fn concurrent_charges_stay_under_the_margin() {
    let dir = cgroup(&(512 * MIB).to_string(), 122 * MIB);
    let budget = MemoryBudget::new(dir.path());
    let limit = (512 - 122 - 32) * MIB;
    let handles: Vec<_> = (0..16)
        .map(|_| {
            let budget = budget.clone();
            std::thread::spawn(move || {
                let mut c = Charge::new(budget.clone());
                let mut refused = 0u64;
                for _ in 0..64 {
                    match c.add(MIB) {
                        Ok(()) => {}
                        Err(e) => {
                            assert_eq!(e.code, "insufficient_memory");
                            refused += 1;
                        }
                    }
                }
                (c, refused)
            })
        })
        .collect();
    let results: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();
    let refused: u64 = results.iter().map(|r| r.1).sum();
    let committed = (16 * 64 - refused) * MIB;
    assert_eq!(budget.outstanding(), committed);
    // A refused step's transient add can refuse a racing one, never admit it.
    assert!(committed <= limit, "{committed} > {limit}");
    assert!(committed > limit - 16 * MIB, "{committed}");
    drop(results);
    assert_eq!(budget.outstanding(), 0);
}

fn ledger_with(id: &str) -> tempfile::TempDir {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join(ticket_name(id)), b"ticket").unwrap();
    dir
}

const ID: &str = "0123456789abcdef0123456789abcdef";

fn ticket_name(id: &str) -> String {
    format!("{id}{TICKET_SUFFIX}")
}

#[tokio::test]
async fn lease_opens_the_dot_ticket_file() {
    let ledger = tempfile::tempdir().unwrap();
    std::fs::write(ledger.path().join(ID), b"ticket").unwrap();
    let err = take_lease(ledger.path(), ID).await.unwrap_err();
    assert_eq!(err.code, "capacity_ticket_invalid", "bare-id file leased");
    std::fs::write(ledger.path().join(ticket_name(ID)), b"ticket").unwrap();
    let lease = take_lease(ledger.path(), ID).await.unwrap();
    assert_eq!(lease.id, ID);
}

/// A malformed or path-like id is refused before any path is built, even
/// when the traversal target exists.
#[tokio::test]
async fn lease_refuses_malformed_and_traversal_ids() {
    let root = tempfile::tempdir().unwrap();
    let ledger = root.path().join("ledger");
    std::fs::create_dir(&ledger).unwrap();
    // Targets of the "../" ids below.
    for name in ["x", &ID[..29]] {
        std::fs::write(root.path().join(ticket_name(name)), b"ticket").unwrap();
    }
    let traversal = format!("../{}", &ID[..29]);
    let long = format!("{ID}0");
    let slash = format!("{}/x", &ID[..30]);
    let nul = format!("{}\0", &ID[..31]);
    let upper = ID.to_uppercase();
    for bad in [
        "../x",
        traversal.as_str(),
        &ID[..31],
        long.as_str(),
        upper.as_str(),
        slash.as_str(),
        nul.as_str(),
    ] {
        let err = take_lease(&ledger, bad).await.unwrap_err();
        assert_eq!(
            (err.status, err.code),
            (503, "capacity_ticket_invalid"),
            "{bad:?}"
        );
    }
}

#[tokio::test]
async fn lease_holds_a_shared_lock_until_dropped() {
    let ledger = ledger_with(ID);
    let lease = take_lease(ledger.path(), ID).await.unwrap();
    assert_eq!(lease.id, ID);
    let keeper = std::fs::File::open(ledger.path().join(ticket_name(ID))).unwrap();
    let ex = rustix::fs::FlockOperation::NonBlockingLockExclusive;
    assert!(
        rustix::fs::flock(&keeper, ex).is_err(),
        "keeper must wait for the lease"
    );
    drop(lease);
    rustix::fs::flock(&keeper, ex).unwrap();
}

#[tokio::test]
async fn lease_refuses_bad_missing_and_released_tickets() {
    let ledger = ledger_with(ID);
    for bad in ["../etc/passwd", "0123456789ABCDEF0123456789ABCDEF", "abc"] {
        let err = take_lease(ledger.path(), bad).await.unwrap_err();
        assert_eq!(err.code, "capacity_ticket_invalid", "{bad}");
    }
    let other = "f".repeat(32);
    let err = take_lease(ledger.path(), &other).await.unwrap_err();
    assert_eq!((err.status, err.code), (503, "capacity_ticket_invalid"));

    // Keeper's release waiter holds the exclusive lock, deletes the
    // ticket and closes: the blocked lease then finds no link.
    let keeper = std::fs::File::open(ledger.path().join(ticket_name(ID))).unwrap();
    rustix::fs::flock(&keeper, rustix::fs::FlockOperation::LockExclusive).unwrap();
    let path = ledger.path().to_path_buf();
    let waiting = tokio::spawn(async move { take_lease(&path, ID).await });
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    assert!(
        !waiting.is_finished(),
        "the lease blocks on the kernel lock"
    );
    std::fs::remove_file(ledger.path().join(ticket_name(ID))).unwrap();
    drop(keeper);
    let err = waiting.await.unwrap().unwrap_err();
    assert!(err.message.contains("already released"), "{err}");
}

#[test]
fn relays_unchanged_covers_413_and_inner_hop_errors_only() {
    let inner = |code: &str, hop: &str| {
        serde_json::json!({"error": {"message": "m", "type": "server_error", "code": code, "hop": hop}})
            .to_string()
    };
    assert!(relays_unchanged(413, b"<html>too large</html>"));
    for code in [
        "request_too_large",
        "insufficient_storage",
        "insufficient_memory",
        "capacity_busy",
        "capacity_ticket_missing",
        "capacity_ticket_invalid",
    ] {
        assert!(
            relays_unchanged(503, inner(code, "cpa").as_bytes()),
            "{code}"
        );
    }
    // No hop, an empty hop or another code: today's handling.
    assert!(!relays_unchanged(
        503,
        inner("insufficient_memory", "").as_bytes()
    ));
    assert!(!relays_unchanged(
        503,
        br#"{"error":{"message":"busy","code":"insufficient_memory"}}"#
    ));
    assert!(!relays_unchanged(
        503,
        inner("server_overloaded", "cpa").as_bytes()
    ));
    assert!(!relays_unchanged(503, b"upstream down"));
}
