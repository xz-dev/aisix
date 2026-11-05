//! large-request-capacity D4 on this hop: the capacity-ticket lease, the
//! per-hop memory charge, and the explicit refusals both produce.
//!
//! No timer, queue, count cap or try-lock: the lease blocks on the kernel
//! lock, and the charge is one atomic byte counter checked against the
//! cgroup's own files.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// The hop name every refusal carries.
pub(crate) const HOP: &str = "aisix";
/// Request header that carries the capacity ticket id.
pub(crate) const TICKET_HEADER: &str = "x-ai-gateway-capacity-ticket";
/// Every hop opens the ticket at `<ledger>/<id>.ticket` (D4; keeper names it).
pub(crate) const TICKET_SUFFIX: &str = ".ticket";
/// Bodies at or above this size need a ticket.
pub(crate) const TICKET_THRESHOLD: u64 = 64 << 20;
/// Margin the memory charge keeps under `memory.max`.
pub(crate) const MEMORY_MARGIN: u64 = 32 << 20;

/// An explicit capacity refusal: HTTP status, stable `error.code`, the
/// limit or required bytes, and a message naming this hop.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CapacityError {
    pub status: u16,
    pub code: &'static str,
    pub limit_bytes: Option<u64>,
    pub message: String,
}

impl std::fmt::Display for CapacityError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl CapacityError {
    /// 413 `request_too_large`: `what` names the limit (and route or path).
    pub(crate) fn too_large(limit: u64, what: &str) -> Self {
        Self {
            status: 413,
            code: "request_too_large",
            limit_bytes: Some(limit),
            message: format!("{HOP}: request exceeds the {limit}-byte {what}"),
        }
    }

    pub(crate) fn insufficient_memory(step: u64, available: u64) -> Self {
        Self {
            status: 503,
            code: "insufficient_memory",
            limit_bytes: Some(step),
            message: format!(
                "{HOP}: insufficient memory: {step} bytes needed, {available} available \
                 above the {MEMORY_MARGIN}-byte margin"
            ),
        }
    }

    pub(crate) fn insufficient_storage(need: u64, free: u64, reserve: u64) -> Self {
        Self {
            status: 507,
            code: "insufficient_storage",
            limit_bytes: Some(reserve),
            message: format!(
                "{HOP}: insufficient storage: writing {need} bytes would leave {} of {free} \
                 free bytes, below the {reserve}-byte reserve",
                free.saturating_sub(need)
            ),
        }
    }

    pub(crate) fn ticket_missing(why: &str) -> Self {
        Self {
            status: 503,
            code: "capacity_ticket_missing",
            limit_bytes: Some(TICKET_THRESHOLD),
            message: format!("{HOP}: a body of {TICKET_THRESHOLD} bytes or more {why}"),
        }
    }

    pub(crate) fn ticket_invalid(why: &str) -> Self {
        Self {
            status: 503,
            code: "capacity_ticket_invalid",
            limit_bytes: None,
            message: format!("{HOP}: capacity ticket invalid: {why}"),
        }
    }

    /// 500: the body already held this request's marker nonce, or a marker
    /// survived to egress unresolved. Nothing is sent.
    pub(crate) fn marker(why: &str) -> Self {
        Self {
            status: 500,
            code: "marker_collision",
            limit_bytes: None,
            message: format!("{HOP}: spool marker {why}"),
        }
    }

    /// 500: the spool could not be written or read.
    pub(crate) fn spool_io(e: &std::io::Error) -> Self {
        Self {
            status: 500,
            code: "spool_io_error",
            limit_bytes: None,
            message: format!("{HOP}: request spool I/O failed: {e}"),
        }
    }
}

/// Whether an upstream error answer reaches the caller unchanged (D9): every
/// provider 413, and an inner hop refusal — a JSON `error.code` in the
/// capacity set plus an `error.hop`. Both are request-scoped: no retry,
/// failover, cooldown or health stats.
pub(crate) fn relays_unchanged(status: u16, body: &[u8]) -> bool {
    if status == 413 {
        return true;
    }
    let Ok(v) = serde_json::from_slice::<serde_json::Value>(body) else {
        return false;
    };
    let err = &v["error"];
    let hop = err["hop"].as_str().is_some_and(|h| !h.is_empty());
    let inner = err["code"].as_str().is_some_and(|c| {
        matches!(
            c,
            "request_too_large" | "insufficient_storage" | "insufficient_memory" | "capacity_busy"
        ) || c.starts_with("capacity_ticket_")
    });
    hop && inner
}

/// A shared lease on a capacity ticket. Dropping it closes the fd, which
/// drops the kernel lock.
#[derive(Debug)]
pub(crate) struct Lease {
    _file: std::fs::File,
    pub id: String,
}

/// Take the lease on ticket `id` in the read-only `ledger`: open `<id>.ticket`
/// read-only and close-on-exec, block on a shared `flock`, then require a
/// link — a ticket released while we waited has none.
pub(crate) async fn take_lease(ledger: &Path, id: &str) -> Result<Lease, CapacityError> {
    if id.len() != 32 || !id.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f')) {
        return Err(CapacityError::ticket_invalid("malformed id"));
    }
    let path = ledger.join(format!("{id}{TICKET_SUFFIX}"));
    let id = id.to_string();
    tokio::task::spawn_blocking(move || {
        let file = std::fs::File::open(&path)
            .map_err(|_| CapacityError::ticket_invalid("cannot open ticket"))?;
        loop {
            match rustix::fs::flock(&file, rustix::fs::FlockOperation::LockShared) {
                Err(rustix::io::Errno::INTR) => continue,
                Err(_) => return Err(CapacityError::ticket_invalid("cannot lock ticket")),
                Ok(()) => break,
            }
        }
        match rustix::fs::fstat(&file) {
            Ok(st) if st.st_nlink > 0 => Ok(Lease { _file: file, id }),
            _ => Err(CapacityError::ticket_invalid("ticket already released")),
        }
    })
    .await
    .map_err(|_| CapacityError::ticket_invalid("lease task failed"))?
}

/// This hop's memory budget: bytes its in-flight requests may still use,
/// checked against its own cgroup (v2) files.
#[derive(Debug)]
pub(crate) struct MemoryBudget {
    cgroup: PathBuf,
    outstanding: AtomicU64,
}

impl MemoryBudget {
    pub(crate) fn new(cgroup: impl Into<PathBuf>) -> Arc<Self> {
        Arc::new(Self {
            cgroup: cgroup.into(),
            outstanding: AtomicU64::new(0),
        })
    }

    /// `memory.max − memory.current`; `None` when the cgroup sets no limit
    /// (no file, or `max`).
    pub(crate) fn headroom(&self) -> Option<u64> {
        let read = |f: &str| std::fs::read_to_string(self.cgroup.join(f)).ok();
        let max: u64 = read("memory.max")?.trim().parse().ok()?;
        let current: u64 = read("memory.current")?.trim().parse().ok()?;
        Some(max.saturating_sub(current))
    }

    #[cfg(test)]
    pub(crate) fn outstanding(&self) -> u64 {
        self.outstanding.load(Ordering::SeqCst)
    }
}

/// One request's charge against a [`MemoryBudget`], released on drop.
#[derive(Debug)]
pub(crate) struct Charge {
    budget: Arc<MemoryBudget>,
    held: u64,
}

impl Charge {
    pub(crate) fn new(budget: Arc<MemoryBudget>) -> Self {
        Self { budget, held: 0 }
    }

    /// Charge `step` more bytes before allocating them: refused with 503
    /// `insufficient_memory` unless `memory.max − memory.current −
    /// outstanding ≥ step + 32 MiB`. `memory.current` already holds bytes
    /// in use, so the double count errs toward refusing.
    pub(crate) fn add(&mut self, step: u64) -> Result<(), CapacityError> {
        let before = self.budget.outstanding.fetch_add(step, Ordering::SeqCst);
        if let Some(head) = self.budget.headroom() {
            if head < before.saturating_add(step).saturating_add(MEMORY_MARGIN) {
                self.budget.outstanding.fetch_sub(step, Ordering::SeqCst);
                let available = head.saturating_sub(before).saturating_sub(MEMORY_MARGIN);
                return Err(CapacityError::insufficient_memory(step, available));
            }
        }
        self.held += step;
        Ok(())
    }
}

impl Drop for Charge {
    fn drop(&mut self) {
        self.budget
            .outstanding
            .fetch_sub(self.held, Ordering::SeqCst);
    }
}

#[cfg(test)]
#[path = "capacity_tests.rs"]
mod tests;
