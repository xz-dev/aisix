//! large-request-capacity: the `/v1/responses` body read through the
//! skeleton + blob spool (D2), with the capacity ticket lease, the inline
//! limit and the per-hop memory charge (D4).

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;

use axum::extract::Request;
use futures::StreamExt;
use serde_json::Value;

use crate::body_spool::SpoolFile;
use crate::capacity::{self, CapacityError, Charge, Lease, MemoryBudget};
use crate::error::ProxyError;
use crate::skeleton::{IngestError, Tokenizer};
use crate::splice::BlobStore;

const MIB: u64 = 1 << 20;
/// Inline (skeleton) bytes allowed per request, and per ticketed request.
pub(crate) const INLINE_LIMIT: u64 = 16 * MIB;
pub(crate) const INLINE_LIMIT_TICKETED: u64 = 8 * MIB;
/// Largest body a fallback path (guardrail chain, bridged target) takes;
/// it needs the whole body in memory.
pub(crate) const FALLBACK_LIMIT: u64 = 16 * MIB;
/// Memory charged per inline byte (M0 measured `k_skel` for AISIX).
const K_SKEL: u64 = 5;
/// Memory charged per body byte on a fallback path.
const K_PATH: u64 = 4;
/// Page-cache bound of one spool (D3), charged when a ticketed body arrives.
const P_H: u64 = 16 * MIB;

/// The deployment's spool settings (`body_spool`), checked at startup.
#[derive(Debug)]
pub struct BodySpool {
    threshold: usize,
    dir: PathBuf,
    ledger: Option<PathBuf>,
    ticket_hosts: Vec<String>,
    reserve: u64,
    ceilings: BTreeMap<String, u64>,
    memory: Arc<MemoryBudget>,
}

impl BodySpool {
    /// `None` when `threshold_bytes` is 0 (off). An unusable spool dir is
    /// a startup error naming it.
    pub fn from_config(cfg: &aisix_core::config::BodySpoolConfig) -> Result<Option<Self>, String> {
        if cfg.threshold_bytes == 0 {
            return Ok(None);
        }
        let dir = PathBuf::from(
            cfg.dir
                .as_deref()
                .ok_or("body_spool.dir is required when body_spool.threshold_bytes > 0")?,
        );
        crate::body_spool::check_dir(&dir)?;
        Ok(Some(Self {
            threshold: usize::try_from(cfg.threshold_bytes).unwrap_or(usize::MAX),
            dir,
            ledger: cfg.ledger_dir.as_ref().map(PathBuf::from),
            ticket_hosts: cfg.ticket_upstream_hosts.clone(),
            reserve: match cfg.disk_reserve_bytes {
                0 => crate::body_spool::DEFAULT_RESERVE,
                n => n,
            },
            ceilings: cfg.route_ceilings.clone(),
            memory: MemoryBudget::new("/sys/fs/cgroup"),
        }))
    }

    /// Read the memory charge's cgroup files from `dir` instead.
    pub fn with_cgroup_dir(mut self, dir: impl Into<PathBuf>) -> Self {
        self.memory = MemoryBudget::new(dir);
        self
    }
}

/// What a spooled request carries through the handler: its blobs, its
/// ticket lease and its memory charge, all released when it is dropped.
#[derive(Debug)]
pub(crate) struct Spooled {
    pub store: Option<Arc<BlobStore>>,
    pub lease: Option<Lease>,
    pub total: u64,
    cfg: Arc<BodySpool>,
    charge: std::sync::Mutex<Charge>,
    fallback_charged: std::sync::atomic::AtomicBool,
}

impl Spooled {
    /// The ticket header value for an upstream at `host`, when it is one
    /// the ticket may be forwarded to.
    pub(crate) fn ticket_for(&self, host: Option<&str>) -> Option<&str> {
        let lease = self.lease.as_ref()?;
        let host = host?;
        self.cfg
            .ticket_hosts
            .iter()
            .any(|h| h == host)
            .then_some(lease.id.as_str())
    }

    /// The 413 for a target whose route ceiling the body exceeds.
    pub(crate) fn check_ceiling(&self, route: &str) -> Result<(), CapacityError> {
        match self.cfg.ceilings.get(route) {
            Some(&c) if self.total > c => Err(CapacityError::too_large(
                c,
                &format!("request ceiling of route {route}"),
            )),
            _ => Ok(()),
        }
    }

    /// Put the blobs back into `body` for a fallback path (`path` names
    /// it): refused above the fallback limit, charged before allocating.
    pub(crate) fn rehydrate_for(&self, path: &str, body: &mut Value) -> Result<(), CapacityError> {
        let Some(store) = self.store.as_ref().filter(|s| !s.blobs.is_empty()) else {
            return Ok(());
        };
        if self.total > FALLBACK_LIMIT {
            return Err(CapacityError::too_large(
                FALLBACK_LIMIT,
                &format!("fallback limit of {path}"),
            ));
        }
        // One charge per request: the whole body is in memory once.
        if !self
            .fallback_charged
            .swap(true, std::sync::atomic::Ordering::SeqCst)
        {
            let charged = self
                .charge
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .add(K_PATH * self.total);
            if let Err(e) = charged {
                self.fallback_charged
                    .store(false, std::sync::atomic::Ordering::SeqCst);
                return Err(e);
            }
        }
        store.rehydrate(body)
    }
}

fn is_json(req: &Request) -> bool {
    let Some(ct) = req
        .headers()
        .get(axum::http::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
    else {
        return false;
    };
    let essence = ct
        .split(';')
        .next()
        .unwrap_or("")
        .trim()
        .to_ascii_lowercase();
    match essence.split_once('/') {
        Some(("application", sub)) => sub == "json" || sub.ends_with("+json"),
        _ => false,
    }
}

fn invalid_json() -> ProxyError {
    ProxyError::InvalidRequest("invalid JSON request body".into())
}

/// Read `req`'s body as a skeleton `Value` plus its spool. Same
/// content-type check, read-error and invalid-JSON answers and 413 as the
/// buffered `Json` extractor; the 413 also covers chunked bodies.
pub(crate) async fn read(
    cfg: &Arc<BodySpool>,
    req: Request,
    limit: usize,
) -> Result<(Value, Spooled), ProxyError> {
    if !is_json(&req) {
        return Err(invalid_json());
    }
    let declared = req
        .headers()
        .get(axum::http::header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok()?.parse::<u64>().ok());
    let ticket = req
        .headers()
        .get(capacity::TICKET_HEADER)
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);
    let missing = |why| ProxyError::Capacity(CapacityError::ticket_missing(why));
    // The lease comes first: before any byte is read, let alone spooled.
    let lease = match (&ticket, &cfg.ledger) {
        (Some(id), Some(ledger)) => Some(
            capacity::take_lease(ledger, id)
                .await
                .map_err(ProxyError::Capacity)?,
        ),
        _ => None,
    };
    let why_missing = if cfg.ledger.is_none() {
        "needs a capacity ticket, and no capacity ledger is configured"
    } else {
        "arrived without a capacity ticket"
    };
    if lease.is_none() && declared.is_some_and(|n| n >= capacity::TICKET_THRESHOLD) {
        return Err(missing(why_missing));
    }
    let inline_limit = if lease.is_some() {
        INLINE_LIMIT_TICKETED
    } else {
        INLINE_LIMIT
    };
    let mut charge = Charge::new(cfg.memory.clone());
    if lease.is_some() {
        let n = declared.unwrap_or(limit as u64);
        charge.add(n / 10 + P_H).map_err(ProxyError::Capacity)?;
    }
    let (dir, reserve) = (cfg.dir.clone(), cfg.reserve);
    let mut tok = Tokenizer::new(
        cfg.threshold,
        Box::new(move || SpoolFile::create(&dir, reserve)),
    );
    let ingest = |e: IngestError| match e {
        IngestError::Invalid => invalid_json(),
        IngestError::Capacity(c) => ProxyError::Capacity(c),
    };
    let mut total = 0u64;
    let mut charged = 0u64;
    let mut frames = req.into_body().into_data_stream();
    while let Some(frame) = frames.next().await {
        let frame =
            frame.map_err(|_| ProxyError::InvalidRequest("failed to read request body".into()))?;
        total += frame.len() as u64;
        if limit > 0 && total > limit as u64 {
            return Err(ProxyError::RequestTooLarge { limit_bytes: limit });
        }
        if lease.is_none() && total >= capacity::TICKET_THRESHOLD {
            return Err(missing(why_missing));
        }
        for piece in frame.chunks(MIB as usize) {
            // Charge inline growth in 1 MiB steps before it is allocated.
            let need = (tok.skeleton.len() as u64 + piece.len() as u64).min(inline_limit + MIB);
            while charged < need {
                charge.add(K_SKEL * MIB).map_err(ProxyError::Capacity)?;
                charged += MIB;
            }
            tok.feed(piece).map_err(ingest)?;
            if tok.skeleton.len() as u64 > inline_limit {
                return Err(ProxyError::Capacity(CapacityError::too_large(
                    inline_limit,
                    "inline-bytes limit",
                )));
            }
        }
    }
    tok.finish().map_err(ingest)?;
    let value: Value = serde_json::from_slice(&tok.skeleton).map_err(|_| invalid_json())?;
    let store = tok.take_spool().map(|spool| {
        Arc::new(BlobStore {
            nonce: tok.nonce().to_string(),
            blobs: std::mem::take(&mut tok.blobs),
            spool,
        })
    });
    Ok((
        value,
        Spooled {
            store,
            lease,
            total,
            cfg: cfg.clone(),
            charge: std::sync::Mutex::new(charge),
            fallback_charged: Default::default(),
        },
    ))
}
