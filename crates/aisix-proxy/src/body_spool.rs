//! large-request-capacity D3 spool storage: one anonymous `O_TMPFILE` per
//! request in a private, disk-backed directory. Writers and readers drop
//! the file's page cache as they go, and a write that would leave less
//! than the reserve free is refused with 507, never buffered in memory.

use std::fs::File;
use std::io;
use std::num::NonZeroU64;
use std::os::unix::fs::{FileExt, PermissionsExt};
use std::path::Path;

use crate::capacity::CapacityError;

/// Bytes written or read between page-cache drops.
const SYNC_EVERY: u64 = 8 << 20;
/// Writes are batched to this size, so the disk floor is checked per batch.
const WRITE_BATCH: usize = 1 << 20;
/// Free bytes a write must leave when no reserve is configured.
pub(crate) const DEFAULT_RESERVE: u64 = 1 << 30;

const TMPFS_MAGIC: u64 = 0x0102_1994;
const RAMFS_MAGIC: u64 = 0x8584_58f6;

/// The startup check: `dir` is a 0700 directory on a disk filesystem (not
/// tmpfs or ramfs), holds no regular file (spools are anonymous, so one
/// can only be a leftover or a mistake), and supports `O_TMPFILE`.
pub fn check_dir(dir: &Path) -> Result<(), String> {
    let shown = dir.display();
    let meta = std::fs::metadata(dir).map_err(|e| format!("spool dir {shown}: {e}"))?;
    if !meta.is_dir() {
        return Err(format!("spool dir {shown} is not a directory"));
    }
    let mode = meta.permissions().mode() & 0o777;
    if mode != 0o700 {
        return Err(format!(
            "spool dir {shown} has mode {mode:o}; 0700 is required"
        ));
    }
    let fs = rustix::fs::statfs(dir).map_err(|e| format!("spool dir {shown}: statfs: {e}"))?;
    #[allow(clippy::unnecessary_cast)]
    let kind = fs.f_type as u64;
    if kind == TMPFS_MAGIC || kind == RAMFS_MAGIC {
        return Err(format!(
            "spool dir {shown} is on tmpfs or ramfs; a spool must be on a disk volume"
        ));
    }
    for entry in std::fs::read_dir(dir).map_err(|e| format!("spool dir {shown}: {e}"))? {
        let entry = entry.map_err(|e| format!("spool dir {shown}: {e}"))?;
        if entry.file_type().is_ok_and(|t| t.is_file()) {
            return Err(format!(
                "spool dir {shown} holds a regular file {}; spools are anonymous, \
                 so remove it after checking what wrote it",
                entry.path().display()
            ));
        }
    }
    open_tmpfile(dir).map_err(|e| format!("spool dir {shown} does not support O_TMPFILE: {e}"))?;
    Ok(())
}

fn open_tmpfile(dir: &Path) -> io::Result<File> {
    use rustix::fs::{Mode, OFlags};
    let fd = rustix::fs::open(
        dir,
        OFlags::TMPFILE | OFlags::RDWR | OFlags::CLOEXEC,
        Mode::from_raw_mode(0o600),
    )?;
    Ok(File::from(fd))
}

fn drop_cache(file: &File, from: u64, to: u64) {
    if let Some(len) = NonZeroU64::new(to.saturating_sub(from)) {
        let _ = rustix::fs::fadvise(file, from, Some(len), rustix::fs::Advice::DontNeed);
    }
}

/// One request's spool. Appended to while the body is read, then shared
/// read-only by every attempt.
#[derive(Debug)]
pub(crate) struct SpoolFile {
    file: File,
    len: u64,
    synced: u64,
    reserve: u64,
    pending: Vec<u8>,
}

impl SpoolFile {
    pub(crate) fn create(dir: &Path, reserve: u64) -> Result<Self, CapacityError> {
        let file = open_tmpfile(dir).map_err(|e| CapacityError::spool_io(&e))?;
        Ok(Self {
            file,
            len: 0,
            synced: 0,
            reserve,
            pending: Vec::new(),
        })
    }

    /// Bytes written so far, pending batch included.
    pub(crate) fn len(&self) -> u64 {
        self.len + self.pending.len() as u64
    }

    pub(crate) fn append(&mut self, mut buf: &[u8]) -> Result<(), CapacityError> {
        while !buf.is_empty() {
            if self.pending.capacity() == 0 {
                self.pending.reserve_exact(WRITE_BATCH);
            }
            let n = buf.len().min(WRITE_BATCH - self.pending.len());
            self.pending.extend_from_slice(&buf[..n]);
            buf = &buf[n..];
            if self.pending.len() == WRITE_BATCH {
                self.flush()?;
            }
        }
        Ok(())
    }

    fn flush(&mut self) -> Result<(), CapacityError> {
        if self.pending.is_empty() {
            return Ok(());
        }
        let need = self.pending.len() as u64;
        let fs = rustix::fs::fstatfs(&self.file)
            .map_err(|e| CapacityError::spool_io(&io::Error::from(e)))?;
        #[allow(clippy::unnecessary_cast)]
        let free = (fs.f_bavail as u64).saturating_mul(fs.f_frsize as u64);
        if free.saturating_sub(need) < self.reserve {
            return Err(CapacityError::insufficient_storage(
                need,
                free,
                self.reserve,
            ));
        }
        self.file
            .write_all_at(&self.pending, self.len)
            .map_err(|e| CapacityError::spool_io(&e))?;
        self.len += need;
        self.pending.clear();
        if self.len - self.synced >= SYNC_EVERY {
            self.sync()?;
        }
        Ok(())
    }

    fn sync(&mut self) -> Result<(), CapacityError> {
        self.file
            .sync_data()
            .map_err(|e| CapacityError::spool_io(&e))?;
        drop_cache(&self.file, self.synced, self.len);
        self.synced = self.len;
        Ok(())
    }

    /// Write out the last batch and drop its page cache; the spool is
    /// read-only from here on.
    pub(crate) fn finish(&mut self) -> Result<(), CapacityError> {
        self.flush()?;
        self.sync()?;
        self.pending = Vec::new();
        Ok(())
    }

    /// Positional read, so attempts may overlap; the range's page cache is
    /// dropped once read.
    pub(crate) fn read_at(&self, buf: &mut [u8], offset: u64) -> io::Result<()> {
        self.file.read_exact_at(buf, offset)?;
        drop_cache(&self.file, offset, offset + buf.len() as u64);
        Ok(())
    }
}

#[cfg(test)]
#[path = "body_spool_tests.rs"]
mod tests;
