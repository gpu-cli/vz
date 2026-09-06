//! Offline, unprivileged image deltas for maintainer-prepared macOS bases.
//!
//! Compatible with the retired CLI's `VZDELTA1` disk format. These APIs never
//! mount disks, invoke sudo, or copy platform identity sidecars. A delta applies
//! only to its exact base bytes; an IPSW version does not establish that identity.
//! Authenticate the distributed delta separately before calling [`apply`].

use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tempfile::NamedTempFile;

const MAGIC: &[u8; 8] = b"VZDELTA1";
const MAX_CHUNK: u32 = 64 * 1024 * 1024;

/// Work phases suitable for a CLI progress bar or streamed API event.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Phase {
    /// Compare base and prepared image to produce the maintainer artifact.
    CreatingPatch,
    /// Check the complete base before writing the staging image.
    VerifyingBase,
    /// Copy the base into a private staging image.
    CopyingBase,
    /// Apply changed chunks to the staging image.
    ApplyingPatch,
    /// Check the complete patched image before publishing it.
    VerifyingOutput,
}

/// Progress counts are local to a phase. Returning an error from the callback
/// cancels work and discards staging files without publishing an output.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Progress {
    /// Current preparation phase.
    pub phase: Phase,
    /// Bytes or chunks completed, as indicated by the phase (patching uses chunks).
    pub completed: u64,
    /// Total bytes or chunks for this phase.
    pub total: u64,
}

/// Content identities captured in a VZDELTA1 header, independent of VM identity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeltaInfo {
    /// Maximum uncompressed bytes in a changed chunk.
    pub chunk_size: u32,
    /// Required base length.
    pub base_size: u64,
    /// Resulting image length.
    pub target_size: u64,
    /// SHA-256 of the complete base image.
    pub base_sha256: [u8; 32],
    /// SHA-256 of the complete prepared image.
    pub target_sha256: [u8; 32],
    /// Number of changed target chunks.
    pub changed_chunks: u64,
}

fn report(
    cb: &mut impl FnMut(Progress) -> Result<()>,
    phase: Phase,
    completed: u64,
    total: u64,
) -> Result<()> {
    cb(Progress {
        phase,
        completed,
        total,
    })
}

fn open_input(path: &Path) -> Result<File> {
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
    }
    let file = options
        .open(path)
        .with_context(|| format!("open image input {}", path.display()))?;
    ensure!(
        file.metadata()?.is_file(),
        "input must be a regular file: {}",
        path.display()
    );
    Ok(file)
}

fn stage(path: &Path) -> Result<NamedTempFile> {
    ensure!(
        fs::symlink_metadata(path).is_err_and(|e| e.kind() == std::io::ErrorKind::NotFound),
        "output already exists or cannot be inspected: {}",
        path.display()
    );
    let parent = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    NamedTempFile::new_in(parent).context("create private image staging file")
}

fn publish(file: NamedTempFile, destination: &Path) -> Result<()> {
    file.as_file().sync_all()?;
    file.persist_noclobber(destination)
        .map_err(|e| e.error)
        .context("publish image without replacing existing output")?;
    let parent = destination
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    File::open(parent)?
        .sync_all()
        .context("sync image output directory")
}

fn hash(
    file: &mut File,
    phase: Phase,
    cb: &mut impl FnMut(Progress) -> Result<()>,
) -> Result<[u8; 32]> {
    file.rewind()?;
    let total = file.metadata()?.len();
    let mut hasher = Sha256::new();
    let mut buffer = vec![0; 1024 * 1024];
    let mut done = 0;
    report(cb, phase, 0, total)?;
    loop {
        let n = file.read(&mut buffer)?;
        if n == 0 {
            break;
        }
        done += n as u64;
        ensure!(done <= total, "image size changed during verification");
        hasher.update(&buffer[..n]);
        report(cb, phase, done, total)?;
    }
    ensure!(done == total, "image size changed during verification");
    Ok(hasher.finalize().into())
}

/// Create a patch from a quiescent exact base and a manually prepared image.
/// Publishes only a complete artifact; no existing output is overwritten.
/// The caller must keep both inputs unchanged throughout this operation.
pub fn create(
    base: &Path,
    prepared: &Path,
    output: &Path,
    chunk_size: u32,
    mut progress: impl FnMut(Progress) -> Result<()>,
) -> Result<DeltaInfo> {
    ensure!(
        chunk_size > 0 && chunk_size <= MAX_CHUNK,
        "chunk size must be 1..=64 MiB in bytes"
    );
    let mut base = open_input(base)?;
    let mut target = open_input(prepared)?;
    let mut staged = stage(output)?;
    let mut info = DeltaInfo {
        chunk_size,
        base_size: base.metadata()?.len(),
        target_size: target.metadata()?.len(),
        base_sha256: [0; 32],
        target_sha256: [0; 32],
        changed_chunks: 0,
    };
    write_header(staged.as_file_mut(), &info)?;
    let total = info.base_size.max(info.target_size);
    let mut base_hash = Sha256::new();
    let mut target_hash = Sha256::new();
    let mut offset = 0;
    report(&mut progress, Phase::CreatingPatch, 0, total)?;
    while offset < total {
        let bn = info
            .base_size
            .saturating_sub(offset)
            .min(u64::from(chunk_size)) as usize;
        let tn = info
            .target_size
            .saturating_sub(offset)
            .min(u64::from(chunk_size)) as usize;
        let mut b = vec![0; bn];
        let mut t = vec![0; tn];
        base.read_exact(&mut b)?;
        target.read_exact(&mut t)?;
        base_hash.update(&b);
        target_hash.update(&t);
        if b != t && tn > 0 {
            let compressed = zstd::stream::encode_all(t.as_slice(), 0)?;
            staged.write_all(&(offset / u64::from(chunk_size)).to_le_bytes())?;
            staged.write_all(&(tn as u32).to_le_bytes())?;
            staged.write_all(&u32::try_from(compressed.len())?.to_le_bytes())?;
            staged.write_all(&compressed)?;
            info.changed_chunks += 1;
        }
        offset = offset.saturating_add(u64::from(chunk_size)).min(total);
        report(&mut progress, Phase::CreatingPatch, offset, total)?;
    }
    ensure!(
        base.metadata()?.len() == info.base_size && target.metadata()?.len() == info.target_size,
        "input size changed while creating patch"
    );
    info.base_sha256 = base_hash.finalize().into();
    info.target_sha256 = target_hash.finalize().into();
    staged.rewind()?;
    write_header(staged.as_file_mut(), &info)?;
    publish(staged, output)?;
    Ok(info)
}

/// Apply an authenticated patch to its exact base without mounting or elevation.
/// Cancellation, malformed records and hash failures discard the staging image.
/// Does not create or reuse a Machine's platform identity or auxiliary storage.
pub fn apply(
    base: &Path,
    patch: &Path,
    output: &Path,
    mut progress: impl FnMut(Progress) -> Result<()>,
) -> Result<DeltaInfo> {
    let mut base = open_input(base)?;
    let mut patch = open_input(patch)?;
    let info = read_header(&mut patch)?;
    ensure!(
        base.metadata()?.len() == info.base_size,
        "base image size mismatch"
    );
    ensure!(
        hash(&mut base, Phase::VerifyingBase, &mut progress)? == info.base_sha256,
        "base image digest mismatch"
    );
    let mut staged = stage(output)?;
    base.rewind()?;
    let mut buf = vec![0; 1024 * 1024];
    let mut copied = 0;
    report(&mut progress, Phase::CopyingBase, 0, info.base_size)?;
    while copied < info.base_size {
        let limit = (info.base_size - copied).min(buf.len() as u64) as usize;
        base.read_exact(&mut buf[..limit])?;
        // Preserve large zero extents instead of allocating an entire sparse disk.
        if buf[..limit].iter().all(|b| *b == 0) {
            staged.seek(SeekFrom::Current(limit as i64))?;
        } else {
            staged.write_all(&buf[..limit])?;
        }
        copied += limit as u64;
        report(&mut progress, Phase::CopyingBase, copied, info.base_size)?;
    }
    staged.as_file().set_len(info.target_size)?;
    let mut previous = None;
    report(&mut progress, Phase::ApplyingPatch, 0, info.changed_chunks)?;
    for index in 0..info.changed_chunks {
        let chunk = read_u64(&mut patch)?;
        ensure!(
            previous.is_none_or(|p| chunk > p),
            "patch chunks must be strictly increasing"
        );
        previous = Some(chunk);
        let target_len = read_u32(&mut patch)?;
        let compressed_len = read_u32(&mut patch)?;
        ensure!(
            target_len > 0 && target_len <= info.chunk_size,
            "invalid target chunk length"
        );
        ensure!(
            compressed_len > 0 && compressed_len <= info.chunk_size * 2 + 1024,
            "invalid compressed chunk length"
        );
        let offset = chunk
            .checked_mul(u64::from(info.chunk_size))
            .context("chunk offset overflow")?;
        let end = offset
            .checked_add(u64::from(target_len))
            .context("chunk end overflow")?;
        ensure!(end <= info.target_size, "chunk extends past target image");
        let mut compressed = vec![0; compressed_len as usize];
        patch.read_exact(&mut compressed)?;
        let mut decoder = zstd::stream::read::Decoder::new(compressed.as_slice())?;
        decoder.window_log_max(26)?;
        let mut decoded = Vec::with_capacity(target_len as usize);
        decoder
            .take(u64::from(target_len) + 1)
            .read_to_end(&mut decoded)?;
        ensure!(
            decoded.len() == target_len as usize,
            "decompressed chunk length mismatch"
        );
        staged.seek(SeekFrom::Start(offset))?;
        staged.write_all(&decoded)?;
        report(
            &mut progress,
            Phase::ApplyingPatch,
            index + 1,
            info.changed_chunks,
        )?;
    }
    let mut trailing = [0];
    ensure!(
        patch.read(&mut trailing)? == 0,
        "unexpected trailing patch data"
    );
    staged.flush()?;
    ensure!(
        hash(staged.as_file_mut(), Phase::VerifyingOutput, &mut progress)? == info.target_sha256,
        "patched output digest mismatch"
    );
    publish(staged, output)?;
    Ok(info)
}

fn write_header(w: &mut File, info: &DeltaInfo) -> Result<()> {
    w.write_all(MAGIC)?;
    w.write_all(&1u32.to_le_bytes())?;
    w.write_all(&info.chunk_size.to_le_bytes())?;
    w.write_all(&info.base_size.to_le_bytes())?;
    w.write_all(&info.target_size.to_le_bytes())?;
    w.write_all(&info.base_sha256)?;
    w.write_all(&info.target_sha256)?;
    w.write_all(&info.changed_chunks.to_le_bytes())?;
    Ok(())
}

fn read_u32(r: &mut File) -> Result<u32> {
    let mut b = [0; 4];
    r.read_exact(&mut b)?;
    Ok(u32::from_le_bytes(b))
}
fn read_u64(r: &mut File) -> Result<u64> {
    let mut b = [0; 8];
    r.read_exact(&mut b)?;
    Ok(u64::from_le_bytes(b))
}

fn read_header(r: &mut File) -> Result<DeltaInfo> {
    let mut magic = [0; 8];
    r.read_exact(&mut magic)?;
    ensure!(
        &magic == MAGIC && read_u32(r)? == 1,
        "unsupported image delta format"
    );
    let chunk_size = read_u32(r)?;
    ensure!(
        chunk_size > 0 && chunk_size <= MAX_CHUNK,
        "invalid delta chunk size"
    );
    let base_size = read_u64(r)?;
    let target_size = read_u64(r)?;
    let mut base_sha256 = [0; 32];
    r.read_exact(&mut base_sha256)?;
    let mut target_sha256 = [0; 32];
    r.read_exact(&mut target_sha256)?;
    let changed_chunks = read_u64(r)?;
    ensure!(
        changed_chunks <= target_size.div_ceil(u64::from(chunk_size)),
        "too many changed chunks"
    );
    Ok(DeltaInfo {
        chunk_size,
        base_size,
        target_size,
        base_sha256,
        target_sha256,
        changed_chunks,
    })
}

#[cfg(test)]
mod tests;
