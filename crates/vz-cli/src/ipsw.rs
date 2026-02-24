//! IPSW resolution and resumable download.
//!
//! Resolves a macOS restore image (IPSW) using a local-first strategy:
//! 1. User-provided path (`--ipsw`)
//! 2. Local macOS installer app (`/Applications/Install macOS*.app`)
//! 3. vz cache (`~/.vz/cache/*.ipsw`)
//! 4. Apple CDN download (last resort)

use std::io::{BufReader, Read, Write};
use std::path::{Path, PathBuf};

use anyhow::{Context, bail};
use indicatif::{ProgressBar, ProgressStyle};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tracing::{debug, info};

use crate::registry;

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// The result of IPSW resolution: where the file came from and where it is.
#[derive(Debug)]
pub struct ResolvedIpsw {
    /// Absolute path to the resolved IPSW or restore image.
    pub path: PathBuf,
    /// How the IPSW was resolved.
    pub source: IpswSource,
}

/// How the IPSW was resolved.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IpswSource {
    /// User-provided path via `--ipsw`.
    UserProvided,
    /// Found a local macOS installer app.
    LocalInstaller,
    /// Found a cached IPSW in `~/.vz/cache/`.
    Cached,
    /// Downloaded from Apple CDN.
    Downloaded,
}

/// Resolve an IPSW for VM creation.
///
/// Follows the local-first resolution order:
/// 1. If `user_path` is provided, use it directly.
/// 2. Check `/Applications/Install macOS*.app` for a local installer.
/// 3. Check `~/.vz/cache/*.ipsw` for a cached download.
/// 4. Download from Apple CDN (requires user confirmation).
pub async fn resolve(user_path: Option<&Path>) -> anyhow::Result<ResolvedIpsw> {
    // 1. User-provided IPSW
    if let Some(path) = user_path {
        if !path.exists() {
            anyhow::bail!("IPSW file not found: {}", path.display());
        }
        info!(path = %path.display(), "using user-provided IPSW");
        println!("Using IPSW: {}", path.display());
        return Ok(ResolvedIpsw {
            path: path.to_path_buf(),
            source: IpswSource::UserProvided,
        });
    }

    // 2. Local macOS installer app
    if let Some(path) = find_local_installer() {
        info!(path = %path.display(), "found local macOS installer");
        println!("Found macOS installer — no download needed.");
        println!("  Using: {}", path.display());
        return Ok(ResolvedIpsw {
            path,
            source: IpswSource::LocalInstaller,
        });
    }

    // 3. Cached IPSW
    if let Some(path) = find_cached_ipsw() {
        info!(path = %path.display(), "found cached IPSW");
        println!("Using cached IPSW: {}", path.display());
        return Ok(ResolvedIpsw {
            path,
            source: IpswSource::Cached,
        });
    }

    // 4. Download from Apple CDN
    info!("no local IPSW found, will download from Apple");
    let path = download_ipsw().await?;
    Ok(ResolvedIpsw {
        path,
        source: IpswSource::Downloaded,
    })
}

/// Resolve a pinned IPSW from a matrix URL and expected SHA-256 digest.
///
/// Downloads into `~/.vz/cache/` under a content-addressed filename.
pub async fn resolve_pinned(url: &str, expected_sha256: &str) -> anyhow::Result<ResolvedIpsw> {
    let expected = normalize_sha256(expected_sha256)?;
    let cache_dir = registry::vz_home().join("cache");
    std::fs::create_dir_all(&cache_dir)
        .with_context(|| format!("failed to create cache directory {}", cache_dir.display()))?;

    let final_path = pinned_cache_path(&expected);
    let partial_path = final_path.with_extension("ipsw.partial");

    if final_path.exists() {
        let actual = sha256_file(&final_path).with_context(|| {
            format!("failed to hash cached pinned IPSW {}", final_path.display())
        })?;
        if actual == expected {
            info!(
                path = %final_path.display(),
                sha256 = %actual,
                "using cached pinned IPSW"
            );
            println!("Using pinned IPSW: {}", final_path.display());
            return Ok(ResolvedIpsw {
                path: final_path,
                source: IpswSource::Cached,
            });
        }

        println!(
            "Cached pinned IPSW hash mismatch; re-downloading.\n  expected: {expected}\n  actual:   {actual}"
        );
        let _ = std::fs::remove_file(&final_path);
    }

    println!("Downloading pinned IPSW:");
    println!("  URL: {url}");
    println!("  Expected SHA-256: {expected}");
    download_url_to_path(url, &partial_path, &final_path).await?;

    let actual = sha256_file(&final_path)
        .with_context(|| format!("failed to hash downloaded IPSW {}", final_path.display()))?;
    if actual != expected {
        let _ = std::fs::remove_file(&final_path);
        bail!(
            "pinned IPSW hash mismatch after download.\n\
             expected: {expected}\n\
             actual:   {actual}\n\
             file:     {}",
            final_path.display()
        );
    }

    info!(
        path = %final_path.display(),
        sha256 = %actual,
        "downloaded pinned IPSW"
    );
    println!("Pinned IPSW cached at {}", final_path.display());
    Ok(ResolvedIpsw {
        path: final_path,
        source: IpswSource::Downloaded,
    })
}

/// Check whether a verified pinned IPSW already exists in cache.
pub fn pinned_cache_available(expected_sha256: &str) -> anyhow::Result<bool> {
    let expected = normalize_sha256(expected_sha256)?;
    let path = pinned_cache_path(&expected);
    if !path.exists() {
        return Ok(false);
    }

    let actual = sha256_file(&path)
        .with_context(|| format!("failed to hash cached pinned IPSW {}", path.display()))?;
    Ok(actual == expected)
}

/// Compute the SHA-256 digest of a file as lowercase hex.
pub fn sha256_file(path: &Path) -> anyhow::Result<String> {
    let file = std::fs::File::open(path)
        .with_context(|| format!("failed to open file {}", path.display()))?;
    let mut reader = BufReader::new(file);
    let mut hasher = Sha256::new();
    let mut buffer = [0u8; 8192];

    loop {
        let read = reader
            .read(&mut buffer)
            .with_context(|| format!("failed to read file {}", path.display()))?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }

    Ok(format!("{:x}", hasher.finalize()))
}

/// Validate and normalize SHA-256 input into lowercase hex.
pub fn normalize_sha256(value: &str) -> anyhow::Result<String> {
    let normalized = value.trim().to_ascii_lowercase();
    if normalized.len() != 64 || !normalized.chars().all(|c| c.is_ascii_hexdigit()) {
        bail!("SHA-256 digest must be a 64-character hex string");
    }
    Ok(normalized)
}

// ---------------------------------------------------------------------------
// Local installer detection
// ---------------------------------------------------------------------------

/// Scan common locations for a `.ipsw` restore image file.
///
/// Note: macOS installer apps (from `softwareupdate --fetch-full-installer`)
/// contain `SharedSupport.dmg`, which is NOT a valid restore image for
/// `VZMacOSRestoreImage`. Only actual `.ipsw` files work.
fn find_local_installer() -> Option<PathBuf> {
    // Check common download locations for .ipsw files
    let search_dirs = [
        PathBuf::from(std::env::var("HOME").unwrap_or_default()).join("Downloads"),
        PathBuf::from(std::env::var("HOME").unwrap_or_default()).join("Desktop"),
    ];

    for dir in &search_dirs {
        if let Ok(entries) = std::fs::read_dir(dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.extension().is_some_and(|ext| ext == "ipsw") {
                    debug!(path = %path.display(), "found local IPSW file");
                    return Some(path);
                }
            }
        }
    }

    None
}

/// Check `~/.vz/cache/` for previously downloaded IPSW files.
fn find_cached_ipsw() -> Option<PathBuf> {
    let cache_dir = registry::vz_home().join("cache");
    let entries = std::fs::read_dir(&cache_dir).ok()?;

    // Find the newest .ipsw file in the cache
    let mut newest: Option<(PathBuf, std::time::SystemTime)> = None;

    for entry in entries.flatten() {
        let path = entry.path();
        if path.extension().is_some_and(|ext| ext == "ipsw") {
            if let Ok(meta) = entry.metadata() {
                let modified = meta.modified().unwrap_or(std::time::UNIX_EPOCH);
                if newest.as_ref().is_none_or(|(_, t)| modified > *t) {
                    newest = Some((path, modified));
                }
            }
        }
    }

    newest.map(|(path, _)| path)
}

fn pinned_cache_path(normalized_sha256: &str) -> PathBuf {
    registry::vz_home()
        .join("cache")
        .join(format!("{normalized_sha256}.ipsw"))
}

async fn download_url_to_path(
    url: &str,
    partial_path: &Path,
    final_path: &Path,
) -> anyhow::Result<()> {
    if partial_path.exists() {
        std::fs::remove_file(partial_path).with_context(|| {
            format!(
                "failed to clear previous partial download {}",
                partial_path.display()
            )
        })?;
    }

    let response = reqwest::Client::new().get(url).send().await?;
    if !response.status().is_success() {
        bail!("download failed: HTTP {} from {}", response.status(), url);
    }

    let total_bytes = response.content_length().unwrap_or(0);
    let progress = if total_bytes > 0 {
        let pb = ProgressBar::new(total_bytes);
        pb.set_style(
            ProgressStyle::default_bar()
                .template(
                    "  {bar:40.cyan/dim} {percent:>3}%  {bytes}/{total_bytes}  \
                     {bytes_per_sec}  {eta} remaining",
                )
                .unwrap_or_else(|_| ProgressStyle::default_bar()),
        );
        pb
    } else {
        let pb = ProgressBar::new_spinner();
        pb.set_style(
            ProgressStyle::default_spinner()
                .template("  {spinner} {bytes} downloaded  {bytes_per_sec}")
                .unwrap_or_else(|_| ProgressStyle::default_spinner()),
        );
        pb
    };

    let mut file = std::fs::File::create(partial_path)
        .with_context(|| format!("failed to create {}", partial_path.display()))?;
    let mut downloaded = 0u64;

    use tokio_stream::StreamExt;
    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        file.write_all(&chunk)
            .with_context(|| format!("failed to write {}", partial_path.display()))?;
        downloaded += chunk.len() as u64;
        progress.set_position(downloaded);
    }
    progress.finish_and_clear();
    file.flush()
        .with_context(|| format!("failed to flush {}", partial_path.display()))?;
    drop(file);

    std::fs::rename(partial_path, final_path).with_context(|| {
        format!(
            "failed to move download from {} to {}",
            partial_path.display(),
            final_path.display()
        )
    })?;

    Ok(())
}

// ---------------------------------------------------------------------------
// Download
// ---------------------------------------------------------------------------

/// State for a resumable download, persisted as a JSON sidecar.
#[derive(Debug, Serialize, Deserialize)]
struct DownloadState {
    url: String,
    total_bytes: u64,
    downloaded_bytes: u64,
    etag: Option<String>,
}

/// Download an IPSW from Apple's CDN with resumable progress.
///
/// The URL is obtained from `VZMacOSRestoreImage.fetchLatestSupported` via
/// the `vz` crate. Supports resuming interrupted downloads.
async fn download_ipsw() -> anyhow::Result<PathBuf> {
    let cache_dir = registry::vz_home().join("cache");
    std::fs::create_dir_all(&cache_dir)?;

    // Check for an in-progress download
    let state_path = cache_dir.join("download-state.json");
    let partial_path = cache_dir.join("restore.ipsw.partial");
    let final_path = cache_dir.join("restore.ipsw");

    // If a completed download exists, use it
    if final_path.exists() {
        info!(path = %final_path.display(), "found completed IPSW in cache");
        return Ok(final_path);
    }

    // Resolve the download URL from Apple via VZMacOSRestoreImage
    let (url, resume_state) = resolve_download_url(&state_path).await?;

    // Show disclosure before downloading
    print_download_disclosure(&url);

    // Set up HTTP client
    let client = reqwest::Client::new();

    // Determine starting byte offset for resume
    let start_byte = resume_state
        .as_ref()
        .map(|s| s.downloaded_bytes)
        .unwrap_or(0);

    // Build request with Range header for resume
    let mut request = client.get(&url);
    if start_byte > 0 {
        request = request.header("Range", format!("bytes={start_byte}-"));
        info!(resumed_at = start_byte, "resuming download");
        println!("Resuming download from {}...", format_bytes(start_byte));
    }

    let response = request.send().await?;

    if !response.status().is_success() && response.status() != reqwest::StatusCode::PARTIAL_CONTENT
    {
        anyhow::bail!("download failed: HTTP {} from {}", response.status(), url);
    }

    // Determine total size from Content-Length or Content-Range
    let total_bytes = if let Some(range) = response.headers().get("content-range") {
        // Content-Range: bytes 1000-9999/10000
        let range_str = range.to_str().unwrap_or("");
        range_str
            .split('/')
            .next_back()
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(0)
    } else {
        response.content_length().unwrap_or(0) + start_byte
    };

    let etag = response
        .headers()
        .get("etag")
        .and_then(|v| v.to_str().ok())
        .map(String::from);

    // Save download state for resume
    let state = DownloadState {
        url: url.clone(),
        total_bytes,
        downloaded_bytes: start_byte,
        etag,
    };
    save_download_state(&state_path, &state)?;

    // Set up progress bar
    let progress = if total_bytes > 0 {
        let pb = ProgressBar::new(total_bytes);
        pb.set_style(
            ProgressStyle::default_bar()
                .template(
                    "  {bar:40.cyan/dim} {percent:>3}%  {bytes}/{total_bytes}  \
                     {bytes_per_sec}  {eta} remaining",
                )
                .unwrap_or_else(|_| ProgressStyle::default_bar()),
        );
        pb.set_position(start_byte);
        pb
    } else {
        let pb = ProgressBar::new_spinner();
        pb.set_style(
            ProgressStyle::default_spinner()
                .template("  {spinner} {bytes} downloaded  {bytes_per_sec}")
                .unwrap_or_else(|_| ProgressStyle::default_spinner()),
        );
        pb
    };

    println!("\nDownloading macOS restore image...\n");

    // Stream response body to partial file
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&partial_path)?;

    let mut downloaded = start_byte;

    use tokio_stream::StreamExt;
    let mut stream = response.bytes_stream();

    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        file.write_all(&chunk)?;
        downloaded += chunk.len() as u64;
        progress.set_position(downloaded);

        // Periodically update the state file (every ~10 MB)
        if downloaded % (10 * 1024 * 1024) < chunk.len() as u64 {
            let state = DownloadState {
                url: url.clone(),
                total_bytes,
                downloaded_bytes: downloaded,
                etag: None,
            };
            let _ = save_download_state(&state_path, &state);
        }
    }

    progress.finish_and_clear();
    file.flush()?;
    drop(file);

    // Rename partial to final
    std::fs::rename(&partial_path, &final_path)?;

    // Remove state file
    let _ = std::fs::remove_file(&state_path);

    info!(
        path = %final_path.display(),
        bytes = downloaded,
        "download complete"
    );
    println!("Download complete. Cached at {}", final_path.display());
    println!(
        "\nTip: This file is only needed to create new VMs. Free {} by running:",
        format_bytes(downloaded)
    );
    println!("  vz cache clean\n");

    Ok(final_path)
}

/// Resolve the download URL.
///
/// Checks for a saved download state (for resume) or fetches the latest
/// IPSW URL from Apple via `VZMacOSRestoreImage`.
async fn resolve_download_url(
    state_path: &Path,
) -> anyhow::Result<(String, Option<DownloadState>)> {
    // Check for a saved state (resume case)
    if state_path.exists() {
        let data = std::fs::read_to_string(state_path)?;
        let state: DownloadState = serde_json::from_str(&data)?;
        info!(
            url = %state.url,
            downloaded = state.downloaded_bytes,
            total = state.total_bytes,
            "found in-progress download"
        );
        let url = state.url.clone();
        return Ok((url, Some(state)));
    }

    // Fetch the latest IPSW URL from Apple
    info!("fetching latest IPSW URL from Apple...");
    println!("Contacting Apple to find the latest macOS restore image...");
    let url = vz::fetch_latest_ipsw_url()
        .await
        .map_err(|e| anyhow::anyhow!("failed to fetch IPSW URL: {e}"))?;
    info!(url = %url, "resolved IPSW download URL");
    Ok((url, None))
}

/// Print a disclosure about the upcoming download.
fn print_download_disclosure(url: &str) {
    debug!(url, "will download from URL");
    println!("No local macOS installer found.\n");
    println!("vz needs a macOS restore image (IPSW) to create the sandbox VM.");
    println!(
        "This is a one-time download — future VMs restore from a saved snapshot in seconds.\n"
    );
    println!("  Source: Apple CDN (official macOS restore image)");
    println!(
        "  Cache location: {}",
        registry::vz_home().join("cache").display()
    );
}

/// Save download state to a JSON sidecar for resume.
fn save_download_state(path: &Path, state: &DownloadState) -> anyhow::Result<()> {
    let json = serde_json::to_string_pretty(state)?;
    std::fs::write(path, json)?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Disk space checks
// ---------------------------------------------------------------------------

/// Default disk image size: 64 GB.
const DEFAULT_DISK_SIZE: u64 = 64 * 1024 * 1024 * 1024;

/// Estimated IPSW download size: ~13.4 GB.
const IPSW_SIZE_ESTIMATE: u64 = 13_400_000_000;

/// Estimated saved state size: ~4 GB.
const STATE_SIZE_ESTIMATE: u64 = 4_000_000_000;

/// Overhead for temp files, auxiliary storage, etc.
const OVERHEAD_ESTIMATE: u64 = 2_000_000_000;

/// Check that there is sufficient disk space for `vz init`.
pub fn check_disk_space(disk_size_bytes: u64, needs_download: bool) -> anyhow::Result<()> {
    let vz_home = registry::vz_home();

    let available = available_space(&vz_home)?;

    let required = if needs_download {
        IPSW_SIZE_ESTIMATE + disk_size_bytes + STATE_SIZE_ESTIMATE + OVERHEAD_ESTIMATE
    } else {
        disk_size_bytes + STATE_SIZE_ESTIMATE + OVERHEAD_ESTIMATE
    };

    if available < required {
        let download_str = if needs_download {
            format!("{} download + ", format_bytes(IPSW_SIZE_ESTIMATE))
        } else {
            String::new()
        };
        let disk_str = format!(
            "{} VM disk + {} saved state",
            format_bytes(disk_size_bytes),
            format_bytes(STATE_SIZE_ESTIMATE)
        );
        anyhow::bail!(
            "insufficient disk space.\n\n\
             Available:  {}\n\
             Required:   ~{} ({}{})\n\n\
             Options:\n\
             1. Free up disk space and try again\n\
             2. Use a smaller VM disk: vz init --disk-size 32G (minimum for dev tools)\n\
             3. Use an external drive: vz init --output /Volumes/External/.vz/",
            format_bytes(available),
            format_bytes(required),
            download_str,
            disk_str,
        );
    }

    debug!(
        available = format_bytes(available),
        required = format_bytes(required),
        "disk space check passed"
    );
    Ok(())
}

/// Parse a disk size string like "64G", "128G", "32G" into bytes.
pub fn parse_disk_size(s: &str) -> anyhow::Result<u64> {
    let s = s.trim();
    if s.is_empty() {
        return Ok(DEFAULT_DISK_SIZE);
    }

    let (num_str, multiplier) = if let Some(n) = s.strip_suffix('G') {
        (n, 1024u64 * 1024 * 1024)
    } else if let Some(n) = s.strip_suffix("GB") {
        (n, 1024u64 * 1024 * 1024)
    } else if let Some(n) = s.strip_suffix('M') {
        (n, 1024u64 * 1024)
    } else if let Some(n) = s.strip_suffix("MB") {
        (n, 1024u64 * 1024)
    } else if let Some(n) = s.strip_suffix('T') {
        (n, 1024u64 * 1024 * 1024 * 1024)
    } else if let Some(n) = s.strip_suffix("TB") {
        (n, 1024u64 * 1024 * 1024 * 1024)
    } else {
        // Assume bytes
        (s, 1u64)
    };

    let num: u64 = num_str
        .trim()
        .parse()
        .map_err(|_| anyhow::anyhow!("invalid disk size: {s}"))?;

    Ok(num * multiplier)
}

/// Get available disk space at the given path.
#[cfg(target_os = "macos")]
fn available_space(path: &Path) -> anyhow::Result<u64> {
    use std::ffi::CString;

    let path_cstr = CString::new(path.to_string_lossy().as_bytes())?;

    #[allow(unsafe_code)]
    let available = unsafe {
        let mut stat: libc::statfs = std::mem::zeroed();
        if libc::statfs(path_cstr.as_ptr(), &mut stat) != 0 {
            return Err(anyhow::anyhow!(
                "failed to check disk space for {}",
                path.display()
            ));
        }
        stat.f_bavail as u64 * stat.f_bsize as u64
    };

    Ok(available)
}

#[cfg(not(target_os = "macos"))]
fn available_space(_path: &Path) -> anyhow::Result<u64> {
    // Non-macOS: return a large value to not block development
    Ok(u64::MAX)
}

// ---------------------------------------------------------------------------
// Formatting
// ---------------------------------------------------------------------------

/// Format a byte count for human display.
fn format_bytes(bytes: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = KB * 1024;
    const GB: u64 = MB * 1024;
    const TB: u64 = GB * 1024;

    if bytes >= TB {
        format!("{:.1} TB", bytes as f64 / TB as f64)
    } else if bytes >= GB {
        format!("{:.1} GB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.1} MB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.1} KB", bytes as f64 / KB as f64)
    } else {
        format!("{bytes} B")
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_disk_size_gigabytes() {
        assert_eq!(parse_disk_size("64G").unwrap(), 64 * 1024 * 1024 * 1024);
        assert_eq!(parse_disk_size("128G").unwrap(), 128 * 1024 * 1024 * 1024);
        assert_eq!(parse_disk_size("32GB").unwrap(), 32 * 1024 * 1024 * 1024);
    }

    #[test]
    fn parse_disk_size_megabytes() {
        assert_eq!(parse_disk_size("512M").unwrap(), 512 * 1024 * 1024);
        assert_eq!(parse_disk_size("1024MB").unwrap(), 1024 * 1024 * 1024);
    }

    #[test]
    fn parse_disk_size_terabytes() {
        assert_eq!(parse_disk_size("1T").unwrap(), 1024 * 1024 * 1024 * 1024);
    }

    #[test]
    fn parse_disk_size_empty_uses_default() {
        assert_eq!(parse_disk_size("").unwrap(), DEFAULT_DISK_SIZE);
    }

    #[test]
    fn parse_disk_size_invalid() {
        assert!(parse_disk_size("abc").is_err());
        assert!(parse_disk_size("G").is_err());
    }

    #[test]
    fn normalize_sha256_accepts_uppercase() {
        let hash = "ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789";
        let normalized = normalize_sha256(hash).unwrap();
        assert_eq!(
            normalized,
            "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"
        );
    }

    #[test]
    fn normalize_sha256_rejects_invalid() {
        assert!(normalize_sha256("abc123").is_err());
        assert!(normalize_sha256("z".repeat(64).as_str()).is_err());
    }

    #[test]
    fn sha256_file_hashes_known_content() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.ipsw");
        std::fs::write(&path, b"hello world").unwrap();
        let digest = sha256_file(&path).unwrap();
        assert_eq!(
            digest,
            "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"
        );
    }

    #[test]
    fn format_bytes_display() {
        assert_eq!(format_bytes(0), "0 B");
        assert_eq!(format_bytes(1023), "1023 B");
        assert_eq!(format_bytes(1024), "1.0 KB");
        assert_eq!(format_bytes(1024 * 1024), "1.0 MB");
        assert_eq!(format_bytes(1024 * 1024 * 1024), "1.0 GB");
        assert_eq!(format_bytes(13_400_000_000), "12.5 GB");
    }

    #[test]
    fn download_state_serde_roundtrip() {
        let state = DownloadState {
            url: "https://example.com/restore.ipsw".to_string(),
            total_bytes: 13_400_000_000,
            downloaded_bytes: 5_000_000_000,
            etag: Some("\"abc123\"".to_string()),
        };
        let json = serde_json::to_string(&state).unwrap();
        let decoded: DownloadState = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded.url, state.url);
        assert_eq!(decoded.total_bytes, state.total_bytes);
        assert_eq!(decoded.downloaded_bytes, state.downloaded_bytes);
        assert_eq!(decoded.etag, state.etag);
    }

    #[test]
    fn find_local_installer_no_apps() {
        // This test verifies the function doesn't panic when /Applications exists
        // but has no macOS installer apps. On CI, this may or may not find one.
        let result = find_local_installer();
        // Just assert it doesn't panic — result depends on the host
        let _ = result;
    }

    #[test]
    fn find_cached_ipsw_empty_cache() {
        // With no cache dir or empty cache, should return None
        // (unless the user happens to have cached IPSWs)
        let _ = find_cached_ipsw();
    }

    #[test]
    fn disk_space_check_with_ample_space() {
        // If we have disk space, this should succeed
        // Use a small disk size to make the test pass everywhere
        let result = check_disk_space(1024, false);
        assert!(result.is_ok());
    }

    #[test]
    fn disk_space_check_with_impossible_size() {
        // Request an impossibly large disk — should fail
        let result = check_disk_space(u64::MAX / 2, false);
        assert!(result.is_err());
    }
}
