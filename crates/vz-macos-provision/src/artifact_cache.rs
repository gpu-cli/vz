//! Download exact, trusted artifact pins into a private content-addressed cache.
//!
//! The installer or signed catalog supplies [`Artifact`]; hashes supplied by an
//! unauthenticated download are not a trust root. Prepared images and mutable
//! Machine disks have separate ownership and are not managed by this cache.

use std::fs::{self, File, OpenOptions};
use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result, ensure};
use fs2::FileExt;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// An exact artifact selected from trusted, versioned release inputs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Artifact {
    /// HTTPS download URL; no embedded credentials or fragment are accepted.
    pub url: String,
    /// Lowercase hexadecimal SHA-256 of the complete downloaded bytes.
    pub sha256: String,
    /// Exact byte length; bounds disk use even if the server lies about size.
    pub size_bytes: u64,
}

impl Artifact {
    /// Validate a pin before filesystem or network effects.
    pub fn validate(&self) -> Result<()> {
        self.validate_url(false)
    }

    fn validate_url(&self, test_loopback: bool) -> Result<()> {
        ensure!(
            self.sha256.len() == 64
                && self
                    .sha256
                    .bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)),
            "artifact requires a lowercase SHA-256 pin"
        );
        ensure!(
            self.size_bytes > 0,
            "artifact requires an exact nonzero length"
        );
        let url = reqwest::Url::parse(&self.url).context("invalid artifact URL")?;
        ensure!(
            url.scheme() == "https"
                || (test_loopback && url.scheme() == "http" && url.host_str() == Some("127.0.0.1")),
            "artifact URL must use HTTPS"
        );
        ensure!(
            url.username().is_empty() && url.password().is_none() && url.fragment().is_none(),
            "artifact URL must not contain credentials or a fragment"
        );
        Ok(())
    }
}

/// User-visible preparation phases, independent of internal transport details.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Phase {
    /// Another caller is preparing this exact artifact.
    Waiting,
    /// Check a previously completed artifact before using it.
    VerifyingCache,
    /// Download and hash the artifact in one pass.
    Downloading,
    /// A verified artifact is available; subsequent calls need no network.
    Available,
}

/// Bounded scalar progress, suitable for forwarding over a gRPC stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Progress {
    /// Current phase.
    pub phase: Phase,
    /// Bytes completed in this phase.
    pub completed: u64,
    /// Exact expected byte count.
    pub total: u64,
}

/// Private cache for immutable downloaded blobs. Concurrent callers serialize
/// per digest; cancellation removes only that call's incomplete staging file.
pub struct ArtifactCache {
    root: PathBuf,
    client: reqwest::Client,
}

impl ArtifactCache {
    /// Open an absolute, caller-owned cache directory. It must be private and
    /// have no symlink ancestry; create its parent before calling this method.
    /// This does not select or alter a Machine or an Environment.
    pub fn new(root: PathBuf) -> Result<Self> {
        ensure!(root.is_absolute(), "artifact cache path must be absolute");
        for ancestor in root.ancestors() {
            match fs::symlink_metadata(ancestor) {
                Ok(m) => ensure!(
                    m.is_dir() && !m.file_type().is_symlink(),
                    "artifact cache ancestry must be directories without symlinks"
                ),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound && ancestor == root => {}
                Err(e) => return Err(e).context("inspect artifact cache ancestry"),
            }
        }
        let mut builder = fs::DirBuilder::new();
        #[cfg(unix)]
        {
            use std::os::unix::fs::DirBuilderExt;
            builder.mode(0o700);
        }
        match builder.create(&root) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(e) => return Err(e).context("create private artifact cache"),
        }
        let metadata = fs::symlink_metadata(&root)?;
        ensure!(
            metadata.is_dir() && !metadata.file_type().is_symlink(),
            "artifact cache must be a directory"
        );
        #[cfg(unix)]
        {
            use std::os::unix::fs::{MetadataExt, PermissionsExt};
            // SAFETY: geteuid only returns the caller's effective user identity.
            #[allow(unsafe_code)]
            let uid = unsafe { libc::geteuid() };
            ensure!(
                metadata.uid() == uid && metadata.permissions().mode() & 0o077 == 0,
                "artifact cache must be caller-owned and private"
            );
        }
        let client = reqwest::Client::builder()
            .https_only(true)
            .connect_timeout(Duration::from_secs(30))
            .read_timeout(Duration::from_secs(60))
            .timeout(Duration::from_secs(3600))
            .build()?;
        Ok(Self { root, client })
    }

    /// Obtain an exact artifact. The progress callback may return an error to
    /// cancel. A valid cache hit is checked locally and makes no HTTP request.
    /// Callers must retain the task until terminal completion or drop it to
    /// cancel; no background download is detached from the operation.
    pub async fn ensure(
        &self,
        artifact: &Artifact,
        progress: impl FnMut(Progress) -> Result<()>,
    ) -> Result<PathBuf> {
        artifact.validate()?;
        self.ensure_validated(artifact, progress).await
    }

    async fn ensure_validated(
        &self,
        artifact: &Artifact,
        mut progress: impl FnMut(Progress) -> Result<()>,
    ) -> Result<PathBuf> {
        let notify = |phase, completed| Progress {
            phase,
            completed,
            total: artifact.size_bytes,
        };
        let lock_path = self.root.join(format!("{}.lock", artifact.sha256));
        let mut options = OpenOptions::new();
        options.read(true).write(true).create(true).truncate(false);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options
                .mode(0o600)
                .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
        }
        let lock = options
            .open(lock_path)
            .context("open persistent artifact preparation lock")?;
        ensure!(
            lock.metadata()?.is_file(),
            "artifact lock must be a regular file"
        );
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            ensure!(
                lock.metadata()?.nlink() == 1,
                "artifact lock must not be hard-linked"
            );
        }
        loop {
            match lock.try_lock_exclusive() {
                Ok(()) => break,
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    progress(notify(Phase::Waiting, 0))?;
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
                Err(e) => return Err(e).context("lock artifact preparation"),
            }
        }
        let path = self.root.join(&artifact.sha256);
        match fs::symlink_metadata(&path) {
            Ok(m) => {
                ensure!(
                    m.is_file() && !m.file_type().is_symlink() && m.len() == artifact.size_bytes,
                    "cached artifact type or length mismatch"
                );
                let mut options = OpenOptions::new();
                options.read(true);
                #[cfg(unix)]
                {
                    use std::os::unix::fs::OpenOptionsExt;
                    options.custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
                }
                let cached = options.open(&path)?;
                ensure!(
                    cached.metadata()?.is_file(),
                    "cached artifact must be a regular file"
                );
                let mut file = tokio::fs::File::from_std(cached);
                let mut hasher = Sha256::new();
                let mut buffer = vec![0; 1024 * 1024];
                let mut done = 0;
                progress(notify(Phase::VerifyingCache, 0))?;
                loop {
                    let n = file.read(&mut buffer).await?;
                    if n == 0 {
                        break;
                    }
                    done += n as u64;
                    ensure!(
                        done <= artifact.size_bytes,
                        "cached artifact exceeded pinned length"
                    );
                    hasher.update(&buffer[..n]);
                    progress(notify(Phase::VerifyingCache, done))?;
                }
                ensure!(
                    done == artifact.size_bytes
                        && format!("{:x}", hasher.finalize()) == artifact.sha256,
                    "cached artifact digest mismatch"
                );
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                progress(notify(Phase::Downloading, 0))?;
                let staged = tempfile::NamedTempFile::new_in(&self.root)?;
                let mut output = tokio::fs::File::from_std(staged.reopen()?);
                let mut response = self
                    .client
                    .get(&artifact.url)
                    .send()
                    .await?
                    .error_for_status()?;
                if let Some(length) = response.content_length() {
                    ensure!(
                        length == artifact.size_bytes,
                        "server length differs from artifact pin"
                    );
                }
                let mut hasher = Sha256::new();
                let mut done: u64 = 0;
                while let Some(chunk) = response.chunk().await? {
                    done = done
                        .checked_add(chunk.len() as u64)
                        .context("download size overflow")?;
                    ensure!(
                        done <= artifact.size_bytes,
                        "download exceeded pinned length"
                    );
                    hasher.update(&chunk);
                    output.write_all(&chunk).await?;
                    progress(notify(Phase::Downloading, done))?;
                }
                ensure!(
                    done == artifact.size_bytes
                        && format!("{:x}", hasher.finalize()) == artifact.sha256,
                    "download digest or length mismatch"
                );
                output.flush().await?;
                output.sync_all().await?;
                drop(output);
                staged
                    .persist_noclobber(&path)
                    .map_err(|e| e.error)
                    .context("publish verified artifact")?;
                File::open(&self.root)?.sync_all()?;
            }
            Err(e) => return Err(e).context("inspect cached artifact"),
        }
        progress(notify(Phase::Available, artifact.size_bytes))?;
        // Dropping the descriptor releases the lock. Its pathname stays in place
        // so waiting callers can never acquire different lock inodes.
        drop(lock);
        Ok(path)
    }
}

#[cfg(test)]
mod tests;
