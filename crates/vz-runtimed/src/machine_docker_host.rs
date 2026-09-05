//! Bounded host Docker commands for an explicitly selected Machine context.
//! Context management never changes Docker's current/default context. A client
//! failure or cancellation is not proof that daemon-side effects are absent.

use anyhow::{Context, Result, bail, ensure};
use sha2::{Digest, Sha256};
use std::fs::{File, Metadata};
use std::io::Read;
use std::os::unix::fs::{DirBuilderExt, MetadataExt};
use std::os::unix::process::CommandExt;
use std::path::{Component, Path, PathBuf};
use std::process::{ExitStatus, Stdio};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio::process::Command;

use crate::machine_runtime_registry::open_trusted_registry_root;

const OUTPUT_LIMIT: u64 = 4 * 1024 * 1024;

#[derive(Debug, Clone)]
pub struct HostDockerClient {
    executable: PathBuf,
    executable_sha256: String,
    config_dir: PathBuf,
}

#[derive(Debug)]
pub struct HostDockerOutput {
    pub status: ExitStatus,
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
}

impl HostDockerOutput {
    pub fn success(self) -> Result<Self> {
        ensure!(
            self.status.success(),
            "host Docker exited {}: {}",
            self.status,
            String::from_utf8_lossy(&self.stderr)
                .chars()
                .take(2048)
                .collect::<String>()
        );
        Ok(self)
    }
}

impl HostDockerClient {
    /// Resolve the normal host installation, never another Docker daemon.
    /// An explicit client/config override is authoritative and never falls back.
    pub fn discover() -> Result<Self> {
        let executable = if let Some(explicit) = std::env::var_os("VZ_DOCKER_CLIENT") {
            let path = PathBuf::from(explicit);
            ensure!(
                path.is_absolute(),
                "VZ_DOCKER_CLIENT must be an absolute executable path"
            );
            path
        } else {
            let search =
                std::env::var_os("PATH").context("host Docker client is not configured")?;
            std::env::split_paths(&search)
                .filter(|directory| directory.is_absolute())
                .map(|directory| directory.join("docker"))
                .find(|path| path.is_file())
                .context("install a supported host Docker client or set VZ_DOCKER_CLIENT")?
        };
        let config = std::env::var_os("VZ_DOCKER_CONFIG")
            .or_else(|| std::env::var_os("DOCKER_CONFIG"))
            .map(PathBuf::from)
            .or_else(|| std::env::var_os("HOME").map(|home| PathBuf::from(home).join(".docker")))
            .context("host Docker configuration directory is not configured")?;
        Self::new(&executable, &config)
    }

    pub fn new(executable: &Path, config_dir: &Path) -> Result<Self> {
        ensure!(
            executable.is_absolute() && config_dir.is_absolute(),
            "Docker paths must be absolute"
        );
        let executable =
            std::fs::canonicalize(executable).context("resolve host Docker executable")?;
        let executable_sha256 = executable_digest(&executable)?;
        let parent = config_dir.parent().context("Docker config has no parent")?;
        ensure!(
            std::fs::canonicalize(parent)? == parent,
            "Docker config parent must be canonical"
        );
        let config_parent = open_trusted_registry_root(parent)?;
        let config_parent_mode = config_parent.metadata()?.mode();
        ensure!(
            config_parent_mode & 0o022 == 0 || config_parent_mode & 0o1000 != 0,
            "Docker config parent must not be non-sticky group/world-writable"
        );
        match std::fs::symlink_metadata(config_dir) {
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                match std::fs::DirBuilder::new().mode(0o700).create(config_dir) {
                    Ok(()) => File::open(parent)?.sync_all()?,
                    Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
                    Err(error) => return Err(error.into()),
                }
            }
            Err(error) => return Err(error.into()),
        }
        validate_config(config_dir)?;
        Ok(Self {
            executable,
            executable_sha256,
            config_dir: config_dir.into(),
        })
    }

    pub fn config_dir(&self) -> &Path {
        &self.config_dir
    }
    pub fn executable(&self) -> &Path {
        &self.executable
    }
    pub fn executable_sha256(&self) -> &str {
        &self.executable_sha256
    }

    /// The retained task drains both bounded pipes and reaps its owned process
    /// group, even if the observing future is dropped. Callers journal mutation
    /// intent before invoking this method and reconcile uncertain outcomes.
    pub async fn run(
        &self,
        context: Option<&str>,
        args: &[String],
        input: Option<File>,
        timeout: Duration,
    ) -> Result<HostDockerOutput> {
        if let Some(context) = context {
            ensure!(
                !context.is_empty()
                    && context != "default"
                    && context.len() <= 256
                    && !context.chars().any(char::is_whitespace),
                "an explicit non-default Machine context is required"
            );
        }
        if context.is_none() {
            let words: Vec<_> = args.iter().map(String::as_str).collect();
            ensure!(
                matches!(
                    words.as_slice(),
                    ["--version"]
                        | ["compose", "version", ..]
                        | ["buildx", "version", ..]
                        | ["context", "inspect", ..]
                        | ["context", "create", ..]
                ),
                "a Machine context is required for every Engine operation"
            );
        }
        ensure!(
            !timeout.is_zero() && timeout <= Duration::from_secs(300),
            "invalid host Docker deadline"
        );
        ensure!(
            executable_digest(&self.executable)? == self.executable_sha256,
            "host Docker executable changed"
        );
        validate_config(&self.config_dir)?;
        let mut command = Command::new(&self.executable);
        command.as_std_mut().arg0("docker").process_group(0);
        command
            .arg("--config")
            .arg(&self.config_dir)
            .arg("--context")
            .arg(context.unwrap_or("default"))
            .args(args)
            .stdin(input.map_or_else(Stdio::null, Stdio::from))
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        for (name, _) in std::env::vars_os() {
            let text = name.to_string_lossy();
            if ["DOCKER_", "COMPOSE_", "BUILDX_"]
                .iter()
                .any(|prefix| text.starts_with(prefix))
            {
                command.env_remove(name);
            }
        }
        for name in [
            "HTTP_PROXY",
            "HTTPS_PROXY",
            "ALL_PROXY",
            "http_proxy",
            "https_proxy",
            "all_proxy",
        ] {
            command.env_remove(name);
        }
        tokio::spawn(async move {
            let mut child = command.spawn().context("spawn exact host Docker client")?;
            let pid = rustix::process::Pid::from_raw(i32::try_from(
                child.id().context("host Docker PID unavailable")?,
            )?)
            .context("invalid host Docker PID")?;
            let stdout = child.stdout.take().context("host Docker stdout missing")?;
            let stderr = child.stderr.take().context("host Docker stderr missing")?;
            let result = tokio::time::timeout(timeout, async {
                // Do not reap the group leader before both streams are drained:
                // on timeout the still-owned PID safely identifies this group.
                let (stdout, stderr) = tokio::try_join!(bounded(stdout), bounded(stderr))?;
                let status = child.wait().await?;
                Ok::<_, anyhow::Error>(HostDockerOutput {
                    status,
                    stdout,
                    stderr,
                })
            })
            .await;
            match result {
                Ok(Ok(output)) => Ok(output),
                failure => {
                    match rustix::process::kill_process_group(pid, rustix::process::Signal::KILL) {
                        Ok(()) | Err(rustix::io::Errno::SRCH) => {}
                        Err(error) => {
                            return Err(error).context(
                                "cannot terminate owned host Docker group; effects uncertain",
                            );
                        }
                    }
                    child
                        .wait()
                        .await
                        .context("reap owned host Docker client")?;
                    match failure {
                        Err(_) => bail!(
                            "host Docker deadline exceeded; daemon-side effects remain uncertain"
                        ),
                        Ok(Err(error)) => Err(error).context(
                            "host Docker output failed; daemon-side effects remain uncertain",
                        ),
                        Ok(Ok(_)) => unreachable!(),
                    }
                }
            }
        })
        .await
        .context("host Docker command supervisor failed")?
    }
}

async fn bounded(pipe: impl AsyncRead + Unpin) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    pipe.take(OUTPUT_LIMIT + 1).read_to_end(&mut bytes).await?;
    ensure!(
        bytes.len() as u64 <= OUTPUT_LIMIT,
        "host Docker stream exceeds 4 MiB limit"
    );
    Ok(bytes)
}

fn validate_config(path: &Path) -> Result<()> {
    let directory = open_trusted_registry_root(path)?;
    let metadata = directory.metadata()?;
    ensure!(
        metadata.uid() == rustix::process::geteuid().as_raw() && metadata.mode() & 0o022 == 0,
        "Docker config directory must be effective-user-owned and not group/world-writable"
    );
    Ok(())
}

fn executable_digest(path: &Path) -> Result<String> {
    executable_digest_with_checkpoint(path, || Ok(()))
}

/// Native executable trust is intentionally separate from private runtime/config
/// storage. macOS administrators are trusted to replace installed applications:
/// only the actual root:admin (Darwin gid 80) /Applications mode 0775 boundary
/// permits non-sticky group write. No nested app directory, lookalike path, other
/// group or world-writable installation receives this exception. Root and the
/// effective user remain trusted principals, as with the pre-existing policy.
fn executable_directory_allowed(path: &Path, uid: u32, gid: u32, mode: u32, euid: u32) -> bool {
    if mode & 0o170000 != 0o040000 || ![0, euid].contains(&uid) {
        return false;
    }
    if mode & 0o022 == 0 || mode & 0o1000 != 0 {
        return true;
    }
    cfg!(target_os = "macos")
        && path == Path::new("/Applications")
        && uid == 0
        && gid == 80
        && mode & 0o7777 == 0o775
}

fn open_executable_parent(path: &Path) -> Result<File> {
    ensure!(
        path.is_absolute(),
        "Docker executable parent must be absolute"
    );
    let flags = rustix::fs::OFlags::RDONLY
        | rustix::fs::OFlags::DIRECTORY
        | rustix::fs::OFlags::NOFOLLOW
        | rustix::fs::OFlags::CLOEXEC;
    let mut directory = File::from(rustix::fs::open("/", flags, rustix::fs::Mode::empty())?);
    let mut current = PathBuf::from("/");
    let check = |directory: &File, current: &Path| -> Result<()> {
        let metadata = directory.metadata()?;
        ensure!(
            executable_directory_allowed(
                current,
                metadata.uid(),
                metadata.gid(),
                metadata.mode(),
                rustix::process::geteuid().as_raw()
            ),
            "untrusted host Docker executable ancestry at {}",
            current.display()
        );
        Ok(())
    };
    check(&directory, &current)?;
    for component in path.components() {
        match component {
            Component::RootDir => {}
            Component::Normal(name) => {
                let child = File::from(rustix::fs::openat(
                    &directory,
                    name,
                    flags,
                    rustix::fs::Mode::empty(),
                )?);
                current.push(name);
                check(&child, &current)?;
                directory = child;
            }
            _ => bail!("Docker executable path must not contain traversal"),
        }
    }
    Ok(directory)
}

fn same_executable_metadata(before: &Metadata, after: &Metadata) -> bool {
    before.dev() == after.dev()
        && before.ino() == after.ino()
        && before.uid() == after.uid()
        && before.gid() == after.gid()
        && before.mode() == after.mode()
        && before.nlink() == after.nlink()
        && before.len() == after.len()
        && before.mtime() == after.mtime()
        && before.mtime_nsec() == after.mtime_nsec()
        && before.ctime() == after.ctime()
        && before.ctime_nsec() == after.ctime_nsec()
}

fn executable_digest_with_checkpoint(
    path: &Path,
    after_read: impl FnOnce() -> Result<()>,
) -> Result<String> {
    let parent_path = path.parent().context("Docker executable has no parent")?;
    let parent = open_executable_parent(parent_path)?;
    let name = path.file_name().context("Docker executable name missing")?;
    let flags = rustix::fs::OFlags::RDONLY
        | rustix::fs::OFlags::NOFOLLOW
        | rustix::fs::OFlags::NONBLOCK
        | rustix::fs::OFlags::CLOEXEC;
    let mut file = File::from(rustix::fs::openat(
        &parent,
        name,
        flags,
        rustix::fs::Mode::empty(),
    )?);
    let before = file.metadata()?;
    ensure!(
        before.is_file()
            && before.nlink() == 1
            && before.mode() & 0o111 != 0
            && before.mode() & 0o022 == 0
            && before.len() <= 512 * 1024 * 1024
            && [0, rustix::process::geteuid().as_raw()].contains(&before.uid()),
        "untrusted host Docker executable"
    );
    let mut hasher = Sha256::new();
    let mut buffer = [0u8; 65536];
    loop {
        let count = file.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        hasher.update(&buffer[..count]);
    }
    after_read()?;
    let after = file.metadata()?;
    ensure!(
        same_executable_metadata(&before, &after),
        "host Docker executable changed during verification"
    );
    // Re-resolve the trusted chain and compare the retained directory and named
    // entry after hashing. This closes check/read replacement windows; subsequent
    // changes by trusted root/euid/administrators remain the stated trust boundary.
    let resolved_parent = open_executable_parent(parent_path)?;
    let original_parent = parent.metadata()?;
    let current_parent = resolved_parent.metadata()?;
    ensure!(
        original_parent.dev() == current_parent.dev()
            && original_parent.ino() == current_parent.ino(),
        "host Docker executable parent was replaced during verification"
    );
    let current = File::from(rustix::fs::openat(
        &resolved_parent,
        name,
        flags,
        rustix::fs::Mode::empty(),
    )?);
    ensure!(
        same_executable_metadata(&after, &current.metadata()?),
        "host Docker executable path was replaced during verification"
    );
    Ok(format!("{:x}", hasher.finalize()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;

    #[test]
    fn application_admin_exception_is_exact_and_never_a_config_policy() {
        assert!(executable_directory_allowed(
            Path::new("/Applications"),
            0,
            80,
            0o040775,
            501
        ));
        for (path, uid, gid, mode) in [
            ("/Applications", 501, 80, 0o040775),
            ("/Applications", 0, 20, 0o040775),
            ("/Applications", 0, 80, 0o040777),
            ("/Applications", 0, 80, 0o042775),
            ("/Applications/App.app", 0, 80, 0o040775),
            ("/private/tmp/Applications", 0, 80, 0o040775),
            ("/Applications", 502, 80, 0o040755),
            ("/Applications", 0, 80, 0o100775),
        ] {
            assert!(
                !executable_directory_allowed(Path::new(path), uid, gid, mode, 501),
                "{path} {uid}:{gid} {mode:o}"
            );
        }
        assert!(executable_directory_allowed(
            Path::new("/private/tmp"),
            0,
            0,
            0o041777,
            501
        ));
        assert!(!executable_directory_allowed(
            Path::new("/private/tmp"),
            0,
            0,
            0o040777,
            501
        ));
    }

    fn executable_fixture() -> Result<(tempfile::TempDir, PathBuf)> {
        let root = tempfile::Builder::new()
            .prefix("vz-client-trust-")
            .tempdir_in("/private/tmp")?;
        let directory = root.path().join("bin");
        std::fs::create_dir(&directory)?;
        let file = directory.join("docker");
        std::fs::write(&file, b"offline non-executed fixture")?;
        std::fs::set_permissions(&file, std::fs::Permissions::from_mode(0o700))?;
        Ok((root, file))
    }

    #[test]
    fn untrusted_executable_parent_and_config_parent_fail_before_config_creation() -> Result<()> {
        let (root, file) = executable_fixture()?;
        executable_digest(&file)?;
        let parent = file.parent().context("fixture parent")?;
        std::fs::set_permissions(parent, std::fs::Permissions::from_mode(0o775))?;
        assert!(HostDockerClient::new(&file, &root.path().join("client")).is_err());
        assert!(!root.path().join("client").exists());
        std::fs::set_permissions(parent, std::fs::Permissions::from_mode(0o700))?;
        let config_parent = root.path().join("unsafe-config-parent");
        std::fs::create_dir(&config_parent)?;
        std::fs::set_permissions(&config_parent, std::fs::Permissions::from_mode(0o775))?;
        assert!(HostDockerClient::new(&file, &config_parent.join("client")).is_err());
        assert!(!config_parent.join("client").exists());
        Ok(())
    }

    #[test]
    fn linked_writable_and_nonregular_executables_reject() -> Result<()> {
        let (root, file) = executable_fixture()?;
        std::fs::hard_link(&file, root.path().join("alias"))?;
        assert!(executable_digest(&file).is_err());
        std::fs::remove_file(root.path().join("alias"))?;
        std::fs::set_permissions(&file, std::fs::Permissions::from_mode(0o775))?;
        assert!(executable_digest(&file).is_err());
        assert!(executable_digest(file.parent().context("parent")?).is_err());
        let link = root.path().join("link");
        std::os::unix::fs::symlink(file.parent().context("parent")?, &link)?;
        assert!(open_executable_parent(&link).is_err());
        Ok(())
    }

    #[test]
    fn hashing_detects_inode_parent_mode_and_hardlink_replacements() -> Result<()> {
        for change in 0..4 {
            let (root, file) = executable_fixture()?;
            let result = executable_digest_with_checkpoint(&file, || {
                match change {
                    0 => {
                        let replacement = root.path().join("replacement");
                        std::fs::write(&replacement, b"offline non-executed fixture")?;
                        std::fs::set_permissions(
                            &replacement,
                            std::fs::Permissions::from_mode(0o700),
                        )?;
                        std::fs::rename(replacement, &file)?;
                    }
                    1 => {
                        std::fs::set_permissions(&file, std::fs::Permissions::from_mode(0o500))?;
                    }
                    2 => {
                        std::fs::hard_link(&file, root.path().join("alias"))?;
                    }
                    _ => {
                        let parent = file.parent().context("parent")?;
                        std::fs::rename(parent, root.path().join("old-bin"))?;
                        std::fs::create_dir(parent)?;
                        std::fs::write(&file, b"offline non-executed fixture")?;
                        std::fs::set_permissions(&file, std::fs::Permissions::from_mode(0o700))?;
                    }
                }
                Ok(())
            });
            assert!(result.is_err(), "replacement {change} escaped verification");
        }
        Ok(())
    }
}
