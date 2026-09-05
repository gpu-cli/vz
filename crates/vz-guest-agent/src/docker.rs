//! Lazy supervision for the downstream Docker Engine facade.
//!
//! Nothing in guest-agent startup invokes this module. The supervisor starts
//! only after the explicit `EnsureDocker` RPC. Native vz OCI operations remain
//! independent of Docker Engine.

use std::fs::OpenOptions;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

use anyhow::{Context, bail};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixStream;
use tokio::process::{Child, Command};
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tracing::{info, warn};

pub(crate) const DOCKER_SOCKET_PATH: &str = "/run/vz-docker/docker.sock";
const CONTAINERD_SOCKET_PATH: &str = "/run/vz-docker/containerd/containerd.sock";
const DOCKER_BIN_DIR: &str = "/mnt/vz-docker-bin";
const YOUKI_BIN_DIR: &str = "/mnt/linux-bin";
const YOUKI_BINARY: &str = "/mnt/linux-bin/youki";
const DOCKER_DATA_ROOT: &str = "/var/lib/docker";
const CONTAINERD_CONFIG: &str = "/var/lib/docker/config/containerd.toml";
const DOCKER_DAEMON_CONFIG: &str = "/var/lib/docker/config/daemon.json";
const STARTUP_TIMEOUT: Duration = Duration::from_secs(60);
const CONTAINERD_STARTUP_TIMEOUT: Duration = Duration::from_secs(20);
const HEALTH_POLL_INTERVAL: Duration = Duration::from_millis(200);
const RESTART_BACKOFF: Duration = Duration::from_secs(1);
const ENGINE_PING_TIMEOUT: Duration = Duration::from_secs(2);
const MAX_ENGINE_PING_RESPONSE_BYTES: u64 = 16 * 1024;
const YOUKI_RUNTIME_NAME: &str = "youki";
// Moby docker-v29.7.2 pins BuildKit v0.32.2. Its embedded builder reads this
// variable and replaces the default runc/buildkit-runc candidate list with the
// one exact command below. Re-audit this private Moby contract on every upgrade.
const MOBY_BUILDKIT_OCI_RUNTIME_ENV: &str = "DOCKER_BUILDKIT_RUNC_COMMAND";
const ENGINE_PING_REQUEST: &[u8] =
    b"GET /_ping HTTP/1.1\r\nHost: docker\r\nConnection: close\r\n\r\n";

const REQUIRED_BINARIES: [&str; 5] = [
    "containerd",
    "containerd-shim-runc-v2",
    "docker-init",
    "docker-proxy",
    "dockerd",
];

/// Lazily owns the background Docker daemon supervisor task.
pub(crate) struct DockerSupervisor {
    worker: Mutex<Option<JoinHandle<()>>>,
}

impl DockerSupervisor {
    pub(crate) fn new() -> Self {
        Self {
            worker: Mutex::new(None),
        }
    }

    /// Validate the layout and start supervision if it is not already active.
    pub(crate) async fn ensure_started(&self) -> anyhow::Result<()> {
        prepare_persistent_layout().await?;
        validate_runtime_invariants()?;

        {
            let mut worker = self.worker.lock().await;
            if worker.as_ref().is_none_or(JoinHandle::is_finished) {
                *worker = Some(tokio::spawn(supervise_docker()));
            }
        }
        Ok(())
    }

    /// Wait for the supervised Engine API to answer its health endpoint.
    pub(crate) async fn wait_ready(&self) -> anyhow::Result<&'static str> {
        wait_for_engine(Path::new(DOCKER_SOCKET_PATH), STARTUP_TIMEOUT)
            .await
            .context("dockerd did not return OK from its Engine API /_ping endpoint")?;
        Ok(DOCKER_SOCKET_PATH)
    }

    #[cfg(test)]
    async fn worker_started(&self) -> bool {
        self.worker.lock().await.is_some()
    }
}

async fn prepare_persistent_layout() -> anyhow::Result<()> {
    #[cfg(not(target_os = "linux"))]
    bail!("Docker facade supervision is supported only in Linux guests");

    #[cfg(target_os = "linux")]
    {
        // The disk is provisioned by the host and must already contain ext4.
        // Refusing a failed mount is intentional: the guest agent never
        // reformats a possibly pre-existing persistent disk.
        let setup = format!(
            r#"
set -eu
umask 077
/bin/busybox mkdir -p {DOCKER_BIN_DIR} {YOUKI_BIN_DIR} {DOCKER_DATA_ROOT} /run/vz-docker/containerd
if ! /bin/busybox grep -q " {DOCKER_BIN_DIR} " /proc/mounts; then
  /bin/busybox mount -t virtiofs vz-docker-bin {DOCKER_BIN_DIR}
fi
if ! /bin/busybox grep -q " {YOUKI_BIN_DIR} " /proc/mounts; then
  /bin/busybox mount -t virtiofs linux-bin {YOUKI_BIN_DIR}
fi
if ! /bin/busybox grep -q " {DOCKER_DATA_ROOT} " /proc/mounts; then
  test -b /dev/vda || {{ echo "Docker persistent data disk /dev/vda is unavailable" >&2; exit 1; }}
  /bin/busybox mount -t ext4 /dev/vda {DOCKER_DATA_ROOT} || {{ echo "Docker data disk must be preformatted ext4" >&2; exit 1; }}
fi
/bin/busybox mkdir -p {DOCKER_DATA_ROOT}/config {DOCKER_DATA_ROOT}/containerd {DOCKER_DATA_ROOT}/engine {DOCKER_DATA_ROOT}/log
/bin/busybox mkdir -p /sys/fs/cgroup /run/vz-docker/containerd /run/vz-docker/dockerd
/bin/busybox chmod 700 {DOCKER_DATA_ROOT} {DOCKER_DATA_ROOT}/config {DOCKER_DATA_ROOT}/containerd {DOCKER_DATA_ROOT}/engine {DOCKER_DATA_ROOT}/log /run/vz-docker /run/vz-docker/containerd /run/vz-docker/dockerd
/bin/busybox mount -t cgroup2 none /sys/fs/cgroup 2>/dev/null || true
"#
        );
        let output = Command::new("/bin/busybox")
            .args(["sh", "-c", &setup])
            .output()
            .await
            .context("failed to prepare Docker facade mounts")?;
        if !output.status.success() {
            bail!(
                "failed to prepare Docker persistent layout: {}{}",
                String::from_utf8_lossy(&output.stderr),
                String::from_utf8_lossy(&output.stdout)
            );
        }

        let mounts = std::fs::read_to_string("/proc/mounts")
            .context("failed to inspect guest mounts after Docker setup")?;
        validate_persistent_mount(&mounts, DOCKER_DATA_ROOT)?;
        validate_exact_mount(&mounts, DOCKER_BIN_DIR, "vz-docker-bin", "virtiofs")?;
        validate_exact_mount(&mounts, YOUKI_BIN_DIR, "linux-bin", "virtiofs")
    }
}

fn validate_runtime_invariants() -> anyhow::Result<()> {
    let mut installed = Vec::new();
    for entry in std::fs::read_dir(DOCKER_BIN_DIR).context("failed to inventory Docker binaries")? {
        let entry = entry?;
        let metadata = std::fs::symlink_metadata(entry.path())?;
        if !metadata.file_type().is_file() || metadata.file_type().is_symlink() {
            bail!(
                "Docker binary inventory contains a non-regular entry: {}",
                entry.path().display()
            );
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            if metadata.permissions().mode() & 0o111 == 0 {
                bail!(
                    "Docker binary is not executable: {}",
                    entry.path().display()
                );
            }
        }
        installed.push(entry.file_name().to_string_lossy().into_owned());
    }
    installed.sort();
    if installed != REQUIRED_BINARIES.map(str::to_owned) {
        bail!(
            "Docker binary inventory must contain exactly the daemon allowlist; found {installed:?}"
        );
    }
    if !Path::new(YOUKI_BINARY).is_file() {
        bail!("youki OCI runtime is missing at {YOUKI_BINARY}");
    }

    validate_regular_config(CONTAINERD_CONFIG)?;
    validate_dockerd_config()?;
    Ok(())
}

fn validate_regular_config(path: &str) -> anyhow::Result<()> {
    let metadata = std::fs::symlink_metadata(path)
        .with_context(|| format!("daemon config is missing at {path}"))?;
    if !metadata.file_type().is_file() || metadata.file_type().is_symlink() {
        bail!("daemon config at {path} must be a regular non-symlink file");
    }
    Ok(())
}

fn validate_dockerd_config() -> anyhow::Result<()> {
    validate_regular_config(DOCKER_DAEMON_CONFIG)?;
    let config = std::fs::read_to_string(DOCKER_DAEMON_CONFIG)
        .context("failed to read Docker daemon config")?;
    validate_dockerd_config_contents(&config)
}

fn validate_dockerd_config_contents(config: &str) -> anyhow::Result<()> {
    let config: serde_json::Value =
        serde_json::from_str(config).context("invalid Docker daemon JSON config")?;
    let config = config
        .as_object()
        .context("Docker daemon config must be a JSON object")?;
    if config.contains_key("runtimes") || config.contains_key("default-runtime") {
        bail!(
            "Docker daemon config must not override the supervisor's immutable youki runtime selection"
        );
    }
    Ok(())
}

#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
fn validate_persistent_mount(mounts: &str, expected_path: &str) -> anyhow::Result<()> {
    validate_exact_mount(mounts, expected_path, "/dev/vda", "ext4")
}

#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
fn validate_exact_mount(
    mounts: &str,
    expected_path: &str,
    expected_source: &str,
    expected_filesystem: &str,
) -> anyhow::Result<()> {
    let mut matching = mounts.lines().filter_map(|line| {
        let mut fields = line.split_whitespace();
        let source = fields.next()?;
        let mount_path = fields.next()?;
        let filesystem = fields.next()?;
        (mount_path == expected_path).then_some((source, filesystem))
    });
    let Some((source, filesystem)) = matching.next() else {
        bail!("Docker prerequisite path {expected_path} is not a mount point");
    };
    if matching.next().is_some() || filesystem != expected_filesystem || source != expected_source {
        bail!(
            "Docker prerequisite path {expected_path} requires exactly one {expected_source} {expected_filesystem} mount (found {source} {filesystem})"
        );
    }
    Ok(())
}

async fn supervise_docker() {
    loop {
        if docker_engine_is_ready(Path::new(DOCKER_SOCKET_PATH)).await {
            while docker_engine_is_ready(Path::new(DOCKER_SOCKET_PATH)).await {
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
            warn!("existing Docker facade stopped responding; restarting supervision");
        }
        if let Err(error) = run_daemons_once().await {
            warn!(%error, "Docker facade daemon cycle failed");
        }
        tokio::time::sleep(RESTART_BACKOFF).await;
    }
}

async fn run_daemons_once() -> anyhow::Result<()> {
    remove_stale_socket(Path::new(CONTAINERD_SOCKET_PATH)).await?;
    remove_stale_socket(Path::new(DOCKER_SOCKET_PATH)).await?;
    let mut containerd = spawn_containerd()?;
    wait_for_socket_or_exit(
        Path::new(CONTAINERD_SOCKET_PATH),
        CONTAINERD_STARTUP_TIMEOUT,
        &mut containerd,
        "containerd",
    )
    .await?;
    let mut dockerd = spawn_dockerd()?;
    info!("Docker facade containerd and dockerd started");

    tokio::select! {
        status = containerd.wait() => {
            let status = status.context("failed waiting for containerd")?;
            warn!(%status, "Docker facade containerd exited");
            let _ = dockerd.kill().await;
        }
        status = dockerd.wait() => {
            let status = status.context("failed waiting for dockerd")?;
            warn!(%status, "Docker facade dockerd exited");
            let _ = containerd.kill().await;
        }
    }
    Ok(())
}

fn spawn_containerd() -> anyhow::Result<Child> {
    let (stdout, stderr) = daemon_logs("containerd")?;
    containerd_command()
        .stdout(stdout)
        .stderr(stderr)
        .kill_on_drop(true)
        .spawn()
        .context("failed to spawn Docker facade containerd")
}

fn containerd_command() -> Command {
    let mut command = Command::new(format!("{DOCKER_BIN_DIR}/containerd"));
    command
        .args([
            "--config",
            CONTAINERD_CONFIG,
            "--root",
            "/var/lib/docker/containerd",
            "--state",
            "/run/vz-docker/containerd",
            "--address",
            CONTAINERD_SOCKET_PATH,
        ])
        .env("PATH", daemon_path());
    command
}

fn spawn_dockerd() -> anyhow::Result<Child> {
    let (stdout, stderr) = daemon_logs("dockerd")?;
    dockerd_command()
        .stdout(stdout)
        .stderr(stderr)
        .kill_on_drop(true)
        .spawn()
        .context("failed to spawn Docker facade dockerd")
}

fn dockerd_command() -> Command {
    let mut command = Command::new(format!("{DOCKER_BIN_DIR}/dockerd"));
    command
        .args(dockerd_args())
        .env("PATH", daemon_path())
        .env(MOBY_BUILDKIT_OCI_RUNTIME_ENV, YOUKI_BINARY);
    command
}

fn dockerd_args() -> Vec<String> {
    vec![
        "--host".to_string(),
        format!("unix://{DOCKER_SOCKET_PATH}"),
        "--containerd".to_string(),
        CONTAINERD_SOCKET_PATH.to_string(),
        "--data-root".to_string(),
        "/var/lib/docker/engine".to_string(),
        "--exec-root".to_string(),
        "/run/vz-docker/dockerd".to_string(),
        "--pidfile".to_string(),
        "/run/vz-docker/dockerd.pid".to_string(),
        "--config-file".to_string(),
        DOCKER_DAEMON_CONFIG.to_string(),
        "--add-runtime".to_string(),
        format!("{YOUKI_RUNTIME_NAME}={YOUKI_BINARY}"),
        "--default-runtime".to_string(),
        YOUKI_RUNTIME_NAME.to_string(),
    ]
}

fn daemon_path() -> String {
    format!("{DOCKER_BIN_DIR}:{YOUKI_BIN_DIR}:/bin:/sbin:/usr/bin:/usr/sbin")
}

fn daemon_logs(name: &str) -> anyhow::Result<(Stdio, Stdio)> {
    let path = PathBuf::from(DOCKER_DATA_ROOT)
        .join("log")
        .join(format!("{name}.log"));
    let stdout = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .with_context(|| format!("failed to open daemon log {}", path.display()))?;
    let stderr = stdout
        .try_clone()
        .with_context(|| format!("failed to clone daemon log {}", path.display()))?;
    Ok((Stdio::from(stdout), Stdio::from(stderr)))
}

async fn wait_for_engine(path: &Path, timeout: Duration) -> anyhow::Result<()> {
    tokio::time::timeout(timeout, async {
        loop {
            if docker_engine_is_ready(path).await {
                return;
            }
            tokio::time::sleep(HEALTH_POLL_INTERVAL).await;
        }
    })
    .await
    .map_err(|_| anyhow::anyhow!("timed out waiting for {}", path.display()))
}

async fn wait_for_socket_or_exit(
    path: &Path,
    timeout: Duration,
    child: &mut Child,
    daemon_name: &str,
) -> anyhow::Result<()> {
    tokio::time::timeout(timeout, async {
        loop {
            if unix_socket_accepts(path).await {
                return Ok(());
            }
            if let Some(status) = child.try_wait().context("failed to poll daemon")? {
                bail!("{daemon_name} exited during startup with {status}");
            }
            tokio::time::sleep(HEALTH_POLL_INTERVAL).await;
        }
    })
    .await
    .map_err(|_| anyhow::anyhow!("timed out waiting for {daemon_name} at {}", path.display()))?
}

async fn unix_socket_accepts(path: &Path) -> bool {
    UnixStream::connect(path).await.is_ok()
}

async fn docker_engine_is_ready(path: &Path) -> bool {
    tokio::time::timeout(ENGINE_PING_TIMEOUT, docker_engine_ping(path))
        .await
        .is_ok_and(|result| result.is_ok())
}

async fn docker_engine_ping(path: &Path) -> anyhow::Result<()> {
    let mut stream = UnixStream::connect(path)
        .await
        .with_context(|| format!("failed to connect to Docker Engine at {}", path.display()))?;
    stream
        .write_all(ENGINE_PING_REQUEST)
        .await
        .context("failed to write Docker Engine ping request")?;

    let mut response = Vec::new();
    stream
        .take(MAX_ENGINE_PING_RESPONSE_BYTES + 1)
        .read_to_end(&mut response)
        .await
        .context("failed to read Docker Engine ping response")?;
    if response.len() as u64 > MAX_ENGINE_PING_RESPONSE_BYTES {
        bail!("Docker Engine ping response exceeded size limit");
    }

    validate_engine_ping_response(&response)
}

fn validate_engine_ping_response(response: &[u8]) -> anyhow::Result<()> {
    let header_end = response
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .context("Docker Engine ping response had no HTTP header terminator")?;
    let (headers, body_with_separator) = response.split_at(header_end);
    let status_line = headers
        .split(|byte| *byte == b'\n')
        .next()
        .context("Docker Engine ping response had no HTTP status")?;
    let status_line = std::str::from_utf8(status_line)
        .context("Docker Engine ping HTTP status was not UTF-8")?
        .trim_end_matches('\r');
    let mut status_parts = status_line.split_whitespace();
    let protocol = status_parts.next().unwrap_or_default();
    let status = status_parts.next().unwrap_or_default();
    if !matches!(protocol, "HTTP/1.0" | "HTTP/1.1") || status != "200" {
        bail!("Docker Engine ping returned unexpected HTTP status {status_line}");
    }
    if &body_with_separator[4..] != b"OK" {
        bail!("Docker Engine ping did not return OK");
    }
    Ok(())
}

async fn remove_stale_socket(path: &Path) -> anyhow::Result<()> {
    if unix_socket_accepts(path).await {
        bail!(
            "refusing to replace active daemon socket at {}",
            path.display()
        );
    }
    let metadata = match std::fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error.into()),
    };
    #[cfg(unix)]
    {
        use std::os::unix::fs::FileTypeExt;
        if !metadata.file_type().is_socket() {
            bail!("refusing to remove non-socket path at {}", path.display());
        }
    }
    std::fs::remove_file(path)
        .with_context(|| format!("failed to remove stale socket at {}", path.display()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn persistent_mount_rejects_initramfs_and_accepts_virtio_ext4() {
        let mounts = "rootfs / rootfs rw 0 0\n/dev/vda /var/lib/docker ext4 rw 0 0\n";
        assert!(validate_persistent_mount(mounts, DOCKER_DATA_ROOT).is_ok());
        let initramfs = "rootfs / rootfs rw 0 0\nrootfs /var/lib/docker rootfs rw 0 0\n";
        assert!(validate_persistent_mount(initramfs, DOCKER_DATA_ROOT).is_err());
        let named_volume = "/dev/vdb /var/lib/docker ext4 rw 0 0\n";
        assert!(validate_persistent_mount(named_volume, DOCKER_DATA_ROOT).is_err());
        let overmounted = format!("{mounts}{named_volume}");
        assert!(validate_persistent_mount(&overmounted, DOCKER_DATA_ROOT).is_err());
    }

    #[test]
    fn binary_mounts_require_exact_tags_and_no_overmount() {
        let mounts = "vz-docker-bin /mnt/vz-docker-bin virtiofs ro 0 0\nlinux-bin /mnt/linux-bin virtiofs ro 0 0\n";
        assert!(validate_exact_mount(mounts, DOCKER_BIN_DIR, "vz-docker-bin", "virtiofs").is_ok());
        assert!(validate_exact_mount(mounts, YOUKI_BIN_DIR, "linux-bin", "virtiofs").is_ok());
        for invalid in [
            "rootfs /mnt/vz-docker-bin rootfs rw 0 0\n",
            "other-machine /mnt/vz-docker-bin virtiofs ro 0 0\n",
            "vz-docker-bin /mnt/vz-docker-bin ext4 rw 0 0\n",
        ] {
            assert!(
                validate_exact_mount(invalid, DOCKER_BIN_DIR, "vz-docker-bin", "virtiofs").is_err()
            );
            assert!(
                validate_exact_mount(
                    &format!("{mounts}{invalid}"),
                    DOCKER_BIN_DIR,
                    "vz-docker-bin",
                    "virtiofs"
                )
                .is_err()
            );
        }
    }

    #[tokio::test]
    async fn supervisor_is_lazy_until_explicit_ensure_call() {
        let supervisor = DockerSupervisor::new();
        assert!(!supervisor.worker_started().await);
    }

    #[test]
    fn daemon_allowlist_does_not_contain_an_oci_runtime() {
        assert!(!REQUIRED_BINARIES.contains(&"runc"));
        assert!(REQUIRED_BINARIES.contains(&"containerd-shim-runc-v2"));
    }

    #[test]
    fn dockerd_always_registers_and_selects_youki() {
        assert_eq!(
            dockerd_args(),
            vec![
                "--host",
                "unix:///run/vz-docker/docker.sock",
                "--containerd",
                "/run/vz-docker/containerd/containerd.sock",
                "--data-root",
                "/var/lib/docker/engine",
                "--exec-root",
                "/run/vz-docker/dockerd",
                "--pidfile",
                "/run/vz-docker/dockerd.pid",
                "--config-file",
                "/var/lib/docker/config/daemon.json",
                "--add-runtime",
                "youki=/mnt/linux-bin/youki",
                "--default-runtime",
                "youki",
            ]
        );
    }

    #[test]
    fn embedded_buildkit_has_one_exact_youki_runtime_without_containerd_override() {
        fn env_value(command: &Command, key: &str) -> Option<String> {
            command
                .as_std()
                .get_envs()
                .find_map(|(name, value)| {
                    (name == key).then(|| value.map(|value| value.to_string_lossy().into_owned()))
                })
                .flatten()
        }

        assert_eq!(
            env_value(&dockerd_command(), MOBY_BUILDKIT_OCI_RUNTIME_ENV).as_deref(),
            Some(YOUKI_BINARY)
        );
        assert_eq!(
            env_value(&containerd_command(), MOBY_BUILDKIT_OCI_RUNTIME_ENV),
            None
        );
    }

    #[test]
    fn dockerd_config_cannot_add_or_override_runtimes() {
        assert!(validate_dockerd_config_contents(r#"{"log-level":"warn"}"#).is_ok());
        assert!(
            validate_dockerd_config_contents(r#"{"runtimes":{"runc":{"path":"/bin/runc"}}}"#)
                .is_err()
        );
        assert!(validate_dockerd_config_contents(r#"{"default-runtime":"runc"}"#).is_err());
        assert!(validate_dockerd_config_contents("not JSON").is_err());
    }

    #[test]
    fn engine_readiness_requires_successful_ping_with_ok_body() {
        assert!(ENGINE_PING_REQUEST.starts_with(b"GET /_ping HTTP/1.1\r\n"));
        assert!(
            validate_engine_ping_response(
                b"HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: 2\r\n\r\nOK",
            )
            .is_ok()
        );
        assert!(
            validate_engine_ping_response(
                b"HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: 2\r\n\r\nNO",
            )
            .is_err()
        );
        assert!(
            validate_engine_ping_response(b"HTTP/1.1 503 Service Unavailable\r\n\r\nOK").is_err()
        );
    }
}
