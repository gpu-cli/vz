use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use oci_distribution::Reference;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::io::AsyncWriteExt;
use tokio::process::Command;
use tracing::warn;
use vz::NetworkConfig;
use vz::SharedDirConfig;
use vz::protocol::{ExecEvent, ExecOutput};
use vz_image::{ImageId, ImageStore};
use vz_linux::{
    EnsureKernelOptions, LinuxError, LinuxVm, LinuxVmConfig, default_linux_dir,
    ensure_kernel_with_options,
};

use crate::RuntimeConfig;
use crate::buildkit_rawjson::BuildkitRawJsonStreamDecoder;
pub use crate::buildkit_rawjson::{
    BuildkitPosition, BuildkitProgressGroup, BuildkitRange, BuildkitSolveStatus,
    BuildkitSourceInfo, BuildkitVertex, BuildkitVertexLog, BuildkitVertexStatus,
    BuildkitVertexWarning,
};

const BUILDKIT_VERSION: &str = "0.19.0";
const BUILDKITD_BINARY: &str = "buildkitd";
const BUILDKIT_RUNC_BINARY: &str = "buildkit-runc";
const BUILDCTL_BINARY: &str = "buildctl";
const VERSION_FILE: &str = "version.json";
const BUILD_OUTPUT_ARCHIVE: &str = "image.tar";
const BUILDKITD_ADDR: &str = "tcp://127.0.0.1:8372";
const BUILDKIT_SETUP_TIMEOUT: Duration = Duration::from_secs(90);
const BUILDKIT_BUILD_TIMEOUT: Duration = Duration::from_secs(30 * 60);
const BUILDKIT_RUNC_GUEST_PATH: &str = "/tmp/runc";
const BUILDKIT_SNAPSHOTTER: &str = "overlayfs";
const BUILDKIT_CACHE_KEEP_DURATION: &str = "168h";
const BUILDKIT_CACHE_KEEP_BYTES: u64 = 10 * 1024 * 1024 * 1024;

/// Destination for built image output.
#[derive(Debug, Clone, Default)]
pub enum BuildOutput {
    /// Import built image directly into local vz image store.
    #[default]
    VzStore,
    /// Push built image to registry.
    RegistryPush,
    /// Write OCI tar archive to host path.
    OciTar {
        /// Destination path for generated archive.
        dest: PathBuf,
    },
}

/// Build progress rendering mode passed to buildctl.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum BuildProgress {
    /// Buildctl picks plain vs tty based on terminal detection.
    #[default]
    Auto,
    /// Always print plain logs.
    Plain,
    /// Always print tty progress UI.
    Tty,
    /// Stream machine-readable status objects (one JSON object per line).
    RawJson,
}

impl BuildProgress {
    fn as_buildctl_value(self) -> &'static str {
        match self {
            Self::Auto => "auto",
            Self::Plain => "plain",
            Self::Tty => "tty",
            Self::RawJson => "rawjson",
        }
    }
}

/// Output stream source for BuildKit log chunks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BuildLogStream {
    Stdout,
    Stderr,
}

/// Event emitted while running a BuildKit build.
#[derive(Debug, Clone)]
pub enum BuildEvent {
    /// Lifecycle status update (VM boot, import stage, etc.).
    Status { message: String },
    /// Raw output bytes from buildctl.
    Output {
        stream: BuildLogStream,
        chunk: Vec<u8>,
    },
    /// Parsed BuildKit solve status from `--progress=rawjson`.
    SolveStatus { status: BuildkitSolveStatus },
    /// Rawjson decode failure for a single output line.
    RawJsonDecodeError { line: String, error: String },
}

/// Request for a Dockerfile build executed by BuildKit.
#[derive(Debug, Clone)]
pub struct BuildRequest {
    /// Host directory used as Docker build context.
    pub context_dir: PathBuf,
    /// Dockerfile path. Relative paths are resolved against `context_dir`.
    pub dockerfile: PathBuf,
    /// Image reference (for local tag and/or registry push).
    pub tag: String,
    /// Optional multi-stage target name.
    pub target: Option<String>,
    /// Build-time key/value arguments.
    pub build_args: BTreeMap<String, String>,
    /// Build secrets forwarded to BuildKit (`id=...,src=...`).
    pub secrets: Vec<String>,
    /// Disable BuildKit cache for this build.
    pub no_cache: bool,
    /// Output destination mode.
    pub output: BuildOutput,
    /// Progress rendering mode.
    pub progress: BuildProgress,
}

/// Successful BuildKit execution result.
#[derive(Debug, Clone)]
pub struct BuildResult {
    /// Stored image manifest digest when imported into local store.
    pub image_id: Option<ImageId>,
    /// Resolved image reference.
    pub tag: String,
    /// Path to emitted archive when output mode writes to disk.
    pub output_path: Option<PathBuf>,
    /// Whether the image was pushed to a registry.
    pub pushed: bool,
}

/// Options for `buildctl prune` cache command.
#[derive(Debug, Clone, Default)]
pub struct CachePruneOptions {
    /// Remove all cache entries.
    pub all: bool,
    /// Keep cache newer than this duration (for example `24h`).
    pub keep_duration: Option<String>,
    /// Keep this much storage (for example `5GB`).
    pub keep_storage: Option<String>,
}

/// BuildKit integration errors.
#[derive(Debug, thiserror::Error)]
pub enum BuildkitError {
    /// Invalid user-provided build configuration.
    #[error("invalid build configuration: {0}")]
    InvalidConfig(String),

    /// HOME is unavailable when resolving `~/.vz/buildkit`.
    #[error("home directory is not set (cannot resolve ~/.vz/buildkit)")]
    HomeDirectoryUnavailable,

    /// Guest-side setup command failed.
    #[error("guest command failed ({command}) with exit code {exit_code}: {stderr}\n{stdout}")]
    GuestCommandFailed {
        /// Command label for diagnostics.
        command: String,
        /// Exit code returned by the guest command.
        exit_code: i32,
        /// Captured stdout.
        stdout: String,
        /// Captured stderr.
        stderr: String,
    },

    /// BuildKit solve or cache command failed.
    #[error("buildctl command failed with exit code {exit_code}: {stderr}\n{stdout}")]
    BuildFailed {
        /// Exit code returned by buildctl.
        exit_code: i32,
        /// Captured stdout.
        stdout: String,
        /// Captured stderr.
        stderr: String,
    },

    /// OCI layout import encountered invalid or unsupported data.
    #[error("invalid OCI image layout: {0}")]
    InvalidOciLayout(String),

    /// Blob digest did not match expected descriptor digest.
    #[error("blob digest mismatch for {digest}: expected {expected}, found {found}")]
    DigestMismatch {
        /// Digest identifier from descriptor.
        digest: String,
        /// Expected hash component.
        expected: String,
        /// Computed hash component.
        found: String,
    },

    /// Unsupported digest algorithm in OCI descriptor.
    #[error("unsupported digest algorithm '{algorithm}' in {digest}")]
    UnsupportedDigestAlgorithm {
        /// Full digest string.
        digest: String,
        /// Algorithm prefix (before colon).
        algorithm: String,
    },

    /// Wrapped Linux guest orchestration error.
    #[error(transparent)]
    Linux(#[from] LinuxError),

    /// Wrapped filesystem I/O error.
    #[error(transparent)]
    Io(#[from] std::io::Error),

    /// Wrapped JSON parse/serialization error.
    #[error(transparent)]
    Json(#[from] serde_json::Error),

    /// Wrapped HTTP download error.
    #[error(transparent)]
    Http(#[from] reqwest::Error),
}

#[derive(Debug, Clone)]
struct BuildkitArtifacts {
    bin_dir: PathBuf,
    cache_dir: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BuildkitVersionFile {
    buildkit: String,
    downloaded_at: u64,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct OciDescriptor {
    media_type: String,
    digest: String,
}

#[derive(Debug, Deserialize)]
struct OciIndex {
    manifests: Vec<OciDescriptor>,
}

#[derive(Debug, Deserialize)]
struct OciManifest {
    config: OciDescriptor,
    layers: Vec<OciDescriptor>,
}

/// Build a Dockerfile and handle the requested output mode.
pub async fn build_image(
    config: &RuntimeConfig,
    request: BuildRequest,
) -> Result<BuildResult, BuildkitError> {
    build_image_with_events(config, request, |_event| {}).await
}

/// Build a Dockerfile and stream lifecycle/output events as they happen.
pub async fn build_image_with_events<F>(
    config: &RuntimeConfig,
    request: BuildRequest,
    mut on_event: F,
) -> Result<BuildResult, BuildkitError>
where
    F: FnMut(BuildEvent),
{
    let context_dir = canonicalize_existing_dir(&request.context_dir)?;
    if request.tag.trim().is_empty() {
        return Err(BuildkitError::InvalidConfig(
            "image tag must not be empty".to_string(),
        ));
    }

    let dockerfile_host = resolve_dockerfile_path(&context_dir, &request.dockerfile)?;
    let dockerfile_relative = dockerfile_host.strip_prefix(&context_dir).map_err(|_| {
        BuildkitError::InvalidConfig(format!(
            "Dockerfile must be inside build context: {}",
            dockerfile_host.display()
        ))
    })?;

    let output_mode = request.output.clone();
    let output_dir = match output_mode {
        BuildOutput::VzStore | BuildOutput::OciTar { .. } => {
            let base_dir = default_buildkit_dir()?;
            let dir = unique_dir(base_dir.join("tmp"), "build-output");
            tokio::fs::create_dir_all(&dir).await?;
            Some(dir)
        }
        BuildOutput::RegistryPush => None,
    };

    on_event(BuildEvent::Status {
        message: "Booting BuildKit VM".to_string(),
    });
    let vm = start_buildkit_vm(config, Some(&context_dir), output_dir.as_deref()).await?;
    on_event(BuildEvent::Status {
        message: "Running BuildKit solve".to_string(),
    });
    let build_result = run_guest_build(
        &vm,
        &request,
        dockerfile_relative,
        "/mnt/build-context",
        output_dir.as_ref().map(|_| "/mnt/build-output/image.tar"),
        &mut on_event,
    )
    .await;
    let stop_result = vm.stop().await;
    if let Err(error) = stop_result {
        warn!(%error, "failed to stop BuildKit VM cleanly");
    }
    build_result?;

    let final_result = match output_mode {
        BuildOutput::VzStore => {
            on_event(BuildEvent::Status {
                message: "Importing OCI archive into local store".to_string(),
            });
            let output_dir = output_dir.as_ref().ok_or_else(|| {
                BuildkitError::InvalidConfig("missing output directory".to_string())
            })?;
            let image_tar = output_dir.join(BUILD_OUTPUT_ARCHIVE);
            if !image_tar.is_file() {
                return Err(BuildkitError::InvalidOciLayout(format!(
                    "build output archive not found: {}",
                    image_tar.display()
                )));
            }

            let data_dir = expand_home_dir(&config.data_dir);
            let store = ImageStore::new(data_dir);
            let image_id = import_oci_tar_to_store(&store, &image_tar, &request.tag).await?;

            BuildResult {
                image_id: Some(image_id),
                tag: request.tag,
                output_path: None,
                pushed: false,
            }
        }
        BuildOutput::OciTar { dest } => {
            on_event(BuildEvent::Status {
                message: "Writing OCI archive output".to_string(),
            });
            let output_dir = output_dir.as_ref().ok_or_else(|| {
                BuildkitError::InvalidConfig("missing output directory".to_string())
            })?;
            let image_tar = output_dir.join(BUILD_OUTPUT_ARCHIVE);
            if !image_tar.is_file() {
                return Err(BuildkitError::InvalidOciLayout(format!(
                    "build output archive not found: {}",
                    image_tar.display()
                )));
            }

            let destination = expand_home_dir(&dest);
            if let Some(parent) = destination.parent() {
                tokio::fs::create_dir_all(parent).await?;
            }
            tokio::fs::copy(&image_tar, &destination).await?;

            BuildResult {
                image_id: None,
                tag: request.tag,
                output_path: Some(destination),
                pushed: false,
            }
        }
        BuildOutput::RegistryPush => BuildResult {
            image_id: None,
            tag: request.tag,
            output_path: None,
            pushed: true,
        },
    };

    if let Some(output_dir) = output_dir
        && let Err(error) = tokio::fs::remove_dir_all(&output_dir).await
    {
        warn!(
            path = %output_dir.display(),
            %error,
            "failed to clean temporary BuildKit output directory"
        );
    }

    Ok(final_result)
}

/// Return a human-readable BuildKit cache usage table (from `buildctl du`).
pub async fn cache_disk_usage(config: &RuntimeConfig) -> Result<String, BuildkitError> {
    let vm = start_buildkit_vm(config, None, None).await?;
    let output = async {
        ensure_guest_buildkit_ready(&vm).await?;
        run_buildctl(
            &vm,
            vec!["du".to_string(), "--verbose".to_string()],
            BUILDKIT_BUILD_TIMEOUT,
            None,
            false,
        )
        .await
    }
    .await;
    let stop_result = vm.stop().await;
    if let Err(error) = stop_result {
        warn!(%error, "failed to stop BuildKit VM cleanly");
    }

    let output = output?;
    if output.exit_code != 0 {
        return Err(BuildkitError::BuildFailed {
            exit_code: output.exit_code,
            stdout: output.stdout,
            stderr: output.stderr,
        });
    }

    Ok(render_command_output(output))
}

/// Prune BuildKit cache and return command output summary.
pub async fn cache_prune(
    config: &RuntimeConfig,
    options: CachePruneOptions,
) -> Result<String, BuildkitError> {
    let vm = start_buildkit_vm(config, None, None).await?;
    let output = async {
        ensure_guest_buildkit_ready(&vm).await?;

        let mut args = vec!["prune".to_string()];
        if options.all {
            args.push("--all".to_string());
        }
        if let Some(keep_duration) = options.keep_duration {
            args.push("--keep-duration".to_string());
            args.push(keep_duration);
        }
        if let Some(keep_storage) = options.keep_storage {
            args.push("--keep-storage".to_string());
            args.push(keep_storage);
        }

        run_buildctl(&vm, args, BUILDKIT_BUILD_TIMEOUT, None, false).await
    }
    .await;
    let stop_result = vm.stop().await;
    if let Err(error) = stop_result {
        warn!(%error, "failed to stop BuildKit VM cleanly");
    }

    let output = output?;
    if output.exit_code != 0 {
        return Err(BuildkitError::BuildFailed {
            exit_code: output.exit_code,
            stdout: output.stdout,
            stderr: output.stderr,
        });
    }

    Ok(render_command_output(output))
}

async fn start_buildkit_vm(
    config: &RuntimeConfig,
    context_dir: Option<&Path>,
    output_dir: Option<&Path>,
) -> Result<LinuxVm, BuildkitError> {
    let artifacts = ensure_buildkit_artifacts().await?;
    let kernel = ensure_kernel_with_options(EnsureKernelOptions {
        install_dir: config.linux_install_dir.clone(),
        bundle_dir: config.linux_bundle_dir.clone(),
        require_exact_agent_version: config.require_exact_agent_version,
    })
    .await?;

    let mut vm_config = LinuxVmConfig::new(kernel.kernel, kernel.initramfs);
    vm_config.cpus = 4;
    vm_config.memory_mb = 4096;
    vm_config.shared_dirs = vec![
        SharedDirConfig {
            tag: "buildkit-bin".to_string(),
            source: artifacts.bin_dir,
            read_only: true,
        },
        SharedDirConfig {
            tag: "buildkit-cache".to_string(),
            source: artifacts.cache_dir,
            read_only: false,
        },
    ];

    if let Some(linux_install_dir) = &config.linux_install_dir {
        vm_config.shared_dirs.push(SharedDirConfig {
            tag: "linux-bin".to_string(),
            source: expand_home_dir(linux_install_dir),
            read_only: true,
        });
    } else if let Ok(default_linux_install_dir) = default_linux_dir() {
        vm_config.shared_dirs.push(SharedDirConfig {
            tag: "linux-bin".to_string(),
            source: default_linux_install_dir,
            read_only: true,
        });
    }

    if let Some(host_ssl_dir) = host_ssl_dir() {
        vm_config.shared_dirs.push(SharedDirConfig {
            tag: "host-ssl".to_string(),
            source: host_ssl_dir,
            read_only: true,
        });
    }

    if let Some(context_dir) = context_dir {
        vm_config.shared_dirs.push(SharedDirConfig {
            tag: "build-context".to_string(),
            source: context_dir.to_path_buf(),
            read_only: true,
        });
    }

    if let Some(output_dir) = output_dir {
        vm_config.shared_dirs.push(SharedDirConfig {
            tag: "build-output".to_string(),
            source: output_dir.to_path_buf(),
            read_only: false,
        });
    }

    if !config.default_network_enabled {
        vm_config.network = Some(NetworkConfig::None);
    }

    let vm = LinuxVm::create(vm_config).await?;
    vm.start().await?;

    if let Err(err) = vm.wait_for_agent(config.agent_ready_timeout).await {
        let _ = vm.stop().await;
        return Err(err.into());
    }

    Ok(vm)
}

async fn run_guest_build(
    vm: &LinuxVm,
    request: &BuildRequest,
    dockerfile_relative: &Path,
    guest_context_dir: &str,
    guest_output_tar: Option<&str>,
    on_event: &mut impl FnMut(BuildEvent),
) -> Result<(), BuildkitError> {
    ensure_guest_buildkit_ready(vm).await?;

    let mut args = vec![
        "build".to_string(),
        "--progress".to_string(),
        request.progress.as_buildctl_value().to_string(),
        "--frontend".to_string(),
        "dockerfile.v0".to_string(),
        "--local".to_string(),
        format!("context={guest_context_dir}"),
        "--local".to_string(),
        format!("dockerfile={guest_context_dir}"),
        "--opt".to_string(),
        format!("filename={}", dockerfile_relative.display()),
    ];

    match &request.output {
        BuildOutput::VzStore | BuildOutput::OciTar { .. } => {
            let guest_output_tar = guest_output_tar.ok_or_else(|| {
                BuildkitError::InvalidConfig("missing guest output archive path".to_string())
            })?;
            args.push("--output".to_string());
            args.push(format!(
                "type=oci,dest={guest_output_tar},name={}",
                request.tag
            ));
        }
        BuildOutput::RegistryPush => {
            args.push("--output".to_string());
            args.push(format!("type=image,name={},push=true", request.tag));
        }
    }

    if let Some(target) = &request.target {
        args.push("--opt".to_string());
        args.push(format!("target={target}"));
    }
    if request.no_cache {
        args.push("--no-cache".to_string());
    }
    for (key, value) in &request.build_args {
        args.push("--opt".to_string());
        args.push(format!("build-arg:{key}={value}"));
    }
    for secret in &request.secrets {
        args.push("--secret".to_string());
        args.push(secret.clone());
    }

    let output = run_buildctl(
        vm,
        args,
        BUILDKIT_BUILD_TIMEOUT,
        Some(on_event),
        request.progress == BuildProgress::RawJson,
    )
    .await?;
    if output.exit_code != 0 {
        return Err(BuildkitError::BuildFailed {
            exit_code: output.exit_code,
            stdout: output.stdout,
            stderr: output.stderr,
        });
    }

    Ok(())
}

async fn ensure_guest_buildkit_ready(vm: &LinuxVm) -> Result<(), BuildkitError> {
    let setup_script = format!(
        r#"
set -eu

/bin/busybox mkdir -p /mnt/buildkit-bin /mnt/buildkit-cache /mnt/linux-bin /var/lib/buildkit /mnt/build-context /mnt/build-output /mnt/host-ssl
/bin/busybox mkdir -p /etc/buildkit
/bin/busybox mount -t virtiofs buildkit-bin /mnt/buildkit-bin 2>/dev/null || true
/bin/busybox mount -t virtiofs buildkit-cache /mnt/buildkit-cache 2>/dev/null || true
/bin/busybox mount -t virtiofs linux-bin /mnt/linux-bin 2>/dev/null || true
/bin/busybox mount -t virtiofs build-context /mnt/build-context 2>/dev/null || true
/bin/busybox mount -t virtiofs build-output /mnt/build-output 2>/dev/null || true
/bin/busybox mount -t virtiofs host-ssl /mnt/host-ssl 2>/dev/null || true
/bin/busybox mkdir -p /sys/fs/cgroup
/bin/busybox mount -t cgroup2 none /sys/fs/cgroup 2>/dev/null || true

/bin/busybox cp /mnt/buildkit-bin/buildkit-runc /tmp/runc-real
/bin/busybox cat >{BUILDKIT_RUNC_GUEST_PATH} <<'RUNC'
#!/bin/sh
set -eu

new_args=""
inserted=0
for arg in "$@"; do
  escaped=$(/bin/busybox sed "s/'/'\\\\''/g" <<EOF
$arg
EOF
)
  new_args="$new_args '$escaped'"
  if [ "$inserted" -eq 0 ] && {{ [ "$arg" = "run" ] || [ "$arg" = "create" ]; }}; then
    new_args="$new_args '--no-pivot'"
    inserted=1
  fi
done

eval "exec /tmp/runc-real $new_args"
RUNC
/bin/busybox chmod 0755 {BUILDKIT_RUNC_GUEST_PATH} /tmp/runc-real
export PATH="/tmp:/mnt/buildkit-bin:$PATH"
if [ -f /mnt/host-ssl/cert.pem ]; then
  /bin/busybox mkdir -p /etc/ssl/certs
  /bin/busybox cp /mnt/host-ssl/cert.pem /etc/ssl/cert.pem
  /bin/busybox cp /mnt/host-ssl/cert.pem /etc/ssl/certs/ca-certificates.crt
  export SSL_CERT_FILE=/mnt/host-ssl/cert.pem
fi

/bin/busybox cat >/etc/buildkit/buildkitd.toml <<'CFG'
[worker.oci]
  binary = "{BUILDKIT_RUNC_GUEST_PATH}"
  gc = true
  snapshotter = "{BUILDKIT_SNAPSHOTTER}"

[[worker.oci.gcpolicy]]
  keepDuration = "{BUILDKIT_CACHE_KEEP_DURATION}"
  all = true

[[worker.oci.gcpolicy]]
  keepBytes = {BUILDKIT_CACHE_KEEP_BYTES}
  all = true
CFG

if ! /mnt/buildkit-bin/buildctl --addr {BUILDKITD_ADDR} debug workers >/dev/null 2>&1; then
  /mnt/buildkit-bin/buildkitd \
    --config /etc/buildkit/buildkitd.toml \
    --addr {BUILDKITD_ADDR} \
    --oci-worker-binary {BUILDKIT_RUNC_GUEST_PATH} \
    --oci-worker-snapshotter {BUILDKIT_SNAPSHOTTER} \
    --root /var/lib/buildkit >/tmp/buildkitd.log 2>&1 &
fi

i=0
while [ "$i" -lt 60 ]; do
  if /mnt/buildkit-bin/buildctl --addr {BUILDKITD_ADDR} debug workers >/dev/null 2>&1; then
    exit 0
  fi
  i=$((i + 1))
  /bin/busybox sleep 1
done

echo "buildkitd did not become ready in guest" >&2
if [ -f /tmp/buildkitd.log ]; then
  /bin/busybox tail -n 200 /tmp/buildkitd.log >&2
fi
exit 1
"#
    );

    run_guest_command(
        vm,
        "setup buildkit guest environment",
        "/bin/busybox",
        vec!["sh".to_string(), "-c".to_string(), setup_script],
        BUILDKIT_SETUP_TIMEOUT,
    )
    .await
}

async fn run_buildctl(
    vm: &LinuxVm,
    args: Vec<String>,
    timeout: Duration,
    mut on_event: Option<&mut dyn FnMut(BuildEvent)>,
    parse_rawjson: bool,
) -> Result<ExecOutput, BuildkitError> {
    let mut full_args = vec!["--addr".to_string(), BUILDKITD_ADDR.to_string()];
    full_args.extend(args);
    let mut stdout_decoder = parse_rawjson.then(BuildkitRawJsonStreamDecoder::default);
    let mut stderr_decoder = parse_rawjson.then(BuildkitRawJsonStreamDecoder::default);
    let mut stdout_started = false;
    let mut stderr_started = false;

    let output = vm
        .exec_capture_streaming(
            "/mnt/buildkit-bin/buildctl".to_string(),
            full_args,
            timeout,
            |event| {
                if let Some(callback) = on_event.as_mut() {
                    match event {
                        ExecEvent::Stdout(chunk) => {
                            callback(BuildEvent::Output {
                                stream: BuildLogStream::Stdout,
                                chunk: chunk.clone(),
                            });
                            if let Some(decoder) = stdout_decoder.as_mut() {
                                for decoded in decoder.push_chunk(chunk) {
                                    match decoded {
                                        Ok(status) => {
                                            stdout_started = true;
                                            callback(BuildEvent::SolveStatus { status });
                                        }
                                        Err(error) => {
                                            if stdout_started || looks_like_json(&error.line) {
                                                callback(BuildEvent::RawJsonDecodeError {
                                                    line: rawjson_line_preview(&error.line),
                                                    error: error.error,
                                                });
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        ExecEvent::Stderr(chunk) => {
                            callback(BuildEvent::Output {
                                stream: BuildLogStream::Stderr,
                                chunk: chunk.clone(),
                            });
                            if let Some(decoder) = stderr_decoder.as_mut() {
                                for decoded in decoder.push_chunk(chunk) {
                                    match decoded {
                                        Ok(status) => {
                                            stderr_started = true;
                                            callback(BuildEvent::SolveStatus { status });
                                        }
                                        Err(error) => {
                                            if stderr_started || looks_like_json(&error.line) {
                                                callback(BuildEvent::RawJsonDecodeError {
                                                    line: rawjson_line_preview(&error.line),
                                                    error: error.error,
                                                });
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        ExecEvent::Exit(_) => {
                            if let Some(decoder) = stdout_decoder.as_mut() {
                                for decoded in decoder.finish() {
                                    match decoded {
                                        Ok(status) => {
                                            stdout_started = true;
                                            callback(BuildEvent::SolveStatus { status });
                                        }
                                        Err(error) => {
                                            if stdout_started || looks_like_json(&error.line) {
                                                callback(BuildEvent::RawJsonDecodeError {
                                                    line: rawjson_line_preview(&error.line),
                                                    error: error.error,
                                                });
                                            }
                                        }
                                    }
                                }
                            }
                            if let Some(decoder) = stderr_decoder.as_mut() {
                                for decoded in decoder.finish() {
                                    match decoded {
                                        Ok(status) => {
                                            stderr_started = true;
                                            callback(BuildEvent::SolveStatus { status });
                                        }
                                        Err(error) => {
                                            if stderr_started || looks_like_json(&error.line) {
                                                callback(BuildEvent::RawJsonDecodeError {
                                                    line: rawjson_line_preview(&error.line),
                                                    error: error.error,
                                                });
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            },
        )
        .await
        .map_err(BuildkitError::from)?;

    Ok(output)
}

fn rawjson_line_preview(line: &[u8]) -> String {
    const MAX_CHARS: usize = 240;
    let mut preview = String::from_utf8_lossy(line).into_owned();
    if preview.chars().count() > MAX_CHARS {
        preview = preview.chars().take(MAX_CHARS).collect::<String>();
        preview.push_str("...");
    }
    preview
}

fn looks_like_json(line: &[u8]) -> bool {
    line.iter()
        .find(|byte| !byte.is_ascii_whitespace())
        .is_some_and(|byte| *byte == b'{' || *byte == b'[')
}

async fn run_guest_command(
    vm: &LinuxVm,
    label: &str,
    command: &str,
    args: Vec<String>,
    timeout: Duration,
) -> Result<(), BuildkitError> {
    let output = vm
        .exec_capture(command.to_string(), args, timeout)
        .await
        .map_err(BuildkitError::from)?;

    if output.exit_code != 0 {
        return Err(BuildkitError::GuestCommandFailed {
            command: label.to_string(),
            exit_code: output.exit_code,
            stdout: output.stdout,
            stderr: output.stderr,
        });
    }
    Ok(())
}

fn render_command_output(output: ExecOutput) -> String {
    let mut rendered = String::new();
    if !output.stdout.trim().is_empty() {
        rendered.push_str(output.stdout.trim_end());
    }
    if !output.stderr.trim().is_empty() {
        if !rendered.is_empty() {
            rendered.push('\n');
        }
        rendered.push_str(output.stderr.trim_end());
    }
    rendered
}

fn host_ssl_dir() -> Option<PathBuf> {
    let ssl_dir = PathBuf::from("/etc/ssl");
    if ssl_dir.join("cert.pem").is_file() {
        Some(ssl_dir)
    } else {
        None
    }
}

async fn ensure_buildkit_artifacts() -> Result<BuildkitArtifacts, BuildkitError> {
    let base_dir = default_buildkit_dir()?;
    let bin_dir = base_dir.join("bin");
    let cache_dir = base_dir.join("cache");
    tokio::fs::create_dir_all(&cache_dir).await?;

    if artifacts_are_current(&base_dir, &bin_dir).await? {
        return Ok(BuildkitArtifacts { bin_dir, cache_dir });
    }

    tokio::fs::create_dir_all(&base_dir).await?;
    let staging_dir = unique_dir(base_dir.clone(), "download");
    tokio::fs::create_dir_all(&staging_dir).await?;
    let tarball_path = staging_dir.join("buildkit.tar.gz");

    let url = format!(
        "https://github.com/moby/buildkit/releases/download/v{version}/buildkit-v{version}.linux-arm64.tar.gz",
        version = BUILDKIT_VERSION
    );
    download_file(&url, &tarball_path).await?;
    extract_buildkit_archive(&tarball_path, &staging_dir).await?;

    let extracted_bin_dir = staging_dir.join("bin");
    let buildkitd_path = extracted_bin_dir.join(BUILDKITD_BINARY);
    let buildctl_path = extracted_bin_dir.join(BUILDCTL_BINARY);
    let runc_path = extracted_bin_dir.join(BUILDKIT_RUNC_BINARY);
    for path in [&buildkitd_path, &buildctl_path, &runc_path] {
        if !path.is_file() {
            return Err(BuildkitError::InvalidConfig(format!(
                "missing expected BuildKit binary: {}",
                path.display()
            )));
        }
        make_executable(path).await?;
    }

    if tokio::fs::metadata(&bin_dir).await.is_ok() {
        tokio::fs::remove_dir_all(&bin_dir).await?;
    }
    tokio::fs::rename(&extracted_bin_dir, &bin_dir).await?;

    let version = BuildkitVersionFile {
        buildkit: BUILDKIT_VERSION.to_string(),
        downloaded_at: unix_timestamp_secs(),
    };
    let version_json = serde_json::to_vec_pretty(&version)?;
    tokio::fs::write(base_dir.join(VERSION_FILE), version_json).await?;

    if let Err(error) = tokio::fs::remove_dir_all(&staging_dir).await {
        warn!(
            path = %staging_dir.display(),
            %error,
            "failed to clean BuildKit staging directory"
        );
    }

    Ok(BuildkitArtifacts { bin_dir, cache_dir })
}

async fn artifacts_are_current(base_dir: &Path, bin_dir: &Path) -> Result<bool, BuildkitError> {
    let version_path = base_dir.join(VERSION_FILE);
    let version_text = match tokio::fs::read_to_string(version_path).await {
        Ok(value) => value,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(err) => return Err(BuildkitError::Io(err)),
    };
    let metadata: BuildkitVersionFile = serde_json::from_str(&version_text)?;
    if metadata.buildkit != BUILDKIT_VERSION {
        return Ok(false);
    }

    for name in [BUILDKITD_BINARY, BUILDCTL_BINARY, BUILDKIT_RUNC_BINARY] {
        if !bin_dir.join(name).is_file() {
            return Ok(false);
        }
    }
    Ok(true)
}

async fn download_file(url: &str, destination: &Path) -> Result<(), BuildkitError> {
    let client = reqwest::Client::new();
    let mut response = client.get(url).send().await?.error_for_status()?;
    let mut file = tokio::fs::File::create(destination).await?;

    while let Some(chunk) = response.chunk().await? {
        file.write_all(&chunk).await?;
    }
    file.flush().await?;
    Ok(())
}

async fn extract_buildkit_archive(
    tarball_path: &Path,
    destination: &Path,
) -> Result<(), BuildkitError> {
    let output = Command::new("tar")
        .arg("-xzf")
        .arg(tarball_path)
        .arg("-C")
        .arg(destination)
        .arg("bin/buildkitd")
        .arg("bin/buildctl")
        .arg("bin/buildkit-runc")
        .output()
        .await?;
    if !output.status.success() {
        return Err(BuildkitError::InvalidConfig(format!(
            "failed to extract BuildKit archive with tar: {}",
            String::from_utf8_lossy(&output.stderr)
        )));
    }
    Ok(())
}

async fn make_executable(path: &Path) -> Result<(), BuildkitError> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        let mut perms = tokio::fs::metadata(path).await?.permissions();
        perms.set_mode(0o755);
        tokio::fs::set_permissions(path, perms).await?;
    }
    Ok(())
}

async fn import_oci_tar_to_store(
    store: &ImageStore,
    image_tar: &Path,
    reference: &str,
) -> Result<ImageId, BuildkitError> {
    let parent = image_tar.parent().ok_or_else(|| {
        BuildkitError::InvalidOciLayout("output tar has no parent directory".to_string())
    })?;
    let extract_dir = unique_dir(parent.to_path_buf(), "oci-import");
    tokio::fs::create_dir_all(&extract_dir).await?;

    let extract_output = Command::new("tar")
        .arg("-xf")
        .arg(image_tar)
        .arg("-C")
        .arg(&extract_dir)
        .output()
        .await?;
    if !extract_output.status.success() {
        return Err(BuildkitError::InvalidOciLayout(format!(
            "unable to unpack OCI tarball: {}",
            String::from_utf8_lossy(&extract_output.stderr)
        )));
    }

    let index_json = tokio::fs::read(extract_dir.join("index.json")).await?;
    let index: OciIndex = serde_json::from_slice(&index_json)?;
    let descriptor = index
        .manifests
        .iter()
        .find(|descriptor| descriptor.media_type.contains("image.manifest"))
        .or_else(|| index.manifests.first())
        .ok_or_else(|| {
            BuildkitError::InvalidOciLayout("index.json contains no manifests".to_string())
        })?;

    let manifest_digest = descriptor.digest.clone();
    let manifest_blob = read_blob(&extract_dir, &manifest_digest).await?;
    verify_blob_digest(&manifest_digest, &manifest_blob)?;
    let manifest: OciManifest = serde_json::from_slice(&manifest_blob)?;

    let config_blob = read_blob(&extract_dir, &manifest.config.digest).await?;
    verify_blob_digest(&manifest.config.digest, &config_blob)?;

    store.ensure_layout()?;
    store.write_manifest_json(&manifest_digest, &manifest_blob)?;
    store.write_config_json(&manifest_digest, &config_blob)?;

    for layer in &manifest.layers {
        let layer_blob = read_blob(&extract_dir, &layer.digest).await?;
        verify_blob_digest(&layer.digest, &layer_blob)?;
        store.write_layer_blob(&layer.digest, &layer.media_type, &layer_blob)?;
    }
    let canonical_reference = canonicalize_reference(reference);
    store.write_reference(&canonical_reference, &manifest_digest)?;
    if canonical_reference != reference {
        store.write_reference(reference, &manifest_digest)?;
    }

    if let Err(error) = tokio::fs::remove_dir_all(&extract_dir).await {
        warn!(
            path = %extract_dir.display(),
            %error,
            "failed to clean OCI import extraction directory"
        );
    }

    Ok(ImageId(manifest_digest))
}

fn canonicalize_reference(reference: &str) -> String {
    Reference::from_str(reference)
        .map(|parsed| parsed.whole())
        .unwrap_or_else(|_| reference.to_string())
}

async fn read_blob(root: &Path, digest: &str) -> Result<Vec<u8>, BuildkitError> {
    let path = blob_path(root, digest)?;
    tokio::fs::read(path).await.map_err(BuildkitError::from)
}

fn blob_path(root: &Path, digest: &str) -> Result<PathBuf, BuildkitError> {
    let (algorithm, encoded) = digest.split_once(':').ok_or_else(|| {
        BuildkitError::InvalidOciLayout(format!("invalid digest format: {digest}"))
    })?;
    Ok(root.join("blobs").join(algorithm).join(encoded))
}

fn verify_blob_digest(digest: &str, data: &[u8]) -> Result<(), BuildkitError> {
    let (algorithm, expected) = digest.split_once(':').ok_or_else(|| {
        BuildkitError::InvalidOciLayout(format!("invalid digest format: {digest}"))
    })?;
    if algorithm != "sha256" {
        return Err(BuildkitError::UnsupportedDigestAlgorithm {
            digest: digest.to_string(),
            algorithm: algorithm.to_string(),
        });
    }

    let mut hasher = Sha256::new();
    hasher.update(data);
    let found = format!("{:x}", hasher.finalize());
    let expected = expected.to_ascii_lowercase();
    if found != expected {
        return Err(BuildkitError::DigestMismatch {
            digest: digest.to_string(),
            expected,
            found,
        });
    }
    Ok(())
}

fn resolve_dockerfile_path(
    context_dir: &Path,
    dockerfile: &Path,
) -> Result<PathBuf, BuildkitError> {
    let path = if dockerfile.is_absolute() {
        dockerfile.to_path_buf()
    } else {
        context_dir.join(dockerfile)
    };
    if !path.is_file() {
        return Err(BuildkitError::InvalidConfig(format!(
            "Dockerfile not found: {}",
            path.display()
        )));
    }
    Ok(path)
}

fn canonicalize_existing_dir(path: &Path) -> Result<PathBuf, BuildkitError> {
    let expanded = expand_home_dir(path);
    let canonical = expanded.canonicalize()?;
    if !canonical.is_dir() {
        return Err(BuildkitError::InvalidConfig(format!(
            "build context is not a directory: {}",
            canonical.display()
        )));
    }
    Ok(canonical)
}

fn expand_home_dir(path: &Path) -> PathBuf {
    if let Some(path_str) = path.to_str() {
        if let Some(rest) = path_str.strip_prefix("~/")
            && let Some(home) = std::env::var_os("HOME")
        {
            return PathBuf::from(home).join(rest);
        }
    }
    path.to_path_buf()
}

fn default_buildkit_dir() -> Result<PathBuf, BuildkitError> {
    let home = std::env::var_os("HOME").ok_or(BuildkitError::HomeDirectoryUnavailable)?;
    Ok(PathBuf::from(home).join(".vz").join("buildkit"))
}

fn unique_dir(parent: PathBuf, prefix: &str) -> PathBuf {
    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    parent.join(format!("{prefix}-{stamp}"))
}

fn unix_timestamp_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use std::fs;
    use std::path::Path;
    use std::process::Command;

    use tempfile::tempdir;

    use super::*;

    #[derive(Debug, Serialize)]
    #[serde(rename_all = "camelCase")]
    struct DescriptorJson<'a> {
        media_type: &'a str,
        digest: String,
        size: usize,
    }

    #[derive(Debug, Serialize)]
    struct ManifestJson<'a> {
        schema_version: u8,
        media_type: &'a str,
        config: DescriptorJson<'a>,
        layers: Vec<DescriptorJson<'a>>,
    }

    #[derive(Debug, Serialize)]
    struct IndexJson<'a> {
        schema_version: u8,
        media_type: &'a str,
        manifests: Vec<DescriptorJson<'a>>,
    }

    fn sha256_digest(data: &[u8]) -> String {
        let mut hasher = Sha256::new();
        hasher.update(data);
        format!("sha256:{:x}", hasher.finalize())
    }

    fn write_blob(root: &Path, digest: &str, data: &[u8]) {
        let (algo, value) = digest.split_once(':').unwrap();
        let blob_path = root.join("blobs").join(algo).join(value);
        fs::create_dir_all(blob_path.parent().unwrap()).unwrap();
        fs::write(blob_path, data).unwrap();
    }

    #[test]
    fn progress_mode_maps_to_buildctl_values() {
        assert_eq!(BuildProgress::Auto.as_buildctl_value(), "auto");
        assert_eq!(BuildProgress::Plain.as_buildctl_value(), "plain");
        assert_eq!(BuildProgress::Tty.as_buildctl_value(), "tty");
        assert_eq!(BuildProgress::RawJson.as_buildctl_value(), "rawjson");
    }

    #[tokio::test]
    async fn import_oci_tar_writes_store_reference_and_blobs() {
        let tmp = tempdir().unwrap();
        let layout = tmp.path().join("layout");
        fs::create_dir_all(layout.join("blobs/sha256")).unwrap();
        fs::write(
            layout.join("oci-layout"),
            r#"{"imageLayoutVersion":"1.0.0"}"#,
        )
        .unwrap();

        let config_json =
            br#"{"architecture":"arm64","os":"linux","config":{"Cmd":["echo","ok"]}}"#;
        let config_digest = sha256_digest(config_json);
        write_blob(&layout, &config_digest, config_json);

        let layer_source = tmp.path().join("layer-src");
        fs::create_dir_all(&layer_source).unwrap();
        fs::write(layer_source.join("message.txt"), "hello from layer\n").unwrap();
        let layer_tar = tmp.path().join("layer.tar");
        let tar_status = Command::new("tar")
            .arg("-cf")
            .arg(&layer_tar)
            .arg("-C")
            .arg(&layer_source)
            .arg(".")
            .status()
            .unwrap();
        assert!(tar_status.success());
        let layer_bytes = fs::read(&layer_tar).unwrap();
        let layer_digest = sha256_digest(&layer_bytes);
        write_blob(&layout, &layer_digest, &layer_bytes);

        let manifest = ManifestJson {
            schema_version: 2,
            media_type: "application/vnd.oci.image.manifest.v1+json",
            config: DescriptorJson {
                media_type: "application/vnd.oci.image.config.v1+json",
                digest: config_digest.clone(),
                size: config_json.len(),
            },
            layers: vec![DescriptorJson {
                media_type: "application/vnd.oci.image.layer.v1.tar",
                digest: layer_digest.clone(),
                size: layer_bytes.len(),
            }],
        };
        let manifest_json = serde_json::to_vec(&manifest).unwrap();
        let manifest_digest = sha256_digest(&manifest_json);
        write_blob(&layout, &manifest_digest, &manifest_json);

        let index = IndexJson {
            schema_version: 2,
            media_type: "application/vnd.oci.image.index.v1+json",
            manifests: vec![DescriptorJson {
                media_type: "application/vnd.oci.image.manifest.v1+json",
                digest: manifest_digest.clone(),
                size: manifest_json.len(),
            }],
        };
        fs::write(
            layout.join("index.json"),
            serde_json::to_vec(&index).unwrap(),
        )
        .unwrap();

        let image_tar = tmp.path().join("image.tar");
        let tar_status = Command::new("tar")
            .arg("-cf")
            .arg(&image_tar)
            .arg("-C")
            .arg(&layout)
            .arg(".")
            .status()
            .unwrap();
        assert!(tar_status.success());

        let store = ImageStore::new(tmp.path().join("oci"));
        let imported = import_oci_tar_to_store(&store, &image_tar, "demo:latest")
            .await
            .unwrap();

        assert_eq!(imported.0, manifest_digest);
        assert_eq!(
            store.read_reference("demo:latest").unwrap(),
            manifest_digest
        );
        assert!(store.read_manifest_json(&manifest_digest).is_ok());
        assert!(store.read_config_json(&manifest_digest).is_ok());
        assert!(store.has_layer_blob(&layer_digest));
    }
}
