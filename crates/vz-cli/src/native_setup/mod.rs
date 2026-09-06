//! Explicit local macOS installation and validated template registration.
mod package;
pub mod toolchain_install;

use anyhow::{Context, Result, ensure};
use clap::Parser;
use fs2::FileExt;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    fs,
    io::{IsTerminal, Read, Write},
    os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt, PermissionsExt},
    path::{Path, PathBuf},
    process::Command,
    sync::atomic::{AtomicBool, Ordering},
    time::Duration,
};
use vz_macos_provision::{
    artifact_cache::{Artifact, ArtifactCache},
    bootstrap::{BootstrapCache, ImageIdentity, Platform, ReleaseManifest},
};

static CANCELLED: AtomicBool = AtomicBool::new(false);
const VERSION: &str = "26.3.1";
const BUILD: &str = "25D2128";
const DISK_SIZE: u64 = 80 * 1024 * 1024 * 1024;

/// Host setup options. No Environment or Machine is created by this operation.
#[derive(Parser, Debug)]
#[command(
    name = "vz-macos-setup",
    about = "Prepare macOS locally once for vz Machines (DEV)"
)]
pub struct Args {
    /// Installed vz prefix; defaults to the parent of this executable's bin directory.
    #[arg(long)]
    pub prefix: Option<PathBuf>,
    /// Optional local Xcode application. Omit for macOS without developer tools.
    #[arg(long)]
    pub xcode: Option<PathBuf>,
    /// Optional existing IPSW; must match the built-in Apple version and SHA-256.
    #[arg(long)]
    pub ipsw: Option<PathBuf>,
    /// Accept the selected Xcode license inside the new VM.
    #[arg(long, requires = "xcode")]
    pub accept_xcode_license: bool,
    /// Emit structured phase and byte progress.
    #[arg(long)]
    pub json: bool,
    /// Internal short-lived privileged operation, invoked only by setup.
    #[arg(long, hide = true)]
    provision_disk: Option<PathBuf>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SetupReceipt {
    schema_version: u32,
    recipe_sha256: String,
    manifest_sha256: String,
    manifest_size: u64,
    bundle: PathBuf,
    files: Vec<LocalStamp>,
}

#[derive(Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct LocalStamp {
    digest: String,
    device: u64,
    inode: u64,
    size: u64,
    modified: (i64, i64),
    changed: (i64, i64),
}
impl LocalStamp {
    fn read(bundle: &Path, digest: &str) -> Result<Self> {
        ensure!(
            digest.len() == 64
                && digest
                    .bytes()
                    .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase()),
            "invalid local blob name"
        );
        let m = fs::symlink_metadata(bundle.join(digest))?;
        ensure!(
            m.is_file()
                && m.nlink() == 1
                && m.mode() & 0o777 == 0o400
                && m.uid() == fs::metadata(bundle)?.uid(),
            "local image files must be private and read-only"
        );
        Ok(Self {
            digest: digest.into(),
            device: m.dev(),
            inode: m.ino(),
            size: m.len(),
            modified: (m.mtime(), m.mtime_nsec()),
            changed: (m.ctime(), m.ctime_nsec()),
        })
    }
}

pub(super) fn check_cancelled() -> Result<()> {
    ensure!(
        !CANCELLED.load(Ordering::SeqCst),
        "macOS setup cancelled; no incomplete image was registered"
    );
    Ok(())
}
pub(super) fn hash_bytes(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}
pub(super) fn hash_file(path: &Path) -> Result<(String, u64)> {
    let mut file = fs::File::open(path)?;
    let mut hash = Sha256::new();
    let mut buffer = vec![0; 4 * 1024 * 1024];
    let mut size = 0;
    loop {
        check_cancelled()?;
        let n = file.read(&mut buffer)?;
        if n == 0 {
            break;
        }
        hash.update(&buffer[..n]);
        size += n as u64;
    }
    Ok((format!("{:x}", hash.finalize()), size))
}
fn private(path: &Path) -> Result<()> {
    match fs::DirBuilder::new().mode(0o700).create(path) {
        Ok(()) => (),
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => (),
        Err(error) => return Err(error.into()),
    }
    ensure!(
        path.canonicalize()? == path,
        "setup paths must be canonical without symlinks"
    );
    let metadata = fs::symlink_metadata(path)?;
    ensure!(
        metadata.is_dir() && metadata.mode() & 0o077 == 0,
        "setup directory must be private"
    );
    // SAFETY: reads only the process identity.
    #[allow(unsafe_code)]
    let uid = unsafe { libc::geteuid() };
    ensure!(
        metadata.uid() == uid,
        "setup directory must belong to the current user"
    );
    Ok(())
}
fn pin(sha256: String, size_bytes: u64) -> Artifact {
    Artifact {
        url: format!("bundle:{sha256}"),
        sha256,
        size_bytes,
    }
}
#[allow(clippy::print_stderr)] // Terminal progress is this installation utility's user interface.
fn event(json: bool, phase: &str, completed: u64, total: u64) -> Result<()> {
    if json {
        writeln!(
            std::io::stdout().lock(),
            "{}",
            serde_json::json!({"phase":phase,"completed":completed,"total":total,
                "timestamp_unix_ms": std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)?.as_millis()})
        )?;
    } else if total > 0 {
        eprint!(
            "\r{phase}: {:.1}%     ",
            completed as f64 * 100.0 / total as f64
        );
        if completed == total {
            eprintln!();
        }
    } else {
        eprintln!("{phase}");
    }
    Ok(())
}
fn copy_blob(source: &Path, bundle: &Path, clone: bool) -> Result<Artifact> {
    let (digest, size) = hash_file(source)?;
    let destination = bundle.join(&digest);
    if clone {
        ensure!(
            Command::new("/bin/cp")
                .arg("-c")
                .arg(source)
                .arg(&destination)
                .status()?
                .success(),
            "APFS clone failed"
        );
    } else {
        fs::copy(source, &destination)?;
    }
    fs::set_permissions(&destination, fs::Permissions::from_mode(0o400))?;
    fs::File::open(&destination)?.sync_all()?;
    Ok(pin(digest, size))
}

/// Perform local setup and atomically register only a fully validated image.
pub async fn run(args: Args) -> Result<()> {
    ctrlc::set_handler(|| {
        CANCELLED.store(true, Ordering::SeqCst);
    })?;
    if let Some(disk) = &args.provision_disk {
        return provision(disk);
    }
    // SAFETY: reads only the process identity.
    #[allow(unsafe_code)]
    let uid = unsafe { libc::geteuid() };
    ensure!(
        uid != 0,
        "run setup as your normal user; it requests sudo only for guest disk provisioning"
    );
    ensure!(
        std::env::consts::ARCH == "aarch64",
        "Apple-silicon host required"
    );
    let executable = std::env::current_exe()?.canonicalize()?;
    let bin = executable
        .parent()
        .context("setup binary directory missing")?;
    let prefix = args
        .prefix
        .clone()
        .unwrap_or(bin.parent().context("installation prefix missing")?.into())
        .canonicalize()?;
    ensure!(
        prefix.join("bin/vz-runtimed").is_file(),
        "install vz-runtimed in the selected prefix first"
    );
    let source = args
        .xcode
        .as_ref()
        .map(|p| p.canonicalize())
        .transpose()
        .context("select an installed Xcode application with --xcode")?;
    ensure!(
        source.is_some() || !args.accept_xcode_license,
        "--accept-xcode-license requires --xcode"
    );
    let toolchain_recipe = source
        .as_ref()
        .map(|source| -> Result<_> {
            Ok(serde_json::json!({"source":source,
            "xcode_info":hash_file(&source.join("Contents/Info.plist"))?.0,
            "anchors":package::source_identity(source)?}))
        })
        .transpose()?;
    let loader = bin.join("vz-agent-loader");
    let agent = bin.join("vz-guest-agent");
    let ipsw_pin: Artifact = serde_json::from_str(include_str!(
        "../../../../config/macos-26.3.1-25D2128-ipsw.json"
    ))?;
    let recipe = serde_json::json!({"schema_version":1,"setup_binary":hash_file(&executable)?.0,
        "ipsw":ipsw_pin,"toolchain":toolchain_recipe,
        "loader":hash_file(&loader)?.0,"agent":hash_file(&agent)?.0});
    let recipe_sha256 = hash_bytes(&serde_json::to_vec(&recipe)?);
    let root = prefix.join("macos-local");
    private(&root)?;
    let lock = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(root.join("setup.lock"))?;
    ensure!(
        lock.metadata()?.is_file() && lock.metadata()?.nlink() == 1,
        "invalid setup lock"
    );
    loop {
        check_cancelled()?;
        match lock.try_lock_exclusive() {
            Ok(()) => break,
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                event(args.json, "Waiting for another macOS setup", 0, 0)?;
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
            Err(e) => return Err(e.into()),
        }
    }
    let receipt_path = root.join(format!("{recipe_sha256}.json"));
    let cache = BootstrapCache::new(root.join("cache"))?;
    if receipt_path.exists() {
        let receipt: SetupReceipt = serde_json::from_slice(&fs::read(&receipt_path)?)?;
        ensure!(
            receipt.schema_version == 1
                && receipt.recipe_sha256 == recipe_sha256
                && receipt.bundle.parent() == Some(&root.join("images")),
            "invalid local setup receipt"
        );
        private(&receipt.bundle)?;
        ensure!(receipt.files.len() == 4, "incomplete local image receipt");
        for expected in &receipt.files {
            ensure!(
                *expected == LocalStamp::read(&receipt.bundle, &expected.digest)?,
                "local image changed since validation"
            );
        }
        cache
            .prepare_installed(
                &pin(receipt.manifest_sha256.clone(), receipt.manifest_size),
                &receipt.bundle,
                |_| check_cancelled(),
            )
            .await?;
        register(&prefix, &receipt)?;
        event(args.json, "Using validated local macOS template", 1, 1)?;
        return Ok(());
    }
    ensure!(
        source.is_none() || args.accept_xcode_license,
        "review the selected Xcode license and rerun with --accept-xcode-license to accept it inside the VM"
    );
    let stage = tempfile::Builder::new()
        .prefix("setup-")
        .permissions(fs::Permissions::from_mode(0o700))
        .tempdir_in(&root)?;
    let toolchain = if let Some(source) = &source {
        let payload = stage.path().join("payload");
        private(&payload)?;
        event(args.json, "Packaging local Xcode", 0, 0)?;
        let pin = package::package(source, &payload)?;
        Some((payload, pin))
    } else {
        None
    };
    check_cancelled()?;
    let downloads = ArtifactCache::new(root.join("downloads"))?;
    let mut last = std::time::Instant::now() - Duration::from_secs(1);
    let mut progress = |p: vz_macos_provision::artifact_cache::Progress| -> Result<()> {
        check_cancelled()?;
        if last.elapsed() >= Duration::from_secs(1) || p.completed == p.total {
            event(
                args.json,
                "Acquiring pinned Apple IPSW",
                p.completed,
                p.total,
            )?;
            last = std::time::Instant::now();
        }
        Ok(())
    };
    let ipsw = if let Some(path) = &args.ipsw {
        downloads
            .ensure_installed(&ipsw_pin, &path.canonicalize()?, &mut progress)
            .await?
    } else {
        downloads.ensure(&ipsw_pin, &mut progress).await?
    };
    check_cancelled()?;
    event(args.json, "Installing pinned macOS locally", 0, 0)?;
    // Virtualization.framework identifies restore media by its .ipsw suffix.
    // Cache blobs use digest-only names; preserve their verified bytes in a
    // private COW clone with the required suffix, without another full copy.
    let restore = stage.path().join("restore.ipsw");
    ensure!(
        Command::new("/bin/cp")
            .arg("-c")
            .arg(&ipsw)
            .arg(&restore)
            .status()?
            .success(),
        "prepare named IPSW clone (APFS required)"
    );
    let installed = vz::install_macos(
        vz::IpswSource::Path(restore),
        &stage.path().join("base.img"),
        DISK_SIZE,
    )
    .await?;
    check_cancelled()?;
    event(
        args.json,
        "Administrator access: install startup loader inside the new guest disk",
        0,
        0,
    )?;
    let status = if std::io::stdin().is_terminal() {
        Command::new("/usr/bin/sudo")
            .arg("--")
            .arg(&executable)
            .arg("--provision-disk")
            .arg(&installed.disk_path)
            .status()?
    } else {
        // Use the standard macOS administrator dialog when there is no terminal.
        // The AppleScript is static; shell arguments are individually quoted.
        let command = shell_words::join([
            "/usr/bin/env".to_string(),
            format!("SUDO_UID={uid}"),
            "PATH=/usr/bin:/bin:/usr/sbin:/sbin".to_string(),
            executable.to_string_lossy().into_owned(),
            "--provision-disk".to_string(),
            installed.disk_path.to_string_lossy().into_owned(),
        ]);
        Command::new("/usr/bin/osascript").args(["-e", "on run argv\n do shell script (item 1 of argv) with administrator privileges\nend run", &command]).status()?
    };
    ensure!(
        status.success(),
        "privileged provisioning did not complete; no template registered"
    );
    check_cancelled()?;
    let fixture = stage.path().join("fixture");
    if toolchain.is_some() {
        for (relative, content) in [
            (
                "Package.swift",
                include_str!("../../../../tests/fixtures/vz-0.4/native-macos-swift/Package.swift"),
            ),
            (
                "Sources/NativeProbe/NativeProbe.swift",
                include_str!(
                    "../../../../tests/fixtures/vz-0.4/native-macos-swift/Sources/NativeProbe/NativeProbe.swift"
                ),
            ),
            (
                "Tests/NativeProbeTests/NativeProbeTests.swift",
                include_str!(
                    "../../../../tests/fixtures/vz-0.4/native-macos-swift/Tests/NativeProbeTests/NativeProbeTests.swift"
                ),
            ),
        ] {
            let path = fixture.join(relative);
            fs::create_dir_all(path.parent().context("fixture parent")?)?;
            fs::write(path, content)?;
        }
    }
    let candidate = stage.path().join("candidate");
    event(
        args.json,
        if toolchain.is_some() {
            "Installing Xcode and validating native Swift build/test/run"
        } else {
            "Validating macOS without developer tools"
        },
        0,
        0,
    )?;
    toolchain_install::run(toolchain_install::Args {
        disk: installed.disk_path,
        hardware: installed.hardware_model_path,
        auxiliary: installed.auxiliary_storage_path,
        output: candidate.clone(),
        payload: toolchain.as_ref().map(|(payload, _)| payload.clone()),
        toolchain_sha256: toolchain.as_ref().map(|(_, pin)| pin.clone()),
        reuse_installed_toolchain: false,
        accept_xcode_license: args.accept_xcode_license,
        fixture: toolchain.as_ref().map(|_| fixture),
        expected_os: Some(format!("{VERSION}/{BUILD}")),
    })
    .await?;
    check_cancelled()?;
    event(args.json, "Verifying and caching prepared image", 0, 0)?;
    let bundle = stage.path().join("bundle");
    private(&bundle)?;
    let local_image = copy_blob(&candidate.join("disk.img"), &bundle, true)?;
    let manifest = ReleaseManifest {
        schema_version: 2,
        development: true,
        macos_version: VERSION.into(),
        macos_build: BUILD.into(),
        base: None,
        patch: None,
        prepared_image: ImageIdentity {
            sha256: local_image.sha256.clone(),
            size_bytes: local_image.size_bytes,
        },
        local_image: Some(local_image),
        platform: Platform {
            architecture: "aarch64".into(),
            minimum_host_version: VERSION.into(),
            minimum_cpu_count: 2,
            minimum_memory_bytes: 4 * 1024 * 1024 * 1024,
            hardware_model: copy_blob(&candidate.join("hardware-model"), &bundle, false)?,
            auxiliary_storage_seed: copy_blob(
                &candidate.join("auxiliary-storage"),
                &bundle,
                false,
            )?,
        },
        guest_agent_sha256: hash_file(&agent)?.0,
        toolchain_sha256: toolchain
            .as_ref()
            .map(|(_, pin)| pin.clone())
            .unwrap_or_default(),
    };
    manifest.validate()?;
    let bytes = serde_json::to_vec_pretty(&manifest)?;
    let manifest_sha256 = hash_bytes(&bytes);
    fs::write(bundle.join(&manifest_sha256), &bytes)?;
    fs::set_permissions(
        bundle.join(&manifest_sha256),
        fs::Permissions::from_mode(0o400),
    )?;
    cache
        .prepare_installed(
            &pin(manifest_sha256.clone(), bytes.len() as u64),
            &bundle,
            |_| check_cancelled(),
        )
        .await?;
    let images = root.join("images");
    private(&images)?;
    let destination = images.join(&manifest_sha256);
    ensure!(
        !destination.exists(),
        "local image publication already exists without its recipe receipt"
    );
    fs::rename(&bundle, &destination)?;
    // Retain exact preparation evidence next to the manifest, not mutable disks.
    let mut evidence = vec!["guest-os.json", "dev-home.json", "shutdown.json"];
    if toolchain.is_some() {
        evidence.extend([
            "verification.json",
            "signature.json",
            "preflight-build.json",
            "preflight-test.json",
            "preflight-run.json",
            "license-acceptance.json",
        ]);
    } else {
        evidence.push("clean.json");
    }
    for name in evidence {
        fs::copy(candidate.join(name), destination.join(name))?;
    }
    fs::write(
        destination.join("recipe.json"),
        serde_json::to_vec_pretty(&recipe)?,
    )?;
    fs::File::open(&destination)?.sync_all()?;
    let files = manifest
        .artifacts()
        .into_iter()
        .map(|a| LocalStamp::read(&destination, &a.sha256))
        .chain(std::iter::once(LocalStamp::read(
            &destination,
            &manifest_sha256,
        )))
        .collect::<Result<Vec<_>>>()?;
    let receipt = SetupReceipt {
        files,
        schema_version: 1,
        recipe_sha256,
        manifest_sha256,
        manifest_size: bytes.len() as u64,
        bundle: destination,
    };
    let mut temporary = tempfile::NamedTempFile::new_in(&root)?;
    serde_json::to_writer_pretty(&mut temporary, &receipt)?;
    temporary.as_file().sync_all()?;
    temporary
        .persist_noclobber(receipt_path)
        .map_err(|e| e.error)?;
    fs::File::open(&root)?.sync_all()?;
    register(&prefix, &receipt)?;
    event(args.json, "macOS ready for vz up", 1, 1)?;
    Ok(())
}

fn register(prefix: &Path, receipt: &SetupReceipt) -> Result<()> {
    check_cancelled()?;
    let status = Command::new(prefix.join("bin/vz-runtimed"))
        .arg("--write-installed-machine-target-catalog")
        .arg(prefix)
        .args([
            "--installed-release-version",
            env!("CARGO_PKG_VERSION"),
            "--preserve-installed-catalog",
        ])
        .arg("--installed-native-bundle")
        .arg(&receipt.bundle)
        .arg("--installed-native-manifest-sha256")
        .arg(&receipt.manifest_sha256)
        .stdout(std::process::Stdio::null())
        .status()?;
    ensure!(
        status.success(),
        "local image validated but catalog registration failed; rerun setup to retry registration"
    );
    Ok(())
}

fn provision(disk: &Path) -> Result<()> {
    use vz_macos_provision::{AgentInstallMode, UserConfig, apply_auto_config, attach_and_mount};
    // SAFETY: reads only process identity.
    #[allow(unsafe_code)]
    let uid = unsafe { libc::geteuid() };
    ensure!(
        uid == 0,
        "guest disk provisioning requires administrator access"
    );
    let owner: u32 = std::env::var("SUDO_UID")
        .context("provisioning must be invoked by setup through sudo")?
        .parse()?;
    ensure!(
        owner != 0
            && disk.is_absolute()
            && disk.canonicalize()? == disk
            && disk.file_name().is_some_and(|n| n == "base.img"),
        "invalid owned setup disk"
    );
    let metadata = fs::symlink_metadata(disk)?;
    let parent = disk.parent().context("missing setup disk parent")?;
    let directory = fs::symlink_metadata(parent)?;
    ensure!(
        metadata.is_file()
            && metadata.nlink() == 1
            && metadata.uid() == owner
            && metadata.len() == DISK_SIZE
            && directory.uid() == owner
            && directory.mode() & 0o077 == 0
            && parent
                .file_name()
                .is_some_and(|n| n.to_string_lossy().starts_with("setup-")),
        "provision only the caller's newly installed private setup image"
    );
    let executable = std::env::current_exe()?.canonicalize()?;
    let bin = executable.parent().context("missing binary directory")?;
    let loader = bin.join("vz-agent-loader");
    let agent = bin.join("vz-guest-agent");
    for binary in [&loader, &agent] {
        ensure!(
            fs::symlink_metadata(binary)?.is_file(),
            "missing installed setup component"
        );
    }
    let mounted = attach_and_mount(disk)?;
    let result = (|| {
        let user = UserConfig::default();
        apply_auto_config(
            &mounted.mount_point,
            &user,
            Some(&loader),
            AgentInstallMode::SystemLaunchDaemon,
            Some("vz-agent-loader"),
        )?;
        apply_auto_config(
            &mounted.mount_point,
            &user,
            Some(&agent),
            AgentInstallMode::LoaderManifest,
            None,
        )?;
        ensure!(
            fs::metadata(
                mounted
                    .mount_point
                    .join("Library/LaunchDaemons/com.vz-agent-loader.plist")
            )?
            .uid()
                == 0,
            "guest loader ownership verification failed"
        );
        Ok(())
    })();
    let detached = mounted.detach();
    result?;
    detached?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn developer_tools_are_explicitly_optional() {
        let clean = Args::try_parse_from(["vz-macos-setup"]).unwrap();
        assert!(clean.xcode.is_none());
        assert!(!clean.accept_xcode_license);
        assert!(Args::try_parse_from(["vz-macos-setup", "--accept-xcode-license"]).is_err());
        let xcode = Args::try_parse_from([
            "vz-macos-setup",
            "--xcode",
            "/Applications/Xcode.app",
            "--accept-xcode-license",
        ])
        .unwrap();
        assert!(xcode.xcode.is_some());
        assert!(xcode.accept_xcode_license);
    }

    #[test]
    fn simultaneous_setup_directory_creation_is_safe_and_reusable() -> Result<()> {
        let root = tempfile::tempdir()?;
        let path = root.path().canonicalize()?.join("macos-local");
        std::thread::scope(|scope| -> Result<()> {
            let threads = (0..16)
                .map(|_| scope.spawn(|| private(&path)))
                .collect::<Vec<_>>();
            for thread in threads {
                thread
                    .join()
                    .map_err(|_| anyhow::anyhow!("setup thread failed"))??;
            }
            Ok(())
        })?;
        assert_eq!(fs::metadata(path)?.mode() & 0o777, 0o700);
        Ok(())
    }

    #[test]
    fn local_stamp_detects_replacement_and_rejects_links() -> Result<()> {
        let root = tempfile::tempdir()?;
        let bundle = root.path().canonicalize()?.join("bundle");
        private(&bundle)?;
        let digest = "a".repeat(64);
        let path = bundle.join(&digest);
        fs::write(&path, b"local image")?;
        fs::set_permissions(&path, fs::Permissions::from_mode(0o400))?;
        let before = LocalStamp::read(&bundle, &digest)?;
        fs::remove_file(&path)?;
        fs::write(&path, b"local image")?;
        fs::set_permissions(&path, fs::Permissions::from_mode(0o400))?;
        assert!(before != LocalStamp::read(&bundle, &digest)?);
        let alias = bundle.join("b".repeat(64));
        fs::hard_link(&path, &alias)?;
        assert!(LocalStamp::read(&bundle, &digest).is_err());
        assert!(LocalStamp::read(&bundle, "../outside").is_err());
        Ok(())
    }
}
