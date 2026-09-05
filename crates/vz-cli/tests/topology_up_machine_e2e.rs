//! Opt-in physical public-CLI lifecycle proof on local Apple Silicon.
//! Run the explicitly selected ignored test with signed release CLI/daemon paths.
//! Developer conformance is an EXPECTED MISSING capability, never a parity PASS.
//! No RuntimeDaemon, StateStore mutation, legacy verb, or substitute guest client.
#![cfg(all(target_os = "macos", target_arch = "aarch64"))]

use anyhow::{Context, Result, bail, ensure};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;
use std::fs::{self, File, OpenOptions};
use std::os::unix::fs::{DirBuilderExt, FileTypeExt, MetadataExt, OpenOptionsExt};
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::{Duration, Instant};
use tokio::process::{Child, Command};
use vz_oci_macos::KernelProfile;
use vz_runtime_contract::{MachineProfile, ProjectId, ResourceOwner};
use vz_runtimed::machine_docker_endpoint::MachineDockerEndpoint;
use vz_runtimed::machine_target_resolver::{
    LINUX_APPLIANCE_IMAGE, LinuxTargetCatalogEntry, MACHINE_TARGET_CATALOG_SCHEMA_VERSION,
    MachineTargetCatalog,
};

const LIMIT: u64 = 4 * 1024 * 1024;
const REQUIRED_DEVELOPER_FAILURE: &str =
    "required host Docker/Compose/buildx conformance and managed context evidence are absent";

fn private_directory(path: &Path) -> Result<()> {
    fs::DirBuilder::new().mode(0o700).create(path)?;
    Ok(())
}
fn write_new(path: &Path, bytes: &[u8]) -> Result<()> {
    use std::io::Write;
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    Ok(())
}
fn file_sha(path: &Path) -> Result<String> {
    use std::io::Read;
    let mut file = File::open(path)?;
    let mut hasher = Sha256::new();
    let mut buffer = [0u8; 65536];
    loop {
        let count = file.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        hasher.update(&buffer[..count]);
    }
    Ok(format!("{:x}", hasher.finalize()))
}
fn required_file(name: &str) -> Result<PathBuf> {
    let path = PathBuf::from(
        std::env::var_os(name).with_context(|| format!("required {name}; no fallback"))?,
    );
    let metadata = fs::symlink_metadata(&path)?;
    ensure!(
        path.is_absolute() && metadata.is_file() && metadata.nlink() == 1,
        "{name} must be an absolute regular single-link file"
    );
    Ok(path)
}

struct Output {
    code: Option<i32>,
    stdout: Vec<u8>,
    stderr: Vec<u8>,
}
impl Output {
    fn success(&self) -> Result<()> {
        ensure!(
            self.code == Some(0),
            "command failed {:?}: {}",
            self.code,
            String::from_utf8_lossy(&self.stderr)
        );
        Ok(())
    }
    fn records(&self) -> Result<Vec<Value>> {
        ensure!(
            self.stderr.is_empty(),
            "unexpected stderr: {}",
            String::from_utf8_lossy(&self.stderr)
        );
        std::str::from_utf8(&self.stdout)?
            .lines()
            .map(|line| serde_json::from_str(line).map_err(Into::into))
            .collect()
    }
}

struct Harness {
    evidence: PathBuf,
    root: PathBuf,
    cli: PathBuf,
    docker: PathBuf,
    docker_config: PathBuf,
    database: PathBuf,
    runtime: PathBuf,
    socket: PathBuf,
    next: usize,
    daemon: Option<Child>,
    cleanup: Vec<(PathBuf, String)>,
}
impl Harness {
    async fn run(&mut self, label: &str, mut command: Command, seconds: u64) -> Result<Output> {
        self.next += 1;
        let prefix = format!("{:03}-{label}", self.next);
        let stdout_path = self.evidence.join(format!("{prefix}.stdout"));
        let stderr_path = self.evidence.join(format!("{prefix}.stderr"));
        let executable = command
            .as_std()
            .get_program()
            .to_string_lossy()
            .into_owned();
        // The executable identity stays canonical and hashed; only argv[0]
        // selects the Docker applet in a multicall distribution.
        let argv0 = if command.as_std().get_program() == self.docker.as_os_str() {
            command.as_std_mut().arg0("docker");
            "docker".to_owned()
        } else {
            executable.clone()
        };
        let args: Vec<_> = command
            .as_std()
            .get_args()
            .map(|arg| arg.to_string_lossy().into_owned())
            .collect();
        let cwd = command.as_std().get_current_dir().map(Path::to_path_buf);
        write_new(
            &self.evidence.join(format!("{prefix}.command.json")),
            &serde_json::to_vec_pretty(
                &json!({"executable":executable,"argv0":argv0,"args":args,"cwd":cwd,"timeout_seconds":seconds,"state_db":self.database,"daemon_socket":self.socket,"runtime_data_dir":self.runtime}),
            )?,
        )?;
        command
            .stdin(Stdio::null())
            .stdout(
                OpenOptions::new()
                    .create_new(true)
                    .write(true)
                    .mode(0o600)
                    .open(&stdout_path)?,
            )
            .stderr(
                OpenOptions::new()
                    .create_new(true)
                    .write(true)
                    .mode(0o600)
                    .open(&stderr_path)?,
            )
            .kill_on_drop(true);
        let started = Instant::now();
        let mut child = command.spawn()?;
        let status = tokio::time::timeout(Duration::from_secs(seconds), child.wait()).await;
        let (status, timed_out) = match status {
            Ok(status) => (status?, false),
            Err(_) => {
                child.kill().await?;
                (child.wait().await?, true)
            }
        };
        write_new(
            &self.evidence.join(format!("{prefix}.exit.json")),
            &serde_json::to_vec_pretty(
                &json!({"exit_code":status.code(),"timed_out":timed_out,"elapsed_millis":started.elapsed().as_millis(),"stdout_sha256":file_sha(&stdout_path)?,"stderr_sha256":file_sha(&stderr_path)?}),
            )?,
        )?;
        ensure!(
            !timed_out,
            "{label} timed out; command outcome may be uncertain, raw receipts retained"
        );
        ensure!(
            fs::metadata(&stdout_path)?.len() <= LIMIT
                && fs::metadata(&stderr_path)?.len() <= LIMIT,
            "{label} output exceeds inspection bound; full raw files retained"
        );
        Ok(Output {
            code: status.code(),
            stdout: fs::read(stdout_path)?,
            stderr: fs::read(stderr_path)?,
        })
    }
    fn command(&self, project: &Path, args: &[&str]) -> Command {
        let mut command = Command::new(&self.cli);
        command
            .current_dir(project)
            .args(args)
            .env("VZ_RUNTIME_STATE_DB", &self.database)
            .env("VZ_RUNTIME_DATA_DIR", &self.runtime)
            .env("VZ_RUNTIME_DAEMON_SOCKET", &self.socket)
            .env("VZ_RUNTIME_DAEMON_AUTOSTART", "0");
        clean_environment(&mut command);
        command
    }
    async fn cli(
        &mut self,
        label: &str,
        project: &Path,
        args: &[&str],
        seconds: u64,
    ) -> Result<Output> {
        self.run(label, self.command(project, args), seconds).await
    }
    async fn project(
        &mut self,
        name: &str,
        profile: MachineProfile,
        digest: &str,
        count: usize,
    ) -> Result<PathBuf> {
        let repository = self.root.join(format!("{name}-git"));
        private_directory(&repository)?;
        for args in [
            vec!["init", "--quiet"],
            vec![
                "-c",
                "user.name=vz physical fixture",
                "-c",
                "user.email=fixture@invalid",
                "-c",
                "commit.gpgsign=false",
                "commit",
                "--quiet",
                "--allow-empty",
                "-m",
                "isolated fixture",
            ],
        ] {
            let mut command = Command::new("/usr/bin/git");
            command.current_dir(&repository).args(args);
            clean_environment(&mut command);
            self.run("git-fixture", command, 15).await?.success()?;
        }
        let project = self.root.join(name);
        let mut command = Command::new("/usr/bin/git");
        command
            .current_dir(&repository)
            .args(["worktree", "add", "--quiet", "--detach"])
            .arg(&project);
        clean_environment(&mut command);
        self.run("git-worktree", command, 15).await?.success()?;
        let machines: Vec<_> = (0..count).map(|i| json!({"schema_version":1,"name":format!("machine-{i}"),"profile":profile,"target":{"os":"linux","arch":"aarch64","image":LINUX_APPLIANCE_IMAGE,"digest":digest},"resources":{"cpus":2,"memory_mb":if profile==MachineProfile::Developer {4096}else{1024}}})).collect();
        // Validate against the published product type before writing the actual input.
        let definition: vz_runtime_contract::ProjectDefinition = serde_json::from_value(
            json!({"schema_version":1,"project_id":ProjectId::generate(),"name":name,"environment":{"schema_version":1,"machines":machines}}),
        )?;
        definition.validate()?;
        write_new(
            &project.join("vz.json"),
            &serde_json::to_vec_pretty(&definition)?,
        )?;
        Ok(project)
    }
    async fn up(&mut self, project: &Path, name: &str, label: &str) -> Result<(Output, Value)> {
        // Register an exact named cleanup target before dispatch, including lost responses.
        if !self
            .cleanup
            .iter()
            .any(|item| item.0 == project && item.1 == name)
        {
            self.cleanup.push((project.into(), name.into()));
        }
        let output = self
            .cli(
                label,
                project,
                &[
                    "--json",
                    "up",
                    "--environment",
                    name,
                    "--timeout",
                    "300",
                    "--request-id",
                    label,
                    "--idempotency-key",
                    label,
                ],
                330,
            )
            .await?;
        let records = output.records()?;
        let completion = records
            .last()
            .and_then(|v| v.pointer("/progress/completion"))
            .filter(|v| !v.is_null())
            .context("missing Up completion")?
            .clone();
        ensure!(completion["admission"]["request_id"] == label);
        let environment_id = completion["admission"]["environment_id"]
            .as_str()
            .context("Up admission omitted immutable Environment ID")?
            .to_owned();
        self.cleanup.retain(|(path, selector)| {
            path != project || (selector != name && selector != &environment_id)
        });
        self.cleanup.push((project.into(), environment_id));
        Ok((output, completion))
    }
    async fn status(&mut self, project: &Path, name: &str) -> Result<Value> {
        let output = self
            .cli(
                "status",
                project,
                &["--json", "status", "--environment", name],
                20,
            )
            .await?;
        output.success()?;
        let value: Value = serde_json::from_slice(&output.stdout)?;
        ensure!(
            value["environments"]
                .as_array()
                .context("status environments")?
                .len()
                == 1
        );
        Ok(value["environments"][0].clone())
    }
    async fn stop(&mut self, project: &Path, name: &str, label: &str) -> Result<Value> {
        let output = self
            .cli(
                label,
                project,
                &[
                    "--json",
                    "stop",
                    "--environment",
                    name,
                    "--timeout",
                    "60",
                    "--request-id",
                    label,
                    "--idempotency-key",
                    label,
                ],
                150,
            )
            .await?;
        output.success()?;
        let records = output.records()?;
        let receipt = records.last().context("Stop terminal missing")?.clone();
        ensure!(
            receipt["terminal"] == true && receipt["operation"]["status"] == "succeeded",
            "Stop lacks positive aggregate receipt"
        );
        let status = self.status(project, name).await?;
        ensure!(status["state"] == "stopped");
        ensure!(
            status["machines"]
                .as_array()
                .context("machines")?
                .iter()
                .all(|machine| machine["state"] == "stopped")
        );
        Ok(receipt)
    }
    async fn exec(
        &mut self,
        project: &Path,
        name: &str,
        label: &str,
        shell: &str,
    ) -> Result<Output> {
        self.cli(
            label,
            project,
            &[
                "exec",
                "--environment",
                name,
                "--machine",
                "machine-0",
                "--no-stdin",
                "--timeout",
                "30",
                "--request-id",
                label,
                "--idempotency-key",
                label,
                "--",
                "/bin/sh",
                "-c",
                shell,
            ],
            45,
        )
        .await
    }
    async fn cleanup(&mut self) -> Vec<String> {
        let mut errors = Vec::new();
        for (index, (project, name)) in self.cleanup.clone().into_iter().enumerate().rev() {
            if let Err(error) = self
                .stop(&project, &name, &format!("cleanup-stop-{index}"))
                .await
            {
                errors.push(format!("{} {name}: {error:#}", project.display()));
            }
        }
        errors
    }

    fn docker_command(&self) -> Command {
        let mut command = Command::new(&self.docker);
        command
            .current_dir(&self.root)
            .arg("--config")
            .arg(&self.docker_config);
        // Never let ambient Docker contexts, TLS flags, API overrides, or
        // a developer's client config reroute this exact Machine probe.
        for (name, _) in std::env::vars_os() {
            if name.to_string_lossy().starts_with("DOCKER_") {
                command.env_remove(name);
            }
        }
        command
            .env_remove("HTTP_PROXY")
            .env_remove("HTTPS_PROXY")
            .env_remove("ALL_PROXY")
            .env_remove("http_proxy")
            .env_remove("https_proxy")
            .env_remove("all_proxy");
        clean_environment(&mut command);
        command
    }

    async fn engine_ids(&mut self, sockets: &[PathBuf], label: &str) -> Result<Vec<String>> {
        let mut identities = Vec::new();
        for (index, socket) in sockets.iter().enumerate() {
            let mut command = self.docker_command();
            command
                .arg("--host")
                .arg(format!("unix://{}", socket.display()))
                .args(["info", "--format", "{{.ID}}"]);
            let output = self.run(&format!("{label}-{index}"), command, 30).await?;
            output.success()?;
            ensure!(
                output.stderr.is_empty(),
                "host Docker info emitted unexpected stderr"
            );
            let raw = std::str::from_utf8(&output.stdout)?;
            let identity = raw.trim();
            ensure!(
                !identity.is_empty()
                    && identity.len() <= 256
                    && !identity.chars().any(char::is_whitespace),
                "invalid Engine identity"
            );
            identities.push(identity.to_owned());
        }
        ensure!(
            identities.iter().collect::<BTreeSet<_>>().len() == sockets.len(),
            "distinct Machine endpoints reached the same Engine"
        );
        Ok(identities)
    }
}

fn clean_environment(command: &mut Command) {
    // The fixture owns its Git repository: inherited configuration must not
    // redirect its index, repository, hooks, or signing to the user's checkout.
    for (name, _) in std::env::vars_os() {
        if name.to_string_lossy().starts_with("GIT_") {
            command.env_remove(name);
        }
    }
    command
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_CONFIG_GLOBAL", "/dev/null");
    for name in [
        "GIT_DIR",
        "GIT_WORK_TREE",
        "GIT_INDEX_FILE",
        "GIT_COMMON_DIR",
        "VZ_CONTROL_PLANE_TRANSPORT",
        "VZ_ENVIRONMENT_ID",
        "VZ_MACHINE_ID",
        "RUST_LOG",
        "VZ_RUNTIMED_MIGRATE_LEGACY_CHECKPOINT_ARTIFACTS",
        "VZ_SANDBOX_DEFAULT_BASE_IMAGE",
        "VZ_SANDBOX_DEFAULT_MAIN_CONTAINER",
        "VZ_TEST_INSTALLED_CLI",
        "VZ_TEST_INSTALLED_DAEMON",
    ] {
        command.env_remove(name);
    }
}

async fn scenario(
    harness: &mut Harness,
    developer_digest: &str,
    hardened_digest: &str,
) -> Result<Value> {
    let developer = harness
        .project("developer", MachineProfile::Developer, developer_digest, 2)
        .await?;
    let hardened = harness
        .project("hardened", MachineProfile::Hardened, hardened_digest, 1)
        .await?;
    let (output, completion) = harness
        .up(&developer, "parallel-dev", "developer-up")
        .await?;
    ensure!(
        output.code == Some(2),
        "Developer must expose current missing conformance, not Ready or unrelated failure"
    );
    ensure!(completion["operation"]["status"] == "failed");
    let steps = completion["operation"]["machine_steps"]
        .as_array()
        .context("Developer Machine steps")?;
    ensure!(
        steps.len() == 2
            && steps.iter().all(|step| step["status"] == "failed"
                && step["failure_reason"]
                    .as_str()
                    .is_some_and(|reason| reason.contains(REQUIRED_DEVELOPER_FAILURE))),
        "both Developer boots must reach explicit missing-conformance boundary"
    );
    let admission = &completion["admission"];
    let mut sockets = Vec::new();
    for step in steps {
        let owner = ResourceOwner {
            project_id: serde_json::from_value(admission["project_id"].clone())?,
            environment_id: serde_json::from_value(admission["environment_id"].clone())?,
            machine_id: Some(serde_json::from_value(step["machine_id"].clone())?),
        };
        let socket = MachineDockerEndpoint::socket_path_for(&harness.runtime, &owner)?;
        let metadata = fs::symlink_metadata(&socket)?;
        ensure!(metadata.file_type().is_socket() && metadata.mode() & 0o777 == 0o600);
        sockets.push(socket);
    }
    ensure!(sockets[0] != sockets[1]);
    let engine_ids = harness.engine_ids(&sockets, "docker-info-before").await?;
    let dev_before = harness.status(&developer, "parallel-dev").await?;
    // Independent live Environment is retained while the Hardened lifecycle runs.
    let (output, first) = harness.up(&hardened, "posix-dev", "hardened-up-1").await?;
    output.success()?;
    ensure!(first["error"].is_null() && first["operation"]["status"] == "succeeded");
    let first_status = harness.status(&hardened, "posix-dev").await?;
    let uname = harness
        .exec(&hardened, "posix-dev", "uname-1", "uname -s")
        .await?;
    uname.success()?;
    ensure!(uname.stdout == b"Linux\n" && uname.stderr.is_empty());
    let nonzero = harness
        .exec(
            &hardened,
            "posix-dev",
            "exit-23",
            "printf exact-nonzero; printf exact-stderr >&2; exit 23",
        )
        .await?;
    ensure!(
        nonzero.code == Some(23)
            && nonzero.stdout == b"exact-nonzero"
            && nonzero.stderr == b"exact-stderr"
    );
    // initramfs switches into an overlay root and exposes its persistent
    // VirtioFS backing share at /vz-rootfs, not the pre-switch /mnt/rootfs.
    // Retain positive mount evidence before writing a scoped sentinel there.
    let backing_mount = harness
        .exec(
            &hardened,
            "posix-dev",
            "backing-mount",
            "awk '$2 == \"/vz-rootfs\" && $3 == \"virtiofs\" { print; found=1 } END { exit !found }' /proc/mounts",
        )
        .await?;
    backing_mount.success()?;
    ensure!(!backing_mount.stdout.is_empty() && backing_mount.stderr.is_empty());
    let token = uuid::Uuid::new_v4().simple().to_string();
    let sentinel = format!("/vz-rootfs/vz-public-cli-{token}");
    let create =
        format!("test ! -e {sentinel} && printf {token} > {sentinel} && sync && cat {sentinel}");
    let written = harness
        .exec(&hardened, "posix-dev", "sentinel-write", &create)
        .await?;
    written.success()?;
    ensure!(written.stdout == token.as_bytes() && written.stderr.is_empty());
    harness
        .stop(&hardened, "posix-dev", "hardened-stop-1")
        .await?;
    ensure!(
        harness.status(&developer, "parallel-dev").await? == dev_before,
        "unrelated Environment state changed"
    );
    for socket in &sockets {
        ensure!(fs::symlink_metadata(socket)?.file_type().is_socket());
    }
    ensure!(
        harness
            .engine_ids(&sockets, "docker-info-after-stop")
            .await?
            == engine_ids,
        "Hardened Stop changed or broke a sibling Engine endpoint"
    );
    let (output, second) = harness.up(&hardened, "posix-dev", "hardened-up-2").await?;
    output.success()?;
    ensure!(second["operation"]["status"] == "succeeded");
    let second_status = harness.status(&hardened, "posix-dev").await?;
    ensure!(
        harness
            .engine_ids(&sockets, "docker-info-after-reup")
            .await?
            == engine_ids,
        "Hardened re-Up changed or broke a sibling Engine endpoint"
    );
    ensure!(
        first_status["environment_id"] == second_status["environment_id"]
            && first_status["machines"][0]["machine_id"]
                == second_status["machines"][0]["machine_id"]
    );
    let read = harness
        .exec(
            &hardened,
            "posix-dev",
            "sentinel-read",
            &format!("cat {sentinel}"),
        )
        .await?;
    read.success()?;
    ensure!(read.stdout == token.as_bytes() && read.stderr.is_empty());
    let removed = harness
        .exec(
            &hardened,
            "posix-dev",
            "sentinel-remove",
            &format!(
                "test \"$(cat {sentinel})\" = {token} && rm -- {sentinel} && test ! -e {sentinel}"
            ),
        )
        .await?;
    removed.success()?;
    harness
        .stop(&hardened, "posix-dev", "hardened-stop-2")
        .await?;
    ensure!(harness.status(&developer, "parallel-dev").await? == dev_before);
    let dev_stop = harness
        .stop(&developer, "parallel-dev", "developer-stop")
        .await?;
    for socket in &sockets {
        ensure!(
            matches!(fs::symlink_metadata(socket),Err(error) if error.kind()==std::io::ErrorKind::NotFound),
            "Developer endpoint remains after positive Stop"
        );
    }
    Ok(
        json!({"developer":{"outcome":"EXPECTED_MISSING_CONFORMANCE","completion":completion,"private_sockets":sockets,"engine_ids":engine_ids,"host_docker_metadata_only":true,"same_engines_usable_after_hardened_stop_and_reup":true,"positive_stop":dev_stop},"hardened":{"first_up":first,"second_up":second,"first_status":first_status,"second_status":second_status,"backing_store_sentinel_persisted":true,"exact_sentinel_removed":sentinel},"cross_environment_state_and_endpoint_preservation":true,"docker_parity_certified":false,"general_workspace_persistence_certified":false,"deterministic_partial_start_physical_regression":"PENDING_SEPARATE_GATE_NO_EXTERNAL_DAEMON_BARRIER"}),
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires explicit signed release CLI/daemon and three real local Mac VMs"]
async fn installed_public_up_exec_stop_machine_lifecycle() -> Result<()> {
    ensure!(
        std::env::var("VZ_TOPOLOGY_UP_MACHINE_E2E").as_deref() == Ok("1"),
        "explicit physical opt-in required"
    );
    ensure!(
        std::env::var("VZ_TEST_INSTALLED_BUILD_PROFILE").as_deref() == Ok("release"),
        "runner must attest release build profile"
    );
    let cli = required_file("VZ_TEST_INSTALLED_CLI")?;
    let daemon = required_file("VZ_TEST_INSTALLED_DAEMON")?;
    let docker = required_file("VZ_TEST_HOST_DOCKER")?;
    let expected_docker_sha =
        std::env::var("VZ_TEST_HOST_DOCKER_SHA256").context("exact host Docker SHA256 required")?;
    ensure!(
        expected_docker_sha.len() == 64
            && expected_docker_sha
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)),
        "host Docker SHA256 must be lowercase hexadecimal"
    );
    ensure!(
        file_sha(&docker)? == expected_docker_sha,
        "host Docker executable hash mismatch"
    );
    let evidence = PathBuf::from(
        std::env::var_os("VZ_TOPOLOGY_UP_MACHINE_EVIDENCE")
            .context("evidence directory required")?,
    );
    let metadata = fs::symlink_metadata(&evidence)?;
    ensure!(
        evidence.is_absolute()
            && metadata.is_dir()
            && metadata.mode() & 0o777 == 0o700
            && fs::read_dir(&evidence)?.next().is_none(),
        "evidence must be existing empty private directory"
    );
    let root = tempfile::Builder::new()
        .prefix("vzu-")
        .tempdir_in("/private/tmp")?
        .keep();
    let docker_config = root.join("docker-client");
    private_directory(&docker_config)?;
    write_new(&docker_config.join("config.json"), b"{}\n")?;
    let mut harness = Harness {
        evidence: evidence.clone(),
        root: root.clone(),
        cli: cli.clone(),
        docker: docker.clone(),
        docker_config: docker_config.clone(),
        database: root.join("state.db"),
        runtime: root.join("r"),
        socket: root.join("d.sock"),
        next: 0,
        daemon: None,
        cleanup: vec![],
    };
    write_new(
        &evidence.join("ownership.json"),
        &serde_json::to_vec_pretty(
            &json!({"retained_root":root,"cli":cli,"cli_sha256":file_sha(&cli)?,"daemon":daemon,"daemon_sha256":file_sha(&daemon)?,"host_docker":docker,"host_docker_sha256":expected_docker_sha,"private_docker_config":docker_config,"build_profile_attestation":"release","automatic_directory_deletion":false}),
        )?,
    )?;
    let mut version_command = harness.docker_command();
    version_command.arg("--version");
    let docker_version = harness
        .run("host-docker-version-preflight", version_command, 15)
        .await?;
    docker_version.success()?;
    ensure!(
        docker_version.stderr.is_empty()
            && std::str::from_utf8(&docker_version.stdout)?.starts_with("Docker version "),
        "canonical Docker applet failed pre-VM version preflight"
    );
    for binary in [&cli, &daemon] {
        let mut command = Command::new("/usr/bin/codesign");
        command.args(["--verify", "--strict"]).arg(binary);
        harness
            .run("codesign-verify", command, 15)
            .await?
            .success()?;
    }
    let mut command = Command::new("/usr/bin/codesign");
    command.args(["-d", "--entitlements", ":-"]).arg(&daemon);
    let entitlements = harness.run("daemon-entitlements", command, 15).await?;
    entitlements.success()?;
    ensure!(
        format!(
            "{}{}",
            String::from_utf8_lossy(&entitlements.stdout),
            String::from_utf8_lossy(&entitlements.stderr)
        )
        .split_whitespace()
        .collect::<String>()
        .contains("<key>com.apple.security.virtualization</key><true/>")
    );
    let repo = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
    let developer = fs::canonicalize(repo.join("linux/out"))?;
    let hardened = fs::canonicalize(repo.join("linux/out/container"))?;
    let dev =
        vz_linux::verify_kernel_bundle_read_only(&developer, KernelProfile::Developer).await?;
    let hard =
        vz_linux::verify_kernel_bundle_read_only(&hardened, KernelProfile::Container).await?;
    let identity = |value: &vz_linux::KernelBundleArtifactIdentity| {
        json!({
            "kernel_sha256":value.kernel_sha256,"initramfs_sha256":value.initramfs_sha256,
            "youki_sha256":value.youki_sha256,"version_sha256":value.version_sha256,
            "aggregate_digest":value.digest
        })
    };
    write_new(
        &evidence.join("verified-bundles.json"),
        &serde_json::to_vec_pretty(&json!({
            "developer":{"directory":developer,"identity":identity(&dev.artifact_identity)},
            "hardened":{"directory":hardened,"identity":identity(&hard.artifact_identity)},
            "validation":"verify_kernel_bundle_read_only","source":"normal linux/out bundles"
        }))?,
    )?;
    let entry = |profile, bundle_dir, digest| LinuxTargetCatalogEntry {
        image: LINUX_APPLIANCE_IMAGE.into(),
        version: "0.4.0-public-cli-e2e".into(),
        profile,
        bundle_dir,
        digest,
        channels: BTreeSet::new(),
    };
    let catalog = MachineTargetCatalog {
        schema_version: MACHINE_TARGET_CATALOG_SCHEMA_VERSION,
        linux: vec![
            entry(
                MachineProfile::Developer,
                developer,
                dev.artifact_identity.digest.clone(),
            ),
            entry(
                MachineProfile::Hardened,
                hardened,
                hard.artifact_identity.digest.clone(),
            ),
        ],
    };
    let catalog_path = root.join("catalog.json");
    write_new(&catalog_path, &serde_json::to_vec_pretty(&catalog)?)?;
    write_new(
        &evidence.join("catalog.json"),
        &serde_json::to_vec_pretty(&catalog)?,
    )?;
    let mut command = Command::new(&daemon);
    command
        .args(["--state-store-path"])
        .arg(&harness.database)
        .arg("--runtime-data-dir")
        .arg(&harness.runtime)
        .arg("--socket-path")
        .arg(&harness.socket)
        .arg("--machine-target-catalog")
        .arg(&catalog_path)
        .current_dir(&root)
        .stdin(Stdio::null())
        .stdout(
            OpenOptions::new()
                .create_new(true)
                .write(true)
                .mode(0o600)
                .open(evidence.join("daemon.stdout"))?,
        )
        .stderr(
            OpenOptions::new()
                .create_new(true)
                .write(true)
                .mode(0o600)
                .open(evidence.join("daemon.stderr"))?,
        );
    clean_environment(&mut command);
    let daemon_args: Vec<_> = command
        .as_std()
        .get_args()
        .map(|arg| arg.to_string_lossy().into_owned())
        .collect();
    write_new(
        &evidence.join("daemon.command.json"),
        &serde_json::to_vec_pretty(&json!({"executable":daemon,"args":daemon_args,"cwd":root}))?,
    )?;
    harness.daemon = Some(command.spawn()?);
    let result = async {
        tokio::time::timeout(Duration::from_secs(20), async {
            loop {
                if tokio::net::UnixStream::connect(&harness.socket)
                    .await
                    .is_ok()
                {
                    break Ok::<(), anyhow::Error>(());
                }
                ensure!(
                    harness
                        .daemon
                        .as_mut()
                        .context("daemon handle")?
                        .try_wait()?
                        .is_none(),
                    "daemon exited before readiness"
                );
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .context("daemon readiness timeout")??;
        scenario(
            &mut harness,
            &dev.artifact_identity.digest,
            &hard.artifact_identity.digest,
        )
        .await
    }
    .await;
    let cleanup_errors = harness.cleanup().await;
    // Keep the exact daemon alive if Stop is uncertain: killing it would discard
    // the only retained boot ownership. Root and all evidence remain available.
    let daemon_exit = if cleanup_errors.is_empty() {
        if let Some(mut child) = harness.daemon.take() {
            if child.try_wait()?.is_some() {
                Some("exited-before-shutdown".to_string())
            } else {
                let pid = child.id().context("owned daemon pid")?;
                let mut command = Command::new("/bin/kill");
                command.args(["-TERM", &pid.to_string()]);
                harness.run("daemon-term", command, 5).await?.success()?;
                match tokio::time::timeout(Duration::from_secs(15), child.wait()).await {
                    Ok(status) => Some(format!("{}", status?)),
                    Err(_) => Some(format!("shutdown-timeout-retained-pid-{pid}")),
                }
            }
        } else {
            None
        }
    } else {
        harness
            .daemon
            .as_ref()
            .and_then(Child::id)
            .map(|pid| format!("cleanup-uncertain-retained-pid-{pid}"))
    };
    let report = json!({"schema_version":1,"scope":"DEV_PHYSICAL_PUBLIC_CLI_NOT_RELEASE_CERTIFICATION","scenario":result.as_ref().ok(),"error":result.as_ref().err().map(|error|format!("{error:#}")),"cleanup_errors":cleanup_errors,"daemon_exit":daemon_exit,"retained_root":root});
    write_new(
        &evidence.join("result.json"),
        &serde_json::to_vec_pretty(&report)?,
    )?;
    if !cleanup_errors.is_empty() {
        bail!(
            "exact CLI Stop cleanup uncertain; root and daemon retained: {}",
            evidence.display()
        );
    }
    result?;
    ensure!(
        daemon_exit.as_deref() == Some("exit status: 0"),
        "daemon did not exit cleanly; inspect retained evidence"
    );
    Ok(())
}
