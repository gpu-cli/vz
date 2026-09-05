//! Offline startup operations, not the full Docker compatibility release gate.
//! Every command is durably admitted before dispatch. An unfinished journal is
//! recovery authority, never permission to repeat uncertain Engine mutations.

use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::fs::File;
use std::io::{Read, Write};
use std::os::unix::fs::{DirBuilderExt, MetadataExt};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::Instant;
use vz_linux::VerifiedDeveloperProbe;
use vz_runtime_contract::{
    CapabilitySet, LifecycleOperationId, MachineCapability, MachineIncarnation, ResourceOwner,
};

use crate::machine_docker_context::ManagedMachineDockerContext;
use crate::machine_docker_host::{HostDockerClient, HostDockerOutput};
use crate::machine_docker_runtime_inventory::VerifiedMachineRuntimeInventory;
use crate::machine_runtime_registry::MachineRuntimeStoreLease;

const JOURNAL: &str = "docker-operational-probe.json";
const LABEL: &str = "dev.vz.startup-probe";
const MARKER: &[u8] = b"vz-developer-probe-v1\n";
const PAYLOAD: &[u8] = b"vz-buildx-startup-probe-v1\n";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MachineDockerOperationalEvidence {
    pub schema_version: u32,
    pub owner: ResourceOwner,
    pub incarnation: MachineIncarnation,
    pub configuration_digest: String,
    pub engine_id: String,
    /// Moby may advertise built-in names without installing those runtimes.
    /// Readiness additionally requires exact guest runtime-binary inventory.
    pub inert_stock_runtime_metadata: Vec<String>,
    pub capabilities: CapabilitySet,
    pub archive_sha256: String,
    pub client_sha256: String,
    pub runtime_inventory: Value,
    pub cleanup_confirmed: bool,
    /// Exact disposable runtime objects, excluding normal Machine BuildKit cache.
    pub cleanup_scope: String,
    pub retained_buildkit_cache: bool,
    pub receipt_path: PathBuf,
    pub receipt_sha256: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Journal {
    schema_version: u32,
    owner: ResourceOwner,
    incarnation: MachineIncarnation,
    configuration_digest: String,
    context: String,
    token: String,
    state: String,
    archive_sha256: String,
    client_sha256: String,
    commands: Vec<Value>,
    resources: Value,
    failure: Option<String>,
}

struct Run<'a> {
    client: &'a HostDockerClient,
    context: &'a ManagedMachineDockerContext,
    store: Arc<MachineRuntimeStoreLease>,
    journal: Journal,
    directory: PathBuf,
    deadline: Instant,
    _lock: File,
}

/// Verify actual unmodified host-client operations on the exact private Machine.
/// Mutating failed/uncertain attempts retain their journal and precise resources
/// for authoritative reconciliation. Proven read-only failures are archived
/// immutably before a new attempt on the same owned Machine incarnation.
pub async fn verify(
    client: &HostDockerClient,
    context: &ManagedMachineDockerContext,
    store: Arc<MachineRuntimeStoreLease>,
    probe: &VerifiedDeveloperProbe,
    inventory: &VerifiedMachineRuntimeInventory,
    incarnation: &MachineIncarnation,
    deadline: Instant,
) -> Result<MachineDockerOperationalEvidence> {
    store.validate_current()?;
    ensure!(
        inventory.owner() == store.owner() && inventory.incarnation() == incarnation,
        "runtime inventory does not belong to this exact Machine incarnation"
    );
    ensure!(
        store.owner().machine_id.as_ref() == Some(&incarnation.machine_id),
        "foreign probe incarnation"
    );
    probe.metadata.validate()?;
    let lock = open_owned(&store, "docker-operational-probe.lock", true)?;
    fs2::FileExt::try_lock_exclusive(&lock).context("another startup probe owns this Machine")?;
    let archived_failure = if let Some(previous) = read_journal(&store)? {
        if previous.state == "completed" {
            validate_previous(&previous, store.owner())?;
            None
        } else {
            validate_no_mutation(&previous, &store, incarnation, context.name())?;
            Some(archive_no_mutation(&previous, &store)?)
        }
    } else {
        None
    };
    let token = format!(
        "vzprobe-{}",
        &hash(LifecycleOperationId::generate().to_string().as_bytes())[..24]
    );
    let directory = store.data_path().join(&token);
    std::fs::DirBuilder::new().mode(0o700).create(&directory)?;
    File::open(store.data_path())?.sync_all()?;
    let journal = Journal {
        schema_version: 1,
        owner: store.owner().clone(),
        incarnation: incarnation.clone(),
        configuration_digest: store.configuration_digest().into(),
        context: context.name().into(),
        token: token.clone(),
        state: "running".into(),
        archive_sha256: probe.metadata.sha256.clone(),
        client_sha256: client.executable_sha256().into(),
        commands: Vec::new(),
        resources: json!({"rootfs_tag":format!("{token}:rootfs"), "build_tag":format!("{token}:built"),
            "engine_container":format!("{token}-engine"), "build_container":format!("{token}-build"),
            "compose_project":format!("{token}-compose"), "compose_container":format!("{token}-compose-service"),
            "directory":directory,"cleanup_scope":"disposable_probe_containers_compose_objects_and_images",
            "retained_buildkit_cache":true,"runtime_inventory":serde_json::to_value(inventory)?,
            "previous_failed_attempt":archived_failure}),
        failure: None,
    };
    let mut run = Run {
        client,
        context,
        store,
        journal,
        directory,
        deadline,
        _lock: lock,
    };
    run.persist()?;
    let result = run.operations(probe).await.and_then(|engine_id| {
        ensure!(
            Instant::now() < deadline,
            "startup probe deadline elapsed before receipt publication"
        );
        Ok(engine_id)
    });
    match result {
        Ok(engine_id) => {
            run.journal.state = "completed".into();
            run.persist()?;
            // Active journals can be superseded; successful receipts cannot.
            let path = run.directory.join("receipt.json");
            let bytes = serde_json::to_vec(&run.journal)?;
            write_new(&path, &bytes)?;
            File::open(&run.directory)?.sync_all()?;
            Ok(MachineDockerOperationalEvidence {
                schema_version: 1,
                owner: run.store.owner().clone(),
                incarnation: incarnation.clone(),
                configuration_digest: run.store.configuration_digest().into(),
                engine_id,
                inert_stock_runtime_metadata: serde_json::from_value(
                    run.journal.resources["inert_stock_runtime_metadata"].clone(),
                )?,
                capabilities: CapabilitySet::new([
                    MachineCapability::DockerEngine,
                    MachineCapability::Compose,
                    MachineCapability::Buildx,
                ]),
                archive_sha256: probe.metadata.sha256.clone(),
                client_sha256: client.executable_sha256().into(),
                runtime_inventory: serde_json::to_value(inventory)?,
                cleanup_confirmed: true,
                cleanup_scope: "disposable_probe_containers_compose_objects_and_images".into(),
                retained_buildkit_cache: true,
                receipt_path: path,
                receipt_sha256: hash(&bytes),
            })
        }
        Err(error) => {
            run.journal.state = "failed_recovery_required".into();
            run.journal.failure = Some(error.to_string().chars().take(2048).collect());
            run.persist().context("persist startup failure ownership")?;
            Err(error).context(
                "Docker startup probe failed; retained exact journal/resources require recovery",
            )
        }
    }
}

impl Run<'_> {
    fn persist(&self) -> Result<()> {
        self.store.validate_current()?;
        let mut temporary = tempfile::NamedTempFile::new_in(self.store.data_path())?;
        serde_json::to_writer(&mut temporary, &self.journal)?;
        temporary.as_file().sync_all()?;
        temporary
            .persist(self.store.data_path().join(JOURNAL))
            .map_err(|error| error.error)?;
        self.store.data_directory().sync_all()?;
        Ok(())
    }

    async fn command(
        &mut self,
        args: Vec<String>,
        input: Option<File>,
        mutation: bool,
    ) -> Result<HostDockerOutput> {
        ensure!(
            self.journal.commands.len() < 100,
            "startup command budget exhausted"
        );
        self.store.validate_current()?;
        self.context.verify(self.client).await?;
        let remaining = self.deadline.saturating_duration_since(Instant::now());
        ensure!(
            !remaining.is_zero(),
            "startup deadline exhausted before dispatch"
        );
        let index = self.journal.commands.len();
        self.journal
            .commands
            .push(json!({"args":args, "mutation":mutation,"state":"admitted"}));
        self.persist()?;
        let result = self
            .client
            .run(
                Some(self.context.name()),
                &args,
                input,
                remaining.min(Duration::from_secs(60)),
            )
            .await;
        let output = match result {
            Ok(output) => output,
            Err(error) => {
                self.journal.commands[index]["state"] = "transport_uncertain".into();
                self.persist()?;
                return Err(error);
            }
        };
        let stdout = format!("command-{index:03}.stdout");
        let stderr = format!("command-{index:03}.stderr");
        write_new(&self.directory.join(&stdout), &output.stdout)?;
        write_new(&self.directory.join(&stderr), &output.stderr)?;
        self.journal.commands[index] = json!({"args":args,"mutation":mutation,"state":"returned",
            "exit_code":output.status.code(),"stdout":stdout,"stderr":stderr,
            "stdout_sha256":hash(&output.stdout),"stderr_sha256":hash(&output.stderr)});
        self.persist()?;
        Ok(output)
    }

    async fn ok(&mut self, args: Vec<String>, mutation: bool) -> Result<Vec<u8>> {
        Ok(self.command(args, None, mutation).await?.success()?.stdout)
    }

    fn resource(&self, name: &str) -> Result<String> {
        Ok(self.journal.resources[name]
            .as_str()
            .context("probe resource name missing")?
            .into())
    }

    async fn operations(&mut self, probe: &VerifiedDeveloperProbe) -> Result<String> {
        // Actual version receipts are scoped observations, not certified ranges.
        self.ok(words(&["--version"]), false).await?;
        self.ok(words(&["compose", "version"]), false).await?;
        self.ok(words(&["buildx", "version"]), false).await?;
        let info = self
            .ok(words(&["info", "--format", "{{json .}}"]), false)
            .await?;
        let engine_id = engine_identity(&info)?;
        ensure!(
            self.context
                .descriptor(&self.journal.incarnation, engine_id.clone())?
                .owner
                == *self.store.owner(),
            "probe context belongs to another Environment or Machine"
        );
        self.journal.resources["engine_id"] = engine_id.clone().into();
        self.journal.resources["inert_stock_runtime_metadata"] =
            serde_json::to_value(inert_metadata(&info)?)?;
        self.persist()?;
        let inspect = self
            .ok(words(&["buildx", "inspect", self.context.name()]), false)
            .await?;
        verify_builder(&inspect, self.context.name())?;

        for key in ["rootfs_tag", "build_tag"] {
            let tag = self.resource(key)?;
            ensure!(
                self.ok(
                    words(&[
                        "image",
                        "ls",
                        "--quiet",
                        "--filter",
                        &format!("reference={tag}")
                    ]),
                    false
                )
                .await?
                .is_empty(),
                "probe image tag already exists; adoption forbidden"
            );
        }
        for key in ["engine_container", "build_container", "compose_container"] {
            let name = self.resource(key)?;
            ensure!(
                self.ok(
                    words(&[
                        "container",
                        "ls",
                        "--all",
                        "--quiet",
                        "--no-trunc",
                        "--filter",
                        &format!("name=^/{name}$")
                    ]),
                    false
                )
                .await?
                .is_empty(),
                "probe container name already exists"
            );
        }
        let compose_project = self.resource("compose_project")?;
        for kind in ["container", "network", "volume"] {
            let mut args = words(&[kind, "ls", "--quiet"]);
            if kind == "container" {
                args.push("--all".into());
            }
            args.extend(words(&[
                "--filter",
                &format!("label=com.docker.compose.project={compose_project}"),
            ]));
            ensure!(
                self.ok(args, false).await?.is_empty(),
                "Compose project already contains resources; adoption forbidden"
            );
        }
        // The build context contains only the three deliberate public inputs,
        // never command receipts, host config, or other Machine-store files.
        let build_directory = self.directory.join("build");
        std::fs::DirBuilder::new()
            .mode(0o700)
            .create(&build_directory)?;
        let archive_bytes = verified_archive(probe)?;
        let archive_copy = build_directory.join("rootfs.tar");
        write_new(&archive_copy, &archive_bytes)?;
        let archive = File::open(&archive_copy)?;
        let rootfs_tag = self.resource("rootfs_tag")?;
        let label = format!("{LABEL}={}", self.journal.token);
        let imported = self
            .command(
                words(&[
                    "image",
                    "import",
                    "--change",
                    &format!("LABEL {label}"),
                    "-",
                    &rootfs_tag,
                ]),
                Some(archive),
                true,
            )
            .await?
            .success()?;
        let rootfs_id = image_id(&imported.stdout)?;
        self.journal.resources["rootfs_id"] = rootfs_id.clone().into();
        self.persist()?;
        self.inspect_image(&rootfs_tag, &rootfs_id).await?;
        let name = self.resource("engine_container")?;
        let cid = self.create_container(&name, &rootfs_id).await?;
        self.ok(words(&["start", &cid]), true).await?;
        ensure!(
            self.ok(
                words(&["exec", &cid, "/bin/cat", "/etc/vz-developer-probe"]),
                true
            )
            .await?
                == MARKER,
            "Engine exec marker mismatch"
        );
        self.remove_container(&cid).await?;

        let compose_name = self.resource("compose_container")?;
        let compose_file = self.directory.join("compose.json");
        write_new(
            &compose_file,
            &serde_json::to_vec(&json!({"services":{"probe":{
            "image":rootfs_id,"pull_policy":"never","container_name":compose_name,"network_mode":"none",
            "command":["/bin/sleep","300"],"labels":{(LABEL):self.journal.token}}}}))?,
        )?;
        self.compose(
            &compose_file,
            &compose_project,
            &["up", "--detach", "--no-build", "--pull", "never"],
        )
        .await?;
        let inspected = self
            .ok(words(&["container", "inspect", &compose_name]), false)
            .await?;
        let compose_cid = owned_container(&inspected, &self.journal.token, Some(&rootfs_id))?;
        self.journal.resources["compose_id"] = compose_cid.clone().into();
        self.persist()?;
        ensure!(
            self.compose(
                &compose_file,
                &compose_project,
                &[
                    "exec",
                    "--no-TTY",
                    "probe",
                    "/bin/cat",
                    "/etc/vz-developer-probe"
                ]
            )
            .await?
                == MARKER,
            "Compose exec marker mismatch"
        );
        // Only this exact single-service project was created; reject injected
        // project siblings before Compose's project-wide teardown.
        let inventory = self
            .ok(
                words(&[
                    "container",
                    "ls",
                    "--all",
                    "--quiet",
                    "--no-trunc",
                    "--filter",
                    &format!("label=com.docker.compose.project={compose_project}"),
                ]),
                false,
            )
            .await?;
        ensure!(
            std::str::from_utf8(&inventory)?.trim() == compose_cid,
            "unexpected Compose project resources"
        );
        self.compose(&compose_file, &compose_project, &["down", "--timeout", "5"])
            .await?;
        self.absent("container", &compose_cid).await?;

        let build_tag = self.resource("build_tag")?;
        write_new(&build_directory.join("payload.txt"), PAYLOAD)?;
        write_new(&build_directory.join("Dockerfile"), format!("FROM scratch\nADD rootfs.tar /\nCOPY payload.txt /vz-probe-payload\nRUN [\"/bin/sh\",\"-c\",\"cat /vz-probe-payload > /vz-probe-executed\"]\nLABEL {label}\n").as_bytes())?;
        // No frontend image, registry base, docker-container builder, or bootstrap.
        let inspect = self
            .ok(words(&["buildx", "inspect", self.context.name()]), false)
            .await?;
        verify_builder(&inspect, self.context.name())?;
        let context_dir = build_directory
            .to_str()
            .context("non-UTF8 build context")?
            .to_string();
        self.ok(
            words(&[
                "buildx",
                "build",
                "--builder",
                self.context.name(),
                "--platform",
                "linux/arm64",
                "--network",
                "none",
                "--load",
                "--no-cache",
                "--tag",
                &build_tag,
                &context_dir,
            ]),
            true,
        )
        .await?;
        let built = self
            .ok(words(&["image", "inspect", &build_tag]), false)
            .await?;
        let built_id = owned_image(&built, &self.journal.token)?;
        self.journal.resources["built_id"] = built_id.clone().into();
        self.persist()?;
        let build_name = self.resource("build_container")?;
        let build_cid = self.create_container(&build_name, &built_id).await?;
        self.ok(words(&["start", &build_cid]), true).await?;
        ensure!(
            self.ok(
                words(&["exec", &build_cid, "/bin/cat", "/vz-probe-executed"]),
                true
            )
            .await?
                == PAYLOAD,
            "Buildx RUN output in loaded image mismatched"
        );
        self.remove_container(&build_cid).await?;
        for (tag, id) in [(&build_tag, &built_id), (&rootfs_tag, &rootfs_id)] {
            self.inspect_image(tag, id).await?;
            self.ok(words(&["image", "rm", tag]), true).await?;
            self.absent("image", id).await?;
        }
        let final_info = self
            .ok(words(&["info", "--format", "{{json .}}"]), false)
            .await?;
        ensure!(
            engine_identity(&final_info)? == engine_id,
            "Machine Engine changed during startup probes"
        );
        ensure!(
            serde_json::to_value(inert_metadata(&final_info)?)?
                == self.journal.resources["inert_stock_runtime_metadata"],
            "Engine runtime metadata changed during startup probes"
        );
        self.context.verify(self.client).await?;
        Ok(engine_id)
    }

    async fn inspect_image(&mut self, tag: &str, id: &str) -> Result<()> {
        let raw = self.ok(words(&["image", "inspect", tag]), false).await?;
        ensure!(
            owned_image(&raw, &self.journal.token)? == id,
            "probe image ID changed"
        );
        Ok(())
    }
    async fn create_container(&mut self, name: &str, image: &str) -> Result<String> {
        let label = format!("{LABEL}={}", self.journal.token);
        let output = self
            .ok(
                words(&[
                    "create",
                    "--name",
                    name,
                    "--label",
                    &label,
                    "--network",
                    "none",
                    image,
                    "/bin/sleep",
                    "300",
                ]),
                true,
            )
            .await?;
        let cid = container_id(&output)?;
        self.journal.resources[name] = cid.clone().into();
        self.persist()?;
        let raw = self
            .ok(words(&["container", "inspect", &cid]), false)
            .await?;
        ensure!(
            owned_container(&raw, &self.journal.token, Some(image))? == cid,
            "created container changed identity"
        );
        Ok(cid)
    }
    async fn remove_container(&mut self, cid: &str) -> Result<()> {
        let raw = self
            .ok(words(&["container", "inspect", cid]), false)
            .await?;
        ensure!(
            owned_container(&raw, &self.journal.token, None)? == cid,
            "cleanup container ownership mismatch"
        );
        self.ok(words(&["container", "rm", "--force", cid]), true)
            .await?;
        self.absent("container", cid).await
    }
    async fn absent(&mut self, kind: &str, id: &str) -> Result<()> {
        let output = self
            .command(words(&[kind, "inspect", id]), None, false)
            .await?;
        verify_absence(&output, kind, id)?;
        Ok(())
    }
    async fn compose(&mut self, file: &Path, project: &str, args: &[&str]) -> Result<Vec<u8>> {
        let mut command = words(&[
            "compose",
            "--file",
            file.to_str().context("Compose path encoding")?,
            "--project-name",
            project,
        ]);
        command.extend(words(args));
        self.ok(command, true).await
    }
}

fn words(values: &[&str]) -> Vec<String> {
    values.iter().map(|value| (*value).into()).collect()
}
fn hash(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}
fn create_new(path: &Path) -> Result<File> {
    use std::os::unix::fs::OpenOptionsExt;
    Ok(std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)?)
}
fn write_new(path: &Path, bytes: &[u8]) -> Result<()> {
    let mut file = create_new(path)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    Ok(())
}
fn open_owned(store: &MachineRuntimeStoreLease, name: &str, create: bool) -> Result<File> {
    use rustix::fs::{Mode, OFlags, openat};
    let flags = OFlags::RDWR
        | OFlags::NOFOLLOW
        | OFlags::NONBLOCK
        | OFlags::CLOEXEC
        | if create {
            OFlags::CREATE
        } else {
            OFlags::empty()
        };
    let file = File::from(openat(
        store.data_directory(),
        name,
        flags,
        Mode::RUSR | Mode::WUSR,
    )?);
    let meta = file.metadata()?;
    ensure!(
        meta.is_file()
            && meta.nlink() == 1
            && meta.mode() & 0o777 == 0o600
            && meta.uid() == rustix::process::geteuid().as_raw(),
        "untrusted probe journal/lock"
    );
    Ok(file)
}
fn read_journal(store: &MachineRuntimeStoreLease) -> Result<Option<Journal>> {
    let file = match open_owned(store, JOURNAL, false) {
        Ok(file) => file,
        Err(error)
            if error.downcast_ref::<rustix::io::Errno>() == Some(&rustix::io::Errno::NOENT) =>
        {
            return Ok(None);
        }
        Err(error) => return Err(error),
    };
    let mut bytes = Vec::new();
    file.take(1024 * 1024 + 1).read_to_end(&mut bytes)?;
    ensure!(bytes.len() <= 1024 * 1024, "probe journal exceeds bound");
    let journal: Journal = serde_json::from_slice(&bytes)?;
    ensure!(journal.schema_version == 1, "unknown probe journal schema");
    Ok(Some(journal))
}
fn validate_previous(journal: &Journal, owner: &ResourceOwner) -> Result<()> {
    ensure!(
        journal.schema_version == 1 && journal.owner == *owner && journal.state == "completed",
        "unfinished or foreign Docker startup probe requires exact owned recovery; not retrying Engine mutations"
    );
    Ok(())
}

/// This is positive non-dispatch evidence, not absence inferred from failure.
/// Client/archive hashes identify the old attempt but are not retry authority:
/// repairing client selection is safe when no Engine mutation was admitted.
fn validate_no_mutation(
    journal: &Journal,
    store: &MachineRuntimeStoreLease,
    incarnation: &MachineIncarnation,
    context: &str,
) -> Result<()> {
    ensure!(
        journal.schema_version == 1
            && journal.owner == *store.owner()
            && journal.configuration_digest == store.configuration_digest()
            && same_incarnation(&journal.incarnation, incarnation)
            && journal.context == context
            && matches!(
                journal.state.as_str(),
                "running" | "failed_recovery_required"
            )
            && valid_hex(&journal.archive_sha256)
            && valid_hex(&journal.client_sha256),
        "prior non-mutating probe has different or malformed ownership/configuration/incarnation"
    );
    let token = journal
        .token
        .strip_prefix("vzprobe-")
        .context("invalid prior probe token")?;
    ensure!(
        token.len() == 24
            && token
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)),
        "invalid prior probe token"
    );
    let directory = store.data_path().join(&journal.token);
    crate::machine_runtime_registry::open_trusted_registry_root(&directory)?;
    let expected = json!({"rootfs_tag":format!("{}:rootfs",journal.token), "build_tag":format!("{}:built",journal.token),
        "engine_container":format!("{}-engine",journal.token), "build_container":format!("{}-build",journal.token),
        "compose_project":format!("{}-compose",journal.token), "compose_container":format!("{}-compose-service",journal.token),
        "directory":directory,"cleanup_scope":"disposable_probe_containers_compose_objects_and_images","retained_buildkit_cache":true});
    let resources = journal
        .resources
        .as_object()
        .context("prior probe resource inventory missing")?;
    for (key, value) in expected.as_object().context("expected inventory")? {
        ensure!(
            resources.get(key) == Some(value),
            "prior probe resource intent changed"
        );
    }
    ensure!(
        resources.keys().all(|key| expected.get(key).is_some()
            || matches!(
                key.as_str(),
                "runtime_inventory"
                    | "engine_id"
                    | "inert_stock_runtime_metadata"
                    | "previous_failed_attempt"
            )),
        "prior probe contains acquired or unknown resource identities"
    );
    ensure!(
        journal.commands.len() <= 100,
        "prior probe command budget invalid"
    );
    for (index, command) in journal.commands.iter().enumerate() {
        let object = command.as_object().context("malformed prior command")?;
        ensure!(
            command["mutation"] == false,
            "prior Engine mutation intent requires recovery, even without a return"
        );
        let args: Vec<String> = serde_json::from_value(command["args"].clone())?;
        ensure!(
            read_only_args(&args, journal),
            "prior command is not a proven read-only operation"
        );
        let state = command["state"].as_str().context("command state missing")?;
        let keys: &[&str] = match state {
            "admitted" | "transport_uncertain" => &["args", "mutation", "state"],
            "returned" => &[
                "args",
                "mutation",
                "state",
                "exit_code",
                "stdout",
                "stderr",
                "stdout_sha256",
                "stderr_sha256",
            ],
            _ => anyhow::bail!("unknown prior command state"),
        };
        ensure!(
            object.len() == keys.len() && keys.iter().all(|key| object.contains_key(*key)),
            "incomplete or expanded prior command receipt"
        );
        if state != "returned" {
            ensure!(
                index + 1 == journal.commands.len(),
                "commands followed an unreturned command"
            );
        } else {
            ensure!(
                command["exit_code"].is_null()
                    || command["exit_code"]
                        .as_i64()
                        .is_some_and(|code| (0..=255).contains(&code)),
                "invalid prior command exit"
            );
            for stream in ["stdout", "stderr"] {
                let name = format!("command-{index:03}.{stream}");
                ensure!(
                    command[stream] == name,
                    "prior command evidence path changed"
                );
                let bytes = read_private(&directory.join(name), 4 * 1024 * 1024)?;
                ensure!(
                    command[format!("{stream}_sha256")] == hash(&bytes),
                    "prior command evidence digest mismatch"
                );
            }
        }
    }
    Ok(())
}

fn same_incarnation(left: &MachineIncarnation, right: &MachineIncarnation) -> bool {
    left.schema_version == right.schema_version
        && left.machine_id == right.machine_id
        && left.incarnation_id == right.incarnation_id
        && left.generation == right.generation
}

fn read_only_args(args: &[String], journal: &Journal) -> bool {
    let args: Vec<_> = args.iter().map(String::as_str).collect();
    match args.as_slice() {
        ["--version"]
        | ["compose", "version"]
        | ["buildx", "version"]
        | ["info", "--format", "{{json .}}"] => true,
        ["buildx", "inspect", context] => *context == journal.context,
        ["image", "ls", "--quiet", "--filter", filter] => {
            ["rootfs_tag", "build_tag"].iter().any(|key| {
                journal.resources[*key]
                    .as_str()
                    .is_some_and(|name| *filter == format!("reference={name}"))
            })
        }
        [
            "container",
            "ls",
            "--all",
            "--quiet",
            "--no-trunc",
            "--filter",
            filter,
        ] => ["engine_container", "build_container", "compose_container"]
            .iter()
            .any(|key| {
                journal.resources[*key]
                    .as_str()
                    .is_some_and(|name| *filter == format!("name=^/{name}$"))
            }),
        ["container", "ls", "--quiet", "--all", "--filter", filter]
        | ["network" | "volume", "ls", "--quiet", "--filter", filter] => {
            journal.resources["compose_project"]
                .as_str()
                .is_some_and(|project| {
                    *filter == format!("label=com.docker.compose.project={project}")
                })
        }
        _ => false,
    }
}

fn read_private(path: &Path, limit: u64) -> Result<Vec<u8>> {
    use rustix::fs::{Mode, OFlags, open};
    let file = File::from(open(
        path,
        OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::NONBLOCK | OFlags::CLOEXEC,
        Mode::empty(),
    )?);
    let metadata = file.metadata()?;
    ensure!(
        metadata.is_file()
            && metadata.nlink() == 1
            && metadata.mode() & 0o777 == 0o600
            && metadata.uid() == rustix::process::geteuid().as_raw()
            && metadata.len() <= limit,
        "untrusted prior probe evidence"
    );
    let mut bytes = Vec::new();
    file.take(limit + 1).read_to_end(&mut bytes)?;
    ensure!(
        bytes.len() as u64 <= limit,
        "prior probe evidence exceeds bound"
    );
    Ok(bytes)
}

fn archive_no_mutation(journal: &Journal, store: &MachineRuntimeStoreLease) -> Result<Value> {
    store.validate_current()?;
    let directory = store.data_path().join(&journal.token);
    crate::machine_runtime_registry::open_trusted_registry_root(&directory)?;
    let path = directory.join("no-mutation-failure.json");
    let bytes = serde_json::to_vec(
        &json!({"schema_version":1,"kind":"docker_startup_no_mutation_attempt",
        "proof_scope":"no_Engine_mutation_intent_admitted_readonly_host_queries_may_have_completed",
        "journal":journal}),
    )?;
    match write_new(&path, &bytes) {
        Ok(()) => (),
        Err(error)
            if error
                .downcast_ref::<std::io::Error>()
                .is_some_and(|error| error.kind() == std::io::ErrorKind::AlreadyExists) =>
        {
            ensure!(
                read_private(&path, 1024 * 1024)? == bytes,
                "previous failed-attempt archive differs; not replacing it"
            );
        }
        Err(error) => return Err(error),
    }
    File::open(&directory)?.sync_all()?;
    Ok(json!({"path":path,"sha256":hash(&bytes)}))
}
fn verified_archive(probe: &VerifiedDeveloperProbe) -> Result<Vec<u8>> {
    use rustix::fs::{Mode, OFlags, open};
    let mut file = File::from(open(
        &probe.archive,
        OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::NONBLOCK | OFlags::CLOEXEC,
        Mode::empty(),
    )?);
    let meta = file.metadata()?;
    ensure!(
        meta.is_file() && meta.nlink() == 1 && meta.len() <= 32 * 1024 * 1024,
        "invalid pinned probe archive"
    );
    let mut bytes = Vec::new();
    (&mut file)
        .take(32 * 1024 * 1024 + 1)
        .read_to_end(&mut bytes)?;
    ensure!(
        hash(&bytes) == probe.metadata.sha256,
        "pinned probe archive changed"
    );
    Ok(bytes)
}
fn verify_absence(output: &HostDockerOutput, kind: &str, id: &str) -> Result<()> {
    let stdout = std::str::from_utf8(&output.stdout)?.trim();
    let stderr = std::str::from_utf8(&output.stderr)?.trim();
    let expected = [
        format!("Error response from daemon: No such {kind}: {id}"),
        format!("Error: No such {kind}: {id}"),
        format!("Error: No such object: {id}"),
        format!("Error response from daemon: No such object: {id}"),
    ];
    ensure!(
        output.status.code() == Some(1)
            && matches!(stdout, "" | "[]")
            && expected.iter().any(|line| line == stderr),
        "exact owned resource absence not proven"
    );
    Ok(())
}

fn image_id(raw: &[u8]) -> Result<String> {
    let id = std::str::from_utf8(raw)?.trim();
    ensure!(
        id.starts_with("sha256:") && valid_hex(&id[7..]),
        "invalid immutable Docker image ID"
    );
    Ok(id.into())
}
fn container_id(raw: &[u8]) -> Result<String> {
    let id = std::str::from_utf8(raw)?.trim();
    ensure!(valid_hex(id), "invalid full Docker container ID");
    Ok(id.into())
}
fn valid_hex(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}
fn one(raw: &[u8]) -> Result<Value> {
    let mut rows: Vec<Value> = serde_json::from_slice(raw)?;
    ensure!(rows.len() == 1, "ambiguous Docker object inspection");
    Ok(rows.remove(0))
}
fn owned_image(raw: &[u8], token: &str) -> Result<String> {
    let row = one(raw)?;
    ensure!(
        row["Config"]["Labels"][LABEL] == token
            && row["Os"] == "linux"
            && row["Architecture"] == "arm64",
        "foreign probe image"
    );
    image_id(row["Id"].as_str().context("image ID missing")?.as_bytes())
}
fn owned_container(raw: &[u8], token: &str, image: Option<&str>) -> Result<String> {
    let row = one(raw)?;
    ensure!(
        row["Config"]["Labels"][LABEL] == token
            && row["HostConfig"]["Runtime"] == "youki"
            && row["HostConfig"]["Privileged"] == false
            && row["HostConfig"]["NetworkMode"] == "none",
        "foreign or policy-overridden probe container"
    );
    if let Some(image) = image {
        ensure!(row["Image"] == image, "wrong probe container image");
    }
    container_id(
        row["Id"]
            .as_str()
            .context("container ID missing")?
            .as_bytes(),
    )
}
fn engine_identity(raw: &[u8]) -> Result<String> {
    let row: Value = serde_json::from_slice(raw)?;
    ensure!(
        row["OSType"] == "linux"
            && matches!(row["Architecture"].as_str(), Some("arm64" | "aarch64"))
            && row["DefaultRuntime"] == "youki",
        "wrong Machine Engine target/runtime"
    );
    let runtimes = row["Runtimes"]
        .as_object()
        .context("Engine runtimes missing")?;
    ensure!(
        runtimes.contains_key("youki")
            && row["Runtimes"]["youki"]["path"] == "/mnt/linux-bin/youki"
            && runtimes.iter().all(|(name, entry)| match name.as_str() {
                "youki" | "io.containerd.youki.v2" => true,
                "runc" | "io.containerd.runc.v2" => *entry == json!({"path":"runc"}),
                _ => false,
            }),
        "non-youki Engine runtime"
    );
    let id = row["ID"].as_str().context("Engine ID missing")?;
    ensure!(
        !id.is_empty() && id.len() <= 256 && !id.chars().any(char::is_control),
        "invalid Engine ID"
    );
    Ok(id.into())
}
fn inert_metadata(raw: &[u8]) -> Result<Vec<String>> {
    let row: Value = serde_json::from_slice(raw)?;
    let runtimes = row["Runtimes"]
        .as_object()
        .context("Engine runtime metadata missing")?;
    Ok(runtimes
        .keys()
        .filter(|name| matches!(name.as_str(), "runc" | "io.containerd.runc.v2"))
        .cloned()
        .collect())
}
fn verify_builder(raw: &[u8], context: &str) -> Result<()> {
    let text = std::str::from_utf8(raw)?;
    let sections: Vec<_> = text.split("\nNodes:\n").collect();
    ensure!(sections.len() == 2, "builder node inventory unavailable");
    let values = |text: &str, key: &str| {
        text.lines()
            .filter_map(|line| line.strip_prefix(key).map(str::trim))
            .map(str::to_owned)
            .collect::<Vec<_>>()
    };
    ensure!(
        values(sections[0], "Name:") == [context]
            && values(sections[0], "Driver:") == ["docker"]
            && values(sections[1], "Endpoint:") == [context]
            && values(sections[1], "Status:") == ["running"]
            && values(sections[1], "Name:").len() == 1
            && !text
                .lines()
                .any(|line| line.trim_start().starts_with("Error:")),
        "Buildx is not the exact running context's embedded docker driver"
    );
    Ok(())
}

#[cfg(test)]
#[path = "machine_docker_operational_probe_tests.rs"]
mod tests;
