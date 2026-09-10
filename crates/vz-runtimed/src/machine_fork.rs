//! Seeding a forked Machine's disk from its parent's, and quiescing first.
//!
//! Identity minting lives in the state store; this module is the physical half.
//! It answers two questions the design leaves open until something measures
//! them, and both answers are load-bearing.
//!
//! ## What actually has to be copied
//!
//! A Developer Linux Machine has no root disk image: it boots kernel plus
//! initramfs, and its root filesystem is the initramfs. Its one persistent block
//! device is the Docker data disk — a 64 GiB sparse ext4 image under the
//! Machine's runtime store — and that disk *is* the warm state: `/var/lib/docker`
//! with its image store, containerd content, BuildKit cache, volumes and
//! containers. Everything else in the store is either content-addressed and
//! identical across Machines with the same target (the pinned kernel bundle) or
//! is re-minted from the fork's own identity (the Docker client config, the
//! endpoint socket, the context name). So the fork copies exactly one file, and
//! copies it copy-on-write.
//!
//! The disk's path is keyed by `sha256(stack_id)`, and `stack_id` is derived
//! from `(project_id, environment_id, machine_id)`, so the fork's new
//! `machine_id` puts its disk at a different path automatically. There is no
//! rename step and no chance of two Machines sharing one image.
//!
//! ## Why quiesce, and why not stop
//!
//! Measured on 2026-09-09: an 80 GiB template disk with 32.9 GiB allocated
//! cloned in 0.029 s for a 28 KB free-space delta, and the *same file held open
//! by a process writing and `fsync`ing continuously* — 10,288 writes completed —
//! cloned in 0.077 s, exit 0. `clonefile` operates on the path and does not
//! contend with an open writer, so a fork does not require the parent stopped.
//!
//! But what a clone of a running Machine captures is *crash-consistent*: the
//! state a power cut would have left. A guest that had written into page cache
//! and not yet flushed loses those writes, so a forked Docker image store could
//! come up with a half-written layer. Asking the guest to `sync` immediately
//! before the clone turns crash-consistent into application-consistent for
//! everything already committed, at the cost of one round trip and no downtime.
//! That is the whole reason this is a quiesce rather than a stop: stopping the
//! parent to fork it would make the parent pay for the fork, which is exactly
//! the cost the feature exists to avoid.
//!
//! The quiesce is best-effort by construction: a parent that is not running has
//! nothing in flight to flush, and its disk is already at rest.

use std::path::{Path, PathBuf};
use std::time::Duration;

use sha2::{Digest, Sha256};
use tracing::{info, warn};
use vz_runtime_contract::{MachineInstance, OperatingSystem, ResourceOwner};

/// How long the guest gets to flush before the clone proceeds without it.
///
/// Bounded rather than unbounded because a fork that hangs on an unresponsive
/// guest is worse than a fork that is crash-consistent: the caller asked for a
/// warm Machine, and a crash-consistent Docker store still boots.
const QUIESCE_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Debug, thiserror::Error)]
pub enum MachineForkError {
    #[error("Machine `{machine}` runs {os:?}; only Linux Machines are forked")]
    UnsupportedTarget {
        machine: String,
        os: OperatingSystem,
    },
    #[error("forked Machine `{machine}` has no parent lineage recorded")]
    MissingLineage { machine: String },
    #[error(
        "parent Machine `{parent}` has no seeded disk at `{path}`; start it once before forking"
    )]
    ParentDiskAbsent { parent: String, path: String },
    #[error("fork `{machine}` already has a disk at `{path}`; refusing to seed over it")]
    DestinationExists { machine: String, path: String },
    #[error("could not seed fork `{machine}` from `{parent}`: {reason}")]
    SeedFailed {
        machine: String,
        parent: String,
        reason: String,
    },
}

/// Only Linux Machines are forked.
///
/// Native macOS guests are licence-capped at two per host, so the design shares
/// one rather than replicating it, and its freshness need is already met without
/// forking: `vz up` creates a macOS Machine as a private clone of a registered
/// template, so a fresh one is a delete followed by an up with no install work.
/// Refusing here is therefore a statement of the design, not a gap in it.
pub fn require_forkable(machine: &MachineInstance) -> Result<(), MachineForkError> {
    if machine.target.os == OperatingSystem::Linux {
        return Ok(());
    }
    Err(MachineForkError::UnsupportedTarget {
        machine: machine.name.clone(),
        os: machine.target.os,
    })
}

/// The Docker data disk inside one Machine's runtime-store data directory.
///
/// Mirrors `vz_oci_macos::Runtime::docker_data_disk_path`, which is private to
/// that crate. The duplication is deliberate and pinned by a test rather than
/// papered over with a new public API on the runtime: this is the one path the
/// fork has to know, and knowing it here keeps the seeding a plain filesystem
/// operation that needs no live VM.
pub fn docker_data_disk_path(store_data_dir: &Path, stack_id: &str) -> PathBuf {
    let digest = Sha256::digest(stack_id.as_bytes());
    store_data_dir
        .join("docker-machines")
        .join(format!("{digest:x}"))
        .join("data.img")
}

/// Copy-on-write seed of one fork's Docker data disk from its parent's.
///
/// Returns the number of bytes of volume free space the clone consumed, which
/// is the only honest measure of what a fork cost: APFS reports both inodes as
/// fully allocated because they share blocks, so per-file size would read a
/// perfect clone as a deep copy.
pub fn seed_forked_docker_disk(
    machine: &MachineInstance,
    parent: &MachineInstance,
    parent_store_data: &Path,
    parent_stack_id: &str,
    fork_store_data: &Path,
    fork_stack_id: &str,
) -> Result<u64, MachineForkError> {
    require_forkable(machine)?;
    if machine.fork.is_none() {
        return Err(MachineForkError::MissingLineage {
            machine: machine.name.clone(),
        });
    }
    let source = docker_data_disk_path(parent_store_data, parent_stack_id);
    let destination = docker_data_disk_path(fork_store_data, fork_stack_id);
    if !source.is_file() {
        return Err(MachineForkError::ParentDiskAbsent {
            parent: parent.name.clone(),
            path: source.display().to_string(),
        });
    }
    if destination.exists() {
        return Err(MachineForkError::DestinationExists {
            machine: machine.name.clone(),
            path: destination.display().to_string(),
        });
    }
    let failed = |reason: String| MachineForkError::SeedFailed {
        machine: machine.name.clone(),
        parent: parent.name.clone(),
        reason,
    };
    if let Some(parent_dir) = destination.parent() {
        std::fs::create_dir_all(parent_dir).map_err(|error| failed(error.to_string()))?;
    }
    let free_before = vz_macos_provision::clone::volume_free_bytes(fork_store_data)
        .map_err(|error| failed(error.to_string()))?;
    let started = std::time::Instant::now();
    vz_macos_provision::clone::clone_path(&source, &destination)
        .map_err(|error| failed(error.to_string()))?;
    let elapsed = started.elapsed();
    let free_after = vz_macos_provision::clone::volume_free_bytes(fork_store_data)
        .map_err(|error| failed(error.to_string()))?;
    let consumed = free_before.saturating_sub(free_after);
    info!(
        machine = %machine.name,
        parent = %parent.name,
        elapsed_ms = elapsed.as_millis() as u64,
        free_space_consumed_bytes = consumed,
        "seeded forked Machine disk copy-on-write"
    );
    Ok(consumed)
}

/// Ask a running Linux guest to flush its filesystems, without stopping it.
///
/// `sync(1)` in the guest, bounded. A parent that is not running returns
/// `Ok(false)`: there is nothing in flight, and its disk is already at rest.
/// A guest that fails to flush is logged and the fork proceeds — see the module
/// docs on why a crash-consistent fork is a worse outcome than no fork only if
/// the caller was promised otherwise, which it is not.
pub async fn quiesce_parent_filesystems(
    runtime: &vz_oci_macos::Runtime,
    parent: &MachineInstance,
    parent_stack_id: &str,
) -> bool {
    if !runtime.has_shared_vm(parent_stack_id).await {
        return false;
    }
    match runtime
        .exec_in_shared_vm(
            parent_stack_id,
            "/bin/sync".to_string(),
            Vec::new(),
            QUIESCE_TIMEOUT,
        )
        .await
    {
        Ok(output) if output.exit_code == 0 => {
            info!(parent = %parent.name, "quiesced parent filesystems before fork");
            true
        }
        Ok(output) => {
            warn!(
                parent = %parent.name,
                exit_code = output.exit_code,
                "parent guest sync failed; fork will be crash-consistent rather than application-consistent"
            );
            false
        }
        Err(error) => {
            warn!(
                parent = %parent.name,
                %error,
                "could not reach parent guest to quiesce; fork will be crash-consistent"
            );
            false
        }
    }
}

impl crate::RuntimeDaemon {
    /// Seed every fork of this Environment whose disk has not been seeded yet.
    ///
    /// Runs after the sibling stores are pinned — so a fork's own store exists
    /// and is fenced — and before the first boot, because a fork must not start
    /// against an empty Docker disk and then be handed its parent's underneath
    /// itself. Already-seeded forks are skipped, which is what makes a re-`up`
    /// of an Environment containing forks an ordinary reconcile.
    pub(crate) async fn seed_environment_forks(
        &self,
        prepared: &crate::environment_runtime_controller::PreparedEnvironmentMachines,
        environment: &vz_runtime_contract::EnvironmentInstance,
        live: &std::collections::HashMap<
            vz_runtime_contract::MachineId,
            std::sync::Arc<crate::machine_runtime_activation::MachineRuntimeActivation>,
        >,
    ) -> Result<(), MachineForkError> {
        let store_data = |machine: &MachineInstance| -> Option<PathBuf> {
            prepared
                .pins()
                .iter()
                .find(|pin| pin.store().owner().machine_id.as_ref() == Some(&machine.machine_id))
                .map(|pin| pin.store().data_path().to_path_buf())
        };
        let stack_id = |machine: &MachineInstance| -> Result<String, MachineForkError> {
            let owner = ResourceOwner {
                project_id: environment.project_id.clone(),
                environment_id: environment.environment_id.clone(),
                machine_id: Some(machine.machine_id.clone()),
            };
            crate::machine_runtime_registry::MachineRuntimeEntry::<
                crate::machine_backend::MachineBackendRuntime,
            >::vm_reservation(&owner)
            .map(|record| record.resource_id)
            .map_err(|error| MachineForkError::SeedFailed {
                machine: machine.name.clone(),
                parent: String::new(),
                reason: error.to_string(),
            })
        };

        for machine in &environment.machines {
            let Some(origin) = &machine.fork else {
                continue;
            };
            require_forkable(machine)?;
            let parent = environment
                .machines
                .iter()
                .find(|candidate| candidate.machine_id == origin.parent_machine_id)
                .ok_or_else(|| MachineForkError::MissingLineage {
                    machine: machine.name.clone(),
                })?;
            let (Some(fork_data), Some(parent_data)) = (store_data(machine), store_data(parent))
            else {
                // A Machine whose store was not pinned in this Up is not ours to
                // seed; the boot loop will fail on it for its own reasons rather
                // than this one silently inventing a disk.
                continue;
            };
            let fork_stack = stack_id(machine)?;
            let parent_stack = stack_id(parent)?;
            if docker_data_disk_path(&fork_data, &fork_stack).exists() {
                continue;
            }

            // Immediately before the clone, and only then: anything the parent
            // flushes after this point is not in the fork, and anything it
            // flushed before is.
            if let Some(activation) = live.get(&parent.machine_id)
                && let Ok(runtime) = activation.entry().runtime().linux()
            {
                let synced = quiesce_parent_filesystems(runtime, parent, &parent_stack).await;
                if !synced {
                    warn!(
                        machine = %machine.name,
                        parent = %parent.name,
                        "seeding a fork from an unquiesced parent; the copy is crash-consistent"
                    );
                }
            }
            seed_forked_docker_disk(
                machine,
                parent,
                &parent_data,
                &parent_stack,
                &fork_data,
                &fork_stack,
            )?;
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "machine_fork_tests.rs"]
mod tests;
