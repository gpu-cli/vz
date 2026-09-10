#![allow(clippy::unwrap_used, clippy::expect_used)]

use super::*;
use vz_runtime_contract::{
    Architecture, CapabilitySet, EnvironmentId, MachineForkOrigin, MachineId, MachineProfile,
    MachineResources, MachineState, TOPOLOGY_SCHEMA_VERSION, TargetSpec,
};

fn machine(name: &str, os: OperatingSystem, fork: Option<MachineForkOrigin>) -> MachineInstance {
    MachineInstance {
        fork,
        docker_context: None,
        schema_version: TOPOLOGY_SCHEMA_VERSION,
        machine_id: MachineId::generate(),
        environment_id: EnvironmentId::new("env_fork").unwrap(),
        name: name.to_string(),
        profile: MachineProfile::Developer,
        target: TargetSpec {
            os,
            arch: Architecture::Aarch64,
            image: "vz-linux-appliance".to_string(),
            version: None,
            channel: None,
            digest: None,
        },
        resources: MachineResources::default(),
        requested_capabilities: CapabilitySet::default(),
        negotiated_capabilities: CapabilitySet::default(),
        backend: None,
        incarnation: None,
        runtime_identity: None,
        state: MachineState::Creating,
        legacy_sandbox_id: None,
    }
}

fn lineage(parent: &MachineInstance, label: &str) -> MachineForkOrigin {
    MachineForkOrigin {
        schema_version: TOPOLOGY_SCHEMA_VERSION,
        parent_machine_id: parent.machine_id.clone(),
        parent_name: parent.name.clone(),
        label: label.to_string(),
    }
}

#[test]
fn a_native_macos_machine_is_never_forked() {
    // Not a gap: macOS guests are licence-capped at two per host, so the design
    // shares one rather than replicating it, and a fresh macOS Machine is
    // already a delete plus an up with no install work.
    let native = machine("ios", OperatingSystem::Macos, None);
    assert!(matches!(
        require_forkable(&native),
        Err(MachineForkError::UnsupportedTarget { .. })
    ));
    assert!(require_forkable(&machine("backend", OperatingSystem::Linux, None)).is_ok());
}

#[test]
fn a_forks_disk_path_is_a_function_of_its_own_identity() {
    let temp = tempfile::tempdir().unwrap();
    // The path is keyed by `sha256(stack_id)`, and `stack_id` is derived from
    // `(project, environment, machine_id)`. A fork's new `machine_id` therefore
    // moves its disk without a rename step, and two Machines can never share
    // one image by accident.
    let parent = docker_data_disk_path(temp.path(), "vzr1-runtime_vm-vm-parent");
    let forked = docker_data_disk_path(temp.path(), "vzr1-runtime_vm-vm-forked");
    assert_ne!(parent, forked);
    assert_eq!(parent.file_name().unwrap(), "data.img");
    assert_eq!(
        docker_data_disk_path(temp.path(), "vzr1-runtime_vm-vm-parent"),
        parent,
        "the path must be a pure function of the stack id"
    );
}

/// The cost claim, measured rather than assumed.
///
/// A fork that deep-copied the Docker data disk would cost as much as a cold
/// boot and the feature would be pointless. The assertion is on **volume free
/// space**, because APFS reports both inodes as fully allocated once they share
/// blocks — the second half of this test proves the naive check would be wrong.
#[test]
fn seeding_a_fork_clones_the_docker_disk_for_free_space_metadata_not_bytes() {
    use std::os::unix::fs::MetadataExt;

    const DISK_BYTES: u64 = 96 * 1024 * 1024;

    let temp = tempfile::tempdir().unwrap();
    let parent_store = temp.path().join("parent/data");
    let fork_store = temp.path().join("fork/data");
    std::fs::create_dir_all(&parent_store).unwrap();
    std::fs::create_dir_all(&fork_store).unwrap();

    let parent = machine("backend", OperatingSystem::Linux, None);
    let forked = machine(
        "backend@feat-x",
        OperatingSystem::Linux,
        Some(lineage(&parent, "feat-x")),
    );

    let source = docker_data_disk_path(&parent_store, "stack-parent");
    std::fs::create_dir_all(source.parent().unwrap()).unwrap();
    // Incompressible, so no filesystem-level compression can make a deep copy
    // look cheap, and actually allocated rather than sparse.
    let mut seed = 0x2545_f491_4f6c_dd1d_u64;
    let mut block = vec![0_u8; 1024 * 1024];
    let mut file = std::fs::File::create(&source).unwrap();
    for _ in 0..(DISK_BYTES / block.len() as u64) {
        for chunk in block.chunks_mut(8) {
            seed = seed
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            chunk.copy_from_slice(&seed.to_le_bytes()[..chunk.len()]);
        }
        std::io::Write::write_all(&mut file, &block).unwrap();
    }
    file.sync_all().unwrap();
    drop(file);
    let source_allocated = std::fs::metadata(&source).unwrap().blocks() * 512;
    assert!(source_allocated >= DISK_BYTES);

    let consumed = seed_forked_docker_disk(
        &forked,
        &parent,
        &parent_store,
        "stack-parent",
        &fork_store,
        "stack-forked",
    )
    .unwrap();

    assert!(
        consumed < DISK_BYTES / 8,
        "forking a {DISK_BYTES}-byte Docker disk consumed {consumed} bytes of volume free space; \
         that is a deep copy, not a fork"
    );

    let destination = docker_data_disk_path(&fork_store, "stack-forked");
    assert_eq!(std::fs::metadata(&destination).unwrap().len(), DISK_BYTES);
    // And why the obvious check is the wrong one: the clone reports the parent's
    // full allocation, because both inodes reference the same blocks.
    assert_eq!(
        std::fs::metadata(&destination).unwrap().blocks() * 512,
        source_allocated
    );

    // Separate inodes. A fork writing into its Docker store must never reach its
    // parent's — that isolation is the correctness argument for forking at all.
    std::fs::write(&destination, b"diverged").unwrap();
    assert_eq!(std::fs::metadata(&source).unwrap().len(), DISK_BYTES);
}

#[test]
fn seeding_refuses_a_missing_parent_disk_an_occupied_destination_and_a_non_fork() {
    let temp = tempfile::tempdir().unwrap();
    let parent_store = temp.path().join("parent/data");
    let fork_store = temp.path().join("fork/data");
    std::fs::create_dir_all(&parent_store).unwrap();
    std::fs::create_dir_all(&fork_store).unwrap();
    let parent = machine("backend", OperatingSystem::Linux, None);
    let forked = machine(
        "backend@feat-x",
        OperatingSystem::Linux,
        Some(lineage(&parent, "feat-x")),
    );

    // A parent that has never booted has no Docker disk, and seeding from
    // nothing would produce a fork that is not warm — the one thing a fork
    // promises. Refused rather than silently creating an empty disk.
    assert!(matches!(
        seed_forked_docker_disk(
            &forked,
            &parent,
            &parent_store,
            "stack-parent",
            &fork_store,
            "stack-forked"
        ),
        Err(MachineForkError::ParentDiskAbsent { .. })
    ));

    let source = docker_data_disk_path(&parent_store, "stack-parent");
    std::fs::create_dir_all(source.parent().unwrap()).unwrap();
    std::fs::write(&source, b"parent docker state").unwrap();

    // A Machine with no lineage is a declared Machine; seeding one would give it
    // a sibling's state under its own declared name.
    let declared = machine("other", OperatingSystem::Linux, None);
    assert!(matches!(
        seed_forked_docker_disk(
            &declared,
            &parent,
            &parent_store,
            "stack-parent",
            &fork_store,
            "stack-forked"
        ),
        Err(MachineForkError::MissingLineage { .. })
    ));

    seed_forked_docker_disk(
        &forked,
        &parent,
        &parent_store,
        "stack-parent",
        &fork_store,
        "stack-forked",
    )
    .unwrap();
    // Re-seeding would discard whatever the fork has done since. The clone would
    // have failed with EEXIST anyway; refusing first makes the reason legible.
    assert!(matches!(
        seed_forked_docker_disk(
            &forked,
            &parent,
            &parent_store,
            "stack-parent",
            &fork_store,
            "stack-forked"
        ),
        Err(MachineForkError::DestinationExists { .. })
    ));
}
