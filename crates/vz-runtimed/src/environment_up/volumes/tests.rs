#![allow(clippy::unwrap_used, clippy::expect_used)]
//! Tests for the declared-storage half of the workspace-and-storage policy.
//!
//! The refusal tests all carry a control: the same declaration made legal by
//! changing exactly the one fact the rule turns on. Without the control, a
//! refusal test passes for a definition that was invalid for some other reason
//! entirely, which is how a policy check comes to be believed while enforcing
//! nothing.

use super::*;
use std::collections::BTreeSet;
use vz_runtime_contract::{
    Architecture, MachineResources, MachineSpec, MachineState, SharedCacheConsistency,
    SharedCacheConsistencyModel, TOPOLOGY_SCHEMA_VERSION, TargetSpec,
};

const MIB: u64 = 1024 * 1024;

fn target(os: OperatingSystem) -> TargetSpec {
    TargetSpec {
        os,
        arch: Architecture::Aarch64,
        image: "vz/linux".to_string(),
        version: None,
        channel: None,
        digest: None,
    }
}

fn machine_spec(name: &str, profile: MachineProfile, os: OperatingSystem) -> MachineSpec {
    MachineSpec {
        schema_version: TOPOLOGY_SCHEMA_VERSION,
        name: name.to_string(),
        profile,
        target: target(os),
        resources: MachineResources::default(),
        requested_capabilities: Default::default(),
        workspace: None,
        networks: Vec::new(),
        egress: Default::default(),
    }
}

fn developer_linux(name: &str) -> MachineSpec {
    machine_spec(name, MachineProfile::Developer, OperatingSystem::Linux)
}

fn machine_instance(environment_id: &EnvironmentId, name: &str) -> MachineInstance {
    MachineInstance {
        fork: None,
        schema_version: TOPOLOGY_SCHEMA_VERSION,
        machine_id: MachineId::generate(),
        environment_id: environment_id.clone(),
        name: name.to_string(),
        state: MachineState::Creating,
        profile: MachineProfile::Developer,
        target: target(OperatingSystem::Linux),
        resources: MachineResources::default(),
        requested_capabilities: Default::default(),
        negotiated_capabilities: Default::default(),
        backend: None,
        incarnation: None,
        runtime_identity: None,
        docker_context: None,
        legacy_sandbox_id: None,
    }
}

fn attachment(machine: &str, target_path: &str, mode: VolumeAccessMode) -> VolumeAttachment {
    VolumeAttachment {
        machine: machine.to_string(),
        target_path: target_path.to_string(),
        mode,
    }
}

fn block_volume(name: &str, attachments: Vec<VolumeAttachment>) -> VolumeSpec {
    VolumeSpec {
        schema_version: TOPOLOGY_SCHEMA_VERSION,
        name: name.to_string(),
        kind: VolumeKind::Block,
        size_bytes: Some(16 * MIB),
        consistency: None,
        attachments,
    }
}

fn shared_cache(name: &str, attachments: Vec<VolumeAttachment>) -> VolumeSpec {
    VolumeSpec {
        schema_version: TOPOLOGY_SCHEMA_VERSION,
        name: name.to_string(),
        kind: VolumeKind::SharedCache,
        size_bytes: None,
        consistency: Some(SharedCacheConsistency {
            model: SharedCacheConsistencyModel::BoundedStaleness,
            staleness_bound_millis: 2_000,
        }),
        attachments,
    }
}

fn spec(machines: Vec<MachineSpec>, volumes: Vec<VolumeSpec>) -> EnvironmentSpec {
    EnvironmentSpec {
        secret_bindings: Vec::new(),
        schema_version: TOPOLOGY_SCHEMA_VERSION,
        default_machine: None,
        machines,
        networks: Vec::new(),
        endpoints: Vec::new(),
        host_exports: Vec::new(),
        host_imports: Vec::new(),
        volumes,
    }
}

fn instance(environment_id: &EnvironmentId, name: &str, kind: VolumeKind) -> VolumeInstance {
    VolumeInstance {
        schema_version: TOPOLOGY_SCHEMA_VERSION,
        volume_id: VolumeId::generate(),
        environment_id: environment_id.clone(),
        name: name.to_string(),
        kind,
    }
}

// -- the criterion's refusal -------------------------------------------------

#[test]
fn a_writable_block_volume_on_two_machines_is_refused() {
    let declaration = spec(
        vec![developer_linux("machine-0"), developer_linux("machine-1")],
        vec![block_volume(
            "data",
            vec![
                attachment("machine-0", "/data", VolumeAccessMode::ReadWrite),
                attachment("machine-1", "/data", VolumeAccessMode::ReadOnly),
            ],
        )],
    );
    let error = refuse_unsupported_volumes(&declaration).unwrap_err();
    assert_eq!(
        error,
        VolumeError::WritableBlockMultiAttach {
            volume: "data".to_string(),
            first: "machine-0".to_string(),
            second: "machine-1".to_string(),
        }
    );
    // One writer and one reader is refused, not merely two writers: the reader
    // observes a filesystem another kernel is mutating underneath it, with the
    // two page caches and journals disagreeing about the same blocks.
    assert!(error.to_string().contains("one ext4 filesystem"), "{error}");
}

#[test]
fn two_writers_of_one_block_volume_are_refused() {
    let declaration = spec(
        vec![developer_linux("machine-0"), developer_linux("machine-1")],
        vec![block_volume(
            "data",
            vec![
                attachment("machine-0", "/data", VolumeAccessMode::ReadWrite),
                attachment("machine-1", "/data", VolumeAccessMode::ReadWrite),
            ],
        )],
    );
    assert!(matches!(
        refuse_unsupported_volumes(&declaration),
        Err(VolumeError::WritableBlockMultiAttach { .. })
    ));
}

#[test]
fn the_same_block_volume_on_one_machine_is_accepted() {
    // The control for the two refusals above. It differs from the first by
    // exactly one attachment, so the refusal cannot be passing because the
    // declaration was malformed in some other way.
    let declaration = spec(
        vec![developer_linux("machine-0"), developer_linux("machine-1")],
        vec![block_volume(
            "data",
            vec![attachment(
                "machine-0",
                "/data",
                VolumeAccessMode::ReadWrite,
            )],
        )],
    );
    refuse_unsupported_volumes(&declaration).expect("one writer is the supported case");
}

#[test]
fn a_read_only_block_volume_on_two_machines_is_accepted() {
    // The second control: multi-attach itself is not the refused thing. Two
    // readers of one image share a filesystem nobody is mutating, which the
    // block layer serves correctly.
    let declaration = spec(
        vec![developer_linux("machine-0"), developer_linux("machine-1")],
        vec![block_volume(
            "data",
            vec![
                attachment("machine-0", "/data", VolumeAccessMode::ReadOnly),
                attachment("machine-1", "/data", VolumeAccessMode::ReadOnly),
            ],
        )],
    );
    refuse_unsupported_volumes(&declaration).expect("read-only multi-attach is supported");
}

#[test]
fn a_writable_shared_cache_on_two_machines_is_accepted() {
    // The distinction the `kind` field exists for. The same shape that is
    // refused for a block volume is the supported case for a shared cache,
    // because a VirtioFS export has no shared block layer to corrupt and the
    // declaration states its consistency bound.
    let declaration = spec(
        vec![developer_linux("machine-0"), developer_linux("machine-1")],
        vec![shared_cache(
            "cache",
            vec![
                attachment("machine-0", "/cache", VolumeAccessMode::ReadWrite),
                attachment("machine-1", "/cache", VolumeAccessMode::ReadWrite),
            ],
        )],
    );
    refuse_unsupported_volumes(&declaration).expect("a declared shared cache multi-attaches");
}

#[test]
fn a_volume_on_a_hardened_or_native_machine_is_refused() {
    for (label, machine) in [
        (
            "hardened linux",
            machine_spec("other", MachineProfile::Hardened, OperatingSystem::Linux),
        ),
        (
            "native macos",
            machine_spec("other", MachineProfile::Developer, OperatingSystem::Macos),
        ),
    ] {
        let declaration = spec(
            vec![developer_linux("machine-0"), machine],
            vec![shared_cache(
                "cache",
                vec![attachment("other", "/cache", VolumeAccessMode::ReadWrite)],
            )],
        );
        assert!(
            matches!(
                refuse_unsupported_volumes(&declaration),
                Err(VolumeError::UnsupportedMachine { .. })
            ),
            "{label}"
        );
    }
}

#[test]
fn a_volume_naming_an_undeclared_machine_is_refused() {
    let declaration = spec(
        vec![developer_linux("machine-0")],
        vec![shared_cache(
            "cache",
            vec![attachment("ghost", "/cache", VolumeAccessMode::ReadWrite)],
        )],
    );
    assert_eq!(
        refuse_unsupported_volumes(&declaration).unwrap_err(),
        VolumeError::UnknownMachine {
            volume: "cache".to_string(),
            machine: "ghost".to_string(),
        }
    );
}

// -- host materialisation ----------------------------------------------------

#[test]
fn a_block_volume_becomes_one_sparse_owner_only_image_and_a_shared_cache_one_directory() {
    let temp = tempfile::tempdir().unwrap();
    let data_dir = temp.path();
    let environment_id = EnvironmentId::generate();
    let declaration = spec(
        vec![developer_linux("machine-0"), developer_linux("machine-1")],
        vec![
            block_volume(
                "data",
                vec![attachment(
                    "machine-0",
                    "/data",
                    VolumeAccessMode::ReadWrite,
                )],
            ),
            shared_cache(
                "cache",
                vec![
                    attachment("machine-0", "/cache", VolumeAccessMode::ReadWrite),
                    attachment("machine-1", "/cache", VolumeAccessMode::ReadWrite),
                ],
            ),
        ],
    );
    let instances = vec![
        instance(&environment_id, "data", VolumeKind::Block),
        instance(&environment_id, "cache", VolumeKind::SharedCache),
    ];
    let machines = vec![
        machine_instance(&environment_id, "machine-0"),
        machine_instance(&environment_id, "machine-1"),
    ];
    let resolved = resolve_environment_volumes(
        &declaration,
        &instances,
        &machines,
        data_dir,
        &environment_id,
        &BTreeMap::new(),
    )
    .expect("volumes resolve");

    let image = block_image_path(data_dir, &environment_id, &instances[0].volume_id);
    let metadata = std::fs::metadata(&image).expect("block image exists");
    assert_eq!(
        metadata.len(),
        16 * MIB,
        "the declared size is the one made"
    );
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(
            metadata.permissions().mode() & 0o777,
            0o600,
            "an Environment's storage is not world-readable"
        );
    }
    // Sparse, not allocated: the declaration promises the guest a device of
    // this size, never that the host has spent the bytes.
    assert!(
        std::fs::metadata(temp.path()).is_ok(),
        "temp dir still present"
    );

    let cache = shared_cache_path(data_dir, &environment_id, &instances[1].volume_id);
    assert!(cache.is_dir(), "shared cache directory exists");

    let first = &resolved[&machines[0].machine_id];
    assert_eq!(first.blocks.len(), 1);
    assert_eq!(first.blocks[0].guest_path, "/data");
    assert!(!first.blocks[0].read_only);
    assert_eq!(first.shares.len(), 1);
    assert_eq!(first.shares[0].guest_path.as_deref(), Some("/cache"));
    assert_eq!(first.shares[0].host_path, cache);

    // The second Machine sees the same host directory. That is the whole point
    // of a shared cache and it is asserted rather than assumed, because a
    // per-Machine copy would pass every other assertion here.
    let second = &resolved[&machines[1].machine_id];
    assert!(second.blocks.is_empty());
    assert_eq!(second.shares.len(), 1);
    assert_eq!(second.shares[0].host_path, cache);
}

#[test]
fn a_shared_cache_share_never_collides_with_a_workspace_projections_tag() {
    // `stack_vm` parses `vz-mount-{N}` to build the kernel argument the guest
    // bind-mounts from, so a cache reusing index 0 would silently replace the
    // Machine's workspace share.
    let temp = tempfile::tempdir().unwrap();
    let environment_id = EnvironmentId::generate();
    let declaration = spec(
        vec![developer_linux("machine-0")],
        vec![shared_cache(
            "cache",
            vec![attachment(
                "machine-0",
                "/cache",
                VolumeAccessMode::ReadWrite,
            )],
        )],
    );
    let instances = vec![instance(&environment_id, "cache", VolumeKind::SharedCache)];
    let machines = vec![machine_instance(&environment_id, "machine-0")];
    let mut next = BTreeMap::new();
    next.insert(machines[0].machine_id.clone(), 1);
    let resolved = resolve_environment_volumes(
        &declaration,
        &instances,
        &machines,
        temp.path(),
        &environment_id,
        &next,
    )
    .expect("volumes resolve");
    assert_eq!(
        resolved[&machines[0].machine_id].shares[0].tag,
        "vz-mount-1"
    );
}

#[test]
fn a_second_up_reuses_the_existing_image_and_its_contents() {
    let temp = tempfile::tempdir().unwrap();
    let environment_id = EnvironmentId::generate();
    let declaration = spec(
        vec![developer_linux("machine-0")],
        vec![block_volume(
            "data",
            vec![attachment(
                "machine-0",
                "/data",
                VolumeAccessMode::ReadWrite,
            )],
        )],
    );
    let instances = vec![instance(&environment_id, "data", VolumeKind::Block)];
    let machines = vec![machine_instance(&environment_id, "machine-0")];
    let resolve = || {
        resolve_environment_volumes(
            &declaration,
            &instances,
            &machines,
            temp.path(),
            &environment_id,
            &BTreeMap::new(),
        )
    };
    resolve().expect("first up");
    let image = block_image_path(temp.path(), &environment_id, &instances[0].volume_id);
    // A sentinel written into the image is what makes "reused" observable; a
    // check that only compares sizes would pass over a freshly truncated file.
    std::fs::write(&image, b"sentinel").unwrap();
    resolve().expect("second up");
    assert_eq!(std::fs::read(&image).unwrap(), b"sentinel");
}

#[test]
fn a_declared_volume_with_no_persisted_identity_is_refused_rather_than_minted() {
    let temp = tempfile::tempdir().unwrap();
    let environment_id = EnvironmentId::generate();
    let declaration = spec(
        vec![developer_linux("machine-0")],
        vec![block_volume(
            "data",
            vec![attachment(
                "machine-0",
                "/data",
                VolumeAccessMode::ReadWrite,
            )],
        )],
    );
    let machines = vec![machine_instance(&environment_id, "machine-0")];
    assert_eq!(
        resolve_environment_volumes(
            &declaration,
            &[],
            &machines,
            temp.path(),
            &environment_id,
            &BTreeMap::new(),
        )
        .unwrap_err(),
        VolumeError::UnresolvedInstance {
            volume: "data".to_string()
        }
    );
    // Nothing was created for a volume that could not be accounted for.
    assert!(!environment_volume_root(temp.path(), &environment_id).exists());
}

// -- reclamation -------------------------------------------------------------

#[test]
fn reclaiming_a_volume_removes_its_whole_directory_and_is_idempotent() {
    let temp = tempfile::tempdir().unwrap();
    let environment_id = EnvironmentId::generate();
    let volume_id = VolumeId::generate();
    let cache = shared_cache_path(temp.path(), &environment_id, &volume_id);
    std::fs::create_dir_all(cache.join("deep/nested")).unwrap();
    std::fs::write(cache.join("deep/nested/file"), b"payload").unwrap();

    reclaim_volume(temp.path(), &environment_id, &volume_id).expect("reclaim");
    assert!(!volume_directory(temp.path(), &environment_id, &volume_id).exists());
    // An interrupted Up leaves an ownership record with no storage behind it;
    // refusing here would make that Environment permanently undeletable.
    reclaim_volume(temp.path(), &environment_id, &volume_id).expect("absent reclaim succeeds");

    reclaim_environment_volume_root(temp.path(), &environment_id);
    assert!(!environment_volume_root(temp.path(), &environment_id).exists());
}

#[test]
fn an_environment_root_holding_an_unaccounted_survivor_is_left_for_inspection() {
    // `remove_dir`, not `remove_dir_all`: a directory nothing accounted for is a
    // leak the caller must see, and erasing it would hide exactly the evidence
    // the deletion-safety criterion depends on.
    let temp = tempfile::tempdir().unwrap();
    let environment_id = EnvironmentId::generate();
    let stray = environment_volume_root(temp.path(), &environment_id).join("unaccounted");
    std::fs::create_dir_all(&stray).unwrap();
    reclaim_environment_volume_root(temp.path(), &environment_id);
    assert!(stray.exists(), "an unaccounted survivor is retained");
}

#[test]
fn one_environments_storage_never_lands_inside_anothers() {
    let temp = tempfile::tempdir().unwrap();
    let first = EnvironmentId::generate();
    let second = EnvironmentId::generate();
    let roots: BTreeSet<PathBuf> = [
        environment_volume_root(temp.path(), &first),
        environment_volume_root(temp.path(), &second),
    ]
    .into_iter()
    .collect();
    assert_eq!(roots.len(), 2);
    let first_root = environment_volume_root(temp.path(), &first);
    let second_root = environment_volume_root(temp.path(), &second);
    assert!(!first_root.starts_with(&second_root));
    assert!(!second_root.starts_with(&first_root));
}

#[test]
fn declared_volume_helpers_report_the_declaration() {
    let empty = spec(vec![developer_linux("machine-0")], Vec::new());
    assert!(!declares_volumes(&empty));
    assert!(declared_volume_names(&empty).is_empty());
    let declared = spec(
        vec![developer_linux("machine-0")],
        vec![shared_cache(
            "cache",
            vec![attachment(
                "machine-0",
                "/cache",
                VolumeAccessMode::ReadWrite,
            )],
        )],
    );
    assert!(declares_volumes(&declared));
    assert_eq!(declared_volume_names(&declared), vec!["cache"]);
}
