#![allow(clippy::unwrap_used, clippy::expect_used)]
//! Adversarial containment tests for declared workspace projections.
//!
//! Every escape test builds a real directory tree and a real symlink and then
//! asserts the escape is refused, so a containment check cannot pass merely
//! because nothing ever tried to escape. The symlink-that-stays-inside case is
//! the control: it proves the rule refuses escapes rather than refusing links.

use super::*;
use std::fs;
use vz_runtime_contract::{
    Architecture, EnvironmentId, EnvironmentSpec, MachineId, MachineInstance, MachineProfile,
    MachineResources, MachineSpec, MachineState, OperatingSystem, TOPOLOGY_SCHEMA_VERSION,
    TargetSpec,
};

/// `root/` with `inside/`, `inside/nested/`, plus an `outside/` sibling that is
/// deliberately NOT under the root.
struct Worktree {
    _temp: tempfile::TempDir,
    root: PathBuf,
    outside: PathBuf,
}

fn worktree() -> Worktree {
    let temp = tempfile::tempdir().expect("tempdir");
    // Canonicalise up front: on macOS the temp dir is itself behind the
    // /var -> /private/var symlink, and a prefix comparison against a
    // non-canonical root would pass or fail for the wrong reason.
    let base = temp.path().canonicalize().expect("canonical temp");
    let root = base.join("root");
    let outside = base.join("outside");
    fs::create_dir_all(root.join("inside/nested")).expect("inside");
    fs::create_dir_all(&outside).expect("outside");
    fs::write(outside.join("secret"), b"not yours").expect("secret");
    Worktree {
        _temp: temp,
        root,
        outside,
    }
}

#[test]
fn plain_relative_paths_resolve_inside_the_worktree_root() {
    let tree = worktree();
    assert_eq!(
        resolve_contained_source("dev", &tree.root, ".").expect("root projects"),
        tree.root
    );
    assert_eq!(
        resolve_contained_source("dev", &tree.root, "inside").expect("subdirectory projects"),
        tree.root.join("inside")
    );
    assert_eq!(
        resolve_contained_source("dev", &tree.root, "inside/nested").expect("nested projects"),
        tree.root.join("inside/nested")
    );
}

#[test]
fn dot_dot_traversal_is_refused_before_the_filesystem_is_touched() {
    let tree = worktree();
    for escape in ["..", "../outside", "inside/../../outside", "inside/.."] {
        let error = resolve_contained_source("dev", &tree.root, escape)
            .expect_err("`..` traversal must be refused");
        assert!(
            matches!(error, WorkspaceProjectionError::InvalidSourcePath { .. }),
            "`{escape}` produced {error:?}"
        );
        assert!(error.to_string().contains("`..` traversal is refused"));
    }
}

#[test]
fn absolute_paths_are_refused() {
    let tree = worktree();
    let absolute = tree.outside.to_string_lossy().into_owned();
    for escape in ["/etc", "/", absolute.as_str()] {
        let error = resolve_contained_source("dev", &tree.root, escape)
            .expect_err("absolute path must be refused");
        assert!(
            error.to_string().contains("absolute paths are refused"),
            "`{escape}` produced {error}"
        );
    }
}

#[test]
fn a_symlink_pointing_outside_the_worktree_is_refused() {
    let tree = worktree();
    // A syntactically perfect relative path: one component, no `..`, not
    // absolute. Only canonicalisation reveals that it leaves the root.
    std::os::unix::fs::symlink(&tree.outside, tree.root.join("escape")).expect("symlink");
    let error = resolve_contained_source("dev", &tree.root, "escape")
        .expect_err("symlink escape must be refused");
    let WorkspaceProjectionError::EscapesWorktreeRoot {
        declared, resolved, ..
    } = &error
    else {
        panic!("expected an escape refusal, got {error:?}");
    };
    assert_eq!(declared, "escape");
    assert_eq!(Path::new(resolved), tree.outside.as_path());
}

#[test]
fn a_symlink_in_an_intermediate_component_cannot_escape_either() {
    let tree = worktree();
    fs::create_dir_all(tree.outside.join("deep")).expect("deep");
    std::os::unix::fs::symlink(&tree.outside, tree.root.join("link")).expect("symlink");
    let error = resolve_contained_source("dev", &tree.root, "link/deep")
        .expect_err("escape through an intermediate symlink must be refused");
    assert!(
        matches!(error, WorkspaceProjectionError::EscapesWorktreeRoot { .. }),
        "got {error:?}"
    );
}

#[test]
fn a_symlink_that_resolves_back_inside_the_worktree_is_allowed() {
    let tree = worktree();
    // The control case. If this failed, the rule would be "no symlinks", which
    // is not the rule and would make the escape tests above vacuous.
    std::os::unix::fs::symlink(tree.root.join("inside/nested"), tree.root.join("shortcut"))
        .expect("symlink");
    let resolved =
        resolve_contained_source("dev", &tree.root, "shortcut").expect("inward symlink is allowed");
    assert_eq!(resolved, tree.root.join("inside/nested"));
}

#[test]
fn a_sibling_directory_sharing_the_roots_name_prefix_does_not_count_as_inside() {
    // `root-evil` starts with the same *string* as `root`. Prefix comparison
    // must be per component, which `Path::starts_with` is; this test fails if
    // anyone rewrites it as a string `starts_with`.
    let tree = worktree();
    let sibling = tree.root.with_file_name("root-evil");
    fs::create_dir_all(&sibling).expect("sibling");
    std::os::unix::fs::symlink(&sibling, tree.root.join("near")).expect("symlink");
    let error = resolve_contained_source("dev", &tree.root, "near")
        .expect_err("a name-prefix sibling is still outside");
    assert!(
        matches!(error, WorkspaceProjectionError::EscapesWorktreeRoot { .. }),
        "got {error:?}"
    );
}

#[test]
fn empty_and_control_character_paths_are_refused() {
    let tree = worktree();
    for escape in ["", "inside//nested", "inside/\u{0}x", "inside/\nx"] {
        assert!(
            resolve_contained_source("dev", &tree.root, escape).is_err(),
            "`{}` must be refused",
            escape.escape_debug()
        );
    }
}

#[test]
fn a_missing_source_is_refused_rather_than_shared_as_nothing() {
    let tree = worktree();
    let error = resolve_contained_source("dev", &tree.root, "absent")
        .expect_err("a missing source must be refused");
    assert!(
        matches!(error, WorkspaceProjectionError::InvalidSourcePath { .. }),
        "got {error:?}"
    );
}

#[test]
fn a_relative_or_missing_worktree_root_is_refused() {
    assert!(matches!(
        resolve_contained_source("dev", Path::new("relative/root"), "."),
        Err(WorkspaceProjectionError::InvalidWorkspaceRoot { .. })
    ));
    assert!(matches!(
        resolve_contained_source("dev", Path::new("/definitely/not/here/vz"), "."),
        Err(WorkspaceProjectionError::InvalidWorkspaceRoot { .. })
    ));
}

fn projection(mode: WorkspaceProjectionMode) -> WorkspaceProjection {
    WorkspaceProjection {
        binding: "src".to_string(),
        target_path: "/work".to_string(),
        mode,
        source_path: "inside".to_string(),
    }
}

#[test]
fn read_write_and_read_only_map_onto_the_virtiofs_carrier() {
    let host = PathBuf::from("/tmp/x");
    let rw = projection_to_volume_mount(
        "dev",
        0,
        &projection(WorkspaceProjectionMode::ReadWrite),
        host.clone(),
    )
    .expect("read_write maps");
    assert_eq!(rw.tag, "vz-mount-0");
    assert_eq!(rw.guest_path.as_deref(), Some("/work"));
    assert!(!rw.read_only);

    let ro = projection_to_volume_mount(
        "dev",
        1,
        &projection(WorkspaceProjectionMode::ReadOnly),
        host,
    )
    .expect("read_only maps");
    assert_eq!(ro.tag, "vz-mount-1");
    assert!(ro.read_only);
}

#[test]
fn the_mount_tag_keeps_the_shape_the_kernel_cmdline_mapping_requires() {
    // `stack_vm` builds `vz.mount.{N}` by stripping this exact prefix, and
    // `linux/initramfs/init` parses that argument. A tag of any other shape
    // would create a VirtioFS share the guest never bind-mounts.
    let mount = projection_to_volume_mount(
        "dev",
        3,
        &projection(WorkspaceProjectionMode::ReadWrite),
        PathBuf::from("/tmp/x"),
    )
    .expect("maps");
    assert_eq!(mount.tag.strip_prefix("vz-mount-"), Some("3"));
}

#[test]
fn a_snapshot_projection_is_shared_writable_because_the_copy_is_private() {
    // A snapshot Machine writes into its own clone, so the share is read-write.
    // Mounting it read-only would make `snapshot` an awkward synonym for
    // `read_only` with a copy nobody could use.
    let mount = projection_to_volume_mount(
        "dev",
        0,
        &projection(WorkspaceProjectionMode::Snapshot),
        PathBuf::from("/tmp/x"),
    )
    .expect("snapshot projections are applied");
    assert!(!mount.read_only);
    assert_eq!(mount.tag, "vz-mount-0");
}

#[test]
fn a_snapshot_copy_is_private_recursive_and_remade_on_every_up() {
    // The three properties `snapshot` promises, each asserted rather than
    // assumed: the copy is recursive, writes into it never reach the source,
    // and a second Up does not merge the previous boot's tree into it.
    let temp = tempfile::tempdir().expect("tempdir");
    let base = temp.path().canonicalize().expect("canonical");
    let source = base.join("source");
    fs::create_dir_all(source.join("nested")).expect("source tree");
    fs::write(source.join("nested/file"), b"original").expect("file");
    let store = base.join("machine-store");
    fs::create_dir_all(&store).expect("store");

    let clone = materialise_snapshot("dev", &source, &store).expect("snapshot");
    assert_eq!(clone, machine_snapshot_path(&store));
    assert_eq!(
        fs::read(clone.join("nested/file")).expect("cloned"),
        b"original"
    );

    // The Machine writes into its copy; the worktree must not see it. This is
    // the entire difference between `snapshot` and `read_write`.
    fs::write(clone.join("nested/file"), b"guest wrote this").expect("guest write");
    fs::write(clone.join("guest-only"), b"new").expect("guest create");
    assert_eq!(
        fs::read(source.join("nested/file")).expect("source"),
        b"original"
    );
    assert!(!source.join("guest-only").exists());

    // A second Up re-clones: the tree is the source as it is NOW, and the
    // previous boot's additions are gone rather than merged in.
    fs::write(source.join("nested/file"), b"changed on the host").expect("host write");
    let again = materialise_snapshot("dev", &source, &store).expect("second snapshot");
    assert_eq!(
        fs::read(again.join("nested/file")).expect("recloned"),
        b"changed on the host"
    );
    assert!(!again.join("guest-only").exists());
}

#[test]
fn snapshot_projections_of_one_source_do_not_collide() {
    // Two snapshots of one source are allowed where two read-write projections
    // of it are refused, because neither Machine mutates the shared subtree.
    let tree = worktree();
    let spec = spec(vec![
        machine_spec("dev-a", Some(projection(WorkspaceProjectionMode::Snapshot))),
        machine_spec("dev-b", Some(projection(WorkspaceProjectionMode::Snapshot))),
    ]);
    refuse_declared_writable_multi_attach(&spec).expect("two snapshots are not a multi-attach");
    let machines = vec![machine_instance("dev-a"), machine_instance("dev-b")];
    let resolved = resolve_environment_workspace_mounts(
        &spec,
        &machines,
        &BTreeSet::from(["src".to_string()]),
        Some(tree.root.to_str().expect("utf-8 root")),
    )
    .expect("snapshots resolve");
    // Both are reported as needing a clone, and both shares still name the
    // SOURCE at this point: the redirect happens once each Machine's store
    // exists.
    assert_eq!(resolved.snapshot_sources.len(), 2);
    for machine in &machines {
        assert_eq!(
            resolved.snapshot_sources[&machine.machine_id],
            tree.root.join("inside")
        );
        assert_eq!(
            resolved.mounts[&machine.machine_id][0].host_path,
            tree.root.join("inside")
        );
    }
}

fn machine_spec(name: &str, workspace: Option<WorkspaceProjection>) -> MachineSpec {
    MachineSpec {
        schema_version: TOPOLOGY_SCHEMA_VERSION,
        name: name.to_string(),
        profile: MachineProfile::Developer,
        target: target(),
        resources: MachineResources::default(),
        requested_capabilities: Default::default(),
        workspace,
        networks: Vec::new(),
        egress: Default::default(),
    }
}

fn target() -> TargetSpec {
    TargetSpec {
        os: OperatingSystem::Linux,
        arch: Architecture::Aarch64,
        image: "vz/linux".to_string(),
        version: None,
        channel: None,
        digest: None,
    }
}

fn machine_instance(name: &str) -> MachineInstance {
    MachineInstance {
        schema_version: TOPOLOGY_SCHEMA_VERSION,
        machine_id: MachineId::generate(),
        environment_id: EnvironmentId::generate(),
        name: name.to_string(),
        state: MachineState::Creating,
        profile: MachineProfile::Developer,
        target: target(),
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

fn spec(machines: Vec<MachineSpec>) -> EnvironmentSpec {
    EnvironmentSpec {
        volumes: Vec::new(),
        schema_version: TOPOLOGY_SCHEMA_VERSION,
        default_machine: None,
        machines,
        networks: Vec::new(),
        endpoints: Vec::new(),
        host_exports: Vec::new(),
        host_imports: Vec::new(),
    }
}

#[test]
fn an_unresolved_symbolic_slot_refuses_before_any_share_is_built() {
    let tree = worktree();
    let spec = spec(vec![machine_spec(
        "dev",
        Some(projection(WorkspaceProjectionMode::ReadWrite)),
    )]);
    let error = resolve_environment_workspace_mounts(
        &spec,
        &[machine_instance("dev")],
        &BTreeSet::new(),
        Some(&tree.root.to_string_lossy()),
    )
    .expect_err("an unresolved slot must refuse");
    assert!(
        matches!(error, WorkspaceProjectionError::UnresolvedSlot { .. }),
        "got {error:?}"
    );
}

#[test]
fn a_declared_projection_without_a_workspace_root_refuses() {
    let spec = spec(vec![machine_spec(
        "dev",
        Some(projection(WorkspaceProjectionMode::ReadWrite)),
    )]);
    let error = resolve_environment_workspace_mounts(
        &spec,
        &[machine_instance("dev")],
        &BTreeSet::from(["src".to_string()]),
        None,
    )
    .expect_err("no root must refuse");
    assert!(matches!(
        error,
        WorkspaceProjectionError::MissingWorkspaceRoot { .. }
    ));
}

#[test]
fn only_machines_that_declare_a_projection_get_shares() {
    let tree = worktree();
    let spec = spec(vec![
        machine_spec("dev", Some(projection(WorkspaceProjectionMode::ReadOnly))),
        machine_spec("plain", None),
    ]);
    let machines = vec![machine_instance("dev"), machine_instance("plain")];
    let mounts = resolve_environment_workspace_mounts(
        &spec,
        &machines,
        &BTreeSet::from(["src".to_string()]),
        Some(&tree.root.to_string_lossy()),
    )
    .expect("resolves");
    assert_eq!(mounts.mounts.len(), 1);
    assert!(mounts.snapshot_sources.is_empty());
    let shares = &mounts.mounts[&machines[0].machine_id];
    assert_eq!(shares.len(), 1);
    assert_eq!(shares[0].host_path, tree.root.join("inside"));
    assert!(shares[0].read_only);
    assert!(!mounts.mounts.contains_key(&machines[1].machine_id));
}

#[test]
fn an_environment_with_no_declared_projection_resolves_to_no_shares() {
    let spec = spec(vec![machine_spec("plain", None)]);
    assert!(declared_workspace_slots(&spec).is_empty());
    assert!(
        resolve_environment_workspace_mounts(
            &spec,
            &[machine_instance("plain")],
            &BTreeSet::new(),
            None,
        )
        .expect("resolves")
        .mounts
        .is_empty()
    );
}

#[test]
fn the_minted_binding_name_is_opaque_and_never_the_symbolic_slot() {
    let minted = minted_binding_name("some-random-worktree-token");
    assert!(minted.starts_with("worktree-"));
    assert_eq!(minted.len(), 41);
    assert_ne!(minted, "src");
    // Deterministic per token, and distinct across tokens.
    assert_eq!(minted, minted_binding_name("some-random-worktree-token"));
    assert_ne!(minted, minted_binding_name("a-different-token"));
}

/// The exact pre-boot sequence `supervise_up` performs, against a real state
/// store.
///
/// Up itself cannot reach this: `validate_supported` still refuses any
/// definition that declares a workspace projection, and that guard stays until
/// vz-9vv.7. So the composition is driven here directly — mint the opaque
/// binding name, reserve it with its slot resolution table while the
/// Environment is still `Creating`, reload the persisted aggregate, and resolve
/// the reloaded slots into VirtioFS shares. Everything but `LinuxVm::create` is
/// covered, and the ordering claim (resolve before the boot loop) is what makes
/// the reload step meaningful: the shares exist before any Machine boots.
#[test]
fn the_supervisors_pre_boot_sequence_resolves_shares_from_durable_state() {
    let tree = worktree();
    let store = vz_stack::StateStore::in_memory().expect("store");
    let definition: vz_runtime_contract::ProjectDefinition =
        serde_json::from_value(serde_json::json!({
            "schema_version": 1,
            "project_id": vz_runtime_contract::ProjectId::generate(),
            "name": "workspace-projection",
            "environment": {
                "schema_version": 1,
                "machines": [{
                    "schema_version": 1,
                    "name": "dev",
                    "profile": "developer",
                    "target": {
                        "os": "linux",
                        "arch": "aarch64",
                        "image": "vz-linux-appliance",
                        "digest": format!("sha256:{}", "a".repeat(64)),
                    },
                    "workspace": {
                        "binding": "src",
                        "target_path": "/work",
                        "mode": "read_write",
                        "source_path": "inside",
                    },
                }],
            },
        }))
        .expect("definition parses");

    let reservation = store
        .resolve_or_reserve_environment_for_up(
            &definition,
            &vz_runtime_contract::EnvironmentSelectionContext {
                explicit: Some(vz_runtime_contract::EnvironmentSelector::Name(
                    "agent".into(),
                )),
                ..Default::default()
            },
            100,
        )
        .expect("reserve");
    let vz_stack::EnvironmentUpReservation::Created { environment } = reservation else {
        panic!("expected a new Environment");
    };
    assert_eq!(
        environment.state,
        vz_runtime_contract::EnvironmentState::Creating,
        "the slot is reserved while the Environment is still Creating, before any boot"
    );

    // 1. Mint, unchanged by decision 8.
    let workspace_key = "random-per-worktree-token";
    let minted = minted_binding_name(workspace_key);
    // 2. Record the resolution table for every declared slot.
    let declared = declared_workspace_slots(&definition.environment);
    assert_eq!(declared, BTreeSet::from(["src".to_string()]));
    assert!(
        !declared.contains(minted.as_str()),
        "the definition can never name what Up mints; that is why the table exists"
    );
    let binding = vz_runtime_contract::WorkspaceBinding {
        schema_version: 1,
        binding_id: vz_runtime_contract::WorkspaceBindingId::generate(),
        project_id: environment.project_id.clone(),
        environment_id: environment.environment_id.clone(),
        name: minted,
        workspace_key: workspace_key.to_string(),
        path_hint: Some("/diagnostic/only".to_string()),
        slots: declared,
    };
    store
        .reserve_workspace_binding_for_environment(&binding, 101)
        .expect("an opaque minted name resolving a declared slot is admitted");

    // 3. Reload the durable aggregate: the shares must come from persisted
    //    state, not from the in-memory value we just built.
    let persisted = store
        .load_project_state(definition.project_id.as_str())
        .expect("load")
        .expect("project");
    let reloaded = &persisted.environments[0];
    let resolved: BTreeSet<String> = reloaded
        .bindings
        .iter()
        .flat_map(|binding| binding.slots.iter().cloned())
        .collect();
    assert_eq!(resolved, BTreeSet::from(["src".to_string()]));

    // 4. Resolve the shares, exactly as the supervisor does before its boot loop.
    let mounts = resolve_environment_workspace_mounts(
        &definition.environment,
        &reloaded.machines,
        &resolved,
        Some(&tree.root.to_string_lossy()),
    )
    .expect("shares resolve");
    let machine_id = &reloaded.machines[0].machine_id;
    let shares = mounts
        .mounts
        .get(machine_id)
        .expect("the Machine gets its share");
    assert_eq!(shares.len(), 1);
    assert_eq!(shares[0].host_path, tree.root.join("inside"));
    assert_eq!(shares[0].guest_path.as_deref(), Some("/work"));
    assert!(!shares[0].read_only);
    assert_eq!(shares[0].tag, "vz-mount-0");

    // 5. The whole point of the ordering: this is a `StackResourceHint` a boot
    //    can be handed, and VirtioFS shares are fixed at VM creation.
    let hint = vz_runtime_contract::StackResourceHint {
        cpus: Some(2),
        memory_mb: Some(2048),
        volume_mounts: shares.clone(),
        ..Default::default()
    };
    assert_eq!(hint.volume_mounts, *shares);

    // 6. A definition whose declared source escapes the worktree is refused at
    //    this same point, before a Machine exists to hold the share.
    std::os::unix::fs::symlink(&tree.outside, tree.root.join("escape")).expect("symlink");
    let mut escaping = definition.clone();
    escaping.environment.machines[0]
        .workspace
        .as_mut()
        .expect("workspace")
        .source_path = "escape".to_string();
    let error = resolve_environment_workspace_mounts(
        &escaping.environment,
        &reloaded.machines,
        &resolved,
        Some(&tree.root.to_string_lossy()),
    )
    .expect_err("an escaping source is refused before the boot loop");
    assert!(matches!(
        error,
        WorkspaceProjectionError::EscapesWorktreeRoot { .. }
    ));
}

// ---------------------------------------------------------------------------
// Writable multi-attach: one host source, two Machines.
// ---------------------------------------------------------------------------
//
// Every case below builds a real tree and asks the real resolver, so a refusal
// cannot pass because nothing was ever shared twice. The read-only case and the
// distinct-sources case are the controls: without them the rule could pass by
// refusing all sharing rather than refusing unserialised writers.

fn writable_at(source: &str) -> WorkspaceProjection {
    let mut projection = projection(WorkspaceProjectionMode::ReadWrite);
    projection.source_path = source.to_string();
    projection
}

fn readable_at(source: &str) -> WorkspaceProjection {
    let mut projection = projection(WorkspaceProjectionMode::ReadOnly);
    projection.source_path = source.to_string();
    projection
}

fn resolve_pair(
    tree: &Worktree,
    first: WorkspaceProjection,
    second: WorkspaceProjection,
) -> Result<ResolvedWorkspaceMounts, WorkspaceProjectionError> {
    let spec = spec(vec![
        machine_spec("dev-a", Some(first)),
        machine_spec("dev-b", Some(second)),
    ]);
    let machines = vec![machine_instance("dev-a"), machine_instance("dev-b")];
    let slots = BTreeSet::from(["src".to_string()]);
    resolve_environment_workspace_mounts(
        &spec,
        &machines,
        &slots,
        Some(tree.root.to_str().expect("utf-8 root")),
    )
}

fn admit_pair(
    first: WorkspaceProjection,
    second: WorkspaceProjection,
) -> Result<(), WorkspaceProjectionError> {
    refuse_declared_writable_multi_attach(&spec(vec![
        machine_spec("dev-a", Some(first)),
        machine_spec("dev-b", Some(second)),
    ]))
}

fn assert_multi_attach(error: WorkspaceProjectionError) {
    assert!(
        matches!(
            error,
            WorkspaceProjectionError::WritableSourceMultiAttach { .. }
        ),
        "expected a multi-attach refusal, got: {error}"
    );
}

#[test]
fn two_machines_writing_one_host_source_are_refused_rather_than_silently_attached() {
    let tree = worktree();
    assert_multi_attach(
        resolve_pair(&tree, writable_at("inside"), writable_at("inside"))
            .expect_err("a second writable attach of one host source must be refused"),
    );
    assert_multi_attach(
        admit_pair(writable_at("inside"), writable_at("inside"))
            .expect_err("and it must be refused at admission, before any binding is reserved"),
    );
}

#[test]
fn a_writer_and_a_reader_of_one_host_source_are_refused_too() {
    // The reader observes a tree the writer mutates underneath it, with no
    // coherence protocol between the two VirtioFS shares.
    let tree = worktree();
    assert_multi_attach(
        resolve_pair(&tree, writable_at("inside"), readable_at("inside"))
            .expect_err("a writer plus a reader of one host source must be refused"),
    );
    assert_multi_attach(
        admit_pair(readable_at("inside"), writable_at("inside"))
            .expect_err("in either declaration order"),
    );
}

#[test]
fn two_machines_reading_one_host_source_read_only_are_allowed() {
    // The control: with no writer there is nothing to serialise, so the rule
    // must not simply refuse all sharing.
    let tree = worktree();
    let mounts = resolve_pair(&tree, readable_at("inside"), readable_at("inside"))
        .expect("two read-only readers of one source are not a multi-attach");
    assert_eq!(mounts.mounts.len(), 2);
    assert!(
        mounts
            .mounts
            .values()
            .flatten()
            .all(|mount| mount.read_only)
    );
    admit_pair(readable_at("inside"), readable_at("inside")).expect("admitted");
}

#[test]
fn distinct_sources_under_one_root_are_not_a_multi_attach() {
    // `inside` and `inside-two` share a string prefix but no directory. A
    // comparison that is not per-component refuses this pair wrongly.
    let tree = worktree();
    fs::create_dir_all(tree.root.join("inside-two")).expect("sibling");
    let mounts = resolve_pair(&tree, writable_at("inside"), writable_at("inside-two"))
        .expect("distinct writable sources are not a multi-attach");
    assert_eq!(mounts.mounts.len(), 2);
    admit_pair(writable_at("inside"), writable_at("inside-two")).expect("admitted");
}

#[test]
fn a_symlink_alias_of_one_source_is_still_the_same_writable_attach() {
    // Two declarations that differ as strings become one directory once the
    // link is followed. Only the resolved-path half of the rule can see this,
    // which is why the syntactic half is not the whole rule.
    let tree = worktree();
    std::os::unix::fs::symlink(tree.root.join("inside"), tree.root.join("alias"))
        .expect("alias symlink");
    assert_multi_attach(
        resolve_pair(&tree, writable_at("inside"), writable_at("alias"))
            .expect_err("a symlink alias names the same host directory"),
    );
    // The declaration-level rule cannot see through the link, and says so by
    // admitting the pair; the resolver above is what refuses it.
    admit_pair(writable_at("inside"), writable_at("alias"))
        .expect("two distinct declared strings pass the syntactic half");
}

#[test]
fn a_writable_source_nested_inside_another_writable_source_is_refused() {
    // Overlap, not equality: one Machine's share contains the other's.
    let tree = worktree();
    assert_multi_attach(
        resolve_pair(&tree, writable_at("."), writable_at("inside/nested"))
            .expect_err("an overlapping writable source must be refused"),
    );
    assert_multi_attach(
        admit_pair(writable_at("."), writable_at("inside/nested"))
            .expect_err("and the containment is visible syntactically"),
    );
    assert_multi_attach(
        admit_pair(writable_at("inside/nested"), writable_at("."))
            .expect_err("in either declaration order"),
    );
}

#[test]
fn a_single_writable_projection_is_untouched_by_the_rule() {
    // The regression guard for the ordinary one-Machine case the rule must not
    // disturb.
    let tree = worktree();
    let spec = spec(vec![machine_spec("dev-a", Some(writable_at("inside")))]);
    let machines = vec![machine_instance("dev-a")];
    let slots = BTreeSet::from(["src".to_string()]);
    let mounts = resolve_environment_workspace_mounts(
        &spec,
        &machines,
        &slots,
        Some(tree.root.to_str().expect("utf-8 root")),
    )
    .expect("one writer is not a multi-attach");
    assert_eq!(mounts.mounts.len(), 1);
    assert!(
        mounts
            .mounts
            .values()
            .flatten()
            .all(|mount| !mount.read_only)
    );
    refuse_declared_writable_multi_attach(&spec).expect("admitted");
}
