#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::sync::{Arc, Barrier};

use tempfile::TempDir;
use vz_cli::developer_environment_context::{
    GitWorkspace, ProcessTopologySelectors, VZ_ENVIRONMENT_ID, VZ_MACHINE_ID,
    discover_existing_git_workspace, resolve_git_workspace,
};
use vz_runtime_contract::{
    Architecture, CapabilitySet, EnvironmentSelectionContext, EnvironmentSelectionSource,
    EnvironmentSpec, EnvironmentState, MachineId, MachineProfile, MachineResources, MachineSpec,
    OperatingSystem, ProjectDefinition, ProjectId, ProjectState, TOPOLOGY_SCHEMA_VERSION,
    TargetSpec, TopologyResolutionError, WorkspaceBinding, WorkspaceBindingId, WorkspaceProjection,
    WorkspaceProjectionMode,
};
use vz_stack::{StackError, StateStore};

const SELECTOR_PROBE: &str = "VZ_TEST_PROCESS_TOPOLOGY_SELECTORS";

#[test]
fn read_only_workspace_discovery_does_not_create_a_token() {
    let fixture = GitFixture::new();
    let metadata = fixture.repo.join(".git/vz");
    assert!(!metadata.exists());
    assert!(
        discover_existing_git_workspace(&fixture.repo)
            .unwrap()
            .is_none()
    );
    assert!(!metadata.exists());
}

#[test]
fn read_only_workspace_discovery_preserves_existing_token() {
    let fixture = GitFixture::new();
    let expected = bind_git_workspace(&fixture.repo).unwrap();
    let token_path = expected.git_dir.join("vz/workspace-id");
    let before = std::fs::metadata(&token_path).unwrap().modified().unwrap();
    let token = std::fs::read(&token_path).unwrap();
    assert_eq!(
        discover_existing_git_workspace(&fixture.repo).unwrap(),
        Some(expected)
    );
    assert_eq!(
        std::fs::metadata(&token_path).unwrap().modified().unwrap(),
        before
    );
    assert_eq!(std::fs::read(token_path).unwrap(), token);
}

#[test]
fn read_only_workspace_discovery_rejects_corruption_without_replacing_it() {
    let fixture = GitFixture::new();
    let expected = bind_git_workspace(&fixture.repo).unwrap();
    let token_path = expected.git_dir.join("vz/workspace-id");
    std::fs::write(&token_path, "corrupt token").unwrap();
    assert!(discover_existing_git_workspace(&fixture.repo).is_err());
    assert_eq!(
        std::fs::read_to_string(token_path).unwrap(),
        "corrupt token"
    );
}

#[cfg(unix)]
#[test]
fn read_only_workspace_discovery_rejects_dangling_token_symlink() {
    let fixture = GitFixture::new();
    let metadata = fixture.repo.join(".git/vz");
    std::fs::create_dir(&metadata).unwrap();
    let token_path = metadata.join("workspace-id");
    let missing = metadata.join("missing-token");
    std::os::unix::fs::symlink(&missing, &token_path).unwrap();
    assert!(discover_existing_git_workspace(&fixture.repo).is_err());
    assert_eq!(std::fs::read_link(token_path).unwrap(), missing);
    assert!(!missing.exists());
}

struct GitFixture {
    _temporary: TempDir,
    repo: PathBuf,
}

impl GitFixture {
    fn new() -> Self {
        let temporary = tempfile::tempdir().unwrap();
        let repo = temporary.path().join("repository");
        run_git(temporary.path(), &["init", repo.to_str().unwrap()]);
        run_git(&repo, &["config", "user.email", "vz-test@example.invalid"]);
        run_git(&repo, &["config", "user.name", "vz test"]);
        std::fs::write(repo.join("seed"), "seed\n").unwrap();
        run_git(&repo, &["add", "seed"]);
        run_git(&repo, &["commit", "--quiet", "-m", "initial"]);
        Self {
            _temporary: temporary,
            repo,
        }
    }

    fn add_worktree(&self, name: &str) -> PathBuf {
        let path = self._temporary.path().join(name);
        run_git(
            &self.repo,
            &[
                "worktree",
                "add",
                "--quiet",
                "--detach",
                path.to_str().unwrap(),
                "HEAD",
            ],
        );
        path
    }

    fn move_worktree(&self, from: &Path, name: &str) -> PathBuf {
        let to = self._temporary.path().join(name);
        run_git(
            &self.repo,
            &[
                "worktree",
                "move",
                from.to_str().unwrap(),
                to.to_str().unwrap(),
            ],
        );
        to
    }
}

fn run_git(cwd: &Path, args: &[&str]) -> Output {
    let output = Command::new("git")
        .arg("-C")
        .arg(cwd)
        .args(args)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "git {:?} failed: {}",
        args,
        String::from_utf8_lossy(&output.stderr)
    );
    output
}

fn project_state(workspace_key: &str, path_hint: &Path) -> ProjectState {
    let definition = ProjectDefinition {
        schema_version: TOPOLOGY_SCHEMA_VERSION,
        project_id: ProjectId::new("prj_real_git_worktree").unwrap(),
        name: "real-git-worktree".to_string(),
        environment: EnvironmentSpec {
            volumes: Vec::new(),
            host_exports: Vec::new(),
            host_imports: Vec::new(),
            schema_version: TOPOLOGY_SCHEMA_VERSION,
            default_machine: None,
            machines: vec![MachineSpec {
                networks: Vec::new(),
                egress: Default::default(),
                schema_version: TOPOLOGY_SCHEMA_VERSION,
                name: "linux".to_string(),
                profile: MachineProfile::Developer,
                target: TargetSpec {
                    os: OperatingSystem::Linux,
                    arch: Architecture::Aarch64,
                    image: "fixture:latest".to_string(),
                    version: None,
                    channel: None,
                    digest: None,
                },
                resources: MachineResources::default(),
                requested_capabilities: CapabilitySet::default(),
                workspace: Some(WorkspaceProjection {
                    binding: "workspace".to_string(),
                    target_path: "/workspace".to_string(),
                    mode: WorkspaceProjectionMode::ReadWrite,
                    source_path: ".".to_string(),
                }),
            }],
            networks: vec![],
            endpoints: vec![],
        },
    };
    let mut environment = definition.instantiate_environment("agent", 100).unwrap();
    assert_eq!(environment.state, EnvironmentState::Creating);
    environment.bindings.push(WorkspaceBinding {
        schema_version: TOPOLOGY_SCHEMA_VERSION,
        binding_id: WorkspaceBindingId::generate(),
        project_id: definition.project_id.clone(),
        environment_id: environment.environment_id.clone(),
        name: format!("worktree-{workspace_key}"),
        workspace_key: workspace_key.to_string(),
        path_hint: Some(path_hint.to_string_lossy().into_owned()),
        slots: std::collections::BTreeSet::from(["workspace".to_string()]),
    });
    ProjectState {
        schema_version: TOPOLOGY_SCHEMA_VERSION,
        definition,
        environments: vec![environment],
    }
}

#[test]
fn real_linked_worktree_move_preserves_selection_and_new_worktree_does_not_adopt() {
    let fixture = GitFixture::new();
    let first_path = fixture.add_worktree("first");
    let first = bind_git_workspace(&first_path).unwrap();
    assert!(first.git_dir.is_absolute());
    assert_eq!(
        std::fs::read_to_string(first.git_dir.join("vz/workspace-id")).unwrap(),
        first.workspace_key
    );

    let store = StateStore::in_memory().unwrap();
    let state = project_state(&first.workspace_key, &first.path_hint);
    store.save_project_state(&state).unwrap();

    let moved_path = fixture.move_worktree(&first_path, "moved");
    let moved = bind_git_workspace(&moved_path.join(".")).unwrap();
    assert_eq!(moved.git_dir, first.git_dir);
    assert_eq!(moved.workspace_key, first.workspace_key);
    assert_ne!(moved.path_hint, first.path_hint);
    let selected = store
        .resolve_environment(
            state.definition.project_id.as_str(),
            &EnvironmentSelectionContext {
                workspace_key: Some(moved.workspace_key.clone()),
                ..EnvironmentSelectionContext::default()
            },
        )
        .unwrap();
    assert_eq!(
        selected.environment_id,
        state.environments[0].environment_id
    );

    let second_path = fixture.add_worktree("second");
    let second = bind_git_workspace(&second_path).unwrap();
    assert_ne!(second.git_dir, moved.git_dir);
    assert_ne!(second.workspace_key, moved.workspace_key);
    let before = store
        .load_project_state(state.definition.project_id.as_str())
        .unwrap();
    let error = store
        .resolve_environment(
            state.definition.project_id.as_str(),
            &EnvironmentSelectionContext {
                workspace_key: Some(second.workspace_key),
                ..EnvironmentSelectionContext::default()
            },
        )
        .unwrap_err();
    assert!(matches!(
        error,
        StackError::TopologyResolution(error)
            if matches!(error.as_ref(), TopologyResolutionError::SelectionRequired { .. })
    ));
    assert_eq!(
        store
            .load_project_state(state.definition.project_id.as_str())
            .unwrap(),
        before
    );
}

/// Concurrent first binders pick exactly one winner, and it stands.
///
/// Resolution no longer publishes, so contenders each mint their own key and
/// only the no-clobber publish arbitrates. What must hold is that the worktree
/// ends up carrying exactly one token, that token belongs to a caller that was
/// told it won, and no loser clobbered it.
#[test]
fn concurrent_first_binding_leaves_exactly_one_winner() {
    let fixture = GitFixture::new();
    let worktree = fixture.add_worktree("concurrent");
    let barrier = Arc::new(Barrier::new(9));
    let handles: Vec<_> = (0..8)
        .map(|_| {
            let worktree = worktree.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                let pending = resolve_git_workspace(&worktree).unwrap();
                let key = pending.workspace().workspace_key.clone();
                let git_dir = pending.workspace().git_dir.clone();
                barrier.wait();
                (key, git_dir, pending.commit().is_ok())
            })
        })
        .collect();
    barrier.wait();

    let outcomes: Vec<_> = handles
        .into_iter()
        .map(|handle| handle.join().unwrap())
        .collect();
    let winners: BTreeSet<_> = outcomes
        .iter()
        .filter(|(_, _, committed)| *committed)
        .map(|(key, _, _)| key.clone())
        .collect();
    assert_eq!(winners.len(), 1);
    let persisted = std::fs::read_to_string(outcomes[0].1.join("vz/workspace-id")).unwrap();
    assert_eq!(Some(&persisted), winners.iter().next());
}

/// Resolving a never-bound worktree mints an identity and writes nothing.
///
/// This is the property a refused `vz up` depends on: the runtime is handed a
/// workspace key it may refuse, and the worktree it was refused in is byte
/// identical afterwards.
#[test]
fn resolving_an_unbound_worktree_mints_without_touching_it() {
    let fixture = GitFixture::new();
    let worktree = fixture.add_worktree("unbound");
    let before = inventory(&worktree);

    let pending = resolve_git_workspace(&worktree).unwrap();
    assert!(!pending.is_bound());
    assert!(pending.workspace().workspace_key.starts_with("wsp_"));
    assert_eq!(inventory(&worktree), before);

    pending.commit().unwrap();
    assert!(pending.is_bound() || inventory(&worktree) != before);
    assert_eq!(
        std::fs::read_to_string(pending.workspace().git_dir.join("vz/workspace-id")).unwrap(),
        pending.workspace().workspace_key
    );
}

/// Resolving an already-bound worktree returns its token and writes nothing.
#[test]
fn resolving_a_bound_worktree_reuses_its_token_without_touching_it() {
    let fixture = GitFixture::new();
    let worktree = fixture.add_worktree("bound");
    let bound = bind_git_workspace(&worktree).unwrap();
    let before = inventory(&worktree);

    let pending = resolve_git_workspace(&worktree).unwrap();
    assert!(pending.is_bound());
    assert_eq!(pending.workspace(), &bound);
    assert_eq!(inventory(&worktree), before);

    pending.commit().unwrap();
    assert_eq!(inventory(&worktree), before);
}

/// Bind a worktree the way a successful `vz up` does: resolve, then publish.
fn bind_git_workspace(cwd: &Path) -> anyhow::Result<GitWorkspace> {
    let pending = resolve_git_workspace(cwd)?;
    pending.commit()?;
    Ok(pending.workspace().clone())
}

/// Every path under `root`, including the private Git metadata, with file
/// contents, so any appearance, disappearance or edit is a difference.
fn inventory(root: &Path) -> BTreeMap<PathBuf, Option<Vec<u8>>> {
    fn walk(root: &Path, at: &Path, into: &mut BTreeMap<PathBuf, Option<Vec<u8>>>) {
        let mut entries: Vec<_> = std::fs::read_dir(at)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .collect();
        entries.sort();
        for path in entries {
            let relative = path.strip_prefix(root).unwrap().to_path_buf();
            let metadata = std::fs::symlink_metadata(&path).unwrap();
            if metadata.is_dir() {
                into.insert(relative, None);
                walk(root, &path, into);
            } else if metadata.is_symlink() {
                into.insert(
                    relative,
                    Some(
                        std::fs::read_link(&path)
                            .unwrap()
                            .into_os_string()
                            .into_encoded_bytes(),
                    ),
                );
            } else {
                into.insert(relative, Some(std::fs::read(&path).unwrap()));
            }
        }
    }
    let mut into = BTreeMap::new();
    walk(root, root, &mut into);
    // A linked worktree keeps its private Git directory outside the checkout.
    let git_dir = resolve_git_workspace(root)
        .map(|pending| pending.workspace().git_dir.clone())
        .unwrap();
    if git_dir.is_dir() && !git_dir.starts_with(root) {
        let mut private = BTreeMap::new();
        walk(&git_dir, &git_dir, &mut private);
        for (path, contents) in private {
            into.insert(Path::new("<git-dir>").join(path), contents);
        }
    }
    into
}

#[test]
fn composition_retains_process_environment_id_above_workspace_binding() {
    let fixture = GitFixture::new();
    let worktree = fixture.add_worktree("selection-composition");
    let workspace = bind_git_workspace(&worktree).unwrap();
    let mut state = project_state(&workspace.workspace_key, &workspace.path_hint);
    let process_environment = state
        .definition
        .instantiate_environment("process-selected", 101)
        .unwrap();
    let process_environment_id = process_environment.environment_id.clone();
    state.environments.push(process_environment);

    let selectors = ProcessTopologySelectors {
        environment_id: Some(process_environment_id.clone()),
        machine_id: None,
    };
    let context = selectors.environment_selection_context(None, &workspace);
    assert_eq!(
        context.process_environment_id,
        Some(process_environment_id.clone())
    );
    assert_eq!(context.workspace_key, Some(workspace.workspace_key));

    let store = StateStore::in_memory().unwrap();
    store.save_project_state(&state).unwrap();
    let selected = store
        .resolve_environment(state.definition.project_id.as_str(), &context)
        .unwrap();
    assert_eq!(selected.environment_id, process_environment_id);
    assert_eq!(selected.source, EnvironmentSelectionSource::Process);
}

#[test]
fn resolution_rejects_non_git_directories_and_invalid_persisted_tokens() {
    let outside = tempfile::tempdir().unwrap();
    assert!(resolve_git_workspace(outside.path()).is_err());

    let fixture = GitFixture::new();
    let worktree = fixture.add_worktree("invalid-token");
    let discovered = bind_git_workspace(&worktree).unwrap();
    std::fs::write(discovered.git_dir.join("vz/workspace-id"), "not valid").unwrap();
    // A corrupt token fails closed; it is never treated as an unbound worktree
    // and replaced with a freshly minted identity.
    assert!(resolve_git_workspace(&worktree).is_err());
    assert_eq!(
        std::fs::read_to_string(discovered.git_dir.join("vz/workspace-id")).unwrap(),
        "not valid"
    );
}

#[test]
fn process_topology_selectors_are_consumed_strictly_in_a_subprocess() {
    if let Some(mode) = std::env::var_os(SELECTOR_PROBE) {
        let mode = mode.to_string_lossy();
        let result = ProcessTopologySelectors::from_current_process();
        match mode.as_ref() {
            "valid" => {
                let selectors = result.unwrap();
                assert_eq!(
                    selectors.environment_id,
                    Some(vz_runtime_contract::EnvironmentId::new("env_process").unwrap())
                );
                assert_eq!(
                    selectors.machine_id,
                    Some(MachineId::new("mch_process").unwrap())
                );
            }
            "invalid-environment" => {
                assert!(result.unwrap_err().to_string().contains(VZ_ENVIRONMENT_ID));
            }
            "invalid-machine" => {
                assert!(result.unwrap_err().to_string().contains(VZ_MACHINE_ID));
            }
            other => panic!("unknown selector probe mode {other}"),
        }
        return;
    }

    for (mode, environment_id, machine_id) in [
        ("valid", "env_process", "mch_process"),
        ("invalid-environment", "not valid", "mch_process"),
        ("invalid-machine", "env_process", "not valid"),
    ] {
        let output = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "process_topology_selectors_are_consumed_strictly_in_a_subprocess",
                "--nocapture",
            ])
            .env(SELECTOR_PROBE, mode)
            .env(VZ_ENVIRONMENT_ID, environment_id)
            .env(VZ_MACHINE_ID, machine_id)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "selector subprocess {mode} failed:\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
}
