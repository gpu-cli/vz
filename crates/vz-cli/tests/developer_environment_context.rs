#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::sync::{Arc, Barrier};

use tempfile::TempDir;
use vz_cli::developer_environment_context::{
    ProcessTopologySelectors, VZ_ENVIRONMENT_ID, VZ_MACHINE_ID, discover_git_workspace,
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
            schema_version: TOPOLOGY_SCHEMA_VERSION,
            machines: vec![MachineSpec {
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
        name: "workspace".to_string(),
        workspace_key: workspace_key.to_string(),
        path_hint: Some(path_hint.to_string_lossy().into_owned()),
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
    let first = discover_git_workspace(&first_path).unwrap();
    assert!(first.git_dir.is_absolute());
    assert_eq!(
        std::fs::read_to_string(first.git_dir.join("vz/workspace-id")).unwrap(),
        first.workspace_key
    );

    let store = StateStore::in_memory().unwrap();
    let state = project_state(&first.workspace_key, &first.path_hint);
    store.save_project_state(&state).unwrap();

    let moved_path = fixture.move_worktree(&first_path, "moved");
    let moved = discover_git_workspace(&moved_path.join(".")).unwrap();
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
    let second = discover_git_workspace(&second_path).unwrap();
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

#[test]
fn concurrent_first_discovery_converges_on_one_token() {
    let fixture = GitFixture::new();
    let worktree = fixture.add_worktree("concurrent");
    let barrier = Arc::new(Barrier::new(9));
    let handles: Vec<_> = (0..8)
        .map(|_| {
            let worktree = worktree.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                barrier.wait();
                discover_git_workspace(&worktree).unwrap()
            })
        })
        .collect();
    barrier.wait();

    let discovered: Vec<_> = handles
        .into_iter()
        .map(|handle| handle.join().unwrap())
        .collect();
    assert_eq!(
        discovered
            .iter()
            .map(|workspace| workspace.workspace_key.as_str())
            .collect::<BTreeSet<_>>()
            .len(),
        1
    );
    let persisted = std::fs::read_to_string(discovered[0].git_dir.join("vz/workspace-id")).unwrap();
    assert_eq!(persisted, discovered[0].workspace_key);
}

#[test]
fn composition_retains_process_environment_id_above_workspace_binding() {
    let fixture = GitFixture::new();
    let worktree = fixture.add_worktree("selection-composition");
    let workspace = discover_git_workspace(&worktree).unwrap();
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
fn discovery_rejects_non_git_directories_and_invalid_persisted_tokens() {
    let outside = tempfile::tempdir().unwrap();
    assert!(discover_git_workspace(outside.path()).is_err());

    let fixture = GitFixture::new();
    let worktree = fixture.add_worktree("invalid-token");
    let discovered = discover_git_workspace(&worktree).unwrap();
    std::fs::write(discovered.git_dir.join("vz/workspace-id"), "not valid").unwrap();
    assert!(discover_git_workspace(&worktree).is_err());
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
