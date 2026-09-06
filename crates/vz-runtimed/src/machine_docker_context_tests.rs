//! Offline ownership tests; the opt-in native Docker case does not start a VM
//! or claim Engine availability, Developer readiness, or compatibility parity.
use super::*;
use crate::machine_runtime_registry::{MachineRuntimeAdmission, MachineRuntimeRegistry};
use serde_json::json;
use vz_runtime_contract::{
    EnvironmentId, EnvironmentLifecycleStatus, EnvironmentState, LifecycleStepStatus, MachineId,
    MachineLifecycleStep, MachineState, ProjectId,
};

fn delete_fixture() -> Result<(
    tempfile::TempDir,
    Arc<MachineRuntimeStoreLease>,
    ContextClaim,
    std::path::PathBuf,
)> {
    use std::os::unix::fs::DirBuilderExt;
    let (root, store) = fixture()?;
    let mut expected = claim(&store)?;
    expected.config_dir = root
        .path()
        .join("client")
        .to_str()
        .context("fixture path")?
        .into();
    expected.endpoint = format!("unix://{}", root.path().join("socket").display());
    let key = format!("{:x}", Sha256::digest(expected.name.as_bytes()));
    let directory = Path::new(&expected.config_dir)
        .join("contexts/meta")
        .join(key);
    std::fs::DirBuilder::new()
        .recursive(true)
        .mode(0o700)
        .create(&directory)?;
    std::fs::write(
        Path::new(&expected.config_dir).join("config.json"),
        b"{\"currentContext\":\"unrelated\"}\n",
    )?;
    std::fs::write(
        directory.join("meta.json"),
        serde_json::to_vec(&json!({"Name": expected.name,
        "Metadata": {"Description": format!("vz managed Machine context v1 {}", serde_json::to_string(&expected)?)},
        "Endpoints": {"docker": {"Host": expected.endpoint, "SkipTLSVerify": false}}}))?,
    )?;
    publish_claim(&store, &expected)?;
    Ok((root, store, expected, directory))
}

fn delete_operation(store: &MachineRuntimeStoreLease) -> EnvironmentLifecycleOperation {
    EnvironmentLifecycleOperation {
        schema_version: 1,
        operation_id: LifecycleOperationId::generate(),
        project_id: store.owner().project_id.clone(),
        environment_id: store.owner().environment_id.clone(),
        kind: EnvironmentLifecycleKind::Delete,
        generation: 2,
        request_id: "delete-test".into(),
        idempotency_key: "delete-test".into(),
        request_hash: format!("sha256:{}", "a".repeat(64)),
        definition_digest: format!("sha256:{}", "b".repeat(64)),
        initial_state: EnvironmentState::Stopped,
        requested_target: EnvironmentState::Deleted,
        status: EnvironmentLifecycleStatus::Running,
        machine_steps: vec![MachineLifecycleStep {
            machine_id: store.owner().machine_id.clone().expect("fixture machine"),
            initial_state: MachineState::Stopped,
            target_state: None,
            expected_incarnation: None,
            resulting_incarnation: None,
            resulting_activation: None,
            status: LifecycleStepStatus::Pending,
            failure_reason: None,
        }],
        cleanup_steps: vec![],
        created_at: 1,
        updated_at: 1,
        completed_at: None,
    }
}

fn prepare_delete(
    store: &Arc<MachineRuntimeStoreLease>,
    claim: &ContextClaim,
) -> Result<PreparedMachineDockerContextDelete> {
    ManagedMachineDockerContext::prepare_existing_delete(
        Arc::clone(store),
        None,
        Path::new(&claim.config_dir),
        Path::new(
            claim
                .endpoint
                .strip_prefix("unix://")
                .context("fixture endpoint")?,
        ),
    )?
    .context("fixture claim missing")
}

#[test]
fn context_delete_preserves_default_sibling_claim_and_replays_exact_operation() -> Result<()> {
    let (_root, store, claim, directory) = delete_fixture()?;
    let sibling = directory
        .parent()
        .context("parent")?
        .join("unrelated-context");
    std::fs::create_dir(&sibling)?;
    std::fs::write(sibling.join("meta.json"), b"foreign sentinel")?;
    let default = Path::new(&claim.config_dir).join("config.json");
    let before = std::fs::read(&default)?;
    let operation = delete_operation(&store);
    let mut prepared = prepare_delete(&store, &claim)?;
    assert!(
        !store.data_path().join(DELETE_INTENT).exists(),
        "prepare mutated store"
    );
    prepared.remove_exact(&operation)?;
    assert!(!directory.exists());
    assert_eq!(std::fs::read(&default)?, before);
    assert_eq!(
        std::fs::read(sibling.join("meta.json"))?,
        b"foreign sentinel"
    );
    assert_eq!(read_claim(&store)?, Some(claim.clone()));
    assert!(store.data_path().join(DELETE_INTENT).is_file());
    prepare_delete(&store, &claim)?.remove_exact(&operation)?;
    let mut other = operation.clone();
    other.operation_id = LifecycleOperationId::generate();
    assert!(
        prepare_delete(&store, &claim)?
            .remove_exact(&other)
            .is_err()
    );
    Ok(())
}

#[test]
fn context_delete_crash_after_metadata_unlink_replays_only_recorded_directory() -> Result<()> {
    let (_root, store, claim, directory) = delete_fixture()?;
    let operation = delete_operation(&store);
    let mut prepared = prepare_delete(&store, &claim)?;
    assert!(
        prepared
            .remove_with_checkpoint(&operation, || anyhow::bail!("simulated crash"))
            .is_err()
    );
    assert!(directory.is_dir());
    assert!(!directory.join("meta.json").exists());
    drop(prepared);
    prepare_delete(&store, &claim)?.remove_exact(&operation)?;
    assert!(!directory.exists());
    Ok(())
}

#[test]
fn context_delete_interruption_before_intent_publication_is_replayable() -> Result<()> {
    for partial in [false, true] {
        let (_root, store, claim, directory) = delete_fixture()?;
        let operation = delete_operation(&store);
        let default = Path::new(&claim.config_dir).join("config.json");
        let default_before = std::fs::read(&default)?;
        let metadata_before = std::fs::read(directory.join("meta.json"))?;
        let sibling = directory
            .parent()
            .context("context parent")?
            .join("sibling");
        std::fs::create_dir(&sibling)?;
        std::fs::write(sibling.join("meta.json"), b"foreign context")?;
        let mut prepared = prepare_delete(&store, &claim)?;
        let interrupted = prepared.remove_with_checkpoints(
            &operation,
            |file| {
                if partial {
                    // An interrupted write may leave any prefix in a pending
                    // file; it must never become the authoritative record.
                    file.set_len(7)?;
                    file.sync_all()?;
                }
                anyhow::bail!("injected interruption before atomic publication")
            },
            || anyhow::bail!("metadata must not be removed before publication"),
        );
        assert!(interrupted.is_err());
        assert!(prepared.previous.is_none());
        assert!(!store.data_path().join(DELETE_INTENT).exists());
        assert_eq!(std::fs::read(directory.join("meta.json"))?, metadata_before);
        assert_eq!(std::fs::read(&default)?, default_before);
        assert_eq!(
            std::fs::read(sibling.join("meta.json"))?,
            b"foreign context"
        );
        let pending = std::fs::read_dir(store.data_path())?
            .collect::<std::io::Result<Vec<_>>>()?
            .into_iter()
            .filter(|entry| {
                entry
                    .file_name()
                    .to_string_lossy()
                    .starts_with(&format!(".{DELETE_INTENT}.pending-"))
            })
            .collect::<Vec<_>>();
        assert_eq!(pending.len(), 1);
        let pending_path = pending[0].path();
        let pending_bytes = std::fs::read(&pending_path)?;
        let pending_identity = std::fs::symlink_metadata(&pending_path)?;
        assert_eq!(pending_identity.mode() & 0o777, 0o600);
        assert_eq!(pending_identity.nlink(), 1);
        if partial {
            assert_eq!(pending_bytes.len(), 7);
        }
        drop(prepared);
        prepare_delete(&store, &claim)?.remove_exact(&operation)?;
        require_deleted_for_store(&store, &operation)?;
        assert!(!directory.exists());
        assert_eq!(std::fs::read(&default)?, default_before);
        assert_eq!(
            std::fs::read(sibling.join("meta.json"))?,
            b"foreign context"
        );
        assert_eq!(std::fs::read(&pending_path)?, pending_bytes);
        assert_eq!(
            std::fs::symlink_metadata(&pending_path)?.ino(),
            pending_identity.ino()
        );
        // Only the exact runtime-tree deletion may later retire this orphan.
        prepare_delete(&store, &claim)?.remove_exact(&operation)?;
    }
    Ok(())
}

#[test]
fn context_delete_atomic_publication_preserves_unknown_final_contender() -> Result<()> {
    use std::os::unix::fs::OpenOptionsExt;
    let (_root, store, claim, directory) = delete_fixture()?;
    let operation = delete_operation(&store);
    let default = Path::new(&claim.config_dir).join("config.json");
    let default_before = std::fs::read(&default)?;
    let metadata_before = std::fs::read(directory.join("meta.json"))?;
    let final_path = store.data_path().join(DELETE_INTENT);
    let mut prepared = prepare_delete(&store, &claim)?;
    assert!(
        prepared
            .remove_with_checkpoints(
                &operation,
                |_| {
                    let mut contender = std::fs::OpenOptions::new()
                        .write(true)
                        .create_new(true)
                        .mode(0o600)
                        .open(&final_path)?;
                    contender.write_all(b"unknown final contender")?;
                    contender.sync_all()?;
                    Ok(())
                },
                || anyhow::bail!("must not unlink metadata after publication conflict"),
            )
            .is_err()
    );
    assert_eq!(std::fs::read(&final_path)?, b"unknown final contender");
    assert_eq!(std::fs::read(directory.join("meta.json"))?, metadata_before);
    assert_eq!(std::fs::read(&default)?, default_before);
    assert!(prepare_delete(&store, &claim).is_err());
    Ok(())
}

#[test]
fn context_delete_pending_content_change_cannot_be_published() -> Result<()> {
    let (_root, store, claim, directory) = delete_fixture()?;
    let metadata_before = std::fs::read(directory.join("meta.json"))?;
    assert!(
        prepare_delete(&store, &claim)?
            .remove_with_checkpoints(
                &delete_operation(&store),
                |file| {
                    file.set_len(3)?;
                    Ok(())
                },
                || anyhow::bail!("must not unlink metadata after changed pending record"),
            )
            .is_err()
    );
    assert!(!store.data_path().join(DELETE_INTENT).exists());
    assert_eq!(std::fs::read(directory.join("meta.json"))?, metadata_before);
    Ok(())
}

#[test]
fn context_delete_rejects_unclaimed_empty_and_tls_paths_without_mutation() -> Result<()> {
    for case in 0..4 {
        let (_root, store, claim, directory) = delete_fixture()?;
        match case {
            0 => std::fs::remove_file(store.data_path().join(CLAIM))?,
            1 => std::fs::remove_file(directory.join("meta.json"))?,
            2 => std::fs::write(directory.join("extra"), b"foreign")?,
            _ => {
                let tls = Path::new(&claim.config_dir)
                    .join("contexts/tls")
                    .join(directory.file_name().context("hash")?);
                std::fs::create_dir_all(tls)?;
            }
        }
        assert!(prepare_delete(&store, &claim).is_err(), "case {case}");
        assert!(directory.is_dir());
        assert!(!store.data_path().join(DELETE_INTENT).exists());
    }
    let (root, store) = fixture()?;
    assert!(
        ManagedMachineDockerContext::prepare_existing_delete(
            store,
            None,
            &root.path().join("absent-client"),
            &root.path().join("absent-socket")
        )?
        .is_none()
    );
    assert!(!root.path().join("absent-client").exists());
    Ok(())
}

#[test]
fn context_delete_rejects_prepared_claim_parent_and_metadata_replacements() -> Result<()> {
    for case in 0..4 {
        let (root, store, claim, directory) = delete_fixture()?;
        let mut prepared = prepare_delete(&store, &claim)?;
        let path = match case {
            0 => store.data_path().join(CLAIM),
            1 => directory.join("meta.json"),
            _ => directory.clone(),
        };
        let held = root.path().join("original");
        std::fs::rename(&path, &held)?;
        match case {
            0 | 1 => {
                std::fs::copy(&held, &path)?;
            }
            2 => {
                std::os::unix::fs::symlink(&held, &path)?;
            }
            _ => {
                std::fs::create_dir(&path)?;
                std::fs::copy(held.join("meta.json"), path.join("meta.json"))?;
            }
        }
        assert!(
            prepared.remove_exact(&delete_operation(&store)).is_err(),
            "case {case}"
        );
        assert!(std::fs::symlink_metadata(&path).is_ok());
        assert!(!store.data_path().join(DELETE_INTENT).exists());
    }
    Ok(())
}

#[test]
fn context_delete_rejects_wrong_operation_owner_kind_generation_and_hash() -> Result<()> {
    let (_root, store, claim, directory) = delete_fixture()?;
    for case in 0..5 {
        let mut operation = delete_operation(&store);
        match case {
            0 => operation.kind = EnvironmentLifecycleKind::Stop,
            1 => operation.environment_id = EnvironmentId::generate(),
            2 => operation.generation = 0,
            3 => operation.request_hash = "unbound".into(),
            _ => operation.machine_steps.clear(),
        }
        assert!(
            prepare_delete(&store, &claim)?
                .remove_exact(&operation)
                .is_err()
        );
        assert!(directory.join("meta.json").is_file());
        assert!(!store.data_path().join(DELETE_INTENT).exists());
    }
    Ok(())
}

#[test]
fn context_delete_rejects_claim_connection_nonce_and_descriptor_drift() -> Result<()> {
    for case in 0..5 {
        let (_root, store, expected, directory) = delete_fixture()?;
        let path = store.data_path().join(CLAIM);
        let mut changed = expected.clone();
        match case {
            0 => changed.owner.project_id = ProjectId::generate(),
            1 => changed.config_dir.push_str("-foreign"),
            2 => changed.endpoint.push_str("-foreign"),
            3 => changed.name.push_str("-foreign"),
            _ => changed.nonce = LifecycleOperationId::generate().to_string(),
        }
        std::fs::write(path, serde_json::to_vec(&changed)?)?;
        assert!(
            prepare_delete(&store, &expected).is_err(),
            "claim drift {case}"
        );
        assert!(directory.join("meta.json").is_file());
        assert!(!store.data_path().join(DELETE_INTENT).exists());
    }
    Ok(())
}

#[test]
fn context_delete_optional_published_descriptor_must_match_existing_claim() -> Result<()> {
    let (_root, store, claim, directory) = delete_fixture()?;
    let context = ManagedMachineDockerContext {
        claim: claim.clone(),
        store: Arc::clone(&store),
    };
    let descriptor = context.descriptor(
        &MachineIncarnation {
            schema_version: 1,
            incarnation_id: vz_runtime_contract::MachineIncarnationId::generate(),
            machine_id: store
                .owner()
                .machine_id
                .clone()
                .context("fixture Machine")?,
            generation: 1,
            created_at: 1,
        },
        "fixture-engine".into(),
    )?;
    let config = Path::new(&claim.config_dir);
    let socket = Path::new(claim.endpoint.strip_prefix("unix://").context("socket")?);
    assert!(
        ManagedMachineDockerContext::prepare_existing_delete(
            Arc::clone(&store),
            Some(&descriptor),
            config,
            socket
        )?
        .is_some()
    );
    for case in 0..4 {
        let mut changed = descriptor.clone();
        match case {
            0 => changed.owner.environment_id = EnvironmentId::generate(),
            1 => changed.name.push_str("-foreign"),
            2 => changed.config_dir.push_str("-foreign"),
            _ => changed.endpoint.push_str("-foreign"),
        }
        assert!(
            ManagedMachineDockerContext::prepare_existing_delete(
                Arc::clone(&store),
                Some(&changed),
                config,
                socket
            )
            .is_err()
        );
    }
    std::fs::remove_file(store.data_path().join(CLAIM))?;
    assert!(
        ManagedMachineDockerContext::prepare_existing_delete(
            Arc::clone(&store),
            Some(&descriptor),
            config,
            socket
        )
        .is_err()
    );
    assert!(directory.join("meta.json").exists());
    assert!(!store.data_path().join(DELETE_INTENT).exists());
    Ok(())
}

#[test]
fn context_delete_never_removes_replacement_after_partial_unlink() -> Result<()> {
    let (root, store, claim, directory) = delete_fixture()?;
    let operation = delete_operation(&store);
    let mut prepared = prepare_delete(&store, &claim)?;
    let held = root.path().join("original-context");
    assert!(
        prepared
            .remove_with_checkpoint(&operation, || {
                std::fs::rename(&directory, &held)?;
                std::fs::create_dir(&directory)?;
                std::fs::write(directory.join("foreign"), b"must survive")?;
                Ok(())
            })
            .is_err()
    );
    assert_eq!(std::fs::read(directory.join("foreign"))?, b"must survive");
    assert!(held.is_dir());
    assert!(prepare_delete(&store, &claim).is_err());
    Ok(())
}

#[test]
fn context_delete_rejects_linked_metadata_and_symlinked_shared_ancestry() -> Result<()> {
    for case in 0..3 {
        let (root, store, claim, directory) = delete_fixture()?;
        let foreign = root.path().join("foreign");
        match case {
            0 => std::fs::hard_link(directory.join("meta.json"), &foreign)?,
            1 => {
                std::fs::rename(directory.join("meta.json"), &foreign)?;
                std::os::unix::fs::symlink(&foreign, directory.join("meta.json"))?;
            }
            _ => {
                let parent = directory.parent().context("context parent")?;
                std::fs::rename(parent, &foreign)?;
                std::os::unix::fs::symlink(&foreign, parent)?;
            }
        }
        assert!(prepare_delete(&store, &claim).is_err());
        assert!(std::fs::symlink_metadata(&foreign).is_ok());
        assert!(!store.data_path().join(DELETE_INTENT).exists());
    }
    Ok(())
}

#[test]
fn context_delete_partial_replay_rejects_generation_hash_and_intent_replacement() -> Result<()> {
    let (root, store, claim, directory) = delete_fixture()?;
    let operation = delete_operation(&store);
    let mut prepared = prepare_delete(&store, &claim)?;
    assert!(
        prepared
            .remove_with_checkpoint(&operation, || anyhow::bail!("simulated crash"))
            .is_err()
    );
    for case in 0..2 {
        let mut changed = operation.clone();
        if case == 0 {
            changed.generation += 1;
        } else {
            changed.request_hash = format!("sha256:{}", "c".repeat(64));
        }
        assert!(
            prepare_delete(&store, &claim)?
                .remove_exact(&changed)
                .is_err()
        );
        assert!(directory.is_dir());
    }
    let mut resumed = prepare_delete(&store, &claim)?;
    let intent = store.data_path().join(DELETE_INTENT);
    let held = root.path().join("old-intent");
    std::fs::rename(&intent, &held)?;
    std::fs::copy(&held, &intent)?;
    assert!(resumed.remove_exact(&operation).is_err());
    assert!(directory.is_dir());
    Ok(())
}

#[test]
fn store_delete_requires_completed_context_removal_and_same_operation() -> Result<()> {
    let (_root, store, claim, directory) = delete_fixture()?;
    let operation = delete_operation(&store);
    assert!(require_deleted_for_store(&store, &operation).is_err());
    let mut prepared = prepare_delete(&store, &claim)?;
    assert!(
        prepared
            .remove_with_checkpoint(&operation, || anyhow::bail!("interrupted"))
            .is_err()
    );
    assert!(
        require_deleted_for_store(&store, &operation).is_err(),
        "empty retained directory is incomplete deletion"
    );
    prepare_delete(&store, &claim)?.remove_exact(&operation)?;
    require_deleted_for_store(&store, &operation)?;
    let mut other = operation.clone();
    other.operation_id = LifecycleOperationId::generate();
    assert!(require_deleted_for_store(&store, &other).is_err());
    std::fs::create_dir(&directory)?;
    std::fs::write(directory.join("meta.json"), b"foreign replacement")?;
    assert!(require_deleted_for_store(&store, &operation).is_err());
    assert_eq!(
        std::fs::read(directory.join("meta.json"))?,
        b"foreign replacement"
    );
    Ok(())
}

#[test]
fn store_delete_without_claim_rejects_orphaned_intent_without_mutation() -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    let (_root, store) = fixture()?;
    let operation = delete_operation(&store);
    require_deleted_for_store(&store, &operation)?;
    let path = store.data_path().join(DELETE_INTENT);
    assert!(!path.exists());
    std::fs::write(&path, b"orphaned intent")?;
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))?;
    assert!(require_deleted_for_store(&store, &operation).is_err());
    assert_eq!(std::fs::read(path)?, b"orphaned intent");
    Ok(())
}

fn fixture() -> Result<(tempfile::TempDir, Arc<MachineRuntimeStoreLease>)> {
    let root = tempfile::Builder::new()
        .prefix("vz-context-")
        .tempdir_in("/private/tmp")?;
    let registry = MachineRuntimeRegistry::<()>::new(root.path().into())?;
    let owner = ResourceOwner {
        project_id: ProjectId::generate(),
        environment_id: EnvironmentId::generate(),
        machine_id: Some(MachineId::generate()),
    };
    let store = registry.acquire_store(
        &owner,
        &MachineRuntimeRegistry::<()>::reservation(&owner)?,
        Some(&format!("sha256:{}", "a".repeat(64))),
        MachineRuntimeAdmission::CreateOrOpen,
    )?;
    Ok((root, store))
}

fn claim(store: &MachineRuntimeStoreLease) -> Result<ContextClaim> {
    Ok(ContextClaim {
        schema_version: 1,
        owner: store.owner().clone(),
        name: store.owner().bounded_resource_name(
            &OwnedResourceKind::DockerContext,
            "docker",
            64,
        )?,
        endpoint: "unix:///private/tmp/owned-unused.sock".into(),
        config_dir: "/private/tmp/owned-client".into(),
        nonce: LifecycleOperationId::generate().to_string(),
    })
}

#[test]
fn exact_claim_is_durable_and_never_overwritten() -> Result<()> {
    let (_root, store) = fixture()?;
    let expected = claim(&store)?;
    assert!(read_claim(&store)?.is_none());
    publish_claim(&store, &expected)?;
    assert_eq!(read_claim(&store)?, Some(expected.clone()));
    let mut foreign = expected.clone();
    foreign.owner.environment_id = EnvironmentId::generate();
    assert!(publish_claim(&store, &foreign).is_err());
    assert_eq!(read_claim(&store)?, Some(expected));
    Ok(())
}

#[test]
fn legacy_context_refuses_private_config_without_creating_or_rewriting_state() -> Result<()> {
    let (_root, store) = fixture()?;
    let expected = claim(&store)?;
    publish_claim(&store, &expected)?;
    let claim_path = store.data_path().join(CLAIM);
    let before = std::fs::read(&claim_path)?;
    let private_path = crate::machine_docker_config::path(&store);
    assert!(!private_path.exists());
    let error = ManagedMachineDockerContext::require_private_config_compatible(&store)
        .expect_err("legacy shared claim must require explicit migration");
    assert!(error.to_string().contains("explicit migration"));
    // Exercise the production ordering too: executable admission may read the
    // trusted binary, but legacy refusal precedes private-config publication.
    assert!(
        HostDockerClient::for_machine(Path::new("/usr/bin/true"), &[], Arc::clone(&store)).is_err()
    );
    assert!(!private_path.exists());
    assert_eq!(std::fs::read(&claim_path)?, before);
    assert_eq!(read_claim(&store)?, Some(expected));
    assert!(std::fs::read_dir(store.data_path())?.all(|entry| {
        entry.is_ok_and(|entry| {
            !entry
                .file_name()
                .to_string_lossy()
                .starts_with(".docker-client.pending-")
        })
    }));
    Ok(())
}

#[test]
fn exact_private_context_claim_is_compatible_without_creating_configuration() -> Result<()> {
    let (_root, store) = fixture()?;
    // Claim-free admission remains read-only and permits the separate creator.
    ManagedMachineDockerContext::require_private_config_compatible(&store)?;
    let private_path = crate::machine_docker_config::path(&store);
    assert!(!private_path.exists());
    let mut expected = claim(&store)?;
    expected.config_dir = private_path.to_str().context("UTF-8 fixture path")?.into();
    publish_claim(&store, &expected)?;
    let claim_path = store.data_path().join(CLAIM);
    let before = std::fs::read(&claim_path)?;
    ManagedMachineDockerContext::require_private_config_compatible(&store)?;
    assert_eq!(std::fs::read(&claim_path)?, before);
    assert!(
        !private_path.exists(),
        "compatibility check must not create config"
    );
    assert_eq!(read_claim(&store)?, Some(expected));
    Ok(())
}

#[test]
fn machine_client_reopen_preserves_atomic_auth_update_and_rejects_plugin_drift() -> Result<()> {
    let (root, store) = fixture()?;
    let plugin_a = root.path().join("plugins-a");
    let plugin_b = root.path().join("plugins-b");
    std::fs::create_dir(&plugin_a)?;
    std::fs::create_dir(&plugin_b)?;
    let plugins = vec![plugin_a.canonicalize()?];
    let client =
        HostDockerClient::for_machine(Path::new("/usr/bin/true"), &plugins, Arc::clone(&store))?;
    let path = crate::machine_docker_config::path(&store);
    assert_eq!(client.config_dir(), path);
    let config_path = path.join("config.json");
    let owner_path = path.join("vz-owner.json");
    let owner_before = std::fs::read(&owner_path)?;
    let mut value: Value = serde_json::from_slice(&std::fs::read(&config_path)?)?;
    value["auths"] = json!({"registry.example.invalid": {"auth": "opaque-private-test-auth"}});
    let replacement = serde_json::to_vec_pretty(&value)?;
    // Docker's Save replaces the config atomically; ownership must be bound to
    // the stable directory/claim, not the old mutable credential-file inode.
    let mut temporary = tempfile::NamedTempFile::new_in(&path)?;
    temporary.write_all(&replacement)?;
    temporary.as_file().sync_all()?;
    temporary.persist(&config_path)?;
    let reopened =
        HostDockerClient::for_machine(Path::new("/usr/bin/true"), &plugins, Arc::clone(&store))?;
    assert_eq!(reopened.config_dir(), path);
    assert_eq!(std::fs::read(&config_path)?, replacement);
    assert_eq!(std::fs::read(&owner_path)?, owner_before);
    let changed = HostDockerClient::for_machine(
        Path::new("/usr/bin/true"),
        &[plugin_b.canonicalize()?],
        Arc::clone(&store),
    );
    assert!(changed.is_err());
    assert_eq!(std::fs::read(&config_path)?, replacement);
    assert_eq!(std::fs::read(&owner_path)?, owner_before);
    Ok(())
}

#[test]
fn malformed_and_linked_claims_are_not_repaired() -> Result<()> {
    let (root, store) = fixture()?;
    let path = store.data_path().join(CLAIM);
    std::os::unix::fs::symlink(root.path().join("missing"), &path)?;
    assert!(read_claim(&store).is_err());
    assert!(publish_claim(&store, &claim(&store)?).is_err());
    std::fs::remove_file(&path)?;
    publish_claim(&store, &claim(&store)?)?;
    let decoy = root.path().join("linked-claim");
    std::fs::hard_link(&path, &decoy)?;
    assert!(read_claim(&store).is_err());
    Ok(())
}

#[test]
fn context_inspection_requires_exact_nonce_owner_endpoint_and_no_credentials() -> Result<()> {
    let (_root, store) = fixture()?;
    let context = ManagedMachineDockerContext {
        claim: claim(&store)?,
        store,
    };
    let exact = json!([{"Name":context.claim.name,"Metadata":{"Description":context.description()?},
        "Endpoints":{"docker":{"Host":context.claim.endpoint,"SkipTLSVerify":false}},"TLSMaterial":{}}]);
    context.verify_inspection(&serde_json::to_vec(&exact)?)?;
    for pointer in [
        "/0/Name",
        "/0/Metadata/Description",
        "/0/Endpoints/docker/Host",
        "/0/Endpoints/docker/SkipTLSVerify",
        "/0/TLSMaterial",
    ] {
        let mut changed = exact.clone();
        *changed.pointer_mut(pointer).context("fixture pointer")? = json!("foreign");
        assert!(
            context
                .verify_inspection(&serde_json::to_vec(&changed)?)
                .is_err()
        );
    }
    let mut extra = exact.clone();
    extra[0]["Endpoints"]["foreign"] = json!({});
    assert!(
        context
            .verify_inspection(&serde_json::to_vec(&extra)?)
            .is_err()
    );
    Ok(())
}

#[tokio::test]
#[ignore = "requires explicit actual host Docker client; offline context operations only"]
async fn actual_host_contexts_are_stable_owned_and_preserve_default() -> Result<()> {
    let executable =
        std::env::var_os("VZ_TEST_HOST_DOCKER").context("explicit host Docker client required")?;
    ensure!(
        Path::new(&executable) == Path::new("/usr/local/bin/docker"),
        "this native regression requires the actual /usr/local/bin/docker installation path"
    );
    let (root, store) = fixture()?;
    let config = root.path().join("docker-client");
    let client = HostDockerClient::new(Path::new(&executable), &config)?;
    println!(
        "{}",
        json!({"phase":"offline_host_client","requested_executable":executable,"canonical_executable":client.executable(),"sha256":client.executable_sha256(),"argv0":"docker","isolated_config":config,"engine_contact_allowed":false})
    );
    let version = client
        .run(None, &["--version".into()], None, Duration::from_secs(10))
        .await?
        .success()?;
    println!(
        "{}",
        json!({"phase":"version","args":["--config",config.to_string_lossy().as_ref(),"--context","default","--version"],"exit_code":version.status.code(),"stdout":String::from_utf8_lossy(&version.stdout),"stderr":String::from_utf8_lossy(&version.stderr)})
    );
    ensure!(
        std::str::from_utf8(&version.stdout)?.starts_with("Docker version ")
            && version.stderr.is_empty()
    );
    let default_bytes = b"{\"currentContext\":\"unrelated-default\"}\n";
    std::fs::write(config.join("config.json"), default_bytes)?;
    let socket = root.path().join("owned-unused.sock");
    // A listener without an Engine makes an accidental connection observable;
    // context metadata operations must not contact even this owned endpoint.
    let no_engine = std::os::unix::net::UnixListener::bind(&socket)?;
    no_engine.set_nonblocking(true)?;
    let first = ManagedMachineDockerContext::ensure(&client, Arc::clone(&store), &socket).await?;
    let second = ManagedMachineDockerContext::ensure(&client, Arc::clone(&store), &socket).await?;
    assert_eq!(first.claim, second.claim);
    first.verify(&client).await?;
    let inspection = client
        .run(
            None,
            &["context".into(), "inspect".into(), first.claim.name.clone()],
            None,
            Duration::from_secs(10),
        )
        .await?
        .success()?;
    println!(
        "{}",
        json!({"phase":"owned_context_inspection","context":first.claim.name,"exit_code":inspection.status.code(),"stdout":String::from_utf8_lossy(&inspection.stdout),"stderr":String::from_utf8_lossy(&inspection.stderr)})
    );
    assert_eq!(std::fs::read(config.join("config.json"))?, default_bytes);
    // No Engine contact is allowed without an explicit non-default context.
    assert!(
        client
            .run(None, &["info".into()], None, Duration::from_secs(1))
            .await
            .is_err()
    );
    assert!(
        client
            .run(
                Some("default"),
                &["info".into()],
                None,
                Duration::from_secs(1)
            )
            .await
            .is_err()
    );
    assert!(
        client
            .run(Some(""), &["info".into()], None, Duration::from_secs(1))
            .await
            .is_err()
    );
    let (foreign_root, foreign_store) = fixture()?;
    let foreign_socket = foreign_root.path().join("unused.sock");
    let no_foreign_engine = std::os::unix::net::UnixListener::bind(&foreign_socket)?;
    no_foreign_engine.set_nonblocking(true)?;
    let foreign_name = foreign_store.owner().bounded_resource_name(
        &OwnedResourceKind::DockerContext,
        "docker",
        64,
    )?;
    client
        .run(
            None,
            &[
                "context".into(),
                "create".into(),
                "--description".into(),
                "not owned by vz".into(),
                "--docker".into(),
                format!("host=unix://{}", foreign_socket.display()),
                foreign_name.clone(),
            ],
            None,
            Duration::from_secs(10),
        )
        .await?
        .success()?;
    let before = client
        .run(
            None,
            &["context".into(), "inspect".into(), foreign_name.clone()],
            None,
            Duration::from_secs(10),
        )
        .await?
        .success()?
        .stdout;
    assert!(
        ManagedMachineDockerContext::ensure(&client, foreign_store, &foreign_socket)
            .await
            .is_err()
    );
    let after = client
        .run(
            None,
            &["context".into(), "inspect".into(), foreign_name.clone()],
            None,
            Duration::from_secs(10),
        )
        .await?
        .success()?
        .stdout;
    assert_eq!(before, after);
    let operation = delete_operation(&store);
    let mut prepared = ManagedMachineDockerContext::prepare_existing_delete(
        Arc::clone(&store),
        None,
        &config,
        &socket,
    )?
    .context("owned host context must have an exact Delete claim")?;
    prepared.remove_exact(&operation)?;
    let removed = client
        .run(
            None,
            &["context".into(), "inspect".into(), first.claim.name.clone()],
            None,
            Duration::from_secs(10),
        )
        .await?;
    ensure!(
        !removed.status.success(),
        "actual host client still resolves deleted context"
    );
    let foreign_after_delete = client
        .run(
            None,
            &["context".into(), "inspect".into(), foreign_name],
            None,
            Duration::from_secs(10),
        )
        .await?
        .success()?
        .stdout;
    assert_eq!(before, foreign_after_delete);
    require_deleted_for_store(&store, &operation)?;
    assert_eq!(std::fs::read(config.join("config.json"))?, default_bytes);
    ensure!(
        matches!(no_engine.accept(),Err(error) if error.kind()==std::io::ErrorKind::WouldBlock),
        "offline context operation contacted selected endpoint"
    );
    ensure!(
        matches!(no_foreign_engine.accept(),Err(error) if error.kind()==std::io::ErrorKind::WouldBlock),
        "offline context operation contacted foreign endpoint"
    );
    println!(
        "{}",
        json!({"phase":"offline_result","owned_claim_stable":true,"owned_context_deleted":true,"foreign_context_unchanged":true,"default_config_exact_bytes":String::from_utf8_lossy(default_bytes),"engine_connections":0,"vm_started":false,"readiness_or_parity_certified":false})
    );
    Ok(())
}

#[tokio::test]
#[ignore = "requires explicit actual host Docker client; Machine-private offline context operations only"]
async fn actual_host_private_configs_keep_context_and_credentials_separate() -> Result<()> {
    let executable =
        std::env::var_os("VZ_TEST_HOST_DOCKER").context("explicit host Docker client required")?;
    ensure!(Path::new(&executable) == Path::new("/usr/local/bin/docker"));
    let (first_root, first_store) = fixture()?;
    let (second_root, second_store) = fixture()?;
    let first =
        HostDockerClient::for_machine(Path::new(&executable), &[], Arc::clone(&first_store))?;
    let second =
        HostDockerClient::for_machine(Path::new(&executable), &[], Arc::clone(&second_store))?;
    ensure!(first.config_dir() != second.config_dir());
    let first_socket = first_root.path().join("unused.sock");
    let second_socket = second_root.path().join("unused.sock");
    let listeners = [
        std::os::unix::net::UnixListener::bind(&first_socket)?,
        std::os::unix::net::UnixListener::bind(&second_socket)?,
    ];
    for listener in &listeners {
        listener.set_nonblocking(true)?;
    }
    let first_context =
        ManagedMachineDockerContext::ensure(&first, Arc::clone(&first_store), &first_socket)
            .await?;
    let second_context =
        ManagedMachineDockerContext::ensure(&second, Arc::clone(&second_store), &second_socket)
            .await?;
    first_context.verify(&first).await?;
    second_context.verify(&second).await?;
    let peer = first
        .run(
            None,
            &[
                "context".into(),
                "inspect".into(),
                second_context.name().into(),
            ],
            None,
            Duration::from_secs(10),
        )
        .await?;
    ensure!(
        !peer.status.success(),
        "private config resolved another Machine context"
    );
    let first_before = std::fs::read(first.config_dir().join("config.json"))?;
    let second_before = std::fs::read(second.config_dir().join("config.json"))?;
    let reopened =
        HostDockerClient::for_machine(Path::new(&executable), &[], Arc::clone(&first_store))?;
    first_context.verify(&reopened).await?;
    ManagedMachineDockerContext::prepare_existing_delete(
        Arc::clone(&first_store),
        None,
        first.config_dir(),
        &first_socket,
    )?
    .context("private context claim missing")?
    .remove_exact(&delete_operation(&first_store))?;
    second_context.verify(&second).await?;
    ensure!(std::fs::read(first.config_dir().join("config.json"))? == first_before);
    ensure!(std::fs::read(second.config_dir().join("config.json"))? == second_before);
    for listener in &listeners {
        ensure!(
            matches!(listener.accept(), Err(error) if error.kind() == std::io::ErrorKind::WouldBlock),
            "offline context operation contacted an Engine endpoint"
        );
    }
    println!(
        "Machine-private native context creation/reopen/isolation/removal PASS; no Engine contact or registry authentication claimed"
    );
    Ok(())
}

#[tokio::test]
async fn expired_startup_cannot_publish_a_context_claim_or_dispatch() -> Result<()> {
    let (root, store) = fixture()?;
    // The executable is never dispatched: expiry must precede claim admission.
    let client = HostDockerClient::new(Path::new("/usr/bin/true"), &root.path().join("client"))?;
    assert!(
        ManagedMachineDockerContext::ensure_before(
            &client,
            Arc::clone(&store),
            &root.path().join("unused.sock"),
            tokio::time::Instant::now()
        )
        .await
        .is_err()
    );
    assert!(read_claim(&store)?.is_none());
    assert!(!client.config_dir().join("contexts").exists());
    Ok(())
}
