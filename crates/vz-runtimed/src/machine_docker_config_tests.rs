//! Offline storage tests: no Docker client, daemon, helper, or VM is launched.
use super::*;
use crate::machine_runtime_registry::{MachineRuntimeAdmission, MachineRuntimeRegistry};
use std::os::unix::fs::{DirBuilderExt, PermissionsExt, symlink};
use vz_runtime_contract::{EnvironmentId, MachineId, ProjectId};

fn fixture() -> Result<(tempfile::TempDir, Arc<MachineRuntimeStoreLease>)> {
    let root = tempfile::Builder::new()
        .prefix("vz-docker-config-")
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

fn create(store: &Arc<MachineRuntimeStoreLease>) -> Result<ManagedMachineDockerConfig> {
    ManagedMachineDockerConfig::ensure(Arc::clone(store), b"{}\n")
}

fn replace(file: &Path, bytes: &[u8]) -> Result<()> {
    let pending = file.with_extension("replacement");
    use std::os::unix::fs::OpenOptionsExt;
    let mut output = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&pending)?;
    output.write_all(bytes)?;
    output.sync_all()?;
    std::fs::rename(pending, file)?;
    Ok(())
}

#[test]
fn creates_private_owner_bound_config_and_reopens_without_resetting_credentials() -> Result<()> {
    let (_root, store) = fixture()?;
    assert!(ManagedMachineDockerConfig::open_existing(Arc::clone(&store))?.is_none());
    let managed = create(&store)?;
    assert_eq!(managed.path(), path(&store));
    assert_eq!(std::fs::metadata(managed.path())?.mode() & 0o7777, 0o700);
    for name in [CONFIG, CLAIM] {
        assert_eq!(
            std::fs::metadata(managed.path().join(name))?.mode() & 0o7777,
            0o600
        );
    }
    let identity = managed.claim.directory.clone();
    let nonce = managed.claim.nonce.clone();
    let credentials = br#"{"auths":{"private.invalid":{"auth":"private-canary"}}}"#;
    replace(&managed.path().join(CONFIG), credentials)?;
    managed.validate_current()?;
    assert_eq!(managed.read_config()?, credentials);
    drop(managed);
    let reopened = ManagedMachineDockerConfig::open_existing(Arc::clone(&store))?
        .context("existing private Docker config missing")?;
    assert_eq!(reopened.claim.directory, identity);
    assert_eq!(reopened.claim.nonce, nonce);
    assert_eq!(reopened.read_config()?, credentials);
    let ensured = ManagedMachineDockerConfig::ensure(store, b"{\"unused\":true}")?;
    assert_eq!(ensured.read_config()?, credentials);
    Ok(())
}

#[test]
fn rejects_unclaimed_existing_directory_without_adoption_or_repair() -> Result<()> {
    let (_root, store) = fixture()?;
    std::fs::DirBuilder::new()
        .mode(0o700)
        .create(path(&store))?;
    std::fs::write(path(&store).join("foreign"), b"preserved")?;
    assert!(create(&store).is_err());
    assert!(!path(&store).join(CLAIM).exists());
    assert!(!path(&store).join(CONFIG).exists());
    assert_eq!(std::fs::read(path(&store).join("foreign"))?, b"preserved");
    Ok(())
}

#[test]
fn second_machine_cannot_adopt_another_machines_directory() -> Result<()> {
    let (_first_root, first) = fixture()?;
    let (_second_root, second) = fixture()?;
    let managed = create(&first)?;
    std::fs::rename(managed.path(), path(&second))?;
    assert!(ManagedMachineDockerConfig::open_existing(second).is_err());
    assert!(managed.validate_current().is_err());
    Ok(())
}

#[test]
fn rejects_replacement_directory_even_with_copied_claim_and_config() -> Result<()> {
    let (root, store) = fixture()?;
    let managed = create(&store)?;
    std::fs::rename(managed.path(), root.path().join("held"))?;
    std::fs::DirBuilder::new()
        .mode(0o700)
        .create(managed.path())?;
    for name in [CONFIG, CLAIM] {
        std::fs::copy(
            root.path().join("held").join(name),
            managed.path().join(name),
        )?;
    }
    assert!(managed.validate_current().is_err());
    assert!(ManagedMachineDockerConfig::open_existing(store).is_err());
    Ok(())
}

#[test]
fn rejects_directory_symlink_and_public_permissions() -> Result<()> {
    let (root, store) = fixture()?;
    let target = root.path().join("foreign");
    std::fs::create_dir(&target)?;
    symlink(&target, path(&store))?;
    assert!(create(&store).is_err());
    std::fs::remove_file(path(&store))?;
    let managed = create(&store)?;
    for mode in [0o750, 0o770, 0o1700] {
        std::fs::set_permissions(managed.path(), std::fs::Permissions::from_mode(mode))?;
        assert!(managed.validate_current().is_err());
    }
    std::fs::set_permissions(managed.path(), std::fs::Permissions::from_mode(0o700))?;
    managed.validate_current()?;
    Ok(())
}

#[test]
fn rejects_missing_malformed_or_changed_claim_without_secret_diagnostics() -> Result<()> {
    for case in 0..5 {
        let (_root, store) = fixture()?;
        let managed = create(&store)?;
        let claim = managed.path().join(CLAIM);
        match case {
            0 => std::fs::remove_file(&claim)?,
            1 => replace(&claim, b"private-secret-invalid-json")?,
            2 => replace(&claim, &managed.claim_bytes)?,
            3 => {
                let mut changed = managed.claim.clone();
                changed.nonce = "lop_invalid".into();
                replace(&claim, &serde_json::to_vec(&changed)?)?;
            }
            _ => {
                let mut changed = managed.claim.clone();
                changed.schema_version = 2;
                replace(&claim, &serde_json::to_vec(&changed)?)?;
            }
        }
        let error = managed
            .validate_current()
            .err()
            .context("must reject changed claim")?;
        assert!(!format!("{error:#}").contains("private-secret"));
        if case != 2 {
            assert!(ManagedMachineDockerConfig::open_existing(store).is_err());
        }
    }
    Ok(())
}

#[test]
fn rejects_symlink_hardlink_fifo_and_directory_for_both_private_files() -> Result<()> {
    for name in [CONFIG, CLAIM] {
        for case in 0..4 {
            let (root, store) = fixture()?;
            let managed = create(&store)?;
            let file = managed.path().join(name);
            let external = root.path().join("external");
            std::fs::rename(&file, &external)?;
            match case {
                0 => symlink(&external, &file)?,
                1 => std::fs::hard_link(&external, &file)?,
                2 => {
                    // rustix does not expose mkfifoat on Apple platforms.
                    ensure!(
                        std::process::Command::new("/usr/bin/mkfifo")
                            .args(["-m", "600"])
                            .arg(&file)
                            .status()?
                            .success(),
                        "create private FIFO test fixture"
                    );
                }
                _ => std::fs::DirBuilder::new().mode(0o700).create(&file)?,
            }
            assert!(managed.validate_current().is_err());
            assert!(ManagedMachineDockerConfig::open_existing(store).is_err());
            assert!(external.exists());
        }
    }
    Ok(())
}

#[test]
fn rejects_oversized_malformed_nonobject_and_nonprivate_mutable_config() -> Result<()> {
    let (_root, store) = fixture()?;
    let managed = create(&store)?;
    let file = managed.path().join(CONFIG);
    for bytes in [
        vec![b' '; CONFIG_LIMIT as usize + 1],
        b"private-canary".to_vec(),
        b"[]".to_vec(),
    ] {
        replace(&file, &bytes)?;
        let error = managed
            .read_config()
            .err()
            .context("must reject invalid config")?;
        assert!(!format!("{error:#}").contains("private-canary"));
    }
    replace(&file, b"{}")?;
    for mode in [0o644, 0o660, 0o4600] {
        std::fs::set_permissions(&file, std::fs::Permissions::from_mode(mode))?;
        assert!(managed.validate_current().is_err());
    }
    Ok(())
}

#[test]
fn invalid_initial_config_creates_no_directory() -> Result<()> {
    let (_root, store) = fixture()?;
    for bytes in [
        b"secret-invalid".to_vec(),
        b"null".to_vec(),
        vec![b' '; CONFIG_LIMIT as usize + 1],
    ] {
        assert!(ManagedMachineDockerConfig::ensure(Arc::clone(&store), &bytes).is_err());
        assert!(!path(&store).exists());
    }
    Ok(())
}

#[test]
fn coherent_read_rejects_atomic_replacement_during_read() -> Result<()> {
    let (_root, store) = fixture()?;
    let managed = create(&store)?;
    assert!(
        read_regular_with_checkpoint(&managed.directory, CONFIG, CONFIG_LIMIT, || {
            replace(&managed.path().join(CONFIG), b"{\"auths\":{}}")
        })
        .is_err()
    );
    managed.validate_current()?;
    Ok(())
}

#[test]
fn coherent_read_rejects_in_place_mutation_during_read() -> Result<()> {
    let (_root, store) = fixture()?;
    let managed = create(&store)?;
    assert!(
        read_regular_with_checkpoint(&managed.directory, CONFIG, CONFIG_LIMIT, || {
            std::fs::write(managed.path().join(CONFIG), b"{\"auths\":{}}")?;
            Ok(())
        })
        .is_err()
    );
    managed.validate_current()?;
    Ok(())
}

#[test]
fn policy_read_rejects_replacement_between_content_read_and_final_identity_guard() -> Result<()> {
    for atomic in [false, true] {
        let (_root, store) = fixture()?;
        let managed = create(&store)?;
        let updated = b"{\"auths\":{}}";
        assert!(
            managed
                .read_config_with_checkpoint(|| {
                    if atomic {
                        replace(&managed.path().join(CONFIG), updated)
                    } else {
                        std::fs::write(managed.path().join(CONFIG), updated)?;
                        Ok(())
                    }
                })
                .is_err()
        );
        assert_eq!(managed.read_config()?, updated);
    }
    Ok(())
}

#[test]
fn recovery_reacquires_original_store_and_config_without_credential_reset() -> Result<()> {
    let (root, store) = fixture()?;
    let owner = store.owner().clone();
    let managed = create(&store)?;
    let claim_bytes = managed.claim_bytes.clone();
    let credentials = br#"{"auths":{"private.invalid":{"auth":"private-canary"}}}"#;
    replace(&managed.path().join(CONFIG), credentials)?;
    drop(managed);
    drop(store);
    let registry = MachineRuntimeRegistry::<()>::new(root.path().into())?;
    let recovered = registry.acquire_store(
        &owner,
        &MachineRuntimeRegistry::<()>::reservation(&owner)?,
        Some(&format!("sha256:{}", "a".repeat(64))),
        MachineRuntimeAdmission::ExistingOnly,
    )?;
    let reopened = ManagedMachineDockerConfig::open_existing(recovered)?
        .context("recovered private Docker config missing")?;
    assert_eq!(reopened.claim_bytes, claim_bytes);
    assert_eq!(reopened.read_config()?, credentials);
    Ok(())
}
