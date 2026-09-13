#![allow(clippy::expect_used)]

use super::*;

fn journal_fixture() -> Result<(tempfile::TempDir, Arc<MachineRuntimeStoreLease>, Journal)> {
    use crate::machine_runtime_registry::{MachineRuntimeAdmission, MachineRuntimeRegistry};
    use vz_runtime_contract::{EnvironmentId, MachineId, MachineIncarnationId, ProjectId};
    let root = tempfile::Builder::new()
        .prefix("vz-probe-journal-")
        .tempdir_in("/private/tmp")?;
    let registry = MachineRuntimeRegistry::<()>::new(root.path().into())?;
    let machine_id = MachineId::generate();
    let owner = ResourceOwner {
        project_id: ProjectId::generate(),
        environment_id: EnvironmentId::generate(),
        machine_id: Some(machine_id.clone()),
    };
    let store = registry.acquire_store(
        &owner,
        &MachineRuntimeRegistry::<()>::reservation(&owner)?,
        Some(&format!("sha256:{}", "a".repeat(64))),
        MachineRuntimeAdmission::CreateOrOpen,
    )?;
    let journal = Journal {
        schema_version: 1,
        owner,
        incarnation: MachineIncarnation {
            schema_version: 1,
            machine_id,
            incarnation_id: MachineIncarnationId::new("inc_probe")?,
            generation: 1,
            created_at: 1,
        },
        configuration_digest: store.configuration_digest().into(),
        context: "exact-context".into(),
        token: "vzprobe-token".into(),
        state: "running".into(),
        archive_sha256: "b".repeat(64),
        client_sha256: "c".repeat(64),
        commands: vec![],
        resources: json!({}),
        failure: None,
    };
    Ok((root, store, journal))
}

fn readonly_fixture() -> Result<(tempfile::TempDir, Arc<MachineRuntimeStoreLease>, Journal)> {
    let (root, store, mut journal) = journal_fixture()?;
    journal.token = format!("vzprobe-{}", "a".repeat(24));
    let directory = store.data_path().join(&journal.token);
    std::fs::DirBuilder::new().mode(0o700).create(&directory)?;
    journal.resources = json!({"rootfs_tag":format!("{}:rootfs",journal.token), "build_tag":format!("{}:built",journal.token),
        "engine_container":format!("{}-engine",journal.token), "build_container":format!("{}-build",journal.token),
        "compose_project":format!("{}-compose",journal.token), "compose_container":format!("{}-compose-service",journal.token),
        "directory":directory,"cleanup_scope":"disposable_probe_containers_compose_objects_and_images","retained_buildkit_cache":true});
    journal.state = "failed_recovery_required".into();
    journal.failure = Some("Compose plugin unavailable".into());
    write_new(&directory.join("command-000.stdout"), b"")?;
    write_new(&directory.join("command-000.stderr"), b"missing plugin\n")?;
    journal.commands.push(json!({"args":["compose","version"],"mutation":false,"state":"returned","exit_code":1,
        "stdout":"command-000.stdout","stderr":"command-000.stderr","stdout_sha256":hash(b""),"stderr_sha256":hash(b"missing plugin\n")}));
    Ok((root, store, journal))
}

#[test]
fn pre_effect_client_failure_can_be_reprobed_without_requiring_old_client_selection() -> Result<()>
{
    let (_root, store, mut journal) = readonly_fixture()?;
    let mut current = journal.incarnation.clone();
    current.created_at += 50; // Failed first Up has not published this diagnostic timestamp.
    validate_no_mutation(&journal, &store, &current, &journal.context)?;
    journal.client_sha256 = "e".repeat(64);
    journal.archive_sha256 = "f".repeat(64);
    validate_no_mutation(&journal, &store, &current, &journal.context)?;
    journal.commands = vec![
        json!({"args":["info","--format","{{json .}}"],"mutation":false,"state":"transport_uncertain"}),
    ];
    validate_no_mutation(&journal, &store, &current, &journal.context)?;
    journal.commands.clear();
    validate_no_mutation(&journal, &store, &current, &journal.context)?;
    Ok(())
}

#[test]
fn mutation_intent_forged_readonly_flag_incomplete_receipts_and_acquired_ids_block_retry()
-> Result<()> {
    let (_root, store, journal) = readonly_fixture()?;
    let valid = serde_json::to_value(&journal)?;
    for (pointer, value) in [
        ("/commands/0/mutation", json!(true)),
        ("/commands/0/args", json!(["create", "container"])),
        (
            "/commands/0/args",
            json!(["buildx", "inspect", "exact-context", "--bootstrap"]),
        ),
        ("/commands/0/state", json!("unknown")),
        ("/commands/0/stdout", json!("../foreign")),
        ("/commands/0/stderr_sha256", json!("0".repeat(64))),
        (
            "/configuration_digest",
            json!(format!("sha256:{}", "b".repeat(64))),
        ),
        ("/incarnation/generation", json!(2)),
        ("/context", json!("foreign-context")),
    ] {
        let mut changed = valid.clone();
        *changed.pointer_mut(pointer).context("mutation pointer")? = value;
        let changed: Journal = serde_json::from_value(changed)?;
        assert!(
            validate_no_mutation(&changed, &store, &journal.incarnation, &journal.context).is_err(),
            "accepted {pointer}"
        );
    }
    let mut changed = journal.clone();
    changed.resources["rootfs_id"] = json!(format!("sha256:{}", "a".repeat(64)));
    assert!(
        validate_no_mutation(&changed, &store, &journal.incarnation, &journal.context).is_err()
    );
    let mut changed = journal.clone();
    changed.commands[0]
        .as_object_mut()
        .context("command")?
        .remove("stdout_sha256");
    assert!(
        validate_no_mutation(&changed, &store, &journal.incarnation, &journal.context).is_err()
    );
    let mut changed = journal.clone();
    changed.owner.environment_id = vz_runtime_contract::EnvironmentId::generate();
    assert!(
        validate_no_mutation(&changed, &store, &journal.incarnation, &journal.context).is_err()
    );
    let mut changed = journal.clone();
    changed.commands =
        vec![json!({"args":["image","import","-","owned"],"mutation":true,"state":"admitted"})];
    assert!(
        validate_no_mutation(&changed, &store, &journal.incarnation, &journal.context).is_err()
    );
    Ok(())
}

#[test]
fn failed_no_mutation_attempt_is_immutably_archived_before_supersession() -> Result<()> {
    let (_root, store, mut journal) = readonly_fixture()?;
    validate_no_mutation(&journal, &store, &journal.incarnation, &journal.context)?;
    let receipt = archive_no_mutation(&journal, &store)?;
    assert_eq!(archive_no_mutation(&journal, &store)?, receipt); // Crash after archive, before replacement.
    let path = PathBuf::from(receipt["path"].as_str().context("path")?);
    let original = read_private(&path, 1024 * 1024)?;
    assert_eq!(receipt["sha256"], hash(&original));
    journal.failure = Some("different failure must not replace original".into());
    assert!(archive_no_mutation(&journal, &store).is_err());
    assert_eq!(read_private(&path, 1024 * 1024)?, original);
    Ok(())
}

#[test]
fn redirected_failure_archive_is_never_adopted() -> Result<()> {
    let (root, store, journal) = readonly_fixture()?;
    let target = root.path().join("foreign");
    write_new(&target, b"preserve")?;
    let path = store
        .data_path()
        .join(&journal.token)
        .join("no-mutation-failure.json");
    std::os::unix::fs::symlink(&target, &path)?;
    assert!(archive_no_mutation(&journal, &store).is_err());
    assert_eq!(std::fs::read(target)?, b"preserve");
    Ok(())
}

#[test]
fn durable_pending_journal_blocks_retry_and_foreign_completed_state_is_not_adopted() -> Result<()> {
    let (_root, store, mut journal) = journal_fixture()?;
    assert!(read_journal(&store)?.is_none());
    journal
        .commands
        .push(json!({"args":["create","exact-name"],"mutation":true,"state":"admitted"}));
    write_new(
        &store.data_path().join(JOURNAL),
        &serde_json::to_vec(&journal)?,
    )?;
    let retained = read_journal(&store)?.context("journal missing")?;
    assert!(validate_previous(&retained, store.owner()).is_err());
    assert_eq!(retained.commands[0]["state"], "admitted");
    journal.state = "failed_recovery_required".into();
    assert!(validate_previous(&journal, store.owner()).is_err());
    journal.state = "completed".into();
    validate_previous(&journal, store.owner())?;
    journal.owner.environment_id = vz_runtime_contract::EnvironmentId::generate();
    assert!(validate_previous(&journal, store.owner()).is_err());
    Ok(())
}

#[test]
fn journal_links_and_duplicate_probe_lock_are_rejected_receipts_are_create_only() -> Result<()> {
    let (root, store, _) = journal_fixture()?;
    let first = open_owned(&store, "docker-operational-probe.lock", true)?;
    fs2::FileExt::try_lock_exclusive(&first)?;
    let second = open_owned(&store, "docker-operational-probe.lock", true)?;
    assert!(fs2::FileExt::try_lock_exclusive(&second).is_err());
    let path = store.data_path().join(JOURNAL);
    std::os::unix::fs::symlink(root.path().join("missing"), &path)?;
    assert!(read_journal(&store).is_err());
    let receipt = store.data_path().join("immutable-receipt.json");
    write_new(&receipt, b"original")?;
    assert!(write_new(&receipt, b"replacement").is_err());
    assert_eq!(std::fs::read(receipt)?, b"original");
    Ok(())
}

#[test]
fn absence_requires_exact_not_found_not_substring_or_transport_failure() {
    use std::os::unix::process::ExitStatusExt;
    let id = "a".repeat(64);
    let output = HostDockerOutput {
        status: std::process::ExitStatus::from_raw(256),
        stdout: b"[]\n".to_vec(),
        stderr: format!("Error: No such object: {id}\n").into_bytes(),
    };
    verify_absence(&output, "container", &id).expect("exact absence");
    let changed = HostDockerOutput {
        stderr: format!("Error: No such object: {id}extra\n").into_bytes(),
        ..output
    };
    assert!(verify_absence(&changed, "container", &id).is_err());
    let changed = HostDockerOutput {
        stderr: format!("Error: No such object: {id}\nadditional fatal diagnostic\n").into_bytes(),
        ..changed
    };
    assert!(verify_absence(&changed, "container", &id).is_err());
}

#[test]
fn immutable_ids_reject_partial_mutable_and_multiple_values() {
    let hex = "a".repeat(64);
    assert_eq!(
        container_id(format!("{hex}\n").as_bytes()).expect("ID"),
        hex
    );
    assert!(image_id(format!("sha256:{hex}\n").as_bytes()).is_ok());
    for raw in [
        "latest".to_string(),
        "a".repeat(12),
        "A".repeat(64),
        format!("{hex}\n{hex}"),
    ] {
        assert!(container_id(raw.as_bytes()).is_err());
        assert!(image_id(raw.as_bytes()).is_err());
    }
}

#[test]
fn engine_identity_rejects_other_targets_and_runtime_fallbacks() {
    let valid = json!({"ID":"exact-engine","OSType":"linux","Architecture":"aarch64","DefaultRuntime":"youki","Runtimes":{"youki":{"path":"/mnt/linux-bin/youki"},"io.containerd.youki.v2":{}}});
    assert_eq!(
        engine_identity(&serde_json::to_vec(&valid).expect("JSON")).expect("Engine"),
        "exact-engine"
    );
    for (field, value) in [
        ("ID", json!("")),
        ("OSType", json!("windows")),
        ("Architecture", json!("x86_64")),
        ("DefaultRuntime", json!("runc")),
        ("Runtimes", json!({"youki":{},"runc":{}})),
    ] {
        let mut changed = valid.clone();
        changed[field] = value;
        assert!(engine_identity(&serde_json::to_vec(&changed).expect("JSON")).is_err());
    }
}

#[test]
fn stock_metadata_is_not_confused_with_installed_runtime_authority() {
    let row = json!({"ID":"engine","OSType":"linux","Architecture":"arm64","DefaultRuntime":"youki", "Runtimes":{"youki":{"path":"/mnt/linux-bin/youki"},"runc":{"path":"runc"},"io.containerd.runc.v2":{"path":"runc"}}});
    let bytes = serde_json::to_vec(&row).expect("JSON");
    engine_identity(&bytes).expect("known inert stock metadata");
    assert_eq!(
        inert_metadata(&bytes).expect("metadata"),
        vec!["io.containerd.runc.v2", "runc"]
    );
    let mut changed = row.clone();
    changed["Runtimes"]["youki"]["path"] = "/usr/bin/unverified-youki".into();
    assert!(engine_identity(&serde_json::to_vec(&changed).expect("JSON")).is_err());
    let mut changed = row.clone();
    changed["Runtimes"]["runc"]["path"] = "/bin/runc".into();
    assert!(engine_identity(&serde_json::to_vec(&changed).expect("JSON")).is_err());
    let mut changed = row;
    changed["Runtimes"]["crun"] = json!({"path":"crun"});
    assert!(engine_identity(&serde_json::to_vec(&changed).expect("JSON")).is_err());
}

#[test]
fn builder_must_be_single_exact_embedded_context_driver() {
    let valid = "Name: machine-context\nDriver: docker\n\nNodes:\nName: machine-context\nEndpoint: machine-context\nStatus: running\n";
    verify_builder(valid.as_bytes(), "machine-context").expect("embedded driver");
    for changed in [
        valid.replace("Driver: docker", "Driver: docker-container"),
        valid.replace("Endpoint: machine-context", "Endpoint: default"),
        valid.replace("Status: running", "Status: stopped"),
        format!("{valid}Name: sibling\nEndpoint: sibling\nStatus: running\n"),
        format!("{valid}Error: unable to connect\n"),
    ] {
        assert!(verify_builder(changed.as_bytes(), "machine-context").is_err());
    }
}

#[test]
fn cleanup_requires_exact_owner_policy_and_image() {
    let id = "a".repeat(64);
    let image = format!("sha256:{}", "b".repeat(64));
    let valid = json!([{"Id":id,"Image":image,"Config":{"Labels":{(LABEL):"exact-token"}},"HostConfig":{"Runtime":"youki","Privileged":false,"NetworkMode":"none"}}]);
    assert_eq!(
        owned_container(
            &serde_json::to_vec(&valid).expect("JSON"),
            "exact-token",
            Some(&image)
        )
        .expect("owned"),
        id
    );
    assert!(
        owned_container(
            &serde_json::to_vec(&valid).expect("JSON"),
            "foreign-token",
            Some(&image)
        )
        .is_err()
    );
    assert!(
        owned_container(
            &serde_json::to_vec(&valid).expect("JSON"),
            "exact-token",
            Some("different")
        )
        .is_err()
    );
    for (field, value) in [
        ("Runtime", json!("runc")),
        ("Privileged", json!(true)),
        ("NetworkMode", json!("host")),
    ] {
        let mut changed = valid.clone();
        changed[0]["HostConfig"][field] = value;
        assert!(
            owned_container(
                &serde_json::to_vec(&changed).expect("JSON"),
                "exact-token",
                None
            )
            .is_err()
        );
    }
    assert!(owned_container(b"[]", "exact-token", None).is_err());
}

#[test]
fn image_cleanup_rejects_foreign_labels_and_wrong_architecture() {
    let valid = json!([{"Id":format!("sha256:{}","c".repeat(64)),"Os":"linux","Architecture":"arm64","Config":{"Labels":{(LABEL):"token"}}}]);
    assert!(owned_image(&serde_json::to_vec(&valid).expect("JSON"), "token").is_ok());
    assert!(owned_image(&serde_json::to_vec(&valid).expect("JSON"), "foreign").is_err());
    let mut changed = valid;
    changed[0]["Architecture"] = "amd64".into();
    assert!(owned_image(&serde_json::to_vec(&changed).expect("JSON"), "token").is_err());
}

/// A completed probe re-attests only when every input it depended on is
/// unchanged. The first assertion is the one that matters most in practice: the
/// recorded inventory also carries the incarnation that measured it, so a
/// whole-value comparison would make this path unreachable on the very boot it
/// exists for - a fast path that is true, untested and never taken.
#[test]
fn reattestation_requires_identical_inputs_and_ignores_the_observing_incarnation() -> Result<()> {
    let (_root, store, mut journal) = journal_fixture()?;
    const STDOUT: &str = "vz-startup-runtime-inventory-v1\nyouki-sha256=dddd\n";
    const SCOPE: &str = "startup_executable_paths_and_pinned_daemon_mounts_not_release_cache_audit";
    journal.state = "completed".into();
    journal.resources = json!({
        "cleanup_scope": "disposable_probe_containers_compose_objects_and_images",
        "retained_buildkit_cache": true,
        "engine_id": "11111111-2222-3333-4444-555555555555",
        // Recorded by an earlier, now-gone incarnation - exactly the situation a
        // stop and start produces.
        "runtime_inventory": {
            "stdout": STDOUT,
            "scope": SCOPE,
            "incarnation": {"schema_version": 1, "generation": 1, "incarnation_id": "inc_probe"},
        },
    });
    let digest = store.configuration_digest().to_string();
    let client = journal.client_sha256.clone();
    let archive = journal.archive_sha256.clone();
    let basis = |journal: &Journal, client: &str, context: &str, config: &str, archive: &str| {
        reattestation_basis_of(journal, client, context, config, archive, STDOUT, SCOPE).is_some()
    };
    assert!(
        basis(&journal, &client, "exact-context", &digest, &archive),
        "an unchanged tuple must re-attest even though the incarnation that recorded it is gone"
    );

    // Each input, changed alone, must force the full probe.
    for (label, changed) in [
        (
            "host client",
            basis(
                &journal,
                &"9".repeat(64),
                "exact-context",
                &digest,
                &archive,
            ),
        ),
        (
            "managed context",
            basis(&journal, &client, "other-context", &digest, &archive),
        ),
        (
            "guest bundle",
            basis(
                &journal,
                &client,
                "exact-context",
                &format!("sha256:{}", "e".repeat(64)),
                &archive,
            ),
        ),
        (
            "probe archive",
            basis(&journal, &client, "exact-context", &digest, &"9".repeat(64)),
        ),
    ] {
        assert!(!changed, "a changed {label} must force the full probe");
    }

    for (label, mutate) in [
        (
            "a different guest runtime",
            Box::new(|j: &mut Journal| {
                j.resources["runtime_inventory"]["stdout"] =
                    "vz-startup-runtime-inventory-v1\nyouki-sha256=other\n".into();
            }) as Box<dyn Fn(&mut Journal)>,
        ),
        (
            "a different inventory scope",
            Box::new(|j: &mut Journal| {
                j.resources["runtime_inventory"]["scope"] = "narrower".into();
            }),
        ),
        (
            "a missing engine identity",
            Box::new(|j: &mut Journal| {
                j.resources["engine_id"] = Value::Null;
            }),
        ),
        (
            "a narrowed cleanup scope",
            Box::new(|j: &mut Journal| {
                j.resources["cleanup_scope"] = "something_else".into();
            }),
        ),
        (
            "a discarded buildkit cache",
            Box::new(|j: &mut Journal| {
                j.resources["retained_buildkit_cache"] = json!(false);
            }),
        ),
        (
            "no recorded inventory at all",
            Box::new(|j: &mut Journal| {
                j.resources["runtime_inventory"] = Value::Null;
            }),
        ),
    ] {
        let mut changed = journal.clone();
        mutate(&mut changed);
        assert!(
            !basis(&changed, &client, "exact-context", &digest, &archive),
            "{label} must force the full probe"
        );
    }
    Ok(())
}
