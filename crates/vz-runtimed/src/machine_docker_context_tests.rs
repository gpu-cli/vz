//! Offline ownership tests; the opt-in native Docker case does not start a VM
//! or claim Engine availability, Developer readiness, or compatibility parity.
use super::*;
use crate::machine_runtime_registry::{MachineRuntimeAdmission, MachineRuntimeRegistry};
use serde_json::json;
use vz_runtime_contract::{EnvironmentId, MachineId, ProjectId};

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
            &["context".into(), "inspect".into(), foreign_name],
            None,
            Duration::from_secs(10),
        )
        .await?
        .success()?
        .stdout;
    assert_eq!(before, after);
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
        json!({"phase":"offline_result","owned_claim_stable":true,"foreign_context_unchanged":true,"default_config_exact_bytes":String::from_utf8_lossy(default_bytes),"engine_connections":0,"vm_started":false,"readiness_or_parity_certified":false})
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
