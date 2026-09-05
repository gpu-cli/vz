#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::path::Path;
use vz_cli::project_definition::discover_project_definition;

fn write_definition(directory: &Path, name: &str) {
    let definition = serde_json::json!({
        "schema_version": 1, "project_id": "prj_read_only_discovery", "name": name,
        "environment": { "schema_version": 1, "machines": [{
            "schema_version": 1, "name": "linux", "profile": "developer",
            "target": { "os": "linux", "arch": "aarch64", "image": "fixture:latest" }
        }] }
    });
    std::fs::write(
        directory.join("vz.json"),
        serde_json::to_vec(&definition).unwrap(),
    )
    .unwrap();
}

#[test]
fn discovers_nearest_valid_definition_without_mutating_it() {
    let fixture = tempfile::tempdir().unwrap();
    write_definition(fixture.path(), "outer");
    let inner = fixture.path().join("inner");
    let cwd = inner.join("src");
    std::fs::create_dir_all(&cwd).unwrap();
    write_definition(&inner, "inner");
    let path = inner.join("vz.json");
    let before = std::fs::read(&path).unwrap();
    let modified = std::fs::metadata(&path).unwrap().modified().unwrap();
    let discovered = discover_project_definition(&cwd).unwrap();
    assert_eq!(discovered.definition.name, "inner");
    assert_eq!(discovered.path, path.canonicalize().unwrap());
    assert_eq!(std::fs::read(&path).unwrap(), before);
    assert_eq!(
        std::fs::metadata(path).unwrap().modified().unwrap(),
        modified
    );
    assert_eq!(std::fs::read_dir(cwd).unwrap().count(), 0);
}

#[test]
fn invalid_nearest_definition_never_falls_back_to_parent() {
    let fixture = tempfile::tempdir().unwrap();
    write_definition(fixture.path(), "outer");
    let inner = fixture.path().join("inner");
    std::fs::create_dir(&inner).unwrap();
    for invalid in [
        "not JSON",
        "{}",
        r#"{"schema_version":999,"project_id":"prj_x","name":"invalid","environment":{"schema_version":1,"machines":[]}}"#,
    ] {
        std::fs::write(inner.join("vz.json"), invalid).unwrap();
        let error = discover_project_definition(&inner).unwrap_err();
        assert_eq!(error.code(), "invalid_definition");
        assert_eq!(
            std::fs::read_to_string(inner.join("vz.json")).unwrap(),
            invalid
        );
    }
}

#[test]
fn missing_definition_does_not_bootstrap_anything() {
    let fixture = tempfile::tempdir().unwrap();
    let error = discover_project_definition(fixture.path()).unwrap_err();
    assert_eq!(error.code(), "definition_not_found");
    assert_eq!(std::fs::read_dir(fixture.path()).unwrap().count(), 0);
}

#[test]
fn published_bootstrap_example_loads_without_rewriting_or_creating_state() {
    let fixture = tempfile::tempdir().unwrap();
    let bytes = include_bytes!("../../../examples/developer-environment/vz.json");
    std::fs::write(fixture.path().join("vz.json"), bytes).unwrap();
    let discovered = discover_project_definition(fixture.path()).unwrap();
    assert_eq!(discovered.definition.environment.machines.len(), 1);
    assert_eq!(discovered.definition.environment.machines[0].name, "dev");
    let round_trip: vz_runtime_contract::ProjectDefinition =
        serde_json::from_slice(&serde_json::to_vec(&discovered.definition).unwrap()).unwrap();
    assert_eq!(discovered.definition, round_trip);
    assert_eq!(
        std::fs::read(fixture.path().join("vz.json")).unwrap(),
        bytes
    );
    assert_eq!(std::fs::read_dir(fixture.path()).unwrap().count(), 1);
}

#[test]
fn unknown_definition_fields_fail_instead_of_silently_dropping_policy() {
    let fixture = tempfile::tempdir().unwrap();
    let mut base: serde_json::Value = serde_json::from_str(include_str!(
        "../../../examples/developer-environment/vz.json"
    ))
    .unwrap();
    base["environment"]["machines"][0]["workspace"] = serde_json::json!({
        "binding": "source", "target_path": "/workspace", "mode": "read_only"
    });
    base["environment"]["machines"][0]["requested_capabilities"] =
        serde_json::json!({"capabilities": ["posix_exec"]});
    base["environment"]["networks"] = serde_json::json!([
        {"schema_version": 1, "name": "private", "kind": "private"}
    ]);
    base["environment"]["endpoints"] = serde_json::json!([
        {"schema_version": 1, "name": "api", "machine": "dev", "network": "private", "protocol": "tcp", "port": 8080}
    ]);
    serde_json::from_value::<vz_runtime_contract::ProjectDefinition>(base.clone())
        .unwrap()
        .validate()
        .unwrap();
    for pointer in [
        "",
        "/environment",
        "/environment/machines/0",
        "/environment/machines/0/target",
        "/environment/machines/0/resources",
        "/environment/machines/0/workspace",
        "/environment/machines/0/requested_capabilities",
        "/environment/networks/0",
        "/environment/endpoints/0",
    ] {
        let mut invalid = base.clone();
        invalid
            .pointer_mut(pointer)
            .unwrap()
            .as_object_mut()
            .unwrap()
            .insert(
                "undeclared_policy".into(),
                serde_json::json!({"allow": false}),
            );
        let bytes = serde_json::to_vec(&invalid).unwrap();
        std::fs::write(fixture.path().join("vz.json"), &bytes).unwrap();
        let error = discover_project_definition(fixture.path()).unwrap_err();
        assert_eq!(error.code(), "invalid_definition", "{pointer}");
        assert!(error.to_string().contains("unknown field"), "{error}");
        assert_eq!(
            std::fs::read(fixture.path().join("vz.json")).unwrap(),
            bytes
        );
        assert_eq!(std::fs::read_dir(fixture.path()).unwrap().count(), 1);
    }
}

#[test]
fn directory_named_definition_fails_without_ancestor_fallback() {
    let fixture = tempfile::tempdir().unwrap();
    write_definition(fixture.path(), "outer");
    let inner = fixture.path().join("inner");
    std::fs::create_dir_all(inner.join("vz.json")).unwrap();
    assert_eq!(
        discover_project_definition(&inner).unwrap_err().code(),
        "invalid_definition"
    );
}

#[cfg(unix)]
#[test]
fn dangling_definition_fails_without_ancestor_fallback() {
    let fixture = tempfile::tempdir().unwrap();
    write_definition(fixture.path(), "outer");
    let inner = fixture.path().join("inner");
    std::fs::create_dir(&inner).unwrap();
    std::os::unix::fs::symlink("missing.json", inner.join("vz.json")).unwrap();
    assert_eq!(
        discover_project_definition(&inner).unwrap_err().code(),
        "definition_read_failed"
    );
    assert!(!inner.join("missing.json").exists());
}
