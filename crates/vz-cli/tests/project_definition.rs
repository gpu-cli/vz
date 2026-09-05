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
