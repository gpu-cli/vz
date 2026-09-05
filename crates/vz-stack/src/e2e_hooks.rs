//! Exact crash-boundary instrumentation compiled only for signed E2E artifacts.

use std::io::Write;
use std::path::Path;

const ENABLE_ENV: &str = "VZ_ENABLE_UNSAFE_E2E_FAULT_INJECTION";
const SELECTOR_ENV: &str = "VZ_TEST_TEARDOWN_FINALIZER_BOUNDARY";
const MARKER_ENV: &str = "VZ_TEST_TEARDOWN_FINALIZER_MARKER";
const STACK_ENV: &str = "VZ_TEST_TEARDOWN_FINALIZER_STACK";
const AUDIT_ENV: &str = "VZ_TEST_TEARDOWN_FINALIZER_AUDIT_LOG";

/// Publish one exact teardown boundary and park until the controller SIGKILLs
/// the process. The selector is a stable boundary ID, optionally qualified by
/// an exact service or volume identity after `:`.
pub fn teardown_boundary(
    boundary: &str,
    stack_name: &str,
    operation_id: &str,
    resource: Option<&str>,
    details: serde_json::Value,
) {
    if std::env::var(ENABLE_ENV).as_deref() != Ok("1") {
        return;
    }
    if std::env::var(STACK_ENV).is_ok_and(|configured| configured != stack_name) {
        return;
    }
    let boundary_id = resource.map_or_else(
        || boundary.to_string(),
        |resource| format!("{boundary}:{resource}"),
    );
    if std::env::var(SELECTOR_ENV).as_deref() != Ok(boundary_id.as_str()) {
        return;
    }
    let payload = serde_json::json!({
        "schema_version": 1,
        "boundary_id": boundary_id,
        "stack_id": stack_name,
        "operation_id": operation_id,
        "resource": resource,
        "details": details,
    });
    if let Ok(audit_path) = std::env::var(AUDIT_ENV) {
        append_audit_event(Path::new(&audit_path), &payload);
    }
    let marker_path = std::env::var(MARKER_ENV)
        .unwrap_or_else(|_| panic!("{MARKER_ENV} is required when E2E fault injection is enabled"));
    let marker_path = Path::new(&marker_path);
    if let Some(parent) = marker_path.parent() {
        std::fs::create_dir_all(parent).unwrap_or_else(|error| {
            panic!(
                "could not create teardown boundary marker directory {}: {error}",
                parent.display()
            )
        });
    }
    if marker_path.exists() {
        return;
    }
    // Publish only a complete, durable payload. The controller may SIGKILL us
    // as soon as the final path becomes visible.
    let pending_path = marker_path.with_extension(format!("pending-{}", std::process::id()));
    let mut marker = match std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&pending_path)
    {
        Ok(marker) => marker,
        Err(error) => panic!(
            "could not create teardown boundary marker {}: {error}",
            marker_path.display()
        ),
    };
    let bytes = match serde_json::to_vec(&payload) {
        Ok(bytes) => bytes,
        Err(error) => panic!("could not serialize teardown boundary marker: {error}"),
    };
    marker.write_all(&bytes).unwrap_or_else(|error| {
        panic!(
            "could not write teardown boundary marker {}: {error}",
            marker_path.display()
        )
    });
    marker.sync_all().unwrap_or_else(|error| {
        panic!(
            "could not fsync teardown boundary marker {}: {error}",
            marker_path.display()
        )
    });
    std::fs::rename(&pending_path, marker_path).unwrap_or_else(|error| {
        panic!(
            "could not publish teardown boundary marker {}: {error}",
            marker_path.display()
        )
    });
    if let Some(parent) = marker_path.parent() {
        std::fs::File::open(parent)
            .and_then(|directory| directory.sync_all())
            .unwrap_or_else(|error| {
                panic!(
                    "could not fsync teardown boundary marker directory {}: {error}",
                    parent.display()
                )
            });
    }

    loop {
        std::thread::park();
    }
}

fn append_audit_event(path: &Path, payload: &serde_json::Value) {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).unwrap_or_else(|error| {
            panic!(
                "could not create teardown boundary audit directory {}: {error}",
                parent.display()
            )
        });
    }
    let mut audit = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .unwrap_or_else(|error| {
            panic!(
                "could not open teardown boundary audit {}: {error}",
                path.display()
            )
        });
    let mut bytes = match serde_json::to_vec(payload) {
        Ok(bytes) => bytes,
        Err(error) => panic!("could not serialize teardown boundary audit event: {error}"),
    };
    bytes.push(b'\n');
    audit.write_all(&bytes).unwrap_or_else(|error| {
        panic!(
            "could not append teardown boundary audit {}: {error}",
            path.display()
        )
    });
    audit.sync_all().unwrap_or_else(|error| {
        panic!(
            "could not fsync teardown boundary audit {}: {error}",
            path.display()
        )
    });
}
