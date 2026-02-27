#![allow(clippy::unwrap_used)]

use super::commands::{cmd_dashboard, event_service_name};
use super::helpers::{
    resolve_compose_file, resolve_service_container_id, resolve_stack_name,
    resolve_stack_registry_auth, split_exec_command,
};
use super::output::{format_event_summary, print_events_table, print_ps_table};
use super::*;
use std::sync::{Mutex, OnceLock};

fn cwd_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

#[test]
fn resolve_stack_registry_auth_defaults_to_none() {
    let opts = StackRegistryAuthOpts::default();
    let auth = resolve_stack_registry_auth(&opts).unwrap();
    assert!(auth.is_none());
}

#[test]
fn resolve_stack_registry_auth_supports_docker_config() {
    let opts = StackRegistryAuthOpts {
        docker_config: true,
        ..Default::default()
    };
    let auth = resolve_stack_registry_auth(&opts).unwrap();
    assert_eq!(auth, Some(vz_image::Auth::DockerConfig));
}

#[test]
fn resolve_stack_registry_auth_supports_basic_credentials() {
    let opts = StackRegistryAuthOpts {
        username: Some("alice".to_string()),
        password: Some("s3cr3t".to_string()),
        ..Default::default()
    };
    let auth = resolve_stack_registry_auth(&opts).unwrap();
    assert_eq!(
        auth,
        Some(vz_image::Auth::Basic {
            username: "alice".to_string(),
            password: "s3cr3t".to_string(),
        })
    );
}

#[test]
fn resolve_stack_name_explicit() {
    let name = resolve_stack_name(Some("myapp"), &PathBuf::from("compose.yaml")).unwrap();
    assert_eq!(name, "myapp");
}

#[test]
fn split_exec_command_separates_head_and_args() {
    let command = vec![
        "/bin/echo".to_string(),
        "hello".to_string(),
        "world".to_string(),
    ];
    let (cmd, args) = split_exec_command(&command).unwrap();
    assert_eq!(cmd, vec!["/bin/echo".to_string()]);
    assert_eq!(args, vec!["hello".to_string(), "world".to_string()]);
}

#[test]
fn split_exec_command_rejects_empty_input() {
    let result = split_exec_command(&[]);
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("cannot be empty"));
}

#[test]
fn resolve_service_container_id_returns_container_for_service() {
    let services = vec![runtime_v2::StackServiceStatus {
        service_name: "web".to_string(),
        phase: "running".to_string(),
        ready: true,
        container_id: "ctr-web-1".to_string(),
        last_error: String::new(),
    }];

    let container_id = resolve_service_container_id("demo", "web", &services).unwrap();
    assert_eq!(container_id, "ctr-web-1");
}

#[test]
fn resolve_service_container_id_errors_when_service_not_running() {
    let services = vec![runtime_v2::StackServiceStatus {
        service_name: "web".to_string(),
        phase: "creating".to_string(),
        ready: false,
        container_id: String::new(),
        last_error: String::new(),
    }];

    let error = resolve_service_container_id("demo", "web", &services).unwrap_err();
    assert!(error.to_string().contains("not running"));
}

#[test]
fn resolve_service_container_id_errors_when_service_missing() {
    let services = vec![runtime_v2::StackServiceStatus {
        service_name: "db".to_string(),
        phase: "running".to_string(),
        ready: true,
        container_id: "ctr-db-1".to_string(),
        last_error: String::new(),
    }];

    let error = resolve_service_container_id("demo", "web", &services).unwrap_err();
    assert!(error.to_string().contains("not found"));
}

#[test]
fn resolve_compose_file_explicit_path() {
    let p = resolve_compose_file(Some(PathBuf::from("/tmp/my-compose.yml"))).unwrap();
    assert_eq!(p, PathBuf::from("/tmp/my-compose.yml"));
}

#[test]
fn resolve_compose_file_discovery_in_tempdir() {
    let _guard = cwd_lock().lock().unwrap();
    let dir = std::env::temp_dir().join("vz-test-compose-discovery");
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();

    // Write a docker-compose.yml (not compose.yaml).
    let target = dir.join("docker-compose.yml");
    std::fs::write(&target, "services: {}").unwrap();

    // Discovery should find it even though compose.yaml doesn't exist.
    let saved = std::env::current_dir().unwrap();
    std::env::set_current_dir(&dir).unwrap();
    let found = resolve_compose_file(None);
    std::env::set_current_dir(&saved).unwrap();

    assert_eq!(found.unwrap(), PathBuf::from("docker-compose.yml"));

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn resolve_compose_file_no_file_errors() {
    let _guard = cwd_lock().lock().unwrap();
    let dir = std::env::temp_dir().join("vz-test-compose-empty");
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();

    let saved = std::env::current_dir().unwrap();
    std::env::set_current_dir(&dir).unwrap();
    let result = resolve_compose_file(None);
    std::env::set_current_dir(&saved).unwrap();

    assert!(result.is_err());
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("no compose file found")
    );

    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn cmd_dashboard_returns_deprecation_message() {
    let error = cmd_dashboard(DashboardArgs {
        name: "demo".to_string(),
        file: None,
        state_dir: None,
    })
    .await
    .expect_err("dashboard should be deprecated");
    assert!(error.to_string().contains("deprecated and removed"));
}

#[test]
fn format_event_summary_covers_all_variants() {
    let events = vec![
        StackEvent::StackApplyStarted {
            stack_name: "s".into(),
            services_count: 2,
        },
        StackEvent::StackApplyCompleted {
            stack_name: "s".into(),
            succeeded: 1,
            failed: 0,
        },
        StackEvent::StackApplyFailed {
            stack_name: "s".into(),
            error: "e".into(),
        },
        StackEvent::ServiceCreating {
            stack_name: "s".into(),
            service_name: "web".into(),
        },
        StackEvent::ServiceReady {
            stack_name: "s".into(),
            service_name: "web".into(),
            runtime_id: "ctr-1".into(),
        },
        StackEvent::ServiceStopping {
            stack_name: "s".into(),
            service_name: "web".into(),
        },
        StackEvent::ServiceStopped {
            stack_name: "s".into(),
            service_name: "web".into(),
            exit_code: 0,
        },
        StackEvent::ServiceFailed {
            stack_name: "s".into(),
            service_name: "web".into(),
            error: "oom".into(),
        },
        StackEvent::PortConflict {
            stack_name: "s".into(),
            service_name: "web".into(),
            port: 80,
        },
        StackEvent::VolumeCreated {
            stack_name: "s".into(),
            volume_name: "v".into(),
        },
        StackEvent::StackDestroyed {
            stack_name: "s".into(),
        },
        StackEvent::HealthCheckPassed {
            stack_name: "s".into(),
            service_name: "web".into(),
        },
        StackEvent::HealthCheckFailed {
            stack_name: "s".into(),
            service_name: "web".into(),
            attempt: 3,
            error: "timeout".into(),
        },
        StackEvent::DependencyBlocked {
            stack_name: "s".into(),
            service_name: "web".into(),
            waiting_on: vec!["db".into()],
        },
    ];

    for event in events {
        let summary = format_event_summary(&event);
        assert!(!summary.is_empty(), "empty summary for {event:?}");
    }
}

#[test]
fn print_ps_table_empty() {
    // Just verify it doesn't panic.
    print_ps_table(&[], None);
}

#[test]
fn print_ps_table_with_services() {
    let observed = vec![
        ServiceObservedState {
            service_name: "web".into(),
            phase: ServicePhase::Running,
            container_id: Some("ctr-abc".into()),
            last_error: None,
            ready: true,
        },
        ServiceObservedState {
            service_name: "db".into(),
            phase: ServicePhase::Pending,
            container_id: None,
            last_error: None,
            ready: false,
        },
    ];
    // Just verify it doesn't panic.
    print_ps_table(&observed, None);
}

#[test]
fn print_events_table_empty() {
    print_events_table(&[]);
}

#[test]
fn event_service_name_returns_name_for_service_events() {
    let event = StackEvent::ServiceCreating {
        stack_name: "s".into(),
        service_name: "web".into(),
    };
    assert_eq!(event_service_name(&event), Some("web"));

    let event = StackEvent::HealthCheckFailed {
        stack_name: "s".into(),
        service_name: "db".into(),
        attempt: 1,
        error: "timeout".into(),
    };
    assert_eq!(event_service_name(&event), Some("db"));
}

#[test]
fn event_service_name_returns_none_for_stack_events() {
    let event = StackEvent::StackApplyStarted {
        stack_name: "s".into(),
        services_count: 2,
    };
    assert_eq!(event_service_name(&event), None);

    let event = StackEvent::VolumeCreated {
        stack_name: "s".into(),
        volume_name: "v".into(),
    };
    assert_eq!(event_service_name(&event), None);

    let event = StackEvent::StackDestroyed {
        stack_name: "s".into(),
    };
    assert_eq!(event_service_name(&event), None);
}
