#![allow(clippy::unwrap_used)]

use super::*;

// ── parse_compose: basic ──────────────────────────────────────

#[test]
fn minimal_compose() {
    let yaml = r#"
services:
  web:
    image: nginx:latest
"#;
    let spec = parse_compose(yaml, "myapp").unwrap();
    assert_eq!(spec.name, "myapp");
    assert_eq!(spec.services.len(), 1);
    assert_eq!(spec.services[0].name, "web");
    assert_eq!(spec.services[0].image, "nginx:latest");
}

#[test]
fn compose_with_name_override() {
    let yaml = r#"
name: custom-name
services:
  web:
    image: nginx:latest
"#;
    let spec = parse_compose(yaml, "fallback").unwrap();
    assert_eq!(spec.name, "custom-name");
}

#[test]
fn version_key_accepted() {
    let yaml = r#"
version: "3.8"
services:
  web:
    image: nginx:latest
"#;
    let spec = parse_compose(yaml, "myapp").unwrap();
    assert_eq!(spec.services.len(), 1);
}

// ── Service fields ────────────────────────────────────────────

#[test]
fn full_service_spec() {
    let yaml = r#"
services:
  web:
    image: nginx:latest
    command: ["nginx", "-g", "daemon off;"]
    entrypoint: ["/entrypoint.sh"]
    environment:
      PORT: "8080"
      DEBUG: "true"
    working_dir: /app
    user: "1000:1000"
    restart: always
"#;
    let spec = parse_compose(yaml, "myapp").unwrap();
    let svc = &spec.services[0];
    assert_eq!(svc.image, "nginx:latest");
    assert_eq!(
        svc.command,
        Some(vec![
            "nginx".to_string(),
            "-g".to_string(),
            "daemon off;".to_string()
        ])
    );
    assert_eq!(svc.entrypoint, Some(vec!["/entrypoint.sh".to_string()]));
    assert_eq!(svc.environment.get("PORT").unwrap(), "8080");
    assert_eq!(svc.environment.get("DEBUG").unwrap(), "true");
    assert_eq!(svc.working_dir, Some("/app".to_string()));
    assert_eq!(svc.user, Some("1000:1000".to_string()));
    assert_eq!(svc.restart_policy, Some(RestartPolicy::Always));
}

#[test]
fn command_shell_form() {
    let yaml = r#"
services:
  web:
    image: nginx:latest
    command: nginx -g daemon off;
"#;
    let spec = parse_compose(yaml, "myapp").unwrap();
    assert_eq!(
        spec.services[0].command,
        Some(vec![
            "nginx".to_string(),
            "-g".to_string(),
            "daemon".to_string(),
            "off;".to_string(),
        ])
    );
}

// ── Environment parsing ───────────────────────────────────────

#[test]
fn environment_list_form() {
    let yaml = r#"
services:
  web:
    image: nginx:latest
    environment:
      - PORT=8080
      - DEBUG=true
      - EMPTY_VAR
"#;
    let spec = parse_compose(yaml, "myapp").unwrap();
    let env = &spec.services[0].environment;
    assert_eq!(env.get("PORT").unwrap(), "8080");
    assert_eq!(env.get("DEBUG").unwrap(), "true");
    assert_eq!(env.get("EMPTY_VAR").unwrap(), "");
}

#[test]
fn environment_numeric_and_bool_values() {
    let yaml = r#"
services:
  web:
    image: nginx:latest
    environment:
      PORT: 8080
      DEBUG: true
"#;
    let spec = parse_compose(yaml, "myapp").unwrap();
    let env = &spec.services[0].environment;
    assert_eq!(env.get("PORT").unwrap(), "8080");
    assert_eq!(env.get("DEBUG").unwrap(), "true");
}

// ── Port parsing ──────────────────────────────────────────────

#[test]
fn ports_short_host_container() {
    let yaml = r#"
services:
  web:
    image: nginx:latest
    ports:
      - "8080:80"
"#;
    let spec = parse_compose(yaml, "myapp").unwrap();
    let port = &spec.services[0].ports[0];
    assert_eq!(port.host_port, Some(8080));
    assert_eq!(port.container_port, 80);
    assert_eq!(port.protocol, "tcp");
}

#[test]
fn ports_short_container_only() {
    let yaml = r#"
services:
  web:
    image: nginx:latest
    ports:
      - "80"
"#;
    let spec = parse_compose(yaml, "myapp").unwrap();
    let port = &spec.services[0].ports[0];
    assert_eq!(port.host_port, None);
    assert_eq!(port.container_port, 80);
}

#[test]
fn ports_short_with_protocol() {
    let yaml = r#"
services:
  web:
    image: nginx:latest
    ports:
      - "8080:80/udp"
"#;
    let spec = parse_compose(yaml, "myapp").unwrap();
    let port = &spec.services[0].ports[0];
    assert_eq!(port.protocol, "udp");
    assert_eq!(port.host_port, Some(8080));
    assert_eq!(port.container_port, 80);
}

#[test]
fn ports_short_with_bind_address() {
    let yaml = r#"
services:
  web:
    image: nginx:latest
    ports:
      - "127.0.0.1:8080:80"
"#;
    let spec = parse_compose(yaml, "myapp").unwrap();
    let port = &spec.services[0].ports[0];
    assert_eq!(port.host_port, Some(8080));
    assert_eq!(port.container_port, 80);
}

#[test]
fn ports_long_form() {
    let yaml = r#"
services:
  web:
    image: nginx:latest
    ports:
      - target: 80
        published: 8080
        protocol: udp
"#;
    let spec = parse_compose(yaml, "myapp").unwrap();
    let port = &spec.services[0].ports[0];
    assert_eq!(port.container_port, 80);
    assert_eq!(port.host_port, Some(8080));
    assert_eq!(port.protocol, "udp");
}

#[test]
fn ports_bare_number() {
    let yaml = r#"
services:
  web:
    image: nginx:latest
    ports:
      - 80
"#;
    let spec = parse_compose(yaml, "myapp").unwrap();
    let port = &spec.services[0].ports[0];
    assert_eq!(port.container_port, 80);
    assert_eq!(port.host_port, None);
}

// ── Mount parsing ─────────────────────────────────────────────

#[test]
fn mount_bind_short() {
    let yaml = r#"
services:
  web:
    image: nginx:latest
    volumes:
      - /host/data:/container/data
"#;
    let spec = parse_compose(yaml, "myapp").unwrap();
    assert_eq!(
        spec.services[0].mounts[0],
        MountSpec::Bind {
            source: "/host/data".to_string(),
            target: "/container/data".to_string(),
            read_only: false,
        }
    );
}

#[test]
fn mount_bind_read_only() {
    let yaml = r#"
services:
  web:
    image: nginx:latest
    volumes:
      - /host/data:/container/data:ro
"#;
    let spec = parse_compose(yaml, "myapp").unwrap();
    assert_eq!(
        spec.services[0].mounts[0],
        MountSpec::Bind {
            source: "/host/data".to_string(),
            target: "/container/data".to_string(),
            read_only: true,
        }
    );
}

#[test]
fn mount_named_volume() {
    let yaml = r#"
services:
  db:
    image: postgres:15
    volumes:
      - dbdata:/var/lib/postgresql/data
volumes:
  dbdata:
"#;
    let spec = parse_compose(yaml, "myapp").unwrap();
    assert_eq!(
        spec.services[0].mounts[0],
        MountSpec::Named {
            source: "dbdata".to_string(),
            target: "/var/lib/postgresql/data".to_string(),
            read_only: false,
        }
    );
}

#[test]
fn mount_ephemeral() {
    let yaml = r#"
services:
  web:
    image: nginx:latest
    volumes:
      - /tmp/scratch
"#;
    let spec = parse_compose(yaml, "myapp").unwrap();
    assert_eq!(
        spec.services[0].mounts[0],
        MountSpec::Ephemeral {
            target: "/tmp/scratch".to_string(),
        }
    );
}

#[test]
fn mount_long_form_bind() {
    let yaml = r#"
services:
  web:
    image: nginx:latest
    volumes:
      - type: bind
        source: /host/path
        target: /container/path
        read_only: true
"#;
    let spec = parse_compose(yaml, "myapp").unwrap();
    assert_eq!(
        spec.services[0].mounts[0],
        MountSpec::Bind {
            source: "/host/path".to_string(),
            target: "/container/path".to_string(),
            read_only: true,
        }
    );
}

#[test]
fn mount_long_form_volume() {
    let yaml = r#"
services:
  db:
    image: postgres:15
    volumes:
      - type: volume
        source: dbdata
        target: /var/lib/postgresql/data
volumes:
  dbdata:
"#;
    let spec = parse_compose(yaml, "myapp").unwrap();
    assert_eq!(
        spec.services[0].mounts[0],
        MountSpec::Named {
            source: "dbdata".to_string(),
            target: "/var/lib/postgresql/data".to_string(),
            read_only: false,
        }
    );
}

#[test]
fn mount_long_form_tmpfs() {
    let yaml = r#"
services:
  web:
    image: nginx:latest
    volumes:
      - type: tmpfs
        target: /tmp
"#;
    let spec = parse_compose(yaml, "myapp").unwrap();
    assert_eq!(
        spec.services[0].mounts[0],
        MountSpec::Ephemeral {
            target: "/tmp".to_string(),
        }
    );
}

#[test]
fn mount_long_form_rejects_unknown_key() {
    let yaml = r#"
services:
  web:
    image: nginx:latest
    volumes:
      - type: bind
        source: /host/path
        target: /container/path
        propagation: rshared
"#;
    let err = parse_compose(yaml, "myapp").unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("services.web.volumes.propagation"));
    assert!(msg.contains("only `type`, `source`, `target`, and `read_only`"));
}

// ── depends_on parsing ────────────────────────────────────────

#[test]
fn depends_on_list_form() {
    let yaml = r#"
services:
  db:
    image: postgres:15
  web:
    image: nginx:latest
    depends_on:
      - db
"#;
    let spec = parse_compose(yaml, "myapp").unwrap();
    let web = spec.services.iter().find(|s| s.name == "web").unwrap();
    assert_eq!(web.depends_on, vec![ServiceDependency::started("db")]);
}

#[test]
fn depends_on_mapping_form_service_healthy() {
    let yaml = r#"
services:
  db:
    image: postgres:15
    healthcheck:
      test: ["CMD", "true"]
  web:
    image: nginx:latest
    depends_on:
      db:
        condition: service_healthy
"#;
    let spec = parse_compose(yaml, "myapp").unwrap();
    let web = spec.services.iter().find(|s| s.name == "web").unwrap();
    assert_eq!(web.depends_on, vec![ServiceDependency::healthy("db")]);
}

#[test]
fn depends_on_mapping_form_service_started() {
    let yaml = r#"
services:
  db:
    image: postgres:15
  web:
    image: nginx:latest
    depends_on:
      db:
        condition: service_started
"#;
    let spec = parse_compose(yaml, "myapp").unwrap();
    let web = spec.services.iter().find(|s| s.name == "web").unwrap();
    assert_eq!(web.depends_on, vec![ServiceDependency::started("db")]);
}

#[test]
fn depends_on_mapping_form_no_condition() {
    let yaml = r#"
services:
  db:
    image: postgres:15
  web:
    image: nginx:latest
    depends_on:
      db: {}
"#;
    let spec = parse_compose(yaml, "myapp").unwrap();
    let web = spec.services.iter().find(|s| s.name == "web").unwrap();
    assert_eq!(web.depends_on, vec![ServiceDependency::started("db")]);
}

#[test]
fn depends_on_mapping_form_service_completed_successfully() {
    let yaml = r#"
services:
  init:
    image: alpine:latest
  web:
    image: nginx:latest
    depends_on:
      init:
        condition: service_completed_successfully
"#;
    let spec = parse_compose(yaml, "myapp").unwrap();
    let web = spec.services.iter().find(|s| s.name == "web").unwrap();
    assert_eq!(
        web.depends_on[0].condition,
        DependencyCondition::ServiceCompletedSuccessfully
    );
}

#[test]
fn depends_on_service_healthy_requires_healthcheck() {
    let yaml = r#"
services:
  db:
    image: postgres:15
  web:
    image: nginx:latest
    depends_on:
      db:
        condition: service_healthy
"#;
    let err = parse_compose(yaml, "myapp").unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("condition `service_healthy`"));
    assert!(msg.contains("has no healthcheck"));
}

#[test]
fn depends_on_rejects_unsupported_dependency_option() {
    let yaml = r#"
services:
  db:
    image: postgres:15
  web:
    image: nginx:latest
    depends_on:
      db:
        condition: service_started
        required: false
"#;
    let err = parse_compose(yaml, "myapp").unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("depends_on.db.required"));
    assert!(msg.contains("only `condition` is supported"));
}

// ── Healthcheck parsing ───────────────────────────────────────

#[test]
fn healthcheck_list_test() {
    let yaml = r#"
services:
  web:
    image: nginx:latest
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost"]
      interval: 30s
      timeout: 5s
      retries: 3
      start_period: 10s
"#;
    let spec = parse_compose(yaml, "myapp").unwrap();
    let hc = spec.services[0].healthcheck.as_ref().unwrap();
    assert_eq!(hc.test, vec!["CMD", "curl", "-f", "http://localhost"]);
    assert_eq!(hc.interval_secs, Some(30));
    assert_eq!(hc.timeout_secs, Some(5));
    assert_eq!(hc.retries, Some(3));
    assert_eq!(hc.start_period_secs, Some(10));
}

#[test]
fn healthcheck_shell_form() {
    let yaml = r#"
services:
  web:
    image: nginx:latest
    healthcheck:
      test: curl -f http://localhost
      interval: 10
"#;
    let spec = parse_compose(yaml, "myapp").unwrap();
    let hc = spec.services[0].healthcheck.as_ref().unwrap();
    assert_eq!(hc.test, vec!["CMD-SHELL", "curl -f http://localhost"]);
    assert_eq!(hc.interval_secs, Some(10));
}

#[test]
fn healthcheck_disabled() {
    let yaml = r#"
services:
  web:
    image: nginx:latest
    healthcheck:
      disable: true
"#;
    let spec = parse_compose(yaml, "myapp").unwrap();
    assert!(spec.services[0].healthcheck.is_none());
}

// ── Restart policy parsing ────────────────────────────────────

#[test]
fn restart_policies() {
    let cases = vec![
        ("no", RestartPolicy::No),
        ("always", RestartPolicy::Always),
        ("unless-stopped", RestartPolicy::UnlessStopped),
        ("on-failure", RestartPolicy::OnFailure { max_retries: None }),
        (
            "on-failure:5",
            RestartPolicy::OnFailure {
                max_retries: Some(5),
            },
        ),
    ];

    for (input, expected) in cases {
        let yaml = format!(
            r#"
services:
  web:
    image: nginx:latest
    restart: {input}
"#
        );
        let spec = parse_compose(&yaml, "myapp").unwrap();
        assert_eq!(
            spec.services[0].restart_policy,
            Some(expected),
            "failed for restart: {input}"
        );
    }
}

// ── Volume parsing (top-level) ────────────────────────────────

#[test]
fn volumes_top_level_empty() {
    let yaml = r#"
services:
  db:
    image: postgres:15
    volumes:
      - dbdata:/var/lib/postgresql/data
volumes:
  dbdata:
"#;
    let spec = parse_compose(yaml, "myapp").unwrap();
    assert_eq!(spec.volumes.len(), 1);
    assert_eq!(spec.volumes[0].name, "dbdata");
    assert_eq!(spec.volumes[0].driver, "local");
    assert!(spec.volumes[0].driver_opts.is_none());
}

#[test]
fn volumes_top_level_with_driver_opts() {
    let yaml = r#"
services:
  db:
    image: postgres:15
    volumes:
      - dbdata:/var/lib/postgresql/data
volumes:
  dbdata:
    driver: local
    driver_opts:
      type: none
      device: /data/db
"#;
    let spec = parse_compose(yaml, "myapp").unwrap();
    let opts = spec.volumes[0].driver_opts.as_ref().unwrap();
    assert_eq!(opts.get("type").unwrap(), "none");
    assert_eq!(opts.get("device").unwrap(), "/data/db");
}

// ── Duration parsing ──────────────────────────────────────────

#[test]
fn duration_parsing() {
    assert_eq!(parse_duration_string("30s"), Some(30));
    assert_eq!(parse_duration_string("5m"), Some(300));
    assert_eq!(parse_duration_string("1h"), Some(3600));
    assert_eq!(parse_duration_string("1m30s"), Some(90));
    assert_eq!(parse_duration_string("1h30m15s"), Some(5415));
    assert_eq!(parse_duration_string("0s"), Some(0));
}

// ── Stop lifecycle parsing ───────────────────────────────────

#[test]
fn stop_signal_parsed() {
    let yaml = r#"
services:
  db:
    image: postgres:16
    stop_signal: SIGQUIT
"#;
    let spec = parse_compose(yaml, "myapp").unwrap();
    assert_eq!(spec.services[0].stop_signal.as_deref(), Some("SIGQUIT"));
}

#[test]
fn stop_grace_period_parsed() {
    let yaml = r#"
services:
  db:
    image: postgres:16
    stop_grace_period: 30s
"#;
    let spec = parse_compose(yaml, "myapp").unwrap();
    assert_eq!(spec.services[0].stop_grace_period_secs, Some(30));
}

#[test]
fn stop_signal_defaults_to_none() {
    let yaml = r#"
services:
  web:
    image: nginx:latest
"#;
    let spec = parse_compose(yaml, "myapp").unwrap();
    assert_eq!(spec.services[0].stop_signal, None);
    assert_eq!(spec.services[0].stop_grace_period_secs, None);
}

#[test]
fn stop_grace_period_compound_duration() {
    let yaml = r#"
services:
  db:
    image: postgres:16
    stop_grace_period: 1m30s
"#;
    let spec = parse_compose(yaml, "myapp").unwrap();
    assert_eq!(spec.services[0].stop_grace_period_secs, Some(90));
}

// ── Rejection tests ───────────────────────────────────────────

#[test]
fn build_short_form_without_image_derives_default_image() {
    let yaml = r#"
services:
  web:
    build: .
"#;
    let spec = parse_compose(yaml, "myapp").unwrap();
    assert_eq!(spec.services[0].name, "web");
    assert_eq!(spec.services[0].image, "web:latest");
}

#[test]
fn build_mapping_form_with_args_is_accepted() {
    let yaml = r#"
services:
  api:
    image: custom/api:dev
    build:
      context: .
      dockerfile: Dockerfile.dev
      target: runtime
      args:
        APP_ENV: development
        PORT: 8080
"#;
    let spec = parse_compose(yaml, "myapp").unwrap();
    assert_eq!(spec.services[0].name, "api");
    assert_eq!(spec.services[0].image, "custom/api:dev");
}

#[test]
fn build_mapping_form_with_cache_from_is_accepted() {
    let yaml = r#"
services:
  web:
    image: web:latest
    build:
      context: .
      cache_from:
        - web:cache
        - ghcr.io/acme/web:buildcache
"#;
    let spec = parse_compose(yaml, "myapp").unwrap();
    assert_eq!(spec.services[0].name, "web");
    assert_eq!(spec.services[0].image, "web:latest");
}

#[test]
fn build_cache_from_rejects_non_string_entries() {
    let yaml = r#"
services:
  web:
    image: web:latest
    build:
      context: .
      cache_from:
        - web:cache
        - 123
"#;
    let err = parse_compose(yaml, "myapp").unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("build.cache_from"));
    assert!(msg.contains("entries must be strings"));
}

#[test]
fn collect_compose_build_specs_returns_normalized_builds() {
    let yaml = r#"
services:
  web:
    image: web:latest
    build:
      context: ./app
      dockerfile: Dockerfile.dev
      target: runtime
      args:
        APP_ENV: dev
      cache_from:
        - ghcr.io/acme/web:cache
  worker:
    build: .
"#;
    let builds = collect_compose_build_specs(yaml).unwrap();
    assert_eq!(builds.len(), 2);

    let web = builds.get("web").unwrap();
    assert_eq!(web.service_name, "web");
    assert_eq!(web.context, "./app");
    assert_eq!(web.dockerfile.as_deref(), Some("Dockerfile.dev"));
    assert_eq!(web.target.as_deref(), Some("runtime"));
    assert_eq!(web.args.get("APP_ENV").map(String::as_str), Some("dev"));
    assert_eq!(web.cache_from, vec!["ghcr.io/acme/web:cache".to_string()]);

    let worker = builds.get("worker").unwrap();
    assert_eq!(worker.context, ".");
    assert!(worker.dockerfile.is_none());
    assert!(worker.target.is_none());
    assert!(worker.args.is_empty());
    assert!(worker.cache_from.is_empty());
}

// ── Network parsing ──────────────────────────────────────────

#[test]
fn parse_top_level_networks_minimal() {
    let yaml = r#"
services:
  web:
    image: nginx:latest
    networks:
      - frontend
networks:
  frontend:
"#;
    let spec = parse_compose(yaml, "myapp").unwrap();
    // "default" is not created since no service lacks explicit networks.
    // "frontend" is the only defined network.
    assert!(spec.networks.iter().any(|n| n.name == "frontend"));
    let frontend = spec.networks.iter().find(|n| n.name == "frontend").unwrap();
    assert_eq!(frontend.driver, "bridge");
    assert_eq!(frontend.subnet, None);
    assert_eq!(spec.services[0].networks, vec!["frontend"]);
}

#[test]
fn parse_networks_with_ipam_subnet() {
    let yaml = r#"
services:
  web:
    image: nginx:latest
    networks:
      - frontend
  db:
    image: postgres:16
    networks:
      - backend
networks:
  frontend:
    driver: bridge
    ipam:
      config:
        - subnet: 172.20.1.0/24
  backend:
"#;
    let spec = parse_compose(yaml, "myapp").unwrap();
    assert_eq!(spec.networks.len(), 2);

    let frontend = spec.networks.iter().find(|n| n.name == "frontend").unwrap();
    assert_eq!(frontend.subnet.as_deref(), Some("172.20.1.0/24"));

    let backend = spec.networks.iter().find(|n| n.name == "backend").unwrap();
    assert_eq!(backend.subnet, None);

    // Check service network assignments.
    let web = spec.services.iter().find(|s| s.name == "web").unwrap();
    assert_eq!(web.networks, vec!["frontend"]);

    let db = spec.services.iter().find(|s| s.name == "db").unwrap();
    assert_eq!(db.networks, vec!["backend"]);
}

#[test]
fn parse_service_networks_list_form() {
    let yaml = r#"
services:
  web:
    image: nginx:latest
    networks:
      - frontend
      - backend
networks:
  frontend:
  backend:
"#;
    let spec = parse_compose(yaml, "myapp").unwrap();
    let web = spec.services.iter().find(|s| s.name == "web").unwrap();
    assert!(web.networks.contains(&"frontend".to_string()));
    assert!(web.networks.contains(&"backend".to_string()));
}

#[test]
fn parse_service_networks_mapping_form() {
    let yaml = r#"
services:
  web:
    image: nginx:latest
    networks:
      frontend: {}
      backend:
networks:
  frontend:
  backend:
"#;
    let spec = parse_compose(yaml, "myapp").unwrap();
    let web = spec.services.iter().find(|s| s.name == "web").unwrap();
    assert!(web.networks.contains(&"frontend".to_string()));
    assert!(web.networks.contains(&"backend".to_string()));
}

#[test]
fn reject_service_network_attachment_options() {
    let yaml = r#"
services:
  web:
    image: nginx:latest
    networks:
      frontend:
        aliases:
          - web-local
networks:
  frontend:
"#;
    let err = parse_compose(yaml, "myapp").unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("services.web.networks.frontend.aliases"));
    assert!(msg.contains("network attachment options are not supported"));
}

#[test]
fn network_mode_bridge_is_accepted() {
    let yaml = r#"
services:
  web:
    image: nginx:latest
    network_mode: bridge
"#;
    let spec = parse_compose(yaml, "myapp").unwrap();
    assert_eq!(spec.services.len(), 1);
    assert_eq!(spec.services[0].name, "web");
}

#[test]
fn network_mode_host_rejected_with_stable_error() {
    let yaml = r#"
services:
  web:
    image: nginx:latest
    network_mode: host
"#;
    let err = parse_compose(yaml, "myapp").unwrap_err();
    let msg = err.to_string();
    assert!(msg.starts_with("unsupported_operation:"));
    assert!(msg.contains("services.web.network_mode"));
    assert!(msg.contains("supported value is `bridge`"));
}

#[test]
fn network_mode_and_networks_are_mutually_exclusive() {
    let yaml = r#"
services:
  web:
    image: nginx:latest
    network_mode: bridge
    networks:
      - frontend
networks:
  frontend:
"#;
    let err = parse_compose(yaml, "myapp").unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("cannot set both `network_mode` and `networks`"));
}

#[test]
fn no_networks_section_creates_implicit_default() {
    let yaml = r#"
services:
  web:
    image: nginx:latest
  db:
    image: postgres:16
"#;
    let spec = parse_compose(yaml, "myapp").unwrap();
    assert_eq!(spec.networks.len(), 1);
    assert_eq!(spec.networks[0].name, "default");
    assert_eq!(spec.networks[0].driver, "bridge");

    // All services join the default network.
    for svc in &spec.services {
        assert_eq!(svc.networks, vec!["default"]);
    }
}

#[test]
fn custom_networks_services_without_explicit_get_default() {
    let yaml = r#"
services:
  web:
    image: nginx:latest
    networks:
      - frontend
  db:
    image: postgres:16
networks:
  frontend:
"#;
    let spec = parse_compose(yaml, "myapp").unwrap();
    // "default" network is auto-created for db.
    assert!(spec.networks.iter().any(|n| n.name == "default"));
    assert!(spec.networks.iter().any(|n| n.name == "frontend"));

    let web = spec.services.iter().find(|s| s.name == "web").unwrap();
    assert_eq!(web.networks, vec!["frontend"]);

    let db = spec.services.iter().find(|s| s.name == "db").unwrap();
    assert_eq!(db.networks, vec!["default"]);
}

#[test]
fn reject_undefined_network_reference() {
    let yaml = r#"
services:
  web:
    image: nginx:latest
    networks:
      - nonexistent
networks:
  frontend:
"#;
    let err = parse_compose(yaml, "myapp").unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("nonexistent"),
        "error should mention the undefined network: {msg}"
    );
}

#[test]
fn reject_non_bridge_driver() {
    let yaml = r#"
services:
  web:
    image: nginx:latest
networks:
  mynet:
    driver: overlay
"#;
    let err = parse_compose(yaml, "myapp").unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("overlay"),
        "error should mention the unsupported driver: {msg}"
    );
}

#[test]
fn reject_top_level_network_unknown_key() {
    let yaml = r#"
services:
  web:
    image: nginx:latest
networks:
  frontend:
    internal: true
"#;
    let err = parse_compose(yaml, "myapp").unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("networks.frontend.internal"));
    assert!(msg.contains("only `driver` and `ipam` are supported"));
}

#[test]
fn accept_deploy_replicas() {
    let yaml = r#"
services:
  web:
    image: nginx:latest
    deploy:
      replicas: 3
"#;
    let spec = parse_compose(yaml, "myapp").unwrap();
    assert_eq!(spec.services[0].resources.replicas, 3);
}

#[test]
fn deploy_resources_reservations_accepted() {
    let yaml = r#"
services:
  web:
    image: nginx:latest
    deploy:
      resources:
        reservations:
          cpus: "0.25"
          memory: "256m"
"#;
    let spec = parse_compose(yaml, "myapp").unwrap();
    let res = &spec.services[0].resources;
    assert_eq!(res.reservation_cpus, Some(0.25));
    assert_eq!(res.reservation_memory_bytes, Some(256 * 1024 * 1024));
    assert_eq!(res.cpus, None);
    assert_eq!(res.memory_bytes, None);
}

#[test]
fn reject_extends() {
    let yaml = r#"
services:
  web:
    image: nginx:latest
    extends:
      service: base
"#;
    let err = parse_compose(yaml, "myapp").unwrap_err();
    assert!(err.to_string().contains("extends"));
}

#[test]
fn reject_configs() {
    let yaml = r#"
services:
  web:
    image: nginx:latest
configs:
  myconfig:
    file: ./config.txt
"#;
    let err = parse_compose(yaml, "myapp").unwrap_err();
    assert!(err.to_string().contains("configs"));
}

#[test]
fn secrets_file_based_accepted() {
    let yaml = r#"
services:
  web:
    image: nginx:latest
    secrets:
      - mysecret
secrets:
  mysecret:
    file: ./secret.txt
"#;
    let spec = parse_compose(yaml, "myapp").unwrap();
    assert_eq!(spec.secrets.len(), 1);
    assert_eq!(spec.secrets[0].name, "mysecret");
    assert_eq!(spec.secrets[0].file(), Some("./secret.txt"));
    assert_eq!(spec.services[0].secrets.len(), 1);
    assert_eq!(spec.services[0].secrets[0].source, "mysecret");
    assert_eq!(spec.services[0].secrets[0].target, "mysecret");
}

#[test]
fn secrets_long_form_with_target() {
    let yaml = r#"
services:
  web:
    image: nginx:latest
    secrets:
      - source: db_password
        target: password.txt
secrets:
  db_password:
    file: ./db_pass.txt
"#;
    let spec = parse_compose(yaml, "myapp").unwrap();
    assert_eq!(spec.services[0].secrets.len(), 1);
    assert_eq!(spec.services[0].secrets[0].source, "db_password");
    assert_eq!(spec.services[0].secrets[0].target, "password.txt");
}

#[test]
fn secrets_long_form_without_target() {
    let yaml = r#"
services:
  web:
    image: nginx:latest
    secrets:
      - source: api_key
secrets:
  api_key:
    file: ./api.key
"#;
    let spec = parse_compose(yaml, "myapp").unwrap();
    assert_eq!(spec.services[0].secrets[0].source, "api_key");
    assert_eq!(spec.services[0].secrets[0].target, "api_key");
}

#[test]
fn secrets_external_rejected() {
    let yaml = r#"
services:
  web:
    image: nginx:latest
secrets:
  mysecret:
    external: true
"#;
    let err = parse_compose(yaml, "myapp").unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("external"),
        "error should mention external: {msg}"
    );
}

#[test]
fn secrets_missing_file_rejected() {
    let yaml = r#"
services:
  web:
    image: nginx:latest
secrets:
  mysecret:
    name: something
"#;
    let err = parse_compose(yaml, "myapp").unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("file"), "error should mention file: {msg}");
}

#[test]
fn secrets_undefined_ref_rejected() {
    let yaml = r#"
services:
  web:
    image: nginx:latest
    secrets:
      - undefined_secret
secrets:
  mysecret:
    file: ./secret.txt
"#;
    let err = parse_compose(yaml, "myapp").unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("undefined_secret"),
        "error should mention the undefined secret: {msg}"
    );
}

#[test]
fn secrets_multiple() {
    let yaml = r#"
services:
  web:
    image: nginx:latest
    secrets:
      - db_pass
      - api_key
secrets:
  db_pass:
    file: ./db.txt
  api_key:
    file: ./api.key
"#;
    let spec = parse_compose(yaml, "myapp").unwrap();
    assert_eq!(spec.secrets.len(), 2);
    assert_eq!(spec.services[0].secrets.len(), 2);
}

#[test]
fn secrets_top_level_without_service_refs() {
    let yaml = r#"
services:
  web:
    image: nginx:latest
secrets:
  unused_secret:
    file: ./unused.txt
"#;
    let spec = parse_compose(yaml, "myapp").unwrap();
    assert_eq!(spec.secrets.len(), 1);
    assert!(spec.services[0].secrets.is_empty());
}

#[test]
fn reject_devices() {
    let yaml = r#"
services:
  web:
    image: nginx:latest
    devices:
      - /dev/sda:/dev/xvdc
"#;
    let err = parse_compose(yaml, "myapp").unwrap_err();
    assert!(err.to_string().contains("devices"));
}

#[test]
fn reject_ipc() {
    let yaml = r#"
services:
  web:
    image: nginx:latest
    ipc: host
"#;
    let err = parse_compose(yaml, "myapp").unwrap_err();
    assert!(err.to_string().contains("ipc"));
}

#[test]
fn reject_pid() {
    let yaml = r#"
services:
  web:
    image: nginx:latest
    pid: host
"#;
    let err = parse_compose(yaml, "myapp").unwrap_err();
    assert!(err.to_string().contains("pid"));
}

#[test]
fn reject_runtime() {
    let yaml = r#"
services:
  web:
    image: nginx:latest
    runtime: runc
"#;
    let err = parse_compose(yaml, "myapp").unwrap_err();
    assert!(err.to_string().contains("runtime"));
}

#[test]
fn reject_profiles() {
    let yaml = r#"
services:
  web:
    image: nginx:latest
    profiles:
      - debug
"#;
    let err = parse_compose(yaml, "myapp").unwrap_err();
    assert!(err.to_string().contains("profiles"));
}

#[test]
fn parse_extra_hosts_entries() {
    let yaml = r#"
services:
  web:
    image: nginx:latest
    extra_hosts:
      - "myhost:192.168.1.10"
      - "other:10.0.0.1"
"#;
    let spec = parse_compose(yaml, "myapp").unwrap();
    let web = &spec.services[0];
    assert_eq!(web.extra_hosts.len(), 2);
    assert_eq!(
        web.extra_hosts[0],
        ("myhost".to_string(), "192.168.1.10".to_string())
    );
    assert_eq!(
        web.extra_hosts[1],
        ("other".to_string(), "10.0.0.1".to_string())
    );
}

#[test]
fn reject_cgroup() {
    let yaml = r#"
services:
  web:
    image: nginx:latest
    cgroup: host
"#;
    let err = parse_compose(yaml, "myapp").unwrap_err();
    assert!(err.to_string().contains("cgroup"));
}

#[test]
fn reject_unknown_top_level_key() {
    let yaml = r#"
services:
  web:
    image: nginx:latest
x-custom:
  foo: bar
"#;
    let err = parse_compose(yaml, "myapp").unwrap_err();
    assert!(err.to_string().contains("x-custom"));
}

#[test]
fn reject_unknown_service_key() {
    let yaml = r#"
services:
  web:
    image: nginx:latest
    ipc: host
"#;
    let err = parse_compose(yaml, "myapp").unwrap_err();
    assert!(err.to_string().contains("ipc"));
}

#[test]
fn reject_non_local_volume_driver() {
    let yaml = r#"
services:
  db:
    image: postgres:15
volumes:
  dbdata:
    driver: nfs
"#;
    let err = parse_compose(yaml, "myapp").unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("nfs") || msg.contains("local"), "{msg}");
}

// ── Validation tests ──────────────────────────────────────────

#[test]
fn validate_undefined_dependency() {
    let yaml = r#"
services:
  web:
    image: nginx:latest
    depends_on:
      - nonexistent
"#;
    let err = parse_compose(yaml, "myapp").unwrap_err();
    assert!(err.to_string().contains("nonexistent"));
}

#[test]
fn validate_undefined_volume_reference() {
    let yaml = r#"
services:
  db:
    image: postgres:15
    volumes:
      - missing_vol:/data
"#;
    let err = parse_compose(yaml, "myapp").unwrap_err();
    assert!(err.to_string().contains("missing_vol"));
}

#[test]
fn parse_service_kind_workspace() {
    let yaml = r#"
services:
  ws:
    image: ghcr.io/acme/workspace:latest
    x-vz:
      kind: workspace
  api:
    image: ghcr.io/acme/api:latest
    x-vz:
      kind: service
"#;
    let spec = parse_compose(yaml, "myapp").unwrap();
    assert_eq!(spec.services.len(), 2);
    assert_eq!(spec.services[0].name, "api");
    assert_eq!(spec.services[0].kind, ServiceKind::Service);
    assert_eq!(spec.services[1].name, "ws");
    assert_eq!(spec.services[1].kind, ServiceKind::Workspace);
}

#[test]
fn reject_invalid_service_kind() {
    let yaml = r#"
services:
  ws:
    image: ghcr.io/acme/workspace:latest
    x-vz:
      kind: daemon
"#;
    let err = parse_compose(yaml, "myapp").unwrap_err();
    assert!(err.to_string().contains("x-vz.kind"));
}

#[test]
fn reject_multiple_workspace_services() {
    let yaml = r#"
services:
  ws-a:
    image: ghcr.io/acme/workspace-a:latest
    x-vz:
      kind: workspace
  ws-b:
    image: ghcr.io/acme/workspace-b:latest
    x-vz:
      kind: workspace
"#;
    let err = parse_compose(yaml, "myapp").unwrap_err();
    assert!(err.to_string().contains("workspace services"));
}

#[test]
fn missing_image_fails() {
    let yaml = r#"
services:
  web:
    command: ["echo", "hello"]
"#;
    let err = parse_compose(yaml, "myapp").unwrap_err();
    assert!(err.to_string().contains("image"));
}

// ── Multi-service compose ─────────────────────────────────────

#[test]
fn web_redis_compose() {
    let yaml = r#"
services:
  web:
    image: myapp:latest
    ports:
      - "8080:80"
    depends_on:
      - redis
    environment:
      REDIS_URL: redis://redis:6379
  redis:
    image: redis:7-alpine
    ports:
      - "6379:6379"
    volumes:
      - redis-data:/data
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 10s
      timeout: 3s
      retries: 5

volumes:
  redis-data:
"#;
    let spec = parse_compose(yaml, "myapp").unwrap();

    // Services sorted by name.
    assert_eq!(spec.services.len(), 2);
    assert_eq!(spec.services[0].name, "redis");
    assert_eq!(spec.services[1].name, "web");

    // Redis service.
    let redis = &spec.services[0];
    assert_eq!(redis.image, "redis:7-alpine");
    assert_eq!(redis.ports[0].container_port, 6379);
    assert_eq!(redis.ports[0].host_port, Some(6379));
    assert!(redis.healthcheck.is_some());
    let hc = redis.healthcheck.as_ref().unwrap();
    assert_eq!(hc.interval_secs, Some(10));
    assert_eq!(hc.timeout_secs, Some(3));
    assert_eq!(hc.retries, Some(5));
    assert_eq!(
        redis.mounts[0],
        MountSpec::Named {
            source: "redis-data".to_string(),
            target: "/data".to_string(),
            read_only: false,
        }
    );

    // Web service.
    let web = &spec.services[1];
    assert_eq!(web.depends_on, vec![ServiceDependency::started("redis")]);
    assert_eq!(
        web.environment.get("REDIS_URL").unwrap(),
        "redis://redis:6379"
    );
    assert_eq!(web.ports[0].host_port, Some(8080));
    assert_eq!(web.ports[0].container_port, 80);

    // Volume.
    assert_eq!(spec.volumes.len(), 1);
    assert_eq!(spec.volumes[0].name, "redis-data");
}

#[test]
fn services_sorted_deterministically() {
    let yaml = r#"
services:
  zeta:
    image: img:latest
  alpha:
    image: img:latest
  middle:
    image: img:latest
"#;
    let spec = parse_compose(yaml, "myapp").unwrap();
    let names: Vec<&str> = spec.services.iter().map(|s| s.name.as_str()).collect();
    assert_eq!(names, vec!["alpha", "middle", "zeta"]);
}

#[test]
fn relative_path_bind_mount() {
    let yaml = r#"
services:
  web:
    image: nginx:latest
    volumes:
      - ./src:/app/src
"#;
    let spec = parse_compose(yaml, "myapp").unwrap();
    assert_eq!(
        spec.services[0].mounts[0],
        MountSpec::Bind {
            source: "./src".to_string(),
            target: "/app/src".to_string(),
            read_only: false,
        }
    );
}

// ── env_file and variable expansion ──────────────────────────

#[test]
fn parse_env_file_basic() {
    let content = r#"
# This is a comment
KEY1=value1
KEY2=value2

# Another comment
KEY3=value with spaces
"#;
    let env = parse_env_file_content(content);
    assert_eq!(env.get("KEY1").unwrap(), "value1");
    assert_eq!(env.get("KEY2").unwrap(), "value2");
    assert_eq!(env.get("KEY3").unwrap(), "value with spaces");
    assert_eq!(env.len(), 3);
}

#[test]
fn parse_env_file_quoted_values() {
    let content = r#"
SINGLE='single quoted'
DOUBLE="double quoted"
UNQUOTED=plain
"#;
    let env = parse_env_file_content(content);
    assert_eq!(env.get("SINGLE").unwrap(), "single quoted");
    assert_eq!(env.get("DOUBLE").unwrap(), "double quoted");
    assert_eq!(env.get("UNQUOTED").unwrap(), "plain");
}

#[test]
fn parse_env_file_export_prefix() {
    let content = "export DB_HOST=localhost\nexport DB_PORT=5432\n";
    let env = parse_env_file_content(content);
    assert_eq!(env.get("DB_HOST").unwrap(), "localhost");
    assert_eq!(env.get("DB_PORT").unwrap(), "5432");
}

#[test]
fn parse_env_file_empty_value() {
    let content = "EMPTY=\n";
    let env = parse_env_file_content(content);
    assert_eq!(env.get("EMPTY").unwrap(), "");
}

#[test]
fn expand_braced_variable() {
    let mut vars = HashMap::new();
    vars.insert("DB_HOST".to_string(), "localhost".to_string());
    vars.insert("DB_PORT".to_string(), "5432".to_string());
    let result = expand_variables("host=${DB_HOST} port=${DB_PORT}", &vars);
    assert_eq!(result, "host=localhost port=5432");
}

#[test]
fn expand_simple_variable() {
    let mut vars = HashMap::new();
    vars.insert("TAG".to_string(), "latest".to_string());
    let result = expand_variables("image: nginx:$TAG", &vars);
    assert_eq!(result, "image: nginx:latest");
}

#[test]
fn expand_default_value() {
    let vars = HashMap::new();
    let result = expand_variables("port=${PORT:-8080}", &vars);
    assert_eq!(result, "port=8080");
}

#[test]
fn expand_default_not_used_when_set() {
    let mut vars = HashMap::new();
    vars.insert("PORT".to_string(), "3000".to_string());
    let result = expand_variables("port=${PORT:-8080}", &vars);
    assert_eq!(result, "port=3000");
}

#[test]
fn expand_default_used_when_empty() {
    let mut vars = HashMap::new();
    vars.insert("PORT".to_string(), String::new());
    let result = expand_variables("port=${PORT:-8080}", &vars);
    assert_eq!(result, "port=8080");
}

#[test]
fn expand_missing_variable_empty() {
    let vars = HashMap::new();
    let result = expand_variables("val=${MISSING}", &vars);
    assert_eq!(result, "val=");
}

#[test]
fn expand_dollar_dollar_literal() {
    let vars = HashMap::new();
    let result = expand_variables("cost: $$100", &vars);
    assert_eq!(result, "cost: $100");
}

#[test]
fn expand_no_variables_unchanged() {
    let vars = HashMap::new();
    let input = "plain text without variables";
    assert_eq!(expand_variables(input, &vars), input);
}

#[test]
fn env_file_accepted_without_dir() {
    // env_file is accepted but silently ignored without compose_dir.
    let yaml = r#"
services:
  web:
    image: nginx:latest
    env_file: .env
"#;
    let spec = parse_compose(yaml, "myapp").unwrap();
    assert_eq!(spec.services[0].image, "nginx:latest");
}

#[test]
fn env_file_loads_from_directory() {
    let dir = tempfile::tempdir().unwrap();
    let env_path = dir.path().join("app.env");
    std::fs::write(&env_path, "DB_HOST=postgres\nDB_PORT=5432\n").unwrap();

    let yaml = r#"
services:
  web:
    image: nginx:latest
    env_file: app.env
"#;
    let spec = parse_compose_with_dir(yaml, "myapp", dir.path()).unwrap();
    let env = &spec.services[0].environment;
    assert_eq!(env.get("DB_HOST").unwrap(), "postgres");
    assert_eq!(env.get("DB_PORT").unwrap(), "5432");
}

#[test]
fn env_file_list_loads_multiple() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("base.env"), "A=1\nB=2\n").unwrap();
    std::fs::write(dir.path().join("override.env"), "B=99\nC=3\n").unwrap();

    let yaml = r#"
services:
  web:
    image: nginx:latest
    env_file:
      - base.env
      - override.env
"#;
    let spec = parse_compose_with_dir(yaml, "myapp", dir.path()).unwrap();
    let env = &spec.services[0].environment;
    assert_eq!(env.get("A").unwrap(), "1");
    assert_eq!(env.get("B").unwrap(), "99"); // overridden
    assert_eq!(env.get("C").unwrap(), "3");
}

#[test]
fn explicit_env_overrides_env_file() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join(".env"), "PORT=3000\nHOST=0.0.0.0\n").unwrap();

    let yaml = r#"
services:
  web:
    image: nginx:latest
    env_file: .env
    environment:
      PORT: "8080"
"#;
    let spec = parse_compose_with_dir(yaml, "myapp", dir.path()).unwrap();
    let env = &spec.services[0].environment;
    assert_eq!(env.get("PORT").unwrap(), "8080"); // explicit wins
    assert_eq!(env.get("HOST").unwrap(), "0.0.0.0"); // from env_file
}

#[test]
fn variable_expansion_in_yaml() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join(".env"), "TAG=v2.1\nPORT=9090\n").unwrap();

    let yaml = r#"
services:
  web:
    image: myapp:${TAG}
    ports:
      - "${PORT}:80"
"#;
    let spec = parse_compose_with_dir(yaml, "myapp", dir.path()).unwrap();
    assert_eq!(spec.services[0].image, "myapp:v2.1");
    assert_eq!(spec.services[0].ports[0].host_port, Some(9090));
    assert_eq!(spec.services[0].ports[0].container_port, 80);
}

#[test]
fn variable_expansion_with_defaults() {
    let dir = tempfile::tempdir().unwrap();
    // No .env file — all defaults should kick in.
    let yaml = r#"
services:
  web:
    image: nginx:${TAG:-latest}
    environment:
      PORT: "${PORT:-8080}"
"#;
    let spec = parse_compose_with_dir(yaml, "myapp", dir.path()).unwrap();
    assert_eq!(spec.services[0].image, "nginx:latest");
    assert_eq!(spec.services[0].environment.get("PORT").unwrap(), "8080");
}

// ── Deploy / resource limits parsing ─────────────────────────

#[test]
fn deploy_resource_limits_cpus_and_memory() {
    let yaml = r#"
services:
  web:
    image: nginx:latest
    deploy:
      resources:
        limits:
          cpus: "0.5"
          memory: "512m"
"#;
    let spec = parse_compose(yaml, "myapp").unwrap();
    let res = &spec.services[0].resources;
    assert!((res.cpus.unwrap() - 0.5).abs() < f64::EPSILON);
    assert_eq!(res.memory_bytes.unwrap(), 512 * 1024 * 1024);
}

#[test]
fn deploy_resource_limits_cpus_as_number() {
    let yaml = r#"
services:
  web:
    image: nginx:latest
    deploy:
      resources:
        limits:
          cpus: 2.0
"#;
    let spec = parse_compose(yaml, "myapp").unwrap();
    assert!((spec.services[0].resources.cpus.unwrap() - 2.0).abs() < f64::EPSILON);
}

#[test]
fn deploy_resource_limits_memory_gigabytes() {
    let yaml = r#"
services:
  web:
    image: nginx:latest
    deploy:
      resources:
        limits:
          memory: "2g"
"#;
    let spec = parse_compose(yaml, "myapp").unwrap();
    assert_eq!(
        spec.services[0].resources.memory_bytes.unwrap(),
        2 * 1024 * 1024 * 1024
    );
}

#[test]
fn deploy_resource_limits_memory_kilobytes() {
    let yaml = r#"
services:
  web:
    image: nginx:latest
    deploy:
      resources:
        limits:
          memory: "256k"
"#;
    let spec = parse_compose(yaml, "myapp").unwrap();
    assert_eq!(spec.services[0].resources.memory_bytes.unwrap(), 256 * 1024);
}

#[test]
fn deploy_empty_is_accepted() {
    let yaml = r#"
services:
  web:
    image: nginx:latest
    deploy:
      resources:
        limits:
"#;
    // Empty limits mapping → no resources set.
    // serde_yml parses `limits:` (no value) as null, not an empty mapping.
    // We should handle this gracefully.
    let result = parse_compose(yaml, "myapp");
    // This might parse `limits` as null, which is fine — returns default.
    assert!(result.is_ok());
    let spec = result.unwrap();
    // Default replicas is 1 (not 0)
    let mut expected = ResourcesSpec::default();
    expected.replicas = 1;
    assert_eq!(spec.services[0].resources, expected);
}

#[test]
fn deploy_no_resources_accepted() {
    let yaml = r#"
services:
  web:
    image: nginx:latest
    deploy:
      resources:
"#;
    // resources with no value is null.
    let result = parse_compose(yaml, "myapp");
    assert!(result.is_ok());
}

#[test]
fn deploy_only_cpus() {
    let yaml = r#"
services:
  web:
    image: nginx:latest
    deploy:
      resources:
        limits:
          cpus: "1.5"
"#;
    let spec = parse_compose(yaml, "myapp").unwrap();
    assert!((spec.services[0].resources.cpus.unwrap() - 1.5).abs() < f64::EPSILON);
    assert!(spec.services[0].resources.memory_bytes.is_none());
}

#[test]
fn deploy_only_memory() {
    let yaml = r#"
services:
  web:
    image: nginx:latest
    deploy:
      resources:
        limits:
          memory: "1g"
"#;
    let spec = parse_compose(yaml, "myapp").unwrap();
    assert!(spec.services[0].resources.cpus.is_none());
    assert_eq!(
        spec.services[0].resources.memory_bytes.unwrap(),
        1024 * 1024 * 1024
    );
}

#[test]
fn reject_deploy_unsupported_limit_key() {
    let yaml = r#"
services:
  web:
    image: nginx:latest
    deploy:
      resources:
        limits:
          devices: []
"#;
    let err = parse_compose(yaml, "myapp").unwrap_err();
    assert!(err.to_string().contains("devices"));
}

#[test]
fn parse_memory_string_variants() {
    assert_eq!(
        parse_memory_string("test", "512m").unwrap(),
        512 * 1024 * 1024
    );
    assert_eq!(
        parse_memory_string("test", "512M").unwrap(),
        512 * 1024 * 1024
    );
    assert_eq!(
        parse_memory_string("test", "1g").unwrap(),
        1024 * 1024 * 1024
    );
    assert_eq!(
        parse_memory_string("test", "1G").unwrap(),
        1024 * 1024 * 1024
    );
    assert_eq!(parse_memory_string("test", "256k").unwrap(), 256 * 1024);
    assert_eq!(parse_memory_string("test", "256K").unwrap(), 256 * 1024);
    assert_eq!(parse_memory_string("test", "1024").unwrap(), 1024);
    assert_eq!(parse_memory_string("test", "1024b").unwrap(), 1024);
    assert!(parse_memory_string("test", "abc").is_err());
    assert!(parse_memory_string("test", "").is_err());
}

// ── Security fields ──────────────────────────────────────────

#[test]
fn cap_add_parses_string_list() {
    let yaml = r#"
services:
  web:
    image: nginx:latest
    cap_add:
      - NET_ADMIN
      - SYS_PTRACE
"#;
    let spec = parse_compose(yaml, "myapp").unwrap();
    assert_eq!(spec.services[0].cap_add, vec!["NET_ADMIN", "SYS_PTRACE"]);
}

#[test]
fn cap_drop_parses_string_list() {
    let yaml = r#"
services:
  web:
    image: nginx:latest
    cap_drop:
      - MKNOD
      - AUDIT_WRITE
"#;
    let spec = parse_compose(yaml, "myapp").unwrap();
    assert_eq!(spec.services[0].cap_drop, vec!["MKNOD", "AUDIT_WRITE"]);
}

#[test]
fn privileged_true() {
    let yaml = r#"
services:
  web:
    image: nginx:latest
    privileged: true
"#;
    let spec = parse_compose(yaml, "myapp").unwrap();
    assert!(spec.services[0].privileged);
}

#[test]
fn privileged_defaults_to_false() {
    let yaml = r#"
services:
  web:
    image: nginx:latest
"#;
    let spec = parse_compose(yaml, "myapp").unwrap();
    assert!(!spec.services[0].privileged);
}

#[test]
fn read_only_true() {
    let yaml = r#"
services:
  web:
    image: nginx:latest
    read_only: true
"#;
    let spec = parse_compose(yaml, "myapp").unwrap();
    assert!(spec.services[0].read_only);
}

#[test]
fn sysctls_mapping_form() {
    let yaml = r#"
services:
  web:
    image: nginx:latest
    sysctls:
      net.core.somaxconn: "1024"
      net.ipv4.tcp_syncookies: "0"
"#;
    let spec = parse_compose(yaml, "myapp").unwrap();
    assert_eq!(spec.services[0].sysctls.len(), 2);
    assert_eq!(spec.services[0].sysctls["net.core.somaxconn"], "1024");
    assert_eq!(spec.services[0].sysctls["net.ipv4.tcp_syncookies"], "0");
}

#[test]
fn sysctls_list_form() {
    let yaml = r#"
services:
  web:
    image: nginx:latest
    sysctls:
      - net.core.somaxconn=1024
      - net.ipv4.tcp_syncookies=0
"#;
    let spec = parse_compose(yaml, "myapp").unwrap();
    assert_eq!(spec.services[0].sysctls.len(), 2);
    assert_eq!(spec.services[0].sysctls["net.core.somaxconn"], "1024");
}

// ── Ulimits ──────────────────────────────────────────────────

#[test]
fn ulimits_single_value() {
    let yaml = r#"
services:
  web:
    image: nginx:latest
    ulimits:
      nofile: 65536
"#;
    let spec = parse_compose(yaml, "myapp").unwrap();
    assert_eq!(spec.services[0].ulimits.len(), 1);
    assert_eq!(spec.services[0].ulimits[0].name, "nofile");
    assert_eq!(spec.services[0].ulimits[0].soft, 65536);
    assert_eq!(spec.services[0].ulimits[0].hard, 65536);
}

#[test]
fn ulimits_soft_hard_form() {
    let yaml = r#"
services:
  web:
    image: nginx:latest
    ulimits:
      nofile:
        soft: 1024
        hard: 65536
"#;
    let spec = parse_compose(yaml, "myapp").unwrap();
    assert_eq!(spec.services[0].ulimits.len(), 1);
    assert_eq!(spec.services[0].ulimits[0].name, "nofile");
    assert_eq!(spec.services[0].ulimits[0].soft, 1024);
    assert_eq!(spec.services[0].ulimits[0].hard, 65536);
}

#[test]
fn ulimits_multiple() {
    let yaml = r#"
services:
  web:
    image: nginx:latest
    ulimits:
      nofile:
        soft: 1024
        hard: 65536
      nproc: 2048
"#;
    let spec = parse_compose(yaml, "myapp").unwrap();
    assert_eq!(spec.services[0].ulimits.len(), 2);
}

#[test]
fn pids_limit_in_deploy() {
    let yaml = r#"
services:
  web:
    image: nginx:latest
    deploy:
      resources:
        limits:
          pids: 100
"#;
    let spec = parse_compose(yaml, "myapp").unwrap();
    assert_eq!(spec.services[0].resources.pids_limit, Some(100));
}

// ── Container identity ───────────────────────────────────────

#[test]
fn container_name_parsed() {
    let yaml = r#"
services:
  web:
    image: nginx:latest
    container_name: my-web-container
"#;
    let spec = parse_compose(yaml, "myapp").unwrap();
    assert_eq!(
        spec.services[0].container_name,
        Some("my-web-container".to_string())
    );
}

#[test]
fn container_name_defaults_to_none() {
    let yaml = r#"
services:
  web:
    image: nginx:latest
"#;
    let spec = parse_compose(yaml, "myapp").unwrap();
    assert_eq!(spec.services[0].container_name, None);
}

#[test]
fn hostname_parsed() {
    let yaml = r#"
services:
  web:
    image: nginx:latest
    hostname: my-web-host
"#;
    let spec = parse_compose(yaml, "myapp").unwrap();
    assert_eq!(spec.services[0].hostname, Some("my-web-host".to_string()));
}

#[test]
fn domainname_parsed() {
    let yaml = r#"
services:
  web:
    image: nginx:latest
    domainname: example.com
"#;
    let spec = parse_compose(yaml, "myapp").unwrap();
    assert_eq!(spec.services[0].domainname, Some("example.com".to_string()));
}

#[test]
fn labels_mapping_form() {
    let yaml = r#"
services:
  web:
    image: nginx:latest
    labels:
      com.example.description: "Web frontend"
      com.example.tier: frontend
"#;
    let spec = parse_compose(yaml, "myapp").unwrap();
    assert_eq!(spec.services[0].labels.len(), 2);
    assert_eq!(
        spec.services[0].labels["com.example.description"],
        "Web frontend"
    );
    assert_eq!(spec.services[0].labels["com.example.tier"], "frontend");
}

#[test]
fn labels_list_form() {
    let yaml = r#"
services:
  web:
    image: nginx:latest
    labels:
      - com.example.description=Web frontend
      - com.example.tier=frontend
"#;
    let spec = parse_compose(yaml, "myapp").unwrap();
    assert_eq!(spec.services[0].labels.len(), 2);
    assert_eq!(
        spec.services[0].labels["com.example.description"],
        "Web frontend"
    );
}

// ── x-vz extension tests ──────────────────────────────────────

#[test]
fn xvz_disk_size_string_gigabytes() {
    let yaml = r#"
services:
  db:
    image: postgres:16
x-vz:
  disk_size: "20g"
"#;
    let spec = parse_compose(yaml, "test").unwrap();
    assert_eq!(spec.disk_size_mb, Some(20 * 1024));
}

#[test]
fn xvz_disk_size_string_megabytes() {
    let yaml = r#"
services:
  db:
    image: postgres:16
x-vz:
  disk_size: "512m"
"#;
    let spec = parse_compose(yaml, "test").unwrap();
    assert_eq!(spec.disk_size_mb, Some(512));
}

#[test]
fn xvz_disk_size_integer() {
    let yaml = r#"
services:
  db:
    image: postgres:16
x-vz:
  disk_size: 1024
"#;
    let spec = parse_compose(yaml, "test").unwrap();
    assert_eq!(spec.disk_size_mb, Some(1024));
}

#[test]
fn xvz_disk_size_absent() {
    let yaml = r#"
services:
  db:
    image: postgres:16
"#;
    let spec = parse_compose(yaml, "test").unwrap();
    assert_eq!(spec.disk_size_mb, None);
}

#[test]
fn xvz_empty_section() {
    let yaml = r#"
services:
  db:
    image: postgres:16
x-vz: {}
"#;
    let spec = parse_compose(yaml, "test").unwrap();
    assert_eq!(spec.disk_size_mb, None);
}

#[test]
fn parse_size_to_mb_variants() {
    assert_eq!(super::parse_size_to_mb("10g"), Some(10 * 1024));
    assert_eq!(super::parse_size_to_mb("10gb"), Some(10 * 1024));
    assert_eq!(super::parse_size_to_mb("512m"), Some(512));
    assert_eq!(super::parse_size_to_mb("512mb"), Some(512));
    assert_eq!(super::parse_size_to_mb("2048k"), Some(2));
    assert_eq!(super::parse_size_to_mb("1024kb"), Some(1));
    assert_eq!(super::parse_size_to_mb("100"), Some(100));
    assert_eq!(super::parse_size_to_mb(""), None);
}

// ── Docker Compose shim entrypoint parity (vz-20i) ──────────────

#[test]
fn entrypoint_list_form() {
    let yaml = r#"
services:
  web:
    image: nginx:latest
    entrypoint: ["/docker-entrypoint.sh", "--flag"]
"#;
    let spec = parse_compose(yaml, "test").unwrap();
    assert_eq!(
        spec.services[0].entrypoint,
        Some(vec![
            "/docker-entrypoint.sh".to_string(),
            "--flag".to_string()
        ])
    );
}

#[test]
fn entrypoint_string_form() {
    let yaml = r#"
services:
  web:
    image: nginx:latest
    entrypoint: /docker-entrypoint.sh --flag
"#;
    let spec = parse_compose(yaml, "test").unwrap();
    assert_eq!(
        spec.services[0].entrypoint,
        Some(vec![
            "/docker-entrypoint.sh".to_string(),
            "--flag".to_string()
        ])
    );
}

#[test]
fn entrypoint_absent_is_none() {
    let yaml = r#"
services:
  web:
    image: nginx:latest
"#;
    let spec = parse_compose(yaml, "test").unwrap();
    assert!(spec.services[0].entrypoint.is_none());
}

#[test]
fn command_list_form() {
    let yaml = r#"
services:
  web:
    image: nginx:latest
    command: ["nginx", "-g", "daemon off;"]
"#;
    let spec = parse_compose(yaml, "test").unwrap();
    assert_eq!(
        spec.services[0].command,
        Some(vec![
            "nginx".to_string(),
            "-g".to_string(),
            "daemon off;".to_string()
        ])
    );
}

#[test]
fn command_absent_is_none() {
    let yaml = r#"
services:
  web:
    image: nginx:latest
"#;
    let spec = parse_compose(yaml, "test").unwrap();
    assert!(spec.services[0].command.is_none());
}

#[test]
fn working_dir_parsed() {
    let yaml = r#"
services:
  web:
    image: node:18
    working_dir: /app/src
"#;
    let spec = parse_compose(yaml, "test").unwrap();
    assert_eq!(spec.services[0].working_dir, Some("/app/src".to_string()));
}

#[test]
fn working_dir_absent_is_none() {
    let yaml = r#"
services:
  web:
    image: nginx:latest
"#;
    let spec = parse_compose(yaml, "test").unwrap();
    assert!(spec.services[0].working_dir.is_none());
}

#[test]
fn user_string_parsed() {
    let yaml = r#"
services:
  web:
    image: nginx:latest
    user: "1000:1000"
"#;
    let spec = parse_compose(yaml, "test").unwrap();
    assert_eq!(spec.services[0].user, Some("1000:1000".to_string()));
}

#[test]
fn user_name_parsed() {
    let yaml = r#"
services:
  web:
    image: nginx:latest
    user: "nginx"
"#;
    let spec = parse_compose(yaml, "test").unwrap();
    assert_eq!(spec.services[0].user, Some("nginx".to_string()));
}

#[test]
fn user_absent_is_none() {
    let yaml = r#"
services:
  web:
    image: nginx:latest
"#;
    let spec = parse_compose(yaml, "test").unwrap();
    assert!(spec.services[0].user.is_none());
}

#[test]
fn env_file_string_accepted() {
    // Without a compose_dir, env_file is accepted but entries are empty.
    let yaml = r#"
services:
  web:
    image: nginx:latest
    env_file: .env
"#;
    let spec = parse_compose(yaml, "test").unwrap();
    // Without compose_dir, env_file entries are not loaded.
    assert!(spec.services[0].environment.is_empty());
}

#[test]
fn env_file_list_accepted() {
    let yaml = r#"
services:
  web:
    image: nginx:latest
    env_file:
      - .env
      - .env.production
"#;
    let spec = parse_compose(yaml, "test").unwrap();
    assert!(spec.services[0].environment.is_empty());
}

#[test]
fn env_file_with_dir_loads_values() {
    let dir = tempfile::tempdir().unwrap();
    let env_path = dir.path().join("app.env");
    std::fs::write(&env_path, "DB_HOST=localhost\nDB_PORT=5432\n").unwrap();

    let yaml = r#"
services:
  web:
    image: nginx:latest
    env_file: app.env
"#;
    let spec = parse_compose_with_dir(yaml, "test", dir.path()).unwrap();
    assert_eq!(
        spec.services[0].environment.get("DB_HOST").unwrap(),
        "localhost"
    );
    assert_eq!(spec.services[0].environment.get("DB_PORT").unwrap(), "5432");
}

#[test]
fn env_file_overridden_by_environment() {
    let dir = tempfile::tempdir().unwrap();
    let env_path = dir.path().join("base.env");
    std::fs::write(&env_path, "PORT=3000\nDEBUG=false\n").unwrap();

    let yaml = r#"
services:
  web:
    image: nginx:latest
    env_file: base.env
    environment:
      PORT: "8080"
"#;
    let spec = parse_compose_with_dir(yaml, "test", dir.path()).unwrap();
    // Explicit environment overrides env_file.
    assert_eq!(spec.services[0].environment.get("PORT").unwrap(), "8080");
    // Non-overridden env_file value is preserved.
    assert_eq!(spec.services[0].environment.get("DEBUG").unwrap(), "false");
}

#[test]
fn labels_mapping_parsed() {
    let yaml = r#"
services:
  web:
    image: nginx:latest
    labels:
      com.example.team: backend
      com.example.version: "1.2.3"
"#;
    let spec = parse_compose(yaml, "test").unwrap();
    assert_eq!(
        spec.services[0].labels.get("com.example.team").unwrap(),
        "backend"
    );
    assert_eq!(
        spec.services[0].labels.get("com.example.version").unwrap(),
        "1.2.3"
    );
}

#[test]
fn labels_list_parsed() {
    let yaml = r#"
services:
  web:
    image: nginx:latest
    labels:
      - com.example.team=backend
      - com.example.version=1.2.3
"#;
    let spec = parse_compose(yaml, "test").unwrap();
    assert_eq!(
        spec.services[0].labels.get("com.example.team").unwrap(),
        "backend"
    );
    assert_eq!(
        spec.services[0].labels.get("com.example.version").unwrap(),
        "1.2.3"
    );
}

#[test]
fn labels_absent_is_empty() {
    let yaml = r#"
services:
  web:
    image: nginx:latest
"#;
    let spec = parse_compose(yaml, "test").unwrap();
    assert!(spec.services[0].labels.is_empty());
}

#[test]
fn restart_no_policy() {
    let yaml = r#"
services:
  web:
    image: nginx:latest
    restart: "no"
"#;
    let spec = parse_compose(yaml, "test").unwrap();
    assert_eq!(spec.services[0].restart_policy, Some(RestartPolicy::No));
}

#[test]
fn restart_always_policy() {
    let yaml = r#"
services:
  web:
    image: nginx:latest
    restart: always
"#;
    let spec = parse_compose(yaml, "test").unwrap();
    assert_eq!(spec.services[0].restart_policy, Some(RestartPolicy::Always));
}

#[test]
fn restart_on_failure_with_retries() {
    let yaml = r#"
services:
  web:
    image: nginx:latest
    restart: "on-failure:3"
"#;
    let spec = parse_compose(yaml, "test").unwrap();
    assert_eq!(
        spec.services[0].restart_policy,
        Some(RestartPolicy::OnFailure {
            max_retries: Some(3)
        })
    );
}

#[test]
fn restart_on_failure_no_retries() {
    let yaml = r#"
services:
  web:
    image: nginx:latest
    restart: on-failure
"#;
    let spec = parse_compose(yaml, "test").unwrap();
    assert_eq!(
        spec.services[0].restart_policy,
        Some(RestartPolicy::OnFailure { max_retries: None })
    );
}

#[test]
fn restart_unless_stopped() {
    let yaml = r#"
services:
  web:
    image: nginx:latest
    restart: unless-stopped
"#;
    let spec = parse_compose(yaml, "test").unwrap();
    assert_eq!(
        spec.services[0].restart_policy,
        Some(RestartPolicy::UnlessStopped)
    );
}

#[test]
fn restart_absent_is_none() {
    let yaml = r#"
services:
  web:
    image: nginx:latest
"#;
    let spec = parse_compose(yaml, "test").unwrap();
    assert!(spec.services[0].restart_policy.is_none());
}

#[test]
fn depends_on_condition_service_healthy() {
    let yaml = r#"
services:
  web:
    image: nginx:latest
    depends_on:
      db:
        condition: service_healthy
  db:
    image: postgres:16
    healthcheck:
      test: ["CMD", "true"]
"#;
    let spec = parse_compose(yaml, "test").unwrap();
    let web = spec.services.iter().find(|s| s.name == "web").unwrap();
    assert_eq!(web.depends_on.len(), 1);
    assert_eq!(web.depends_on[0].service, "db");
    assert_eq!(
        web.depends_on[0].condition,
        DependencyCondition::ServiceHealthy
    );
}

#[test]
fn depends_on_condition_service_started() {
    let yaml = r#"
services:
  web:
    image: nginx:latest
    depends_on:
      db:
        condition: service_started
  db:
    image: postgres:16
"#;
    let spec = parse_compose(yaml, "test").unwrap();
    let web = spec.services.iter().find(|s| s.name == "web").unwrap();
    assert_eq!(
        web.depends_on[0].condition,
        DependencyCondition::ServiceStarted
    );
}

#[test]
fn depends_on_condition_service_completed_successfully() {
    let yaml = r#"
services:
  app:
    image: myapp:latest
    depends_on:
      init:
        condition: service_completed_successfully
  init:
    image: myinit:latest
"#;
    let spec = parse_compose(yaml, "test").unwrap();
    let app = spec.services.iter().find(|s| s.name == "app").unwrap();
    assert_eq!(
        app.depends_on[0].condition,
        DependencyCondition::ServiceCompletedSuccessfully
    );
}

#[test]
fn depends_on_simple_list_defaults_to_started() {
    let yaml = r#"
services:
  web:
    image: nginx:latest
    depends_on:
      - db
      - cache
  db:
    image: postgres:16
  cache:
    image: redis:7
"#;
    let spec = parse_compose(yaml, "test").unwrap();
    let web = spec.services.iter().find(|s| s.name == "web").unwrap();
    assert_eq!(web.depends_on.len(), 2);
    assert!(
        web.depends_on
            .iter()
            .all(|d| d.condition == DependencyCondition::ServiceStarted)
    );
}

#[test]
fn depends_on_absent_is_empty() {
    let yaml = r#"
services:
  web:
    image: nginx:latest
"#;
    let spec = parse_compose(yaml, "test").unwrap();
    assert!(spec.services[0].depends_on.is_empty());
}

/// Comprehensive compose file exercising all vz-20i parity fields at once.
#[test]
fn full_parity_compose_all_fields() {
    let dir = tempfile::tempdir().unwrap();
    let env_path = dir.path().join("app.env");
    std::fs::write(&env_path, "CACHE_TTL=300\n").unwrap();

    let yaml = r#"
name: parity-test
services:
  web:
    image: myapp:v2
    entrypoint: ["/entrypoint.sh", "--init"]
    command: ["serve", "--port", "8080"]
    working_dir: /app
    user: "1001:1001"
    env_file: app.env
    environment:
      NODE_ENV: production
    labels:
      com.example.tier: frontend
      com.example.managed-by: vz
    restart: on-failure:5
    depends_on:
      db:
        condition: service_healthy
      cache:
        condition: service_started
  db:
    image: postgres:16
    healthcheck:
      test: ["CMD-SHELL", "pg_isready"]
      interval: 10s
      timeout: 5s
      retries: 3
  cache:
    image: redis:7
    restart: always
"#;

    let spec = parse_compose_with_dir(yaml, "fallback", dir.path()).unwrap();
    assert_eq!(spec.name, "parity-test");
    assert_eq!(spec.services.len(), 3);

    // Find services by name (sorted).
    let cache = spec.services.iter().find(|s| s.name == "cache").unwrap();
    let db = spec.services.iter().find(|s| s.name == "db").unwrap();
    let web = spec.services.iter().find(|s| s.name == "web").unwrap();

    // web assertions
    assert_eq!(
        web.entrypoint,
        Some(vec!["/entrypoint.sh".to_string(), "--init".to_string()])
    );
    assert_eq!(
        web.command,
        Some(vec![
            "serve".to_string(),
            "--port".to_string(),
            "8080".to_string()
        ])
    );
    assert_eq!(web.working_dir, Some("/app".to_string()));
    assert_eq!(web.user, Some("1001:1001".to_string()));
    assert_eq!(web.environment.get("NODE_ENV").unwrap(), "production");
    assert_eq!(web.environment.get("CACHE_TTL").unwrap(), "300");
    assert_eq!(web.labels.get("com.example.tier").unwrap(), "frontend");
    assert_eq!(web.labels.get("com.example.managed-by").unwrap(), "vz");
    assert_eq!(
        web.restart_policy,
        Some(RestartPolicy::OnFailure {
            max_retries: Some(5)
        })
    );
    assert_eq!(web.depends_on.len(), 2);
    let db_dep = web.depends_on.iter().find(|d| d.service == "db").unwrap();
    assert_eq!(db_dep.condition, DependencyCondition::ServiceHealthy);
    let cache_dep = web
        .depends_on
        .iter()
        .find(|d| d.service == "cache")
        .unwrap();
    assert_eq!(cache_dep.condition, DependencyCondition::ServiceStarted);

    // db assertions
    assert!(db.healthcheck.is_some());
    let hc = db.healthcheck.as_ref().unwrap();
    assert_eq!(hc.test, vec!["CMD-SHELL", "pg_isready"]);
    assert_eq!(hc.interval_secs, Some(10));
    assert_eq!(hc.timeout_secs, Some(5));
    assert_eq!(hc.retries, Some(3));

    // cache assertions
    assert_eq!(cache.restart_policy, Some(RestartPolicy::Always));
}

#[test]
fn logging_driver_none_disallows_options() {
    let yaml = r#"
services:
  web:
    image: nginx:latest
    logging:
      driver: none
      options:
        max-size: "10m"
"#;
    let err = parse_compose(yaml, "test").unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("logging.options"));
    assert!(msg.contains("driver` is `none`"));
}

#[test]
fn logging_driver_none_is_accepted_without_options() {
    let yaml = r#"
services:
  web:
    image: nginx:latest
    logging:
      driver: none
"#;
    let spec = parse_compose(yaml, "test").unwrap();
    let logging = spec.services[0].logging.as_ref().unwrap();
    assert_eq!(logging.driver, "none");
    assert!(logging.options.is_empty());
}

#[test]
fn logging_driver_syslog_is_rejected_as_unsupported() {
    let yaml = r#"
services:
  web:
    image: nginx:latest
    logging:
      driver: syslog
"#;
    let err = parse_compose(yaml, "test").unwrap_err();
    let msg = err.to_string();
    assert!(msg.starts_with("unsupported_operation:"));
    assert!(msg.contains("services.web.logging.driver"));
    assert!(msg.contains("syslog"));
}

#[test]
fn logging_unknown_driver_is_validation_error() {
    let yaml = r#"
services:
  web:
    image: nginx:latest
    logging:
      driver: journald
"#;
    let err = parse_compose(yaml, "test").unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("unsupported logging.driver"));
    assert!(msg.contains("journald"));
}

#[test]
fn logging_json_file_validates_supported_options() {
    let yaml = r#"
services:
  web:
    image: nginx:latest
    logging:
      driver: json-file
      options:
        max-size: "10m"
        max-file: "3"
"#;
    let spec = parse_compose(yaml, "test").unwrap();
    let logging = spec.services[0].logging.as_ref().unwrap();
    assert_eq!(logging.driver, "json-file");
    assert_eq!(
        logging.options.get("max-size").map(String::as_str),
        Some("10m")
    );
    assert_eq!(
        logging.options.get("max-file").map(String::as_str),
        Some("3")
    );
}

#[test]
fn logging_json_file_rejects_labels_option() {
    let yaml = r#"
services:
  web:
    image: nginx:latest
    logging:
      driver: json-file
      options:
        labels: "com.example.team"
"#;
    let err = parse_compose(yaml, "test").unwrap_err();
    let msg = err.to_string();
    assert!(msg.starts_with("unsupported_operation:"));
    assert!(msg.contains("services.web.logging.options.labels"));
    assert!(msg.contains("not supported yet"));
}

#[test]
fn logging_json_file_rejects_tag_option() {
    let yaml = r#"
services:
  web:
    image: nginx:latest
    logging:
      driver: json-file
      options:
        tag: "my-web"
"#;
    let err = parse_compose(yaml, "test").unwrap_err();
    let msg = err.to_string();
    assert!(msg.starts_with("unsupported_operation:"));
    assert!(msg.contains("services.web.logging.options.tag"));
    assert!(msg.contains("not supported yet"));
}

#[test]
fn logging_json_file_rejects_unknown_option_key() {
    let yaml = r#"
services:
  web:
    image: nginx:latest
    logging:
      driver: json-file
      options:
        mode: non-blocking
"#;
    let err = parse_compose(yaml, "test").unwrap_err();
    let msg = err.to_string();
    assert!(msg.starts_with("unsupported_operation:"));
    assert!(msg.contains("services.web.logging.options.mode"));
}
