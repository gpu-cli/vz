#![allow(clippy::unwrap_used)]

use std::collections::BTreeMap;
use std::fs;
use std::path::Path;
use std::process::Command;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde::Serialize;
use sha2::{Digest, Sha256};
use tempfile::tempdir;
use vz_image::{ImageId, ImageStore};
use vz_runtime_contract::{Build, BuildSpec, BuildState};

use super::*;
use crate::RuntimeConfig;

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct DescriptorJson<'a> {
    media_type: &'a str,
    digest: String,
    size: usize,
}

#[derive(Debug, Serialize)]
struct ManifestJson<'a> {
    schema_version: u8,
    media_type: &'a str,
    config: DescriptorJson<'a>,
    layers: Vec<DescriptorJson<'a>>,
}

#[derive(Debug, Serialize)]
struct IndexJson<'a> {
    schema_version: u8,
    media_type: &'a str,
    manifests: Vec<DescriptorJson<'a>>,
}

fn sha256_digest(data: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(data);
    format!("sha256:{:x}", hasher.finalize())
}

fn write_blob(root: &Path, digest: &str, data: &[u8]) {
    let (algo, value) = digest.split_once(':').unwrap();
    let blob_path = root.join("blobs").join(algo).join(value);
    fs::create_dir_all(blob_path.parent().unwrap()).unwrap();
    fs::write(blob_path, data).unwrap();
}

#[test]
fn progress_mode_maps_to_buildctl_values() {
    assert_eq!(BuildProgress::Auto.as_buildctl_value(), "auto");
    assert_eq!(BuildProgress::Plain.as_buildctl_value(), "plain");
    assert_eq!(BuildProgress::Tty.as_buildctl_value(), "tty");
    assert_eq!(BuildProgress::RawJson.as_buildctl_value(), "rawjson");
}

#[test]
fn parse_dockerfile_registries_extracts_from_lines() {
    let dockerfile = r#"
        FROM --platform=$BUILDPLATFORM golang:1.22 AS builder
        FROM ghcr.io/example/base:latest
        FROM builder AS final
        # FROM should not be parsed in comments
    "#;

    let registries = parse_dockerfile_registries(dockerfile);
    assert!(registries.contains("docker.io"));
    assert!(registries.contains("ghcr.io"));
}

#[test]
fn parse_dockerfile_syntax_registry_extracts_registry() {
    let dockerfile = r#"
        # syntax=docker/dockerfile:1.4
        FROM alpine:3.20
    "#;
    assert_eq!(
        parse_dockerfile_syntax_registry(dockerfile).as_deref(),
        Some("docker.io")
    );
}

#[test]
fn registries_for_build_always_includes_docker_hub_for_frontend() {
    let dockerfile = r#"
        # syntax=docker/dockerfile:1.4
        FROM mcr.microsoft.com/dotnet/sdk:8.0
    "#;
    let request = BuildRequest {
        context_dir: PathBuf::from("."),
        dockerfile: PathBuf::from("Dockerfile"),
        tag: "example:test".to_string(),
        target: None,
        cache_from: Vec::new(),
        build_args: BTreeMap::new(),
        secrets: vec![],
        no_cache: false,
        output: BuildOutput::VzStore,
        progress: BuildProgress::Plain,
    };
    let registries = registries_for_build(dockerfile, &request);
    assert!(registries.contains("docker.io"));
    assert!(registries.contains("mcr.microsoft.com"));
}

#[test]
fn docker_hub_registry_keys_include_helper_and_host_variants() {
    let keys = docker_auth_keys_for_registry("docker.io");
    assert!(keys.iter().any(|k| k == "https://index.docker.io/v1/"));
    assert!(keys.iter().any(|k| k == "docker.io"));
    assert!(keys.iter().any(|k| k == "registry-1.docker.io"));
}

#[tokio::test]
async fn import_oci_tar_writes_store_reference_and_blobs() {
    let tmp = tempdir().unwrap();
    let layout = tmp.path().join("layout");
    fs::create_dir_all(layout.join("blobs/sha256")).unwrap();
    fs::write(
        layout.join("oci-layout"),
        r#"{"imageLayoutVersion":"1.0.0"}"#,
    )
    .unwrap();

    let config_json = br#"{"architecture":"arm64","os":"linux","config":{"Cmd":["echo","ok"]}}"#;
    let config_digest = sha256_digest(config_json);
    write_blob(&layout, &config_digest, config_json);

    let layer_source = tmp.path().join("layer-src");
    fs::create_dir_all(&layer_source).unwrap();
    fs::write(layer_source.join("message.txt"), "hello from layer\n").unwrap();
    let layer_tar = tmp.path().join("layer.tar");
    let tar_status = Command::new("tar")
        .arg("-cf")
        .arg(&layer_tar)
        .arg("-C")
        .arg(&layer_source)
        .arg(".")
        .status()
        .unwrap();
    assert!(tar_status.success());
    let layer_bytes = fs::read(&layer_tar).unwrap();
    let layer_digest = sha256_digest(&layer_bytes);
    write_blob(&layout, &layer_digest, &layer_bytes);

    let manifest = ManifestJson {
        schema_version: 2,
        media_type: "application/vnd.oci.image.manifest.v1+json",
        config: DescriptorJson {
            media_type: "application/vnd.oci.image.config.v1+json",
            digest: config_digest.clone(),
            size: config_json.len(),
        },
        layers: vec![DescriptorJson {
            media_type: "application/vnd.oci.image.layer.v1.tar",
            digest: layer_digest.clone(),
            size: layer_bytes.len(),
        }],
    };
    let manifest_json = serde_json::to_vec(&manifest).unwrap();
    let manifest_digest = sha256_digest(&manifest_json);
    write_blob(&layout, &manifest_digest, &manifest_json);

    let index = IndexJson {
        schema_version: 2,
        media_type: "application/vnd.oci.image.index.v1+json",
        manifests: vec![DescriptorJson {
            media_type: "application/vnd.oci.image.manifest.v1+json",
            digest: manifest_digest.clone(),
            size: manifest_json.len(),
        }],
    };
    fs::write(
        layout.join("index.json"),
        serde_json::to_vec(&index).unwrap(),
    )
    .unwrap();

    let image_tar = tmp.path().join("image.tar");
    let tar_status = Command::new("tar")
        .arg("-cf")
        .arg(&image_tar)
        .arg("-C")
        .arg(&layout)
        .arg(".")
        .status()
        .unwrap();
    assert!(tar_status.success());

    let store = ImageStore::new(tmp.path().join("oci"));
    let imported = import_oci_tar_to_store(&store, &image_tar, "demo:latest")
        .await
        .unwrap();

    assert_eq!(imported.0, manifest_digest);
    assert_eq!(
        store.read_reference("demo:latest").unwrap(),
        manifest_digest
    );
    assert!(store.read_manifest_json(&manifest_digest).is_ok());
    assert!(store.read_config_json(&manifest_digest).is_ok());
    assert!(store.has_layer_blob(&layer_digest));
}

#[derive(Debug)]
enum ScriptedRunResult {
    Success(BuildResult),
}

#[derive(Debug)]
struct ScriptedRun {
    events: Vec<BuildEvent>,
    result: ScriptedRunResult,
}

#[derive(Debug, Default)]
struct ScriptedBuildPipeline {
    runs: AtomicUsize,
    scripted: Mutex<std::collections::VecDeque<ScriptedRun>>,
}

impl ScriptedBuildPipeline {
    fn from_runs(runs: Vec<ScriptedRun>) -> Arc<Self> {
        Arc::new(Self {
            runs: AtomicUsize::new(0),
            scripted: Mutex::new(std::collections::VecDeque::from(runs)),
        })
    }

    fn run_count(&self) -> usize {
        self.runs.load(Ordering::SeqCst)
    }
}

impl BuildPipeline for ScriptedBuildPipeline {
    fn run(
        &self,
        _config: RuntimeConfig,
        _request: BuildRequest,
        mut on_event: BuildEventSink,
    ) -> BuildPipelineFuture {
        self.runs.fetch_add(1, Ordering::SeqCst);
        let run = self
            .scripted
            .lock()
            .unwrap()
            .pop_front()
            .expect("missing scripted build run");

        Box::pin(async move {
            for event in run.events {
                on_event(event);
            }
            match run.result {
                ScriptedRunResult::Success(result) => Ok(result),
            }
        })
    }
}

fn scripted_success_run(digest: Option<&str>, events: Vec<BuildEvent>) -> ScriptedRun {
    ScriptedRun {
        events,
        result: ScriptedRunResult::Success(BuildResult {
            image_id: digest.map(|value| ImageId(value.to_string())),
            tag: "test:latest".to_string(),
            output_path: None,
            pushed: false,
        }),
    }
}

fn test_build_spec(arg_value: &str) -> BuildSpec {
    BuildSpec {
        context: ".".to_string(),
        dockerfile: Some("Dockerfile".to_string()),
        target: None,
        args: BTreeMap::from([("ARG".to_string(), arg_value.to_string())]),
        cache_from: Vec::new(),
        image_tag: None,
        secrets: Vec::new(),
        no_cache: false,
        push: false,
        output_oci_tar_dest: None,
    }
}

async fn wait_for_terminal_build(manager: &BuildManager, build_id: &str) -> Build {
    for _ in 0..120 {
        let build = manager.get_build(build_id).await.unwrap();
        if build.state.is_terminal() {
            return build;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("build did not reach terminal state");
}

#[tokio::test]
async fn build_manager_idempotency_reuses_existing_build() {
    let pipeline =
        ScriptedBuildPipeline::from_runs(vec![scripted_success_run(Some("sha256:abc"), vec![])]);
    let manager = BuildManager::with_pipeline(RuntimeConfig::default(), pipeline.clone());

    let first = manager
        .start_build(
            "sandbox-1",
            test_build_spec("same"),
            Some("idem-same".to_string()),
        )
        .await
        .unwrap();
    let second = manager
        .start_build(
            "sandbox-1",
            test_build_spec("same"),
            Some("idem-same".to_string()),
        )
        .await
        .unwrap();

    assert_eq!(first.build_id, second.build_id);
    let finished = wait_for_terminal_build(&manager, &first.build_id).await;
    assert_eq!(finished.state, BuildState::Succeeded);
    assert_eq!(pipeline.run_count(), 1);
}

#[tokio::test]
async fn build_manager_idempotency_conflict_is_rejected() {
    let pipeline =
        ScriptedBuildPipeline::from_runs(vec![scripted_success_run(Some("sha256:abc"), vec![])]);
    let manager = BuildManager::with_pipeline(RuntimeConfig::default(), pipeline);

    let _first = manager
        .start_build(
            "sandbox-1",
            test_build_spec("first"),
            Some("idem-conflict".to_string()),
        )
        .await
        .unwrap();
    let error = manager
        .start_build(
            "sandbox-1",
            test_build_spec("second"),
            Some("idem-conflict".to_string()),
        )
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        BuildManagerError::IdempotencyConflict { .. }
    ));
}

#[tokio::test]
async fn build_manager_missing_digest_transitions_to_failed() {
    let pipeline = ScriptedBuildPipeline::from_runs(vec![scripted_success_run(None, vec![])]);
    let manager = BuildManager::with_pipeline(RuntimeConfig::default(), pipeline);

    let build = manager
        .start_build("sandbox-1", test_build_spec("digestless"), None)
        .await
        .unwrap();
    let finished = wait_for_terminal_build(&manager, &build.build_id).await;

    assert_eq!(finished.state, BuildState::Failed);
    assert!(finished.ended_at.is_some());
    assert!(finished.result_digest.is_none());
}

#[tokio::test]
async fn build_manager_streams_events_in_order() {
    let pipeline = ScriptedBuildPipeline::from_runs(vec![scripted_success_run(
        Some("sha256:abc"),
        vec![
            BuildEvent::Status {
                message: "phase-1".to_string(),
            },
            BuildEvent::Output {
                stream: BuildLogStream::Stdout,
                chunk: b"hello".to_vec(),
            },
            BuildEvent::Status {
                message: "phase-2".to_string(),
            },
        ],
    )]);
    let manager = BuildManager::with_pipeline(RuntimeConfig::default(), pipeline);

    let build = manager
        .start_build("sandbox-1", test_build_spec("events"), None)
        .await
        .unwrap();
    let finished = wait_for_terminal_build(&manager, &build.build_id).await;
    assert_eq!(finished.state, BuildState::Succeeded);

    let events = manager
        .stream_build_events(&build.build_id, None)
        .await
        .unwrap();
    assert!(events.len() >= 4);
    assert!(
        events
            .windows(2)
            .all(|pair| pair[0].event_id < pair[1].event_id)
    );

    let cursor = events[1].event_id;
    let tail = manager
        .stream_build_events(&build.build_id, Some(cursor))
        .await
        .unwrap();
    assert!(!tail.is_empty());
    assert!(tail.iter().all(|event| event.event_id > cursor));
}
