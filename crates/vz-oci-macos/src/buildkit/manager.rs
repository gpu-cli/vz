//! Build manager lifecycle and idempotency state machine.
//!
//! Thread-safety invariant:
//! - `BuildManagerState` is mutated only while holding the shared async mutex.
//!
//! Ordering invariants:
//! - Build event IDs are monotonic per build.
//! - Terminal build states are immutable once reached.
//! - Result digests must be stable once set.

use std::collections::BTreeMap;
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;

use serde::Serialize;
use tokio::sync::{Mutex, mpsc, oneshot};
use vz_runtime_contract::{Build, BuildSpec, BuildState, Event, EventScope};

use crate::RuntimeConfig;

use super::common::unix_timestamp_secs;
use super::pipeline::build_image_with_events;
use super::{
    BuildEvent, BuildLogStream, BuildManagerError, BuildOutput, BuildProgress, BuildRequest,
    BuildResult, BuildkitError,
};

pub(crate) type BuildPipelineFuture =
    Pin<Box<dyn Future<Output = Result<BuildResult, BuildkitError>> + Send>>;
pub(crate) type BuildEventSink = Box<dyn FnMut(BuildEvent) + Send + 'static>;

pub(crate) trait BuildPipeline: Send + Sync {
    fn run(
        &self,
        config: RuntimeConfig,
        request: BuildRequest,
        on_event: BuildEventSink,
    ) -> BuildPipelineFuture;
}

#[derive(Debug, Default)]
struct InGuestBuildPipeline;

impl BuildPipeline for InGuestBuildPipeline {
    fn run(
        &self,
        config: RuntimeConfig,
        request: BuildRequest,
        mut on_event: BuildEventSink,
    ) -> BuildPipelineFuture {
        Box::pin(async move {
            let (tx, rx) = oneshot::channel();
            std::thread::spawn(move || {
                let result = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .map_err(BuildkitError::Io)
                    .and_then(|runtime| {
                        runtime.block_on(async move {
                            build_image_with_events(&config, request, &mut on_event).await
                        })
                    });
                let _ = tx.send(result);
            });

            match rx.await {
                Ok(result) => result,
                Err(_) => Err(BuildkitError::InvalidConfig(
                    "build worker thread exited before returning a result".to_string(),
                )),
            }
        })
    }
}

/// Lightweight async manager for background BuildKit jobs.
#[derive(Clone)]
pub struct BuildManager {
    inner: Arc<BuildManagerInner>,
}

struct BuildManagerInner {
    config: RuntimeConfig,
    runner: Arc<dyn BuildPipeline>,
    state: Mutex<BuildManagerState>,
}

struct BuildManagerState {
    next_build_number: u64,
    builds: BTreeMap<String, BuildRecord>,
    idempotency: BTreeMap<String, IdempotencyReservation>,
}

struct IdempotencyReservation {
    normalized_request: String,
    build_id: String,
}

struct BuildRecord {
    build: Build,
    events: Vec<Event>,
    next_event_id: u64,
    task: Option<tokio::task::JoinHandle<()>>,
}

#[derive(Debug, Serialize)]
struct NormalizedStartBuildRequest<'a> {
    sandbox_id: &'a str,
    context: String,
    dockerfile: String,
    target: Option<String>,
    cache_from: Vec<String>,
    image_tag: Option<String>,
    args: &'a BTreeMap<String, String>,
}

impl Default for BuildManagerState {
    fn default() -> Self {
        Self {
            next_build_number: 1,
            builds: BTreeMap::new(),
            idempotency: BTreeMap::new(),
        }
    }
}

impl BuildManager {
    /// Create a manager that executes builds through the in-guest BuildKit pipeline.
    pub fn new(config: RuntimeConfig) -> Self {
        Self::with_pipeline(config, Arc::new(InGuestBuildPipeline))
    }

    pub(crate) fn with_pipeline(config: RuntimeConfig, runner: Arc<dyn BuildPipeline>) -> Self {
        Self {
            inner: Arc::new(BuildManagerInner {
                config,
                runner,
                state: Mutex::new(BuildManagerState::default()),
            }),
        }
    }
    /// Start a build in the background and return the tracked build object.
    pub async fn start_build(
        &self,
        sandbox_id: impl Into<String>,
        build_spec: BuildSpec,
        idempotency_key: Option<String>,
    ) -> Result<Build, BuildManagerError> {
        let sandbox_id = sandbox_id.into();
        let normalized_request = normalize_start_build_request(&sandbox_id, &build_spec)?;
        let idempotency_key = normalize_idempotency_key(idempotency_key);

        let (build_id, build) = {
            let mut state = self.inner.state.lock().await;

            if let Some(key) = idempotency_key.as_ref() {
                if let Some(existing) = state.idempotency.get(key) {
                    if existing.normalized_request == normalized_request {
                        if let Some(record) = state.builds.get(&existing.build_id) {
                            return Ok(record.build.clone());
                        }
                    } else {
                        return Err(BuildManagerError::IdempotencyConflict {
                            key: key.clone(),
                            existing_build_id: existing.build_id.clone(),
                        });
                    }
                }
            }

            let build_id = format!("build-{}", state.next_build_number);
            state.next_build_number = state.next_build_number.saturating_add(1);

            let build = Build {
                build_id: build_id.clone(),
                sandbox_id: sandbox_id.clone(),
                build_spec: build_spec.clone(),
                state: BuildState::Queued,
                result_digest: None,
                started_at: unix_timestamp_secs(),
                ended_at: None,
            };
            let mut record = BuildRecord::new(build.clone());
            record.append_state_event(BuildState::Queued, None);
            state.builds.insert(build_id.clone(), record);

            if let Some(key) = idempotency_key {
                state.idempotency.insert(
                    key,
                    IdempotencyReservation {
                        normalized_request: normalized_request.clone(),
                        build_id: build_id.clone(),
                    },
                );
            }

            (build_id, build)
        };

        let manager = self.clone();
        let build_id_for_task = build_id.clone();
        let handle = tokio::spawn(async move {
            manager.run_build(build_id_for_task).await;
        });
        self.attach_task_handle(&build_id, handle).await;

        Ok(build)
    }

    /// Return the latest build snapshot.
    pub async fn get_build(&self, build_id: &str) -> Result<Build, BuildManagerError> {
        let state = self.inner.state.lock().await;
        let record =
            state
                .builds
                .get(build_id)
                .ok_or_else(|| BuildManagerError::BuildNotFound {
                    build_id: build_id.to_string(),
                })?;
        Ok(record.build.clone())
    }

    /// Return build events ordered by event ID.
    pub async fn stream_build_events(
        &self,
        build_id: &str,
        after_event_id: Option<u64>,
    ) -> Result<Vec<Event>, BuildManagerError> {
        let state = self.inner.state.lock().await;
        let record =
            state
                .builds
                .get(build_id)
                .ok_or_else(|| BuildManagerError::BuildNotFound {
                    build_id: build_id.to_string(),
                })?;

        let mut events = record
            .events
            .iter()
            .filter(|event| after_event_id.is_none_or(|id| event.event_id > id))
            .cloned()
            .collect::<Vec<_>>();
        events.sort_by_key(|event| event.event_id);
        Ok(events)
    }

    /// Cancel a queued/running build.
    pub async fn cancel_build(&self, build_id: &str) -> Result<Build, BuildManagerError> {
        let mut state = self.inner.state.lock().await;
        let record =
            state
                .builds
                .get_mut(build_id)
                .ok_or_else(|| BuildManagerError::BuildNotFound {
                    build_id: build_id.to_string(),
                })?;

        if record.build.state.is_terminal() {
            return Ok(record.build.clone());
        }

        if let Some(task) = record.task.take() {
            task.abort();
        }

        record.mark_canceled("canceled by caller");
        Ok(record.build.clone())
    }

    async fn attach_task_handle(&self, build_id: &str, task: tokio::task::JoinHandle<()>) {
        let mut state = self.inner.state.lock().await;
        if let Some(record) = state.builds.get_mut(build_id) {
            record.task = Some(task);
        } else {
            task.abort();
        }
    }

    async fn run_build(&self, build_id: String) {
        let request = match self.mark_build_running(&build_id).await {
            Some(request) => request,
            None => {
                self.clear_task_handle(&build_id).await;
                return;
            }
        };

        let (event_tx, event_rx) = mpsc::unbounded_channel::<BuildEvent>();
        let collector = {
            let manager = self.clone();
            let build_id = build_id.clone();
            tokio::spawn(async move {
                manager.collect_build_events(build_id, event_rx).await;
            })
        };

        let run_result = self
            .inner
            .runner
            .run(
                self.inner.config.clone(),
                request,
                Box::new(move |event| {
                    let _ = event_tx.send(event);
                }),
            )
            .await;
        let _ = collector.await;

        match run_result {
            Ok(result) => self.complete_build_success(&build_id, result).await,
            Err(error) => {
                self.complete_build_failure(&build_id, error.to_string())
                    .await
            }
        }
        self.clear_task_handle(&build_id).await;
    }

    async fn mark_build_running(&self, build_id: &str) -> Option<BuildRequest> {
        let mut state = self.inner.state.lock().await;
        let record = state.builds.get_mut(build_id)?;
        if record.build.state != BuildState::Queued {
            return None;
        }
        if let Err(error) = record.mark_running() {
            record.mark_failed(format!("failed to mark build running: {error}"));
            return None;
        }
        Some(build_request_from_spec(&record.build))
    }

    async fn collect_build_events(
        &self,
        build_id: String,
        mut events: mpsc::UnboundedReceiver<BuildEvent>,
    ) {
        while let Some(event) = events.recv().await {
            let mut state = self.inner.state.lock().await;
            let Some(record) = state.builds.get_mut(&build_id) else {
                return;
            };
            record.append_build_event(event);
        }
    }

    async fn complete_build_success(&self, build_id: &str, result: BuildResult) {
        let mut state = self.inner.state.lock().await;
        let Some(record) = state.builds.get_mut(build_id) else {
            return;
        };
        if record.build.state.is_terminal() {
            return;
        }

        let Some(digest) = result.image_id.map(|image_id| image_id.0) else {
            record.mark_failed("build completed without a result digest");
            return;
        };

        if let Err(error) = record.mark_succeeded(digest) {
            record.mark_failed(error);
        }
    }

    async fn complete_build_failure(&self, build_id: &str, reason: String) {
        let mut state = self.inner.state.lock().await;
        let Some(record) = state.builds.get_mut(build_id) else {
            return;
        };
        if record.build.state.is_terminal() {
            return;
        }
        record.mark_failed(reason);
    }

    async fn clear_task_handle(&self, build_id: &str) {
        let mut state = self.inner.state.lock().await;
        if let Some(record) = state.builds.get_mut(build_id) {
            record.task = None;
        }
    }
}

impl BuildRecord {
    fn new(build: Build) -> Self {
        Self {
            build,
            events: Vec::new(),
            next_event_id: 1,
            task: None,
        }
    }

    fn append_event(&mut self, event_type: impl Into<String>, payload: BTreeMap<String, String>) {
        let event = Event {
            event_id: self.next_event_id,
            ts: unix_timestamp_secs(),
            scope: EventScope::Build,
            scope_id: self.build.build_id.clone(),
            event_type: event_type.into(),
            payload,
            trace_id: None,
        };
        self.events.push(event);
        self.next_event_id = self.next_event_id.saturating_add(1);
    }

    fn append_state_event(&mut self, state: BuildState, reason: Option<String>) {
        let mut payload = BTreeMap::new();
        payload.insert("state".to_string(), build_state_label(state).to_string());
        if let Some(reason) = reason {
            payload.insert("reason".to_string(), reason);
        }
        self.append_event(format!("build.state.{}", build_state_label(state)), payload);
    }

    fn append_build_event(&mut self, event: BuildEvent) {
        let mut payload = BTreeMap::new();
        let event_type = match event {
            BuildEvent::Status { message } => {
                payload.insert("message".to_string(), message);
                "build.status".to_string()
            }
            BuildEvent::Output { stream, chunk } => {
                payload.insert(
                    "stream".to_string(),
                    match stream {
                        BuildLogStream::Stdout => "stdout",
                        BuildLogStream::Stderr => "stderr",
                    }
                    .to_string(),
                );
                payload.insert(
                    "chunk".to_string(),
                    String::from_utf8_lossy(&chunk).into_owned(),
                );
                "build.output".to_string()
            }
            BuildEvent::SolveStatus { status } => {
                payload.insert("status".to_string(), format!("{status:?}"));
                "build.solve_status".to_string()
            }
            BuildEvent::RawJsonDecodeError { line, error } => {
                payload.insert("line".to_string(), line);
                payload.insert("error".to_string(), error);
                "build.rawjson_decode_error".to_string()
            }
        };
        self.append_event(event_type, payload);
    }

    fn mark_running(&mut self) -> Result<(), String> {
        if self.build.state != BuildState::Queued {
            return Ok(());
        }
        self.build
            .transition_to(BuildState::Running)
            .map_err(|error| error.to_string())?;
        self.build
            .ensure_lifecycle_consistency()
            .map_err(|error| error.to_string())?;
        self.append_state_event(BuildState::Running, None);
        Ok(())
    }

    fn mark_succeeded(&mut self, digest: String) -> Result<(), String> {
        if self.build.state.is_terminal() {
            return Ok(());
        }
        if self.build.state != BuildState::Running {
            return Err(format!(
                "cannot mark build succeeded from {} state",
                build_state_label(self.build.state)
            ));
        }
        ensure_immutable_digest(&digest)?;
        if let Some(existing) = self.build.result_digest.as_ref()
            && existing != &digest
        {
            return Err(format!("result digest changed from {existing} to {digest}"));
        }

        self.build.result_digest = Some(digest);
        self.build
            .transition_to(BuildState::Succeeded)
            .map_err(|error| error.to_string())?;
        self.build.ended_at = Some(unix_timestamp_secs());
        self.build
            .ensure_lifecycle_consistency()
            .map_err(|error| error.to_string())?;
        self.append_state_event(BuildState::Succeeded, None);
        Ok(())
    }

    fn mark_failed(&mut self, reason: impl Into<String>) {
        if self.build.state.is_terminal() {
            return;
        }

        let reason = reason.into();
        if self.build.transition_to(BuildState::Failed).is_ok() {
            self.build.ended_at = Some(unix_timestamp_secs());
        }
        let _ = self.build.ensure_lifecycle_consistency();
        self.append_state_event(BuildState::Failed, Some(reason));
    }

    fn mark_canceled(&mut self, reason: impl Into<String>) {
        if self.build.state.is_terminal() {
            return;
        }

        let reason = reason.into();
        if self.build.transition_to(BuildState::Canceled).is_ok() {
            self.build.ended_at = Some(unix_timestamp_secs());
        }
        let _ = self.build.ensure_lifecycle_consistency();
        self.append_state_event(BuildState::Canceled, Some(reason));
    }
}

fn normalize_start_build_request(
    sandbox_id: &str,
    build_spec: &BuildSpec,
) -> Result<String, BuildManagerError> {
    let request = NormalizedStartBuildRequest {
        sandbox_id: sandbox_id.trim(),
        context: normalize_request_component(&build_spec.context, "."),
        dockerfile: normalize_request_component(
            build_spec.dockerfile.as_deref().unwrap_or("Dockerfile"),
            "Dockerfile",
        ),
        target: normalize_optional_request_component(build_spec.target.as_deref()),
        cache_from: normalize_cache_from_entries(&build_spec.cache_from),
        image_tag: normalize_optional_request_component(build_spec.image_tag.as_deref()),
        args: &build_spec.args,
    };
    serde_json::to_string(&request).map_err(|error| BuildManagerError::RequestNormalization {
        details: error.to_string(),
    })
}

fn normalize_request_component(value: &str, default: &str) -> String {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        default.to_string()
    } else {
        trimmed.to_string()
    }
}

fn normalize_optional_request_component(value: Option<&str>) -> Option<String> {
    value.and_then(|value| {
        let trimmed = value.trim();
        (!trimmed.is_empty()).then(|| trimmed.to_string())
    })
}

fn normalize_cache_from_entries(entries: &[String]) -> Vec<String> {
    entries
        .iter()
        .map(|entry| entry.trim())
        .filter(|entry| !entry.is_empty())
        .map(ToString::to_string)
        .collect()
}

fn normalize_idempotency_key(key: Option<String>) -> Option<String> {
    key.and_then(|key| {
        let normalized = key.trim();
        (!normalized.is_empty()).then(|| normalized.to_string())
    })
}

fn build_request_from_spec(build: &Build) -> BuildRequest {
    let image_tag = normalize_optional_request_component(build.build_spec.image_tag.as_deref())
        .unwrap_or_else(|| format!("vz-build:{}", build.build_id));

    BuildRequest {
        context_dir: PathBuf::from(normalize_request_component(&build.build_spec.context, ".")),
        dockerfile: PathBuf::from(normalize_request_component(
            build
                .build_spec
                .dockerfile
                .as_deref()
                .unwrap_or("Dockerfile"),
            "Dockerfile",
        )),
        tag: image_tag,
        target: normalize_optional_request_component(build.build_spec.target.as_deref()),
        cache_from: normalize_cache_from_entries(&build.build_spec.cache_from),
        build_args: build.build_spec.args.clone(),
        secrets: Vec::new(),
        no_cache: false,
        output: BuildOutput::VzStore,
        progress: BuildProgress::RawJson,
    }
}

fn ensure_immutable_digest(digest: &str) -> Result<(), String> {
    let Some(bytes) = digest.strip_prefix("sha256:") else {
        return Err("result digest must use sha256:<hex> format".to_string());
    };
    if bytes.is_empty() {
        return Err("result digest must include digest bytes".to_string());
    }
    Ok(())
}

fn build_state_label(state: BuildState) -> &'static str {
    match state {
        BuildState::Queued => "queued",
        BuildState::Running => "running",
        BuildState::Succeeded => "succeeded",
        BuildState::Failed => "failed",
        BuildState::Canceled => "canceled",
    }
}
