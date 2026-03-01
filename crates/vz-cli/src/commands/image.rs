//! `vz image` — OCI image management subcommands.

use std::path::Path;

use anyhow::{Context, anyhow, bail};
use clap::{Args, Subcommand};
use reqwest::StatusCode as HttpStatusCode;
use serde::{Deserialize, Serialize};
use vz_runtime_proto::runtime_v2;
use vz_runtimed_client::DaemonClient;

use super::runtime_daemon::{
    ControlPlaneTransport, control_plane_transport, daemon_client_config, default_state_db_path,
    runtime_api_base_url,
};

/// Manage OCI images.
#[derive(Args, Debug)]
pub struct ImageArgs {
    #[command(subcommand)]
    pub action: ImageCommand,
}

#[derive(Subcommand, Debug)]
pub enum ImageCommand {
    /// Pull and cache an OCI image locally.
    Pull(super::oci::PullArgs),

    /// Build a Dockerfile into the local vz image store.
    #[cfg(target_os = "macos")]
    Build(Box<super::build::BuildArgs>),

    /// List cached OCI images.
    Ls(super::oci::ImagesArgs),

    /// Remove stale image and layer artifacts.
    Prune(super::oci::PruneArgs),
}

fn pull_auth_overrides_requested(opts: &super::oci::ContainerOpts) -> bool {
    opts.docker_config || opts.username.is_some() || opts.password.is_some()
}

async fn connect_image_daemon(
    state_db: &Path,
    auto_spawn_override: Option<bool>,
) -> anyhow::Result<DaemonClient> {
    let mut config = daemon_client_config(state_db)?;
    if let Some(auto_spawn) = auto_spawn_override {
        config.auto_spawn = auto_spawn;
    }

    DaemonClient::connect_with_config(config)
        .await
        .with_context(|| {
            format!(
                "failed to connect to vz-runtimed for state db {}",
                state_db.display()
            )
        })
}

#[derive(Debug, Deserialize)]
struct ApiErrorPayload {
    code: String,
    message: String,
    request_id: String,
}

#[derive(Debug, Deserialize)]
struct ApiErrorEnvelope {
    error: ApiErrorPayload,
}

#[derive(Debug, Deserialize)]
struct ApiImagePayload {
    image_ref: String,
    resolved_digest: String,
    platform: String,
    source_registry: String,
    pulled_at: u64,
}

#[derive(Debug, Deserialize)]
struct ApiImageListResponse {
    images: Vec<ApiImagePayload>,
}

#[derive(Debug, Serialize)]
struct ApiPullImageRequest {
    image_ref: String,
}

#[derive(Debug, Deserialize)]
struct ApiPullImageResponse {
    image: ApiImagePayload,
    receipt_id: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ApiPruneImagesResponse {
    removed_refs: u64,
    removed_manifests: u64,
    removed_configs: u64,
    removed_layer_dirs: u64,
    remaining_images: u64,
    receipt_id: Option<String>,
}

fn image_payload_from_api(payload: ApiImagePayload) -> runtime_v2::ImagePayload {
    runtime_v2::ImagePayload {
        image_ref: payload.image_ref,
        resolved_digest: payload.resolved_digest,
        platform: payload.platform,
        source_registry: payload.source_registry,
        pulled_at: payload.pulled_at,
    }
}

fn runtime_api_url(path: &str) -> anyhow::Result<String> {
    let base = runtime_api_base_url()?;
    Ok(format!(
        "{}/{}",
        base.trim_end_matches('/'),
        path.trim_start_matches('/')
    ))
}

async fn api_error_response(response: reqwest::Response, context: &str) -> anyhow::Error {
    let status = response.status();
    let body = response.bytes().await.unwrap_or_default();
    if let Ok(error) = serde_json::from_slice::<ApiErrorEnvelope>(&body) {
        return anyhow!(
            "{context}: api error {} {} (request_id={})",
            error.error.code,
            error.error.message,
            error.error.request_id
        );
    }

    let snippet = String::from_utf8_lossy(&body);
    anyhow!("{context}: api status {status} body={snippet}")
}

async fn api_pull_image(image_ref: &str) -> anyhow::Result<ApiPullImageResponse> {
    let url = runtime_api_url("/v1/images/pull")?;
    let response = reqwest::Client::new()
        .post(url)
        .json(&ApiPullImageRequest {
            image_ref: image_ref.to_string(),
        })
        .send()
        .await
        .context("failed to call api pull image")?;
    if !response.status().is_success() {
        return Err(api_error_response(response, "failed to pull image via api").await);
    }
    response
        .json()
        .await
        .context("failed to decode api pull image response")
}

async fn api_list_images() -> anyhow::Result<Vec<runtime_v2::ImagePayload>> {
    let url = runtime_api_url("/v1/images")?;
    let response = reqwest::Client::new()
        .get(url)
        .send()
        .await
        .context("failed to call api list images")?;
    if !response.status().is_success() {
        return Err(api_error_response(response, "failed to list images via api").await);
    }
    let payload: ApiImageListResponse = response
        .json()
        .await
        .context("failed to decode api list images response")?;
    Ok(payload
        .images
        .into_iter()
        .map(image_payload_from_api)
        .collect())
}

async fn api_prune_images() -> anyhow::Result<ApiPruneImagesResponse> {
    let url = runtime_api_url("/v1/images/prune")?;
    let response = reqwest::Client::new()
        .post(url)
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .body("{}")
        .send()
        .await
        .context("failed to call api prune images")?;
    if response.status() == HttpStatusCode::METHOD_NOT_ALLOWED {
        return Err(anyhow!(
            "api endpoint /v1/images/prune is unavailable; update vz-api to latest daemon-backed image surface"
        ));
    }
    if !response.status().is_success() {
        return Err(api_error_response(response, "failed to prune images via api").await);
    }
    response
        .json()
        .await
        .context("failed to decode api prune images response")
}

fn print_images(images: Vec<runtime_v2::ImagePayload>) {
    if images.is_empty() {
        println!("No images found.");
        return;
    }

    println!(
        "{:<40}  {:<14}  {:<18}  REF",
        "DIGEST", "PLATFORM", "REGISTRY"
    );
    for image in images {
        let digest = if image.resolved_digest.len() > 40 {
            image.resolved_digest[..40].to_string()
        } else {
            image.resolved_digest.clone()
        };
        println!(
            "{:<40}  {:<14}  {:<18}  {}",
            digest, image.platform, image.source_registry, image.image_ref
        );
    }
}

async fn run_pull_stream_for_state_db(
    args: super::oci::PullArgs,
    state_db: &Path,
    auto_spawn_override: Option<bool>,
) -> anyhow::Result<()> {
    if pull_auth_overrides_requested(&args.opts) {
        bail!("registry auth flags are not supported for daemon-backed `vz image pull` yet");
    }

    let mut client = connect_image_daemon(state_db, auto_spawn_override).await?;
    let mut stream = client
        .pull_image(runtime_v2::PullImageRequest {
            image_ref: args.image.clone(),
            metadata: None,
        })
        .await?;

    let mut completion: Option<runtime_v2::PullImageResponse> = None;
    while let Some(event) = stream.message().await? {
        match event.payload {
            Some(runtime_v2::pull_image_event::Payload::Progress(progress)) => {
                println!("[{}] {}", progress.phase, progress.detail);
            }
            Some(runtime_v2::pull_image_event::Payload::Completion(done)) => {
                completion = Some(done);
            }
            None => {}
        }
    }

    let done = completion.context("daemon pull stream ended without terminal completion event")?;
    let image = done
        .image
        .context("daemon pull completion was missing image payload")?;
    println!(
        "Pulled {image_ref} as {digest}",
        image_ref = image.image_ref,
        digest = image.resolved_digest
    );
    if !done.receipt_id.trim().is_empty() {
        println!("Receipt: {}", done.receipt_id);
    }
    Ok(())
}

pub(crate) async fn run_pull_stream(args: super::oci::PullArgs) -> anyhow::Result<()> {
    match control_plane_transport()? {
        ControlPlaneTransport::DaemonGrpc => {
            let state_db = default_state_db_path();
            run_pull_stream_for_state_db(args, &state_db, None).await
        }
        ControlPlaneTransport::ApiHttp => {
            if pull_auth_overrides_requested(&args.opts) {
                bail!("registry auth flags are not supported for api-http `vz image pull` yet");
            }

            let done = api_pull_image(&args.image).await?;
            let image = image_payload_from_api(done.image);
            println!(
                "Pulled {image_ref} as {digest}",
                image_ref = image.image_ref,
                digest = image.resolved_digest
            );
            if let Some(receipt_id) = done.receipt_id
                && !receipt_id.trim().is_empty()
            {
                println!("Receipt: {receipt_id}");
            }
            Ok(())
        }
    }
}

async fn run_prune_stream_for_state_db(
    _args: super::oci::PruneArgs,
    state_db: &Path,
    auto_spawn_override: Option<bool>,
) -> anyhow::Result<()> {
    let mut client = connect_image_daemon(state_db, auto_spawn_override).await?;
    let mut stream = client
        .prune_images(runtime_v2::PruneImagesRequest { metadata: None })
        .await?;

    let mut completion: Option<runtime_v2::PruneImagesResponse> = None;
    while let Some(event) = stream.message().await? {
        match event.payload {
            Some(runtime_v2::prune_images_event::Payload::Progress(progress)) => {
                println!("[{}] {}", progress.phase, progress.detail);
            }
            Some(runtime_v2::prune_images_event::Payload::Completion(done)) => {
                completion = Some(done);
            }
            None => {}
        }
    }

    let done = completion.context("daemon prune stream ended without terminal completion event")?;
    println!(
        "Pruned images: refs={refs}, manifests={manifests}, configs={configs}, layer_dirs={layer_dirs}, remaining={remaining}",
        refs = done.removed_refs,
        manifests = done.removed_manifests,
        configs = done.removed_configs,
        layer_dirs = done.removed_layer_dirs,
        remaining = done.remaining_images
    );
    if !done.receipt_id.trim().is_empty() {
        println!("Receipt: {}", done.receipt_id);
    }
    Ok(())
}

async fn run_prune_stream(args: super::oci::PruneArgs) -> anyhow::Result<()> {
    match control_plane_transport()? {
        ControlPlaneTransport::DaemonGrpc => {
            let state_db = default_state_db_path();
            run_prune_stream_for_state_db(args, &state_db, None).await
        }
        ControlPlaneTransport::ApiHttp => {
            let _ = args;
            let done = api_prune_images().await?;
            println!(
                "Pruned images: refs={refs}, manifests={manifests}, configs={configs}, layer_dirs={layer_dirs}, remaining={remaining}",
                refs = done.removed_refs,
                manifests = done.removed_manifests,
                configs = done.removed_configs,
                layer_dirs = done.removed_layer_dirs,
                remaining = done.remaining_images
            );
            if let Some(receipt_id) = done.receipt_id
                && !receipt_id.trim().is_empty()
            {
                println!("Receipt: {receipt_id}");
            }
            Ok(())
        }
    }
}

/// Run the image subcommand.
pub async fn run(args: ImageArgs) -> anyhow::Result<()> {
    match args.action {
        ImageCommand::Pull(pull_args) => run_pull_stream(pull_args).await,
        #[cfg(target_os = "macos")]
        ImageCommand::Build(build_args) => super::build::run(*build_args).await,
        ImageCommand::Ls(_images_args) => {
            let images = match control_plane_transport()? {
                ControlPlaneTransport::DaemonGrpc => {
                    let state_db = default_state_db_path();
                    let mut client = connect_image_daemon(&state_db, None).await?;
                    client
                        .list_images(runtime_v2::ListImagesRequest { metadata: None })
                        .await?
                        .images
                }
                ControlPlaneTransport::ApiHttp => api_list_images().await?,
            };
            print_images(images);
            Ok(())
        }
        ImageCommand::Prune(prune_args) => run_prune_stream(prune_args).await,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::OsString;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Mutex, OnceLock};

    use crate::commands::oci::{ContainerOpts, ImagesArgs, PruneArgs, PullArgs};
    use axum::{
        Json, Router,
        extract::Json as ExtractJson,
        routing::{get, post},
    };

    static ENV_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

    fn set_env_var(key: &str, value: &str) -> Option<OsString> {
        let previous = std::env::var_os(key);
        // SAFETY: test code serializes env mutation with ENV_LOCK.
        unsafe {
            std::env::set_var(key, value);
        }
        previous
    }

    fn restore_env_var(key: &str, previous: Option<OsString>) {
        // SAFETY: test code serializes env mutation with ENV_LOCK.
        unsafe {
            match previous {
                Some(value) => std::env::set_var(key, value),
                None => std::env::remove_var(key),
            }
        }
    }

    struct ApiHttpEnvGuard {
        previous_transport: Option<OsString>,
        previous_api_base_url: Option<OsString>,
        previous_autostart: Option<OsString>,
        previous_socket: Option<OsString>,
    }

    impl ApiHttpEnvGuard {
        fn new(base_url: &str) -> Self {
            Self {
                previous_transport: set_env_var("VZ_CONTROL_PLANE_TRANSPORT", "api-http"),
                previous_api_base_url: set_env_var("VZ_RUNTIME_API_BASE_URL", base_url),
                previous_autostart: set_env_var("VZ_RUNTIME_DAEMON_AUTOSTART", "0"),
                previous_socket: set_env_var(
                    "VZ_RUNTIME_DAEMON_SOCKET",
                    "/tmp/definitely-does-not-exist.sock",
                ),
            }
        }
    }

    impl Drop for ApiHttpEnvGuard {
        fn drop(&mut self) {
            restore_env_var("VZ_RUNTIME_DAEMON_SOCKET", self.previous_socket.take());
            restore_env_var(
                "VZ_RUNTIME_DAEMON_AUTOSTART",
                self.previous_autostart.take(),
            );
            restore_env_var("VZ_RUNTIME_API_BASE_URL", self.previous_api_base_url.take());
            restore_env_var("VZ_CONTROL_PLANE_TRANSPORT", self.previous_transport.take());
        }
    }

    async fn spawn_test_api_server(
        app: Router,
    ) -> (
        String,
        tokio::sync::oneshot::Sender<()>,
        tokio::task::JoinHandle<std::io::Result<()>>,
    ) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind api test listener");
        let address = listener.local_addr().expect("resolve listener address");
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let server = tokio::spawn(async move {
            axum::serve(listener, app)
                .with_graceful_shutdown(async {
                    let _ = shutdown_rx.await;
                })
                .await
        });
        (format!("http://{address}"), shutdown_tx, server)
    }

    #[tokio::test]
    async fn pull_stream_reports_unreachable_daemon_when_autostart_disabled() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let state_db = tmp.path().join("state").join("stack-state.db");
        let error = run_pull_stream_for_state_db(
            PullArgs {
                image: "alpine:3.20".to_string(),
                opts: ContainerOpts::default(),
            },
            &state_db,
            Some(false),
        )
        .await
        .expect_err("missing daemon socket should fail");

        assert!(
            error
                .to_string()
                .contains("failed to connect to vz-runtimed for state db"),
            "unexpected error: {error:#}"
        );
    }

    #[tokio::test]
    async fn prune_stream_reports_unreachable_daemon_when_autostart_disabled() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let state_db = tmp.path().join("state").join("stack-state.db");
        let error = run_prune_stream_for_state_db(
            PruneArgs {
                opts: ContainerOpts::default(),
            },
            &state_db,
            Some(false),
        )
        .await
        .expect_err("missing daemon socket should fail");

        assert!(
            error
                .to_string()
                .contains("failed to connect to vz-runtimed for state db"),
            "unexpected error: {error:#}"
        );
    }

    #[tokio::test]
    async fn image_ls_uses_api_http_transport_when_configured() {
        let _guard = ENV_LOCK
            .get_or_init(|| Mutex::new(()))
            .lock()
            .expect("lock env mutation guard");

        let app = Router::new().route(
            "/v1/images",
            get(|| async move {
                Json(serde_json::json!({
                    "request_id": "req-test",
                    "images": [],
                }))
            }),
        );
        let (base_url, shutdown_tx, server) = spawn_test_api_server(app).await;
        let _env_guard = ApiHttpEnvGuard::new(&base_url);

        let result = run(ImageArgs {
            action: ImageCommand::Ls(ImagesArgs {
                opts: ContainerOpts::default(),
            }),
        })
        .await;

        let _ = shutdown_tx.send(());
        let _ = server.await;

        assert!(
            result.is_ok(),
            "image ls should succeed over api-http transport: {result:#?}"
        );
    }

    #[tokio::test]
    async fn image_pull_uses_api_http_transport_when_configured() {
        let _guard = ENV_LOCK
            .get_or_init(|| Mutex::new(()))
            .lock()
            .expect("lock env mutation guard");

        let pull_calls = std::sync::Arc::new(AtomicUsize::new(0));
        let pull_calls_clone = pull_calls.clone();
        let app = Router::new().route(
            "/v1/images/pull",
            post(
                move |ExtractJson(payload): ExtractJson<serde_json::Value>| {
                    let pull_calls = pull_calls_clone.clone();
                    async move {
                        pull_calls.fetch_add(1, Ordering::SeqCst);
                        assert_eq!(
                            payload["image_ref"].as_str(),
                            Some("alpine:3.20"),
                            "CLI should pass image_ref in api-http pull body"
                        );
                        Json(serde_json::json!({
                            "request_id": "req-pull",
                            "image": {
                                "image_ref": "alpine:3.20",
                                "resolved_digest": "sha256:abc123",
                                "platform": "linux/arm64",
                                "source_registry": "docker.io",
                                "pulled_at": 1730000000u64
                            },
                            "receipt_id": "rcp-pull-1"
                        }))
                    }
                },
            ),
        );

        let (base_url, shutdown_tx, server) = spawn_test_api_server(app).await;
        let _env_guard = ApiHttpEnvGuard::new(&base_url);

        let result = run(ImageArgs {
            action: ImageCommand::Pull(PullArgs {
                image: "alpine:3.20".to_string(),
                opts: ContainerOpts::default(),
            }),
        })
        .await;

        let _ = shutdown_tx.send(());
        let _ = server.await;

        assert!(
            result.is_ok(),
            "image pull should succeed over api-http transport: {result:#?}"
        );
        assert_eq!(
            pull_calls.load(Ordering::SeqCst),
            1,
            "api-http pull endpoint should be invoked exactly once"
        );
    }

    #[tokio::test]
    async fn image_prune_uses_api_http_transport_when_configured() {
        let _guard = ENV_LOCK
            .get_or_init(|| Mutex::new(()))
            .lock()
            .expect("lock env mutation guard");

        let prune_calls = std::sync::Arc::new(AtomicUsize::new(0));
        let prune_calls_clone = prune_calls.clone();
        let app = Router::new().route(
            "/v1/images/prune",
            post(move || {
                let prune_calls = prune_calls_clone.clone();
                async move {
                    prune_calls.fetch_add(1, Ordering::SeqCst);
                    Json(serde_json::json!({
                        "request_id": "req-prune",
                        "removed_refs": 0u64,
                        "removed_manifests": 0u64,
                        "removed_configs": 0u64,
                        "removed_layer_dirs": 0u64,
                        "remaining_images": 0u64,
                        "receipt_id": "rcp-prune-1"
                    }))
                }
            }),
        );

        let (base_url, shutdown_tx, server) = spawn_test_api_server(app).await;
        let _env_guard = ApiHttpEnvGuard::new(&base_url);

        let result = run(ImageArgs {
            action: ImageCommand::Prune(PruneArgs {
                opts: ContainerOpts::default(),
            }),
        })
        .await;

        let _ = shutdown_tx.send(());
        let _ = server.await;

        assert!(
            result.is_ok(),
            "image prune should succeed over api-http transport: {result:#?}"
        );
        assert_eq!(
            prune_calls.load(Ordering::SeqCst),
            1,
            "api-http prune endpoint should be invoked exactly once"
        );
    }
}
