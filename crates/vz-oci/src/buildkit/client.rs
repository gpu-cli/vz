use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use bollard_buildkit_proto::moby::buildkit::v1::control_client::ControlClient;
use bollard_buildkit_proto::moby::buildkit::v1::{
    BytesMessage, Exporter, InfoRequest, InfoResponse, ListWorkersRequest, ListWorkersResponse,
    SolveRequest, StatusRequest, StatusResponse,
};
use tokio::sync::{Mutex, mpsc};
use tokio_stream::wrappers::ReceiverStream;
use tonic::metadata::{Ascii, MetadataKey, MetadataMap, MetadataValue};
use tonic::transport::Channel;
use uuid::Uuid;

/// Errors returned by [`BuildClient`] and [`BuildSession`].
#[derive(Debug, thiserror::Error)]
pub enum BuildClientError {
    #[error(transparent)]
    GrpcStatus(#[from] tonic::Status),

    #[error("build session stream is already attached")]
    SessionAlreadyAttached,

    #[error("build session stream is closed")]
    SessionClosed,
}

/// Target destination for a built image.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BuildOutput {
    /// Push the built image to a registry.
    Registry { name: String },
    /// Export an OCI tarball on the host.
    OciTarball { dest: PathBuf },
    /// Export filesystem output to a local directory.
    Local { dest: PathBuf },
    /// Export as OCI layout for later import into vz image store.
    VzStore { tag: String },
}

/// Build secret forwarding specification.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SecretSpec {
    pub id: String,
    pub source: PathBuf,
}

/// SSH forwarding specification.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SshSpec {
    pub id: String,
    pub socket: Option<PathBuf>,
}

/// Build request mapped to BuildKit `Control.Solve` options.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BuildRequest {
    pub context_dir: PathBuf,
    pub dockerfile: Option<PathBuf>,
    pub target: Option<String>,
    pub build_args: HashMap<String, String>,
    pub output: BuildOutput,
    pub no_cache: bool,
    pub secrets: Vec<SecretSpec>,
    pub ssh: Vec<SshSpec>,
    pub platform: Option<String>,
}

/// Result for a successful `Control.Solve` invocation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BuildResult {
    pub build_ref: String,
    pub exporter_response: HashMap<String, String>,
}

impl BuildRequest {
    /// Convert this request to a BuildKit solve payload.
    pub fn to_solve_request(
        &self,
        build_ref: impl Into<String>,
        session: Option<&str>,
    ) -> SolveRequest {
        let mut frontend_attrs = HashMap::new();
        frontend_attrs.insert("context".to_string(), "local://context".to_string());
        frontend_attrs.insert("dockerfile".to_string(), "local://dockerfile".to_string());

        if let Some(dockerfile) = &self.dockerfile
            && let Some(name) = dockerfile.file_name()
        {
            frontend_attrs.insert("filename".to_string(), name.to_string_lossy().into_owned());
        }
        if let Some(target) = &self.target {
            frontend_attrs.insert("target".to_string(), target.clone());
        }
        if self.no_cache {
            frontend_attrs.insert("no-cache".to_string(), "true".to_string());
        }
        if let Some(platform) = &self.platform {
            frontend_attrs.insert("platform".to_string(), platform.clone());
        }

        for (key, value) in &self.build_args {
            frontend_attrs.insert(format!("build-arg:{key}"), value.clone());
        }
        for secret in &self.secrets {
            frontend_attrs.insert(
                format!("secret:{}", secret.id),
                format!("id={},src={}", secret.id, secret.source.display()),
            );
        }
        for ssh in &self.ssh {
            let value = match &ssh.socket {
                Some(socket) => format!("id={},src={}", ssh.id, socket.display()),
                None => format!("id={}", ssh.id),
            };
            frontend_attrs.insert(format!("ssh:{}", ssh.id), value);
        }

        SolveRequest {
            r#ref: build_ref.into(),
            definition: None,
            exporter_deprecated: String::new(),
            exporter_attrs_deprecated: HashMap::new(),
            session: session.unwrap_or_default().to_string(),
            frontend: "dockerfile.v0".to_string(),
            frontend_attrs,
            cache: None,
            entitlements: Vec::new(),
            frontend_inputs: HashMap::new(),
            internal: false,
            source_policy: None,
            exporters: vec![self.output.as_exporter()],
        }
    }
}

impl BuildOutput {
    fn as_exporter(&self) -> Exporter {
        match self {
            Self::Registry { name } => Exporter {
                r#type: "image".to_string(),
                attrs: HashMap::from([
                    ("name".to_string(), name.clone()),
                    ("push".to_string(), "true".to_string()),
                ]),
            },
            Self::OciTarball { dest } => Exporter {
                r#type: "oci".to_string(),
                attrs: HashMap::from([("dest".to_string(), dest.to_string_lossy().into_owned())]),
            },
            Self::Local { dest } => Exporter {
                r#type: "local".to_string(),
                attrs: HashMap::from([("dest".to_string(), dest.to_string_lossy().into_owned())]),
            },
            Self::VzStore { tag } => Exporter {
                r#type: "oci".to_string(),
                attrs: HashMap::from([("name".to_string(), tag.clone())]),
            },
        }
    }
}

/// Core BuildKit control-plane client over a caller-provided gRPC channel.
#[derive(Clone)]
pub struct BuildClient {
    control: ControlClient<Channel>,
}

impl BuildClient {
    /// Create a client using an existing channel (for example vsock-backed).
    pub fn new(channel: Channel) -> Self {
        Self {
            control: ControlClient::new(channel),
        }
    }

    /// Create and start a BuildKit session stream bound to this control client.
    pub async fn start_session(&mut self) -> Result<BuildSession, BuildClientError> {
        let session = BuildSession::new();
        let outbound_rx = session.take_outbound_receiver().await?;

        let mut request = tonic::Request::new(ReceiverStream::new(outbound_rx));
        append_session_metadata(request.metadata_mut(), &session.metadata());
        let mut response_stream = self.control.session(request).await?.into_inner();

        tokio::spawn(
            async move { while let Ok(Some(_frame)) = response_stream.message().await {} },
        );

        Ok(session)
    }

    /// Run `Control.Info`.
    pub async fn info(&mut self) -> Result<InfoResponse, BuildClientError> {
        let response = self.control.info(InfoRequest {}).await?;
        Ok(response.into_inner())
    }

    /// Run `Control.ListWorkers`.
    pub async fn list_workers(
        &mut self,
        filter: Vec<String>,
    ) -> Result<ListWorkersResponse, BuildClientError> {
        let response = self
            .control
            .list_workers(ListWorkersRequest { filter })
            .await?;
        Ok(response.into_inner())
    }

    /// Submit a `Control.Solve` request, optionally attaching session metadata.
    pub async fn solve(
        &mut self,
        mut request: SolveRequest,
        session: Option<&BuildSession>,
    ) -> Result<BuildResult, BuildClientError> {
        if let Some(session) = session
            && request.session.is_empty()
        {
            request.session = session.id().to_string();
        }

        let mut grpc_request = tonic::Request::new(request);
        if let Some(session) = session {
            append_session_metadata(grpc_request.metadata_mut(), &session.metadata());
        }

        let response = self.control.solve(grpc_request).await?.into_inner();
        Ok(BuildResult {
            build_ref: response
                .exporter_response
                .get("buildx.build.ref")
                .cloned()
                .unwrap_or_default(),
            exporter_response: response.exporter_response,
        })
    }

    /// Build helper that starts a session and submits a solve request.
    pub async fn build(&mut self, request: &BuildRequest) -> Result<BuildResult, BuildClientError> {
        let session = self.start_session().await?;
        let build_ref = format!("build-{}", Uuid::new_v4());
        let solve_request = request.to_solve_request(build_ref.clone(), Some(session.id()));

        let mut result = self.solve(solve_request, Some(&session)).await?;
        if result.build_ref.is_empty() {
            result.build_ref = build_ref;
        }
        Ok(result)
    }

    /// Open a `Control.Status` stream for a build reference.
    pub async fn status(
        &mut self,
        build_ref: impl Into<String>,
    ) -> Result<tonic::Streaming<StatusResponse>, BuildClientError> {
        let response = self
            .control
            .status(StatusRequest {
                r#ref: build_ref.into(),
            })
            .await?;
        Ok(response.into_inner())
    }

    /// Mutable access to the raw generated control client.
    pub fn control_client(&mut self) -> &mut ControlClient<Channel> {
        &mut self.control
    }
}

/// BuildKit session metadata + outbound message sender.
#[derive(Debug, Clone)]
pub struct BuildSession {
    id: String,
    shared_key: String,
    outbound_tx: mpsc::Sender<BytesMessage>,
    outbound_rx: Arc<Mutex<Option<mpsc::Receiver<BytesMessage>>>>,
}

impl BuildSession {
    fn new() -> Self {
        let (outbound_tx, outbound_rx) = mpsc::channel::<BytesMessage>(128);
        Self {
            id: Uuid::new_v4().to_string(),
            shared_key: format!("session-{}", Uuid::new_v4()),
            outbound_tx,
            outbound_rx: Arc::new(Mutex::new(Some(outbound_rx))),
        }
    }

    async fn take_outbound_receiver(
        &self,
    ) -> Result<mpsc::Receiver<BytesMessage>, BuildClientError> {
        let mut guard = self.outbound_rx.lock().await;
        guard.take().ok_or(BuildClientError::SessionAlreadyAttached)
    }

    /// Session identifier to set in `SolveRequest.session`.
    pub fn id(&self) -> &str {
        &self.id
    }

    /// Session metadata headers expected by BuildKit control RPCs.
    pub fn metadata(&self) -> HashMap<String, Vec<String>> {
        let mut metadata = HashMap::new();
        metadata.insert(
            "x-docker-expose-session-uuid".to_string(),
            vec![self.id.clone()],
        );
        metadata.insert(
            "x-docker-expose-session-name".to_string(),
            vec![self.shared_key.clone()],
        );
        metadata.insert(
            "x-docker-expose-session-sharedkey".to_string(),
            vec![self.shared_key.clone()],
        );
        metadata.insert(
            "x-docker-expose-session-grpc-method".to_string(),
            vec![
                "/grpc.health.v1.Health/Check".to_string(),
                "/moby.filesync.v1.FileSync/DiffCopy".to_string(),
                "/moby.filesync.v1.FileSync/TarStream".to_string(),
                "/moby.filesync.v1.Auth/Credentials".to_string(),
                "/moby.filesync.v1.Auth/FetchToken".to_string(),
                "/moby.filesync.v1.Auth/GetTokenAuthority".to_string(),
                "/moby.filesync.v1.Auth/VerifyTokenAuthority".to_string(),
                "/moby.buildkit.secrets.v1.Secrets/GetSecret".to_string(),
            ],
        );
        metadata
    }

    /// Send a raw frame into the session stream.
    pub async fn send(&self, frame: BytesMessage) -> Result<(), BuildClientError> {
        self.outbound_tx
            .send(frame)
            .await
            .map_err(|_| BuildClientError::SessionClosed)
    }
}

fn append_session_metadata(metadata: &mut MetadataMap, values: &HashMap<String, Vec<String>>) {
    for (key, headers) in values {
        let Ok(parsed_key) = key.parse::<MetadataKey<Ascii>>() else {
            continue;
        };
        for header in headers {
            if let Ok(parsed_value) = header.parse::<MetadataValue<Ascii>>() {
                metadata.append(parsed_key.clone(), parsed_value);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use std::collections::HashMap;
    use std::path::PathBuf;

    use super::{BuildOutput, BuildRequest, SecretSpec, SshSpec, append_session_metadata};

    #[test]
    fn append_session_metadata_adds_valid_ascii_headers() {
        let mut metadata = tonic::metadata::MetadataMap::new();
        let mut values = HashMap::new();
        values.insert(
            "x-docker-expose-session-uuid".to_string(),
            vec!["abc".to_string()],
        );
        values.insert(
            "x-docker-expose-session-name".to_string(),
            vec!["name".to_string()],
        );

        append_session_metadata(&mut metadata, &values);

        assert_eq!(metadata.get("x-docker-expose-session-uuid").unwrap(), "abc");
        assert_eq!(
            metadata.get("x-docker-expose-session-name").unwrap(),
            "name"
        );
    }

    #[test]
    fn append_session_metadata_ignores_invalid_keys_or_values() {
        let mut metadata = tonic::metadata::MetadataMap::new();
        let mut values = HashMap::new();
        values.insert("invalid key".to_string(), vec!["abc".to_string()]);
        values.insert(
            "x-docker-expose-session-name".to_string(),
            vec!["\nnot-ascii".to_string()],
        );

        append_session_metadata(&mut metadata, &values);

        assert!(metadata.get("x-docker-expose-session-name").is_none());
        assert!(metadata.get("invalid key").is_none());
    }

    #[test]
    fn build_request_maps_to_expected_frontend_attrs() {
        let request = BuildRequest {
            context_dir: PathBuf::from("/tmp/context"),
            dockerfile: Some(PathBuf::from("subdir/Containerfile")),
            target: Some("prod".to_string()),
            build_args: HashMap::from([("FOO".to_string(), "bar".to_string())]),
            output: BuildOutput::Registry {
                name: "example.com/ns/img:latest".to_string(),
            },
            no_cache: true,
            secrets: vec![SecretSpec {
                id: "npmrc".to_string(),
                source: PathBuf::from("/tmp/.npmrc"),
            }],
            ssh: vec![SshSpec {
                id: "default".to_string(),
                socket: Some(PathBuf::from("/tmp/agent.sock")),
            }],
            platform: Some("linux/arm64".to_string()),
        };

        let solve = request.to_solve_request("build-ref", Some("session-id"));

        assert_eq!(solve.r#ref, "build-ref");
        assert_eq!(solve.session, "session-id");
        assert_eq!(solve.frontend, "dockerfile.v0");
        assert_eq!(
            solve.frontend_attrs.get("filename"),
            Some(&"Containerfile".to_string())
        );
        assert_eq!(
            solve.frontend_attrs.get("build-arg:FOO"),
            Some(&"bar".to_string())
        );
        assert_eq!(
            solve.frontend_attrs.get("secret:npmrc"),
            Some(&"id=npmrc,src=/tmp/.npmrc".to_string())
        );
        assert_eq!(
            solve.frontend_attrs.get("ssh:default"),
            Some(&"id=default,src=/tmp/agent.sock".to_string())
        );
        assert_eq!(solve.exporters.len(), 1);
        assert_eq!(solve.exporters[0].r#type, "image");
        assert_eq!(
            solve.exporters[0].attrs.get("push"),
            Some(&"true".to_string())
        );
    }
}
