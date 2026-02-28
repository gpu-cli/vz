use std::collections::HashMap;
use std::pin::Pin;
use std::task::{Context, Poll};

use bollard_buildkit_proto::health::health_check_response::ServingStatus;
use bollard_buildkit_proto::health::health_server::{Health, HealthServer};
use bollard_buildkit_proto::health::{
    HealthCheckRequest, HealthCheckResponse, HealthListRequest, HealthListResponse,
};
use bollard_buildkit_proto::moby::buildkit::v1::BytesMessage;
use bollard_buildkit_proto::moby::filesync::v1::auth_server::AuthServer;
use bollard_buildkit_proto::moby::filesync::v1::file_sync_server::FileSyncServer;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::server::Connected;

use super::auth::DockerAuthProvider;
use super::filesync::LocalFileSync;

/// Callback services served inside a BuildKit session tunnel.
#[derive(Debug, Clone)]
pub struct SessionCallbackServices {
    pub filesync: LocalFileSync,
    pub auth: DockerAuthProvider,
}

/// Errors emitted by callback tunnel execution.
#[derive(Debug, thiserror::Error)]
pub enum SessionTunnelError {
    #[error(transparent)]
    GrpcStatus(#[from] tonic::Status),

    #[error(transparent)]
    Transport(#[from] tonic::transport::Error),

    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[error("session outbound channel closed")]
    OutboundClosed,
}

/// Run callback tunnel over `Control.Session` byte stream.
pub async fn run_session_callback_tunnel(
    inbound: tonic::Streaming<BytesMessage>,
    outbound: mpsc::Sender<BytesMessage>,
    services: SessionCallbackServices,
) -> Result<(), SessionTunnelError> {
    let (bridge_side, server_side) = tokio::io::duplex(1024 * 1024);
    let (bridge_reader, bridge_writer) = tokio::io::split(bridge_side);

    let inbound_pump = pump_inbound_frames(inbound, bridge_writer);
    let outbound_pump = pump_outbound_frames(bridge_reader, outbound);
    let serve = serve_callback_services(server_side, services);

    tokio::try_join!(inbound_pump, outbound_pump, serve)?;
    Ok(())
}

async fn pump_inbound_frames(
    mut inbound: tonic::Streaming<BytesMessage>,
    mut writer: tokio::io::WriteHalf<tokio::io::DuplexStream>,
) -> Result<(), SessionTunnelError> {
    while let Some(frame) = inbound.message().await? {
        writer.write_all(&frame.data).await?;
    }
    writer.shutdown().await?;
    Ok(())
}

async fn pump_outbound_frames(
    mut reader: tokio::io::ReadHalf<tokio::io::DuplexStream>,
    outbound: mpsc::Sender<BytesMessage>,
) -> Result<(), SessionTunnelError> {
    let mut buffer = vec![0_u8; 16 * 1024];
    loop {
        let read = reader.read(&mut buffer).await?;
        if read == 0 {
            break;
        }
        outbound
            .send(BytesMessage {
                data: buffer[..read].to_vec(),
            })
            .await
            .map_err(|_| SessionTunnelError::OutboundClosed)?;
    }
    Ok(())
}

async fn serve_callback_services(
    io: tokio::io::DuplexStream,
    services: SessionCallbackServices,
) -> Result<(), SessionTunnelError> {
    let incoming = tokio_stream::once(Result::<SessionTunnelIo, std::io::Error>::Ok(
        SessionTunnelIo { inner: io },
    ));
    let health = SessionHealthService;

    tonic::transport::Server::builder()
        .add_service(HealthServer::new(health))
        .add_service(FileSyncServer::new(super::filesync::FileSyncService::new(
            services.filesync,
        )))
        .add_service(AuthServer::new(services.auth))
        .serve_with_incoming(incoming)
        .await?;
    Ok(())
}

#[derive(Debug)]
struct SessionTunnelIo {
    inner: tokio::io::DuplexStream,
}

impl Connected for SessionTunnelIo {
    type ConnectInfo = ();

    fn connect_info(&self) -> Self::ConnectInfo {}
}

impl AsyncRead for SessionTunnelIo {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl AsyncWrite for SessionTunnelIo {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, std::io::Error>> {
        Pin::new(&mut self.inner).poll_write(cx, buf)
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

#[derive(Debug)]
struct SessionHealthService;

#[tonic::async_trait]
impl Health for SessionHealthService {
    type WatchStream = ReceiverStream<Result<HealthCheckResponse, tonic::Status>>;

    async fn check(
        &self,
        _request: tonic::Request<HealthCheckRequest>,
    ) -> Result<tonic::Response<HealthCheckResponse>, tonic::Status> {
        Ok(tonic::Response::new(HealthCheckResponse {
            status: ServingStatus::Serving as i32,
        }))
    }

    async fn list(
        &self,
        _request: tonic::Request<HealthListRequest>,
    ) -> Result<tonic::Response<HealthListResponse>, tonic::Status> {
        Ok(tonic::Response::new(HealthListResponse {
            statuses: HashMap::from([(
                "".to_string(),
                HealthCheckResponse {
                    status: ServingStatus::Serving as i32,
                },
            )]),
        }))
    }

    async fn watch(
        &self,
        _request: tonic::Request<HealthCheckRequest>,
    ) -> Result<tonic::Response<Self::WatchStream>, tonic::Status> {
        let (tx, rx) = mpsc::channel(1);
        let _ = tx
            .send(Ok(HealthCheckResponse {
                status: ServingStatus::Serving as i32,
            }))
            .await;
        Ok(tonic::Response::new(ReceiverStream::new(rx)))
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use tokio_stream::StreamExt;

    use super::SessionHealthService;
    use crate::buildkit::session_tunnel::Health;

    #[tokio::test]
    async fn health_service_reports_serving() {
        let health = SessionHealthService;
        let response = health
            .check(tonic::Request::new(
                bollard_buildkit_proto::health::HealthCheckRequest {
                    service: String::new(),
                },
            ))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(
            response.status,
            bollard_buildkit_proto::health::health_check_response::ServingStatus::Serving as i32
        );
    }

    #[tokio::test]
    async fn health_watch_emits_initial_status() {
        let health = SessionHealthService;
        let mut stream = health
            .watch(tonic::Request::new(
                bollard_buildkit_proto::health::HealthCheckRequest {
                    service: String::new(),
                },
            ))
            .await
            .unwrap()
            .into_inner();

        let first = stream.next().await.unwrap().unwrap();
        assert_eq!(
            first.status,
            bollard_buildkit_proto::health::health_check_response::ServingStatus::Serving as i32
        );
    }
}
