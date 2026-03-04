use std::path::Path;
use std::time::Duration;

use hyper_util::rt::TokioIo;
use tonic::transport::{Channel, Endpoint, Uri};
use tonic::{Code, Status};
use tower::service_fn;

use crate::{DaemonClientError, Result};

pub(crate) async fn connect_channel(socket_path: &Path, timeout: Duration) -> Result<Channel> {
    if !socket_path.exists() {
        return Err(DaemonClientError::Unavailable {
            socket_path: socket_path.to_path_buf(),
            reason: "socket does not exist".to_string(),
        });
    }

    let socket_path_buf = socket_path.to_path_buf();
    let endpoint = Endpoint::try_from("http://[::]:50051")
        .map_err(|error| DaemonClientError::Unavailable {
            socket_path: socket_path.to_path_buf(),
            reason: format!("invalid endpoint: {error}"),
        })?
        .connect_timeout(timeout);

    endpoint
        .connect_with_connector(service_fn(move |_: Uri| {
            let socket_path = socket_path_buf.clone();
            async move {
                tokio::net::UnixStream::connect(socket_path)
                    .await
                    .map(TokioIo::new)
            }
        }))
        .await
        .map_err(|error| DaemonClientError::Unavailable {
            socket_path: socket_path.to_path_buf(),
            reason: error.to_string(),
        })
}

pub(crate) fn status_to_client_error(socket_path: &Path, status: Status) -> DaemonClientError {
    if matches!(
        status.code(),
        Code::Unavailable | Code::DeadlineExceeded | Code::Unknown
    ) {
        return DaemonClientError::Unavailable {
            socket_path: socket_path.to_path_buf(),
            reason: status.to_string(),
        };
    }
    DaemonClientError::Grpc(Box::new(status))
}
