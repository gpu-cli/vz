use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use hyper_util::rt::TokioIo;
use tokio::net::{UnixListener, UnixStream};
use tokio::task::JoinHandle;
use tonic::transport::{Channel, Endpoint, Uri};
use tower::service_fn;
use tracing::warn;
use vz_linux::LinuxVm;

use super::BuildkitError;

const BUILDKIT_GUEST_TCP_PORT: u16 = 8372;
const BUILDKIT_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

struct SocketCleanupGuard {
    path: PathBuf,
}

impl SocketCleanupGuard {
    fn new(path: PathBuf) -> Self {
        Self { path }
    }
}

impl Drop for SocketCleanupGuard {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

/// Create a direct tonic gRPC channel to guest BuildKit over port-forwarded vsock.
pub async fn create_buildkit_channel(vm: Arc<LinuxVm>) -> Result<Channel, BuildkitError> {
    Endpoint::try_from("http://[::]:50051")
        .map_err(|error| {
            BuildkitError::InvalidConfig(format!(
                "failed to create BuildKit gRPC endpoint: {error}"
            ))
        })?
        .connect_timeout(BUILDKIT_CONNECT_TIMEOUT)
        .connect_with_connector(service_fn(move |_: Uri| {
            let vm = Arc::clone(&vm);
            async move {
                let stream = vm
                    .open_port_forward_stream(BUILDKIT_GUEST_TCP_PORT, "tcp", Some("127.0.0.1"))
                    .await
                    .map_err(io_error_from_linux)?;
                Ok::<_, io::Error>(TokioIo::new(stream))
            }
        }))
        .await
        .map_err(|error| {
            BuildkitError::InvalidConfig(format!(
                "failed to connect BuildKit gRPC channel over vsock: {error}"
            ))
        })
}

/// Start a Unix domain socket proxy that forwards to BuildKit in the guest VM.
///
/// Each accepted Unix connection opens a fresh guest port-forward stream to
/// `buildkitd` (`127.0.0.1:8372`) and relays bytes bidirectionally.
///
/// The returned task runs until aborted or the listener fails. Aborting the task
/// also removes the socket path.
pub async fn start_unix_proxy(
    vm: Arc<LinuxVm>,
    socket_path: impl AsRef<Path>,
) -> Result<JoinHandle<Result<(), BuildkitError>>, BuildkitError> {
    let socket_path = socket_path.as_ref().to_path_buf();
    prepare_socket_path(&socket_path)?;
    let listener = UnixListener::bind(&socket_path)?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&socket_path, std::fs::Permissions::from_mode(0o600))?;
    }

    Ok(tokio::spawn(async move {
        let _cleanup = SocketCleanupGuard::new(socket_path);
        loop {
            let (stream, _addr) = listener.accept().await?;
            let vm = Arc::clone(&vm);
            tokio::spawn(async move {
                if let Err(error) = relay_unix_connection(vm, stream).await {
                    warn!(%error, "BuildKit unix proxy connection relay failed");
                }
            });
        }
    }))
}

fn prepare_socket_path(socket_path: &Path) -> Result<(), BuildkitError> {
    if let Some(parent) = socket_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    if socket_path.exists() {
        std::fs::remove_file(socket_path)?;
    }
    Ok(())
}

async fn relay_unix_connection(
    vm: Arc<LinuxVm>,
    mut host_stream: UnixStream,
) -> Result<(), BuildkitError> {
    let mut guest_stream = vm
        .open_port_forward_stream(BUILDKIT_GUEST_TCP_PORT, "tcp", Some("127.0.0.1"))
        .await
        .map_err(BuildkitError::from)?;

    tokio::io::copy_bidirectional(&mut host_stream, &mut guest_stream).await?;
    Ok(())
}

fn io_error_from_linux(error: vz_linux::LinuxError) -> io::Error {
    io::Error::new(io::ErrorKind::ConnectionAborted, error.to_string())
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use tempfile::tempdir;

    use super::{io_error_from_linux, prepare_socket_path};
    use vz_linux::LinuxError;

    #[test]
    fn prepare_socket_path_replaces_existing_socket_file() {
        let temp = tempdir().unwrap();
        let socket_path = temp.path().join("proxy").join("buildkit.sock");
        std::fs::create_dir_all(socket_path.parent().unwrap()).unwrap();
        std::fs::write(&socket_path, b"stale-socket").unwrap();

        prepare_socket_path(&socket_path).unwrap();

        assert!(socket_path.parent().unwrap().is_dir());
        assert!(!socket_path.exists());
    }

    #[test]
    fn io_error_from_linux_preserves_original_message() {
        let error = io_error_from_linux(LinuxError::InvalidConfig("bad vm".to_string()));
        assert!(error.to_string().contains("bad vm"));
    }
}
