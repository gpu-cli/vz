//! Private, exact-Machine Docker transport on macOS.
//!
//! A trusted controller supplies the authorized activation and a private host
//! endpoint directory. Same-UID host processes and directory ACL configuration
//! remain trusted, as for the Machine registry. This is transport, not yet the
//! managed-context, host bind translation, or durable endpoint-recovery adapter.

use std::fs::File;
use std::future::Future;
use std::io;
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use rustix::fs::{
    AtFlags, FileType, Mode, RenameFlags, chmodat, fstat, renameat_with, statat, unlinkat,
};
use serde::Serialize;
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::UnixListener;
use tokio::sync::oneshot;
use tokio::task::{JoinHandle, JoinSet};
use vz_oci_macos::{KernelProfile, MacosOciError};
use vz_runtime_contract::{LifecycleOperationId, OwnedResourceKind, ResourceOwner};

use crate::machine_runtime_activation::MachineRuntimeActivation;
use crate::machine_runtime_registry::{
    MachineRuntimeRegistry, MachineRuntimeRegistryError, open_trusted_registry_root,
};

#[derive(Debug, Error)]
pub enum MachineDockerEndpointError {
    #[error("Docker endpoint ownership conflict: {0}")]
    Conflict(String),
    #[error(transparent)]
    Registry(#[from] MachineRuntimeRegistryError),
    #[error(transparent)]
    Runtime(#[from] MacosOciError),
    #[error("Docker endpoint I/O: {0}")]
    Io(#[from] io::Error),
    #[error("Docker endpoint supervisor: {0}")]
    Task(#[from] tokio::task::JoinError),
    #[error(
        "cannot verify Docker staging socket {path}; listener closed and any remaining path preserved: {source}"
    )]
    UnverifiedStaging {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
}

impl From<rustix::io::Errno> for MachineDockerEndpointError {
    fn from(error: rustix::io::Errno) -> Self {
        Self::Io(error.into())
    }
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct MachineDockerEndpointShutdown {
    pub accepted_connections: u64,
    pub completed_connections: u64,
    pub cancelled_connections: u64,
    pub failed_connections: u64,
    pub active_connections: usize,
    pub socket_removed: bool,
}

trait EngineIo: AsyncRead + AsyncWrite + Send + Unpin {}
impl<T: AsyncRead + AsyncWrite + Send + Unpin> EngineIo for T {}
type EngineConnection = Box<dyn EngineIo>;
type Connector = Arc<
    dyn Fn() -> Pin<Box<dyn Future<Output = io::Result<EngineConnection>> + Send>> + Send + Sync,
>;

/// Explicit shutdown waits for every relay before releasing the Machine lease.
/// Drop requests the same bounded teardown; it never leaves untracked clients.
#[must_use = "retain the endpoint and await shutdown before stopping its Machine"]
pub struct MachineDockerEndpoint {
    activation: Option<std::sync::Weak<MachineRuntimeActivation>>,
    socket_path: PathBuf,
    shutdown: Option<oneshot::Sender<()>>,
    task: Option<JoinHandle<Result<MachineDockerEndpointShutdown, MachineDockerEndpointError>>>,
}

impl MachineDockerEndpoint {
    /// Bounded stable logical endpoint identity. There is no mutable global route.
    pub fn socket_path_for(
        root: &Path,
        owner: &ResourceOwner,
    ) -> Result<PathBuf, MachineDockerEndpointError> {
        MachineRuntimeRegistry::<()>::reservation(owner)?;
        let name = owner
            .bounded_resource_name(
                &OwnedResourceKind::Other("docker_endpoint".into()),
                "dkr",
                40,
            )
            .map_err(|error| MachineDockerEndpointError::Conflict(error.to_string()))?;
        let path = root.join(format!("{name}.sock"));
        validate_socket_path(&path)?;
        Ok(path)
    }

    pub async fn start(
        activation: Arc<MachineRuntimeActivation>,
        socket_path: &Path,
    ) -> Result<Self, MachineDockerEndpointError> {
        if activation.verified_profile() != Some(KernelProfile::Developer) {
            return Err(MachineDockerEndpointError::Conflict(
                "only Developer Linux Machines have Docker endpoints".into(),
            ));
        }
        let parent = socket_path
            .parent()
            .ok_or_else(|| MachineDockerEndpointError::Conflict("socket has no parent".into()))?;
        if Self::socket_path_for(parent, activation.owner())? != socket_path {
            return Err(MachineDockerEndpointError::Conflict(
                "endpoint path does not match exact Machine owner".into(),
            ));
        }
        // Refuse existing paths before even lazy-starting this Machine's Engine.
        let _parent = private_parent(socket_path)?;
        require_absent(&_parent, socket_path)?;
        activation.ensure_docker_ready().await?;
        let socket = OwnedSocket::bind(socket_path)?;
        let provenance = Arc::downgrade(&activation);
        let connector: Connector = Arc::new(move || {
            let activation = Arc::clone(&activation);
            Box::pin(async move {
                let stream =
                    tokio::time::timeout(Duration::from_secs(10), activation.open_docker_stream())
                        .await
                        .map_err(|error| io::Error::new(io::ErrorKind::TimedOut, error))?
                        .map_err(|error| {
                            io::Error::new(io::ErrorKind::ConnectionAborted, error.to_string())
                        })?;
                // The supervisor's connector retains its activation through all
                // relay joins. A connection never acquires a replacement by name.
                Ok(Box::new(stream) as EngineConnection)
            })
        });
        let mut endpoint = Self::spawn(socket, connector);
        endpoint.activation = Some(provenance);
        Ok(endpoint)
    }

    pub(crate) fn belongs_to(&self, activation: &Arc<MachineRuntimeActivation>) -> bool {
        self.activation
            .as_ref()
            .is_some_and(|owner| std::sync::Weak::ptr_eq(owner, &Arc::downgrade(activation)))
    }

    fn spawn(socket: OwnedSocket, connector: Connector) -> Self {
        let socket_path = socket.path.clone();
        let (shutdown, stop) = oneshot::channel();
        let task = tokio::spawn(supervise(socket, connector, stop));
        Self {
            activation: None,
            socket_path,
            shutdown: Some(shutdown),
            task: Some(task),
        }
    }

    pub fn socket_path(&self) -> &Path {
        &self.socket_path
    }

    pub async fn shutdown(
        mut self,
    ) -> Result<MachineDockerEndpointShutdown, MachineDockerEndpointError> {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        let task = self.task.take().ok_or_else(|| {
            MachineDockerEndpointError::Conflict("endpoint supervisor missing".into())
        })?;
        task.await?
    }
}

impl Drop for MachineDockerEndpoint {
    fn drop(&mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        // The owned supervisor drains its JoinSet and removes the exact socket.
        // Aborting it here would skip the joined teardown boundary.
    }
}

async fn supervise(
    mut socket: OwnedSocket,
    connector: Connector,
    mut stop: oneshot::Receiver<()>,
) -> Result<MachineDockerEndpointShutdown, MachineDockerEndpointError> {
    let mut clients = JoinSet::new();
    let mut receipt = MachineDockerEndpointShutdown::default();
    let listener = socket
        .listener
        .take()
        .ok_or_else(|| MachineDockerEndpointError::Conflict("listener missing".into()))?;
    let failure = loop {
        tokio::select! {
            biased;
            _ = &mut stop => break None,
            result = clients.join_next(), if !clients.is_empty() => {
                if let Some(result) = result { count_client(result, &mut receipt); }
            }
            accepted = listener.accept(), if clients.len() < 128 => {
                let (mut host, _) = match accepted {
                    Ok(connection) => connection,
                    Err(error) => break Some(error),
                };
                receipt.accepted_connections += 1;
                let connect = Arc::clone(&connector);
                clients.spawn(async move {
                    let mut guest = connect().await?;
                    tokio::io::copy_bidirectional(&mut host, &mut guest).await?;
                    Ok(())
                });
            }
        }
    };
    drop(listener);
    clients.abort_all();
    while let Some(result) = clients.join_next().await {
        count_client(result, &mut receipt);
    }
    receipt.active_connections = clients.len();
    receipt.socket_removed = socket.remove_exact()?;
    drop(connector);
    if let Some(error) = failure {
        return Err(error.into());
    }
    Ok(receipt)
}

fn count_client(
    result: Result<io::Result<()>, tokio::task::JoinError>,
    receipt: &mut MachineDockerEndpointShutdown,
) {
    match result {
        Ok(Ok(())) => receipt.completed_connections += 1,
        Err(error) if error.is_cancelled() => receipt.cancelled_connections += 1,
        Ok(Err(error)) => {
            receipt.failed_connections += 1;
            tracing::warn!(error_kind = ?error.kind(), "Machine Docker relay failed");
        }
        Err(error) => {
            receipt.failed_connections += 1;
            tracing::warn!(
                panicked = error.is_panic(),
                "Machine Docker relay task failed"
            );
        }
    }
}

fn validate_socket_path(path: &Path) -> Result<(), MachineDockerEndpointError> {
    if !path.is_absolute()
        || path.as_os_str().as_bytes().len() >= 104
        || path.components().any(|part| {
            !matches!(
                part,
                std::path::Component::RootDir | std::path::Component::Normal(_)
            )
        })
    {
        return Err(MachineDockerEndpointError::Conflict(
            "endpoint requires a bounded absolute path without traversal".into(),
        ));
    }
    Ok(())
}

fn private_parent(path: &Path) -> Result<File, MachineDockerEndpointError> {
    validate_socket_path(path)?;
    let parent = open_trusted_registry_root(
        path.parent()
            .ok_or_else(|| MachineDockerEndpointError::Conflict("socket has no parent".into()))?,
    )?;
    let stat = fstat(&parent)?;
    if stat.st_uid != rustix::process::geteuid().as_raw()
        || Mode::from_raw_mode(stat.st_mode) != Mode::from_raw_mode(0o700)
    {
        return Err(MachineDockerEndpointError::Conflict(
            "endpoint directory must be effective-user-owned mode 0700".into(),
        ));
    }
    Ok(parent)
}

fn require_absent(parent: &File, path: &Path) -> Result<(), MachineDockerEndpointError> {
    let name = path
        .file_name()
        .ok_or_else(|| MachineDockerEndpointError::Conflict("socket name missing".into()))?;
    match statat(parent, name, AtFlags::SYMLINK_NOFOLLOW) {
        Err(rustix::io::Errno::NOENT) => Ok(()),
        Ok(_) => Err(MachineDockerEndpointError::Conflict(
            "existing endpoint path will not be adopted or replaced".into(),
        )),
        Err(error) => Err(error.into()),
    }
}

struct OwnedSocket {
    path: PathBuf,
    parent: File,
    device: i64,
    inode: u64,
    listener: Option<UnixListener>,
    removed: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SocketBindPhase {
    Bound,
    IdentityCaptured,
    PermissionsApplied,
    BeforePublish,
    Published,
}

impl OwnedSocket {
    fn bind(path: &Path) -> Result<Self, MachineDockerEndpointError> {
        Self::bind_with_checkpoint(path, |_, _| Ok(()))
    }

    fn bind_with_checkpoint(
        path: &Path,
        mut checkpoint: impl FnMut(SocketBindPhase, &Path) -> io::Result<()>,
    ) -> Result<Self, MachineDockerEndpointError> {
        let parent = private_parent(path)?;
        require_absent(&parent, path)?;
        let directory = path
            .parent()
            .ok_or_else(|| MachineDockerEndpointError::Conflict("socket parent missing".into()))?;
        let stage_name = format!(".d-{}", LifecycleOperationId::generate());
        let stage_path = directory.join(&stage_name);
        validate_socket_path(&stage_path)?;
        // The canonical endpoint is never visible until the staged inode and
        // permissions are verified. Other operations must not inspect/adopt an
        // unpublished operation's random staging name in this trusted directory.
        let unverified = |source: io::Error| {
            tracing::warn!(
                staging_path = %stage_path.display(),
                error_kind = ?source.kind(),
                "Unverified Docker staging path preserved; cleanup not certified"
            );
            MachineDockerEndpointError::UnverifiedStaging {
                path: stage_path.clone(),
                source,
            }
        };
        let listener = UnixListener::bind(&stage_path).map_err(&unverified)?;
        checkpoint(SocketBindPhase::Bound, &stage_path).map_err(&unverified)?;
        let stat = statat(&parent, stage_name.as_str(), AtFlags::SYMLINK_NOFOLLOW)
            .map_err(|error| unverified(error.into()))?;
        if !FileType::from_raw_mode(stat.st_mode).is_socket()
            || stat.st_nlink != 1
            || stat.st_uid != rustix::process::geteuid().as_raw()
        {
            return Err(unverified(io::Error::other(
                "bound staging endpoint has unexpected identity",
            )));
        }
        let mut socket = Self {
            path: stage_path,
            parent,
            device: i64::from(stat.st_dev),
            inode: stat.st_ino,
            listener: Some(listener),
            removed: false,
        };
        checkpoint(SocketBindPhase::IdentityCaptured, &socket.path)?;
        socket.verify_identity()?;
        chmodat(
            &socket.parent,
            stage_name.as_str(),
            Mode::from_raw_mode(0o600),
            AtFlags::empty(),
        )?;
        checkpoint(SocketBindPhase::PermissionsApplied, &socket.path)?;
        checkpoint(SocketBindPhase::BeforePublish, &socket.path)?;
        let current = private_parent(path)?;
        let before = fstat(&socket.parent)?;
        let after = fstat(&current)?;
        if (before.st_dev, before.st_ino) != (after.st_dev, after.st_ino) {
            return Err(MachineDockerEndpointError::Conflict(
                "endpoint directory changed during bind".into(),
            ));
        }
        let staged = socket.verify_identity()?;
        if Mode::from_raw_mode(staged.st_mode) != Mode::from_raw_mode(0o600) {
            return Err(MachineDockerEndpointError::Conflict(
                "staged endpoint permissions changed before publication".into(),
            ));
        }
        let name = path
            .file_name()
            .ok_or_else(|| MachineDockerEndpointError::Conflict("socket name missing".into()))?;
        renameat_with(
            &socket.parent,
            stage_name.as_str(),
            &socket.parent,
            name,
            RenameFlags::NOREPLACE,
        )?;
        // Transfer cleanup ownership immediately, before any fallible operation.
        socket.path = path.into();
        checkpoint(SocketBindPhase::Published, &socket.path)?;
        socket.parent.sync_all()?;
        Ok(socket)
    }

    fn verify_identity(&self) -> Result<rustix::fs::Stat, MachineDockerEndpointError> {
        let name = self
            .path
            .file_name()
            .ok_or_else(|| MachineDockerEndpointError::Conflict("socket name missing".into()))?;
        let stat = statat(&self.parent, name, AtFlags::SYMLINK_NOFOLLOW)?;
        if (i64::from(stat.st_dev), stat.st_ino) != (self.device, self.inode)
            || !FileType::from_raw_mode(stat.st_mode).is_socket()
            || stat.st_nlink != 1
            || stat.st_uid != rustix::process::geteuid().as_raw()
        {
            return Err(MachineDockerEndpointError::Conflict(
                "staged endpoint no longer matches the bound socket".into(),
            ));
        }
        Ok(stat)
    }

    fn remove_exact(&mut self) -> Result<bool, MachineDockerEndpointError> {
        if self.removed {
            return Ok(true);
        }
        self.listener.take();
        let name = self
            .path
            .file_name()
            .ok_or_else(|| MachineDockerEndpointError::Conflict("socket name missing".into()))?;
        let stat = match statat(&self.parent, name, AtFlags::SYMLINK_NOFOLLOW) {
            Ok(stat) => stat,
            Err(rustix::io::Errno::NOENT) => {
                self.removed = true;
                return Ok(true);
            }
            Err(error) => return Err(error.into()),
        };
        if (i64::from(stat.st_dev), stat.st_ino) != (self.device, self.inode)
            || !FileType::from_raw_mode(stat.st_mode).is_socket()
        {
            return Err(MachineDockerEndpointError::Conflict(
                "replacement endpoint is not owned by this listener".into(),
            ));
        }
        unlinkat(&self.parent, name, AtFlags::empty())?;
        self.parent.sync_all()?;
        self.removed = true;
        Ok(true)
    }
}

impl Drop for OwnedSocket {
    fn drop(&mut self) {
        if let Err(error) = self.remove_exact() {
            tracing::warn!(
                socket_path = %self.path.display(),
                error = %error,
                "Machine Docker socket cleanup not certified"
            );
        }
    }
}

#[cfg(test)]
#[path = "machine_docker_endpoint_tests.rs"]
mod tests;
