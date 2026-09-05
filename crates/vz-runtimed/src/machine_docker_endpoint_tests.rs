use std::os::unix::fs::{FileTypeExt, MetadataExt, PermissionsExt, symlink};
use std::task::{Context, Poll};

use tempfile::TempDir;
use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream, ReadBuf};
use tokio::net::UnixStream;
use tokio::sync::mpsc;
use vz_runtime_contract::{EnvironmentId, MachineId, ProjectId};

use super::*;

fn private_root() -> TempDir {
    let root = tempfile::Builder::new()
        .prefix("vz-dkr-")
        .tempdir_in("/private/tmp")
        .expect("short owned temporary root");
    std::fs::set_permissions(root.path(), std::fs::Permissions::from_mode(0o700))
        .expect("private endpoint root");
    root
}

fn owner(environment: &str, machine: &str) -> ResourceOwner {
    ResourceOwner {
        project_id: ProjectId::new("prj_endpoint_test").expect("Project ID"),
        environment_id: EnvironmentId::new(environment).expect("Environment ID"),
        machine_id: Some(MachineId::new(machine).expect("Machine ID")),
    }
}

fn path(root: &TempDir) -> PathBuf {
    root.path().join("engine.sock")
}

fn identity(path: &Path) -> (u64, u64, u32, u64) {
    let metadata = std::fs::symlink_metadata(path).expect("existing identity");
    (
        metadata.dev(),
        metadata.ino(),
        metadata.mode(),
        metadata.nlink(),
    )
}

fn assert_accounted(receipt: &MachineDockerEndpointShutdown) {
    assert_eq!(
        receipt.accepted_connections,
        receipt.completed_connections + receipt.cancelled_connections + receipt.failed_connections
    );
    assert_eq!(receipt.active_connections, 0);
    assert!(receipt.socket_removed);
}

fn connector() -> (Connector, mpsc::UnboundedReceiver<DuplexStream>) {
    let (sender, receiver) = mpsc::unbounded_channel();
    let connect: Connector = Arc::new(move || {
        let sender = sender.clone();
        Box::pin(async move {
            let (relay, engine) = tokio::io::duplex(4096);
            sender.send(engine).expect("test owns engine receiver");
            Ok(Box::new(relay) as EngineConnection)
        })
    });
    (connect, receiver)
}

#[test]
fn socket_identity_is_stable_and_distinguishes_siblings_and_environments() {
    let root = private_root();
    let first = owner("env_first", "mch_first");
    let first_path = MachineDockerEndpoint::socket_path_for(root.path(), &first).expect("path");
    assert_eq!(
        first_path,
        MachineDockerEndpoint::socket_path_for(root.path(), &first).expect("stable path")
    );
    for other in [
        owner("env_first", "mch_second"),
        owner("env_second", "mch_first"),
    ] {
        assert_ne!(
            first_path,
            MachineDockerEndpoint::socket_path_for(root.path(), &other).expect("distinct path")
        );
    }
    assert_eq!(first_path.parent(), Some(root.path()));
    assert!(first_path.as_os_str().as_bytes().len() < 104);
}

#[test]
fn socket_paths_reject_relative_traversal_and_overlong_names() {
    for invalid in [
        PathBuf::from("relative/engine.sock"),
        PathBuf::from("/private/tmp/../engine.sock"),
        PathBuf::from(format!("/private/tmp/{}", "x".repeat(104))),
    ] {
        assert!(validate_socket_path(&invalid).is_err(), "{invalid:?}");
    }
    let no_machine = ResourceOwner {
        machine_id: None,
        ..owner("env_first", "mch_first")
    };
    assert!(
        MachineDockerEndpoint::socket_path_for(Path::new("/private/tmp"), &no_machine).is_err()
    );
    assert!(
        MachineDockerEndpoint::socket_path_for(
            &PathBuf::from(format!("/private/tmp/{}", "x".repeat(80))),
            &owner("env_first", "mch_first"),
        )
        .is_err()
    );
}

#[tokio::test]
async fn bind_requires_existing_private_nonsymlink_parent() {
    let root = private_root();
    let missing = root.path().join("missing");
    assert!(OwnedSocket::bind(&missing.join("engine.sock")).is_err());
    assert!(!missing.exists());

    for mode in [0o755, 0o750, 0o770] {
        std::fs::set_permissions(root.path(), std::fs::Permissions::from_mode(mode))
            .expect("set nonprivate mode");
        assert!(OwnedSocket::bind(&path(&root)).is_err(), "mode {mode:o}");
        assert!(!path(&root).exists());
        assert_eq!(
            std::fs::metadata(root.path()).expect("root").mode() & 0o777,
            mode
        );
    }
    std::fs::set_permissions(root.path(), std::fs::Permissions::from_mode(0o700))
        .expect("restore private mode");
    let target = root.path().join("actual");
    std::fs::create_dir(&target).expect("actual directory");
    std::fs::set_permissions(&target, std::fs::Permissions::from_mode(0o700))
        .expect("private actual directory");
    let alias = root.path().join("alias");
    symlink(&target, &alias).expect("directory alias");
    assert!(OwnedSocket::bind(&alias.join("engine.sock")).is_err());
    assert!(!target.join("engine.sock").exists());
    assert_eq!(std::fs::read_link(&alias).expect("alias preserved"), target);
}

#[tokio::test]
async fn bind_preserves_existing_regular_file_symlink_and_socket() {
    let root = private_root();
    let socket_path = path(&root);
    std::fs::write(&socket_path, b"foreign endpoint sentinel").expect("foreign file");
    let before = identity(&socket_path);
    assert!(OwnedSocket::bind(&socket_path).is_err());
    assert_eq!(identity(&socket_path), before);
    assert_eq!(
        std::fs::read(&socket_path).expect("sentinel"),
        b"foreign endpoint sentinel"
    );
    std::fs::remove_file(&socket_path).expect("remove owned fixture file");

    let target = root.path().join("target");
    std::fs::write(&target, b"symlink target sentinel").expect("target");
    symlink(&target, &socket_path).expect("foreign symlink");
    let before = identity(&socket_path);
    let target_before = identity(&target);
    assert!(OwnedSocket::bind(&socket_path).is_err());
    assert_eq!(identity(&socket_path), before);
    assert_eq!(identity(&target), target_before);
    assert_eq!(
        std::fs::read(&target).expect("target untouched"),
        b"symlink target sentinel"
    );
    std::fs::remove_file(&socket_path).expect("remove owned fixture symlink");

    let foreign = UnixListener::bind(&socket_path).expect("foreign listener");
    let before = identity(&socket_path);
    assert!(OwnedSocket::bind(&socket_path).is_err());
    assert_eq!(identity(&socket_path), before);
    let host = UnixStream::connect(&socket_path)
        .await
        .expect("foreign listener still usable");
    let (peer, _) = foreign.accept().await.expect("foreign accepts connection");
    drop((host, peer, foreign));
}

#[tokio::test]
async fn bound_socket_is_private_and_exact_removal_is_idempotent() {
    let root = private_root();
    let socket_path = path(&root);
    let mut socket = OwnedSocket::bind(&socket_path).expect("bind endpoint");
    let metadata = std::fs::symlink_metadata(&socket_path).expect("socket metadata");
    assert!(metadata.file_type().is_socket());
    assert_eq!(metadata.mode() & 0o777, 0o600);
    assert_eq!(metadata.uid(), rustix::process::geteuid().as_raw());
    assert_eq!(metadata.nlink(), 1);
    assert!(socket.remove_exact().expect("remove owned socket"));
    assert!(!socket_path.exists());
    assert!(socket.remove_exact().expect("idempotent removal"));
}

#[tokio::test]
async fn replacement_socket_inode_is_never_removed() {
    let root = private_root();
    let socket_path = path(&root);
    let mut original = OwnedSocket::bind(&socket_path).expect("original endpoint");
    let original_identity = identity(&socket_path);
    std::fs::remove_file(&socket_path).expect("replace fixture path");
    let replacement = UnixListener::bind(&socket_path).expect("replacement endpoint");
    let replacement_identity = identity(&socket_path);
    assert_ne!(original_identity.1, replacement_identity.1);
    assert!(original.remove_exact().is_err());
    assert_eq!(identity(&socket_path), replacement_identity);
    drop(original);
    assert_eq!(identity(&socket_path), replacement_identity);
    let host = UnixStream::connect(&socket_path)
        .await
        .expect("replacement still usable");
    let (peer, _) = replacement.accept().await.expect("replacement accepts");
    drop((host, peer, replacement));
}

#[tokio::test]
async fn socket_is_only_published_after_verification_and_connects_after_rename() {
    let root = private_root();
    let socket_path = path(&root);
    let mut staged_path = None;
    let mut phases = Vec::new();
    let mut socket = OwnedSocket::bind_with_checkpoint(&socket_path, |phase, current_path| {
        phases.push(phase);
        if phase != SocketBindPhase::Published {
            assert!(
                !socket_path.exists(),
                "canonical path absent before publication"
            );
            assert_ne!(current_path, socket_path);
            assert!(current_path.as_os_str().as_bytes().len() < 104);
            staged_path = Some(current_path.to_path_buf());
        } else {
            assert_eq!(current_path, socket_path);
            assert_eq!(identity(current_path).2 & 0o777, 0o600);
        }
        Ok(())
    })
    .expect("staged publication");
    assert_eq!(
        phases,
        [
            SocketBindPhase::Bound,
            SocketBindPhase::IdentityCaptured,
            SocketBindPhase::PermissionsApplied,
            SocketBindPhase::BeforePublish,
            SocketBindPhase::Published,
        ]
    );
    assert!(!staged_path.expect("captured staging path").exists());
    assert_eq!(
        std::fs::read_dir(root.path())
            .expect("root entries")
            .count(),
        1
    );
    let mut host = UnixStream::connect(&socket_path)
        .await
        .expect("connect to renamed Mac socket");
    let (mut guest, _) = socket
        .listener
        .as_ref()
        .expect("listener")
        .accept()
        .await
        .expect("accept after rename");
    host.write_all(b"host").await.expect("host writes");
    let mut request = [0; 4];
    guest.read_exact(&mut request).await.expect("guest reads");
    assert_eq!(&request, b"host");
    guest.write_all(b"guest").await.expect("guest writes");
    let mut response = [0; 5];
    host.read_exact(&mut response).await.expect("host reads");
    assert_eq!(&response, b"guest");
    drop((host, guest));
    assert!(socket.remove_exact().expect("cleanup published inode"));
    assert_eq!(
        std::fs::read_dir(root.path()).expect("clean root").count(),
        0
    );
}

#[tokio::test]
async fn unknown_staging_identity_is_retained_and_reported_without_publication() {
    let root = private_root();
    let socket_path = path(&root);
    let error = OwnedSocket::bind_with_checkpoint(&socket_path, |phase, _| {
        if phase == SocketBindPhase::Bound {
            Err(io::Error::other("injected identity lookup failure"))
        } else {
            Ok(())
        }
    })
    .err()
    .expect("identity lookup failed");
    let MachineDockerEndpointError::UnverifiedStaging {
        path: retained,
        source,
    } = &error
    else {
        panic!("expected exact uncertain staging path, got {error}");
    };
    assert_eq!(source.kind(), io::ErrorKind::Other);
    assert_eq!(retained.parent(), Some(root.path()));
    assert_ne!(retained, &socket_path);
    assert!(
        std::fs::symlink_metadata(retained)
            .expect("retained socket")
            .file_type()
            .is_socket()
    );
    assert!(error.to_string().contains(&retained.display().to_string()));
    assert!(error.to_string().contains("preserved"));
    assert!(!socket_path.exists());
    assert!(
        UnixStream::connect(retained).await.is_err(),
        "listener closed on error"
    );
    assert_eq!(
        std::fs::read_dir(root.path())
            .expect("retained entries")
            .count(),
        1
    );
}

#[tokio::test]
async fn every_failure_after_identity_capture_removes_only_owned_socket() {
    for fail_at in [
        SocketBindPhase::IdentityCaptured,
        SocketBindPhase::PermissionsApplied,
        SocketBindPhase::BeforePublish,
        SocketBindPhase::Published,
    ] {
        let root = private_root();
        let socket_path = path(&root);
        let result = OwnedSocket::bind_with_checkpoint(&socket_path, |phase, _| {
            if phase == fail_at {
                Err(io::Error::other("injected guarded failure"))
            } else {
                Ok(())
            }
        });
        assert!(result.is_err(), "phase {fail_at:?}");
        assert!(!socket_path.exists(), "phase {fail_at:?}");
        assert_eq!(
            std::fs::read_dir(root.path()).expect("clean root").count(),
            0,
            "phase {fail_at:?}"
        );
    }
}

#[tokio::test]
async fn no_replace_publication_preserves_a_concurrently_created_final_listener() {
    let root = private_root();
    let socket_path = path(&root);
    let mut foreign_listener = None;
    let mut foreign_identity = None;
    let result = OwnedSocket::bind_with_checkpoint(&socket_path, |phase, _| {
        if phase == SocketBindPhase::BeforePublish {
            foreign_listener = Some(UnixListener::bind(&socket_path)?);
            foreign_identity = Some(identity(&socket_path));
        }
        Ok(())
    });
    assert!(result.is_err());
    assert_eq!(
        identity(&socket_path),
        foreign_identity.expect("foreign identity")
    );
    assert_eq!(
        std::fs::read_dir(root.path())
            .expect("only foreign endpoint")
            .count(),
        1
    );
    let host = UnixStream::connect(&socket_path)
        .await
        .expect("foreign listener survives");
    let (peer, _) = foreign_listener
        .as_ref()
        .expect("foreign listener")
        .accept()
        .await
        .expect("foreign accepts");
    drop((host, peer, foreign_listener));
}

#[tokio::test]
async fn staged_replacements_are_neither_chmodded_published_nor_removed() {
    for replace_at in [
        SocketBindPhase::IdentityCaptured,
        SocketBindPhase::BeforePublish,
    ] {
        let root = private_root();
        let socket_path = path(&root);
        let moved = root.path().join("original.sock");
        let mut replacement = None;
        let mut replacement_identity = None;
        let result = OwnedSocket::bind_with_checkpoint(&socket_path, |phase, stage| {
            if phase == replace_at {
                std::fs::rename(stage, &moved)?;
                std::fs::write(stage, b"replacement sentinel")?;
                std::fs::set_permissions(stage, std::fs::Permissions::from_mode(0o400))?;
                replacement = Some(stage.to_path_buf());
                replacement_identity = Some(identity(stage));
            }
            Ok(())
        });
        assert!(result.is_err(), "phase {replace_at:?}");
        let replacement = replacement.expect("replacement path");
        assert_eq!(
            identity(&replacement),
            replacement_identity.expect("replacement identity")
        );
        assert_eq!(
            std::fs::read(&replacement).expect("replacement preserved"),
            b"replacement sentinel"
        );
        assert!(moved.exists(), "unknown relocated path not removed");
        assert!(!socket_path.exists());
    }
}

#[tokio::test]
async fn overlong_staging_path_is_rejected_before_creating_any_socket() {
    let root = private_root();
    let padding = 99 - root.path().as_os_str().as_bytes().len() - 1;
    let parent = root.path().join("x".repeat(padding));
    std::fs::create_dir(&parent).expect("long private directory");
    std::fs::set_permissions(&parent, std::fs::Permissions::from_mode(0o700))
        .expect("private mode");
    let socket_path = parent.join("s");
    validate_socket_path(&socket_path).expect("canonical path alone is bounded");
    assert!(OwnedSocket::bind(&socket_path).is_err());
    assert_eq!(
        std::fs::read_dir(&parent)
            .expect("unchanged parent")
            .count(),
        0
    );
}

struct DropTrackedStream {
    stream: DuplexStream,
    dropped: Option<oneshot::Sender<()>>,
}

impl AsyncRead for DropTrackedStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.stream).poll_read(context, buffer)
    }
}

impl AsyncWrite for DropTrackedStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.stream).poll_write(context, bytes)
    }

    fn poll_flush(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.stream).poll_flush(context)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.stream).poll_shutdown(context)
    }
}

impl Drop for DropTrackedStream {
    fn drop(&mut self) {
        if let Some(dropped) = self.dropped.take() {
            let _ = dropped.send(());
        }
    }
}

#[tokio::test]
async fn relay_preserves_large_binary_payload_and_delayed_response_after_host_eof() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let root = private_root();
        let socket_path = path(&root);
        let (sender, mut receiver) = mpsc::unbounded_channel();
        let connect: Connector = Arc::new(move || {
            let sender = sender.clone();
            Box::pin(async move {
                let (relay, engine) = tokio::io::duplex(4096);
                let (dropped, observed) = oneshot::channel();
                sender.send((engine, observed)).expect("engine receiver");
                Ok(Box::new(DropTrackedStream {
                    stream: relay,
                    dropped: Some(dropped),
                }) as EngineConnection)
            })
        });
        let endpoint =
            MachineDockerEndpoint::spawn(OwnedSocket::bind(&socket_path).expect("socket"), connect);
        let host = UnixStream::connect(&socket_path)
            .await
            .expect("host connection");
        let (mut engine, dropped) = receiver.recv().await.expect("accepted engine");
        let request: Vec<u8> = (0..1_048_593).map(|index| (index % 256) as u8).collect();
        let response: Vec<u8> = (0..1_572_881)
            .map(|index| (255 - index % 256) as u8)
            .collect();
        let expected_request = request.clone();
        let expected_response = response.clone();
        let guest = tokio::spawn(async move {
            let mut received = Vec::new();
            engine
                .read_to_end(&mut received)
                .await
                .expect("request until half-close");
            assert_eq!(received, expected_request);
            tokio::time::sleep(Duration::from_millis(25)).await;
            engine
                .write_all(&response)
                .await
                .expect("response after host EOF");
            engine.shutdown().await.expect("guest half-close");
        });
        let (mut reader, mut writer) = host.into_split();
        let upload = tokio::spawn(async move {
            writer.write_all(&request).await.expect("binary request");
            writer.shutdown().await.expect("host half-close");
        });
        let mut received = Vec::new();
        reader
            .read_to_end(&mut received)
            .await
            .expect("delayed response");
        assert_eq!(received, expected_response);
        upload.await.expect("upload task");
        guest.await.expect("guest task");
        dropped.await.expect("relay has finished");
        let receipt = endpoint.shutdown().await.expect("shutdown");
        assert_accounted(&receipt);
        assert_eq!(receipt.accepted_connections, 1);
        assert_eq!(receipt.completed_connections, 1);
        assert_eq!(receipt.cancelled_connections, 0);
        assert_eq!(receipt.failed_connections, 0);
        assert!(!socket_path.exists());
    })
    .await
    .expect("bounded half-close relay test");
}

#[tokio::test]
async fn shutdown_cancels_and_joins_every_active_relay() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let root = private_root();
        let socket_path = path(&root);
        let (connect, mut engines) = connector();
        let endpoint =
            MachineDockerEndpoint::spawn(OwnedSocket::bind(&socket_path).expect("socket"), connect);
        let mut hosts = Vec::new();
        let mut peers = Vec::new();
        for _ in 0..3 {
            hosts.push(UnixStream::connect(&socket_path).await.expect("host"));
            peers.push(engines.recv().await.expect("accepted engine"));
        }
        let receipt = endpoint.shutdown().await.expect("joined shutdown");
        assert_accounted(&receipt);
        assert_eq!(receipt.accepted_connections, 3);
        assert_eq!(receipt.cancelled_connections, 3);
        assert_eq!(receipt.completed_connections, 0);
        assert_eq!(receipt.failed_connections, 0);
        for mut engine in peers {
            assert_eq!(
                engine.read(&mut [0; 1]).await.expect("peer EOF after join"),
                0
            );
        }
        assert!(
            engines.recv().await.is_none(),
            "connector released after joins"
        );
        assert!(!socket_path.exists());
        drop(hosts);
    })
    .await
    .expect("bounded cancellation test");
}

#[tokio::test]
async fn failed_connector_is_counted_once_and_endpoint_still_shuts_down() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let root = private_root();
        let socket_path = path(&root);
        let (failed, mut observed) = mpsc::unbounded_channel();
        let connect: Connector = Arc::new(move || {
            let failed = failed.clone();
            Box::pin(async move {
                failed.send(()).expect("failure observer");
                Err(io::Error::new(
                    io::ErrorKind::ConnectionRefused,
                    "expected test failure",
                ))
            })
        });
        let endpoint =
            MachineDockerEndpoint::spawn(OwnedSocket::bind(&socket_path).expect("socket"), connect);
        let mut host = UnixStream::connect(&socket_path).await.expect("host");
        observed.recv().await.expect("connector attempted");
        assert_eq!(
            host.read(&mut [0; 1])
                .await
                .expect("failed connector closes host"),
            0
        );
        let receipt = endpoint
            .shutdown()
            .await
            .expect("shutdown after connector failure");
        assert_accounted(&receipt);
        assert_eq!(receipt.accepted_connections, 1);
        assert_eq!(receipt.failed_connections, 1);
        assert_eq!(receipt.completed_connections, 0);
        assert_eq!(receipt.cancelled_connections, 0);
    })
    .await
    .expect("bounded connector failure test");
}

#[tokio::test]
async fn shutdown_joins_a_connection_still_waiting_for_its_engine() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let root = private_root();
        let socket_path = path(&root);
        let (sender, mut receiver) = mpsc::unbounded_channel();
        let connect: Connector = Arc::new(move || {
            let sender = sender.clone();
            Box::pin(async move {
                let (stream, _peer) = tokio::io::duplex(1);
                let (dropped, observed) = oneshot::channel();
                let _guard = DropTrackedStream {
                    stream,
                    dropped: Some(dropped),
                };
                sender.send(observed).expect("pending connection observer");
                std::future::pending::<io::Result<EngineConnection>>().await
            })
        });
        let endpoint =
            MachineDockerEndpoint::spawn(OwnedSocket::bind(&socket_path).expect("socket"), connect);
        let mut host = UnixStream::connect(&socket_path).await.expect("host");
        let dropped = receiver.recv().await.expect("connector is pending");
        let receipt = endpoint.shutdown().await.expect("join pending connector");
        assert_accounted(&receipt);
        assert_eq!(receipt.accepted_connections, 1);
        assert_eq!(receipt.cancelled_connections, 1);
        assert_eq!(receipt.completed_connections, 0);
        assert_eq!(receipt.failed_connections, 0);
        dropped.await.expect("pending connector future was dropped");
        assert_eq!(host.read(&mut [0; 1]).await.expect("host EOF"), 0);
        assert!(receiver.recv().await.is_none(), "connector released");
    })
    .await
    .expect("bounded pending connector shutdown");
}

#[tokio::test]
async fn endpoint_drop_requests_joined_cleanup_and_releases_connector() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let root = private_root();
        let socket_path = path(&root);
        let (connect, mut engines) = connector();
        let endpoint =
            MachineDockerEndpoint::spawn(OwnedSocket::bind(&socket_path).expect("socket"), connect);
        let host = UnixStream::connect(&socket_path).await.expect("host");
        let mut engine = engines.recv().await.expect("active engine");
        drop(endpoint);
        assert_eq!(
            engine
                .read(&mut [0; 1])
                .await
                .expect("relay cancelled on Drop"),
            0
        );
        assert!(
            engines.recv().await.is_none(),
            "supervisor released connector"
        );
        assert!(
            !socket_path.exists(),
            "socket cleanup precedes connector release"
        );
        drop(host);
    })
    .await
    .expect("bounded Drop cleanup test");
}
