//! Owned opaque Docker relays. No detached directional workers or destinations.

use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_stream::{Stream, StreamExt};
use tonic::Status;
use vz_agent_proto::{
    DockerForwardConnected, DockerForwardEof, DockerForwardFrame, DockerForwardWriteClosed,
    docker_forward_frame::Frame,
};

const MAX_DATA: usize = 65536;

/// Dropping a response cancels its one coordinator and both borrowed halves.
pub struct DockerForwardStream {
    receiver: mpsc::Receiver<Result<DockerForwardFrame, Status>>,
    task: JoinHandle<()>,
}

impl Stream for DockerForwardStream {
    type Item = Result<DockerForwardFrame, Status>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.receiver.poll_recv(cx)
    }
}

impl Drop for DockerForwardStream {
    fn drop(&mut self) {
        self.task.abort();
    }
}

#[cfg(test)]
pub(crate) fn start<S, T>(inbound: S, target: T) -> DockerForwardStream
where
    S: Stream<Item = Result<DockerForwardFrame, Status>> + Send + Unpin + 'static,
    T: AsyncRead + AsyncWrite + Send + Unpin + 'static,
{
    let (sender, receiver) = mpsc::channel(8);
    let task = tokio::spawn(async move {
        let result = tokio::select! {
            () = sender.closed() => return,
            result = relay(inbound, target, &sender) => result,
        };
        if let Err(error) = result {
            let _ = sender.send(Err(error)).await;
        }
    });
    DockerForwardStream { receiver, task }
}

/// A Machine shutdown cancels each relay and waits for its ownership permit to
/// drop before daemon shutdown. The cancellation cannot leave a borrowed half.
pub(crate) fn start_owned<S>(
    inbound: S,
    target: (
        tokio::net::UnixStream,
        tokio::sync::OwnedRwLockReadGuard<()>,
        tokio::sync::watch::Receiver<bool>,
    ),
) -> DockerForwardStream
where
    S: Stream<Item = Result<DockerForwardFrame, Status>> + Send + Unpin + 'static,
{
    let (stream, permit, mut shutdown) = target;
    let (sender, receiver) = mpsc::channel(8);
    let task = tokio::spawn(async move {
        let result = tokio::select! {
            () = sender.closed() => return,
            _ = shutdown.wait_for(|value| *value) => return,
            result = relay(inbound, stream, &sender) => result,
        };
        // relay owns both socket halves; once it returns they are gone. A
        // non-reading response observer must not retain a data-plane permit
        // while the final diagnostic waits behind its full response queue.
        drop(permit);
        if let Err(error) = result {
            let _ = sender.send(Err(error)).await;
        }
    });
    DockerForwardStream { receiver, task }
}

async fn relay<S, T>(
    mut inbound: S,
    target: T,
    sender: &mpsc::Sender<Result<DockerForwardFrame, Status>>,
) -> Result<(), Status>
where
    S: Stream<Item = Result<DockerForwardFrame, Status>> + Unpin,
    T: AsyncRead + AsyncWrite + Unpin,
{
    sender
        .send(Ok(DockerForwardFrame {
            frame: Some(Frame::Connected(DockerForwardConnected {})),
        }))
        .await
        .map_err(|_| Status::cancelled("Docker response closed"))?;
    let (mut reader, mut writer) = tokio::io::split(target);
    let write = async {
        let mut eof = false;
        while let Some(frame) = inbound.next().await {
            match frame?.frame {
                Some(Frame::Data(data)) if !eof && !data.is_empty() && data.len() <= MAX_DATA => {
                    writer
                        .write_all(&data)
                        .await
                        .map_err(|error| Status::unavailable(format!("Docker write: {error}")))?;
                }
                Some(Frame::Eof(_)) if !eof => {
                    writer.shutdown().await.map_err(|error| {
                        Status::unavailable(format!("Docker half-close: {error}"))
                    })?;
                    sender
                        .send(Ok(DockerForwardFrame {
                            frame: Some(Frame::WriteClosed(DockerForwardWriteClosed {})),
                        }))
                        .await
                        .map_err(|_| Status::cancelled("Docker response closed"))?;
                    eof = true;
                }
                _ => {
                    return Err(Status::invalid_argument(
                        "invalid Docker data frame or frame after EOF",
                    ));
                }
            }
        }
        if !eof {
            return Err(Status::cancelled(
                "Docker request ended without directional EOF",
            ));
        }
        Ok(())
    };
    let read = async {
        let mut data = vec![0; MAX_DATA];
        loop {
            let count = reader
                .read(&mut data)
                .await
                .map_err(|error| Status::unavailable(format!("Docker read: {error}")))?;
            let frame = if count == 0 {
                Frame::Eof(DockerForwardEof {})
            } else {
                Frame::Data(data[..count].to_vec())
            };
            sender
                .send(Ok(DockerForwardFrame { frame: Some(frame) }))
                .await
                .map_err(|_| Status::cancelled("Docker response closed"))?;
            if count == 0 {
                return Ok(());
            }
        }
    };
    tokio::try_join!(write, read)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
    use super::*;
    use std::time::Duration;
    use tokio_stream::wrappers::ReceiverStream;

    #[expect(
        clippy::result_large_err,
        reason = "mirrors the tonic stream item type the forwarder consumes"
    )]
    fn packet(frame: Frame) -> Result<DockerForwardFrame, Status> {
        Ok(DockerForwardFrame { frame: Some(frame) })
    }

    #[tokio::test]
    async fn machine_shutdown_closes_forward_and_releases_ownership_permit() {
        let (stream, mut target) = tokio::net::UnixStream::pair().unwrap();
        let gate = std::sync::Arc::new(tokio::sync::RwLock::new(()));
        let permit = std::sync::Arc::clone(&gate).read_owned().await;
        let (shutdown, receiver) = tokio::sync::watch::channel(false);
        let mut response = start_owned(tokio_stream::pending(), (stream, permit, receiver));
        response.next().await.unwrap().unwrap();
        assert!(gate.try_write().is_err());
        shutdown.send_replace(true);
        let _exclusive = tokio::time::timeout(Duration::from_secs(1), gate.write())
            .await
            .unwrap();
        assert_eq!(target.read(&mut [0; 1]).await.unwrap(), 0);
        assert!(response.next().await.is_none());
    }

    #[tokio::test]
    async fn full_error_queue_cannot_retain_forwarding_ownership() {
        tokio::time::timeout(Duration::from_secs(3), async {
            let (stream, mut target) = tokio::net::UnixStream::pair().unwrap();
            let gate = std::sync::Arc::new(tokio::sync::RwLock::new(()));
            let permit = std::sync::Arc::clone(&gate).read_owned().await;
            let (_shutdown, receiver) = tokio::sync::watch::channel(false);
            let (inbound, requests) = mpsc::channel(1);
            let mut response =
                start_owned(ReceiverStream::new(requests), (stream, permit, receiver));
            response.next().await.unwrap().unwrap();
            let writer = tokio::spawn(async move {
                let _ = target.write_all(&vec![1; 2 * 1024 * 1024]).await;
            });
            while response.receiver.len() != 8 {
                tokio::task::yield_now().await;
            }
            inbound
                .send(Ok(DockerForwardFrame { frame: None }))
                .await
                .unwrap();
            let _exclusive = gate.write().await;
            assert_eq!(response.receiver.len(), 8);
            drop(response);
            writer.await.unwrap();
        })
        .await
        .unwrap();
    }

    struct FailingTarget;

    impl AsyncRead for FailingTarget {
        fn poll_read(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            _buf: &mut tokio::io::ReadBuf<'_>,
        ) -> Poll<std::io::Result<()>> {
            Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::ConnectionReset,
                "injected target reset",
            )))
        }
    }

    impl AsyncWrite for FailingTarget {
        fn poll_write(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            bytes: &[u8],
        ) -> Poll<std::io::Result<usize>> {
            Poll::Ready(Ok(bytes.len()))
        }
        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }
        fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    #[tokio::test]
    async fn target_failure_is_not_clean_eof() {
        let mut response = start(tokio_stream::pending(), FailingTarget);
        response.next().await.unwrap().unwrap();
        let error = response.next().await.unwrap().unwrap_err();
        assert_eq!(error.code(), tonic::Code::Unavailable);
        assert!(error.message().contains("injected target reset"));
        assert!(response.next().await.is_none());
    }

    #[tokio::test]
    async fn large_bidirectional_payload_and_delayed_response_after_half_close() {
        tokio::time::timeout(Duration::from_secs(5), async {
            let (host, guest) = tokio::io::duplex(97);
            let (sender, receiver) = mpsc::channel(1);
            let mut response = start(ReceiverStream::new(receiver), host);
            assert!(matches!(
                response.next().await.unwrap().unwrap().frame,
                Some(Frame::Connected(_))
            ));
            let request: Vec<u8> = (0..300_001).map(|index| (index % 251) as u8).collect();
            let reply: Vec<u8> = (0..400_003).map(|index| (index % 239) as u8).collect();
            let expected_request = request.clone();
            let expected_reply = reply.clone();
            let guest_work = async move {
                let (mut reader, mut writer) = tokio::io::split(guest);
                let read = async move {
                    let mut bytes = Vec::new();
                    reader.read_to_end(&mut bytes).await.unwrap();
                    assert_eq!(bytes, expected_request);
                };
                let write = async move {
                    writer.write_all(&reply).await.unwrap();
                    writer.shutdown().await.unwrap();
                };
                tokio::join!(read, write);
            };
            let send = async move {
                for chunk in request.chunks(8191) {
                    sender
                        .send(packet(Frame::Data(chunk.to_vec())))
                        .await
                        .unwrap();
                }
                sender
                    .send(packet(Frame::Eof(DockerForwardEof {})))
                    .await
                    .unwrap();
            };
            let receive = async move {
                let mut bytes = Vec::new();
                let mut eof = false;
                let mut acknowledged = false;
                while let Some(frame) = response.next().await {
                    match frame.unwrap().frame {
                        Some(Frame::Data(data)) => {
                            assert!(!eof);
                            bytes.extend(data);
                        }
                        Some(Frame::Eof(_)) => {
                            assert!(!eof);
                            eof = true;
                        }
                        Some(Frame::WriteClosed(_)) => {
                            assert!(!acknowledged);
                            acknowledged = true;
                        }
                        _ => panic!("unexpected frame"),
                    }
                }
                assert!(eof);
                assert!(acknowledged);
                assert_eq!(bytes, expected_reply);
            };
            tokio::join!(guest_work, send, receive);

            let (host, mut guest) = tokio::io::duplex(32);
            let (sender, receiver) = mpsc::channel(1);
            let mut response = start(ReceiverStream::new(receiver), host);
            response.next().await.unwrap().unwrap();
            sender
                .send(packet(Frame::Data(b"request".to_vec())))
                .await
                .unwrap();
            sender
                .send(packet(Frame::Eof(DockerForwardEof {})))
                .await
                .unwrap();
            drop(sender);
            let mut request = Vec::new();
            guest.read_to_end(&mut request).await.unwrap();
            assert_eq!(request, b"request");
            guest.write_all(b"reply after EOF").await.unwrap();
            guest.shutdown().await.unwrap();
            let mut bytes = Vec::new();
            let mut eof = false;
            let mut acknowledged = false;
            while let Some(frame) = response.next().await {
                match frame.unwrap().frame {
                    Some(Frame::Data(data)) => bytes.extend(data),
                    Some(Frame::Eof(_)) => eof = true,
                    Some(Frame::WriteClosed(_)) => acknowledged = true,
                    _ => panic!("unexpected frame"),
                }
            }
            assert_eq!(bytes, b"reply after EOF");
            assert!(eof && acknowledged);
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn malformed_or_incomplete_requests_are_errors_and_cancellation_closes_target() {
        tokio::time::timeout(Duration::from_secs(5), async {
            for frame in [
                None,
                Some(Frame::Data(Vec::new())),
                Some(Frame::Data(vec![0; MAX_DATA + 1])),
                Some(Frame::Connected(DockerForwardConnected {})),
            ] {
                let (host, mut guest) = tokio::io::duplex(8);
                let (sender, receiver) = mpsc::channel(1);
                let mut response = start(ReceiverStream::new(receiver), host);
                response.next().await.unwrap().unwrap();
                if let Some(frame) = frame {
                    sender.send(packet(frame)).await.unwrap();
                }
                drop(sender);
                assert!(response.next().await.unwrap().is_err());
                assert!(response.next().await.is_none());
                assert_eq!(guest.read(&mut [0; 1]).await.unwrap(), 0);
            }
            let (host, mut guest) = tokio::io::duplex(8);
            let (_sender, receiver) = mpsc::channel(1);
            let mut response = start(ReceiverStream::new(receiver), host);
            response.next().await.unwrap().unwrap();
            drop(response);
            assert_eq!(guest.read(&mut [0; 1]).await.unwrap(), 0);
        })
        .await
        .unwrap();
    }
}
