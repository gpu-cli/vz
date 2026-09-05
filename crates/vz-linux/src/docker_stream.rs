//! Opaque, bounded Docker forwarding with independent read/write half-closes.

use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::sync::mpsc;
use tokio_stream::Stream;
use tokio_util::sync::PollSender;
use vz_agent_proto::{DockerForwardEof, DockerForwardFrame, docker_forward_frame::Frame};

const MAX_DATA: usize = 65536;

/// Exact-connection Engine byte stream. The caller retains its Machine boot lease.
pub struct GrpcDockerStream {
    inbound: Pin<Box<dyn Stream<Item = Result<DockerForwardFrame, tonic::Status>> + Send>>,
    outbound: PollSender<DockerForwardFrame>,
    buffered: Vec<u8>,
    offset: usize,
    read_eof_received: bool,
    read_closed: bool,
    write_eof_sent: bool,
    write_acknowledged: bool,
    write_closed: bool,
}

impl GrpcDockerStream {
    pub(crate) fn new(
        inbound: impl Stream<Item = Result<DockerForwardFrame, tonic::Status>> + Send + 'static,
        outbound: mpsc::Sender<DockerForwardFrame>,
    ) -> Self {
        Self {
            inbound: Box::pin(inbound),
            outbound: PollSender::new(outbound),
            buffered: Vec::new(),
            offset: 0,
            read_eof_received: false,
            read_closed: false,
            write_eof_sent: false,
            write_acknowledged: false,
            write_closed: false,
        }
    }

    // Only the second half to complete waits here. The first half has already
    // returned terminal success and never polls inbound again. Thus no competing
    // read/shutdown wakers or unbounded response buffering are necessary.
    fn poll_write_acknowledgement(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if self.write_acknowledged {
            return Poll::Ready(Ok(()));
        }
        match std::task::ready!(self.inbound.as_mut().poll_next(cx)) {
            Some(Ok(DockerForwardFrame {
                frame: Some(Frame::WriteClosed(_)),
            })) if self.write_eof_sent => {
                self.write_acknowledged = true;
                Poll::Ready(Ok(()))
            }
            Some(Err(error)) => {
                Poll::Ready(Err(broken(format!("Docker EOF acknowledgement: {error}"))))
            }
            None => Poll::Ready(Err(broken(
                "Docker response ended before request EOF was acknowledged",
            ))),
            _ => Poll::Ready(Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid Docker frame after response EOF",
            ))),
        }
    }
}

fn broken(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::BrokenPipe, message.into())
}

impl AsyncRead for GrpcDockerStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        output: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        if output.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }
        if this.offset == this.buffered.len() {
            if this.read_closed {
                return Poll::Ready(Ok(()));
            }
            loop {
                if this.read_eof_received {
                    if this.write_eof_sent {
                        std::task::ready!(this.poll_write_acknowledgement(cx))?;
                    }
                    this.read_closed = true;
                    return Poll::Ready(Ok(()));
                }
                match std::task::ready!(this.inbound.as_mut().poll_next(cx)) {
                    Some(Ok(DockerForwardFrame {
                        frame: Some(Frame::Data(data)),
                    })) if !data.is_empty() && data.len() <= MAX_DATA => {
                        this.buffered = data;
                        this.offset = 0;
                        break;
                    }
                    Some(Ok(DockerForwardFrame {
                        frame: Some(Frame::Eof(_)),
                    })) => {
                        this.read_eof_received = true;
                    }
                    Some(Ok(DockerForwardFrame {
                        frame: Some(Frame::WriteClosed(_)),
                    })) if this.write_eof_sent && !this.write_acknowledged => {
                        this.write_acknowledged = true;
                    }
                    Some(Err(error)) => {
                        return Poll::Ready(Err(broken(format!("Docker forwarding: {error}"))));
                    }
                    None => {
                        return Poll::Ready(Err(broken(
                            "Docker response ended without directional EOF",
                        )));
                    }
                    _ => {
                        return Poll::Ready(Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "invalid Docker response frame",
                        )));
                    }
                }
            }
        }
        let count = output.remaining().min(this.buffered.len() - this.offset);
        output.put_slice(&this.buffered[this.offset..this.offset + count]);
        this.offset += count;
        Poll::Ready(Ok(()))
    }
}

impl AsyncWrite for GrpcDockerStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        data: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        if this.write_eof_sent {
            return Poll::Ready(Err(broken("Docker write half closed")));
        }
        if data.is_empty() {
            return Poll::Ready(Ok(0));
        }
        std::task::ready!(this.outbound.poll_reserve(cx))
            .map_err(|_| broken("Docker request closed"))?;
        let count = data.len().min(MAX_DATA);
        this.outbound
            .send_item(DockerForwardFrame {
                frame: Some(Frame::Data(data[..count].to_vec())),
            })
            .map_err(|_| broken("Docker request closed"))?;
        Poll::Ready(Ok(count))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        if this.write_closed {
            return Poll::Ready(Ok(()));
        }
        if !this.write_eof_sent {
            std::task::ready!(this.outbound.poll_reserve(cx))
                .map_err(|_| broken("Docker request closed"))?;
            this.outbound
                .send_item(DockerForwardFrame {
                    frame: Some(Frame::Eof(DockerForwardEof {})),
                })
                .map_err(|_| broken("Docker request closed"))?;
            this.outbound.close();
            this.write_eof_sent = true;
        }
        if this.read_closed {
            std::task::ready!(this.poll_write_acknowledgement(cx))?;
        }
        this.write_closed = true;
        Poll::Ready(Ok(()))
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
    use super::*;
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio_stream::wrappers::ReceiverStream;
    use vz_agent_proto::DockerForwardWriteClosed;

    #[tokio::test]
    async fn backpressure_and_half_close_preserve_large_binary_data() {
        tokio::time::timeout(Duration::from_secs(5), async {
            let (request_sender, mut request_receiver) = mpsc::channel(1);
            let (response_sender, response_receiver) = mpsc::channel(1);
            let stream =
                GrpcDockerStream::new(ReceiverStream::new(response_receiver), request_sender);
            let (mut read, mut write) = tokio::io::split(stream);
            let payload: Vec<u8> = (0..1_000_003).map(|index| (index % 251) as u8).collect();
            let expected = payload.clone();
            let writer = async move {
                write.write_all(&payload).await.unwrap();
                write.shutdown().await.unwrap();
                write.shutdown().await.unwrap();
                assert!(write.write_all(b"after eof").await.is_err());
            };
            let peer = async move {
                let mut received = Vec::new();
                let mut eof = false;
                while let Some(frame) = request_receiver.recv().await {
                    match frame.frame {
                        Some(Frame::Data(bytes)) => {
                            assert!(!eof);
                            assert!(bytes.len() <= MAX_DATA);
                            received.extend(bytes);
                        }
                        Some(Frame::Eof(_)) => {
                            assert!(!eof);
                            eof = true;
                        }
                        _ => panic!("unexpected frame"),
                    }
                    tokio::task::yield_now().await;
                }
                assert!(eof);
                assert_eq!(received, expected);
                response_sender
                    .send(Ok(DockerForwardFrame {
                        frame: Some(Frame::WriteClosed(DockerForwardWriteClosed {})),
                    }))
                    .await
                    .unwrap();
                for chunk in received.chunks(8179) {
                    response_sender
                        .send(Ok(DockerForwardFrame {
                            frame: Some(Frame::Data(chunk.to_vec())),
                        }))
                        .await
                        .unwrap();
                }
                response_sender
                    .send(Ok(DockerForwardFrame {
                        frame: Some(Frame::Eof(DockerForwardEof {})),
                    }))
                    .await
                    .unwrap();
                received
            };
            let reader = async move {
                let mut bytes = Vec::new();
                read.read_to_end(&mut bytes).await.unwrap();
                bytes
            };
            let ((), sent, received) = tokio::join!(writer, peer, reader);
            assert_eq!(sent, received);
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn abrupt_end_errors_and_drop_cancels_both_directions() {
        let (sender, mut receiver) = mpsc::channel(1);
        let (response_sender, response_receiver) = mpsc::channel(1);
        let mut stream = GrpcDockerStream::new(ReceiverStream::new(response_receiver), sender);
        response_sender
            .send(Err(tonic::Status::unavailable("injected socket failure")))
            .await
            .unwrap();
        let error = stream.read(&mut [0; 1]).await.unwrap_err();
        assert!(error.to_string().contains("injected socket failure"));
        drop(response_sender);
        assert!(stream.read(&mut [0; 1]).await.is_err());
        drop(stream);
        assert!(receiver.recv().await.is_none());
        let (sender, _receiver) = mpsc::channel(1);
        let (response_sender, response_receiver) = mpsc::channel(1);
        let stream = GrpcDockerStream::new(ReceiverStream::new(response_receiver), sender);
        drop(stream);
        assert!(response_sender.is_closed());
    }

    #[tokio::test]
    async fn reverse_half_close_waits_until_guest_consumes_all_queued_request_bytes() {
        tokio::time::timeout(Duration::from_secs(5), async {
            let (request_sender, mut request_receiver) = mpsc::channel(4);
            let (response_sender, response_receiver) = mpsc::channel(1);
            let mut stream =
                GrpcDockerStream::new(ReceiverStream::new(response_receiver), request_sender);
            response_sender
                .send(Ok(DockerForwardFrame {
                    frame: Some(Frame::Eof(DockerForwardEof {})),
                }))
                .await
                .unwrap();
            // Remote FIN is observable before local FIN: the write half remains usable.
            assert_eq!(stream.read(&mut [0; 1]).await.unwrap(), 0);
            stream.write_all(b"queued after remote EOF").await.unwrap();
            let mut shutdown = Box::pin(stream.shutdown());
            // Enqueue is not delivery. Before the peer even polls, shutdown must
            // NOT let copy_bidirectional finish and drop/reset the tonic RPC.
            std::future::poll_fn(|cx| {
                assert!(std::future::Future::poll(shutdown.as_mut(), cx).is_pending());
                Poll::Ready(())
            })
            .await;
            assert_eq!(
                request_receiver.recv().await.unwrap().frame,
                Some(Frame::Data(b"queued after remote EOF".to_vec()))
            );
            assert!(matches!(
                request_receiver.recv().await.unwrap().frame,
                Some(Frame::Eof(_))
            ));
            assert!(request_receiver.recv().await.is_none());
            std::future::poll_fn(|cx| {
                assert!(std::future::Future::poll(shutdown.as_mut(), cx).is_pending());
                Poll::Ready(())
            })
            .await;
            response_sender
                .send(Ok(DockerForwardFrame {
                    frame: Some(Frame::WriteClosed(DockerForwardWriteClosed {})),
                }))
                .await
                .unwrap();
            shutdown.await.unwrap();
            drop(stream);
            assert!(response_sender.is_closed());
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn shutdown_first_allows_unbounded_response_but_terminal_eof_requires_ack() {
        tokio::time::timeout(Duration::from_secs(5), async {
            let (request_sender, mut request_receiver) = mpsc::channel(1);
            let (response_sender, response_receiver) = mpsc::channel(1);
            let mut stream =
                GrpcDockerStream::new(ReceiverStream::new(response_receiver), request_sender);
            stream.shutdown().await.unwrap(); // Must not wait for ack or response here.
            let peer = async move {
                assert!(matches!(
                    request_receiver.recv().await.unwrap().frame,
                    Some(Frame::Eof(_))
                ));
                assert!(request_receiver.recv().await.is_none());
                for _ in 0..128 {
                    response_sender
                        .send(Ok(DockerForwardFrame {
                            frame: Some(Frame::Data(vec![0x92; MAX_DATA])),
                        }))
                        .await
                        .unwrap();
                }
                response_sender
                    .send(Ok(DockerForwardFrame {
                        frame: Some(Frame::Eof(DockerForwardEof {})),
                    }))
                    .await
                    .unwrap();
                // Deliberately omit acknowledgement: this terminal connection
                // is incomplete even though its response direction had EOF.
            };
            let reader = async move {
                let mut bytes = Vec::new();
                let error = stream.read_to_end(&mut bytes).await.unwrap_err();
                assert_eq!(bytes, vec![0x92; MAX_DATA * 128]);
                assert!(
                    error
                        .to_string()
                        .contains("before request EOF was acknowledged")
                );
            };
            tokio::join!(peer, reader);
        })
        .await
        .unwrap();
    }
}
