//! Typed message channel with length-prefixed JSON framing.
//!
//! Provides a [`Channel`] abstraction for structured host-guest communication
//! over any async byte stream (vsock, TCP, in-memory duplex, etc.).
//!
//! # Wire Format
//!
//! Each message is framed as:
//! ```text
//! +-------------------+-------------------+
//! | length (4 bytes)  | JSON payload      |
//! | little-endian u32 | (length bytes)    |
//! +-------------------+-------------------+
//! ```
//!
//! Maximum frame size is 16 MiB ([`MAX_FRAME_SIZE`](crate::protocol::MAX_FRAME_SIZE)).
//!
//! # Example
//!
//! ```rust,ignore
//! use vz_sandbox::channel::Channel;
//! use vz_sandbox::protocol::{Request, Response};
//!
//! let (client, server) = tokio::io::duplex(64 * 1024);
//! let channel: Channel<Request, Response> = Channel::new(client);
//! channel.send(&Request::Ping { id: 1 }).await?;
//! ```

use std::marker::PhantomData;

use serde::{Serialize, de::DeserializeOwned};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::protocol::MAX_FRAME_SIZE;

/// Error type for channel operations.
#[derive(Debug, thiserror::Error)]
pub enum ChannelError {
    /// Frame exceeds the maximum allowed size.
    #[error("frame too large: {size} bytes (max {MAX_FRAME_SIZE})")]
    FrameTooLarge {
        /// The size of the oversized frame.
        size: usize,
    },

    /// JSON serialization or deserialization failed.
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),

    /// Underlying I/O error (connection closed, broken pipe, etc.).
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}

/// Read a length-prefixed JSON frame from an async reader.
///
/// Returns the deserialized message. Rejects frames larger than
/// [`MAX_FRAME_SIZE`].
pub async fn read_frame<T: DeserializeOwned>(
    reader: &mut (impl AsyncRead + Unpin),
) -> Result<T, ChannelError> {
    let mut len_buf = [0u8; 4];
    reader.read_exact(&mut len_buf).await?;
    let len = u32::from_le_bytes(len_buf) as usize;

    if len > MAX_FRAME_SIZE {
        return Err(ChannelError::FrameTooLarge { size: len });
    }

    let mut payload = vec![0u8; len];
    reader.read_exact(&mut payload).await?;

    let msg: T = serde_json::from_slice(&payload)?;
    Ok(msg)
}

/// Write a length-prefixed JSON frame to an async writer.
///
/// Serializes the message to JSON, writes a 4-byte LE u32 length prefix,
/// then the JSON payload, and flushes the writer.
pub async fn write_frame<T: Serialize>(
    writer: &mut (impl AsyncWrite + Unpin),
    msg: &T,
) -> Result<(), ChannelError> {
    let json = serde_json::to_vec(msg)?;

    if json.len() > MAX_FRAME_SIZE {
        return Err(ChannelError::FrameTooLarge { size: json.len() });
    }

    let len = (json.len() as u32).to_le_bytes();
    writer.write_all(&len).await?;
    writer.write_all(&json).await?;
    writer.flush().await?;
    Ok(())
}

/// A typed bidirectional channel over an async byte stream.
///
/// Wraps any `AsyncRead + AsyncWrite` stream (vsock, TCP, duplex, etc.)
/// and provides typed send/recv operations using length-prefixed JSON framing.
///
/// The `Req` type is what this end sends; `Resp` is what it receives.
///
/// # Example
///
/// ```rust,ignore
/// use vz_sandbox::channel::Channel;
/// use vz_sandbox::protocol::{Request, Response};
///
/// // Host side: sends Request, receives Response
/// let host_channel: Channel<Request, Response> = Channel::new(stream);
/// host_channel.send(&Request::Ping { id: 1 }).await?;
/// let resp = host_channel.recv().await?;
///
/// // Guest side: receives Request, sends Response (types are swapped)
/// let guest_channel: Channel<Response, Request> = Channel::new(stream);
/// let req = guest_channel.recv().await?;
/// guest_channel.send(&Response::Pong { id: 1 }).await?;
/// ```
pub struct Channel<Req, Resp> {
    reader: tokio::sync::Mutex<Box<dyn AsyncRead + Unpin + Send>>,
    writer: tokio::sync::Mutex<Box<dyn AsyncWrite + Unpin + Send>>,
    _phantom: PhantomData<(Req, Resp)>,
}

impl<Req: Serialize, Resp: DeserializeOwned> Channel<Req, Resp> {
    /// Create a channel over the given async stream.
    ///
    /// The stream is split internally into read and write halves,
    /// allowing concurrent send and recv operations.
    pub fn new<S>(stream: S) -> Self
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        let (reader, writer) = tokio::io::split(stream);
        Self {
            reader: tokio::sync::Mutex::new(Box::new(reader)),
            writer: tokio::sync::Mutex::new(Box::new(writer)),
            _phantom: PhantomData,
        }
    }

    /// Send a message to the other end.
    pub async fn send(&self, msg: &Req) -> Result<(), ChannelError> {
        let mut writer = self.writer.lock().await;
        write_frame(&mut *writer, msg).await
    }

    /// Receive a message from the other end.
    pub async fn recv(&self) -> Result<Resp, ChannelError> {
        let mut reader = self.reader.lock().await;
        read_frame(&mut *reader).await
    }

    /// Send a request and wait for the response (request-response pattern).
    pub async fn request(&self, msg: &Req) -> Result<Resp, ChannelError> {
        self.send(msg).await?;
        self.recv().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::{Handshake, HandshakeAck, Request, Response};

    #[tokio::test]
    async fn frame_round_trip_ping_pong() {
        let (mut client, mut server) = tokio::io::duplex(64 * 1024);

        let req = Request::Ping { id: 42 };
        write_frame(&mut client, &req).await.expect("write");

        let received: Request = read_frame(&mut server).await.expect("read");
        assert_eq!(req, received);
    }

    #[tokio::test]
    async fn frame_round_trip_with_base64_data() {
        let (mut client, mut server) = tokio::io::duplex(64 * 1024);

        let resp = Response::Stdout {
            exec_id: 1,
            data: b"hello world\n".to_vec(),
        };
        write_frame(&mut client, &resp).await.expect("write");

        let received: Response = read_frame(&mut server).await.expect("read");
        assert_eq!(resp, received);
    }

    #[tokio::test]
    async fn frame_round_trip_handshake() {
        let (mut client, mut server) = tokio::io::duplex(64 * 1024);

        let handshake = Handshake {
            protocol_version: 1,
            capabilities: vec!["resource_stats".to_string()],
        };
        write_frame(&mut client, &handshake).await.expect("write");

        let received: Handshake = read_frame(&mut server).await.expect("read");
        assert_eq!(handshake, received);
    }

    #[tokio::test]
    async fn frame_round_trip_handshake_ack() {
        let (mut client, mut server) = tokio::io::duplex(64 * 1024);

        let ack = HandshakeAck {
            protocol_version: 1,
            agent_version: "0.1.0".to_string(),
            capabilities: vec![],
        };
        write_frame(&mut client, &ack).await.expect("write");

        let received: HandshakeAck = read_frame(&mut server).await.expect("read");
        assert_eq!(ack, received);
    }

    #[tokio::test]
    async fn oversized_frame_rejected_on_read() {
        let (mut client, mut server) = tokio::io::duplex(64);

        // Write a length prefix that exceeds MAX_FRAME_SIZE
        let fake_len: u32 = (MAX_FRAME_SIZE as u32) + 1;
        client
            .write_all(&fake_len.to_le_bytes())
            .await
            .expect("write len");

        let result: Result<Request, ChannelError> = read_frame(&mut server).await;
        assert!(result.is_err());
        let err = result.err().expect("should be error");
        assert!(
            matches!(err, ChannelError::FrameTooLarge { .. }),
            "expected FrameTooLarge, got: {err}"
        );
    }

    #[tokio::test]
    async fn zero_length_frame() {
        let (mut client, mut server) = tokio::io::duplex(64 * 1024);

        // Write a zero-length frame (no JSON payload)
        let len: u32 = 0;
        client
            .write_all(&len.to_le_bytes())
            .await
            .expect("write len");

        // serde_json should fail to parse empty input
        let result: Result<Request, ChannelError> = read_frame(&mut server).await;
        assert!(result.is_err());
        assert!(matches!(
            result.err().expect("error"),
            ChannelError::Json(_)
        ));
    }

    #[tokio::test]
    async fn connection_closed_returns_io_error() {
        let (client, mut server) = tokio::io::duplex(64 * 1024);

        // Drop the client side immediately
        drop(client);

        let result: Result<Request, ChannelError> = read_frame(&mut server).await;
        assert!(result.is_err());
        assert!(matches!(result.err().expect("error"), ChannelError::Io(_)));
    }

    #[tokio::test]
    async fn multiple_frames_in_sequence() {
        let (mut client, mut server) = tokio::io::duplex(64 * 1024);

        let msgs = vec![
            Request::Ping { id: 1 },
            Request::Ping { id: 2 },
            Request::SystemInfo { id: 3 },
        ];

        for msg in &msgs {
            write_frame(&mut client, msg).await.expect("write");
        }

        for expected in &msgs {
            let received: Request = read_frame(&mut server).await.expect("read");
            assert_eq!(expected, &received);
        }
    }

    #[tokio::test]
    async fn channel_send_recv() {
        let (client, server) = tokio::io::duplex(64 * 1024);

        let host: Channel<Request, Response> = Channel::new(client);
        let guest: Channel<Response, Request> = Channel::new(server);

        host.send(&Request::Ping { id: 1 }).await.expect("send");
        let received = guest.recv().await.expect("recv");
        assert_eq!(received, Request::Ping { id: 1 });

        guest.send(&Response::Pong { id: 1 }).await.expect("send");
        let response = host.recv().await.expect("recv");
        assert_eq!(response, Response::Pong { id: 1 });
    }

    #[tokio::test]
    async fn channel_request_response() {
        let (client, server) = tokio::io::duplex(64 * 1024);

        let host: Channel<Request, Response> = Channel::new(client);
        let guest: Channel<Response, Request> = Channel::new(server);

        // Spawn guest handler
        let handle = tokio::spawn(async move {
            let req = guest.recv().await.expect("recv");
            if let Request::Ping { id } = req {
                guest.send(&Response::Pong { id }).await.expect("send pong");
            }
        });

        let resp = host
            .request(&Request::Ping { id: 42 })
            .await
            .expect("request");
        assert_eq!(resp, Response::Pong { id: 42 });

        handle.await.expect("guest task");
    }

    #[tokio::test]
    async fn channel_exec_streaming_flow() {
        let (client, server) = tokio::io::duplex(64 * 1024);

        let host: Channel<Request, Response> = Channel::new(client);
        let guest: Channel<Response, Request> = Channel::new(server);

        // Simulate an exec flow: host sends Exec, guest streams back stdout + exit code
        let handle = tokio::spawn(async move {
            let req = guest.recv().await.expect("recv");
            if let Request::Exec { id, .. } = req {
                guest
                    .send(&Response::Stdout {
                        exec_id: id,
                        data: b"line 1\n".to_vec(),
                    })
                    .await
                    .expect("stdout 1");
                guest
                    .send(&Response::Stdout {
                        exec_id: id,
                        data: b"line 2\n".to_vec(),
                    })
                    .await
                    .expect("stdout 2");
                guest
                    .send(&Response::ExitCode {
                        exec_id: id,
                        code: 0,
                    })
                    .await
                    .expect("exit");
            }
        });

        host.send(&Request::Exec {
            id: 1,
            command: "echo".to_string(),
            args: vec!["hello".to_string()],
            working_dir: None,
            env: vec![],
            user: None,
        })
        .await
        .expect("send exec");

        let r1 = host.recv().await.expect("recv stdout 1");
        assert_eq!(
            r1,
            Response::Stdout {
                exec_id: 1,
                data: b"line 1\n".to_vec(),
            }
        );

        let r2 = host.recv().await.expect("recv stdout 2");
        assert_eq!(
            r2,
            Response::Stdout {
                exec_id: 1,
                data: b"line 2\n".to_vec(),
            }
        );

        let r3 = host.recv().await.expect("recv exit");
        assert_eq!(
            r3,
            Response::ExitCode {
                exec_id: 1,
                code: 0
            }
        );

        handle.await.expect("guest task");
    }

    #[tokio::test]
    async fn frame_with_large_payload() {
        // Test with a payload close to but under the max size
        let (mut client, mut server) = tokio::io::duplex(1024 * 1024);

        let large_data = vec![0xABu8; 100_000];
        let resp = Response::Stdout {
            exec_id: 1,
            data: large_data.clone(),
        };
        write_frame(&mut client, &resp).await.expect("write large");

        let received: Response = read_frame(&mut server).await.expect("read large");
        assert_eq!(resp, received);
    }
}
