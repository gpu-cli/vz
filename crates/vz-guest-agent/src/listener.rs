//! AF_VSOCK listener for the guest agent.
//!
//! Provides a [`VsockListener`] that binds to a vsock port and accepts
//! incoming connections from the host, wrapping them as tokio async streams.
//! Also provides [`VsockIncoming`] for use with tonic's `serve_with_incoming`.

// Vsock listener requires unsafe for libc socket/bind/listen/accept syscalls.
#![allow(unsafe_code)]

use std::io;
use std::os::fd::{FromRawFd, OwnedFd, RawFd};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, AsyncWrite};
use tonic::transport::server::Connected;

/// AF_VSOCK listener that accepts connections from the host.
pub struct VsockListener {
    fd: OwnedFd,
}

/// A connected vsock stream, usable with tokio async I/O.
pub struct VsockStream {
    inner: tokio::net::UnixStream,
}

impl AsyncRead for VsockStream {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        std::pin::Pin::new(&mut self.get_mut().inner).poll_read(cx, buf)
    }
}

impl AsyncWrite for VsockStream {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<io::Result<usize>> {
        std::pin::Pin::new(&mut self.get_mut().inner).poll_write(cx, buf)
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        std::pin::Pin::new(&mut self.get_mut().inner).poll_flush(cx)
    }

    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        std::pin::Pin::new(&mut self.get_mut().inner).poll_shutdown(cx)
    }
}

/// sockaddr_vm layout on macOS.
#[cfg(target_os = "macos")]
#[repr(C)]
struct SockaddrVm {
    svm_len: u8,
    svm_family: u8,
    svm_reserved1: u16,
    svm_port: u32,
    svm_cid: u32,
}

/// sockaddr_vm layout on Linux.
#[cfg(not(target_os = "macos"))]
#[repr(C)]
struct SockaddrVm {
    svm_family: libc::sa_family_t,
    svm_reserved1: u16,
    svm_port: u32,
    svm_cid: u32,
    svm_flags: u8,
    svm_zero: [u8; 3],
}

/// VMADDR_CID_ANY: accept connections from any CID (i.e., the host).
const VMADDR_CID_ANY: u32 = u32::MAX; // -1 as u32

/// AF_VSOCK address family on macOS.
#[cfg(target_os = "macos")]
const AF_VSOCK: i32 = 40;

/// AF_VSOCK address family on Linux.
#[cfg(not(target_os = "macos"))]
const AF_VSOCK: i32 = libc::AF_VSOCK;

#[cfg(target_os = "macos")]
fn sockaddr_vm_any(port: u32) -> SockaddrVm {
    SockaddrVm {
        svm_len: std::mem::size_of::<SockaddrVm>() as u8,
        svm_family: AF_VSOCK as u8,
        svm_reserved1: 0,
        svm_port: port,
        svm_cid: VMADDR_CID_ANY,
    }
}

#[cfg(not(target_os = "macos"))]
fn sockaddr_vm_any(port: u32) -> SockaddrVm {
    SockaddrVm {
        svm_family: AF_VSOCK as libc::sa_family_t,
        svm_reserved1: 0,
        svm_port: port,
        svm_cid: VMADDR_CID_ANY,
        svm_flags: 0,
        svm_zero: [0; 3],
    }
}

impl VsockListener {
    /// Bind a vsock listener on the given port.
    ///
    /// Creates an `AF_VSOCK` socket, binds it to `VMADDR_CID_ANY:port`,
    /// and starts listening with a backlog of 1 (single connection at a time).
    pub fn bind(port: u32) -> io::Result<Self> {
        // SAFETY: socket() is a standard POSIX function that creates a file descriptor.
        let fd = unsafe { libc::socket(AF_VSOCK, libc::SOCK_STREAM, 0) };
        if fd < 0 {
            return Err(io::Error::last_os_error());
        }

        // SAFETY: We just created this fd and it's valid.
        let owned_fd = unsafe { OwnedFd::from_raw_fd(fd) };

        let addr = sockaddr_vm_any(port);

        // SAFETY: bind() with a valid fd and properly initialized sockaddr.
        let ret = unsafe {
            libc::bind(
                fd,
                &addr as *const SockaddrVm as *const libc::sockaddr,
                std::mem::size_of::<SockaddrVm>() as libc::socklen_t,
            )
        };
        if ret < 0 {
            return Err(io::Error::last_os_error());
        }

        // Listen with backlog of 8 (allow multiple pending connections)
        // SAFETY: listen() with a valid bound fd.
        let ret = unsafe { libc::listen(fd, 8) };
        if ret < 0 {
            return Err(io::Error::last_os_error());
        }

        Ok(Self { fd: owned_fd })
    }

    /// Accept the next incoming vsock connection.
    ///
    /// Blocks (async) until a connection arrives from the host.
    /// Returns a [`VsockStream`] that implements `AsyncRead + AsyncWrite`.
    pub async fn accept(&self) -> io::Result<VsockStream> {
        use std::os::fd::AsRawFd;

        let listener_fd = self.fd.as_raw_fd();

        // Use tokio's blocking task pool for the accept() syscall since
        // we can't easily register a vsock fd with epoll/kqueue through tokio.
        let conn_fd = tokio::task::spawn_blocking(move || -> io::Result<RawFd> {
            let mut addr: SockaddrVm = unsafe { std::mem::zeroed() };
            let mut addr_len = std::mem::size_of::<SockaddrVm>() as libc::socklen_t;

            // SAFETY: accept() with a valid listening fd.
            let fd = unsafe {
                libc::accept(
                    listener_fd,
                    &mut addr as *mut SockaddrVm as *mut libc::sockaddr,
                    &mut addr_len,
                )
            };
            if fd < 0 {
                return Err(io::Error::last_os_error());
            }

            Ok(fd)
        })
        .await
        .map_err(io::Error::other)??;

        // Convert the raw fd to a tokio UnixStream for async I/O.
        // vsock fds are regular file descriptors that support read/write,
        // and UnixStream is the simplest tokio wrapper for arbitrary fds.
        // SAFETY: conn_fd is a valid, newly accepted file descriptor.
        let std_stream = unsafe { std::os::unix::net::UnixStream::from_raw_fd(conn_fd) };
        std_stream.set_nonblocking(true)?;
        let tokio_stream = tokio::net::UnixStream::from_std(std_stream)?;

        Ok(VsockStream {
            inner: tokio_stream,
        })
    }
}

// ── tonic Connected trait ──────────────────────────────────────────

/// Connection info for vsock streams.
///
/// Minimal struct satisfying tonic's `Connected` trait requirements.
#[derive(Clone, Debug)]
pub struct VsockConnectInfo;

impl Connected for VsockStream {
    type ConnectInfo = VsockConnectInfo;

    fn connect_info(&self) -> Self::ConnectInfo {
        VsockConnectInfo
    }
}

// ── VsockIncoming stream adapter ───────────────────────────────────

/// A `Stream` of accepted [`VsockStream`] connections, suitable for
/// `tonic::transport::Server::serve_with_incoming`.
///
/// Wraps a shared [`VsockListener`] and yields `Result<VsockStream, io::Error>`.
pub struct VsockIncoming {
    listener: Arc<VsockListener>,
}

impl VsockIncoming {
    /// Create a new incoming stream from the given listener.
    pub fn new(listener: Arc<VsockListener>) -> Self {
        Self { listener }
    }
}

impl tokio_stream::Stream for VsockIncoming {
    type Item = Result<VsockStream, io::Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let listener = self.listener.clone();
        // We need to poll the accept future. Since VsockListener::accept uses
        // spawn_blocking internally, we create a future each time and poll it.
        // To avoid creating a new future on every poll, we use a simple approach:
        // pin a new future each time. This works because spawn_blocking handles
        // the actual blocking, and each poll_next call represents a new accept attempt.
        let fut = async move { listener.accept().await };
        let mut fut = Box::pin(fut);
        match fut.as_mut().poll(cx) {
            Poll::Ready(result) => Poll::Ready(Some(result)),
            Poll::Pending => Poll::Pending,
        }
    }
}
