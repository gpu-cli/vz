//! AF_VSOCK listener for the guest agent.
//!
//! Provides a [`VsockListener`] that binds to a vsock port and accepts
//! incoming connections from the host, wrapping them as tokio async streams.

// Vsock listener requires unsafe for libc socket/bind/listen/accept syscalls.
#![allow(unsafe_code)]

use std::io;
use std::os::fd::{FromRawFd, OwnedFd, RawFd};

use tokio::io::{AsyncRead, AsyncWrite};

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

/// macOS-specific sockaddr_vm layout.
/// On macOS, AF_VSOCK = 40 and the struct includes svm_len.
#[repr(C)]
struct SockaddrVm {
    svm_len: u8,
    svm_family: u8,
    svm_reserved1: u16,
    svm_port: u32,
    svm_cid: u32,
}

/// VMADDR_CID_ANY: accept connections from any CID (i.e., the host).
const VMADDR_CID_ANY: u32 = u32::MAX; // -1 as u32

/// AF_VSOCK address family on macOS.
const AF_VSOCK: i32 = 40;

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

        let addr = SockaddrVm {
            svm_len: std::mem::size_of::<SockaddrVm>() as u8,
            svm_family: AF_VSOCK as u8,
            svm_reserved1: 0,
            svm_port: port,
            svm_cid: VMADDR_CID_ANY,
        };

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

        // Listen with backlog of 1 (single connection at a time)
        // SAFETY: listen() with a valid bound fd.
        let ret = unsafe { libc::listen(fd, 1) };
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
