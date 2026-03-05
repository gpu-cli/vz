//! AF_VSOCK listener for the bootstrap loader.
//!
//! Minimal vsock binding — no tonic, no gRPC.
//! Accepts a single host connection at a time and provides async read/write.

#![allow(unsafe_code)]

use std::io;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};

use tokio::io::{AsyncRead, AsyncWrite};
use tracing::warn;

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

// ── sockaddr_vm ────────────────────────────────────────────────────

#[cfg(target_os = "macos")]
#[repr(C)]
struct SockaddrVm {
    svm_len: u8,
    svm_family: u8,
    svm_reserved1: u16,
    svm_port: u32,
    svm_cid: u32,
}

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

const VMADDR_CID_ANY: u32 = u32::MAX;
const VMADDR_CID_HOST: u32 = 2;

#[cfg(target_os = "macos")]
const AF_VSOCK: i32 = 40;

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

// ── Listener impl ──────────────────────────────────────────────────

impl VsockListener {
    /// Bind a vsock listener on the given port.
    pub fn bind(port: u32) -> io::Result<Self> {
        // SAFETY: socket() creates a file descriptor.
        let fd = unsafe { libc::socket(AF_VSOCK, libc::SOCK_STREAM, 0) };
        if fd < 0 {
            return Err(io::Error::last_os_error());
        }

        // SAFETY: fd is valid, transfer ownership immediately.
        let owned_fd = unsafe { OwnedFd::from_raw_fd(fd) };

        let addr = sockaddr_vm_any(port);

        // SAFETY: bind() with valid fd and properly initialized sockaddr.
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

        // SAFETY: listen() on a valid bound fd.
        let ret = unsafe { libc::listen(fd, 4) };
        if ret < 0 {
            return Err(io::Error::last_os_error());
        }

        Ok(Self { fd: owned_fd })
    }

    /// Accept the next incoming vsock connection from the host.
    pub async fn accept(&self) -> io::Result<VsockStream> {
        let listener_fd = self.fd.as_raw_fd();

        loop {
            let (conn_fd, source_cid) =
                tokio::task::spawn_blocking(move || -> io::Result<(RawFd, u32)> {
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

                    Ok((fd, addr.svm_cid))
                })
                .await
                .map_err(io::Error::other)??;

            if source_cid != VMADDR_CID_HOST {
                // SAFETY: close rejected fd.
                unsafe {
                    libc::close(conn_fd);
                }
                warn!(source_cid, "rejected vsock connection from non-host CID");
                continue;
            }

            // SAFETY: conn_fd is valid, convert to async stream.
            let std_stream = unsafe { std::os::unix::net::UnixStream::from_raw_fd(conn_fd) };
            std_stream.set_nonblocking(true)?;
            let tokio_stream = tokio::net::UnixStream::from_std(std_stream)?;

            return Ok(VsockStream {
                inner: tokio_stream,
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn host_cid_is_2() {
        assert_eq!(VMADDR_CID_HOST, 2);
    }

    #[test]
    fn cid_any_is_max() {
        assert_eq!(VMADDR_CID_ANY, u32::MAX);
    }
}
