//! vsock host-guest communication.
//!
//! vsock provides a socket-based communication channel between
//! the host and guest without requiring network configuration.
//!
//! `VsockStream` implements `tokio::io::AsyncRead` and `AsyncWrite`,
//! making it compatible with all tokio IO combinators.

use std::cell::Cell;
use std::io;
use std::os::fd::{AsFd, AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use objc2::rc::Retained;
use objc2::runtime::{Bool, NSObjectProtocol, ProtocolObject};
use objc2::{AnyThread, DefinedClass, define_class, msg_send};
use objc2_foundation::NSObject;
use objc2_virtualization::{
    VZVirtioSocketConnection, VZVirtioSocketDevice, VZVirtioSocketListener,
    VZVirtioSocketListenerDelegate,
};
use tokio::io::unix::AsyncFd;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::sync::mpsc;

use crate::error::VzError;

/// Wrapper to send a `VZVirtioSocketConnection` across thread boundaries.
///
/// `Retained<VZVirtioSocketConnection>` is not `Send` because ObjC objects
/// aren't generally thread-safe. However, we only access the connection's
/// file descriptor (via `dup()`) after crossing the thread boundary, and
/// file descriptors are safe to use from any thread.
pub(crate) struct SendableConnection(pub Retained<VZVirtioSocketConnection>);

// SAFETY: After crossing the thread boundary, we only access the file
// descriptor (which is thread-safe). The ObjC connection is retained purely
// to keep the fd alive until we dup it.
unsafe impl Send for SendableConnection {}

/// A bidirectional byte stream over vsock.
///
/// Wraps a `VZVirtioSocketConnection`'s file descriptor with
/// `tokio::io::unix::AsyncFd` for non-blocking async I/O.
///
/// Implements `tokio::io::AsyncRead` and `tokio::io::AsyncWrite`.
pub struct VsockStream {
    /// Async wrapper around the connection's file descriptor.
    fd: AsyncFd<OwnedFd>,
    /// The underlying connection (retained to keep the fd alive).
    /// The fd is duplicated via `dup()` so we own it independently,
    /// but we keep the connection alive to prevent any side effects.
    _connection: Arc<ConnectionHandle>,
}

/// Holds the ObjC connection object, preventing it from being
/// deallocated while the stream is alive.
struct ConnectionHandle {
    #[allow(dead_code)]
    connection: Retained<VZVirtioSocketConnection>,
}

// SAFETY: VZVirtioSocketConnection is accessed only for its file descriptor,
// which has been duplicated. The connection is only retained to prevent
// deallocation. No ObjC methods are called after construction.
unsafe impl Send for ConnectionHandle {}
unsafe impl Sync for ConnectionHandle {}

impl VsockStream {
    /// Create a `VsockStream` from a `VZVirtioSocketConnection`.
    ///
    /// Duplicates the file descriptor so it is owned independently.
    /// The connection is retained to prevent premature deallocation.
    pub(crate) fn from_connection(
        connection: Retained<VZVirtioSocketConnection>,
    ) -> Result<Self, VzError> {
        let raw_fd = unsafe { connection.fileDescriptor() };
        if raw_fd < 0 {
            return Err(VzError::VsockFailed {
                port: 0,
                reason: "connection has a closed file descriptor".into(),
            });
        }

        // Duplicate the fd so we own it independently of the ObjC connection.
        // SAFETY: raw_fd is a valid, open file descriptor from the framework.
        let dup_fd = unsafe { libc::dup(raw_fd) };
        if dup_fd < 0 {
            return Err(VzError::VsockFailed {
                port: 0,
                reason: format!(
                    "failed to duplicate file descriptor: {}",
                    io::Error::last_os_error()
                ),
            });
        }

        // Set the duplicated fd to non-blocking mode for use with AsyncFd.
        // SAFETY: dup_fd is a valid, open file descriptor we just created.
        let flags = unsafe { libc::fcntl(dup_fd, libc::F_GETFL) };
        if flags < 0 {
            unsafe { libc::close(dup_fd) };
            return Err(VzError::VsockFailed {
                port: 0,
                reason: format!("failed to get fd flags: {}", io::Error::last_os_error()),
            });
        }
        let ret = unsafe { libc::fcntl(dup_fd, libc::F_SETFL, flags | libc::O_NONBLOCK) };
        if ret < 0 {
            unsafe { libc::close(dup_fd) };
            return Err(VzError::VsockFailed {
                port: 0,
                reason: format!(
                    "failed to set non-blocking mode: {}",
                    io::Error::last_os_error()
                ),
            });
        }

        // SAFETY: dup_fd is a valid fd that we own (from dup). Transfer ownership to OwnedFd.
        let owned_fd = unsafe { OwnedFd::from_raw_fd(dup_fd) };

        let async_fd = AsyncFd::new(owned_fd).map_err(|e| VzError::VsockFailed {
            port: 0,
            reason: format!("failed to register fd with tokio: {e}"),
        })?;

        Ok(Self {
            fd: async_fd,
            _connection: Arc::new(ConnectionHandle { connection }),
        })
    }
}

impl AsyncRead for VsockStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        loop {
            let mut guard = match self.fd.poll_read_ready(cx) {
                Poll::Ready(Ok(guard)) => guard,
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                Poll::Pending => return Poll::Pending,
            };

            let unfilled = buf.initialize_unfilled();
            let fd = self.fd.as_fd().as_raw_fd();
            // SAFETY: Reading from a valid fd into a properly sized buffer.
            let n = unsafe { libc::read(fd, unfilled.as_mut_ptr().cast(), unfilled.len()) };

            if n >= 0 {
                buf.advance(n as usize);
                return Poll::Ready(Ok(()));
            }

            let err = io::Error::last_os_error();
            if err.kind() == io::ErrorKind::WouldBlock {
                guard.clear_ready();
                continue;
            }
            return Poll::Ready(Err(err));
        }
    }
}

impl AsyncWrite for VsockStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        loop {
            let mut guard = match self.fd.poll_write_ready(cx) {
                Poll::Ready(Ok(guard)) => guard,
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                Poll::Pending => return Poll::Pending,
            };

            let fd = self.fd.as_fd().as_raw_fd();
            // SAFETY: Writing from a valid buffer to a valid fd.
            let n = unsafe { libc::write(fd, buf.as_ptr().cast(), buf.len()) };

            if n >= 0 {
                return Poll::Ready(Ok(n as usize));
            }

            let err = io::Error::last_os_error();
            if err.kind() == io::ErrorKind::WouldBlock {
                guard.clear_ready();
                continue;
            }
            return Poll::Ready(Err(err));
        }
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        // The fd is unbuffered, so flush is a no-op.
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let fd = self.fd.as_fd().as_raw_fd();
        // Shut down the write half of the socket.
        // SAFETY: Calling shutdown on a valid fd with SHUT_WR.
        let ret = unsafe { libc::shutdown(fd, libc::SHUT_WR) };
        if ret < 0 {
            let err = io::Error::last_os_error();
            // ENOTCONN is expected if already disconnected.
            if err.kind() != io::ErrorKind::NotConnected {
                return Poll::Ready(Err(err));
            }
        }
        Poll::Ready(Ok(()))
    }
}

impl AsRawFd for VsockStream {
    fn as_raw_fd(&self) -> RawFd {
        self.fd.as_fd().as_raw_fd()
    }
}

// ---------------------------------------------------------------------------
// VsockListener
// ---------------------------------------------------------------------------

/// Accepts incoming vsock connections from the guest.
///
/// Created via `Vm::vsock_listen()`. Incoming connections from the
/// guest are delivered through an internal channel. Call `accept()`
/// to receive the next connection as a `VsockStream`.
pub struct VsockListener {
    /// Channel receiving raw connections from the ObjC delegate.
    /// Converted to `VsockStream` in `accept()` on the tokio thread.
    rx: mpsc::UnboundedReceiver<SendableConnection>,
    /// The ObjC listener delegate, retained to prevent deallocation.
    _handle: Arc<ListenerHandle>,
}

/// Holds ObjC objects for the listener.
struct ListenerHandle {
    _listener: Retained<VZVirtioSocketListener>,
    _delegate: Retained<VsockListenerDelegate>,
}

// SAFETY: The ObjC objects are only accessed from the VM's dispatch queue.
// The Rust-side VsockListener only reads from the mpsc channel.
unsafe impl Send for ListenerHandle {}
unsafe impl Sync for ListenerHandle {}

impl VsockListener {
    /// Create a new VsockListener.
    ///
    /// Sets up a `VZVirtioSocketListener` with a delegate that forwards
    /// incoming connections to the returned listener via an mpsc channel.
    pub(crate) fn new(device: &VZVirtioSocketDevice, port: u32) -> Result<Self, VzError> {
        let (tx, rx) = mpsc::unbounded_channel::<SendableConnection>();

        // Create the ObjC listener and delegate
        let delegate = VsockListenerDelegate::new(tx);
        let listener = unsafe { VZVirtioSocketListener::new() };
        unsafe {
            listener.setDelegate(Some(delegate.as_protocol()));
        }

        // Register the listener on the device for the given port
        unsafe {
            device.setSocketListener_forPort(&listener, port);
        }

        Ok(Self {
            rx,
            _handle: Arc::new(ListenerHandle {
                _listener: listener,
                _delegate: delegate,
            }),
        })
    }

    /// Accept the next incoming connection.
    ///
    /// Waits for a guest to connect on this listener's port and
    /// returns a `VsockStream` for the connection.
    pub async fn accept(&mut self) -> Result<VsockStream, VzError> {
        let conn = self.rx.recv().await.ok_or_else(|| VzError::VsockFailed {
            port: 0,
            reason: "listener channel closed".into(),
        })?;
        // Create VsockStream here on the tokio thread (AsyncFd needs a reactor).
        VsockStream::from_connection(conn.0)
    }
}

// ---------------------------------------------------------------------------
// VsockListenerDelegate (ObjC class via define_class!)
// ---------------------------------------------------------------------------

/// Ivar storage for the listener delegate.
pub(crate) struct VsockListenerDelegateIvars {
    tx: Cell<Option<mpsc::UnboundedSender<SendableConnection>>>,
}

define_class!(
    // SAFETY: NSObject has no subclassing requirements.
    #[unsafe(super(NSObject))]
    #[ivars = VsockListenerDelegateIvars]
    #[name = "VZRustVsockListenerDelegate"]
    pub(crate) struct VsockListenerDelegate;

    unsafe impl NSObjectProtocol for VsockListenerDelegate {}

    unsafe impl VZVirtioSocketListenerDelegate for VsockListenerDelegate {
        #[unsafe(method(listener:shouldAcceptNewConnection:fromSocketDevice:))]
        fn listener_should_accept(
            &self,
            _listener: &VZVirtioSocketListener,
            connection: &VZVirtioSocketConnection,
            _device: &VZVirtioSocketDevice,
        ) -> Bool {
            if let Some(tx) = self.ivars().tx.take() {
                // Retain the connection so it lives beyond this callback.
                // SAFETY: connection is a valid ObjC object from the framework callback.
                let retained_conn = unsafe {
                    Retained::retain(connection as *const _ as *mut VZVirtioSocketConnection)
                };
                if let Some(conn) = retained_conn {
                    // Send the raw connection through the channel.
                    // VsockStream creation happens on the tokio thread in accept().
                    let _ = tx.send(SendableConnection(conn));
                    self.ivars().tx.set(Some(tx));
                    return Bool::YES;
                }
                self.ivars().tx.set(Some(tx));
            }
            Bool::NO
        }
    }
);

impl VsockListenerDelegate {
    fn new(tx: mpsc::UnboundedSender<SendableConnection>) -> Retained<Self> {
        let ivars = VsockListenerDelegateIvars {
            tx: Cell::new(Some(tx)),
        };
        let this = Self::alloc().set_ivars(ivars);
        unsafe { msg_send![super(this), init] }
    }

    fn as_protocol(&self) -> &ProtocolObject<dyn VZVirtioSocketListenerDelegate> {
        ProtocolObject::from_ref(self)
    }
}
