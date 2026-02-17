//! vsock host↔guest communication.
//!
//! vsock provides a socket-based communication channel between
//! the host and guest without requiring network configuration.

/// A bidirectional byte stream over vsock.
///
/// Implements `tokio::io::AsyncRead` and `tokio::io::AsyncWrite`.
pub struct VsockStream {
    // Will hold: VZVirtioSocketConnection from vz-sys
}

/// Accepts incoming vsock connections from the guest.
pub struct VsockListener {
    // Will hold: listener state on VZVirtioSocketDevice
}

impl VsockListener {
    /// Accept the next incoming connection.
    pub async fn accept(&self) -> Result<VsockStream, crate::VzError> {
        todo!("Phase 1: implement vsock accept")
    }
}

// TODO: Phase 1
// impl tokio::io::AsyncRead for VsockStream { ... }
// impl tokio::io::AsyncWrite for VsockStream { ... }
