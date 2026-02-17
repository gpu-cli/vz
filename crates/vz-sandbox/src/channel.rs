//! Typed message protocol over vsock.
//!
//! Provides a request/response channel for structured communication
//! between host and guest over vsock. Useful for tool forwarding
//! where the host holds secrets and the guest sends tool requests.

use std::marker::PhantomData;

use serde::{Serialize, de::DeserializeOwned};

/// A typed bidirectional channel over vsock.
///
/// Messages are length-prefixed JSON over the underlying vsock stream.
///
/// # Example
///
/// ```rust,ignore
/// #[derive(Serialize, Deserialize)]
/// struct ToolRequest { name: String, args: Vec<String> }
///
/// #[derive(Serialize, Deserialize)]
/// struct ToolResponse { exit_code: i32, stdout: String }
///
/// let channel: Channel<ToolRequest, ToolResponse> = Channel::new(vsock_stream);
/// let resp = channel.request(ToolRequest {
///     name: "git".into(),
///     args: vec!["status".into()],
/// }).await?;
/// ```
pub struct Channel<Req, Resp> {
    // Will hold: VsockStream
    _phantom: PhantomData<(Req, Resp)>,
}

impl<Req: Serialize, Resp: DeserializeOwned> Channel<Req, Resp> {
    /// Send a request to the other end.
    pub async fn send(&self, _req: Req) -> anyhow::Result<()> {
        todo!("Phase 2: implement send")
    }

    /// Receive a response from the other end.
    pub async fn recv(&self) -> anyhow::Result<Resp> {
        todo!("Phase 2: implement recv")
    }

    /// Send a request and wait for the response.
    pub async fn request(&self, req: Req) -> anyhow::Result<Resp> {
        self.send(req).await?;
        self.recv().await
    }
}
