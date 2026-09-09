//! The Environment's public-like edge: what `NetworkKind::SimulatedPublic` is.
//!
//! A private network is a switch and nothing else. A public-like one is that
//! same switch plus one more port, which the daemon keeps instead of attaching
//! to a Machine, and on which it runs the edge every clause of criterion 6
//! names: a resolver that answers this Environment's names and no others, a TLS
//! listener that presents certificates this Environment issued for the names it
//! declared, a proxy that carries the plaintext to the declared origin over a
//! connection the edge opens itself, and a filter that drops everything else.
//!
//! Why an extra port and a userspace stack rather than a host interface: the
//! product contract forbids the shared vmnet NAT segment as an authorisation
//! boundary, and a host interface would be exactly that — one address every VM
//! on the machine can reach, standing in for a per-Environment boundary it
//! cannot enforce. The edge is instead a member of one Environment's fabric,
//! and a frame can only reach it through a socket that fabric handed out. It
//! also means the edge binds nothing on the host: there is no host listener to
//! find, on loopback or anywhere else.
//!
//! What this is not: it is not external egress. `EgressPolicy` still admits
//! `Offline` alone, because reaching a host off the fabric needs a translation
//! towards the host's own network and a policy that decides which hosts, and
//! neither is here. The edge translates addresses only between an Environment's
//! client and an Environment's own declared origin.

pub mod dns;
pub mod edge;
pub mod firewall;
pub mod identity;

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tracing::info;

use crate::environment_switch::plan::NetworkPlan;
pub use edge::{EdgeConfig, EdgeError, EdgeRoute, EdgeShutdown};
pub use identity::IdentityError;

#[derive(Debug, thiserror::Error)]
pub enum GatewayError {
    #[error(transparent)]
    Identity(#[from] IdentityError),
    #[error(transparent)]
    Edge(#[from] EdgeError),
    #[error("edge trust anchor at {path}: {source}")]
    Anchor {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("edge already stopped")]
    AlreadyStopped,
    #[error("edge supervisor: {0}")]
    Task(#[from] tokio::task::JoinError),
}

/// One Environment's running edge on one network.
///
/// Explicit shutdown joins the task before its switch is stopped, for the same
/// reason a Machine's is: the edge holds the guest end of a switch port, and a
/// switch torn down under a live port would look to the edge like a network
/// that stopped forwarding.
#[must_use = "retain the edge and await shutdown before stopping its switch"]
#[derive(Debug)]
pub struct EnvironmentGateway {
    shutdown: Option<oneshot::Sender<()>>,
    task: Option<JoinHandle<EdgeShutdown>>,
    anchor: PathBuf,
    address: std::net::Ipv4Addr,
}

impl EnvironmentGateway {
    /// Start the edge for one planned network and publish its trust anchor.
    ///
    /// `anchor_root` is where the Environment's own certificate authority is
    /// written, so a client inside the Environment can be given the one thing
    /// it needs to verify the edge. Only the certificate is written; the key
    /// that issues under it never leaves this process, so a reader of the
    /// anchor can verify the edge and cannot impersonate it.
    pub fn start(
        environment_id: &str,
        network: &NetworkPlan,
        socket: std::os::fd::OwnedFd,
        anchor_root: &Path,
    ) -> Result<Option<Self>, GatewayError> {
        let Some(gateway) = &network.gateway else {
            return Ok(None);
        };
        let routes: Vec<EdgeRoute> = network
            .endpoints
            .iter()
            .map(|endpoint| EdgeRoute {
                name: endpoint.name.clone(),
                origin: endpoint.origin,
                port: endpoint.port,
            })
            .collect();
        let names: Vec<String> = routes.iter().map(|route| route.name.clone()).collect();
        // An Environment may declare a public-like network and publish nothing
        // on it. That edge still resolves (to nothing) and still filters; it
        // just has no name to issue for and accepts no connection, which is why
        // the authority is only minted when there is a name.
        let identity = if names.is_empty() {
            None
        } else {
            Some(Arc::new(identity::EdgeIdentity::issue(
                environment_id,
                &names,
            )?))
        };
        let anchor = anchor_root
            .join(environment_id)
            .join(network.network_id.as_str())
            .join("authority.pem");
        if let Some(identity) = &identity {
            write_anchor(&anchor, identity.authority_pem())?;
        }
        let config = EdgeConfig {
            environment_id: environment_id.to_string(),
            network: network.name.clone(),
            mac: gateway.mac,
            address: gateway.address,
            prefix: network.cidr.prefix(),
            mtu: crate::environment_switch::plan::FABRIC_MTU,
            routes,
        };
        let identity = match identity {
            Some(identity) => identity,
            // No declared name, so nothing to issue for. An authority with no
            // certificates under it is still minted rather than skipped, so the
            // edge's shape does not depend on what happens to be declared.
            None => Arc::new(identity::EdgeIdentity::issue(
                environment_id,
                &[format!("edge.{}.invalid", network.name)],
            )?),
        };
        let socket = edge::port_socket(socket)?;
        let running = edge::Edge::new(config, identity)?;
        let (stop, stopped) = oneshot::channel();
        let task = tokio::spawn(running.run(socket, stopped));
        info!(
            environment_id = %environment_id,
            network = %network.name,
            address = %gateway.address,
            names = names.len(),
            anchor = %anchor.display(),
            "started Environment edge gateway"
        );
        Ok(Some(Self {
            shutdown: Some(stop),
            task: Some(task),
            anchor,
            address: gateway.address,
        }))
    }

    pub fn address(&self) -> std::net::Ipv4Addr {
        self.address
    }

    pub fn anchor(&self) -> &Path {
        &self.anchor
    }

    /// Stop the edge and report what it did.
    pub async fn shutdown(&mut self) -> Result<EdgeShutdown, GatewayError> {
        let (Some(stop), Some(task)) = (self.shutdown.take(), self.task.take()) else {
            return Err(GatewayError::AlreadyStopped);
        };
        let _ = stop.send(());
        Ok(task.await?)
    }
}

impl Drop for EnvironmentGateway {
    fn drop(&mut self) {
        if let Some(stop) = self.shutdown.take() {
            let _ = stop.send(());
        }
    }
}

fn write_anchor(path: &Path, pem: &str) -> Result<(), GatewayError> {
    let anchor = |source| GatewayError::Anchor {
        path: path.to_path_buf(),
        source,
    };
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(anchor)?;
    }
    // Replaced rather than appended: an Environment that came back up minted a
    // new authority, and a reader that found both would not know which one the
    // running edge is presenting.
    std::fs::write(path, pem.as_bytes()).map_err(anchor)
}

/// Every Environment's running edges, keyed the way its switches are.
#[derive(Default)]
pub struct EnvironmentEdgeShutdown {
    pub networks: BTreeMap<String, EdgeShutdown>,
}

#[cfg(test)]
#[path = "environment_gateway_tests.rs"]
mod tests;
