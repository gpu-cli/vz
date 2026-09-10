//! The guest half of a host import: a loopback listener that dials the host.
//!
//! An import is the only direction in which this guest originates a connection
//! to its host, and the contract's rule is that reaching the host is not the
//! same as being authorized to reach a host service. So the guest holds three
//! things per declaration and nothing more: a name, a loopback port to bind,
//! and the credential to present. It holds no host address of any kind, and
//! [`vz::host_import::GuestHostImportGrant`] has no field able to carry one.
//!
//! Two properties are enforced here rather than assumed:
//!
//! * **Loopback only.** Every listener binds `127.0.0.1`, never `0.0.0.0`. A
//!   wildcard listener would put the import on the Machine's fabric NIC, where
//!   a sibling Machine on the same declared network could reach it — that
//!   sibling is not the authorized Machine, and the port relay would have
//!   become a second, undeclared path to a host service.
//! * **Replacement, not accumulation.** `configure` installs exactly the set it
//!   is given. An Environment whose declarations shrank does not keep serving
//!   the import it dropped, because a listener nothing declares any more is an
//!   undeclared boundary.
//!
//! What this module cannot do is choose a destination. Its outbound call is
//! `connect_host(HOST_IMPORT_RELAY_PORT)` with a compile-time constant port to
//! `VMADDR_CID_HOST`, followed by an open frame whose only variable content is
//! the declaration name and the secret. Everything about *where on the host*
//! the stream terminates is decided by the host terminator.

// Only a Linux Machine declares host imports; the module is compiled for the
// native macOS agent too so the RPC surface is identical, and its listeners are
// simply never configured there.
use std::collections::BTreeMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::OnceLock;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tracing::{info, warn};

use vz::host_import::{
    GuestHostImportGrant, HOST_IMPORT_RELAY_PORT, HostImportOpen, REPLY_ACCEPTED, encode_open,
};

use crate::listener::connect_host;

/// The address every import listener binds. Never configurable.
pub(crate) const IMPORT_BIND_ADDRESS: Ipv4Addr = Ipv4Addr::LOCALHOST;

/// Why a set of grants could not be installed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ConfigureError {
    /// Two grants named the same declaration, or the same loopback port. The
    /// second would silently shadow the first, so neither is installed.
    Duplicate(String),
    /// A grant this format cannot express: an empty name, an over-long name, or
    /// port 0 (which the kernel would resolve to an arbitrary free port the
    /// host never granted).
    Invalid(String),
    /// The loopback port could not be bound.
    Bind { name: String, reason: String },
}

impl std::fmt::Display for ConfigureError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Duplicate(detail) => write!(f, "duplicate host import grant: {detail}"),
            Self::Invalid(detail) => write!(f, "unusable host import grant: {detail}"),
            Self::Bind { name, reason } => write!(
                f,
                "host import '{name}' could not bind its guest loopback port: {reason}"
            ),
        }
    }
}

impl std::error::Error for ConfigureError {}

/// The installed set, and the accept loops serving it.
struct Installed {
    grants: Vec<GuestHostImportGrant>,
    tasks: Vec<JoinHandle<()>>,
}

impl Installed {
    async fn stop(self) {
        for task in &self.tasks {
            task.abort();
        }
        // Awaiting the aborted handles is what makes rebinding the same port
        // immediately afterwards sound: the listener socket is closed when its
        // task's future is dropped, and the drop has happened once the join
        // resolves.
        for task in self.tasks {
            let _ = task.await;
        }
    }
}

/// This guest's installed imports. One per process, like the forward grants:
/// the agent serves one Machine and an import is a property of that Machine.
pub(crate) struct HostImports {
    installed: Mutex<Option<Installed>>,
}

impl HostImports {
    fn new() -> Self {
        Self {
            installed: Mutex::new(None),
        }
    }

    /// Install exactly `grants`, replacing whatever was installed before.
    ///
    /// Re-sending the identical set is the idempotent case a second Up of one
    /// definition produces, and it deliberately does not rebind: tearing a live
    /// listener down and putting an identical one back would drop connections
    /// that are relaying at that moment.
    pub(crate) async fn configure(
        &self,
        grants: Vec<GuestHostImportGrant>,
    ) -> Result<Vec<String>, ConfigureError> {
        validate(&grants)?;
        let mut installed = self.installed.lock().await;
        if let Some(current) = installed.as_ref() {
            if current.grants == grants {
                return Ok(grants.into_iter().map(|grant| grant.name).collect());
            }
        }
        if let Some(current) = installed.take() {
            current.stop().await;
        }
        let mut tasks = Vec::with_capacity(grants.len());
        let mut names = Vec::with_capacity(grants.len());
        for grant in &grants {
            let address = SocketAddr::new(IpAddr::V4(IMPORT_BIND_ADDRESS), grant.guest_port);
            let listener = match TcpListener::bind(address).await {
                Ok(listener) => listener,
                Err(error) => {
                    // Nothing partially installed survives a failure: the
                    // listeners already bound in this call are torn down before
                    // the error is reported, so a caller that retries starts
                    // from the same state it started from.
                    Installed {
                        grants: Vec::new(),
                        tasks,
                    }
                    .stop()
                    .await;
                    return Err(ConfigureError::Bind {
                        name: grant.name.clone(),
                        reason: error.to_string(),
                    });
                }
            };
            info!(
                import = %grant.name,
                address = %address,
                "host import listener bound on guest loopback"
            );
            names.push(grant.name.clone());
            let serving = grant.clone();
            tasks.push(tokio::spawn(async move {
                serve(listener, serving).await;
            }));
        }
        *installed = Some(Installed { grants, tasks });
        Ok(names)
    }

    /// The names currently installed, for diagnostics and tests.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) async fn installed_names(&self) -> Vec<String> {
        self.installed
            .lock()
            .await
            .as_ref()
            .map(|installed| {
                installed
                    .grants
                    .iter()
                    .map(|grant| grant.name.clone())
                    .collect()
            })
            .unwrap_or_default()
    }
}

/// Refuse a set that cannot be installed unambiguously, before anything binds.
fn validate(grants: &[GuestHostImportGrant]) -> Result<(), ConfigureError> {
    let mut by_name: BTreeMap<&str, ()> = BTreeMap::new();
    let mut by_port: BTreeMap<u16, &str> = BTreeMap::new();
    for grant in grants {
        if grant.name.is_empty() {
            return Err(ConfigureError::Invalid(
                "empty declaration name".to_string(),
            ));
        }
        if grant.name.len() > vz::host_import::MAX_NAME_BYTES {
            return Err(ConfigureError::Invalid(format!(
                "declaration name '{}' exceeds {} bytes",
                grant.name,
                vz::host_import::MAX_NAME_BYTES
            )));
        }
        // Port 0 asks the kernel to pick. The host granted an exact port, and a
        // kernel-chosen one is a port nothing declared.
        if grant.guest_port == 0 {
            return Err(ConfigureError::Invalid(format!(
                "host import '{}' declares guest port 0",
                grant.name
            )));
        }
        if by_name.insert(grant.name.as_str(), ()).is_some() {
            return Err(ConfigureError::Duplicate(format!(
                "two grants name '{}'",
                grant.name
            )));
        }
        if let Some(previous) = by_port.insert(grant.guest_port, grant.name.as_str()) {
            return Err(ConfigureError::Duplicate(format!(
                "'{}' and '{}' both bind guest loopback port {}",
                previous, grant.name, grant.guest_port
            )));
        }
    }
    Ok(())
}

/// Accept on one import's loopback listener until the task is aborted.
async fn serve(listener: TcpListener, grant: GuestHostImportGrant) {
    loop {
        match listener.accept().await {
            Ok((inbound, _peer)) => {
                let grant = grant.clone();
                tokio::spawn(async move {
                    if let Err(error) = relay(inbound, &grant).await {
                        warn!(import = %grant.name, error = %error, "host import relay failed");
                    }
                });
            }
            Err(error) => {
                warn!(import = %grant.name, error = %error, "host import listener accept failed");
                return;
            }
        }
    }
}

/// Carry one accepted guest connection to the host terminator.
async fn relay(mut inbound: TcpStream, grant: &GuestHostImportGrant) -> Result<(), String> {
    let frame = encode_open(&HostImportOpen {
        name: grant.name.clone(),
        credential: grant.credential,
    })
    .map_err(|error| error.to_string())?;
    let mut host = connect_host(HOST_IMPORT_RELAY_PORT)
        .await
        .map_err(|error| format!("vsock dial to the host relay failed: {error}"))?;
    host.write_all(&frame)
        .await
        .map_err(|error| format!("open frame write failed: {error}"))?;
    host.flush()
        .await
        .map_err(|error| format!("open frame flush failed: {error}"))?;
    let mut reply = [0u8; 1];
    host.read_exact(&mut reply)
        .await
        .map_err(|error| format!("host relay closed before replying: {error}"))?;
    if reply[0] != REPLY_ACCEPTED {
        // The host refused the grant. The guest learns only that, never why:
        // distinguishing "no such import" from "wrong credential" would tell a
        // caller which half of the grant to keep guessing at.
        return Err("the host refused this import grant".to_string());
    }
    tokio::io::copy_bidirectional(&mut inbound, &mut host)
        .await
        .map_err(|error| format!("relay failed: {error}"))?;
    Ok(())
}

/// The process-wide import set.
pub(crate) fn imports() -> &'static HostImports {
    static IMPORTS: OnceLock<HostImports> = OnceLock::new();
    IMPORTS.get_or_init(HostImports::new)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    use super::*;

    fn grant(name: &str, port: u16) -> GuestHostImportGrant {
        GuestHostImportGrant {
            name: name.to_string(),
            guest_port: port,
            credential: [1u8; vz::host_import::CREDENTIAL_BYTES],
        }
    }

    /// A free loopback port, released before the caller binds it. Racy in
    /// principle; the alternative is a hard-coded port that collides with
    /// whatever else this machine runs.
    fn free_port() -> u16 {
        let listener = std::net::TcpListener::bind(("127.0.0.1", 0)).expect("ephemeral bind");
        let port = listener.local_addr().expect("local addr").port();
        drop(listener);
        port
    }

    #[test]
    fn the_bind_address_is_loopback_and_not_a_wildcard() {
        assert_eq!(IMPORT_BIND_ADDRESS, Ipv4Addr::new(127, 0, 0, 1));
        assert!(IMPORT_BIND_ADDRESS.is_loopback());
        assert_ne!(IMPORT_BIND_ADDRESS, Ipv4Addr::UNSPECIFIED);
    }

    #[test]
    fn a_repeated_name_or_port_is_refused_before_anything_binds() {
        assert!(matches!(
            validate(&[grant("db", 15432), grant("db", 15433)]),
            Err(ConfigureError::Duplicate(_))
        ));
        assert!(matches!(
            validate(&[grant("db", 15432), grant("cache", 15432)]),
            Err(ConfigureError::Duplicate(_))
        ));
        assert!(validate(&[grant("db", 15432), grant("cache", 15433)]).is_ok());
    }

    #[test]
    fn a_kernel_chosen_port_is_refused_because_nothing_granted_it() {
        assert!(matches!(
            validate(&[grant("db", 0)]),
            Err(ConfigureError::Invalid(_))
        ));
    }

    #[test]
    fn a_name_the_open_frame_cannot_carry_is_refused() {
        assert!(matches!(
            validate(&[grant("", 15432)]),
            Err(ConfigureError::Invalid(_))
        ));
        let long = "n".repeat(vz::host_import::MAX_NAME_BYTES + 1);
        assert!(matches!(
            validate(&[grant(&long, 15432)]),
            Err(ConfigureError::Invalid(_))
        ));
    }

    #[tokio::test]
    async fn a_configured_import_listens_on_loopback_and_nowhere_else() {
        let imports = HostImports::new();
        let port = free_port();
        let bound = imports
            .configure(vec![grant("db", port)])
            .await
            .expect("configured");
        assert_eq!(bound, vec!["db".to_string()]);
        // Loopback answers.
        TcpStream::connect(("127.0.0.1", port))
            .await
            .expect("loopback reaches the import listener");
        // A non-loopback local address does not. Binding the same port on
        // 0.0.0.0 would fail if a wildcard listener already held it, so a
        // successful wildcard bind proves the import listener is not one.
        let wildcard = TcpListener::bind((Ipv4Addr::UNSPECIFIED, port)).await;
        assert!(
            wildcard.is_ok(),
            "a wildcard bind on the same port must be free, proving the import \
             listener holds only 127.0.0.1: {wildcard:?}"
        );
    }

    /// A grant is for the declared stream protocol and carries nothing else.
    ///
    /// `HostImportSpec::protocol` is `TransportProtocol`, whose only variant is
    /// `Tcp`, and the projection the guest is handed
    /// ([`GuestHostImportGrant`]) has no protocol field at all — so a datagram
    /// grant is unconstructible rather than refused. What that leaves to prove
    /// is the observable half: the guest port an import binds is a stream
    /// listener and *nothing is bound to it as a datagram socket*, so a
    /// datagram addressed to the declared port reaches no import.
    ///
    /// Binding the same port as UDP is the proof: it succeeds, which it could
    /// not do if this process already held a datagram socket there.
    #[tokio::test]
    async fn a_declared_import_binds_a_stream_listener_and_no_datagram_socket() {
        let imports = HostImports::new();
        let port = free_port();
        imports
            .configure(vec![grant("db", port)])
            .await
            .expect("configured");
        // The declared stream port answers.
        TcpStream::connect(("127.0.0.1", port))
            .await
            .expect("loopback reaches the import listener");
        // The same port as a datagram socket is free, so the import serves no
        // datagrams: nothing in this agent listens for them.
        let datagram = tokio::net::UdpSocket::bind(("127.0.0.1", port)).await;
        assert!(
            datagram.is_ok(),
            "a UDP bind on the declared import port must be free, proving the \
             grant carries the declared stream protocol only: {datagram:?}"
        );
    }

    #[tokio::test]
    async fn reconfiguring_replaces_rather_than_accumulates() {
        let imports = HostImports::new();
        let first = free_port();
        let second = free_port();
        imports
            .configure(vec![grant("db", first)])
            .await
            .expect("first set");
        assert_eq!(imports.installed_names().await, vec!["db".to_string()]);
        imports
            .configure(vec![grant("cache", second)])
            .await
            .expect("second set");
        assert_eq!(imports.installed_names().await, vec!["cache".to_string()]);
        // The dropped declaration's listener is gone, not merely unreferenced:
        // its port is bindable again.
        TcpListener::bind(("127.0.0.1", first))
            .await
            .expect("the withdrawn import released its loopback port");
    }

    #[tokio::test]
    async fn an_empty_set_withdraws_every_listener() {
        let imports = HostImports::new();
        let port = free_port();
        imports
            .configure(vec![grant("db", port)])
            .await
            .expect("configured");
        imports.configure(Vec::new()).await.expect("withdrawn");
        assert!(imports.installed_names().await.is_empty());
        TcpListener::bind(("127.0.0.1", port))
            .await
            .expect("the withdrawn import released its loopback port");
    }

    #[tokio::test]
    async fn an_unbindable_port_leaves_nothing_half_installed() {
        let imports = HostImports::new();
        let free = free_port();
        let held = std::net::TcpListener::bind(("127.0.0.1", 0)).expect("held");
        let taken = held.local_addr().expect("addr").port();
        let error = imports
            .configure(vec![grant("db", free), grant("cache", taken)])
            .await
            .expect_err("the second grant cannot bind");
        assert!(matches!(error, ConfigureError::Bind { .. }), "{error:?}");
        assert!(imports.installed_names().await.is_empty());
        // The first grant's listener was rolled back, so its port is free.
        TcpListener::bind(("127.0.0.1", free))
            .await
            .expect("the rolled-back grant released its loopback port");
    }
}
