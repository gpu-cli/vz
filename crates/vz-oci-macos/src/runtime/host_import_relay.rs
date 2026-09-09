//! The host half of a host import: a per-Machine vsock terminator.
//!
//! The export relay in [`super::networking`] is host-initiated — the host binds
//! `127.0.0.1:<host_port>` and dials the guest. This is the opposite direction
//! and needs a different shape, because the contract's rule for an import is
//! stricter than for an export:
//!
//! > Host imports require exact authenticated Environment/Machine grants to a
//! > declared host-loopback service, independently of external egress. NAT
//! > aliases and wildcard/LAN listeners are not authorization.
//!
//! Three things follow, and this module is where each is enforced:
//!
//! * **The listener is Machine-scoped.** `Vm::vsock_listen` registers a
//!   `VZVirtioSocketListener` on exactly one VM's `VZVirtioSocketDevice`, so a
//!   connection accepted here provably originated in that VM — the framework
//!   sets the source, and the guest cannot forge it. The terminator's table
//!   therefore holds only the imports declared for that Machine. A sibling
//!   Machine, or one in a sibling Environment, has a different VM, a different
//!   listener, a different table and different credentials.
//! * **The guest never names the destination.** The open frame carries a
//!   declaration name; [`HostImportTerminator::authorize`] resolves it to the
//!   stored `host_port`. Nothing in the wire format can express an address, and
//!   nothing here parses the name as one.
//! * **The grant is authenticated.** Reaching the relay port is not
//!   authorization. Each declaration carries a 32-byte credential minted for
//!   one Machine and one boot; a name with no grant and a name with the wrong
//!   credential are both refused, indistinguishably, with no host socket ever
//!   opened.
//!
//! The only host destination this module can ever dial is
//! `127.0.0.1:<stored host_port>` — the address literal is written once, here,
//! and comes from the grant rather than from anything on the wire. There is no
//! code path that binds or dials a wildcard or LAN address for an import.

use std::collections::BTreeMap;
use std::net::Ipv4Addr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::watch;
use tokio::task::JoinSet;
use tracing::{info, warn};

use vz::host_import::{
    CREDENTIAL_BYTES, HOST_IMPORT_RELAY_PORT, HostImportGrant, HostImportOpen,
    OPEN_FRAME_HEADER_BYTES, REPLY_ACCEPTED, REPLY_REFUSED, credentials_match, decode_open,
    open_frame_len, parse_open_header,
};

use crate::error::MacosOciError as OciError;

/// The only host address an import may terminate against.
///
/// A constant rather than a parameter: a caller that could choose the address
/// could choose a LAN address, and the criterion denies exactly that.
pub(crate) const IMPORT_TERMINATION_ADDRESS: Ipv4Addr = Ipv4Addr::LOCALHOST;

/// How long a guest has to send its complete open frame.
///
/// A connection that opens and says nothing would otherwise hold a relay task
/// forever. Short, because the frame is at most 102 bytes and the guest writes
/// it immediately after connecting.
const OPEN_FRAME_DEADLINE: Duration = Duration::from_secs(5);

/// How long the host waits for its own loopback service to accept.
const TERMINATION_DEADLINE: Duration = Duration::from_secs(10);

/// Why one relay connection ended without carrying traffic.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Refusal {
    /// The frame was not a well-formed open frame.
    Malformed(String),
    /// The guest took too long to send its open frame.
    OpenTimeout,
    /// This Machine has no import by that name. Includes an import declared for
    /// a *different* Machine: that name is simply absent from this table.
    UnknownImport { name: String },
    /// The name exists but the credential presented is not its credential.
    BadCredential { name: String },
    /// The grant was authorized, but the declared host service did not answer.
    /// The distinction matters: this is the host's service being down, not the
    /// boundary refusing.
    Unreachable { name: String, reason: String },
}

impl std::fmt::Display for Refusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Malformed(detail) => write!(f, "malformed host import open frame: {detail}"),
            Self::OpenTimeout => write!(f, "host import open frame did not arrive in time"),
            Self::UnknownImport { name } => write!(
                f,
                "this Machine declares no host import named '{name}'; nothing was dialled"
            ),
            Self::BadCredential { name } => write!(
                f,
                "host import '{name}' was presented the wrong credential; nothing was dialled"
            ),
            Self::Unreachable { name, reason } => write!(
                f,
                "host import '{name}' is authorized but its declared loopback service did not answer: {reason}"
            ),
        }
    }
}

/// A refusal that never reveals which half of the grant was wrong.
///
/// The wire reply is one byte and the guest learns only "refused". Telling it
/// whether the *name* or the *credential* was wrong would let it enumerate the
/// declarations of its own Machine one guess at a time.
const fn wire_refusal() -> u8 {
    REPLY_REFUSED
}

/// Why a terminator could not be constructed.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub(crate) enum HostImportRelayError {
    #[error("two host import grants name '{name}'; one name resolves to one host service")]
    DuplicateName { name: String },
    #[error("host import '{name}' declares host port 0, which is not a bindable service")]
    ZeroHostPort { name: String },
    #[error("host import '{name}' has a name the relay open frame cannot carry")]
    UnusableName { name: String },
}

/// What one accepted connection did, for the shutdown receipt.
///
/// Only the success case is named: every refusal is an `Err(Refusal)` carrying
/// which rule refused it, so a second "refused" variant here would be a way to
/// report a denial without saying why.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Outcome {
    /// The grant was authorized and bytes were relayed.
    Relayed,
}

/// The grants one Machine's relay will honour, and nothing else.
pub(crate) struct HostImportTerminator {
    grants: BTreeMap<String, HostImportGrant>,
}

impl HostImportTerminator {
    pub(crate) fn new(grants: &[HostImportGrant]) -> Result<Self, HostImportRelayError> {
        let mut table = BTreeMap::new();
        for grant in grants {
            if grant.host_port == 0 {
                return Err(HostImportRelayError::ZeroHostPort {
                    name: grant.name.clone(),
                });
            }
            if grant.name.is_empty() || grant.name.len() > vz::host_import::MAX_NAME_BYTES {
                return Err(HostImportRelayError::UnusableName {
                    name: grant.name.clone(),
                });
            }
            if table.insert(grant.name.clone(), grant.clone()).is_some() {
                return Err(HostImportRelayError::DuplicateName {
                    name: grant.name.clone(),
                });
            }
        }
        Ok(Self { grants: table })
    }

    /// The host loopback port an open frame is entitled to, or why it is not.
    ///
    /// Both failure modes are computed the same way and cost the same work: the
    /// credential comparison is constant-time, and a missing name does not
    /// short-circuit any faster than a wrong credential does in practice,
    /// because neither touches the network.
    pub(crate) fn authorize(&self, open: &HostImportOpen) -> Result<u16, Refusal> {
        let Some(grant) = self.grants.get(&open.name) else {
            return Err(Refusal::UnknownImport {
                name: open.name.clone(),
            });
        };
        if !credentials_match(&open.credential, &grant.credential) {
            return Err(Refusal::BadCredential {
                name: open.name.clone(),
            });
        }
        Ok(grant.host_port)
    }

    /// Read one open frame from a guest-initiated stream.
    async fn read_open<S: AsyncRead + Unpin>(stream: &mut S) -> Result<HostImportOpen, Refusal> {
        let mut header = [0u8; OPEN_FRAME_HEADER_BYTES];
        tokio::time::timeout(OPEN_FRAME_DEADLINE, stream.read_exact(&mut header))
            .await
            .map_err(|_| Refusal::OpenTimeout)?
            .map_err(|error| Refusal::Malformed(error.to_string()))?;
        let name_len =
            parse_open_header(&header).map_err(|error| Refusal::Malformed(error.to_string()))?;
        let mut rest = vec![0u8; name_len + CREDENTIAL_BYTES];
        tokio::time::timeout(OPEN_FRAME_DEADLINE, stream.read_exact(&mut rest))
            .await
            .map_err(|_| Refusal::OpenTimeout)?
            .map_err(|error| Refusal::Malformed(error.to_string()))?;
        let mut frame = Vec::with_capacity(open_frame_len(name_len));
        frame.extend_from_slice(&header);
        frame.extend_from_slice(&rest);
        decode_open(&frame).map_err(|error| Refusal::Malformed(error.to_string()))
    }

    /// Terminate one guest-initiated stream against its declared host service.
    ///
    /// Generic over the stream so the whole decision path — frame, grant
    /// lookup, credential, reply, connect — is exercisable without a VM. The
    /// vsock transport is the only part a test cannot stand in for, and it
    /// carries no policy.
    pub(crate) async fn terminate<S>(&self, mut guest: S) -> Result<Outcome, Refusal>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        let open = match Self::read_open(&mut guest).await {
            Ok(open) => open,
            Err(refusal) => {
                write_refusal(&mut guest).await;
                return Err(refusal);
            }
        };
        let host_port = match self.authorize(&open) {
            Ok(port) => port,
            Err(refusal) => {
                warn!(
                    import = %open.name,
                    refusal = %refusal,
                    "host import refused; no host connection attempted"
                );
                write_refusal(&mut guest).await;
                return Err(refusal);
            }
        };
        // The one place a host destination is chosen, from the stored grant and
        // a constant loopback address. Nothing from the wire reaches here.
        let mut host = match tokio::time::timeout(
            TERMINATION_DEADLINE,
            TcpStream::connect((IMPORT_TERMINATION_ADDRESS, host_port)),
        )
        .await
        {
            Ok(Ok(stream)) => stream,
            Ok(Err(error)) => {
                write_refusal(&mut guest).await;
                return Err(Refusal::Unreachable {
                    name: open.name,
                    reason: error.to_string(),
                });
            }
            Err(_) => {
                write_refusal(&mut guest).await;
                return Err(Refusal::Unreachable {
                    name: open.name,
                    reason: format!(
                        "no answer within {:.0}s",
                        TERMINATION_DEADLINE.as_secs_f64()
                    ),
                });
            }
        };
        if let Err(error) = guest.write_all(&[REPLY_ACCEPTED]).await {
            return Err(Refusal::Malformed(format!(
                "accepted reply could not be written: {error}"
            )));
        }
        if let Err(error) = guest.flush().await {
            return Err(Refusal::Malformed(format!(
                "accepted reply could not be flushed: {error}"
            )));
        }
        info!(
            import = %open.name,
            host_port,
            "host import authorized; relaying to the declared loopback service"
        );
        if let Err(error) = tokio::io::copy_bidirectional(&mut guest, &mut host).await {
            warn!(import = %open.name, error = %error, "host import relay ended with an error");
        }
        Ok(Outcome::Relayed)
    }
}

/// Tell the guest it was refused, and nothing else.
///
/// Best effort: the guest may already be gone. The refusal stands either way,
/// because nothing was dialled.
async fn write_refusal<S: AsyncWrite + Unpin>(guest: &mut S) {
    let _ = guest.write_all(&[wire_refusal()]).await;
    let _ = guest.flush().await;
}

/// Counters retained for the shutdown receipt.
#[derive(Debug, Default)]
pub(crate) struct HostImportRelayCounters {
    pub(crate) accepted: AtomicU64,
    pub(crate) relayed: AtomicU64,
    pub(crate) refused: AtomicU64,
}

/// One Machine's live import relay.
pub(crate) struct HostImportRelay {
    shutdown_tx: watch::Sender<bool>,
    task: Option<tokio::task::JoinHandle<()>>,
    counters: Arc<HostImportRelayCounters>,
    names: Vec<String>,
}

impl HostImportRelay {
    /// The declarations this relay serves, for evidence and diagnostics.
    pub(crate) fn names(&self) -> &[String] {
        &self.names
    }

    /// Accepted, relayed and refused counts so far.
    pub(crate) fn counts(&self) -> (u64, u64, u64) {
        (
            self.counters.accepted.load(Ordering::Relaxed),
            self.counters.relayed.load(Ordering::Relaxed),
            self.counters.refused.load(Ordering::Relaxed),
        )
    }

    pub(crate) async fn shutdown(&mut self) -> Result<(), OciError> {
        let _ = self.shutdown_tx.send(true);
        let Some(mut task) = self.task.take() else {
            return Ok(());
        };
        match tokio::time::timeout(Duration::from_secs(5), &mut task).await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(error)) if error.is_cancelled() => Ok(()),
            Ok(Err(error)) => Err(OciError::InvalidConfig(format!(
                "VZ_STACK_TEARDOWN_VIOLATION:HOST_IMPORT_RELAY_SHUTDOWN_FAILED host import relay task failed: {error}"
            ))),
            Err(_) => {
                task.abort();
                let _ = task.await;
                Err(OciError::InvalidConfig(
                    "VZ_STACK_TEARDOWN_VIOLATION:HOST_IMPORT_RELAY_SHUTDOWN_FAILED host import relay did not stop within 5s".to_string(),
                ))
            }
        }
    }
}

impl Drop for HostImportRelay {
    fn drop(&mut self) {
        let _ = self.shutdown_tx.send(true);
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

/// Install one Machine's import relay on its own VM.
///
/// `Vm::vsock_listen` scopes the listener to this VM's socket device, which is
/// what makes the Machine identity of every accepted connection structural
/// rather than asserted. An empty grant list installs nothing at all: a Machine
/// with no declared import must have no relay port answering, because "absent
/// by default" is a clause of the criterion and not an implementation detail.
pub(crate) async fn start_host_import_relay(
    vm: Arc<vz::vm::Vm>,
    grants: Vec<HostImportGrant>,
) -> Result<Option<HostImportRelay>, OciError> {
    if grants.is_empty() {
        return Ok(None);
    }
    let names: Vec<String> = grants.iter().map(|grant| grant.name.clone()).collect();
    let terminator = Arc::new(
        HostImportTerminator::new(&grants)
            .map_err(|error| OciError::InvalidConfig(error.to_string()))?,
    );
    let mut listener = vm
        .vsock_listen(HOST_IMPORT_RELAY_PORT)
        .await
        .map_err(|error| {
            OciError::InvalidConfig(format!(
                "host import relay could not listen on vsock port {HOST_IMPORT_RELAY_PORT}: {error}"
            ))
        })?;
    let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
    let counters = Arc::new(HostImportRelayCounters::default());
    let task_counters = Arc::clone(&counters);
    let task = tokio::spawn(async move {
        let mut connections = JoinSet::new();
        loop {
            tokio::select! {
                changed = shutdown_rx.changed() => {
                    if changed.is_err() || *shutdown_rx.borrow() {
                        break;
                    }
                }
                accepted = listener.accept() => {
                    match accepted {
                        Ok(accepted) => {
                            task_counters.accepted.fetch_add(1, Ordering::Relaxed);
                            let terminator = Arc::clone(&terminator);
                            let counters = Arc::clone(&task_counters);
                            // The framework reports the source VM on every
                            // accept. It is the same VM for every connection on
                            // this listener by construction; recording it makes
                            // that checkable in a log rather than only in prose.
                            let source = accepted.source.as_u64();
                            connections.spawn(async move {
                                match terminator.terminate(accepted.stream).await {
                                    Ok(Outcome::Relayed) => {
                                        counters.relayed.fetch_add(1, Ordering::Relaxed);
                                    }
                                    Err(_) => {
                                        counters.refused.fetch_add(1, Ordering::Relaxed);
                                    }
                                }
                                let _ = source;
                            });
                        }
                        Err(error) => {
                            warn!(error = %error, "host import relay accept failed");
                            break;
                        }
                    }
                }
                joined = connections.join_next(), if !connections.is_empty() => {
                    if let Some(Err(error)) = joined {
                        if !error.is_cancelled() {
                            warn!(error = %error, "host import relay connection task failed");
                        }
                    }
                }
            }
        }
        connections.abort_all();
        while connections.join_next().await.is_some() {}
    });
    info!(
        imports = ?names,
        vsock_port = HOST_IMPORT_RELAY_PORT,
        "host import relay listening on this Machine's vsock device"
    );
    Ok(Some(HostImportRelay {
        shutdown_tx,
        task: Some(task),
        counters,
        names,
    }))
}

#[cfg(test)]
#[path = "host_import_relay/tests.rs"]
mod tests;
