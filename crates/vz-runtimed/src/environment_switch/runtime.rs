//! The running switch: one task per network, owning both ends of every port.
//!
//! A port is a connected `AF_UNIX` datagram socket pair. The runtime keeps the
//! host end and hands the guest end to that Machine's NIC, so a network is a set
//! of sockets this process forwards between. That is what makes an Environment's
//! fabric unreachable from another Environment: there is no shared segment, and
//! a frame can only enter a fabric through a socket the runtime handed out.
//!
//! Datagrams carry exactly one Ethernet frame, which is what the file-handle
//! attachment requires and also what makes the forwarding decision total: a read
//! yields one whole frame or nothing.
//!
//! Delivery is lossy by construction. A port whose receive buffer is full has
//! its frame dropped and counted rather than blocking the forwarder, because
//! blocking would let one slow Machine stall every other Machine on the network,
//! and Ethernet does not promise delivery in the first place.

use std::collections::BTreeMap;
use std::os::fd::OwnedFd;
use std::sync::Arc;

use rustix::net::sockopt;
use tokio::net::UnixDatagram;
use tokio::sync::{mpsc, oneshot};
use tokio::task::{JoinHandle, JoinSet};
use tracing::{info, warn};

use super::{Counters, Disposition, Fabric, FabricError, FrameHeader, MacAddress, PortId};

/// The largest datagram a port will read. The file-handle attachment allows an
/// MTU up to 65535, and a read shorter than the frame truncates it silently, so
/// the buffer is that maximum rather than the configured MTU.
const MAX_FRAME_BYTES: usize = 65535;
/// Frames in flight between the readers and the forwarder. Bounded, so a burst
/// is dropped at a counted point rather than growing memory without limit.
const QUEUE_DEPTH: usize = 1024;
/// One mebibyte of send buffer and four of receive: the ratio the file-handle
/// attachment requires and the multiple it recommends. The host end is sized the
/// same way, because a buffer too small to hold whole frames loses whole frames.
const SEND_BUFFER_BYTES: usize = 1024 * 1024;
const RECEIVE_BUFFER_BYTES: usize = 4 * 1024 * 1024;

#[derive(Debug, thiserror::Error)]
pub enum SwitchError {
    #[error("switch port setup: {0}")]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Fabric(#[from] FabricError),
    #[error("switch supervisor: {0}")]
    Task(#[from] tokio::task::JoinError),
    #[error("switch already stopped")]
    AlreadyStopped,
}

/// What one switch did, reported once its task has been joined.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SwitchShutdown {
    pub frames_read: u64,
    pub frames_delivered: u64,
    /// Why the fabric refused frames, by rule.
    pub counters: Counters,
    /// Frames a port could not accept because its receive buffer was full.
    pub undeliverable: u64,
}

/// The guest end of one port, to be handed to that Machine's NIC.
#[derive(Debug)]
pub struct GuestPort {
    pub port: PortId,
    pub address: MacAddress,
    pub socket: OwnedFd,
}

/// One Environment network's running switch.
///
/// Explicit shutdown joins the task before the caller may release the Machines.
/// Drop requests the same bounded teardown rather than leaving it running.
#[must_use = "retain the switch and await shutdown before stopping its Machines"]
#[derive(Debug)]
pub struct NetworkSwitch {
    shutdown: Option<oneshot::Sender<()>>,
    task: Option<JoinHandle<SwitchShutdown>>,
    ports: BTreeMap<PortId, MacAddress>,
    /// Where [`Self::add_port`] asks the forwarding task to attach a port.
    ///
    /// The task owns the fabric, the host ends and the reader set, so a port can
    /// only be added from inside it. Retaining this sender means the frame
    /// channel never closes on its own, which is sound because the frame
    /// channel was never what stopped the switch: the task selects `biased` on
    /// the shutdown oneshot first, and both `shutdown()` and `Drop` send it.
    /// Channel closure was only ever a backstop for "every reader died".
    control: mpsc::Sender<AddPort>,
}

/// One request to attach a port to a switch that is already forwarding.
struct AddPort {
    port: PortId,
    address: MacAddress,
    host: Arc<UnixDatagram>,
    /// Carries the fabric's own refusal back to the caller. A port the fabric
    /// rejects -- a duplicate id, a repeated address -- must fail the Machine
    /// that asked for it, not be logged and forgotten inside the task.
    settled: oneshot::Sender<Result<(), FabricError>>,
}

impl NetworkSwitch {
    /// Create every port, start forwarding, and return the guest ends.
    ///
    /// The fabric is built before any socket, so a duplicate port or a repeated
    /// address is refused without first creating descriptors that would then
    /// need unwinding.
    ///
    /// `network` names this switch in its diagnostics. Two Environments may
    /// declare the same network name and a Machine may hold ports on several
    /// networks, so a refusal that did not say which switch decided it could
    /// not be attributed to a fabric at all.
    pub fn start(
        network: &str,
        members: impl IntoIterator<Item = (PortId, MacAddress)>,
    ) -> Result<(Self, Vec<GuestPort>), SwitchError> {
        let members: Vec<(PortId, MacAddress)> = members.into_iter().collect();
        let mut fabric = Fabric::new();
        for (port, address) in &members {
            fabric.attach(*port, *address)?;
        }

        let mut hosts = BTreeMap::new();
        let mut guests = Vec::with_capacity(members.len());
        for (port, address) in &members {
            let (host, guest) = UnixDatagram::pair()?;
            size_buffers(&host)?;
            let guest = OwnedFd::from(guest.into_std()?);
            size_buffers(&guest)?;
            hosts.insert(*port, Arc::new(host));
            guests.push(GuestPort {
                port: *port,
                address: *address,
                socket: guest,
            });
        }

        let (stop, mut stopped) = oneshot::channel();
        let (sender, mut received) = mpsc::channel::<(PortId, Vec<u8>)>(QUEUE_DEPTH);
        let (control, mut controls) = mpsc::channel::<AddPort>(QUEUE_DEPTH);
        let mut readers = JoinSet::new();
        for (port, socket) in &hosts {
            spawn_reader(&mut readers, *port, Arc::clone(socket), sender.clone());
        }
        // The sender is retained rather than dropped, because a port added later
        // needs a reader and a reader needs a sender. See `NetworkSwitch.control`
        // for why this does not affect how the switch stops.

        let ports: BTreeMap<PortId, MacAddress> = members.into_iter().collect();
        let network = network.to_string();
        let task = tokio::spawn(async move {
            let mut receipt = SwitchShutdown::default();
            loop {
                let frame = tokio::select! {
                    biased;
                    _ = &mut stopped => break,
                    // Attaching a port is handled where the fabric, the host
                    // ends and the reader set actually live. Checked before
                    // frames so a Machine waiting to be attached is not held
                    // behind a busy network, and it yields immediately when
                    // there is nothing to attach.
                    request = controls.recv() => {
                        let Some(request) = request else { break };
                        let AddPort { port, address, host, settled } = request;
                        let outcome = fabric.attach(port, address);
                        if outcome.is_ok() {
                            hosts.insert(port, Arc::clone(&host));
                            spawn_reader(&mut readers, port, host, sender.clone());
                            info!(
                                network = %network,
                                %port,
                                %address,
                                "Environment network switch attached a port to a running fabric"
                            );
                        }
                        // A caller that stopped waiting is not a reason to undo
                        // an attachment the fabric accepted; the port simply has
                        // no guest yet, which is the state it was in a moment ago.
                        let _ = settled.send(outcome);
                        continue;
                    },
                    frame = received.recv() => frame,
                };
                let Some((ingress, frame)) = frame else { break };
                receipt.frames_read += 1;
                let targets = match fabric.forward(ingress, &frame) {
                    Disposition::Unicast(port) => vec![port],
                    Disposition::Group(ports) => ports,
                    Disposition::Drop(reason) => {
                        // Once per reason, not once per frame: the rules are
                        // what a refusal has to be attributed to, and a fabric
                        // that refuses everything would otherwise say so
                        // thousands of times or, as it did, not at all. The
                        // counters are only reported at shutdown, and an
                        // Environment under investigation is exactly the one
                        // that is not being torn down.
                        if fabric.counters().dropped.get(&reason) == Some(&1) {
                            warn!(
                                network = %network,
                                ingress = %ingress,
                                assigned = ?fabric.address_of(ingress),
                                header = ?FrameHeader::parse(&frame),
                                ?reason,
                                "Environment network switch refused a frame"
                            );
                        }
                        continue;
                    }
                };
                for target in targets {
                    // A full receive buffer means that Machine is not keeping up.
                    // The frame is dropped and counted, never retried, so one slow
                    // Machine cannot stall the network.
                    match hosts.get(&target).map(|socket| socket.try_send(&frame)) {
                        Some(Ok(_)) => {
                            receipt.frames_delivered += 1;
                            // The first delivery is the one fact that says a
                            // guest end is really attached at both ends; every
                            // later one says the same thing again.
                            if receipt.frames_delivered == 1 {
                                info!(
                                    network = %network,
                                    ingress = %ingress,
                                    egress = %target,
                                    "Environment network switch carried its first frame"
                                );
                            }
                        }
                        Some(Err(_)) | None => receipt.undeliverable += 1,
                    }
                }
            }
            readers.abort_all();
            while readers.join_next().await.is_some() {}
            receipt.counters = fabric.counters().clone();
            receipt
        });

        Ok((
            Self {
                shutdown: Some(stop),
                task: Some(task),
                ports,
                control,
            },
            guests,
        ))
    }

    /// Attach one more port to a switch that is already forwarding.
    ///
    /// This is what lets a Machine join an Environment whose fabric is already
    /// up, which a fork is: its parent is running and holding its own port, so
    /// rebuilding the switch would mean tearing down the very Machine the fork
    /// exists to inherit warm state from.
    ///
    /// The guest end is created here and returned, so a caller that is refused
    /// gets no descriptor at all; the host end and the fabric mutation are
    /// handed to the forwarding task, which owns them. The port is live for
    /// forwarding when this returns.
    pub async fn add_port(
        &mut self,
        port: PortId,
        address: MacAddress,
    ) -> Result<GuestPort, SwitchError> {
        let (host, guest) = UnixDatagram::pair()?;
        size_buffers(&host)?;
        let guest = OwnedFd::from(guest.into_std()?);
        size_buffers(&guest)?;
        let (settled, decided) = oneshot::channel();
        self.control
            .send(AddPort {
                port,
                address,
                host: Arc::new(host),
                settled,
            })
            .await
            .map_err(|_| SwitchError::AlreadyStopped)?;
        decided.await.map_err(|_| SwitchError::AlreadyStopped)??;
        self.ports.insert(port, address);
        Ok(GuestPort {
            port,
            address,
            socket: guest,
        })
    }

    /// The address assigned to each attached port.
    pub fn ports(&self) -> &BTreeMap<PortId, MacAddress> {
        &self.ports
    }

    /// Stop forwarding and join the task, returning what it did.
    pub async fn shutdown(&mut self) -> Result<SwitchShutdown, SwitchError> {
        let stop = self.shutdown.take().ok_or(SwitchError::AlreadyStopped)?;
        // A closed channel means the task already exited on its own, which is
        // not a failure to stop.
        let _ = stop.send(());
        let task = self.task.take().ok_or(SwitchError::AlreadyStopped)?;
        Ok(task.await?)
    }
}

impl Drop for NetworkSwitch {
    fn drop(&mut self) {
        if let Some(stop) = self.shutdown.take() {
            let _ = stop.send(());
        }
        // The task observes the stop and drains its readers. Aborting it here
        // would skip that joined teardown.
    }
}

/// Read whole frames off one port's host end into the forwarder.
///
/// Shared by construction and by [`NetworkSwitch::add_port`] so a port added to
/// a running fabric is read exactly the way every other port is.
fn spawn_reader(
    readers: &mut JoinSet<()>,
    port: PortId,
    socket: Arc<UnixDatagram>,
    sender: mpsc::Sender<(PortId, Vec<u8>)>,
) {
    readers.spawn(async move {
        let mut buffer = vec![0_u8; MAX_FRAME_BYTES];
        loop {
            let Ok(read) = socket.recv(&mut buffer).await else {
                return;
            };
            if sender.send((port, buffer[..read].to_vec())).await.is_err() {
                return;
            }
        }
    });
}

/// Size a port's buffers for whole frames, on either end.
fn size_buffers<Fd: rustix::fd::AsFd>(socket: &Fd) -> std::io::Result<()> {
    sockopt::set_socket_send_buffer_size(socket, SEND_BUFFER_BYTES)?;
    sockopt::set_socket_recv_buffer_size(socket, RECEIVE_BUFFER_BYTES)?;
    Ok(())
}

#[cfg(test)]
#[path = "runtime_tests.rs"]
mod tests;
