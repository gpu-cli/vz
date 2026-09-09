//! The running edge: one stack, on one switch port, inside one Environment.
//!
//! The edge holds the guest end of an ordinary switch port. It is not attached
//! to a VM and it is not attached to the host's network stack; the host kernel
//! never sees these packets at all, because the whole path is a datagram socket
//! pair and a userspace stack in this process. That is what makes "nothing
//! listens on the host LAN" a structural property rather than a configuration:
//! there is no host socket to bind, no interface to bind it to, and no address
//! outside the Environment's own range anywhere in this file.
//!
//! What it does with that port is the four clauses of a public-like edge:
//!
//! * it answers the Environment's names, and only those (`dns`);
//! * it terminates TLS for the names the Environment declared, and only those
//!   (`identity`);
//! * it carries the plaintext to the declared origin over a connection it opens
//!   itself, so the origin sees the edge as its peer and never the client —
//!   which is the address translation, and is also the evidence that the path
//!   crossed the edge rather than going straight to the origin;
//! * and it drops everything else before the stack parses it (`firewall`).
//!
//! The proxy is a termination, not a forward. There is no route through this
//! stack: a packet addressed to anything but the edge is dropped, so a Machine
//! cannot use the edge to reach a sibling it has no declared path to, and the
//! origin connection exists only because the edge decided to open it.

use std::collections::{BTreeMap, VecDeque};
use std::io::{Read, Write};
use std::net::Ipv4Addr;
use std::os::fd::OwnedFd;
use std::sync::Arc;

use smoltcp::iface::{Config, Interface, PollResult, SocketHandle, SocketSet};
use smoltcp::phy::{Checksum, ChecksumCapabilities, Device, DeviceCapabilities, Medium};
use smoltcp::socket::{tcp, udp};
use smoltcp::time::Instant;
use smoltcp::wire::{EthernetAddress, HardwareAddress, IpAddress, IpCidr, IpListenEndpoint};
use tokio::net::UnixDatagram;
use tokio::sync::oneshot;
use tracing::{info, warn};

use super::dns;
use super::firewall::{Firewall, FirewallCounters, INGRESS_PORT, Verdict};
use super::identity::EdgeIdentity;
use crate::environment_switch::MacAddress;

/// How many ingress connections one edge terminates at once.
///
/// Bounded, and bounded small. Each session holds a TLS state machine and two
/// stack sockets, and an edge that accepted without limit would let one
/// Environment's client exhaust the daemon rather than its own Environment.
/// A ninth connection waits for a slot rather than being refused, because the
/// listener backlog is where waiting belongs.
const MAX_SESSIONS: usize = 8;
/// Per-socket buffers. One MTU is not enough for a TLS record plus a response
/// body, and these are the only allocations that scale with concurrency.
const SOCKET_BUFFER_BYTES: usize = 64 * 1024;
/// The largest datagram a port will read; the switch's own bound.
const MAX_FRAME_BYTES: usize = 65535;
/// How many frames the stack may have queued in either direction before the
/// loop stops reading and lets it drain.
const QUEUE_DEPTH: usize = 256;
/// Where the edge's translated local ports start. The range runs from here to
/// the last port there is, so wrapping back to it is the only bound needed.
const FIRST_EPHEMERAL_PORT: u16 = 49152;
/// The longest the loop will wait when the stack has no timer of its own. It is
/// an upper bound on the wait, never a delay that is taken: the loop wakes on
/// the port becoming readable, and this only bounds how long it would otherwise
/// block if nothing at all happened.
const IDLE_WAIT: std::time::Duration = std::time::Duration::from_millis(500);

#[derive(Debug, thiserror::Error)]
pub enum EdgeError {
    #[error("edge port: {0}")]
    Io(#[from] std::io::Error),
    #[error("edge stack: {0}")]
    Stack(String),
}

/// One declared service the edge publishes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EdgeRoute {
    /// The name the edge answers for and presents a certificate for.
    pub name: String,
    /// The Machine address the edge opens its own connection to.
    pub origin: Ipv4Addr,
    /// The port on `origin`.
    pub port: u16,
}

/// Everything one edge is, before it holds a socket.
#[derive(Debug, Clone)]
pub struct EdgeConfig {
    pub environment_id: String,
    pub network: String,
    pub mac: MacAddress,
    pub address: Ipv4Addr,
    pub prefix: u8,
    pub mtu: u32,
    pub routes: Vec<EdgeRoute>,
}

/// What one edge did, reported once its task has been joined.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct EdgeShutdown {
    /// Frames the filter judged, by verdict.
    pub firewall: FirewallCounters,
    /// Queries the resolver answered, by decision.
    pub resolved: u64,
    pub unresolved: u64,
    /// Ingress connections that reached a declared origin.
    pub sessions_routed: u64,
    /// Ingress connections that ended without reaching one: an undeclared name,
    /// a handshake that failed, or an origin that refused.
    pub sessions_refused: u64,
    /// Bytes carried in each direction across the edge.
    pub bytes_to_origin: u64,
    pub bytes_to_client: u64,
    /// Why the last session that ended abnormally did.
    ///
    /// Kept because a refused session is otherwise indistinguishable from one
    /// that simply never arrived, and the difference is the whole diagnosis: a
    /// count of refusals says something went wrong somewhere, and this says
    /// where.
    pub last_refusal: Option<String>,
}

// -- the port as a smoltcp device ------------------------------------------------

/// The switch port, seen by the stack as a network device.
///
/// Frames are queued rather than read and written inline because the stack's
/// device trait is synchronous and the port is not: the loop fills `inbound`
/// from the socket, lets the stack drain it, and then writes whatever the stack
/// produced. Both queues are bounded, so a stack that stops draining costs a
/// counted drop rather than unbounded memory.
struct FabricPort {
    inbound: VecDeque<Vec<u8>>,
    outbound: VecDeque<Vec<u8>>,
    mtu: usize,
}

struct Received(Vec<u8>);
struct Sending<'queue>(&'queue mut VecDeque<Vec<u8>>);

impl smoltcp::phy::RxToken for Received {
    fn consume<R, F: FnOnce(&[u8]) -> R>(self, f: F) -> R {
        f(&self.0)
    }
}

impl smoltcp::phy::TxToken for Sending<'_> {
    fn consume<R, F: FnOnce(&mut [u8]) -> R>(self, length: usize, f: F) -> R {
        let mut frame = vec![0_u8; length];
        let result = f(&mut frame);
        self.0.push_back(frame);
        result
    }
}

impl Device for FabricPort {
    type RxToken<'a>
        = Received
    where
        Self: 'a;
    type TxToken<'a>
        = Sending<'a>
    where
        Self: 'a;

    fn receive(&mut self, _timestamp: Instant) -> Option<(Self::RxToken<'_>, Self::TxToken<'_>)> {
        if self.outbound.len() >= QUEUE_DEPTH {
            return None;
        }
        let frame = self.inbound.pop_front()?;
        Some((Received(frame), Sending(&mut self.outbound)))
    }

    fn transmit(&mut self, _timestamp: Instant) -> Option<Self::TxToken<'_>> {
        if self.outbound.len() >= QUEUE_DEPTH {
            return None;
        }
        Some(Sending(&mut self.outbound))
    }

    fn capabilities(&self) -> DeviceCapabilities {
        let mut capabilities = DeviceCapabilities::default();
        capabilities.medium = Medium::Ethernet;
        capabilities.max_transmission_unit = self.mtu;
        // Every checksum is computed and verified here. There is no offload
        // anywhere on this path: the peer is a guest kernel writing real frames
        // into a datagram socket, and nothing between the two would fix up a
        // checksum this stack declined to compute.
        let mut checksum = ChecksumCapabilities::default();
        checksum.ipv4 = Checksum::Both;
        checksum.tcp = Checksum::Both;
        checksum.udp = Checksum::Both;
        checksum.icmpv4 = Checksum::Both;
        capabilities.checksum = checksum;
        capabilities
    }
}

// -- one terminated connection ---------------------------------------------------

/// One ingress connection: a TLS session with a client, and the connection the
/// edge opened to the origin on its behalf.
struct Session {
    tls: rustls::ServerConnection,
    origin: Option<OriginLink>,
    /// Bytes taken out of the TLS state machine that the client's socket could
    /// not accept yet, and plaintext the origin's socket could not accept yet.
    ///
    /// They are held rather than dropped because taking a byte out of a state
    /// machine is not the same as sending it: a socket that cannot send right
    /// now — because the handshake it belongs to has not finished, or because
    /// its window is full — will be able to later, and a record dropped in
    /// between is a session that stalls forever with no error anywhere.
    to_client: Vec<u8>,
    to_origin: Vec<u8>,
    /// Set once the session can no longer make progress. The client socket is
    /// closed on the next pass, and the slot returns to listening.
    finished: bool,
    /// Whether this session ever reached a declared origin, which is what
    /// separates a routed connection from a refused one in the receipt.
    routed: bool,
}

struct OriginLink {
    socket: SocketHandle,
    local_port: u16,
    address: Ipv4Addr,
    /// Whether the origin ever accepted. Until it has, the connection is
    /// half-open in both directions, which is not the same as finished:
    /// a socket in `SynSent` can neither send nor receive yet, and reading
    /// that as "the origin is done" would tear the session down between the
    /// edge's first packet and the origin's first reply.
    established: bool,
    /// Set once the origin can yield nothing further, so the edge stops waiting
    /// for bytes that are not coming and closes the client's half in turn.
    drained: bool,
}

// -- the edge --------------------------------------------------------------------

pub struct Edge {
    config: EdgeConfig,
    identity: Arc<EdgeIdentity>,
    port: FabricPort,
    interface: Interface,
    sockets: SocketSet<'static>,
    resolver: SocketHandle,
    /// One listening socket per session slot, and the session bound to it.
    listeners: Vec<SocketHandle>,
    sessions: Vec<Option<Session>>,
    firewall: Firewall,
    /// Every name this Environment publishes on this network, and the address
    /// it answers with: the edge's own, always.
    names: BTreeMap<String, Ipv4Addr>,
    routes: BTreeMap<String, EdgeRoute>,
    next_ephemeral: u16,
    receipt: EdgeShutdown,
    started: std::time::Instant,
}

impl Edge {
    /// Build the stack for one network's edge.
    pub fn new(config: EdgeConfig, identity: Arc<EdgeIdentity>) -> Result<Self, EdgeError> {
        let mut port = FabricPort {
            inbound: VecDeque::new(),
            outbound: VecDeque::new(),
            mtu: usize::try_from(config.mtu).unwrap_or(1500),
        };
        let started = std::time::Instant::now();
        let mut interface_config = Config::new(HardwareAddress::Ethernet(EthernetAddress(
            config.mac.bytes(),
        )));
        // Derived from the Environment and the network, so a restarted edge
        // produces the same sequence-number space seed as the one it replaced
        // rather than a random one, for the same reason the addresses are
        // derived: the peers are the same peers.
        interface_config.random_seed = seed(&config.environment_id, &config.network);
        let mut interface = Interface::new(interface_config, &mut port, Instant::from_micros(0));
        interface.update_ip_addrs(|addresses| {
            let _ = addresses.push(IpCidr::new(
                IpAddress::Ipv4(Ipv4Addr::from(config.address.octets())),
                config.prefix,
            ));
        });

        let mut sockets = SocketSet::new(Vec::new());
        let resolver = sockets.add(udp::Socket::new(
            udp::PacketBuffer::new(
                vec![udp::PacketMetadata::EMPTY; 16],
                vec![0_u8; dns::MAX_MESSAGE * 16],
            ),
            udp::PacketBuffer::new(
                vec![udp::PacketMetadata::EMPTY; 16],
                vec![0_u8; dns::MAX_MESSAGE * 16],
            ),
        ));

        let routes: BTreeMap<String, EdgeRoute> = config
            .routes
            .iter()
            .map(|route| (route.name.to_ascii_lowercase(), route.clone()))
            .collect();
        let names = routes
            .keys()
            .map(|name| (name.clone(), config.address))
            .collect();
        let ingress = !routes.is_empty();
        // Slots exist only where there is something to accept. An edge with no
        // declared `https` endpoint resolves names and accepts no connection,
        // and does not hold eight idle listeners saying otherwise.
        let slots = if ingress { MAX_SESSIONS } else { 0 };
        let listeners = (0..slots)
            .map(|_| {
                sockets.add(tcp::Socket::new(
                    tcp::SocketBuffer::new(vec![0_u8; SOCKET_BUFFER_BYTES]),
                    tcp::SocketBuffer::new(vec![0_u8; SOCKET_BUFFER_BYTES]),
                ))
            })
            .collect::<Vec<_>>();

        let mut edge = Self {
            firewall: Firewall::new(config.address, config.prefix, ingress),
            config,
            identity,
            port,
            interface,
            sockets,
            resolver,
            sessions: (0..slots).map(|_| None).collect(),
            listeners,
            names,
            routes,
            next_ephemeral: FIRST_EPHEMERAL_PORT,
            receipt: EdgeShutdown::default(),
            started,
        };
        edge.open_listeners()?;
        Ok(edge)
    }

    fn open_listeners(&mut self) -> Result<(), EdgeError> {
        let socket = self.sockets.get_mut::<udp::Socket>(self.resolver);
        socket
            .bind(IpListenEndpoint {
                addr: None,
                port: super::firewall::DNS_PORT,
            })
            .map_err(|error| EdgeError::Stack(format!("resolver bind: {error:?}")))?;
        for handle in self.listeners.clone() {
            let socket = self.sockets.get_mut::<tcp::Socket>(handle);
            socket
                .listen(INGRESS_PORT)
                .map_err(|error| EdgeError::Stack(format!("ingress listen: {error:?}")))?;
        }
        Ok(())
    }

    fn now(&self) -> Instant {
        Instant::from_micros(i64::try_from(self.started.elapsed().as_micros()).unwrap_or(i64::MAX))
    }

    /// Run until asked to stop, then report what the edge did.
    pub async fn run(
        mut self,
        socket: UnixDatagram,
        mut stop: oneshot::Receiver<()>,
    ) -> EdgeShutdown {
        let mut buffer = vec![0_u8; MAX_FRAME_BYTES];
        loop {
            // Everything the port has, judged one frame at a time, before the
            // stack is given any of it. A dropped frame never reaches a parser.
            while self.port.inbound.len() < QUEUE_DEPTH {
                match socket.try_recv(&mut buffer) {
                    Ok(read) => {
                        let frame = &buffer[..read];
                        if let Verdict::Accept(_) = self.firewall.judge(frame) {
                            self.port.inbound.push_back(frame.to_vec());
                        }
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => break,
                    Err(_) => return self.receipt,
                }
            }

            let timestamp = self.now();
            let polled = self
                .interface
                .poll(timestamp, &mut self.port, &mut self.sockets);
            self.serve_resolver();
            self.serve_ingress();
            // The application layer above may have queued bytes the stack has
            // not been given a chance to send yet, so it polls once more before
            // it decides there is nothing to do.
            self.interface
                .poll(self.now(), &mut self.port, &mut self.sockets);

            let mut wrote = false;
            while let Some(frame) = self.port.outbound.pop_front() {
                wrote = true;
                if socket.send(&frame).await.is_err() {
                    return self.receipt;
                }
            }

            if polled == PollResult::SocketStateChanged || wrote || !self.port.inbound.is_empty() {
                continue;
            }
            let delay = self
                .interface
                .poll_delay(self.now(), &self.sockets)
                .map(|delay| std::time::Duration::from_micros(delay.total_micros()))
                .unwrap_or(IDLE_WAIT)
                .min(IDLE_WAIT);
            tokio::select! {
                biased;
                _ = &mut stop => break,
                readable = tokio::time::timeout(delay, socket.readable()) => {
                    if matches!(readable, Ok(Err(_))) {
                        break;
                    }
                }
            }
        }
        self.receipt.firewall = self.firewall.counters();
        self.receipt
    }

    // -- the resolver ------------------------------------------------------------

    fn serve_resolver(&mut self) {
        loop {
            let socket = self.sockets.get_mut::<udp::Socket>(self.resolver);
            let Ok((payload, metadata)) = socket.recv() else {
                return;
            };
            let query = dns::parse_query(payload);
            let response = match &query {
                Ok(query) => {
                    let decision = dns::decide(query.question(), &self.names);
                    match decision {
                        dns::Decision::Answer(_) => self.receipt.resolved += 1,
                        _ => self.receipt.unresolved += 1,
                    }
                    Some(dns::respond(query, decision))
                }
                Err(error) => {
                    self.receipt.unresolved += 1;
                    dns::reject(payload, *error)
                }
            };
            let Some(response) = response else { continue };
            let socket = self.sockets.get_mut::<udp::Socket>(self.resolver);
            if socket.send_slice(&response, metadata.endpoint).is_err() {
                warn!(
                    network = %self.config.network,
                    "Environment edge could not answer a resolver query"
                );
            }
        }
    }

    // -- ingress -----------------------------------------------------------------

    fn serve_ingress(&mut self) {
        for slot in 0..self.listeners.len() {
            let handle = self.listeners[slot];
            let Some(mut session) = self.sessions[slot].take() else {
                let socket = self.sockets.get_mut::<tcp::Socket>(handle);
                if socket.is_active() {
                    match rustls::ServerConnection::new(self.identity.server_config()) {
                        Ok(tls) => {
                            self.sessions[slot] = Some(Session {
                                tls,
                                origin: None,
                                to_client: Vec::new(),
                                to_origin: Vec::new(),
                                finished: false,
                                routed: false,
                            });
                        }
                        Err(error) => {
                            warn!(network = %self.config.network, %error,
                                  "Environment edge could not start a TLS session");
                            socket.abort();
                        }
                    }
                } else if !socket.is_open() {
                    // A slot whose connection ended returns to the backlog.
                    let _ = socket.listen(INGRESS_PORT);
                }
                continue;
            };
            self.pump(handle, &mut session);
            if session.finished {
                self.retire(handle, session);
            } else {
                self.sessions[slot] = Some(session);
            }
        }
    }

    /// Move one session forward as far as it can go this pass.
    fn pump(&mut self, client: SocketHandle, session: &mut Session) {
        // 1. Ciphertext from the client into the TLS state machine.
        {
            let socket = self.sockets.get_mut::<tcp::Socket>(client);
            if !socket.is_open() {
                if session.origin.is_none() {
                    self.receipt.last_refusal =
                        Some(format!("client closed in state {:?}", socket.state()));
                }
                session.finished = true;
                return;
            }
            while socket.can_recv() {
                let consumed = socket.recv(|received| {
                    let available = received.len();
                    if available == 0 {
                        return (0, Ok(0));
                    }
                    // `read_tls` takes what it can hold and advances the
                    // cursor; whatever it left behind stays in the socket for
                    // the next pass rather than being dropped as consumed.
                    let mut cursor: &[u8] = received;
                    let outcome = session.tls.read_tls(&mut cursor);
                    let taken = available - cursor.len();
                    (taken, outcome)
                });
                match consumed {
                    Ok(Ok(0)) | Err(_) => break,
                    Ok(Ok(_)) => {}
                    Ok(Err(error)) => {
                        self.receipt.last_refusal = Some(format!("read: {error}"));
                        session.finished = true;
                        return;
                    }
                }
                if let Err(error) = session.tls.process_new_packets() {
                    self.receipt.last_refusal = Some(format!("tls: {error}"));
                    session.finished = true;
                    return;
                }
            }
        }

        // 2. Once the handshake names a service, open the edge's own connection
        //    to that service's origin. This is the translation: the origin's
        //    peer is the edge, on an ephemeral port the edge chose, and the
        //    client's address appears nowhere on that connection.
        if session.origin.is_none() && !session.tls.is_handshaking() {
            let requested = session
                .tls
                .server_name()
                .map(str::to_ascii_lowercase)
                .unwrap_or_default();
            match self.routes.get(&requested).cloned() {
                Some(route) => match self.open_origin(&route) {
                    Ok(link) => {
                        info!(
                            network = %self.config.network,
                            name = %route.name,
                            origin = %route.origin,
                            port = route.port,
                            translated_port = link.local_port,
                            "Environment edge opened an ingress path to a declared origin"
                        );
                        session.routed = true;
                        self.receipt.sessions_routed += 1;
                        session.origin = Some(link);
                    }
                    Err(error) => {
                        warn!(network = %self.config.network, %error,
                              "Environment edge could not reach a declared origin");
                        self.receipt.last_refusal = Some(format!("origin: {error}"));
                        session.finished = true;
                        return;
                    }
                },
                None => {
                    // Unreachable in practice: a name with no route has no
                    // certificate either, so the handshake ended before this.
                    // It is still refused here rather than assumed away.
                    self.receipt.last_refusal = Some(format!("no route for `{requested}`"));
                    session.finished = true;
                    return;
                }
            }
        }

        // 3. Plaintext both ways.
        if let Some(link) = &mut session.origin {
            // `read_to_end` reports "nothing more right now" the same way it
            // reports a broken session, so an empty read is not on its own a
            // reason to end one; whatever it did yield is kept.
            let _ = session.tls.reader().read_to_end(&mut session.to_origin);
            if !session.to_origin.is_empty() {
                let socket = self.sockets.get_mut::<tcp::Socket>(link.socket);
                if socket.can_send()
                    && let Ok(sent) = socket.send_slice(&session.to_origin)
                {
                    self.receipt.bytes_to_origin += sent as u64;
                    session.to_origin.drain(..sent);
                }
            }

            let mut from_origin = Vec::new();
            {
                let socket = self.sockets.get_mut::<tcp::Socket>(link.socket);
                while socket.can_recv() {
                    let taken = socket.recv(|received| (received.len(), received.to_vec()));
                    match taken {
                        Ok(bytes) if !bytes.is_empty() => from_origin.extend_from_slice(&bytes),
                        _ => break,
                    }
                }
                link.established |= socket.state() == tcp::State::Established;
                // Two ways an origin link ends, and neither of them is "the
                // connection has not started yet": the origin accepted and has
                // since closed its half, or it never accepted at all. A socket
                // in `SynSent` can neither send nor receive, so a rule that
                // only asked whether it could would end the session between the
                // edge's first packet and the origin's first reply.
                let ended = if link.established {
                    !socket.may_recv()
                } else {
                    socket.state() == tcp::State::Closed
                };
                link.drained |= ended;
            }
            if !from_origin.is_empty() {
                self.receipt.bytes_to_client += from_origin.len() as u64;
                let _ = session.tls.writer().write_all(&from_origin);
            }
        }

        // 4. Ciphertext back to the client, held until the socket takes it.
        while session.tls.wants_write() {
            match session.tls.write_tls(&mut session.to_client) {
                Ok(0) | Err(_) => break,
                Ok(_) => {}
            }
        }
        if !session.to_client.is_empty() {
            let socket = self.sockets.get_mut::<tcp::Socket>(client);
            if socket.can_send()
                && let Ok(sent) = socket.send_slice(&session.to_client)
            {
                session.to_client.drain(..sent);
            }
        }
        // The session is over only once the origin has finished and everything
        // it produced has actually been handed to the client's socket.
        if session.origin.as_ref().is_some_and(|link| link.drained)
            && session.to_client.is_empty()
            && !session.tls.wants_write()
        {
            session.finished = true;
        }
    }

    /// Open the edge's own connection towards one declared origin.
    fn open_origin(&mut self, route: &EdgeRoute) -> Result<OriginLink, EdgeError> {
        let local_port = self.next_translated_port();
        let socket = tcp::Socket::new(
            tcp::SocketBuffer::new(vec![0_u8; SOCKET_BUFFER_BYTES]),
            tcp::SocketBuffer::new(vec![0_u8; SOCKET_BUFFER_BYTES]),
        );
        let handle = self.sockets.add(socket);
        let socket = self.sockets.get_mut::<tcp::Socket>(handle);
        let outcome = socket.connect(
            self.interface.context(),
            (IpAddress::Ipv4(route.origin), route.port),
            local_port,
        );
        if let Err(error) = outcome {
            self.sockets.remove(handle);
            return Err(EdgeError::Stack(format!(
                "origin {}:{}: {error:?}",
                route.origin, route.port
            )));
        }
        // The reply path exists because the edge opened it, and only for the
        // origin it was opened towards.
        self.firewall.open_translation(local_port, route.origin);
        Ok(OriginLink {
            socket: handle,
            local_port,
            address: route.origin,
            established: false,
            drained: false,
        })
    }

    fn next_translated_port(&mut self) -> u16 {
        let port = self.next_ephemeral;
        // Wrapping rather than saturating: the range ends at the largest port
        // there is, so the successor of the last one is the first one again.
        self.next_ephemeral = port.checked_add(1).unwrap_or(FIRST_EPHEMERAL_PORT);
        port
    }

    /// End one session and give its resources back.
    fn retire(&mut self, client: SocketHandle, session: Session) {
        if !session.routed {
            self.receipt.sessions_refused += 1;
        }
        if let Some(link) = session.origin {
            let socket = self.sockets.get_mut::<tcp::Socket>(link.socket);
            socket.close();
            socket.abort();
            self.sockets.remove(link.socket);
            // Closed the moment the flow is gone, so the translated port is not
            // a hole that outlives the connection that justified it.
            self.firewall.close_translation(link.local_port);
            let _ = link.address;
        }
        let socket = self.sockets.get_mut::<tcp::Socket>(client);
        socket.close();
    }
}

fn seed(environment_id: &str, network: &str) -> u64 {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(b"vz.environment.network.edge.seed.v1\n");
    for field in [environment_id, network] {
        hasher.update(u64::try_from(field.len()).unwrap_or(u64::MAX).to_be_bytes());
        hasher.update(field.as_bytes());
    }
    let digest = hasher.finalize();
    let mut bytes = [0_u8; 8];
    bytes.copy_from_slice(&digest[..8]);
    u64::from_be_bytes(bytes)
}

/// Turn the guest end of a switch port into the socket the edge reads and
/// writes.
pub(crate) fn port_socket(socket: OwnedFd) -> Result<UnixDatagram, EdgeError> {
    let socket = std::os::unix::net::UnixDatagram::from(socket);
    socket.set_nonblocking(true)?;
    Ok(UnixDatagram::from_std(socket)?)
}
