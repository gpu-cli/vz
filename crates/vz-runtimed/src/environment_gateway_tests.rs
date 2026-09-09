//! The edge, exercised over a real switch by real peers.
//!
//! Nothing here is a stand-in for the thing under test. The switch is the
//! product's switch, started from the product's plan; the frames are real
//! Ethernet frames over the real datagram-socket ports it hands out; the client
//! is a real TLS client that verifies the Environment's own authority; and the
//! origin is a real TCP server that reports the peer address it actually saw.
//! What that last fact settles is the criterion's hardest clause: the origin
//! sees the *edge*, so the path provably went through the edge and not straight
//! to the origin, and not over anything on the host.
//!
//! The two peers are userspace stacks in this process for the same reason the
//! edge is one: a switch port is a socket, and anything holding one end of it is
//! a station on that fabric whether it is a VM or not. Using VMs here would test
//! the boot path, which `check_private_topology_paths` already does on hardware,
//! and would test nothing about the edge that these do not.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::collections::{BTreeMap, VecDeque};
use std::io::{Read, Write};
use std::net::Ipv4Addr;
use std::os::fd::OwnedFd;
use std::sync::Arc;

use smoltcp::iface::{Config, Interface, SocketHandle, SocketSet};
use smoltcp::phy::{Checksum, ChecksumCapabilities, Device, DeviceCapabilities, Medium};
use smoltcp::socket::{tcp, udp};
use smoltcp::time::Instant;
use smoltcp::wire::{EthernetAddress, HardwareAddress, IpAddress, IpCidr, IpListenEndpoint};
use vz_runtime_contract::{
    Architecture, EndpointInstance, EndpointProtocol, EnvironmentInstance, EnvironmentState,
    MachineId, MachineInstance, MachineProfile, MachineState, NetworkAttachmentInstance,
    NetworkInstance, NetworkKind, OperatingSystem, ProjectId, TOPOLOGY_SCHEMA_VERSION, TargetSpec,
};

use super::*;
use crate::environment_switch::plan::{FabricPlan, plan_environment_fabric};
use crate::environment_switch::runtime::NetworkSwitch;
use crate::environment_switch::{MacAddress, PortId};

const PROJECT: &str = "prj_0123456789abcdef0123456789abcdef";
const ENVIRONMENT: &str = "env_0123456789abcdef0123456789abcdef";
const PUBLISHED_NAME: &str = "api.shop.test";
const ORIGIN_PORT: u16 = 8443;
const CIDR: &str = "10.42.0.0/24";
/// How long any of these exchanges may take before the test gives up. Generous,
/// because it bounds a failure rather than pacing a success: every wait below
/// ends on the condition it is waiting for.
const DEADLINE: std::time::Duration = std::time::Duration::from_secs(20);

// -- an Environment that declares a public-like network ---------------------------

fn environment_id() -> vz_runtime_contract::EnvironmentId {
    vz_runtime_contract::EnvironmentId::new(ENVIRONMENT.to_string()).unwrap()
}

fn machine_id(suffix: u8) -> MachineId {
    MachineId::new(format!("mch_{suffix:032x}")).unwrap()
}

fn network_id() -> vz_runtime_contract::NetworkId {
    vz_runtime_contract::NetworkId::new(format!("net_{:032x}", 1)).unwrap()
}

fn machine(suffix: u8) -> MachineInstance {
    MachineInstance {
        schema_version: TOPOLOGY_SCHEMA_VERSION,
        machine_id: machine_id(suffix),
        environment_id: environment_id(),
        name: format!("machine-{suffix}"),
        profile: MachineProfile::Developer,
        target: TargetSpec {
            os: OperatingSystem::Linux,
            arch: Architecture::Aarch64,
            image: "vz/linux".to_string(),
            version: None,
            channel: None,
            digest: None,
        },
        resources: Default::default(),
        requested_capabilities: Default::default(),
        negotiated_capabilities: Default::default(),
        backend: None,
        incarnation: None,
        runtime_identity: None,
        docker_context: None,
        state: MachineState::Stopped,
        legacy_sandbox_id: None,
    }
}

/// One public-like network, two Machines, one `https` endpoint on the first.
fn published_environment(protocol: EndpointProtocol) -> EnvironmentInstance {
    EnvironmentInstance {
        schema_version: TOPOLOGY_SCHEMA_VERSION,
        environment_id: environment_id(),
        project_id: ProjectId::new(PROJECT.to_string()).unwrap(),
        name: "shop".to_string(),
        definition_digest: format!("sha256:{}", "a".repeat(64)),
        state: EnvironmentState::Stopped,
        lifecycle_generation: 0,
        active_operation_id: None,
        bindings: Vec::new(),
        machines: vec![machine(1), machine(2)],
        networks: vec![NetworkInstance {
            schema_version: TOPOLOGY_SCHEMA_VERSION,
            network_id: network_id(),
            environment_id: environment_id(),
            name: "edge".to_string(),
            kind: NetworkKind::SimulatedPublic,
            cidr: Some(CIDR.to_string()),
        }],
        endpoints: vec![EndpointInstance {
            schema_version: TOPOLOGY_SCHEMA_VERSION,
            endpoint_id: vz_runtime_contract::EndpointId::new(format!("epn_{:032x}", 1)).unwrap(),
            environment_id: environment_id(),
            machine_id: machine_id(1),
            network_id: network_id(),
            name: "api".to_string(),
            protocol,
            port: ORIGIN_PORT,
            hostname: Some(PUBLISHED_NAME.to_string()),
        }],
        network_attachments: vec![
            NetworkAttachmentInstance {
                schema_version: TOPOLOGY_SCHEMA_VERSION,
                attachment_id: vz_runtime_contract::NetworkAttachmentId::new(format!(
                    "att_{:032x}",
                    1
                ))
                .unwrap(),
                environment_id: environment_id(),
                machine_id: machine_id(1),
                network_id: network_id(),
            },
            NetworkAttachmentInstance {
                schema_version: TOPOLOGY_SCHEMA_VERSION,
                attachment_id: vz_runtime_contract::NetworkAttachmentId::new(format!(
                    "att_{:032x}",
                    2
                ))
                .unwrap(),
                environment_id: environment_id(),
                machine_id: machine_id(2),
                network_id: network_id(),
            },
        ],
        host_exports: Vec::new(),
        host_imports: Vec::new(),
        egress: Vec::new(),
        // Criterion 17 added volumes to the aggregate; the edge carries no
        // storage, so this fixture declares none.
        volumes: Vec::new(),
        ownership: Vec::new(),
        legacy_migration: None,
        created_at: 0,
        updated_at: 0,
    }
}

// -- a station on the fabric ------------------------------------------------------

struct PeerPort {
    inbound: VecDeque<Vec<u8>>,
    outbound: VecDeque<Vec<u8>>,
}

struct Rx(Vec<u8>);
struct Tx<'queue>(&'queue mut VecDeque<Vec<u8>>);

impl smoltcp::phy::RxToken for Rx {
    fn consume<R, F: FnOnce(&[u8]) -> R>(self, f: F) -> R {
        f(&self.0)
    }
}

impl smoltcp::phy::TxToken for Tx<'_> {
    fn consume<R, F: FnOnce(&mut [u8]) -> R>(self, length: usize, f: F) -> R {
        let mut frame = vec![0_u8; length];
        let result = f(&mut frame);
        self.0.push_back(frame);
        result
    }
}

impl Device for PeerPort {
    type RxToken<'a>
        = Rx
    where
        Self: 'a;
    type TxToken<'a>
        = Tx<'a>
    where
        Self: 'a;

    fn receive(&mut self, _now: Instant) -> Option<(Self::RxToken<'_>, Self::TxToken<'_>)> {
        let frame = self.inbound.pop_front()?;
        Some((Rx(frame), Tx(&mut self.outbound)))
    }

    fn transmit(&mut self, _now: Instant) -> Option<Self::TxToken<'_>> {
        Some(Tx(&mut self.outbound))
    }

    fn capabilities(&self) -> DeviceCapabilities {
        let mut capabilities = DeviceCapabilities::default();
        capabilities.medium = Medium::Ethernet;
        capabilities.max_transmission_unit = 1500;
        let mut checksum = ChecksumCapabilities::default();
        checksum.ipv4 = Checksum::Both;
        checksum.tcp = Checksum::Both;
        checksum.udp = Checksum::Both;
        capabilities.checksum = checksum;
        capabilities
    }
}

/// One Machine's end of a switch port, with a stack on it.
struct Peer {
    port: PeerPort,
    interface: Interface,
    sockets: SocketSet<'static>,
    socket: std::os::unix::net::UnixDatagram,
    started: std::time::Instant,
}

impl Peer {
    fn new(mac: MacAddress, address: Ipv4Addr, prefix: u8, guest: OwnedFd) -> Self {
        let socket = std::os::unix::net::UnixDatagram::from(guest);
        socket.set_nonblocking(true).unwrap();
        let mut port = PeerPort {
            inbound: VecDeque::new(),
            outbound: VecDeque::new(),
        };
        let config = Config::new(HardwareAddress::Ethernet(EthernetAddress(mac.bytes())));
        let mut interface = Interface::new(config, &mut port, Instant::from_micros(0));
        interface.update_ip_addrs(|addresses| {
            let _ = addresses.push(IpCidr::new(IpAddress::Ipv4(address), prefix));
        });
        Self {
            port,
            interface,
            sockets: SocketSet::new(Vec::new()),
            socket,
            started: std::time::Instant::now(),
        }
    }

    fn now(&self) -> Instant {
        Instant::from_micros(i64::try_from(self.started.elapsed().as_micros()).unwrap_or(i64::MAX))
    }

    /// Read whatever the fabric delivered, let the stack act on it, and write
    /// back whatever the stack produced.
    fn poll(&mut self) {
        let mut buffer = vec![0_u8; 65535];
        while let Ok(read) = self.socket.recv(&mut buffer) {
            self.port.inbound.push_back(buffer[..read].to_vec());
        }
        let now = self.now();
        self.interface.poll(now, &mut self.port, &mut self.sockets);
        while let Some(frame) = self.port.outbound.pop_front() {
            let _ = self.socket.send(&frame);
        }
    }
}

/// Drive every peer until `ready` holds, or fail with what it was waiting for.
///
/// This is a driver, not a delay: the stacks are synchronous and only advance
/// when polled, and the yield between passes is what lets the edge's own task
/// run. Nothing here waits out a fixed interval and then assumes success.
async fn settle(peers: &mut [&mut Peer], mut ready: impl FnMut(&mut [&mut Peer]) -> bool) -> bool {
    let deadline = std::time::Instant::now() + DEADLINE;
    loop {
        for peer in peers.iter_mut() {
            peer.poll();
        }
        if ready(peers) {
            return true;
        }
        if std::time::Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(std::time::Duration::from_millis(1)).await;
    }
}

// -- the fabric under test --------------------------------------------------------

struct Fabric {
    _switch: NetworkSwitch,
    gateway: EnvironmentGateway,
    plan: FabricPlan,
    guests: BTreeMap<PortId, OwnedFd>,
    _anchors: tempfile::TempDir,
}

fn start_fabric(environment: &EnvironmentInstance) -> Fabric {
    let plan = plan_environment_fabric(environment).expect("plan");
    let network = &plan.networks[0];
    let (switch, guests) = NetworkSwitch::start(&network.name, network.members()).expect("switch");
    let mut guests: BTreeMap<PortId, OwnedFd> = guests
        .into_iter()
        .map(|guest| (guest.port, guest.socket))
        .collect();
    let anchors = tempfile::tempdir().expect("anchor root");
    let edge_port = network
        .gateway
        .as_ref()
        .expect("a public network has an edge");
    let gateway = EnvironmentGateway::start(
        ENVIRONMENT,
        network,
        guests.remove(&edge_port.port).expect("edge guest end"),
        anchors.path(),
    )
    .expect("edge")
    .expect("a public network starts an edge");
    Fabric {
        _switch: switch,
        gateway,
        plan,
        guests,
        _anchors: anchors,
    }
}

impl Fabric {
    fn network(&self) -> &crate::environment_switch::plan::NetworkPlan {
        &self.plan.networks[0]
    }

    fn peer(&mut self, machine: u8) -> Peer {
        let network = &self.plan.networks[0];
        let port = network
            .ports
            .iter()
            .find(|port| port.machine_id == machine_id(machine))
            .expect("machine port");
        Peer::new(
            port.mac,
            port.address,
            network.cidr.prefix(),
            self.guests.remove(&port.port).expect("guest end"),
        )
    }

    fn address_of(&self, machine: u8) -> Ipv4Addr {
        self.plan.networks[0]
            .ports
            .iter()
            .find(|port| port.machine_id == machine_id(machine))
            .expect("machine port")
            .address
    }

    fn edge_address(&self) -> Ipv4Addr {
        self.network().gateway.as_ref().expect("edge").address
    }
}

// -- what the peers do ------------------------------------------------------------

fn dns_query(name: &str) -> Vec<u8> {
    let mut message = vec![0x13, 0x37, 0x01, 0x00, 0, 1, 0, 0, 0, 0, 0, 0];
    for label in name.split('.') {
        message.push(u8::try_from(label.len()).unwrap());
        message.extend_from_slice(label.as_bytes());
    }
    message.push(0);
    message.extend_from_slice(&1_u16.to_be_bytes());
    message.extend_from_slice(&1_u16.to_be_bytes());
    message
}

/// The A record in a response, if it has one.
fn answered_address(response: &[u8]) -> Option<Ipv4Addr> {
    if response.len() < 12 || u16::from_be_bytes([response[6], response[7]]) != 1 {
        return None;
    }
    let address = response.get(response.len() - 4..)?;
    Some(Ipv4Addr::new(
        address[0], address[1], address[2], address[3],
    ))
}

fn rcode(response: &[u8]) -> u8 {
    response[3] & 0x0f
}

/// The trust anchor the Environment published, as a rustls root store.
fn roots(anchor: &std::path::Path) -> rustls::RootCertStore {
    let pem = std::fs::read(anchor).expect("published anchor");
    let mut store = rustls::RootCertStore::empty();
    for certificate in rustls_pemfile::certs(&mut pem.as_slice()) {
        store.add(certificate.expect("anchor certificate")).unwrap();
    }
    assert_eq!(store.len(), 1, "one authority, not a bundle");
    store
}

fn client_config(anchor: &std::path::Path) -> Arc<rustls::ClientConfig> {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    Arc::new(
        rustls::ClientConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_root_certificates(roots(anchor))
            .with_no_client_auth(),
    )
}

/// An origin: accept one connection, report the peer it saw, answer, close.
struct Origin {
    listener: SocketHandle,
    observed_peer: Option<Ipv4Addr>,
    answered: bool,
    body: String,
}

impl Origin {
    fn listen(peer: &mut Peer, body: &str) -> Self {
        let socket = tcp::Socket::new(
            tcp::SocketBuffer::new(vec![0_u8; 8192]),
            tcp::SocketBuffer::new(vec![0_u8; 8192]),
        );
        let listener = peer.sockets.add(socket);
        peer.sockets
            .get_mut::<tcp::Socket>(listener)
            .listen(IpListenEndpoint {
                addr: None,
                port: ORIGIN_PORT,
            })
            .unwrap();
        Self {
            listener,
            observed_peer: None,
            answered: false,
            body: body.to_string(),
        }
    }

    fn serve(&mut self, peer: &mut Peer) {
        let socket = peer.sockets.get_mut::<tcp::Socket>(self.listener);
        if let Some(endpoint) = socket.remote_endpoint()
            && let IpAddress::Ipv4(address) = endpoint.addr
        {
            self.observed_peer = Some(address);
        }
        if !socket.can_recv() || self.answered {
            return;
        }
        let request = socket
            .recv(|received| (received.len(), received.to_vec()))
            .unwrap_or_default();
        if !request.windows(4).any(|window| window == b"\r\n\r\n") {
            return;
        }
        // The origin reports the peer it actually saw. That is the whole
        // evidentiary point of this server: the body is written by the origin
        // out of the origin's own socket state, not by the test.
        let observed = self
            .observed_peer
            .map(|address| address.to_string())
            .unwrap_or_else(|| "unknown".to_string());
        let body = format!("{}\npeer={observed}\n", self.body);
        let response = format!(
            "HTTP/1.0 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );
        socket.send_slice(response.as_bytes()).unwrap();
        socket.close();
        self.answered = true;
    }
}

/// A client: resolve a name at the edge, then fetch it over TLS.
struct Client {
    resolver: SocketHandle,
    stream: Option<SocketHandle>,
    tls: Option<rustls::ClientConnection>,
    response: Vec<u8>,
    pending: Vec<u8>,
    established: bool,
    finished: bool,
    failure: Option<String>,
}

impl Client {
    fn new(peer: &mut Peer) -> Self {
        let socket = udp::Socket::new(
            udp::PacketBuffer::new(vec![udp::PacketMetadata::EMPTY; 4], vec![0_u8; 4096]),
            udp::PacketBuffer::new(vec![udp::PacketMetadata::EMPTY; 4], vec![0_u8; 4096]),
        );
        let resolver = peer.sockets.add(socket);
        peer.sockets
            .get_mut::<udp::Socket>(resolver)
            .bind(IpListenEndpoint {
                addr: None,
                port: 40000,
            })
            .unwrap();
        Self {
            resolver,
            stream: None,
            tls: None,
            response: Vec::new(),
            pending: Vec::new(),
            established: false,
            finished: false,
            failure: None,
        }
    }

    fn ask(&mut self, peer: &mut Peer, edge: Ipv4Addr, name: &str) {
        peer.sockets
            .get_mut::<udp::Socket>(self.resolver)
            .send_slice(
                &dns_query(name),
                (IpAddress::Ipv4(edge), super::firewall::DNS_PORT),
            )
            .unwrap();
    }

    fn heard(&mut self, peer: &mut Peer) -> Option<Vec<u8>> {
        peer.sockets
            .get_mut::<udp::Socket>(self.resolver)
            .recv()
            .ok()
            .map(|(payload, _)| payload.to_vec())
    }

    fn connect(
        &mut self,
        peer: &mut Peer,
        address: Ipv4Addr,
        name: &str,
        config: Arc<rustls::ClientConfig>,
    ) {
        let socket = tcp::Socket::new(
            tcp::SocketBuffer::new(vec![0_u8; 16384]),
            tcp::SocketBuffer::new(vec![0_u8; 16384]),
        );
        let handle = peer.sockets.add(socket);
        peer.sockets
            .get_mut::<tcp::Socket>(handle)
            .connect(
                peer.interface.context(),
                (IpAddress::Ipv4(address), super::firewall::INGRESS_PORT),
                40001,
            )
            .unwrap();
        let server = rustls_pki_types::ServerName::try_from(name.to_string()).unwrap();
        let mut tls = rustls::ClientConnection::new(config, server).unwrap();
        tls.writer()
            .write_all(format!("GET / HTTP/1.0\r\nHost: {name}\r\n\r\n").as_bytes())
            .unwrap();
        self.stream = Some(handle);
        self.tls = Some(tls);
    }

    fn pump(&mut self, peer: &mut Peer) {
        let (Some(handle), Some(tls)) = (self.stream, self.tls.as_mut()) else {
            return;
        };
        {
            let socket = peer.sockets.get_mut::<tcp::Socket>(handle);
            while socket.can_recv() {
                let taken = socket.recv(|received| {
                    let available = received.len();
                    if available == 0 {
                        return (0, Ok(0));
                    }
                    let mut cursor: &[u8] = received;
                    let outcome = tls.read_tls(&mut cursor);
                    (available - cursor.len(), outcome)
                });
                match taken {
                    Ok(Ok(0)) | Err(_) => break,
                    Ok(Ok(_)) => {}
                    Ok(Err(error)) => {
                        self.failure = Some(error.to_string());
                        self.finished = true;
                        return;
                    }
                }
                if let Err(error) = tls.process_new_packets() {
                    self.failure = Some(error.to_string());
                    self.finished = true;
                    return;
                }
            }
            self.established |= socket.state() == tcp::State::Established;
            if self.established && !socket.may_recv() {
                self.finished = true;
            }
            if !socket.is_open() {
                self.finished = true;
            }
        }
        let _ = tls.reader().read_to_end(&mut self.response);
        while tls.wants_write() {
            match tls.write_tls(&mut self.pending) {
                Ok(0) | Err(_) => break,
                Ok(_) => {}
            }
        }
        if !self.pending.is_empty() {
            let socket = peer.sockets.get_mut::<tcp::Socket>(handle);
            if socket.can_send()
                && let Ok(sent) = socket.send_slice(&self.pending)
            {
                self.pending.drain(..sent);
            }
        }
    }

    fn body(&self) -> String {
        let text = String::from_utf8_lossy(&self.response).to_string();
        match text.split_once("\r\n\r\n") {
            Some((_, body)) => body.to_string(),
            None => text,
        }
    }
}

// -- the criterion ----------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_client_reaches_a_declared_origin_through_the_edge() {
    let environment = published_environment(EndpointProtocol::Https);
    let mut fabric = start_fabric(&environment);
    let edge_address = fabric.edge_address();
    let origin_address = fabric.address_of(1);
    let client_address = fabric.address_of(2);
    let anchor = fabric.gateway.anchor().to_path_buf();
    assert_ne!(edge_address, origin_address);
    assert_ne!(edge_address, client_address);

    let mut server_peer = fabric.peer(1);
    let mut client_peer = fabric.peer(2);
    let mut origin = Origin::listen(&mut server_peer, "SERVED-BY-THE-DECLARED-ORIGIN");
    let mut client = Client::new(&mut client_peer);

    // 1. The name is resolved by the Environment's own resolver, at the edge.
    //    Nothing put this name in a file: it exists only because the edge
    //    answers for it.
    client.ask(&mut client_peer, edge_address, PUBLISHED_NAME);
    let mut answer = None;
    let answered = settle(&mut [&mut server_peer, &mut client_peer], |peers| {
        answer = client.heard(peers[1]);
        answer.is_some()
    })
    .await;
    assert!(answered, "the edge answered for the declared name");
    let answer = answer.expect("an answer");
    assert_eq!(rcode(&answer), 0, "the declared name resolves");
    let resolved = answered_address(&answer).expect("an A record");
    // The clause that makes this ingress rather than a private shortcut: the
    // name is the edge's address, and the client never learns the origin's.
    assert_eq!(
        resolved, edge_address,
        "a declared public name resolves to the edge, not to the Machine behind it"
    );
    assert_ne!(resolved, origin_address);

    // 2. TLS to that address, verified against the Environment's own authority.
    //    A client that did not trust this exact authority could not complete
    //    this handshake, and no public authority could have issued for `.test`.
    let config = client_config(&anchor);
    client.connect(&mut client_peer, resolved, PUBLISHED_NAME, config);
    let fetched = settle(&mut [&mut server_peer, &mut client_peer], |peers| {
        origin.serve(peers[0]);
        client.pump(peers[1]);
        client.finished && !client.response.is_empty()
    })
    .await;
    assert_eq!(client.failure, None, "the TLS session completed");
    if !fetched {
        let receipt = fabric.gateway.shutdown().await.expect("edge shutdown");
        panic!(
            "the fetch did not cross the edge: client(established={} finished={} bytes={}) \
             origin(peer={:?} answered={}) edge({receipt:?})",
            client.established,
            client.finished,
            client.response.len(),
            origin.observed_peer,
            origin.answered,
        );
    }

    // 3. What came back is what the origin served.
    let body = client.body();
    assert!(
        body.contains("SERVED-BY-THE-DECLARED-ORIGIN"),
        "the client read the origin's own body: {body:?}"
    );

    // 4. And the origin's peer was the edge. This is the address translation
    //    and it is also the proof of the path: had the client reached the
    //    origin directly, or had anything on the host carried this, the origin
    //    would have reported a different address.
    assert_eq!(
        origin.observed_peer,
        Some(edge_address),
        "the origin's peer is the edge, not the client"
    );
    assert!(
        body.contains(&format!("peer={edge_address}")),
        "the origin reported the edge as its peer: {body:?}"
    );
    assert!(
        !body.contains(&client_address.to_string()),
        "the client's own address never appears on the origin's connection: {body:?}"
    );

    let receipt = fabric.gateway.shutdown().await.expect("edge shutdown");
    assert_eq!(receipt.sessions_routed, 1);
    assert_eq!(receipt.resolved, 1);
    assert!(
        receipt.bytes_to_origin > 0 && receipt.bytes_to_client > 0,
        "{receipt:?}"
    );
    // Every class the criterion names was exercised, and the filter counted it.
    for class in [
        firewall::Admitted::Arp,
        firewall::Admitted::Resolver,
        firewall::Admitted::Ingress,
        firewall::Admitted::Translated,
    ] {
        assert!(
            receipt.firewall.accepted.get(&class).copied().unwrap_or(0) > 0,
            "the edge admitted {class:?}: {:?}",
            receipt.firewall.accepted
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_undeclared_name_does_not_resolve_and_is_not_served() {
    let environment = published_environment(EndpointProtocol::Https);
    let mut fabric = start_fabric(&environment);
    let edge_address = fabric.edge_address();
    let anchor = fabric.gateway.anchor().to_path_buf();
    let mut client_peer = fabric.peer(2);
    let mut client = Client::new(&mut client_peer);

    client.ask(&mut client_peer, edge_address, "admin.shop.test");
    let mut answer = None;
    assert!(
        settle(&mut [&mut client_peer], |peers| {
            answer = client.heard(peers[0]);
            answer.is_some()
        })
        .await,
        "the edge answered the undeclared name"
    );
    let answer = answer.expect("an answer");
    // NXDOMAIN, not a forwarded lookup: the Environment's resolver is the whole
    // name space its Machines can see.
    assert_eq!(rcode(&answer), 3, "an undeclared name does not resolve");
    assert_eq!(answered_address(&answer), None);

    // And the edge has no certificate for it, so the handshake ends before any
    // routing decision is reached.
    client.connect(
        &mut client_peer,
        edge_address,
        "admin.shop.test",
        client_config(&anchor),
    );
    settle(&mut [&mut client_peer], |peers| {
        client.pump(peers[0]);
        client.finished || client.failure.is_some()
    })
    .await;
    assert!(
        client.body().is_empty(),
        "nothing was served: {:?}",
        client.body()
    );

    let receipt = fabric.gateway.shutdown().await.expect("edge shutdown");
    assert_eq!(receipt.sessions_routed, 0);
    assert_eq!(receipt.resolved, 0);
    assert_eq!(receipt.unresolved, 1);
}

#[test]
fn an_endpoint_the_edge_cannot_terminate_is_refused_rather_than_published() {
    // The edge terminates `https`. A `tcp` endpoint on a public-like network
    // would have no listener to be published behind, so the plan refuses it
    // instead of resolving its name to an address nothing accepts on.
    let environment = published_environment(EndpointProtocol::Tcp);
    let error = plan_environment_fabric(&environment).expect_err("refused");
    assert!(
        matches!(
            error,
            crate::environment_switch::plan::FabricPlanError::UnservedPublicEndpoint { .. }
        ),
        "{error}"
    );
}

#[test]
fn a_public_network_publishes_no_static_host_entry_for_its_names() {
    // If the name were also in the Machine's static table, the resolver would
    // never be asked and the split-DNS clause would go unexercised while
    // looking exactly like success.
    let environment = published_environment(EndpointProtocol::Https);
    let plan = plan_environment_fabric(&environment).expect("plan");
    assert!(plan.networks[0].hosts().is_empty());
    assert_eq!(plan.networks[0].endpoints.len(), 1);
    let published = &plan.networks[0].endpoints[0];
    assert_eq!(published.name, PUBLISHED_NAME);
    assert_eq!(
        published.address,
        plan.networks[0].gateway.as_ref().unwrap().address
    );
    assert_ne!(published.address, published.origin);
}

#[test]
fn the_edge_takes_the_reserved_offset_and_never_a_machine_address() {
    let environment = published_environment(EndpointProtocol::Https);
    let plan = plan_environment_fabric(&environment).expect("plan");
    let network = &plan.networks[0];
    let edge = network.gateway.as_ref().expect("edge");
    assert_eq!(edge.address, Ipv4Addr::new(10, 42, 0, 1));
    for port in &network.ports {
        assert_ne!(port.address, edge.address);
        assert_ne!(port.mac, edge.mac);
    }
    // The edge is a member of the switch like any other station.
    assert_eq!(network.members().len(), network.ports.len() + 1);
    assert!(edge.mac.is_locally_administered() && !edge.mac.is_group());
}

// -- the filter -------------------------------------------------------------------

/// One Ethernet frame carrying one IPv4 packet, built to order.
fn frame(
    source: Ipv4Addr,
    destination: Ipv4Addr,
    protocol: u8,
    destination_port: u16,
    payload: &[u8],
) -> Vec<u8> {
    let mut packet = vec![0x45, 0, 0, 0, 0, 0, 0, 0, 64, protocol, 0, 0];
    packet.extend_from_slice(&source.octets());
    packet.extend_from_slice(&destination.octets());
    packet.extend_from_slice(&1234_u16.to_be_bytes());
    packet.extend_from_slice(&destination_port.to_be_bytes());
    packet.extend_from_slice(payload);
    let total = u16::try_from(packet.len()).unwrap();
    packet[2..4].copy_from_slice(&total.to_be_bytes());
    let mut ethernet = vec![0_u8; 12];
    ethernet.extend_from_slice(&0x0800_u16.to_be_bytes());
    ethernet.extend_from_slice(&packet);
    ethernet
}

#[test]
fn the_edge_admits_only_what_it_serves() {
    use firewall::{Admitted, Refused, Verdict};
    let edge = Ipv4Addr::new(10, 42, 0, 1);
    let guest = Ipv4Addr::new(10, 42, 0, 9);
    let origin = Ipv4Addr::new(10, 42, 0, 7);
    let mut wall = firewall::Firewall::new(edge, 24, true);

    assert_eq!(
        wall.judge(&frame(guest, edge, 17, 53, &[0; 8])),
        Verdict::Accept(Admitted::Resolver)
    );
    assert_eq!(
        wall.judge(&frame(guest, edge, 6, 443, &[0; 16])),
        Verdict::Accept(Admitted::Ingress)
    );
    // Not a route: a packet aimed past the edge is dropped, so no Machine can
    // use the edge to reach a sibling it has no declared path to.
    assert_eq!(
        wall.judge(&frame(guest, origin, 6, 443, &[0; 16])),
        Verdict::Drop(Refused::NotTheEdge)
    );
    // A source outside the network's own range, at an edge that translates
    // addresses, is refused rather than translated.
    assert_eq!(
        wall.judge(&frame(
            Ipv4Addr::new(192, 168, 64, 5),
            edge,
            6,
            443,
            &[0; 16]
        )),
        Verdict::Drop(Refused::ForeignSource)
    );
    // Anything else addressed to the edge: default deny.
    assert_eq!(
        wall.judge(&frame(guest, edge, 6, 22, &[0; 16])),
        Verdict::Drop(Refused::UnservedPort)
    );
    assert_eq!(
        wall.judge(&frame(guest, edge, 1, 0, &[0; 16])),
        Verdict::Drop(Refused::UnservedProtocol)
    );

    // The reply path exists only for a flow the edge itself opened, and only
    // from the origin it was opened towards.
    assert_eq!(
        wall.judge(&frame(origin, edge, 6, 49152, &[0; 16])),
        Verdict::Drop(Refused::UnservedPort)
    );
    wall.open_translation(49152, origin);
    assert_eq!(
        wall.judge(&frame(origin, edge, 6, 49152, &[0; 16])),
        Verdict::Accept(Admitted::Translated)
    );
    assert_eq!(
        wall.judge(&frame(guest, edge, 6, 49152, &[0; 16])),
        Verdict::Drop(Refused::UnexpectedOrigin)
    );
    wall.close_translation(49152);
    assert_eq!(
        wall.judge(&frame(origin, edge, 6, 49152, &[0; 16])),
        Verdict::Drop(Refused::UnservedPort)
    );

    let counters = wall.counters();
    assert_eq!(counters.accepted.get(&Admitted::Translated), Some(&1));
    assert_eq!(counters.refused.get(&Refused::NotTheEdge), Some(&1));
}

#[test]
fn an_edge_with_nothing_published_accepts_no_connection() {
    use firewall::{Refused, Verdict};
    let edge = Ipv4Addr::new(10, 42, 0, 1);
    let guest = Ipv4Addr::new(10, 42, 0, 9);
    let mut wall = firewall::Firewall::new(edge, 24, false);
    assert_eq!(
        wall.judge(&frame(guest, edge, 6, 443, &[0; 16])),
        Verdict::Drop(Refused::UnservedPort)
    );
}

// -- the resolver -----------------------------------------------------------------

#[test]
fn the_resolver_answers_declared_names_and_refuses_the_rest() {
    let mut table = BTreeMap::new();
    table.insert(PUBLISHED_NAME.to_string(), Ipv4Addr::new(10, 42, 0, 1));

    let query = dns::parse_query(&dns_query(PUBLISHED_NAME)).expect("query");
    assert_eq!(
        dns::decide(query.question(), &table),
        dns::Decision::Answer(Ipv4Addr::new(10, 42, 0, 1))
    );
    let response = dns::respond(&query, dns::decide(query.question(), &table));
    assert_eq!(rcode(&response), 0);
    assert_eq!(
        answered_address(&response),
        Some(Ipv4Addr::new(10, 42, 0, 1))
    );

    // Case is not an identity. A client that asked in mixed case gets the same
    // answer, and the response still echoes what it asked.
    let shouted = dns::parse_query(&dns_query("API.SHOP.TEST")).expect("query");
    assert_eq!(
        dns::decide(shouted.question(), &table),
        dns::Decision::Answer(Ipv4Addr::new(10, 42, 0, 1))
    );

    let elsewhere = dns::parse_query(&dns_query("example.com")).expect("query");
    assert_eq!(
        dns::decide(elsewhere.question(), &table),
        dns::Decision::Unknown
    );
    assert_eq!(
        rcode(&dns::respond(&elsewhere, dns::Decision::Unknown)),
        3,
        "an undeclared name is NXDOMAIN, never a lookup somewhere else"
    );
}

#[test]
fn the_resolver_refuses_a_name_it_cannot_read_rather_than_looping() {
    // A compression pointer in a question is the classic way a name parser is
    // made to loop. It is refused, and refused with an answer, so a client
    // fails now rather than at its own timeout.
    let mut message = dns_query(PUBLISHED_NAME);
    message[12] = 0xc0;
    message[13] = 0x0c;
    let error = dns::parse_query(&message).expect_err("refused");
    let response = dns::reject(&message, error).expect("an answer");
    assert_eq!(rcode(&response), 1);
    assert_eq!(&response[0..2], &message[0..2], "the client's own id");
}

// -- the authority ----------------------------------------------------------------

#[test]
fn the_environment_issues_only_for_the_names_it_declared() {
    let identity =
        identity::EdgeIdentity::issue(ENVIRONMENT, &[PUBLISHED_NAME.to_string()]).expect("issue");
    assert!(
        identity
            .authority_pem()
            .starts_with("-----BEGIN CERTIFICATE-----")
    );
    // The authority is a certificate and nothing else: the key that issues
    // under it never leaves the process, so publishing this cannot let a reader
    // impersonate the edge.
    assert!(!identity.authority_pem().contains("PRIVATE KEY"));
    assert!(identity::EdgeIdentity::issue(ENVIRONMENT, &[]).is_err());
}

#[test]
fn the_edge_and_a_client_that_trusts_the_environment_complete_a_handshake() {
    // The TLS half alone, with no fabric under it: whatever the stack does or
    // does not carry, these two must agree, and a failure here is a failure of
    // the Environment's own issuance rather than of its network.
    let anchors = tempfile::tempdir().unwrap();
    let identity =
        identity::EdgeIdentity::issue(ENVIRONMENT, &[PUBLISHED_NAME.to_string()]).unwrap();
    let anchor = anchors.path().join("authority.pem");
    std::fs::write(&anchor, identity.authority_pem()).unwrap();

    let mut server = rustls::ServerConnection::new(identity.server_config()).unwrap();
    let mut client = rustls::ClientConnection::new(
        client_config(&anchor),
        rustls_pki_types::ServerName::try_from(PUBLISHED_NAME.to_string()).unwrap(),
    )
    .unwrap();

    fn carry(
        from: &mut impl std::ops::DerefMut<Target = rustls::ConnectionCommon<impl rustls::SideData>>,
    ) -> Vec<u8> {
        let mut wire = Vec::new();
        while from.wants_write() {
            if from.write_tls(&mut wire).is_err() {
                break;
            }
        }
        wire
    }

    let mut failure = None;
    for _ in 0..32 {
        let to_server = carry(&mut client);
        if !to_server.is_empty() {
            let mut cursor: &[u8] = &to_server;
            while !cursor.is_empty() && server.read_tls(&mut cursor).is_ok() {
                if let Err(problem) = server.process_new_packets() {
                    failure = Some(format!("the edge rejected the client: {problem}"));
                    break;
                }
            }
        }
        let to_client = carry(&mut server);
        if !to_client.is_empty() {
            let mut cursor: &[u8] = &to_client;
            while !cursor.is_empty() && client.read_tls(&mut cursor).is_ok() {
                if let Err(problem) = client.process_new_packets() {
                    failure = Some(format!("the client rejected the edge: {problem}"));
                    break;
                }
            }
        }
        if failure.is_some() || (!client.is_handshaking() && !server.is_handshaking()) {
            break;
        }
    }
    assert_eq!(failure, None);
    assert!(!server.is_handshaking() && !client.is_handshaking());
    // The edge knows which declared service the client asked for, which is what
    // its routing decision is made from.
    assert_eq!(server.server_name(), Some(PUBLISHED_NAME));
}
