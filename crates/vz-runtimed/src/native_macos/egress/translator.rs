//! The far end of a native Machine's egress link: a userspace stack that turns
//! the guest's TCP connections and DNS queries into the host's own.
//!
//! It is a termination, never a forward. No guest packet leaves this process:
//! a TCP connection is accepted by the stack here and a separate connection is
//! opened from the host towards the same destination, and a DNS query is
//! answered with the reply the host's own resolver gave. The guest therefore
//! reaches exactly the destinations `policy::admit` lets this process connect
//! to, and nothing on the host's network ever sees a guest address.
//!
//! What it does not carry: UDP other than DNS, ICMP, and IPv6. A guest that
//! needs QUIC falls back to TCP, as every QUIC client does on a network that
//! drops it.

use std::collections::HashMap;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::sync::Arc;
use std::time::Duration;

use smoltcp::iface::{Config, Interface, PollResult, SocketHandle, SocketSet};
use smoltcp::socket::{tcp, udp};
use smoltcp::time::Instant;
use smoltcp::wire::{
    ArpOperation, ArpPacket, ArpRepr, EthernetAddress, EthernetFrame, EthernetProtocol,
    HardwareAddress, IpAddress, IpCidr, IpEndpoint, IpListenEndpoint, IpProtocol, Ipv4Packet,
    TcpPacket,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixDatagram;
use tokio::sync::{Notify, mpsc, oneshot};
use tokio::task::JoinHandle;
use tracing::{debug, info};

use super::policy;
use crate::environment_gateway::edge::{FabricPort, MAX_FRAME_BYTES, QUEUE_DEPTH};
use crate::environment_switch::MacAddress;

/// How many guest connections one link carries at once. Bounded so one guest
/// exhausts its own link rather than the daemon; a connection past the bound is
/// refused, which a guest sees as a reset and retries.
const MAX_FLOWS: usize = 256;
/// Per-direction buffer of one connection. Above 64 KiB so the stack can scale
/// its window; the host side of the path is local and fast, and a window that
/// cannot cover it is what would bound throughput.
const FLOW_BUFFER_BYTES: usize = 256 * 1024;
/// The largest piece of a stream moved between the stack and a host socket at
/// once, and the depth of each direction's queue in such pieces.
const CHUNK_BYTES: usize = 32 * 1024;
const CHUNKS_IN_FLIGHT: usize = 8;
/// How long the host may take to open a connection before the guest's is reset.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(15);
/// A connection that never completed its handshake is reclaimed after this.
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(30);
/// Keep-alive towards the guest, and how long unanswered keep-alives or
/// retransmissions may last before the connection is presumed dead. Without
/// them a guest that vanished mid-connection would hold its slot and its host
/// socket for as long as the link lives.
const KEEP_ALIVE: smoltcp::time::Duration = smoltcp::time::Duration::from_secs(30);
const PEER_TIMEOUT: smoltcp::time::Duration = smoltcp::time::Duration::from_secs(180);
/// DNS: concurrent forwarded queries, and how long each upstream gets.
const MAX_QUERIES: usize = 64;
const QUERY_TIMEOUT: Duration = Duration::from_secs(3);
const DNS_PORT: u16 = 53;
const DNS_MESSAGE_BYTES: usize = 4096;
/// The longest the loop blocks when nothing at all is happening.
const IDLE_WAIT: Duration = Duration::from_millis(500);
const RESOLV_CONF: &str = "/etc/resolv.conf";

/// The link's addressing, fixed for every Machine: each link is a private
/// point-to-point segment, so there is nothing for two links' addresses to
/// collide with.
#[derive(Debug, Clone)]
pub(crate) struct TranslatorConfig {
    pub machine: String,
    pub mac: MacAddress,
    pub address: Ipv4Addr,
    pub prefix: u8,
    pub mtu: u32,
}

/// What one link did, reported once its task has been joined.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct EgressReceipt {
    pub connections_opened: u64,
    pub connections_refused: u64,
    pub connections_failed: u64,
    pub queries_answered: u64,
    pub queries_unanswered: u64,
    pub bytes_to_host: u64,
    pub bytes_to_guest: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct FlowKey {
    guest: SocketAddrV4,
    destination: SocketAddrV4,
}

enum HostEvent {
    Data(Vec<u8>),
    Closed,
    Failed,
}

/// One guest connection and the host connection opened for it.
struct Flow {
    key: FlowKey,
    handle: SocketHandle,
    /// Dropped once the guest has finished sending, which is what tells the
    /// host side to shut down its write half.
    to_host: Option<mpsc::Sender<Vec<u8>>>,
    from_host: mpsc::Receiver<HostEvent>,
    pending: Vec<u8>,
    sent: usize,
    host_closed: bool,
    closed_towards_guest: bool,
    /// Whether the guest's handshake ever completed. Until it has, a socket
    /// that can neither send nor receive is waiting, not finished.
    established: bool,
    failed: bool,
    opened: std::time::Instant,
    task: JoinHandle<()>,
}

impl Drop for Flow {
    fn drop(&mut self) {
        // The host socket belongs to this flow and to nothing else, so it goes
        // with it rather than waiting for the remote end to close.
        self.task.abort();
    }
}

pub(crate) struct Translator {
    config: TranslatorConfig,
    port: FabricPort,
    interface: Interface,
    sockets: SocketSet<'static>,
    resolver: SocketHandle,
    flows: HashMap<FlowKey, Flow>,
    wake: Arc<Notify>,
    answers: mpsc::Receiver<(IpEndpoint, Option<Vec<u8>>)>,
    answer: mpsc::Sender<(IpEndpoint, Option<Vec<u8>>)>,
    queries: usize,
    receipt: EgressReceipt,
    started: std::time::Instant,
}

impl Translator {
    pub(crate) fn new(config: TranslatorConfig) -> Result<Self, String> {
        let mut port = FabricPort::new(config.mtu);
        let mut interface_config = Config::new(HardwareAddress::Ethernet(EthernetAddress(
            config.mac.bytes(),
        )));
        interface_config.random_seed = seed(&config.machine);
        let mut interface = Interface::new(interface_config, &mut port, Instant::from_micros(0));
        interface.update_ip_addrs(|addresses| {
            let _ = addresses.push(IpCidr::new(IpAddress::Ipv4(config.address), config.prefix));
        });
        // Every destination is routed via this stack's own address, which with
        // AnyIP set is what makes the stack accept a SYN addressed to any host:
        // the connection is terminated here and re-originated by the host.
        interface
            .routes_mut()
            .add_default_ipv4_route(config.address)
            .map_err(|error| format!("egress default route: {error:?}"))?;
        interface.set_any_ip(true);

        let mut sockets = SocketSet::new(Vec::new());
        let buffer = || {
            udp::PacketBuffer::new(
                vec![udp::PacketMetadata::EMPTY; MAX_QUERIES],
                vec![0_u8; DNS_MESSAGE_BYTES * 16],
            )
        };
        let mut resolver = udp::Socket::new(buffer(), buffer());
        resolver
            .bind(IpListenEndpoint {
                addr: Some(IpAddress::Ipv4(config.address)),
                port: DNS_PORT,
            })
            .map_err(|error| format!("egress resolver bind: {error:?}"))?;
        let resolver = sockets.add(resolver);
        let (answer, answers) = mpsc::channel(MAX_QUERIES);
        Ok(Self {
            config,
            port,
            interface,
            sockets,
            resolver,
            flows: HashMap::new(),
            wake: Arc::new(Notify::new()),
            answers,
            answer,
            queries: 0,
            receipt: EgressReceipt::default(),
            started: std::time::Instant::now(),
        })
    }

    fn now(&self) -> Instant {
        Instant::from_micros(i64::try_from(self.started.elapsed().as_micros()).unwrap_or(i64::MAX))
    }

    /// Run until asked to stop or until the guest end of the link closes.
    pub(crate) async fn run(
        mut self,
        socket: UnixDatagram,
        mut stop: oneshot::Receiver<()>,
    ) -> EgressReceipt {
        let mut buffer = vec![0_u8; MAX_FRAME_BYTES];
        loop {
            while self.port.inbound.len() < QUEUE_DEPTH {
                match socket.try_recv(&mut buffer) {
                    Ok(read) => {
                        let frame = buffer[..read].to_vec();
                        if self.observe(&frame) {
                            self.port.inbound.push_back(frame);
                        }
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => break,
                    Err(_) => return self.finish(),
                }
            }

            let polled = self
                .interface
                .poll(self.now(), &mut self.port, &mut self.sockets);
            let served = self.serve_resolver() | self.serve_flows();
            self.interface
                .poll(self.now(), &mut self.port, &mut self.sockets);

            let mut wrote = false;
            while let Some(frame) = self.port.outbound.pop_front() {
                wrote = true;
                if socket.send(&frame).await.is_err() {
                    return self.finish();
                }
            }

            if polled == PollResult::SocketStateChanged
                || served
                || wrote
                || !self.port.inbound.is_empty()
            {
                continue;
            }
            let delay = self
                .interface
                .poll_delay(self.now(), &self.sockets)
                .map(|delay| Duration::from_micros(delay.total_micros()))
                .unwrap_or(IDLE_WAIT)
                .min(IDLE_WAIT);
            let wake = Arc::clone(&self.wake);
            tokio::select! {
                biased;
                _ = &mut stop => break,
                _ = wake.notified() => {}
                readable = tokio::time::timeout(delay, socket.readable()) => {
                    if matches!(readable, Ok(Err(_))) {
                        break;
                    }
                }
            }
        }
        self.finish()
    }

    fn finish(mut self) -> EgressReceipt {
        self.flows.clear();
        info!(
            machine = %self.config.machine,
            receipt = ?self.receipt,
            "native Machine egress link stopped"
        );
        self.receipt
    }

    /// See a frame before the stack does, open a flow for a new connection, and
    /// say whether the stack should see the frame at all.
    ///
    /// This is where the destination policy is applied: a SYN to a destination
    /// `policy::admit` refuses gets no listening socket, so the stack answers
    /// it with a reset and no host connection is ever attempted.
    ///
    /// ARP is the one thing withheld. With AnyIP set the stack would answer an
    /// ARP request for any address, including the guest's own duplicate-address
    /// probe, and the guest would then refuse its address as already in use.
    /// Only the gateway's own address is answered for.
    fn observe(&mut self, frame: &[u8]) -> bool {
        if let Some(target) = arp_request_target(frame) {
            return target == self.config.address;
        }
        let Some((key, opening)) = tcp_flow(frame) else {
            return true;
        };
        if !opening || self.flows.contains_key(&key) {
            return true;
        }
        let destination = *key.destination.ip();
        if let Err(refusal) = policy::admit(destination, &policy::host_addresses()) {
            self.receipt.connections_refused += 1;
            debug!(
                machine = %self.config.machine,
                destination = %key.destination,
                ?refusal,
                "native Machine egress refused a connection"
            );
            return true;
        }
        if self.flows.len() >= MAX_FLOWS {
            self.receipt.connections_refused += 1;
            return true;
        }
        let mut socket = tcp::Socket::new(
            tcp::SocketBuffer::new(vec![0_u8; FLOW_BUFFER_BYTES]),
            tcp::SocketBuffer::new(vec![0_u8; FLOW_BUFFER_BYTES]),
        );
        socket.set_nagle_enabled(false);
        socket.set_keep_alive(Some(KEEP_ALIVE));
        socket.set_timeout(Some(PEER_TIMEOUT));
        if socket
            .listen(IpListenEndpoint {
                addr: Some(IpAddress::Ipv4(destination)),
                port: key.destination.port(),
            })
            .is_err()
        {
            return true;
        }
        let handle = self.sockets.add(socket);
        let (to_host, host_writes) = mpsc::channel(CHUNKS_IN_FLIGHT);
        let (host_events, from_host) = mpsc::channel(CHUNKS_IN_FLIGHT);
        let task = tokio::spawn(carry(
            key.destination,
            host_writes,
            host_events,
            Arc::clone(&self.wake),
        ));
        self.receipt.connections_opened += 1;
        self.flows.insert(
            key,
            Flow {
                key,
                handle,
                to_host: Some(to_host),
                from_host,
                pending: Vec::new(),
                sent: 0,
                host_closed: false,
                closed_towards_guest: false,
                established: false,
                failed: false,
                opened: std::time::Instant::now(),
                task,
            },
        );
        true
    }

    /// Move bytes between every flow's stack socket and its host connection,
    /// and retire the flows that are done. Returns whether anything moved.
    fn serve_flows(&mut self) -> bool {
        let mut progressed = false;
        let mut finished = Vec::new();
        for flow in self.flows.values_mut() {
            let socket = self.sockets.get_mut::<tcp::Socket>(flow.handle);
            use tcp::State;
            if !matches!(
                socket.state(),
                State::Listen | State::SynReceived | State::SynSent | State::Closed
            ) {
                flow.established = true;
            }

            // Host to guest.
            loop {
                if flow.sent == flow.pending.len() {
                    flow.pending.clear();
                    flow.sent = 0;
                    if flow.host_closed || flow.failed {
                        break;
                    }
                    match flow.from_host.try_recv() {
                        Ok(HostEvent::Data(data)) => flow.pending = data,
                        Ok(HostEvent::Closed) => {
                            flow.host_closed = true;
                            break;
                        }
                        Ok(HostEvent::Failed) | Err(mpsc::error::TryRecvError::Disconnected) => {
                            flow.failed = true;
                            break;
                        }
                        Err(mpsc::error::TryRecvError::Empty) => break,
                    }
                }
                if !socket.can_send() {
                    break;
                }
                match socket.send_slice(&flow.pending[flow.sent..]) {
                    Ok(0) | Err(_) => break,
                    Ok(sent) => {
                        flow.sent += sent;
                        self.receipt.bytes_to_guest += sent as u64;
                        progressed = true;
                    }
                }
            }
            if flow.failed && socket.state() != State::Closed {
                // A host connection that could not be opened, or that broke,
                // is a reset towards the guest: a FIN would claim the stream
                // ended cleanly.
                if !flow.established {
                    self.receipt.connections_failed += 1;
                }
                socket.abort();
                progressed = true;
            } else if flow.host_closed
                && flow.pending.is_empty()
                && !flow.closed_towards_guest
                && flow.established
            {
                socket.close();
                flow.closed_towards_guest = true;
                progressed = true;
            }

            // Guest to host.
            if let Some(to_host) = &flow.to_host {
                while socket.can_recv() {
                    let Ok(permit) = to_host.try_reserve() else {
                        break;
                    };
                    let Ok(data) = socket.recv(|buffer| {
                        let take = buffer.len().min(CHUNK_BYTES);
                        (take, buffer[..take].to_vec())
                    }) else {
                        break;
                    };
                    self.receipt.bytes_to_host += data.len() as u64;
                    permit.send(data);
                    progressed = true;
                }
                if flow.established && !socket.may_recv() && socket.recv_queue() == 0 {
                    flow.to_host = None;
                    progressed = true;
                }
            }

            let state = socket.state();
            let done = (flow.established || flow.failed)
                && matches!(state, State::Closed | State::TimeWait);
            let stalled = matches!(state, State::Listen | State::SynReceived)
                && flow.opened.elapsed() > HANDSHAKE_TIMEOUT;
            if done || stalled {
                finished.push(flow.key);
            }
        }
        for key in finished {
            if let Some(flow) = self.flows.remove(&key) {
                self.sockets.remove(flow.handle);
                progressed = true;
            }
        }
        progressed
    }

    /// Answer what the host's resolver has answered, and forward new queries.
    fn serve_resolver(&mut self) -> bool {
        let mut progressed = false;
        while let Ok((endpoint, answer)) = self.answers.try_recv() {
            self.queries = self.queries.saturating_sub(1);
            progressed = true;
            match answer {
                Some(answer) => {
                    self.receipt.queries_answered += 1;
                    let socket = self.sockets.get_mut::<udp::Socket>(self.resolver);
                    let _ = socket.send_slice(&answer, endpoint);
                }
                None => self.receipt.queries_unanswered += 1,
            }
        }
        loop {
            let socket = self.sockets.get_mut::<udp::Socket>(self.resolver);
            let Ok((query, metadata)) = socket.recv() else {
                break;
            };
            progressed = true;
            if self.queries >= MAX_QUERIES || query.len() < 12 {
                self.receipt.queries_unanswered += 1;
                continue;
            }
            self.queries += 1;
            tokio::spawn(resolve(
                query.to_vec(),
                metadata.endpoint,
                self.answer.clone(),
                Arc::clone(&self.wake),
            ));
        }
        progressed
    }
}

/// The address an ARP request asks about, when the frame is one.
fn arp_request_target(frame: &[u8]) -> Option<Ipv4Addr> {
    let ethernet = EthernetFrame::new_checked(frame).ok()?;
    if ethernet.ethertype() != EthernetProtocol::Arp {
        return None;
    }
    let arp = ArpPacket::new_checked(ethernet.payload()).ok()?;
    match ArpRepr::parse(&arp).ok()? {
        ArpRepr::EthernetIpv4 {
            operation: ArpOperation::Request,
            target_protocol_addr,
            ..
        } => Some(target_protocol_addr),
        _ => None,
    }
}

/// The 4-tuple of a TCP segment in an Ethernet frame, and whether it opens a
/// connection (SYN without ACK).
fn tcp_flow(frame: &[u8]) -> Option<(FlowKey, bool)> {
    let ethernet = EthernetFrame::new_checked(frame).ok()?;
    if ethernet.ethertype() != EthernetProtocol::Ipv4 {
        return None;
    }
    let ip = Ipv4Packet::new_checked(ethernet.payload()).ok()?;
    if ip.next_header() != IpProtocol::Tcp || ip.more_frags() || ip.frag_offset() != 0 {
        return None;
    }
    let tcp = TcpPacket::new_checked(ip.payload()).ok()?;
    let key = FlowKey {
        guest: SocketAddrV4::new(ip.src_addr(), tcp.src_port()),
        destination: SocketAddrV4::new(ip.dst_addr(), tcp.dst_port()),
    };
    Some((key, tcp.syn() && !tcp.ack()))
}

/// The host half of one flow: open the connection, then copy in both
/// directions until both are done.
async fn carry(
    destination: SocketAddrV4,
    mut writes: mpsc::Receiver<Vec<u8>>,
    events: mpsc::Sender<HostEvent>,
    wake: Arc<Notify>,
) {
    let stream =
        match tokio::time::timeout(CONNECT_TIMEOUT, tokio::net::TcpStream::connect(destination))
            .await
        {
            Ok(Ok(stream)) => stream,
            _ => {
                let _ = events.send(HostEvent::Failed).await;
                wake.notify_one();
                return;
            }
        };
    let _ = stream.set_nodelay(true);
    let (mut reader, mut writer) = stream.into_split();
    let write_events = events.clone();
    let write_wake = Arc::clone(&wake);
    let write = async move {
        while let Some(chunk) = writes.recv().await {
            if writer.write_all(&chunk).await.is_err() {
                let _ = write_events.send(HostEvent::Failed).await;
                write_wake.notify_one();
                return;
            }
            // A chunk written frees a slot the loop may be waiting on.
            write_wake.notify_one();
        }
        let _ = writer.shutdown().await;
    };
    let read = async move {
        let mut buffer = vec![0_u8; CHUNK_BYTES];
        loop {
            let event = match reader.read(&mut buffer).await {
                Ok(0) => HostEvent::Closed,
                Ok(read) => HostEvent::Data(buffer[..read].to_vec()),
                Err(_) => HostEvent::Failed,
            };
            let last = !matches!(event, HostEvent::Data(_));
            if events.send(event).await.is_err() {
                return;
            }
            wake.notify_one();
            if last {
                return;
            }
        }
    };
    tokio::join!(write, read);
}

/// Forward one query to the host's resolvers and hand back the first answer.
async fn resolve(
    query: Vec<u8>,
    endpoint: IpEndpoint,
    answer: mpsc::Sender<(IpEndpoint, Option<Vec<u8>>)>,
    wake: Arc<Notify>,
) {
    let mut reply = None;
    for upstream in upstream_resolvers() {
        if let Some(found) = ask(&query, upstream).await {
            reply = Some(found);
            break;
        }
    }
    let _ = answer.send((endpoint, reply)).await;
    wake.notify_one();
}

async fn ask(query: &[u8], upstream: SocketAddr) -> Option<Vec<u8>> {
    let bind: SocketAddr = if upstream.is_ipv4() {
        (Ipv4Addr::UNSPECIFIED, 0).into()
    } else {
        (std::net::Ipv6Addr::UNSPECIFIED, 0).into()
    };
    let socket = tokio::net::UdpSocket::bind(bind).await.ok()?;
    socket.connect(upstream).await.ok()?;
    socket.send(query).await.ok()?;
    let mut buffer = vec![0_u8; DNS_MESSAGE_BYTES];
    let deadline = tokio::time::Instant::now() + QUERY_TIMEOUT;
    loop {
        let read = tokio::time::timeout_at(deadline, socket.recv(&mut buffer))
            .await
            .ok()?
            .ok()?;
        // A connected UDP socket only receives from its peer; the id is what
        // ties the datagram to this query rather than to a stale one.
        if read >= 12 && buffer[..2] == query[..2] {
            buffer.truncate(read);
            return Some(buffer);
        }
    }
}

/// The resolvers the host itself uses, read at query time so a host that
/// moves between networks keeps resolving through whichever it is on now.
fn upstream_resolvers() -> Vec<SocketAddr> {
    std::fs::read_to_string(RESOLV_CONF)
        .map(|text| parse_resolv_conf(&text))
        .unwrap_or_default()
}

fn parse_resolv_conf(text: &str) -> Vec<SocketAddr> {
    text.lines()
        .filter_map(|line| {
            let mut fields = line.split_whitespace();
            (fields.next()? == "nameserver").then_some(())?;
            let address: std::net::IpAddr = fields.next()?.parse().ok()?;
            Some(SocketAddr::new(address, DNS_PORT))
        })
        .collect()
}

fn seed(machine: &str) -> u64 {
    use sha2::{Digest, Sha256};
    let digest = Sha256::new()
        .chain_update(b"vz.native.egress.seed.v1\n")
        .chain_update(machine.as_bytes())
        .finalize();
    let mut bytes = [0_u8; 8];
    bytes.copy_from_slice(&digest[..8]);
    u64::from_be_bytes(bytes)
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    #[test]
    fn resolv_conf_yields_every_parseable_nameserver_in_order() {
        let parsed = parse_resolv_conf(
            "# comment\ndomain example\nnameserver 192.168.1.1\nnameserver fe80::1%en0\nnameserver 2001:db8::53\nnameserver\nsearch x\n",
        );
        assert_eq!(
            parsed,
            vec![
                SocketAddr::from(([192, 168, 1, 1], 53)),
                "[2001:db8::53]:53"
                    .parse()
                    .unwrap_or_else(|_| unreachable!()),
            ]
        );
    }

    fn syn(source: [u8; 4], destination: [u8; 4], ack: bool) -> Vec<u8> {
        segment(
            [2, 0, 0, 0, 0, 2],
            [2, 0, 0, 0, 0, 1],
            source,
            destination,
            443,
            ack,
        )
    }

    /// An Ethernet frame carrying a SYN, as a guest NIC would write it.
    pub(crate) fn syn_frame(
        source_mac: [u8; 6],
        destination_mac: [u8; 6],
        source: [u8; 4],
        destination: [u8; 4],
        port: u16,
    ) -> Vec<u8> {
        segment(
            source_mac,
            destination_mac,
            source,
            destination,
            port,
            false,
        )
    }

    fn segment(
        source_mac: [u8; 6],
        destination_mac: [u8; 6],
        source: [u8; 4],
        destination: [u8; 4],
        port: u16,
        ack: bool,
    ) -> Vec<u8> {
        use smoltcp::phy::ChecksumCapabilities;
        use smoltcp::wire::{EthernetRepr, Ipv4Repr, TcpControl, TcpRepr, TcpSeqNumber};
        let tcp = TcpRepr {
            src_port: 50000,
            dst_port: port,
            control: TcpControl::Syn,
            seq_number: TcpSeqNumber(1),
            ack_number: ack.then_some(TcpSeqNumber(1)),
            window_len: 1024,
            window_scale: None,
            max_seg_size: None,
            sack_permitted: false,
            sack_ranges: [None; 3],
            timestamp: None,
            payload: &[],
        };
        let ip = Ipv4Repr {
            src_addr: Ipv4Addr::from(source),
            dst_addr: Ipv4Addr::from(destination),
            next_header: IpProtocol::Tcp,
            payload_len: tcp.buffer_len(),
            hop_limit: 64,
        };
        let ethernet = EthernetRepr {
            src_addr: EthernetAddress(source_mac),
            dst_addr: EthernetAddress(destination_mac),
            ethertype: EthernetProtocol::Ipv4,
        };
        let mut frame = vec![0_u8; ethernet.buffer_len() + ip.buffer_len() + tcp.buffer_len()];
        let mut ethernet_frame = EthernetFrame::new_unchecked(&mut frame);
        ethernet.emit(&mut ethernet_frame);
        let mut ip_packet = Ipv4Packet::new_unchecked(ethernet_frame.payload_mut());
        ip.emit(&mut ip_packet, &ChecksumCapabilities::default());
        let mut tcp_packet = TcpPacket::new_unchecked(ip_packet.payload_mut());
        tcp.emit(
            &mut tcp_packet,
            &IpAddress::Ipv4(ip.src_addr),
            &IpAddress::Ipv4(ip.dst_addr),
            &ChecksumCapabilities::default(),
        );
        frame
    }

    fn arp_request(target: [u8; 4]) -> Vec<u8> {
        use smoltcp::wire::EthernetRepr;
        let arp = ArpRepr::EthernetIpv4 {
            operation: ArpOperation::Request,
            source_hardware_addr: EthernetAddress([2, 0, 0, 0, 0, 2]),
            source_protocol_addr: Ipv4Addr::UNSPECIFIED,
            target_hardware_addr: EthernetAddress([0; 6]),
            target_protocol_addr: Ipv4Addr::from(target),
        };
        let ethernet = EthernetRepr {
            src_addr: EthernetAddress([2, 0, 0, 0, 0, 2]),
            dst_addr: EthernetAddress::BROADCAST,
            ethertype: EthernetProtocol::Arp,
        };
        let mut frame = vec![0_u8; ethernet.buffer_len() + arp.buffer_len()];
        let mut ethernet_frame = EthernetFrame::new_unchecked(&mut frame);
        ethernet.emit(&mut ethernet_frame);
        arp.emit(&mut ArpPacket::new_unchecked(ethernet_frame.payload_mut()));
        frame
    }

    /// The ARP reply a guest owning `mac` would give to a request in `frame`.
    pub(crate) fn arp_reply_for(frame: &[u8], mac: [u8; 6]) -> Option<Vec<u8>> {
        use smoltcp::wire::EthernetRepr;
        let ethernet = EthernetFrame::new_checked(frame).ok()?;
        if ethernet.ethertype() != EthernetProtocol::Arp {
            return None;
        }
        let ArpRepr::EthernetIpv4 {
            operation: ArpOperation::Request,
            source_hardware_addr,
            source_protocol_addr,
            target_protocol_addr,
            ..
        } = ArpRepr::parse(&ArpPacket::new_checked(ethernet.payload()).ok()?).ok()?
        else {
            return None;
        };
        let arp = ArpRepr::EthernetIpv4 {
            operation: ArpOperation::Reply,
            source_hardware_addr: EthernetAddress(mac),
            source_protocol_addr: target_protocol_addr,
            target_hardware_addr: source_hardware_addr,
            target_protocol_addr: source_protocol_addr,
        };
        let reply = EthernetRepr {
            src_addr: EthernetAddress(mac),
            dst_addr: source_hardware_addr,
            ethertype: EthernetProtocol::Arp,
        };
        let mut bytes = vec![0_u8; reply.buffer_len() + arp.buffer_len()];
        let mut reply_frame = EthernetFrame::new_unchecked(&mut bytes);
        reply.emit(&mut reply_frame);
        arp.emit(&mut ArpPacket::new_unchecked(reply_frame.payload_mut()));
        Some(bytes)
    }

    /// The guest's duplicate-address probe for its own address must go
    /// unanswered, or it refuses the address; only the gateway is answered for.
    #[tokio::test]
    async fn only_the_gateways_own_address_is_answered_for_in_arp() {
        let mut translator = translator();
        assert!(translator.observe(&arp_request([198, 18, 0, 1])));
        assert!(!translator.observe(&arp_request([198, 18, 0, 2])));
        assert!(!translator.observe(&arp_request([1, 1, 1, 1])));
    }

    #[test]
    fn a_syn_without_ack_opens_a_flow_and_a_syn_ack_does_not() {
        let (key, opening) =
            tcp_flow(&syn([198, 18, 0, 2], [1, 1, 1, 1], false)).unwrap_or_else(|| unreachable!());
        assert!(opening);
        assert_eq!(
            key.guest,
            SocketAddrV4::new(Ipv4Addr::new(198, 18, 0, 2), 50000)
        );
        assert_eq!(
            key.destination,
            SocketAddrV4::new(Ipv4Addr::new(1, 1, 1, 1), 443)
        );
        let (_, opening) =
            tcp_flow(&syn([198, 18, 0, 2], [1, 1, 1, 1], true)).unwrap_or_else(|| unreachable!());
        assert!(!opening);
    }

    fn translator() -> Translator {
        Translator::new(TranslatorConfig {
            machine: "test".into(),
            mac: MacAddress::new([2, 0, 0, 0, 0, 1]),
            address: Ipv4Addr::new(198, 18, 0, 1),
            prefix: 24,
            mtu: 1500,
        })
        .unwrap_or_else(|error| unreachable!("{error}"))
    }

    /// The policy gate, observed where it acts: a SYN to a refused destination
    /// opens no flow (so the stack resets it and the host never connects), and
    /// a SYN to a public one opens exactly one however often it is retransmitted.
    #[tokio::test]
    async fn only_an_admitted_destination_gets_a_host_connection() {
        let mut translator = translator();
        for refused in [
            [127, 0, 0, 1],
            [192, 168, 64, 3],
            [10, 0, 0, 2],
            [198, 18, 0, 1],
            [169, 254, 169, 254],
        ] {
            translator.observe(&syn([198, 18, 0, 2], refused, false));
        }
        assert!(translator.flows.is_empty());
        assert_eq!(translator.receipt.connections_refused, 5);

        let public = syn([198, 18, 0, 2], [192, 0, 2, 1], false);
        translator.observe(&public);
        assert!(
            translator.flows.is_empty(),
            "documentation space is not the Internet"
        );

        let public = syn([198, 18, 0, 2], [1, 1, 1, 1], false);
        translator.observe(&public);
        translator.observe(&public);
        assert_eq!(
            translator.flows.len(),
            1,
            "a retransmitted SYN is the same flow"
        );
        assert_eq!(translator.receipt.connections_opened, 1);
    }
}
