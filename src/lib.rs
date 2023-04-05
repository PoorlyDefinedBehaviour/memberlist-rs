use anyhow::{anyhow, Context, Result};
use byteorder::{ReadBytesExt, WriteBytesExt};
use rand::{seq::SliceRandom, Rng};
use std::{
    collections::{BinaryHeap, HashSet},
    io::{Cursor, Write},
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
    time::Duration,
};
use tokio::{
    net::UdpSocket,
    select,
    sync::mpsc::{Receiver, Sender},
};
use tracing::{error, info};

/// How many bytes the biggest possible peer takes in a message.
const PEER_SIZE_IN_BYTES: usize = 1  + // address type
    16 + // ipv6 octets
    2  + // port
    1  + // status
    4; // incarnation number

trait SocketAddrExt {
    fn address_type(&self) -> PeerAddressType;

    fn ip_octets(&self) -> &[u8];
}

impl SocketAddrExt for SocketAddr {
    fn address_type(&self) -> PeerAddressType {
        match self.ip() {
            IpAddr::V4(ip) => PeerAddressType::Ipv4,
            IpAddr::V6(ip) => PeerAddressType::Ipv6,
        }
    }

    fn ip_octets(&self) -> &[u8] {
        match self.ip() {
            IpAddr::V4(ip) => ip.octets().as_ref(),
            IpAddr::V6(ip) => ip.octets().as_ref(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
struct Peer {
    /// The peer address.
    addr: SocketAddr,
    /// The peer status in this member view.
    status: PeerStatus,
    /// Whenever the peer is declared as alive, its incarnation number is incremented.
    incarnation_number: u32,
    /// The amount of times this peer has been piggybacked on messages sent to other peers.
    dissemination_count: u32,
}

impl Peer {
    fn is_dead(&self) -> bool {
        match self.status {
            PeerStatus::Suspected => true,
            PeerStatus::Alive | PeerStatus::Dead => false,
        }
    }
}

struct OrderByDisseminationCount<'a>(&'a Peer);

impl<'a> PartialEq for OrderByDisseminationCount<'a> {
    fn eq(&self, other: &Self) -> bool {
        self.0.dissemination_count == other.0.dissemination_count
    }
}

impl<'a> Eq for OrderByDisseminationCount<'a> {}

impl<'a> PartialOrd for OrderByDisseminationCount<'a> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<'a> Ord for OrderByDisseminationCount<'a> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.dissemination_count.cmp(&other.0.dissemination_count)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
enum PeerStatus {
    Alive,
    Dead,
    Suspected,
}

impl From<u8> for PeerStatus {
    fn from(value: u8) -> Self {
        match value {
            1 => PeerStatus::Alive,
            2 => PeerStatus::Dead,
            3 => PeerStatus::Suspected,
            value => unreachable!("unexpected peer status. status={value}"),
        }
    }
}

impl PeerStatus {
    fn as_u8(&self) -> u8 {
        match self {
            PeerStatus::Alive => 1,
            PeerStatus::Dead => 2,
            PeerStatus::Suspected => 3,
        }
    }
}

/// Used by the library client to interact with the member.
#[derive(Debug)]
pub struct MemberlistHandler {
    sender: Sender<Message>,
}

#[derive(Debug)]
pub struct Config {
    /// Addresses of peers that this member will contact to join the cluster.
    pub join_peers: Vec<SocketAddr>,
    /// How many messages can be held in the channel used to buffer actions sent to this member before blocking.
    pub mailbox_buffer_size: usize,
    /// The amount of time to wait for between failure detection attempts.
    pub failure_detection_attempt_interval: Duration,
    /// The amount of time a peer has to respond to a ping request.
    pub peer_ping_request_timeout: Duration,
    /// Maximum number of peers used to detect if a peer is alive when it doesn't respond to a ping request.
    pub max_number_peers_for_indirect_probe: usize,
    /// Maximum number of peers that can be piggybacked on messages.
    pub max_number_peers_for_dissemination: usize,
    /// Address to bind this member's udp socket to.
    pub socket_addr: String,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            join_peers: Vec::new(),
            mailbox_buffer_size: 64,
            failure_detection_attempt_interval: Duration::from_secs(4),
            peer_ping_request_timeout: Duration::from_secs(2),
            max_number_peers_for_indirect_probe: 3,
            max_number_peers_for_dissemination: 3,
            socket_addr: "0.0.0.0:9157".to_owned(),
        }
    }
}

#[derive(Debug)]
struct Memberlist {
    /// This member's config.
    config: Config,
    /// The index in of the next peer in `peers` to check for failure.
    next_peer_index: usize,
    /// List of peers this member knows about.
    peers: Vec<Peer>,
    /// Channel to receive actions to perform.
    receiver: Receiver<Message>,
    /// UDP socket used to communicate with other peers.
    udp_socket: UdpSocket,
}

/// Represents an action that this member should perform.
#[derive(Debug)]
enum Message {}

/// The type of a message sent from one peer to other.
#[derive(Debug)]
enum PeerMessageType {
    /// Find out if a peer is alive.
    Ping,
    /// Response to a Ping request.
    Ack,
    /// Ask other peers to find out if a peer is alive.
    PingReq,
    /// The peer could be reached.
    PingReqAlive,
    /// The peer could not be reached.
    PingReqUnreachable,
}

impl PeerMessageType {
    const fn as_u8(&self) -> u8 {
        match self {
            PeerMessageType::Ping => 1,
            PeerMessageType::Ack => 2,
            PeerMessageType::PingReq => 3,
            PeerMessageType::PingReqAlive => 4,
            PeerMessageType::PingReqUnreachable => 5,
        }
    }
}

#[derive(Debug)]
enum PeerAddressType {
    Ipv4,
    Ipv6,
}

impl PeerAddressType {
    fn as_u8(&self) -> u8 {
        match self {
            PeerAddressType::Ipv4 => 1,
            PeerAddressType::Ipv6 => 2,
        }
    }
}

/// Represents a message received from a peer.
#[derive(Debug, PartialEq)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
enum PeerMessage {
    Ping(PingMessage),
    PingReq(PingReqMessage),
    Ack(AckMessage),
}

#[derive(Debug, PartialEq)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
struct AckMessage {
    piggybacked_peers: Vec<Peer>,
}

#[derive(Debug, PartialEq)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
struct PingReqMessage {
    target_peer_addr: SocketAddr,
}

#[derive(Debug, PartialEq)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
struct PingMessage {
    piggybacked_peers: Vec<Peer>,
}

fn write_socket_addr(mut buffer: impl std::io::Write, addr: SocketAddr) -> std::io::Result<()> {
    buffer.write_u8(addr.address_type().as_u8())?;
    buffer.write_all(addr.ip_octets())?;
    buffer.write_u16::<byteorder::BigEndian>(addr.port())?;
    Ok(())
}

fn read_socket_addr(mut reader: impl std::io::Read) -> std::io::Result<SocketAddr> {
    let address_type = reader.read_u8()?;
    let num_octets = reader.read_u32::<byteorder::BigEndian>()?;

    if address_type == PeerAddressType::Ipv4.as_u8() {
        let mut octets = [0_8; 4];
        reader.read_exact(&mut octets)?;
        let port = reader.read_u16::<byteorder::BigEndian>()?;
        return Ok(SocketAddr::new(IpAddr::V4(Ipv4Addr::from(octets)), port));
    } else if address_type == PeerAddressType::Ipv6.as_u8() {
        let mut octets = [0_u8; 16];
        reader.read_exact(&mut octets)?;
        let port = reader.read_u16::<byteorder::BigEndian>()?;
        return Ok(SocketAddr::new(IpAddr::V6(Ipv6Addr::from(octets)), port));
    }

    unreachable!()
}

fn write_peers(mut buffer: impl std::io::Write, peers: &[Peer]) -> std::io::Result<()> {
    buffer.write_u32::<byteorder::BigEndian>(peers.len() as u32)?;

    for peer in peers {
        // Address
        write_socket_addr(buffer, peer.addr)?;

        // Status
        buffer.write_u8(peer.status.as_u8())?;

        // Incarnation
        buffer.write_u32::<byteorder::BigEndian>(peer.incarnation_number)?;
    }

    Ok(())
}

fn read_peers(mut reader: impl std::io::Read) -> Result<Vec<Peer>> {
    let num_peers = reader.read_u32::<byteorder::BigEndian>()?;
    let mut peers = Vec::with_capacity(num_peers as usize);

    for _ in 0..num_peers {
        let addr = read_socket_addr(reader)?;
        let status = reader.read_u8()?;
        let incarnation_number = reader.read_u32::<byteorder::BigEndian>()?;
        peers.push(Peer {
            addr,
            status: PeerStatus::from(status),
            incarnation_number,
            dissemination_count: 0,
        });
    }

    Ok(peers)
}

impl TryFrom<&PeerMessage> for Vec<u8> {
    type Error = anyhow::Error;

    fn try_from(value: &PeerMessage) -> std::result::Result<Self, Self::Error> {
        let mut buffer = Vec::new();

        match value {
            PeerMessage::Ping { peers } => {
                buffer.write_u8(PeerMessageType::Ping.as_u8())?;
                write_peers(&mut buffer, peers)?;
            }
            PeerMessage::Ack { peers } => {
                buffer.write_u8(PeerMessageType::Ack.as_u8())?;
                write_peers(&mut buffer, peers)?;
            }
            PeerMessage::PingReq { target_peer } => {
                buffer.write_u8(PeerMessageType::PingReq.as_u8())?;
                write_socket_addr(&mut buffer, *target_peer)?;
            }
        }

        Ok(buffer)
    }
}

impl TryFrom<&[u8]> for PingMessage {
    type Error = anyhow::Error;

    fn try_from(value: &[u8]) -> std::result::Result<Self, Self::Error> {
        Ok(PingMessage {
            piggybacked_peers: read_peers(value)?,
        })
    }
}

impl TryFrom<&[u8]> for PingReqMessage {
    type Error = anyhow::Error;

    fn try_from(value: &[u8]) -> std::result::Result<Self, Self::Error> {
        Ok(PingReqMessage {
            target_peer_addr: read_socket_addr(value)?,
        })
    }
}

impl TryFrom<&[u8]> for AckMessage {
    type Error = anyhow::Error;

    fn try_from(value: &[u8]) -> std::result::Result<Self, Self::Error> {
        Ok(AckMessage {
            piggybacked_peers: read_peers(value)?,
        })
    }
}

impl TryFrom<&[u8]> for PeerMessage {
    type Error = anyhow::Error;

    fn try_from(value: &[u8]) -> std::result::Result<Self, Self::Error> {
        if value.is_empty() {
            return Err(anyhow!("input is empty"));
        }

        let mut reader = Cursor::new(value);

        let message_type = reader.read_u8()?;
        match message_type {
            message_type if message_type == PeerMessageType::Ping.as_u8() => {
                Ok(PeerMessage::Ping {
                    peers: read_peers(&mut reader)?,
                })
            }
            message_type if message_type == PeerMessageType::PingReq.as_u8() => {
                Ok(PeerMessage::PingReq {
                    target_peer: read_socket_addr(&mut reader)?,
                })
            }
            message_type if message_type == PeerMessageType::Ack.as_u8() => Ok(PeerMessage::Ack {
                peers: read_peers(&mut reader)?,
            }),
            message_type => {
                unreachable!("unhandled message type. message_type={message_type}")
            }
        }
    }
}

impl Memberlist {
    pub async fn new(config: Config) -> Result<MemberlistHandler> {
        assert!(
            config.join_peers.len() > 0,
            "at least one peer is required so this member can contact it to join the cluster"
        );
        assert!(config.mailbox_buffer_size > 0, "must be greater than 0");

        let (sender, receiver) = tokio::sync::mpsc::channel(config.mailbox_buffer_size);

        let memberlist = Self {
            udp_socket: UdpSocket::bind(&config.socket_addr)
                .await
                .context("binding local udp socket")?,
            config,
            next_peer_index: 0,
            peers: Vec::new(),
            receiver,
        };

        tokio::spawn(memberlist.control_loop());
        Ok(MemberlistHandler { sender })
    }

    fn set_peer_status_and_notify(&mut self, peer_index: usize, status: PeerStatus) {
        self.peers[peer_index].status = status;
        // TODO: notify
    }

    fn create_if_not_exists_and_return(&mut self, peer_addr: SocketAddr) -> usize {
        match self.peers.iter().position(|p| p.addr == peer_addr) {
            None => {
                self.peers.push(Peer {
                    addr: peer_addr,
                    status: PeerStatus::Alive,
                    incarnation_number: 0,
                });
                self.peers.len() - 1
                // TODO: notify new peer? probably not, let the peer notify
            }
            Some(i) => {
                self.set_peer_status_and_notify(i, PeerStatus::Alive);
                i
            }
        }
    }

    fn advance_peer_index(&mut self) -> usize {
        let reached_last_peer = self.next_peer_index == self.peers.len() - 1;

        let peer_index = self.next_peer_index;

        self.next_peer_index = self.next_peer_index + 1 % self.peers.len();

        if reached_last_peer {
            self.peers.shuffle(&mut rand::thread_rng());
        }

        assert!(self.next_peer_index < self.peers.len());

        peer_index
    }

    fn select_peers_for_dissemination(&self, target_peer_addr: SocketAddr) -> Vec<Peer> {
        // Get `max_number_peers_for_dissemination` with the lowest dissemination count.
        let mut heap = BinaryHeap::with_capacity(self.config.max_number_peers_for_dissemination);

        for peer in self.peers.iter() {
            heap.push(OrderByDisseminationCount(peer));

            if heap.len() > self.config.max_number_peers_for_dissemination {
                let _ = heap.pop();
            }
        }

        heap.into_iter().map(|v| v.0).cloned().collect()
    }

    /// The main control loop of this member.
    /// Every operation that will be performed by the member is handled here.
    async fn control_loop(mut self) {
        let mut detect_failure_interval =
            tokio::time::interval(self.config.failure_detection_attempt_interval);

        loop {
            select! {
                _ = detect_failure_interval.tick() => {
                    let target_peer_index = self.advance_peer_index();

                    let target_peer = &self.peers[target_peer_index];

                    let peer_status = select! {
                        result = self.ping(target_peer.addr, self.select_peers_for_dissemination(target_peer.addr)) => {
                            // TODO: let othe peers know what this peer thinks.
                            if result.is_ok() {
                                PeerStatus::Alive
                            } else {
                                PeerStatus::Suspected
                            }
                        },
                        peer_status = async {
                            tokio::time::sleep(self.config.peer_ping_request_timeout).await;

                            // The number of peers that we have to choose from.
                            // We may not have the number of peers requested in by `max_number_peers_for_indirect_probe`.
                            // Subtract 1 from `peers.len()` to take the target peer into account.
                            let num_peers = std::cmp::min(
                                self.peers.len().saturating_sub(1),
                                self.config.max_number_peers_for_indirect_probe,
                            );

                            let (sender, receiver) = tokio::sync::mpsc::channel(num_peers);

                            // Ask other peers to check if peer is alive.
                            tokio::spawn(futures::future::join_all(
                                self.random_peers_for_ping_req(target_peer, num_peers)
                                    .into_iter()
                                    .map(|peer| async {
                                        let response = self.ping_req(peer.addr, target_peer.addr).await;
                                        let _ = sender.send(response).await;
                                    })
                            ));


                            if let Some(ack_message) = receiver.recv().await {
                                // TODO: handle piggy backed peers.
                                   PeerStatus::Alive
                            } else {
                                    PeerStatus::Suspected
                            }
                        } => { peer_status }
                    };

                    self.set_peer_status_and_notify(target_peer_index, peer_status);

                    // TODO: tell the other peers about the target peer status.
                },
                result = async {
                    let mut buffer = [0_u8;128];
                    self.udp_socket.recv_from(&mut buffer).await.map(|(_bytes_read, addr)| (buffer, addr))
                } => {
                    let Ok((buffer, client_addr)) = result else {
                        error!("error reading peer messages from udp socket");
                        continue;
                    };

                    let message = match PeerMessage::try_from(buffer.as_ref()) {
                        Err(err) => {
                            error!(?err, "error parsing message from peer");
                            continue;
                        },
                        Ok(v) => v
                    };

                    match message {
                        PeerMessage::Ping { peers } => {
                            let peer_index = self.create_if_not_exists_and_return(client_addr);
                            // if let Err(err) = self.pong(peer.addr).await {
                            //     error!(?err, "unable to send ping response");
                            // }
                        },
                        PeerMessage::PingReq { target_peer } => {
                            match self.handle_ping_request(target_peer).await {
                                Err(err) => {
                                    info!(?err, "pingreq: peer is not reachable from this node");
                                },
                                Ok(()) => {
                                    let buffer = [PeerMessageType::PingReqUnreachable.as_u8()];
                                    if let Err(err) = self.udp_socket.send_to(&buffer, client_addr).await {
                                        error!(?err, "error sending pingreq response to client");
                                    }
                                }
                            }
                        },
                        PeerMessage::Ack { peers } => {
                            todo!()
                        }
                    }
                },
                message = self.receiver.recv() => {
                    let message = match message {
                        None => {
                            info!("sender channel closed, member list handler has been dropped, exiting control loop");
                            return;
                        },
                        Some(v) => v
                    };
                }
            }
        }
    }

    fn random_peers_for_ping_req(&self, target_peer: &Peer, num_peers: usize) -> HashSet<&Peer> {
        let mut peers = HashSet::with_capacity(num_peers);

        for _ in 0..num_peers {
            loop {
                let peer_index = rand::thread_rng().gen_range(0..self.peers.len());
                let peer = &self.peers[peer_index];

                // We need a peer thats not the target peer or ourselves.
                if peer != target_peer && !peers.contains(&peer) {
                    peers.insert(peer);
                }
            }
        }

        peers
    }

    async fn pong(&self, peer_addr: SocketAddr) -> Result<()> {
        todo!()
    }

    async fn ping(&self, peer_addr: SocketAddr, piggyback_peers: Vec<Peer>) -> Result<AckMessage> {
        let socket = UdpSocket::from_std({
            let s = socket2::Socket::new(
                socket2::Domain::IPV4,
                socket2::Type::DGRAM,
                Some(socket2::Protocol::UDP),
            )?;

            s.set_nonblocking(true)
                .context("setting socket to non blocking")?;

            s.set_reuse_address(true)
                .context("setting reuse_address to true")?;

            // TODO: parsing all the time is wasteful.
            let addr = self
                .config
                .socket_addr
                .parse::<SocketAddr>()
                .context("parsing socket addr")?;

            s.bind(&addr.into()).context("binding local socket")?;

            s.into()
        })?;

        socket
            .connect(peer_addr)
            .await
            .context("connecting to peer")?;

        let message = PeerMessage::Ping(PingMessage {
            piggybacked_peers: piggyback_peers,
        });

        let buffer: Vec<u8> = Vec::try_from(&message)?;

        socket
            .send(&buffer)
            .await
            .context("sending ping message to peer")?;

        let mut buffer =
            vec![0_u8; self.config.max_number_peers_for_dissemination * PEER_SIZE_IN_BYTES];

        loop {
            let _bytes_read = socket
                .recv(&mut buffer)
                .await
                .context("trying to receive ping request response")?;

            if buffer.is_empty() {
                return Err(anyhow!("tried to read ping response but got empty buffer"));
            }

            if buffer[0] == PeerMessageType::Ack.as_u8() {
                return Ok(AckMessage::try_from(buffer.as_ref())?);
            }
        }
    }

    async fn ping_req(&self, peer_addr: SocketAddr, target_peer_addr: SocketAddr) -> Result<bool> {
        let socket = UdpSocket::from_std({
            let s = socket2::Socket::new(
                socket2::Domain::IPV4,
                socket2::Type::DGRAM,
                Some(socket2::Protocol::UDP),
            )?;

            s.set_nonblocking(true)
                .context("setting socket to non blocking")?;

            s.set_reuse_address(true)
                .context("setting reuse_address to true")?;

            // TODO: parsing all the time is wasteful.
            let addr = self
                .config
                .socket_addr
                .parse::<SocketAddr>()
                .context("parsing socket addr")?;

            s.bind(&addr.into()).context("binding local socket")?;

            s.into()
        })?;

        socket
            .connect(peer_addr)
            .await
            .context("connecting to peer")?;

        let mut buffer = Vec::new();

        buffer
            .write_u8(PeerMessageType::PingReq.as_u8())
            .context("writing message type")?;

        match target_peer_addr.ip() {
            IpAddr::V4(ip) => {
                buffer.write_u8(PeerAddressType::Ipv4.as_u8())?;
                buffer
                    .write_all(&ip.octets())
                    .context("writing target peer to buffer")?;
            }
            IpAddr::V6(ip) => {
                buffer.write_u8(PeerAddressType::Ipv6.as_u8())?;
                buffer
                    .write_all(&ip.octets())
                    .context("writing target peer to buffer")?;
            }
        }

        socket
            .send(&buffer)
            .await
            .context("sending pingreq message to peer")?;

        let mut buffer = [0_u8; 1];
        assert_eq!(1, std::mem::size_of_val(&PeerMessageType::Pong.as_u8()));

        loop {
            let bytes_read = socket
                .recv(&mut buffer)
                .await
                .context("trying to receive pingreq request response")?;

            if bytes_read == buffer.len() {
                if buffer[0] == PeerMessageType::PingReqAlive.as_u8() {
                    return Ok(true);
                } else if buffer[0] == PeerMessageType::PingReqUnreachable.as_u8() {
                    return Ok(false);
                }
            }
        }
    }

    async fn handle_ping_request(&self, target_peer_addr: SocketAddr) -> Result<()> {
        let reachable = self
            .ping(target_peer_addr)
            .await
            .context("pinging peer for ping req")?;
        Ok(())
    }

    fn handle_alive_message(&mut self, peer_addr: SocketAddr, incarnation_number: u32) {
        let peer_index = self.create_if_not_exists_and_return(peer_addr);
        let peer = &mut self.peers[peer_index];

        // Received an old message, it can be ignored.
        if peer.incarnation_number > incarnation_number {
            return;
        }

        peer.incarnation_number = incarnation_number;
        peer.status = PeerStatus::Alive;
    }

    fn handle_suspected_message(&mut self, peer_addr: SocketAddr, incarnation_number: u32) {
        let peer_index = self.create_if_not_exists_and_return(peer_addr);
        let peer = &mut self.peers[peer_index];

        // Received an old message, it can be ignored.
        if peer.incarnation_number > incarnation_number {
            return;
        }

        peer.incarnation_number = incarnation_number;
        peer.status = PeerStatus::Suspected;
    }

    fn handle_dead_message(&mut self, peer_addr: SocketAddr, incarnation_number: u32) {
        let peer_index = self.create_if_not_exists_and_return(peer_addr);
        let peer = &mut self.peers[peer_index];

        // Received an old message, it can be ignored.
        if peer.incarnation_number > incarnation_number {
            return;
        }

        peer.incarnation_number = incarnation_number;
        peer.status = PeerStatus::Dead;
    }

    fn multicast_peers(&self) -> Result<()> {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn message_to_bytes_and_bytes_to_message(m1: PeerMessage) {
            let buffer = Vec::try_from(&m1).unwrap();
            let m2 = PeerMessage::try_from(buffer.as_ref()).unwrap();
            assert_eq!(m1, m2);
        }
    }
}
