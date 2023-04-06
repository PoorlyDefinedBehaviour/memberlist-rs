use anyhow::{anyhow, Context, Result};
use byteorder::{ReadBytesExt, WriteBytesExt};
use rand::{seq::SliceRandom, Rng};
use std::{
    collections::{BinaryHeap, HashMap, HashSet},
    io::Cursor,
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
    sync::Arc,
    time::Duration,
};
use tokio::{
    net::UdpSocket,
    select,
    sync::mpsc::{Receiver, Sender},
};
use tracing::{error, warn};

const RECV_BUFFER_SIZE_IN_BYTES: usize = 2048;

trait SocketAddrExt {
    fn address_type(&self) -> PeerAddressType;
}

impl SocketAddrExt for SocketAddr {
    fn address_type(&self) -> PeerAddressType {
        match self.ip() {
            IpAddr::V4(_) => PeerAddressType::Ipv4,
            IpAddr::V6(_) => PeerAddressType::Ipv6,
        }
    }
}

trait IpAddrExt {
    fn ipv4_octets(&self) -> [u8; 4];
    fn ipv6_octets(&self) -> [u8; 16];
}

impl IpAddrExt for IpAddr {
    fn ipv4_octets(&self) -> [u8; 4] {
        match self {
            IpAddr::V4(ip) => ip.octets(),
            IpAddr::V6(ip) => unreachable!(),
        }
    }
    fn ipv6_octets(&self) -> [u8; 16] {
        match self {
            IpAddr::V4(ip) => unreachable!(),
            IpAddr::V6(ip) => ip.octets(),
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
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
    pub socket_addr: SocketAddr,
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
            socket_addr: "0.0.0.0:9157".parse().unwrap(),
        }
    }
}

#[derive(Debug)]
struct Memberlist {
    /// This member's config.
    config: Config,
    /// The index of the next peer in `peers` to check for failure.
    next_peer_index: usize,
    /// List of peers this member knows about.
    peers: Vec<Peer>,
    /// Channel to receive actions to perform.
    receiver: Receiver<Message>,
    /// UDP socket used to communicate with other peers.
    udp_socket: Arc<UdpSocket>,
    /// The current generation of this peer.
    incarnation_number: u32,
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
}

impl PeerMessageType {
    const fn as_u8(&self) -> u8 {
        match self {
            PeerMessageType::Ping => 1,
            PeerMessageType::Ack => 2,
            PeerMessageType::PingReq => 3,
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
#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
enum PeerMessage {
    Ping(PingMessage),
    PingReq(PingReqMessage),
    Ack(AckMessage),
}

impl PeerMessage {
    fn try_from_buffer(buffer: &[u8], min_dissemination_count: u32) -> Result<Self> {
        if buffer.is_empty() {
            return Err(anyhow!("input is empty"));
        }

        let mut reader = Cursor::new(buffer);

        let message_type = reader.read_u8()?;
        match message_type {
            message_type if message_type == PeerMessageType::Ping.as_u8() => {
                Ok(PeerMessage::Ping(PingMessage {
                    piggybacked_peers: read_peers(&mut reader, min_dissemination_count)?,
                }))
            }
            message_type if message_type == PeerMessageType::PingReq.as_u8() => {
                Ok(PeerMessage::PingReq(PingReqMessage {
                    piggybacked_peers: read_peers(&mut reader, min_dissemination_count)?,
                    target_peer_addr: read_socket_addr(&mut reader)?,
                }))
            }
            message_type if message_type == PeerMessageType::Ack.as_u8() => {
                Ok(PeerMessage::Ack(AckMessage {
                    piggybacked_peers: read_peers(&mut reader, min_dissemination_count)?,
                }))
            }
            message_type => {
                unreachable!("unhandled message type. message_type={message_type}")
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
struct AckMessage {
    piggybacked_peers: Vec<Peer>,
}

impl AckMessage {
    fn try_from_buffer(buffer: &[u8], min_dissemination_count: u32) -> Result<Self> {
        Ok(AckMessage {
            piggybacked_peers: read_peers(&mut Cursor::new(buffer), min_dissemination_count)?,
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
struct PingReqMessage {
    piggybacked_peers: Vec<Peer>,
    target_peer_addr: SocketAddr,
}

impl PingReqMessage {
    fn try_from_buffer(buffer: &[u8], min_dissemination_count: u32) -> Result<Self> {
        let mut reader = Cursor::new(buffer);
        Ok(PingReqMessage {
            piggybacked_peers: read_peers(&mut reader, min_dissemination_count)?,
            target_peer_addr: read_socket_addr(&mut reader)?,
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
struct PingMessage {
    piggybacked_peers: Vec<Peer>,
}

impl PingMessage {
    fn try_from_buffer(buffer: &[u8], min_dissemination_count: u32) -> Result<Self> {
        Ok(PingMessage {
            piggybacked_peers: read_peers(&mut Cursor::new(buffer), min_dissemination_count)?,
        })
    }
}

fn write_socket_addr(buffer: &mut impl std::io::Write, addr: SocketAddr) -> std::io::Result<()> {
    buffer.write_u8(addr.address_type().as_u8())?;
    if addr.is_ipv4() {
        buffer.write_all(&addr.ip().ipv4_octets())?;
    } else {
        buffer.write_all(&addr.ip().ipv6_octets())?;
    }

    buffer.write_u16::<byteorder::BigEndian>(addr.port())?;
    Ok(())
}

fn read_socket_addr(reader: &mut impl std::io::Read) -> std::io::Result<SocketAddr> {
    let address_type = reader.read_u8()?;

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

fn write_peers(buffer: &mut impl std::io::Write, peers: &[Peer]) -> std::io::Result<()> {
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

fn read_peers(reader: &mut impl std::io::Read, min_dissemination_count: u32) -> Result<Vec<Peer>> {
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
            dissemination_count: min_dissemination_count,
        });
    }

    Ok(peers)
}

impl TryFrom<&PeerMessage> for Vec<u8> {
    type Error = anyhow::Error;

    fn try_from(value: &PeerMessage) -> std::result::Result<Self, Self::Error> {
        let mut buffer = Vec::new();

        match value {
            PeerMessage::Ping(PingMessage { piggybacked_peers }) => {
                buffer.write_u8(PeerMessageType::Ping.as_u8())?;
                write_peers(&mut buffer, piggybacked_peers)?;
            }
            PeerMessage::Ack(AckMessage { piggybacked_peers }) => {
                buffer.write_u8(PeerMessageType::Ack.as_u8())?;
                write_peers(&mut buffer, piggybacked_peers)?;
            }
            PeerMessage::PingReq(PingReqMessage {
                piggybacked_peers,
                target_peer_addr,
            }) => {
                buffer.write_u8(PeerMessageType::PingReq.as_u8())?;
                write_peers(&mut buffer, piggybacked_peers)?;
                write_socket_addr(&mut buffer, *target_peer_addr)?;
            }
        }

        Ok(buffer)
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
            udp_socket: Arc::new(
                UdpSocket::bind(&config.socket_addr)
                    .await
                    .context("binding local udp socket")?,
            ),
            config,
            next_peer_index: 0,
            peers: Vec::new(),
            receiver,
            incarnation_number: 0,
        };

        tokio::spawn(memberlist.control_loop());
        Ok(MemberlistHandler { sender })
    }

    /// Returns a peer representing this process.
    fn self_peer(&self) -> Peer {
        Peer {
            addr: self.config.socket_addr,
            status: PeerStatus::Alive,
            incarnation_number: self.incarnation_number,
            dissemination_count: 0,
        }
    }

    fn set_peer_status_and_notify(&mut self, peer_index: usize, status: PeerStatus) {
        self.peers[peer_index].status = status;
        // TODO: notify
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

    // TODO: include the peer itself in the list of peers.
    // Get `max_number_peers_for_dissemination` with the lowest dissemination count.
    fn select_peers_for_dissemination(&self, target_peer_addr: SocketAddr) -> Vec<Peer> {
        let mut heap = BinaryHeap::with_capacity(self.config.max_number_peers_for_dissemination);

        for peer in self.peers.iter() {
            // Do not send the peer to itself.
            if peer.addr == target_peer_addr {
                continue;
            }

            heap.push(OrderByDisseminationCount(peer));

            if heap.len() > self.config.max_number_peers_for_dissemination {
                let _ = heap.pop();
            }
        }

        let mut peers: Vec<_> = heap.into_iter().map(|v| v.0).cloned().collect();
        peers.push(self.self_peer());
        peers
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
                        let target_peer_addr = self.peers[target_peer_index].addr;
                        self.ping(target_peer_index, self.select_peers_for_dissemination(target_peer_addr)).await;
                    },
                    result = async {
                        let mut buffer = [0_u8; RECV_BUFFER_SIZE_IN_BYTES];
                        self.udp_socket.recv_from(&mut buffer).await.map(|(_bytes_read, addr)| (buffer, addr))
                    } => {
                        let Ok((buffer, client_addr)) = result else {
                            error!("error reading peer messages from udp socket");
                            continue;
                        };

                        let message = match PeerMessage::try_from_buffer(buffer.as_ref(), min_dissemination_count(&self.peers)) {
                            Err(err) => {
                                error!(?err, "error parsing message from peer");
                                continue;
                            },
                            Ok(v) => v
                        };

                        if let Err(err) = self.handle_peer_message(client_addr, message).await {
                            error!(?err, "error handling peer message");
                        }
                    // },
                    // message = self.receiver.recv() => {
                    //     let message = match message {
                    //         None => {
                    //             info!("sender channel closed, member list handler has been dropped, exiting control loop");
                    //             return;
                    //         },
                    //         Some(v) => v
                    //     };
                    // }
                }
            }
        }
    }

    // TODO: include the peer itself in the message.
    async fn disseminate_ping(
        &mut self,
        target_peer_index: usize,
        piggybacking_peers: Vec<Peer>,
    ) -> HashMap<SocketAddr, Peer> {
        let target_peer_addr = self.peers[target_peer_index].addr;

        let min_dissemination_count = min_dissemination_count(&self.peers);

        // The number of peers that we have to choose from.
        // We may not have the number of peers requested in by `max_number_peers_for_indirect_probe`.
        // Subtract 1 from `peers.len()` to take the target peer into account.
        let num_peers_for_indirect_probe = std::cmp::min(
            self.peers.len().saturating_sub(1),
            self.config.max_number_peers_for_indirect_probe,
        );

        let peers_for_ping_req =
            self.random_peers_for_ping_req(target_peer_addr, num_peers_for_indirect_probe);

        let peer_ping_request_timeout = self.config.peer_ping_request_timeout;

        let udp_socket = Arc::clone(&self.udp_socket);

        let (sender, mut receiver) = tokio::sync::mpsc::channel(1);

        let ping_message = PeerMessage::Ping(PingMessage {
            piggybacked_peers: piggybacking_peers.clone(),
        });
        let ping_req_message = PeerMessage::PingReq(PingReqMessage {
            piggybacked_peers: piggybacking_peers,
            target_peer_addr,
        });

        let send_message_input = SendMessageInput {
            peer_socket_addr: self.config.socket_addr,
            target_peer_addr: target_peer_addr,
            max_number_peers_for_dissemination: self.config.max_number_peers_for_dissemination,
            min_dissemination_count,
        };

        let ping_handle = {
            let sender = sender.clone();
            let udp_socket = Arc::clone(&udp_socket);
            let send_message_input = send_message_input.clone();
            tokio::spawn(async move {
                if let Ok(ack_message) =
                    send_message(&udp_socket, send_message_input, ping_message).await
                {
                    if let Err(err) = sender.send(ack_message).await {
                        error!(?err, "unable to send ping ack message to channel");
                    }
                }
            })
        };

        let ping_req_handle = tokio::spawn(async move {
            tokio::time::sleep(peer_ping_request_timeout).await;

            futures::future::join_all(peers_for_ping_req.into_iter().map(|peer_addr| {
                let udp_socket = Arc::clone(&udp_socket);
                let send_message_input = SendMessageInput {
                    peer_socket_addr: peer_addr,
                    ..send_message_input.clone()
                };
                let ping_req_message = ping_req_message.clone();
                let sender = sender.clone();

                async move {
                    if let Ok(ack_message) =
                        send_message(&udp_socket, send_message_input, ping_req_message.clone())
                            .await
                    {
                        if let Err(err) = sender.send(ack_message).await {
                            error!(?err, "unable to send ack message from pingreq to channel");
                        }
                    }
                }
            }))
        });

        // Different peers may send different views of the same peers.
        // Keep only the latest information about each peer.
        let mut piggybacked_peers = HashMap::new();

        while let Some(ack_message) = receiver.recv().await {
            for piggybacked_peer in ack_message.piggybacked_peers {
                let new_status = piggybacked_peer.status;
                let new_incarnation_number = piggybacked_peer.incarnation_number;

                let entry = piggybacked_peers
                    .entry(piggybacked_peer.addr)
                    .or_insert(piggybacked_peer);

                if entry.incarnation_number < new_incarnation_number {
                    entry.status = new_status;
                    entry.incarnation_number = new_incarnation_number;
                }
            }
        }

        ping_handle.abort();
        ping_req_handle.abort();

        return piggybacked_peers;
    }

    async fn ping(&mut self, target_peer_index: usize, piggybacking_peers: Vec<Peer>) {
        let peers_received_in_acks = self
            .disseminate_ping(target_peer_index, piggybacking_peers)
            .await;

        // If the peer target peer is unreachable.
        if peers_received_in_acks.is_empty() {
            self.suspect(target_peer_index).await;
            return;
        }

        let this_peer_is_suspected = peers_received_in_acks
            .into_iter()
            .map(|(_, peer)| self.peer_received(peer))
            .any(|suspected| suspected);

        if this_peer_is_suspected {
            todo!();
        }
    }

    async fn confirm_alive(&self) {
        todo!()
    }

    async fn suspect(&mut self, target_peer_index: usize) {
        let piggybacking_peers =
            self.select_peers_for_dissemination(self.peers[target_peer_index].addr);

        let target_peer = &mut self.peers[target_peer_index];
        target_peer.status = PeerStatus::Suspected;

        let peers_received_in_acks = self
            .disseminate_ping(target_peer_index, piggybacking_peers)
            .await;

        let this_peer_is_suspected = peers_received_in_acks
            .into_iter()
            .map(|(_, peer)| self.peer_received(peer))
            .any(|suspected| suspected);

        // Some peer thinks this peer may be dead, let them know it is alive.
        if this_peer_is_suspected {
            self.confirm_alive().await;
            // // TODO: should be a confirm message.
            // let target_peer_index = self.advance_peer_index();
            // let piggybacking_peers =
            //     self.select_peers_for_dissemination(self.peers[target_peer_index].addr);
            // let target_peer_addr = self.peers[target_peer_index].addr;
            // // Increase the incarnation number to override older suspect and dead messages.
            // self.incarnation_number += 1;
            // for (_peer_addr, peer) in self.disseminate_ping(piggybacking_peers).await {
            //     // Ignore suspections of the peer itself to avoid sending alive messages in a loop.
            //     let _ = self.peer_received(peer);
            // }
        }
    }

    fn random_peers_for_ping_req(
        &self,
        target_peer_addr: SocketAddr,
        num_peers: usize,
    ) -> HashSet<SocketAddr> {
        let mut peers = HashSet::with_capacity(num_peers);

        for _ in 0..num_peers {
            loop {
                let peer_index = rand::thread_rng().gen_range(0..self.peers.len());
                let peer = &self.peers[peer_index];

                // We need a peer thats not the target peer or ourselves.
                if peer.addr != target_peer_addr && !peers.contains(&peer.addr) {
                    peers.insert(peer.addr);
                }
            }
        }

        peers
    }

    async fn handle_peer_message(
        &mut self,
        client_addr: SocketAddr,
        message: PeerMessage,
    ) -> Result<()> {
        match message {
            // TODO: add message number to each message(may be useful to receive responses)
            PeerMessage::Ping(PingMessage { piggybacked_peers }) => {
                ack(client_addr)
                    .await
                    .context("unable to send ping response")?;
            }
            PeerMessage::PingReq(PingReqMessage {
                piggybacked_peers,
                target_peer_addr,
            }) => {
                let _ = self.peers_received(piggybacked_peers);

                let input = PingInput {
                    peer_socket_addr: self.config.socket_addr,
                    target_peer_addr: target_peer_addr,
                    piggybacking_peers: self.select_peers_for_dissemination(target_peer_addr),
                    max_number_peers_for_dissemination: self
                        .config
                        .max_number_peers_for_dissemination,
                    min_dissemination_count: min_dissemination_count(&self.peers),
                };
                if let Ok(ack_message) = ping(&self.udp_socket, input).await {
                    self.peers_received(ack_message.piggybacked_peers);

                    ack(client_addr)
                        .await
                        .context("unable to send pingreq response")?;
                }
            }
            PeerMessage::Ack(AckMessage { piggybacked_peers }) => {
                todo!()
            }
        }

        Ok(())
    }

    fn get_peer_by_socket_addr(&self, addr: SocketAddr) -> Option<&Peer> {
        self.peers.iter().find(|peer| peer.addr == addr)
    }

    // TODO: Bad time complexity. Fix it. (ordered set?)
    fn peer_received(&mut self, peer: Peer) -> bool {
        let min_dissemination_count = min_dissemination_count(&self.peers);

        if peer.addr == self.config.socket_addr {
            // Someone thinks this peer may be dead,
            // the peer needs to let the other peers know it is alive.
            let peer_is_suspected = peer.status == PeerStatus::Suspected;

            // TODO: handle dead case?

            return peer_is_suspected;
        }

        match self.peers.iter().position(|p| p.addr == peer.addr) {
            // We don't know about this peer yet.
            None => {
                if peer.status == PeerStatus::Dead {
                    return false;
                }
                self.peers.push(Peer {
                    dissemination_count: min_dissemination_count,
                    ..peer
                });
            }
            // We already know about this peer, the info we have about it may be outdated.
            Some(known_peer_index) => {
                let known_peer = &mut self.peers[known_peer_index];
                // This is and old message that can be ignored.
                if peer.incarnation_number < known_peer.incarnation_number {
                    return false;
                }
                match peer.status {
                    PeerStatus::Dead => {
                        self.peers.remove(known_peer_index);
                    }
                    PeerStatus::Alive | PeerStatus::Suspected => {
                        known_peer.status = peer.status;
                        known_peer.incarnation_number = peer.incarnation_number;
                    }
                }
            }
        }

        false
    }

    fn peers_received(&mut self, peers: Vec<Peer>) -> bool {
        let mut peer_is_suspected = false;

        for peer in peers.into_iter() {
            peer_is_suspected = self.peer_received(peer);
        }

        peer_is_suspected
    }
}

/// Returns the dissemination count of the peer with the lowest dissemination count.
fn min_dissemination_count(peers: &[Peer]) -> u32 {
    peers
        .iter()
        .min_by_key(|peer| peer.dissemination_count)
        .map(|peer| peer.dissemination_count)
        .unwrap_or(0)
}

#[derive(Clone)]
struct PingInput {
    peer_socket_addr: SocketAddr,
    target_peer_addr: SocketAddr,
    piggybacking_peers: Vec<Peer>,
    max_number_peers_for_dissemination: usize,
    min_dissemination_count: u32,
}

#[derive(Clone)]
struct SendMessageInput {
    peer_socket_addr: SocketAddr,
    target_peer_addr: SocketAddr,
    max_number_peers_for_dissemination: usize,
    min_dissemination_count: u32,
}

async fn send_message(
    udp_socket: &UdpSocket,
    input: SendMessageInput,
    message: PeerMessage,
) -> Result<AckMessage> {
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

        s.bind(&input.peer_socket_addr.into())
            .context("binding local socket")?;

        s.into()
    })?;

    socket
        .connect(input.target_peer_addr)
        .await
        .context("connecting to peer")?;

    let buffer: Vec<u8> = Vec::try_from(&message)?;

    socket
        .send(&buffer)
        .await
        .context("sending message to peer")?;

    let mut buffer = vec![0_u8; RECV_BUFFER_SIZE_IN_BYTES];

    loop {
        let _bytes_read = socket
            .recv(&mut buffer)
            .await
            .context("trying to message response")?;

        if buffer.is_empty() {
            return Err(anyhow!(
                "tried to read message response but got empty buffer"
            ));
        }

        if buffer[0] == PeerMessageType::Ack.as_u8() {
            return Ok(AckMessage::try_from_buffer(
                buffer.as_ref(),
                input.min_dissemination_count,
            )?);
        }
    }
}

async fn ping(udp_socket: &UdpSocket, input: PingInput) -> Result<AckMessage> {
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

        s.bind(&input.peer_socket_addr.into())
            .context("binding local socket")?;

        s.into()
    })?;

    socket
        .connect(input.target_peer_addr)
        .await
        .context("connecting to peer")?;

    let message = PeerMessage::Ping(PingMessage {
        piggybacked_peers: input.piggybacking_peers,
    });

    let buffer: Vec<u8> = Vec::try_from(&message)?;

    socket
        .send(&buffer)
        .await
        .context("sending ping message to peer")?;

    let mut buffer = vec![0_u8; RECV_BUFFER_SIZE_IN_BYTES];

    loop {
        let _bytes_read = socket
            .recv(&mut buffer)
            .await
            .context("trying to receive ping request response")?;

        if buffer.is_empty() {
            return Err(anyhow!("tried to read ping response but got empty buffer"));
        }

        if buffer[0] == PeerMessageType::Ack.as_u8() {
            return Ok(AckMessage::try_from_buffer(
                buffer.as_ref(),
                input.min_dissemination_count,
            )?);
        }
    }
}

async fn ack(peer_addr: SocketAddr) -> Result<()> {
    todo!()
}

#[derive(Clone)]
struct PingReqInput {
    peer_socket_addr: SocketAddr,
    target_peer_addr: SocketAddr,
    piggybacking_peers: Vec<Peer>,
    max_number_peers_for_dissemination: usize,
    min_dissemination_count: u32,
}

async fn ping_req(udp_socket: &UdpSocket, input: PingReqInput) -> Result<AckMessage> {
    todo!()
    // let socket = UdpSocket::from_std({
    //     let s = socket2::Socket::new(
    //         socket2::Domain::IPV4,
    //         socket2::Type::DGRAM,
    //         Some(socket2::Protocol::UDP),
    //     )?;

    //     s.set_nonblocking(true)
    //         .context("setting socket to non blocking")?;

    //     s.set_reuse_address(true)
    //         .context("setting reuse_address to true")?;

    //     // TODO: parsing all the time is wasteful.
    //     let addr = self
    //         .config
    //         .socket_addr
    //         .parse::<SocketAddr>()
    //         .context("parsing socket addr")?;

    //     s.bind(&addr.into()).context("binding local socket")?;

    //     s.into()
    // })?;

    // socket
    //     .connect(peer_addr)
    //     .await
    //     .context("connecting to peer")?;

    // let mut buffer = Vec::new();

    // buffer
    //     .write_u8(PeerMessageType::PingReq.as_u8())
    //     .context("writing message type")?;

    // match target_peer_addr.ip() {
    //     IpAddr::V4(ip) => {
    //         buffer.write_u8(PeerAddressType::Ipv4.as_u8())?;
    //         buffer
    //             .write_all(&ip.octets())
    //             .context("writing target peer to buffer")?;
    //     }
    //     IpAddr::V6(ip) => {
    //         buffer.write_u8(PeerAddressType::Ipv6.as_u8())?;
    //         buffer
    //             .write_all(&ip.octets())
    //             .context("writing target peer to buffer")?;
    //     }
    // }

    // socket
    //     .send(&buffer)
    //     .await
    //     .context("sending pingreq message to peer")?;

    // let mut buffer = [0_u8; 1];
    // assert_eq!(1, std::mem::size_of_val(&PeerMessageType::Ack.as_u8()));

    // loop {
    //     let bytes_read = socket
    //         .recv(&mut buffer)
    //         .await
    //         .context("trying to receive pingreq request response")?;

    //     todo!()
    // }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn message_peers(m1: &PeerMessage) -> Vec<Peer> {
        match m1 {
            PeerMessage::Ping(PingMessage { piggybacked_peers }) => piggybacked_peers.clone(),
            PeerMessage::PingReq(PingReqMessage { .. }) => Vec::new(),
            PeerMessage::Ack(AckMessage { piggybacked_peers }) => piggybacked_peers.clone(),
        }
    }

    proptest! {
        #[test]
        fn message_to_bytes_and_bytes_to_message(m1: PeerMessage) {
            let buffer: Vec<u8> = Vec::try_from(&m1).unwrap();
            let min_dissemination_count = min_dissemination_count(&message_peers(&m1));
            let m2 = PeerMessage::try_from_buffer(buffer.as_ref(),min_dissemination_count).unwrap();
            assert_eq!(m1, m2);
        }
    }
}
