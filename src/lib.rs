use anyhow::{Context, Result};

use dissemination::DisseminatePingInput;
use rand::{seq::SliceRandom, Rng};
use std::{
    collections::{BinaryHeap, HashSet},
    net::SocketAddr,
    sync::Arc,
    time::Duration,
};
use tokio::{
    net::UdpSocket,
    select,
    sync::mpsc::{Receiver, Sender},
};
use tracing::{debug, error, info};

use crate::dissemination::RECV_BUFFER_SIZE_IN_BYTES;
mod dissemination;
mod serialization;

#[derive(Debug, PartialEq)]
pub enum Notification {
    /// A new peer has joined the cluster.
    Join(SocketAddr),
    /// The peer is considered dead.
    Leave(SocketAddr),
}

pub async fn join(config: Config) -> Result<Receiver<Notification>> {
    Memberlist::start(config)
        .await
        .context("starting memberlist actor")
}

/// A peer state from this peer's view.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
struct Peer {
    /// The peer address.
    addr: SocketAddr,
    /// The peer status in this member view.
    status: PeerStatus,
    /// Whenever the peer is declared as alive, its incarnation number is incremented.
    incarnation_number: u32,
    /// The amount of times this peer has been piggybacked on messages
    /// sent to other peers by this peer.
    dissemination_count: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
struct PiggybackingPeer {
    /// The peer address.
    addr: SocketAddr,
    /// The peer status in this member view.
    status: PeerStatus,
    /// Whenever the peer is declared as alive, its incarnation number is incremented.
    incarnation_number: u32,
}

impl From<&Peer> for PiggybackingPeer {
    fn from(peer: &Peer) -> Self {
        Self {
            addr: peer.addr,
            status: peer.status,
            incarnation_number: peer.incarnation_number,
        }
    }
}

impl From<(PiggybackingPeer, u32)> for Peer {
    fn from((peer, min_dissemination_count): (PiggybackingPeer, u32)) -> Self {
        Self {
            addr: peer.addr,
            status: peer.status,
            incarnation_number: peer.incarnation_number,
            dissemination_count: min_dissemination_count,
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

#[derive(Debug)]
pub struct Config {
    /// Addresses of peers that this member will contact to join the cluster.
    pub join_peers: Vec<SocketAddr>,
    /// How many messages held in the notifications channel before blocking.
    pub mailbox_buffer_size: usize,
    /// The amount of time to wait for between failure detection attempts.
    pub failure_detection_attempt_interval: Duration,
    /// The amount of time this peer is willing to wait for failure detection
    /// to conclude a round of requests.
    /// Must be greater than `peer_ping_request_timeout_for_indirect_probe` and `peer_ping_req_request_timeout`.
    pub failure_detection_timeout: Duration,
    /// The amount of time to wait for a ping response to arrive before starting an indirect probe.
    pub peer_ping_request_timeout_for_indirect_probe: Duration,
    /// The amount of time a peer has to respond to a ping req request.
    pub peer_ping_req_request_timeout: Duration,
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
            failure_detection_timeout: Duration::from_secs(6),
            peer_ping_request_timeout_for_indirect_probe: Duration::from_secs(2),
            peer_ping_req_request_timeout: Duration::from_secs(2),
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
    /// Channel used to send messages containing peer updates.
    sender: Sender<Notification>,
    /// UDP socket used to communicate with other peers.
    udp_socket: UdpSocket,
    /// The current generation of this peer.
    incarnation_number: u32,
    /// True when another peer suspects this peer is dead.
    is_suspected: bool,
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

impl From<u8> for PeerMessageType {
    fn from(value: u8) -> Self {
        if value == PeerMessageType::Ping.as_u8() {
            return PeerMessageType::Ping;
        }
        if value == PeerMessageType::Ack.as_u8() {
            return PeerMessageType::Ack;
        }
        if value == PeerMessageType::PingReq.as_u8() {
            return PeerMessageType::PingReq;
        }

        unreachable!("unexpected peer message type. value={value}");
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

#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
struct AckMessage {
    piggybacking_peers: Vec<PiggybackingPeer>,
}

#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
struct PingReqMessage {
    piggybacking_peers: Vec<PiggybackingPeer>,
    target_peer_addr: SocketAddr,
}

#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
struct PingMessage {
    piggybacking_peers: Vec<PiggybackingPeer>,
}

impl Memberlist {
    async fn start(config: Config) -> Result<Receiver<Notification>> {
        assert!(
            config.join_peers.len() > 0,
            "at least one peer is required so this member can contact it to join the cluster"
        );
        assert!(config.mailbox_buffer_size > 0, "must be greater than 0");

        info!(?config, "starting peer");

        let (sender, receiver) = tokio::sync::mpsc::channel(config.mailbox_buffer_size);

        // Check that
        let udp_socket = dissemination::udp_socket_with_addr_reuse(config.socket_addr)?;

        let memberlist = Self {
            udp_socket,
            next_peer_index: 0,
            peers: config
                .join_peers
                .iter()
                .map(|peer_addr| Peer {
                    addr: *peer_addr,
                    status: PeerStatus::Alive,
                    // We don't know the incarnation number at this point.
                    incarnation_number: 0,
                    dissemination_count: 0,
                })
                .collect(),
            sender,
            incarnation_number: 0,
            is_suspected: false,
            config,
        };

        tokio::spawn(memberlist.control_loop());
        Ok(receiver)
    }

    /// Returns a peer representing this process.
    fn self_piggybacking_peer(&self) -> PiggybackingPeer {
        PiggybackingPeer {
            addr: self.config.socket_addr,
            status: PeerStatus::Alive,
            incarnation_number: self.incarnation_number,
        }
    }

    fn next_peer_index(&mut self) -> Option<usize> {
        if self.peers.is_empty() {
            return None;
        }

        let reached_last_peer = self.next_peer_index == self.peers.len() - 1;

        let peer_index = self.next_peer_index;

        self.next_peer_index = self.next_peer_index + 1 % self.peers.len();

        if reached_last_peer {
            self.peers.shuffle(&mut rand::thread_rng());
        }

        assert!(self.next_peer_index < self.peers.len());

        Some(peer_index)
    }

    // TODO: include the peer itself in the list of peers.
    // Get `max_number_peers_for_dissemination` with the lowest dissemination count.
    fn select_peers_for_dissemination(&mut self) -> Vec<PiggybackingPeer> {
        let mut heap = BinaryHeap::with_capacity(self.config.max_number_peers_for_dissemination);

        for peer in self.peers.iter() {
            heap.push(OrderByDisseminationCount(peer));

            if heap.len() > self.config.max_number_peers_for_dissemination {
                let _ = heap.pop();
            }
        }

        let mut peers: Vec<_> = heap
            .into_iter()
            .map(|v| PiggybackingPeer::from(v.0))
            .collect();

        if self.is_suspected {
            // Increase the incarnatio number to let other peers know that this peer is alive.
            self.incarnation_number += 1;
        }

        peers.push(self.self_piggybacking_peer());

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
                        self.ping_next_peer().await;
                    },
                    result = async {
                        let mut buffer = [0_u8; RECV_BUFFER_SIZE_IN_BYTES];
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

    #[tracing::instrument(name = "Memberlist::ping_next_peer", skip_all)]
    async fn ping_next_peer(&mut self) {
        let target_peer_index = match self.next_peer_index() {
            None => {
                info!("not connected to other peers");
                return;
            }
            Some(v) => v,
        };

        let target_peer_addr = self.peers[target_peer_index].addr;

        tracing::Span::current().record("target_peer_addr", target_peer_addr.to_string());

        let piggybacking_peers = self.select_peers_for_dissemination();

        let num_peers_for_indirect_probe = std::cmp::min(
            self.peers.len(),
            self.config.max_number_peers_for_indirect_probe,
        );

        let peers_for_ping_req = self.random_peers_for_ping_req(num_peers_for_indirect_probe);
        dbg!(&peers_for_ping_req);

        match tokio::time::timeout(
            self.config.failure_detection_timeout,
            dissemination::disseminate_ping(DisseminatePingInput {
                peer_socket_addr: self.config.socket_addr,
                target_peer_addr: target_peer_addr,
                piggybacking_peers,
                peers_for_ping_req,
                peer_ping_request_timeout_for_indirect_probe: self
                    .config
                    .peer_ping_request_timeout_for_indirect_probe,
                peer_ping_req_request_timeout: self.config.peer_ping_req_request_timeout,
            }),
        )
        .await
        {
            Err(err) => {
                debug!(?err, "ping dissemination timed out");
            }
            Ok(peers_received_in_acks) => {
                // If the peer target peer is unreachable.
                if peers_received_in_acks.is_empty() {
                    debug!("peer is unreachable");
                    self.suspect(target_peer_index).await;
                    return;
                }

                for (_, peer) in peers_received_in_acks {
                    self.peer_received(peer).await;
                }
            }
        }
    }

    #[tracing::instrument(name = "Memberlist::suspect", skip_all, fields(
        suspect_peer_index = suspect_peer_index,
        suspect_peer_addr = ?self.peers[suspect_peer_index].addr,
    ))]
    async fn suspect(&mut self, suspect_peer_index: usize) {
        info!("marking peer as suspected");
        let target_peer = &mut self.peers[suspect_peer_index];
        target_peer.status = PeerStatus::Suspected;
    }

    fn random_peers_for_ping_req(&self, num_peers: usize) -> HashSet<SocketAddr> {
        let mut peers = HashSet::with_capacity(num_peers);

        let mut current_next_peer_index = self.next_peer_index;

        println!("aaaaaa num_peers {:?}", num_peers);
        for _ in 0..num_peers {
            let peer = &self.peers[current_next_peer_index];
            current_next_peer_index = current_next_peer_index + 1 % self.peers.len();

            peers.insert(peer.addr);
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
            PeerMessage::Ping(PingMessage { piggybacking_peers }) => {
                self.peers_received(piggybacking_peers).await;

                dissemination::ack(
                    self.config.socket_addr,
                    client_addr,
                    self.select_peers_for_dissemination(),
                )
                .await
                .context("unable to send ping response")?;
            }
            PeerMessage::PingReq(PingReqMessage {
                piggybacking_peers,
                target_peer_addr,
            }) => {
                self.peers_received(piggybacking_peers).await;

                if let Ok(ack_message) = dissemination::send_message(
                    self.config.socket_addr,
                    target_peer_addr,
                    PeerMessage::Ping(PingMessage {
                        piggybacking_peers: self.select_peers_for_dissemination(),
                    }),
                )
                .await
                {
                    self.peers_received(ack_message.piggybacking_peers).await;

                    dissemination::ack(
                        self.config.socket_addr,
                        client_addr,
                        self.select_peers_for_dissemination(),
                    )
                    .await
                    .context("unable to send pingreq response")?;
                }
            }
            PeerMessage::Ack(AckMessage { piggybacking_peers }) => {
                todo!()
            }
        }

        Ok(())
    }

    // TODO: Bad time complexity. Fix it. (ordered set?)
    async fn peer_received(&mut self, peer: PiggybackingPeer) {
        let min_dissemination_count = min_dissemination_count(&self.peers);

        if peer.addr == self.config.socket_addr {
            // Someone thinks this peer may be dead,
            // the peer needs to let the other peers know it is alive.
            self.is_suspected |= peer.status == PeerStatus::Suspected;

            // TODO: handle dead case
            return;
        }

        match self.peers.iter().position(|p| p.addr == peer.addr) {
            // We don't know about this peer yet.
            None => {
                if peer.status == PeerStatus::Dead {
                    return;
                }
                if let Err(err) = self.sender.send(Notification::Join(peer.addr)).await {
                    error!(?err, "unable to send new peer notification");
                }
                let peer = Peer::from((peer, min_dissemination_count));
                debug!(?peer, "adding peer to peer list");
                self.peers.push(peer);
            }
            // We already know about this peer, the info we have about it may be outdated.
            Some(known_peer_index) => {
                let known_peer = &mut self.peers[known_peer_index];
                // This is and old message that can be ignored.
                if peer.incarnation_number < known_peer.incarnation_number {
                    return;
                }
                match peer.status {
                    PeerStatus::Dead => {
                        if let Err(err) = self.sender.send(Notification::Leave(peer.addr)).await {
                            error!(?err, "unable to send dead peer notification");
                        }
                        self.peers.remove(known_peer_index);
                    }
                    PeerStatus::Alive | PeerStatus::Suspected => {
                        known_peer.status = peer.status;
                        known_peer.incarnation_number = peer.incarnation_number;
                    }
                }
            }
        }
    }

    async fn peers_received(&mut self, peers: Vec<PiggybackingPeer>) {
        for peer in peers {
            self.peer_received(peer).await;
        }
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
