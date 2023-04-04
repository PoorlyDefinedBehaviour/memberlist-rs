use anyhow::{Context, Result};
use rand::{seq::SliceRandom, Rng};
use std::{
    collections::HashSet,
    net::SocketAddr,
    time::{Duration, Instant},
};
use tokio::{
    io::AsyncWriteExt,
    net::UdpSocket,
    select,
    sync::mpsc::{Receiver, Sender},
};
use tracing::info;

pub type PeerAddr = String;

#[derive(Debug, PartialEq, Eq, Hash)]
struct Peer {
    /// The peer address.
    addr: PeerAddr,
    /// The peer status in this member view.
    status: PeerStatus,
    /// Whenever the peer is declared as alive, its incarnation number is incremented.
    incarnation_number: u64,
}

#[derive(Debug, PartialEq, Eq, Hash)]
enum PeerStatus {
    Alive,
    Dead,
    Suspected {
        /// When this peer status changed to Suspected.
        timestamp: Instant,
    },
}

/// Used by the library client to interact with the member.
#[derive(Debug)]
pub struct MemberlistHandler {
    sender: Sender<Message>,
}

#[derive(Debug)]
pub struct Config {
    /// Addresses of peers that this member will contact to join the cluster.
    pub join_peers: Vec<PeerAddr>,
    /// How many messages can be held in the channel used to buffer actions sent to this member before blocking.
    pub mailbox_buffer_size: usize,
    /// The amount of time to wait for between failure detection attempts.
    pub failure_detection_attempt_interval: Duration,
    /// The amount of time a peer has to respond to a ping request.
    pub peer_ping_request_timeout: Duration,
    /// Maximum number of peers used to detect if a peer is alive when it doesn't respond to a ping request.
    pub max_number_peers_for_indirect_probe: usize,
    /// Address to bind this member udp socket to.
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

/// Represents a change in the member list, a member joined for example.
#[derive(Debug)]
pub enum Notification {}

/// The type of a message sent from one peer to other.
#[derive(Debug)]
pub enum PeerMessageType {
    /// Find out if a peer is alive.
    Ping,
    /// Response to a Ping request.
    Pong,
    /// Ask other peers to find out if a peer is alive.
    PingReq,
    /// The peer could be reached.
    PingReqAlive,
    /// The peer could not be reached.
    PingReqUnreachable,
}

impl PeerMessageType {
    fn as_u8(&self) -> u8 {
        match self {
            PeerMessageType::Ping => 1,
            PeerMessageType::Pong => 2,
            PeerMessageType::PingReq => 3,
            PeerMessageType::PingReqAlive => 4,
            PeerMessageType::PingReqUnreachable => 5,
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
                        result = self.ping(target_peer) => {
                            // TODO: let othe peers know what this peer thinks.
                            if result.is_ok() {
                                PeerStatus::Alive
                            } else {
                                PeerStatus::Suspected{ timestamp: Instant::now() }
                            }
                        },
                        peer_status = async {
                            tokio::time::sleep(self.config.peer_ping_request_timeout).await;

                            // Ask other peers to check if peer is alive.
                            let target_peer_is_reachable = futures::future::join_all(
                                self.random_peers_for_ping_req(target_peer)
                                    .into_iter()
                                    .map(|peer| self.ping_req(peer, target_peer))
                            )
                            .await
                            .into_iter()
                            .any(|response| response.unwrap_or(false));

                            if target_peer_is_reachable {
                                PeerStatus::Alive
                            } else {
                                PeerStatus::Suspected{ timestamp: Instant::now() }
                            }
                        } => { peer_status }
                    };

                    self.peers[target_peer_index].status = peer_status;
                    // TODO: tell the other peers about the target peer status.
               },
                message = self.receiver.recv() => {
                    let message = match message {
                        None => {
                            info!("sender channel closed, member list handler has been dropped, exiting control loop");
                            return;
                        },
                        Some(v) => v
                    };

                    match message {

                    }
                }
            }
        }
    }

    fn random_peers_for_ping_req(&self, target_peer: &Peer) -> HashSet<&Peer> {
        // The number of peers that we have to choose from. We may not have the number of peers requested in by `max_number_peers_for_indirect_probe`.
        // Subtract 1 from `peers.len()` to take the target peer into account.
        let num_peers = std::cmp::min(
            self.peers.len().saturating_sub(1),
            self.config.max_number_peers_for_indirect_probe,
        );

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

    async fn ping(&self, peer: &Peer) -> Result<()> {
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
            .connect(&peer.addr)
            .await
            .context("connecting to peer")?;

        let buffer = [PeerMessageType::Ping.as_u8()];

        socket
            .send(&buffer)
            .await
            .context("sending ping message to peer")?;

        let mut buffer = [0_u8; 1];
        assert_eq!(1, std::mem::size_of_val(&PeerMessageType::Pong.as_u8()));

        loop {
            let bytes_read = socket
                .recv(&mut buffer)
                .await
                .context("trying to receive ping request response")?;

            if bytes_read == buffer.len() && buffer[0] == PeerMessageType::Pong.as_u8() {
                return Result::<()>::Ok(());
            }
        }
    }

    async fn ping_req(&self, peer: &Peer, target_peer: &Peer) -> Result<bool> {
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
            .connect(&peer.addr)
            .await
            .context("connecting to peer")?;

        let mut buffer = Vec::new();
        buffer
            .write_u8(PeerMessageType::PingReq.as_u8())
            .await
            .context("writing message type")?;
        buffer
            .write_all(target_peer.addr.as_bytes())
            .await
            .context("writing target peer to buffer")?;

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
                let alive = PeerMessageType::PingReqAlive.as_u8();
                let dead = PeerMessageType::PingReqUnreachable.as_u8();
                match buffer[0] {
                    alive => return Ok(true),
                    dead => return Ok(false),
                    _ => {}
                }
            }
        }
    }
}
