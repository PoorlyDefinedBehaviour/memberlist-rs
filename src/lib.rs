use anyhow::{Context, Result};

use rand::seq::SliceRandom;
use rpc_service::RPCService;
use std::{
    collections::{BinaryHeap, HashMap, HashSet},
    net::SocketAddr,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{
    select,
    sync::mpsc::{Receiver, Sender},
};
use tonic::{transport::Server, Request};
use tracing::{debug, error, info, warn};

use crate::grpc_server::DisseminationServer;

mod corev1;

mod grpc_server;
mod rpc_service;

/// How many times a dead peer must be piggybacked in a message before being removed from the member list in this member.
const MIN_DISSEMINATIONS_BEFORE_DEAD_PEER_REMOVAL: u32 = 2;

#[derive(Debug, PartialEq)]
pub enum Notification {
    /// A new peer has joined the cluster.
    Join(SocketAddr),
    /// The peer is considered dead.
    Leave(SocketAddr),
    /// A heartbeat has been received from the peer.
    Alive(SocketAddr)
}

pub async fn join(config: Config) -> Result<Receiver<Notification>> {
    Memberlist::start(config, RPCService::new())
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

impl Peer {
    fn dissemination_count_when_declared_as_dead(&self) -> Option<u32> {
        match self.status {
            PeerStatus::Alive | PeerStatus::Suspected { .. } => None,
            PeerStatus::Dead { declared_dead_at_dissemination_count } => Some(declared_dead_at_dissemination_count),
        }
    }

    fn is_alive(&self) -> bool {
        self.status == PeerStatus::Alive
    }

    fn suspicion_time(&self) -> Option<Instant> {
        match self.status {
            PeerStatus::Alive => None,
            PeerStatus::Dead { .. } => None,
            PeerStatus::Suspected { timestamp } => timestamp,
        }
    }
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

impl PiggybackingPeer {
    fn is_dead(&self) -> bool {
        matches!(self.status, PeerStatus::Dead { .. })
    }

    fn is_suspected(&self) -> bool {
        matches!(self.status, PeerStatus::Suspected { .. })
    }
}

impl From<&mut Peer> for PiggybackingPeer {
    fn from(peer: &mut Peer) -> Self {
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

struct OrderByDisseminationCount<'a>(&'a mut Peer);

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
    Dead { declared_dead_at_dissemination_count: u32 },
    Suspected { timestamp: Option<Instant> },
}

impl From<u8> for PeerStatus {
    fn from(value: u8) -> Self {
        match value {
            1 => PeerStatus::Alive,
            2 => PeerStatus::Dead { declared_dead_at_dissemination_count: 0 },
            3 => PeerStatus::Suspected { timestamp: None },
            value => unreachable!("unexpected peer status. status={value}"),
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
    /// The amount of time to wait for a ping response to arrive before starting an indirect probe.
    pub peer_ping_request_timeout_for_indirect_probe: Duration,
    /// The amount of time to wait for a ping response.
    pub peer_ping_dissemination_timeout: Duration,
    /// The amount of time to wait between peer suspicion checks.
    pub peer_suspicion_check_interval: Duration,
    /// The amount of time a suspected peer has to let the others peers know that it is alive.
    pub peer_suspicion_timeout: Duration,
    /// Maximum number of peers used to detect if a peer is alive when it doesn't respond to a ping request.
    pub max_peers_for_indirect_probe: usize,
    /// Maximum number of peers that can be piggybacked on messages.
    pub max_piggybacking_peers: usize,
    /// Address to bind this member's udp socket to.
    pub socket_addr: SocketAddr,
}

impl Default for Config {
    fn default() -> Self {
        let failure_detection_attempt_interval = Duration::from_secs(10);
        Self {
            join_peers: Vec::new(),
            mailbox_buffer_size: 64,
            failure_detection_attempt_interval,
            peer_ping_request_timeout_for_indirect_probe: Duration::from_millis(500),
            peer_ping_dissemination_timeout:Duration::from_secs(1),
            peer_suspicion_check_interval:Duration::from_secs(1),
            peer_suspicion_timeout: failure_detection_attempt_interval*  3,
            max_peers_for_indirect_probe: 3,
            max_piggybacking_peers: 3,
            socket_addr: "0.0.0.0:9157".parse().unwrap(),
        }
    }
}

#[derive(Debug)]
pub(crate) struct DisseminatePingInput {
    pub target_peer_addr: SocketAddr,
    pub piggybacking_peers: Vec<PiggybackingPeer>,
    pub peers_for_ping_req: HashSet<SocketAddr>,
    pub peer_ping_request_timeout_for_indirect_probe: Duration,
}

#[derive(Debug)]
struct Memberlist {
    /// This member's config.
    config: Config,
    /// The index of the next peer in `peers` to check for failure.
    next_peer_index: usize,
    /// List of peers this member knows about.
    peers: Vec<Peer>,
    /// Channel used to receive RPC requests from the other peers.
    request_receiver: Receiver<PeerRequest>,
    /// Channel used to send messages containing peer updates.
    notification_sender: Sender<Notification>,
    /// The current generation of this peer.
    incarnation_number: u32,
    /// True when another peer suspects this peer is dead.
    is_suspected: bool,
    /// Service used to make RPC requests.
    rpc_service: Arc<RPCService>,
}

/// Represents a message received from a peer.
#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
enum PeerMessage {
    Ping(PingMessage),
    PingReq(PingReqMessage),
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

/// Represents a request received by the grpc server.
#[derive(Debug)]
struct PeerRequest {
    peer_message: PeerMessage,
    response_sender: tokio::sync::oneshot::Sender<Result<AckMessage>>,
}

impl Memberlist {
    async fn start(config: Config, rpc_service: RPCService) -> Result<Receiver<Notification>> {
        assert!(
            config.join_peers.len() > 0,
            "at least one peer is required so this member can contact it to join the cluster"
        );
        assert!(config.mailbox_buffer_size > 0, "must be greater than 0");

        info!(?config, "starting peer");

        let (notification_sender, notification_receiver) =
            tokio::sync::mpsc::channel(config.mailbox_buffer_size);
        let (request_sender, request_receiver) =
            tokio::sync::mpsc::channel(config.mailbox_buffer_size);

        let svc =
            corev1::member_server::MemberServer::new(DisseminationServer::new(request_sender));

        // TOOD: handle error if server can't start
        let socket_addr = config.socket_addr;
        tokio::spawn(async move {
            if let Err(err) = Server::builder().add_service(svc).serve(socket_addr).await {
                error!(
                    ?err,
                    "unable to start grpc server, will not receive peer requests"
                );
            }
        });

        let memberlist = Self {
            rpc_service: Arc::new(rpc_service),
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
            request_receiver,
            notification_sender,
            incarnation_number: 0,
            is_suspected: false,
            config,
        };

        tokio::spawn(memberlist.control_loop());
        Ok(notification_receiver)
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

        self.next_peer_index = (self.next_peer_index + 1) % self.peers.len();
        

        if reached_last_peer {
            self.peers.shuffle(&mut rand::thread_rng());
        }

        assert!(self.next_peer_index < self.peers.len());

        Some(peer_index)
    }

    // TODO: include the peer itself in the list of peers.
    // Get `max_piggybacking_peers` with the lowest dissemination count.
    fn select_peers_for_dissemination(&mut self) -> Vec<PiggybackingPeer> {
        let mut heap = BinaryHeap::with_capacity(self.config.max_piggybacking_peers);

        for peer in self.peers.iter_mut() {
            heap.push(OrderByDisseminationCount(peer));

            if heap.len() > self.config.max_piggybacking_peers {
                let _ = heap.pop();
            }
        }

        let mut peers = Vec::with_capacity(heap.len());

        for order in heap.into_iter() {
            order.0.dissemination_count += 1;
            peers.push(PiggybackingPeer::from(order.0));
        }

        peers.push(self.self_piggybacking_peer());

        peers
    }

    /// The main control loop of this member.
    /// Every operation that will be performed by the member is handled here.
    async fn control_loop(mut self) {
        let mut detect_failure_interval =
            tokio::time::interval(self.config.failure_detection_attempt_interval);

        let mut peer_suspicion_check_interval = tokio::time::interval(self.config.peer_suspicion_check_interval);

        loop {
            select! {
                    _ = detect_failure_interval.tick() => {
                        self.ping_next_peer().await;
                    },
                    _ = peer_suspicion_check_interval.tick() => {
                        self.mark_suspected_peers_as_dead().await;
                    },
                    request = self.request_receiver.recv() => {
                        let request = request.expect("bug: request receiver dropped");

                        match self.handle_peer_message(request.peer_message).await {
                            Err(err) => {
                                error!(?err, "error handling peer request message");
                                if let Err(err) = request.response_sender.send(Err(err)) {
                                    warn!(?err, "error sending response to peer request response channel");
                                }
                            }
                            Ok(v) => {
                                if let Err(err) = request.response_sender.send(Ok(v)) {
                                    warn!(?err, "error sending response to peer request response channel");
                                }
                            }
                        }
                }
            }
        }
    }

    #[tracing::instrument(name = "Memberlist::mark_suspected_peers_as_dead", skip_all)]
    async fn mark_suspected_peers_as_dead(&mut self) {
        let mut peers_to_remove = HashSet::new();

        for (i, peer) in self.peers.iter_mut().enumerate() {
            if let Some(timestamp) = peer.suspicion_time() {
                let now = Instant::now();
                if now - timestamp > self.config.peer_suspicion_timeout {
                    debug!(
                        peer_suspicion_timeout = ?self.config.peer_suspicion_timeout, 
                        ?peer,
                        "marking suspected peer as dead"
                    );
                    peer.status = PeerStatus::Dead { declared_dead_at_dissemination_count: peer.dissemination_count };
                }
            }

            if let Some(declared_dead_at_dissemination_count) = peer.dissemination_count_when_declared_as_dead() {
                if peer.dissemination_count - declared_dead_at_dissemination_count >= MIN_DISSEMINATIONS_BEFORE_DEAD_PEER_REMOVAL {
                    debug!(
                        ?peer, ?MIN_DISSEMINATIONS_BEFORE_DEAD_PEER_REMOVAL, 
                        "peer has been dead for at least MIN_DISSEMINATIONS_BEFORE_DEAD_PEER_REMOVAL, removing from member list"
                    );
                    if let Err(err) = self.notification_sender.send(Notification::Leave(peer.addr)).await {
                        error!(?err, "error sending leave notification to notification sender");
                    }
                    peers_to_remove.insert(i);
                }
            }
        }

        for peer_index in peers_to_remove {
            self.peers.remove(peer_index);
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
        debug!(?target_peer_addr, "will ping peer");

        tracing::Span::current().record("target_peer_addr", target_peer_addr.to_string());

        let piggybacking_peers = self.select_peers_for_dissemination();

        let num_peers_for_indirect_probe = std::cmp::min(
            self.peers.len(),
            self.config.max_peers_for_indirect_probe,
        );

        let peers_for_ping_req = self.random_peers_for_ping_req(num_peers_for_indirect_probe);
       
        let peers_received_in_acks = self.disseminate_ping(DisseminatePingInput {
            target_peer_addr: target_peer_addr,
            piggybacking_peers,
            peers_for_ping_req,
            peer_ping_request_timeout_for_indirect_probe: self
                .config
                .peer_ping_request_timeout_for_indirect_probe,
        })
        .await;
        
        
        // If the target peer is unreachable.
        if peers_received_in_acks.is_empty() {
            debug!("peer is unreachable");
            self.suspect(target_peer_index).await;
            return;
        }

        for (_, peer) in peers_received_in_acks {
            self.peer_received(peer).await;
        }
    }

    #[tracing::instrument(name = "Memberlist::suspect", skip_all, fields(
        suspect_peer_index = suspect_peer_index,
        suspect_peer_addr = ?self.peers[suspect_peer_index].addr,
    ))]
    async fn suspect(&mut self, suspect_peer_index: usize) {
        let target_peer = &mut self.peers[suspect_peer_index];
        if target_peer.is_alive(){
            info!("marking peer as suspected");
            target_peer.status = PeerStatus::Suspected{timestamp: Some(Instant::now())};
        }
    }

    fn random_peers_for_ping_req(&self, num_peers: usize) -> HashSet<SocketAddr> {
        let mut peers = HashSet::with_capacity(num_peers);
        

        let mut current_next_peer_index = self.next_peer_index;

        for _ in 0..num_peers {
            let peer = &self.peers[current_next_peer_index];
            current_next_peer_index = (current_next_peer_index + 1) % self.peers.len();

            peers.insert(peer.addr);
        }

        peers
    }

    #[tracing::instrument(name = "Memberlist::handle_peer_message", skip_all, fields(
        message = ?message
    ))]
    async fn handle_peer_message(&mut self, message: PeerMessage) -> Result<AckMessage> {
        debug!("handling peer message");

        match message {
            // TODO: add message number to each message(may be useful to receive responses)
            PeerMessage::Ping(PingMessage { piggybacking_peers }) => {
                self.peers_received(piggybacking_peers).await;

                Ok(AckMessage {
                    piggybacking_peers: self.select_peers_for_dissemination(),
                })
            }
            PeerMessage::PingReq(PingReqMessage {
                piggybacking_peers,
                target_peer_addr,
            }) => {
                self.peers_received(piggybacking_peers).await;

                let piggybacking_peers = self
                    .select_peers_for_dissemination()
                    .into_iter()
                    .map(corev1::Peer::from)
                    .collect();

                let request = Request::new(corev1::PingRequest { piggybacking_peers });

                let ack_response = self
                    .rpc_service
                    .ping(target_peer_addr, request)
                    .await
                    .context("sending ping request because of ping req")?;

                self.peers_received(
                    ack_response
                        .piggybacking_peers
                        .into_iter()
                        .map(PiggybackingPeer::from)
                        .collect(),
                )
                .await;

                Ok(AckMessage {
                    piggybacking_peers: self.select_peers_for_dissemination(),
                })
            }
        }
    }

    // TODO: Bad time complexity. Fix it. (ordered set?)
    #[tracing::instrument(name = "Memberlist::peer_received", skip_all, fields(
        peer = ?peer
    ))]
    async fn peer_received(&mut self, peer: PiggybackingPeer) {
        let min_dissemination_count = min_dissemination_count(&self.peers);

        if peer.addr == self.config.socket_addr {
            // Someone thinks this peer may be dead,
            // the peer needs to let the other peers know it is alive.
            self.is_suspected |= peer.is_suspected();

            if self.is_suspected {
                debug!("another peer marked this peer as suspected");
            }

            // TODO: handle dead case
            return;
        }

        match self.peers.iter().position(|p| p.addr == peer.addr) {
            // We don't know about this peer yet.
            None => {
                if peer.is_dead() {
                    return;
                }
                if let Err(err) = self
                    .notification_sender
                    .send(Notification::Join(peer.addr))
                    .await
                {
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
                    debug!(
                        peer_incarnation_number = peer.incarnation_number, 
                        known_peer_incarnation_number = known_peer.incarnation_number,
                        "received peer with incarnation number lower than the current incarnation number, ignoring peer"
                    );
                    return;
                }
                match peer.status {
                    PeerStatus::Dead { .. } => {
                        if let Err(err) = self
                            .notification_sender
                            .send(Notification::Leave(peer.addr))
                            .await
                        {
                            error!(?err, "unable to send dead peer notification");
                        }
                        debug!("peer marked as dead, removing from peer list");
                        self.peers.remove(known_peer_index);
                    }
                    PeerStatus::Alive | PeerStatus::Suspected { .. } => {
                        if let Err(err) = self.notification_sender.send(Notification::Alive(peer.addr)).await {
                            error!(?err, "unable to send peer alive notification");
                        }

                        known_peer.status = peer.status;
                        known_peer.incarnation_number = peer.incarnation_number;
                    }
                }
            }
        }
    }

    #[tracing::instrument(name = "Memberlist::peers_received", skip_all, fields(
        peers = ?peers
    ))]
    async fn peers_received(&mut self, peers: Vec<PiggybackingPeer>) {
        for peer in peers {
            self.peer_received(peer).await;
        }
    }

    pub async fn disseminate_ping(
        &mut self,
        input: DisseminatePingInput,
    ) -> HashMap<SocketAddr, PiggybackingPeer> {
        let piggybacking_peers: Vec<_> = input
            .piggybacking_peers
            .into_iter()
            .map(corev1::Peer::from)
            .collect();

        let (sender, mut receiver) = tokio::sync::mpsc::channel(1);

        let ping_handle = {
            let sender = sender.clone();
            let piggybacking_peers = piggybacking_peers.clone();
            let target_peer_addr = input.target_peer_addr;
            let rpc_service = Arc::clone(&self.rpc_service);

            tokio::spawn(async move {
                let request = Request::new(corev1::PingRequest { piggybacking_peers });

                if let Ok(ack_response) = rpc_service
                    .ping(
                        target_peer_addr,
                        request,
                    )
                    .await
                {
                    debug!(?ack_response, "received ack for ping message");
                    if let Err(err) = sender.send(ack_response).await {
                        error!(?err, "unable to send ping ack message to channel");
                    }
                }
            })
        };

        let rpc_service = Arc::clone(&self.rpc_service);
        let ping_req_handle = tokio::spawn(async move {
            tokio::time::sleep(input.peer_ping_request_timeout_for_indirect_probe).await;

            futures::future::join_all(input.peers_for_ping_req.into_iter().map(|peer_addr| {
                let sender = sender.clone();
                let piggybacking_peers = piggybacking_peers.clone();
                let rpc_service = Arc::clone(&rpc_service);

                async move {
                    let request = Request::new(corev1::PingReqRequest {
                        piggybacking_peers,
                        target_peer_addr: Some(corev1::PeerAddr::from(input.target_peer_addr)),
                    });

                    match rpc_service.ping_req(peer_addr, request).await {
                        Err(err) => {
                            debug!(?err, "pingreq request error");
                        }
                        Ok(ack_response) => {
                            debug!(?ack_response, "received ack for ping request");
                            if let Err(err) = sender.send(ack_response).await {
                                error!(?err, "unable to send ack message from pingreq to channel");
                            }
                        }
                    }
                }
            }))
        });

        // Different peers may send different views of the same peers.
        // Keep only the latest information about each peer.
        let mut piggybacking_peers = HashMap::new();
        
        let _= tokio::time::timeout(self.config.peer_ping_dissemination_timeout, async {
            while let Some(ack_response) = receiver.recv().await {
                for piggybacked_peer in ack_response.piggybacking_peers {
                    let new_status = piggybacked_peer.status;
                    let new_incarnation_number = piggybacked_peer.incarnation_number;
                    let peer_addr = SocketAddr::from(
                        piggybacked_peer
                            .addr
                            .as_ref()
                            .expect("peer addr is required, it not being included is a bug"),
                    );

                    let entry = piggybacking_peers
                        .entry(peer_addr)
                        .or_insert(PiggybackingPeer::from(piggybacked_peer));

                    if entry.incarnation_number < new_incarnation_number {
                        entry.status = PeerStatus::from(new_status);
                        entry.incarnation_number = new_incarnation_number;
                    }
                }
            }}
        ).await;

        ping_handle.abort();
        ping_req_handle.abort();

        piggybacking_peers
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
