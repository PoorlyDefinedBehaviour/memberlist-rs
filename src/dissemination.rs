use anyhow::{anyhow, Context, Result};
use std::{
    collections::{HashMap, HashSet},
    net::SocketAddr,
    time::Duration,
};
use tokio::net::UdpSocket;
use tracing::{debug, error};

use crate::{
    AckMessage, PeerMessage, PeerMessageType, PiggybackingPeer, PingMessage, PingReqMessage,
};

pub(crate) const RECV_BUFFER_SIZE_IN_BYTES: usize = 2048;

pub(crate) struct DisseminatePingInput {
    pub peer_socket_addr: SocketAddr,
    pub target_peer_addr: SocketAddr,
    pub piggybacking_peers: Vec<PiggybackingPeer>,
    pub peers_for_ping_req: HashSet<SocketAddr>,
    pub peer_ping_request_timeout: Duration,
}

pub(crate) async fn disseminate_ping(
    input: DisseminatePingInput,
) -> HashMap<SocketAddr, PiggybackingPeer> {
    let (sender, mut receiver) = tokio::sync::mpsc::channel(1);

    let ping_handle = {
        let sender = sender.clone();
        let piggybacking_peers = input.piggybacking_peers.clone();

        tokio::spawn(async move {
            if let Ok(ack_message) = ping(
                input.peer_socket_addr,
                input.target_peer_addr,
                piggybacking_peers,
            )
            .await
            {
                if let Err(err) = sender.send(ack_message).await {
                    error!(?err, "unable to send ping ack message to channel");
                }
            }
        })
    };

    let ping_req_handle = tokio::spawn(async move {
        tokio::time::sleep(input.peer_ping_request_timeout).await;

        futures::future::join_all(input.peers_for_ping_req.into_iter().map(|peer_addr| {
            let sender = sender.clone();
            let piggybacking_peers = input.piggybacking_peers.clone();

            async move {
                if let Ok(ack_message) = ping_req(
                    input.peer_socket_addr,
                    peer_addr,
                    input.target_peer_addr,
                    piggybacking_peers,
                )
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
    let mut piggybacking_peers = HashMap::new();

    while let Some(ack_message) = receiver.recv().await {
        for piggybacked_peer in ack_message.piggybacking_peers {
            let new_status = piggybacked_peer.status;
            let new_incarnation_number = piggybacked_peer.incarnation_number;

            let entry = piggybacking_peers
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

    return piggybacking_peers;
}

pub(crate) async fn ping(
    peer_socket_addr: SocketAddr,
    target_peer_addr: SocketAddr,
    piggybacking_peers: Vec<PiggybackingPeer>,
) -> Result<AckMessage> {
    debug!(?peer_socket_addr, ?target_peer_addr, "sending ping mesage");

    let ack_message = send_message(
        peer_socket_addr,
        target_peer_addr,
        PeerMessage::Ping(PingMessage { piggybacking_peers }),
    )
    .await
    .context("sending ping message")?;

    Ok(ack_message)
}

pub(crate) async fn ping_req(
    peer_socket_addr: SocketAddr,
    target_peer_addr: SocketAddr,
    indirect_peer_addr: SocketAddr,
    piggybacking_peers: Vec<PiggybackingPeer>,
) -> Result<AckMessage> {
    let ack_message = send_message(
        peer_socket_addr,
        target_peer_addr,
        PeerMessage::PingReq(PingReqMessage {
            piggybacking_peers: piggybacking_peers,
            target_peer_addr: indirect_peer_addr,
        }),
    )
    .await
    .context("sending pingreq message")?;
    Ok(ack_message)
}

pub(crate) async fn ack(
    peer_socket_addr: SocketAddr,
    target_peer_addr: SocketAddr,
    piggybacking_peers: Vec<PiggybackingPeer>,
) -> Result<()> {
    let _ = send_message(
        peer_socket_addr,
        target_peer_addr,
        PeerMessage::Ack(AckMessage { piggybacking_peers }),
    )
    .await
    .context("sending ack message")?;
    Ok(())
}

pub(crate) fn udp_socket_with_addr_reuse(addr: SocketAddr) -> Result<UdpSocket> {
    UdpSocket::from_std({
        let s = socket2::Socket::new(
            socket2::Domain::IPV4,
            socket2::Type::DGRAM,
            Some(socket2::Protocol::UDP),
        )?;

        s.set_nonblocking(true)
            .context("setting socket to non blocking")?;

        s.set_reuse_address(true)
            .context("setting reuse_address to true")?;

        s.bind(&addr.into()).context("binding local socket")?;

        s.into()
    })
    .context("binding udp socket")
}

#[tracing::instrument(name = "dissemination::send_message", skip_all, fields(
    peer_socket_addr = ?peer_socket_addr,
    target_peer_addr = ?target_peer_addr,
    message = ?message,
))]
pub(crate) async fn send_message(
    peer_socket_addr: SocketAddr,
    target_peer_addr: SocketAddr,
    message: PeerMessage,
) -> Result<AckMessage> {
    debug!("sending message to peer");

    let socket = udp_socket_with_addr_reuse(peer_socket_addr)?;

    socket
        .connect(target_peer_addr)
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
            debug!("received empty response from peer");
            return Err(anyhow!(
                "tried to read message response but got empty buffer"
            ));
        }

        if buffer[0] == PeerMessageType::Ack.as_u8() {
            return Ok(AckMessage::try_from(buffer.as_ref())?);
        }

        debug!(
            peer_message_type = buffer[0],
            "received response with unexpected message type"
        );
    }
}
