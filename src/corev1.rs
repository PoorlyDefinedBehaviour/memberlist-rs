use std::net::SocketAddr;

pub(crate) mod corev1 {
    tonic::include_proto!("corev1");
}

pub use corev1::*;

use crate::PiggybackingPeer;

impl From<PiggybackingPeer> for corev1::Peer {
    fn from(value: PiggybackingPeer) -> Self {
        Self {
            addr: Some(corev1::PeerAddr::from(value.addr)),
            status: corev1::PeerStatus::from(value.status) as i32,
            incarnation_number: value.incarnation_number,
        }
    }
}

impl From<corev1::Peer> for PiggybackingPeer {
    fn from(peer: corev1::Peer) -> Self {
        Self {
            addr: peer.addr.as_ref().unwrap().into(),
            status: peer.status.into(),
            incarnation_number: peer.incarnation_number,
        }
    }
}

impl From<crate::PeerStatus> for corev1::PeerStatus {
    fn from(value: crate::PeerStatus) -> Self {
        match value {
            crate::PeerStatus::Alive => corev1::PeerStatus::Alive,
            crate::PeerStatus::Dead { .. } => corev1::PeerStatus::Dead,
            crate::PeerStatus::Suspected { .. } => corev1::PeerStatus::Suspected,
        }
    }
}

impl From<i32> for crate::PeerStatus {
    fn from(value: i32) -> Self {
        if value == corev1::PeerStatus::Alive as i32 {
            return crate::PeerStatus::Alive;
        }
        if value == corev1::PeerStatus::Dead as i32 {
            return crate::PeerStatus::Dead {
                declared_dead_at_dissemination_count: 0,
            };
        }
        if value == corev1::PeerStatus::Suspected as i32 {
            return crate::PeerStatus::Suspected { timestamp: None };
        }
        unreachable!("unexpected peer status: {value}");
    }
}

impl From<SocketAddr> for corev1::PeerAddr {
    fn from(addr: SocketAddr) -> Self {
        corev1::PeerAddr {
            r#type: match addr.ip() {
                std::net::IpAddr::V4(_) => 1,
                std::net::IpAddr::V6(_) => 2,
            },
            addr: addr.to_string(),
        }
    }
}

impl From<&corev1::PeerAddr> for SocketAddr {
    fn from(peer_addr: &corev1::PeerAddr) -> Self {
        match peer_addr.r#type {
            1 | 2 => peer_addr.addr.parse().unwrap(),
            typ => unreachable!("unexpected ip address type: {typ}"),
        }
    }
}

impl From<corev1::PingRequest> for crate::PingMessage {
    fn from(request: corev1::PingRequest) -> Self {
        Self {
            piggybacking_peers: request
                .piggybacking_peers
                .into_iter()
                .map(crate::PiggybackingPeer::from)
                .collect(),
        }
    }
}

impl From<corev1::PingReqRequest> for crate::PingReqMessage {
    fn from(request: corev1::PingReqRequest) -> Self {
        Self {
            target_peer_addr: SocketAddr::from(request.target_peer_addr.as_ref().unwrap()),
            piggybacking_peers: request
                .piggybacking_peers
                .into_iter()
                .map(crate::PiggybackingPeer::from)
                .collect(),
        }
    }
}

impl From<crate::AckMessage> for corev1::AckResponse {
    fn from(message: crate::AckMessage) -> Self {
        Self {
            piggybacking_peers: message
                .piggybacking_peers
                .into_iter()
                .map(corev1::Peer::from)
                .collect(),
        }
    }
}
