use crate::{
    AckMessage, Peer, PeerAddressType, PeerMessage, PeerMessageType, PeerStatus, PiggybackingPeer,
    PingMessage, PingReqMessage,
};
use anyhow::{anyhow, Result};
use byteorder::{ReadBytesExt, WriteBytesExt};
use std::{
    io::Cursor,
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
};

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
                Ok(PeerMessage::Ping(PingMessage {
                    piggybacking_peers: read_peers(&mut reader)?,
                }))
            }
            message_type if message_type == PeerMessageType::PingReq.as_u8() => {
                Ok(PeerMessage::PingReq(PingReqMessage {
                    piggybacking_peers: read_peers(&mut reader)?,
                    target_peer_addr: read_socket_addr(&mut reader)?,
                }))
            }
            message_type if message_type == PeerMessageType::Ack.as_u8() => {
                Ok(PeerMessage::Ack(AckMessage {
                    piggybacking_peers: read_peers(&mut reader)?,
                }))
            }
            message_type => {
                unreachable!("unhandled message type. message_type={message_type}")
            }
        }
    }
}

impl TryFrom<&PeerMessage> for Vec<u8> {
    type Error = anyhow::Error;

    fn try_from(value: &PeerMessage) -> std::result::Result<Self, Self::Error> {
        let mut buffer = Vec::new();

        match value {
            PeerMessage::Ping(PingMessage { piggybacking_peers }) => {
                buffer.write_u8(PeerMessageType::Ping.as_u8())?;
                write_peers(&mut buffer, piggybacking_peers)?;
            }
            PeerMessage::Ack(AckMessage { piggybacking_peers }) => {
                buffer.write_u8(PeerMessageType::Ack.as_u8())?;
                write_peers(&mut buffer, piggybacking_peers)?;
            }
            PeerMessage::PingReq(PingReqMessage {
                piggybacking_peers,
                target_peer_addr,
            }) => {
                buffer.write_u8(PeerMessageType::PingReq.as_u8())?;
                write_peers(&mut buffer, piggybacking_peers)?;
                write_socket_addr(&mut buffer, *target_peer_addr)?;
            }
        }

        Ok(buffer)
    }
}

impl TryFrom<&[u8]> for AckMessage {
    type Error = anyhow::Error;

    fn try_from(value: &[u8]) -> std::result::Result<Self, Self::Error> {
        Ok(AckMessage {
            piggybacking_peers: read_peers(&mut Cursor::new(value))?,
        })
    }
}

impl TryFrom<&[u8]> for PingMessage {
    type Error = anyhow::Error;

    fn try_from(value: &[u8]) -> std::result::Result<Self, Self::Error> {
        Ok(PingMessage {
            piggybacking_peers: read_peers(&mut Cursor::new(value))?,
        })
    }
}

impl TryFrom<&[u8]> for PingReqMessage {
    type Error = anyhow::Error;

    fn try_from(value: &[u8]) -> Result<Self> {
        let mut reader = Cursor::new(value);
        Ok(PingReqMessage {
            piggybacking_peers: read_peers(&mut reader)?,
            target_peer_addr: read_socket_addr(&mut reader)?,
        })
    }
}

pub(crate) fn write_socket_addr(
    buffer: &mut impl std::io::Write,
    addr: SocketAddr,
) -> std::io::Result<()> {
    buffer.write_u8(addr.address_type().as_u8())?;
    if addr.is_ipv4() {
        buffer.write_all(&addr.ip().ipv4_octets())?;
    } else {
        buffer.write_all(&addr.ip().ipv6_octets())?;
    }

    buffer.write_u16::<byteorder::BigEndian>(addr.port())?;
    Ok(())
}

pub(crate) fn read_socket_addr(reader: &mut impl std::io::Read) -> std::io::Result<SocketAddr> {
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

pub(crate) fn write_peers(
    buffer: &mut impl std::io::Write,
    peers: &[PiggybackingPeer],
) -> std::io::Result<()> {
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

pub(crate) fn read_peers(reader: &mut impl std::io::Read) -> Result<Vec<PiggybackingPeer>> {
    let num_peers = reader.read_u32::<byteorder::BigEndian>()?;

    let mut peers = Vec::with_capacity(num_peers as usize);

    for _ in 0..num_peers {
        let addr = read_socket_addr(reader)?;
        let status = reader.read_u8()?;
        let incarnation_number = reader.read_u32::<byteorder::BigEndian>()?;
        peers.push(PiggybackingPeer {
            addr,
            status: PeerStatus::from(status),
            incarnation_number,
        });
    }

    Ok(peers)
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn message_to_bytes_and_bytes_to_message(m1: PeerMessage) {
            let buffer: Vec<u8> = Vec::try_from(&m1).unwrap();
            let m2 = PeerMessage::try_from(buffer.as_ref()).unwrap();
            assert_eq!(m1, m2);
        }
    }
}
