use std::{
    collections::HashSet,
    net::{AddrParseError, SocketAddr},
};

use anyhow::{Context, Result};
use memberlist::{Config, Notification};
use tracing_subscriber::{prelude::*, EnvFilter};

// RUST_LOG=memberlist=debug SOCKET_ADDR=0.0.0.0:9000 JOIN_PEERS=0.0.0.0:9001 cargo run --example main
// RUST_LOG=memberlist=debug SOCKET_ADDR=0.0.0.0:9001 JOIN_PEERS=0.0.0.0:9000 cargo run --example main
#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer())
        .with(EnvFilter::from_env("RUST_LOG"))
        .init();

    let socket_addr: SocketAddr = std::env::var("SOCKET_ADDR")
        .context("SOCKET_ADDR env var is required")?
        .parse()?;

    let join_peers: Vec<SocketAddr> = std::env::var("JOIN_PEERS")
        .context("JOIN_PEERS env var is required")?
        .split(",")
        .map(|s| s.parse())
        .collect::<Result<Vec<SocketAddr>, AddrParseError>>()?;

    let mut receiver = memberlist::join(Config {
        join_peers,
        socket_addr,
        ..Config::default()
    })
    .await?;

    let mut members = HashSet::new();

    while let Some(notification) = receiver.recv().await {
        dbg!(&notification);
        match notification {
            Notification::Join(peer_addr) | Notification::Alive(peer_addr) => {
                members.insert(peer_addr);
            }
            Notification::Leave(peer_addr) => {
                members.remove(&peer_addr);
            }
        }
        dbg!(&members);
    }

    Ok(())
}
