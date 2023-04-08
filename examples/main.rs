use std::net::{AddrParseError, SocketAddr};

use anyhow::{Context, Result};
use memberlist::Config;
use tracing_subscriber::{prelude::*, EnvFilter};

// RUST_LOG=debug SOCKET_ADDR=0.0.0.0:9000 JOIN_PEERS=0.0.0.0:9001 cargo run --example main
// RUST_LOG=debug SOCKET_ADDR=0.0.0.0:9001 JOIN_PEERS=0.0.0.0:9000 cargo run --example main
#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer())
        .with(EnvFilter::from_env("RUST_LOG"))
        .init();

    tracing::debug!("test debug");
    tracing::info!("test info");

    let socket_addr = std::env::var("SOCKET_ADDR")
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

    while let Some(notification) = receiver.recv().await {
        dbg!(notification);
    }

    Ok(())
}
