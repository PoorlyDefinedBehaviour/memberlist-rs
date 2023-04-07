use anyhow::{Context, Result};
use memberlist::Config;
use tracing_subscriber::{prelude::*, EnvFilter};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer())
        .with(EnvFilter::from_env("RUST_LOG"))
        .init();

    tracing::debug!("test debug");
    tracing::info!("test info");

    let socket_addr = std::env::var("SOCKET_ADDR")
        .context("SOCKET_ADDR is env var is required")?
        .parse()?;

    let mut receiver = memberlist::join(Config {
        join_peers: vec!["0.0.0.0:9000".parse()?, "0.0.0.0:9001".parse()?],
        socket_addr,
        ..Config::default()
    })
    .await?;

    while let Some(notification) = receiver.recv().await {
        dbg!(notification);
    }

    Ok(())
}
