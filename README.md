# Example

```rust
use std::{
    collections::HashSet,
    net::{AddrParseError, SocketAddr},
};

use anyhow::{Context, Result};
use memberlist::{Config, Notification};

#[tokio::main]
async fn main() -> Result<()> {
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
        match notification {
            Notification::Join(peer_addr) | Notification::Alive(peer_addr) => {
                members.insert(peer_addr);
            }
            Notification::Leave(peer_addr) => {
                members.remove(&peer_addr);
            }
        }
    }

    Ok(())
}

```

# References

https://www.cs.cornell.edu/projects/Quicksilver/public_pdfs/SWIM.pdf