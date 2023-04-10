use anyhow::{Context, Result};
use std::net::SocketAddr;

use tracing::debug;

use crate::corev1;
use crossbeam_skiplist::SkipMap;
use tonic::{transport::Channel, Request};

// TODO: handle disconnections.
#[derive(Debug)]
pub(crate) struct RPCService {
    /// Map from peer address to rpc client.
    clients: SkipMap<SocketAddr, corev1::member_client::MemberClient<Channel>>,
}

impl RPCService {
    pub fn new() -> Self {
        Self {
            clients: SkipMap::new(),
        }
    }

    #[tracing::instrument(name = "RPCService::get_client_for_addr", skip_all, fields(
        addr = ?addr
    ))]
    async fn get_client_for_addr(
        &self,
        addr: SocketAddr,
    ) -> Result<corev1::member_client::MemberClient<Channel>> {
        if let Some(entry) = self.clients.get(&addr) {
            debug!("already connected to addr, returning cached client");
            // TODO: is cloning MemberClient cheap?
            return Ok(entry.value().clone());
        }

        // This is the first time we are connecting to `addr`.
        let client = corev1::member_client::MemberClient::connect(format!("http://{addr}"))
            .await
            .context("connecting to client")?;

        self.clients.insert(addr, client.clone());

        debug!("connected to client for the first time");
        Ok(client)
    }

    pub async fn ping(
        &self,
        target_addr: SocketAddr,
        request: Request<corev1::PingRequest>,
    ) -> Result<corev1::AckResponse> {
        let mut client = self
            .get_client_for_addr(target_addr)
            .await
            .context("getting rpc client")?;

        let response = client.ping(request).await.context("sending ping request")?;

        Ok(response.into_inner())
    }

    pub async fn ping_req(
        &self,
        target_addr: SocketAddr,
        request: Request<corev1::PingReqRequest>,
    ) -> Result<corev1::AckResponse> {
        let mut client = self
            .get_client_for_addr(target_addr)
            .await
            .context("getting rpc client")?;

        let response = client
            .ping_req(request)
            .await
            .context("sending ping req request")?;

        Ok(response.into_inner())
    }
}
