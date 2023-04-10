use anyhow::Result;
use tokio::sync::mpsc::Sender;
use tonic::{Request, Response, Status};
use tracing::error;

use crate::{corev1, PeerRequest, PingMessage, PingReqMessage};

pub(crate) struct DisseminationServer {
    request_sender: Sender<PeerRequest>,
}

impl DisseminationServer {
    pub fn new(request_sender: Sender<PeerRequest>) -> Self {
        Self { request_sender }
    }
}

#[tonic::async_trait]
impl corev1::member_server::Member for DisseminationServer {
    #[tracing::instrument(name = "DisseminationServer::ping", skip_all, fields(
        request = ?request
    ))]
    async fn ping(
        &self,
        request: Request<corev1::PingRequest>,
    ) -> Result<Response<corev1::AckResponse>, Status> {
        let (sender, receiver) = tokio::sync::oneshot::channel();

        if let Err(err) = self
            .request_sender
            .send(PeerRequest {
                peer_message: crate::PeerMessage::Ping(PingMessage::from(request.into_inner())),
                response_sender: sender,
            })
            .await
        {
            error!(
                ?err,
                "error sending ping request message to request channel"
            );
            return Err(Status::internal(err.to_string()));
        }

        match receiver.await {
            Err(err) => {
                let msg = "ping request response channel sender closed unexpectedly";
                error!(?err, msg);
                return Err(Status::internal(msg));
            }
            Ok(result) => match result {
                Err(err) => {
                    error!(?err, "error handling ping request");
                    return Err(Status::internal(err.to_string()));
                }
                Ok(ack_message) => Ok(Response::new(corev1::AckResponse::from(ack_message))),
            },
        }
    }

    #[tracing::instrument(name = "DisseminationServer::ping_req", skip_all, fields(
        request = ?request
    ))]
    async fn ping_req(
        &self,
        request: Request<corev1::PingReqRequest>,
    ) -> Result<Response<corev1::AckResponse>, Status> {
        let (sender, receiver) = tokio::sync::oneshot::channel();

        if let Err(err) = self
            .request_sender
            .send(PeerRequest {
                peer_message: crate::PeerMessage::PingReq(PingReqMessage::from(
                    request.into_inner(),
                )),
                response_sender: sender,
            })
            .await
        {
            error!(
                ?err,
                "error sending ping req request message to request channel"
            );
            return Err(Status::internal(err.to_string()));
        }

        match receiver.await {
            Err(err) => {
                let msg = "ping req request response channel sender closed unexpectedly";
                error!(?err, msg);
                return Err(Status::internal(msg));
            }
            Ok(result) => match result {
                Err(err) => {
                    error!(?err, "error handling ping req request");
                    return Err(Status::internal(err.to_string()));
                }
                Ok(ack_message) => Ok(Response::new(corev1::AckResponse::from(ack_message))),
            },
        }
    }
}
