// Potential future improvements: implement cancellation & shutdowns gracefully.

use super::common::PingRpc;
use chrono::{DateTime, Utc};
use derive_new;
use futures_util::StreamExt;
use std::net::IpAddr;
use std::sync::{Arc, RwLock};
use tarpc::{
    context,
    server::{self, Channel, incoming::Incoming},
    tokio_serde::formats::Json,
};

#[derive(derive_new::new)]
pub struct Payload {
    vec_payload: Vec<u8>,
    str_payload: String,
}

#[derive(derive_new::new, Clone)]
pub struct PingRpcServer {
    payload: Arc<RwLock<Payload>>,
}

impl PingRpc for PingRpcServer {
    async fn ping_latest_value(self, _: context::Context) -> (Vec<u8>, String, DateTime<Utc>) {
        let payload = self.payload.read().unwrap();
        (
            payload.vec_payload.clone(),
            payload.str_payload.clone(),
            chrono::Utc::now(),
        )
    }
}

pub struct PingServer {
    payload: Arc<RwLock<Payload>>,
}

impl PingServer {
    pub fn new(
        addr: IpAddr,
        port: u16,
        vec_payload: Vec<u8>,
        str_payload: String,
    ) -> anyhow::Result<Self> {
        let payload = Arc::new(RwLock::new(Payload::new(vec_payload, str_payload)));
        let shared_payload = payload.clone();

        // Spawn a separate thread to launch the thread which handles connections & broadcasts values.
        let background_connection = async move {
            let server_addr = (addr, port);
            let mut listener = tarpc::serde_transport::tcp::listen(&server_addr, Json::default)
                .await
                .unwrap();
            listener.config_mut().max_frame_length(usize::MAX);
            listener
                .filter_map(|r| futures::future::ready(r.ok()))
                .map(server::BaseChannel::with_defaults)
                .max_channels_per_key(1024 * 1024, |t| t.transport().peer_addr().unwrap().ip())
                .map(|channel| {
                    let server = PingRpcServer::new(Arc::clone(&shared_payload));
                    channel.execute(server.serve()).for_each(|fut| async {
                        fut.await;
                    })
                })
                .buffer_unordered(4096)
                .for_each(|_| async {})
                .await;
        };
        tokio::spawn(background_connection);

        Ok(Self { payload })
    }

    pub fn update_payload(self: &mut Self, vec_payload: Vec<u8>, str_payload: String) -> () {
        let mut payload = self.payload.write().unwrap();
        payload.vec_payload = vec_payload;
        payload.str_payload = str_payload;
    }
}
