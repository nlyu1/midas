use super::common::PingRpcClient;
use crate::utils::OrError;
use chrono::TimeDelta;
use std::net::Ipv6Addr;
use tarpc::{client, context, tokio_serde::formats::Json};

pub struct PingClient {
    client: PingRpcClient,
}

impl PingClient {
    pub async fn new(address: Ipv6Addr, port: u16) -> anyhow::Result<Self> {
        let server_addr = (address, port);
        let mut transport = tarpc::serde_transport::tcp::connect(server_addr, Json::default);
        transport.config_mut().max_frame_length(usize::MAX);
        let client = PingRpcClient::new(client::Config::default(), transport.await?).spawn();
        Ok(Self { client })
    }

    pub async fn ping(&self) -> OrError<(Vec<u8>, String, TimeDelta)> {
        let (vec, s, transmit_time) = self
            .client
            .ping_latest_value(context::current())
            .await
            .map_err(|e| format!("Heartbeat Rpc error: {}", e))?;
        let time_delta = chrono::Utc::now().signed_duration_since(transmit_time);
        Ok((vec, s, time_delta))
    }
}
