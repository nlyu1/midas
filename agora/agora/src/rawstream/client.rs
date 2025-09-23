use super::protocol::RawStreamerClient;
use crate::utils::OrError;
use std::net::Ipv6Addr;
use tarpc::{client, context, tokio_serde::formats::Json};
use tokio::sync::broadcast;
use tokio_stream::wrappers::BroadcastStream;

pub struct RawStreamClient {
    client: RawStreamerClient,
}

impl RawStreamClient {
    pub async fn new(address: Ipv6Addr, port: u16) -> OrError<Self> {
        // Connect to the RawStreamer service
        let mut transport = tarpc::serde_transport::tcp::connect((address, port), Json::default);
        transport.config_mut().max_frame_length(usize::MAX);

        let client = RawStreamerClient::new(
            client::Config::default(),
            transport
                .await
                .map_err(|e| format!("Failed to connect to publisher: {}", e))?,
        )
        .spawn();

        Ok(Self { client })
    }

    pub async fn subscribe(&self) -> OrError<broadcast::Receiver<Vec<u8>>> {
        self.client.subscribe(context::current()).await?
        // After fixing compilation errors, should return OrError<Vec<u8> stream>. Process using BroadcastStream
    }
}
