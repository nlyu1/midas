use super::protocol::{DEFAULT_PORT, RawStreamer};
use crate::metaserver::publisher_info::PublisherInfo;
use crate::utils::OrError;
use futures::StreamExt;
use std::net::{IpAddr, Ipv6Addr};
use std::sync::Arc;
use tarpc::context;
use tokio::sync::broadcast;

use futures::{future, prelude::*};
use tarpc::{
    server::{self, Channel},
    tokio_serde::formats::Json,
};

#[derive(Clone)]
pub struct RawStreamServer {
    tx: broadcast::Sender<Vec<u8>>,
}

impl RawStreamer for RawStreamServer {
    async fn subscribe(self, _: context::Context) -> broadcast::Receiver<Vec<u8>> {
        self.tx.subscribe()
    }

    async fn ping(self, _: context::Context) -> String {
        "pong".to_string()
    }
}

impl RawStreamServer {
    pub fn new(buffer_size: usize) -> (Self, broadcast::Sender<Vec<u8>>) {
        let (tx, _) = broadcast::channel(buffer_size);
        let server = Self { tx: tx.clone() };
        (server, tx)
    }

    pub async fn run_server<S>(
        input_stream: S,
        publisher_info: PublisherInfo,
        buffer_size: Option<usize>,
    ) -> anyhow::Result<()>
    where
        S: futures::Stream<Item = Vec<u8>> + Send + 'static,
    {
        let buffer_size = buffer_size.unwrap_or(1024);
        let (server, tx) = Self::new(buffer_size);

        // Spawn background task to read from input stream and broadcast
        let mut input_stream = Box::pin(input_stream);
        tokio::spawn(async move {
            while let Some(data) = input_stream.next().await {
                if tx.send(data).is_err() {
                    // All receivers have been dropped
                    break;
                }
            }
        });

        let server_addr = publisher_info.socket_addr();
        println!("RawStream server listening on {}", server_addr);

        let mut listener = tarpc::serde_transport::tcp::listen(&server_addr, Json::default).await?;
        listener.config_mut().max_frame_length(usize::MAX);

        listener
            .filter_map(|r| future::ready(r.ok()))
            .map(server::BaseChannel::with_defaults)
            .max_channels_per_key(10, |t| t.transport().peer_addr().unwrap().ip())
            .map(|channel| {
                let server_instance = server.clone();
                channel
                    .execute(server_instance.serve())
                    .for_each(|fut| async {
                        tokio::spawn(fut);
                    })
            })
            .buffer_unordered(10)
            .for_each(|_| async {})
            .await;

        Ok(())
    }
}
