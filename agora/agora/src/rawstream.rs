use crate::utils::OrError;
use futures_util::{SinkExt, Stream, StreamExt};
use std::net::{Ipv6Addr, SocketAddr};
use tokio::net::TcpListener;
use tokio::sync::broadcast;
use tokio_stream::wrappers::BroadcastStream;
use tokio_tungstenite::{accept_async, connect_async, tungstenite::Message};

pub const DEFAULT_PORT: u16 = 8081;

pub struct RawStreamClient {
    receiver: broadcast::Receiver<Vec<u8>>,
}

impl RawStreamClient {
    pub fn new(address: Ipv6Addr, port: Option<u16>) -> OrError<Self> {
        let port = port.unwrap_or(DEFAULT_PORT);
        let (tx, rx) = broadcast::channel::<Vec<u8>>(1024);

        // Construct WebSocket URL for IPv6 address
        let addr_string = format!("ws://[{}]:{}", address, port);

        // Spawn background task for connection handling
        tokio::spawn(async move {
            // Question: why the loop here?? Does this allow us to reconnect after receiving an erroneous message?
            loop {
                match connect_async(&addr_string).await {
                    Ok((ws_stream, _)) => {
                        let (_, mut ws_receiver) = ws_stream.split();

                        // Forward messages from WebSocket to broadcast channel
                        // What happens if receiver.next().await is not Some ...? Do we just lose connection?
                        // What happens if we call "subscribe" then?
                        while let Some(msg) = ws_receiver.next().await {
                            if let Ok(Message::Binary(data)) = msg {
                                if tx.send(data.to_vec()).is_err() {
                                    break; // No more subscribers
                                }
                            }
                        }
                    }
                    Err(_) => {
                        // Retry connection after delay
                        tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                    }
                }
            }
        });
        // Question: what does the "capacity" specify?
        // What happens if the publisher keeps sending at a high rate, we create using new(), then before calling "subscribe" there're more than capacity messages pushed? Does this block the publisher?
        Ok(Self { receiver: rx })
    }

    pub fn subscribe(&self) -> BroadcastStream<Vec<u8>> {
        // What does it mean to resubscribe?
        BroadcastStream::new(self.receiver.resubscribe())
    }
}

pub struct RawStreamServer {
    address: Ipv6Addr,
    port: u16,
}

impl RawStreamServer {
    pub fn new(address: Ipv6Addr, port: Option<u16>) -> OrError<Self> {
        Ok(Self {
            address,
            port: port.unwrap_or(DEFAULT_PORT),
        })
    }

    pub async fn serve(
        &mut self,
        mut input_stream: impl Stream<Item = Vec<u8>> + Unpin + Send + 'static,
    ) -> OrError<()> {
        let addr = SocketAddr::new(self.address.into(), self.port);
        let listener = TcpListener::bind(&addr).await.map_err(|e| e.to_string())?;

        // Create broadcast channel to fan out input_stream to all clients
        let (tx, _) = broadcast::channel::<Vec<u8>>(1024);

        // Task 1: Ingestion - pull data from input_stream and broadcast it
        let ingest_tx = tx.clone();
        tokio::spawn(async move {
            // What happens if stream is None? Does this effectively signal EOF?
            while let Some(data) = input_stream.next().await {
                let _ = ingest_tx.send(data); // Ignore error if no clients
            }
        });

        // Task 2: Connection handling - accept new clients and serve them
        loop {
            let (tcp_stream, _) = listener.accept().await.map_err(|e| e.to_string())?;
            let mut client_rx = tx.subscribe();

            tokio::spawn(async move {
                // Don't understand any of this
                if let Ok(ws_stream) = accept_async(tcp_stream).await {
                    let (mut ws_sender, _) = ws_stream.split();

                    // Forward broadcast messages to this client
                    while let Ok(data) = client_rx.recv().await {
                        if ws_sender.send(Message::Binary(data.into())).await.is_err() {
                            break; // Client disconnected
                        }
                    }
                }
            });
        }
    }
}
