use crate::utils::OrError;
use chrono::Utc;
use futures_util::{SinkExt, StreamExt};
use std::net::{Ipv6Addr, SocketAddr};
use tokio::net::TcpListener;
use tokio::sync::broadcast;
use tokio_stream::wrappers::BroadcastStream;
use tokio_tungstenite::{accept_async, connect_async, tungstenite::Message};

/// A WebSocket client that connects to a RawStreamServer and receives messages.
pub struct RawStreamClient<T>
where
    T: Clone + Send + 'static + Into<Vec<u8>> + TryFrom<Vec<u8>>,
    <T as TryFrom<Vec<u8>>>::Error: std::fmt::Display,
{
    receiver: broadcast::Receiver<T>,
}

impl<T> RawStreamClient<T>
where
    T: Clone + Send + 'static + Into<Vec<u8>> + TryFrom<Vec<u8>>,
    <T as TryFrom<Vec<u8>>>::Error: std::fmt::Display,
{
    fn log_error(message: &str) {
        let timestamp = Utc::now().format("%H:%M:%S");
        eprintln!("RawStreamClient error {}: {}", timestamp, message);
    }
    /// Creates a new WebSocket client that connects to the specified server.
    ///
    /// # Parameters
    ///
    /// - `address`: IPv6 address of the server to connect to
    /// - `port`: Port number of the server
    /// - `poll_connection_every_ms`: How often to retry connection attempts (default: 100ms)
    /// - `buffer_size`: Maximum number of messages to buffer (default: 1024)
    ///
    /// # Returns
    ///
    /// A client instance that immediately starts attempting to connect in the background.
    /// Use `subscribe()` to get a stream of messages.
    pub fn new(
        address: Ipv6Addr,
        port: u16,
        poll_connection_every_ms: Option<u64>,
        buffer_size: Option<usize>,
    ) -> OrError<Self> {
        let poll_interval = poll_connection_every_ms.unwrap_or(100);
        let buffer_capacity = buffer_size.unwrap_or(1024);
        let (tx, rx) = broadcast::channel::<T>(buffer_capacity);

        // Construct WebSocket URL for IPv6 address
        let addr_string = format!("ws://[{}]:{}", address, port);

        // Spawn background task for connection handling
        tokio::spawn(async move {
            loop {
                // Main reconnection loop - runs indefinitely
                match connect_async(&addr_string).await {
                    // Branch: Try to establish WebSocket connection
                    Ok((ws_stream, _)) => {
                        // SUCCESS: WebSocket connection established
                        let (_, mut ws_receiver) = ws_stream.split();
                        loop {
                            // Message processing loop - runs until connection drops
                            match ws_receiver.next().await {
                                // Branch: Wait for next message from WebSocket
                                Some(Ok(Message::Binary(data))) => {
                                    match T::try_from(data.to_vec()) {
                                        Ok(converted) => {
                                            if tx.send(converted).is_err() {
                                                Self::log_error("broadcast channel closed");
                                                return; // Exit task
                                            }
                                        }
                                        // ERROR: Invalid data for type T (e.g., invalid UTF-8 for String)
                                        Err(conversion_err) => {
                                            Self::log_error(&format!(
                                                "invalid message data: {}",
                                                conversion_err
                                            ));
                                        }
                                    }
                                }
                                Some(Ok(msg)) => {
                                    Self::log_error(&format!(
                                        "received non-binary message from WebSocket: {:?}",
                                        msg
                                    ));
                                }
                                Some(Err(ws_error)) => {
                                    Self::log_error(&format!(
                                        "WebSocket protocol error: {}",
                                        ws_error
                                    ));
                                    break;
                                }
                                None => {
                                    Self::log_error("connection closed by server");
                                    break;
                                }
                            }
                        }
                    }
                    Err(connect_error) => {
                        Self::log_error(&format!(
                            "failed to connect to {}: {}",
                            addr_string, connect_error
                        ));
                    }
                }
                Self::log_error(&format!(
                    "retrying connection to {} in {}ms",
                    addr_string, poll_interval
                ));
                // Wait before retry (for both connection failures and disconnections)
                tokio::time::sleep(tokio::time::Duration::from_millis(poll_interval)).await;
            }
        });
        Ok(Self { receiver: rx })
    }

    /// Creates a new independent stream of messages from this client.
    /// Multiple subscribers can consume the same messages independently.
    pub fn subscribe(&self) -> BroadcastStream<T> {
        BroadcastStream::new(self.receiver.resubscribe())
    }
}

pub struct RawStreamServer<T>
where
    T: Clone + Send + 'static + Into<Vec<u8>> + TryFrom<Vec<u8>>,
    <T as TryFrom<Vec<u8>>>::Error: std::fmt::Display,
{
    sender: tokio::sync::mpsc::UnboundedSender<T>,
}

impl<T> RawStreamServer<T>
where
    T: Clone + Send + 'static + Into<Vec<u8>> + TryFrom<Vec<u8>>,
    <T as TryFrom<Vec<u8>>>::Error: std::fmt::Display,
{
    pub async fn new(address: Ipv6Addr, port: u16, buffer_size: Option<usize>) -> OrError<Self> {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();

        // Set up server address and bind listener
        let addr = SocketAddr::new(address.into(), port);
        let listener = TcpListener::bind(&addr).await.map_err(|e| e.to_string())?;

        // Create broadcast channel to fan out input_stream to all clients
        let buffer_capacity = buffer_size.unwrap_or(1024);
        let (broadcast_tx, _) = broadcast::channel::<T>(buffer_capacity);

        // Convert receiver to stream for ingestion
        let mut input_stream = tokio_stream::wrappers::UnboundedReceiverStream::new(rx);

        // Task 1: Ingestion - pull data from input_stream and broadcast it
        let ingest_tx = broadcast_tx.clone();
        tokio::spawn(async move {
            while let Some(data) = input_stream.next().await {
                let _ = ingest_tx.send(data); // Ignore error if no clients
            }
        });

        // Task 2: Connection handling - accept new clients and serve them
        tokio::spawn(async move {
            loop {
                if let Ok((tcp_stream, _)) = listener.accept().await {
                    let mut client_rx = broadcast_tx.subscribe();

                    tokio::spawn(async move {
                        if let Ok(ws_stream) = accept_async(tcp_stream).await {
                            let (mut ws_sender, _) = ws_stream.split();

                            // Forward broadcast messages to this client
                            while let Ok(data) = client_rx.recv().await {
                                if ws_sender
                                    .send(Message::Binary(data.into().into()))
                                    .await
                                    .is_err()
                                {
                                    break; // Client disconnected
                                }
                            }
                        }
                    });
                }
            }
        });

        Ok(Self { sender: tx })
    }

    pub fn publish(&self, value: T) -> OrError<()> {
        self.sender
            .send(value)
            .map_err(|_| "Channel closed".to_string())
    }
}
