use crate::ConnectionHandle;
use crate::utils::OrError;
use chrono::Utc;
use futures_util::{SinkExt, StreamExt};
use std::path::Path;
use tokio::net::UnixListener;
use tokio::sync::broadcast;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::BroadcastStream;
use tokio_tungstenite::{accept_async, connect_async, tungstenite::Message};

/// A WebSocket client that connects to a RawStreamServer and receives messages.
pub struct RawStreamClient<T>
where
    T: Clone + Send + 'static + Into<Vec<u8>> + TryFrom<Vec<u8>>,
    <T as TryFrom<Vec<u8>>>::Error: std::fmt::Display,
{
    receiver: broadcast::Receiver<T>,
    bg_handle: JoinHandle<()>,
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
    pub fn new(
        host_gateway: ConnectionHandle,
        socket_path: &Path,
        poll_connection_every_ms: Option<u64>,
        buffer_size: Option<usize>,
    ) -> OrError<Self> {
        let poll_interval = poll_connection_every_ms.unwrap_or(100);
        let buffer_capacity = buffer_size.unwrap_or(4096);
        let (tx, rx) = broadcast::channel::<T>(buffer_capacity);

        // Construct WebSocket URL: ws://host:port/rawstream/{path}
        // Gateway will map this to /tmp/agora/{path}/rawstream.sock
        let addr_string = format!("ws://{}/rawstream/{}", host_gateway, socket_path.to_string_lossy());

        // Spawn background task for connection handling
        let bg_handle = tokio::spawn(async move {
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
        Ok(Self {
            receiver: rx,
            bg_handle,
        })
    }

    /// Creates a new independent stream of messages from this client.
    /// Multiple subscribers can consume the same messages independently.
    pub fn subscribe(&self) -> BroadcastStream<T> {
        BroadcastStream::new(self.receiver.resubscribe())
    }
}

impl<T> Drop for RawStreamClient<T>
where
    T: Clone + Send + 'static + Into<Vec<u8>> + TryFrom<Vec<u8>>,
    <T as TryFrom<Vec<u8>>>::Error: std::fmt::Display,
{
    fn drop(&mut self) {
        self.bg_handle.abort();
    }
}

pub struct RawStreamServer<T>
where
    T: Clone + Send + 'static + Into<Vec<u8>> + TryFrom<Vec<u8>>,
    <T as TryFrom<Vec<u8>>>::Error: std::fmt::Display,
{
    sender: tokio::sync::mpsc::UnboundedSender<T>,
    ingest_handle: JoinHandle<()>,
    connection_handle: JoinHandle<()>,
    socket_path: std::path::PathBuf,
}

impl<T> RawStreamServer<T>
where
    T: Clone + Send + 'static + Into<Vec<u8>> + TryFrom<Vec<u8>>,
    <T as TryFrom<Vec<u8>>>::Error: std::fmt::Display,
{
    pub async fn new(socket_path: &Path, buffer_size: Option<usize>) -> OrError<Self> {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();

        // Create parent directory if it doesn't exist
        if let Some(parent) = socket_path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| {
                format!(
                    "Failed to create socket directory {}: {}",
                    parent.display(),
                    e
                )
            })?;
        }
        // Remove existing socket file if present (from previous unclean shutdown)
        if socket_path.exists() {
            std::fs::remove_file(socket_path).map_err(|e| {
                format!(
                    "Failed to remove existing socket {}: {}",
                    socket_path.display(),
                    e
                )
            })?;
        }
        // Bind Unix domain socket listener at the specified path
        let listener = UnixListener::bind(socket_path).map_err(|e| {
            format!(
                "Failed to bind Unix socket {}: {}",
                socket_path.display(),
                e
            )
        })?;
        // Create broadcast channel to fan out input_stream to all clients
        let buffer_capacity = buffer_size.unwrap_or(4096);
        let (broadcast_tx, _) = broadcast::channel::<T>(buffer_capacity);
        // Convert receiver to stream for ingestion
        let mut input_stream = tokio_stream::wrappers::UnboundedReceiverStream::new(rx);
        // Task 1: Ingestion - pull data from input_stream and broadcast it
        let ingest_tx = broadcast_tx.clone();
        let ingest_handle = tokio::spawn(async move {
            while let Some(data) = input_stream.next().await {
                let _ = ingest_tx.send(data); // Ignore error if no clients
            }
        });
        // Task 2: Connection handling - accept new clients and serve them
        let connection_handle = tokio::spawn(async move {
            loop {
                if let Ok((unix_stream, _)) = listener.accept().await {
                    let mut client_rx = broadcast_tx.subscribe();
                    tokio::spawn(async move {
                        if let Ok(ws_stream) = accept_async(unix_stream).await {
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

        Ok(Self {
            sender: tx,
            ingest_handle,
            connection_handle,
            socket_path: socket_path.to_path_buf(),
        })
    }

    pub fn publish(&self, value: T) -> OrError<()> {
        self.sender
            .send(value)
            .map_err(|_| "Channel closed".to_string())
    }
}

impl<T> Drop for RawStreamServer<T>
where
    T: Clone + Send + 'static + Into<Vec<u8>> + TryFrom<Vec<u8>>,
    <T as TryFrom<Vec<u8>>>::Error: std::fmt::Display,
{
    fn drop(&mut self) {
        self.ingest_handle.abort();
        self.connection_handle.abort();

        // Clean up socket file
        if self.socket_path.exists() {
            let _ = std::fs::remove_file(&self.socket_path);
        }
    }
}
