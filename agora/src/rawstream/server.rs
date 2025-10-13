//! UDS WebSocket server for broadcasting messages to N clients.
//! `RawStreamServer<T>` uses dual-task architecture: ingestion (receives from `publish()`) + connection handler (fans out to clients via `tokio::broadcast`).

use crate::utils::{OrError, prepare_socket_path};
use crate::agora_error;
use anyhow::Context;
use futures_util::{SinkExt, StreamExt};
use tokio::net::UnixListener;
use tokio::sync::broadcast;
use tokio::task::JoinHandle;
use tokio_tungstenite::{accept_async, tungstenite::Message};

/// UDS WebSocket server that broadcasts messages to N clients via `tokio::broadcast`.
/// Two tasks: ingestion (receives from `publish()`) and connection handler (fans out to clients).
pub struct RawStreamServer<T>
where
    T: Clone + Send + 'static + Into<Vec<u8>> + TryFrom<Vec<u8>>,
    <T as TryFrom<Vec<u8>>>::Error: std::fmt::Display,
{
    sender: tokio::sync::mpsc::UnboundedSender<T>,
    ingest_handle: JoinHandle<()>,
    connection_handle: JoinHandle<()>,
    socket_path: String,
}

impl<T> RawStreamServer<T>
where
    T: Clone + Send + 'static + Into<Vec<u8>> + TryFrom<Vec<u8>>,
    <T as TryFrom<Vec<u8>>>::Error: std::fmt::Display,
{
    /// Creates UDS WebSocket server with broadcast to multiple clients.
    /// Architecture: Two async tasks share broadcast channel for 1-to-N fanout.
    /// Error: Socket bind fails → propagates to `Publisher::new`.
    /// Called by: `Publisher::new`
    pub async fn new(socket_path: &str, buffer_size: Option<usize>) -> OrError<Self> {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();

        // Create parent directory and clean up existing socket
        prepare_socket_path(socket_path)?;

        // Bind Unix domain socket listener
        let listener = UnixListener::bind(socket_path).context(agora_error!(
            "rawstream::RawStreamServer",
            "new",
            &format!("failed to bind Unix socket at {}", socket_path)
        ))?;
        // Create broadcast channel to fan out to all connected clients
        let buffer_capacity = buffer_size.unwrap_or(4096);
        let (broadcast_tx, _) = broadcast::channel::<T>(buffer_capacity);
        let mut input_stream = tokio_stream::wrappers::UnboundedReceiverStream::new(rx);

        // Task 1: Ingestion - receives data from publish() → broadcasts to all clients
        let ingest_tx = broadcast_tx.clone();
        let ingest_handle = tokio::spawn(async move {
            while let Some(data) = input_stream.next().await {
                let _ = ingest_tx.send(data); // Ignore error if no clients listening
            }
        });

        // Task 2: Connection handling - accepts new clients and spawns per-client tasks
        let connection_handle = tokio::spawn(async move {
            loop {
                if let Ok((unix_stream, _)) = listener.accept().await {
                    let mut client_rx = broadcast_tx.subscribe();
                    tokio::spawn(async move {
                        if let Ok(ws_stream) = accept_async(unix_stream).await {
                            let (mut ws_sender, _) = ws_stream.split();
                            // Forward broadcast messages to this specific client
                            while let Ok(data) = client_rx.recv().await {
                                if ws_sender
                                    .send(Message::Binary(data.into().into()))
                                    .await
                                    .is_err()
                                {
                                    // Client disconnected - this task exits, others unaffected
                                    break;
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
            socket_path: socket_path.to_string(),
        })
    }

    pub fn publish(&self, value: T) -> OrError<()> {
        self.sender
            .send(value)
            .map_err(|_| anyhow::anyhow!(agora_error!("rawstream::RawStreamServer", "publish", "channel closed")))
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
        if std::path::Path::new(&self.socket_path).exists() {
            let _ = std::fs::remove_file(&self.socket_path);
        }
    }
}
