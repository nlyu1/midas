use crate::utils::{OrError, prepare_socket_path};
use futures_util::{SinkExt, StreamExt};
use tokio::net::UnixListener;
use tokio::sync::broadcast;
use tokio::task::JoinHandle;
use tokio_tungstenite::{accept_async, tungstenite::Message};

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
    pub async fn new(socket_path: &str, buffer_size: Option<usize>) -> OrError<Self> {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();

        // Create parent directory and clean up existing socket
        prepare_socket_path(socket_path)?;

        // Bind Unix domain socket listener at the specified path
        let listener = UnixListener::bind(socket_path).map_err(|e| {
            format!(
                "Failed to bind Unix socket {}: {}",
                socket_path,
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
            socket_path: socket_path.to_string(),
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
        if std::path::Path::new(&self.socket_path).exists() {
            let _ = std::fs::remove_file(&self.socket_path);
        }
    }
}
