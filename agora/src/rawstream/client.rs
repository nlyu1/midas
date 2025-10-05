use crate::ConnectionHandle;
use crate::utils::OrError;
use chrono::Utc;
use futures_util::StreamExt;
use tokio::sync::broadcast;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::BroadcastStream;
use tokio_tungstenite::{connect_async, tungstenite::Message};

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
        socket_path: &str,
        poll_connection_every_ms: Option<u64>,
        buffer_size: Option<usize>,
    ) -> OrError<Self> {
        let poll_interval = poll_connection_every_ms.unwrap_or(100);
        let buffer_capacity = buffer_size.unwrap_or(4096);
        let (tx, rx) = broadcast::channel::<T>(buffer_capacity);

        // Construct WebSocket URL: ws://host:port/rawstream/{path}
        // Gateway will map this to /tmp/agora/{path}/rawstream.sock
        let addr_string = format!(
            "ws://{}/rawstream/{}",
            host_gateway,
            socket_path
        );

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
