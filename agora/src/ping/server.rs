//! UDS WebSocket server for ping-pong protocol with current value responses.
//! `PingServer` listens on UDS, responds to `"ping"` with JSON containing binary/string payload + timestamp. Thread-safe payload updates via `RwLock`.

use super::PingResponse;
use crate::utils::{OrError, prepare_socket_path};
use crate::agora_error;
use anyhow::Context;
use chrono::Utc;
use futures_util::{SinkExt, StreamExt};
use std::sync::{Arc, RwLock};
use tokio::net::UnixListener;
use tokio::task::JoinHandle;
use tokio_tungstenite::{accept_async, tungstenite::Message};

#[derive(Clone)]
struct Payload {
    vec_payload: Vec<u8>,
    str_payload: String,
}

/// UDS WebSocket server responding to `"ping"` with JSON payload (current value + timestamp).
/// Protocol: Receives `"ping"` text → sends JSON `PingResponse` with `vec_payload`, `str_payload`, `timestamp`.
/// Payload updates: Thread-safe via `RwLock`, shared across all connection handlers.
pub struct PingServer {
    payload: Arc<RwLock<Payload>>,
    bg_handle: JoinHandle<()>,
    socket_path: String,
}

impl PingServer {
    /// Creates UDS WebSocket server for ping-pong protocol.
    /// Responds to `"ping"` text messages with JSON containing current value and timestamp.
    /// Error: Socket bind fails → propagates to `Publisher::new`.
    /// Called by: `Publisher::new`
    pub async fn new(
        agora_path: &str,
        vec_payload: Vec<u8>,
        str_payload: String,
    ) -> OrError<Self> {
        let socket_path = format!("/tmp/agora/{}/ping.sock", agora_path);
        prepare_socket_path(&socket_path)?;

        let listener = UnixListener::bind(&socket_path).context(agora_error!(
            "ping::PingServer",
            "new",
            &format!("failed to bind socket at {}", socket_path)
        ))?;

        let payload = Arc::new(RwLock::new(Payload {
            vec_payload,
            str_payload,
        }));
        let shared_payload = payload.clone();

        // Background task: accept connections and respond to pings
        let bg_handle = tokio::spawn(async move {
            loop {
                if let Ok((stream, _)) = listener.accept().await {
                    let payload = shared_payload.clone();
                    tokio::spawn(async move {
                        if let Ok(ws_stream) = accept_async(stream).await {
                            let (mut write, mut read) = ws_stream.split();
                            while let Some(msg) = read.next().await {
                                // Protocol: receive "ping" → send JSON response
                                if let Ok(Message::Text(text)) = msg
                                    && text == "ping" {
                                    let response = {
                                        let p = payload.read().unwrap();
                                        PingResponse {
                                            vec_payload: p.vec_payload.clone(),
                                            str_payload: p.str_payload.clone(),
                                            timestamp: Utc::now(),
                                        }
                                    };
                                    if let Ok(json) = serde_json::to_string(&response) {
                                        let _ = write.send(Message::Text(json.into())).await;
                                    }
                                }
                            }
                        }
                    });
                }
            }
        });

        Ok(Self {
            payload,
            bg_handle,
            socket_path,
        })
    }

    /// Updates current payload under `RwLock`. Called by `Publisher::publish()`.
    pub fn update_payload(&mut self, vec_payload: &[u8], str_payload: &str) {
        let mut payload = self.payload.write().unwrap();
        payload.vec_payload = vec_payload.to_vec();
        payload.str_payload = str_payload.to_string();
    }
}

impl Drop for PingServer {
    fn drop(&mut self) {
        self.bg_handle.abort();
        if std::path::Path::new(&self.socket_path).exists() {
            let _ = std::fs::remove_file(&self.socket_path);
        }
    }
}
