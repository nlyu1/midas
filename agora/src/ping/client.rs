//! WebSocket ping client for publisher health checks and current value queries.
//! `PingClient` connects via gateway, sends `"ping"` text, receives JSON with binary/string payload + timestamp. Used by metaserver pruning and subscribers.

use super::PingResponse;
use crate::utils::OrError;
use crate::ConnectionHandle;
use crate::agora_error;
use anyhow::{bail, Context};
use chrono::TimeDelta;
use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt};
use std::fmt;
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{connect_async, MaybeTlsStream, WebSocketStream};

type WsSink = SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>;
type WsStream = SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>;

/// WebSocket ping client for health checks and current value queries via gateway.
/// Protocol: Sends `"ping"` text → receives JSON with binary payload, string payload, timestamp.
/// Network: Connects to `ws://gateway/ping/{path}` → proxies to `/tmp/agora/{path}/ping.sock`
pub struct PingClient {
    ws_write: WsSink,
    ws_read: WsStream,
}

impl fmt::Debug for PingClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PingClient").finish()
    }
}

impl PingClient {
    /// Creates WebSocket ping client to publisher via gateway.
    /// Network: `ws://gateway/ping/{path}` → `/tmp/agora/{path}/ping.sock`
    /// Error: Connection fails → propagates to `Subscriber::new`, `ServerState::confirm_publisher`.
    /// Called by: `Subscriber::new`, `OmniSubscriber::new`, `ServerState::confirm_publisher`/`prune_stale_publishers`
    pub async fn new(
        agora_path: &str,
        gateway_connection: ConnectionHandle,
    ) -> OrError<Self> {
        let url = format!(
            "ws://{}/ping/{}",
            gateway_connection,
            agora_path
        );

        let (ws_stream, _) = connect_async(&url)
            .await
            .context(agora_error!(
                "ping::PingClient",
                "new",
                &format!("failed to connect to {}", url)
            ))?;
        let (ws_write, ws_read) = ws_stream.split();

        Ok(Self { ws_write, ws_read })
    }

    /// Sends `"ping"` text message, receives JSON response with current value and timestamp.
    /// Returns `(binary_payload, string_payload, round_trip_time)`.
    /// Error: Send/receive fails or connection closed → propagates to `Subscriber::get`, metaserver pruning.
    pub async fn ping(&mut self) -> OrError<(Vec<u8>, String, TimeDelta)> {
        // Send ping request
        self.ws_write
            .send(Message::Text("ping".to_string().into()))
            .await
            .context(agora_error!("ping::PingClient", "ping", "failed to send ping"))?;

        // Wait for response
        if let Some(msg) = self.ws_read.next().await {
            match msg {
                Ok(Message::Text(json)) => {
                    let response: PingResponse = serde_json::from_str(&json).context(agora_error!(
                        "ping::PingClient",
                        "ping",
                        "failed to parse response"
                    ))?;

                    let time_delta = chrono::Utc::now().signed_duration_since(response.timestamp);

                    Ok((response.vec_payload, response.str_payload, time_delta))
                }
                Ok(_) => bail!(agora_error!("ping::PingClient", "ping", "unexpected message type")),
                Err(e) => Err(e).context(agora_error!("ping::PingClient", "ping", "WebSocket error")),
            }
        } else {
            bail!(agora_error!("ping::PingClient", "ping", "connection closed"));
        }
    }
}
