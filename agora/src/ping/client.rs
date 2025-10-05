use super::PingResponse;
use crate::utils::OrError;
use crate::ConnectionHandle;
use chrono::TimeDelta;
use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt};
use std::fmt;
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{connect_async, MaybeTlsStream, WebSocketStream};

type WsSink = SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>;
type WsStream = SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>;

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
            .map_err(|e| format!("Agora PingClient creation error: failed to connect to {}: {}", url, e))?;
        let (ws_write, ws_read) = ws_stream.split();

        Ok(Self { ws_write, ws_read })
    }

    pub async fn ping(&mut self) -> OrError<(Vec<u8>, String, TimeDelta)> {
        // Send ping request
        self.ws_write
            .send(Message::Text("ping".to_string().into()))
            .await
            .map_err(|e| format!("Failed to send ping: {}", e))?;

        // Wait for response
        if let Some(msg) = self.ws_read.next().await {
            match msg {
                Ok(Message::Text(json)) => {
                    let response: PingResponse = serde_json::from_str(&json)
                        .map_err(|e| format!("Failed to parse response: {}", e))?;

                    let time_delta = chrono::Utc::now().signed_duration_since(response.timestamp);

                    Ok((response.vec_payload, response.str_payload, time_delta))
                }
                Ok(_) => Err("Unexpected message type".to_string()),
                Err(e) => Err(format!("WebSocket error: {}", e)),
            }
        } else {
            Err("Connection closed".to_string())
        }
    }
}
