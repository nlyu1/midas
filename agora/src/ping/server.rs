use super::PingResponse;
use crate::utils::{OrError, prepare_socket_path};
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

pub struct PingServer {
    payload: Arc<RwLock<Payload>>,
    bg_handle: JoinHandle<()>,
    socket_path: String,
}

impl PingServer {
    pub async fn new(
        agora_path: &str,
        vec_payload: Vec<u8>,
        str_payload: String,
    ) -> OrError<Self> {
        let socket_path = format!("/tmp/agora/{}/ping.sock", agora_path);
        // Create parent directory
        prepare_socket_path(&socket_path)?;
        // Bind UDS
        let listener = UnixListener::bind(&socket_path).map_err(|e| {
            format!(
                "Agora PingServer creation error: cannot bind to socket {:?}. {}",
                socket_path, e
            )
        })?;

        let payload = Arc::new(RwLock::new(Payload {
            vec_payload,
            str_payload,
        }));
        let shared_payload = payload.clone();

        let bg_handle = tokio::spawn(async move {
            loop {
                if let Ok((stream, _)) = listener.accept().await {
                    let payload = shared_payload.clone();
                    tokio::spawn(async move {
                        if let Ok(ws_stream) = accept_async(stream).await {
                            let (mut write, mut read) = ws_stream.split();
                            // Wait for ping request
                            while let Some(msg) = read.next().await {
                                if let Ok(Message::Text(text)) = msg {
                                    if text == "ping" {
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

    pub fn update_payload(&mut self, vec_payload: Vec<u8>, str_payload: String) {
        let mut payload = self.payload.write().unwrap();
        payload.vec_payload = vec_payload;
        payload.str_payload = str_payload;
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
