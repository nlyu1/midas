use crate::ConnectionHandle;
use crate::utils::OrError;
use futures_util::{SinkExt, StreamExt};
use local_ip_address::local_ip;
use std::path::PathBuf;
use tokio::net::{TcpListener, UnixStream};
use tokio::task::JoinHandle;
use tokio_tungstenite::tungstenite::handshake::server::{Request, Response};
use tokio_tungstenite::{accept_hdr_async, client_async};

pub struct Gateway {
    connection: ConnectionHandle,
    task_handle: JoinHandle<()>,
}

impl Gateway {
    pub async fn new(port: u16) -> OrError<Self> {
        let ip = local_ip().map_err(|e| format!("Agora Gateway error: cannot get own ip {}", e))?;
        let connection = ConnectionHandle::new(ip, port);

        // Bind TCP listener
        let addr = std::net::SocketAddr::new(ip, port);
        let listener = TcpListener::bind(&addr)
            .await
            .map_err(|e| format!("Failed to bind gateway to {}: {}", addr, e))?;

        eprintln!("Agora Gateway listening on {}", addr);

        // Spawn accept loop
        let task_handle = tokio::spawn(async move {
            loop {
                match listener.accept().await {
                    Ok((tcp_stream, peer_addr)) => {
                        tokio::spawn(async move {
                            if let Err(e) = handle_connection(tcp_stream).await {
                                eprintln!("Gateway connection from {} error: {}", peer_addr, e);
                            }
                        });
                    }
                    Err(e) => {
                        eprintln!("Gateway accept error: {}", e);
                    }
                }
            }
        });

        Ok(Self {
            connection,
            task_handle,
        })
    }

    pub fn connection(&self) -> &ConnectionHandle {
        &self.connection
    }
}

async fn handle_connection(tcp_stream: tokio::net::TcpStream) -> OrError<()> {
    let mut agora_path = String::new();

    // Accept WebSocket with custom header callback to extract path
    let ws_stream = accept_hdr_async(tcp_stream, |req: &Request, response: Response| {
        let path = req.uri().path();
        // Path parsing: {addr}:{port}/rawstream/{path} gets mapped to /tmp/agora/{path}/rawstream.sock

        // Expected format: /rawstream/{path}
        if let Some(stripped) = path.strip_prefix("/rawstream/") {
            agora_path = stripped.to_string();
            Ok(response)
        } else {
            eprintln!("Gateway: invalid path format: {}", path);
            Err(tokio_tungstenite::tungstenite::http::Response::builder()
                .status(400)
                .body(Some(format!("Invalid path: {}", path)))
                .unwrap())
        }
    })
    .await
    .map_err(|e| format!("WebSocket upgrade failed: {}", e))?;

    if agora_path.is_empty() {
        return Err("Failed to extract path from request".to_string());
    }

    // Connect to UDS at /tmp/agora/{path}/rawstream.sock
    let uds_path = PathBuf::from(format!("/tmp/agora/{}/rawstream.sock", agora_path));
    let unix_stream = UnixStream::connect(&uds_path)
        .await
        .map_err(|e| format!("Failed to connect to UDS {}: {}", uds_path.display(), e))?;

    // Upgrade UDS connection to WebSocket
    let (uds_ws_stream, _) = client_async("ws://localhost/", unix_stream)
        .await
        .map_err(|e| format!("Failed to upgrade UDS to WebSocket: {}", e))?;

    // Split both streams
    let (mut ext_write, mut ext_read) = ws_stream.split();
    let (mut int_write, mut int_read) = uds_ws_stream.split();

    // Bidirectional forwarding
    let ext_to_int = tokio::spawn(async move {
        while let Some(msg) = ext_read.next().await {
            match msg {
                Ok(msg) => {
                    if int_write.send(msg).await.is_err() {
                        break;
                    }
                }
                Err(_) => break,
            }
        }
    });

    let int_to_ext = tokio::spawn(async move {
        while let Some(msg) = int_read.next().await {
            match msg {
                Ok(msg) => {
                    if ext_write.send(msg).await.is_err() {
                        break;
                    }
                }
                Err(_) => break,
            }
        }
    });

    // Wait for either direction to complete
    tokio::select! {
        _ = ext_to_int => {},
        _ = int_to_ext => {},
    }

    Ok(())
}

impl Drop for Gateway {
    fn drop(&mut self) {
        self.task_handle.abort();
    }
}
