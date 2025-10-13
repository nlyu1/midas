//! TCP-to-UDS WebSocket gateway enabling cross-node publisher access.
//! Routes external connections to local Unix domain sockets: `/rawstream/{path}` → `/tmp/agora/{path}/rawstream.sock`, `/ping/{path}` → `/tmp/agora/{path}/ping.sock`.

use crate::ConnectionHandle;
use crate::utils::OrError;
use anyhow::{bail, Context};
use futures_util::{SinkExt, StreamExt};
use local_ip_address::local_ip;
use tokio::net::{TcpListener, UnixStream};
use tokio::task::JoinHandle;
use tokio_tungstenite::tungstenite::handshake::server::{Request, Response};
use tokio_tungstenite::{accept_hdr_async, client_async};

/// TCP-to-UDS WebSocket proxy enabling cross-node publisher access.
/// Listens on TCP, routes requests to local UDS sockets based on URL path.
/// Routing: `/rawstream/{path}` → `/tmp/agora/{path}/rawstream.sock`, `/ping/{path}` → `/tmp/agora/{path}/ping.sock`
pub struct Gateway {
    connection: ConnectionHandle,
    task_handle: JoinHandle<()>,
}

impl Gateway {
    /// Creates TCP WebSocket gateway that proxies to local UDS endpoints.
    /// Network: Listens on TCP, accepts WebSocket connections, proxies to UDS.
    /// URL routing: `/rawstream/{path}` or `/ping/{path}` → `/tmp/agora/{path}/{service}.sock`
    /// Error: Bind fails → propagates to caller. Connection errors logged per-connection.
    /// Called by: User code (main gateway process)
    pub async fn new(port: u16) -> OrError<Self> {
        let ip = local_ip().context("Agora Gateway error: cannot get own ip")?;
        let connection = ConnectionHandle::new(ip, port);

        let addr = std::net::SocketAddr::new(ip, port);
        let listener = TcpListener::bind(&addr)
            .await
            .context(format!("Failed to bind gateway to {}", addr))?;

        eprintln!("Agora Gateway listening on {}", addr);

        // Accept loop: spawn per-connection handler tasks
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

// Handles single gateway connection: TCP WebSocket ↔ UDS WebSocket bidirectional proxy.
// URL routing determines UDS target, then forwards all messages in both directions.
async fn handle_connection(tcp_stream: tokio::net::TcpStream) -> OrError<()> {
    let mut agora_path = String::new();
    let mut service_type = String::new();

    // Accept WebSocket, extract path from URL during handshake
    let ws_stream = accept_hdr_async(tcp_stream, |req: &Request, response: Response| {
        let path = req.uri().path();

        // URL routing: map external path to UDS socket path
        if let Some(stripped) = path.strip_prefix("/rawstream/") {
            // /rawstream/{path} → /tmp/agora/{path}/rawstream.sock
            agora_path = stripped.to_string();
            service_type = "rawstream".to_string();
            Ok(response)
        } else if let Some(stripped) = path.strip_prefix("/ping/") {
            // /ping/{path} → /tmp/agora/{path}/ping.sock
            agora_path = stripped.to_string();
            service_type = "ping".to_string();
            Ok(response)
        } else {
            // Invalid URL - reject with 400
            eprintln!("Gateway: invalid path format: {}", path);
            Err(tokio_tungstenite::tungstenite::http::Response::builder()
                .status(400)
                .body(Some(format!("Invalid path: {}", path)))
                .unwrap())
        }
    })
    .await
    .context("WebSocket upgrade failed")?;

    if agora_path.is_empty() || service_type.is_empty() {
        bail!("Failed to extract path from request");
    }

    // Connect to local UDS endpoint
    let uds_path = format!("/tmp/agora/{}/{}.sock", agora_path, service_type);
    let unix_stream = UnixStream::connect(&uds_path)
        .await
        .context(format!("Failed to connect to UDS {}", uds_path))?;

    // Upgrade UDS to WebSocket
    let (uds_ws_stream, _) = client_async("ws://localhost/", unix_stream)
        .await
        .context("Failed to upgrade UDS to WebSocket")?;

    // Split both WebSocket streams for bidirectional forwarding
    let (mut ext_write, mut ext_read) = ws_stream.split();
    let (mut int_write, mut int_read) = uds_ws_stream.split();

    // Task 1: External → Internal forwarding
    let ext_to_int = tokio::spawn(async move {
        while let Some(msg) = ext_read.next().await {
            match msg {
                Ok(msg) => {
                    if int_write.send(msg).await.is_err() {
                        break; // UDS disconnected
                    }
                }
                Err(_) => break, // External connection error
            }
        }
    });

    // Task 2: Internal → External forwarding
    let int_to_ext = tokio::spawn(async move {
        while let Some(msg) = int_read.next().await {
            match msg {
                Ok(msg) => {
                    if ext_write.send(msg).await.is_err() {
                        break; // External disconnected
                    }
                }
                // UDS error
                Err(_) => {
                    break;
                }
            }
        }
    });

    // Wait for either direction to close - then terminate both
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
