//! Typed publisher implementation: `Publisher<T>` registers with metaserver, creates dual endpoints (bytes + string),
//! and provides health check server. Handles registration sequence: register → create sockets → confirm.

use super::Agorable;
use crate::agora_error_cause;
use crate::metaserver::AgoraClient;
use crate::ping::PingServer;
use crate::rawstream::RawStreamServer;
use crate::utils::{ConnectionHandle, OrError, strip_and_verify};
use std::marker::PhantomData;

/// User interface for starting an Agora service.
/// User provides service `name`, `path`, `initial_value`, metaserver and gateway connections.
/// 1. **Assumes that Gateway is running** on the publisher host's specified port
/// 2. **Assumes connection to Metaserver** at `metaserver_connection`.
/// The publisher completes a handshake with metaserver by:
/// 1. Initiate a metaclient and register path with metaserver.
/// 2. Initiate a `PingServer` which responds with last values. Metaserver holds a `PingClient` and confirms the publisher upon successful pinging at `/tmp/agora/{path}/ping.sock`.
/// 3. Initiate `RawStreamServer`s for bytes and strings at `/tmp/agora/{path}/bytes/rawstream.sock` and `/tmp/agora/{path}/string/rawstream.sock`. These are relayed by the Gateway.
pub struct Publisher<T: Agorable> {
    rawstream_byteserver: RawStreamServer<Vec<u8>>,
    rawstream_omniserver: RawStreamServer<String>,
    pingserver: PingServer,
    _phantom: PhantomData<T>,
}

impl<T: Agorable> Publisher<T> {
    /// Creates publisher with registration sequence: register → create sockets → confirm.
    /// Creates three UDS WebSocket servers:
    /// - `/tmp/agora/{path}/bytes/rawstream.sock` (binary messages for `Subscriber<T>`)
    /// - `/tmp/agora/{path}/string/rawstream.sock` (string messages for `OmniSubscriber`)
    /// - `/tmp/agora/{path}/ping.sock` (health checks and current value queries)
    /// Error: Any step fails → propagates to user code.
    /// Called by: User code, `Relay::new`
    pub async fn new(
        name: String,
        path: String,
        initial_value: T,
        metaserver_connection: ConnectionHandle,
        local_gateway_port: u16,
    ) -> OrError<Self> {
        // Step 1: Connect to metaserver
        let metaclient = AgoraClient::new(metaserver_connection).await.map_err(|e| {
            agora_error_cause!("core::Publisher", "new", "failed to create AgoraClient", e)
        })?;

        // Step 2: Register with metaserver (adds path to registry, not yet confirmed)
        let publisher_info = metaclient
            .register_publisher(name, path.clone(), local_gateway_port)
            .await?;
        let _local_gateway_connection = *publisher_info.connection();

        let normalized_path = strip_and_verify(&path)?;

        // Step 3: Create UDS socket paths for dual endpoints (bytes + strings)
        let bytes_socket_path_str = format!("/tmp/agora/{}/bytes/rawstream.sock", normalized_path);
        let string_socket_path_str =
            format!("/tmp/agora/{}/string/rawstream.sock", normalized_path);

        let (vec_payload, str_payload) = Self::value_to_payloads(&initial_value)?;

        // Step 4a: Create ping server at base path
        let pingserver =
            PingServer::new(&normalized_path, vec_payload.clone(), str_payload.clone())
                .await
                .map_err(|e| {
                    agora_error_cause!("core::Publisher", "new", "failed to create ping server", e)
                })?;

        // Step 4b: Create binary rawstream server (for Subscriber\<T>)
        let rawstream_byteserver = RawStreamServer::new(&bytes_socket_path_str, None)
            .await
            .map_err(|e| {
                agora_error_cause!(
                    "core::Publisher",
                    "new",
                    "failed to create byte rawstream server",
                    e
                )
            })?;

        // Step 4c: Create string rawstream server (for OmniSubscriber)
        let rawstream_omniserver = RawStreamServer::new(&string_socket_path_str, None)
            .await
            .map_err(|e| {
                agora_error_cause!(
                    "core::Publisher",
                    "new",
                    "failed to create string rawstream server",
                    e
                )
            })?;

        // Step 5: Confirm publisher (metaserver pings to verify sockets are live)
        metaclient.confirm_publisher(&path).await.map_err(|e| {
            agora_error_cause!("core::Publisher", "new", "failed to confirm publisher", e)
        })?;

        Ok(Self {
            rawstream_byteserver,
            rawstream_omniserver,
            pingserver,
            _phantom: PhantomData,
        })
    }

    fn value_to_payloads(value: &T) -> OrError<(Vec<u8>, String)> {
        let vec_payload = postcard::to_allocvec(value).map_err(|e| {
            agora_error_cause!(
                "core::Publisher",
                "value_to_payloads",
                "failed to serialize value to bytes",
                e
            )
        })?;
        let str_payload = value.to_string();
        Ok((vec_payload, str_payload))
    }

    /// Publishes value to all endpoints (ping + binary stream + string stream).
    /// Updates ping server's current value, then broadcasts to all connected `Subscriber<T>` and `OmniSubscriber` instances.
    pub async fn publish(&mut self, value: T) -> OrError<()> {
        let (vec_payload, str_payload) = Self::value_to_payloads(&value)?;

        // Update ping server (for health checks and get() calls)
        self.pingserver.update_payload(&vec_payload, &str_payload);

        // Broadcast to binary subscribers (Subscriber\<T>)
        self.rawstream_byteserver.publish(vec_payload)?;

        // Broadcast to string subscribers (OmniSubscriber)
        self.rawstream_omniserver.publish(str_payload)?;

        Ok(())
    }
}
