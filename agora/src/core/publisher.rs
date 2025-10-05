use super::Agorable;
use crate::metaserver::AgoraClient;
use crate::ping::PingServer;
use crate::rawstream::RawStreamServer;
use crate::utils::{ConnectionHandle, OrError, strip_and_verify};
use std::marker::PhantomData;
pub struct Publisher<T: Agorable> {
    _metaclient: AgoraClient,
    rawstream_byteserver: RawStreamServer<Vec<u8>>,
    rawstream_omniserver: RawStreamServer<String>,
    pingserver: PingServer,
    _phantom: PhantomData<T>,
}

impl<T: Agorable> Publisher<T> {
    pub async fn new(
        name: String,
        path: String,
        initial_value: T,
        metaserver_connection: ConnectionHandle,
        local_gateway_port: u16,
    ) -> OrError<Self> {
        let metaclient = AgoraClient::new(metaserver_connection).await.map_err(|e| {
            format!(
                "Agora subscriber error: failed to create AgoraClient: {}",
                e
            )
        })?;
        let publisher_info = metaclient
            .register_publisher(name, path.clone(), local_gateway_port)
            .await?;
        let _local_gateway_connection = publisher_info.connection();

        // Strip and verify path
        let normalized_path = strip_and_verify(&path)?;

        // Create socket path strings with suffixes for rawstream servers
        let bytes_socket_path_str = format!("/tmp/agora/{}/bytes/rawstream.sock", normalized_path);
        let string_socket_path_str = format!("/tmp/agora/{}/string/rawstream.sock", normalized_path);

        // Serialize initial value to both formats
        let (vec_payload, str_payload) = Self::value_to_payloads(&initial_value)?;

        // Create ping server at base path with initial payloads
        let pingserver = PingServer::new(&normalized_path, vec_payload.clone(), str_payload.clone())
            .await
            .map_err(|e| format!("Failed to create ping server: {}", e))?;

        // Create rawstream server for bytes at {path}/bytes
        let rawstream_byteserver = RawStreamServer::new(&bytes_socket_path_str, None)
            .await
            .map_err(|e| {
                format!(
                    "AgoraPublisher Error: failed to create byte rawstream server: {}",
                    e
                )
            })?;

        // Create rawstream server for strings at {path}/string
        let rawstream_omniserver = RawStreamServer::new(&string_socket_path_str, None)
            .await
            .map_err(|e| {
                format!(
                    "AgoraPublisher Error: failed to create string rawstream server: {}",
                    e
                )
            })?;

        // Confirm publisher registration with metaserver
        metaclient
            .confirm_publisher(path)
            .await
            .map_err(|e| format!("Failed to confirm publisher: {}", e))?;
        Ok(Self {
            _metaclient: metaclient,
            rawstream_byteserver,
            rawstream_omniserver,
            pingserver,
            _phantom: PhantomData,
        })
    }

    fn value_to_payloads(value: &T) -> OrError<(Vec<u8>, String)> {
        let vec_payload = postcard::to_allocvec(value)
            .map_err(|e| format!("Failed to serialize value to bytes: {}", e))?;
        let str_payload = value.to_string();
        Ok((vec_payload, str_payload))
    }

    pub async fn publish(&mut self, value: T) -> OrError<()> {
        // Compute vec and string payloads
        let (vec_payload, str_payload) = Self::value_to_payloads(&value)?;
        // Update last value on ping server
        self.pingserver
            .update_payload(vec_payload.clone(), str_payload.clone());
        // Publish to both rawstream servers
        self.rawstream_byteserver.publish(vec_payload)?;
        self.rawstream_omniserver.publish(str_payload)?;
        Ok(())
    }
}
