use super::common::Agorable;
use crate::metaserver::AgoraClient;
use crate::ping::PingServer;
use crate::rawstream::RawStreamServer;
use crate::utils::OrError;
use std::marker::PhantomData;
use std::net::Ipv6Addr;

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
        metaserver_addr: Ipv6Addr,
        metaserver_port: u16,
    ) -> OrError<Self> {
        let metaclient = AgoraClient::new(metaserver_addr, metaserver_port).await
            .map_err(|e| format!(
                "AgoraPublisher Error: failed to create _metaclient, make sure to spin up metaserver first: {}", e))?;
        let publisher_info = metaclient.register_publisher(name, path.clone()).await?;

        let (vec_payload, str_payload) = Self::value_to_payloads(&initial_value)?;

        // This immediately spawns background ping server
        let pingserver = PingServer::new(
            publisher_info.ping_socket.ip().clone().into(),
            publisher_info.ping_socket.port(),
            vec_payload,
            str_payload,
        )
        .map_err(|e| format!("Failed to create ping server: {}", e))?;
        let rawstream_byteserver = RawStreamServer::new(
            publisher_info.service_socket.ip().clone(),
            publisher_info.service_socket.port(),
            None,
        )
        .await
        .map_err(|e| {
            format!(
                "AgoraPublisher Error: failed to create byte rawstream server: {}",
                e
            )
        })?;
        let rawstream_omniserver = RawStreamServer::new(
            publisher_info.string_socket.ip().clone(),
            publisher_info.string_socket.port(),
            None,
        )
        .await
        .map_err(|e| {
            format!(
                "AgoraPublisher Error: failed to create string rawstream server: {}",
                e
            )
        })?;

        // Next, establish handshake with server by asking server to confirm
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
