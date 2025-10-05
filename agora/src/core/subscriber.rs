use super::Agorable;
use crate::ConnectionHandle;
use crate::metaserver::AgoraClient;
use crate::ping::PingClient;
use crate::rawstream::RawStreamClient;
use crate::utils::{OrError, strip_and_verify};
use futures_util::StreamExt;
use futures_util::stream::Stream;
use std::marker::PhantomData;
use std::pin::Pin;

pub struct Subscriber<T: Agorable> {
    _metaclient: AgoraClient,
    rawstreamclient: RawStreamClient<Vec<u8>>,
    pingclient: PingClient,
    _phantom: PhantomData<T>,
}

impl<T: Agorable> Subscriber<T> {
    /// Creates a new typed Subscriber for a given publisher path.
    ///
    /// # Path Format
    /// Accepts slash-separated paths with or without leading slash:
    /// - `"test/publisher"` or `"/test/publisher"`
    /// - `"api/v1/data"` or `"/api/v1/data"`
    /// - `"sensor"` or `"/sensor"`
    pub async fn new(
        path: String,
        metaserver_connection: ConnectionHandle,
    ) -> OrError<Subscriber<T>> {
        let metaclient = AgoraClient::new(metaserver_connection).await.map_err(|e| {
            format!(
                "Agora subscriber error: failed to create AgoraClient: {}",
                e
            )
        })?;

        // Strip and verify path
        let normalized_path = strip_and_verify(&path)?;

        let publisher_info = metaclient
            .get_publisher_info(normalized_path.clone())
            .await?;
        let host_gateway_connection = publisher_info.connection();

        // Append "/bytes" suffix for binary rawstream
        let bytes_path_str = format!("{}/bytes", normalized_path);

        let rawstreamclient: RawStreamClient<Vec<u8>> =
            RawStreamClient::new(host_gateway_connection.clone(), &bytes_path_str, None, None)
                .map_err(|e| {
                    format!(
                        "Agora subscriber Error: failed to create byte rawstream client {}",
                        e
                    )
                })?;
        let pingclient = PingClient::new(&normalized_path, host_gateway_connection)
            .await
            .map_err(|e| format!("Agora subscriber Error: failed to create ping client {}", e))?;

        Ok(Self {
            _metaclient: metaclient,
            rawstreamclient,
            pingclient,
            _phantom: PhantomData,
        })
    }

    pub async fn get(&mut self) -> OrError<T> {
        let (current_bytes, _, _td) = self.pingclient.ping().await?;
        let current_value: T = postcard::from_bytes(&current_bytes).map_err(|e| {
            format!(
                "AgoraSubscriber Error: failed to deserialize current value in get() method: {}",
                e
            )
        })?;
        Ok(current_value)
    }

    pub async fn get_stream(
        &mut self,
    ) -> OrError<(T, Pin<Box<dyn Stream<Item = OrError<T>> + Send>>)> {
        let (current_bytes, _, _td) = self.pingclient.ping().await?;
        let current_value: T = postcard::from_bytes(&current_bytes)
            .map_err(|e| format!("Failed to deserialize current value: {}", e))?;

        // Create stream that deserializes Vec<u8> to T
        let raw_stream = self.rawstreamclient.subscribe();
        let typed_stream = raw_stream.map(|result| match result {
            Ok(bytes) => postcard::from_bytes::<T>(&bytes)
                .map_err(|e| format!("Failed to deserialize stream value: {}", e)),
            Err(e) => Err(format!("Stream error: {}", e)),
        });

        let boxed_stream: Pin<Box<dyn Stream<Item = OrError<T>> + Send>> = Box::pin(typed_stream);

        Ok((current_value, boxed_stream))
    }
}

pub struct OmniSubscriber {
    _metaclient: AgoraClient,
    rawstreamclient: RawStreamClient<String>,
    pingclient: PingClient,
}

impl OmniSubscriber {
    /// Creates a new OmniSubscriber (String-based) for a given publisher path.
    ///
    /// # Path Format
    /// Accepts slash-separated paths with or without leading slash:
    /// - `"test/publisher"` or `"/test/publisher"`
    /// - `"api/v1/data"` or `"/api/v1/data"`
    /// - `"sensor"` or `"/sensor"`
    pub async fn new(
        path: String,
        metaserver_connection: ConnectionHandle,
    ) -> OrError<OmniSubscriber> {
        let metaclient = AgoraClient::new(metaserver_connection)
            .await
            .map_err(|e| format!("Failed to create AgoraClient: {}", e))?;

        // Strip and verify path
        let normalized_path = strip_and_verify(&path)?;

        let publisher_info = metaclient
            .get_publisher_info(normalized_path.clone())
            .await?;
        let host_gateway_connection = publisher_info.connection();

        // Append "/string" suffix for string rawstream
        let string_path_str = format!("{}/string", normalized_path);

        let rawstreamclient: RawStreamClient<String> = RawStreamClient::new(
            host_gateway_connection.clone(),
            &string_path_str,
            None,
            None,
        )
        .map_err(|e| {
            format!(
                "Agora subscriber Error: failed to create string rawstream client {}",
                e
            )
        })?;

        let pingclient = PingClient::new(&normalized_path, host_gateway_connection)
            .await
            .map_err(|e| format!("Agora subscriber Error: failed to create ping client {}", e))?;

        Ok(Self {
            _metaclient: metaclient,
            rawstreamclient,
            pingclient,
        })
    }

    pub async fn get(&mut self) -> OrError<String> {
        let (_current_bytes, current_string, _td) = self.pingclient.ping().await?;
        Ok(current_string)
    }

    // Returns current String value and a stream of String updates
    pub async fn get_stream(
        &mut self,
    ) -> OrError<(String, Pin<Box<dyn Stream<Item = OrError<String>> + Send>>)> {
        // Get current value from ping (second field is String)
        let (_current_bytes, current_string, _td) = self.pingclient.ping().await?;

        // Create stream that passes through String values directly
        let raw_stream = self.rawstreamclient.subscribe();
        let string_stream = raw_stream.map(|result| match result {
            Ok(string_value) => Ok(string_value),
            Err(e) => Err(format!("Stream error: {}", e)),
        });

        let boxed_stream: Pin<Box<dyn Stream<Item = OrError<String>> + Send>> =
            Box::pin(string_stream);

        Ok((current_string, boxed_stream))
    }
}
