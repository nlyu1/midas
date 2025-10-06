//! Subscriber implementations: `Subscriber<T>` for typed streams (binary endpoint) and `OmniSubscriber` for type-agnostic monitoring (string endpoint).
//! Both query metaserver for publisher location, connect to gateway-proxied WebSocket streams, and provide current value + stream access.

use super::Agorable;
use crate::ConnectionHandle;
use crate::agora_error_cause;
use crate::metaserver::AgoraClient;
use crate::ping::PingClient;
use crate::rawstream::RawStreamClient;
use crate::utils::{OrError, strip_and_verify};
use futures_util::StreamExt;
use futures_util::stream::Stream;
use std::marker::PhantomData;
use std::pin::Pin;

/// Typed subscriber that queries metaserver for publisher location and connects to binary endpoint.
/// Requires: Publisher exists and is confirmed in metaserver.
/// Network: Queries metaserver → connects to `ws://gateway/rawstream/{path}/bytes` → proxies to `/tmp/agora/{path}/bytes/rawstream.sock`.
pub struct Subscriber<T: Agorable> {
    _metaclient: AgoraClient,
    rawstreamclient: RawStreamClient<Vec<u8>>,
    pingclient: PingClient,
    _phantom: PhantomData<T>,
}

impl<T: Agorable> Subscriber<T> {
    /// Creates typed subscriber by querying metaserver for publisher location.
    /// Network flow: metaserver query → get gateway address → connect to binary endpoint.
    /// Connects to `ws://gateway/rawstream/{path}/bytes` (via gateway proxy to UDS).
    /// Error: Publisher not found or connection fails → propagates to user code.
    /// Called by: User code, `Relay::swapon`
    pub async fn new(
        path: String,
        metaserver_connection: ConnectionHandle,
    ) -> OrError<Subscriber<T>> {
        // Step 1: Connect to metaserver
        let metaclient = AgoraClient::new(metaserver_connection).await.map_err(|e| {
            agora_error_cause!("core::Subscriber", "new", "failed to create AgoraClient", e)
        })?;

        let normalized_path = strip_and_verify(&path)?;

        // Step 2: Query metaserver for publisher location (pings publisher to verify alive)
        let publisher_info = metaclient
            .get_publisher_info(&normalized_path)
            .await?;
        let host_gateway_connection = *publisher_info.connection();

        // Step 3: Connect to binary endpoint (path/bytes for Subscriber\<T>)
        let bytes_path_str = format!("{}/bytes", normalized_path);

        let rawstreamclient: RawStreamClient<Vec<u8>> =
            RawStreamClient::new(host_gateway_connection, &bytes_path_str, None, None)
                .map_err(|e| {
                    agora_error_cause!(
                        "core::Subscriber",
                        "new",
                        "failed to create byte rawstream client",
                        e
                    )
                })?;

        // Step 4: Create ping client for synchronous queries
        let pingclient = PingClient::new(&normalized_path, host_gateway_connection)
            .await
            .map_err(|e| {
                agora_error_cause!("core::Subscriber", "new", "failed to create ping client", e)
            })?;

        Ok(Self {
            _metaclient: metaclient,
            rawstreamclient,
            pingclient,
            _phantom: PhantomData,
        })
    }

    /// Fetches current value via ping (one-time query, no streaming).
    /// Error: Ping fails or deserialization fails → propagates to caller.
    pub async fn get(&mut self) -> OrError<T> {
        let (current_bytes, _, _td) = self.pingclient.ping().await?;
        let current_value: T = postcard::from_bytes(&current_bytes).map_err(|e| {
            agora_error_cause!(
                "core::Subscriber",
                "get",
                "failed to deserialize current value",
                e
            )
        })?;
        Ok(current_value)
    }

    /// Returns current value + stream of future updates.
    /// Stream auto-reconnects on disconnect (handled by `RawStreamClient`).
    /// Error: Initial ping fails → propagates to caller. Stream errors appear in stream items.
    pub async fn get_stream(
        &mut self,
    ) -> OrError<(T, Pin<Box<dyn Stream<Item = OrError<T>> + Send>>)> {
        // Get initial value via ping
        let (current_bytes, _, _td) = self.pingclient.ping().await?;
        let current_value: T = postcard::from_bytes(&current_bytes).map_err(|e| {
            agora_error_cause!(
                "core::Subscriber",
                "get_stream",
                "failed to deserialize current value. If Omnisubscriber is succeeding, then double-check if published data type aligns with subscriber type.",
                e
            )
        })?;

        // Create stream that deserializes binary messages to T
        let raw_stream = self.rawstreamclient.subscribe();
        let typed_stream = raw_stream.map(|result| match result {
            Ok(bytes) => postcard::from_bytes::<T>(&bytes).map_err(|e| {
                agora_error_cause!(
                    "core::Subscriber",
                    "get_stream",
                    "failed to deserialize stream value",
                    e
                )
            }),
            Err(e) => Err(agora_error_cause!(
                "core::Subscriber",
                "get_stream",
                "stream error",
                e
            )),
        });

        let boxed_stream: Pin<Box<dyn Stream<Item = OrError<T>> + Send>> = Box::pin(typed_stream);

        Ok((current_value, boxed_stream))
    }
}

/// Type-agnostic subscriber receiving string representations via Display trait.
/// Identical to `Subscriber<T>` but connects to `/string` endpoint instead of `/bytes`.
pub struct OmniSubscriber {
    _metaclient: AgoraClient,
    rawstreamclient: RawStreamClient<String>,
    pingclient: PingClient,
}

impl OmniSubscriber {
    /// Creates type-agnostic subscriber receiving string representations.
    /// Identical to `Subscriber::new` but connects to `/string` endpoint instead of `/bytes`.
    /// Error: Publisher not found or connection fails → propagates to user code.
    /// Called by: User code
    pub async fn new(
        path: String,
        metaserver_connection: ConnectionHandle,
    ) -> OrError<OmniSubscriber> {
        let metaclient = AgoraClient::new(metaserver_connection).await.map_err(|e| {
            agora_error_cause!(
                "core::OmniSubscriber",
                "new",
                "failed to create AgoraClient",
                e
            )
        })?;

        let normalized_path = strip_and_verify(&path)?;

        let publisher_info = metaclient
            .get_publisher_info(&normalized_path)
            .await?;
        let host_gateway_connection = *publisher_info.connection();

        // Connect to string endpoint (path/string for OmniSubscriber)
        let string_path_str = format!("{}/string", normalized_path);

        let rawstreamclient: RawStreamClient<String> = RawStreamClient::new(
            host_gateway_connection,
            &string_path_str,
            None,
            None,
        )
        .map_err(|e| {
            agora_error_cause!(
                "core::OmniSubscriber",
                "new",
                "failed to create string rawstream client",
                e
            )
        })?;

        let pingclient = PingClient::new(&normalized_path, host_gateway_connection)
            .await
            .map_err(|e| {
                agora_error_cause!(
                    "core::OmniSubscriber",
                    "new",
                    "failed to create ping client",
                    e
                )
            })?;

        Ok(Self {
            _metaclient: metaclient,
            rawstreamclient,
            pingclient,
        })
    }

    /// Fetches current value as string via ping.
    pub async fn get(&mut self) -> OrError<String> {
        let (_current_bytes, current_string, _td) = self.pingclient.ping().await?;
        Ok(current_string)
    }

    /// Returns current string value + stream of future string updates.
    /// Stream auto-reconnects on disconnect.
    pub async fn get_stream(
        &mut self,
    ) -> OrError<(String, Pin<Box<dyn Stream<Item = OrError<String>> + Send>>)> {
        let (_current_bytes, current_string, _td) = self.pingclient.ping().await?;

        // String stream passes through values directly (no deserialization needed)
        let raw_stream = self.rawstreamclient.subscribe();
        let string_stream = raw_stream.map(|result| match result {
            Ok(string_value) => Ok(string_value),
            Err(e) => Err(agora_error_cause!(
                "core::OmniSubscriber",
                "get_stream",
                "stream error",
                e
            )),
        });

        let boxed_stream: Pin<Box<dyn Stream<Item = OrError<String>> + Send>> =
            Box::pin(string_stream);

        Ok((current_string, boxed_stream))
    }
}
