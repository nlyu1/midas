use super::common::Agorable;
use crate::metaserver::AgoraClient;
use crate::ping::PingClient;
use crate::rawstream::RawStreamClient;
use crate::utils::OrError;
use futures_util::stream::Stream;
use futures_util::StreamExt;
use std::marker::PhantomData;
use std::net::Ipv6Addr;
use std::pin::Pin;

pub struct Subscriber<T: Agorable> {
    _metaclient: AgoraClient,
    rawstreamclient: RawStreamClient<Vec<u8>>,
    pingclient: PingClient,
    _phantom: PhantomData<T>,
}

impl<T: Agorable> Subscriber<T> {
    // It's the user's burden that they're querying paths with the right stuff
    pub async fn new(path: String, metaserver_addr: Ipv6Addr, metaserver_port: u16) -> OrError<Subscriber<T>> {
        let metaclient = AgoraClient::new(metaserver_addr, metaserver_port).await
            .map_err(|e| format!("Failed to create AgoraClient: {}", e))?; 
        let publisher_info = metaclient.get_publisher_info(path).await?; 
        let rawstreamclient : RawStreamClient<Vec<u8>> = RawStreamClient::new(
            publisher_info.service_socket.ip().clone(),
            publisher_info.service_socket.port(),
            None, None
        ).map_err(|e| format!("Agora subscriber Error: failed to create byte rawstream client {}", e))?;
        let pingclient = PingClient::new(
            publisher_info.ping_socket.ip().clone(),
            publisher_info.ping_socket.port()
        ).await.map_err(|e| format!("Agora subscriber Error: failed to create ping client {}", e))?;
        return Ok(
            Self {
                _metaclient : metaclient, 
                rawstreamclient, 
                pingclient,
                _phantom: PhantomData
            }
        )
    }

    pub async fn get(self) -> OrError<T> {
        let (current_bytes, _, _td) = self.pingclient.ping().await?;
        let current_value: T = postcard::from_bytes(&current_bytes)
            .map_err(|e| format!("Failed to deserialize current value: {}", e))?;
        Ok(current_value)
    }

    pub async fn get_stream(self) -> OrError<(T, Pin<Box<dyn Stream<Item = OrError<T>> + Send>>)> {
        let (current_bytes, _, _td) = self.pingclient.ping().await?;
        let current_value: T = postcard::from_bytes(&current_bytes)
            .map_err(|e| format!("Failed to deserialize current value: {}", e))?;
        
        // Create stream that deserializes Vec<u8> to T
        let raw_stream = self.rawstreamclient.subscribe();
        let typed_stream = raw_stream.map(|result| {
            match result {
                Ok(bytes) => {
                    postcard::from_bytes::<T>(&bytes)
                        .map_err(|e| format!("Failed to deserialize stream value: {}", e))
                }
                Err(e) => Err(format!("Stream error: {}", e))
            }
        });
        
        let boxed_stream: Pin<Box<dyn Stream<Item = OrError<T>> + Send>> = 
            Box::pin(typed_stream);
        
        Ok((current_value, boxed_stream))
    }
}

pub struct OmniSubscriber {
    _metaclient: AgoraClient,
    rawstreamclient: RawStreamClient<String>,
    pingclient: PingClient,
}

impl OmniSubscriber {
    // Same interfaces as above, except that we fix to T=String and use the String rawstream service on ports (string_socket)
    pub async fn new(path: String, metaserver_addr: Ipv6Addr, metaserver_port: u16) -> OrError<OmniSubscriber> {
        let _metaclient = AgoraClient::new(metaserver_addr, metaserver_port).await
            .map_err(|e| format!("Failed to create AgoraClient: {}", e))?; 
        let publisher_info = _metaclient.get_publisher_info(path).await?; 
        
        // Use string_socket instead of service_socket for String data
        let rawstreamclient: RawStreamClient<String> = RawStreamClient::new(
            publisher_info.string_socket.ip().clone(),
            publisher_info.string_socket.port(),
            None, None
        ).map_err(|e| format!("Agora OmniSubscriber Error: failed to create string rawstream client {}", e))?;
        
        let pingclient = PingClient::new(
            publisher_info.ping_socket.ip().clone(),
            publisher_info.ping_socket.port()
        ).await.map_err(|e| format!("Agora OmniSubscriber Error: failed to create ping client {}", e))?;
        
        Ok(Self {
            _metaclient, 
            rawstreamclient, 
            pingclient,
        })
    }

    pub async fn get(self) -> OrError<String> {
        let (_current_bytes, current_string, _td) = self.pingclient.ping().await?;
        Ok(current_string)
    }

    // Returns current String value and a stream of String updates
    pub async fn get_stream(self) -> OrError<(String, Pin<Box<dyn Stream<Item = OrError<String>> + Send>>)> {
        // Get current value from ping (second field is String)
        let (_current_bytes, current_string, _td) = self.pingclient.ping().await?;
        
        // Create stream that passes through String values directly
        let raw_stream = self.rawstreamclient.subscribe();
        let string_stream = raw_stream.map(|result| {
            match result {
                Ok(string_value) => Ok(string_value),
                Err(e) => Err(format!("Stream error: {}", e))
            }
        });
        
        let boxed_stream: Pin<Box<dyn Stream<Item = OrError<String>> + Send>> = 
            Box::pin(string_stream);
        
        Ok((current_string, boxed_stream))
    }
}