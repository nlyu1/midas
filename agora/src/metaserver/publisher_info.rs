//! Publisher metadata containing name, gateway connection, and path.
//! `PublisherInfo` is returned by metaserver queries and transmitted via TARPC for service discovery.

use crate::ConnectionHandle;

/// Publisher metadata bundling name, gateway address, and registry path.
/// Returned by `AgoraMetaServer::register_publisher` and `get_publisher_info`, transmitted to subscribers for connection setup.
#[derive(Debug, PartialEq, Eq, Clone, serde::Serialize, serde::Deserialize)]
pub struct PublisherInfo {
    name: String,
    host_connection: ConnectionHandle,
    agora_path: String,
}

impl PublisherInfo {
    pub fn new(name: &str, host_connection: ConnectionHandle, agora_path: &str) -> Self {
        Self {
            name: String::from(name),
            host_connection,
            agora_path: String::from(agora_path),
        }
    }

    /// Get the socket address for tcp-websocket connections
    pub fn connection(&self) -> &ConnectionHandle {
        &self.host_connection
    }

    /// Get the socket address for heartbeat connections
    pub fn path(&self) -> &str {
        &self.agora_path
    }

    /// Get a human-readable connection string
    pub fn connection_string(&self) -> String {
        format!("{}/rawstream/{}", self.host_connection, &self.agora_path)
    }

    /// Get the name of this publisher
    pub fn name(&self) -> &str {
        &self.name
    }
}
