//! TARPC client for metaserver RPC operations (register, confirm, query publishers).
//! `AgoraClient` wraps `AgoraMetaClient`, provides high-level API for `Publisher`, `Subscriber`, `Relay` to interact with metaserver.

use super::protocol::AgoraMetaClient;
use super::publisher_info::PublisherInfo;
use crate::ConnectionHandle;
use crate::utils::OrError;
use crate::utils::{TreeNode, TreeNodeRef, TreeTrait};
use crate::agora_error_cause;
use tarpc::{client, context, tokio_serde::formats::Json};

/// TARPC client for metaserver RPC communication (service discovery and publisher lifecycle).
/// Maintains persistent TCP connection to metaserver, provides high-level API over `AgoraMetaClient`.
/// Used by: `Publisher`, `Subscriber`, `OmniSubscriber`, `Relay` for registration/query operations.
pub struct AgoraClient {
    metaserver_connection: ConnectionHandle,
    client: AgoraMetaClient,
}

impl AgoraClient {
    /// Creates TARPC client with persistent TCP connection to metaserver.
    /// Error: Connection fails → propagates to `Publisher::new`, `Subscriber::new`, `Relay::new`.
    /// Called by: `Publisher::new`, `Subscriber::new`, `OmniSubscriber::new`, `Relay::swapon`
    pub async fn new(metaserver_connection: ConnectionHandle) -> OrError<Self> {
        let mut transport =
            tarpc::serde_transport::tcp::connect(metaserver_connection.addr_port(), Json::default);
        transport.config_mut().max_frame_length(usize::MAX);
        let client = AgoraMetaClient::new(
            client::Config::default(),
            transport.await.map_err(|e|
                agora_error_cause!("metaserver::AgoraClient", "new", "failed to create tarpc client", e)
            )?,
        )
        .spawn();
        Ok(Self {
            metaserver_connection,
            client,
        })
    }

    pub async fn register_publisher(
        &self,
        name: String,
        path: String,
        gateway_port: u16,
    ) -> OrError<PublisherInfo> {
        let host_connection = ConnectionHandle::new_local(gateway_port)?;
        self
            .client
            .register_publisher(context::current(), name, path, host_connection)
            .await
            .map_err(|e| agora_error_cause!("metaserver::AgoraClient", "register_publisher", "RPC call failed", e))?
    }

    pub async fn confirm_publisher(&self, path: &str) -> OrError<()> {
        self
            .client
            .confirm_publisher(context::current(), path.to_string())
            .await
            .map_err(|e| agora_error_cause!("metaserver::AgoraClient", "confirm_publisher", "RPC call failed", e))?
    }

    pub async fn remove_publisher(&self, path: &str) -> OrError<PublisherInfo> {
        self
            .client
            .remove_publisher(context::current(), path.to_string())
            .await
            .map_err(|e| agora_error_cause!("metaserver::AgoraClient", "remove_publisher", "RPC call failed", e))?
    }

    pub async fn get_path_tree(&self) -> OrError<TreeNodeRef> {
        let tree_repr = self
            .client
            .path_tree(context::current())
            .await
            .map_err(|e| agora_error_cause!("metaserver::AgoraClient", "get_path_tree", "RPC call failed", e))?;

        TreeNode::from_repr(&tree_repr)
    }

    pub async fn get_publisher_info(&self, path: &str) -> OrError<PublisherInfo> {
        self
            .client
            .publisher_info(context::current(), path.to_string())
            .await
            .map_err(|e| agora_error_cause!("metaserver::AgoraClient", "get_publisher_info", "RPC call failed", e))?
    }
}

impl Clone for AgoraClient {
    fn clone(&self) -> Self {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async move { Self::new(self.metaserver_connection).await.unwrap() })
    }
}
