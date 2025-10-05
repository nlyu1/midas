use super::protocol::AgoraMetaClient;
use super::publisher_info::PublisherInfo;
use crate::ConnectionHandle;
use crate::utils::OrError;
use crate::utils::{TreeNode, TreeNodeRef, TreeTrait};
use tarpc::{client, context, tokio_serde::formats::Json};

pub struct AgoraClient {
    metaserver_connection: ConnectionHandle,
    client: AgoraMetaClient,
}

// Wrapper around tarpc-generated AgoraMetaClient to have persistent connection.
impl AgoraClient {
    pub async fn new(metaserver_connection: ConnectionHandle) -> OrError<Self> {
        let mut transport =
            tarpc::serde_transport::tcp::connect(metaserver_connection.addr_port(), Json::default);
        transport.config_mut().max_frame_length(usize::MAX);
        let client = AgoraMetaClient::new(
            client::Config::default(),
            transport.await.map_err(|e| {
                format!(
                    "Agora MetaClient error: failed to create tarpc client. {}",
                    e
                )
            })?,
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
            .map_err(|e| format!("RPC error: {}", e))?
    }

    pub async fn confirm_publisher(&self, path: String) -> OrError<()> {
        self
            .client
            .confirm_publisher(context::current(), path)
            .await
            .map_err(|e| format!("RPC error: {}", e))?
    }

    pub async fn remove_publisher(&self, path: String) -> OrError<PublisherInfo> {
        self
            .client
            .remove_publisher(context::current(), path)
            .await
            .map_err(|e| format!("RPC error: {}", e))?
    }

    pub async fn get_path_tree(&self) -> OrError<TreeNodeRef> {
        let tree_repr = self
            .client
            .path_tree(context::current())
            .await
            .map_err(|e| format!("RPC error: {}", e))?;

        // Reconstruct TreeNodeRef from string representation
        // println!("Tree representation: {}", tree_repr);
        TreeNode::from_repr(&tree_repr)
    }

    pub async fn get_publisher_info(&self, path: String) -> OrError<PublisherInfo> {
        self
            .client
            .publisher_info(context::current(), path)
            .await
            .map_err(|e| format!("RPC error: {}", e))?
    }
}

impl Clone for AgoraClient {
    fn clone(&self) -> Self {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async move { Self::new(self.metaserver_connection.clone()).await.unwrap() })
    }
}
