use super::protocol::{AgoraMetaClient, DEFAULT_PORT};
use super::publisher_info::PublisherInfo;
use crate::utils::OrError;
use crate::utils::{TreeNode, TreeNodeRef, TreeTrait};
use std::net::{IpAddr, Ipv6Addr};
use tarpc::{client, context, tokio_serde::formats::Json};

pub struct AgoraClient {
    client: AgoraMetaClient,
}

// Wrapper around tarpc-generated AgoraMetaClient to have persistent connection.
impl AgoraClient {
    pub async fn new(port: Option<u16>) -> anyhow::Result<Self> {
        let server_addr = (
            IpAddr::V6(Ipv6Addr::LOCALHOST),
            port.unwrap_or(DEFAULT_PORT),
        );
        let mut transport = tarpc::serde_transport::tcp::connect(server_addr, Json::default);
        transport.config_mut().max_frame_length(usize::MAX);
        let client = AgoraMetaClient::new(client::Config::default(), transport.await?).spawn();
        Ok(Self { client })
    }

    pub async fn register_publisher(&self, name: String, path: String) -> OrError<PublisherInfo> {
        self.client
            .register_publisher(context::current(), name, path)
            .await
            .map_err(|e| format!("RPC error: {}", e))?
    }

    pub async fn remove_publisher(&self, path: String) -> OrError<PublisherInfo> {
        self.client
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
        println!("Tree representation: {}", tree_repr);
        TreeNode::from_repr(&tree_repr)
    }

    pub async fn get_publisher_info(&self, path: String) -> OrError<PublisherInfo> {
        self.client
            .publisher_info(context::current(), path)
            .await
            .map_err(|e| format!("RPC error: {}", e))?
    }
}
