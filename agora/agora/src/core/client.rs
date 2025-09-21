use super::protocol::{Agorable, RawStreamerClient};
use crate::metaserver::{AgoraMetaClient, DEFAULT_PORT};
use crate::utils::OrError;
use crate::utils::pathtree::{TreeNode, TreeNodeRef, TreeTrait};
use std::marker::PhantomData;
use std::net::{IpAddr, Ipv6Addr};
use tarpc::{client, context, tokio_serde::formats::Json};

pub struct AgoraSubscriber<T: Agorable> {
    client: RawStreamerClient,
    metaclient: AgoraMetaClient,
    _marker: PhantomData<T>,
}

// Wrapper around tarpc-generated RawStreamerClient to have persistent connection.
impl<T> AgoraSubscriber<T>
where
    T: Agorable,
{
    pub async fn new(path: String, metaserver_port: Option<u16>) -> OrError<Self> {
        let metaclient = AgoraMetaClient::new(metaserver_port).await?;

        // Get publisher info from metaserver to connect to the actual publisher
        let publisher_info = metaclient.get_publisher_info(path).await?;

        // Connect to the publisher's address
        let mut transport =
            tarpc::serde_transport::tcp::connect(publisher_info.socket_addr(), Json::default);
        transport.config_mut().max_frame_length(usize::MAX);
        let client = RawStreamerClient::new(client::Config::default(), transport.await?).spawn();

        Ok(Self {
            client,
            metaclient,
            _marker: PhantomData,
        })
    }

    pub async fn register_publisher(&self, name: String, path: String) -> OrError<PublisherInfo> {
        self.metaclient.register_publisher(name, path).await
    }

    pub async fn remove_publisher(&self, path: String) -> OrError<PublisherInfo> {
        self.metaclient.remove_publisher(path).await
    }

    pub async fn get_path_tree(&self) -> OrError<TreeNodeRef> {
        self.metaclient.get_path_tree().await
    }

    pub async fn get_publisher_info(&self, path: String) -> OrError<PublisherInfo> {
        self.metaclient.get_publisher_info(path).await
    }
}
