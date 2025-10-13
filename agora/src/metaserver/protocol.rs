//! TARPC service trait defining metaserver RPC protocol.
//! `AgoraMeta` specifies async methods for publisher lifecycle: register, confirm, remove, query, and path tree inspection.

use super::publisher_info::PublisherInfo;
use crate::ConnectionHandle;
use crate::utils::RpcError;

#[tarpc::service]
pub trait AgoraMeta {
    /// Registers a new publisher at the specified path.
    async fn register_publisher(
        name: String,
        path: String,
        host_connection: ConnectionHandle,
    ) -> RpcError<PublisherInfo>;
    /// Confirms a registered publisher by pinging it.
    async fn confirm_publisher(path: String) -> RpcError<()>;
    /// Removes a publisher from the specified path.
    async fn remove_publisher(path: String) -> RpcError<PublisherInfo>;
    /// Returns the path tree as a string representation.
    async fn path_tree() -> String;
    /// Retrieves publisher information for the specified path.
    async fn publisher_info(path: String) -> RpcError<PublisherInfo>;
}
