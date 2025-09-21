use super::publisher_info::PublisherInfo;
use crate::utils::OrError;

pub const DEFAULT_PORT: u16 = 8080;

#[tarpc::service]
pub trait AgoraMeta {
    /// Registers a new publisher at the specified path.
    async fn register_publisher(name: String, path: String) -> OrError<PublisherInfo>;
    /// Removes a publisher from the specified path.
    async fn remove_publisher(path: String) -> OrError<PublisherInfo>;
    /// Returns the path tree as a string representation.
    async fn path_tree() -> OrError<String>;
    /// Retrieves publisher information for the specified path.
    async fn publisher_info(path: String) -> OrError<PublisherInfo>;
}
