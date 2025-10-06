//! Metaserver module for centralized service discovery and publisher registry.
//! Provides TARPC-based RPC server (`AgoraMetaServer`), client (`AgoraClient`), shared state (`ServerState`), and publisher metadata (`PublisherInfo`).

mod server;
pub use server::AgoraMetaServer;

mod publisher_info;
pub use publisher_info::PublisherInfo;

mod client;
pub use client::AgoraClient;

mod protocol;
pub use protocol::AgoraMeta;

mod state;
pub use state::ServerState;
