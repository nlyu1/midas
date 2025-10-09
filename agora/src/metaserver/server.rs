//! TARPC-based metaserver for service discovery with shared state and background pruning.
//! `AgoraMetaServer` implements `AgoraMeta` RPC trait, manages `ServerState` via `RwLock`, runs background task to prune stale publishers every 500ms.

use super::ServerState;
use super::protocol::AgoraMeta;
use super::publisher_info::PublisherInfo;
use crate::ConnectionHandle;
use crate::constants::CHECK_PUBLISHER_LIVELINESS_EVERY_MS;
use crate::utils::OrError;
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};
use tokio::task::JoinHandle;

use std::net::IpAddr;
use tarpc::context;
// use tarpc::server::incoming::Incoming;

use futures::prelude::*;
use tarpc::{
    server::{self, Channel},
    tokio_serde::formats::Json,
};
use tokio::time::{Duration, interval};

/// TARPC-based metaserver managing publisher registry with shared state architecture.
/// Architecture: Single `ServerState` protected by `RwLock`, multiple concurrent TARPC connections, background pruning task.
/// RPC protocol: `AgoraMeta` trait defines service discovery methods (register, confirm, query publishers).
#[derive(Clone)]
pub struct AgoraMetaServer {
    state: Arc<RwLock<ServerState>>,
    bg_handle: Arc<Mutex<JoinHandle<()>>>,
}

impl AgoraMeta for AgoraMetaServer {
    async fn register_publisher(
        self,
        _: context::Context,
        name: String,
        path: String,
        host_connection: ConnectionHandle,
    ) -> OrError<PublisherInfo> {
        let mut state = self.state.write().await;
        state.register_publisher(name, path, host_connection)
    }

    async fn confirm_publisher(self, _: context::Context, path: String) -> OrError<()> {
        let mut state = self.state.write().await;
        state.confirm_publisher(&path).await
    }

    async fn remove_publisher(self, _: context::Context, path: String) -> OrError<PublisherInfo> {
        let mut state = self.state.write().await;
        state.remove_publisher(&path)
    }

    async fn path_tree(self, _: context::Context) -> String {
        let state = self.state.read().await;
        state.get_path_tree_repr()
    }

    async fn publisher_info(self, _: context::Context, path: String) -> OrError<PublisherInfo> {
        let mut state = self.state.write().await;
        state.get_publisher_info(&path).await
    }
}

impl AgoraMetaServer {
    fn new(shared_state: Arc<RwLock<ServerState>>, bg_handle: Arc<Mutex<JoinHandle<()>>>) -> Self {
        Self {
            state: shared_state,
            bg_handle,
        }
    }

    /// Starts TARPC metaserver with shared state model.
    /// Architecture: One `ServerState` (`RwLock`ed), many client connections, one pruning task.
    /// Network: Listens on TCP for TARPC connections, serves `AgoraMeta` RPC methods.
    /// Background: Prunes stale publishers every 500ms by pinging them.
    pub async fn run_server(address: IpAddr, port: u16) -> anyhow::Result<()> {
        let server_addr = (address, port);

        // Single shared state accessed by all TARPC connections
        let shared_state = Arc::new(RwLock::new(ServerState::new()));

        // TARPC TCP listener with JSON serialization
        let mut listener = tarpc::serde_transport::tcp::listen(&server_addr, Json::default).await?;
        println!("Metaserver active on {}:{}", address, port);
        listener.config_mut().max_frame_length(usize::MAX);

        // Background pruning task: pings publishers every 500ms, removes dead ones
        let pruning_state = Arc::clone(&shared_state);
        let bg_handle = Arc::new(Mutex::new(tokio::spawn(async move {
            let mut interval = interval(Duration::from_millis(CHECK_PUBLISHER_LIVELINESS_EVERY_MS));
            loop {
                interval.tick().await;
                let pruned_paths = {
                    let mut state = pruning_state.write().await;
                    state.prune_stale_publishers().await
                }; // Drop lock before printing
                if !pruned_paths.is_empty() {
                    println!("Pruned stale publishers: {:?}", pruned_paths);
                }
            }
        })));

        // TARPC connection processing pipeline
        listener
            .filter_map(|r| futures::future::ready(r.ok())) // Ignore accept errors
            .map(server::BaseChannel::with_defaults)
            // .max_channels_per_key(1024 * 1024, |t| t.transport().peer_addr().unwrap().ip())
            .map(|channel| {
                // Each channel = one client connection
                // All connections share the same ServerState
                let server =
                    AgoraMetaServer::new(Arc::clone(&shared_state), Arc::clone(&bg_handle));
                channel.execute(server.serve()).for_each(|fut| async {
                    fut.await;
                })
            })
            .buffer_unordered(32768 * 1024)
            .for_each(|_| async {}) // Run forever
            .await;

        Ok(())
    }
}

impl Drop for AgoraMetaServer {
    fn drop(&mut self) {
        if Arc::strong_count(&self.bg_handle) == 1
            && let Ok(handle_guard) = self.bg_handle.try_lock()
        {
            handle_guard.abort();
            println!("Background pruning task stopped");
        }
    }
}
