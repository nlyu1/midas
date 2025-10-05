use super::protocol::AgoraMeta;
use super::publisher_info::PublisherInfo;
// use crate::ping::PingClient;
use crate::constants::CHECK_PUBLISHER_LIVELINESS_EVERY_MS;
use crate::utils::{OrError, TreeNode, TreeNodeRef, TreeTrait};
use crate::ConnectionHandle; 
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};
use tokio::task::JoinHandle;

use std::net::IpAddr;
use tarpc::context;
use tarpc::server::incoming::Incoming;

use futures::prelude::*;
use tarpc::{
    server::{self, Channel},
    tokio_serde::formats::Json,
};
use tokio::time::{Duration, interval};

// Shared server state that will be accessed by all connections
#[derive(Debug)]
pub struct ServerState {
    pub path_tree: TreeNodeRef,
    pub publishers: HashMap<String, PublisherInfo>,
    // pub confirmed_publishers: HashMap<String, PingClient>, // For each client, initialize a ping-client
    pub confirmed_publishers: HashMap<String, i32>,
}

// Todo: separate two things for serverState: query new address, and (2) confirm that publisher is running
// After confirmation, server runs a ping-client. There's also a

impl ServerState {
    pub fn new() -> Self {
        Self {
            path_tree: TreeNode::new("agora"),
            publishers: HashMap::new(),
            confirmed_publishers: HashMap::new(),
        }
    }

    pub fn path_tree(&self) -> TreeNodeRef {
        self.path_tree.clone()
    }

    pub fn register_publisher(
        &mut self,
        name: String,
        path: String,
        host_connection: ConnectionHandle,
    ) -> OrError<PublisherInfo> {
        // Validate path format strictly
        self.validate_path_format(&path)?;

        // Check if publisher already exists
        if self.publishers.contains_key(&path) {
            let publisher_info = self.publishers.get(&path).unwrap();
            return Err(format!(
                "Publisher {:?} already registered at {}. \
                 Check path or use `update` instead.",
                publisher_info, path
            ));
        }

        // Check if any parent path is already a publisher (should be directories only)
        self.validate_parent_paths_are_directories(&path)?;

        // Check if the path already exists in the tree as a directory
        // Publishers can only be registered at new paths, not existing directory nodes
        if self.path_tree.get_child(&path).is_ok() {
            return Err(format!(
                "Path '{}' already exists as a directory in the tree. \
                 Publishers can only be registered at new paths.",
                path
            ));
        }

        // Build parent paths as needed
        self.ensure_path_exists(&path)?;

        // The designated uds path of an agora publisher is just its agora path
        let publisher_info = PublisherInfo::new(&name, host_connection, &path);
        println!(
            "Registered publisher {:?} at path {}",
            publisher_info, &path
        );
        self.publishers.insert(path, publisher_info.clone());
        Ok(publisher_info)
    }

    // pub async fn confirm_publisher(&mut self, path: String) -> OrError<()> {
    //     // Checks that publisher has already been registered
    //     if !self.publishers.contains_key(&path) {
    //         return Err(format!(
    //             "Metaserver confirmation error: \
    //              please register path {} before confirming",
    //             path
    //         ));
    //     }

    //     // Checks against duplicate confirmation
    //     if self.confirmed_publishers.contains_key(&path) {
    //         return Err(format!(
    //             "Metaserver confirmation error: \
    //              {} already registered and confirmed",
    //             path
    //         ));
    //     }

    //     // Extract address and port from stored info
    //     let publisher_info = self.publishers.get(&path).unwrap();

    //     let pingclient = PingClient::new(
    //         publisher_info.socket(), publisher_info.uds_path())
    //         .await
    //         .map_err(|e| {
    //             let _ = self.remove_publisher(path.clone());
    //             eprintln!(
    //                 "Removed registered publisher {} upon unsuccessful confirmation",
    //                 &path
    //             );
    //             format!(
    //                 "Publisher confirmation failed: metaserver failed to create ping client! \
    //                  You might have forgotten to run network setup: {}",
    //                 e
    //             )
    //         })?;
    //     let _ = pingclient.ping().await.map_err(|e| {
    //         let _ = self.remove_publisher(path.clone());
    //         eprintln!(
    //             "Removed registered publisher {} upon unsuccessful confirmation",
    //             &path
    //         );
    //         e
    //     })?; // Make sure that we can ping and obtain results

    //     // Now that things are ok, add pingclient
    //     self.confirmed_publishers.insert(path, pingclient);
    //     Ok(())
    // }

    pub fn remove_publisher(&mut self, path: String) -> OrError<PublisherInfo> {
        // Validate path format strictly
        self.validate_path_format(&path)?;

        // Check if publisher exists and remove it
        // Note: Due to parent invariant, publishers can only exist at leaf nodes,
        // so we don't need to check for child publishers anymore
        match self.publishers.remove(&path) {
            Some(publisher_info) => {
                self.path_tree.remove_child_and_branch(&path)?;
                // Remove from confirmed_publishers if it exists (it may not if confirmation failed)
                self.confirmed_publishers.remove(&path);
                Ok(publisher_info)
            }
            None => Err(format!(
                "Can only remove paths associated with publishers: \
                 path '{}' is not associated with any publishers.",
                path
            )),
        }
    }

    pub fn get_path_tree_repr(&self) -> String {
        self.path_tree.to_repr()
    }

    pub async fn get_publisher_info(&mut self, path: String) -> OrError<PublisherInfo> {
        // Validate path format strictly
        self.validate_path_format(&path)?;

        match (
            self.publishers.get(&path),
            self.confirmed_publishers.get_mut(&path),
        ) {
            (Some(publisher), Some(pingclient)) => {
                // pingclient
                //     .ping()
                //     .await
                //     .map_err(|_| format!("Cannot ping {}. Publisher might be stale", path))?;
                Ok(publisher.clone())
            }
            (Some(_), None) => Err(format!(
                "Publisher at {} is registered but not confirmed",
                path
            )),
            (None, _) => Err(format!("Publisher not registered at {}", path)),
        }
    }

    fn validate_path_format(&self, path: &str) -> OrError<()> {
        // Strict path validation rules
        if path.is_empty() {
            return Err("Path cannot be empty".to_string());
        }

        // No leading or trailing slashes allowed
        if path.starts_with('/') {
            return Err(format!(
                "Path '{}' cannot start with '/' - \
                 use relative paths only",
                path
            ));
        }
        if path.ends_with('/') {
            return Err(format!(
                "Path '{}' cannot end with '/' - \
                 trailing slashes not allowed",
                path
            ));
        }

        // No double slashes allowed
        if path.contains("//") {
            return Err(format!(
                "Path '{}' contains double slashes '//' - \
                 not allowed",
                path
            ));
        }

        // No empty segments (this catches things like "a//b")
        let segments: Vec<&str> = path.split('/').collect();
        for (i, segment) in segments.iter().enumerate() {
            if segment.is_empty() {
                return Err(format!(
                    "Path '{}' has empty segment at position {} - \
                     not allowed",
                    path, i
                ));
            }

            // Optional: validate segment characters (no spaces, special chars, etc.)
            if segment.trim() != *segment {
                return Err(format!(
                    "Path segment '{}' has leading/trailing whitespace - \
                     not allowed",
                    segment
                ));
            }
        }

        Ok(())
    }

    fn validate_parent_paths_are_directories(&self, path: &str) -> OrError<()> {
        // Check if any parent path is already registered as a publisher
        // Example: if registering "dir/subdir/content", check if "dir" or "dir/subdir" are publishers

        let path_parts: Vec<&str> = path.split('/').collect();
        let mut current_path = String::new();

        // Check each parent path (excluding the final path itself)
        for (i, part) in path_parts.iter().enumerate() {
            if i == path_parts.len() - 1 {
                break; // Skip the final segment (the path we're trying to register)
            }

            if current_path.is_empty() {
                current_path = part.to_string();
            } else {
                current_path = format!("{}/{}", current_path, part);
            }

            // Check if this parent path is a publisher
            if self.publishers.contains_key(&current_path) {
                return Err(format!(
                    "Path parent should all be directories, but '{}' is associated \
                     with a publisher. Consider removing first.",
                    current_path
                ));
            }
        }

        Ok(())
    }

    pub async fn prune_stale_publishers(&mut self) -> Vec<String> {
        let mut stale_paths: Vec<String> = Vec::new();

        // Collect paths to check to avoid borrowing issues
        let paths_to_check: Vec<String> = self.confirmed_publishers.keys().cloned().collect();

        // Iterate over all confirmed publishers and ping each one
        for path in paths_to_check {
            if let Some(ping_client) = self.confirmed_publishers.get_mut(&path) {
                // if let Err(_) = ping_client.ping().await {
                //     // Ping failed, mark as stale
                //     stale_paths.push(path.clone());
                // }
            }
        }

        // Remove all stale publishers
        for path in &stale_paths {
            if let Err(e) = self.remove_publisher(path.clone()) {
                eprintln!("Failed to remove stale publisher at {}: {}", path, e);
            }
        }

        stale_paths
    }

    fn ensure_path_exists(&mut self, path: &str) -> OrError<()> {
        // Path is already validated, so we can split safely
        let path_parts: Vec<&str> = path.split('/').collect();
        let mut current_path = String::new();

        for part in path_parts {
            if current_path.is_empty() {
                current_path = part.to_string();
            } else {
                current_path = format!("{}/{}", current_path, part);
            }

            // Check if this path segment exists, if not create it
            if self.path_tree.get_child(&current_path).is_err() {
                // We need to create this path segment
                let parent_path = if current_path.contains('/') {
                    let last_slash = current_path.rfind('/').unwrap();
                    &current_path[..last_slash]
                } else {
                    ""
                };

                let parent_node = if parent_path.is_empty() {
                    self.path_tree.clone()
                } else {
                    self.path_tree.get_child(parent_path)?
                };

                let new_node = TreeNode::new(part);
                parent_node.add_child(new_node);
            }
        }

        Ok(())
    }
}

// Asynchronous wrapper around synchronous ServerState implementation that handles networking with defaults.
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
        host_connection: ConnectionHandle
    ) -> OrError<PublisherInfo> {
        let mut state = self.state.write().await;
        state.register_publisher(name, path, host_connection)
    }

    async fn confirm_publisher(self, _: context::Context, path: String) -> OrError<()> {
        Ok(())
        // let mut state = self.state.write().await;
        // state.confirm_publisher(path).await
    }

    async fn remove_publisher(self, _: context::Context, path: String) -> OrError<PublisherInfo> {
        let mut state = self.state.write().await;
        state.remove_publisher(path)
    }

    async fn path_tree(self, _: context::Context) -> String {
        let state = self.state.read().await;
        state.get_path_tree_repr()
    }

    async fn publisher_info(self, _: context::Context, path: String) -> OrError<PublisherInfo> {
        let mut state = self.state.write().await;
        state.get_publisher_info(path).await
    }
}

impl AgoraMetaServer {
    fn new(shared_state: Arc<RwLock<ServerState>>, bg_handle: Arc<Mutex<JoinHandle<()>>>) -> Self {
        Self {
            state: shared_state,
            bg_handle,
        }
    }

    pub async fn run_server(address: IpAddr, port: u16) -> anyhow::Result<()> {
        let server_addr = (address, port);

        // Create a single shared server state that all connections will use
        let shared_state = Arc::new(RwLock::new(ServerState::new()));

        // JSON transport is provided by the json_transport tarpc module. It makes it easy
        // to start up a serde-powered json serialization strategy over TCP.
        let mut listener = tarpc::serde_transport::tcp::listen(&server_addr, Json::default).await?;
        println!("Listening on port {}", listener.local_addr().port());
        listener.config_mut().max_frame_length(usize::MAX);

        // Start a background process to obtain write lock on shared_state and prunes. Should execute once every constant (see top) number of ms
        let pruning_state = Arc::clone(&shared_state);
        let bg_handle = Arc::new(Mutex::new(tokio::spawn(async move {
            let mut interval = interval(Duration::from_millis(CHECK_PUBLISHER_LIVELINESS_EVERY_MS));
            loop {
                interval.tick().await;
                let pruned_paths = {
                    let mut state = pruning_state.write().await;
                    state.prune_stale_publishers().await
                }; // Lock is dropped here
                if !pruned_paths.is_empty() {
                    println!("Pruned stale publishers: {:?}", pruned_paths);
                }
            }
        })));

        // Listener yields a stream of connections. Each connection is a stream of requests.
        listener
            // Ignore accept errors.
            .filter_map(|r| futures::future::ready(r.ok()))
            .map(server::BaseChannel::with_defaults)
            // An IP can register many many services.
            .max_channels_per_key(1024 * 1024, |t| t.transport().peer_addr().unwrap().ip())
            // serve is generated by the service attribute. It takes as input any type implementing
            // the generated AgoraMeta trait.
            .map(|channel| {
                // Each channel **represents a single, persistent connection to a client.**
                // BUT they all share the same server state!
                let server =
                    AgoraMetaServer::new(Arc::clone(&shared_state), Arc::clone(&bg_handle));
                channel.execute(server.serve()).for_each(
                    // Async wrapper around tokio::spawn
                    |fut| async {
                        fut.await; // Sequentially wait for each future to complete
                    },
                )
            })
            .buffer_unordered(32768)
            // for_each runs stream to completion. For this particular case, it will run forever.
            // An example stream which terminates would finite-length stream.
            .for_each(|_| async {})
            .await;

        Ok(())
    }
}

impl Drop for AgoraMetaServer {
    fn drop(&mut self) {
        // Only cleanup if this is the last reference
        if Arc::strong_count(&self.bg_handle) == 1 {
            if let Ok(handle_guard) = self.bg_handle.try_lock() {
                handle_guard.abort();
                println!("Background pruning task stopped");
            }
        }
    }
}
