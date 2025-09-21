use super::common::{AgoraMeta, DEFAULT_PORT};
use super::publisher_info::PublisherInfo;
use crate::utils::OrError;
use crate::utils::pathtree::{TreeNode, TreeNodeRef, TreeTrait};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use std::net::{IpAddr, Ipv6Addr};
use tarpc::context;

use futures::{future, prelude::*};
use tarpc::{
    server::{self, Channel, incoming::Incoming},
    tokio_serde::formats::Json,
};

// Shared server state that will be accessed by all connections
#[derive(Debug)]
struct ServerState {
    path_tree: TreeNodeRef,
    publishers: HashMap<String, PublisherInfo>,
}

impl ServerState {
    fn new() -> Self {
        Self {
            path_tree: TreeNode::new("agora"),
            publishers: HashMap::new(),
        }
    }
}

#[derive(Clone)]
pub struct AgoraMetaServer {
    state: Arc<Mutex<ServerState>>,
}

impl AgoraMeta for AgoraMetaServer {
    async fn register_publisher(
        self,
        _: context::Context,
        publisher: PublisherInfo,
        path: String,
    ) -> OrError<()> {
        let mut state = self.state.lock().unwrap();

        // Validate path format strictly
        self.validate_path_format(&path)?;

        // Check if publisher already exists
        if state.publishers.contains_key(&path) {
            return Err(format!(
                "Publisher {:?} already registered at {}. Check path or use `update` instead.",
                publisher, path
            ));
        }

        // Check if any parent path is already a publisher (should be directories only)
        self.validate_parent_paths_are_directories(&state, &path)?;

        // Build parent paths as needed
        self.ensure_path_exists(&mut state, &path)?;

        // Store the publisher
        state.publishers.insert(path, publisher);
        Ok(())
    }

    async fn update_publisher(
        self,
        _: context::Context,
        publisher: PublisherInfo,
        path: String,
    ) -> OrError<()> {
        let mut state = self.state.lock().unwrap();

        // Validate path format strictly
        self.validate_path_format(&path)?;

        if let Err(_) = state.path_tree.get_child(&path) {
            return Err(format!(
                "Path {} does not exist. Current tree:\n{}. Check path or use `register` instead.",
                path,
                state.path_tree.to_string()
            ));
        }
        match state.publishers.insert(path.clone(), publisher.clone()) {
            Some(_) => Ok(()),
            None => Err(format!(
                "No publisher {:?} registered at {}. Check path or use `register` instead.",
                publisher, path
            )),
        }
    }

    async fn remove_publisher(self, _: context::Context, path: String) -> OrError<()> {
        let mut state = self.state.lock().unwrap();

        // Validate path format strictly
        self.validate_path_format(&path)?;

        // Check if publisher exists before doing anything
        if !state.publishers.contains_key(&path) {
            return Err(format!(
                "Can only remove paths associated with publishers: path '{}' is not associated with any publishers.",
                path
            ));
        }

        // Check for child publishers BEFORE removing
        let child_publishers = state
            .publishers
            .keys()
            .filter(|p| p.starts_with(&format!("{}/", path)))
            .collect::<Vec<_>>();

        if !child_publishers.is_empty() {
            return Err(format!(
                "Cannot remove path '{}' with child publishers {:?}. Remove children first.",
                path, child_publishers
            ));
        }

        // Now safe to remove the publisher
        state.publishers.remove(&path);

        // Try to remove the path from the tree if it exists
        if let Ok(_) = state.path_tree.get_child(&path) {
            if let Err(e) = state.path_tree.remove_child(&path) {
                // Don't fail the operation if tree removal fails
                eprintln!("TreeNode operation failed: {}", e);
            }
        }

        Ok(())
    }

    async fn path_tree(self, _: context::Context) -> OrError<String> {
        let state = self.state.lock().unwrap();
        Ok(state.path_tree.to_repr())
    }

    async fn publisher_info(self, _: context::Context, path: String) -> OrError<PublisherInfo> {
        let state = self.state.lock().unwrap();

        // Validate path format strictly
        self.validate_path_format(&path)?;

        match state.publishers.get(&path) {
            Some(publisher) => Ok(publisher.clone()),
            None => Err(format!("Publisher not registered at {}", path)),
        }
    }
}

impl AgoraMetaServer {
    fn new(shared_state: Arc<Mutex<ServerState>>) -> Self {
        Self {
            state: shared_state,
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
                "Path '{}' cannot start with '/' - use relative paths only",
                path
            ));
        }
        if path.ends_with('/') {
            return Err(format!(
                "Path '{}' cannot end with '/' - trailing slashes not allowed",
                path
            ));
        }

        // No double slashes allowed
        if path.contains("//") {
            return Err(format!(
                "Path '{}' contains double slashes '//' - not allowed",
                path
            ));
        }

        // No empty segments (this catches things like "a//b")
        let segments: Vec<&str> = path.split('/').collect();
        for (i, segment) in segments.iter().enumerate() {
            if segment.is_empty() {
                return Err(format!(
                    "Path '{}' has empty segment at position {} - not allowed",
                    path, i
                ));
            }

            // Optional: validate segment characters (no spaces, special chars, etc.)
            if segment.trim() != *segment {
                return Err(format!(
                    "Path segment '{}' has leading/trailing whitespace - not allowed",
                    segment
                ));
            }
        }

        Ok(())
    }

    fn validate_parent_paths_are_directories(
        &self,
        state: &ServerState,
        path: &str,
    ) -> OrError<()> {
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
            if state.publishers.contains_key(&current_path) {
                return Err(format!(
                    "Path parent should all be directories, but '{}' is associated with a publisher. Consider removing first.",
                    current_path
                ));
            }
        }

        Ok(())
    }

    fn ensure_path_exists(&self, state: &mut ServerState, path: &str) -> OrError<()> {
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
            if state.path_tree.get_child(&current_path).is_err() {
                // We need to create this path segment
                let parent_path = if current_path.contains('/') {
                    let last_slash = current_path.rfind('/').unwrap();
                    &current_path[..last_slash]
                } else {
                    ""
                };

                let parent_node = if parent_path.is_empty() {
                    state.path_tree.clone()
                } else {
                    state.path_tree.get_child(parent_path)?
                };

                let new_node = TreeNode::new(part);
                parent_node.add_child(new_node);
            }
        }

        Ok(())
    }

    pub async fn run_server(port: Option<u16>) -> anyhow::Result<()> {
        let port = port.unwrap_or(DEFAULT_PORT);
        let server_addr = (IpAddr::V6(Ipv6Addr::LOCALHOST), port);

        // Create a single shared server state that all connections will use
        let shared_state = Arc::new(Mutex::new(ServerState::new()));

        // JSON transport is provided by the json_transport tarpc module. It makes it easy
        // to start up a serde-powered json serialization strategy over TCP.
        let mut listener = tarpc::serde_transport::tcp::listen(&server_addr, Json::default).await?;
        println!("Listening on port {}", listener.local_addr().port());
        listener.config_mut().max_frame_length(usize::MAX);

        // Listener yields a stream of connections. Each connection is a stream of requests.
        listener
            // Ignore accept errors.
            .filter_map(|r| future::ready(r.ok()))
            .map(server::BaseChannel::with_defaults)
            // Limit channels to 1 per IP.
            .max_channels_per_key(1, |t| t.transport().peer_addr().unwrap().ip())
            // serve is generated by the service attribute. It takes as input any type implementing
            // the generated AgoraMeta trait.
            .map(|channel| {
                // Each channel **represents a single, persistent connection to a client.**
                // BUT they all share the same server state!
                let server = AgoraMetaServer::new(Arc::clone(&shared_state));
                channel.execute(server.serve()).for_each(
                    // Async wrapper around tokio::spawn
                    |fut| async {
                        // tokio::spawn(fut);
                        fut.await;
                    },
                )
            })
            // Max 10 channels.
            .buffer_unordered(10)
            // for_each runs stream to completion. For this particular case, it will run forever.
            // An example stream which terminates would finite-length stream.
            .for_each(|_| async {})
            .await;

        Ok(())
    }
}
