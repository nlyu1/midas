use super::protocol::{AgoraMeta, DEFAULT_PORT};
use super::publisher_info::PublisherInfo;
use crate::utils::{OrError, PublisherAddressManager, TreeNode, TreeNodeRef, TreeTrait};
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
pub struct ServerState {
    path_tree: TreeNodeRef,
    publishers: HashMap<String, PublisherInfo>,
    address_manager: PublisherAddressManager,
}

impl ServerState {
    pub fn new() -> Self {
        Self {
            path_tree: TreeNode::new("agora"),
            publishers: HashMap::new(),
            address_manager: PublisherAddressManager::new(),
        }
    }

    pub fn path_tree(&self) -> TreeNodeRef {
        self.path_tree.clone()
    }

    pub fn register_publisher(&mut self, name: String, path: String) -> OrError<PublisherInfo> {
        // Validate path format strictly
        self.validate_path_format(&path)?;

        // Check if publisher already exists
        if self.publishers.contains_key(&path) {
            let publisher_info = self.publishers.get(&path).unwrap();
            return Err(format!(
                "Publisher {:?} already registered at {}. Check path or use `update` instead.",
                publisher_info, path
            ));
        }

        // Check if any parent path is already a publisher (should be directories only)
        self.validate_parent_paths_are_directories(&path)?;

        // Check if the path already exists in the tree as a directory
        // Publishers can only be registered at new paths, not existing directory nodes
        if self.path_tree.get_child(&path).is_ok() {
            return Err(format!(
                "Path '{}' already exists as a directory in the tree. Publishers can only be registered at new paths.",
                path
            ));
        }

        // Build parent paths as needed
        self.ensure_path_exists(&path)?;

        // Try allocating address for publisher. If succeeded, register
        let service_address = self.address_manager.allocate_publisher_address()?;
        let publisher_info = PublisherInfo::new(&name, service_address, 8080);
        println!(
            "Registered publisher {:?} at path {}",
            publisher_info, &path
        );
        self.publishers.insert(path, publisher_info.clone());
        Ok(publisher_info)
    }

    pub fn remove_publisher(&mut self, path: String) -> OrError<PublisherInfo> {
        // Validate path format strictly
        self.validate_path_format(&path)?;

        // Check if publisher exists and remove it
        // Note: Due to parent validation, publishers can only exist at leaf nodes,
        // so we don't need to check for child publishers anymore
        match self.publishers.remove(&path) {
            Some(publisher_info) => {
                // Try to remove the path from the tree if it exists
                if let Ok(_) = self.path_tree.get_child(&path) {
                    if let Err(e) = self.path_tree.remove_child(&path) {
                        // Don't fail the operation if tree removal fails
                        eprintln!("TreeNode operation failed: {}", e);
                    }
                }
                println!("Removed publisher {:?} from path {}", publisher_info, &path);
                Ok(publisher_info)
            }
            None => Err(format!(
                "Can only remove paths associated with publishers: path '{}' is not associated with any publishers.",
                path
            )),
        }
    }

    pub fn get_path_tree_repr(&self) -> String {
        self.path_tree.to_repr()
    }

    pub fn get_publisher_info(&self, path: String) -> OrError<PublisherInfo> {
        // Validate path format strictly
        self.validate_path_format(&path)?;

        match self.publishers.get(&path) {
            Some(publisher) => Ok(publisher.clone()),
            None => Err(format!("Publisher not registered at {}", path)),
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
                    "Path parent should all be directories, but '{}' is associated with a publisher. Consider removing first.",
                    current_path
                ));
            }
        }

        Ok(())
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
    state: Arc<Mutex<ServerState>>,
}

impl AgoraMeta for AgoraMetaServer {
    async fn register_publisher(
        self,
        _: context::Context,
        name: String,
        path: String,
    ) -> OrError<PublisherInfo> {
        let mut state = self.state.lock().unwrap();
        state.register_publisher(name, path)
    }

    async fn remove_publisher(self, _: context::Context, path: String) -> OrError<PublisherInfo> {
        let mut state = self.state.lock().unwrap();
        state.remove_publisher(path)
    }

    async fn path_tree(self, _: context::Context) -> OrError<String> {
        let state = self.state.lock().unwrap();
        Ok(state.get_path_tree_repr())
    }

    async fn publisher_info(self, _: context::Context, path: String) -> OrError<PublisherInfo> {
        let state = self.state.lock().unwrap();
        state.get_publisher_info(path)
    }
}

impl AgoraMetaServer {
    fn new(shared_state: Arc<Mutex<ServerState>>) -> Self {
        Self {
            state: shared_state,
        }
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
