use super::publisher_info::PublisherInfo;
use crate::ConnectionHandle;
use crate::ping::PingClient;
use crate::utils::{OrError, TreeNode, TreeNodeRef, TreeTrait};
use crate::{agora_error, agora_error_cause};
use std::collections::HashMap;

// Shared server state that will be accessed by all connections
#[derive(Debug)]
pub struct ServerState {
    pub path_tree: TreeNodeRef,
    pub publishers: HashMap<String, PublisherInfo>,
    pub confirmed_publishers: HashMap<String, PingClient>,
}

impl Default for ServerState {
    fn default() -> Self {
        Self::new()
    }
}

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

    /// Registers publisher at path after validation.
    /// Validates: path format, not duplicate, parents are directories, path is new leaf.
    /// Error: Validation fails → returns to AgoraClient RPC caller → Publisher::new.
    /// Called by: AgoraMetaServer (TARPC handler) ← AgoraClient::register_publisher ← Publisher::new
    pub fn register_publisher(
        &mut self,
        name: String,
        path: String,
        host_connection: ConnectionHandle,
    ) -> OrError<PublisherInfo> {
        self.validate_path_format(&path)?;

        // Check for duplicate registration
        if self.publishers.contains_key(&path) {
            let publisher_info = self.publishers.get(&path).unwrap();
            return Err(agora_error!("metaserver::ServerState", "register_publisher",
                &format!("publisher {:?} already registered at {}. Check path or use `update` instead",
                    publisher_info, path)));
        }

        // Invariant: All parent paths must be directories (not publishers)
        self.validate_parent_paths_are_directories(&path)?;

        // Path must be new (not existing directory node)
        if self.path_tree.get_child(&path).is_ok() {
            return Err(agora_error!("metaserver::ServerState", "register_publisher",
                &format!("path '{}' already exists as a directory in the tree. Publishers can only be registered at new paths", path)));
        }

        // Create directory nodes for path segments
        self.ensure_path_exists(&path)?;

        let publisher_info = PublisherInfo::new(&name, host_connection, &path);
        self.publishers.insert(path.clone(), publisher_info.clone());
        println!(
            "Registered publisher {:?} at path {}",
            publisher_info, &path
        );
        Ok(publisher_info)
    }

    /// Confirms publisher by creating ping client and testing connection.
    /// Auto-removes publisher from registry if ping fails.
    /// Error: Not registered, already confirmed, or ping fails → returns to Publisher::new.
    /// Called by: AgoraMetaServer (TARPC) ← AgoraClient::confirm_publisher ← Publisher::new
    pub async fn confirm_publisher(&mut self, path: String) -> OrError<()> {
        if !self.publishers.contains_key(&path) {
            return Err(agora_error!("metaserver::ServerState", "confirm_publisher",
                &format!("please register path {} before confirming", path)));
        }

        if self.confirmed_publishers.contains_key(&path) {
            return Err(agora_error!("metaserver::ServerState", "confirm_publisher",
                &format!("path {} already registered and confirmed", path)));
        }

        let publisher_info = self.publishers.get(&path).unwrap();

        // Create ping client - if fails, auto-remove registration
        let mut pingclient = PingClient::new(&path, publisher_info.connection())
            .await
            .map_err(|e| {
                let _ = self.remove_publisher(path.clone());
                eprintln!(
                    "Removed registered publisher {} upon unsuccessful confirmation",
                    &path
                );
                agora_error_cause!("metaserver::ServerState", "confirm_publisher",
                    "failed to create ping client. Are you running the gateway?", e)
            })?;

        // Test ping - if fails, auto-remove registration
        let _ = pingclient.ping().await.inspect_err(|_| {
            let _ = self.remove_publisher(path.clone());
            eprintln!(
                "Removed registered publisher {} upon unsuccessful confirmation",
                &path
            );
        })?;

        // Success: store ping client for health checks
        println!("Publisher {} confirmed.", &path);
        self.confirmed_publishers.insert(path, pingclient);
        Ok(())
    }

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
            None => Err(agora_error!("metaserver::ServerState", "remove_publisher",
                &format!("path '{}' is not associated with any publishers", path))),
        }
    }

    pub fn get_path_tree_repr(&self) -> String {
        self.path_tree.to_repr()
    }

    /// Returns publisher info after pinging to verify it's alive.
    /// Error: Not found, not confirmed, or ping fails → returns to Subscriber::new.
    /// Called by: AgoraMetaServer (TARPC) ← AgoraClient::get_publisher_info ← Subscriber::new
    pub async fn get_publisher_info(&mut self, path: String) -> OrError<PublisherInfo> {
        self.validate_path_format(&path)?;

        match (
            self.publishers.get(&path),
            self.confirmed_publishers.get_mut(&path),
        ) {
            (Some(publisher), Some(pingclient)) => {
                // Ping before returning to ensure publisher is alive
                pingclient
                    .ping()
                    .await
                    .map_err(|e| agora_error_cause!("metaserver::ServerState", "get_publisher_info",
                        &format!("cannot ping {}. Publisher might be stale", path), e))?;
                Ok(publisher.clone())
            }
            (Some(_), None) => Err(agora_error!("metaserver::ServerState", "get_publisher_info",
                &format!("publisher at {} is registered but not confirmed", path))),
            (None, _) => Err(agora_error!("metaserver::ServerState", "get_publisher_info",
                &format!("publisher not registered at {}", path))),
        }
    }

    fn validate_path_format(&self, path: &str) -> OrError<()> {
        if path.is_empty() {
            return Err(agora_error!("metaserver::ServerState", "validate_path_format",
                "path cannot be empty"));
        }

        if path.starts_with('/') {
            return Err(agora_error!("metaserver::ServerState", "validate_path_format",
                &format!("path '{}' cannot start with '/' - use relative paths only", path)));
        }
        if path.ends_with('/') {
            return Err(agora_error!("metaserver::ServerState", "validate_path_format",
                &format!("path '{}' cannot end with '/' - trailing slashes not allowed", path)));
        }

        if path.contains("//") {
            return Err(agora_error!("metaserver::ServerState", "validate_path_format",
                &format!("path '{}' contains double slashes '//' - not allowed", path)));
        }

        let segments: Vec<&str> = path.split('/').collect();
        for (i, segment) in segments.iter().enumerate() {
            if segment.is_empty() {
                return Err(agora_error!("metaserver::ServerState", "validate_path_format",
                    &format!("path '{}' has empty segment at position {} - not allowed", path, i)));
            }

            if segment.trim() != *segment {
                return Err(agora_error!("metaserver::ServerState", "validate_path_format",
                    &format!("path segment '{}' has leading/trailing whitespace - not allowed", segment)));
            }
        }

        Ok(())
    }

    // Validates that all parent paths are directories (not publishers).
    // Invariant: Publishers are leaves, all ancestors are pure directories.
    fn validate_parent_paths_are_directories(&self, path: &str) -> OrError<()> {
        let path_parts: Vec<&str> = path.split('/').collect();
        let mut current_path = String::new();

        // Check each parent (not the leaf itself)
        for (i, part) in path_parts.iter().enumerate() {
            if i == path_parts.len() - 1 {
                break; // Skip leaf - we're registering a publisher here
            }

            if current_path.is_empty() {
                current_path = part.to_string();
            } else {
                current_path = format!("{}/{}", current_path, part);
            }

            // Error if parent is a publisher
            if self.publishers.contains_key(&current_path) {
                return Err(agora_error!("metaserver::ServerState", "validate_parent_paths_are_directories",
                    &format!("path parent should all be directories, but '{}' is associated with a publisher. Consider removing first", current_path)));
            }
        }

        Ok(())
    }

    /// Pings all confirmed publishers, removes those that fail to respond.
    /// Called by: Background pruning task in AgoraMetaServer::run_server (every 500ms).
    /// Returns: List of pruned paths for logging.
    pub async fn prune_stale_publishers(&mut self) -> Vec<String> {
        let mut stale_paths: Vec<String> = Vec::new();

        // Snapshot paths to avoid borrow issues
        let paths_to_check: Vec<String> = self.confirmed_publishers.keys().cloned().collect();

        // Ping each publisher - collect failures
        for path in paths_to_check {
            if let Some(ping_client) = self.confirmed_publishers.get_mut(&path)
                && let Err(_) = ping_client.ping().await {
                stale_paths.push(path.clone());
            }
        }

        // Remove stale publishers from registry and tree
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
