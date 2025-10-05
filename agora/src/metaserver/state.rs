use super::publisher_info::PublisherInfo;
use crate::ConnectionHandle;
use crate::ping::PingClient;
use crate::utils::{OrError, TreeNode, TreeNodeRef, TreeTrait};
use std::collections::HashMap;

// Shared server state that will be accessed by all connections
#[derive(Debug)]
pub struct ServerState {
    pub path_tree: TreeNodeRef,
    pub publishers: HashMap<String, PublisherInfo>,
    pub confirmed_publishers: HashMap<String, PingClient>, // For each client, initialize a ping-client
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
        self.publishers.insert(path.clone(), publisher_info.clone());
        println!(
            "Registered publisher {:?} at path {}",
            publisher_info, &path
        );
        Ok(publisher_info)
    }

    pub async fn confirm_publisher(&mut self, path: String) -> OrError<()> {
        // Checks that publisher has already been registered
        if !self.publishers.contains_key(&path) {
            return Err(format!(
                "Metaserver confirmation error: \
                 please register path {} before confirming",
                path
            ));
        }

        // Checks against duplicate confirmation
        if self.confirmed_publishers.contains_key(&path) {
            return Err(format!(
                "Metaserver confirmation error: \
                 {} already registered and confirmed",
                path
            ));
        }

        // Extract address and port from stored info
        let publisher_info = self.publishers.get(&path).unwrap();

        let mut pingclient = PingClient::new(&path, publisher_info.connection())
            .await
            .map_err(|e| {
                let _ = self.remove_publisher(path.clone());
                eprintln!(
                    "Removed registered publisher {} upon unsuccessful confirmation",
                    &path
                );
                format!(
                    "Publisher confirmation failed: metaserver failed to create ping client! \
                     Are you running the gateway?: {}",
                    e
                )
            })?;
        let _ = pingclient.ping().await.map_err(|e| {
            let _ = self.remove_publisher(path.clone());
            eprintln!(
                "Removed registered publisher {} upon unsuccessful confirmation",
                &path
            );
            e
        })?; // Make sure that we can ping and obtain results

        // Now that things are ok, add pingclient
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
                pingclient
                    .ping()
                    .await
                    .map_err(|_| format!("Cannot ping {}. Publisher might be stale", path))?;
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
                if let Err(_) = ping_client.ping().await {
                    // Ping failed, mark as stale
                    stale_paths.push(path.clone());
                }
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
