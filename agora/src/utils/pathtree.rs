//! Hierarchical tree structure for metaserver publisher registry.
//! `TreeNode` provides thread-safe path operations (add, remove, traverse) with invariant: publishers are leaves, ancestors are directories.

use crate::utils::OrError;
use crate::agora_error;
use anyhow::{anyhow, bail};
use std::fmt;
use std::sync::{Arc, Weak, Mutex};

/// Hierarchical tree node for publisher path registry in metaserver.
/// Thread-safe via `Arc`/`Weak` parent-child links and `Mutex`-protected children vector.
/// Invariant: Publishers are leaves, all ancestors are directories. Supports path operations (add, remove, traverse).
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct TreeNode {
    name: String,
    children: Mutex<Vec<Arc<TreeNode>>>,
    parent: Mutex<Option<Weak<TreeNode>>>,
}

pub type TreeNodeRef = Arc<TreeNode>;

pub trait TreeTrait {
    fn new(name: &str) -> Arc<Self>;
    fn add_children(self: &Arc<Self>, names: &[&str]);
    fn add_child(self: &Arc<Self>, child: TreeNodeRef);
    fn get_child(self: &Arc<Self>, path: &str) -> OrError<TreeNodeRef>;
    fn remove_child(self: &Arc<Self>, name: &str) -> OrError<()>;
    fn remove_child_and_branch(self: &Arc<Self>, path: &str) -> OrError<()>;
    fn parent(&self) -> OrError<TreeNodeRef>;
    fn root(self: &Arc<Self>) -> TreeNodeRef;
    fn children(&self) -> Vec<TreeNodeRef>;
    fn name(&self) -> &str;
    fn path(&self) -> String;
    fn is_root(&self) -> bool;
    fn is_leaf(&self) -> bool;
    fn display_tree(&self) -> String;
    fn to_repr(&self) -> String;
    fn from_repr(repr: &str) -> OrError<TreeNodeRef>;
}

impl TreeTrait for TreeNode {
    /// Creates a new tree node with the given name.
    /// Panics if name contains '/' (use paths for hierarchy instead).
    fn new(name: &str) -> Arc<Self> {
        assert!(!name.contains('/'), "TreeNode name cannot contain slashes");
        Arc::new(TreeNode {
            name: name.into(),
            children: Mutex::new(Vec::new()),
            parent: Mutex::new(None),
        })
    }

    fn add_children(self: &Arc<Self>, names: &[&str]) {
        for name in names {
            let child = TreeNode::new(name);
            *child.parent.lock().unwrap() = Some(Arc::downgrade(self));
            self.children.lock().unwrap().push(child);
        }
    }

    fn add_child(self: &Arc<Self>, child: TreeNodeRef) {
        // Set parent of child
        *child.parent.lock().unwrap() = Some(Arc::downgrade(self));
        // Add child to this node
        self.children.lock().unwrap().push(child);
    }

    /// Traverses tree by slash-separated path (e.g., `"a/b/c"`).
    /// Returns the target node if found.
    /// Error: Child not found at any level → propagates to `ServerState` methods.
    fn get_child(self: &Arc<Self>, path: &str) -> OrError<TreeNodeRef> {
        if path.is_empty() {
            return Ok(self.clone());
        }

        let path_parts: Vec<&str> = path.split('/').collect();
        let mut current_node = self.clone();

        for part in path_parts {
            if part.is_empty() {
                continue; // Skip empty parts (e.g., from leading/trailing slashes)
            }

            current_node = current_node.get_immediate_child(part)?;
        }

        Ok(current_node)
    }

    fn remove_child(self: &TreeNodeRef, path: &str) -> OrError<()> {
        let child = self.get_child(path)?;
        let child_name = child.name();
        child.parent()?.remove_immediate_child(child_name)
    }

    /// Removes leaf node and all ancestors up to first branching point (node with >1 child).
    /// Used by `ServerState` to clean up publisher paths when they're removed.
    /// Error: Target is root or has children → returns error to `ServerState::remove_publisher`.
    fn remove_child_and_branch(self: &TreeNodeRef, path: &str) -> OrError<()> {
        let child = self.get_child(path)?;
        if child.is_root() {
            bail!(agora_error!("utils::TreeNode", "remove_child_and_branch", "cannot remove root node"));
        }
        if !child.is_leaf() {
            bail!(agora_error!("utils::TreeNode", "remove_child_and_branch", "cannot remove non-leaf node"));
        }
        // Find branching point: walk up until node with >1 children
        let (branching_ancestor, branch_name) = child.branching_ancestor()?;
        branching_ancestor.remove_immediate_child(&branch_name)?;
        Ok(())
    }

    fn parent(&self) -> OrError<TreeNodeRef> {
        match self.parent.lock().unwrap().as_ref() {
            Some(parent) => Ok(parent.upgrade().unwrap()),
            None => bail!(agora_error!("utils::TreeNode", "parent", &format!("parent not found for '{}'", self.name))),
        }
    }

    fn root(self: &Arc<Self>) -> TreeNodeRef {
        match self.parent() {
            Ok(parent) => parent.root(),
            Err(_) => Arc::clone(self),
        }
    }

    fn children(&self) -> Vec<TreeNodeRef> {
        self.children.lock().unwrap().clone()
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn path(&self) -> String {
        if self.is_root() {
            format!("/{}", self.name())
        } else {
            format!("{}/{}", self.parent().unwrap().path(), self.name())
        }
    }

    fn is_root(&self) -> bool {
        self.parent.lock().unwrap().is_none()
    }

    fn is_leaf(&self) -> bool {
        self.children.lock().unwrap().is_empty()
    }

    fn display_tree(&self) -> String {
        self.to_string_helper("", true)
    }

    fn to_repr(&self) -> String {
        // Simple JSON-like representation: {"name": "root", "children": [...]}
        self.to_repr_helper()
    }

    /// Deserializes tree from custom JSON-like format: `"leaf"` or `{"parent":[children...]}`.
    /// Error: Invalid format → propagates to `AgoraClient::get_path_tree` caller.
    fn from_repr(repr: &str) -> OrError<TreeNodeRef> {
        TreeNode::from_repr_helper(repr)
    }
}

impl fmt::Display for TreeNode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Remove the trailing newline for cleaner display formatting
        let tree_str = self.display_tree();
        write!(f, "{}", tree_str.trim_end())
    }
}

impl TreeNode {
    fn to_string_helper(&self, prefix: &str, is_last: bool) -> String {
        let mut result = String::new();

        // Add current node
        let connector = if is_last { "└── " } else { "├── " };
        result.push_str(&format!("{}{}{}\n", prefix, connector, self.name));

        // Prepare prefix for children
        let child_prefix = if is_last {
            format!("{}    ", prefix) // Four spaces for last items
        } else {
            format!("{}│   ", prefix) // Vertical bar + three spaces for non-last items
        };

        let children = self.children.lock().unwrap();
        let child_count = children.len();

        for (i, child) in children.iter().enumerate() {
            let is_last_child = i == child_count - 1;
            result.push_str(&child.to_string_helper(&child_prefix, is_last_child));
        }

        result
    }

    fn get_immediate_child(self: &Arc<Self>, name: &str) -> OrError<TreeNodeRef> {
        self.children
            .lock().unwrap()
            .iter()
            .find(|child| child.name == name)
            .cloned()
            .ok_or_else(|| {
                anyhow!(agora_error!("utils::TreeNode", "get_immediate_child",
                    &format!("child '{}' not found under '{}'", name, self.name)))
            })
    }

    fn remove_immediate_child(self: &TreeNodeRef, name: &str) -> OrError<()> {
        let mut children = self.children.lock().unwrap();
        let initial_len = children.len();

        children.retain(|child| child.name != name);

        if children.len() == initial_len {
            bail!(agora_error!("utils::TreeNode", "remove_immediate_child",
                &format!("child '{}' not found under '{}'", name, self.name)));
        } else {
            Ok(())
        }
    }

    // Returns the closest-to-root node whose child path contains the current node
    // Asserts that this is not the root; this should have been handled on the top-level
    fn branching_ancestor(self: &Arc<Self>) -> OrError<(TreeNodeRef, String)> {
        assert!(
            !self.is_root(),
            "Helper should not have been called on root node. Fix bug."
        );
        let parent = self.parent().unwrap();
        if parent.is_root() {
            return Ok((parent, self.name.clone()));
        }
        // If parent has more than one child, then it's the immediate branching ancestor
        if parent.children().len() > 1 {
            return Ok((parent, self.name.clone()));
        }
        // Otherwise, keep looking up the tree
        parent.branching_ancestor()
    }

    fn to_repr_helper(&self) -> String {
        let children_repr: Vec<String> = self
            .children
            .lock().unwrap()
            .iter()
            .map(|child| child.to_repr_helper())
            .collect();

        if children_repr.is_empty() {
            format!("\"{}\"", self.name)
        } else {
            format!("{{\"{}\":[{}]}}", self.name, children_repr.join(","))
        }
    }

    fn from_repr_helper(repr: &str) -> OrError<TreeNodeRef> {
        let repr = repr.trim();

        // Branch 1: Leaf node - just a quoted string "name"
        if repr.starts_with('"') && repr.ends_with('"') && !repr.contains('[') {
            let name = &repr[1..repr.len() - 1];
            return Ok(TreeNode::new(name));
        }

        // Branch 2: Parent node - {"name":[children...]}
        if !repr.starts_with('{') || !repr.ends_with('}') {
            bail!(agora_error!("utils::TreeNode", "from_repr",
                &format!("invalid format: expected {{...}} or \"...\", got: {}", repr)));
        }

        let inner = &repr[1..repr.len() - 1];

        // Parse: name (before ':') and children array (after ':')
        if let Some(colon_pos) = inner.find(':') {
            let name_part = inner[..colon_pos].trim();
            if !name_part.starts_with('"') || !name_part.ends_with('"') {
                bail!(agora_error!("utils::TreeNode", "from_repr",
                    &format!("invalid name format: {}", name_part)));
            }
            let name = &name_part[1..name_part.len() - 1];

            let children_part = inner[colon_pos + 1..].trim();
            if !children_part.starts_with('[') || !children_part.ends_with(']') {
                bail!(agora_error!("utils::TreeNode", "from_repr",
                    &format!("invalid children format: {}", children_part)));
            }

            let node = TreeNode::new(name);

            // Recursively parse children array
            let children_inner = &children_part[1..children_part.len() - 1].trim();
            if !children_inner.is_empty() {
                let child_reprs = TreeNode::split_repr_array(children_inner)?;
                for child_repr in child_reprs {
                    let child = TreeNode::from_repr_helper(&child_repr)?;
                    node.add_child(child);
                }
            }

            Ok(node)
        } else {
            bail!(agora_error!("utils::TreeNode", "from_repr",
                &format!("invalid format: no colon found in {}", inner)));
        }
    }

    // Splits comma-separated array respecting nesting depth and quotes.
    // Only splits on commas at depth=0 (outside all braces/brackets and quotes).
    fn split_repr_array(s: &str) -> OrError<Vec<String>> {
        let mut result = Vec::new();
        let mut current = String::new();
        let mut depth = 0;
        let mut in_quotes = false;
        let mut escape_next = false;

        for ch in s.chars() {
            if escape_next {
                current.push(ch);
                escape_next = false;
                continue;
            }

            match ch {
                '\\' => {
                    escape_next = true;
                    current.push(ch);
                }
                '"' => {
                    in_quotes = !in_quotes;
                    current.push(ch);
                }
                '{' | '[' if !in_quotes => {
                    depth += 1;
                    current.push(ch);
                }
                '}' | ']' if !in_quotes => {
                    depth -= 1;
                    current.push(ch);
                }
                ',' if !in_quotes && depth == 0 => {
                    // Split point: top-level comma outside quotes
                    result.push(current.trim().to_string());
                    current.clear();
                }
                _ => {
                    current.push(ch);
                }
            }
        }

        if !current.trim().is_empty() {
            result.push(current.trim().to_string());
        }

        Ok(result)
    }
}
