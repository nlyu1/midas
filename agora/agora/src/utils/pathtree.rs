use crate::utils::OrError;
use std::cell::RefCell;
use std::fmt;
use std::rc::{Rc, Weak};

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct TreeNode {
    name: String,
    children: RefCell<Vec<Rc<TreeNode>>>,
    parent: RefCell<Option<Weak<TreeNode>>>,
}

pub type TreeNodeRef = Rc<TreeNode>;

pub trait TreeTrait {
    fn new(name: &str) -> Rc<Self>;
    fn add_children(self: &Rc<Self>, names: &[&str]);
    fn add_child(self: &Rc<Self>, child: TreeNodeRef);
    fn get_child(self: &Rc<Self>, path: &str) -> OrError<TreeNodeRef>;
    fn remove_child(self: &Rc<Self>, name: &str) -> OrError<()>;
    fn parent(&self) -> OrError<TreeNodeRef>;
    fn root(self: &Rc<Self>) -> TreeNodeRef;
    fn children(&self) -> Vec<TreeNodeRef>;
    fn name(&self) -> &str;
    fn path(&self) -> String;
    fn is_root(&self) -> bool;
    fn is_leaf(&self) -> bool;
    fn display_tree(&self) -> String;
    fn to_repr(&self) -> String;
    fn from_repr(repr: &str) -> OrError<TreeNodeRef>;
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

        let children = self.children.borrow();
        let child_count = children.len();

        for (i, child) in children.iter().enumerate() {
            let is_last_child = i == child_count - 1;
            result.push_str(&child.to_string_helper(&child_prefix, is_last_child));
        }

        result
    }

    fn get_immediate_child(self: &Rc<Self>, name: &str) -> OrError<TreeNodeRef> {
        // Returns child if exists. Note that modifying child will modify original tree.
        // Change type annotation as necessary to complete this functionality.
        self.children
            .borrow()
            .iter()
            .find(|child| child.name == name)
            .cloned()
            .ok_or_else(|| {
                format!(
                    "Cannot get immediate child: '{}' not found under '{}'",
                    name, self.name
                )
            })
    }

    fn remove_immediate_child(self: &TreeNodeRef, name: &str) -> OrError<()> {
        // Look for "name" if exists and deletes; frees correctly. Else complains [name] not found under [self.path]
        let mut children = self.children.borrow_mut();
        let initial_len = children.len();

        children.retain(|child| child.name != name);

        if children.len() == initial_len {
            Err(format!(
                "Cannot remove immediate child: '{}' not found under '{}'",
                name, self.name
            ))
        } else {
            Ok(())
        }
    }

    fn to_repr_helper(&self) -> String {
        let children_repr: Vec<String> = self
            .children
            .borrow()
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
        // Simple parser for our custom format
        let repr = repr.trim();

        // Handle leaf node case: just a quoted string
        if repr.starts_with('"') && repr.ends_with('"') && !repr.contains('[') {
            let name = &repr[1..repr.len() - 1]; // Remove quotes
            return Ok(TreeNode::new(name));
        }

        // Handle parent node case: {"name":[children...]}
        if !repr.starts_with('{') || !repr.ends_with('}') {
            return Err(format!(
                "Invalid format: expected {{...}} or \"...\", got: {}",
                repr
            ));
        }

        let inner = &repr[1..repr.len() - 1]; // Remove outer braces

        // Find the name (quoted string before the colon)
        if let Some(colon_pos) = inner.find(':') {
            let name_part = inner[..colon_pos].trim();
            if !name_part.starts_with('"') || !name_part.ends_with('"') {
                return Err(format!("Invalid name format: {}", name_part));
            }
            let name = &name_part[1..name_part.len() - 1]; // Remove quotes

            let children_part = inner[colon_pos + 1..].trim();
            if !children_part.starts_with('[') || !children_part.ends_with(']') {
                return Err(format!("Invalid children format: {}", children_part));
            }

            let node = TreeNode::new(name);

            // Parse children array
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
            Err(format!("Invalid format: no colon found in {}", inner))
        }
    }

    fn split_repr_array(s: &str) -> OrError<Vec<String>> {
        // Simple array splitter that respects nesting
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

impl TreeTrait for TreeNode {
    fn new(name: &str) -> Rc<Self> {
        // assert that name does not contain slashes
        println!("Creating TreeNode with name: {}", name);
        assert!(!name.contains('/'), "TreeNode name cannot contain slashes");
        Rc::new(TreeNode {
            name: name.into(),
            children: RefCell::new(Vec::new()),
            parent: RefCell::new(None),
        })
    }

    fn add_children(self: &Rc<Self>, names: &[&str]) {
        // Create children of names as specified; set their parents correctly
        for name in names {
            let child = TreeNode::new(&name);
            // Set parent of child
            *child.parent.borrow_mut() = Some(Rc::downgrade(self));
            // Add child to this node
            self.children.borrow_mut().push(child);
        }
    }

    fn add_child(self: &Rc<Self>, child: TreeNodeRef) {
        // Set parent of child
        *child.parent.borrow_mut() = Some(Rc::downgrade(self));
        // Add child to this node
        self.children.borrow_mut().push(child);
    }

    fn get_child(self: &Rc<Self>, path: &str) -> OrError<TreeNodeRef> {
        // Same as "get_child", except might be recursive child1/child2/...
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
        let child = self.get_child(&path)?;
        let child_name = child.name();
        child.parent()?.remove_immediate_child(child_name)
    }

    fn parent(&self) -> OrError<TreeNodeRef> {
        match self.parent.borrow().as_ref() {
            Some(parent) => Ok(parent.upgrade().unwrap()),
            None => Err(format!("Parent not found for '{}'", self.name)),
        }
    }

    fn root(self: &Rc<Self>) -> TreeNodeRef {
        match self.parent() {
            Ok(parent) => parent.root(),
            Err(_) => Rc::clone(self),
        }
    }

    fn children(&self) -> Vec<TreeNodeRef> {
        self.children.borrow().clone()
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn path(&self) -> String {
        if self.is_root() {
            format!("/{}", self.name().to_string())
        } else {
            format!("{}/{}", self.parent().unwrap().path(), self.name())
        }
    }

    fn is_root(&self) -> bool {
        self.parent.borrow().is_none()
    }

    fn is_leaf(&self) -> bool {
        self.children.borrow().is_empty()
    }

    fn display_tree(&self) -> String {
        self.to_string_helper("", true)
    }

    fn to_repr(&self) -> String {
        // Simple JSON-like representation: {"name": "root", "children": [...]}
        self.to_repr_helper()
    }

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
