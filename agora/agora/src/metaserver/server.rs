use super::publisher_info::PublisherInfo;
use crate::utils::OrError;
use crate::utils::pathtree::{TreeNode, TreeNodeRef, TreeTrait};
use std::collections::HashMap;

pub struct Metaserver {
    path_tree: TreeNodeRef,
    publishers: HashMap<String, PublisherInfo>,
    port: u16,
}

pub enum MetaserverRequest {
    RegisterPublisher(PublisherInfo, String),
    UpdatePublisher(PublisherInfo, String),
    RemovePublisher(String),
    GetPublisher(String),
    GetPathTree,
    Stop,
}

impl Metaserver {
    pub fn new(name: &str, port: u16) -> Self {
        Self {
            path_tree: TreeNode::new(name),
            publishers: HashMap::new(),
            port: port,
        }
    }

    pub fn register_publisher(self: &mut Self, info: &PublisherInfo, path: &str) -> OrError<()> {
        if let Ok(_) = self.path_tree.get_child(path) {
            return Err(format!(
                "Path {} already exists. Use update_publisher instead",
                path
            ));
        }
        match self.publishers.insert(path.to_string(), info.clone()) {
            None => Ok(()),
            Some(_) => Err(format!(
                "Publisher {:?} already registered at {}",
                info, path
            )),
        }
    }

    pub fn update_publisher(self: &mut Self, info: &PublisherInfo, path: &str) -> OrError<()> {
        if let Err(_) = self.path_tree.get_child(path) {
            return Err(format!(
                "Path {} does not exist. Current tree:\n{}",
                path,
                self.path_tree.to_string()
            ));
        }
        match self.publishers.insert(path.to_string(), info.clone()) {
            Some(_) => Ok(()),
            None => Err(format!("Publisher {:?} not registered at {}", info, path)),
        }
    }

    pub fn remove_publisher(self: &mut Self, path: &str) -> OrError<()> {
        match self.publishers.remove(path) {
            Some(_) => Ok(()),
            None => Err(format!(
                "Unsuccessful removal: publisher not registered at {}",
                path
            )),
        }
    }

    pub fn publisher_info(self: &Self, path: &str) -> OrError<PublisherInfo> {
        match self.publishers.get(path) {
            Some(publisher) => Ok(publisher.clone()),
            None => Err(format!("Publisher not registered at {}", path)),
        }
    }

    pub fn path_tree(&self) -> &TreeNodeRef {
        &self.path_tree
    }

    pub fn stop(self: &mut Self) {
        // Frees all relevant resources and returns
    }

    // This function blocks until server is stopped.
    // pub fn serve(self: &mut Self) {
    //     // Bind to specified port and listen to various types of requests
    //     // Handle each request type.
    //     // parse request_time
    //     while true {
    //         let request_type : MetaserverRequest = ... ;
    //         match request_type {
    //             MetaserverRequest::RegisterPublisher(publisher, path) => {
    //                 self.register_publisher(&publisher, &path);
    //             }
    //             MetaserverRequest::UpdatePublisher(publisher, path) => {
    //                 self.update_publisher(&publisher, &path);
    //             }
    //             ...
    //         }
    //     }
    // }
}
