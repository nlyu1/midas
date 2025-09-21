use agora::metaserver::{AgoraMetaServer, PublisherInfo};
use agora::utils::{TreeNode, TreeTrait};

fn create_test_instance() -> Metaserver {
    let process = Metaserver::new("test_server", 8080);

    // Create a basic tree structure for testing
    process
        .path_tree()
        .add_children(&["api", "static", "admin"]);
    let api = process.path_tree().get_child("api").unwrap();
    api.add_children(&["v1", "v2"]);
    let v1 = api.get_child("v1").unwrap();
    v1.add_children(&["users", "posts", "auth"]);

    process
}

fn create_test_publisher() -> PublisherInfo {
    PublisherInfo::new("test_publisher")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn publisher_register_success() {
        let mut process = create_test_instance();
        let publisher = create_test_publisher();

        // Test registering a new publisher at a path that doesn't exist in tree yet
        let result = process.register_publisher(&publisher, "new/endpoint");
        assert!(
            result.is_ok(),
            "Should successfully register publisher at new path"
        );

        // Test registering at another new path
        let publisher2 = PublisherInfo::new("another_publisher");
        let result2 = process.register_publisher(&publisher2, "different/path");
        assert!(
            result2.is_ok(),
            "Should successfully register publisher at different new path"
        );

        // Test registering at root level for non-existent path
        let publisher3 = PublisherInfo::new("root_publisher");
        let result3 = process.register_publisher(&publisher3, "nonexistent");
        assert!(
            result3.is_ok(),
            "Should successfully register publisher at new root path"
        );
    }

    #[test]
    fn publisher_register_fail() {
        let mut process = create_test_instance();
        let publisher = create_test_publisher();

        // Try to register at a path that already exists in the tree - should fail
        let result = process.register_publisher(&publisher, "api/v1/users");
        assert!(
            result.is_err(),
            "Should fail to register publisher at existing path"
        );
        assert!(
            result.unwrap_err().contains("already exists"),
            "Error message should indicate path already exists"
        );

        // Try to register at another existing path
        let result2 = process.register_publisher(&publisher, "admin");
        assert!(
            result2.is_err(),
            "Should fail to register publisher at existing root path"
        );
        assert!(
            result2.unwrap_err().contains("already exists"),
            "Error message should indicate path already exists"
        );

        // Register at a valid new path first
        let valid_result = process.register_publisher(&publisher, "new/path");
        assert!(
            valid_result.is_ok(),
            "Should succeed to register at new path"
        );

        // Try to register at same path again - should fail due to duplicate registration
        let publisher2 = PublisherInfo::new("duplicate_publisher");
        let duplicate_result = process.register_publisher(&publisher2, "new/path");
        assert!(
            duplicate_result.is_err(),
            "Should fail to register duplicate at same path"
        );
        assert!(
            duplicate_result.unwrap_err().contains("already registered"),
            "Error message should indicate publisher already registered"
        );
    }

    #[test]
    fn publisher_update_success() {
        let mut process = create_test_instance();
        let publisher = create_test_publisher();

        // First register a publisher at a new path (not in existing tree)
        let result = process.register_publisher(&publisher, "new/endpoint");
        assert!(result.is_ok(), "Initial registration should succeed");

        // Add the path to the tree so update can work
        process.path_tree().add_child(TreeNode::new("new"));
        let new_node = process.path_tree().get_child("new").unwrap();
        new_node.add_child(TreeNode::new("endpoint"));

        // Now update the publisher - should succeed because path exists in tree AND publisher is registered
        let updated_publisher = PublisherInfo::new("updated_publisher");
        let result2 = process.update_publisher(&updated_publisher, "new/endpoint");
        assert!(
            result2.is_ok(),
            "Should successfully update existing publisher"
        );

        // Test updating at an existing tree path with registered publisher
        let root_publisher = PublisherInfo::new("root_publisher");
        // Register at a path not in tree first
        process
            .register_publisher(&root_publisher, "newroot")
            .unwrap();
        // Add to tree
        process.path_tree().add_child(TreeNode::new("newroot"));
        // Now update should work
        let updated_root = PublisherInfo::new("updated_root_publisher");
        let result3 = process.update_publisher(&updated_root, "newroot");
        assert!(
            result3.is_ok(),
            "Should successfully update publisher at root level"
        );
    }

    #[test]
    fn publisher_update_fail() {
        let mut process = create_test_instance();
        let publisher = create_test_publisher();

        // Try to update a publisher at a path that doesn't exist in tree
        let result = process.update_publisher(&publisher, "nonexistent/path");
        assert!(
            result.is_err(),
            "Should fail to update publisher at nonexistent path"
        );
        assert!(
            result.unwrap_err().contains("does not exist"),
            "Error message should indicate path does not exist"
        );

        // Try to update a publisher that exists in tree but is not registered
        let result2 = process.update_publisher(&publisher, "api/v1/posts");
        assert!(
            result2.is_err(),
            "Should fail to update unregistered publisher"
        );
        assert!(
            result2.unwrap_err().contains("not registered"),
            "Error message should indicate publisher not registered"
        );

        // Test with empty path - this should return the root, but no publisher registered there
        let result3 = process.update_publisher(&publisher, "");
        assert!(
            result3.is_err(),
            "Should fail to update publisher with empty path"
        );
        assert!(
            result3.unwrap_err().contains("not registered"),
            "Error message should indicate publisher not registered"
        );

        // Test updating after registering but then path becomes invalid due to tree changes
        process.register_publisher(&publisher, "temp/path").unwrap();
        // Add path to tree temporarily
        process.path_tree().add_child(TreeNode::new("temp"));
        let temp_node = process.path_tree().get_child("temp").unwrap();
        temp_node.add_child(TreeNode::new("path"));
        // Remove the path from tree
        process.path_tree().remove_child("temp").unwrap();
        // Now update should fail because path no longer exists in tree
        let updated_publisher = PublisherInfo::new("updated");
        let result4 = process.update_publisher(&updated_publisher, "temp/path");
        assert!(
            result4.is_err(),
            "Should fail to update when path removed from tree"
        );
        assert!(
            result4.unwrap_err().contains("does not exist"),
            "Error message should indicate path does not exist"
        );
    }
}
