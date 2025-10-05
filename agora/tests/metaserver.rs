mod common;

use common::{create_test_server_state, default_test_connection, test_connection};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn publisher_register_success() {
        let mut process = create_test_server_state();

        // Test registering a new publisher at a path that doesn't exist in tree yet
        let result = process.register_publisher(
            "test_publisher".to_string(),
            "new/endpoint".to_string(),
            default_test_connection(),
        );
        assert!(
            result.is_ok(),
            "Should successfully register publisher at new path"
        );
        let publisher_info = result.unwrap();
        assert_eq!(publisher_info.name(), "test_publisher");

        // Test registering at another new path
        let result2 = process.register_publisher(
            "another_publisher".to_string(),
            "different/path".to_string(),
            test_connection(8082),
        );
        assert!(
            result2.is_ok(),
            "Should successfully register publisher at different new path"
        );

        // Test registering at root level for non-existent path
        let result3 = process.register_publisher(
            "root_publisher".to_string(),
            "nonexistent".to_string(),
            test_connection(8083),
        );
        assert!(
            result3.is_ok(),
            "Should successfully register publisher at new root path"
        );
    }

    #[test]
    fn publisher_register_fail() {
        let mut process = create_test_server_state();

        // Try to register with empty path - should fail
        let empty_path_result = process.register_publisher(
            "test_publisher".to_string(),
            "".to_string(),
            default_test_connection(),
        );
        assert!(
            empty_path_result.is_err(),
            "Should fail to register with empty path"
        );
        assert!(
            empty_path_result.unwrap_err().contains("cannot be empty"),
            "Error message should indicate path cannot be empty"
        );

        // Register at a valid new path first
        let valid_result = process.register_publisher(
            "test_publisher".to_string(),
            "new/path".to_string(),
            default_test_connection(),
        );
        assert!(
            valid_result.is_ok(),
            "Should succeed to register at new path"
        );

        // Try to register at same path again - should fail due to duplicate registration
        let duplicate_result = process.register_publisher(
            "duplicate_publisher".to_string(),
            "new/path".to_string(),
            test_connection(8082),
        );
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
    fn publisher_register_fail_parent_is_publisher() {
        let mut process = create_test_server_state();

        // First register a publisher at "dir"
        let dir_result = process.register_publisher(
            "dir_publisher".to_string(),
            "dir".to_string(),
            default_test_connection(),
        );
        assert!(
            dir_result.is_ok(),
            "Should successfully register publisher at 'dir'"
        );

        // Now try to register a publisher at "dir/content" - should fail
        // because "dir" is already a publisher and should be a directory only
        let content_result = process.register_publisher(
            "content_publisher".to_string(),
            "dir/content".to_string(),
            test_connection(8082),
        );
        assert!(
            content_result.is_err(),
            "Should fail to register under path that has a publisher"
        );
        assert!(
            content_result
                .unwrap_err()
                .contains("associated with a publisher"),
            "Error message should indicate parent path is associated with a publisher"
        );

        // Also test that it fails for deeper nesting: "dir/sub/content" should also fail
        // because "dir" is a publisher
        let deep_content_result = process.register_publisher(
            "deep_content_publisher".to_string(),
            "dir/sub/content".to_string(),
            test_connection(8083),
        );
        assert!(
            deep_content_result.is_err(),
            "Should fail to register under deep path when parent is a publisher"
        );
        assert!(
            deep_content_result
                .unwrap_err()
                .contains("associated with a publisher"),
            "Error message should indicate parent path 'dir' is associated with a publisher"
        );
    }

    #[test]
    fn publisher_register_child_then_parent_fails() {
        let mut process = create_test_server_state();

        // First register a publisher at a deeper path "parent/child"
        let child_result = process.register_publisher(
            "child_publisher".to_string(),
            "parent/child".to_string(),
            default_test_connection(),
        );
        assert!(
            child_result.is_ok(),
            "Should successfully register publisher at 'parent/child'"
        );

        // Now try to register a publisher at "parent" - this should fail
        // because "parent" already exists as a directory in the tree (created during child registration)
        // Publishers can only be registered at new paths, not existing directory nodes
        let parent_result = process.register_publisher(
            "parent_publisher".to_string(),
            "parent".to_string(),
            test_connection(8082),
        );
        assert!(
            parent_result.is_err(),
            "Should fail to register at path that already exists as directory"
        );
        assert!(
            parent_result
                .unwrap_err()
                .contains("already exists as a directory"),
            "Error message should indicate path already exists as directory"
        );
    }

    #[test]
    fn publisher_register_fail_existing_directory() {
        let mut process = create_test_server_state();

        // Try to register at paths that already exist in the initial tree structure
        // These should fail because they're already directory nodes

        // Try to register at "api" - should fail
        let api_result = process.register_publisher(
            "test_publisher".to_string(),
            "api".to_string(),
            default_test_connection(),
        );
        assert!(
            api_result.is_err(),
            "Should fail to register at existing directory 'api'"
        );
        assert!(
            api_result
                .unwrap_err()
                .contains("already exists as a directory"),
            "Error message should indicate path already exists as directory"
        );

        // Try to register at "api/v1" - should fail
        let api_v1_result = process.register_publisher(
            "test_publisher2".to_string(),
            "api/v1".to_string(),
            test_connection(8082),
        );
        assert!(
            api_v1_result.is_err(),
            "Should fail to register at existing directory 'api/v1'"
        );
        assert!(
            api_v1_result
                .unwrap_err()
                .contains("already exists as a directory"),
            "Error message should indicate path already exists as directory"
        );

        // Try to register at "api/v1/users" - should fail
        let users_result = process.register_publisher(
            "test_publisher3".to_string(),
            "api/v1/users".to_string(),
            test_connection(8083),
        );
        assert!(
            users_result.is_err(),
            "Should fail to register at existing directory 'api/v1/users'"
        );
        assert!(
            users_result
                .unwrap_err()
                .contains("already exists as a directory"),
            "Error message should indicate path already exists as directory"
        );
    }

    #[tokio::test]
    async fn empty_path_validation() {
        let mut process = create_test_server_state();

        // Test that all operations reject empty paths

        // Register with empty path
        let register_result = process.register_publisher(
            "test_publisher".to_string(),
            "".to_string(),
            default_test_connection(),
        );
        assert!(register_result.is_err());
        assert!(register_result.unwrap_err().contains("cannot be empty"));

        // Remove with empty path
        let remove_result = process.remove_publisher("".to_string());
        assert!(remove_result.is_err());
        assert!(remove_result.unwrap_err().contains("cannot be empty"));

        // Get publisher info with empty path
        let info_result = process.get_publisher_info("".to_string()).await;
        assert!(info_result.is_err());
        assert!(info_result.unwrap_err().contains("cannot be empty"));
    }
}
