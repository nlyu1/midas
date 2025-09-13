use agora::utils::{TreeNode, TreeTrait};
use std::rc::Rc;

fn create_test_sample() -> Rc<TreeNode> {
    let root = TreeNode::new("project");
    root.add_children(&["src", "target", "tests", "docs"]);
    let src = root.get_child("src").unwrap();
    src.add_children(&["main.rs", "utils.rs"]);
    let target = root.get_child("target").unwrap();
    target.add_children(&["debug", "release"]);
    let tests = root.get_child("tests").unwrap();
    tests.add_children(&["test1.rs", "test2.rs"]);
    return root;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn print_children_test() {
        let root = TreeNode::new("project");
        root.add_children(&["src", "docs", "tests"]);
        let tree_output = root.to_string();

        let expected = "└── project\n    ├── src\n    ├── docs\n    └── tests";
        assert_eq!(
            tree_output, expected,
            "Tree structure should match expected format"
        );
    }

    #[test]
    fn children_test() {
        // Tests that:
        // - Root has 4 children, as specified
        // - Immediate and recursive children can be accessed by path
        let root = create_test_sample();
        assert_eq!(root.children().len(), 4, "Root should have 4 children");

        // Check that children can be accessed
        assert_eq!(
            root.get_child("src").unwrap().name(),
            "src",
            "src should be a child of root"
        );
        assert_eq!(
            root.get_child("docs").unwrap().name(),
            "docs",
            "docs should be a child of root"
        );
        assert_eq!(
            root.get_child("tests").unwrap().name(),
            "tests",
            "tests should be a child of root"
        );
        assert_eq!(
            root.get_child("target").unwrap().name(),
            "target",
            "target should be a child of root"
        );
        // Nonexistent child should return None
        match root.get_child("does_not_exist") {
            Err(_) => (),
            Ok(_) => panic!("should not be a child of root"),
        }

        assert_eq!(
            root.get_child("src/main.rs").unwrap().name(),
            "main.rs",
            "main.rs should be a child of src"
        )
    }

    #[test]
    fn path_test() {
        let root = create_test_sample();
        assert_eq!(
            root.get_child("src/main.rs").unwrap().path(),
            "/project/src/main.rs",
        );
        assert_eq!(root.path(), "/project", "path should be project");
        assert_eq!(root.get_child("src").unwrap().path(), "/project/src")
    }

    #[test]
    fn remove_child_test() {
        let root = create_test_sample();
        root.remove_child("src").unwrap();
        // Checks that removal is accurate
        assert_eq!(root.children().len(), 3, "Root should have 3 children");
        assert!(root.get_child("src").is_err(), "src should be removed");
        assert!(
            root.get_child("src/main.rs").is_err(),
            "src/main.rs should be removed"
        );
        // Cannot remove nonexistent child
        match root.remove_child("does_not_exist") {
            Err(_) => (),
            Ok(_) => panic!("should not be able to remove does_not_exist"),
        }
        // Remove non-immediate child
        let _ = root.get_child("tests/test2.rs").unwrap();
        root.remove_child("tests/test2.rs").unwrap();
        assert_eq!(
            root.get_child("tests").unwrap().children().len(),
            1,
            "tests should have 1 child"
        );
        assert!(
            root.get_child("tests/test2.rs").is_err(),
            "test2.rs should be removed"
        );
        assert_eq!(
            root.to_string(),
            "└── project\n    ├── target\n    │   ├── debug\n    │   └── release\n    ├── tests\n    │   └── test1.rs\n    └── docs"
        )
    }

    #[test]
    fn root_parent_test() {
        let root = create_test_sample();
        let child = root.get_child("target/debug").unwrap();
        assert_eq!(root.to_string(), child.root().to_string());

        child.root().remove_child("target/release").unwrap();
        assert_eq!(root.to_string(), child.root().to_string());
    }

    #[test]
    fn is_root_leaf_test() {
        let root = create_test_sample();
        assert!(!root.is_leaf(), "Root should not be a leaf");
        assert!(root.is_root(), "Root should be a root");
        let leaf = root.get_child("tests/test1.rs").unwrap();
        assert!(leaf.is_leaf(), "Leaf should be a leaf");
        assert!(!leaf.is_root(), "Leaf should not be a root");
    }

    #[test]
    fn print_test() {
        let root = create_test_sample();
        let tree_output = root.to_string();
        let expected = "└── project\n    ├── src\n    │   ├── main.rs\n    │   └── utils.rs\n    ├── target\n    │   ├── debug\n    │   └── release\n    ├── tests\n    │   ├── test1.rs\n    │   └── test2.rs\n    └── docs";
        assert_eq!(
            tree_output, expected,
            "Tree structure should match expected format"
        );
    }

    #[test]
    fn parent_test() {
        let root = create_test_sample();
        let src = root.get_child("src").unwrap();
        assert!(root.parent().is_err(), "Root should have no parent");
        assert_eq!(
            src.parent().unwrap().name(),
            "project",
            "src should have project as parent"
        );
        assert_eq!(
            src.get_child("main.rs").unwrap().parent().unwrap().name(),
            "src",
            "main.rs should have src as parent"
        );
    }
}
