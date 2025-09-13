use agora::utils::{TreeNode, TreeTrait};

fn main() {
    println!("Testing TreeNode functionality...\n");

    // Create root and build tree structure
    let root = TreeNode::new("project");
    root.add_children(&["src", "docs", "tests"]);
    let src = root.get_child("src").unwrap();
    src.add_children(&["main.rs", "utils.rs"]);
    // Using the Display trait - can use println! directly
    println!("{}", root);

    // Or using the to_string method explicitly
    // println!("{}", root.to_string());
}
