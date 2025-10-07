use agora::metaserver::ServerState;
use agora::utils::TreeTrait;
use agora::ConnectionHandle;
use std::net::{IpAddr, Ipv4Addr};

/// Creates a test ConnectionHandle with a given port.
/// Uses localhost (127.0.0.1) as the default IP address.
pub fn test_connection(port: u16) -> ConnectionHandle {
    ConnectionHandle::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port)
}

/// Creates a test ConnectionHandle with default test port (8081).
pub fn default_test_connection() -> ConnectionHandle {
    test_connection(8081)
}

/// Creates a fresh ServerState instance with a pre-configured directory tree.
/// The tree structure mimics a typical API layout:
/// ```
/// agora/
/// ├── api/
/// │   ├── v1/
/// │   │   ├── users/
/// │   │   ├── posts/
/// │   │   └── auth/
/// │   └── v2/
/// ├── static/
/// └── admin/
/// ```
pub fn create_test_server_state() -> ServerState {
    let state = ServerState::new();
    state.path_tree().add_children(&["api", "static", "admin"]);
    let api = state.path_tree().get_child("api").unwrap();
    api.add_children(&["v1", "v2"]);
    let v1 = api.get_child("v1").unwrap();
    v1.add_children(&["users", "posts", "auth"]);
    state
}
