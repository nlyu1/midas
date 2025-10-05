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
