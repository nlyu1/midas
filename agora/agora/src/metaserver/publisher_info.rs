use crate::utils::{OrError, PublisherAddressManager};
use std::net::{IpAddr, Ipv6Addr, SocketAddrV6};

pub const DEFAULT_SERVICE_PORT: u16 = 8081;
pub const DEFAULT_HEARTBEAT_PORT: u16 = 8082;

#[derive(Debug, PartialEq, Eq, Clone, serde::Serialize, serde::Deserialize)]
pub struct PublisherInfo {
    name: String,
    // address: Ipv6Addr,
    // port: u16,
    service_socket: SocketAddrV6,
    heartbeat_socket: SocketAddrV6,
}

impl PublisherInfo {
    pub fn new(name: &str, service_socket: SocketAddrV6, heartbeat_socket: SocketAddrV6) -> Self {
        Self {
            name: name.to_string(),
            service_socket,
            heartbeat_socket,
        }
    }

    /// Create a new publisher with auto-allocated address from the manager
    pub fn new_with_manager(name: &str, manager: &mut PublisherAddressManager) -> OrError<Self> {
        let address = manager.allocate_publisher_address()?;
        let service_socket = SocketAddrV6::new(address, DEFAULT_SERVICE_PORT, 0, 0);
        let heartbeat_socket = SocketAddrV6::new(address, DEFAULT_HEARTBEAT_PORT, 0, 0);
        Ok(Self {
            name: name.to_string(),
            service_socket,
            heartbeat_socket,
        })
    }

    pub fn for_demo() -> Self {
        let demo_addr = Ipv6Addr::new(0xfe80, 0, 0, 0, 0, 0, 0x1000, 1);
        Self {
            name: "demo".to_string(),
            service_socket: SocketAddrV6::new(demo_addr, DEFAULT_SERVICE_PORT, 0, 0),
            heartbeat_socket: SocketAddrV6::new(demo_addr, DEFAULT_HEARTBEAT_PORT, 0, 0),
        }
    }

    /// Get the socket address for tcp-websocket connections
    pub fn service_socket_addr(&self) -> (IpAddr, u16) {
        (
            IpAddr::V6(self.service_socket.ip().clone()),
            self.service_socket.port(),
        )
    }

    /// Get the socket address for heartbeat connections
    pub fn heartbeat_socket_addr(&self) -> (IpAddr, u16) {
        (
            IpAddr::V6(self.heartbeat_socket.ip().clone()),
            self.heartbeat_socket.port(),
        )
    }

    /// Get a human-readable connection string
    pub fn connection_string(&self) -> String {
        let (service_address, service_port) = self.service_socket_addr();
        let (heartbeat_address, heartbeat_port) = self.heartbeat_socket_addr();
        format!(
            "(bytestream [{}]:{}, heartbeat [{}]:{})",
            service_address, service_port, heartbeat_address, heartbeat_port
        )
    }

    /// Get the name of this publisher
    pub fn name(&self) -> &str {
        &self.name
    }
}
