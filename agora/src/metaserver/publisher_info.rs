use crate::constants::{PUBLISHER_OMNISTRING_PORT, PUBLISHER_PING_PORT, PUBLISHER_SERVICE_PORT};
use crate::utils::{OrError, PublisherAddressManager};
use std::net::{IpAddr, Ipv6Addr, SocketAddrV6};

#[derive(Debug, PartialEq, Eq, Clone, serde::Serialize, serde::Deserialize)]
pub struct PublisherInfo {
    name: String,
    // address: Ipv6Addr,
    // port: u16,
    pub service_socket: SocketAddrV6,
    pub string_socket: SocketAddrV6,
    pub ping_socket: SocketAddrV6,
}

impl PublisherInfo {
    pub fn new(
        name: &str,
        service_socket: SocketAddrV6,
        string_socket: SocketAddrV6,
        ping_socket: SocketAddrV6,
    ) -> Self {
        Self {
            name: name.to_string(),
            service_socket,
            string_socket,
            ping_socket,
        }
    }

    /// Create a new publisher with auto-allocated address from the manager
    pub fn new_with_manager(name: &str, manager: &mut PublisherAddressManager) -> OrError<Self> {
        let address = manager.allocate_publisher_address()?;
        let service_socket = SocketAddrV6::new(address, PUBLISHER_SERVICE_PORT, 0, 0);
        let ping_socket = SocketAddrV6::new(address, PUBLISHER_PING_PORT, 0, 0);
        let string_socket = SocketAddrV6::new(address, PUBLISHER_OMNISTRING_PORT, 0, 0);
        Ok(Self {
            name: name.to_string(),
            service_socket,
            string_socket,
            ping_socket,
        })
    }

    pub fn for_demo() -> Self {
        let demo_addr = Ipv6Addr::new(0xfe80, 0, 0, 0, 0, 0, 0x1000, 1);
        Self {
            name: "demo".to_string(),
            service_socket: SocketAddrV6::new(demo_addr, PUBLISHER_SERVICE_PORT, 0, 0),
            string_socket: SocketAddrV6::new(demo_addr, PUBLISHER_OMNISTRING_PORT, 0, 0),
            ping_socket: SocketAddrV6::new(demo_addr, PUBLISHER_PING_PORT, 0, 0),
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
    pub fn ping_socket_addr(&self) -> (IpAddr, u16) {
        (
            IpAddr::V6(self.ping_socket.ip().clone()),
            self.ping_socket.port(),
        )
    }

    /// Get the socket address for string connections
    pub fn string_socket_addr(&self) -> (IpAddr, u16) {
        (
            IpAddr::V6(self.string_socket.ip().clone()),
            self.string_socket.port(),
        )
    }

    /// Get a human-readable connection string
    pub fn connection_string(&self) -> String {
        let (service_address, service_port) = self.service_socket_addr();
        let (heartbeat_address, heartbeat_port) = self.ping_socket_addr();
        let (string_address, string_port) = self.string_socket_addr();
        format!(
            "(bytestream [{}]:{}, heartbeat [{}]:{}, string [{}]:{})",
            service_address,
            service_port,
            heartbeat_address,
            heartbeat_port,
            string_address,
            string_port
        )
    }

    /// Get the name of this publisher
    pub fn name(&self) -> &str {
        &self.name
    }
}
