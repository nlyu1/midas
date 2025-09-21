use crate::utils::{OrError, PublisherAddressManager};
use std::net::{IpAddr, Ipv6Addr};

#[derive(Debug, PartialEq, Eq, Clone, serde::Serialize, serde::Deserialize)]
pub struct PublisherInfo {
    name: String,
    address: Ipv6Addr,
    port: u16,
}

impl PublisherInfo {
    pub fn new(name: &str, address: Ipv6Addr, port: u16) -> Self {
        Self {
            name: name.to_string(),
            address,
            port,
        }
    }

    /// Create a new publisher with auto-allocated address from the manager
    pub fn new_with_manager(name: &str, manager: &mut PublisherAddressManager) -> OrError<Self> {
        let address = manager.allocate_publisher_address()?;
        Ok(Self {
            name: name.to_string(),
            address,
            port: 8081, // Default port for publishers
        })
    }

    pub fn for_demo() -> Self {
        Self {
            name: "demo".to_string(),
            address: Ipv6Addr::new(0xfe80, 0, 0, 0, 0, 0, 0x1000, 1), // fe80::1000:1
            port: 8081,
        }
    }

    /// Get the socket address for tarpc connections
    pub fn socket_addr(&self) -> (IpAddr, u16) {
        (IpAddr::V6(self.address), self.port)
    }

    /// Get a human-readable connection string
    pub fn connection_string(&self) -> String {
        format!("[{}]:{}", self.address, self.port)
    }

    /// Get the name of this publisher
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Get the IPv6 address
    pub fn address(&self) -> Ipv6Addr {
        self.address
    }

    /// Get the port
    ///
    pub fn port(&self) -> u16 {
        self.port
    }
}
