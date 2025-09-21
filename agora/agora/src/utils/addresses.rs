use crate::utils::OrError;
use std::collections::HashSet;
use std::net::Ipv6Addr;

/// Simple address manager for localhost development
/// Allocates unique IPv6 link-local addresses (fe80::/10) for each publisher
#[derive(Debug)]
pub struct PublisherAddressManager {
    next_index: u32,
    max_addresses: u32,
    allocated_addresses: HashSet<Ipv6Addr>,
}

impl PublisherAddressManager {
    /// Create a new address manager for localhost development
    pub fn new() -> Self {
        Self {
            next_index: 1,
            max_addresses: u16::MAX as u32, // 65535 publishers should be enough for localhost!
            allocated_addresses: HashSet::new(),
        }
    }

    fn propose_new_address(&mut self) -> OrError<Ipv6Addr> {
        if self.next_index > self.max_addresses {
            return Err(format!(
                "Exhausted address space! Used {}/{} addresses",
                self.allocated_addresses.len(),
                self.max_addresses
            ));
        }

        // Always generate link-local addresses: fe80::1000:index
        let proposed_addr = Ipv6Addr::new(0xfe80, 0, 0, 0, 0, 0, 0x1000, self.next_index as u16);

        self.next_index += 1;
        Ok(proposed_addr)
    }

    pub fn verify_address(addr: Ipv6Addr) -> bool {
        // Verify it's in the link-local range (fe80::/10)
        matches!(addr.segments()[0], 0xfe80..=0xfebf)
    }

    /// Enhanced verification that actually tests if we can bind to the address
    pub async fn verify_address_bindable(addr: Ipv6Addr, port: u16) -> bool {
        use tokio::net::TcpListener;
        match TcpListener::bind((addr, port)).await {
            Ok(_) => true,
            Err(_) => false,
        }
    }

    fn new_address(&mut self) -> OrError<Ipv6Addr> {
        // Try up to 10 times to find a valid, unused address
        for _attempt in 1..=10 {
            let proposed = self.propose_new_address()?;

            // Skip if we've already allocated this address
            if self.allocated_addresses.contains(&proposed) {
                continue;
            }

            // Basic verification
            if !Self::verify_address(proposed) {
                return Err(format!("Proposed address {} failed verification", proposed));
            }

            // Track the allocation
            self.allocated_addresses.insert(proposed);
            return Ok(proposed);
        }

        Err("Failed to allocate address after 10 attempts".to_string())
    }

    /// Public interface - allocate a new address for a publisher
    pub fn allocate_publisher_address(&mut self) -> OrError<Ipv6Addr> {
        self.new_address()
    }

    /// Get info about current allocations
    pub fn allocation_info(&self) -> String {
        format!(
            "AddressManager: {}/{} addresses allocated, next_index: {}",
            self.allocated_addresses.len(),
            self.max_addresses,
            self.next_index
        )
    }

    /// Release an address back to the pool (for cleanup)
    pub fn release_address(&mut self, addr: Ipv6Addr) -> bool {
        self.allocated_addresses.remove(&addr)
    }
}
