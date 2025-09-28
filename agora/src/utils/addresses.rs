use crate::constants::{
    ULA_PREFIX_SEGMENT_0, ULA_PREFIX_SEGMENT_1, ULA_PREFIX_SEGMENT_2, ULA_PREFIX_SEGMENT_3,
};
use crate::utils::OrError;
use std::collections::HashSet;
use std::net::Ipv6Addr;

/// Simple address manager for localhost development
/// Allocates unique IPv6 ULA addresses in the full suffix range for each publisher
#[derive(Debug)]
pub struct PublisherAddressManager {
    allocated_addresses: HashSet<u64>,
    uid: u16,
}

impl PublisherAddressManager {
    /// Create a new address manager for localhost development
    /// uid: 16-bit identifier used as the fifth segment of the ULA address
    pub fn new(uid: u16) -> Self {
        Self {
            allocated_addresses: HashSet::new(),
            uid,
        }
    }

    fn suffix_to_address(&self, suffix: u64) -> Ipv6Addr {
        // Generate ULA addresses: fde5:402f:ab0a:1:<uid>:<48-bit suffix>
        // With ip_nonlocal_bind=1, any address in the subnet can be bound
        let mid_high = (suffix >> 32) as u16;
        let mid_low = (suffix >> 16) as u16;
        let low = suffix as u16;

        Ipv6Addr::new(
            ULA_PREFIX_SEGMENT_0,
            ULA_PREFIX_SEGMENT_1,
            ULA_PREFIX_SEGMENT_2,
            ULA_PREFIX_SEGMENT_3,
            self.uid,
            mid_high,
            mid_low,
            low,
        )
    }

    fn address_to_suffix(&self, addr: Ipv6Addr) -> Option<u64> {
        let segments = addr.segments();
        if segments[0] == ULA_PREFIX_SEGMENT_0
            && segments[1] == ULA_PREFIX_SEGMENT_1
            && segments[2] == ULA_PREFIX_SEGMENT_2
            && segments[3] == ULA_PREFIX_SEGMENT_3
            && segments[4] == self.uid
        {
            Some(
                ((segments[5] as u64) << 32)
                    | ((segments[6] as u64) << 16)
                    | (segments[7] as u64),
            )
        } else {
            None
        }
    }

    pub fn verify_address(&self, addr: Ipv6Addr) -> bool {
        // Verify it's in our ULA range fde5:402f:ab0a:1:<uid>::/80
        self.address_to_suffix(addr).is_some()
    }

    /// Enhanced verification that actually tests if we can bind to the address
    pub async fn verify_address_bindable(&self, addr: Ipv6Addr, port: u16) -> bool {
        use tokio::net::TcpListener;
        // First verify it's in our managed range
        if !self.verify_address(addr) {
            return false;
        }
        match TcpListener::bind((addr, port)).await {
            Ok(_) => true,
            Err(_) => false,
        }
    }

    fn new_address(&mut self) -> OrError<Ipv6Addr> {
        use rand::Rng;
        let mut rng = rand::rng();

        // Try up to 1000 times to find an unused address
        for _attempt in 1..=1000 {
            // Generate 48-bit suffix (mask to ensure it fits in 48 bits)
            let suffix: u64 = rng.random::<u64>() & 0x0000_FFFF_FFFF_FFFF;

            // Skip if we've already allocated this suffix
            if self.allocated_addresses.contains(&suffix) {
                continue;
            }

            // Generate the address
            let proposed = self.suffix_to_address(suffix);

            // Track the allocation
            self.allocated_addresses.insert(suffix);
            return Ok(proposed);
        }

        Err("Failed to allocate address after 1000 attempts".to_string())
    }

    /// Public interface - allocate a new address for a publisher
    pub fn allocate_publisher_address(&mut self) -> OrError<Ipv6Addr> {
        self.new_address()
    }

    /// Free an address back to the pool, allowing it to be reused
    pub fn free_address(&mut self, addr: Ipv6Addr) -> OrError<()> {
        if let Some(suffix) = self.address_to_suffix(addr) {
            if self.allocated_addresses.remove(&suffix) {
                Ok(())
            } else {
                Err(format!("Address {} was not allocated by this manager", addr))
            }
        } else {
            Err(format!("Address {} is not in the managed ULA range for UID {}", addr, self.uid))
        }
    }

    /// Get info about current allocations
    pub fn allocation_info(&self) -> String {
        format!(
            "AddressManager (UID {}): {} addresses allocated from 48-bit suffix space",
            self.uid,
            self.allocated_addresses.len()
        )
    }
}
