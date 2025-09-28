use agora::utils::PublisherAddressManager;
use std::net::Ipv6Addr;

#[test]
fn test_basic_address_allocation() {
    let mut manager = PublisherAddressManager::new();

    // Should start with no allocations
    assert!(
        manager
            .allocation_info()
            .contains("0 addresses allocated")
    );

    // Allocate first address
    let addr1 = manager.allocate_publisher_address().unwrap();
    assert!(PublisherAddressManager::verify_address(addr1));

    // Allocate second address
    let addr2 = manager.allocate_publisher_address().unwrap();
    assert!(PublisherAddressManager::verify_address(addr2));

    // Addresses should be different
    assert_ne!(addr1, addr2);

    println!("✅ Allocated addresses: {} and {}", addr1, addr2);
    println!("✅ Manager status: {}", manager.allocation_info());
}

#[test]
fn test_address_verification() {
    // ULA addresses in our range should be valid
    assert!(PublisherAddressManager::verify_address(Ipv6Addr::new(
        0xfde5, 0x402f, 0xab0a, 0x0001, 0, 0, 0, 1
    )));

    // Other ranges should fail
    assert!(!PublisherAddressManager::verify_address(
        Ipv6Addr::new(0x2001, 0xdb8, 0, 0, 0, 0, 0, 1) // Global
    ));

    assert!(!PublisherAddressManager::verify_address(
        Ipv6Addr::new(0xfe80, 0, 0, 0, 0, 0, 0, 1) // Link local
    ));

    println!("✅ Address verification works correctly");
}

#[test]
fn test_address_release() {
    let mut manager = PublisherAddressManager::new();

    let addr = manager.allocate_publisher_address().unwrap();
    assert!(
        manager
            .allocation_info()
            .contains("1 addresses allocated")
    );

    // Release the address
    assert!(manager.free_address(addr).is_ok());
    assert!(
        manager
            .allocation_info()
            .contains("0 addresses allocated")
    );

    // Can't release the same address twice
    assert!(manager.free_address(addr).is_err());

    println!("✅ Address release works correctly");
}

#[test]
fn test_many_allocations() {
    let mut manager = PublisherAddressManager::new();
    let mut addresses = Vec::new();

    // Allocate 100 addresses
    for i in 1..=100 {
        let addr = manager
            .allocate_publisher_address()
            .expect(&format!("Failed to allocate address {}", i));
        addresses.push(addr);
    }

    // All should be different
    let unique_count = addresses
        .iter()
        .collect::<std::collections::HashSet<_>>()
        .len();
    assert_eq!(unique_count, 100);

    // Should all be valid ULA addresses in our range
    for addr in addresses.iter() {
        assert!(PublisherAddressManager::verify_address(*addr));
    }

    println!("✅ Successfully allocated 100 unique addresses");
    println!("✅ Final status: {}", manager.allocation_info());
}

#[tokio::test]
async fn test_address_bindable_verification() {
    let test_addr = Ipv6Addr::new(0xfde5, 0x402f, 0xab0a, 0x0001, 0, 0, 0, 999);
    let test_port = 9999;

    // This should work on most systems (though may fail in some restricted environments)
    let bindable = PublisherAddressManager::verify_address_bindable(test_addr, test_port).await;

    println!(
        "✅ Address {}:{} bindable: {}",
        test_addr, test_port, bindable
    );

    // Test with a port that's likely to be in use (port 80)
    let busy_port_bindable = PublisherAddressManager::verify_address_bindable(test_addr, 80).await;

    println!(
        "✅ Address {}:80 bindable: {}",
        test_addr, busy_port_bindable
    );
}

#[test]
fn test_demo_usage() {
    println!("\n=== PublisherAddressManager Demo ===");

    let mut manager = PublisherAddressManager::new();
    println!("📍 Created local address manager");

    // Simulate creating 3 publishers
    let publishers = vec!["btc_ticker", "eth_ticker", "doge_ticker"];
    let mut allocated_addresses = Vec::new();

    for publisher_name in publishers {
        let addr = manager.allocate_publisher_address().unwrap();
        allocated_addresses.push((publisher_name, addr));
        println!(
            "🚀 Publisher '{}' assigned address: [{}]:8081",
            publisher_name, addr
        );
        // Verify the address is in the correct range
        assert!(PublisherAddressManager::verify_address(addr));
    }

    println!("\n📊 {}", manager.allocation_info());

    println!("\n🔗 Connection strings:");
    for (name, addr) in &allocated_addresses {
        println!("   {} -> tarpc://[{}]:8081", name, addr);
    }

    println!("\n✨ Each publisher can now run on the same port (8081) with unique IPs!");
    println!("   No port conflicts, unlimited scaling potential!");
}

#[test]
fn test_address_pattern() {
    let mut manager = PublisherAddressManager::new();

    // Test that addresses follow expected pattern
    for _i in 1..=5 {
        let addr = manager.allocate_publisher_address().unwrap();

        // Verify it's a valid ULA address in our range
        assert!(PublisherAddressManager::verify_address(addr));
        assert!(addr.to_string().starts_with("fde5:402f:ab0a:1:"));
    }

    println!("✅ Address pattern is correct: fde5:402f:ab0a:1:*");
}
