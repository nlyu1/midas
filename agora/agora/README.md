# Agora: IPv6-based Publisher-Subscriber System

Agora is a high-performance publisher-subscriber system that uses IPv6 ULA (Unique Local Address) networking to enable massive-scale, cross-device communication.

## Architecture

1. `metaserver`: uses `tarpc` to implement service discovery.
    - Each publisher-subscriber is registered at a filesystem-like path.
    - The metaserver allocates unique IPv6 addresses for each publisher.
    - Registration logic:
        - Allocate ULA IPv6 address for service
        - Initialize a heartbeat client. Deregister service if heartbeat fails.
2. `rawstream`: uses `tokio-tungstenite` to establish single-publisher-multiple-subscriber data flow.
    - No interaction with metaserver at all. Purely point-to-point byte / stream streaming logic.
3. `heartbeat`: RPC server which, upon pinged, returns the last written update value of the server.
4. `core`: integrates `metaserver` and `rawstream` to implement publishers and subscribers
    - `publisher` initialization: `path`, `initial_value`
        a. Initialize a `metaserver` client and register to obtain socket.
        b. Initialize a `rawstream` server at obtained socket of type bytes, and another of type string
        c. Initializes a heartbeat server which returns last_value
        d. Serializes `stream<T>` to `stream<bytes>` and transmits over `rawstream`
    - `subscriber` initialization: `subscribe<T>(path) -> stream<T>`
        a. Initialize `metaserver` client to obtain the service socket.
        b. Initialize `rawstream` client at obtained socket
        c. Serializes `stream<bytes>` to `stream<T>`
        d. Upon subscription, returns `T, stream<T>`.
    - Same for `omnisubscriber`

## IPv6 ULA Network Architecture

Agora uses IPv6 Unique Local Addresses (ULA) to enable massive-scale addressing and cross-device connectivity.

### Network Scheme

**ULA Prefix**: `fde5:402f:ab0a:1::/64` (RFC 4193 compliant random prefix)

**Address Allocation Pattern**:
- Metaserver: `fde5:402f:ab0a:1::1:8080`
- Publisher 1: `fde5:402f:ab0a:1::1:8081` (service), `fde5:402f:ab0a:1::1:8082` (omnistring), `fde5:402f:ab0a:1::1:8083` (ping)
- Publisher 2: `fde5:402f:ab0a:1::2:8081` (service), `fde5:402f:ab0a:1::2:8082` (omnistring), `fde5:402f:ab0a:1::2:8083` (ping)
- Publisher N: `fde5:402f:ab0a:1::N:808X`

**Scale**: Up to 65,535 unique publishers (2^16 addresses)

**Cross-Device Access**:
- ✅ Any device on the same Wi-Fi network can connect
- ✅ Addresses are routed through the Wi-Fi interface
- ✅ No port conflicts - each publisher gets unique IPv6 addresses

### Key Technical Components

1. **`net.ipv6.ip_nonlocal_bind=1`**: Enables binding to any address in the ULA subnet without individual assignment
2. **ULA Subnet Assignment**: The entire `/64` subnet is assigned to the network interface
3. **PublisherAddressManager**: Systematically allocates sequential IPv6 addresses

### Network Topology

```
[Other Device] ──► [Wi-Fi Router] ──► [Your Machine wlp7s0: fde5:402f:ab0a:1::/64]
                                          │
                                          ├─► fde5:402f:ab0a:1::1:8080 (metaserver)
                                          ├─► fde5:402f:ab0a:1::1:8081 (publisher1)
                                          ├─► fde5:402f:ab0a:1::2:8081 (publisher2)
                                          └─► fde5:402f:ab0a:1::N:8081 (publisherN)
```

## Manual Setup on Ubuntu

To enable IPv6 ULA networking manually:

### 1. Generate Random ULA Prefix

```bash
# Generate RFC 4193 compliant random ULA prefix
python3 -c "import random; print(f'fd{random.randint(0, 255):02x}:{random.randint(0, 65535):04x}:{random.randint(0, 65535):04x}')"
# Example output: fde5:402f:ab0a
```

### 2. Enable IPv6 Non-Local Bind

**What is `ip_nonlocal_bind`?**

The `net.ipv6.ip_nonlocal_bind` kernel parameter allows applications to bind to IPv6 addresses that are not configured on any interface. This is crucial for our use case because:

- **Without it**: You can only bind to addresses explicitly assigned to interfaces (like `::1`, or specific addresses added with `ip addr add`)
- **With it**: You can bind to ANY address in a subnet, as long as routing is configured properly
- **Our usage**: Enables binding to `fde5:402f:ab0a:1::1`, `fde5:402f:ab0a:1::2`, etc. without adding each address individually

```bash
# Enable temporarily (until reboot)
sudo sysctl -w net.ipv6.ip_nonlocal_bind=1

# Enable permanently (survives reboot)
echo 'net.ipv6.ip_nonlocal_bind = 1' | sudo tee -a /etc/sysctl.conf
sudo sysctl -p
```

### 3. Create ULA Subnet

First, find your Wi-Fi interface name:

```bash
# Method 1: List all network interfaces
ip link show

# Method 2: Find interfaces with IP addresses
ip addr show

# Method 3: Find wireless interfaces specifically
iwconfig 2>/dev/null | grep -E '^[a-z]' | cut -d' ' -f1

# Method 4: Look for common Wi-Fi interface patterns
ip link show | grep -E "(wlan|wlp|wifi)"

# Example output:
# 3: wlp7s0: <BROADCAST,MULTICAST,UP,LOWER_UP> mtu 1500 qdisc noqueue state UP
# Your interface name is: wlp7s0
```

**Common Wi-Fi interface naming patterns:**
- **Modern Ubuntu**: `wlp7s0`, `wlp3s0` (wireless PCI device)
- **Older systems**: `wlan0`, `wlan1`
- **USB Wi-Fi**: `wlx123abc456def`
- **Some systems**: `wifi0`

Then assign the ULA subnet to your Wi-Fi interface:

```bash
# Replace 'wlp7s0' with your actual Wi-Fi interface name
# Replace 'fde5:402f:ab0a' with your generated prefix

# Assign the entire /64 subnet to your network interface
sudo ip -6 addr add fde5:402f:ab0a:1::/64 dev wlp7s0

# Verify assignment
ip -6 addr show wlp7s0 | grep fde5
# Should show: inet6 fde5:402f:ab0a:1::/64 scope global

# Test binding to random addresses in the subnet
python3 -c "
import socket
for i in [1, 2, 100, 0xdead]:
    addr = f'fde5:402f:ab0a:1::{i:x}'
    try:
        sock = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
        sock.bind((addr, 0))
        sock.close()
        print(f'✅ {addr} - Can bind')
    except Exception as e:
        print(f'❌ {addr} - Cannot bind: {e}')
"
```

### Automated Setup

For convenience, use the provided setup script:

```bash
# Clean up any old configuration
sudo ./setup/cleanup_old_ipv6.sh

# Apply persistent configuration
sudo ./setup/persistent_ipv6_setup.sh
```

This configuration is persistent across reboots and enables massive-scale IPv6 addressing for your Agora deployment. 

## Development Status

- [x] IPv6 ULA networking infrastructure
- [x] Metaserver with address allocation
- [x] Raw stream publisher-subscriber
- [x] Heartbeat monitoring
- [x] Core publisher/subscriber wrappers
- [ ] Metaserver lease system
- [ ] Python bindings
- [ ] TypeScript browser frontend

## Usage

```bash
# Start metaserver
cargo run --bin metaserver

# Start a publisher (in another terminal)
cargo run --bin publisher_example

# Start a subscriber (in another terminal)
cargo run --bin subscriber_example
```

**Note**: The current implementation uses a hardcoded ULA subnet `fde5:402f:ab0a:1::/64`. For production use, consider making this configurable.


## Other network notes:

1. On linux: use `ip -6 a` to figure out address
    - Check that it's enabled: `ip -6 addr show | grep fde5`
    - Show interfaces: `ip link show`
2. 

# Maturin

1. 