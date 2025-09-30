# Agora: IPv6-based Publisher-Subscriber System

Agora is a distributed publisher-subscriber messaging system built on IPv6 with path-based service discovery. It enables real-time streaming communication between publishers and subscribers using statically typed messages, with support for both Rust and Python applications.

## Core Concepts

**Publisher\<T>**: Creates a typed publisher for messages of _Agorable_ type `T`. Each publisher exposes three endpoints:
- Service endpoint for typed binary messages
- String endpoint for type-agnostic monitoring
- Ping endpoint for health checks and one-time queries

**Subscriber\<T>**: Connects to publishers of the same type `T` for streaming typed messages.

**OmniSubscriber**: Type-agnostic subscriber that receives string representations from any publisher.

**Agorable**: Trait for types that can be published/subscribed. Built-in support for `String`, `i64`, `bool`, `f64`, `f32`.

**Relay\<T>**: Dynamic publisher that relays typed messages from switchable source paths to a fixed destination. Enables contiguous streams from discontinuous sources and cross-metaserver bridging via `swapon()`.

**MetaServer**: Central registry that manages publisher discovery and IPv6 address allocation.

## Quick Start

1. **Build the project**:
   ```bash
   cargo build
   ```

2. **Setup IPv6 networking** (Ubuntu):
   ```bash
   sudo ./setup/persistent_ipv6_setup.sh
   ```

3. **Start the MetaServer**:
   ```bash
   cargo run --bin metaserver
   ```

4. **Explore with MetaClient**:
   ```bash
   cargo run --bin metaclient
   ```

## Examples

The following example demonstrates the basic workflow: 
### 1. Start MetaServer
```bash
cargo run --bin metaserver
# Listening on port 8080
```

### 2. Create a Publisher
```bash
cargo run --bin publisher_example
# Enter publisher name: my_publisher
# Enter path: chat/general
# Enter initial message: Hello, world!
```

### 3. Subscribe to Messages
In another terminal:
```bash
cargo run --bin subscriber_example
# Enter path to subscribe to: chat/general
```

### 4. Monitor with MetaClient
```bash
cargo run --bin metaclient
# Use 'print' to see active publishers
# Use 'monitor <path>' to watch messages
# Use 'info <path>' for publisher details
```

**Available MetaClient commands:**
- `print` - Show current path tree
- `info <path>` - Get publisher information
- `monitor <path>` - Watch live messages (Ctrl+D to exit)
- `remove <path>` - Remove a publisher
- `help` - Show command help
- `quit` - Exit

## Python Integration

Agora provides Python bindings through PyO3 and Maturin, supporting typed publishers and subscribers.

**Setup:**
1. Install maturin in your Python environment:
   ```bash
   pip install maturin
   ```

2. Build and install the Python package:
   ```bash
   # Development build (faster, includes debug info)
   maturin develop

   # Release build (optimized)
   maturin develop --release
   ```

**Available Python types:**
- **Publishers**: `PyStringPublisher`, `PyI64Publisher`, `PyBoolPublisher`, `PyF64Publisher`, `PyF32Publisher`
- **Subscribers**: `PyStringSubscriber`, `PyI64Subscriber`, `PyBoolSubscriber`, `PyF64Subscriber`, `PyF32Subscriber`, `PyOmniSubscriber`
- **Relays**: `PyStringRelay`, `PyI64Relay`, `PyBoolRelay`, `PyF64Relay`, `PyF32Relay`

Each relay is initialized with `new(name, dest_path, initial_value, metaserver_addr, metaserver_port)` and supports dynamic source switching via `swapon(src_path, metaserver_addr, metaserver_port)`.

**TODO: add python examples later**

Start an agora server by `cargo run --bin metaserver -- -p 8080`. 

## Network Setup (Ubuntu)

Agora uses IPv6 Unique Local Addresses (ULA) for scalable addressing and service isolation. The MetaServer automatically allocates IPv6 addresses from the `fde5:402f:ab0a:1:<uid>::/80` subnet for each publisher, where `<uid>` is the MetaServer's port number.

### Automatic Setup
Run the provided script for persistent configuration:
```bash
sudo ./setup/persistent_ipv6_setup.sh
```

This script:
- Enables `net.ipv6.ip_nonlocal_bind=1` for flexible address binding
- Configures the ULA subnet via netplan
- Sets up local routing for the address range

### Manual Setup
If you prefer manual configuration:

1. **Enable non-local IPv6 binding:**
   ```bash
   sudo sysctl -w net.ipv6.ip_nonlocal_bind=1
   echo 'net.ipv6.ip_nonlocal_bind = 1' | sudo tee -a /etc/sysctl.conf
   ```

2. **Find your network interface:**
   ```bash
   ip link show
   # Look for your WiFi interface (typically wlp7s0, wlp3s0, etc.)
   ```

3. **Configure the ULA subnet:**
   ```bash
   # Replace wlp7s0 with your interface name
   sudo ip -6 addr add fde5:402f:ab0a:1::/64 dev wlp7s0
   ```

### Address Allocation
- **MetaServer**: `[::1]:8080` (localhost)
- **Publishers**: `fde5:402f:ab0a:1:<uid>:<suffix>:PORT` where `<uid>` is the MetaServer port and `<suffix>` is a random 48-bit identifier
- **Port scheme**: 8081 (service), 8082 (omnistring), 8083 (ping)

## Architecture

### Project Structure
```
agora/
├── src/
│   ├── bin/                    # Example binaries
│   │   ├── metaserver.rs      # Central service registry
│   │   ├── metaclient.rs      # Interactive exploration tool
│   │   ├── publisher_example.rs
│   │   ├── subscriber_example.rs
│   │   └── relay_example.rs   # Relay usage example
│   ├── core/                   # Core pub-sub types
│   │   ├── publisher.rs       # Publisher<T> implementation
│   │   ├── subscriber.rs      # Subscriber<T> and OmniSubscriber
│   │   └── common.rs          # Agorable trait
│   ├── relay.rs               # Relay<T> implementation
│   ├── metaserver/            # Service registry implementation
│   ├── ping/                  # Health check RPC system
│   ├── rawstream/             # Low-level streaming protocol
│   ├── pywrappers/            # Python bindings
│   │   ├── publishers.rs      # Typed publisher wrappers
│   │   ├── subscribers.rs     # Typed subscriber wrappers
│   │   ├── relays.rs          # Typed relay wrappers
│   │   └── async_helpers.rs   # Runtime utilities
│   ├── utils/                 # Address management, path trees
│   └── constants.rs           # Network configuration
├── python/
│   ├── agora/                 # Python package
│   └── tests/                 # Python integration tests
└── setup/                     # IPv6 network configuration scripts
```

### Component Overview

**MetaServer (`metaserver/`)**:
- Central service registry using gRPC/tarpc
- Allocates IPv6 addresses for publishers using port-based UID addressing
- Manages service discovery by path
- Monitors service health and removes stale entries

**Core Types (`core/` and `relay.rs`)**:
- `Publisher<T>`: Publishes typed messages at a path
  - Maintains three endpoints: service (bytes), omnistring (strings), ping (health)
  - Registers with MetaServer and handles automatic reconnection
- `Subscriber<T>`: Subscribes to typed messages from a specific path
- `OmniSubscriber`: Type-agnostic subscriber receiving string representations
- `Agorable`: Trait for serializable message types
- `Relay<T>`: Dynamic relay combining Publisher (fixed destination) with switchable Subscriber (dynamic source)
  - **Architecture**: Spawns two async tasks:
    - `stream_out`: Consumes from internal channel, publishes to fixed destination
    - `stream_in`: Subscribes to current source, feeds into channel
  - **API**:
    - `new(name, dest_path, initial_value, metaserver_addr, metaserver_port)`: Creates relay with destination publisher
    - `swapon(src_path, metaserver_addr, metaserver_port)`: Atomically switches to new source by aborting old `stream_in` task and spawning new one
  - **Use cases**:
    - <u>Contiguous streaming from discontinuous sources</u>: `src0` streams until $t_1$, `src1` from $t_0 < t_1$ onwards. Initialize relay at `src0`, call `swapon(src1)` during overlap $[t_0, t_1]$ for seamless transition.
    - <u>Endpoint rerouting</u>: Redirect persistent process publishing to `path0` → `path1` without restart by creating relay at `path1` initialized to `path0`.
    - <u>Cross-metaserver bridging</u>: Relay from `(metaserver_0, port_0)` → `(metaserver_1, port_1)` remains functional even if `metaserver_0` dies after initial connection. 

**Communication Layer**:
- **RawStream**: WebSocket-based streaming for real-time message delivery
- **Ping System**: tarpc-based RPC for health checks and one-time queries
- **IPv6 Addressing**: ULA subnet for scalable service isolation

**Python Integration (`pywrappers/`)**:
- PyO3 bindings for all core types
- Typed publishers/subscribers/relays for String, i64, bool, f64, f32
- Synchronous and streaming APIs 

# Future todo's

1. Component integration tests
2. Browser explorer & plotting (typescript integration)

Current Architecture Analysis

Your system has these key components:
- MetaServer: Central registry for publishers (src/bin/metaserver.rs)
- Publishers: Data producers with ping + rawstream servers (src/core/publisher.rs)
- Subscribers: Data consumers (src/core/subscriber.rs)
- RawStream: WebSocket streaming (src/rawstream.rs)
- Ping: Heartbeat/liveness system (src/ping/)

Recommended Integration Testing Strategy

1. Test Infrastructure Setup

Add dev-dependencies to Cargo.toml:
[dev-dependencies]
tokio-test = "0.4"
serial_test = "3.0"
tempfile = "3.8"

2. Testing Framework Architecture

Create tests/integration/ directory structure:
tests/
├── integration/
│   ├── mod.rs                 # Test utilities & helpers
│   ├── test_metaserver.rs     # MetaServer integration tests
│   ├── test_pubsub.rs         # Publisher-Subscriber integration
│   ├── test_rawstream.rs      # RawStream integration tests
│   └── test_full_system.rs    # End-to-end system tests
└── common/
   ├── mod.rs
   ├── test_metaserver.rs     # MetaServer test helper
   ├── test_publisher.rs      # Publisher test helper
   └── test_subscriber.rs     # Subscriber test helper

3. Key Testing Patterns

Test Helper Infrastructure (tests/common/mod.rs):
- MetaServerFixture: Spawns metaserver on random port, manages lifecycle
- PublisherFixture: Creates publishers with cleanup
- SubscriberFixture: Creates subscribers with timeout handling
- NetworkIsolation: Uses unique IPv6 addresses per test (your PublisherAddressManager is
perfect for this)

Port Management Strategy:
- Use random available ports (avoid conflicts)
- Leverage your existing PublisherAddressManager for IPv6 address allocation
- Each test gets isolated network namespace

4. Test Categories

A. Component Integration Tests:
// tests/integration/test_metaserver.rs
#[tokio::test]
async fn test_publisher_registration_and_discovery() {
   let metaserver = MetaServerFixture::new().await;
   let publisher = PublisherFixture::new("test_pub", "test/path",
metaserver.addr()).await;
   let subscriber = SubscriberFixture::new("test/path", metaserver.addr()).await;

   // Test registration, discovery, and communication
}

B. Service Interaction Tests:
- Publisher registration → MetaServer storage → Subscriber discovery
- Ping heartbeat flow between components
- RawStream WebSocket connection and data flow
- Publisher failure → MetaServer cleanup → Subscriber reconnection

C. Failure Mode Tests:
- Network partitions (kill connections temporarily)
- Component crashes (kill processes, restart)
- Resource exhaustion scenarios
- Concurrent access patterns

5. Implementation Recommendations

Test Utilities (tests/common/test_metaserver.rs):
pub struct MetaServerFixture {
   handle: JoinHandle<()>,
   address: Ipv6Addr,
   port: u16,
}

impl MetaServerFixture {
   pub async fn new() -> Self {
         let port = get_random_port();
         let address = Ipv6Addr::LOCALHOST;

         let handle = tokio::spawn(async move {
            AgoraMetaServer::run_server(address, port).await.unwrap()
         });

         // Wait for server to be ready
         wait_for_server_ready(address, port).await;

         Self { handle, address, port }
   }
}

impl Drop for MetaServerFixture {
   fn drop(&mut self) {
         self.handle.abort();
   }
}

Isolated Testing Pattern:
#[tokio::test]
#[serial_test::serial] // Prevent concurrent network tests
async fn test_publisher_subscriber_flow() {
   let metaserver = MetaServerFixture::new().await;

   // Publisher publishes data
   let mut publisher = Publisher::new(
         "test_publisher".to_string(),
         "stocks/AAPL".to_string(),
         StockPrice { price: 150.0 },
         metaserver.address(),
         metaserver.port()
   ).await.unwrap();

   // Subscriber receives data
   let mut subscriber: Subscriber<StockPrice> = Subscriber::new(
         "stocks/AAPL".to_string(),
         metaserver.address(),
         metaserver.port()
   ).await.unwrap();

   // Test data flow
   publisher.publish(StockPrice { price: 155.0 }).await.unwrap();
   let received = subscriber.stream().next().await.unwrap();
   assert_eq!(received.price, 155.0);
}

6. Advantages of This Approach

Minimal Additional Code:
- Reuses your existing components as-is
- Test fixtures are lightweight wrappers
- Uses your PublisherAddressManager for network isolation

Robust & Reliable:
- Each test runs in isolation (unique ports/addresses)
- Proper cleanup prevents resource leaks
- Timeout handling prevents hanging tests
- serial_test prevents network conflicts

Comprehensive Coverage:
- Tests actual network communication
- Validates complete interaction flows
- Catches integration bugs unit tests miss
- Tests failure scenarios effectively

7. Test Execution Strategy

Development Workflow:
# Run unit tests (fast)
cargo test --lib

# Run integration tests (slower)
cargo test --test integration

# Run specific integration test
cargo test --test integration test_publisher_subscriber_flow

This approach gives you thorough integration testing while leveraging your existing
architecture and minimizing additional complexity. The key is creating lightweight test
fixtures that manage component lifecycles and using your address manager for network
isolation.