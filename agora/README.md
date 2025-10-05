# Agora: Publisher-Subscriber System

Agora is a distributed publisher-subscriber messaging system built with path-based service discovery. It enables real-time streaming communication between publishers and subscribers using statically typed messages, supporting **network coordination between Unix nodes** for **both Rust and Python** applications.

## Core Concepts

**Publisher\<T>**: Creates a typed publisher for messages of _Agorable_ type `T`. Each publisher exposes three endpoints:
- Service endpoint for typed binary messages
- String endpoint for type-agnostic monitoring
- Ping endpoint for health checks and one-time queries

**Subscriber\<T>**: Connects to publishers of the same type `T` for streaming typed messages.

**OmniSubscriber**: Type-agnostic subscriber that receives string representations from any publisher.

**Agorable**: Trait for types that can be published/subscribed. Built-in support for `String`, `i64`, `bool`, `f64`, `f32`.

**MetaServer**: Central registry that manages publisher discovery and IPv6 address allocation. 

**Relay\<T>**: Dynamic publisher that relays typed messages from switchable source paths to a fixed destination. Enables contiguous streams from discontinuous sources and cross-metaserver bridging via `swapon()`.

## Quick Start

1. **Build the project**:
   ```bash
   cargo build
   ```

2. **Start the MetaServer on main node**: 
   ```bash
   cargo run --bin metaserver
   # Metaserver started on 192.168.0.75:8080
   ```

3. **Start gateway on each node** (including main): 
   ```bash
   cargo run --bin gateway
   # Agora Gateway listening on 192.168.0.75:8081
   ```

4. **Explore with MetaClient on any node** (addr=localhost, port=8080) if not passed:
   ```bash
   cargo run --bin metaclient -- --host 192.168.0.75
   ```

## Python Integration

Agora provides Python bindings through PyO3 and Maturin, supporting typed publishers and subscribers.
1. Install maturin in your Python environment:
   ```bash
   pip install maturin
   ```

2. Build and install the Python package:
   ```bash
   maturin develop --release
   ```

## Examples

The following example demonstrates the basic workflow. 
```bash 
# Main node: provide metaserver port (default 8080)
cargo run --bin metaserver 
# Metaserver started on 192.168.0.75:8080
```
### Publish messages
Run on any node with connection to main (can be main): 
1. **Start gateway process**. Only the publisher needs to start gateway. 
```bash 
# Replace with main node IP
cargo run --bin gateway
# Gateway running at 192.168.0.69:8081
```
2. **Run example publisher**
```bash 
cargo run --bin publisher -- --host 192.168.0.75
```

### Subscribe to messages
Run on any machine with publisher & main node connection: subscriber **don't need to know publisher gateway port**! 
```bash
# Provide metaserver IP (default localhost) and port (default 8080) 
cargo run --bin subscriber -- --host 192.168.0.75
```

### Monitor with MetaClient
```bash
# Provide metaserver IP (default localhost) and port (default 8080)
cargo run --bin metaclient -- --host 192.168.0.75
# Use 'print' to see active publishers
# Use 'monitor <path>' to watch messages
# Use 'info <path>' for publisher details
```

### Relay (advanced)

Relays provide dynamic data routing across agora paths and metaservers. Use-cases include: 

1. Hot-swapping metaserver without restarting existing publishers. 
2. Combining data streams. 

Run on the main node: 
```bash
cargo run --bin metaserver # Metaserver active on 192.168.0.75:8080
# Separate terminal: 
cargo run --bin gateway # Agora Gateway listening on 192.168.0.75:8081
# Separate terminal: create `src0_node0` publisher by answering `src0_node0` to all three prompts
cargo run --bin publisher 
# Separate terminal: create `src1_node0` publisher by answering `src0_node0` to all three prompts
cargo run --bin publisher 
```

```bash
```

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

#### Checking that metaserver is actually running: 
`ss -6 -tlnp | grep 8080`