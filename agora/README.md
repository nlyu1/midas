# Agora: Publisher-Subscriber System

Agora is a distributed publisher-subscriber messaging system built with path-based service discovery. It enables real-time streaming communication between publishers and subscribers using statically typed messages, supporting **network coordination between Unix nodes** for **both Rust and Python** applications.

## Core Concepts

**Publisher\<T>**: Creates a typed publisher for messages of _Agorable_ type `T`. Each publisher exposes three endpoints:
- Binary endpoint for typed messages (via Postcard serialization, consumed by `Subscriber<T>`)
- String endpoint for type-agnostic monitoring (via `Display` trait, consumed by `OmniSubscriber`)
- Ping endpoint for health checks and one-time current value queries

**Subscriber\<T>**: Connects to publishers of the same type `T` for streaming typed messages.

**OmniSubscriber**: Type-agnostic subscriber that receives string representations from any publisher.

**Agorable**: Trait for types that can be published/subscribed. Built-in support for `String`, `i64`, `bool`, `f64`, `f32`.

**MetaServer**: Central registry that manages publisher discovery and location tracking. 

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

3. **Start gateway on each publishing node** (including main):
   ```bash
   cargo run --bin gateway
   # Agora Gateway listening on 192.168.0.75:8081
   # - Ready to proxy connections
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

1. **Start gateway process** (only publishing nodes need a gateway):
```bash
cargo run --bin gateway
# Agora Gateway listening on 192.168.0.69:8081
# - Ready to proxy connections:
#   - /rawstream/{path}/bytes → /tmp/agora/{path}/bytes/rawstream.sock
#   - /rawstream/{path}/string → /tmp/agora/{path}/string/rawstream.sock
#   - /ping/{path} → /tmp/agora/{path}/ping.sock
```

2. **Run example publisher**:
```bash
cargo run --bin publisher -- --host 192.168.0.75
```

### Subscribe to messages
Run on any node with connection to metaserver. Subscribers **automatically discover publisher gateway addresses** via metaserver: 
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

We exemplify usage using two nodes: first **start metaservers** 
```bash
# Node 0
cargo run --bin metaserver # Metaserver active on 192.168.0.75:8080
cargo run --bin gateway # Agora Gateway listening on 192.168.0.75:8081
# Node 1
cargo run --bin metaserver # Metaserver active on 192.168.0.69:8080
```
Start two publishers on node 0:
```bash
# Separate terminal: create `src0_node0` publisher by answering `src0_node0` to all three prompts
cargo run --bin publisher
# Separate terminal: create `src1_node0` publisher by answering `src1_node0` to all three prompts
cargo run --bin publisher
```
Start a relay on node 0, _publishing to node 1_: 
```bash 
# Run on node 0. Enter `dest_node1` when prompted, with any initial value
cargo run --bin relay_example -- --host 192.168.0.69 
# When prompted, swap on `src0_node0` from `192.168.0.75`
```
Start a subscriber on node 1:
```bash
# Node 1. Subscribe to `dest_node1`
cargo run --bin subscriber
```
Now, try publishing values in the `src0_node0` and `src1_node0` processes separately: only one of them will be published to `dest_node1`, and this can be controlled by the relay process. 
- **Sharp edge!!**: user is responsible for creating `Relay<T>` of the correct type as the publisher! Channel type mismatch will result in opaque runtime errors. 

## Architecture

### Service Discovery
One Agora node runs the **metaserver** process, which responds to TCP connections on port 8080 (default `METASERVER_PORT` in `src/constants.rs`). The metaserver is implemented as a [TARPC](https://docs.rs/tarpc/latest/tarpc/) RPC server.
- For each service, the metaserver stores the publisher's **IP address** and **gateway port**. Subscribers query the metaserver to discover publisher locations.
- The metaserver maintains a **ping client** to each registered service, polling every 500ms (`CHECK_PUBLISHER_LIVELINESS_EVERY_MS`). Non-responsive services are automatically removed from the registry.

### Publishing Processes
Each publishing Agora node runs a **gateway** process, which listens for TCP WebSocket connections on port 8081 (default `GATEWAY_PORT` in `src/constants.rs`). Each `Publisher<T>` instance creates three Unix Domain Socket (UDS) WebSocket servers on the local node:

1. **Typed binary stream**: `/tmp/agora/{path}/bytes/rawstream.sock`
   - Serves `Vec<u8>` payloads serialized via [Postcard](https://docs.rs/postcard/)
   - Accessed by typed `Subscriber<T>` instances

2. **String stream**: `/tmp/agora/{path}/string/rawstream.sock`
   - Serves human-readable `String` representations (via `Display` trait)
   - Accessed by `OmniSubscriber` instances for type-agnostic monitoring

3. **Ping endpoint**: `/tmp/agora/{path}/ping.sock`
   - Serves current value snapshots and health checks
   - Returns both binary and string payloads with timestamps

### Subscribing Processes
Each subscriber instantiates an `AgoraClient` (metaclient), which queries the metaserver for the publisher's IP and gateway port. The subscriber then connects to the remote publisher via the gateway's TCP WebSocket endpoints:

- **Typed subscriber** (`Subscriber<T>`):
  - Stream: `ws://{publisher_ip}:{gateway_port}/rawstream/{path}/bytes`
  - Ping: `ws://{publisher_ip}:{gateway_port}/ping/{path}`

- **Type-agnostic subscriber** (`OmniSubscriber`):
  - Stream: `ws://{publisher_ip}:{gateway_port}/rawstream/{path}/string`
  - Ping: `ws://{publisher_ip}:{gateway_port}/ping/{path}`

The **gateway** acts as a bidirectional proxy, forwarding each TCP WebSocket connection to the corresponding local UDS endpoint:
- `ws://{gateway_ip}:{gateway_port}/rawstream/{path}/bytes` ↔ `/tmp/agora/{path}/bytes/rawstream.sock`
- `ws://{gateway_ip}:{gateway_port}/rawstream/{path}/string` ↔ `/tmp/agora/{path}/string/rawstream.sock`
- `ws://{gateway_ip}:{gateway_port}/ping/{path}` ↔ `/tmp/agora/{path}/ping.sock`

This architecture decentralizes data flow: the metaserver only handles discovery, while actual message streaming occurs directly between gateways and publishers via WebSocket pipes. 

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
- Central service registry using [TARPC](https://docs.rs/tarpc/latest/tarpc/) RPC framework
- Stores publisher locations (IP address and gateway port) indexed by path
- Provides service discovery via `get_publisher_info(path)` and `register_publisher(name, path, gateway_port)` RPC methods
- Monitors service health via WebSocket ping clients, removing non-responsive publishers every 500ms

**Core Types (`core/` and `relay.rs`)**:
- `Publisher<T>`: Publishes typed messages at a path
  - Creates three local UDS WebSocket endpoints: bytes (`/tmp/agora/{path}/bytes/rawstream.sock`), strings (`/tmp/agora/{path}/string/rawstream.sock`), and ping (`/tmp/agora/{path}/ping.sock`)
  - Registers with MetaServer, storing its gateway connection details for subscriber discovery
  - Serializes values to both Postcard binary format and `Display` string format
- `Subscriber<T>`: Subscribes to typed binary messages from a specific path
  - Queries MetaServer for publisher location, then connects to remote gateway
  - Uses `get()` for current value snapshot via ping, `get_stream()` for continuous updates
- `OmniSubscriber`: Type-agnostic subscriber receiving string representations
  - Identical API to `Subscriber<T>`, but connects to string endpoint instead of bytes
- `Agorable`: Trait for publishable types (requires `Serialize + Deserialize + Display + Clone + Send`)
- `Relay<T>`: Dynamic message router combining fixed-destination Publisher with switchable-source Subscriber
  - **Architecture**: Two async tasks communicate via unbounded channel:
    - `stream_out`: Consumes from channel → publishes to fixed destination path
    - `stream_in`: Subscribes to current source path → sends to channel
  - **API**:
    - `new(name, dest_path, initial_value, dest_metaserver_connection, local_gateway_port)`: Creates relay with destination publisher
    - `swapon(src_path, src_metaserver_connection)`: Atomically switches source by aborting old `stream_in` task and spawning new subscriber
  - **Use cases**:
    - <u>Contiguous streaming from discontinuous sources</u>: `src0` streams until $t_1$, `src1` from $t_0 < t_1$ onwards. Initialize relay at `src0`, call `swapon(src1)` during overlap $[t_0, t_1]$ for seamless transition.
    - <u>Endpoint rerouting</u>: Redirect persistent process publishing to `path0` → `path1` without restart by creating relay at `path1` initialized to `path0`.
    - <u>Cross-metaserver bridging</u>: Relay from `(metaserver_0, port_0)` → `(metaserver_1, port_1)` remains functional even if `metaserver_0` dies after initial connection, enabling multi-cluster communication.

**Communication Layer**:
- **Gateway (`gateway.rs`)**: Bidirectional TCP-to-UDS WebSocket proxy, enabling cross-node communication
  - Maps `/rawstream/{path}/bytes` → `/tmp/agora/{path}/bytes/rawstream.sock`, `/rawstream/{path}/string` → `/tmp/agora/{path}/string/rawstream.sock`, and `/ping/{path}` → `/tmp/agora/{path}/ping.sock`
  - Each connection spawns independent forwarding tasks for decentralized data flow
- **RawStream (`rawstream/`)**: WebSocket-based pub-sub protocol using Tokio broadcast channels
  - Server: Binds UDS listener, fans out messages to all connected clients via `broadcast::channel`
  - Client: Connects via gateway WebSocket, auto-reconnects on failure with 100ms retry interval
- **Ping (`ping/`)**: WebSocket-based request-response protocol for health checks and current value queries
  - Server: Returns `PingResponse { vec_payload, str_payload, timestamp }` on `"ping"` text message
  - Client: Sends `"ping"`, receives JSON response, calculates round-trip time

**Python Integration (`pywrappers/`)**:
- PyO3 bindings for all core types
- Typed publishers/subscribers/relays for String, i64, bool, f64, f32
- Synchronous and streaming APIs 