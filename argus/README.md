# Argus

Argus is a market data ingestion and publishing service that streams real-time data from cryptocurrency exchanges to Agora paths. It handles exchange WebSocket connections, parses market data updates, and publishes them as typed Agora messages for downstream consumption.

## Design

Argus implements a **WebSocket → Agora** data pipeline with automatic universe management:

### WebSocket Workers
Each exchange data source is implemented as a `WebstreamWorker<T>` that:
1. Connects to the exchange WebSocket API
2. Subscribes to market data channels for a given symbol universe
3. Parses incoming messages into typed structs implementing `Agorable`
4. Publishes each update to Agora paths in the format `{prefix}/{payload_type}/{symbol}`

Workers handle WebSocket lifecycle automatically:
- Heartbeat/ping responses to maintain connections
- Automatic reconnection on disconnect with 5-second retry
- Channel-based message routing (subscription confirmations, data, pong responses)

### Universe Management with Versioned Paths
To support dynamic symbol universes (new listings, delistings) without disrupting downstream subscribers, Argus uses a **versioned path + relay** architecture:

**Temporary versioned paths** (internal, used by WebstreamWorkers):
- `argus/tmp/{exchange}/{spot|perp}_{version}/{payload_type}/{symbol}`
- Created for each universe snapshot
- Short-lived: dropped when universe changes

**Stable consumer paths** (external, used by subscribers):
- `{prefix}/{spot|perp}/{payload_type}/{symbol}`
- Persistent across universe changes
- Backed by `Relay<T>` instances

**Universe change flow**:
1. `UniverseManager` queries exchange REST API periodically (default: 60s)
2. On universe change detection:
   - Increment version number
   - Spawn new `WebstreamWorker` instances on new versioned paths
   - Call `Relay::swapon()` to atomically switch relay sources
   - Drop old workers

This ensures continuous data streams to subscribers even as the symbol universe evolves.

## Hyperliquid

Argus publishes Hyperliquid perpetual and spot market data to Agora. All paths use the prefix defined in `src/constants.rs`:

```rust
pub const HYPERLIQUID_AGORA_PREFIX: &str = "argus/hyperliquid";
```

### Agora Paths

Consumer-facing stable paths follow the pattern:
```
{HYPERLIQUID_AGORA_PREFIX}/{market_type}/{payload_type}/{symbol}
```

Where:
- `market_type`: `perp` (perpetual futures) or `spot` (spot markets)
- `payload_type`: `last_trade`, `bbo`, `orderbook`, `perp_context`, or `spot_context`
- `symbol`: Normalized symbol (e.g., `BTC_PERP`, `ETH_PERP`, `WOW-USDC`)

**Examples**:
```
argus/hyperliquid/perp/last_trade/BTC_PERP
argus/hyperliquid/perp/bbo/ETH_PERP
argus/hyperliquid/perp/orderbook/SOL_PERP
argus/hyperliquid/perp/perp_context/BTC_PERP
argus/hyperliquid/spot/last_trade/WOW-USDC
argus/hyperliquid/spot/bbo/PURR-USDC
argus/hyperliquid/spot/spot_context/WOW-USDC
```

Internal versioned paths (managed automatically, not for direct consumption):
```
argus/tmp/hyperliquid/{market_type}_{version}/{payload_type}/{symbol}
```

### Data Types

All data types implement the `HyperliquidStreamable` trait and are published as `AgorableOption<T>`:

**TradeUpdate** (`last_trade`):
```rust
pub struct TradeUpdate {
    pub symbol: TradingSymbol,
    pub received_time: DateTime<Utc>,
    pub trade_id: u64,
    pub price: Price,
    pub size: TradeSize,
    pub trade_time: DateTime<Utc>,
    pub is_buy: bool,  // true if buyer side, false if seller side
}
```
Hyperliquid sends trades as arrays `[{coin, px, sz, time, tid, side}, ...]`. The parser extracts ALL trades from each message (critical: earlier implementation bug only processed first trade).

**BboUpdate** (`bbo`):
```rust
pub struct BboUpdate {
    pub symbol: TradingSymbol,
    pub received_time: DateTime<Utc>,
    pub time: DateTime<Utc>,
    pub bid_price: Price,
    pub bid_size: TradeSize,
    pub bid_orders: u32,
    pub ask_price: Price,
    pub ask_size: TradeSize,
    pub ask_orders: u32,
}
```
Best bid/offer updates with size and order count.

**OrderbookSnapshot** (`orderbook`):
```rust
pub struct OrderbookSnapshot {
    pub symbol: TradingSymbol,
    pub received_time: DateTime<Utc>,
    pub time: DateTime<Utc>,
    pub bid_levels: Vec<(Price, TradeSize, u32)>,  // (price, size, n_orders)
    pub ask_levels: Vec<(Price, TradeSize, u32)>,
}
```
Full L2 orderbook snapshots with all price levels.

**PerpAssetContext** (`perp_context`):
```rust
pub struct PerpAssetContext {
    pub symbol: TradingSymbol,
    pub received_time: DateTime<Utc>,
    pub mark_price: Price,
    pub mid_price: Option<Price>,
    pub funding_rate: f64,           // As decimal (e.g., 0.0001 = 0.01%)
    pub open_interest: Option<f64>,  // In USD
    pub volume_24h: Option<f64>,     // In USD
    pub oracle_price: Option<Price>,
}
```
Perpetual-specific market metadata including funding rate and open interest.

**SpotAssetContext** (`spot_context`):
```rust
pub struct SpotAssetContext {
    pub symbol: TradingSymbol,
    pub received_time: DateTime<Utc>,
    pub mark_price: Price,
    pub mid_price: Option<Price>,
    pub volume_24h: Option<f64>,          // In USD
    pub circulating_supply: Option<f64>,  // Spot-specific
    pub total_supply: Option<f64>,        // Spot-specific
}
```
Spot-specific market metadata including supply information.

### Symbol Normalization

Hyperliquid uses different symbol formats than Argus's normalized format:

**Perpetuals**:
- Hyperliquid: `BTC`, `ETH`, `SOL`
- Normalized: `BTC_PERP`, `ETH_PERP`, `SOL_PERP`

**Spots**:
- Hyperliquid: `@109` (token index), `PURR/USDC` (direct pair)
- Normalized: `WOW-USDC`, `PURR-USDC` (human-readable names)

The `UniverseManager` maintains a `BiMap<TradingSymbol, TradingSymbol>` for bidirectional translation. Each `HyperliquidStreamable` implementation extracts the Hyperliquid symbol from its data, translates to normalized format, and embeds it in the published struct.

### Usage

**Start Hyperliquid publisher** (publishes all active perpetuals and spots):
```bash
cargo run --bin hyperliquid-publisher
```

**Subscribe to trades** (Rust):
```rust
use agora::{AgorableOption, ConnectionHandle, Subscriber};
use argus::crypto::hyperliquid::TradeUpdate;

let metaserver = ConnectionHandle::new_local(8000)?;
let mut sub = Subscriber::<AgorableOption<TradeUpdate>>::new(
    "argus/hyperliquid/perp/last_trade/BTC_PERP".to_string(),
    metaserver,
).await?;

let (current, mut stream) = sub.get_stream().await?;
while let Some(Ok(AgorableOption(Some(trade)))) = stream.next().await {
    println!("Trade: {} @ {}", trade.size.to_f64(), trade.price.to_f64());
}
```

**Debug trade stream**:
```bash
cargo run --bin trade-debug
```
This tool creates a single-version `HyperliquidWebstreamWorker<TradeUpdate>` for the top 5 perpetuals and prints trades with statistics.

### Configuration

Key constants in `src/constants.rs`:

```rust
pub const AGORA_METASERVER_DEFAULT_PORT: u16 = 8000;
pub const AGORA_GATEWAY_PORT: u16 = 8001;

pub const HYPERLIQUID_WEBSTREAM_ENDPOINT: &str = "wss://api.hyperliquid.xyz/ws";
pub const HYPERLIQUID_INFO_ENDPOINT: &str = "https://api.hyperliquid.xyz/info";
pub const HYPERLIQUID_AGORA_PREFIX: &str = "argus/hyperliquid";
pub const HYPERLIQUID_DATA_SUFFIX: &str = "hyperliquid";

pub const WORKER_INIT_DELAY_MS: u64 = 500;  // Wait before relay swapon
pub const RELAY_BATCH_SIZE: usize = 10;      // Batch relay creation
pub const RELAY_BATCH_DELAY_MS: u64 = 100;   // Delay between batches
```

### Implementation Details

**HyperliquidStreamable trait** (`src/crypto/hyperliquid.rs`):
```rust
pub trait HyperliquidStreamable: Agorable + Sized {
    fn of_channel_data(
        data: serde_json::Value,
        symbol_map: &BiMap<TradingSymbol, TradingSymbol>,
    ) -> OrError<Vec<Self>>;

    fn subscription_type() -> String;    // "trades", "bbo", "l2Book", "activeAssetCtx"
    fn payload_identifier() -> String;   // "last_trade", "bbo", "orderbook", etc.
    fn symbol(&self) -> TradingSymbol;   // For routing in webstream worker
}
```

Each data type extracts coin from its specific structure (object for BBO/context, array for trades), normalizes via `symbol_map`, and returns `Vec<Self>`. The webstream worker iterates results and routes via `item.symbol()`.

**HyperliquidWebstreamWorker** (`src/crypto/hyperliquid/webstream.rs`):
Generic worker parameterized by `T: HyperliquidStreamable`. Creates one publisher per symbol, subscribes to Hyperliquid WebSocket channel, parses messages via `T::of_channel_data()`, and publishes to Agora.

**HyperliquidPublisher** (`src/crypto/hyperliquid/publisher.rs`):
Orchestrates universe management with versioned workers and relay swapping. Spawns separate workers for perpetuals and spots, each with 4 data types (trades, bbo, orderbook, context).

**UniverseManager** (`src/crypto/hyperliquid/universe.rs`):
Queries Hyperliquid REST API (`metaAndAssetCtxs` endpoint) to get active symbol lists. Sorts perpetuals by 24h volume. Maintains symbol translation BiMap.
