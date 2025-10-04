# Hyperliquid API Examples

This directory contains minimal example scripts for interacting with the Hyperliquid perpetuals API.

## Overview

### Perpetuals Trading
- `rest_api_example.py` - REST API examples for discovering available perpetuals and metadata
- `websocket_example.py` - WebSocket examples for real-time perpetuals data streaming
- `stream_top_perps.py` - **Practical example**: Discover top perpetuals by volume and stream their data

### Spot Trading
- `spot_rest_api_example.py` - REST API examples for discovering available spot pairs and tokens
- `spot_websocket_example.py` - WebSocket examples for real-time spot market data streaming

## Prerequisites

Install required packages:

```bash
pip install requests websockets
```

## REST API Examples

The REST API script demonstrates how to:
1. Retrieve all perpetual dexs
2. Retrieve perpetuals metadata (universe and margin tables)
3. Retrieve asset contexts (mark price, funding, open interest, etc.)
4. Filter actively trading perpetuals

### Usage

```bash
python rest_api_example.py
```

This will output:
- List of all perpetual dexs
- Complete metadata about available perpetuals
- Real-time market data (mark price, funding, volume, etc.)
- A filtered view of actively trading perpetuals sorted by volume

### Key Insights

**Discovering actively trading perps:**
1. Call `metaAndAssetCtxs` endpoint to get both metadata and real-time data
2. Filter out assets with `isDelisted: true`
3. Check `dayNtlVlm` (24h volume) to see which are actively trading
4. Use `universe` field to get list of all available perpetual symbols

## WebSocket Examples

The WebSocket script demonstrates real-time streaming for:
- `allMids` - All mid prices (useful for discovering all active markets)
- `trades` - Individual trades for specific coins
- `bbo` - Best bid/offer updates
- `l2Book` - Full L2 orderbook snapshots
- `activeAssetCtx` - Real-time perp metadata (funding, mark price, OI, etc.)

### Usage

```bash
python websocket_example.py
```

By default, it runs the comprehensive example which subscribes to multiple feeds for BTC, ETH, and SOL.

You can modify the script to run specific examples:
- `example_all_mids()` - Just mid prices
- `example_trades_and_bbo()` - Trades and BBO for specific coins
- `example_orderbook()` - L2 orderbook
- `example_active_asset_context()` - Real-time perp metadata
- `example_comprehensive()` - All of the above

### Subscription Types

| Type | Purpose | Data Frequency |
|------|---------|----------------|
| `allMids` | All mid prices | Per block (~0.5s intervals) |
| `trades` | Trade executions | Real-time on each trade |
| `bbo` | Best bid/offer | Only when BBO changes |
| `l2Book` | Full orderbook | Snapshots per block (~0.5s) |
| `activeAssetCtx` | Perp metadata | Per block (~0.5s) |

## Answering Your Questions

### 1. How to figure out the universe of actively trading perps?

**Method 1: REST API**
```python
# Call metaAndAssetCtxs endpoint
response = requests.post(
    "https://api.hyperliquid.xyz/info",
    json={"type": "metaAndAssetCtxs"}
)
metadata, asset_contexts = response.json()

# Get list of non-delisted perps
active_perps = [
    asset["name"] 
    for asset in metadata["universe"] 
    if not asset.get("isDelisted")
]
```

**Method 2: WebSocket**
```python
# Subscribe to allMids to see all actively quoted markets
await client.subscribe({"type": "allMids"})
# You'll receive a dict with all coin symbols as keys
```

### 2. Subscribing to data for specific symbols

Once you have the universe of symbols, subscribe to multiple feeds:

```python
coins = ["BTC", "ETH", "SOL"]  # From universe

for coin in coins:
    # Trades
    await client.subscribe({"type": "trades", "coin": coin})
    
    # Best bid/offer
    await client.subscribe({"type": "bbo", "coin": coin})
    
    # L2 Orderbook
    await client.subscribe({"type": "l2Book", "coin": coin})
    
    # Perp metadata (funding, mark price, OI)
    await client.subscribe({"type": "activeAssetCtx", "coin": coin})
```

## Data Collection Strategy

For perpetuals data collection:

1. **Initial Discovery** (REST API):
   - Call `metaAndAssetCtxs` to get full universe
   - Store metadata (decimals, max leverage, etc.)
   - Get initial snapshot of market data

2. **Real-time Streaming** (WebSocket):
   - Subscribe to `allMids` to track all active markets
   - Subscribe to `activeAssetCtx` for each coin to get funding updates
   - Subscribe to `trades` and `bbo` for market activity
   - Subscribe to `l2Book` if you need full orderbook depth

3. **Recommended Feeds for Data Recording**:
   - `trades` - Complete trade history with prices, sizes, and timestamps
   - `activeAssetCtx` - Funding rates, mark price, open interest (updated per block)
   - `bbo` - For spread analysis and market quality metrics
   - `l2Book` - Optional, for full orderbook analysis (higher bandwidth)

## Practical Data Collection Script

The `stream_top_perps.py` script demonstrates a complete workflow:

```bash
# Stream data for top 5 perpetuals by volume for 60 seconds
python stream_top_perps.py 5 60

# Stream data for top 10 perpetuals for 120 seconds
python stream_top_perps.py 10 120
```

This script:
1. Discovers actively trading perpetuals via REST API
2. Filters to top N by 24h volume
3. Subscribes to trades, BBO, and funding data via WebSocket
4. Prints real-time updates and statistics

This is the recommended starting point for building a data collection system.

## Performance Notes

- WebSocket messages arrive at high frequency (~every 0.5s for per-block updates)
- `l2Book` sends full snapshots, so bandwidth can be significant for multiple coins
- `bbo` is more efficient than `l2Book` if you only need top of book
- Consider filtering by volume/liquidity to focus on most active markets
- Top 10 perpetuals by volume typically represent >80% of total trading activity

## Spot Trading Examples

The Spot trading scripts demonstrate how to interact with Hyperliquid's spot markets, which include both canonical pairs (like PURR/USDC) and programmatic pairs for HyperEVM tokens.

### REST API - Spot Trading

```bash
python spot_rest_api_example.py
```

This will output:
- List of all spot tokens and their metadata
- All trading pairs (both canonical and programmatic @N pairs)
- Real-time market data (prices, volume, supply)
- Top actively trading spot pairs by volume

### WebSocket - Spot Trading

```bash
python spot_websocket_example.py
```

By default, it streams data for PURR/USDC and top volume pairs like @109 (WOW/USDC) and @145 (VORTX/USDC).

### Understanding Spot Pairs

Hyperliquid spot markets use two naming conventions:
1. **Canonical pairs**: Like `PURR/USDC` - officially named pairs
2. **Programmatic pairs**: Like `@109` - pairs with programmatic names

To map `@N` to actual token names:
- Use `spotMeta` REST endpoint to get the `universe` and `tokens` arrays
- For pair `@N`, look up `universe[N].tokens` to get token indices
- Map token indices to names in the `tokens` array

Example:
```python
# @109 has tokens=[98, 0]
# Token 98 = "WOW", Token 0 = "USDC"
# Therefore @109 = WOW/USDC
```

### Top Spot Pairs by Volume

Based on recent data, the most actively traded spot pairs are:
- **HYPE/USDC** (~$100M daily volume) - Hyperliquid's native token
- **VORTX/USDC** (~$100M+ daily volume)
- **WOW/USDC** (~$90M+ daily volume)
- **PURR/USDC** - The first canonical spot pair
- **USOL/USDC** - Wrapped SOL on HyperEVM
- **ANON/USDC**, **EX/USDC** - Other popular HyperEVM tokens

Most spot trading volume is concentrated in HyperEVM native tokens rather than wrapped versions of major cryptocurrencies.

## API Documentation

- REST API (Perpetuals): https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/info-endpoint/perpetuals
- REST API (Spot): https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/info-endpoint/spot
- WebSocket API: https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket/subscriptions

