#!/usr/bin/env python3
"""
Hyperliquid Spot WebSocket Examples

This script demonstrates real-time data streaming for spot trading pairs:
- Trades for specific spot pairs
- BBO (best bid/offer) updates
- L2 orderbook snapshots
- Active asset context (real-time spot metadata)

Note: Spot pairs on Hyperliquid include both canonical pairs (e.g., PURR/USDC)
and programmatic pairs (e.g., @142 for VORTX/USDC).
"""

import asyncio
import websockets
import json
from datetime import datetime
from typing import List, Dict, Any


WS_URL = "wss://api.hyperliquid.xyz/ws"


class HyperliquidSpotWebSocketClient:
    """WebSocket client for Hyperliquid Spot API."""
    
    def __init__(self):
        self.ws = None
        self.subscriptions = []
    
    async def connect(self):
        """Establish WebSocket connection."""
        print(f"Connecting to {WS_URL}...")
        self.ws = await websockets.connect(WS_URL)
        print("✓ Connected!\n")
    
    async def subscribe(self, subscription: Dict[str, Any]):
        """
        Subscribe to a data feed.
        
        Args:
            subscription: Subscription configuration dict
        """
        message = {
            "method": "subscribe",
            "subscription": subscription
        }
        
        await self.ws.send(json.dumps(message))
        self.subscriptions.append(subscription)
        print(f"📡 Subscribed to: {subscription}")
    
    async def unsubscribe(self, subscription: Dict[str, Any]):
        """Unsubscribe from a data feed."""
        message = {
            "method": "unsubscribe",
            "subscription": subscription
        }
        
        await self.ws.send(json.dumps(message))
        if subscription in self.subscriptions:
            self.subscriptions.remove(subscription)
        print(f"🔌 Unsubscribed from: {subscription}")
    
    async def listen(self, duration_seconds: int = 30):
        """
        Listen for WebSocket messages.
        
        Args:
            duration_seconds: How long to listen for messages
        """
        print(f"\n{'='*80}")
        print(f"LISTENING FOR SPOT MARKET MESSAGES (for {duration_seconds} seconds)")
        print(f"{'='*80}\n")
        
        start_time = asyncio.get_event_loop().time()
        message_count = 0
        
        try:
            while asyncio.get_event_loop().time() - start_time < duration_seconds:
                try:
                    message = await asyncio.wait_for(self.ws.recv(), timeout=1.0)
                    message_count += 1
                    
                    data = json.loads(message)
                    self._handle_message(data, message_count)
                    
                except asyncio.TimeoutError:
                    continue
                except websockets.exceptions.ConnectionClosed:
                    print("\n❌ WebSocket connection closed")
                    break
        
        except KeyboardInterrupt:
            print("\n\n⚠️  Interrupted by user")
        
        print(f"\n{'='*80}")
        print(f"Total messages received: {message_count}")
        print(f"{'='*80}\n")
    
    def _handle_message(self, data: Dict[str, Any], msg_num: int):
        """Process and display incoming WebSocket messages."""
        channel = data.get("channel", "unknown")
        timestamp = datetime.now().strftime("%H:%M:%S.%f")[:-3]
        
        if channel == "subscriptionResponse":
            print(f"\n✓ [{timestamp}] Subscription confirmed: {data.get('data', {}).get('type')}")
            return
        
        # Format message based on channel type
        print(f"\n--- Message #{msg_num} [{timestamp}] Channel: {channel} ---")
        
        if channel == "trades":
            trades = data.get("data", [])
            print(f"Trades ({len(trades)} trades):")
            for trade in trades:
                side_icon = "🟢" if trade["side"] == "B" else "🔴"
                print(f"  {side_icon} {trade['coin']}: {trade['side']} {trade['sz']} @ ${trade['px']} "
                      f"(time: {trade['time']}, tid: {trade['tid']})")
        
        elif channel == "l2Book":
            book_data = data.get("data", {})
            coin = book_data.get("coin")
            levels = book_data.get("levels", [[], []])
            bids, asks = levels[0][:3], levels[1][:3]
            
            print(f"L2 Book for {coin}:")
            print("  Asks (sell orders):")
            for ask in reversed(asks):
                print(f"    ${ask['px']:<12} | {ask['sz']:<12} ({ask['n']} orders)")
            print("  ---")
            print("  Bids (buy orders):")
            for bid in bids:
                print(f"    ${bid['px']:<12} | {bid['sz']:<12} ({bid['n']} orders)")
        
        elif channel == "bbo":
            bbo_data = data.get("data", {})
            coin = bbo_data.get("coin")
            bbo = bbo_data.get("bbo", [None, None])
            bid, ask = bbo[0], bbo[1]
            
            print(f"BBO for {coin}:")
            if bid:
                print(f"  Bid: ${bid['px']} (size: {bid['sz']}, {bid['n']} orders)")
            if ask:
                print(f"  Ask: ${ask['px']} (size: {ask['sz']}, {ask['n']} orders)")
            if bid and ask:
                spread = float(ask['px']) - float(bid['px'])
                mid = (float(ask['px']) + float(bid['px'])) / 2
                spread_bps = (spread / mid) * 10000
                print(f"  Spread: ${spread:.6f} ({spread_bps:.2f} bps)")
        
        elif channel == "activeAssetCtx":
            asset_data = data.get("data", {})
            coin = asset_data.get("coin")
            ctx = asset_data.get("ctx", {})
            
            print(f"Active Asset Context for {coin}:")
            print(f"  Mark Price: ${ctx.get('markPx')}")
            print(f"  Mid Price: ${ctx.get('midPx')}")
            print(f"  24h Volume: ${ctx.get('dayNtlVlm')}")
            print(f"  Circulating Supply: {ctx.get('circulatingSupply')}")
            print(f"  Total Supply: {ctx.get('totalSupply')}")
        
        else:
            # For unknown channels, print raw data
            print(json.dumps(data, indent=2)[:500] + "...")
    
    async def close(self):
        """Close WebSocket connection."""
        if self.ws:
            await self.ws.close()
            print("\n🔌 WebSocket connection closed")


async def example_spot_trades(coins: List[str] = ["PURR/USDC", "@145"]):
    """
    Example: Subscribe to spot trades.
    
    Args:
        coins: List of spot pair names (e.g., "PURR/USDC" or "@145" for programmatic pairs)
    """
    print("\n" + "="*80)
    print(f"EXAMPLE 1: Spot Trades for {coins}")
    print("="*80)
    
    client = HyperliquidSpotWebSocketClient()
    await client.connect()
    
    # Subscribe to trades for each coin
    for coin in coins:
        await client.subscribe({"type": "trades", "coin": coin})
    
    await client.listen(duration_seconds=15)
    await client.close()


async def example_spot_bbo(coins: List[str] = ["PURR/USDC", "@109"]):
    """
    Example: Subscribe to spot BBO (best bid/offer).
    
    Args:
        coins: List of spot pair names
    """
    print("\n" + "="*80)
    print(f"EXAMPLE 2: Spot BBO for {coins}")
    print("="*80)
    
    client = HyperliquidSpotWebSocketClient()
    await client.connect()
    
    # Subscribe to BBO for each coin
    for coin in coins:
        await client.subscribe({"type": "bbo", "coin": coin})
    
    await client.listen(duration_seconds=15)
    await client.close()


async def example_spot_orderbook(coins: List[str] = ["PURR/USDC"]):
    """
    Example: Subscribe to spot L2 orderbook.
    
    Args:
        coins: List of spot pair names
    """
    print("\n" + "="*80)
    print(f"EXAMPLE 3: Spot L2 Orderbook for {coins}")
    print("="*80)
    
    client = HyperliquidSpotWebSocketClient()
    await client.connect()
    
    # Subscribe to L2 book for each coin
    for coin in coins:
        await client.subscribe({"type": "l2Book", "coin": coin})
    
    await client.listen(duration_seconds=15)
    await client.close()


async def example_spot_comprehensive(coins: List[str] = ["PURR/USDC", "@109", "@145"]):
    """
    Comprehensive example: Subscribe to multiple feeds for spot pairs.
    
    This demonstrates monitoring:
    - Trades
    - BBO (best bid/offer)
    - Active asset context (market metadata)
    
    Args:
        coins: List of spot pair names
              @109 = WOW/USDC (based on high volume)
              @145 = VORTX/USDC (based on high volume)
    """
    print("\n" + "="*80)
    print(f"EXAMPLE 4: Comprehensive - Multiple feeds for {coins}")
    print("="*80)
    print("\nNote: Programmatic pair names like @N are used for non-canonical pairs.")
    print("You can map these to actual token names using the REST API.\n")
    
    client = HyperliquidSpotWebSocketClient()
    await client.connect()
    
    # Subscribe to multiple feeds for each coin
    for coin in coins:
        await client.subscribe({"type": "trades", "coin": coin})
        await client.subscribe({"type": "bbo", "coin": coin})
        await client.subscribe({"type": "activeAssetCtx", "coin": coin})
    
    await client.listen(duration_seconds=30)
    await client.close()


async def main():
    """Run WebSocket examples for spot trading."""
    print("\n" + "="*80)
    print("HYPERLIQUID SPOT WEBSOCKET EXAMPLES")
    print("="*80)
    print("\nThis script demonstrates real-time spot market data streaming.")
    print("\nNote: Spot pairs include:")
    print("  - Canonical pairs: PURR/USDC")
    print("  - Programmatic pairs: @N (e.g., @109 for WOW/USDC, @145 for VORTX/USDC)")
    print("\nRun spot_rest_api_example.py first to discover available pairs and their names.\n")
    
    try:
        # Run examples one by one
        # Comment/uncomment examples as needed
        
        # Example 1: Spot trades
        # await example_spot_trades(coins=["PURR/USDC", "@109"])
        
        # Example 2: Spot BBO
        # await example_spot_bbo(coins=["PURR/USDC", "@109"])
        
        # Example 3: Spot orderbook
        # await example_spot_orderbook(coins=["PURR/USDC"])
        
        # Example 4: Comprehensive (recommended)
        await example_spot_comprehensive(coins=["PURR/USDC", "@109", "@145"])
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    asyncio.run(main())

