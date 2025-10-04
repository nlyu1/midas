#!/usr/bin/env python3
"""
Hyperliquid WebSocket API Examples

This script demonstrates how to subscribe to real-time data streams:
1. All mids (all mid prices)
2. Trades for specific coins
3. BBO (best bid/offer) for specific coins
4. L2 orderbook for specific coins
5. Active asset context (real-time perp metadata)

The script will connect to the WebSocket, subscribe to multiple feeds,
and print incoming messages for a specified duration.
"""

import asyncio
import websockets
import json
import sys
from datetime import datetime
from typing import List, Dict, Any


WS_URL = "wss://api.hyperliquid.xyz/ws"


class HyperliquidWebSocketClient:
    """WebSocket client for Hyperliquid API."""
    
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
        print(f"LISTENING FOR MESSAGES (for {duration_seconds} seconds)")
        print(f"{'='*80}\n")
        
        start_time = asyncio.get_event_loop().time()
        message_count = 0
        
        try:
            while asyncio.get_event_loop().time() - start_time < duration_seconds:
                try:
                    # Wait for message with timeout
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
        
        if channel == "allMids":
            mids = data.get("data", {}).get("mids", {})
            print(f"All Mids (showing first 5):")
            for i, (coin, price) in enumerate(list(mids.items())[:5]):
                print(f"  {coin}: ${price}")
            if len(mids) > 5:
                print(f"  ... and {len(mids) - 5} more")
        
        elif channel == "trades":
            trades = data.get("data", [])
            print(f"Trades ({len(trades)} trades):")
            for trade in trades:
                print(f"  {trade['coin']}: {trade['side']} {trade['sz']} @ ${trade['px']} "
                      f"(time: {trade['time']}, tid: {trade['tid']})")
        
        elif channel == "l2Book":
            book_data = data.get("data", {})
            coin = book_data.get("coin")
            levels = book_data.get("levels", [[], []])
            bids, asks = levels[0][:3], levels[1][:3]  # Show top 3 levels
            
            print(f"L2 Book for {coin}:")
            print("  Asks:")
            for ask in reversed(asks):
                print(f"    ${ask['px']:<10} | {ask['sz']:<10} ({ask['n']} orders)")
            print("  ---")
            print("  Bids:")
            for bid in bids:
                print(f"    ${bid['px']:<10} | {bid['sz']:<10} ({bid['n']} orders)")
        
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
            print(f"  Funding Rate: {ctx.get('funding')}")
            print(f"  Open Interest: ${ctx.get('openInterest')}")
            print(f"  24h Volume: ${ctx.get('dayNtlVlm')}")
            print(f"  Oracle Price: ${ctx.get('oraclePx')}")
        
        else:
            # For unknown channels, just print the raw data
            print(json.dumps(data, indent=2))
    
    async def close(self):
        """Close WebSocket connection."""
        if self.ws:
            await self.ws.close()
            print("\n🔌 WebSocket connection closed")


async def example_all_mids():
    """Example: Subscribe to all mid prices."""
    print("\n" + "="*80)
    print("EXAMPLE 1: All Mids (all perpetual mid prices)")
    print("="*80)
    
    client = HyperliquidWebSocketClient()
    await client.connect()
    
    # Subscribe to all mids
    await client.subscribe({"type": "allMids"})
    
    # Listen for 10 seconds
    await client.listen(duration_seconds=10)
    
    await client.close()


async def example_trades_and_bbo(coins: List[str] = ["BTC", "ETH"]):
    """Example: Subscribe to trades and BBO for specific coins."""
    print("\n" + "="*80)
    print(f"EXAMPLE 2: Trades and BBO for {coins}")
    print("="*80)
    
    client = HyperliquidWebSocketClient()
    await client.connect()
    
    # Subscribe to trades and BBO for each coin
    for coin in coins:
        await client.subscribe({"type": "trades", "coin": coin})
        await client.subscribe({"type": "bbo", "coin": coin})
    
    # Listen for 20 seconds
    await client.listen(duration_seconds=20)
    
    await client.close()


async def example_orderbook(coins: List[str] = ["BTC"]):
    """Example: Subscribe to L2 orderbook for specific coins."""
    print("\n" + "="*80)
    print(f"EXAMPLE 3: L2 Orderbook for {coins}")
    print("="*80)
    
    client = HyperliquidWebSocketClient()
    await client.connect()
    
    # Subscribe to L2 book for each coin
    for coin in coins:
        await client.subscribe({"type": "l2Book", "coin": coin})
    
    # Listen for 15 seconds
    await client.listen(duration_seconds=15)
    
    await client.close()


async def example_active_asset_context(coins: List[str] = ["BTC", "ETH"]):
    """Example: Subscribe to active asset context (real-time perp metadata)."""
    print("\n" + "="*80)
    print(f"EXAMPLE 4: Active Asset Context for {coins}")
    print("="*80)
    
    client = HyperliquidWebSocketClient()
    await client.connect()
    
    # Subscribe to active asset context for each coin
    for coin in coins:
        await client.subscribe({"type": "activeAssetCtx", "coin": coin})
    
    # Listen for 15 seconds
    await client.listen(duration_seconds=15)
    
    await client.close()


async def example_comprehensive(coins: List[str] = ["BTC", "ETH", "SOL"]):
    """
    Comprehensive example: Subscribe to multiple feeds simultaneously.
    
    This demonstrates a realistic use case where you want to monitor:
    - All mid prices
    - Trades for specific coins
    - BBO for specific coins
    - Active asset context for specific coins
    """
    print("\n" + "="*80)
    print(f"EXAMPLE 5: Comprehensive - Multiple feeds for {coins}")
    print("="*80)
    
    client = HyperliquidWebSocketClient()
    await client.connect()
    
    # Subscribe to all mids (for universe discovery)
    await client.subscribe({"type": "allMids"})
    
    # Subscribe to specific coin feeds
    for coin in coins:
        await client.subscribe({"type": "trades", "coin": coin})
        await client.subscribe({"type": "bbo", "coin": coin})
        await client.subscribe({"type": "activeAssetCtx", "coin": coin})
    
    # Listen for 30 seconds
    await client.listen(duration_seconds=30)
    
    await client.close()


async def main():
    """Run WebSocket examples."""
    print("\n" + "="*80)
    print("HYPERLIQUID WEBSOCKET EXAMPLES")
    print("="*80)
    print("\nThis script demonstrates real-time data streaming from Hyperliquid.")
    print("You can run different examples or modify the code to suit your needs.\n")
    
    try:
        # Run examples one by one
        # Comment/uncomment examples as needed
        
        # Example 1: All mids
        # await example_all_mids()
        
        # Example 2: Trades and BBO
        # await example_trades_and_bbo(coins=["BTC", "ETH"])
        
        # Example 3: L2 Orderbook
        # await example_orderbook(coins=["BTC"])
        
        # Example 4: Active asset context
        # await example_active_asset_context(coins=["BTC", "ETH"])
        
        # Example 5: Comprehensive (recommended)
        await example_comprehensive(coins=["BTC", "ETH", "SOL"])
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    asyncio.run(main())

