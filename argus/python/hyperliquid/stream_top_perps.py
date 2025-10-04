#!/usr/bin/env python3
"""
Stream data for top perpetuals by volume.

This script demonstrates a complete workflow:
1. Use REST API to discover actively trading perpetuals
2. Filter to top N by 24h volume
3. Stream real-time data for those perpetuals via WebSocket

This is a practical example for data collection.
"""

import asyncio
import websockets
import requests
import json
from datetime import datetime
from typing import List, Dict, Any


BASE_URL = "https://api.hyperliquid.xyz/info"
WS_URL = "wss://api.hyperliquid.xyz/ws"


def discover_top_perpetuals(top_n: int = 10) -> List[Dict[str, Any]]:
    """
    Discover the top N perpetuals by 24h volume using REST API.
    
    Args:
        top_n: Number of top perpetuals to return
        
    Returns:
        List of perpetual info dicts sorted by volume
    """
    print(f"\n{'='*80}")
    print(f"DISCOVERING TOP {top_n} PERPETUALS BY VOLUME")
    print(f"{'='*80}\n")
    
    # Get metadata and asset contexts
    response = requests.post(
        BASE_URL,
        headers={"Content-Type": "application/json"},
        json={"type": "metaAndAssetCtxs"}
    )
    response.raise_for_status()
    
    metadata, asset_contexts = response.json()
    
    # Build list of active perpetuals with their data
    active_perps = []
    for i, asset in enumerate(metadata["universe"]):
        # Skip delisted assets
        if asset.get("isDelisted"):
            continue
        
        if i < len(asset_contexts):
            ctx = asset_contexts[i]
            volume = float(ctx.get("dayNtlVlm", 0))
            
            # Only include perps with non-zero volume
            if volume > 0:
                active_perps.append({
                    "name": asset["name"],
                    "markPx": ctx["markPx"],
                    "midPx": ctx.get("midPx"),
                    "funding": ctx.get("funding"),
                    "openInterest": ctx.get("openInterest"),
                    "volume": volume,
                    "maxLeverage": asset["maxLeverage"],
                    "szDecimals": asset["szDecimals"],
                    "onlyIsolated": asset.get("onlyIsolated", False)
                })
    
    # Sort by volume descending
    active_perps.sort(key=lambda x: x["volume"], reverse=True)
    
    # Take top N
    top_perps = active_perps[:top_n]
    
    # Print summary
    print(f"Found {len(active_perps)} active perpetuals with volume")
    print(f"\nTop {top_n} by 24h volume:\n")
    print(f"{'Rank':<6} {'Symbol':<10} {'Mark Price':<15} {'24h Volume':<18} {'Funding %':<12} {'OI':<15}")
    print("-" * 90)
    
    for i, perp in enumerate(top_perps, 1):
        funding_pct = float(perp["funding"]) * 100 if perp["funding"] else 0
        symbol = perp["name"] + ("*" if perp["onlyIsolated"] else "")
        print(f"{i:<6} {symbol:<10} ${float(perp['markPx']):<14.4f} "
              f"${perp['volume']:<17,.0f} {funding_pct:<11.4f}% ${float(perp['openInterest']):<14,.0f}")
    
    return top_perps


class PerpDataCollector:
    """WebSocket client for collecting perpetual data."""
    
    def __init__(self, perps: List[Dict[str, Any]]):
        self.perps = perps
        self.coins = [p["name"] for p in perps]
        self.ws = None
        self.stats = {
            "trades": 0,
            "bbo_updates": 0,
            "funding_updates": 0,
            "messages": 0
        }
    
    async def connect(self):
        """Connect to WebSocket."""
        print(f"\n{'='*80}")
        print(f"CONNECTING TO WEBSOCKET")
        print(f"{'='*80}\n")
        self.ws = await websockets.connect(WS_URL)
        print("✓ Connected to WebSocket\n")
    
    async def subscribe_all(self):
        """Subscribe to all relevant feeds for the selected perpetuals."""
        print(f"Subscribing to feeds for {len(self.coins)} perpetuals...\n")
        
        # Subscribe to trades, BBO, and active asset context for each coin
        for coin in self.coins:
            # Trades - individual executions
            await self._subscribe({"type": "trades", "coin": coin})
            
            # BBO - best bid/offer
            await self._subscribe({"type": "bbo", "coin": coin})
            
            # Active asset context - funding, mark price, OI
            await self._subscribe({"type": "activeAssetCtx", "coin": coin})
        
        # Also subscribe to allMids for quick price overview
        await self._subscribe({"type": "allMids"})
        
        print(f"\n✓ Subscribed to {len(self.coins) * 3 + 1} feeds")
    
    async def _subscribe(self, subscription: Dict[str, Any]):
        """Send subscription message."""
        message = {
            "method": "subscribe",
            "subscription": subscription
        }
        await self.ws.send(json.dumps(message))
    
    async def collect(self, duration_seconds: int = 60):
        """
        Collect data for specified duration.
        
        Args:
            duration_seconds: How long to collect data
        """
        print(f"\n{'='*80}")
        print(f"COLLECTING DATA (for {duration_seconds} seconds)")
        print(f"{'='*80}\n")
        print("Press Ctrl+C to stop early\n")
        
        start_time = asyncio.get_event_loop().time()
        
        try:
            while asyncio.get_event_loop().time() - start_time < duration_seconds:
                try:
                    message = await asyncio.wait_for(self.ws.recv(), timeout=1.0)
                    data = json.loads(message)
                    self._process_message(data)
                    
                except asyncio.TimeoutError:
                    continue
                except websockets.exceptions.ConnectionClosed:
                    print("\n❌ WebSocket connection closed")
                    break
        
        except KeyboardInterrupt:
            print("\n\n⚠️  Stopped by user")
        
        self._print_summary()
    
    def _process_message(self, data: Dict[str, Any]):
        """Process incoming WebSocket message."""
        self.stats["messages"] += 1
        channel = data.get("channel", "unknown")
        
        if channel == "subscriptionResponse":
            return
        
        timestamp = datetime.now().strftime("%H:%M:%S.%f")[:-3]
        
        # Process based on channel type
        if channel == "trades":
            trades = data.get("data", [])
            self.stats["trades"] += len(trades)
            
            # Print trade information
            for trade in trades:
                side_icon = "🟢" if trade["side"] == "B" else "🔴"
                print(f"[{timestamp}] {side_icon} TRADE {trade['coin']:<6} "
                      f"{trade['sz']:>10} @ ${trade['px']:<10} "
                      f"(tid: {trade['tid']})")
        
        elif channel == "bbo":
            self.stats["bbo_updates"] += 1
            bbo_data = data.get("data", {})
            coin = bbo_data.get("coin")
            bbo = bbo_data.get("bbo", [None, None])
            
            if bbo[0] and bbo[1]:
                bid_px = float(bbo[0]["px"])
                ask_px = float(bbo[1]["px"])
                mid = (bid_px + ask_px) / 2
                spread_bps = ((ask_px - bid_px) / mid) * 10000
                
                print(f"[{timestamp}] 📊 BBO   {coin:<6} "
                      f"${bid_px:<10.4f} / ${ask_px:<10.4f} "
                      f"(spread: {spread_bps:.2f} bps)")
        
        elif channel == "activeAssetCtx":
            self.stats["funding_updates"] += 1
            asset_data = data.get("data", {})
            coin = asset_data.get("coin")
            ctx = asset_data.get("ctx", {})
            
            funding = float(ctx.get("funding", 0)) * 100
            oi = ctx.get("openInterest", "N/A")
            mark_px = ctx.get("markPx", "N/A")
            
            print(f"[{timestamp}] 💰 CTX   {coin:<6} "
                  f"Mark: ${mark_px:<10} OI: ${oi:<12} "
                  f"Funding: {funding:.4f}%")
        
        elif channel == "allMids":
            # Optionally process allMids (usually too verbose)
            # mids = data.get("data", {}).get("mids", {})
            pass
    
    def _print_summary(self):
        """Print collection summary."""
        print(f"\n{'='*80}")
        print("COLLECTION SUMMARY")
        print(f"{'='*80}")
        print(f"Total messages received: {self.stats['messages']}")
        print(f"Total trades: {self.stats['trades']}")
        print(f"BBO updates: {self.stats['bbo_updates']}")
        print(f"Funding/context updates: {self.stats['funding_updates']}")
        print(f"{'='*80}\n")
    
    async def close(self):
        """Close WebSocket connection."""
        if self.ws:
            await self.ws.close()
            print("🔌 WebSocket closed")


async def main(top_n: int = 5, duration: int = 60):
    """
    Main workflow.
    
    Args:
        top_n: Number of top perpetuals to stream
        duration: How long to collect data in seconds
    """
    try:
        # Step 1: Discover top perpetuals via REST API
        top_perps = discover_top_perpetuals(top_n)
        
        if not top_perps:
            print("\n❌ No active perpetuals found")
            return
        
        # Step 2: Stream data via WebSocket
        collector = PerpDataCollector(top_perps)
        await collector.connect()
        await collector.subscribe_all()
        await collector.collect(duration_seconds=duration)
        await collector.close()
        
        print("\n✅ Data collection complete")
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    import sys
    
    # Parse command line arguments
    top_n = int(sys.argv[1]) if len(sys.argv) > 1 else 5
    duration = int(sys.argv[2]) if len(sys.argv) > 2 else 60
    
    print("\n" + "="*80)
    print("HYPERLIQUID PERPETUALS DATA COLLECTOR")
    print("="*80)
    print(f"\nConfiguration:")
    print(f"  - Top N perpetuals: {top_n}")
    print(f"  - Collection duration: {duration} seconds")
    print(f"\nUsage: {sys.argv[0]} [top_n] [duration_seconds]")
    
    asyncio.run(main(top_n, duration))

