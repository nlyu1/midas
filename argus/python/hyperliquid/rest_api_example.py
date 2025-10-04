#!/usr/bin/env python3
"""
Hyperliquid REST API Examples

This script demonstrates how to:
1. Retrieve all perpetual dexs
2. Retrieve perpetuals metadata (universe and margin tables)
3. Retrieve asset contexts (mark price, funding, open interest, etc.)

These endpoints help you discover what perpetuals are available for trading.
"""

import requests
import json


BASE_URL = "https://api.hyperliquid.xyz/info"


def post_request(request_body: dict) -> dict:
    """Helper function to make POST requests to the Hyperliquid API."""
    headers = {"Content-Type": "application/json"}
    response = requests.post(BASE_URL, headers=headers, json=request_body)
    response.raise_for_status()
    return response.json()


def get_all_perp_dexs():
    """
    Retrieve all perpetual dexs.
    
    Returns a list of perp dexs. The first element (null) represents the default/main dex.
    """
    print("\n" + "="*80)
    print("1. RETRIEVING ALL PERPETUAL DEXS")
    print("="*80)
    
    request_body = {
        "type": "perpDexs"
    }
    
    result = post_request(request_body)
    print(json.dumps(result, indent=2))
    return result


def get_perpetuals_metadata(dex_name: str = ""):
    """
    Retrieve perpetuals metadata (universe and margin tables).
    
    Args:
        dex_name: Perp dex name. Empty string (default) represents the first/main perp dex.
    
    Returns metadata including:
    - universe: list of all available perpetual assets with their properties
    - marginTables: margin tier configurations
    """
    print("\n" + "="*80)
    print(f"2. RETRIEVING PERPETUALS METADATA (dex='{dex_name}')")
    print("="*80)
    
    request_body = {
        "type": "meta"
    }
    
    # Only add dex if it's not the default
    if dex_name:
        request_body["dex"] = dex_name
    
    result = post_request(request_body)
    print(json.dumps(result, indent=2))
    
    # Print a summary of available coins
    print("\n--- AVAILABLE PERPETUALS ---")
    for asset in result.get("universe", []):
        status = " (DELISTED)" if asset.get("isDelisted") else ""
        isolated = " (ISOLATED ONLY)" if asset.get("onlyIsolated") else ""
        print(f"  - {asset['name']}: "
              f"max leverage {asset['maxLeverage']}x, "
              f"size decimals {asset['szDecimals']}"
              f"{status}{isolated}")
    
    return result


def get_asset_contexts():
    """
    Retrieve perpetuals asset contexts.
    
    This combines metadata with real-time market data including:
    - markPx: mark price
    - funding: current funding rate
    - openInterest: current open interest
    - dayNtlVlm: 24h notional volume
    - midPx: mid price
    - oraclePx: oracle price
    - premium: funding premium
    - prevDayPx: previous day price
    """
    print("\n" + "="*80)
    print("3. RETRIEVING ASSET CONTEXTS (metadata + real-time market data)")
    print("="*80)
    
    request_body = {
        "type": "metaAndAssetCtxs"
    }
    
    result = post_request(request_body)
    
    # Result is a tuple: [metadata, asset_contexts]
    metadata = result[0]
    asset_contexts = result[1]
    
    print("\n--- METADATA ---")
    print(json.dumps(metadata, indent=2)[:500] + "...")  # Truncate for readability
    
    print("\n--- ASSET CONTEXTS (sample of first 3) ---")
    for i, ctx in enumerate(asset_contexts[:3]):
        coin_name = metadata["universe"][i]["name"]
        print(f"\n{coin_name}:")
        print(f"  Mark Price: ${ctx['markPx']}")
        print(f"  Mid Price: ${ctx.get('midPx', 'N/A')}")
        print(f"  Oracle Price: ${ctx.get('oraclePx', 'N/A')}")
        print(f"  Funding Rate: {ctx.get('funding', 'N/A')}")
        print(f"  Open Interest: ${ctx.get('openInterest', 'N/A')}")
        print(f"  24h Volume: ${ctx.get('dayNtlVlm', 'N/A')}")
        print(f"  24h Change: {((float(ctx['markPx']) / float(ctx['prevDayPx']) - 1) * 100):.2f}%")
    
    print("\n--- FULL ASSET CONTEXTS (all assets) ---")
    print(json.dumps(asset_contexts, indent=2))
    
    return metadata, asset_contexts


def get_actively_trading_perps():
    """
    Determine which perpetuals are actively trading.
    
    This combines metadata with asset contexts to filter out delisted perps
    and show which ones have active markets.
    """
    print("\n" + "="*80)
    print("4. ACTIVELY TRADING PERPETUALS (filtered view)")
    print("="*80)
    
    metadata, asset_contexts = get_asset_contexts()
    
    print("\n--- ACTIVELY TRADING PERPS ---")
    active_perps = []
    
    for i, asset in enumerate(metadata["universe"]):
        # Skip delisted assets
        if asset.get("isDelisted"):
            continue
        
        if i < len(asset_contexts):
            ctx = asset_contexts[i]
            active_perps.append({
                "name": asset["name"],
                "markPx": ctx["markPx"],
                "openInterest": ctx.get("openInterest", "N/A"),
                "dayNtlVlm": ctx.get("dayNtlVlm", "N/A"),
                "maxLeverage": asset["maxLeverage"],
                "onlyIsolated": asset.get("onlyIsolated", False)
            })
    
    # Sort by volume
    active_perps.sort(key=lambda x: float(x["dayNtlVlm"]) if x["dayNtlVlm"] != "N/A" else 0, reverse=True)
    
    print(f"\nTotal active perpetuals: {len(active_perps)}\n")
    print(f"{'Symbol':<10} {'Mark Price':<15} {'Open Interest':<15} {'24h Volume':<18} {'Max Lev':<8}")
    print("-" * 80)
    
    for perp in active_perps[:20]:  # Show top 20 by volume
        symbol = perp["name"] + ("*" if perp["onlyIsolated"] else "")
        print(f"{symbol:<10} ${float(perp['markPx']):<14.4f} "
              f"${perp['openInterest']:<14} ${perp['dayNtlVlm']:<17} {perp['maxLeverage']}x")
    
    print("\n* = Isolated margin only")
    
    return active_perps


def main():
    """Run all examples."""
    try:
        # 1. Get all perp dexs
        dexs = get_all_perp_dexs()
        
        # 2. Get perpetuals metadata for the main dex
        metadata = get_perpetuals_metadata()
        
        # 3. Get asset contexts (metadata + real-time data)
        # Uncomment the line below if you want to run it separately:
        # metadata, asset_contexts = get_asset_contexts()
        
        # 4. Get filtered view of actively trading perps
        active_perps = get_actively_trading_perps()
        
        print("\n" + "="*80)
        print("SUMMARY")
        print("="*80)
        print(f"Total perpetual dexs: {len([d for d in dexs if d is not None])}")
        print(f"Total assets in universe: {len(metadata['universe'])}")
        print(f"Actively trading perps: {len(active_perps)}")
        
    except Exception as e:
        print(f"\nError: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()

