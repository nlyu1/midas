#!/usr/bin/env python3
"""
Hyperliquid Spot REST API Examples

This script demonstrates how to:
1. Retrieve spot trading pairs metadata
2. Retrieve spot asset contexts (market data)
3. Discover actively trading spot pairs by volume

Spot trading on Hyperliquid includes both canonical pairs (like PURR/USDC) 
and many programmatic pairs (labeled as @N) for HyperEVM tokens.
"""

import requests
import json
from typing import List, Dict, Any


BASE_URL = "https://api.hyperliquid.xyz/info"


def post_request(request_body: dict) -> dict:
    """Helper function to make POST requests to the Hyperliquid API."""
    headers = {"Content-Type": "application/json"}
    response = requests.post(BASE_URL, headers=headers, json=request_body)
    response.raise_for_status()
    return response.json()


def get_spot_metadata():
    """
    Retrieve spot trading pairs metadata.
    
    Returns metadata including:
    - tokens: list of all tokens with their properties
    - universe: list of all trading pairs
    """
    print("\n" + "="*80)
    print("1. RETRIEVING SPOT METADATA")
    print("="*80)
    
    request_body = {"type": "spotMeta"}
    result = post_request(request_body)
    
    # Print tokens summary
    tokens = result.get("tokens", [])
    canonical_tokens = [t for t in tokens if t.get("isCanonical")]
    
    print(f"\nTotal tokens: {len(tokens)}")
    print(f"Canonical tokens: {len(canonical_tokens)}")
    
    print("\nSample canonical tokens:")
    for token in canonical_tokens[:10]:
        print(f"  - {token['name']} (index {token['index']}): "
              f"szDecimals={token['szDecimals']}, weiDecimals={token['weiDecimals']}")
    
    # Print universe summary
    universe = result.get("universe", [])
    canonical_pairs = [p for p in universe if p.get("isCanonical")]
    
    print(f"\nTotal trading pairs: {len(universe)}")
    print(f"Canonical pairs: {len(canonical_pairs)}")
    
    print("\nCanonical spot pairs:")
    for pair in canonical_pairs:
        token_indices = pair["tokens"]
        token0 = tokens[token_indices[0]]["name"]
        token1 = tokens[token_indices[1]]["name"]
        print(f"  - {pair['name']} = {token0}/{token1}")
    
    return result


def get_spot_asset_contexts():
    """
    Retrieve spot asset contexts (metadata + real-time market data).
    
    Returns market data including:
    - markPx: mark price
    - midPx: mid price
    - dayNtlVlm: 24h notional volume
    - dayBaseVlm: 24h base volume
    - circulatingSupply: circulating supply
    - totalSupply: total supply
    """
    print("\n" + "="*80)
    print("2. RETRIEVING SPOT ASSET CONTEXTS")
    print("="*80)
    
    request_body = {"type": "spotMetaAndAssetCtxs"}
    result = post_request(request_body)
    
    # Result is a tuple: [metadata, asset_contexts]
    metadata = result[0]
    asset_contexts = result[1]
    
    tokens = metadata.get("tokens", [])
    universe = metadata.get("universe", [])
    
    print(f"\nTotal spot pairs: {len(asset_contexts)}")
    
    # Show sample data for first few pairs
    print("\n--- SAMPLE ASSET CONTEXTS (first 5) ---")
    for i, ctx in enumerate(asset_contexts[:5]):
        pair_name = ctx.get("coin", "Unknown")
        print(f"\n{pair_name}:")
        print(f"  Mark Price: ${ctx.get('markPx')}")
        print(f"  Mid Price: ${ctx.get('midPx')}")
        print(f"  24h Volume (notional): ${ctx.get('dayNtlVlm')}")
        print(f"  24h Volume (base): {ctx.get('dayBaseVlm')}")
        print(f"  Circulating Supply: {ctx.get('circulatingSupply')}")
        print(f"  Total Supply: {ctx.get('totalSupply')}")
        
        if ctx.get('prevDayPx') and ctx.get('prevDayPx') != '0.0':
            change_pct = ((float(ctx['markPx']) / float(ctx['prevDayPx']) - 1) * 100)
            print(f"  24h Change: {change_pct:.2f}%")
    
    return metadata, asset_contexts


def get_actively_trading_spots(min_volume: float = 10000.0, top_n: int = 20):
    """
    Discover actively trading spot pairs by volume.
    
    Args:
        min_volume: Minimum 24h volume in USDC to include
        top_n: Number of top pairs to return
    
    Returns:
        List of spot pairs with their market data, sorted by volume
    """
    print("\n" + "="*80)
    print(f"3. ACTIVELY TRADING SPOT PAIRS (min volume: ${min_volume:,.0f})")
    print("="*80)
    
    metadata, asset_contexts = get_spot_asset_contexts()
    tokens = metadata.get("tokens", [])
    
    # Build token index map for name lookup
    token_map = {t["index"]: t["name"] for t in tokens}
    
    # Filter pairs with volume above threshold
    active_spots = []
    for ctx in asset_contexts:
        volume = float(ctx.get("dayNtlVlm", 0))
        
        if volume >= min_volume:
            coin_name = ctx.get("coin", "")
            
            # Try to resolve actual token names for @N pairs
            display_name = coin_name
            if coin_name.startswith("@") and coin_name in metadata.get("universe", []):
                # Find the pair in universe
                for pair in metadata["universe"]:
                    if pair.get("name") == coin_name:
                        token_indices = pair.get("tokens", [])
                        if len(token_indices) == 2:
                            token0_name = token_map.get(token_indices[0], f"Token{token_indices[0]}")
                            token1_name = token_map.get(token_indices[1], f"Token{token_indices[1]}")
                            display_name = f"{token0_name}/{token1_name}"
                        break
            
            active_spots.append({
                "coin": coin_name,
                "display_name": display_name,
                "markPx": ctx.get("markPx"),
                "midPx": ctx.get("midPx"),
                "volume": volume,
                "dayBaseVlm": ctx.get("dayBaseVlm"),
                "circulatingSupply": ctx.get("circulatingSupply"),
            })
    
    # Sort by volume descending
    active_spots.sort(key=lambda x: x["volume"], reverse=True)
    
    # Take top N
    top_spots = active_spots[:top_n]
    
    print(f"\nFound {len(active_spots)} spot pairs with volume >= ${min_volume:,.0f}")
    print(f"\nTop {len(top_spots)} by 24h volume:\n")
    print(f"{'Rank':<6} {'Pair':<25} {'Mark Price':<15} {'24h Volume (USDC)':<20} {'Base Volume':<15}")
    print("-" * 95)
    
    for i, spot in enumerate(top_spots, 1):
        display = spot["display_name"] if spot["display_name"] != spot["coin"] else spot["coin"]
        print(f"{i:<6} {display:<25} ${float(spot['markPx']):<14.6f} "
              f"${spot['volume']:<19,.2f} {float(spot['dayBaseVlm']):<14,.2f}")
    
    return top_spots


def get_spot_token_details(token_names: List[str]):
    """
    Get detailed information about specific spot tokens.
    
    Args:
        token_names: List of token names to look up (e.g., ["HYPE", "PURR"])
    """
    print("\n" + "="*80)
    print(f"4. TOKEN DETAILS FOR: {', '.join(token_names)}")
    print("="*80)
    
    metadata, asset_contexts = get_spot_asset_contexts()
    tokens = metadata.get("tokens", [])
    universe = metadata.get("universe", [])
    
    # Find tokens by name
    for token_name in token_names:
        matching_tokens = [t for t in tokens if t["name"] == token_name]
        
        if not matching_tokens:
            print(f"\n❌ Token '{token_name}' not found")
            continue
        
        token = matching_tokens[0]
        print(f"\n{token['name']}:")
        print(f"  Index: {token['index']}")
        print(f"  Size Decimals: {token['szDecimals']}")
        print(f"  Wei Decimals: {token['weiDecimals']}")
        print(f"  Token ID: {token['tokenId']}")
        print(f"  Is Canonical: {token.get('isCanonical', False)}")
        
        if token.get("evmContract"):
            print(f"  EVM Contract: {token['evmContract']['address']}")
        
        # Find all trading pairs with this token
        token_index = token['index']
        pairs_with_token = []
        
        for i, pair in enumerate(universe):
            token_indices = pair.get("tokens", [])
            if token_index in token_indices:
                # Get the context for this pair
                if i < len(asset_contexts):
                    ctx = asset_contexts[i]
                    other_token_idx = token_indices[1] if token_indices[0] == token_index else token_indices[0]
                    other_token_name = next((t["name"] for t in tokens if t["index"] == other_token_idx), "Unknown")
                    
                    pairs_with_token.append({
                        "pair": pair["name"],
                        "other_token": other_token_name,
                        "volume": float(ctx.get("dayNtlVlm", 0)),
                        "markPx": ctx.get("markPx")
                    })
        
        if pairs_with_token:
            # Sort by volume
            pairs_with_token.sort(key=lambda x: x["volume"], reverse=True)
            print(f"\n  Trading pairs (top 5 by volume):")
            for pair_info in pairs_with_token[:5]:
                print(f"    - {token_name}/{pair_info['other_token']}: "
                      f"${pair_info['markPx']} (vol: ${pair_info['volume']:,.2f})")


def main():
    """Run all spot examples."""
    try:
        # 1. Get spot metadata
        metadata = get_spot_metadata()
        
        # 2. Get spot asset contexts (metadata + real-time data)
        # This is called by get_actively_trading_spots
        
        # 3. Get actively trading spot pairs
        top_spots = get_actively_trading_spots(min_volume=10000.0, top_n=15)
        
        # 4. Get details for specific tokens
        get_spot_token_details(["HYPE", "PURR", "USOL"])
        
        print("\n" + "="*80)
        print("SUMMARY")
        print("="*80)
        print(f"Total tokens: {len(metadata.get('tokens', []))}")
        print(f"Total trading pairs: {len(metadata.get('universe', []))}")
        print(f"Active pairs (>$10k volume): {len(top_spots)}")
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()

