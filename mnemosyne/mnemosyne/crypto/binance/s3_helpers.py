import re
import urllib.request
import xml.etree.ElementTree as ET

"""
binance_universe.py

This helper module provides functions for listing and parsing S3 bucket pages and defines a 
BinanceUniverse class for building a universe of Binance trade tickers with contiguous date ranges.

Helper functions:
    - s3_list_pages(base_url, prefix, delimiter="/")
    - get_all_keys(base_url, prefix)
    - get_all_trade_pairs(base_url, prefix)

BinanceUniverse class:
    - __init__(base_url, cache_path, refresh_cache): builds or reads a cache (CSV) of symbols with contiguous date ranges.
    - symbols(min_dates=0): returns a list of ticker symbols that have at least min_dates days of data.
    - symbol_dates(symbol): returns (start_date, end_date) for the given symbol.
"""


def s3_list_pages(base_url, prefix, delimiter="/"):
    """
    Generator that yields parsed XML pages from an S3 bucket listing,
    handling pagination using the marker.

    Args:
        base_url (str): S3 endpoint (e.g., "https://s3-ap-northeast-1.amazonaws.com/data.binance.vision")
        prefix (str): The prefix for filtering objects (e.g., "spot/daily/trades/BTCUSDT")
        delimiter (str): Delimiter used in the request (default is "/")

    Yields:
        tuple: (root, ns, fixed_prefix) where:
            - root: parsed XML root element.
            - ns: dict with namespace mapping (may be empty).
            - fixed_prefix: the adjusted prefix (ensuring it starts with "data/" and ends with "/")
    """
    # Ensure prefix has the proper format: starts with "data/" and ends with "/"
    if not prefix.startswith("data/"):
        prefix = "data/" + prefix
    if not prefix.endswith("/"):
        prefix = prefix + "/"

    url = f"{base_url}?prefix={prefix}&delimiter={delimiter}"
    marker = None

    while True:
        paged_url = url + (f"&marker={marker}" if marker else "")
        response = urllib.request.urlopen(paged_url)
        xml_content = response.read().decode("utf-8")
        root = ET.fromstring(xml_content)

        # Detect namespace if present
        ns = {}
        m = re.match(r"\{(.*)\}", root.tag)
        if m:
            ns["s3"] = m.group(1)

        yield root, ns, prefix

        # Check pagination: use <IsTruncated> and <NextMarker>
        if ns:
            is_truncated_elem = root.find(".//s3:IsTruncated", ns)
            next_marker_elem = root.find(".//s3:NextMarker", ns)
        else:
            is_truncated_elem = root.find(".//IsTruncated")
            next_marker_elem = root.find(".//NextMarker")

        if is_truncated_elem is not None and is_truncated_elem.text.lower() == "true":
            if next_marker_elem is not None and next_marker_elem.text:
                marker = next_marker_elem.text
            else:
                break  # No marker provided, exit loop
        else:
            break  # No more pages


def get_all_keys(
    base_url="https://s3-ap-northeast-1.amazonaws.com/data.binance.vision", prefix=""
):
    """
    Retrieve all object keys from an S3 bucket listing by handling pagination.

    Args:
        base_url (str): S3 endpoint.
        prefix (str): Prefix for filtering objects (e.g., "spot/daily/trades/BTCUSDT")

    Returns:
        list[str]: List of all object keys.
    """
    all_keys = []
    for root, ns, _ in s3_list_pages(base_url, prefix, delimiter="/"):
        if ns:
            keys = [elem.text for elem in root.findall(".//s3:Key", ns)]
        else:
            keys = [elem.text for elem in root.findall(".//Key")]
        all_keys.extend(keys)
    return all_keys


def get_all_trade_pairs(
    base_url="https://s3-ap-northeast-1.amazonaws.com/data.binance.vision",
    prefix="spot/daily/trades",
):
    """
    Retrieve all trade pairs from an S3 bucket listing. It assumes that trade pairs are represented
    as directory prefixes under the given prefix.

    Args:
        base_url (str): S3 endpoint.
        prefix (str): Prefix for filtering objects (e.g., "spot/daily/trades")

    Returns:
        list[str]: List of trade pair names (e.g., "BTCUSDT", "EOSUSDT").
    """
    trade_pairs = set()
    for root, ns, fixed_prefix in s3_list_pages(base_url, prefix, delimiter="/"):
        if ns:
            common_prefixes = root.findall(".//s3:CommonPrefixes", ns)
        else:
            common_prefixes = root.findall(".//CommonPrefixes")
        for cp in common_prefixes:
            prefix_elem = cp.find("s3:Prefix", ns) if ns else cp.find("Prefix")
            if prefix_elem is not None:
                full_prefix = (
                    prefix_elem.text
                )  # e.g., "data/spot/daily/trades/EOSUSDT/"
                trade_pair = full_prefix.replace(fixed_prefix + "/", "").strip("/")
                if trade_pair:
                    trade_pairs.add(trade_pair.split("/")[-1])
    return list(trade_pairs)
