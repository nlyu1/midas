import re
import xml.etree.ElementTree as ET
import httpx


async def s3_list_pages(base_url, prefix, delimiter="/"):
    """
    Async generator that yields parsed XML pages from an S3 bucket listing,
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
    print("Fetching s3 list pages from:", url)

    async with httpx.AsyncClient() as client:
        while True:
            paged_url = url + (f"&marker={marker}" if marker else "")
            response = await client.get(paged_url)
            xml_content = response.text
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

            if (
                is_truncated_elem is not None
                and is_truncated_elem.text.lower() == "true"
            ):
                if next_marker_elem is not None and next_marker_elem.text:
                    marker = next_marker_elem.text
                else:
                    break  # No marker provided, exit loop
            else:
                break  # No more pages


async def get_all_keys(
    base_url="https://s3-ap-northeast-1.amazonaws.com/data.binance.vision", prefix=""
):
    """
    Retrieve all object keys from an S3 bucket listing by handling pagination (async).

    Args:
        base_url (str): S3 endpoint.
        prefix (str): Prefix for filtering objects (e.g., "spot/daily/trades/BTCUSDT")

    Returns:
        list[str]: List of all object keys.
    """
    all_keys = []
    async for root, ns, _ in s3_list_pages(base_url, prefix, delimiter="/"):
        if ns:
            keys = [elem.text for elem in root.findall(".//s3:Key", ns)]
        else:
            keys = [elem.text for elem in root.findall(".//Key")]
        all_keys.extend(keys)
    return all_keys


async def get_all_trade_pairs(
    base_url="https://s3-ap-northeast-1.amazonaws.com/data.binance.vision",
    prefix="spot/daily/trades",
):
    """
    Retrieve all trade pairs from an S3 bucket listing (async). It assumes that trade pairs are represented
    as directory prefixes under the given prefix.

    Args:
        base_url (str): S3 endpoint.
        prefix (str): Prefix for filtering objects (e.g., "spot/daily/trades")

    Returns:
        list[str]: List of trade pair names (e.g., "BTCUSDT", "EOSUSDT").
    """
    trade_pairs = set()
    async for root, ns, fixed_prefix in s3_list_pages(base_url, prefix, delimiter="/"):
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


# ----------------------
# Helper functions for symbol file retrieval
# (These use the helper functions above.)
# ----------------------
async def get_binance_usdt_symbols(
    base_url="https://s3-ap-northeast-1.amazonaws.com/data.binance.vision",
    prefix="spot/daily/trades",
):
    """
    Returns a list of universe symbols (tickers) by processing the S3 trade pairs (async).
    (Strips 'USDT' from trade pairs.)
    """
    trade_pairs = await get_all_trade_pairs(base_url, prefix)
    # Filter for trade pairs that end with 'USDT' and remove 'USDT'
    return list(
        map(
            lambda x: x.replace("USDT", ""),
            filter(lambda x: x.endswith("USDT"), trade_pairs),
        )
    )
