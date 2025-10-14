# %%

import polars as pl
import asyncio
import datetime
import zipfile
from mnemosyne.crypto.binance.s3_helpers import get_all_keys, get_binance_usdt_symbols

from mnemosyne.utils import verify_directory
from mnemosyne.constants import BINANCE_DATA_PATH
from mnemosyne.multiprocessing import ParallelMap, chunk_list
import httpx
from datetime import date as Date
from pathlib import Path
import random


# ============================================
# Module-level helpers (single source of truth)
# ============================================


def get_csv_schema():
    """Returns the CSV schema for Binance trade data"""
    return {
        "columns": [
            "trade_id",
            "price",
            "quantity",
            "quote_quantity",
            "time",
            "is_buyer_maker",
            "is_best_match",
        ],
        "dtypes": {
            "trade_id": pl.Int64,
            "price": pl.Float64,
            "quantity": pl.Float64,
            "quote_quantity": pl.Float64,
            "time": pl.Int64,
            "is_buyer_maker": pl.Boolean,
            "is_best_match": pl.Boolean,
        },
    }


def convert_binance_timestamps(df):
    """
    Convert Binance timestamps handling format change (pre-2025: ms, post-2025: us).
    Returns dataframe with 'datetime' column instead of 'time'.
    """
    cutoff_ms = 1735689600000  # 2025-01-01 in milliseconds

    return df.with_columns(
        pl.when(pl.col("time") >= cutoff_ms)
        .then(pl.col("time"))  # Already in microseconds
        .otherwise(pl.col("time") * 1000)  # Convert ms to us
        .cast(pl.Datetime("us"))
        .alias("datetime")
    ).drop("time")


def build_download_url(data_base_url, binance_data_suffix, symbol, date):
    """Build the download URL for a specific symbol and date"""
    return f"{data_base_url}/{binance_data_suffix}/{symbol}USDT/{symbol}USDT-{binance_data_suffix}-{date}.zip"


def process_zip_to_parquet(zip_path, hive_path, symbol, date):
    """
    Synchronous function: unzip, read CSV, convert timestamps, write parquet, delete zip.
    This is the SINGLE implementation used by both class methods and multiprocessing workers.

    Returns:
        Number of rows processed
    """
    hive_path.parent.mkdir(parents=True, exist_ok=True)

    schema = get_csv_schema()

    with zipfile.ZipFile(zip_path, "r") as z:
        # Find the CSV file in the archive
        csv_file = next(f for f in z.namelist() if f.endswith(".csv"))

        # Read CSV with proper schema
        with z.open(csv_file) as csv_fp:
            df = pl.read_csv(
                csv_fp,
                has_header=False,
                new_columns=schema["columns"],
                schema_overrides=schema["dtypes"],
            )

        # Convert timestamps
        df = convert_binance_timestamps(df)

        # Add metadata columns
        df = df.with_columns(
            [pl.lit(symbol).alias("symbol"), pl.lit(date).alias("date")]
        )

        # Write parquet with optimized settings
        df.write_parquet(
            hive_path,
            compression="zstd",
            compression_level=3,
            statistics=True,
            use_pyarrow=True,
        )

    num_rows = len(df)
    print(symbol, date, num_rows)
    # Clean up: delete zip file after successful processing
    # zip_path.unlink()

    return num_rows


# ============================================
# Multiprocessing worker functions
# ============================================


def _worker_process_pairs(args):
    """
    Worker entry point for multiprocessing. Processes a chunk of (symbol, date) pairs.
    Each process runs in its own event loop with its own HTTP client.

    Args:
        args: Tuple of (pairs_chunk, config_dict)

    Returns:
        List of (symbol, date, status, error_msg) tuples
    """
    pairs_chunk, config = args
    return asyncio.run(_async_worker_chunk(pairs_chunk, config))


async def _async_worker_chunk(pairs_chunk, config):
    """
    Async worker that processes a chunk of pairs with independent HTTP client.
    Each process creates its own httpx client to avoid pickling/sharing issues.

    Args:
        pairs_chunk: List of (symbol, date) tuples
        config: Dict with paths and URLs

    Returns:
        List of (symbol, date, status, error_msg) tuples
    """
    hive_data_path = config["hive_data_path"]
    raw_data_path = config["raw_data_path"]
    data_base_url = config["data_base_url"]
    binance_data_suffix = config["binance_data_suffix"]

    results = []

    # Create fresh client for this process with connection limits
    limits = httpx.Limits(max_keepalive_connections=5, max_connections=10)
    timeout = httpx.Timeout(60.0, connect=10.0)
    async with httpx.AsyncClient(
        timeout=timeout, limits=limits, follow_redirects=True
    ) as client:
        for symbol, date in pairs_chunk:
            print("Processing:", symbol, date)
            try:
                # Build paths
                date_str = date.strftime("%Y-%m-%d")
                raw_path = (
                    raw_data_path
                    / f"{symbol}USDT"
                    / f"{symbol}USDT-{binance_data_suffix}-{date_str}.zip"
                )
                hive_path = (
                    hive_data_path
                    / f"date={date}"
                    / f"symbol={symbol}"
                    / "data.parquet"
                )

                # Skip if already exists
                if hive_path.is_file():
                    results.append((symbol, date, "skipped", None))
                    continue

                # Download if needed
                if True:  # not raw_path.is_file():
                    print(f"  {symbol} {date}: Downloading...")
                    raw_path.parent.mkdir(parents=True, exist_ok=True)
                    url = build_download_url(
                        data_base_url, binance_data_suffix, symbol, date
                    )
                    print("Downloading from", url)
                    response = await client.get(url)
                    print(
                        f"  {symbol} {date}: Download complete ({response.status_code})"
                    )

                    if response.status_code == 200:
                        raw_path.write_bytes(response.content)
                    else:
                        results.append(
                            (
                                symbol,
                                date,
                                "download_failed",
                                f"HTTP {response.status_code}",
                            )
                        )
                        continue
                else:
                    print(f"  {symbol} {date}: Using cached zip")

                # Process the file (reuses the same helper as class methods!)
                print(f"  {symbol} {date}: Processing zip...")
                await asyncio.to_thread(
                    process_zip_to_parquet, raw_path, hive_path, symbol, date
                )
                print(f"  {symbol} {date}: Done!")
                results.append((symbol, date, "success", None))

            except Exception as e:
                results.append((symbol, date, "error", str(e)[:100]))

    return results


class BinanceTradeBook:
    def __init__(
        self,
        hive_data_path: Path,
        raw_data_path: Path,
        data_base_url: str,  # Used to actually download data
        binance_data_suffix: str,  # e.g. trades, bookTicker
        prefix: str,  # Prefix is used to fetch universe of (symbol, date) pairs
        earliest_date: Date | None = None,
        latest_date: Date | None = None,
    ):
        # Will populate with .zip files upon downloads
        self.raw_data_path = raw_data_path
        # Will populate via /date={date}/symbol={symbol}/data.parquet style
        self.hive_data_path = hive_data_path

        self.universe_cache_path = hive_data_path / "universe.parquet"
        self.base_url = "https://s3-ap-northeast-1.amazonaws.com/data.binance.vision"
        self.data_base_url = data_base_url
        self.binance_data_suffix = binance_data_suffix
        self.prefix = f"{prefix}/{binance_data_suffix}"
        self.client = httpx.AsyncClient()

        verify_directory(self.raw_data_path, assert_empty=False, delete_contents=False)
        verify_directory(self.hive_data_path, assert_empty=False, delete_contents=False)
        self.earliest_date = earliest_date
        self.latest_date = latest_date

    async def initialize_universe(self, recheck_universe=False):
        if recheck_universe or not self.universe_cache_path.exists():
            symbols = await get_binance_usdt_symbols(self.base_url, prefix=self.prefix)
            self.universe = {}

            # Fetch keys for all symbols concurrently
            async def fetch_keys(symbol):
                paths = await get_all_keys(self.base_url, f"{self.prefix}/{symbol}USDT")
                paths = filter(lambda x: not x.endswith(".CHECKSUM"), paths)
                # Example: 'data/futures/um/daily/trades/BTCUSDT/BTCUSDT-trades-2019-09-08.zip' -> '2019-09-08
                date_strings = list(
                    map(lambda x: x.split("/")[-1].split(".")[0][-10:], paths)
                )
                return symbol, date_strings

            results = await asyncio.gather(*[fetch_keys(symbol) for symbol in symbols])
            universe_df = {"symbol": [], "date": []}
            for symbol, result in results:
                universe_df["symbol"].extend([symbol] * len(result))
                universe_df["date"].extend(result)
            universe_df = pl.DataFrame(universe_df).with_columns(
                pl.col("date").str.to_date("%Y-%m-%d")
            )
            universe_df.write_parquet(self.universe_cache_path)
        universe_df = pl.read_parquet(self.universe_cache_path)
        # Apply custom filters as necessary
        latest_date = (
            universe_df["date"].max() if self.latest_date is None else self.latest_date
        )
        earliest_date = (
            universe_df["date"].min()
            if self.earliest_date is None
            else self.earliest_date
        )
        self.universe_df: pl.DataFrame = universe_df.filter(
            (pl.col("date") >= earliest_date) & (pl.col("date") <= latest_date)
        )

    def get_universe_df(self) -> pl.DataFrame:
        return self.universe_df

    async def symbol_date_df(self, symbol: str, date: Date) -> pl.LazyFrame:
        if (
            len(
                self.universe_df.filter(
                    (pl.col("date") == date) & (pl.col("symbol") == symbol)
                )
            )
            == 0
        ):
            raise RuntimeError(f"Unable to fetch {symbol} on {date}")

        hive_path = self._hive_path(symbol, date)
        if hive_path.is_file():
            return pl.scan_parquet(hive_path)
        await self._raw_download(symbol, date)
        await self._process_raw_download(symbol, date)
        return pl.scan_parquet(hive_path)

    def _hive_path(self, symbol, date):
        return (
            self.hive_data_path / f"date={date}" / f"symbol={symbol}" / "data.parquet"
        )

    def _raw_path(self, symbol, date):
        date_str = date.strftime("%Y-%m-%d")
        return (
            self.raw_data_path
            / f"{symbol}USDT"
            / f"{symbol}USDT-{self.binance_data_suffix}-{date_str}.zip"
        )

    async def _process_raw_download(self, symbol, date) -> None:
        """
        Unzips the raw file, reads CSV data, converts timestamps, and writes to hive-style parquet.
        Uses thread pool for CPU-bound operations to avoid blocking event loop.
        """
        zip_path = self._raw_path(symbol, date)
        hive_path = self._hive_path(symbol, date)

        # Use the shared helper function
        num_rows = await asyncio.to_thread(
            process_zip_to_parquet, zip_path, hive_path, symbol, date
        )
        print(f"✓ Processed {symbol} on {date}: {num_rows:,} rows → {hive_path}")

    async def _raw_download(self, symbol, date) -> None:
        raw_path = self._raw_path(symbol, date)
        verify_directory(raw_path.parent)

        if raw_path.is_file():
            return

        try:
            url = build_download_url(
                self.data_base_url, self.binance_data_suffix, symbol, date
            )
            response = await self.client.get(url)

            if response.status_code == 200:
                raw_path.write_bytes(response.content)
                print(f"✓ Downloaded {symbol}, {date}")
            else:
                raise RuntimeError(f"HTTP {response.status_code}")
        except Exception as e:
            raise RuntimeError(f"Unable to download zip for {symbol}/{date}: {e}")

    def nohive_symbol_date_pairs(self) -> pl.DataFrame:
        """
        Returns (symbol, date) pairs that are in the universe but not yet on disk.
        Efficiently checks without loading all data into memory.
        """
        try:
            # Scan existing parquet files to get what's already on disk
            # This only reads metadata, not the actual trade data
            q = (
                pl.scan_parquet(
                    f"{self.hive_data_path}/**/data.parquet", hive_partitioning=True
                )
                .select(pl.col("date"), pl.col("symbol"))
                .unique()
            )
            onhive_pairs = q.collect()
        except Exception:
            # If no files exist yet, return entire universe
            return self.universe_df.select(["symbol", "date"])

        # Anti-join: get pairs in universe that are NOT on disk
        missing_pairs = self.universe_df.join(
            onhive_pairs,
            on=["symbol", "date"],
            how="anti",  # Keep only rows from left that don't match right
        ).select(["symbol", "date"])

        return missing_pairs

    def update_universe(self, num_workers=16, shuffle=True):
        """
        Download and process all missing (symbol, date) pairs using multiprocessing.

        Args:
            num_workers: Number of parallel processes to use
            shuffle: Whether to shuffle pairs before processing (good for load balancing)

        Returns:
            Dict with summary statistics
        """
        missing = self.nohive_symbol_date_pairs()
        pairs = [(row["symbol"], row["date"]) for row in missing.iter_rows(named=True)]

        if not pairs:
            print("✓ No missing pairs to download")
            return {"total": 0, "success": 0, "skipped": 0, "failed": 0}

        print(f"Found {len(pairs)} missing (symbol, date) pairs")
        if shuffle:
            random.shuffle(pairs)

        # Package config that can be pickled
        config = {
            "hive_data_path": self.hive_data_path,
            "raw_data_path": self.raw_data_path,
            "data_base_url": self.data_base_url,
            "binance_data_suffix": self.binance_data_suffix,
        }

        # Chunk pairs for workers
        chunk_size = len(pairs) // num_workers + 1
        chunks = chunk_list(pairs, chunk_size, assert_div=False)
        print(f"Starting download with {num_workers} workers, {len(chunks)} chunks...")
        args = [(chunk, config) for chunk in chunks]
        pmap = ParallelMap(max_workers=num_workers, pbar=True, use_thread=False)
        results = pmap(_worker_process_pairs, args)
        flat_results = [item for sublist in results for item in sublist]

        # Compute summary statistics
        stats = {
            "total": len(flat_results),
            "success": sum(
                1 for _, _, status, _ in flat_results if status == "success"
            ),
            "skipped": sum(
                1 for _, _, status, _ in flat_results if status == "skipped"
            ),
            "failed": sum(
                1
                for _, _, status, _ in flat_results
                if status not in ["success", "skipped"]
            ),
        }

        # Print errors if any
        errors = [
            (s, d, status, err)
            for s, d, status, err in flat_results
            if status not in ["success", "skipped"]
        ]
        if errors:
            print(f"\n⚠ {len(errors)} failures:")
            for symbol, date, status, error in errors[:10]:  # Show first 10
                print(f"  {symbol} {date}: {status} - {error}")
            if len(errors) > 10:
                print(f"  ... and {len(errors) - 10} more")

        print(
            f"\n✓ Completed: {stats['success']} successful, {stats['skipped']} skipped, {stats['failed']} failed"
        )

        return stats


# %%

raw_data_path = BINANCE_DATA_PATH / "raw" / "spot" / "usdt" / "last_trade"
hive_data_path = BINANCE_DATA_PATH / "spot" / "usdt" / "last_trade"
data_base_url = "https://data.binance.vision/data/spot/daily/trades"
prefix = "spot/daily"
binance_data_suffix = "trades"

# %%

# raw_data_path = BINANCE_DATA_PATH / "raw" / "futures" / "um" / "last_trade"
# hive_data_path = BINANCE_DATA_PATH / "futures" / "um" / "last_trade"
# data_base_url = "https://data.binance.vision/data/futures/um/daily"
# prefix = "futures/um/daily"
# binance_data_suffix = "trades"

Tb = BinanceTradeBook(
    hive_data_path,
    raw_data_path,
    data_base_url=data_base_url,
    binance_data_suffix=binance_data_suffix,
    prefix=prefix,
    earliest_date=datetime.date(2023, 1, 1),
)
await Tb.initialize_universe(recheck_universe=False)

test_date = Date(2025, 10, 2)

df = await Tb.symbol_date_df("SHIB", test_date)
df

# %%
Tb.get_universe_df()

# %%
Tb.update_universe()
# %%
pl.scan_parquet(hive_data_path, hive_partitioning=True).collect()

# %%
pl.scan_parquet(hive_data_path / "**/data.parquet", hive_partitioning=True).collect()

# %%
import polars as pl
import mnemosyne as ms
from pathlib import Path

hive_path = Path(ms.DatasetType.BinanceSpotTrades.hive_path("USDC"))
pl.read_parquet(hive_path / "hive_symbol_date_pairs.parquet")

# %%
list(path.glob("*.parquet"))
