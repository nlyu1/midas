import polars as pl
import asyncio
from ..utils import printv
import functools
from pathlib import Path
from typing import Callable, Any


def cache_parquet(
    cache_path: str | Path, version: Any, verbose: bool = True
) -> Callable:
    """
    A decorator to cache the result of a function that returns a Polars DataFrame.

    The cache is stored as a Parquet file with LZ4 compression. Caching is
    invalidated based on the provided version.

    IMPORTANT: This cache is argument-agnostic. It only considers the function
    name and the `version`. If you change the arguments to the function in a way
    that should produce a different result, you MUST bump the `version` number
    to invalidate the old cache.

    Args:
        cache_path (str | Path): The directory where cache files will be stored.
        version (Any): The version of the cache. If this changes, the old
                       cache for the function call will be deleted and the
                       function will be re-run.
    """
    cache_dir = Path(cache_path)

    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> pl.DataFrame:
            # 1. Ensure the cache directory exists
            cache_dir.mkdir(parents=True, exist_ok=True)

            # 2. Define the file name based on the function name and version.
            # This cache does NOT depend on the function's arguments.
            func_name = func.__name__
            current_cache_filename = f"{func_name}_v{version}.parquet.lz4"
            current_cache_path = cache_dir / current_cache_filename

            # 3. Invalidate and delete old versions
            # This handles the "if version is bumped, deletes the previous result" rule.
            for old_file in cache_dir.glob(f"{func_name}_v*.parquet.lz4"):
                if old_file.name != current_cache_filename:
                    print(f"🧹 Deleting old cache version: {old_file.name}")
                    old_file.unlink()

            # 4. Check for cache hit or miss
            if current_cache_path.exists():
                # --- Cache Hit ---
                printv(
                    f"✅ Cache hit for '{func_name}'. Loading from '{current_cache_path}'.",
                    verbose,
                )
                return pl.read_parquet(current_cache_path)
            else:
                # --- Cache Miss ---
                printv(f"❌ Cache miss for '{func_name}'. Running function...", verbose)

                # Execute the actual function
                result_df = func(*args, **kwargs)

                if not isinstance(result_df, pl.DataFrame):
                    raise TypeError(
                        "The decorated function must return a Polars DataFrame."
                    )

                # Write the new cache file
                printv(f"📝 Writing new cache to '{current_cache_path}'...", verbose)
                result_df.write_parquet(current_cache_path, compression="lz4")
                return result_df

        return wrapper

    return decorator
