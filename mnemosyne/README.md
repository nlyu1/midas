# Mnemosyne

## Lossless Data Collection

### Binance

Automated collection of Binance spot and futures trade data from public S3 buckets.

#### Data Types

- **Spot last_trade**: Daily trade-by-trade data from spot markets
- **UM Futures last_trade**: USDT-margined perpetual futures trades

#### Directory Structure

**Hive (processed parquets)**:
- Spot: `/data/mnemosyne/binance/lossless/spot/last_trade/peg_symbol=USDT/`
- Futures: `/data/mnemosyne/binance/lossless/futures/um/last_trade/peg_symbol=USDT/`
- Format: `date={YYYY-MM-DD}/symbol={SYMBOL}/data.parquet`
- Caches: `universe.parquet`, `hive_symbol_date_pairs.parquet` (in hive root)

**Raw (temporary zips)**:
- Spot: `/data/mnemosyne/binance/raw/spot/last_trade/peg_symbol=USDT/`
- Futures: `/data/mnemosyne/binance/raw/futures/um/last_trade/peg_symbol=USDT/`
- Format: `{SYMBOL}USDT/{SYMBOL}USDT-trades-{YYYY-MM-DD}.zip`

#### S3 Sources

**Listing API** (discover symbols/dates):
- Base: `https://s3-ap-northeast-1.amazonaws.com/data.binance.vision`
- Query: `?prefix=data/spot/daily/trades/BTCUSDT&delimiter=/`

**Download CDN** (fetch files):
- Spot: `https://data.binance.vision/data/spot/daily/trades/BTCUSDT/BTCUSDT-trades-2025-10-05.zip`
- Futures: `https://data.binance.vision/data/futures/um/daily/trades/BTCUSDT/BTCUSDT-trades-2025-10-05.zip`

#### Initialization & Feature Flags

| Flag | Default | Behavior |
|------|---------|----------|
| `--peg-symbol` | `USDT` | Currency filter (`USDT` or `USDC`) - determines which symbols to fetch |
| `--earliest-date` | `2022-01-01` | Universe lower bound - only fetch data from this date forward |
| `--recompute-universe` | `false` | Force S3 refresh - **expensive**: queries all symbols & dates from S3 |
| `--recompute-onhive` | `false` | Revalidate all local parquets - **expensive**: re-scans entire filesystem |
| `--parallelism` | `32` | Download worker threads - higher = faster but more network load |
| `--yes` | `false` | Skip confirmation prompt before downloading |

**Cache behavior**:
- **Universe**: Cached to avoid repeated S3 queries (100k+ API calls for full refresh)
- **Hive**: Incremental validation (only checks new files not in cache)
- **Corrupted files**: Auto-deleted during validation

**Example usage**:
```bash
# Spot trades (USDT pairs from 2022-01-01, 32 parallel workers)
cargo run --bin binance-spot-trades -- --peg-symbol USDT --earliest-date 2022-01-01

# Futures trades (force universe refresh, 64 workers)
cargo run --bin binance-futures-trades -- --recompute-universe --parallelism 64

# Quick test (single symbol-date, skip confirmation)
cargo run --bin binance-spot-trades -- --test-symbol BTC --test-date 2025-10-05 --yes
```