# Argus

Argus is a central observational service for ingesting, recording, and publishing marketdata across a wide range of data sources

1. **Ingestion**: subscribes to each data source (exchange) and 
    - Saves to `ARGUS_DATA_PATH/{source}/{data_type}/{symbol}/{year}/{date}.pq


## Abstraction

Network layer -> agora -> processing layer (PTS response, storage, etc). Any TCP reconnection / hotswapping happens before the agora level, so agora subscribers are long-living. 


### Storage abstraction

This component handles agora -> hive-partitioned storage. Each update type (e.g. `Bbo`) has: 
- A dedicated type `T` (`BboEvent`) with `of_json` and `to_schema` types
- A stateful handler `StorageHandler<T>(flush_interval)` (flush_interval default to `1m`) which:
    - Consumes `T` from agora given path
    - After `flush_interval`, writes to temp path. 
    - **Put all temp files in a single temp folder**, and make it easy to manually delete stale temp files after swapping. 
    
`FlushHandler<T>`: service per data type (all symbols) which responds to the following. This abstraction is used to handle cases where we want to swap out the generation process without interrupting data persistence. 
- `flush(symbol, hash)`: merges temp file (symbol, time_chunk) from `symbol/random_hash` into latest date's parquet, creating new one if necessary. 
- `bump(symbol, hash)` and `bump_all(hash)`: start accepting new hash. 

### Network abstraction

### AgoraRouter

Used to implement hot swapping between two paths `path1` and `path2` to `path3` such that `path3` is persistent, even while `path1` and `path2` are not, so long as the union of `path1` and `path2` are persistent. 

- `AgoraRouter` with `new(to_path, initial_value)` and `relay(from_path)`. 

#### Persistent connections

This component handles json input -> agora. 

- `Worker(stream_names, agora_paths)`: one defintion per data source (Binance). Responsible for maintaining this (there's only one) combined websocket stream connection to Binance
    - Handles `ping` from Binance
    - `initialization`: creates a combined websocket stream to socket. For each individual stream:
        - Creates hive partition path for particular stream if path doesn't exist yet
        - Spawns a **temporary** agora publisher and **hot-swaps** this onto the given persistent agora paths (it's an `AgoraHotswapRouter` service). 
    - `del`: kills all temporary agora publishers. Callee (`Dispatcher`) is responsible for making sure that hot swap is completed before killing the worker. 

- `Dispatcher`: one definition per data source with specific logic. Using Binance as an example, `Dispatcher` dynamically manages a pool of `Workers` by creating and killing them:
    - Change events: universe change upon query, or websocket timeout (once every 24h per connection). 
    - Queries universe once every `interval=1h` for a `universe` list. 
        - Computes diff and dispatches to workers, creating new ones as necessary. 
        - For each distinct `stream_name`, spawn a **persistent** agora publisher (`AgoraHotswapRouter`). 
    - Makes sure that no connections are stale: Binance reconnects a connection every 24h. To solve this issue, we make sure that no worker is alive for more than e.g. 12h, though their tasks will keep overlapping. 
    - Kills the persistent agora publisher if pair is removed from universe