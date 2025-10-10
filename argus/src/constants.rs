pub const AGORA_METASERVER_DEFAULT_PORT: u16 = 8000; // Port for hosting own agora server
pub const AGORA_GATEWAY_PORT: u16 = 8001;
pub const ARGUS_DATA_PATH: &str = "/home/nlyu/Data/argus";
pub const BINANCE_SPOT_WEBSTREAM_ENDPOINT: &str = "wss://ws-api.binance.com:443/ws-api/v3"; // "wss://stream.binance.us:9443/ws";
pub const HYPERLIQUID_WEBSTREAM_ENDPOINT: &str = "wss://api.hyperliquid.xyz/ws";
pub const HYPERLIQUID_INFO_ENDPOINT: &str = "https://api.hyperliquid.xyz/info";
pub const WORKER_INIT_DELAY_MS: u64 = 500; // Wait for workers to initialize before relay operations. Used in crypto/hyperliquid/publisher.rs

// Constant endpoint is published to {..}/{perp | spot}/{data_type}/{date}/{symbol}
pub const HYPERLIQUID_AGORA_PREFIX: &str = "argus/hyperliquid";
// {ARGUS_DATA_PATH}/{..}/{perp | spot}
pub const HYPERLIQUID_DATA_SUFFIX: &str = "hyperliquid";

// Time between reclaiming
pub const RELAY_BATCH_SIZE: usize = 10;
pub const RELAY_BATCH_DELAY_MS: u64 = 100;
pub const HYPERLIQUID_ARCHIVER_FLUSH_INTERVAL_SECONDS: u64 = 10; 