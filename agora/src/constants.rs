/// Central port configuration for all Agora services

/// Default port for the Agora Metaserver
pub const METASERVER_PORT: u16 = 8080;

/// Default port for the Agora Gateway.
pub const GATEWAY_PORT: u16 = 8081;

/// Metaserver pings publisher once this interval. Non-responding publishers are removed
pub const CHECK_PUBLISHER_LIVELINESS_EVERY_MS: u64 = 500;
