/// Central port configuration for all Agora services
///
/// This module provides a single source of truth for all default port assignments
/// across the Agora ecosystem, preventing port conflicts and ensuring consistency.

/// Default port for the Agora Meta Server
pub const METASERVER_PORT: u16 = 8080;
pub const GATEWAY_PORT: u16 = 8081;

/// Metaserver pings publisher once this interval. Non-responding publishers are removed
pub const CHECK_PUBLISHER_LIVELINESS_EVERY_MS: u64 = 500;
