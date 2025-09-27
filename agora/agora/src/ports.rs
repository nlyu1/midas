/// Central port configuration for all Agora services
///
/// This module provides a single source of truth for all default port assignments
/// across the Agora ecosystem, preventing port conflicts and ensuring consistency.

/// Default port for the Agora Meta Server
pub const METASERVER_DEFAULT_PORT: u16 = 8080;

/// Default port for Ping Server
pub const PING_DEFAULT_PORT: u16 = 8083;

/// Default port for publisher service connections (bytes -> custom types)
pub const PUBLISHER_SERVICE_PORT: u16 = 8081;

/// Default port for publisher string connections (string for omni connection)
pub const PUBLISHER_OMNISTRING_PORT: u16 = 8082;

/// Default port for publisher pinging connections
pub const PUBLISHER_PING_PORT: u16 = 8083;

/// Port range reserved for Agora services
pub const AGORA_PORT_RANGE: std::ops::RangeInclusive<u16> = 8080..=8099;

/// Validates that a port is within the Agora reserved range
pub fn is_agora_port(port: u16) -> bool {
    AGORA_PORT_RANGE.contains(&port)
}
