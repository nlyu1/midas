//! Streaming WebSocket module for continuous publisher data transmission.
//! Provides `RawStreamClient` (auto-reconnecting, broadcast fanout) and `RawStreamServer` (UDS, 1-to-N broadcasting) for real-time message streams.

mod client;
mod server;

pub use client::RawStreamClient;
pub use server::RawStreamServer;
