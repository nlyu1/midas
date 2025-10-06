//! Ping protocol module for publisher health checks and current value queries.
//! Provides `PingClient` (via gateway) and `PingServer` (UDS) for synchronous request-response communication with publishers.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PingResponse {
    pub vec_payload: Vec<u8>,
    pub str_payload: String,
    pub timestamp: DateTime<Utc>,
}

mod client;
mod server;

pub use client::PingClient;
pub use server::PingServer;
