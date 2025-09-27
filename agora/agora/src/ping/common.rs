use chrono::{DateTime, Utc};

#[tarpc::service]
pub trait PingRpc {
    /// Returns the latest value written to server in bytes and string. Also timestamp.
    async fn ping_latest_value() -> (Vec<u8>, String, DateTime<Utc>);
}
