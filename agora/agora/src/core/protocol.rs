use crate::utils::OrError;
use tokio::sync::broadcast;

pub const DEFAULT_PORT: u16 = 8081;

#[tarpc::service]
pub trait RawStreamer {
    async fn subscribe() -> OrError<broadcast::Receiver<Vec<u8>>>;
}

pub trait Agorable: Send + Sync + 'static + Serialize + Deserialize<'de> {}
