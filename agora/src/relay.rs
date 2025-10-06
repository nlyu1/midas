use crate::utils::{ConnectionHandle, OrError};
use crate::{Agorable, Publisher, Subscriber};
use crate::{agora_error, agora_error_cause};
use futures_util::StreamExt;

pub struct Relay<T: Agorable> {
    stream_out: tokio::task::JoinHandle<()>,
    stream_in: Option<tokio::task::JoinHandle<()>>,
    src_subscriber: Option<Subscriber<T>>, // We need to keep ownership of this to prevent drop
    tx: tokio::sync::mpsc::UnboundedSender<T>,
    dest_path: String,
}

impl<T: Agorable> Relay<T> {
    /// Creates relay with fixed destination publisher and no source yet.
    /// Architecture: Two async tasks communicate via unbounded channel:
    /// - stream_in: Subscribes to source → sends values to channel
    /// - stream_out: Receives from channel → publishes to destination
    /// Error: Publisher creation fails → propagates to user code.
    /// Called by: User code
    pub async fn new(
        name: String,
        dest_path: String,
        initial_value: T,
        dest_metaserver_connection: ConnectionHandle,
        local_gateway_port: u16,
    ) -> OrError<Self> {
        // Create destination publisher
        let mut publisher = Publisher::new(
            name,
            dest_path.clone(),
            initial_value,
            dest_metaserver_connection,
            local_gateway_port,
        )
        .await?;

        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let dest_path_ = dest_path.clone();

        // Task: stream_out - receives from channel, publishes to destination
        let stream_out = tokio::task::spawn(async move {
            while let Some(t) = rx.recv().await {
                if let Err(e) = publisher.publish(t).await {
                    eprintln!("{}", agora_error_cause!("relay::Relay", "stream_out",
                        &format!("could not publish to {}", dest_path_), e));
                }
            }
        });

        Ok(Self {
            stream_out,
            stream_in: None,
            src_subscriber: None,
            tx,
            dest_path,
        })
    }

    /// Atomically switches source by aborting old stream_in task and spawning new subscriber.
    /// Enables seamless source switching for contiguous streams from discontinuous publishers.
    /// Error: Subscriber creation fails → returns to caller, relay keeps old source.
    /// Called by: User code
    pub async fn swapon(
        &mut self,
        src_path: String,
        src_metaserver_connection: ConnectionHandle,
    ) -> OrError<()> {
        // Create new subscriber to source
        let mut src_subscriber =
            Subscriber::<T>::new(src_path.clone(), src_metaserver_connection).await?;
        let (value, mut stream) = src_subscriber.get_stream().await?;

        // Abort old source task if exists
        if let Some(stream_in) = &self.stream_in {
            stream_in.abort();
        }

        let tx = self.tx.clone();
        let dest_path = self.dest_path.clone();

        // Task: stream_in - receives from source subscriber, sends to channel → stream_out
        let stream_in = tokio::task::spawn(async move {
            let _ = tx.send(value); // Send initial value
            while let Some(result) = stream.next().await {
                match result {
                    Ok(value) => {
                        if tx.send(value).is_err() {
                            // Channel closed - stream_out task died
                            eprintln!("{}", agora_error!("relay::Relay", "swapon",
                                &format!("publisher task died (relay: {} -> {})", src_path, dest_path)));
                            break;
                        }
                    }
                    Err(e) => {
                        // Stream error (deserialization, etc.) - logged, task exits
                        eprintln!("{}", agora_error_cause!("relay::Relay", "swapon",
                            &format!("stream error (relay: {} -> {})", src_path, dest_path), e));
                        break;
                    }
                }
            }
        });

        // Store new subscriber and task (keeps subscriber alive, prevents drop)
        self.src_subscriber = Some(src_subscriber);
        self.stream_in = Some(stream_in);
        Ok(())
    }
}

impl<T: Agorable> Drop for Relay<T> {
    fn drop(&mut self) {
        self.stream_out.abort();
        if let Some(stream_in) = &self.stream_in {
            stream_in.abort();
        }
    }
}
