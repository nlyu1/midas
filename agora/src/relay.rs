use crate::utils::OrError;
use crate::{Agorable, Publisher, Subscriber};
use futures_util::StreamExt;
use std::net::Ipv6Addr;

pub struct Relay<T: Agorable> {
    stream_out: tokio::task::JoinHandle<()>,
    stream_in: Option<tokio::task::JoinHandle<()>>,
    src_subscriber: Option<Subscriber<T>>, // We need to keep ownership of this to prevent drop
    tx: tokio::sync::mpsc::UnboundedSender<T>,
    dest_path: String,
}

impl<T: Agorable> Relay<T> {
    pub async fn new(
        name: String,
        dest_path: String,
        initial_value: T,
        metaserver_addr: Ipv6Addr,
        metaserver_port: u16,
    ) -> OrError<Self> {
        let mut publisher = Publisher::new(
            name,
            dest_path.clone(),
            initial_value,
            metaserver_addr,
            metaserver_port,
        )
        .await?;

        // Should this be async move or normal thread?
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let dest_path_ = dest_path.clone();
        // This should spawn immediately, right?
        let stream_out = tokio::task::spawn(async move {
            while let Some(t) = rx.recv().await {
                if let Err(e) = publisher.publish(t).await {
                    eprintln!(
                        "Agora Relay ({}->?) error: could not publish {}",
                        dest_path_, e
                    );
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

    // Immediately spawns background process to
    pub async fn swapon(
        &mut self,
        src_path: String,
        metaserver_addr: Ipv6Addr,
        metaserver_port: u16,
    ) -> OrError<()> {
        let mut src_subscriber =
            Subscriber::<T>::new(src_path.clone(), metaserver_addr, metaserver_port).await?;
        let (value, mut stream) = src_subscriber.get_stream().await?;
        if let Some(stream_in) = &self.stream_in {
            stream_in.abort();
        }
        let tx = self.tx.clone();
        let dest_path = self.dest_path.clone();
        let stream_in = tokio::task::spawn(async move {
            let _ = tx.send(value);
            while let Some(result) = stream.next().await {
                match result {
                    Ok(value) => {
                        if tx.send(value).is_err() {
                            eprintln!(
                                "Agora Relay ({} -> {}) error: publisher task died",
                                src_path, dest_path
                            );
                            break;
                        }
                    }
                    Err(_) => {
                        eprintln!(
                            "Agora Relay ({} -> {}) error: stream error",
                            src_path, dest_path
                        );
                        break;
                    }
                }
            }
        });
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
