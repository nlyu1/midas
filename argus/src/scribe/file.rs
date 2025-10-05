use crate::types::TradingSymbol;

use super::ArgusParquetable;
use agora::metaserver::AgoraClient;
use agora::utils::{OrError, TreeTrait};
use agora::{Agorable, Subscriber};
use chrono::TimeDelta;
use futures_util::StreamExt;
use std::net::Ipv6Addr;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::task::JoinHandle;

pub struct SinglePathScribe<T: Agorable + ArgusParquetable + Clone + Send + Sync> {
    data: Arc<Mutex<Vec<T>>>,
    flush_path: PathBuf,
    collection_handle: JoinHandle<()>,
    flush_handle: JoinHandle<()>,
}

impl<T: Agorable + ArgusParquetable + Clone + Send + Sync + 'static> SinglePathScribe<T> {
    pub async fn new(
        agora_path: String,
        flush_duration: Duration,
        flush_path: &Path,
        metaserver_addr: Ipv6Addr,
        metaserver_port: u16,
    ) -> OrError<Self> {
        let data = Arc::new(Mutex::new(Vec::new()));
        let data_clone = Arc::clone(&data);
        let flush_path_buf = flush_path.to_path_buf();

        // Data collection task: continuously collect data from subscriber
        let collection_handle = tokio::spawn(async move {
            let mut subscriber = Subscriber::<T>::new(agora_path, metaserver_addr, metaserver_port)
                .await
                .unwrap();
            let (current_value, mut stream) = subscriber.get_stream().await.unwrap();

            data_clone.lock().unwrap().push(current_value);

            while let Some(result) = stream.next().await {
                if let Ok(message) = result {
                    data_clone.lock().unwrap().push(message);
                }
            }
        });

        // Flush task: periodically flush data to disk
        let data_clone = Arc::clone(&data);
        let flush_path_clone = flush_path_buf.clone();
        let flush_handle = tokio::spawn(async move {
            let mut interval = tokio::time::interval(flush_duration);
            interval.tick().await; // Skip immediate first tick

            loop {
                interval.tick().await;

                if let Err(e) = Self::flush(Arc::clone(&data_clone), &flush_path_clone).await {
                    eprintln!("Flush error for {:?}: {}", flush_path_clone, e);
                }
            }
        });

        Ok(Self {
            data,
            flush_path: flush_path_buf,
            collection_handle,
            flush_handle,
        })
    }

    /// Atomically flushes accumulated data to disk using blocking I/O
    async fn flush(data: Arc<Mutex<Vec<T>>>, path: &Path) -> OrError<()> {
        // Atomically swap data with empty vector (no race condition)
        let data_snapshot = {
            let mut data_guard = data.lock().unwrap();
            std::mem::take(&mut *data_guard)
        };

        let record_count = data_snapshot.len();
        if record_count == 0 {
            return Ok(()); // Nothing to flush
        }

        let path_clone = path.to_path_buf();

        // Use spawn_blocking to avoid blocking tokio runtime
        tokio::task::spawn_blocking(move || {
            ArgusParquetable::write_to_parquet(data_snapshot, &path_clone)
        })
        .await
        .map_err(|e| format!("Flush task join error: {}", e))??;

        println!(
            "Argus filescribe: flushed {} records to {:?}",
            record_count, path
        );
        Ok(())
    }
}

impl<T: Agorable + ArgusParquetable + Clone + Send + Sync + 'static> SinglePathScribe<T> {
    pub async fn shutdown(self) -> OrError<()> {
        Self::flush(Arc::clone(&self.data), &self.flush_path).await?;
        self.collection_handle.abort();
        self.flush_handle.abort();
        Ok(())
    }
}

impl<T: Agorable + ArgusParquetable + Clone + Send + Sync> Drop for SinglePathScribe<T> {
    fn drop(&mut self) {
        self.collection_handle.abort();
        self.flush_handle.abort();
    }
}

/// Manages multiple SinglePathScribe instances for an agora directory prefix
/// Looks under agora_prefix/{symbol} and starts one SinglePathScribe for each symbol.
/// Caller needs to ensure that agora values under path are valid of type T, and that children are registered under name=symbol.
pub struct AgoraDirScribe<T: Agorable + ArgusParquetable + Clone + Send + Sync> {
    scribes: Vec<SinglePathScribe<T>>,
}

impl<T: Agorable + ArgusParquetable + Clone + Send + Sync + 'static> AgoraDirScribe<T> {
    pub async fn new(
        metaserver_addr: Ipv6Addr,
        metaserver_port: u16,
        agora_prefix: String,
        flush_interval: TimeDelta,
        output_dir: PathBuf,
    ) -> OrError<Self> {
        let metaclient = AgoraClient::new(metaserver_addr, metaserver_port)
            .await
            .map_err(|e| {
                format!(
                    "Argus AgoraDirScribe error: cannot create agora metaclient. {}",
                    e
                )
            })?;
        let pathtree = metaclient.get_path_tree().await?;
        let children = pathtree
            .get_child(&agora_prefix)
            .map_err(|e| {
                format!(
                    "Argus AgoraDirScribe error: cannot identify children of {}: {}. Filetree: \n{}",
                    agora_prefix,
                    e,
                    pathtree.display_tree(),
                )
            })?
            .children();

        let symbols: Result<Vec<TradingSymbol>, String> = children
            .into_iter()
            .map(|node| TradingSymbol::from_str(node.name()))
            .collect();

        let symbols = symbols?;
        println!("Found {} symbols: {:?}", symbols.len(), symbols);

        let flush_duration = flush_interval
            .to_std()
            .map_err(|e| format!("Invalid flush interval: {}", e))?;

        // Start one SinglePathScribe for each symbol
        let mut scribes = Vec::new();
        for symbol in symbols {
            let symbol_str = symbol.to_string();
            let agora_path = format!("{}/{}", agora_prefix, symbol_str);
            let flush_path = output_dir.join(format!("{}.parquet", symbol_str));

            let scribe = SinglePathScribe::<T>::new(
                agora_path.clone(),
                flush_duration,
                &flush_path,
                metaserver_addr,
                metaserver_port,
            )
            .await
            .map_err(|e| format!("Failed to start scribe for {}: {}", agora_path, e))?;

            println!("Started scribe for {} -> {:?}", agora_path, flush_path);
            scribes.push(scribe);
        }

        Ok(Self { scribes })
    }

    /// Gracefully shutdown all scribes by flushing remaining data
    pub async fn shutdown(mut self) -> OrError<()> {
        let scribes = std::mem::take(&mut self.scribes);
        for scribe in scribes {
            if let Err(e) = scribe.shutdown().await {
                eprintln!("Error shutting down scribe: {}", e);
            }
        }
        Ok(())
    }

    /// Get the number of scribes running
    pub fn count(&self) -> usize {
        self.scribes.len()
    }
}

impl<T: Agorable + ArgusParquetable + Clone + Send + Sync> Drop for AgoraDirScribe<T> {
    fn drop(&mut self) {
        // Note: Individual scribes will be dropped and their Drop impls will handle cleanup
        // Use shutdown() for graceful termination with final flush
        eprintln!(
            "Warning: AgoraDirScribe dropped without calling shutdown(). {} scribe(s) may have unflushed data.",
            self.scribes.len()
        );
    }
}
