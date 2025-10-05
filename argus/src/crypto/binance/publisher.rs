use super::{BboUpdate, BinanceStreamable, OrderbookDiffUpdate, TradeUpdate};
use crate::constants::BINANCE_SPOT_WEBSTREAM_ENDPOINT;
use crate::types::TradingSymbol;
use agora::utils::OrError;
use agora::{AgorableOption, Publisher};
use futures_util::{SinkExt, StreamExt};
// use rand::Rng;
use std::collections::HashMap;
use std::marker::PhantomData;
use std::net::Ipv6Addr;
use tokio::task::JoinHandle;
use tokio_tungstenite::{connect_async, tungstenite::Message};

pub struct BinanceWebstreamWorker<T: BinanceStreamable> {
    agora_paths: Vec<String>,
    dispatch_handle: JoinHandle<()>,
    _phantom: PhantomData<T>,
}

impl<T: BinanceStreamable> BinanceWebstreamWorker<T> {
    pub async fn new(
        metaserver_addr: Ipv6Addr,
        metaserver_port: u16,
        agora_prefix: String,
        symbols: &[TradingSymbol],
    ) -> OrError<Self> {
        if symbols.is_empty() {
            return Err("BinanceWebstreamWorker error: symbols list cannot be empty".to_string());
        }
        if symbols.len() > 1024 {
            return Err(
                "BinanceWebstreamWorker error: don't pass in more than 1024 tasks per worker"
                    .to_string(),
            );
        }

        // let mut rng = rand::rng();
        // let random_hash: String = (0..6)
        //     .map(|_| rng.sample(rand::distr::Alphanumeric) as char)
        //     .collect();

        // Agora paths for each symbol
        let agora_paths: Vec<String> = symbols
            .iter()
            .map(|symbol: &TradingSymbol| {
                format!(
                    "{}/{}/{}",
                    agora_prefix,
                    T::payload_identifier(),
                    symbol.to_string()
                )
            })
            .collect();

        // Create publishers for each symbol
        let mut publishers: Vec<Publisher<AgorableOption<T>>> = Vec::new();
        for (symbol, agora_path) in symbols.iter().zip(agora_paths.iter()) {
            // Make sure this is true! Scribe relies on data to identify symbols
            let publisher_name = symbol.to_string();
            let publisher = Publisher::<AgorableOption<T>>::new(
                publisher_name,
                agora_path.clone(),
                AgorableOption(None),
                metaserver_addr,
                metaserver_port,
            )
            .await?;
            publishers.push(publisher);
        }

        // Build symbol->publisher mapping for dispatch
        let symbol_to_publisher: HashMap<String, usize> = symbols
            .iter()
            .enumerate()
            .map(|(idx, symbol)| (symbol.to_string().to_lowercase(), idx))
            .collect();

        // Build websocket URL with all streams
        let stream_names: Vec<String> = symbols
            .iter()
            .map(|symbol| {
                format!(
                    "{}{}",
                    symbol.to_string().to_lowercase(),
                    T::websocket_suffix()
                )
            })
            .collect();
        let ws_url = format!(
            "{}/{}",
            BINANCE_SPOT_WEBSTREAM_ENDPOINT,
            stream_names.join("/")
        );
        println!("{}", ws_url);

        // Spawn worker task to handle websocket connection
        let worker_task = tokio::spawn(async move {
            loop {
                match connect_async(&ws_url).await {
                    Ok((ws_stream, _)) => {
                        let (mut write, mut read) = ws_stream.split();

                        while let Some(message) = read.next().await {
                            match message {
                                Ok(Message::Ping(ping_data)) => {
                                    // Respond to Ping with Pong to keep connection alive
                                    if let Err(e) = write.send(Message::Pong(ping_data)).await {
                                        eprintln!(
                                            "BinanceWebstreamWorker error sending Pong: {}",
                                            e
                                        );
                                        break;
                                    }
                                }
                                Ok(Message::Text(text)) => {
                                    // Parse the message and route to correct publisher
                                    match T::of_json_bytes(text) {
                                        Ok(parsed_msg) => {
                                            // Extract symbol and find the corresponding publisher
                                            let symbol_str =
                                                parsed_msg.symbol().to_string().to_lowercase();
                                            if let Some(&publisher_idx) =
                                                symbol_to_publisher.get(&symbol_str)
                                            {
                                                if let Some(publisher) =
                                                    publishers.get_mut(publisher_idx)
                                                {
                                                    if let Err(e) = publisher
                                                        .publish(AgorableOption(Some(parsed_msg)))
                                                        .await
                                                    {
                                                        eprintln!(
                                                            "BinanceWebstreamWorker publish error for {}: {}",
                                                            symbol_str, e
                                                        );
                                                    }
                                                }
                                            } else {
                                                eprintln!(
                                                    "BinanceWebstreamWorker: no publisher found for symbol {}",
                                                    symbol_str
                                                );
                                            }
                                        }
                                        Err(e) => {
                                            eprintln!("BinanceWebstreamWorker parse error: {}", e);
                                        }
                                    }
                                }
                                Ok(Message::Close(_)) => {
                                    eprintln!(
                                        "BinanceWebstreamWorker: received Close frame, disconnecting"
                                    );
                                    break;
                                }
                                Ok(_) => {
                                    // Ignore other message types (Pong, Binary, Frame)
                                }
                                Err(e) => {
                                    eprintln!("BinanceWebstreamWorker websocket error: {}", e);
                                    break;
                                }
                            }
                        }
                    }
                    Err(e) => {
                        eprintln!(
                            "BinanceWebstreamWorker connection error: {}, retrying in 5s...",
                            e
                        );
                    }
                }
                // Wait before retry (for both connection failures and disconnections)
                tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
            }
        });

        Ok(Self {
            agora_paths,
            dispatch_handle: worker_task,
            _phantom: PhantomData,
        })
    }

    /// Returns the agora paths for all publishers managed by this worker
    pub fn agora_paths(&self) -> &[String] {
        &self.agora_paths
    }
}

impl<T: BinanceStreamable> Drop for BinanceWebstreamWorker<T> {
    fn drop(&mut self) {
        // Upon agora publisher drop, pingserver and rawstream server aborts background handles
        self.dispatch_handle.abort()
    }
}

pub struct BinanceWebstreamSymbol {
    trade_worker: BinanceWebstreamWorker<TradeUpdate>,
    bbo_worker: BinanceWebstreamWorker<BboUpdate>,
    orderbookdiff_worker: BinanceWebstreamWorker<OrderbookDiffUpdate>,
}

impl BinanceWebstreamSymbol {
    pub async fn new(
        metaserver_addr: Ipv6Addr,
        metaserver_port: u16,
        agora_prefix: String,
        symbols: &[TradingSymbol],
    ) -> OrError<Self> {
        let trade_worker = BinanceWebstreamWorker::<TradeUpdate>::new(
            metaserver_addr,
            metaserver_port,
            agora_prefix.clone(),
            symbols,
        )
        .await?;
        let orderbookdiff_worker = BinanceWebstreamWorker::<OrderbookDiffUpdate>::new(
            metaserver_addr,
            metaserver_port,
            agora_prefix.clone(),
            symbols,
        )
        .await?;
        let bbo_worker = BinanceWebstreamWorker::<BboUpdate>::new(
            metaserver_addr,
            metaserver_port,
            agora_prefix,
            symbols,
        )
        .await?;
        Ok(Self {
            trade_worker,
            bbo_worker,
            orderbookdiff_worker,
        })
    }
}
