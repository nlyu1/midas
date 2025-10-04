use argus::crypto::binance::{BboUpdate, BinanceStreamable, OrderbookDiffUpdate, TradeUpdate};
use futures_util::{sink::SinkExt, stream::StreamExt};
use tokio_tungstenite::connect_async;
use tungstenite::client::IntoClientRequest;
use tungstenite::protocol::Message;

// Change to other types to see different messages
type T = OrderbookDiffUpdate;

#[tokio::main]
async fn main() {
    // Option 1: Direct stream endpoint (Option 2 not supported by Binance.US)'
    let tickers = ["solusdt", "ethusdt", "btcusdt", "dogeusdt", "xrpusdt"];
    let url_base = "wss://stream.binance.us:9443/ws";
    let url_str = tickers.iter().fold(url_base.to_string(), |acc, ticker| {
        format!("{}/{}{}", acc, ticker, T::websocket_suffix())
    });
    println!("URL: {}", url_str);
    let request = url_str.into_client_request().unwrap();
    println!("Request: {:?}", request);

    // Attempt to connect to the WebSocket stream.
    let (ws_stream, response) = connect_async(request).await.expect("Failed to connect");

    println!("HTTP Response: {}", response.status());
    println!("-----------------------------------");

    // Split the stream into a sender and receiver.
    // We only need the receiver (read half) to listen for messages from the server,
    // but we need the sender (write half) to respond to Pings.
    let (mut write, mut read) = ws_stream.split();

    // No subscription needed for direct stream endpoints - data starts flowing immediately
    println!("Connected to direct stream, waiting for data...");

    // The main loop to process incoming messages.
    while let Some(message) = read.next().await {
        match message {
            Ok(msg) => {
                match msg {
                    // This is the most important part for keeping the connection alive.
                    // Binance sends a Ping frame every 20 seconds. We must respond with a Pong
                    // frame containing the same payload to avoid being disconnected.
                    Message::Ping(ping_data) => {
                        println!("Received Ping, sending Pong back...");
                        if let Err(e) = write.send(Message::Pong(ping_data)).await {
                            eprintln!("Error sending Pong: {}", e);
                            break; // Break loop on error
                        }
                    }
                    // This will contain the actual trade data in JSON format.
                    Message::Text(text) => {
                        println!("Received raw data: {}", text);
                        match T::of_json_bytes(text) {
                            Ok(t) => println!("{}", t),
                            Err(s) => println!("{}", s),
                        }
                    }
                    Message::Close(_) => {
                        println!("Received Close frame. Disconnecting.");
                        break;
                    }
                    // Other message types we don't need to handle for this example.
                    Message::Pong(_) => { /* Ignore */ }
                    Message::Binary(_) => { /* Ignore */ }
                    Message::Frame(_) => { /* Ignore */ }
                }
            }
            Err(e) => {
                eprintln!("Error receiving message: {}", e);
                break; // Break loop on error
            }
        }
    }

    println!("-----------------------------------");
    println!("WebSocket connection closed.");
}
