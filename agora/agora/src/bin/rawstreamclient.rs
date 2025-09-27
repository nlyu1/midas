use agora::ports::PUBLISHER_SERVICE_PORT;
use agora::rawstream::RawStreamClient;
use clap::Parser;
use futures_util::StreamExt;
use std::net::Ipv6Addr;

#[derive(Parser)]
#[command(version, about = "Raw Stream Client - connects to WebSocket server and prints messages", long_about = None)]
struct Args {
    #[arg(short, long, default_value_t = PUBLISHER_SERVICE_PORT)]
    port: u16,

    #[arg(long, default_value = "::1")]
    host: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    // Parse the IPv6 address
    let address: Ipv6Addr = args.host.parse()
        .map_err(|_| anyhow::anyhow!("Invalid IPv6 address: {}", args.host))?;

    println!("🔗 Connecting to Raw Stream Server at [{}]:{}", address, args.port);

    // Create client with String type
    let client: RawStreamClient<String> = match RawStreamClient::new(address, args.port, None, None) {
        Ok(client) => {
            println!("✅ Connected successfully!");
            client
        }
        Err(e) => {
            eprintln!("❌ Failed to connect to server: {}", e);
            eprintln!("Make sure the RawStreamServer is running with: cargo run --bin rawstreamserver");
            return Ok(());
        }
    };

    println!("📡 Listening for messages... (Press Ctrl+C to exit)\n");

    // Subscribe to the stream and listen for messages
    let mut stream = client.subscribe();

    while let Some(result) = stream.next().await {
        match result {
            Ok(message) => {
                let now = chrono::Utc::now();
                let received_timestamp = format!("{}:{:06.3}",
                    now.format("%M:%S"),
                    now.timestamp_subsec_micros() as f64 / 1000.0
                );
                println!("📨 Received: {}", message);
                println!("    received at [{}]", received_timestamp);
            }
            Err(e) => {
                eprintln!("❌ Error receiving message: {}", e);
                // Continue listening for more messages despite errors
            }
        }
    }

    println!("🔌 Connection closed");
    Ok(())
}