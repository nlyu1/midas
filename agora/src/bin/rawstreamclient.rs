use agora::constants::GATEWAY_PORT;
use agora::rawstream::RawStreamClient;
use agora::ConnectionHandle;
use clap::Parser;
use futures_util::StreamExt;
use indoc::indoc;
use local_ip_address::local_ip;
use std::net::IpAddr;

#[derive(Parser)]
#[command(version, about = "Raw Stream Client - connects to WebSocket server via gateway", long_about = None)]
struct Args {
    #[arg(short, long, default_value_t = GATEWAY_PORT)]
    port: u16,

    #[arg(long, help = "Gateway host IP address (defaults to local IP)")]
    host: Option<String>,

    #[arg(long, default_value = "test/publisher", help = "Directory path (maps to /tmp/agora/{directory}/rawstream.sock)")]
    directory: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    // Parse the IP address (supports both IPv4 and IPv6)
    let address: IpAddr = if let Some(host) = args.host {
        host.parse()
            .map_err(|_| anyhow::anyhow!("Invalid IP address: {}", host))?
    } else {
        local_ip().map_err(|e| anyhow::anyhow!("Failed to get local IP: {}", e))?
    };

    let gateway = ConnectionHandle::new(address, args.port);

    println!(
        "Connecting to Raw Stream via gateway at {}/rawstream/{}",
        gateway,
        args.directory
    );

    // Create client with String type
    let client: RawStreamClient<String> =
        match RawStreamClient::new(gateway, &args.directory, None, None) {
            Ok(client) => {
                println!("✅ Connected successfully!");
                client
            }
            Err(e) => {
                eprintln!("❌ Failed to connect to server: {}", e);
                print!(
                    "{}",
                    indoc! {"
                    Make sure:
                    1. Gateway is running: cargo run --bin gateway
                    2. RawStreamServer is running: cargo run --bin rawstreamserver
                "}
                );
                return Ok(());
            }
        };

    println!("Listening for messages... (Press Ctrl+C to exit)\n");

    // Subscribe to the stream and listen for messages
    let mut stream = client.subscribe();

    while let Some(result) = stream.next().await {
        match result {
            Ok(message) => {
                let now = chrono::Utc::now();
                let received_timestamp = format!(
                    "{}:{:06.3}",
                    now.format("%M:%S"),
                    now.timestamp_subsec_micros() as f64 / 1000.0
                );
                println!("Received: {}", message);
                println!("    received at [{}]", received_timestamp);
            }
            Err(e) => {
                eprintln!("❌ Error receiving message: {}", e);
                // Continue listening for more messages despite errors
            }
        }
    }

    println!("Connection closed");
    Ok(())
}
