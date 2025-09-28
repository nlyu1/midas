use agora::constants::PUBLISHER_SERVICE_PORT;
use agora::rawstream::RawStreamServer;
use clap::Parser;
use std::io::{self, Write};
use std::net::Ipv6Addr;
use tokio::io::{AsyncBufReadExt, BufReader};

#[derive(Parser)]
#[command(version, about = "Raw Stream Server - streams user input to WebSocket clients", long_about = None)]
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

    println!("🚀 Starting Raw Stream Server on [{}]:{}", address, args.port);
    println!("📝 Type messages and press Enter to broadcast them to connected clients");
    println!("🔌 Clients can connect via WebSocket to ws://[{}]:{}", address, args.port);
    println!("💡 Type 'quit' or 'exit' to stop the server\n");

    // Create and start the server (this initializes everything)
    let server: RawStreamServer<String> = RawStreamServer::new(address, args.port, None).await
        .map_err(|e| anyhow::anyhow!(e))?;

    println!("✅ Server started successfully!");

    // Create stdin reader for input loop
    let stdin = tokio::io::stdin();
    let reader = BufReader::new(stdin);
    let mut lines = reader.lines();

    // Input loop - read from stdin and publish to server
    loop {
        print!("rawstreamserver> ");
        io::stdout().flush().ok();

        match lines.next_line().await {
            Ok(Some(line)) => {
                let line = line.trim().to_string();
                if line.is_empty() {
                    continue; // Skip empty lines
                } else if line == "quit" || line == "exit" {
                    println!("👋 Shutting down server...");
                    break;
                } else {
                    let now = chrono::Utc::now();
                    let timestamp = format!("{}:{:06.3}",
                        now.format("%M:%S"),
                        now.timestamp_subsec_micros() as f64 / 1000.0
                    );
                    let timestamped_message = format!("[{}] {}", timestamp, line);
                    println!("📡 Broadcasting: {}", timestamped_message);
                    if let Err(e) = server.publish(timestamped_message) {
                        eprintln!("❌ Failed to publish message: {}", e);
                        break;
                    }
                }
            }
            Ok(None) | Err(_) => {
                println!("📥 Input stream closed");
                break;
            }
        }
    }

    Ok(())
}