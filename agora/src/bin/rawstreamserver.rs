use agora::rawstream::RawStreamServer;
use clap::Parser;
use indoc::indoc;
use std::io::{self, Write};
use tokio::io::{AsyncBufReadExt, BufReader};

#[derive(Parser)]
#[command(version, about = "Raw Stream Server - streams user input to WebSocket clients via UDS", long_about = None)]
struct Args {
    #[arg(long, default_value = "test/publisher", help = "Directory path under /tmp/agora")]
    directory: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    let uds_path = format!("/tmp/agora/{}/rawstream.sock", args.directory);

    println!("🚀 Starting Raw Stream Server at UDS: {}", uds_path);
    print!(
        "{}",
        indoc! {"
            📝 Type messages and press Enter to broadcast them to connected clients
        "}
    );
    println!(
        "🔌 Clients connect via gateway: ws://[gateway_host]:port/rawstream/{}",
        args.directory
    );
    print!(
        "{}",
        indoc! {"
            💡 Type 'quit' or 'exit' to stop the server
        "}
    );

    // Create and start the server
    let server: RawStreamServer<String> = RawStreamServer::new(&uds_path, None)
        .await
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
                    let timestamp = format!(
                        "{}:{:06.3}",
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
