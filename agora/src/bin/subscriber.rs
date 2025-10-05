use agora::constants::METASERVER_PORT;
use agora::{Agorable, ConnectionHandle, Subscriber};
use clap::Parser;
use futures_util::StreamExt;
use indoc::indoc;
use local_ip_address::local_ip;
use serde::{Deserialize, Serialize};
use std::fmt::Display;
use std::io::{self, Write};
use std::net::IpAddr;
use tokio;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Cli {
    /// Port for the metaserver
    #[arg(short, long, default_value_t = METASERVER_PORT)]
    port: u16,

    #[arg(long, help = "Metaserver host IP address (defaults to local IP)")]
    host: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Message {
    content: String,
    timestamp: u64,
}

impl Display for Message {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[{}] {}", self.timestamp, self.content)
    }
}

impl Agorable for Message {}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    print!(
        "{}",
        indoc! {"
            📻 Agora Subscriber Example
            This example demonstrates subscribing to messages from an Agora path.
        "}
    );
    println!(
        "Make sure the metaserver is running on port {}!\n",
        cli.port
    );

    // Get subscriber configuration from user
    print!("Enter path to subscribe to (e.g., chat/general): ");
    io::stdout().flush()?;
    let mut path = String::new();
    io::stdin().read_line(&mut path)?;
    let path = path.trim().to_string();

    println!("🔌 Connecting to metaserver and creating subscriber...");

    // Parse the IP address (supports both IPv4 and IPv6)
    let address: IpAddr = if let Some(host) = cli.host {
        host.parse()
            .map_err(|_| format!("Invalid IP address: {}", host))?
    } else {
        local_ip().map_err(|e| format!("Failed to get local IP: {}", e))?
    };

    let metaserver_connection = ConnectionHandle::new(address, cli.port);

    // Create subscriber
    let mut subscriber = Subscriber::<Message>::new(path.clone(), metaserver_connection)
        .await
        .map_err(|e| format!("Failed to create subscriber: {}", e))?;

    println!("✅ Subscriber created successfully for path '{}'", path);
    print!(
        "{}",
        indoc! {"
            📡 Getting current value and starting stream...
        "}
    );

    // Get current value and start streaming
    let (current_value, mut stream) = subscriber
        .get_stream()
        .await
        .map_err(|e| format!("Failed to get stream: {}", e))?;

    println!("📥 Current value: {}", current_value);
    print!(
        "{}",
        indoc! {"
            🎧 Listening for new messages (Ctrl+C to exit):
        "}
    );
    println!("{}", "─".repeat(50));

    // Listen for new messages
    while let Some(result) = stream.next().await {
        match result {
            Ok(message) => {
                println!("📨 New message: {}", message);
            }
            Err(e) => {
                eprintln!("❌ Stream error: {}", e);
                // Continue listening despite errors
            }
        }
    }

    println!("📻 Stream ended. Goodbye!");
    Ok(())
}
