use agora::{Agorable, Subscriber};
use agora::constants::METASERVER_DEFAULT_PORT;
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use std::fmt::Display;
use std::io::{self, Write};
use std::net::Ipv6Addr;
use tokio;

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
    println!("📻 Agora Subscriber Example");
    println!("This example demonstrates subscribing to messages from an Agora path.");
    println!("Make sure the metaserver is running on port {}!", METASERVER_DEFAULT_PORT);
    println!();

    // Get subscriber configuration from user
    print!("Enter path to subscribe to (e.g., chat/general): ");
    io::stdout().flush()?;
    let mut path = String::new();
    io::stdin().read_line(&mut path)?;
    let path = path.trim().to_string();

    println!("🔌 Connecting to metaserver and creating subscriber...");

    // Create subscriber
    let mut subscriber = Subscriber::<Message>::new(
        path.clone(),
        Ipv6Addr::LOCALHOST,
        METASERVER_DEFAULT_PORT,
    )
    .await
    .map_err(|e| format!("Failed to create subscriber: {}", e))?;

    println!("✅ Subscriber created successfully for path '{}'", path);
    println!("📡 Getting current value and starting stream...");
    println!();

    // Get current value and start streaming
    let (current_value, mut stream) = subscriber.get_stream().await
        .map_err(|e| format!("Failed to get stream: {}", e))?;

    println!("📥 Current value: {}", current_value);
    println!("🎧 Listening for new messages (Ctrl+C to exit):");
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