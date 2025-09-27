use agora::OmniSubscriber;
use agora::ports::METASERVER_DEFAULT_PORT;
use futures_util::StreamExt;
use std::io::{self, Write};
use std::net::Ipv6Addr;
use tokio;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🌐 Agora OmniSubscriber Example");
    println!("This example demonstrates subscribing to string messages from an Agora path.");
    println!("OmniSubscriber works with any publisher's string representation.");
    println!("Make sure the metaserver is running on port {}!", METASERVER_DEFAULT_PORT);
    println!();

    // Get subscriber configuration from user
    print!("Enter path to omni-subscribe to (e.g., chat/general): ");
    io::stdout().flush()?;
    let mut path = String::new();
    io::stdin().read_line(&mut path)?;
    let path = path.trim().to_string();

    println!("🔌 Connecting to metaserver and creating omni-subscriber...");

    // Create omni-subscriber
    let omnisubscriber = OmniSubscriber::new(
        path.clone(),
        Ipv6Addr::LOCALHOST,
        METASERVER_DEFAULT_PORT,
    )
    .await
    .map_err(|e| format!("Failed to create omni-subscriber: {}", e))?;

    println!("✅ OmniSubscriber created successfully for path '{}'", path);
    println!("📡 Getting current string value and starting stream...");
    println!();

    // Get current value and start streaming
    let (current_value, mut stream) = omnisubscriber.get_stream().await
        .map_err(|e| format!("Failed to get stream: {}", e))?;

    println!("📥 Current string value: {}", current_value);
    println!("🎧 Listening for new string messages (Ctrl+C to exit):");
    println!("{}", "─".repeat(50));

    // Listen for new string messages
    while let Some(result) = stream.next().await {
        match result {
            Ok(string_message) => {
                println!("📨 New string: {}", string_message);
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