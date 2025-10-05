use agora::constants::METASERVER_DEFAULT_PORT;
use agora::{Agorable, Publisher};
use clap::Parser;
use indoc::indoc;
use serde::{Deserialize, Serialize};
use std::fmt::Display;
use std::io::{self, Write};
use std::net::Ipv6Addr;
use tokio;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Cli {
    /// Port for the metaserver
    #[arg(short, long, default_value_t = METASERVER_DEFAULT_PORT)]
    port: u16,

    #[arg(short, long, default_value = "::1")]
    address: Ipv6Addr,
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
            🚀 Agora Publisher Example
            This example demonstrates publishing messages to an Agora path.
        "}
    );
    println!(
        "Make sure the metaserver is running on port {}!\n",
        cli.port
    );

    // Get publisher configuration from user
    print!("Enter publisher name: ");
    io::stdout().flush()?;
    let mut name = String::new();
    io::stdin().read_line(&mut name)?;
    let name = name.trim().to_string();

    print!("Enter path (e.g., chat/general): ");
    io::stdout().flush()?;
    let mut path = String::new();
    io::stdin().read_line(&mut path)?;
    let path = path.trim().to_string();

    print!("Enter initial message: ");
    io::stdout().flush()?;
    let mut initial_content = String::new();
    io::stdin().read_line(&mut initial_content)?;
    let initial_content = initial_content.trim().to_string();

    let initial_message = Message {
        content: initial_content,
        timestamp: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_secs(),
    };

    println!("📡 Connecting to metaserver and creating publisher...");

    // Create publisher
    let mut publisher = Publisher::new(
        name.clone(),
        path.clone(),
        initial_message,
        cli.address,
        cli.port,
    )
    .await
    .map_err(|e| format!("Failed to create publisher: {}", e))?;

    println!(
        "✅ Publisher '{}' created successfully for path '{}'",
        name, path
    );
    print!(
        "{}",
        indoc! {"
            💬 Enter messages to publish (Ctrl+C to exit):
        "}
    );

    // Main publishing loop
    loop {
        print!("> ");
        io::stdout().flush()?;

        let mut input = String::new();
        match io::stdin().read_line(&mut input) {
            Ok(0) => {
                // EOF (Ctrl+D)
                println!("\n👋 Goodbye!");
                break;
            }
            Ok(_) => {
                let content = input.trim();
                if content.is_empty() {
                    continue;
                }

                let message = Message {
                    content: content.to_string(),
                    timestamp: std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)?
                        .as_secs(),
                };

                match publisher.publish(message.clone()).await {
                    Ok(()) => {
                        println!("📤 Published: {}", message);
                    }
                    Err(e) => {
                        eprintln!("❌ Failed to publish message: {}", e);
                    }
                }
            }
            Err(e) => {
                eprintln!("❌ Error reading input: {}", e);
                break;
            }
        }
    }

    Ok(())
}
