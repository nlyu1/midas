use agora::metaserver::{AgoraClient, DEFAULT_PORT, PublisherInfo};
use agora::utils::TreeTrait;
use clap::Parser;
use std::io::{self, Write};

#[derive(Parser)]
#[command(version, about = "Interactive MetaClient for Agora MetaServer", long_about = None)]
struct Args {
    #[arg(short, long, default_value_t = DEFAULT_PORT)]
    port: u16,

    #[arg(long, default_value = "localhost")]
    host: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    // Try to connect to the server
    println!("Connecting to MetaServer at {}:{}...", args.host, args.port);

    let client = match AgoraClient::new(Some(args.port)).await {
        Ok(client) => {
            println!("✅ Connected successfully!");
            client
        }
        Err(e) => {
            eprintln!("❌ Failed to connect to server: {}", e);
            eprintln!("Make sure the MetaServer is running with: cargo run --bin metaserver");
            return Ok(());
        }
    };

    // Print initial tree state
    print_path_tree(&client).await;

    // Enter interactive loop
    println!("\nWelcome to the Agora MetaClient!");
    println!("Available commands:");
    println!("  register <path>    - Register a demo publisher at path");
    println!("  update <path>      - Update publisher at path");
    println!("  remove <path>      - Remove publisher at path");
    println!("  info <path>        - Get publisher info at path");
    println!("  print              - Print current path tree");
    println!("  help               - Show this help message");
    println!("  quit/exit          - Exit the client");
    println!();

    loop {
        print!("metaclient> ");
        io::stdout().flush()?;

        let mut input = String::new();
        io::stdin().read_line(&mut input)?;
        let input = input.trim();

        if input.is_empty() {
            continue;
        }

        let parts: Vec<&str> = input.split_whitespace().collect();
        let command = parts[0].to_lowercase();

        match command.as_str() {
            "quit" | "exit" => {
                println!("Goodbye!");
                break;
            }
            "help" => {
                show_help();
            }
            "print" => {
                print_path_tree(&client).await;
            }
            "register" => {
                if parts.len() < 2 {
                    println!("Usage: register <path>");
                    continue;
                }
                let path = parts[1];
                register_publisher(&client, path).await;
                print_path_tree(&client).await;
            }
            "update" => {
                if parts.len() < 2 {
                    println!("Usage: update <path>");
                    continue;
                }
                let path = parts[1];
                update_publisher(&client, path).await;
                print_path_tree(&client).await;
            }
            "remove" => {
                if parts.len() < 2 {
                    println!("Usage: remove <path>");
                    continue;
                }
                let path = parts[1];
                remove_publisher(&client, path).await;
                print_path_tree(&client).await;
            }
            "info" => {
                if parts.len() < 2 {
                    println!("Usage: info <path>");
                    continue;
                }
                let path = parts[1];
                get_publisher_info(&client, path).await;
                // Don't print tree after info command since it's just a query
            }
            _ => {
                println!(
                    "Unknown command: {}. Type 'help' for available commands.",
                    command
                );
            }
        }
    }

    Ok(())
}

fn show_help() {
    println!("Available commands:");
    println!("  register <path>    - Register a demo publisher at path");
    println!("  update <path>      - Update publisher at path");
    println!("  remove <path>      - Remove publisher at path");
    println!("  info <path>        - Get publisher info at path");
    println!("  print              - Print current path tree");
    println!("  help               - Show this help message");
    println!("  quit/exit          - Exit the client");
}

async fn print_path_tree(client: &AgoraClient) {
    match client.get_path_tree().await {
        Ok(tree) => {
            println!("\n📁 Current Path Tree:");
            println!("{}", tree.display_tree());
            println!();
        }
        Err(e) => {
            println!("❌ Failed to get path tree: {}", e);
        }
    }
}

async fn register_publisher(client: &AgoraClient, path: &str) {
    let publisher = PublisherInfo::new("demo");

    match client.register_publisher(publisher, path.to_string()).await {
        Ok(()) => {
            println!("✅ Successfully registered publisher at '{}'", path);
        }
        Err(e) => {
            println!("❌ Failed to register publisher: {}", e);
        }
    }
}

async fn update_publisher(client: &AgoraClient, path: &str) {
    let publisher = PublisherInfo::new("demo-updated");

    match client.update_publisher(publisher, path.to_string()).await {
        Ok(()) => {
            println!("✅ Successfully updated publisher at '{}'", path);
        }
        Err(e) => {
            println!("❌ Failed to update publisher: {}", e);
        }
    }
}

async fn remove_publisher(client: &AgoraClient, path: &str) {
    match client.remove_publisher(path.to_string()).await {
        Ok(()) => {
            println!("✅ Successfully removed publisher at '{}'", path);
        }
        Err(e) => {
            println!("❌ Failed to remove publisher: {}", e);
        }
    }
}

async fn get_publisher_info(client: &AgoraClient, path: &str) {
    match client.get_publisher_info(path.to_string()).await {
        Ok(publisher) => {
            println!("📋 Publisher info at '{}': {:?}", path, publisher);
        }
        Err(e) => {
            println!("❌ Failed to get publisher info: {}", e);
        }
    }
}
