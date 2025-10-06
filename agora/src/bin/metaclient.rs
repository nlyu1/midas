use agora::constants::METASERVER_PORT;
use agora::metaserver::AgoraClient;
use agora::utils::TreeTrait;
use agora::{ConnectionHandle, OmniSubscriber};
use clap::Parser;
use futures_util::StreamExt;
use indoc::indoc;
use local_ip_address::local_ip;
use std::io::{self, Write};
use std::net::IpAddr;
use tokio::io::{AsyncReadExt, stdin};
use tokio::select;

#[derive(Parser)]
#[command(version, about = "Interactive MetaClient for exploring and monitoring Agora MetaServer", long_about = None)]
struct Args {
    #[arg(short, long, default_value_t = METASERVER_PORT)]
    port: u16,

    #[arg(long, help = "Metaserver host IP address (defaults to local IP)")]
    host: Option<String>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    // Parse the IP address (supports both IPv4 and IPv6)
    let address: IpAddr = if let Some(host) = &args.host {
        host.parse()
            .map_err(|_| anyhow::anyhow!("Invalid IP address: {}", host))?
    } else {
        local_ip().map_err(|e| anyhow::anyhow!("Failed to get local IP: {}", e))?
    };

    let metaserver_connection = ConnectionHandle::new(address, args.port);

    let client = match AgoraClient::new(metaserver_connection.clone()).await {
        Ok(client) => {
            println!(
                "Successfully connected to metaserver at {}:{}...",
                address, args.port
            );
            client
        }
        Err(e) => {
            eprintln!("❌ Failed to connect to metaserver: {}", e);
            eprintln!("Make sure the MetaServer is running with: cargo run --bin metaserver");
            return Ok(());
        }
    };

    // Print initial tree state
    print_path_tree(&client).await;

    // Enter interactive loop
    println!("\nWelcome to the Agora MetaClient!");
    println!(
        "This tool is used for exploring the current running metaserver and monitoring processes."
    );
    print!(
        "{}",
        indoc! {"
            Available commands:
              remove <path>      - Remove publisher at path
              info <path>        - Get publisher info at path
              monitor <path>     - Monitor string outputs from publisher at path (Ctrl+D to exit)
              print              - Print current path tree
              help               - Show this help message
              quit/exit          - Exit the client
        "}
    );

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
            "monitor" => {
                if parts.len() < 2 {
                    println!("Usage: monitor <path>");
                    continue;
                }
                let path = parts[1];
                monitor_path(path, metaserver_connection.clone()).await;
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
    print!(
        "{}",
        indoc! {"
            Available commands:
              remove <path>      - Remove publisher at path
              info <path>        - Get publisher info at path
              monitor <path>     - Monitor string outputs from publisher at path (Ctrl+D to exit)
              print              - Print current path tree
              help               - Show this help message
              quit/exit          - Exit the client
        "}
    );
}

async fn print_path_tree(client: &AgoraClient) {
    match client.get_path_tree().await {
        Ok(tree) => {
            println!("\nCurrent Path Tree:");
            println!("{}", tree.display_tree());
            println!();
        }
        Err(e) => {
            println!("❌ Failed to get path tree: {}", e);
        }
    }
}

async fn remove_publisher(client: &AgoraClient, path: &str) {
    match client.remove_publisher(path.to_string()).await {
        Ok(removed_publisher) => {
            println!(
                "✅ Successfully removed publisher at '{}': {:?}",
                path, removed_publisher
            );
        }
        Err(e) => {
            println!("❌ Failed to remove publisher: {}", e);
        }
    }
}

async fn get_publisher_info(client: &AgoraClient, path: &str) {
    match client.get_publisher_info(path.to_string()).await {
        Ok(publisher) => {
            println!("Publisher info at '{}': {:?}", path, publisher);
        }
        Err(e) => {
            println!("❌ Failed to get publisher info: {}", e);
        }
    }
}

async fn monitor_path(path: &str, metaserver_connection: ConnectionHandle) {
    println!(
        "Starting to monitor path '{}' - Press Ctrl+D to exit",
        path
    );

    // Create omnisubscriber
    let mut omni_subscriber =
        match OmniSubscriber::new(path.to_string(), metaserver_connection).await {
            Ok(subscriber) => subscriber,
            Err(e) => {
                println!(
                    "❌ Failed to create omnisubscriber for path '{}': {}",
                    path, e
                );
                return;
            }
        };

    // Get initial value and stream
    let (_current_value, mut stream) = match omni_subscriber.get_stream().await {
        Ok((current, stream)) => {
            println!("Current value: {}", current);
            (current, stream)
        }
        Err(e) => {
            println!("❌ Failed to get stream for path '{}': {}", path, e);
            return;
        }
    };

    // Create a stdin handle for Ctrl+D detection
    let mut stdin_reader = stdin();
    let mut buffer = [0; 1];

    println!("Monitoring for new outputs...");

    loop {
        select! {
            // Check for new stream data
            stream_result = stream.next() => {
                match stream_result {
                    Some(Ok(value)) => {
                        println!("New output: {}", value);
                    }
                    Some(Err(e)) => {
                        println!("❌ Stream error: {}", e);
                        break;
                    }
                    None => {
                        println!("Stream ended");
                        break;
                    }
                }
            }
            // Check for stdin input (Ctrl+D)
            stdin_result = stdin_reader.read(&mut buffer) => {
                match stdin_result {
                    Ok(0) => {
                        // EOF received (Ctrl+D)
                        println!("\nReceived Ctrl+D, exiting monitor mode");
                        break;
                    }
                    Ok(_) => {
                        // Some input received, continue monitoring
                        continue;
                    }
                    Err(e) => {
                        println!("❌ Stdin error: {}", e);
                        break;
                    }
                }
            }
        }
    }

    println!("✅ Stopped monitoring path '{}'", path);
}
