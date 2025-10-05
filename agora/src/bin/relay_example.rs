use agora::constants::{GATEWAY_PORT, METASERVER_PORT};
use agora::{ConnectionHandle, Relay};
use clap::Parser;
use indoc::indoc;
use local_ip_address::local_ip;
use std::io::{self, Write};
use std::net::IpAddr;
use tokio;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Cli {
    /// Port for the destination metaserver
    #[arg(short, long, default_value_t = METASERVER_PORT)]
    port: u16,

    #[arg(long, help = "Metaserver host IP address (defaults to local IP)")]
    host: Option<String>,

    /// Port for the local gateway
    #[arg(long, default_value_t = GATEWAY_PORT)]
    gateway_port: u16,
}

fn read_input(prompt: &str) -> Result<String, Box<dyn std::error::Error>> {
    print!("{}", prompt);
    io::stdout().flush()?;
    let mut input = String::new();
    io::stdin().read_line(&mut input)?;
    Ok(input.trim().to_string())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    print!(
        "{}",
        indoc! {"
            🔄 Agora Relay Example
            This example demonstrates relaying messages between Agora paths.
        "}
    );
    println!(
        "Make sure the metaserver is running on port {}!\n",
        cli.port
    );

    // Setup relay configuration
    println!("📡 Setting up relay destination:");

    let dest_path = read_input("Enter destination path (e.g., relay/output): ")?;
    let name = dest_path.clone(); // name = dest_path as requested

    let initial_value = read_input("Enter initial value: ")?;

    // Parse the IP address (supports both IPv4 and IPv6)
    let address: IpAddr = if let Some(host) = cli.host {
        host.parse()
            .map_err(|_| format!("Invalid IP address: {}", host))?
    } else {
        local_ip().map_err(|e| format!("Failed to get local IP: {}", e))?
    };

    let dest_metaserver_connection = ConnectionHandle::new(address, cli.port);
    println!(
        "Attempting metaserver connection: {}",
        dest_metaserver_connection
    );

    println!("🚀 Creating relay...");

    // Create relay
    let mut relay = Relay::<String>::new(
        name.clone(),
        dest_path.clone(),
        initial_value,
        dest_metaserver_connection,
        cli.gateway_port,
    )
    .await
    .map_err(|e| format!("Failed to create relay: {}", e))?;

    println!("✅ Relay created successfully!");
    println!("📍 Destination: '{}'", dest_path);
    print!(
        "{}",
        indoc! {"

            🔄 Ready for swap operations. Enter source configurations:
            💡 Press Ctrl+C to exit
        "}
    );
    println!("{}", "─".repeat(50));

    // Main swap loop
    loop {
        println!();
        println!("🔀 New swap operation:");

        let src_path = match read_input("Enter source path (or 'quit' to exit): ") {
            Ok(path) => {
                if path == "quit" || path == "exit" {
                    println!("👋 Goodbye!");
                    break;
                }
                if path.is_empty() {
                    println!("⚠️  Source path cannot be empty");
                    continue;
                }
                path
            }
            Err(e) => {
                eprintln!("❌ Error reading input: {}", e);
                continue;
            }
        };

        // Get source metaserver configuration
        let src_host = match read_input("Enter source metaserver host (press Enter for local IP): ")
        {
            Ok(host) => {
                if host.is_empty() {
                    local_ip().map_err(|e| format!("Failed to get local IP: {}", e))?
                } else {
                    host.parse()
                        .map_err(|_| format!("Invalid IP address: {}", host))?
                }
            }
            Err(e) => {
                eprintln!("❌ Error reading input: {}", e);
                continue;
            }
        };

        let src_metaserver_port = match read_input(&format!(
            "Enter source metaserver port (default: {}): ",
            cli.port
        )) {
            Ok(port_str) => {
                if port_str.is_empty() {
                    cli.port
                } else {
                    match port_str.parse() {
                        Ok(port) => port,
                        Err(e) => {
                            eprintln!("❌ Invalid port: {}", e);
                            continue;
                        }
                    }
                }
            }
            Err(_) => cli.port,
        };

        let src_metaserver_connection = ConnectionHandle::new(src_host, src_metaserver_port);
        println!("🔄 Swapping to source: '{}' -> '{}'", src_path, dest_path);
        println!("📡 Source metaserver: {}", src_metaserver_connection);

        // Perform swapon
        match relay
            .swapon(src_path.clone(), src_metaserver_connection)
            .await
        {
            Ok(()) => {
                println!("✅ Successfully swapped to source '{}'", src_path);
                println!("📡 Now relaying: {} -> {}", src_path, dest_path);
            }
            Err(e) => {
                eprintln!("❌ Failed to swap: {}", e);
                println!("🔄 Relay continues with previous source");
            }
        }
    }

    Ok(())
}
