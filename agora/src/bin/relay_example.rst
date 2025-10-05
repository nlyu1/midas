use agora::Relay;
use agora::constants::METASERVER_PORT;
use clap::Parser;
use indoc::indoc;
use std::io::{self, Write};
use std::net::Ipv6Addr;
use tokio;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Cli {
    /// Port for the metaserver
    #[arg(short, long, default_value_t = METASERVER_PORT)]
    port: u16,
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
    println!("Make sure the metaserver is running on port {}!\n", cli.port);

    // Setup relay configuration
    println!("📡 Setting up relay destination:");

    let dest_path = read_input("Enter destination path (e.g., relay/output): ")?;
    let name = dest_path.clone(); // name = dest_path as requested

    let initial_value = read_input("Enter initial value: ")?;

    let metaserver_addr = Ipv6Addr::LOCALHOST; // Assume default address

    let metaserver_port_str =
        read_input(&format!("Enter metaserver port (default: {}): ", cli.port))?;
    let metaserver_port = if metaserver_port_str.is_empty() {
        cli.port
    } else {
        metaserver_port_str.parse()?
    };

    println!("🚀 Creating relay...");

    // Create relay
    let mut relay = Relay::<String>::new(
        name.clone(),
        dest_path.clone(),
        initial_value,
        metaserver_addr,
        metaserver_port,
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

        let src_metaserver_addr = Ipv6Addr::LOCALHOST; // Assume default address

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

        println!("🔄 Swapping to source: '{}' -> '{}'", src_path, dest_path);

        // Perform swapon
        match relay
            .swapon(src_path.clone(), src_metaserver_addr, src_metaserver_port)
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
