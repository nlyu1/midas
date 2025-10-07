use agora::ping::PingServer;
use clap::Parser;
use std::io::{self, Write};
use tokio::io::{AsyncBufReadExt, BufReader};

#[derive(Parser)]
#[command(version, about = "Ping Server - responds to ping requests with latest payload", long_about = None)]
struct Args {
    #[arg(long, default_value = "test/publisher", help = "Directory path under /tmp/agora")]
    directory: String,

    #[arg(long, default_value = "initial payload", help = "Initial string payload")]
    initial_payload: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    println!(
        "Starting Ping Server at UDS: /tmp/agora/{}/ping.sock",
        args.directory
    );
    println!("Type messages and press Enter to update the ping payload");
    println!(
        "Clients connect via gateway: ws://[gateway_host]:port/ping/{}",
        args.directory
    );
    println!("Type 'quit' or 'exit' to stop the server\n");

    // Create server with initial payload
    let initial_vec = args.initial_payload.as_bytes().to_vec();
    let mut server = PingServer::new(&args.directory, initial_vec.clone(), args.initial_payload.clone())
        .await
        .map_err(|e| anyhow::anyhow!(e))?;

    println!("✅ Server started successfully!");
    println!("Initial payload: \"{}\"", args.initial_payload);

    // Create stdin reader for input loop
    let stdin = tokio::io::stdin();
    let reader = BufReader::new(stdin);
    let mut lines = reader.lines();

    // Input loop - read from stdin and update payload
    loop {
        print!("pingserver> ");
        io::stdout().flush().ok();

        match lines.next_line().await {
            Ok(Some(line)) => {
                let line = line.trim().to_string();
                if line.is_empty() {
                    continue; // Skip empty lines
                } else if line == "quit" || line == "exit" {
                    println!("Shutting down server...");
                    break;
                } else {
                    let vec_payload = line.as_bytes().to_vec();
                    println!("Updating payload: \"{}\"", line);
                    server.update_payload(&vec_payload, &line);
                }
            }
            Ok(None) | Err(_) => {
                println!("Input stream closed");
                break;
            }
        }
    }

    Ok(())
}
