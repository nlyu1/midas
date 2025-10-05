use agora::constants::GATEWAY_PORT;
use agora::ping::PingClient;
use agora::ConnectionHandle;
use clap::Parser;
use indoc::indoc;
use local_ip_address::local_ip;
use std::net::IpAddr;

#[derive(Parser)]
#[command(version, about = "Ping Client - pings server every second via gateway", long_about = None)]
struct Args {
    #[arg(short, long, default_value_t = GATEWAY_PORT)]
    port: u16,

    #[arg(long, help = "Gateway host IP address (defaults to local IP)")]
    host: Option<String>,

    #[arg(long, default_value = "test/publisher", help = "Directory path (maps to /tmp/agora/{directory}/ping.sock)")]
    directory: String,

    #[arg(long, default_value_t = 1000, help = "Ping interval in milliseconds")]
    interval_ms: u64,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    // Parse the IP address (supports both IPv4 and IPv6)
    let address: IpAddr = if let Some(host) = args.host {
        host.parse()
            .map_err(|_| anyhow::anyhow!("Invalid IP address: {}", host))?
    } else {
        local_ip().map_err(|e| anyhow::anyhow!("Failed to get local IP: {}", e))?
    };

    let gateway = ConnectionHandle::new(address, args.port);

    println!(
        "🔗 Connecting to Ping Server via gateway at {}/ping/{}",
        gateway, args.directory
    );

    // Create ping client
    let mut client = match PingClient::new(&args.directory, gateway).await {
        Ok(client) => {
            println!("✅ Connected successfully!");
            client
        }
        Err(e) => {
            eprintln!("❌ Failed to connect to server: {}", e);
            print!(
                "{}",
                indoc! {"
                    Make sure:
                    1. Gateway is running: cargo run --bin gateway
                    2. PingServer is running: cargo run --bin pingserver
                "}
            );
            return Ok(());
        }
    };

    println!(
        "📡 Pinging every {}ms... (Press Ctrl+C to exit)\n",
        args.interval_ms
    );

    let interval = tokio::time::Duration::from_millis(args.interval_ms);

    loop {
        match client.ping().await {
            Ok((vec_payload, str_payload, time_delta)) => {
                let now = chrono::Utc::now();
                let timestamp = format!(
                    "{}:{:06.3}",
                    now.format("%M:%S"),
                    now.timestamp_subsec_micros() as f64 / 1000.0
                );

                println!("📨 [{}] Ping response:", timestamp);
                println!("    String payload: \"{}\"", str_payload);
                println!("    Vec payload: {} bytes", vec_payload.len());
                println!("    Round-trip time: {}ms", time_delta.num_milliseconds());
            }
            Err(e) => {
                eprintln!("❌ Ping failed: {}", e);
                // Continue trying despite errors
            }
        }

        tokio::time::sleep(interval).await;
    }
}
