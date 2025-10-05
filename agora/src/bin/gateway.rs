use agora::constants::GATEWAY_PORT;
use agora::gateway::Gateway;
use clap::Parser;

#[derive(Parser)]
#[command(version, about = "Agora Gateway - proxies external TCP/WS to internal UDS", long_about = None)]
struct Args {
    #[arg(short, long, default_value_t = GATEWAY_PORT)]
    port: u16,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    println!("🌐 Starting Agora Gateway on port {}", args.port);

    let gateway = Gateway::new(args.port)
        .await
        .map_err(|e| anyhow::anyhow!(e))?;

    println!(
        "✅ Gateway running at {}:{}",
        gateway.connection().addr(),
        gateway.connection().port()
    );
    println!("📡 Ready to proxy connections:");
    println!("   - /rawstream/{{path}} → /tmp/agora/{{path}}/rawstream.sock");
    println!("   - /ping/{{path}} → /tmp/agora/{{path}}/ping.sock");
    println!("Press Ctrl+C to exit\n");

    // Keep running indefinitely (drop on Ctrl+C)
    std::future::pending::<()>().await;

    Ok(())
}
