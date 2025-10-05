use agora::constants::METASERVER_PORT;
use agora::metaserver::AgoraMetaServer;
use clap::Parser;
use local_ip_address::local_ip;
use std::net::IpAddr;

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg(short, long, default_value_t = METASERVER_PORT)]
    port: u16,

    #[arg(long, help = "Metaserver host IP address (defaults to local IP)")]
    host: Option<String>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let address: IpAddr = if let Some(host) = args.host {
        host.parse()?
    } else {
        local_ip()?
    };
    AgoraMetaServer::run_server(address, args.port).await
}
