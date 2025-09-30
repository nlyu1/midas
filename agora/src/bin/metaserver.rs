use agora::constants::METASERVER_DEFAULT_PORT;
use agora::metaserver::AgoraMetaServer;
use clap::Parser;
use std::net::Ipv6Addr;

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg(short, long, default_value_t = METASERVER_DEFAULT_PORT)]
    port: u16,

    #[arg(short, long, default_value = "::1")]
    address: Ipv6Addr,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    AgoraMetaServer::run_server(args.address, args.port).await
}
