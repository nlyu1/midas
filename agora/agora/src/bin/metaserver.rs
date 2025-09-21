use agora::metaserver::AgoraMetaServer;
use agora::metaserver::DEFAULT_PORT;
use clap::Parser;

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg(short, long, default_value_t = DEFAULT_PORT)]
    port: u16,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    AgoraMetaServer::run_server(Some(args.port)).await
}
