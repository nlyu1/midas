use clap::{Parser, Subcommand};

/// A fictional CLI to manage a list of things
#[derive(Parser)]
struct Cli {
    /// The subcommand to run
    #[clap(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    Client(tarpc_example::client::Flags),
    Server(tarpc_example::server::Flags),
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    // You can now match on the command to dispatch to the correct logic.
    match &cli.command {
        Commands::Client(flags) => {
            println!("Running client...");
            println!("Flags: {flags:?}");
            tarpc_example::client::main(flags).await.unwrap();
        }
        Commands::Server(flags) => {
            println!("Running server...");
            println!("Flags: {flags:?}");
            tarpc_example::server::main(flags).await.unwrap();
        }
    }
}
