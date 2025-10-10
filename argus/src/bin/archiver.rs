use argus::constants::ARGUS_DATA_PATH;
use argus::Archiver;
use indoc::indoc;
use std::io::{self, Write};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    println!("Welcome to the Argus Archiver!");
    println!("This tool organizes temporary parquet files into hive-partitioned structure.\n");

    // Prompt for destination path
    print!("Enter destination path (e.g., hyperliquid/spot): ");
    io::stdout().flush()?;
    let mut dest_path = String::new();
    io::stdin().read_line(&mut dest_path)?;
    let dest_path = dest_path.trim();

    let target_dir = format!("{}/{}", ARGUS_DATA_PATH, dest_path);
    println!("Target directory: {}", target_dir);

    // Prompt for source path
    print!("Enter source path (e.g., /tmp/hyperliquid): ");
    io::stdout().flush()?;
    let mut src_path = String::new();
    io::stdin().read_line(&mut src_path)?;
    let src_path = src_path.trim().to_string();

    // Prompt for data types
    print!("Enter data types (comma-separated, e.g., last_trade,bbo): ");
    io::stdout().flush()?;
    let mut data_types_input = String::new();
    io::stdin().read_line(&mut data_types_input)?;
    let data_types: Vec<String> = data_types_input
        .trim()
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();

    if data_types.is_empty() {
        eprintln!("❌ No data types provided. Exiting.");
        return Ok(());
    }

    println!("\nInitializing archiver...");
    println!("  Target: {}", target_dir);
    println!("  Source: {}", src_path);
    println!("  Data types: {:?}", data_types);

    let mut archiver = match Archiver::new(&target_dir, &data_types, &src_path).await {
        Ok(archiver) => {
            println!("\n✅ Archiver initialized successfully!");
            archiver
        }
        Err(e) => {
            eprintln!("\n❌ Failed to initialize archiver: {}", e);
            return Ok(());
        }
    };

    println!();
    show_help();

    // Enter REPL loop
    loop {
        print!("archiver> ");
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
                println!("Shutting down archiver...");
                archiver.shutdown().await.map_err(|e| anyhow::anyhow!(e))?;
                println!("Goodbye!");
                break;
            }
            "help" => {
                show_help();
            }
            "swapon" => {
                if parts.len() < 2 {
                    println!("Usage: swapon <src_path>");
                    continue;
                }
                let new_src_path = parts[1];
                match archiver.swap_on(new_src_path) {
                    Ok(_) => println!("✅ Swapped source directory to: {}", new_src_path),
                    Err(e) => println!("❌ Failed to swap source directory: {}", e),
                }
            }
            "last_updates" => {
                print_last_updates(&archiver);
            }
            "target_dir" => {
                println!("Target directory: {}", archiver.target_dir());
            }
            "src_dir" => {
                println!("Source directory: {}", archiver.src_dir());
            }
            "data_types" => {
                println!("Data types: {:?}", archiver.data_types());
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
              swapon <src_path>  - Switch to a new source directory
              last_updates       - Show time since last update for each data type/symbol
              target_dir         - Show target directory
              src_dir            - Show current source directory
              data_types         - Show monitored data types
              help               - Show this help message
              quit/exit          - Shutdown archiver and exit
        "}
    );
}

fn print_last_updates(archiver: &Archiver) {
    let updates = archiver.time_since_last_update();

    if updates.is_empty() {
        println!("No updates tracked yet.");
        return;
    }

    println!("\nTime since last update:");
    for (data_type, symbols) in updates.iter() {
        println!("  {}:", data_type);
        if symbols.is_empty() {
            println!("    (no symbols tracked)");
        } else {
            for (symbol, duration) in symbols {
                let seconds = duration.num_seconds();
                if seconds < 60 {
                    println!("    {}: {}s ago", symbol.to_string(), seconds);
                } else if seconds < 3600 {
                    println!("    {}: {}m ago", symbol.to_string(), seconds / 60);
                } else {
                    println!("    {}: {}h ago", symbol.to_string(), seconds / 3600);
                }
            }
        }
    }
    println!();
}
