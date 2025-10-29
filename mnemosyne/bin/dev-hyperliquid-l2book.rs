use anyhow::Result;
use chrono::NaiveDate;
use clap::Parser;
use mnemosyne::crypto::hyperliquid::l2book::{read_hyperliquid_l2book_bydate, read_hyperliquid_l2book_lz4};
use std::path::PathBuf;

#[derive(Parser)]
#[command(version, about = "Debug Hyperliquid L2 book reader", long_about = None)]
struct Args {
    /// Path to raw data directory
    #[arg(long, default_value = "/bigdata/mnemosyne/hyperliquid/raw/futures/market_data")]
    raw_data_path: PathBuf,

    /// Date to read (YYYY-MM-DD)
    #[arg(long, default_value = "2023-04-15")]
    date: String,

    /// Test single file instead of full date
    #[arg(long)]
    single_file: Option<PathBuf>,

    /// Symbol for single file test
    #[arg(long, default_value = "BTC")]
    symbol: String,
}

fn main() -> Result<()> {
    println!("=== Hyperliquid L2 Book Debug ===\n");

    let args = Args::parse();

    if let Some(file_path) = args.single_file {
        println!("Testing single file: {:?}", file_path);
        println!("Symbol: {}\n", args.symbol);

        match read_hyperliquid_l2book_lz4(&file_path, &args.symbol) {
            Ok(df) => {
                println!("✓ Successfully read file");
                println!("  Shape: {:?}", df.shape());
                println!("  Schema: {:?}", df.schema());
                println!("\nFirst 5 rows:");
                println!("{}", df.head(Some(5)));
            }
            Err(e) => {
                eprintln!("✗ Failed to read file:");
                eprintln!("  Error: {:?}", e);
                return Err(e);
            }
        }
    } else {
        let date = NaiveDate::parse_from_str(&args.date, "%Y-%m-%d")?;
        println!("Testing AGGREGATED function (same as Python binding)");
        println!("Date: {}", date);
        println!("Raw data path: {:?}\n", args.raw_data_path);

        // First, scan what files exist
        let date_str = date.format("%Y%m%d").to_string();
        let mut total_files = 0;
        for hour in 0..24 {
            let hour_path = args.raw_data_path
                .join(&date_str)
                .join(hour.to_string())
                .join("l2Book");
            if let Ok(entries) = std::fs::read_dir(&hour_path) {
                let count = entries
                    .filter_map(|e| e.ok())
                    .filter(|e| e.path().extension().and_then(|s| s.to_str()) == Some("lz4"))
                    .count();
                if count > 0 {
                    println!("  Hour {:2}: {} files", hour, count);
                    total_files += count;
                }
            }
        }
        println!("\nTotal files: {}\n", total_files);

        if total_files == 0 {
            eprintln!("No files found for date {}", date);
            return Ok(());
        }

        println!("Calling read_hyperliquid_l2book_bydate()...");
        match read_hyperliquid_l2book_bydate(&args.raw_data_path, date) {
            Ok(df) => {
                println!("\n✓ SUCCESS: Aggregated function completed");
                println!("  Shape: {:?}", df.shape());
                println!("  Schema: {:?}", df.schema());
                println!("\nFirst 5 rows:");
                println!("{}", df.head(Some(5)));
                println!("\nLast 5 rows:");
                println!("{}", df.tail(Some(5)));
            }
            Err(e) => {
                eprintln!("\n✗ FAILED: Aggregated function error");
                eprintln!("  Error: {:?}", e);
                return Err(e);
            }
        }
    }

    Ok(())
}
