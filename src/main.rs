use crate::cli::Commands;
use clap::Parser;

mod cli;
mod fetch;
mod fetcher;
mod license_blob;
mod log_syntax;
mod process;
mod progress;
mod resume;
mod summarize;
mod telemetry;
mod util;

#[tokio::main]
async fn main() {
    let args = cli::Cli::parse();
    match args.command {
        Commands::Combine(fetch) => {
            fetch::fetch(fetch.prepare()).await;
        }
        Commands::Summarize(parse) => {
            let result = summarize::parse(parse).await;
            if result.is_err() {
                eprintln!("Error: {}", result.unwrap_err());
                std::process::exit(1);
            }
        }
    }
}
