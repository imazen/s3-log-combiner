use crate::cli::{Commands};
use clap::Parser;

mod cli;
mod fetch;
mod parsing;
mod telemetry;
mod license_blob;
mod fetcher;
mod process;
mod progress;

#[tokio::main]
async fn main() {
    let args = cli::Cli::parse();
    match args.command {
        Commands::Combine(fetch) => {
            fetch::fetch(fetch.prepare()).await;
        }
        Commands::Summarize(parse) => {
            let result = parsing::parse(parse).await;
            if result.is_err(){
                eprintln!("Error: {}", result.unwrap_err());
                std::process::exit(1);
            }
        }
    }
}
