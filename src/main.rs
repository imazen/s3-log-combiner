use crate::cli::Commands;
use clap::Parser;

mod cli;
mod fetch;
mod fetcher;
mod json_report;
mod license_blob;
mod log_syntax;
mod process;
mod progress;
mod recompress;
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
        Commands::Recompress(args) => {
            let result = tokio::task::spawn_blocking(move || recompress::run(args))
                .await
                .expect("recompress task panicked");
            if let Err(e) = result {
                eprintln!("Error: {}", e);
                std::process::exit(1);
            }
        }
    }
}
