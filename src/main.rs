use crate::cli::{last_file_in_directory, Commands, FetchArgs};
use crate::fetch::Config;
use clap::Parser;
use std::env;

mod cli;
mod fetch;
mod parsing;

#[tokio::main]
async fn main() {
    let args = cli::Cli::parse();
    match args.command {
        Commands::Fetch(fetch) => {
            fetch::fetch(get_fetch_config(fetch)).await;
        }
        Commands::Parse(parse) => {
            let result = parsing::parse(parse).await;
            if result.is_err(){
                eprintln!("Error: {}", result.unwrap_err());
                std::process::exit(1);
            }
        }
    }
}

pub fn get_fetch_config(args: FetchArgs) -> Config {
    let FetchArgs {
        bucket,
        region,
        output_directory,
        start_after,
        max_connections,
        target_size_kb,
        access_key,
        secret_key,
        quiet,
        verbose,
        retry,
        continue_on_fatal_error,
        mut clear_columns
    } = args;
    clear_columns.sort();
    let dir = output_directory.unwrap_or_else(|| env::current_dir().unwrap().join(&bucket));

    let start_at = start_after.or_else(|| last_file_in_directory(&dir));

    Config {
        bucket,
        region,
        output_directory: dir,
        start_at,
        max_connections,
        clear_columns,
        target_size_kb,
        access_key: access_key, //.expect("AWS access key ID not set"),
        secret_key: secret_key, //.expect("AWS secret access key not set"),
        quiet,
        verbose,
        retry,
        continue_on_fatal_error,
    }
}
