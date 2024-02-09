use crate::Config;
use clap::Parser;
use std::env;
use std::ffi::OsString;
use std::fs::{self, ReadDir};
use std::path::{Path, PathBuf};

/// Concatenates consecutive S3 (typically log) files with high concurrency and resiliency.
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct FetchArgs {
    /// Specifies the S3 bucket name
    #[arg(long, value_parser)]
    bucket: String,

    /// Specifies the AWS region
    #[arg(
        long,

        env = "AWS_DEFAULT_REGION",
        value_parser,
        default_value_t = ("us-west-2".to_string())
    )]
    region: String,

    /// Specifies the output directory
    #[arg(long, value_parser)]
    output_directory: Option<PathBuf>,

    /// Specifies the starting point (blob name) for log file fetching
    #[arg(long, value_parser)]
    start_after: Option<String>,

    /// Specifies the maximum number of concurrent connections
    #[arg(long, default_value_t = 20000, value_parser)]
    max_connections: usize,

    /// Specifies the target size of each output file chunk in kilobytes
    #[arg(long, default_value_t = 51200, value_parser)] // Default to 50MB in KB
    target_size_kb: usize,

    /// Retry count per file
    #[arg(long, default_value_t = 10, value_parser)] // Default to 50MB in KB
    retry: usize,

    /// AWS access key ID
    #[arg(long, env = "AWS_ACCESS_KEY_ID", value_parser)]
    access_key: Option<String>,

    /// AWS secret access key
    #[arg(long, env = "AWS_SECRET_ACCESS_KEY", value_parser)]
    secret_key: Option<String>,

    /// Quiet flag
    #[arg(long, short, action)]
    quiet: bool,

    /// Verbose flag
    #[arg(long, action)]
    verbose: bool,

    #[arg(long, action)]
    continue_on_fatal_error: bool,
}

struct ParseArgs {
    split_by_query_key: String,
}

pub fn parse_args() -> Config {
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
    } = FetchArgs::parse();

    let dir = output_directory.unwrap_or_else(|| env::current_dir().unwrap().join(&bucket));

    let start_at = start_after.or_else(|| last_file_in_directory(&dir));

    Config {
        bucket,
        region,
        output_directory: dir,
        start_at,
        max_connections,
        target_size_kb,
        access_key: access_key.expect("AWS access key ID not set"),
        secret_key: secret_key.expect("AWS secret access key not set"),
        quiet,
        verbose,
        retry,
        continue_on_fatal_error,
    }
}

fn last_file_in_directory<P: AsRef<Path>>(directory_path: P) -> Option<String> {
    let path = directory_path.as_ref();
    // Check if the directory exists
    if path.is_dir() {
        // Read the directory
        let read_dir: ReadDir = match fs::read_dir(path) {
            Ok(dir) => dir,
            Err(_) => return None,
        };

        let incomplete = OsString::from("incomplete");
        let error = OsString::from("err");

        let mut files: Vec<PathBuf> = read_dir
            .filter_map(|entry| entry.ok()) // Filter out Err results
            .map(|entry| entry.path()) // Get the path of each entry
            .filter(|path| path.is_file()) // Keep only files
            .collect();

        // Sort the files alphabetically
        files.sort_unstable();

        let mut fail = false;
        for path in files.iter() {
            if let Some(ref ext) = path.extension() {
                if ext.eq_ignore_ascii_case(&incomplete) {
                    eprintln!("Found errored batch {:?}, please clean up the errored and all subsequent files first.", &path);
                    fail = true;
                }
                if ext.eq_ignore_ascii_case(&error) {
                    eprintln!("Found incomplete batch {:?}, please clean up the incomplete and all subsequent files first.", &path);
                    fail = true;
                }
            }
        }
        if fail {
            std::process::exit(1);
        }

        // Get the last file's name
        if let Some(last_file_path) = files.last() {
            return last_file_path
                .file_name()
                .and_then(|name| name.to_str())
                .map(|s| s.to_string());
        }
    }
    None
}
