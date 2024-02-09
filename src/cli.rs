// In cli.rs
use crate::Config;
use clap::{Arg, Command};
use std::env;
use std::fs::{self, ReadDir};
use std::path::{Path, PathBuf};

pub fn parse_args() -> Config {
    let matches = Command::new("S3 File Combiner")
        .version("1.0")
        .author("Lilith River lilith@imazen.io")
        .about("Concatenates consecutive S3 (typically log) files with high concurrency and resiliency.")
        .arg(
            Arg::new("bucket")
                .long("bucket")
                .help("Specifies the S3 bucket name")
                .required(true)
                .num_args(1),
        )
        .arg_required_else_help(true)
        .arg(
            Arg::new("region")
                .long("region")
                .help("Specifies the AWS region")
                .env("AWS_DEFAULT_REGION")
                .num_args(1),
        )
        .arg(
            Arg::new("output-directory")
                .long("output-directory")
                .help("Specifies the output directory")
                .num_args(1),
        )
        .arg(
            Arg::new("start-after")
                .long("start-after")
                .help("Specifies the starting point (blob name) for log file fetching")
                .num_args(1),
        )
        .arg(
            Arg::new("max-connections")
                .long("max-connections")
                .help("Specifies the maximum number of concurrent connections")
                .default_value("2000")
                .num_args(1),
        )
        .arg(
            Arg::new("target-size-kb")
                .long("target-size-kb")
                .help("Specifies the target size of each output file chunk in kilobytes")
                .default_value("51200") // Default to 50MB in KB
                .num_args(1),
        )
        .arg(
            Arg::new("access-key")
                .long("access-key")
                .help("AWS access key ID")
                .num_args(1)
                .env("AWS_ACCESS_KEY_ID"),
        )
        .arg(
            Arg::new("secret-key")
                .long("secret-key")
                .help("AWS secret access key")
                .num_args(1)
                //.hide_env_values(true)
                .env("AWS_SECRET_ACCESS_KEY"),
        )
        .get_matches();

    let dir = matches
        .get_one::<PathBuf>("output-directory")
        .cloned()
        .unwrap_or_else(|| {
            env::current_dir()
                .unwrap()
                .join(matches.get_one::<String>("bucket").cloned().unwrap())
        });
    let start_at = matches
        .get_one::<String>("start-after")
        .cloned()
        .or_else(|| last_file_in_directory(&dir));

    Config {
        bucket: matches.get_one::<String>("bucket").cloned().unwrap(),
        region: matches
            .get_one::<String>("region")
            .cloned()
            .or_else(|| env::var("AWS_DEFAULT_REGION").ok()),
        output_directory: dir,
        start_at,
        max_connections: *matches.get_one("max-connections").unwrap(),
        target_size_kb: *matches.get_one("target-size-kb").unwrap(), // Convert to bytes
        access_key: matches
            .get_one::<String>("access-key")
            .cloned()
            .unwrap_or_else(|| env::var("AWS_ACCESS_KEY_ID").expect("AWS access key ID not set")),
        secret_key: matches
            .get_one::<String>("secret-key")
            .cloned()
            .unwrap_or_else(|| {
                env::var("AWS_SECRET_ACCESS_KEY").expect("AWS secret access key not set")
            }),
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

        let mut files: Vec<PathBuf> = read_dir
            .filter_map(|entry| entry.ok()) // Filter out Err results
            .map(|entry| entry.path()) // Get the path of each entry
            .filter(|path| path.is_file()) // Keep only files
            .collect();

        // Sort the files alphabetically
        files.sort_unstable();

        // Get the last file's name
        if let Some(last_file_path) = files.last() {
            return last_file_path
                .file_name()
                .and_then(|name| name.to_str())
                .map(|s| s.to_string());
        }
    }
    Option::None
}
