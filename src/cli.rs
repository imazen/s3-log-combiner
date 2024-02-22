use std::env;
use clap::{Args, Parser, Subcommand};
use std::ffi::OsString;
use std::fs::{self, ReadDir};
use std::path::{Path, PathBuf};

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
pub struct Cli {
    #[command(subcommand)]
    pub(crate) command: Commands,
}

#[derive(Subcommand, Debug)]
pub enum Commands {
    #[command(arg_required_else_help = true)]
    Combine(FetchArgs),
    #[command(arg_required_else_help = true)]
    Summarize(QueryParseArgs),
}

/// Concatenates consecutive S3 (typically log) files with high concurrency and resiliency.
#[derive(Debug, Clone, Args)]
pub struct FetchArgs {
    /// Specifies the S3 bucket name
    #[arg(long, value_parser)]
    pub from_bucket: String,

    #[arg(long, value_parser)]
    pub from_prefix: Option<String>,

    #[arg(long, value_parser)]
    pub to_bucket: Option<String>,

    #[arg(long, value_parser)]
    pub to_prefix: Option<String>,

    /// Specifies the AWS region
    #[arg(
        long,
        env = "AWS_DEFAULT_REGION",
        value_parser,
        default_value = "us-west-2"
    )]
    pub from_region: String,

    #[arg(
    long,
    env = "AWS_DEFAULT_REGION",
    value_parser,
    default_value = "us-west-2"
    )]
    pub to_region: String,

    /// Specifies the output directory
    #[arg(long, value_parser)]
    pub output_directory: Option<PathBuf>,

    /// Specifies the starting point (blob name) for log file fetching.
    /// Only blobs after the given key will be fetched
    /// Doesn't work with express directory buckets
    #[arg(long, value_parser)]
    pub resume_location: Option<String>,

    /// If a target bucket is specified, this key (appended to the to-prefix) will be used
    /// to read and write the last log file fetched and flushed
    #[arg(long, value_parser, default_value="start_after_bookmark")]
    pub track_resume_location_in_key: String,

    /// After successfully uploading a processed batch of logs,
    /// the originals will be deleted if all of their entries
    /// Are older that the given number of minutes.
    /// Additionally, the very last log file of a batch won't be deleted, since it's used
    /// as a bookmark.
    #[arg(long, default_value_t = 20000, value_parser)]
    pub delete_after_uploading_if_aged_minutes: Option<usize>,

    /// Specifies the maximum number of concurrent connections
    #[arg(long, default_value_t = 20000, value_parser)]
    pub max_fetch_connections: usize,

    /// Specifies how many kilobytes of data to ingest and process at a time.
    /// Memory usage is usually a multiple of this.
    #[arg(long, default_value_t = 51200, value_parser)] // Default to 50MB in KB
    pub fetch_batch_size_kb: usize,

    /// Replaces specified columns (by index) with '-'
    /// Defaults to 0, 5, 11,12,13,14, 18: bucket owner, Request ID, Bytes Sent, Object Size, Total Time, Turn Around Time, Host ID
    #[arg(long, default_values_t = vec![0, 5, 11,12,13,14, 18], value_parser)]
    pub clear_columns: Vec<u32>,

    /// When specified, only the listed columns will be retained, all others will be replaced with -
    /// Inverse of clear_columns
    #[arg(long, value_parser)]
    pub preserve_columns: Option<Vec<u32>>,

    /// When specified, columns will be deleted rather than replaced with '-'
    /// This changes the file format
    #[arg(long, action)]
    pub drop_cleared_columns: bool,

    // Uploaded blobs will be compressed
    #[arg(long, action)]
    pub compress: bool,

    /// Retry count per file fetch or push
    #[arg(long, default_value_t = 10, value_parser)] // Default to 50MB in KB
    pub retry: usize,

    /// AWS access key ID
    #[arg(long, env = "AWS_ACCESS_KEY_ID", value_parser)]
    pub access_key: String,

    /// AWS secret access key
    #[arg(long, env = "AWS_SECRET_ACCESS_KEY", value_parser)]
    pub secret_key: String,

    /// Quiet flag
    #[arg(long, short, action)]
    pub quiet: bool,

    /// Verbose flag
    #[arg(long, action)]
    pub verbose: bool,

    #[arg(long, action)]
    pub continue_on_fatal_error: bool,

    #[arg(long, action)]
    pub imazen_telemetry_processing: bool,
}

impl FetchArgs{
    pub fn prepare(mut self) -> FetchArgs{
        self.clear_columns.sort();
        if self.to_bucket.is_none() {
            self.output_directory = Option::from(self.output_directory.unwrap_or_else(|| env::current_dir().unwrap().join(&self.from_bucket)));
            if self.resume_location.is_none(){
                self.resume_location = last_file_in_directory(&self.output_directory.as_ref().unwrap());
            }
        }
        self
    }

}

#[derive(Args, Debug)]
pub struct QueryParseArgs {
    /// Specifies the output directory
    #[arg(long, short, value_parser)]
    pub output_directory: Option<PathBuf>,

    /// Specifies the input directories with files to parse
    #[arg(long, short, value_parser)]
    pub input: Vec<PathBuf>,

    // license_id
    #[arg(long, value_parser, default_value="license_id")]
    pub split_by_key: String,
    // manager_id
    #[arg(long, value_parser, default_value="manager_id")]
    pub keep_unique_of_key: String,

    #[arg(long, default_value = "REST.GET.OBJECT", value_parser)]
    pub filter_operation_type: String,

    #[arg(long, default_value = "v1/licenses/latest/", value_parser)]
    pub filter_key_prefix: String, // autoremove bucket name from key if present

                                   //&reporting_version=4
                                   //&proc_guid
                                   //& proc_working_set_mb
                                   //&proc_info_version=4.2.8
                                   //&h_logical_cores=2
                                   //&h_mac_digest=RZT9gciMsRVjZnoADuJTcA
                                   //&p=&p=
                                   //40b77a18bbb673482588cba24c8b41dab0667c38cbb9f934403639af5d324c02 licenses.imazen.net [25/Dec/2023:23:47:48 +0000] 54.208.251.139 - VHN3S4DWQPAXYFGH REST.GET.OBJECT v1/licenses/latest/04d04ad5881bcf7ac42f35ad3b27377778addc7bc77f111972b25d28282f5d3b.txt "GET /licenses.imazen.net/v1/licenses/latest/04d04ad5881bcf7ac42f35ad3b27377778addc7bc77f111972b25d28282f5d3b.txt?license_id=1032462609&manager_id=2e71cac1-afa3-4f0c-b304-a89ffe64bae0&first_heartbeat=1703548057&new_heartbeats=2&total_heartbeats=2&reporting_version=4&proc_64=1&proc_guid=i-x39UaTmkmOGLK5BCTWHw&proc_sys_dotnet=4.7%20or%20later&proc_iis=8.5&proc_integrated_pipeline=1&proc_id_hash=0V7_gghM&proc_asyncmodule=0&proc_working_set_mb=368&proc_git_commit=ee9c96cb&proc_info_version=4.2.8&proc_file_version=4.2.8.1168&proc_apppath_hash=GNaz8VUj&h_logical_cores=2
    // &h_mac_digest=RZT9gciMsRVjZnoADuJTcA&h_os64=1&h_network_drives_count=0&h_other_drives_count=0&h_fixed_drives_count=2
    // &h_fixed_drive=NTFS%2C111%2C268&h_fixed_drive=NTFS*%2C168%2C274
    // &p=SizeLimiting&p=Ronaele.UI.Web.CustomS3Reader&p=DiskCache
    // &provider_prefix=%2Fproduct-images%2F&provider_flags=1%2C0%2C0%2C0%2C1%2C1%2C1&diskcache_autoclean=0&diskcache_asyncwrites=0&diskcache_subfolders=8192&diskcache_network_drive=0&diskcache_filesystem=NTFS&diskcache_drive_avail=168231051264&diskcache_drive_total=274874757120
    // &diskcache_virtualpath=%2Fimagecache
                                   //&counter_update_failed=0&jobs_completed_total=0
                                   //&jobs_completed_per_second_max=0&jobs_completed_per_minute_max=0
                                   //&jobs_completed_per_15_mins_max=0&jobs_completed_per_hour_max=0
                                   //&encoded_pixels_total=0&encoded_pixels_per_second_max=0
                                   //&encoded_pixels_per_minute_max=0&encoded_pixels_per_15_mins_max=0
                                   //&encoded_pixels_per_hour_max=0&decoded_pixels_total=0
                                   //&decoded_pixels_per_second_max=0&decoded_pixels_per_minute_max=0
                                   //&decoded_pixels_per_15_mins_max=0&decoded_pixels_per_hour_max=0
                                   //&blob_read_bytes_total=0&blob_read_bytes_per_second_max=0
                                   //&blob_read_bytes_per_minute_max=0&blob_read_bytes_per_15_mins_max=0
                                   //&blob_read_bytes_per_hour_max=0&blob_reads_total=0&blob_reads_per_second_max=0\
                                   //&blob_reads_per_minute_max=0&blob_reads_per_15_mins_max=0&blob_reads_per_hour_max=0
                                   //&encode_times_5th=0&encode_times_25th=0&encode_times_50th=0&encode_times_75th=0\
                                   //&encode_times_95th=0&encode_times_100th=0&output_aspect_ratio_5th=0&output_aspect_ratio_25th=0
                                   //&output_aspect_ratio_50th=0&output_aspect_ratio_75th=0&output_aspect_ratio_95th=0
                                   //&output_aspect_ratio_100th=0&source_aspect_ratio_5th=0&source_aspect_ratio_25th=0\
                                   //&source_aspect_ratio_50th=0&source_aspect_ratio_75th=0&source_aspect_ratio_95th=0
                                   //&source_aspect_ratio_100th=0&output_width_5th=0&output_width_25th=0&output_width_50th=0
                                   // &output_width_75th=0&output_width_95th=0&output_width_100th=0&output_pixels_5th=0&
                                   // output_pixels_25th=0&output_pixels_50th=0&output_pixels_75th=0&output_pixels_95th=0
                                   // &output_pixels_100th=0&source_pixels_5th=0&source_pixels_25th=0&source_pixels_50th=0
                                   // &source_pixels_75th=0&source_pixels_95th=0&source_pixels_100th=0&decode_times_5th=0
                                   // &decode_times_25th=0&decode_times_50th=0&decode_times_75th=0&decode_times_95th=0
                                   // &decode_times_100th=0&scaling_ratio_5th=0&scaling_ratio_25th=0&scaling_ratio_50th=0
                                   // &scaling_ratio_75th=0&scaling_ratio_95th=0&scaling_ratio_100th=0&job_other_time_5th=0
                                   // &job_other_time_25th=0&job_other_time_50th=0&job_other_time_75th=0&job_other_time_95th=
                                   // 0&job_other_time_100th=0&job_times_5th=0&job_times_25th=0&job_times_50th=0&job_times_75th=0
                                   // &job_times_95th=0&job_times_100th=0&source_height_5th=0&source_height_25th=0&source_height_50th=0
                                   // &source_height_75th=0&source_height_95th=0&source_height_100th=0&source_width_5th=0&source_width_25th=0
                                   // &source_width_50th=0&source_width_75th=0&source_width_95th=0&source_width_100th=0
                                   // &collect_info_times_5th=0&collect_info_times_25th=0&collect_info_times_50th=0&collect_info_times_75th=0
                                   // &collect_info_times_95th=0&collect_info_times_100th=0&output_height_5th=0&output_height_25th=
                                   // &output_height_50th=0&output_height_75th=0&output_height_95th=0&output_height_100th=0
                                   // &blob_read_times_5th=0&blob_read_times_25th=0&blob_read_times_50th=0&blob_read_times_75th=0
                                   // &blob_read_times_95th=0&blob_read_times_100th=0&image_domains=&page_domains=&query_keys=
                                   // &extra_job_query_keys= HTTP/1.1" 200 - 716 716 17 16 "-" "-" -
                                   // kjglkhvlkgY= - ECDHE-RSA-AES128-SHA - s3-us-west-2.amazonaws.com TLSv1.2 - Yes
                                   //
}

#[test]
fn verify_cli() {
    use clap::CommandFactory;
    Cli::command().debug_assert()
}

pub fn last_file_in_directory<P: AsRef<Path>>(directory_path: P) -> Option<String> {
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
