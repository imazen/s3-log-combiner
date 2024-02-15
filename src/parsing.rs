use std::collections::HashMap;
use std::{env, fs};
use std::path::{Path, PathBuf};
use std::process;

use chrono::{Datelike, DateTime, FixedOffset, TimeZone, Utc};
use tokio::fs::File;
use tokio::io::BufWriter;

use crate::cli::QueryParseArgs;

fn expand_input_filenames(files_and_dirs: Vec<PathBuf>) -> Vec<PathBuf> {
    // If there are no inputs, exit with an error
    if files_and_dirs.is_empty() {
        eprintln!("Error: No input files or directories provided.");
        process::exit(1);
    }

    let mut expanded_files = Vec::new();

    // Iterate through each file or directory
    for file_or_dir in files_and_dirs {
        // Check if it's a file
        if file_or_dir.is_file() {
            // Add the file to the list if it doesn't have .incomplete or .err extension
            let extension = file_or_dir.extension().unwrap_or_default().to_string_lossy().to_lowercase();
            if extension != "incomplete" && extension != "err" {
                expanded_files.push(file_or_dir);
            }
        } else if file_or_dir.is_dir() {
            // If it's a directory, expand it to include all files (excluding .incomplete and .err)
            if let Ok(entries) = fs::read_dir(&file_or_dir) {
                for entry in entries {
                    if let Ok(entry) = entry {
                        let path = entry.path();
                        if path.is_file() {
                            let extension = path.extension().unwrap_or_default().to_string_lossy().to_lowercase();
                            if extension != "incomplete" && extension != "err" {
                                expanded_files.push(path);
                            }
                        }
                    }
                }
            } else {
                eprintln!("Error: Unable to read directory {:?}", file_or_dir);
                process::exit(1);
            }
        } else {
            // If it's neither a file nor a directory, print an error and exit
            eprintln!("Error: {:?} is not a valid file or directory.", file_or_dir);
            process::exit(1);
        }
    }

    // Sort the resulting files alphabetically
    expanded_files.sort();

    expanded_files
}

//&decoded_pixels_total=0
//&decoded_pixels_per_second_max=0&decoded_pixels_per_minute_max=0
//&decoded_pixels_per_15_mins_max=0&decoded_pixels_per_hour_max=0
#[derive(Debug,Copy,Clone)]
struct ThroughputStat{
    name: &'static str,
    total: u32,
    per_sec_peak: u32,
    per_min_peak: u32,
    per_hour_peak: u32,
    per_15min_peak: u32
}

impl ThroughputStat {
    fn parse(pairs: &[(&str, &str)], name: &'static str) -> Option<Self> {
        let total = find_and_parse_2(pairs, name, "_total")?;
        let per_sec_peak = find_and_parse_2(pairs, name, "_per_second_max")?;
        let per_min_peak = find_and_parse_2(pairs, name, "_per_minute_max")?;
        let per_hour_peak = find_and_parse_2(pairs, name, "_per_hour_max")?;
        let per_15min_peak = find_and_parse_2(pairs, name, "_per_15_mins_max")?;

        Some(ThroughputStat {
            name,
            total,
            per_sec_peak,
            per_min_peak,
            per_hour_peak,
            per_15min_peak,
        })
    }
}

//
// &blob_read_times_5th=0&blob_read_times_25th=0&blob_read_times_50th=0&blob_read_times_75th=0
// &blob_read_times_95th=0&blob_read_times_100th=0
#[derive(Debug,Copy,Clone)]
struct PercentileStat{
    name: &'static str,
    p5: u32,
    p25: u32,
    p50: u32,
    p75: u32,
    p95: u32,
    p100: u32
}

impl PercentileStat {
    fn parse(pairs: &[(&str, &str)], name: &'static str) -> Option<Self> {
        let p5 = find_and_parse_2(pairs, name, "_5th")?;
        let p25 = find_and_parse_2(pairs, name, "_25th")?;
        let p50 = find_and_parse_2(pairs, name, "_50th")?;
        let p75 = find_and_parse_2(pairs, name, "_75th")?;
        let p95 = find_and_parse_2(pairs, name, "_95th")?;
        let p100 = find_and_parse_2(pairs, name, "_100th")?;

        Some(PercentileStat {
            name,
            p5,
            p25,
            p50,
            p75,
            p95,
            p100,
        })
    }
}


//&proc_guid
//&proc_working_set_mb
//&proc_info_version=4.2.8
//&proc_id_hash=
//&proc_working_set_mb
//&proc_iis=
#[derive(Debug,Clone)]
struct ProcessAttrs{
    // proc_guid=
    guid: String,
    // proc_info_version=
    info_version: String,
    file_version: String,
    // proc_id_hash
    process_id_hash: Option<String>,
    //proc_default_commands
    default_commands: Option<String>,
    //&proc_git_commit
    git_commit: Option<String>,

    //&proc_apppath_hash: (6 char hash)
    app_path_hash: Option<String>,
    // proc_64=0/1
    is64bit:bool,
}
impl ProcessAttrs {
    fn parse(pairs: &[(&str, &str)]) -> Self {
        ProcessAttrs {
            guid: find_value(pairs, "proc_guid").unwrap_or_default().to_string(),
            info_version: find_value(pairs, "proc_info_version").unwrap_or_default().to_string(),
            file_version: find_value(pairs, "proc_file_version").unwrap_or_default().to_string(),
            process_id_hash: find_value(pairs, "proc_id_hash").map(|s| s.to_string()),
            default_commands: find_value(pairs, "proc_default_commands").map(|s| s.to_string()),
            git_commit: find_value(pairs, "proc_git_commit").map(|s| s.to_string()),
            app_path_hash: find_value(pairs, "proc_apppath_hash").map(|s| s.to_string()),
            is64bit: parse_bool(pairs, "proc_64").unwrap_or(false),
        }
    }
}


//&h_logical_cores=2
//&h_mac_digest=RZT9gciMsRVasfJTcA
// &h_os64=1
// &h_network_drives_count=0
// &h_other_drives_count=0
// &h_fixed_drives_count=2
// &h_fixed_drive=NTFS%2C111%2C268
// &h_fixed_drive=NTFS*%2C168%2C274
#[derive(Debug,Clone)]
struct HardwareAttrs{
    logical_cores: i32,
    mac_digest: String,
    os64bit: bool,
    network_drive_count: i32,
    fixed_drive_count: i32,
    other_drive_count: i32,
    fixed_drives: Vec<Drive>,
    other_drives: Vec<Drive>,
    network_drives: Vec<Drive>,
}

impl HardwareAttrs {
    fn parse(pairs: &[(&str, &str)]) -> Self {
        // A function to parse and decode drives from their key identifier
        let parse_drives = |key: &str| -> Vec<Drive> {
            pairs.iter()
                .filter_map(|&(k, v)| {
                    if k == key {
                        // Decode each drive string exactly once before attempting to parse
                        Drive::parse(&url_decode(v))
                    } else {
                        None
                    }
                })
                .collect()
        };

        HardwareAttrs {
            logical_cores: find_and_parse(pairs, "h_logical_cores").unwrap_or(0),
            mac_digest: find_value(pairs, "h_mac_digest").unwrap_or_default().to_string(),
            os64bit: parse_bool(pairs, "h_os64").unwrap_or(false),
            network_drive_count: find_and_parse(pairs, "h_network_drives_count").unwrap_or(0),
            fixed_drive_count: find_and_parse(pairs, "h_fixed_drives_count").unwrap_or(0),
            other_drive_count: find_and_parse(pairs, "h_other_drives_count").unwrap_or(0),
            fixed_drives: parse_drives("h_fixed_drive"),
            other_drives: parse_drives("h_other_drive"),
            network_drives: parse_drives("h_network_drive"),
        }
    }
}


//Parse from comma delimited format filesystem(*appdrive),availgb,totalgb.
//Example value: "NTFS*%2C168%2C274"
#[derive(Debug,Clone)]
struct Drive{
    app_drive: bool, // if the filesystem ends with *, trim it and set this true
    filesystem: String,
    available_gb: i32,
    total_gb: i32
}
impl Drive {
    fn parse(decoded_input: &str) -> Option<Self> {
        // Split the decoded string by ',' to extract the parts
        let parts: Vec<&str> = decoded_input.split(',').collect();
        if parts.len() != 3 { return None; }

        // Extract and process the filesystem part
        let filesystem_info = parts[0];
        let app_drive = filesystem_info.ends_with('*');
        let filesystem = if app_drive {
            filesystem_info.trim_end_matches('*')
        } else {
            filesystem_info
        };

        // Parse the available and total GB
        let available_gb = parts[1].parse().ok()?;
        let total_gb = parts[2].parse().ok()?;

        Some(Drive {
            app_drive,
            filesystem: filesystem.to_string(),
            available_gb,
            total_gb,
        })
    }
}



#[derive(Debug,Clone)]
struct PipelineStats{
    // defaults to zero
    source_file_ext_tiff: i32,
    source_file_ext_tif: i32,
    source_file_ext_bmp: i32,
    source_file_ext_jpg: i32,
    source_file_ext_png: i32,
    //&module_response_ext_gif=3&counter_update_failed=0&postauth_ok=662884
    // &module_response_ext_jpg=11944&postauth_errors_ImageMissingException=711&postauth_errors_ImageCorruptedException=163&source_multiple_x8=104&source_multiple_8x8=99&module_response_ext_png=11150&postauth_errors_ImageProcessingException=1&source_file_ext_jpg=569&source_multiple_16x16=6&postauthjob_ok=673&source_multiple_8x=112&module_response_ext_webp=488819&source_file_ext_png=103&postauth_errors_SizeLimitException=3331&postauth_404_=2387&postauthjob_errors=3494&source_file_ext_gif=1&postauth_errors=4206&postauthjob_errors_SizeLimitException=3331&postauthjob_errors_ImageCorruptedException=163&source_multiple_4x4=641
    //
    //
    // &jobs_completed_total=673&jobs_completed_per_second_max=12&jobs_completed_per_minute_max=45&jobs_completed_per_15_mins_max=258&jobs_completed_per_hour_max=341&
}
impl PipelineStats {
    fn parse(pairs: &[(&str, &str)]) -> Self {
        PipelineStats {
            source_file_ext_tiff: find_and_parse(pairs, "source_file_ext_tiff").unwrap_or(0),
            source_file_ext_tif: find_and_parse(pairs, "source_file_ext_tif").unwrap_or(0),
            source_file_ext_bmp: find_and_parse(pairs, "source_file_ext_bmp").unwrap_or(0),
            source_file_ext_jpg: find_and_parse(pairs, "source_file_ext_jpg").unwrap_or(0),
            source_file_ext_png: find_and_parse(pairs, "source_file_ext_png").unwrap_or(0),
            // Add additional fields here, following the same pattern.
        }
    }
}
#[derive(Debug,Clone)]
struct Report{
    ip_str: String,
    //&manager_id
    manager_id: String,
    logged_at: DateTime<Utc>,
    //&reporting_version=4/100
    reporting_version: i32,
    //&truncated=true
    report_truncated: bool,
    //&total_heartbeats=x
    total_heartbeats: i64,
    //&first_heartbeat=(seconds since jan 1 1970)
    first_heartbeat: Option<DateTime<Utc>>,
    //&imageflow=1 (default 0)
    is_imageflow: bool,
    //&p=v1&p=v2 (duplicated)
    plugins: Vec<String>,
    //&query_keys=a,b,c (comma delimited)
    query_keys: Vec<String>,
    //&extra_job_query_keys=a,b,c
    extra_job_query_keys: Vec<String>,
    //&image_domains=v,s,c
    image_domains: Vec<String>,
    //&page_domains=v,s,c
    page_domains: Vec<String>,
    //&enabled_cache=x
    enabled_cache: Option<String>,
    pipeline: PipelineStats,
    hardware: HardwareAttrs,
    process: ProcessAttrs,
    jobs_completed_total: Option<u64>,
    jobs_completed: Option<ThroughputStat>,
    encoded_pixels: Option<ThroughputStat>,
    decoded_pixels: Option<ThroughputStat>,
    blob_read_bytes: Option<ThroughputStat>,
    blob_reads: Option<ThroughputStat>,
    encode_times: Option<PercentileStat>,
    decode_times: Option<PercentileStat>,
    job_times: Option<PercentileStat>,
    blob_read_times: Option<PercentileStat>,
}


// if (streamCache != null) query.AddString("stream_cache", streamCache.GetType().Name);
// query.Add("map_web_root", options.MapWebRoot);
// query.Add("use_presets_exclusively", options.UsePresetsExclusively);
// query.Add("request_signing_default", options.RequestSignatureOptions?.DefaultRequirement.ToString() ?? "never");
// query.Add("default_cache_control", options.DefaultCacheControlString);

impl Report {

    fn parse_line(line: &LogLine) -> Option<Report> {
        line.request_query.map(|q| Report::parse(q, line.time, line.ip_str))
    }
    fn parse(query: &str, log_time: DateTime<Utc>, ip: &str) -> Self {
        let pairs = parse_query_string(query);

        // Utilize the helper functions to extract and parse various fields
        Report {
            ip_str: ip.to_string(),
            manager_id: find_value(&pairs, "manager_id").unwrap_or_default(),
            logged_at: log_time,
            reporting_version: find_and_parse(&pairs, "reporting_version").unwrap_or(0),
            report_truncated: parse_bool(&pairs, "truncated").unwrap_or(false),
            total_heartbeats: find_and_parse(&pairs, "total_heartbeats").unwrap_or(0),
            first_heartbeat: find_and_parse(&pairs, "first_heartbeat")
                .map(|v| Utc.timestamp(v, 0)),
            is_imageflow: parse_bool(&pairs, "imageflow").unwrap_or(false),
            plugins: find_repeated_values_decode(&pairs, "p"),
            query_keys: parse_comma_delimited(find_value(&pairs, "query_keys").unwrap_or_default().as_str()),
            extra_job_query_keys: parse_comma_delimited(find_value(&pairs, "extra_job_query_keys").unwrap_or_default().as_str()),
            image_domains: parse_comma_delimited(find_value(&pairs, "image_domains").unwrap_or_default().as_str()),
            page_domains: parse_comma_delimited(find_value(&pairs, "page_domains").unwrap_or_default().as_str()),
            enabled_cache: find_value(&pairs, "enabled_cache"),
            hardware: HardwareAttrs::parse(&pairs),
            process: ProcessAttrs::parse(&pairs),
            pipeline: PipelineStats::parse(&pairs),
            jobs_completed_total: find_and_parse(&pairs, "jobs_completed_total"),
            jobs_completed: ThroughputStat::parse(&pairs, "jobs_completed"),
            encoded_pixels: ThroughputStat::parse(&pairs, "encoded_pixels"),
            decoded_pixels: ThroughputStat::parse(&pairs, "decoded_pixels"),
            blob_read_bytes: ThroughputStat::parse(&pairs, "blob_read_bytes"),
            blob_reads: ThroughputStat::parse(&pairs, "blob_reads"),
            encode_times: PercentileStat::parse(&pairs, "encode_times"),
            decode_times: PercentileStat::parse(&pairs, "decode_times"),
            job_times: PercentileStat::parse(&pairs, "job_times"),
            blob_read_times: PercentileStat::parse(&pairs, "blob_read_times"),
            // Initialize other fields as needed...
        }
    }
}

#[derive(Debug,Clone)]
pub struct Summary {
    image_domains: Vec<String>,
    page_domains: Vec<String>,
    reporter_ips: Vec<String>,
    query_keys: Vec<String>,
    extra_job_query_keys: Vec<String>,
    plugins: Vec<String>,
    // format [cores=logical_cores][x64|x86](mac digest string)
    machines: Vec<String>,
    jobs_completed_total: u64,
    encoded_pixels_total: u64,
    decoded_pixels_total: u64,
    info_versions: Vec<String>,
    default_command_sets: Vec<String>,
    last_full_report_from: Option<DateTime<Utc>>
}

impl Summary {
    fn default() -> Summary {
        Summary{
            image_domains: vec![],
            page_domains: vec![],
            reporter_ips: vec![],
            query_keys: vec![],
            extra_job_query_keys: vec![],
            plugins: vec![],
            machines: vec![],
            jobs_completed_total: 0,
            encoded_pixels_total: 0,
            decoded_pixels_total: 0,
            info_versions: vec![],
            default_command_sets: vec![],
            last_full_report_from: None
        }
    }
}

impl Summary {
    // Adds a unique item to a vector if it does not already contain it
    fn add_unique<T: PartialEq + Clone>(vec: &mut Vec<T>, item: T) {
        if !vec.contains(&item) {
            vec.push(item);
        }
    }

    fn add_from(&mut self, r: &Report, full_report: bool) {
        if full_report {
            self.last_full_report_from
                = Some(self.last_full_report_from.map(|v| v.max(r.logged_at))
                .unwrap_or(r.logged_at));
        }
        // Utilize Summary::add_unique for adding unique items
        r.image_domains.iter().for_each(|d| Summary::add_unique(&mut self.image_domains, d.clone()));
        r.page_domains.iter().for_each(|d| Summary::add_unique(&mut self.page_domains, d.clone()));
        Summary::add_unique(&mut self.reporter_ips, r.ip_str.clone());
        r.query_keys.iter().for_each(|k| Summary::add_unique(&mut self.query_keys, k.clone()));
        r.extra_job_query_keys.iter().for_each(|k| Summary::add_unique(&mut self.extra_job_query_keys, k.clone()));
        r.plugins.iter().for_each(|p| Summary::add_unique(&mut self.plugins, p.clone()));

        // Format and add unique machine information
        let machine_info = format!("[cores={}][{}]{})", r.hardware.logical_cores, if r.hardware.os64bit { "x64" } else { "x86" }, r.hardware.mac_digest);
        Summary::add_unique(&mut self.machines, machine_info);
        if full_report {
            // Utilize sum_values for aggregating totals
            if let Some(total) = r.jobs_completed_total {
                self.jobs_completed_total += total;
            }
            if let Some(encoded_pixels) = &r.encoded_pixels {
                self.encoded_pixels_total += encoded_pixels.total as u64;
            }
            if let Some(decoded_pixels) = &r.decoded_pixels {
                self.decoded_pixels_total += decoded_pixels.total as u64;
            }
        }

        // Add unique info versions and default command sets
        Summary::add_unique(&mut self.info_versions, r.process.info_version.clone());
        if let Some(commands) = &r.process.default_commands {
            Summary::add_unique(&mut self.default_command_sets, commands.clone());
        }
    }
}


fn url_decode(input: &str) -> String {
    percent_encoding::percent_decode(input.as_bytes())
        .decode_utf8_lossy()
        .into_owned()
}
// Finds the first value for the given key and attempts to parse it into the desired type.
fn find_and_parse<T: std::str::FromStr>(pairs: &[(&str, &str)], key: &str) -> Option<T> {
    pairs.iter()
        .find(|&&(k, _)| k == key)
        .and_then(|&(_, v)| v.parse().ok())
}
fn find_and_parse_2<T: std::str::FromStr>(pairs: &[(&str, &str)], key_part_a: &str, key_part_b: &str) -> Option<T> {
    pairs.iter()
        .find(|&&(k, _)| k.starts_with(key_part_a) && k.ends_with(key_part_b) && k.len() == key_part_a.len() + key_part_b.len())
        .and_then(|&(_, v)| v.parse().ok())
}

fn find_value<'a>(pairs: &'a [(&str, &str)], key: &'a str) -> Option<String> {
    pairs.iter()
        .find(|&&(k, _)| k == key)
        .map(|&(_, v)| v.to_string())
}
fn find_repeated_values_decode<'a>(pairs: &'a [(&str, &str)], key: &'a str) -> Vec<String> {
    pairs.iter()
        .filter(|&&(k, _)| k == key)
        .map(|&(_, v)| url_decode(v))
        .collect()
}

// Specialized function for parsing boolean values represented as "0" or "1".
fn parse_bool(pairs: &[(&str, &str)], key: &str) -> Option<bool> {
    find_and_parse::<i32>(pairs, key).map(|value| value != 0)
}

// Parses a delimited string into a Vec of a specified type. Caller should already have url decoded
fn parse_delimited<T: std::str::FromStr>(input: &str, delimiter: char) -> Vec<T> {
    input.split(delimiter)
        .filter_map(|item| item.parse().ok())
        .collect()
}

fn parse_query_string(input: &str) -> Vec<(&str, &str)> {
    let without_q = if input.starts_with('?') { &input[1..]} else { input };
    without_q.split('&')
        .filter_map(|part| {
            let mut parts = part.splitn(2, '=');
            if let Some(key) = parts.next() {
                let value = parts.next().unwrap_or("");
                Some((key, value))
            } else {
                None
            }
        })
        .collect()
}

fn parse_comma_delimited(input: &str) -> Vec<String> {
    let decoded = url_decode(input);
    decoded.split(',')
        .map(|s| s.to_string())
        .collect()
}


#[derive(Debug, Clone)]
struct LogLine<'a> {
    //[25/Dec/2023:23:42:03 +0000]
    time: DateTime<Utc>,
    ip_str: &'a str,
    operation: &'a str,
    bucket: &'a str,
    key: &'a str,
    response_status_str: &'a str,
    request_path: &'a str,
    request_query: Option<&'a str>,
    split_value: Option<&'a str>,
    unique_value: Option<&'a str>,
    truncated: bool,
}

struct SplitWhitespaceBrackets<'a> {
    s: &'a str,
    current_pos: usize,
    bracket_depth: usize,
    in_quotes: bool,
}

impl<'a> SplitWhitespaceBrackets<'a> {
    fn new(s: &'a str) -> Self {
        SplitWhitespaceBrackets {
            s,
            current_pos: 0,
            bracket_depth: 0,
            in_quotes: false
        }
    }
}


impl<'a> Iterator for SplitWhitespaceBrackets<'a> {
    type Item = &'a str;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current_pos >= self.s.len() {
            return None;
        }

        let start = self.current_pos;
        let mut end = start;

        while self.current_pos < self.s.len() {
            let c = self.s[self.current_pos..].chars().next().unwrap();

            match c {
                '"' => self.in_quotes = !self.in_quotes,
                '[' => self.bracket_depth += 1,
                ']' if self.bracket_depth > 0 => self.bracket_depth -= 1,
                ' ' if self.bracket_depth <= 0 && !self.in_quotes => {
                    end = self.current_pos;
                    self.bracket_depth = 0;
                    self.current_pos += 1; // Move past the current whitespace
                    // Handle consecutive whitespaces producing empty elements
                    if start == end {
                        return Some("");
                    }
                    return Some(&self.s[start..end]);
                },
                _ => {}
            }

            if self.bracket_depth == 0 && !self.in_quotes && c != ' ' {
                end = self.current_pos + 1; // Move end to the next character for non-whitespace
            }

            self.current_pos += 1; // Always move to the next character
        }

        // Handle the case where the last character(s) are not whitespace or are within brackets
        if start != self.current_pos || self.s.ends_with(' ') {
            return Some(&self.s[start..self.current_pos]);
        }

        None
    }
}

fn parse_s3_log_line<'a>(line: &'a str, split_by_key: &str, unique_key: &str) -> LogLine<'a> {
    let mut parts = SplitWhitespaceBrackets::new(line);


    let bucket = parts.nth(1).unwrap_or("");
    let mut time;
    if let Some(time_str) = parts.nth(0){
        let result= DateTime::parse_from_str(time_str,
                                        "[%d/%b/%Y:%H:%M:%S %z]");
        if let Err(e) = result{
            panic!("Error parsing date: '{time_str}'\n{e}");
        }
        time = result.unwrap().with_timezone(&Utc);

    }else{
        eprintln!("Error parsing line: {line}");
        std::process::exit(3);

    }

    let ip_str = parts.nth(0).unwrap_or(""); // Accounts for the previous next()
    let operation = parts.nth(2).unwrap_or("");
    let key = parts.nth(0).unwrap_or(""); // Skips requester ID
    let request_command = parts.nth(0).unwrap_or(""); // Skips to Request-URI
    let request_uri = request_command.split(' ').nth(1).unwrap_or("");

    let response_status_str = parts.next().unwrap_or("");

    let mut request_path = Some(request_uri);
    let mut request_query = None;
    let mut split_value = None;
    let mut unique_value = None;
    let mut truncated = false;

    // Parse the query string from the request URI
    if let Some(query_start) = request_uri.find('?') {
        let query_string = &request_uri[query_start + 1..]; // Skip '?' character
        let mut pairs = query_string.split('&');
        for pair in pairs {
            let mut parts = pair.split('=');
            if let Some(key) = parts.next() {
                if let Some(value) = parts.next() {
                    match key {
                        k if k == split_by_key => split_value = Some(value),
                        k if k == unique_key => unique_value = Some(value),
                        k if k == "truncated" && value != "false" => truncated = true,
                        _ => {}
                    }
                }
            }
        }
        request_query = Some(&request_uri[query_start..]);
        request_path = Some(&request_uri[..query_start]);
    }

    LogLine {
        time,
        ip_str,
        operation,
        bucket,
        key,
        response_status_str,
        request_path: request_path.unwrap_or(""),
        request_query,
        split_value,
        unique_value,
        truncated
    }
}

#[derive(Debug,Clone)]
struct SplitUnique{
    unique_id: String,
    last_full_report_from: DateTime<Utc>,
    last_full_report: Option<Report>,
    last_report_from: DateTime<Utc>,
    last_report: Option<Report>,
}

impl SplitUnique {
    fn default(id: String) -> SplitUnique {
        SplitUnique{
            unique_id: id,
            last_full_report_from: Default::default(),
            last_full_report: None,
            last_report_from: Default::default(),
            last_report: None,
        }
    }
}

impl SplitUnique{
    fn update_with(&mut self, line: &LogLine){
        if line.request_query.is_none() { return; }
        if line.time > self.last_report_from{
            self.last_report = Report::parse_line(&line)
        }
        self.last_report_from = line.time;

        if !line.truncated {
            if line.time > self.last_full_report_from {
                self.last_full_report = self.last_report.clone();
            }
            self.last_full_report_from = line.time;
        }
    }

}
//https://github.com/imazen/imageflow-dotnet-server/blob/c85beb9cd798b272ced56e2e7d903de82b0a8a31/src/Imazen.Common/Instrumentation/GlobalPerf.cs#L266
#[derive(Debug,Clone)]
struct SplitDataSink{
    split_segment: String,
    summary_path: PathBuf,
    split_id: String,
    split_date_from: DateTime<Utc>,
    split_date_to: DateTime<Utc>,
    uniques: HashMap<String, SplitUnique>,
    day_sink: bool,
}
impl SplitDataSink {
    // combine all the SplitUnique.last_full_report and last.report data
    // get all unique hmac fingerprints,
    // all unique page domains
    // all unique image domains
    // sum of jobs processed
    //
    
    fn summarize(&self) -> Summary{
        let mut s = Summary::default();
        self.uniques.iter().for_each(|(k,v)| {
            if let Some(ref r1) = v.last_full_report{
                s.add_from(r1, true);

            }
            if let Some(ref r2) = v.last_report{
                s.add_from(r2,v.last_full_report.is_none());
            }
        });
        s
    }

}

// Define a helper function that accepts any DateTime with a fixed offset, converts it to UTC,
// and then returns the first and last milliseconds of the month in UTC.
fn first_and_last_millisecond_of_month<Tz: TimeZone>(datetime: DateTime<Tz>) -> (DateTime<Utc>, DateTime<Utc>) {
    // Convert the input DateTime to UTC
    let datetime_utc = datetime.with_timezone(&Utc);

    // Calculate the first millisecond of the month
    let first_millisecond = Utc.ymd(datetime_utc.year(), datetime_utc.month(), 1)
        .and_hms(0, 0, 0);

    // Calculate the first day of the next month, then subtract one second to get the last millisecond of the current month
    let next_month = Utc.ymd(datetime_utc.year(), datetime_utc.month(), 1)
        .and_hms(0, 0, 0)
        .checked_add_signed(chrono::Duration::days(32)).unwrap() // Add days to ensure the next month
        .with_day(1).unwrap() // First day of the next month
        .checked_sub_signed(chrono::Duration::seconds(1)).unwrap(); // Subtract one second to get the last millisecond

    (first_millisecond, next_month)
}

// Define a helper function that accepts any DateTime with a fixed offset, converts it to UTC,
// and then returns the first and last milliseconds of the day in UTC.
fn first_and_last_millisecond_of_day<Tz: TimeZone>(datetime: DateTime<Tz>) -> (DateTime<Utc>, DateTime<Utc>) {
    // Convert the input DateTime to UTC
    let datetime_utc = datetime.with_timezone(&Utc);

    // Calculate the first millisecond of the day
    let first_millisecond = datetime_utc.date().and_hms(0, 0, 0);

    // Calculate the last millisecond of the day by setting hour to 23, minute and second to 59, and adding 999 milliseconds
    let last_millisecond = datetime_utc.date().and_hms(23, 59, 59)
        .checked_add_signed(chrono::Duration::milliseconds(999)).unwrap();

    (first_millisecond, last_millisecond)
}
impl SplitDataSink{
    fn create_for(id: &str, output_root: &PathBuf, month: DateTime<Utc>,
        day: Option<DateTime<Utc>>) -> SplitDataSink{

        let dir_sep =  "\\"; //TODO, get plat slash
        let mut first_time;
        let mut last_time;
        let mut day_sink = false;
        let day_str = if let Some(date) = day {
            (first_time, last_time) = first_and_last_millisecond_of_day(date);
            day_sink = true;
            let day = date.naive_utc().day();
            format!("{dir_sep}{day}")
        } else {
            (first_time, last_time) = first_and_last_millisecond_of_month(month);
            "".to_string()
        };

        let month_ix = month.naive_utc().month();
        let year = month.naive_utc().year();
        let split_segment = format!("{id}{dir_sep}{year}{dir_sep}{month_ix}{day_str}");
        let sub_path = Path::join(output_root.as_path(),&split_segment);
        SplitDataSink{
            split_segment: split_segment.clone(),
            summary_path: Path::join(sub_path.as_path(), "summary.txt"),
            split_id: id.to_owned(),
            split_date_from: first_time,
            split_date_to: last_time,
            day_sink,
            uniques: HashMap::new()
        }
    }

    fn matches(&self, line: &LogLine) -> bool{
        line.time > self.split_date_from && line.time < self.split_date_to
        && line.split_value.is_some() && line.split_value.unwrap() == self.split_id
    }

}
use tokio::fs::OpenOptions;
use tokio::io::{self, AsyncWriteExt, BufReader, AsyncBufReadExt};
use crate::fetch;

pub async fn parse(config: QueryParseArgs) -> io::Result<()> {
    let QueryParseArgs {
        output_directory,
        input,
        split_by_key,
        keep_unique_of_key,
        filter_operation_type,
        filter_key_prefix,
    } = config;
    let output_dir = output_directory.unwrap_or_else(|| env::current_dir().unwrap().join("parsed"));


    let mut input_paths = expand_input_filenames(input);
    input_paths.sort();
    input_paths.reverse();
    let mut data: HashMap<String, Vec<SplitDataSink>> = HashMap::new();


    // Phase 1: collection
    // Loop through each line in each file, in reverse order
    // parse to LogLine, to get the time, split_id
    // if no key in 'data' matches the split value, create a new entry
    // if no month sinks in the vec .matches the line, create a new month sink
    // for every matching sink
    // - check if there is a matching key for the unique dedup value in the uniques field of the sink
    // - if not, create a new hashmap entry

    // Phase 2: summarization
    // For each sink, call summarize() and put it in the summary file as specified by the path

    // Reverse to ensure we're starting from the latest log file
    for path in input_paths.iter() {
        let path_str = path.to_str().unwrap().to_string();
        println!("Reading {path_str}");
        let file = OpenOptions::new().read(true).open(path).await?;
        let reader = BufReader::new(file);
        let mut lines = reader.lines();

        while let Some(line) = lines.next_line().await? {
            let log_line = parse_s3_log_line(&line, &split_by_key, &keep_unique_of_key);

            // Filter by operation type and key prefix if specified
            if !filter_operation_type.is_empty() && log_line.operation != filter_operation_type {
                println!("Skipping operation type {}",log_line.operation);
                continue;
            }
            if !filter_key_prefix.is_empty() && !log_line.key.starts_with(&filter_key_prefix) {
                println!("Skipping key {}",log_line.key);
                continue;
            }


            if let Some(split_value) = log_line.split_value {
                let sinks = data.entry(split_value.to_string()).or_insert_with(Vec::new);

                //TODO later figure out month summary
                // Check if there's an existing sink that matches the current log line, otherwise create a new one
                let mut existing_ix = sinks.iter().position(|ref s| s.matches(&log_line));

                if existing_ix.is_none(){
                    let new_sink =
                        SplitDataSink::create_for(split_value, &output_dir, log_line.time, Some(log_line.time));
                    sinks.push(new_sink);
                    existing_ix = Some(sinks.len() -1);
                }
                let sink = &mut sinks[existing_ix.unwrap()];


                let unique_id = log_line.unique_value.unwrap_or_default().to_string();
                // Update the sink with the current log line
                sink.uniques.entry(unique_id.clone())
                    .and_modify(|e| e.update_with(&log_line))
                    .or_insert_with(|| {
                        let mut unique = SplitUnique::default(unique_id); // Assuming default() is implemented
                        unique.update_with(&log_line);
                        unique
                    });

            }else{
                println!("No split value for {line}");
            }
        }
    }

    // Summarization Phase
    for (_, sinks) in data.into_iter() {
        for sink in sinks {
            let summary = sink.summarize();
            crate::fetch::create_dirs_if_missing(sink.summary_path.as_path()).await;

            let path = sink.summary_path.to_str().unwrap().to_string();
            println!("Creating '{path}'...");
            // Open or create the summary file for writing
            let mut summary_file = OpenOptions::new().write(true).create_new(true).open(sink.summary_path).await?;
            // Serialize the summary to a string or JSON as needed
            let summary_text = format!("{:#?}", summary); // Assuming you want JSON output
            // Write the summary to file
            summary_file.write_all(summary_text.as_bytes()).await?;
            summary_file.flush().await?;

        }
    }
    println!("Done.");
    Ok(())
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_split() {
        let input = "This is a test";
        let expected = vec!["This", "is", "a", "test"];
        let result: Vec<&str> = SplitWhitespaceBrackets::new(input).collect();
        assert_eq!(result, expected);
    }

    #[test]
    fn test_with_brackets() {
        let input = "This [is a] test";
        let expected = vec!["This", "[is a]", "test"];
        let result: Vec<&str> = SplitWhitespaceBrackets::new(input).collect();
        assert_eq!(result, expected);
    }

    #[test]
    fn test_nested_brackets() {
        let input = "Testing [nested [brackets]] here";
        let expected = vec!["Testing", "[nested [brackets]]", "here"];
        let result: Vec<&str> = SplitWhitespaceBrackets::new(input).collect();
        assert_eq!(result, expected);
    }

    #[test]
    fn test_empty_string() {
        let input = "";
        let expected: Vec<&str> = Vec::new();
        let result: Vec<&str> = SplitWhitespaceBrackets::new(input).collect();
        assert_eq!(result, expected);
    }

    #[test]
    fn test_only_brackets() {
        let input = "[only brackets]";
        let expected = vec!["[only brackets]"];
        let result: Vec<&str> = SplitWhitespaceBrackets::new(input).collect();
        assert_eq!(result, expected);
    }

    #[test]
    fn test_only_quotes() {
        let input = "\"in quotes\"";
        let expected = vec!["\"in quotes\""];
        let result: Vec<&str> = SplitWhitespaceBrackets::new(input).collect();
        assert_eq!(result, expected);
    }
    #[test]
    fn test_mixed() {
        let input = "value1 [value 2] \"in [this]quotes\" value4";
        let expected = vec!["value1", "[value 2]", "\"in [this]quotes\"", "value4"];
        let result: Vec<&str> = SplitWhitespaceBrackets::new(input).collect();
        assert_eq!(result, expected);
    }


    #[test]
    fn test_multiple_consecutive_whitespaces() {
        let input = "This  is a   test";
        let expected = vec!["This","", "is", "a","","", "test"];
        let result: Vec<&str> = SplitWhitespaceBrackets::new(input).collect();
        assert_eq!(result, expected);
    }

    #[test]
    fn test_brackets_with_no_whitespace() {
        let input = "This[is]a[test]";
        let expected = vec!["This[is]a[test]"];
        let result: Vec<&str> = SplitWhitespaceBrackets::new(input).collect();
        assert_eq!(result, expected);
    }

    #[test]
    fn test_log_line(){
        let input = r#"40b77a18 licenses.imazen.net [14/Feb/2024:22:38:49 +0000] 111.11.111.11 - S12ASFAFE62BDGFQ REST.GET.OBJECT v1/licenses/latest/blabla.txt "GET /licenses.imazen.net/v1/licenses/latest/blabla.txt?license_id=00000&manager_id=aasacs&extra_job_query_keys= HTTP/1.1" 200 - 700 700 19 18 "-" "-" - OTvwduQ/0a/U6Q+c1tnae+2jyokFgi++7I= - ECDHE-RSA-AES128-GCM-SHA256 - s3-us-west-2.amazonaws.com TLSv1.2 - Yes"#;
        let v:Vec<&str> = SplitWhitespaceBrackets::new(input).collect();
        let expected:Vec<&str> = vec!["40b77a18",
                                      "licenses.imazen.net",
                                      "[14/Feb/2024:22:38:49 +0000]",
                                      "111.11.111.11",
                                      "-",
                                      "S12ASFAFE62BDGFQ",
                                      "REST.GET.OBJECT",
                                      "v1/licenses/latest/blabla.txt",
                                      "\"GET /licenses.imazen.net/v1/licenses/latest/blabla.txt?license_id=00000&manager_id=aasacs&extra_job_query_keys= HTTP/1.1\"",
                                      "200",
                                      "-",
                                      "700",
                                      "700",
                                      "19",
                                      "18",
                                      "\"-\"",
                                      "\"-\"",
                                      "-",
                                      "OTvwduQ/0a/U6Q+c1tnae+2jyokFgi++7I=",
                                      "-",
                                      "ECDHE-RSA-AES128-GCM-SHA256",
                                      "-",
                                      "s3-us-west-2.amazonaws.com",
                                      "TLSv1.2",
                                      "-",
                                      "Yes"];
        assert_eq!(v, expected);

        let log_line = parse_s3_log_line(input, "license_id", "manager_id");

        assert_eq!(log_line.bucket, expected[1]);
        assert_eq!(log_line.ip_str, expected[3]);
        assert_eq!(log_line.key, expected[7]);
        assert_eq!(log_line.truncated, false);
        assert_eq!(log_line.operation, expected[6]);
        assert_eq!(log_line.split_value, Some("00000"));
        assert_eq!(log_line.unique_value, Some("aasacs"));
        assert_eq!(log_line.response_status_str, "200");
        assert_eq!(log_line.request_path, "/licenses.imazen.net/v1/licenses/latest/blabla.txt");
        assert_eq!(log_line.request_query, Some("?license_id=00000&manager_id=aasacs&extra_job_query_keys="));


    }
}
