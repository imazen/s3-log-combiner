// This is specific to Imazen's telemetry system,
// Which uses S3 blob reads and their associated querystring

use chrono::{DateTime, TimeZone, Utc};

//&decoded_pixels_total=0
//&decoded_pixels_per_second_max=0&decoded_pixels_per_minute_max=0
//&decoded_pixels_per_15_mins_max=0&decoded_pixels_per_hour_max=0
#[derive(Debug, Copy, Clone)]
pub(crate) struct ThroughputStat {
    pub(crate) name: &'static str,
    pub(crate) total: u32,
    pub(crate) per_sec_peak: u32,
    pub(crate) per_min_peak: u32,
    pub(crate) per_hour_peak: u32,
    pub(crate) per_15min_peak: u32,
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
#[derive(Debug, Copy, Clone)]
pub(crate) struct PercentileStat {
    pub(crate) name: &'static str,
    pub(crate) p5: u32,
    pub(crate) p25: u32,
    pub(crate) p50: u32,
    pub(crate) p75: u32,
    pub(crate) p95: u32,
    pub(crate) p100: u32,
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
#[derive(Debug, Clone)]
pub(crate) struct ProcessAttrs {
    // proc_guid=
    guid: String,
    // proc_info_version=
    pub(crate) info_version: String,
    pub(crate) file_version: String,
    // proc_id_hash
    process_id_hash: Option<String>,
    //proc_default_commands
    default_commands: Option<String>,
    //&proc_git_commit
    pub(crate) git_commit: Option<String>,

    //&proc_apppath_hash: (6 char hash)
    app_path_hash: Option<String>,
    // proc_64=0/1
    pub(crate) is64bit: bool,

    // Additional platform info
    // proc_sys_dotnet - e.g., "4.7 or later"
    pub(crate) sys_dotnet: Option<String>,
    // proc_iis - e.g., "8.5", "10.0"
    pub(crate) iis_version: Option<String>,
    // proc_integrated_pipeline
    pub(crate) integrated_pipeline: Option<bool>,
    // proc_asyncmodule
    pub(crate) async_module: Option<bool>,
    // proc_working_set_mb
    pub(crate) working_set_mb: Option<i32>,
}
impl ProcessAttrs {
    fn parse(pairs: &[(&str, &str)]) -> Self {
        ProcessAttrs {
            guid: find_value(pairs, "proc_guid")
                .unwrap_or_default()
                .to_string(),
            info_version: find_value(pairs, "proc_info_version")
                .unwrap_or_default()
                .to_string(),
            file_version: find_value(pairs, "proc_file_version")
                .unwrap_or_default()
                .to_string(),
            process_id_hash: find_value(pairs, "proc_id_hash").map(|s| s.to_string()),
            default_commands: find_value(pairs, "proc_default_commands").map(|s| s.to_string()),
            git_commit: find_value(pairs, "proc_git_commit").map(|s| s.to_string()),
            app_path_hash: find_value(pairs, "proc_apppath_hash").map(|s| s.to_string()),
            is64bit: parse_bool(pairs, "proc_64").unwrap_or(false),
            sys_dotnet: find_value(pairs, "proc_sys_dotnet"),
            iis_version: find_value(pairs, "proc_iis"),
            integrated_pipeline: parse_bool(pairs, "proc_integrated_pipeline"),
            async_module: parse_bool(pairs, "proc_asyncmodule"),
            working_set_mb: find_and_parse(pairs, "proc_working_set_mb"),
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
#[derive(Debug, Clone)]
pub(crate) struct HardwareAttrs {
    pub(crate) logical_cores: i32,
    pub(crate) mac_digest: String,
    pub(crate) os64bit: bool,
    pub(crate) network_drive_count: i32,
    pub(crate) fixed_drive_count: i32,
    pub(crate) other_drive_count: i32,
    pub(crate) fixed_drives: Vec<Drive>,
    pub(crate) other_drives: Vec<Drive>,
    pub(crate) network_drives: Vec<Drive>,
}

impl HardwareAttrs {
    fn parse(pairs: &[(&str, &str)]) -> Self {
        // A function to parse and decode drives from their key identifier
        let parse_drives = |key: &str| -> Vec<Drive> {
            pairs
                .iter()
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
            mac_digest: find_value(pairs, "h_mac_digest")
                .unwrap_or_default()
                .to_string(),
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
#[derive(Debug, Clone)]
pub(crate) struct Drive {
    pub(crate) app_drive: bool, // if the filesystem ends with *, trim it and set this true
    pub(crate) filesystem: String,
    pub(crate) available_gb: i32,
    pub(crate) total_gb: i32,
}
impl Drive {
    fn parse(decoded_input: &str) -> Option<Self> {
        // Split the decoded string by ',' to extract the parts
        let parts: Vec<&str> = decoded_input.split(',').collect();
        if parts.len() != 3 {
            return None;
        }

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

#[derive(Debug, Clone)]
pub(crate) struct PipelineStats {
    // Source format counts
    pub(crate) source_file_ext_tiff: i32,
    pub(crate) source_file_ext_tif: i32,
    pub(crate) source_file_ext_bmp: i32,
    pub(crate) source_file_ext_jpg: i32,
    pub(crate) source_file_ext_png: i32,
    pub(crate) source_file_ext_gif: i32,
    pub(crate) source_file_ext_webp: i32,

    // Response/output format counts
    pub(crate) module_response_ext_jpg: i32,
    pub(crate) module_response_ext_png: i32,
    pub(crate) module_response_ext_gif: i32,
    pub(crate) module_response_ext_webp: i32,

    // Image scaling/dimension multipliers (e.g., source_multiple_8x8 = 8x scale on both dimensions)
    // These track how much images are being resized
    pub(crate) source_multiple_2x: i32,    // 2x on one dimension
    pub(crate) source_multiple_2x2: i32,   // 2x on both dimensions
    pub(crate) source_multiple_4x: i32,
    pub(crate) source_multiple_4x4: i32,
    pub(crate) source_multiple_8x: i32,
    pub(crate) source_multiple_x8: i32,
    pub(crate) source_multiple_8x8: i32,
    pub(crate) source_multiple_16x: i32,
    pub(crate) source_multiple_x16: i32,
    pub(crate) source_multiple_16x16: i32,
    pub(crate) source_multiple_32x: i32,
    pub(crate) source_multiple_32x32: i32,

    // Error/success counters
    pub(crate) postauth_ok: i64,
    pub(crate) postauth_errors: i64,
    pub(crate) postauth_404: i64,
    pub(crate) postauthjob_ok: i64,
    pub(crate) postauthjob_errors: i64,

    // Specific error types
    pub(crate) errors_image_missing: i64,
    pub(crate) errors_image_corrupted: i64,
    pub(crate) errors_image_processing: i64,
    pub(crate) errors_size_limit: i64,
}
impl PipelineStats {
    fn parse(pairs: &[(&str, &str)]) -> Self {
        PipelineStats {
            // Source formats
            source_file_ext_tiff: find_and_parse(pairs, "source_file_ext_tiff").unwrap_or(0),
            source_file_ext_tif: find_and_parse(pairs, "source_file_ext_tif").unwrap_or(0),
            source_file_ext_bmp: find_and_parse(pairs, "source_file_ext_bmp").unwrap_or(0),
            source_file_ext_jpg: find_and_parse(pairs, "source_file_ext_jpg").unwrap_or(0),
            source_file_ext_png: find_and_parse(pairs, "source_file_ext_png").unwrap_or(0),
            source_file_ext_gif: find_and_parse(pairs, "source_file_ext_gif").unwrap_or(0),
            source_file_ext_webp: find_and_parse(pairs, "source_file_ext_webp").unwrap_or(0),

            // Response formats
            module_response_ext_jpg: find_and_parse(pairs, "module_response_ext_jpg").unwrap_or(0),
            module_response_ext_png: find_and_parse(pairs, "module_response_ext_png").unwrap_or(0),
            module_response_ext_gif: find_and_parse(pairs, "module_response_ext_gif").unwrap_or(0),
            module_response_ext_webp: find_and_parse(pairs, "module_response_ext_webp").unwrap_or(0),

            // Image scaling multipliers
            source_multiple_2x: find_and_parse(pairs, "source_multiple_2x").unwrap_or(0),
            source_multiple_2x2: find_and_parse(pairs, "source_multiple_2x2").unwrap_or(0),
            source_multiple_4x: find_and_parse(pairs, "source_multiple_4x").unwrap_or(0),
            source_multiple_4x4: find_and_parse(pairs, "source_multiple_4x4").unwrap_or(0),
            source_multiple_8x: find_and_parse(pairs, "source_multiple_8x").unwrap_or(0),
            source_multiple_x8: find_and_parse(pairs, "source_multiple_x8").unwrap_or(0),
            source_multiple_8x8: find_and_parse(pairs, "source_multiple_8x8").unwrap_or(0),
            source_multiple_16x: find_and_parse(pairs, "source_multiple_16x").unwrap_or(0),
            source_multiple_x16: find_and_parse(pairs, "source_multiple_x16").unwrap_or(0),
            source_multiple_16x16: find_and_parse(pairs, "source_multiple_16x16").unwrap_or(0),
            source_multiple_32x: find_and_parse(pairs, "source_multiple_32x").unwrap_or(0),
            source_multiple_32x32: find_and_parse(pairs, "source_multiple_32x32").unwrap_or(0),

            // Success/error counters
            postauth_ok: find_and_parse(pairs, "postauth_ok").unwrap_or(0),
            postauth_errors: find_and_parse(pairs, "postauth_errors").unwrap_or(0),
            postauth_404: find_and_parse(pairs, "postauth_404_").unwrap_or(0),
            postauthjob_ok: find_and_parse(pairs, "postauthjob_ok").unwrap_or(0),
            postauthjob_errors: find_and_parse(pairs, "postauthjob_errors").unwrap_or(0),

            // Specific error types
            errors_image_missing: find_and_parse(pairs, "postauth_errors_ImageMissingException").unwrap_or(0),
            errors_image_corrupted: find_and_parse(pairs, "postauth_errors_ImageCorruptedException").unwrap_or(0),
            errors_image_processing: find_and_parse(pairs, "postauth_errors_ImageProcessingException").unwrap_or(0),
            errors_size_limit: find_and_parse(pairs, "postauth_errors_SizeLimitException").unwrap_or(0),
        }
    }
}
#[derive(Debug, Clone)]
pub(crate) struct Report {
    pub(crate) ip_str: String,
    //&manager_id
    manager_id: String,
    pub(crate) logged_at: DateTime<Utc>,
    //&reporting_version=4/100
    reporting_version: i32,
    //&truncated=true
    report_truncated: bool,
    //&total_heartbeats=x
    total_heartbeats: i64,
    //&first_heartbeat=(seconds since jan 1 1970)
    first_heartbeat: Option<DateTime<Utc>>,
    //&imageflow=1 (default 0)
    pub(crate) is_imageflow: bool,
    //&p=v1&p=v2 (duplicated)
    pub(crate) plugins: Vec<String>,
    //&query_keys=a,b,c (comma delimited)
    pub(crate) query_keys: Vec<String>,
    //&extra_job_query_keys=a,b,c
    pub(crate) extra_job_query_keys: Vec<String>,
    //&image_domains=v,s,c
    pub(crate) image_domains: Vec<String>,
    //&page_domains=v,s,c
    pub(crate) page_domains: Vec<String>,
    //&enabled_cache=x
    enabled_cache: Option<String>,
    pub(crate) pipeline: PipelineStats,
    pub(crate) hardware: HardwareAttrs,
    pub(crate) process: ProcessAttrs,
    pub(crate) jobs_completed_total: Option<u64>,
    pub(crate) jobs_completed: Option<ThroughputStat>,
    pub(crate) encoded_pixels: Option<ThroughputStat>,
    pub(crate) decoded_pixels: Option<ThroughputStat>,
    pub(crate) blob_read_bytes: Option<ThroughputStat>,
    pub(crate) blob_reads: Option<ThroughputStat>,
    pub(crate) encode_times: Option<PercentileStat>,
    pub(crate) decode_times: Option<PercentileStat>,
    pub(crate) job_times: Option<PercentileStat>,
    pub(crate) blob_read_times: Option<PercentileStat>,
    //&enabled_cache
    pub(crate) cache_type: Option<String>,
}

// if (streamCache != null) query.AddString("stream_cache", streamCache.GetType().Name);
// query.Add("map_web_root", options.MapWebRoot);
// query.Add("use_presets_exclusively", options.UsePresetsExclusively);
// query.Add("request_signing_default", options.RequestSignatureOptions?.DefaultRequirement.ToString() ?? "never");
// query.Add("default_cache_control", options.DefaultCacheControlString);

impl Report {
    pub fn parse(query: &str, log_time: DateTime<Utc>, ip: &str) -> Self {
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
                .map(|v| Utc.timestamp_opt(v, 0).latest())
                .flatten(),
            is_imageflow: parse_bool(&pairs, "imageflow").unwrap_or(false),
            plugins: find_repeated_values_decode(&pairs, "p"),
            query_keys: parse_comma_delimited(
                find_value(&pairs, "query_keys")
                    .unwrap_or_default()
                    .as_str(),
            ),
            extra_job_query_keys: parse_comma_delimited(
                find_value(&pairs, "extra_job_query_keys")
                    .unwrap_or_default()
                    .as_str(),
            ),
            image_domains: parse_comma_delimited(
                find_value(&pairs, "image_domains")
                    .unwrap_or_default()
                    .as_str(),
            ),
            page_domains: parse_comma_delimited(
                find_value(&pairs, "page_domains")
                    .unwrap_or_default()
                    .as_str(),
            ),
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
            cache_type: find_value(&pairs, "stream_cache"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Summary {
    pub(crate) image_domains: Vec<String>,
    pub(crate) page_domains: Vec<String>,
    pub(crate) reporter_ips: Vec<String>,
    pub(crate) query_keys: Vec<String>,
    pub(crate) extra_job_query_keys: Vec<String>,
    pub(crate) plugins: Vec<String>,
    // format [cores=logical_cores][x64|x86](mac digest string)
    pub(crate) machines: Vec<String>,
    pub(crate) jobs_completed_total: u64,
    pub(crate) encoded_pixels_total: u64,
    pub(crate) decoded_pixels_total: u64,
    pub(crate) info_versions: Vec<String>,
    pub(crate) default_command_sets: Vec<String>,
    pub(crate) last_full_report_from: Option<DateTime<Utc>>,
}

impl Summary {
    pub(crate) fn default() -> Summary {
        Summary {
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
            last_full_report_from: None,
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

    pub(crate) fn add_from(&mut self, r: &Report, full_report: bool) {
        if full_report {
            self.last_full_report_from = Some(
                self.last_full_report_from
                    .map(|v| v.max(r.logged_at))
                    .unwrap_or(r.logged_at),
            );
        }
        // Utilize Summary::add_unique for adding unique items
        r.image_domains
            .iter()
            .for_each(|d| Summary::add_unique(&mut self.image_domains, d.clone()));
        r.page_domains
            .iter()
            .for_each(|d| Summary::add_unique(&mut self.page_domains, d.clone()));
        Summary::add_unique(&mut self.reporter_ips, r.ip_str.clone());
        r.query_keys
            .iter()
            .for_each(|k| Summary::add_unique(&mut self.query_keys, k.clone()));
        r.extra_job_query_keys
            .iter()
            .for_each(|k| Summary::add_unique(&mut self.extra_job_query_keys, k.clone()));
        r.plugins
            .iter()
            .for_each(|p| Summary::add_unique(&mut self.plugins, p.clone()));

        // Format and add unique machine information
        let machine_info = format!(
            "[cores={}][{}]{})",
            r.hardware.logical_cores,
            if r.hardware.os64bit { "x64" } else { "x86" },
            r.hardware.mac_digest
        );
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
    pairs
        .iter()
        .find(|&&(k, _)| k == key)
        .and_then(|&(_, v)| v.parse().ok())
}
fn find_and_parse_2<T: std::str::FromStr>(
    pairs: &[(&str, &str)],
    key_part_a: &str,
    key_part_b: &str,
) -> Option<T> {
    pairs
        .iter()
        .find(|&&(k, _)| {
            k.starts_with(key_part_a)
                && k.ends_with(key_part_b)
                && k.len() == key_part_a.len() + key_part_b.len()
        })
        .and_then(|&(_, v)| v.parse().ok())
}

fn find_value<'a>(pairs: &'a [(&str, &str)], key: &'a str) -> Option<String> {
    pairs
        .iter()
        .find(|&&(k, _)| k == key)
        .map(|&(_, v)| v.to_string())
}
fn find_repeated_values_decode<'a>(pairs: &'a [(&str, &str)], key: &'a str) -> Vec<String> {
    pairs
        .iter()
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
    input
        .split(delimiter)
        .filter_map(|item| item.parse().ok())
        .collect()
}

fn parse_query_string(input: &str) -> Vec<(&str, &str)> {
    let without_q = if input.starts_with('?') {
        &input[1..]
    } else {
        input
    };
    without_q
        .split('&')
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
    decoded.split(',').map(|s| s.to_string()).collect()
}

// ============================================================================
// Enhanced Analytics Structures
// ============================================================================

use serde::Serialize;
use std::collections::{BTreeMap, HashMap, HashSet};

/// Product type detection based on is_imageflow flag and version string
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum ProductType {
    Imageflow,
    ImageResizer,
    Unknown,
}

impl ProductType {
    pub fn from_report(report: &Report) -> Self {
        if report.is_imageflow {
            return ProductType::Imageflow;
        }
        let version = &report.process.info_version;
        if version.starts_with("0.") {
            ProductType::Imageflow
        } else if version.starts_with("4.") || version.starts_with("5.") {
            ProductType::ImageResizer
        } else {
            ProductType::Unknown
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            ProductType::Imageflow => "imageflow",
            ProductType::ImageResizer => "imageresizer",
            ProductType::Unknown => "unknown",
        }
    }
}

/// Weekly metrics for time-series analysis
#[derive(Debug, Clone, Default)]
pub struct WeeklyMetrics {
    pub week_key: String,
    pub jobs_completed: u64,
    pub unique_machines: HashSet<String>,
    pub unique_ips: HashSet<String>,
    pub report_count: u64,
    pub first_report: Option<DateTime<Utc>>,
    pub last_report: Option<DateTime<Utc>>,
}

/// Daily machine stats for concurrency tracking
#[derive(Debug, Clone, Default)]
pub struct DailyMachineStats {
    pub date_key: String,
    pub unique_machines: HashSet<String>,
    pub jobs_completed: u64,
}

/// Domain usage statistics
#[derive(Debug, Clone, Default)]
pub struct DomainStats {
    pub domain: String,
    pub job_count: u64,
    pub first_seen: Option<DateTime<Utc>>,
    pub last_seen: Option<DateTime<Utc>>,
    pub associated_ips: HashSet<String>,
    pub uses_imageflow: bool,
    pub uses_imageresizer: bool,
}

/// Trend direction for time-series analysis
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum TrendDirection {
    Rising,
    Declining,
    Steady,
    Insufficient,
}

impl TrendDirection {
    pub fn as_str(&self) -> &'static str {
        match self {
            TrendDirection::Rising => "rising",
            TrendDirection::Declining => "declining",
            TrendDirection::Steady => "steady",
            TrendDirection::Insufficient => "insufficient_data",
        }
    }
}

/// Enhanced summary with historical tracking
#[derive(Debug, Clone)]
pub struct EnhancedSummary {
    pub base: Summary,
    pub weekly_metrics: BTreeMap<String, WeeklyMetrics>,
    pub daily_machines: BTreeMap<String, DailyMachineStats>,
    pub imageflow_jobs: u64,
    pub imageresizer_jobs: u64,
    pub imageflow_machines: HashSet<String>,
    pub imageresizer_machines: HashSet<String>,
    pub domain_stats: HashMap<String, DomainStats>,
    pub job_trend: TrendDirection,
    pub machine_trend: TrendDirection,
    pub first_activity: Option<DateTime<Utc>>,
    pub last_activity: Option<DateTime<Utc>>,
}

impl Default for EnhancedSummary {
    fn default() -> Self {
        EnhancedSummary {
            base: Summary::default(),
            weekly_metrics: BTreeMap::new(),
            daily_machines: BTreeMap::new(),
            imageflow_jobs: 0,
            imageresizer_jobs: 0,
            imageflow_machines: HashSet::new(),
            imageresizer_machines: HashSet::new(),
            domain_stats: HashMap::new(),
            job_trend: TrendDirection::Insufficient,
            machine_trend: TrendDirection::Insufficient,
            first_activity: None,
            last_activity: None,
        }
    }
}

impl EnhancedSummary {
    pub fn add_from_enhanced(&mut self, report: &Report, full_report: bool) {
        // Call base implementation
        self.base.add_from(report, full_report);

        let week_key = crate::util::week_key_from_datetime(report.logged_at);
        let date_key = crate::util::date_key_from_datetime(report.logged_at);
        let mac_digest = &report.hardware.mac_digest;
        let product = ProductType::from_report(report);

        // Track activity window
        self.first_activity = Some(
            self.first_activity
                .map(|f| f.min(report.logged_at))
                .unwrap_or(report.logged_at),
        );
        self.last_activity = Some(
            self.last_activity
                .map(|l| l.max(report.logged_at))
                .unwrap_or(report.logged_at),
        );

        // Update weekly metrics
        let weekly = self.weekly_metrics.entry(week_key.clone()).or_insert_with(|| {
            WeeklyMetrics {
                week_key: week_key.clone(),
                ..Default::default()
            }
        });

        if full_report {
            if let Some(jobs) = report.jobs_completed_total {
                weekly.jobs_completed += jobs;
            }
        }
        weekly.unique_machines.insert(mac_digest.clone());
        weekly.unique_ips.insert(report.ip_str.clone());
        weekly.report_count += 1;
        weekly.first_report = Some(
            weekly
                .first_report
                .map(|f| f.min(report.logged_at))
                .unwrap_or(report.logged_at),
        );
        weekly.last_report = Some(
            weekly
                .last_report
                .map(|l| l.max(report.logged_at))
                .unwrap_or(report.logged_at),
        );

        // Update daily machine stats
        let daily = self.daily_machines.entry(date_key.clone()).or_insert_with(|| {
            DailyMachineStats {
                date_key: date_key.clone(),
                ..Default::default()
            }
        });
        daily.unique_machines.insert(mac_digest.clone());
        if full_report {
            if let Some(jobs) = report.jobs_completed_total {
                daily.jobs_completed += jobs;
            }
        }

        // Product-specific tracking
        if full_report {
            if let Some(jobs) = report.jobs_completed_total {
                match product {
                    ProductType::Imageflow => {
                        self.imageflow_jobs += jobs;
                        self.imageflow_machines.insert(mac_digest.clone());
                    }
                    ProductType::ImageResizer => {
                        self.imageresizer_jobs += jobs;
                        self.imageresizer_machines.insert(mac_digest.clone());
                    }
                    ProductType::Unknown => {}
                }
            }
        }

        // Domain tracking
        for domain in &report.image_domains {
            if domain.is_empty() {
                continue;
            }
            let stats = self.domain_stats.entry(domain.clone()).or_insert_with(|| {
                DomainStats {
                    domain: domain.clone(),
                    ..Default::default()
                }
            });
            if full_report {
                if let Some(jobs) = report.jobs_completed_total {
                    stats.job_count += jobs;
                }
            }
            stats.first_seen = Some(
                stats
                    .first_seen
                    .map(|f| f.min(report.logged_at))
                    .unwrap_or(report.logged_at),
            );
            stats.last_seen = Some(
                stats
                    .last_seen
                    .map(|l| l.max(report.logged_at))
                    .unwrap_or(report.logged_at),
            );
            stats.associated_ips.insert(report.ip_str.clone());
            match product {
                ProductType::Imageflow => stats.uses_imageflow = true,
                ProductType::ImageResizer => stats.uses_imageresizer = true,
                ProductType::Unknown => {}
            }
        }
    }

    /// Compute trends from weekly data (call before serialization)
    pub fn compute_trends(&mut self) {
        let weeks: Vec<_> = self.weekly_metrics.values().collect();

        if weeks.len() < 3 {
            self.job_trend = TrendDirection::Insufficient;
            self.machine_trend = TrendDirection::Insufficient;
            return;
        }

        // Get last 3 weeks (BTreeMap is sorted)
        let recent: Vec<_> = weeks.iter().rev().take(3).collect();

        // Job trend (compare oldest to newest of the 3)
        let job_values: Vec<u64> = recent.iter().rev().map(|w| w.jobs_completed).collect();
        self.job_trend = Self::calculate_trend(&job_values);

        // Machine trend
        let machine_values: Vec<u64> = recent
            .iter()
            .rev()
            .map(|w| w.unique_machines.len() as u64)
            .collect();
        self.machine_trend = Self::calculate_trend(&machine_values);
    }

    fn calculate_trend(values: &[u64]) -> TrendDirection {
        if values.len() < 2 {
            return TrendDirection::Insufficient;
        }
        let first = values[0] as f64;
        let last = values[values.len() - 1] as f64;
        if first == 0.0 && last == 0.0 {
            return TrendDirection::Steady;
        }
        let change_pct = if first > 0.0 {
            (last - first) / first * 100.0
        } else {
            100.0 // From zero to something = rising
        };

        if change_pct > 10.0 {
            TrendDirection::Rising
        } else if change_pct < -10.0 {
            TrendDirection::Declining
        } else {
            TrendDirection::Steady
        }
    }

    /// Check if this license uses Imageflow
    pub fn uses_imageflow(&self) -> bool {
        self.imageflow_jobs > 0 || !self.imageflow_machines.is_empty()
    }

    /// Check if this license uses ImageResizer
    pub fn uses_imageresizer(&self) -> bool {
        self.imageresizer_jobs > 0 || !self.imageresizer_machines.is_empty()
    }
}
