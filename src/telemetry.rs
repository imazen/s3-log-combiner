use chrono::{DateTime, TimeZone, Utc};

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
pub(crate) struct Report{
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
                .map(|v| Utc.timestamp_opt(v, 0).latest()).flatten(),
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
    pub(crate) fn default() -> Summary {
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

    pub(crate) fn add_from(&mut self, r: &Report, full_report: bool) {
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
