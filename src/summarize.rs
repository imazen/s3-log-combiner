use crate::cli::QueryParseArgs;
use crate::json_report::{
    AggregateReport, DailyMachineCount, DomainReport, ErrorTracker, FormatTracker,
    GlobalImageflowDomainTracker, ImageflowGlobalReport, ImageScalingTracker,
    InfrastructureTracker, JsonReport, LicenseDistribution, LicenseReport, PerformanceTracker,
    PlatformTracker, ProductMetrics, VersionTracker, WeeklyData,
};
use crate::log_syntax::{S3LogLine, SplitLogColumns};
use crate::telemetry::{EnhancedSummary, ProductType, Report, Summary};
use async_compression::tokio::bufread;
use atomic_refcell::AtomicRefCell;
use chrono::{DateTime, Datelike, TimeZone, Utc};
use futures_util::{StreamExt, TryStreamExt};
use rusoto_core::{HttpClient, Region};
use rusoto_s3::{S3Client, S3};
use std::collections::{HashMap, HashSet};
use std::ops::Index;
use std::path::{Path, PathBuf};
use std::process;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::{env, fs};

#[derive(Debug, Clone)]
struct SplitUnique {
    unique_id: String,
    pub last_full_report_from: DateTime<Utc>,
    pub last_full_report: Option<Report>,
    pub last_report_from: DateTime<Utc>,
    pub last_report: Option<Report>,
}

impl SplitUnique {
    fn default(id: String) -> SplitUnique {
        SplitUnique {
            unique_id: id,
            last_full_report_from: Default::default(),
            last_full_report: None,
            last_report_from: Default::default(),
            last_report: None,
        }
    }
}

// global truncated var
static TRUNCATED_TOTAL: AtomicU64 = AtomicU64::new(0);

impl SplitUnique {
    fn parse_line(line: &S3LogLine) -> Option<Report> {
        line.request_query
            .map(|q| Report::parse(q, line.time, line.ip_str))
    }
    fn update_with(&mut self, line: &S3LogLine) {
        if line.request_query.is_none() {
            return;
        }
        if line.time > self.last_report_from {
            self.last_report = Self::parse_line(&line)
        }
        self.last_report_from = line.time;

        if !line.truncated {
            if line.time > self.last_full_report_from {
                self.last_full_report = self.last_report.clone();
            }
            self.last_full_report_from = line.time;
        } else {
            TRUNCATED_TOTAL.store(
                TRUNCATED_TOTAL.load(std::sync::atomic::Ordering::Relaxed) + 1,
                std::sync::atomic::Ordering::Relaxed,
            );
        }
    }
}

//https://github.com/imazen/imageflow-dotnet-server/blob/c85beb9cd798b272ced56e2e7d903de82b0a8a31/src/Imazen.Common/Instrumentation/GlobalPerf.cs#L266
#[derive(Debug, Clone)]
struct SplitDataSink {
    split_segment: String,
    summary_path: PathBuf,
    split_id: String,
    split_date_from: DateTime<Utc>,
    split_date_to: DateTime<Utc>,
    pub uniques: HashMap<String, SplitUnique>,
    day_sink: bool,
}

impl SplitDataSink {
    // combine all the SplitUnique.last_full_report and last.report data
    // get all unique hmac fingerprints,
    // all unique page domains
    // all unique image domains
    // sum of jobs processed
    //

    fn summarize(&self) -> Summary {
        let mut s = Summary::default();
        self.uniques.iter().for_each(|(_, v)| {
            if let Some(ref r1) = v.last_full_report {
                s.add_from(r1, true);
            }
            if let Some(ref r2) = v.last_report {
                s.add_from(r2, v.last_full_report.is_none());
            }
        });
        s
    }

    fn summarize_enhanced(&self) -> EnhancedSummary {
        let mut s = EnhancedSummary::default();
        self.uniques.iter().for_each(|(_, v)| {
            if let Some(ref r1) = v.last_full_report {
                s.add_from_enhanced(r1, true);
            }
            if let Some(ref r2) = v.last_report {
                s.add_from_enhanced(r2, v.last_full_report.is_none());
            }
        });
        s.compute_trends();
        s
    }

    fn create_for(
        id: &str,
        output_root: &PathBuf,
        month: DateTime<Utc>,
        day: Option<DateTime<Utc>>,
    ) -> SplitDataSink {
        let dir_sep = "\\"; //TODO, get plat slash
        let first_time;
        let last_time;
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
        let sub_path = Path::join(output_root.as_path(), &split_segment);
        SplitDataSink {
            split_segment: split_segment.clone(),
            summary_path: Path::join(sub_path.as_path(), "summary.txt"),
            split_id: id.to_owned(),
            split_date_from: first_time,
            split_date_to: last_time,
            day_sink,
            uniques: HashMap::new(),
        }
    }

    fn matches(&self, line: &S3LogLine) -> bool {
        line.time > self.split_date_from
            && line.time < self.split_date_to
            && line.split_value.is_some()
            && line.split_value.unwrap() == self.split_id
    }

    fn update_with_unconditional(&mut self, line: &S3LogLine) {
        let unique_id = *line.unique_value.as_ref().unwrap();

        if !self.uniques.contains_key(unique_id) {
            self.uniques.insert(
                unique_id.to_string(),
                SplitUnique::default(unique_id.to_string()),
            );
        }
        self.uniques.get_mut(unique_id).unwrap().update_with(&line);
    }
}

use crate::fetch::BlobResult;
use crate::license_blob::{LicenseBlob, LicenseStatus};
use crate::progress::BLOB_BYTES_READ;
use crate::util::{
    expand_input_filenames_recursive, first_and_last_millisecond_of_day,
    first_and_last_millisecond_of_month,
};
use tokio::fs::OpenOptions;
use tokio::io::{self, AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::sync::mpsc;
use tokio::sync::mpsc::Receiver;

async fn enqueue_lines(input_paths: Vec<PathBuf>, tx: mpsc::Sender<String>) -> io::Result<()> {
    for path in input_paths {
        let path_str = path.to_str().unwrap().to_string();
        //println!("Reading {path_str}");
        let file = OpenOptions::new().read(true).open(path).await?;
        let reader = BufReader::new(file);

        if path_str.ends_with(".zst") {
            println!("Decompressing {path_str}...");
            let mut decoder = bufread::ZstdDecoder::new(reader);
            let mut buf_decoder = BufReader::new(decoder);
            let mut lines = buf_decoder.lines();
            while let Some(line) = lines.next_line().await? {
                tx.send(line)
                    .await
                    .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
            }
        } else {
            let mut lines = reader.lines();
            while let Some(line) = lines.next_line().await? {
                tx.send(line)
                    .await
                    .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
            }
        }
    }
    Ok(())
}

async fn enqueue_lines_ig(input_paths: Vec<PathBuf>, tx: mpsc::Sender<String>) -> io::Result<()> {
    let result = enqueue_lines(input_paths, tx).await;
    if let Err(ref e) = result {
        eprintln!("Error enqueuing lines: {:#?}", e);
    }
    result
}

fn shard(input: &[PathBuf], shards: usize) -> Vec<Vec<PathBuf>> {
    // we want the first [shard] shards to contain the first [shard] items, etc.
    // thus we chunk by shard count, then transpose the result
    let mut result = Vec::with_capacity(shards);
    for _ in 0..shards {
        result.push(Vec::with_capacity(input.len() / shards + 1));
    }
    for c in input.chunks(shards) {
        for (i, item) in c.iter().enumerate() {
            result[i].push((*item).clone());
        }
    }
    result
}

struct FetchBlobRequest {
    key: String,
    bucket: String,
    to_dir: PathBuf,
}

impl FetchBlobRequest {
    fn from_s3_log_line(line: &S3LogLine, output_dir: &PathBuf) -> FetchBlobRequest {
        let mut to_dir = output_dir.clone();
        to_dir.push(&line.split_value.unwrap());

        FetchBlobRequest {
            key: line.key.to_string(),
            bucket: line.bucket.to_string(),
            to_dir,
        }
    }
}

pub async fn parse(config: QueryParseArgs) -> io::Result<()> {
    let mut input_paths = expand_input_filenames_recursive(config.clone().input);
    input_paths.sort();
    input_paths.reverse();

    let (line_tx, line_rx) = mpsc::channel::<String>(10000);
    let (write_tx, write_rx) = mpsc::channel::<(PathBuf, String)>(100000);
    let (fetch_tx, fetch_rx) = mpsc::channel::<FetchBlobRequest>(300);

    let mut tasks = shard(&input_paths, 16)
        .into_iter()
        .map(|set| tokio::spawn(enqueue_lines_ig(set, line_tx.clone())))
        .collect::<Vec<_>>();
    std::mem::drop(line_tx);

    let writing_task = tokio::task::spawn(write_files_ig(write_rx));
    tasks.push(writing_task);

    let fetching_task = tokio::task::spawn(fetch_licenses_ig(fetch_rx, config.clone()));
    tasks.push(fetching_task);

    let processing_task = tokio::task::spawn_blocking(move || {
        process_lines_ig(line_rx, write_tx, fetch_tx, config.clone())
    });

    tasks.push(processing_task);
    // Wait for all tasks to complete, ensuring all polling
    // is done before we return.

    let _ = futures::future::try_join_all(tasks).await?;
    Ok(())
}

async fn fetch_licenses_ig(
    fetch_rx: Receiver<FetchBlobRequest>,
    config: QueryParseArgs,
) -> io::Result<()> {
    let result = fetch_licenses(fetch_rx, config).await;
    if let Err(ref e) = result {
        eprintln!("Error fetching licenses: {:#?}", e);
    }
    result
}

async fn fetch_licenses(
    mut rx: Receiver<FetchBlobRequest>,
    config: QueryParseArgs,
) -> io::Result<()> {
    let region = config
        .region
        .parse::<Region>()
        .expect(&format!("Failed to parse AWS region '{}", config.region));

    let client = S3Client::new_with(
        HttpClient::new().expect("Failed to create HTTP client"),
        rusoto_core::credential::StaticProvider::new(
            config.access_key.to_string(),
            config.secret_key.to_string(),
            None,
            None,
        ),
        region,
    );

    while let Some(req) = rx.recv().await {
        // fetch the blob using rusoto
        // parse it with LicenseBlob::from and summarize
        // if it's not valid, write it to a .invalid file alongside the dir
        // otherwise, write it to a .valid file inside the dir
        let result = client
            .get_object(rusoto_s3::GetObjectRequest {
                bucket: req.bucket.to_string(),
                key: req.key.to_string(),
                ..Default::default()
            })
            .await;

        let stream = match result {
            Ok(r) => r.body.unwrap(),
            Err(e) => {
                let key = req.key.to_string();
                let bucket = req.bucket.to_string();
                eprintln!("Error fetching {key} from {bucket}: {e}");
                return Err(io::Error::new(io::ErrorKind::Other, e));
            }
        };
        let bytes = stream
            .map_ok(|b| b.to_vec())
            .try_concat()
            .await
            .expect("Failed to read blob");

        let str = std::str::from_utf8(&bytes).expect("Failed to parse license blob as UTF-8");

        let blob =
            crate::license_blob::LicenseBlob::from(str).expect("Failed to parse license blob");

        if let Some(ref id) = blob.get_str("Id") {
            LICENSE_BLOBS
                .borrow_mut()
                .insert(id.to_string(), blob.clone());
        }

        let status = blob.status();
        let mut target_path = req.to_dir.clone();
        match status {
            LicenseStatus::ActiveWithFeatures(f) => {
                target_path.push("license.summary.txt");
            }
            _ => {
                target_path.set_extension(format!("{}.txt", status.as_str_lowercase()));
            }
        }
        // create dir if missing
        let parent_dir = target_path.parent().unwrap();
        tokio::fs::create_dir_all(parent_dir).await?;

        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&target_path)
            .await?;
        file.write_all(blob.describe().as_bytes()).await?;
    }

    Ok(())
}

use std::sync::LazyLock;
// create a map of license ids to license blobs, arc mutex
static LICENSE_BLOBS: LazyLock<AtomicRefCell<HashMap<String, LicenseBlob>>> =
    LazyLock::new(|| AtomicRefCell::new(HashMap::new()));

async fn write_files_ig(mut rx: Receiver<(PathBuf, String)>) -> io::Result<()> {
    let result = write_files(rx).await;
    if let Err(ref e) = result {
        eprintln!("Error writing files: {:#?}", e);
    }
    result
}

async fn write_files(mut rx: Receiver<(PathBuf, String)>) -> io::Result<()> {
    let mut summaries_written = 0;
    println!("Waiting for summaries...");
    while let Some((path, contents)) = rx.recv().await {
        crate::fetch::create_dirs_if_missing(path.as_path()).await;
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)
            .await?;
        file.write_all(contents.as_bytes()).await?;
        file.flush().await?;
        summaries_written += 1;
    }
    println!("All done... Wrote {} summaries", summaries_written);
    Ok(())
}

fn process_lines_ig(
    mut line_rx: mpsc::Receiver<String>,
    mut write_tx: mpsc::Sender<(PathBuf, String)>,
    mut fetch_tx: mpsc::Sender<FetchBlobRequest>,
    config: QueryParseArgs,
) -> io::Result<()> {
    let result = process_lines(line_rx, write_tx, fetch_tx, config);
    if let Err(ref e) = result {
        eprintln!("Error processing lines: {:#?}", e);
    }
    result
}

struct GroupedSinks {
    sinks: Vec<SplitDataSink>,
    pub all: SplitDataSink,
    output_dir: PathBuf,
    split_id: String,
}
// impl iter over sinks and all, &SplitDataSink

impl GroupedSinks {
    fn iter_sinks(&self) -> impl Iterator<Item = &SplitDataSink> {
        self.sinks.iter().chain(std::iter::once(&self.all))
    }
    fn update_with(&mut self, log_line: &S3LogLine) {
        if log_line.unique_value.is_none() {
            return;
        }
        let unique_id = *log_line.unique_value.as_ref().unwrap();

        let mut existing_ix = self.sinks.iter().position(|ref s| s.matches(&log_line));

        if existing_ix.is_none() {
            let new_sink =
                SplitDataSink::create_for(&self.split_id, &self.output_dir, log_line.time, None);
            self.sinks.push(new_sink);
            existing_ix = Some(self.sinks.len() - 1);
        }
        self.sinks[existing_ix.unwrap()].update_with_unconditional(&log_line);
        self.all.update_with_unconditional(&log_line);
    }
}

impl GroupedSinks {
    fn new(output_dir: PathBuf, split_id: String) -> GroupedSinks {
        let summary_path = Path::join(&output_dir, &format!("{split_id}.summary.txt"));
        GroupedSinks {
            sinks: Vec::new(),
            all: SplitDataSink {
                split_segment: split_id.to_string(),
                summary_path,
                split_id: split_id.to_string(),
                split_date_from: Utc::now(),
                split_date_to: Utc::now(),
                uniques: HashMap::with_capacity(1000),
                day_sink: false,
            },
            output_dir,
            split_id,
        }
    }
}

fn process_lines(
    mut line_rx: mpsc::Receiver<String>,
    mut write_tx: mpsc::Sender<(PathBuf, String)>,
    mut fetch_tx: mpsc::Sender<FetchBlobRequest>,
    config: QueryParseArgs,
) -> io::Result<()> {
    let mut data: HashMap<String, GroupedSinks> = HashMap::new();
    let QueryParseArgs {
        output_directory,
        split_by_key,
        keep_unique_of_key,
        filter_operation_type,
        filter_key_prefix,
        ..
    } = config;
    let output_dir = output_directory.unwrap_or_else(|| env::current_dir().unwrap().join("parsed"));

    // Enhanced analytics trackers
    let mut version_tracker = VersionTracker::new();
    let mut format_tracker = FormatTracker::new();
    let mut global_imageflow_tracker = GlobalImageflowDomainTracker::new();
    let mut infrastructure_tracker = InfrastructureTracker::new();
    let mut platform_tracker = PlatformTracker::new();
    let mut performance_tracker = PerformanceTracker::new();
    let mut error_tracker = ErrorTracker::new();
    let mut scaling_tracker = ImageScalingTracker::new();

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

    let data_cutoff = Utc::now() - chrono::Duration::days(7);

    let mut licenses_requested = HashSet::new();

    let mut line_count = 0;
    while let Some(line) = line_rx.blocking_recv() {
        line_count += 1;

        if line_count % 100000 == 0 {
            println!(
                "Processed {}k lines, with {} truncated",
                line_count / 1000,
                TRUNCATED_TOTAL.load(Ordering::Relaxed)
            );
        }
        if line_count % 1000000 == 0 {
            write_summaries_ig(&data, &write_tx)?;
        }
        let log_line = S3LogLine::from_line_str(&line, &split_by_key, &keep_unique_of_key);

        if log_line.time < data_cutoff {
            continue;
        }

        // Filter by operation type and key prefix if specified
        if !filter_operation_type.is_empty() && log_line.operation != filter_operation_type {
            //println!("Skipping operation type {}", log_line.operation);
            continue;
        }
        if !filter_key_prefix.is_empty() && !log_line.key.starts_with(&filter_key_prefix) {
            //println!("Skipping key {}", log_line.key);
            continue;
        }

        if let Some(split_value) = log_line.split_value {
            // Filter by license id if specified
            if let Some(ref license_id) = config.license_id {
                if split_value != license_id {
                    continue;
                } else {
                    println!("Found log line for license id {license_id}")
                }
            }

            if !licenses_requested.contains(split_value) {
                fetch_tx
                    .blocking_send(FetchBlobRequest::from_s3_log_line(&log_line, &output_dir))
                    .unwrap();
                licenses_requested.insert(split_value.to_string());
            }

            if !data.contains_key(split_value) {
                let main_summary = output_dir.clone();
                data.insert(
                    split_value.to_string(),
                    GroupedSinks::new(output_dir.clone(), split_value.to_string()),
                );
            }
            let sinks = data.get_mut(split_value).unwrap();

            //TODO later figure out month summary
            // Check if there's an existing sink that matches the current log line, otherwise create a new one
            sinks.update_with(&log_line);
        } else {
            //println!("No split value for {line}");
        }
    }

    println!("Processing complete. Populating analytics trackers...");

    // Populate trackers from collected data (done after fetching completes to avoid borrow conflicts)
    for (license_id, sinks) in &data {
        let license_owner: Option<String> = LICENSE_BLOBS
            .borrow()
            .get(license_id)
            .and_then(|b| b.get_str("Owner").map(|s| s.to_string()));

        for unique in sinks.all.uniques.values() {
            // Process last_full_report if available
            if let Some(ref report) = unique.last_full_report {
                version_tracker.add_report(report, license_id, license_owner.as_deref());
                format_tracker.add_report(report, license_id, license_owner.as_deref());
                global_imageflow_tracker.add_imageflow_domains(report, license_id);
                infrastructure_tracker.add_report(report, license_id);
                platform_tracker.add_report(report, license_id);
                performance_tracker.add_report(report);
                error_tracker.add_report(report, license_id);
                scaling_tracker.add_report(report, license_id);
            }
            // Process last_report if it's different from last_full_report
            if let Some(ref report) = unique.last_report {
                if unique.last_full_report.is_none() {
                    version_tracker.add_report(report, license_id, license_owner.as_deref());
                    format_tracker.add_report(report, license_id, license_owner.as_deref());
                    global_imageflow_tracker.add_imageflow_domains(report, license_id);
                    infrastructure_tracker.add_report(report, license_id);
                    platform_tracker.add_report(report, license_id);
                    performance_tracker.add_report(report);
                    error_tracker.add_report(report, license_id);
                    scaling_tracker.add_report(report, license_id);
                }
            }
        }
    }

    println!("Generating enhanced reports...");

    // filter sinks to those that have image job activity in the 90 days.
    // Create a single file report of
    // license ids, names, unique reporter IPs, software versions, and job counts
    // also list the last report date.
    let mut filtered_sinks = data
        .values()
        .filter(|s| {
            s.all.uniques.values().any(|u| {
                u.last_report.is_some()
                    && u.last_report_from > Utc::now() - chrono::Duration::days(90)
            })
        })
        .collect::<Vec<_>>();
    let mut report_contents = String::new();
    let mut low_usage_report = String::new();
    let mut violation_report = String::new();
    for sink in filtered_sinks {
        let uniquesinks = sink
            .all
            .uniques
            .values()
            .filter(|u| {
                u.last_report.is_some()
                    && u.last_report_from > Utc::now() - chrono::Duration::days(90)
            })
            .collect::<Vec<_>>();
        // sum the job counts
        let job_count = uniquesinks
            .iter()
            .map(|u| {
                u.last_report
                    .as_ref()
                    .unwrap()
                    .jobs_completed_total
                    .unwrap_or(0)
            })
            .sum::<u64>();
        let last_report_date = uniquesinks
            .iter()
            .map(|u| u.last_report_from)
            .min()
            .unwrap();
        let license_id = sink.all.split_id.clone();
        let license_blob_maybe = LICENSE_BLOBS.borrow().get(&license_id).map(|b| b.clone());

        let mut license_line = format!("License {license_id}");
        let mut valid_status = None;
        if let Some(ref license_blob) = license_blob_maybe {
            license_line = license_blob.summary.clone();
            match license_blob.status() {
                LicenseStatus::ActiveWithFeatures(_) => {
                    valid_status = Some(true);
                }
                bad => {
                    valid_status = Some(false);
                    license_line.push_str(&format!(" (status: {})", bad.as_str_lowercase()));
                }
            }
        }

        let reporter_ips = uniquesinks
            .iter()
            .map(|u| u.last_report.as_ref().unwrap().ip_str.to_string())
            .collect::<HashSet<_>>();
        let software_version = uniquesinks
            .iter()
            .map(|u| {
                u.last_report
                    .as_ref()
                    .unwrap()
                    .process
                    .info_version
                    .to_string()
            })
            .collect::<HashSet<_>>();

        let ip_str = reporter_ips.len();
        let ver_str = software_version
            .into_iter()
            .collect::<Vec<String>>()
            .join(", ");
        let line = format!("{license_line}: Usage count in last 90d: {job_count}, last log: {last_report_date} using software: {ver_str} on {ip_str} ips \n");
        report_contents.push_str(&line);

        if Some(false) == valid_status {
            if job_count > 0 {
                violation_report.push_str(&line);
            }
        } else if job_count < 100 {
            low_usage_report.push_str(&line);
        }
    }
    write_tx
        .blocking_send((output_dir.join("report.txt"), report_contents))
        .unwrap();
    write_tx.blocking_send((output_dir.join("low_usage_report.txt"), low_usage_report));
    write_tx.blocking_send((output_dir.join("violation_report.txt"), violation_report));

    // Generate enhanced reports
    println!("Generating JSON report...");
    let json_report = generate_json_report(&data, &version_tracker, &format_tracker, &global_imageflow_tracker);
    let json_str = serde_json::to_string_pretty(&json_report).unwrap_or_else(|e| {
        eprintln!("Error serializing JSON report: {}", e);
        "{}".to_string()
    });
    write_tx
        .blocking_send((output_dir.join("report.json"), json_str))
        .unwrap();

    println!("Generating Imageflow report...");
    let imageflow_report = generate_imageflow_report(&data, &global_imageflow_tracker);
    write_tx
        .blocking_send((output_dir.join("imageflow_report.txt"), imageflow_report))
        .unwrap();

    println!("Generating weekly breakdown...");
    let weekly_report = generate_weekly_breakdown(&data);
    write_tx
        .blocking_send((output_dir.join("weekly_breakdown.txt"), weekly_report))
        .unwrap();

    println!("Generating compatibility report...");
    let compat_report = generate_compatibility_report(&version_tracker);
    write_tx
        .blocking_send((output_dir.join("compatibility_report.txt"), compat_report))
        .unwrap();

    println!("Generating infrastructure report...");
    let infra_report = generate_infrastructure_report(&infrastructure_tracker);
    write_tx
        .blocking_send((output_dir.join("infrastructure_report.txt"), infra_report))
        .unwrap();

    println!("Generating platform report...");
    let platform_report = generate_platform_report(&platform_tracker);
    write_tx
        .blocking_send((output_dir.join("platform_report.txt"), platform_report))
        .unwrap();

    println!("Generating performance report...");
    let perf_report = generate_performance_report(&performance_tracker);
    write_tx
        .blocking_send((output_dir.join("performance_report.txt"), perf_report))
        .unwrap();

    println!("Generating error report...");
    let err_report = generate_error_report(&error_tracker);
    write_tx
        .blocking_send((output_dir.join("error_report.txt"), err_report))
        .unwrap();

    println!("Generating image scaling report...");
    let scaling_report = generate_scaling_report(&scaling_tracker);
    write_tx
        .blocking_send((output_dir.join("scaling_report.txt"), scaling_report))
        .unwrap();

    println!("Generating plugin report...");
    let plugin_report = generate_plugin_report(&data);
    write_tx
        .blocking_send((output_dir.join("plugin_report.txt"), plugin_report))
        .unwrap();

    // Summarization Phase
    write_summaries(&data, &write_tx)?;
    println!("All enhanced reports generated successfully.");
    Ok(())
}

fn write_summaries_ig(
    data: &HashMap<String, GroupedSinks>,
    write_tx: &mpsc::Sender<(PathBuf, String)>,
) -> io::Result<()> {
    let result = write_summaries(data, write_tx);
    if let Err(ref e) = result {
        eprintln!("Error writing summaries: {:#?}", e);
    }
    result
}

fn write_summaries(
    data: &HashMap<String, GroupedSinks>,
    write_tx: &mpsc::Sender<(PathBuf, String)>,
) -> io::Result<()> {
    println!("Summarizing...");
    let mut count = 0;
    for sink in data.values().map(|s| s.iter_sinks()).flatten() {
        write_tx
            .blocking_send((
                sink.summary_path.clone(),
                format!("{:#?}", sink.summarize()),
            ))
            .unwrap();
        count += 1;
        //.map_err(|e| io::Error::new(io::ErrorKind::Other, format!("{:#?}", e)))?;
    }
    println!("Finished enqueuing {count} summaries");
    Ok(())
}

// ============================================================================
// Enhanced Report Generation
// ============================================================================

fn generate_json_report(
    data: &HashMap<String, GroupedSinks>,
    version_tracker: &VersionTracker,
    format_tracker: &FormatTracker,
    global_imageflow_tracker: &GlobalImageflowDomainTracker,
) -> JsonReport {
    let mut licenses = Vec::new();
    let mut imageflow_only = 0usize;
    let mut imageresizer_only = 0usize;
    let mut both_products = 0usize;
    let mut inactive = 0usize;
    let mut total_jobs: u64 = 0;
    let mut total_imageflow_jobs: u64 = 0;
    let mut total_imageresizer_jobs: u64 = 0;
    let mut all_machines: HashSet<String> = HashSet::new();
    let mut imageflow_licenses = 0usize;
    let mut imageresizer_licenses = 0usize;

    for (license_id, sinks) in data {
        let enhanced = sinks.all.summarize_enhanced();

        // Get license info
        let license_blob = LICENSE_BLOBS.borrow().get(license_id).cloned();
        let owner = license_blob.as_ref().and_then(|b| b.get_str("Owner").map(|s| s.to_string()));
        let features = license_blob
            .as_ref()
            .map(|b| b.get_features())
            .unwrap_or_default();
        let status = license_blob
            .as_ref()
            .map(|b| b.status().as_str_lowercase().to_string())
            .unwrap_or_else(|| "unknown".to_string());

        // Count product usage
        let uses_if = enhanced.uses_imageflow();
        let uses_ir = enhanced.uses_imageresizer();
        if uses_if && uses_ir {
            both_products += 1;
        } else if uses_if {
            imageflow_only += 1;
        } else if uses_ir {
            imageresizer_only += 1;
        } else {
            inactive += 1;
        }
        if uses_if {
            imageflow_licenses += 1;
        }
        if uses_ir {
            imageresizer_licenses += 1;
        }

        // Aggregate totals
        total_jobs += enhanced.base.jobs_completed_total;
        total_imageflow_jobs += enhanced.imageflow_jobs;
        total_imageresizer_jobs += enhanced.imageresizer_jobs;
        all_machines.extend(enhanced.imageflow_machines.iter().cloned());
        all_machines.extend(enhanced.imageresizer_machines.iter().cloned());

        // Build weekly data
        let weekly_data: Vec<WeeklyData> = enhanced
            .weekly_metrics
            .values()
            .map(|w| WeeklyData {
                week: w.week_key.clone(),
                jobs_completed: w.jobs_completed,
                unique_machines: w.unique_machines.len(),
                unique_ips: w.unique_ips.len(),
                report_count: w.report_count,
            })
            .collect();

        // Build daily machine counts
        let daily_machine_counts: Vec<DailyMachineCount> = enhanced
            .daily_machines
            .values()
            .map(|d| DailyMachineCount {
                date: d.date_key.clone(),
                concurrent_machines: d.unique_machines.len(),
                jobs: d.jobs_completed,
            })
            .collect();

        // Build domain reports
        let domains: Vec<DomainReport> = enhanced
            .domain_stats
            .values()
            .map(|d| DomainReport {
                domain: d.domain.clone(),
                job_count: d.job_count,
                ip_count: d.associated_ips.len(),
                uses_imageflow: d.uses_imageflow,
                uses_imageresizer: d.uses_imageresizer,
            })
            .collect();

        licenses.push(LicenseReport {
            license_id: license_id.clone(),
            license_status: status,
            owner,
            features,
            total_jobs: enhanced.base.jobs_completed_total,
            unique_machines: enhanced.imageflow_machines.len() + enhanced.imageresizer_machines.len(),
            unique_ips: enhanced.base.reporter_ips.len(),
            imageflow: ProductMetrics {
                jobs: enhanced.imageflow_jobs,
                machines: enhanced.imageflow_machines.len(),
            },
            imageresizer: ProductMetrics {
                jobs: enhanced.imageresizer_jobs,
                machines: enhanced.imageresizer_machines.len(),
            },
            weekly_data,
            daily_machine_counts,
            job_trend: enhanced.job_trend.as_str().to_string(),
            machine_trend: enhanced.machine_trend.as_str().to_string(),
            domains,
            first_activity: enhanced.first_activity.map(|d| d.to_rfc3339()),
            last_activity: enhanced.last_activity.map(|d| d.to_rfc3339()),
        });
    }

    let total = licenses.len();
    let imageflow_percent = if total > 0 {
        (imageflow_licenses as f64 / total as f64) * 100.0
    } else {
        0.0
    };
    let imageresizer_percent = if total > 0 {
        (imageresizer_licenses as f64 / total as f64) * 100.0
    } else {
        0.0
    };

    JsonReport {
        generated_at: Utc::now().to_rfc3339(),
        license_distribution: LicenseDistribution {
            total,
            imageflow_only,
            imageresizer_only,
            both_products,
            inactive,
            imageflow_percent,
            imageresizer_percent,
        },
        licenses,
        imageflow_global: ImageflowGlobalReport {
            total_jobs: total_imageflow_jobs,
            total_machines: all_machines.len(),
            total_licenses: imageflow_licenses,
            all_domains: global_imageflow_tracker.to_global_domain_reports(),
        },
        aggregate: AggregateReport {
            total_licenses_active: total - inactive,
            total_jobs,
            total_unique_machines: all_machines.len(),
            imageflow_jobs: total_imageflow_jobs,
            imageresizer_jobs: total_imageresizer_jobs,
        },
        version_compatibility: version_tracker.to_reports(),
        feature_deprecation_candidates: version_tracker.find_deprecation_candidates(3),
        source_format_usage: format_tracker.to_usage_map(),
    }
}

fn generate_imageflow_report(
    data: &HashMap<String, GroupedSinks>,
    global_tracker: &GlobalImageflowDomainTracker,
) -> String {
    let mut report = String::new();
    report.push_str("=== GLOBAL IMAGEFLOW REPORT ===\n\n");

    // Aggregate stats
    let mut total_jobs: u64 = 0;
    let mut total_machines: HashSet<String> = HashSet::new();
    let mut license_count = 0;

    for (_, sinks) in data {
        let enhanced = sinks.all.summarize_enhanced();
        if enhanced.uses_imageflow() {
            total_jobs += enhanced.imageflow_jobs;
            total_machines.extend(enhanced.imageflow_machines.iter().cloned());
            license_count += 1;
        }
    }

    report.push_str(&format!("Total Imageflow Jobs: {}\n", total_jobs));
    report.push_str(&format!("Total Unique Machines: {}\n", total_machines.len()));
    report.push_str(&format!("Total Licenses Using Imageflow: {}\n\n", license_count));

    report.push_str("=== ALL IMAGEFLOW DOMAINS ===\n\n");
    let domains = global_tracker.to_global_domain_reports();
    for d in domains {
        report.push_str(&format!(
            "{}: {} jobs, {} licenses, {} machines\n",
            d.domain, d.total_jobs, d.license_count, d.machine_count
        ));
        if let Some(first) = &d.first_seen {
            report.push_str(&format!("  First seen: {}\n", first));
        }
        if let Some(last) = &d.last_seen {
            report.push_str(&format!("  Last seen: {}\n", last));
        }
    }

    report
}

fn generate_weekly_breakdown(data: &HashMap<String, GroupedSinks>) -> String {
    let mut report = String::new();
    report.push_str("=== WEEKLY ACTIVITY BREAKDOWN ===\n\n");

    // Aggregate weekly data across all licenses
    let mut weekly_totals: std::collections::BTreeMap<String, (u64, HashSet<String>, usize)> =
        std::collections::BTreeMap::new();

    for (license_id, sinks) in data {
        let enhanced = sinks.all.summarize_enhanced();
        for (week, metrics) in &enhanced.weekly_metrics {
            let entry = weekly_totals.entry(week.clone()).or_insert_with(|| {
                (0, HashSet::new(), 0)
            });
            entry.0 += metrics.jobs_completed;
            entry.1.extend(metrics.unique_machines.iter().cloned());
            entry.2 += 1; // license count
        }
    }

    for (week, (jobs, machines, licenses)) in weekly_totals.iter().rev() {
        report.push_str(&format!(
            "{}: {} jobs, {} machines, {} active licenses\n",
            week, jobs, machines.len(), licenses
        ));
    }

    report
}

fn generate_compatibility_report(version_tracker: &VersionTracker) -> String {
    use std::collections::HashMap;

    let mut report = String::new();
    report.push_str("=== COMPATIBILITY REPORT ===\n");
    report.push_str("Feature usage sorted by number of licenses\n\n");

    // Aggregate features across all versions, separately for each product
    // Use the raw VersionStats which has actual license IDs
    struct AggregatedFeature {
        job_count: u64,
        license_ids: HashSet<String>,
        license_names: Vec<String>,
    }

    let mut imageflow_query_keys: HashMap<String, AggregatedFeature> = HashMap::new();
    let mut imageflow_extra_keys: HashMap<String, AggregatedFeature> = HashMap::new();
    let mut imageflow_plugins: HashMap<String, AggregatedFeature> = HashMap::new();
    let mut imageflow_versions: Vec<(String, u64, usize)> = Vec::new();

    let mut imageresizer_query_keys: HashMap<String, AggregatedFeature> = HashMap::new();
    let mut imageresizer_extra_keys: HashMap<String, AggregatedFeature> = HashMap::new();
    let mut imageresizer_plugins: HashMap<String, AggregatedFeature> = HashMap::new();
    let mut imageresizer_versions: Vec<(String, u64, usize)> = Vec::new();

    // Iterate over raw version stats to get actual license IDs
    for stats in version_tracker.versions.values() {
        let is_imageflow = stats.product == "imageflow";
        let (query_keys, extra_keys, plugins, versions) = if is_imageflow {
            (&mut imageflow_query_keys, &mut imageflow_extra_keys, &mut imageflow_plugins, &mut imageflow_versions)
        } else {
            (&mut imageresizer_query_keys, &mut imageresizer_extra_keys, &mut imageresizer_plugins, &mut imageresizer_versions)
        };

        versions.push((stats.version.clone(), stats.job_count, stats.license_ids.len()));

        // Aggregate query keys with actual license IDs
        for (key, feature_stats) in &stats.query_keys {
            let entry = query_keys.entry(key.clone()).or_insert_with(|| AggregatedFeature {
                job_count: 0,
                license_ids: HashSet::new(),
                license_names: Vec::new(),
            });
            entry.job_count += feature_stats.job_count;
            entry.license_ids.extend(feature_stats.license_ids.iter().cloned());
            for (id, name) in &feature_stats.license_names {
                if !entry.license_names.contains(name) {
                    entry.license_names.push(name.clone());
                }
            }
        }

        // Aggregate extra query keys
        for (key, feature_stats) in &stats.extra_job_query_keys {
            let entry = extra_keys.entry(key.clone()).or_insert_with(|| AggregatedFeature {
                job_count: 0,
                license_ids: HashSet::new(),
                license_names: Vec::new(),
            });
            entry.job_count += feature_stats.job_count;
            entry.license_ids.extend(feature_stats.license_ids.iter().cloned());
            for (id, name) in &feature_stats.license_names {
                if !entry.license_names.contains(name) {
                    entry.license_names.push(name.clone());
                }
            }
        }

        // Aggregate plugins
        for (key, feature_stats) in &stats.plugins {
            let entry = plugins.entry(key.clone()).or_insert_with(|| AggregatedFeature {
                job_count: 0,
                license_ids: HashSet::new(),
                license_names: Vec::new(),
            });
            entry.job_count += feature_stats.job_count;
            entry.license_ids.extend(feature_stats.license_ids.iter().cloned());
            for (id, name) in &feature_stats.license_names {
                if !entry.license_names.contains(name) {
                    entry.license_names.push(name.clone());
                }
            }
        }
    }

    // Helper to write a feature table
    fn write_feature_table(
        report: &mut String,
        title: &str,
        features: &HashMap<String, AggregatedFeature>,
    ) {
        if features.is_empty() {
            return;
        }
        report.push_str(&format!("{}\n", title));
        report.push_str(&format!("{:-<80}\n", ""));
        report.push_str(&format!("{:<50} {:>12} {:>12}\n", "Feature", "Licenses", "Jobs"));
        report.push_str(&format!("{:-<80}\n", ""));

        let mut sorted: Vec<_> = features.iter().collect();
        sorted.sort_by(|a, b| b.1.license_ids.len().cmp(&a.1.license_ids.len()));

        for (name, feat) in sorted {
            let display_name = if name.len() > 48 { &name[..48] } else { name };
            let low_usage = if feat.license_ids.len() <= 3 && !feat.license_names.is_empty() {
                format!(" [{}]", feat.license_names.join(", "))
            } else {
                String::new()
            };
            report.push_str(&format!(
                "{:<50} {:>12} {:>12}{}\n",
                display_name,
                feat.license_ids.len(),
                feat.job_count,
                low_usage
            ));
        }
        report.push('\n');
    }

    // Helper to write version table
    fn write_version_table(report: &mut String, title: &str, versions: &[(String, u64, usize)]) {
        if versions.is_empty() {
            return;
        }
        report.push_str(&format!("{}\n", title));
        report.push_str(&format!("{:-<80}\n", ""));
        report.push_str(&format!("{:<50} {:>12} {:>12}\n", "Version", "Licenses", "Jobs"));
        report.push_str(&format!("{:-<80}\n", ""));

        let mut sorted = versions.to_vec();
        sorted.sort_by(|a, b| b.2.cmp(&a.2)); // Sort by license count

        for (version, jobs, licenses) in sorted {
            let display_version = if version.len() > 48 { &version[..48] } else { &version };
            report.push_str(&format!(
                "{:<50} {:>12} {:>12}\n",
                display_version, licenses, jobs
            ));
        }
        report.push('\n');
    }

    // ================== IMAGEFLOW SECTION ==================
    report.push_str("================================================================================\n");
    report.push_str("                              IMAGEFLOW\n");
    report.push_str("================================================================================\n\n");

    write_feature_table(&mut report, "QUERY KEYS", &imageflow_query_keys);
    write_feature_table(&mut report, "EXTRA JOB QUERY KEYS", &imageflow_extra_keys);
    write_feature_table(&mut report, "PLUGINS", &imageflow_plugins);
    write_version_table(&mut report, "VERSIONS", &imageflow_versions);

    // ================== IMAGERESIZER SECTION ==================
    report.push_str("================================================================================\n");
    report.push_str("                              IMAGERESIZER\n");
    report.push_str("================================================================================\n\n");

    write_feature_table(&mut report, "QUERY KEYS", &imageresizer_query_keys);
    write_feature_table(&mut report, "EXTRA JOB QUERY KEYS", &imageresizer_extra_keys);
    write_feature_table(&mut report, "PLUGINS", &imageresizer_plugins);
    write_version_table(&mut report, "VERSIONS", &imageresizer_versions);

    // Deprecation candidates section
    let candidates = version_tracker.find_deprecation_candidates(3);
    if !candidates.is_empty() {
        report.push_str("================================================================================\n");
        report.push_str("                    DEPRECATION CANDIDATES (<=3 licenses)\n");
        report.push_str("================================================================================\n\n");

        for c in &candidates {
            report.push_str(&format!(
                "{} '{}' (version {}): {} license(s)\n",
                c.feature_type, c.feature_name, c.version, c.license_count
            ));
            for lic in &c.licenses {
                report.push_str(&format!("  - {} ({})\n", lic.owner, lic.id));
            }
        }
    }

    report
}

fn generate_infrastructure_report(tracker: &InfrastructureTracker) -> String {
    let mut report = String::new();
    report.push_str("=== INFRASTRUCTURE REPORT ===\n\n");

    // CPU Core Distribution
    report.push_str("CPU CORE DISTRIBUTION\n");
    report.push_str(&format!("{:-<60}\n", ""));
    report.push_str(&format!("{:<20} {:>15} {:>15}\n", "Cores", "Machines", "Licenses"));
    report.push_str(&format!("{:-<60}\n", ""));

    let mut cores: Vec<_> = tracker.core_distribution.iter().collect();
    cores.sort_by_key(|(c, _)| *c);
    for (core_count, (machines, licenses)) in cores {
        report.push_str(&format!(
            "{:<20} {:>15} {:>15}\n",
            core_count, machines, licenses.len()
        ));
    }
    report.push('\n');

    // OS Architecture
    report.push_str("OS ARCHITECTURE\n");
    report.push_str(&format!("{:-<60}\n", ""));
    for (arch, (machines, _)) in &tracker.os_architecture {
        report.push_str(&format!("{}: {} machines\n", arch, machines));
    }
    report.push('\n');

    // Process Architecture
    report.push_str("PROCESS ARCHITECTURE (32-bit vs 64-bit)\n");
    report.push_str(&format!("{:-<60}\n", ""));
    for (arch, (machines, _)) in &tracker.process_architecture {
        report.push_str(&format!("{}: {} machines\n", arch, machines));
    }
    report.push('\n');

    // Filesystem Types
    report.push_str("FILESYSTEM TYPES\n");
    report.push_str(&format!("{:-<60}\n", ""));
    report.push_str(&format!("{:<20} {:>12} {:>15} {:>10}\n", "Filesystem", "Drives", "Total GB", "Licenses"));
    report.push_str(&format!("{:-<60}\n", ""));

    let mut filesystems: Vec<_> = tracker.filesystem_types.iter().collect();
    filesystems.sort_by(|a, b| b.1.1.cmp(&a.1.1)); // Sort by total GB
    for (fs, (drives, total_gb, licenses)) in filesystems {
        report.push_str(&format!(
            "{:<20} {:>12} {:>15} {:>10}\n",
            fs, drives, total_gb, licenses.len()
        ));
    }
    report.push('\n');

    // Storage Summary
    report.push_str("STORAGE SUMMARY\n");
    report.push_str(&format!("{:-<60}\n", ""));
    report.push_str(&format!("Total Storage Tracked: {} GB ({:.1} TB)\n",
        tracker.total_storage_gb,
        tracker.total_storage_gb as f64 / 1024.0
    ));
    report.push_str(&format!("Total Available: {} GB ({:.1} TB)\n",
        tracker.total_available_gb,
        tracker.total_available_gb as f64 / 1024.0
    ));
    let utilization = if tracker.total_storage_gb > 0 {
        100.0 - (tracker.total_available_gb as f64 / tracker.total_storage_gb as f64 * 100.0)
    } else {
        0.0
    };
    report.push_str(&format!("Average Utilization: {:.1}%\n", utilization));
    report.push('\n');

    // Storage Tiers
    report.push_str("STORAGE TIERS (per machine)\n");
    report.push_str(&format!("{:-<60}\n", ""));
    let tier_order = ["<100GB", "100-500GB", "500GB-1TB", "1TB+"];
    for tier in tier_order {
        let count = tracker.storage_tiers.get(tier).unwrap_or(&0);
        report.push_str(&format!("{:<20}: {} machines\n", tier, count));
    }

    report
}

fn generate_platform_report(tracker: &PlatformTracker) -> String {
    let mut report = String::new();
    report.push_str("=== PLATFORM REPORT (.NET/IIS) ===\n\n");

    // .NET Version Distribution
    if !tracker.dotnet_versions.is_empty() {
        report.push_str(".NET FRAMEWORK VERSIONS\n");
        report.push_str(&format!("{:-<60}\n", ""));
        report.push_str(&format!("{:<40} {:>15}\n", "Version", "Machines"));
        report.push_str(&format!("{:-<60}\n", ""));

        let mut versions: Vec<_> = tracker.dotnet_versions.iter().collect();
        versions.sort_by(|a, b| b.1.0.cmp(&a.1.0));
        for (version, (machines, _)) in versions {
            report.push_str(&format!("{:<40} {:>15}\n", version, machines));
        }
        report.push('\n');
    }

    // IIS Version Distribution
    if !tracker.iis_versions.is_empty() {
        report.push_str("IIS VERSIONS\n");
        report.push_str(&format!("{:-<60}\n", ""));
        report.push_str(&format!("{:<40} {:>15}\n", "Version", "Machines"));
        report.push_str(&format!("{:-<60}\n", ""));

        let mut versions: Vec<_> = tracker.iis_versions.iter().collect();
        versions.sort_by(|a, b| b.1.0.cmp(&a.1.0));
        for (version, (machines, _)) in versions {
            report.push_str(&format!("{:<40} {:>15}\n", version, machines));
        }
        report.push('\n');
    }

    // Pipeline Mode
    report.push_str("PIPELINE MODE\n");
    report.push_str(&format!("{:-<60}\n", ""));
    report.push_str(&format!("Integrated Pipeline: {}\n", tracker.integrated_pipeline_count));
    report.push_str(&format!("Classic Pipeline: {}\n", tracker.classic_pipeline_count));
    report.push_str(&format!("Async Module Enabled: {}\n", tracker.async_module_count));
    report.push('\n');

    // Cache Types
    if !tracker.cache_types.is_empty() {
        report.push_str("CACHE TYPES\n");
        report.push_str(&format!("{:-<60}\n", ""));
        report.push_str(&format!("{:<40} {:>15}\n", "Cache Type", "Machines"));
        report.push_str(&format!("{:-<60}\n", ""));

        let mut caches: Vec<_> = tracker.cache_types.iter().collect();
        caches.sort_by(|a, b| b.1.0.cmp(&a.1.0));
        for (cache, (machines, _)) in caches {
            report.push_str(&format!("{:<40} {:>15}\n", cache, machines));
        }
        report.push('\n');
    }

    // Memory Distribution
    if !tracker.memory_distribution.is_empty() {
        report.push_str("WORKING SET MEMORY DISTRIBUTION\n");
        report.push_str(&format!("{:-<60}\n", ""));
        let tier_order = ["<100MB", "100-500MB", "500MB-1GB", "1-2GB", "2GB+"];
        for tier in tier_order {
            let count = tracker.memory_distribution.get(tier).unwrap_or(&0);
            if *count > 0 {
                report.push_str(&format!("{:<20}: {} reports\n", tier, count));
            }
        }
        report.push('\n');
    }

    // Git Commits (Build Versions)
    if !tracker.git_commits.is_empty() {
        report.push_str("BUILD VERSIONS (Git Commits)\n");
        report.push_str(&format!("{:-<60}\n", ""));
        report.push_str(&format!("{:<45} {:>10}\n", "Commit", "Licenses"));
        report.push_str(&format!("{:-<60}\n", ""));

        let mut commits: Vec<_> = tracker.git_commits.iter().collect();
        commits.sort_by(|a, b| b.1.len().cmp(&a.1.len()));
        for (commit, licenses) in commits.iter().take(20) {
            let display_commit = if commit.len() > 40 { &commit[..40] } else { commit };
            report.push_str(&format!("{:<45} {:>10}\n", display_commit, licenses.len()));
        }
        if tracker.git_commits.len() > 20 {
            report.push_str(&format!("... and {} more\n", tracker.git_commits.len() - 20));
        }
    }

    report
}

fn generate_performance_report(tracker: &PerformanceTracker) -> String {
    let mut report = String::new();
    report.push_str("=== PERFORMANCE REPORT ===\n\n");

    let summary = tracker.summarize();

    // Throughput Stats
    report.push_str("JOB THROUGHPUT\n");
    report.push_str(&format!("{:-<60}\n", ""));
    report.push_str(&format!("Peak Jobs/Second: {}\n", summary.jobs_per_second_max));
    report.push_str(&format!("Average Jobs/Second: {:.1}\n", summary.jobs_per_second_avg));
    report.push_str(&format!("Peak Jobs/Minute: {}\n", summary.jobs_per_minute_max));
    report.push_str(&format!("Peak Jobs/Hour: {}\n", summary.jobs_per_hour_max));
    report.push('\n');

    // Helper to convert nanoseconds to ms with formatting
    let ns_to_ms = |ns: u32| -> String {
        if ns == 0 {
            "N/A".to_string()
        } else {
            format!("{:.2}", ns as f64 / 1_000.0) // Divide by 1000 to get microseconds, not ms
        }
    };

    // Latency Stats (values are in nanoseconds * 1000 or microseconds from the telemetry)
    report.push_str("LATENCY PERCENTILES\n");
    report.push_str(&format!("{:-<70}\n", ""));
    report.push_str(&format!("{:<25} {:>10} {:>12} {:>12} {:>10}\n",
        "Operation", "Samples", "P50 (us)", "P95 (us)", "Max (us)"));
    report.push_str(&format!("{:-<70}\n", ""));

    // Job times
    report.push_str(&format!("{:<25} {:>10} {:>12} {:>12} {:>10}\n",
        "Job Processing",
        tracker.job_times_p50.len(),
        ns_to_ms(summary.job_time_p50_median),
        ns_to_ms(summary.job_time_p95_median),
        ns_to_ms(summary.job_time_p100_max)
    ));

    // Encode times
    report.push_str(&format!("{:<25} {:>10} {:>12} {:>12} {:>10}\n",
        "Encoding",
        tracker.encode_times_p50.len(),
        ns_to_ms(summary.encode_time_p50_median),
        ns_to_ms(summary.encode_time_p95_median),
        "N/A"
    ));

    // Decode times
    report.push_str(&format!("{:<25} {:>10} {:>12} {:>12} {:>10}\n",
        "Decoding",
        tracker.decode_times_p50.len(),
        ns_to_ms(summary.decode_time_p50_median),
        ns_to_ms(summary.decode_time_p95_median),
        "N/A"
    ));

    // Blob read times
    report.push_str(&format!("{:<25} {:>10} {:>12} {:>12} {:>10}\n",
        "Blob Read",
        tracker.blob_read_times_p50.len(),
        ns_to_ms(summary.blob_read_time_p50_median),
        ns_to_ms(summary.blob_read_time_p95_median),
        "N/A"
    ));
    report.push('\n');

    // Detailed timing distribution
    report.push_str("JOB TIME DISTRIBUTION (microseconds)\n");
    report.push_str(&format!("{:-<60}\n", ""));
    if !tracker.job_times_p50.is_empty() {
        let mut sorted = tracker.job_times_p50.clone();
        sorted.sort();
        let len = sorted.len();
        let p25 = sorted[len / 4];
        let p50 = sorted[len / 2];
        let p75 = sorted[len * 3 / 4];
        let p90 = sorted[len * 9 / 10];
        let p99 = sorted[len * 99 / 100];
        let min = sorted[0];
        let max = sorted[len - 1];
        report.push_str(&format!("Min: {:.1} us\n", min as f64 / 1000.0));
        report.push_str(&format!("P25: {:.1} us\n", p25 as f64 / 1000.0));
        report.push_str(&format!("P50 (Median): {:.1} us\n", p50 as f64 / 1000.0));
        report.push_str(&format!("P75: {:.1} us\n", p75 as f64 / 1000.0));
        report.push_str(&format!("P90: {:.1} us\n", p90 as f64 / 1000.0));
        report.push_str(&format!("P99: {:.1} us\n", p99 as f64 / 1000.0));
        report.push_str(&format!("Max: {:.1} us\n", max as f64 / 1000.0));
        report.push_str(&format!("Samples: {}\n", len));
    } else {
        report.push_str("No timing data available\n");
    }
    report.push('\n');

    // Pixel Throughput
    report.push_str("PIXEL THROUGHPUT\n");
    report.push_str(&format!("{:-<60}\n", ""));
    report.push_str(&format!("Total Encoded Pixels: {} ({:.2} billion)\n",
        summary.encoded_pixels_total,
        summary.encoded_pixels_total as f64 / 1_000_000_000.0
    ));
    report.push_str(&format!("Total Decoded Pixels: {} ({:.2} billion)\n",
        summary.decoded_pixels_total,
        summary.decoded_pixels_total as f64 / 1_000_000_000.0
    ));
    report.push_str(&format!("Peak Encoded Pixels/Second: {} ({:.1} MP/s)\n",
        summary.encoded_pixels_per_sec_peak,
        summary.encoded_pixels_per_sec_peak as f64 / 1_000_000.0
    ));
    report.push_str(&format!("Peak Decoded Pixels/Second: {} ({:.1} MP/s)\n",
        summary.decoded_pixels_per_sec_peak,
        summary.decoded_pixels_per_sec_peak as f64 / 1_000_000.0
    ));

    report
}

fn generate_scaling_report(tracker: &ImageScalingTracker) -> String {
    let mut report = String::new();
    report.push_str("=== SOURCE IMAGE DIMENSION ALIGNMENT REPORT ===\n\n");
    report.push_str("This report tracks whether source image dimensions are divisible by\n");
    report.push_str("common alignment values (4, 8, 16). 8x8 alignment is optimal for JPEG\n");
    report.push_str("compression (DCT blocks), 16x16 for video codecs (macroblocks).\n\n");

    let total = tracker.total_scaling_ops();

    // Summary
    report.push_str("ALIGNMENT SUMMARY\n");
    report.push_str(&format!("{:-<60}\n", ""));
    report.push_str(&format!("Total Alignment Observations: {}\n", total));
    report.push_str(&format!("Licenses with Alignment Data: {}\n", tracker.license_scaling.len()));
    report.push('\n');

    // Distribution by alignment
    report.push_str("ALIGNMENT DISTRIBUTION\n");
    report.push_str(&format!("{:-<70}\n", ""));
    report.push_str(&format!("{:<28} {:>15} {:>12}\n", "Alignment Type", "Count", "Percentage"));
    report.push_str(&format!("{:-<70}\n", ""));

    let dist = tracker.get_distribution();
    if dist.is_empty() {
        report.push_str("No alignment data available\n");
    } else {
        for (label, count, pct) in dist {
            report.push_str(&format!("{:<25} {:>15} {:>11.1}%\n", label, count, pct));
        }
    }
    report.push('\n');

    // Detailed breakdown
    report.push_str("DETAILED BREAKDOWN BY FIELD\n");
    report.push_str(&format!("{:-<70}\n", ""));
    report.push_str(&format!("{:<30} {:>15} {:>20}\n", "Bucket", "Count", "Meaning"));
    report.push_str(&format!("{:-<70}\n", ""));

    let buckets = [
        ("source_multiple_4x4", tracker.scale_4x4, "w AND h divisible by 4"),
        ("source_multiple_8x", tracker.scale_8x, "width divisible by 8"),
        ("source_multiple_x8", tracker.scale_x8, "height divisible by 8"),
        ("source_multiple_8x8", tracker.scale_8x8, "w AND h %8 (JPEG blocks)"),
        ("source_multiple_16x16", tracker.scale_16x16, "w AND h %16 (macroblocks)"),
    ];

    for (name, count, meaning) in buckets {
        if count > 0 {
            report.push_str(&format!("{:<30} {:>15} {:>20}\n", name, count, meaning));
        }
    }
    report.push('\n');

    // Top licenses by scaling operations
    if !tracker.license_scaling.is_empty() {
        report.push_str("TOP LICENSES BY ALIGNMENT OBSERVATIONS\n");
        report.push_str(&format!("{:-<60}\n", ""));
        report.push_str(&format!("{:<25} {:>15}\n", "License ID", "Observations"));
        report.push_str(&format!("{:-<60}\n", ""));

        let mut sorted: Vec<_> = tracker.license_scaling.iter().collect();
        sorted.sort_by(|a, b| b.1.cmp(a.1));

        for (id, count) in sorted.iter().take(20) {
            let display_id = if id.len() > 23 { &id[..23] } else { id };
            report.push_str(&format!("{:<25} {:>15}\n", display_id, count));
        }

        if sorted.len() > 20 {
            report.push_str(&format!("... and {} more\n", sorted.len() - 20));
        }
    }

    report
}

fn generate_error_report(tracker: &ErrorTracker) -> String {
    let mut report = String::new();
    report.push_str("=== ERROR REPORT ===\n\n");

    // Overall Stats
    report.push_str("OVERALL STATISTICS\n");
    report.push_str(&format!("{:-<60}\n", ""));
    let total_requests = tracker.total_ok + tracker.total_errors;
    report.push_str(&format!("Total Requests: {}\n", total_requests));
    report.push_str(&format!("Successful: {} ({:.2}%)\n",
        tracker.total_ok,
        if total_requests > 0 { tracker.total_ok as f64 / total_requests as f64 * 100.0 } else { 0.0 }
    ));
    report.push_str(&format!("Errors: {} ({:.2}%)\n",
        tracker.total_errors,
        tracker.overall_error_rate()
    ));
    report.push_str(&format!("404 Not Found: {}\n", tracker.total_404));
    report.push('\n');

    // Job-level Stats
    let total_jobs = tracker.job_ok + tracker.job_errors;
    if total_jobs > 0 {
        report.push_str("JOB-LEVEL STATISTICS\n");
        report.push_str(&format!("{:-<60}\n", ""));
        report.push_str(&format!("Total Jobs: {}\n", total_jobs));
        report.push_str(&format!("Successful: {}\n", tracker.job_ok));
        report.push_str(&format!("Failed: {} ({:.2}%)\n",
            tracker.job_errors,
            tracker.job_errors as f64 / total_jobs as f64 * 100.0
        ));
        report.push('\n');
    }

    // Error Breakdown
    if !tracker.errors_by_type.is_empty() {
        report.push_str("ERROR BREAKDOWN BY TYPE\n");
        report.push_str(&format!("{:-<60}\n", ""));
        report.push_str(&format!("{:<30} {:>15} {:>12}\n", "Error Type", "Count", "% of Errors"));
        report.push_str(&format!("{:-<60}\n", ""));

        let mut errors: Vec<_> = tracker.errors_by_type.iter().collect();
        errors.sort_by(|a, b| b.1.cmp(a.1));
        for (error_type, count) in errors {
            let pct = if tracker.total_errors > 0 {
                *count as f64 / tracker.total_errors as f64 * 100.0
            } else {
                0.0
            };
            report.push_str(&format!("{:<30} {:>15} {:>11.1}%\n", error_type, count, pct));
        }
        report.push('\n');
    }

    // Response Format Distribution
    if !tracker.response_formats.is_empty() {
        report.push_str("OUTPUT FORMAT DISTRIBUTION\n");
        report.push_str(&format!("{:-<60}\n", ""));
        report.push_str(&format!("{:<20} {:>15} {:>12}\n", "Format", "Count", "Percentage"));
        report.push_str(&format!("{:-<60}\n", ""));

        let total_response: i64 = tracker.response_formats.values().sum();
        let mut formats: Vec<_> = tracker.response_formats.iter().collect();
        formats.sort_by(|a, b| b.1.cmp(a.1));
        for (format, count) in formats {
            let pct = if total_response > 0 {
                *count as f64 / total_response as f64 * 100.0
            } else {
                0.0
            };
            report.push_str(&format!("{:<20} {:>15} {:>11.1}%\n", format, count, pct));
        }
        report.push('\n');
    }

    // Source Format Distribution
    if !tracker.source_formats.is_empty() {
        report.push_str("SOURCE FORMAT DISTRIBUTION\n");
        report.push_str(&format!("{:-<60}\n", ""));
        report.push_str(&format!("{:<20} {:>15} {:>12}\n", "Format", "Count", "Percentage"));
        report.push_str(&format!("{:-<60}\n", ""));

        let total_source: i64 = tracker.source_formats.values().sum();
        let mut formats: Vec<_> = tracker.source_formats.iter().collect();
        formats.sort_by(|a, b| b.1.cmp(a.1));
        for (format, count) in formats {
            let pct = if total_source > 0 {
                *count as f64 / total_source as f64 * 100.0
            } else {
                0.0
            };
            report.push_str(&format!("{:<20} {:>15} {:>11.1}%\n", format, count, pct));
        }
        report.push('\n');
    }

    // High Error Licenses
    let high_error = tracker.high_error_licenses(5.0);
    if !high_error.is_empty() {
        report.push_str("LICENSES WITH HIGH ERROR RATES (>5%)\n");
        report.push_str(&format!("{:-<80}\n", ""));
        report.push_str(&format!("{:<20} {:>12} {:>15} {:>15}\n", "License ID", "Error Rate", "OK", "Errors"));
        report.push_str(&format!("{:-<80}\n", ""));

        for (id, rate, ok, errors) in high_error.iter().take(20) {
            let display_id = if id.len() > 18 { &id[..18] } else { id };
            report.push_str(&format!("{:<20} {:>11.1}% {:>15} {:>15}\n", display_id, rate, ok, errors));
        }
        if high_error.len() > 20 {
            report.push_str(&format!("... and {} more\n", high_error.len() - 20));
        }
    }

    report
}

fn generate_plugin_report(data: &HashMap<String, GroupedSinks>) -> String {
    let mut report = String::new();
    report.push_str("=== PLUGIN REPORT ===\n\n");

    // Collect plugins with license counts
    let mut plugin_licenses: HashMap<String, HashSet<String>> = HashMap::new();

    for (license_id, grouped) in data {
        let summary = grouped.all.summarize();
        for plugin in &summary.plugins {
            plugin_licenses
                .entry(plugin.clone())
                .or_insert_with(HashSet::new)
                .insert(license_id.clone());
        }
    }

    // Sort by license count descending
    let mut sorted: Vec<_> = plugin_licenses
        .iter()
        .map(|(plugin, licenses)| (plugin.clone(), licenses.len()))
        .collect();
    sorted.sort_by(|a, b| b.1.cmp(&a.1));

    report.push_str("PLUGINS BY LICENSE COUNT\n");
    report.push_str(&format!("{:-<80}\n", ""));
    report.push_str(&format!("{:>10}  {}\n", "Licenses", "Plugin Name"));
    report.push_str(&format!("{:-<80}\n", ""));

    if sorted.is_empty() {
        report.push_str("No plugins recorded\n");
    } else {
        for (plugin, count) in &sorted {
            report.push_str(&format!("{:>10}  {}\n", count, plugin));
        }
    }

    report.push_str(&format!("\nTotal unique plugins: {}\n", sorted.len()));
    report
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::log_syntax::SplitLogColumns;
}
