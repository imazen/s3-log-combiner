use crate::telemetry::Report;
use crate::telemetry::Summary;
use std::collections::{HashMap, HashSet};
use std::ops::Index;
use std::path::{Path, PathBuf};
use std::process;
use std::sync::Arc;
use std::{env, fs};

use crate::cli::QueryParseArgs;
use crate::log_syntax::{S3LogLine, SplitLogColumns};
use chrono::{DateTime, Datelike, TimeZone, Utc};
use futures_util::{StreamExt, TryStreamExt};
use rusoto_core::{HttpClient, Region};
use rusoto_s3::{S3Client, S3};

#[derive(Debug, Clone)]
struct SplitUnique {
    unique_id: String,
    last_full_report_from: DateTime<Utc>,
    last_full_report: Option<Report>,
    last_report_from: DateTime<Utc>,
    last_report: Option<Report>,
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
use crate::license_blob::LicenseStatus;
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
        let mut lines = reader.lines();

        while let Some(line) = lines.next_line().await? {
            tx.send(line)
                .await
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
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
        let bytes = stream.map_ok(|b| b.to_vec()).try_concat().await.unwrap();

        let str = std::str::from_utf8(&bytes).unwrap();

        let blob = crate::license_blob::LicenseBlob::from(str).unwrap();

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
    all: SplitDataSink,
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
            println!("Processed {}k lines", line_count / 1000);
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

    // Summarization Phase
    write_summaries(&data, &write_tx)?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::log_syntax::SplitLogColumns;
}
