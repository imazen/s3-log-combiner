mod cli;

use futures_util::{StreamExt, TryStreamExt};
use rusoto_core::{HttpClient, Region, RusotoError};
use rusoto_s3::{GetObjectError, GetObjectRequest, ListObjectsV2Request, S3Client, S3};
use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tokio::fs;
use tokio::fs::File;
use tokio::io::{AsyncWriteExt, BufWriter};
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::{mpsc, Mutex};
use tokio::time::sleep;
use tokio_stream::wrappers::ReceiverStream;

#[derive(Debug, Clone)]
pub struct Config {
    pub bucket: String,
    pub region: String,
    pub output_directory: PathBuf,
    pub start_at: Option<String>,
    pub max_connections: usize,
    pub target_size_kb: usize,
    pub access_key: String,
    pub secret_key: String,
    pub quiet: bool,
    pub verbose: bool,
}

#[derive(Debug, Clone)]
pub struct BlobEntry {
    pub name: String,
    pub index: usize,
    pub size: Option<i64>,
    pub batch_index: usize,
    pub first_index_of_batch: usize,
}
#[derive(Debug, Clone)]
struct BlobResult {
    entry: BlobEntry,
    contents: Option<Vec<u8>>,
    error: Option<Box<String>>,
}

#[derive(Debug, Clone)]
struct OrderedBlobResult {
    blob: BlobResult,
    open_new_file: Option<PathBuf>,
}
#[derive(Debug, Clone)]
struct BatchOutputMetadata {
    first_item_index: Option<usize>,
    last_item_index: Option<usize>,
    final_path: Option<std::path::PathBuf>,
    pending: bool,
}

struct DataCollection {
    outputs: Arc<Mutex<Vec<BatchOutputMetadata>>>,
    results: Arc<Mutex<BTreeMap<usize, BlobResult>>>,
}

async fn drain_ready_batches(
    finished: bool,
    data: &DataCollection,
    tx: Sender<OrderedBlobResult>,
    config: &Config,
) {
    let mut outputs = data.outputs.lock().await;
    let mut results = data.results.lock().await;

    if finished {
        if let Some((_, last_blob)) = results.iter().next_back() {
            if !outputs.is_empty() {
                let last_output = outputs.last_mut().unwrap();
                if last_output.pending && last_output.last_item_index.is_none() {
                    if config.verbose {
                        println!(
                            "Last batch of blobs needed a last_item_index, using {:?}",
                            last_blob.entry.index
                        );
                    }
                    last_output.last_item_index = Some(last_blob.entry.index);
                }
            }
        }
    }

    for output in outputs.iter_mut() {
        if output.pending {
            if let Some(last_item_index) = output.last_item_index {
                if output.final_path.is_none() || output.first_item_index.is_none() {
                    if results.contains_key(&last_item_index) {
                        let last_item = results.get(&last_item_index).unwrap();
                        if output.first_item_index.is_none() {
                            output.first_item_index = Some(last_item.entry.first_index_of_batch)
                        }

                        if output.final_path.is_none() {
                            // Construct the final path for the batch
                            let mut final_path = std::path::PathBuf::from(&config.output_directory);
                            final_path.push(last_item.entry.name.to_string());
                            output.final_path = Some(final_path);
                        }
                    } else if finished {
                        panic!("Cannot complete this batch");
                    }
                }

                if output.first_item_index.is_none() {
                    continue;
                }
                let first_item_index = output.first_item_index.unwrap();

                if last_item_index < first_item_index {}
                // Check if all items in the batch are ready
                let mut all_items_ready = true;
                let batch_size = last_item_index as i64 - first_item_index as i64;
                for i in first_item_index..=last_item_index {
                    if !results.contains_key(&i) {
                        all_items_ready = false;
                        break;
                    }
                }

                if all_items_ready {
                    if config.verbose {
                        println!(
                            "Sending {} ordered results to the writer thread.",
                            batch_size
                        );
                    }
                    // All items are ready, send OrderedBlobResult for each
                    for i in first_item_index..=last_item_index {
                        if let Some(blob_result) = results.remove(&i) {
                            let open_new_file = if i == first_item_index {
                                output.final_path.clone()
                            } else {
                                None
                            };
                            let ordered_result = OrderedBlobResult {
                                blob: blob_result,
                                open_new_file,
                            };
                            if tx.send(ordered_result).await.is_err() {
                                eprintln!("Failed to send OrderedBlobResult");
                                panic!();
                            }
                        }
                    }
                    output.pending = false; // Mark this batch as not pending anymore
                } else if finished {
                    panic!("Not all items completed in the batch")
                }
            } else if finished {
                panic!("Failed to determine a value for last_item_index in batch")
            }
        }
    }

    // Clean up: remove any outputs that are not pending anymore
    outputs.retain(|output| output.pending);
}
impl Default for BatchOutputMetadata {
    fn default() -> Self {
        BatchOutputMetadata {
            first_item_index: None, // No items initially
            last_item_index: None,  // No items initially
            final_path: None,       // No path initially
            pending: true,          // Assuming new batches are pending by default
        }
    }
}
fn populate<T: Clone>(index: usize, v: &mut Vec<T>, default_value: T) {
    let target_len = index + 1; // Convert index to usize for compatibility with Vec length

    if target_len > v.len() {
        // If the target length is greater than the current length,
        // extend the vector with the default value until it reaches the target length.
        v.resize(target_len, default_value);
    }
}
async fn reorder_logs(
    rx: Receiver<BlobResult>,
    tx: Sender<OrderedBlobResult>,
    config_copy: Config,
) {
    let root = DataCollection {
        outputs: Arc::new(Mutex::new(Vec::new())),
        results: Arc::new(Mutex::new(BTreeMap::new())),
    };
    let config = &config_copy;
    let out = &root;
    let rx_stream = ReceiverStream::new(rx);
    rx_stream
        .for_each(|blob| {
            let blob_index = blob.entry.index;
            let batch_index = blob.entry.batch_index;
            let first_index_of_batch = blob.entry.first_index_of_batch;
            let tx_clone = tx.clone();
            //print!("~");
            async move {
                if blob_index == first_index_of_batch {
                    let mut outputs_lock = (&out).outputs.lock().await;
                    populate(batch_index, outputs_lock.as_mut(), Default::default());
                    outputs_lock[batch_index].first_item_index = Some(first_index_of_batch);

                    if batch_index > 0 {
                        let prev_batch_index = batch_index - 1;
                        outputs_lock[prev_batch_index].last_item_index =
                            Some(first_index_of_batch - 1);
                    }
                }
                {
                    let mut results_lock = (&out).results.lock().await;
                    if let Some(replaced) = results_lock.insert(blob_index, blob) {
                        eprintln!("BUG? Oops, replaced {:?}", replaced);
                    }
                }
                if blob_index > 0 && blob_index % 100 == 0 {
                    drain_ready_batches(false, &out, tx_clone.clone(), &config).await;
                }
            }
        })
        .await;
    if !config.quiet {
        println!("Emptied unordered results stream");
    }
    drain_ready_batches(true, &out, tx, &config).await;
}

async fn create_parent_dirs_if_missing(file_path: &Path) -> std::io::Result<()> {
    if let Some(parent) = file_path.parent() {
        fs::create_dir_all(parent).await?;
    }
    Ok(())
}

async fn create_dirs_if_missing(file_path: &Path) {
    if let Err(e) = create_parent_dirs_if_missing(&file_path).await {
        panic!("Failed to create directories: {}", e);
    } else {
    }
}

#[tokio::main]
async fn main() {
    let config = cli::parse_args();

    // Use the `config` for the rest of your application logic
    if !config.quiet {
        println!(
            "Configuration: {:#?}",
            Config {
                access_key: "[REDACTED]".to_string(),
                secret_key: "[REDACTED]".to_string(),
                ..config.clone()
            }
        ); // Placeholder for demonstration
    }

    // Example of using the config
    let region = config
        .region
        .parse::<Region>()
        .expect("Failed to parse AWS region.");
    let s3_client = Arc::new(S3Client::new_with(
        HttpClient::new().expect("Failed to create HTTP client"),
        rusoto_core::credential::StaticProvider::new(
            config.access_key.to_string(),
            config.secret_key.to_string(),
            None,
            None,
        ),
        region,
    ));

    // Create channels for each stage of the pipeline
    let (entry_tx, entry_rx) = mpsc::channel::<BlobEntry>(config.max_connections * 4);
    let (blob_tx, blob_rx) = mpsc::channel::<BlobResult>(config.max_connections * 4);
    let (ordered_tx, ordered_rx) = mpsc::channel::<OrderedBlobResult>(config.max_connections * 4);

    // Launch listing task
    let listing_task = tokio::spawn(list_s3_logs(s3_client.clone(), config.clone(), entry_tx));

    // Launch fetching task
    let fetching_task = tokio::spawn(fetch_logs(
        s3_client.clone(),
        entry_rx,
        blob_tx,
        config.clone(),
    ));

    // Launch reordering task
    let reordering_task = tokio::spawn(reorder_logs(blob_rx, ordered_tx, config.clone()));

    // Launch writing task
    let writing_task = tokio::spawn(write_logs(ordered_rx));

    // Wait for all tasks to complete
    let (_listing_result, _fetching_result, _reordering_result, _writing_result) =
        tokio::join!(listing_task, fetching_task, reordering_task, writing_task);
}

async fn fetch_logs(
    s3: Arc<S3Client>,
    rx: Receiver<BlobEntry>,
    tx: Sender<BlobResult>,
    config: Config,
) {
    let rx_stream = ReceiverStream::new(rx);
    rx_stream
        .for_each_concurrent(config.max_connections, |entry| {
            let s3_clone = s3.clone();
            let tx_clone = tx.clone();
            let config_clone = config.clone();
            async move {
                let mut attempts = 0;
                let max_attempts = 5;
                let mut backoff = Duration::from_millis(200); // Start with 200ms

                loop {
                    let get_req = GetObjectRequest {
                        bucket: config_clone.bucket.clone(),
                        key: entry.name.clone(),
                        ..Default::default()
                    };

                    match s3_clone.get_object(get_req).await {
                        Ok(output) => {
                            let contents = if let Some(stream) = output.body {
                                let body = stream.map_ok(|b| b.to_vec()).try_concat().await;
                                body.ok()
                            } else {
                                None
                            };
                            let result = BlobResult {
                                entry: entry.clone(),
                                contents,
                                error: None,
                            };
                            tx_clone.send(result).await.unwrap(); // Handle send error in real application
                            break; // Success, exit loop
                        }
                        Err(e) => {
                            attempts += 1;
                            if attempts >= max_attempts || !is_recoverable(&e) {
                                eprintln!(
                                    "ERROR: Unrecoverable blob fetch error for {:?}: {:?}",
                                    entry.name.clone(),
                                    e
                                );

                                let result = BlobResult {
                                    entry: entry.clone(),
                                    contents: None,
                                    error: Some(Box::new(format!("{:?}", e))),
                                };
                                tx_clone.send(result).await.unwrap(); // Handle send error in real application
                                break; // Exit loop after max attempts or non-recoverable error
                            } else {
                                if (!config.quiet && attempts > 1) || config.verbose {
                                    eprintln!(
                                        "Retrying {:?} in  {:?}",
                                        entry.name.clone(),
                                        backoff
                                    );
                                }
                                sleep(backoff).await;
                                backoff *= 2; // Exponential backoff
                            }
                        }
                    }
                }
            }
        })
        .await;
}

fn is_recoverable(error: &RusotoError<GetObjectError>) -> bool {
    matches!(
        error,
        RusotoError::Unknown(ref r) if matches!(
            r.status.as_u16(),
            429 | 500 | 502 | 503 | 504
        ),

    ) || matches!(error, RusotoError::HttpDispatch(_))
    // Additional error handling logic can be added here if there are other
    // specific error variants or conditions you want to handle.
}

async fn list_s3_logs(s3: Arc<S3Client>, config: Config, tx: Sender<BlobEntry>) {
    let mut continuation_token = None;
    let mut max_errors = 5;
    let mut batch_bytes = 0;
    let mut batch_index = 0;
    let mut first_index_of_batch = 0;
    let mut total_listed = 0;
    let mut ix = 0;
    loop {
        let list_req = ListObjectsV2Request {
            bucket: config.bucket.to_string(),
            start_after: config.start_at.clone(),
            max_keys: Some(config.max_connections as i64),
            continuation_token: continuation_token.clone(),
            ..Default::default()
        };

        match s3.list_objects_v2(list_req).await {
            Ok(output) => {
                if let Some(contents) = output.contents {
                    total_listed += contents.len();
                    if config.verbose {
                        println!("Listed {:?} blobs in total", total_listed);
                    }
                    for object in contents {
                        if let Some(key) = object.key {
                            let size = object.size.unwrap_or(0);

                            if batch_bytes > 0
                                && batch_bytes + size > (config.target_size_kb * 1000) as i64
                            {
                                // Too big, time to split
                                batch_index += 1;
                                first_index_of_batch = ix;
                                batch_bytes = 0;
                            }
                            batch_bytes += size;
                            if let Err(e) = tx
                                .send(BlobEntry {
                                    index: ix,
                                    name: key,
                                    first_index_of_batch,
                                    size: object.size,
                                    batch_index,
                                })
                                .await
                            {
                                panic!("Error sending key through channel: {:?}", e);
                            }
                            ix += 1;
                        }
                    }
                    max_errors += 1; // For each success, increment error tolerance
                }
                if let Some(false) = output.is_truncated {
                    break;
                }
                continuation_token = output.next_continuation_token;
            }
            Err(e) => {
                eprintln!("Failed to list objects: {:?}", e);
                max_errors -= 1;
                if max_errors < 0 {
                    panic!();
                }
            }
        }
    }
}

async fn write_logs(rx: Receiver<OrderedBlobResult>) {
    let mut current_writer: Option<BufWriter<File>> = None;
    let mut current_writer_path: Option<PathBuf> = None;
    let mut current_error_path: Option<PathBuf> = None;
    let mut current_blob_count = 0;
    let mut current_error_writer: Option<BufWriter<File>> = None;

    let mut stream = ReceiverStream::new(rx);

    while let Some(result) = stream.next().await {
        if let Some(new_file) = &result.open_new_file {
            // Flush and close the current file if it exists
            let has_writer = current_writer.is_some();
            if let Some(mut writer) = current_writer.take() {
                writer
                    .flush()
                    .await
                    .expect("Failed to flush and close file");
            }

            if has_writer {
                if let Some(ref current_path) = current_writer_path {
                    if current_error_writer.is_none() {
                        // rename.
                        let mut final_name = current_path.clone();
                        final_name.set_extension("");
                        tokio::fs::rename(current_path, &final_name).await.unwrap();
                        println!(
                            "Combined {:?} blobs into {:?}",
                            current_blob_count,
                            &final_name.file_name().unwrap()
                        );
                    }
                }
            }
            if let Some(mut writer) = current_error_writer.take() {
                writer
                    .flush()
                    .await
                    .expect("Failed to flush and close error file");
                eprintln!(
                    "See unrecoverable errors in {:?}",
                    current_error_path.unwrap()
                )
            }
            let mut writer_path = new_file.clone();
            writer_path.set_extension("incomplete");
            current_writer_path = Some(writer_path.clone());
            let mut err_path = new_file.clone();
            err_path.set_extension("err");
            current_error_path = Some(err_path.clone());
            current_blob_count = 0;
            // Create new file.
            create_dirs_if_missing(&writer_path).await;

            // delete .incomplete and .err if they exist
            let err_exists = tokio::fs::try_exists(&err_path).await;
            if let Ok(true) = err_exists {
                let _ = tokio::fs::remove_file(&err_path).await;
            }

            // Will overwrite anyway
            let file = File::create(&writer_path)
                .await
                .expect("Failed to create file");
            current_writer = Some(BufWriter::new(file));
            current_error_writer = None;
        }

        // Write contents to the current file
        if let Some(mut contents) = result.blob.contents {
            current_blob_count += 1;
            if let Some(writer) = current_writer.as_mut() {
                if !contents.ends_with(b"\n") {
                    contents.push(b'\n');
                }
                writer
                    .write_all(&contents)
                    .await
                    .expect("Failed to write data");
            }
        }
        if let Some(err) = result.blob.error {
            if current_error_writer.is_none() {
                let file = File::create(current_error_path.as_ref().unwrap())
                    .await
                    .expect("Failed to create error file");
                current_error_writer = Some(BufWriter::new(file));
            }
            if let Some(w) = current_error_writer.as_mut() {
                w.write(
                    format!(
                        "Failed to fetch {:?} - error: {:?}\n",
                        result.blob.entry.name, err
                    )
                    .as_bytes(),
                )
                .await
                .expect("TODO: panic message");
            }
        }
    }

    // Flush and close the last file if it exists
    if let Some(mut writer) = current_writer {
        writer
            .flush()
            .await
            .expect("Failed to flush and close the last file");
    }
    if let Some(mut writer) = current_error_writer {
        writer
            .flush()
            .await
            .expect("Failed to flush and close the last error file");
    }
    println!("Flushed all files to disk. Complete!");
}

impl PartialEq for BlobResult {
    fn eq(&self, other: &Self) -> bool {
        self.entry.index == other.entry.index
    }
}

impl Eq for BlobResult {}

impl PartialOrd for BlobResult {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for BlobResult {
    fn cmp(&self, other: &Self) -> Ordering {
        self.entry.index.cmp(&other.entry.index)
    }
}
