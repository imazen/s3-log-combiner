use futures_util::{StreamExt, TryStreamExt};
use rusoto_core::{HttpClient, Region, RusotoError};
use rusoto_s3::{GetObjectError, GetObjectRequest, ListObjectsV2Request, S3Client, S3};
use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::fs;
use tokio::fs::File;
use tokio::io::{AsyncWriteExt, BufWriter};
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::{mpsc, Mutex};
use tokio::time::sleep;
use tokio_stream::wrappers::ReceiverStream;

static LIST_REQUESTS: AtomicUsize = AtomicUsize::new(0);
static LIST_REQUEST_EST_BYTES: AtomicUsize = AtomicUsize::new(0);
static BATCHES_STARTED: AtomicUsize = AtomicUsize::new(0);
static FETCHES_ENQUEUED: AtomicUsize = AtomicUsize::new(0);
static FETCHES_DEQUEUED: AtomicUsize = AtomicUsize::new(0);

static BLOB_BYTES_READ: AtomicUsize = AtomicUsize::new(0);
static BLOB_RESULTS_ENQUEUED: AtomicUsize = AtomicUsize::new(0);
static BLOB_RESULTS_DEQUEUED: AtomicUsize = AtomicUsize::new(0);

static ORDERED_WRITES_ENQUEUED: AtomicUsize = AtomicUsize::new(0);
static ORDERED_WRITES_DEQUEUED: AtomicUsize = AtomicUsize::new(0);

static FILES_WRITTEN: AtomicUsize = AtomicUsize::new(0);
static UNRECOVERABLE_ERRORS: AtomicUsize = AtomicUsize::new(0);
static BLOB_BYTES_WRITTEN: AtomicUsize = AtomicUsize::new(0);

static LAST_UPDATE_BYTES_READ: AtomicUsize = AtomicUsize::new(0);
static LAST_UPDATE_EST_LIST_BYTES_READ: AtomicUsize = AtomicUsize::new(0);
static LAST_UPDATE_BYTES_WRITTEN: AtomicUsize = AtomicUsize::new(0);
static LAST_UPDATE_PRINTED: AtomicU64 = AtomicU64::new(0);

static JOB_FINISHED: AtomicBool = AtomicBool::new(false);

static UPDATE_FREQUENCY_SECONDS: i32 = 8;

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
    pub retry: usize,
    pub continue_on_fatal_error: bool,
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
    final_path: Option<PathBuf>,
    pending: bool,
}

struct DataCollection {
    outputs: Arc<Mutex<Vec<BatchOutputMetadata>>>,
    results: Arc<Mutex<BTreeMap<usize, BlobResult>>>,
}

pub async fn fetch(config: Config) {
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

    // The channel between listing and fetching
    let (entry_tx, entry_rx) = mpsc::channel::<BlobEntry>(config.max_connections * 2);
    // The channel between fetching and ordering
    let (blob_tx, blob_rx) = mpsc::channel::<BlobResult>(config.target_size_kb);
    // The channel between ordered results and writing to disk
    let (ordered_tx, ordered_rx) = mpsc::channel::<OrderedBlobResult>(config.max_connections);

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

    let status_task = tokio::spawn(print_status_update_tokio(config.clone()));
    // Wait for all tasks to complete
    let (_listing_result, _fetching_result, _reordering_result, _writing_result) =
        tokio::join!(listing_task, fetching_task, reordering_task, writing_task);
    status_task.abort();
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
        if !outputs.is_empty() {
            let last_output = outputs.last_mut().unwrap();
            if last_output.pending && last_output.last_item_index.is_none() {
                let mut last_result_index = None;
                if let Some((_, last_blob)) = results.iter().next_back() {
                    last_result_index = Some(last_blob.entry.index);
                } else {
                    eprintln!("No results exist ")
                }
                if config.verbose {
                    println!(
                        "Last batch of blobs needed a last_item_index, using {:?}",
                        last_result_index
                    );
                }
                last_output.last_item_index = last_result_index;
            }
        }
    }
    let mut output_ix = -1;
    let outputs_count = outputs.len();
    for output in outputs.iter_mut() {
        output_ix += 1;
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
                            ORDERED_WRITES_ENQUEUED
                                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        }
                    }
                    output.pending = false; // Mark this batch as not pending anymore
                } else if finished {
                    panic!(
                        "Not all components finished downloading in batch {:?} of {:?}",
                        output_ix + 1,
                        outputs_count
                    );
                }
            } else if finished {
                eprintln!(
                    "Failed to determine a value for last_item_index in batch {:?} of {:?}",
                    output_ix + 1,
                    outputs_count
                );
            }
        }
    }
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
            BLOB_RESULTS_DEQUEUED.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
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
                if blob_index > 0 && blob_index == first_index_of_batch || blob_index % 1000 == 0 {
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

async fn print_status_update_tokio(config: Config) {
    if config.quiet {
        return;
    }
    loop {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_secs();
        let last_update = LAST_UPDATE_PRINTED.load(std::sync::atomic::Ordering::Relaxed);

        if now - last_update >= UPDATE_FREQUENCY_SECONDS as u64 {
            if last_update != 0 {
                let bytes_read_since_last_update = BLOB_BYTES_READ
                    .load(std::sync::atomic::Ordering::Relaxed)
                    - LAST_UPDATE_BYTES_READ.load(std::sync::atomic::Ordering::Relaxed)
                    + LIST_REQUEST_EST_BYTES.load(std::sync::atomic::Ordering::Relaxed)
                    - LAST_UPDATE_EST_LIST_BYTES_READ.load(std::sync::atomic::Ordering::Relaxed);
                let bytes_written_since_last_update = BLOB_BYTES_WRITTEN
                    .load(std::sync::atomic::Ordering::Relaxed)
                    - LAST_UPDATE_BYTES_WRITTEN.load(std::sync::atomic::Ordering::Relaxed);

                let mbps_read = (bytes_read_since_last_update as f64 / 1_024_000.0)
                    / (UPDATE_FREQUENCY_SECONDS as f64)
                    * 8f64;
                let mbps_written = (bytes_written_since_last_update as f64 / 1_024_000.0)
                    / (UPDATE_FREQUENCY_SECONDS as f64)
                    * 8f64;

                let fetches_net = FETCHES_ENQUEUED.load(std::sync::atomic::Ordering::Relaxed)
                    as i64
                    - FETCHES_DEQUEUED.load(std::sync::atomic::Ordering::Relaxed) as i64;
                let blob_results_net =
                    BLOB_RESULTS_ENQUEUED.load(std::sync::atomic::Ordering::Relaxed) as i64
                        - BLOB_RESULTS_DEQUEUED.load(std::sync::atomic::Ordering::Relaxed) as i64;

                let blob_reordering =
                    BLOB_RESULTS_DEQUEUED.load(std::sync::atomic::Ordering::Relaxed) as i64
                        - ORDERED_WRITES_ENQUEUED.load(std::sync::atomic::Ordering::Relaxed) as i64;

                let ordered_writes_net =
                    ORDERED_WRITES_ENQUEUED.load(std::sync::atomic::Ordering::Relaxed) as i64
                        - ORDERED_WRITES_DEQUEUED.load(std::sync::atomic::Ordering::Relaxed) as i64;

                let avg_blobs_per_list = FETCHES_ENQUEUED
                    .load(std::sync::atomic::Ordering::Relaxed)
                    / (LIST_REQUESTS.load(std::sync::atomic::Ordering::Relaxed) + 1);

                let total_unrecoverable_errors =
                    UNRECOVERABLE_ERRORS.load(std::sync::atomic::Ordering::Relaxed);
                let total_gb_read = BLOB_BYTES_READ.load(std::sync::atomic::Ordering::Relaxed)
                    as f64
                    / 1_024_000_000.0;
                let total_gb_written = BLOB_BYTES_WRITTEN.load(std::sync::atomic::Ordering::Relaxed)
                    as f64
                    / 1_024_000_000.0;
                let total_requests = LIST_REQUESTS.load(std::sync::atomic::Ordering::Relaxed)
                    + BLOB_RESULTS_ENQUEUED.load(std::sync::atomic::Ordering::Relaxed);

                println!("{:.2}Mbps down, {:.2}Mbps disk, {} fetches queued, {} results reordering, {} results queued, {} writes queued, {} fatal errors, {:.2} Gb written, {:.2} Gb fetched, {} HTTP requests, {} avg blobs/listing",
                             mbps_read, mbps_written, fetches_net, blob_reordering, blob_results_net, ordered_writes_net, total_unrecoverable_errors, total_gb_written, total_gb_read, total_requests, avg_blobs_per_list);
            }
            LAST_UPDATE_BYTES_READ.store(
                BLOB_BYTES_READ.load(std::sync::atomic::Ordering::Relaxed),
                std::sync::atomic::Ordering::Relaxed,
            );
            LAST_UPDATE_BYTES_WRITTEN.store(
                BLOB_BYTES_WRITTEN.load(std::sync::atomic::Ordering::Relaxed),
                std::sync::atomic::Ordering::Relaxed,
            );
            LAST_UPDATE_EST_LIST_BYTES_READ.store(
                LIST_REQUEST_EST_BYTES.load(std::sync::atomic::Ordering::Relaxed),
                std::sync::atomic::Ordering::Relaxed,
            );
            LAST_UPDATE_PRINTED.store(now, std::sync::atomic::Ordering::Relaxed);
        }

        sleep(Duration::from_secs(UPDATE_FREQUENCY_SECONDS as u64)).await;
    }
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
                //tokio::task::spawn(
                fetch_log(entry, s3_clone, tx_clone, config_clone).await; //);
            }
        })
        .await;
}

async fn fetch_log(
    entry: BlobEntry,
    s3_clone: Arc<S3Client>,
    tx_clone: Sender<BlobResult>,
    config: Config,
) {
    FETCHES_DEQUEUED.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

    let mut attempts = 0;
    let max_attempts = config.retry;
    let mut backoff = Duration::from_millis(200); // Start with 200ms

    loop {
        let get_req = GetObjectRequest {
            bucket: config.bucket.clone(),
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
                let byte_count = contents.as_ref().map(|c| c.len()).unwrap_or(0);
                let result = BlobResult {
                    entry: entry.clone(),
                    contents,
                    error: None,
                };
                tx_clone.send(result).await.unwrap(); // Handle send error in real application
                BLOB_RESULTS_ENQUEUED.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                BLOB_BYTES_READ.fetch_add(byte_count, std::sync::atomic::Ordering::Relaxed);
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
                    UNRECOVERABLE_ERRORS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    if !config.continue_on_fatal_error {
                        std::process::exit(2);
                    }
                    let result = BlobResult {
                        entry: entry.clone(),
                        contents: None,
                        error: Some(Box::new(format!("{:?}", e))),
                    };
                    tx_clone.send(result).await.unwrap(); // Handle send error in real application
                    break; // Exit loop after max attempts or non-recoverable error
                } else {
                    if (!config.quiet && attempts > 1) || config.verbose {
                        eprintln!("Retrying {:?} in  {:?}", entry.name.clone(), backoff);
                    }
                    sleep(backoff).await;
                    backoff *= 2; // Exponential backoff
                }
            }
        }
    }
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
    let mut max_keys = 500i64;
    let max_max_keys = 99000
        .min(config.target_size_kb as i64 * 2)
        .min((config.max_connections as i64 * 10).max(max_keys));
    if !config.quiet {
        println!(
            "Requesting between {} and {} objects per ListObjectsV2 request.",
            max_keys, max_max_keys
        );
    }

    loop {
        let list_req = ListObjectsV2Request {
            bucket: config.bucket.to_string(),
            start_after: config.start_at.clone(),
            max_keys: Some(max_keys),
            continuation_token: continuation_token.clone(),
            ..Default::default()
        };
        if max_keys < max_max_keys {
            // approach max_max_keys exponentially
            // Slow start makes for a fast start.
            max_keys = (max_keys * 2).min(max_max_keys);
        }
        match s3.list_objects_v2(list_req).await {
            Ok(output) => {
                LIST_REQUESTS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

                if let Some(contents) = output.contents {
                    total_listed += contents.len();

                    let mut est_bytes = 0;
                    if config.verbose {
                        println!("Listed {:?} blobs in total", total_listed);
                    }
                    for object in contents {
                        est_bytes += object.e_tag.map(|t| t.len() + 15).unwrap_or(0)
                                + object.last_modified.map(|t| t.len() + 30).unwrap_or(0)
                                + object.storage_class.map(|t| t.len() + 30).unwrap_or(0)
                                //+ object.owner.map(|t| t.display_name.len() + 15).unwrap_or(0)
                                + object.size.as_ref().map(|ref t| 30).unwrap_or(0)
                                + object.key.as_ref().map(|ref t| t.len() + 30).unwrap_or(0);
                        if let Some(key) = object.key {
                            let size = object.size.unwrap_or(0);

                            if batch_bytes > 0
                                && batch_bytes + size > (config.target_size_kb * 1000) as i64
                            {
                                // Too big, time to split
                                batch_index += 1;
                                first_index_of_batch = ix;
                                batch_bytes = 0;
                                BATCHES_STARTED.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
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
                            FETCHES_ENQUEUED.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            ix += 1;
                        }
                    }
                    LIST_REQUEST_EST_BYTES
                        .fetch_add(est_bytes, std::sync::atomic::Ordering::Relaxed);
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

async fn finish_writing(
    current_writer: Option<&mut BufWriter<File>>,
    current_error_writer: Option<&mut BufWriter<File>>,
    current_writer_path: Option<&PathBuf>,
    current_error_path: Option<&PathBuf>,
    current_blob_count: i32,
) {
    // Flush and close the current file if it exists
    let has_writer = current_writer.is_some();
    if let Some(writer) = current_writer {
        writer
            .flush()
            .await
            .expect("Failed to flush and close file");

        FILES_WRITTEN.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    if has_writer {
        if let Some(current_path) = current_writer_path {
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
    if let Some(writer) = current_error_writer {
        writer
            .flush()
            .await
            .expect("Failed to flush and close error file");
        eprintln!(
            "See unrecoverable errors in {:?}",
            current_error_path.unwrap()
        )
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
        ORDERED_WRITES_DEQUEUED.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        if let Some(new_file) = &result.open_new_file {
            finish_writing(
                current_writer.as_mut(),
                current_error_writer.as_mut(),
                current_writer_path.as_ref(),
                current_error_path.as_ref(),
                current_blob_count,
            )
            .await;
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

                BLOB_BYTES_WRITTEN.fetch_add(contents.len(), std::sync::atomic::Ordering::Relaxed);
            } else {
                panic!();
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
    finish_writing(
        current_writer.as_mut(),
        current_error_writer.as_mut(),
        current_writer_path.as_ref(),
        current_error_path.as_ref(),
        current_blob_count,
    )
    .await;
    println!("Flushed all files to disk. Complete!");
    JOB_FINISHED.store(true, std::sync::atomic::Ordering::Relaxed);
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
