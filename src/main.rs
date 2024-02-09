mod cli;

use futures_util::{StreamExt, TryStreamExt};
use rusoto_core::{HttpClient, Region, RusotoError};
use rusoto_s3::{GetObjectError, GetObjectRequest, ListObjectsV2Request, S3Client, S3};
use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::fs::File;
use tokio::io::{AsyncWriteExt, BufWriter};
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::{mpsc, Mutex};
use tokio::time::sleep;
use tokio_stream::wrappers::ReceiverStream;

#[derive(Debug, Clone)]
pub struct Config {
    pub bucket: String,
    pub region: Option<String>,
    pub output_directory: PathBuf,
    pub start_at: Option<String>,
    pub max_connections: usize,
    pub target_size_kb: usize,
    pub access_key: String,
    pub secret_key: String,
}

#[derive(Debug, Clone)]
pub struct BlobEntry {
    pub name: String,
    pub index: usize,
    pub size: Option<i64>,
    pub cumulative_size: i64,
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
    open_new_file: Option<std::path::PathBuf>,
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
    rx: Sender<OrderedBlobResult>,
    tx: &Config,
) {
    let mut outputs = data.outputs.lock().await;
    let mut results = data.results.lock().await;

    if finished {
        if let Some(last_result) = results.iter().next_back() {
            let last_index = *last_result.0;
            for output in outputs.iter_mut() {
                if output.pending && output.last_item_index.is_none() {
                    output.last_item_index = Some(last_index);
                }
            }
        }
    }

    for output in outputs.iter_mut() {
        if output.pending {
            if let Some(last_item_index) = output.last_item_index {
                if output.final_path.is_none() && results.contains_key(&last_item_index) {
                    // Construct the final path for the batch
                    let mut final_path = std::path::PathBuf::from(&tx.output_directory);
                    final_path.push(
                        results
                            .get(&last_item_index)
                            .unwrap()
                            .entry
                            .name
                            .to_string(),
                    );
                    output.final_path = Some(final_path);
                }

                // Check if all items in the batch are ready
                let mut all_items_ready = true;
                for i in output.first_item_index.unwrap()..=last_item_index {
                    if !results.contains_key(&i) {
                        all_items_ready = false;
                        break;
                    }
                }

                if all_items_ready {
                    // All items are ready, send OrderedBlobResult for each
                    for i in output.first_item_index.unwrap()..=last_item_index {
                        if let Some(blob_result) = results.remove(&i) {
                            let open_new_file = if i == output.first_item_index.unwrap() {
                                output.final_path.clone()
                            } else {
                                None
                            };
                            let ordered_result = OrderedBlobResult {
                                blob: blob_result,
                                open_new_file,
                            };
                            if rx.send(ordered_result).await.is_err() {
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
    let target_len = index + 1; // Convert index to usize for compatibility with Vec's length

    if target_len > v.len() {
        // If the target length is greater than the current length,
        // extend the vector with the default value until it reaches the target length.
        v.resize(target_len, default_value);
    }
}
async fn reorder_logs(
    rx: Receiver<BlobResult>,
    tx: Sender<OrderedBlobResult>,
    out: &DataCollection,
    config: &Config,
) {
    let rx_stream = ReceiverStream::new(rx);
    rx_stream
        .for_each(|blob| {
            let blob_index = blob.entry.index;
            let batch_index = blob.entry.batch_index;
            let first_index_of_batch = blob.entry.first_index_of_batch;
            let tx_clone = tx.clone();
            async move {
                if blob_index == first_index_of_batch {
                    let mut outputs_lock = out.outputs.lock().await;
                    populate(batch_index, outputs_lock.as_mut(), Default::default());
                    outputs_lock[batch_index].first_item_index = Some(first_index_of_batch);

                    if batch_index > 0 {
                        let prev_batch_index = batch_index - 1;
                        outputs_lock[prev_batch_index].last_item_index =
                            Some(first_index_of_batch - 1);
                    }
                }
                let mut results_lock = out.results.lock().await;
                if let Some(replaced) = results_lock.insert(blob_index, blob) {
                    eprintln!("Ooops, replaced {:?}", replaced);
                }
                if blob_index % 100 == 0 {
                    drain_ready_batches(false, out, tx_clone.clone(), config).await;
                }
            }
        })
        .await;

    drain_ready_batches(true, out, tx, config).await;
}

#[tokio::main]
async fn main() {
    let config = cli::parse_args();

    // Use the `config` for the rest of your application logic
    println!("Configuration: {:?}", config); // Placeholder for demonstration

    // Example of using the config
    let region = config
        .region
        .as_ref()
        .unwrap()
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
    let max_connections = config.max_connections;

    let (entry_tx, entry_rx) = mpsc::channel(max_connections * 4);

    // Start listing tasks
    let tx_clone = entry_tx.clone();
    let s3_clone = s3_client.clone();
    let config_clone = config.clone();
    tokio::spawn(async move {
        list_s3_logs(s3_clone, &config_clone, tx_clone).await;
    });

    let (blob_tx, blob_rx) = mpsc::channel(max_connections * 4);

    // Start fetching tasks
    let s3_clone = s3_client.clone();
    let config_clone = config.clone();
    tokio::spawn(async move {
        fetch_logs(s3_clone, entry_rx, blob_tx, &config_clone).await;
    });

    let (ordered_tx, ordered_rx) = mpsc::channel(max_connections * 4);

    let collection = DataCollection {
        outputs: Arc::new(Default::default()),
        results: Arc::new(Default::default()),
    };
    // start reordering
    tokio::spawn(async move {
        reorder_logs(blob_rx, ordered_tx, &collection, &config).await;
    });

    // Start writing
    write_logs(ordered_rx).await;
}

async fn fetch_logs(
    s3: Arc<S3Client>,
    rx: Receiver<BlobEntry>,
    tx: Sender<BlobResult>,
    config: &Config,
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
                let mut backoff = Duration::from_secs(1); // Start with 1 second

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
                            let _ = tx_clone.send(result).await; // Handle send error in real application
                            break; // Success, exit loop
                        }
                        Err(e) => {
                            attempts += 1;
                            if attempts >= max_attempts || !is_recoverable(&e) {
                                let result = BlobResult {
                                    entry: entry.clone(),
                                    contents: None,
                                    error: Some(Box::new(format!("{:?}", e))),
                                };
                                let _ = tx_clone.send(result).await; // Handle send error in real application
                                break; // Exit loop after max attempts or non-recoverable error
                            } else {
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

// fn is_recoverable(error: &RusotoError<GetObjectError>) -> bool {
//     matches!(error, RusotoError::HttpDispatch(ref dispatch_error) if matches!(dispatch_error.response().status().as_u16(), 429 | 500 | 502 | 503 | 504))
//         || matches!(
//             error,
//             RusotoError::Service(GetObjectError::ServiceUnavailable(_))
//         )
//         || matches!(error, RusotoError::Service(GetObjectError::SlowDown(_)))
// }
fn is_recoverable(error: &RusotoError<GetObjectError>) -> bool {
    matches!(
        error,
        RusotoError::Unknown(ref r) if matches!(
            r.status.as_u16(),
            429 | 500 | 502 | 503 | 504
        )
    )
    // Additional error handling logic can be added here if there are other
    // specific error variants or conditions you want to handle.
}

async fn list_s3_logs(s3: Arc<S3Client>, config: &Config, tx: Sender<BlobEntry>) {
    let mut continuation_token = None;
    let mut max_errors = 5;
    let mut cumulative_size = 0;
    let mut previous_batch_index = 0;
    let mut first_index_of_batch = 0;
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
                    for object in contents {
                        if let Some(key) = object.key {
                            let size = object.size.unwrap_or(0);

                            cumulative_size += size.max(0);
                            let batch_index =
                                cumulative_size as usize / (config.target_size_kb * 1000);
                            let first_of_batch = batch_index == previous_batch_index;
                            if first_of_batch {
                                first_index_of_batch = ix;
                            }
                            previous_batch_index = batch_index;
                            if let Err(e) = tx
                                .send(BlobEntry {
                                    index: ix,
                                    name: key,
                                    first_index_of_batch,
                                    size: object.size,
                                    cumulative_size,
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
    let mut current_error_path: Option<PathBuf> = None;
    let mut current_error_writer: Option<BufWriter<File>> = None;

    let mut stream = ReceiverStream::new(rx);

    while let Some(result) = stream.next().await {
        if let Some(new_file) = &result.open_new_file {
            // Flush and close the current file if it exists
            if let Some(mut writer) = current_writer.take() {
                writer
                    .flush()
                    .await
                    .expect("Failed to flush and close file");
            }
            if let Some(mut writer) = current_error_writer.take() {
                writer
                    .flush()
                    .await
                    .expect("Failed to flush and close error file");
            }
            let mut p = new_file.clone();
            p.set_extension(".err");
            current_error_path = Some(p);
            let file = File::create(new_file).await.expect("Failed to create file");
            current_writer = Some(BufWriter::new(file));
            current_error_writer = None;
        }

        // Write contents to the current file
        if let Some(mut contents) = result.blob.contents {
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
                w.write(format!("Error: {:?}", err).as_bytes())
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
