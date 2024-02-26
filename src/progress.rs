use crate::cli::FetchArgs;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::time::sleep;

pub static LIST_REQUESTS: AtomicUsize = AtomicUsize::new(0);
pub static LIST_REQUEST_EST_BYTES: AtomicUsize = AtomicUsize::new(0);
pub static BATCHES_STARTED: AtomicUsize = AtomicUsize::new(0);
pub static FETCHES_ENQUEUED: AtomicUsize = AtomicUsize::new(0);
pub static FETCHES_DEQUEUED: AtomicUsize = AtomicUsize::new(0);

pub static UPLOADS_ENQUEUED: AtomicUsize = AtomicUsize::new(0);
pub static UPLOADS_DEQUEUED: AtomicUsize = AtomicUsize::new(0);

pub static BLOB_BYTES_READ: AtomicUsize = AtomicUsize::new(0);
pub static BLOB_RESULTS_ENQUEUED: AtomicUsize = AtomicUsize::new(0);
pub static BLOB_RESULTS_DEQUEUED: AtomicUsize = AtomicUsize::new(0);

pub static BLOB_RESULTS_ORDERED: AtomicUsize = AtomicUsize::new(0);

pub static FILES_WRITTEN: AtomicUsize = AtomicUsize::new(0);
pub static UNRECOVERABLE_ERRORS: AtomicUsize = AtomicUsize::new(0);
pub static BLOB_BYTES_WRITTEN: AtomicUsize = AtomicUsize::new(0);
pub static BLOB_BYTES_UPLOADED: AtomicUsize = AtomicUsize::new(0);

static LAST_UPDATE_BYTES_READ: AtomicUsize = AtomicUsize::new(0);
static LAST_UPDATE_EST_LIST_BYTES_READ: AtomicUsize = AtomicUsize::new(0);
static LAST_UPDATE_BYTES_WRITTEN: AtomicUsize = AtomicUsize::new(0);
static LAST_UPDATE_BYTES_UPLOADED: AtomicUsize = AtomicUsize::new(0);
static LAST_UPDATE_PRINTED: AtomicU64 = AtomicU64::new(0);

pub(crate) static JOB_FINISHED: AtomicBool = AtomicBool::new(false);

static UPDATE_FREQUENCY_SECONDS: i32 = 8;

pub(crate) async fn print_status_update_tokio(config: FetchArgs) {
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

                let bytes_uploaded_since_last_update = BLOB_BYTES_UPLOADED
                    .load(std::sync::atomic::Ordering::Relaxed)
                    - LAST_UPDATE_BYTES_UPLOADED.load(std::sync::atomic::Ordering::Relaxed);

                let mbps_read = (bytes_read_since_last_update as f64 / 1_024_000.0)
                    / (UPDATE_FREQUENCY_SECONDS as f64)
                    * 8f64;
                let mbps_written = (bytes_written_since_last_update as f64 / 1_024_000.0)
                    / (UPDATE_FREQUENCY_SECONDS as f64)
                    * 8f64;

                let mbps_up = (bytes_uploaded_since_last_update as f64 / 1_024_000.0)
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
                        - BLOB_RESULTS_ORDERED.load(std::sync::atomic::Ordering::Relaxed) as i64;

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

                println!("{:.2}Mbps down, {:.2}Mbps disk, {} fetches queued, {} results reordering, {} results queued, {} fatal errors, {:.2} Gb written, {:.2} Gb fetched, {} HTTP requests, {} avg blobs/listing",
                         mbps_read, mbps_written, fetches_net, blob_reordering, blob_results_net, total_unrecoverable_errors, total_gb_written, total_gb_read, total_requests, avg_blobs_per_list);
            }
            LAST_UPDATE_BYTES_READ.store(
                BLOB_BYTES_READ.load(std::sync::atomic::Ordering::Relaxed),
                std::sync::atomic::Ordering::Relaxed,
            );
            LAST_UPDATE_BYTES_WRITTEN.store(
                BLOB_BYTES_WRITTEN.load(std::sync::atomic::Ordering::Relaxed),
                std::sync::atomic::Ordering::Relaxed,
            );
            LAST_UPDATE_BYTES_UPLOADED.store(
                BLOB_BYTES_UPLOADED.load(std::sync::atomic::Ordering::Relaxed),
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
