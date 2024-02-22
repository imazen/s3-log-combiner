use std::path::PathBuf;
use tokio::fs::File;
use tokio::io::{AsyncWriteExt, BufWriter};
use tokio::sync::mpsc::Receiver;
use tokio_stream::wrappers::ReceiverStream;
use crate::cli::FetchArgs;
use crate::fetch::{BlobResult, create_dirs_if_missing};

pub(crate) async fn process_batch(items: Vec<BlobResult>, last_item_ingested: String, config: &FetchArgs){

    // Should write files, upload files, upload checkpoint markers,
    // generate and write or upload summaries
    //
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

        crate::fetch::FILES_WRITTEN.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
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

async fn write_logs(rx: Receiver<crate::fetch::OrderedBlobResult>, config: FetchArgs) {
    let mut current_writer: Option<BufWriter<File>> = None;
    let mut current_writer_path: Option<PathBuf> = None;
    let mut current_error_path: Option<PathBuf> = None;
    let mut current_blob_count = 0;
    let mut current_error_writer: Option<BufWriter<File>> = None;

    let mut stream = ReceiverStream::new(rx);

    while let Some(result) = stream.next().await {
        crate::fetch::ORDERED_WRITES_DEQUEUED.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        if let Some(new_file) = &result.starts_new_batch {
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
        if let Some(contents) = result.blob.contents {
            current_blob_count += 1;
            if let Some(writer) = current_writer.as_mut() {
                let contents_len = contents.len();
                write_filtered(writer, contents, &config).await.expect("Failed to write data");

                crate::fetch::BLOB_BYTES_WRITTEN.fetch_add(contents_len, std::sync::atomic::Ordering::Relaxed);
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
    crate::fetch::JOB_FINISHED.store(true, std::sync::atomic::Ordering::Relaxed);
}

async fn write_filtered(writer: &mut BufWriter<File>, contents: Vec<u8>, config: &FetchArgs)
                        -> std::io::Result<()> {
    //if config.clear_columns.is_empty(){
    writer
        .write_all(&contents)
        .await?;
    if !contents.ends_with(b"\n") {
        writer.write_u8(b'\n').await?;
    }
    return Ok(());
    //}
    // TODO: implement
    //let mut buffer = Vec::with_capacity(4096);
    // loop through contents, one line (\n) at a time
    // each cell is space delimited
    // Don't use utf-8, just do bytewise seeking.
    // config.clear_columns contains Vec<u32> of zero-based columns to replace with "-"
    // When the entire line has been buffered and all the cleared columns dropped,
    // call write_all

}