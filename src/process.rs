use crate::cli::FetchArgs;
use crate::fetch::{create_dirs_if_missing, BlobResult};
use std::path::PathBuf;
use tokio::fs::File;
use tokio::io::{AsyncWriteExt, BufWriter};

pub(crate) async fn process_batch(
    items: Vec<BlobResult>,
    last_item_ingested: String,
    config: &FetchArgs,
) {
    // Should write files, upload files, upload checkpoint markers,
    // generate and write or upload summaries
    //
    write_batch(items, last_item_ingested, config).await;
}

async fn write_batch(items: Vec<BlobResult>, last_item_ingested: String, config: &FetchArgs) {
    let mut successful_blob_count = 0;
    let mut error_writer: Option<BufWriter<File>> = None;

    let mut new_file = PathBuf::from(
        &config
            .output_directory
            .as_ref()
            .expect("Only output directory is supported so far"),
    );
    new_file.push(&last_item_ingested);
    let mut writer_path = new_file.clone();
    writer_path.set_extension("incomplete");
    let mut err_path = new_file.clone();
    err_path.set_extension("err");
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
    let mut writer = BufWriter::new(file);

    for result in items {
        // Write contents to the current file
        if let Some(contents) = result.contents {
            successful_blob_count += 1;
            let contents_len = contents.len();
            write_filtered(&mut writer, contents, &config)
                .await
                .expect("Failed to write data");

            crate::progress::BLOB_BYTES_WRITTEN
                .fetch_add(contents_len, std::sync::atomic::Ordering::Relaxed);
        }
        if let Some(err) = result.error {
            if error_writer.is_none() {
                let file = File::create(err_path.as_path())
                    .await
                    .expect("Failed to create error file");
                error_writer = Some(BufWriter::new(file));
            }
            if let Some(w) = error_writer.as_mut() {
                w.write(
                    format!(
                        "Failed to fetch {:?} - error: {:?}\n",
                        result.entry.name, err
                    )
                        .as_bytes(),
                )
                    .await
                    .expect("TODO: panic message");
            }
        }
    }

    writer
        .flush()
        .await
        .expect("Failed to flush and close file");

    crate::progress::FILES_WRITTEN.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

    if error_writer.is_none() {
        // rename.
        let mut final_name = writer_path.clone();
        final_name.set_extension("");
        tokio::fs::rename(writer_path, &final_name).await.unwrap();
        println!(
            "Combined {:?} blobs into {:?}",
            successful_blob_count,
            &final_name.file_name().unwrap()
        );
    }

    if let Some(mut writer) = error_writer {
        writer
            .flush()
            .await
            .expect("Failed to flush and close error file");
        eprintln!("See unrecoverable errors in {:?}", err_path)
    }
}

async fn write_filtered(
    writer: &mut BufWriter<File>,
    contents: Vec<u8>,
    config: &FetchArgs,
) -> std::io::Result<()> {
    //if config.clear_columns.is_empty(){
    writer.write_all(&contents).await?;
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
