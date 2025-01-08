use crate::cli::FetchArgs;
use crate::fetch::{create_dirs_if_missing, BlobResult};
use crate::log_syntax::SplitLogColumns;
use async_compression::tokio::write::ZstdEncoder;
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
    let current_time = chrono::Utc::now();
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
    let enable_ldm = async_compression::zstd::CParameter::enable_long_distance_matching(true);
    let mut encoder = ZstdEncoder::with_quality_and_params(
        BufWriter::new(file),
        async_compression::Level::Default,
        &[enable_ldm],
    );

    let mut writer = BufWriter::new(encoder);

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

    writer.shutdown().await.unwrap();

    if error_writer.is_none() {
        // rename.
        let mut final_name = writer_path.clone();
        final_name.set_extension("zst");
        tokio::fs::rename(writer_path, &final_name).await.unwrap();
        println!(
            "Combined {:?} blobs into {:?}",
            successful_blob_count,
            &final_name.file_name().unwrap()
        );
    }

    let elapsed = chrono::Utc::now() - current_time;
    crate::progress::TIME_SPENT_WRITING.fetch_add(
        elapsed.num_milliseconds() as u64,
        std::sync::atomic::Ordering::Relaxed,
    );

    crate::progress::FILES_WRITTEN.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

    if let Some(mut writer) = error_writer {
        writer
            .flush()
            .await
            .expect("Failed to flush and close error file");
        eprintln!("See unrecoverable errors in {:?}", err_path)
    }
}

async fn write_filtered(
    writer: &mut BufWriter<ZstdEncoder<BufWriter<File>>>,
    contents: Vec<u8>,
    config: &FetchArgs,
) -> std::io::Result<()> {
    // parse the contents Vec<u8> into a &str, iterate lines,
    // use SplitLogColumns::new(line) to iterate columns,
    // keep only columns 1-3 and 6-10, write '-' to the rest.
    // buffered writing to the writer to improve compression.
    // ensure all lines end in \n
    if config.keep_columns.len() == 0 {
        writer.write_all(&contents).await?;
        if !contents.ends_with(b"\n") {
            writer.write_u8(b'\n').await?;
        }
        return Ok(());
    }

    let keep_columns: Vec<usize> = config.keep_columns.iter().map(|c| *c as usize).collect();
    let utf8_contents = std::str::from_utf8(&contents).expect("Log file is not valid UTF-8");
    let mut lines = utf8_contents.lines();
    let mut filtered_line = String::new();
    while let Some(line) = lines.next() {
        filtered_line.clear();
        let mut columns = SplitLogColumns::new(line);

        for (i, column) in columns.enumerate() {
            if keep_columns.contains(&i) {
                filtered_line.push_str(&column);
            } else {
                filtered_line.push('-');
            }
            filtered_line.push(' ');
        }
        filtered_line.push('\n');
        writer.write_all(filtered_line.as_bytes()).await?;
    }

    Ok(())
}
