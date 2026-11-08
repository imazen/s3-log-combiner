use crate::cli::RecompressArgs;
use crate::log_syntax::SplitLogColumns;
use std::fs::{self, File};
use std::io::{self, BufRead, BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;

const ZSTD_MAGIC: [u8; 4] = [0x28, 0xB5, 0x2F, 0xFD];

#[derive(Default)]
struct Stats {
    skipped_zst: usize,
    skipped_binary: usize,
    skipped_other_ext: usize,
    skipped_output_exists: usize,
    skipped_empty: usize,
    processed: usize,
    deleted: usize,
    failed: Vec<(PathBuf, String)>,
    input_bytes: u64,
    output_bytes: u64,
}

pub fn run(args: RecompressArgs) -> io::Result<()> {
    let keep_cols: Vec<usize> = args.keep_columns.iter().map(|c| *c as usize).collect();

    let mut queue: Vec<PathBuf> = Vec::new();
    for input in &args.inputs {
        gather(input, args.recursive, &mut queue)?;
    }
    queue.sort();

    let jobs = args.jobs.unwrap_or_else(|| {
        std::thread::available_parallelism()
            .map(|n| (n.get().saturating_sub(1)).max(1))
            .unwrap_or(1)
    });

    println!(
        "Found {} candidate files. level={} long={} keep_columns={:?} delete_original={} dry_run={} jobs={}",
        queue.len(),
        args.level,
        args.long,
        args.keep_columns,
        args.delete_original,
        args.dry_run,
        jobs
    );

    let total = queue.len();
    let cursor = AtomicUsize::new(0);
    let stats = Mutex::new(Stats::default());
    let queue_ref = &queue;
    let keep_cols_ref = &keep_cols;
    let args_ref = &args;
    let cursor_ref = &cursor;
    let stats_ref = &stats;

    std::thread::scope(|s| {
        for _ in 0..jobs {
            s.spawn(move || loop {
                let idx = cursor_ref.fetch_add(1, Ordering::Relaxed);
                if idx >= queue_ref.len() {
                    break;
                }
                let path = &queue_ref[idx];
                let mut local = Stats::default();
                let result = process_file(path, keep_cols_ref, args_ref, &mut local, idx + 1, total);
                if let Err(e) = result {
                    eprintln!("FAIL  {}: {}", path.display(), e);
                    local.failed.push((path.clone(), e.to_string()));
                }
                merge_stats(&mut stats_ref.lock().unwrap(), local);
            });
        }
    });

    let stats = stats.into_inner().unwrap();

    println!();
    println!("=== Recompress summary ===");
    println!("Scanned:               {}", total);
    println!("Processed:             {}", stats.processed);
    println!("Deleted originals:     {}", stats.deleted);
    println!("Skipped .zst:          {}", stats.skipped_zst);
    println!("Skipped (binary):      {}", stats.skipped_binary);
    println!("Skipped (extension):   {}", stats.skipped_other_ext);
    println!("Skipped (.zst exists): {}", stats.skipped_output_exists);
    println!("Skipped (empty):       {}", stats.skipped_empty);
    println!("Failed:                {}", stats.failed.len());
    if stats.input_bytes > 0 {
        let ratio = stats.output_bytes as f64 / stats.input_bytes as f64;
        println!(
            "Bytes:   {} -> {}  ({:.2}% of original)",
            stats.input_bytes,
            stats.output_bytes,
            ratio * 100.0
        );
    }
    if !stats.failed.is_empty() {
        println!();
        for (p, e) in &stats.failed {
            println!("  FAIL {}: {}", p.display(), e);
        }
        return Err(io::Error::new(
            io::ErrorKind::Other,
            format!("{} file(s) failed", stats.failed.len()),
        ));
    }
    Ok(())
}

fn merge_stats(into: &mut Stats, from: Stats) {
    into.skipped_zst += from.skipped_zst;
    into.skipped_binary += from.skipped_binary;
    into.skipped_other_ext += from.skipped_other_ext;
    into.skipped_output_exists += from.skipped_output_exists;
    into.skipped_empty += from.skipped_empty;
    into.processed += from.processed;
    into.deleted += from.deleted;
    into.input_bytes += from.input_bytes;
    into.output_bytes += from.output_bytes;
    into.failed.extend(from.failed);
}

fn gather(root: &Path, recursive: bool, out: &mut Vec<PathBuf>) -> io::Result<()> {
    let meta = fs::metadata(root)?;
    if meta.is_file() {
        out.push(root.to_path_buf());
        return Ok(());
    }
    if meta.is_dir() {
        for entry in fs::read_dir(root)? {
            let entry = entry?;
            let path = entry.path();
            let ft = entry.file_type()?;
            if ft.is_dir() {
                if recursive {
                    gather(&path, recursive, out)?;
                }
            } else if ft.is_file() {
                out.push(path);
            }
        }
    }
    Ok(())
}

fn process_file(
    path: &Path,
    keep_cols: &[usize],
    args: &RecompressArgs,
    stats: &mut Stats,
    seq: usize,
    total: usize,
) -> io::Result<()> {
    let name = path
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or_default();

    let lower = name.to_ascii_lowercase();
    if lower.ends_with(".zst") {
        stats.skipped_zst += 1;
        return Ok(());
    }
    if lower.ends_with(".err")
        || lower.ends_with(".summary.txt")
        || lower.ends_with(".partial")
        || lower.ends_with(".md")
        || lower.ends_with(".toml")
        || lower.ends_with(".lock")
        || lower.ends_with(".json")
    {
        stats.skipped_other_ext += 1;
        return Ok(());
    }

    let meta = fs::metadata(path)?;
    if meta.len() == 0 {
        stats.skipped_empty += 1;
        if args.verbose {
            println!("skip empty  {}", path.display());
        }
        return Ok(());
    }

    // Sniff first 4 bytes for binary/compressed signatures
    let mut head = [0u8; 4];
    {
        let mut f = File::open(path)?;
        let n = f.read(&mut head)?;
        if n >= 4 && head == ZSTD_MAGIC {
            stats.skipped_zst += 1;
            if args.verbose {
                println!("skip zstd-by-magic  {}", path.display());
            }
            return Ok(());
        }
        // gzip 1F 8B, bzip2 'BZh', xz FD 37 7A 58, zip PK
        if (n >= 2 && head[0] == 0x1F && head[1] == 0x8B)
            || (n >= 3 && &head[..3] == b"BZh")
            || (n >= 4 && &head[..4] == [0xFD, 0x37, 0x7A, 0x58])
            || (n >= 2 && head[0] == b'P' && head[1] == b'K')
        {
            stats.skipped_binary += 1;
            eprintln!("skip binary  {}", path.display());
            return Ok(());
        }
    }

    // Determine output path: strip trailing ".incomplete" if present, then add .zst
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    let mut out_name = name.to_string();
    if out_name.to_ascii_lowercase().ends_with(".incomplete") {
        out_name.truncate(out_name.len() - ".incomplete".len());
    }
    let final_out = parent.join(format!("{}.zst", out_name));
    let partial_out = parent.join(format!("{}.zst.partial", out_name));

    if final_out.exists() {
        stats.skipped_output_exists += 1;
        eprintln!(
            "skip exists  {} -> {}",
            path.display(),
            final_out.display()
        );
        return Ok(());
    }

    if args.dry_run {
        println!(
            "DRY  {}  ->  {}  ({} bytes in)",
            path.display(),
            final_out.display(),
            meta.len()
        );
        return Ok(());
    }

    // Clean up any leftover .partial
    if partial_out.exists() {
        let _ = fs::remove_file(&partial_out);
    }

    let input_lines = compress_with_filter(path, &partial_out, keep_cols, args)?;

    // Verify by decompressing and comparing line count
    let decomp_lines = count_lines_zstd(&partial_out)?;
    if decomp_lines != input_lines {
        let _ = fs::remove_file(&partial_out);
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "line count mismatch after round-trip: input={} output={}",
                input_lines, decomp_lines
            ),
        ));
    }

    let out_size = fs::metadata(&partial_out)?.len();
    fs::rename(&partial_out, &final_out)?;

    stats.processed += 1;
    stats.input_bytes += meta.len();
    stats.output_bytes += out_size;

    let ratio = if meta.len() > 0 {
        out_size as f64 / meta.len() as f64 * 100.0
    } else {
        0.0
    };
    println!(
        "[{:>4}/{}] OK   {}  ->  {}  ({} -> {} bytes, {:.1}%, {} lines)",
        seq,
        total,
        path.display(),
        final_out.file_name().unwrap().to_string_lossy(),
        meta.len(),
        out_size,
        ratio,
        input_lines
    );

    if args.delete_original {
        fs::remove_file(path)?;
        stats.deleted += 1;
    }

    Ok(())
}

fn compress_with_filter(
    input: &Path,
    output: &Path,
    keep_cols: &[usize],
    args: &RecompressArgs,
) -> io::Result<u64> {
    let in_file = File::open(input)?;
    let reader = BufReader::new(in_file);

    let out_file = File::create(output)?;
    let mut encoder = zstd::stream::Encoder::new(BufWriter::new(out_file), args.level)?;
    if args.long {
        // Window log of 27 = 128 MiB, plenty for log files
        encoder.long_distance_matching(true)?;
        encoder.window_log(27)?;
    }
    let mut writer = encoder.auto_finish();

    let mut line_count: u64 = 0;
    let mut buf = String::new();
    let mut filtered = String::new();
    let mut reader = reader;

    loop {
        buf.clear();
        let read = match read_line_lossy(&mut reader, &mut buf) {
            Ok(n) => n,
            Err(e) => return Err(e),
        };
        if read == 0 {
            break;
        }
        // Strip trailing newline (CR/LF) for processing; we'll re-add \n
        while buf.ends_with('\n') || buf.ends_with('\r') {
            buf.pop();
        }
        if keep_cols.is_empty() {
            writer.write_all(buf.as_bytes())?;
            writer.write_all(b"\n")?;
        } else {
            filtered.clear();
            for (i, column) in SplitLogColumns::new(&buf).enumerate() {
                if keep_cols.contains(&i) {
                    filtered.push_str(column);
                } else {
                    filtered.push('-');
                }
                filtered.push(' ');
            }
            // Trim trailing space, add newline
            if filtered.ends_with(' ') {
                filtered.pop();
            }
            filtered.push('\n');
            writer.write_all(filtered.as_bytes())?;
        }
        line_count += 1;
    }

    writer.flush()?;
    drop(writer); // triggers auto_finish, which finalizes the zstd frame

    Ok(line_count)
}

fn read_line_lossy<R: BufRead>(reader: &mut R, buf: &mut String) -> io::Result<usize> {
    let mut bytes = Vec::new();
    let n = reader.read_until(b'\n', &mut bytes)?;
    if n == 0 {
        return Ok(0);
    }
    let s = String::from_utf8_lossy(&bytes);
    buf.push_str(&s);
    Ok(n)
}

fn count_lines_zstd(path: &Path) -> io::Result<u64> {
    let f = File::open(path)?;
    let mut decoder = zstd::stream::Decoder::new(BufReader::new(f))?;
    let mut buf = [0u8; 64 * 1024];
    let mut count: u64 = 0;
    let mut last_was_newline = true; // empty file => 0 lines
    loop {
        let n = decoder.read(&mut buf)?;
        if n == 0 {
            break;
        }
        for &b in &buf[..n] {
            if b == b'\n' {
                count += 1;
                last_was_newline = true;
            } else {
                last_was_newline = false;
            }
        }
    }
    // If file ends without trailing newline, count the final partial line
    if !last_was_newline {
        count += 1;
    }
    Ok(count)
}
