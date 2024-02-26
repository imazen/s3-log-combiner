use chrono::{DateTime, Datelike, TimeZone, Utc};
use std::ffi::OsString;
use std::fs::ReadDir;
use std::path::{Path, PathBuf};
use std::{fs, process};

// Define a helper function that accepts any DateTime with a fixed offset, converts it to UTC,
// and then returns the first and last milliseconds of the month in UTC.
pub fn first_and_last_millisecond_of_month<Tz: TimeZone>(
    datetime: DateTime<Tz>,
) -> (DateTime<Utc>, DateTime<Utc>) {
    // Convert the input DateTime to UTC
    let datetime_utc = datetime.with_timezone(&Utc);

    // Calculate the first millisecond of the month
    let first_millisecond = Utc
        .with_ymd_and_hms(datetime_utc.year(), datetime_utc.month(), 1, 0, 0, 0)
        .unwrap();

    // Calculate the first day of the next month, then subtract one second to get the last millisecond of the current month
    let next_month = Utc
        .with_ymd_and_hms(datetime_utc.year(), datetime_utc.month(), 1, 0, 0, 0)
        .unwrap()
        .checked_add_signed(chrono::Duration::days(32))
        .unwrap() // Add days to ensure the next month
        .with_day(1)
        .unwrap() // First day of the next month
        .checked_sub_signed(chrono::Duration::seconds(1))
        .unwrap(); // Subtract one second to get the last millisecond

    (first_millisecond, next_month)
}

// Define a helper function that accepts any DateTime with a fixed offset, converts it to UTC,
// and then returns the first and last milliseconds of the day in UTC.
pub fn first_and_last_millisecond_of_day<Tz: TimeZone>(
    datetime: DateTime<Tz>,
) -> (DateTime<Utc>, DateTime<Utc>) {
    // Convert the input DateTime to UTC
    let datetime_utc = datetime.with_timezone(&Utc);

    // Calculate the first millisecond of the day
    let first_millisecond = datetime_utc
        .date_naive()
        .and_hms_opt(0, 0, 0)
        .unwrap()
        .and_utc();

    // Calculate the last millisecond of the day by setting hour to 23, minute and second to 59, and adding 999 milliseconds
    let last_millisecond = datetime_utc
        .date_naive()
        .and_hms_opt(23, 59, 59)
        .unwrap()
        .and_utc()
        .checked_add_signed(chrono::Duration::milliseconds(999))
        .unwrap();

    (first_millisecond, last_millisecond)
}

pub fn expand_input_filenames_recursive(files_and_dirs: Vec<PathBuf>) -> Vec<PathBuf> {
    // If there are no inputs, exit with an error
    if files_and_dirs.is_empty() {
        eprintln!("Error: No input files or directories provided.");
        process::exit(1);
    }

    let mut expanded_files = Vec::new();

    // Iterate through each file or directory
    for file_or_dir in files_and_dirs {
        // Check if it's a file
        if file_or_dir.is_file() {
            // Add the file to the list if it doesn't have .incomplete or .err extension
            let extension = file_or_dir
                .extension()
                .unwrap_or_default()
                .to_string_lossy()
                .to_lowercase();
            if extension != "incomplete" && extension != "err" {
                expanded_files.push(file_or_dir);
            }
        } else if file_or_dir.is_dir() {
            // If it's a directory, expand it to include all files (excluding .incomplete and .err)
            if let Ok(entries) = fs::read_dir(&file_or_dir) {
                for entry in entries {
                    if let Ok(entry) = entry {
                        let path = entry.path();
                        if path.is_file() {
                            let extension = path
                                .extension()
                                .unwrap_or_default()
                                .to_string_lossy()
                                .to_lowercase();
                            if extension != "incomplete" && extension != "err" {
                                expanded_files.push(path);
                            }
                        }
                    }
                }
            } else {
                eprintln!("Error: Unable to read directory {:?}", file_or_dir);
                process::exit(1);
            }
        } else {
            // If it's neither a file nor a directory, print an error and exit
            eprintln!("Error: {:?} is not a valid file or directory.", file_or_dir);
            process::exit(1);
        }
    }

    // Sort the resulting files alphabetically
    expanded_files.sort();

    expanded_files
}

pub fn last_complete_file_in_directory<P: AsRef<Path>>(directory_path: P) -> Option<String> {
    let path = directory_path.as_ref();
    // Check if the directory exists
    if path.is_dir() {
        // Read the directory
        let read_dir: ReadDir = match fs::read_dir(path) {
            Ok(dir) => dir,
            Err(_) => return None,
        };

        let incomplete = OsString::from("incomplete");
        let error = OsString::from("err");

        let mut files: Vec<PathBuf> = read_dir
            .filter_map(|entry| entry.ok()) // Filter out Err results
            .map(|entry| entry.path()) // Get the path of each entry
            .filter(|path| path.is_file()) // Keep only files
            .collect();

        // Sort the files alphabetically
        files.sort_unstable();

        let mut fail = false;
        for path in files.iter() {
            if let Some(ref ext) = path.extension() {
                if ext.eq_ignore_ascii_case(&incomplete) {
                    eprintln!("Found errored batch {:?}, please clean up the errored and all subsequent files first.", &path);
                    fail = true;
                }
                if ext.eq_ignore_ascii_case(&error) {
                    eprintln!("Found incomplete batch {:?}, please clean up the incomplete and all subsequent files first.", &path);
                    fail = true;
                }
            }
        }
        if fail {
            std::process::exit(1);
        }

        // Get the last file's name
        if let Some(last_file_path) = files.last() {
            return last_file_path
                .file_name()
                .and_then(|name| name.to_str())
                .map(|s| s.to_string());
        }
    }
    None
}
