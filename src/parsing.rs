use crate::telemetry::Summary;
use crate::telemetry::Report;
use std::collections::HashMap;
use std::{env, fs};
use std::path::{Path, PathBuf};
use std::process;

use chrono::{Datelike, DateTime, TimeZone, Utc};
use crate::cli::QueryParseArgs;

fn expand_input_filenames(files_and_dirs: Vec<PathBuf>) -> Vec<PathBuf> {
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
            let extension = file_or_dir.extension().unwrap_or_default().to_string_lossy().to_lowercase();
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
                            let extension = path.extension().unwrap_or_default().to_string_lossy().to_lowercase();
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


#[derive(Debug, Clone)]
struct LogLine<'a> {
    //[25/Dec/2023:23:42:03 +0000]
    time: DateTime<Utc>,
    ip_str: &'a str,
    operation: &'a str,
    bucket: &'a str,
    key: &'a str,
    response_status_str: &'a str,
    request_path: &'a str,
    request_query: Option<&'a str>,
    split_value: Option<&'a str>,
    unique_value: Option<&'a str>,
    truncated: bool,
}

struct SplitLogColumns<'a> {
    s: &'a str,
    current_pos: usize,
    bracket_depth: usize,
    in_quotes: bool,
}

impl<'a> SplitLogColumns<'a> {
    fn new(s: &'a str) -> Self {
        SplitLogColumns {
            s,
            current_pos: 0,
            bracket_depth: 0,
            in_quotes: false
        }
    }
}


impl<'a> Iterator for SplitLogColumns<'a> {
    type Item = &'a str;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current_pos >= self.s.len() {
            return None;
        }

        let start = self.current_pos;
        let mut end = start;

        while self.current_pos < self.s.len() {
            let c = self.s[self.current_pos..].chars().next().unwrap();

            match c {
                '"' => self.in_quotes = !self.in_quotes,
                '[' => self.bracket_depth += 1,
                ']' if self.bracket_depth > 0 => self.bracket_depth -= 1,
                ' ' if self.bracket_depth <= 0 && !self.in_quotes => {
                    end = self.current_pos;
                    self.bracket_depth = 0;
                    self.current_pos += 1; // Move past the current whitespace
                    // Handle consecutive whitespaces producing empty elements
                    if start == end {
                        return Some("");
                    }
                    return Some(&self.s[start..end]);
                },
                _ => {}
            }

            if self.bracket_depth == 0 && !self.in_quotes && c != ' ' {
                end = self.current_pos + 1; // Move end to the next character for non-whitespace
            }

            self.current_pos += 1; // Always move to the next character
        }

        // Handle the case where the last character(s) are not whitespace or are within brackets
        if start != self.current_pos || self.s.ends_with(' ') {
            return Some(&self.s[start..self.current_pos]);
        }

        None
    }
}

fn parse_s3_log_line<'a>(line: &'a str, split_by_key: &str, unique_key: &str) -> LogLine<'a> {
    let mut parts = SplitLogColumns::new(line);


    let bucket = parts.nth(1).unwrap_or("");
    let time;
    if let Some(time_str) = parts.nth(0){
        let result= DateTime::parse_from_str(time_str,
                                        "[%d/%b/%Y:%H:%M:%S %z]");
        if let Err(e) = result{
            panic!("Error parsing date: '{time_str}'\n{e}");
        }
        time = result.unwrap().with_timezone(&Utc);

    }else{
        eprintln!("Error parsing line: {line}");
        std::process::exit(3);

    }

    let ip_str = parts.nth(0).unwrap_or(""); // Accounts for the previous next()
    let operation = parts.nth(2).unwrap_or("");
    let key = parts.nth(0).unwrap_or(""); // Skips requester ID
    let request_command = parts.nth(0).unwrap_or(""); // Skips to Request-URI
    let request_uri = request_command.split(' ').nth(1).unwrap_or("");

    let response_status_str = parts.next().unwrap_or("");

    let mut request_path = Some(request_uri);
    let mut request_query = None;
    let mut split_value = None;
    let mut unique_value = None;
    let mut truncated = false;

    // Parse the query string from the request URI
    if let Some(query_start) = request_uri.find('?') {
        let query_string = &request_uri[query_start + 1..]; // Skip '?' character
        let pairs = query_string.split('&');
        for pair in pairs {
            let mut parts = pair.split('=');
            if let Some(key) = parts.next() {
                if let Some(value) = parts.next() {
                    match key {
                        k if k == split_by_key => split_value = Some(value),
                        k if k == unique_key => unique_value = Some(value),
                        k if k == "truncated" && value != "false" => truncated = true,
                        _ => {}
                    }
                }
            }
        }
        request_query = Some(&request_uri[query_start..]);
        request_path = Some(&request_uri[..query_start]);
    }

    LogLine {
        time,
        ip_str,
        operation,
        bucket,
        key,
        response_status_str,
        request_path: request_path.unwrap_or(""),
        request_query,
        split_value,
        unique_value,
        truncated
    }
}

#[derive(Debug,Clone)]
struct SplitUnique{
    unique_id: String,
    last_full_report_from: DateTime<Utc>,
    last_full_report: Option<Report>,
    last_report_from: DateTime<Utc>,
    last_report: Option<Report>,
}

impl SplitUnique {
    fn default(id: String) -> SplitUnique {
        SplitUnique{
            unique_id: id,
            last_full_report_from: Default::default(),
            last_full_report: None,
            last_report_from: Default::default(),
            last_report: None,
        }
    }
}

impl SplitUnique{
    fn parse_line(line: &LogLine) -> Option<Report> {
        line.request_query.map(|q| Report::parse(q, line.time, line.ip_str))
    }
    fn update_with(&mut self, line: &LogLine){
        if line.request_query.is_none() { return; }
        if line.time > self.last_report_from{
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
#[derive(Debug,Clone)]
struct SplitDataSink{
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
    
    fn summarize(&self) -> Summary{
        let mut s = Summary::default();
        self.uniques.iter().for_each(|(_,v)| {
            if let Some(ref r1) = v.last_full_report{
                s.add_from(r1, true);

            }
            if let Some(ref r2) = v.last_report{
                s.add_from(r2,v.last_full_report.is_none());
            }
        });
        s
    }

}

// Define a helper function that accepts any DateTime with a fixed offset, converts it to UTC,
// and then returns the first and last milliseconds of the month in UTC.
fn first_and_last_millisecond_of_month<Tz: TimeZone>(datetime: DateTime<Tz>) -> (DateTime<Utc>, DateTime<Utc>) {
    // Convert the input DateTime to UTC
    let datetime_utc = datetime.with_timezone(&Utc);

    // Calculate the first millisecond of the month
    let first_millisecond = Utc.with_ymd_and_hms(datetime_utc.year(), datetime_utc.month(), 1,
                                                 0, 0, 0).unwrap();

    // Calculate the first day of the next month, then subtract one second to get the last millisecond of the current month
    let next_month = Utc.with_ymd_and_hms(datetime_utc.year(), datetime_utc.month(), 1,
                                          0, 0, 0)
        .unwrap()
        .checked_add_signed(chrono::Duration::days(32)).unwrap() // Add days to ensure the next month
        .with_day(1).unwrap() // First day of the next month
        .checked_sub_signed(chrono::Duration::seconds(1)).unwrap(); // Subtract one second to get the last millisecond

    (first_millisecond, next_month)
}

// Define a helper function that accepts any DateTime with a fixed offset, converts it to UTC,
// and then returns the first and last milliseconds of the day in UTC.
fn first_and_last_millisecond_of_day<Tz: TimeZone>(datetime: DateTime<Tz>) -> (DateTime<Utc>, DateTime<Utc>) {
    // Convert the input DateTime to UTC
    let datetime_utc = datetime.with_timezone(&Utc);

    // Calculate the first millisecond of the day
    let first_millisecond = datetime_utc
        .date_naive()
        .and_hms_opt(0, 0, 0)
        .unwrap().and_utc();

    // Calculate the last millisecond of the day by setting hour to 23, minute and second to 59, and adding 999 milliseconds
    let last_millisecond =
        datetime_utc.date_naive()
            .and_hms_opt(23, 59, 59)
            .unwrap()
            .and_utc()
        .checked_add_signed(chrono::Duration::milliseconds(999)).unwrap();

    (first_millisecond, last_millisecond)
}
impl SplitDataSink{
    fn create_for(id: &str, output_root: &PathBuf, month: DateTime<Utc>,
        day: Option<DateTime<Utc>>) -> SplitDataSink{

        let dir_sep =  "\\"; //TODO, get plat slash
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
        let sub_path = Path::join(output_root.as_path(),&split_segment);
        SplitDataSink{
            split_segment: split_segment.clone(),
            summary_path: Path::join(sub_path.as_path(), "summary.txt"),
            split_id: id.to_owned(),
            split_date_from: first_time,
            split_date_to: last_time,
            day_sink,
            uniques: HashMap::new()
        }
    }

    fn matches(&self, line: &LogLine) -> bool{
        line.time > self.split_date_from && line.time < self.split_date_to
        && line.split_value.is_some() && line.split_value.unwrap() == self.split_id
    }

}
use tokio::fs::OpenOptions;
use tokio::io::{self, AsyncWriteExt, BufReader, AsyncBufReadExt};

pub async fn parse(config: QueryParseArgs) -> io::Result<()> {
    let QueryParseArgs {
        output_directory,
        input,
        split_by_key,
        keep_unique_of_key,
        filter_operation_type,
        filter_key_prefix,
    } = config;
    let output_dir = output_directory.unwrap_or_else(|| env::current_dir().unwrap().join("parsed"));


    let mut input_paths = expand_input_filenames(input);
    input_paths.sort();
    input_paths.reverse();
    let mut data: HashMap<String, Vec<SplitDataSink>> = HashMap::new();


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

    // Reverse to ensure we're starting from the latest log file
    for path in input_paths.iter() {
        let path_str = path.to_str().unwrap().to_string();
        println!("Reading {path_str}");
        let file = OpenOptions::new().read(true).open(path).await?;
        let reader = BufReader::new(file);
        let mut lines = reader.lines();

        while let Some(line) = lines.next_line().await? {
            let log_line = parse_s3_log_line(&line, &split_by_key, &keep_unique_of_key);

            // Filter by operation type and key prefix if specified
            if !filter_operation_type.is_empty() && log_line.operation != filter_operation_type {
                println!("Skipping operation type {}",log_line.operation);
                continue;
            }
            if !filter_key_prefix.is_empty() && !log_line.key.starts_with(&filter_key_prefix) {
                println!("Skipping key {}",log_line.key);
                continue;
            }


            if let Some(split_value) = log_line.split_value {
                let sinks = data.entry(split_value.to_string()).or_insert_with(Vec::new);

                //TODO later figure out month summary
                // Check if there's an existing sink that matches the current log line, otherwise create a new one
                let mut existing_ix = sinks.iter().position(|ref s| s.matches(&log_line));

                if existing_ix.is_none(){
                    let new_sink =
                        SplitDataSink::create_for(split_value, &output_dir, log_line.time, Some(log_line.time));
                    sinks.push(new_sink);
                    existing_ix = Some(sinks.len() -1);
                }
                let sink = &mut sinks[existing_ix.unwrap()];


                let unique_id = log_line.unique_value.unwrap_or_default().to_string();
                // Update the sink with the current log line
                sink.uniques.entry(unique_id.clone())
                    .and_modify(|e| e.update_with(&log_line))
                    .or_insert_with(|| {
                        let mut unique = SplitUnique::default(unique_id); // Assuming default() is implemented
                        unique.update_with(&log_line);
                        unique
                    });

            }else{
                println!("No split value for {line}");
            }
        }
    }

    // Summarization Phase
    for (_, sinks) in data.into_iter() {
        for sink in sinks {
            let summary = sink.summarize();
            crate::fetch::create_dirs_if_missing(sink.summary_path.as_path()).await;

            let path = sink.summary_path.to_str().unwrap().to_string();
            println!("Creating '{path}'...");
            // Open or create the summary file for writing
            let mut summary_file = OpenOptions::new().write(true).create_new(true).open(sink.summary_path).await?;
            // Serialize the summary to a string or JSON as needed
            let summary_text = format!("{:#?}", summary); // Assuming you want JSON output
            // Write the summary to file
            summary_file.write_all(summary_text.as_bytes()).await?;
            summary_file.flush().await?;

        }
    }
    println!("Done.");
    Ok(())
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_split() {
        let input = "This is a test";
        let expected = vec!["This", "is", "a", "test"];
        let result: Vec<&str> = SplitLogColumns::new(input).collect();
        assert_eq!(result, expected);
    }

    #[test]
    fn test_with_brackets() {
        let input = "This [is a] test";
        let expected = vec!["This", "[is a]", "test"];
        let result: Vec<&str> = SplitLogColumns::new(input).collect();
        assert_eq!(result, expected);
    }

    #[test]
    fn test_nested_brackets() {
        let input = "Testing [nested [brackets]] here";
        let expected = vec!["Testing", "[nested [brackets]]", "here"];
        let result: Vec<&str> = SplitLogColumns::new(input).collect();
        assert_eq!(result, expected);
    }

    #[test]
    fn test_empty_string() {
        let input = "";
        let expected: Vec<&str> = Vec::new();
        let result: Vec<&str> = SplitLogColumns::new(input).collect();
        assert_eq!(result, expected);
    }

    #[test]
    fn test_only_brackets() {
        let input = "[only brackets]";
        let expected = vec!["[only brackets]"];
        let result: Vec<&str> = SplitLogColumns::new(input).collect();
        assert_eq!(result, expected);
    }

    #[test]
    fn test_only_quotes() {
        let input = "\"in quotes\"";
        let expected = vec!["\"in quotes\""];
        let result: Vec<&str> = SplitLogColumns::new(input).collect();
        assert_eq!(result, expected);
    }
    #[test]
    fn test_mixed() {
        let input = "value1 [value 2] \"in [this]quotes\" value4";
        let expected = vec!["value1", "[value 2]", "\"in [this]quotes\"", "value4"];
        let result: Vec<&str> = SplitLogColumns::new(input).collect();
        assert_eq!(result, expected);
    }


    #[test]
    fn test_multiple_consecutive_whitespaces() {
        let input = "This  is a   test";
        let expected = vec!["This","", "is", "a","","", "test"];
        let result: Vec<&str> = SplitLogColumns::new(input).collect();
        assert_eq!(result, expected);
    }

    #[test]
    fn test_brackets_with_no_whitespace() {
        let input = "This[is]a[test]";
        let expected = vec!["This[is]a[test]"];
        let result: Vec<&str> = SplitLogColumns::new(input).collect();
        assert_eq!(result, expected);
    }

    #[test]
    fn test_log_line(){
        let input = r#"40b77a18 licenses.imazen.net [14/Feb/2024:22:38:49 +0000] 111.11.111.11 - S12ASFAFE62BDGFQ REST.GET.OBJECT v1/licenses/latest/blabla.txt "GET /licenses.imazen.net/v1/licenses/latest/blabla.txt?license_id=00000&manager_id=aasacs&extra_job_query_keys= HTTP/1.1" 200 - 700 700 19 18 "-" "-" - OTvwduQ/0a/U6Q+c1tnae+2jyokFgi++7I= - ECDHE-RSA-AES128-GCM-SHA256 - s3-us-west-2.amazonaws.com TLSv1.2 - Yes"#;
        let v:Vec<&str> = SplitLogColumns::new(input).collect();
        let expected:Vec<&str> = vec!["40b77a18",
                                      "licenses.imazen.net",
                                      "[14/Feb/2024:22:38:49 +0000]",
                                      "111.11.111.11",
                                      "-",
                                      "S12ASFAFE62BDGFQ",
                                      "REST.GET.OBJECT",
                                      "v1/licenses/latest/blabla.txt",
                                      "\"GET /licenses.imazen.net/v1/licenses/latest/blabla.txt?license_id=00000&manager_id=aasacs&extra_job_query_keys= HTTP/1.1\"",
                                      "200",
                                      "-",
                                      "700",
                                      "700",
                                      "19",
                                      "18",
                                      "\"-\"",
                                      "\"-\"",
                                      "-",
                                      "OTvwduQ/0a/U6Q+c1tnae+2jyokFgi++7I=",
                                      "-",
                                      "ECDHE-RSA-AES128-GCM-SHA256",
                                      "-",
                                      "s3-us-west-2.amazonaws.com",
                                      "TLSv1.2",
                                      "-",
                                      "Yes"];
        assert_eq!(v, expected);

        let log_line = parse_s3_log_line(input, "license_id", "manager_id");

        assert_eq!(log_line.bucket, expected[1]);
        assert_eq!(log_line.ip_str, expected[3]);
        assert_eq!(log_line.key, expected[7]);
        assert_eq!(log_line.truncated, false);
        assert_eq!(log_line.operation, expected[6]);
        assert_eq!(log_line.split_value, Some("00000"));
        assert_eq!(log_line.unique_value, Some("aasacs"));
        assert_eq!(log_line.response_status_str, "200");
        assert_eq!(log_line.request_path, "/licenses.imazen.net/v1/licenses/latest/blabla.txt");
        assert_eq!(log_line.request_query, Some("?license_id=00000&manager_id=aasacs&extra_job_query_keys="));


    }
}
