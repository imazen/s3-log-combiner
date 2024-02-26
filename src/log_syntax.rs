use chrono::{DateTime, Datelike, TimeZone, Utc};

/// Supports common log syntax within a single line; splits on column boundaries (spaces) even when quoted or bracketed cells are used.
pub struct SplitLogColumns<'a> {
    s: &'a str,
    current_pos: usize,
    bracket_depth: usize,
    in_quotes: bool,
}

impl<'a> SplitLogColumns<'a> {
    pub fn new(s: &'a str) -> Self {
        SplitLogColumns {
            s,
            current_pos: 0,
            bracket_depth: 0,
            in_quotes: false,
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
                }
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

#[derive(Debug, Clone)]
pub struct S3LogLine<'a> {
    //[25/Dec/2023:23:42:03 +0000]
    pub(crate) time: DateTime<Utc>,
    pub(crate) ip_str: &'a str,
    pub(crate) operation: &'a str,
    pub bucket: &'a str,
    pub(crate) key: &'a str,
    response_status_str: &'a str,
    request_path: &'a str,
    pub(crate) request_query: Option<&'a str>,
    pub(crate) split_value: Option<&'a str>,
    pub(crate) unique_value: Option<&'a str>,
    pub(crate) truncated: bool,
}

impl<'a> S3LogLine<'a> {
    pub fn from_line_str(line: &'a str, split_by_key: &str, unique_key: &str) -> S3LogLine<'a> {
        let mut parts = SplitLogColumns::new(line);

        let bucket = parts.nth(1).unwrap_or("");
        let time;
        if let Some(time_str) = parts.nth(0) {
            let result = DateTime::parse_from_str(time_str, "[%d/%b/%Y:%H:%M:%S %z]");
            if let Err(e) = result {
                panic!("Error parsing date: '{time_str}'\n{e}");
            }
            time = result.unwrap().with_timezone(&Utc);
        } else {
            eprintln!("Error parsing line: {line}");
            std::process::exit(3);
        }

        let ip_str = parts.nth(0).unwrap_or(""); // Accounts for the previous next()
        let operation = parts.nth(2).unwrap_or("");
        let key = parts.nth(0).unwrap_or(""); // Skips requester ID
        let request_command = parts.nth(0).unwrap_or(""); // Skips to Request-URI
        let request_uri = request_command.split(' ').nth(1).unwrap_or("");

        let response_status_str = parts.next().unwrap_or("");
        //let host_header = parts.nth(12).unwrap_or("");
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

        S3LogLine {
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
            truncated,
        }
    }
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
        let expected = vec!["This", "", "is", "a", "", "", "test"];
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
    fn test_log_line() {
        let input = r#"40b77a18 licenses.imazen.net [14/Feb/2024:22:38:49 +0000] 111.11.111.11 - S12ASFAFE62BDGFQ REST.GET.OBJECT v1/licenses/latest/blabla.txt "GET /licenses.imazen.net/v1/licenses/latest/blabla.txt?license_id=00000&manager_id=aasacs&extra_job_query_keys= HTTP/1.1" 200 - 700 700 19 18 "-" "-" - OTvwduQ/0a/U6Q+c1tnae+2jyokFgi++7I= - ECDHE-RSA-AES128-GCM-SHA256 - s3-us-west-2.amazonaws.com TLSv1.2 - Yes"#;
        let v: Vec<&str> = SplitLogColumns::new(input).collect();
        let expected: Vec<&str> = vec!["40b77a18",
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

        let log_line = S3LogLine::parse_from_str(input, "license_id", "manager_id");

        assert_eq!(log_line.bucket, expected[1]);
        assert_eq!(log_line.ip_str, expected[3]);
        assert_eq!(log_line.key, expected[7]);
        assert_eq!(log_line.truncated, false);
        assert_eq!(log_line.operation, expected[6]);
        assert_eq!(log_line.split_value, Some("00000"));
        assert_eq!(log_line.unique_value, Some("aasacs"));
        assert_eq!(log_line.response_status_str, "200");
        assert_eq!(
            log_line.request_path,
            "/licenses.imazen.net/v1/licenses/latest/blabla.txt"
        );
        assert_eq!(
            log_line.request_query,
            Some("?license_id=00000&manager_id=aasacs&extra_job_query_keys=")
        );
    }
}
