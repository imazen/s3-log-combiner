use crate::cli::QueryParseArgs;

pub async fn parse(config: QueryParseArgs) {
    let QueryParseArgs {
        output_directory,
        input,
        split_by_key,
        keep_unique_of_key,
        filter_operation_type,
        filter_key_prefix,
    } = config;
}

fn parse_s3_log_line(
    line: &str,
    mut process_entry: impl FnMut(&str, &str, &str, &str, &str, &str, &str),
) {
    let mut parts = line.split_whitespace();

    let bucket = parts.next().unwrap_or("");
    let time = parts.nth(1).unwrap_or(""); // Skips directly to the time part
    let ipaddr = parts.nth(2).unwrap_or(""); // Accounts for the previous next()
    let operation = parts.next().unwrap_or("");
    let key = parts.nth(1).unwrap_or(""); // Skips requester ID
    let response_status = parts.next().unwrap_or("");
    let request_uri = parts.nth(3).unwrap_or(""); // Skips to Request-URI

    // Filter to GET requests
    if operation.contains("REST.GET") {
        // Parse the query string from the request URI
        if let Some(query_start) = request_uri.find('?') {
            let query_string = &request_uri[query_start + 1..];
            process_entry(
                bucket,
                time,
                ipaddr,
                operation,
                key,
                response_status,
                query_string,
            );
        }
    }
}
