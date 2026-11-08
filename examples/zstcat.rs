// Tiny helper: prints the first N lines of a .zst file. Used to sanity-check `recompress` output.
// Usage: cargo run --release --example zstcat -- <path.zst> [lines]

use std::env;
use std::fs::File;
use std::io::{BufRead, BufReader};

fn main() -> std::io::Result<()> {
    let mut args = env::args().skip(1);
    let path = args.next().expect("usage: zstcat <path.zst> [n_lines]");
    let n: usize = args
        .next()
        .map(|s| s.parse().unwrap_or(5))
        .unwrap_or(5);

    let f = File::open(&path)?;
    let dec = zstd::stream::Decoder::new(BufReader::new(f))?;
    let r = BufReader::new(dec);
    for (i, line) in r.lines().enumerate() {
        if i >= n {
            break;
        }
        println!("{}", line?);
    }
    Ok(())
}
