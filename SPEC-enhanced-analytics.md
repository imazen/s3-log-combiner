# Enhanced Analytics Reporting Specification

## Overview

Add JSON reporting, time-series analytics, and product differentiation (Imageflow vs ImageResizer) to the log summarization system.

## Goals

1. **JSON reports** alongside text for programmatic parsing
2. **Product breakdown** - Track Imageflow vs ImageResizer usage separately
3. **License distribution** - What % use Imageflow, ImageResizer, both, or neither
4. **Weekly time-series** - Trend analysis (steady/declining/rising) per customer
5. **Daily concurrent machines** - Unique mac_digest per day per license
6. **Domain tracking** - Per-license domain lists + global Imageflow domain aggregation

## Configuration

- **Time granularity**: Weekly buckets for trends
- **Concurrency window**: Same-day = machines are concurrent
- **History window**: 52 weeks + 90 days
- **Memory budget**: ~350MB additional RAM

## Data Structures

### ProductType (enum)
```rust
pub enum ProductType {
    Imageflow,      // is_imageflow=true OR version starts with "0."
    ImageResizer,   // version starts with "4." or "5."
    Unknown,
}
```

### WeeklyMetrics
```rust
pub struct WeeklyMetrics {
    pub week_key: String,                    // "2024-W03"
    pub jobs_completed: u64,
    pub unique_machines: HashSet<String>,    // mac_digest
    pub unique_ips: HashSet<String>,
    pub report_count: u64,
}
```

### DailyMachineStats
```rust
pub struct DailyMachineStats {
    pub date_key: String,                    // "2024-01-15"
    pub unique_machines: HashSet<String>,
    pub jobs_completed: u64,
}
```

### DomainStats
```rust
pub struct DomainStats {
    pub domain: String,
    pub job_count: u64,
    pub first_seen: Option<DateTime<Utc>>,
    pub last_seen: Option<DateTime<Utc>>,
    pub associated_ips: HashSet<String>,
    pub uses_imageflow: bool,
    pub uses_imageresizer: bool,
}
```

### TrendDirection (enum)
```rust
pub enum TrendDirection {
    Rising,        // >10% increase over 3 weeks
    Declining,     // >10% decrease over 3 weeks
    Steady,        // within +/-10%
    Insufficient,  // <3 weeks of data
}
```

### VersionStats (for compatibility tracking)
```rust
pub struct VersionStats {
    pub product: ProductType,
    pub version: String,
    pub query_keys: HashMap<String, FeatureUsage>,
    pub extra_job_query_keys: HashMap<String, FeatureUsage>,
    pub plugins: HashMap<String, FeatureUsage>,
    pub job_count: u64,
    pub license_ids: HashSet<String>,
}

/// Per-feature usage statistics
pub struct FeatureUsage {
    pub feature_name: String,
    pub job_count: u64,                    // How many jobs used this feature
    pub license_ids: HashSet<String>,      // Which licenses use it
    pub license_names: Vec<String>,        // Owner names for easy reading
}
```

This enables identifying:
- Features used by only 1 license → deprecation candidates
- Features used by many licenses → critical to maintain
- Features with high job counts → performance-critical

### EnhancedSummary
```rust
pub struct EnhancedSummary {
    pub base: Summary,
    pub weekly_metrics: BTreeMap<String, WeeklyMetrics>,
    pub daily_machines: BTreeMap<String, DailyMachineStats>,
    pub imageflow_jobs: u64,
    pub imageresizer_jobs: u64,
    pub imageflow_machines: HashSet<String>,
    pub imageresizer_machines: HashSet<String>,
    pub domain_stats: HashMap<String, DomainStats>,
    pub job_trend: TrendDirection,
    pub machine_trend: TrendDirection,
}
```

## JSON Output Schema

```json
{
  "generated_at": "2024-01-15T12:00:00Z",
  "license_distribution": {
    "total": 150,
    "imageflow_only": 20,
    "imageresizer_only": 80,
    "both_products": 40,
    "inactive": 10,
    "imageflow_percent": 40.0,
    "imageresizer_percent": 80.0
  },
  "licenses": [{
    "license_id": "123456",
    "license_status": "active",
    "owner": "Acme Corp",
    "features": ["Imageflow", "R_Elite"],
    "total_jobs": 150000,
    "unique_machines": 5,
    "unique_ips": 3,
    "imageflow": { "jobs": 140000, "machines": 4 },
    "imageresizer": { "jobs": 10000, "machines": 1 },
    "weekly_data": [
      {
        "week": "2024-W01",
        "jobs_completed": 50000,
        "unique_machines": 5,
        "unique_ips": 3,
        "report_count": 168
      }
    ],
    "daily_machine_counts": [
      { "date": "2024-01-15", "concurrent_machines": 5, "jobs": 8000 }
    ],
    "job_trend": "rising",
    "machine_trend": "steady",
    "domains": [
      { "domain": "example.com", "job_count": 100000, "ip_count": 2 }
    ],
    "first_activity": "2024-01-01T00:00:00Z",
    "last_activity": "2024-01-15T12:00:00Z"
  }],
  "imageflow_global": {
    "total_jobs": 4000000,
    "total_machines": 400,
    "total_licenses": 50,
    "all_domains": [
      {
        "domain": "bigsite.com",
        "total_jobs": 500000,
        "license_count": 3,
        "machine_count": 15,
        "first_seen": "2024-01-01T00:00:00Z",
        "last_seen": "2024-01-15T00:00:00Z"
      }
    ]
  },
  "aggregate": {
    "total_licenses_active": 150,
    "total_jobs": 5000000,
    "total_unique_machines": 500,
    "imageflow_jobs": 4000000,
    "imageresizer_jobs": 1000000
  },
  "version_compatibility": [
    {
      "product": "imageresizer",
      "version": "4.2.8",
      "job_count": 2345678,
      "license_count": 89,
      "query_keys": {
        "width": { "job_count": 2000000, "license_count": 85 },
        "height": { "job_count": 1900000, "license_count": 82 },
        "mode": { "job_count": 500000, "license_count": 40 }
      },
      "plugins": {
        "DiskCache": { "job_count": 2300000, "license_count": 88 },
        "S3Reader2": { "job_count": 5000, "license_count": 1, "licenses": ["Acme Corp"] },
        "PrettyGifs": { "job_count": 100000, "license_count": 15 }
      }
    }
  ],
  "feature_deprecation_candidates": [
    {
      "feature_type": "plugin",
      "feature_name": "S3Reader2",
      "version": "4.2.8",
      "license_count": 1,
      "licenses": [{"id": "123456", "owner": "Acme Corp", "job_count": 5000}]
    }
  ],
  "source_format_usage": {
    "jpg": { "job_count": 5000000, "license_count": 140 },
    "png": { "job_count": 3000000, "license_count": 120 },
    "webp": { "job_count": 500000, "license_count": 40 },
    "gif": { "job_count": 200000, "license_count": 30 },
    "tiff": { "job_count": 50000, "license_count": 10 },
    "tif": { "job_count": 10000, "license_count": 5 },
    "bmp": { "job_count": 5000, "license_count": 3, "licenses": ["OldCorp", "LegacyInc", "RetroSoft"] }
  }
}
```

## Output Files

| File | Description |
|------|-------------|
| `report.json` | Full structured data (schema above) |
| `report.txt` | Existing text summary (unchanged) |
| `imageflow_report.txt` | Global Imageflow stats + ALL domains using Imageflow |
| `weekly_breakdown.txt` | Week-by-week activity across all licenses |
| `compatibility_report.txt` | Query keys & plugins by product version (for maintenance planning) |
| `low_usage_report.txt` | Existing (unchanged) |
| `violation_report.txt` | Existing (unchanged) |

## Files to Modify

| File | Changes |
|------|---------|
| `Cargo.toml` | Add `serde`, `serde_json` |
| `src/util.rs` | Add `week_key_from_datetime()` |
| `src/telemetry.rs` | Add EnhancedSummary, WeeklyMetrics, etc.; make `mac_digest` pub(crate) |
| `src/json_report.rs` | **NEW** - JSON serialization |
| `src/summarize.rs` | Add `summarize_enhanced()`, report generation |
| `src/main.rs` | Register `json_report` module |

## Trend Calculation

Compare last 3 weeks:
- Compute % change from week[0] to week[2]
- Rising: >+10%
- Declining: <-10%
- Steady: within +/-10%
- Insufficient: <3 data points

## Product Detection Logic

```rust
fn detect_product(report: &Report) -> ProductType {
    if report.is_imageflow {
        return ProductType::Imageflow;
    }
    let version = &report.process.info_version;
    if version.starts_with("0.") {
        ProductType::Imageflow
    } else if version.starts_with("4.") || version.starts_with("5.") {
        ProductType::ImageResizer
    } else {
        ProductType::Unknown
    }
}
```

## Implementation Order

1. Add serde dependencies to Cargo.toml
2. Add `week_key_from_datetime()` to util.rs
3. Add data structures to telemetry.rs
4. Create json_report.rs
5. Add `summarize_enhanced()` to SplitDataSink
6. Add report generation functions
7. Wire up in process_lines()
8. Test with existing log data
