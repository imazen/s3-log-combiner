# Continuation Point: Enhanced Analytics Implementation

## Current Branch
`enhanced-analytics`

## What's Done

### 1. Cargo.toml
- Added `serde` and `serde_json` dependencies

### 2. src/util.rs
- Added `week_key_from_datetime()` - returns "2024-W03" format
- Added `date_key_from_datetime()` - returns "2024-01-15" format

### 3. src/telemetry.rs
- Made `HardwareAttrs` pub(crate) with `mac_digest` and `logical_cores` accessible
- Made Report fields accessible: `logged_at`, `is_imageflow`, `image_domains`, `page_domains`, `hardware`
- Added new structures at end of file:
  - `ProductType` enum (Imageflow, ImageResizer, Unknown) with detection logic
  - `WeeklyMetrics` struct for weekly time-series
  - `DailyMachineStats` struct for daily concurrency tracking
  - `DomainStats` struct for domain usage
  - `TrendDirection` enum (Rising, Declining, Steady, Insufficient)
  - `EnhancedSummary` struct with full implementation including:
    - `add_from_enhanced()` method
    - `compute_trends()` method
    - `uses_imageflow()` / `uses_imageresizer()` helpers

## What's Left

### 4. Create src/json_report.rs (NEW FILE)
JSON serialization structures:
- `JsonReport` - top-level report
- `LicenseReport` - per-license data
- `LicenseDistribution` - % using Imageflow, ImageResizer, both, neither
- `ImageflowGlobalReport` - global Imageflow domain aggregation
- `WeeklyData`, `DailyMachineCount`, `DomainReport`, etc.

### 5. Modify src/summarize.rs
- Add `summarize_enhanced()` method to `SplitDataSink`
- Add report generation functions:
  - `generate_json_report()`
  - `generate_imageflow_global_report()`
  - `generate_weekly_breakdown()`
- Update `process_lines()` to generate new reports

### 6. Register module in src/main.rs
```rust
mod json_report;
```

### 7. Test with existing log data
Run summarize command and verify output

## Key Requirements (from user)
- JSON reports alongside text
- Track Imageflow vs ImageResizer separately
- License distribution stats (% using each product)
- Weekly time-series with trends (rising/declining/steady)
- Daily concurrent machines (unique mac_digest per day)
- Per-license domain lists
- Global Imageflow domain aggregation (all domains using Imageflow across all licenses)
- 52 weeks + 90 days of history
- ~350MB RAM budget acceptable

## Output Files to Generate
| File | Content |
|------|---------|
| `report.json` | Full structured JSON |
| `report.txt` | Existing text (keep) |
| `imageflow_report.txt` | Global Imageflow + all domains |
| `weekly_breakdown.txt` | Week-by-week activity |
| `low_usage_report.txt` | Keep existing |
| `violation_report.txt` | Keep existing |

## Spec File
See `SPEC-enhanced-analytics.md` for full specification including JSON schema.

## To Resume
1. `git checkout enhanced-analytics`
2. Continue with step 4: Create `src/json_report.rs`
3. Then modify `src/summarize.rs`
4. Register module and test
