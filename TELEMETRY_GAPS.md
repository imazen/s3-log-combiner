# Client Telemetry Data Reporting Gaps

**Date:** 2026-02-27
**Context:** Analysis of s3-log-combiner output vs. SUMMARY_ENHANCEMENTS_SPEC goals
**Affected repo:** `wimageflow-dotnet-server` (Imageflow .NET Server)

---

## Root Cause: Job Tracking Disabled in Imageflow Server

In `src/Imazen.Routing/Promises/Pipelines/ImagingMiddleware.cs` lines 293-299:

```csharp
// TODO: restore instrumentation
// GlobalPerf.Singleton.JobComplete(new ImageJobInstrumentation(jobResult)
// {
//     FinalCommandKeys = FinalQuery.Keys,
//     ImageDomain = ImageDomain,
//     PageDomain = PageDomain
// });
```

This single commented-out block causes **every Imageflow license** (~27% of customer base, 59 licenses) to report:

| Field | Expected | Actual |
|-------|----------|--------|
| `jobs_completed_total` | Real count | **0** |
| `image_domains` | Serving domains | **[""]** |
| `page_domains` | Referrer domains | **[""]** |
| `query_keys` | URL params used | **[""]** |
| `jobs_completed_per_*_max` | Peak rates | **0** |
| `decoded_pixels_total` | Pixel throughput | **0** |
| `encoded_pixels_total` | Pixel throughput | **0** |
| `job_times_*` percentiles | Timing data | **0** |
| `source_file_ext_*` | Format distribution | **0** |
| `source_multiple_*` | Dimension stats | **0** |

ImageResizer licenses (v4.x/5.x, ~73%) are unaffected — they use separate code.

---

## Additional Structural Issues

### 1. `outerRequest` not stored in ImagingPromise

The HTTP request adapter is passed to the `ImagingPromise` constructor but not stored as a field. It's only used locally for watermark child requests. By the time `TryGetBlobAsync` runs (where JobComplete should be called), there's no access to the request's Host or Referer headers for domain extraction.

### 2. `FinalQuery.Keys` no longer exists

The old code referenced `FinalQuery.Keys` but the rewrite changed this to `FinalCommandString` (a querystring-format string like `width=300&height=200`). Needs parsing back into keys via `QueryHelpers.ParseNullableQuery()`.

### 3. Cumulative counters with no period attribution

`jobs_completed_total` is a running counter since process start. When a process restarts, the counter resets to 0 with no history. There's no `jobs_completed_since_last_report` or `process_uptime_seconds` field to let the server normalize the data.

### 4. 7-day S3 log window labeled as "90 days"

The s3-log-combiner uses `data_cutoff = Utc::now() - Duration::days(7)` (summarize.rs:525). The "Usage count in last 90d" label in reports is misleading — it's actually the sum of machines' latest cumulative counters from the last 7 days of S3 access logs. True 90-day windowing would require persistent monthly archival (TODO at summarize.rs:585).

### 5. Zero-job licenses with active machines

Licenses like 250582605 (On Demand Solutions) show `jobs_completed_total: 0` despite having 2+ active machines and multiple IPs. This is caused by the disabled JobComplete call — Imageflow processes simply never report job counts.

### 6. Empty version strings

Some licenses report `info_version: ""`, causing `ProductType::from_report()` to classify them as `Unknown` instead of Imageflow or ImageResizer. This affects the license distribution stats (e.g., `inactive: 1` undercounts).

### 7. No per-domain job attribution

`DomainStats.job_count` in the s3-log-combiner adds the machine's entire `jobs_completed_total` to every domain that machine reports. A machine serving 3 domains gets its full count added to all 3. The client doesn't provide per-domain breakdowns.

---

## Impact on SUMMARY_ENHANCEMENTS_SPEC Goals

| Spec Goal | Status | Blocker |
|-----------|--------|---------|
| Recent usage windows (90d/30d) | Blocked | 7-day log window + cumulative counters |
| Monthly job counts | Blocked | No monthly archival (TODO in summarize.rs) |
| Trend analysis (growing/declining) | Degraded | Only ~7 days of data points |
| Domain classification | Blocked for Imageflow | Empty domains due to disabled JobComplete |
| Reseller detection | Blocked for Imageflow | Empty domains |
| Activity status (churned) | Partial | Works within 7-day window |
| Infrastructure scale | Works | mac_digest, IPs, cores all reported correctly |
| Peak concurrent machines | Works | DailyMachineStats functional |

---

## Fix Plan for wimageflow-dotnet-server

### Change 1: Store outerRequest in ImagingPromise

**File:** `src/Imazen.Routing/Promises/Pipelines/ImagingMiddleware.cs`

Add a private property to the `ImagingPromise` record:
```csharp
private IHttpRequestStreamAdapter OuterRequest { get; init; }
```

Assign in constructor:
```csharp
OuterRequest = outerRequest;
```

### Change 2: Re-enable JobComplete with updated data extraction

**File:** `src/Imazen.Routing/Promises/Pipelines/ImagingMiddleware.cs`

Replace lines 293-299 with:
```csharp
var imageDomain = OuterRequest.GetServerHost();
var pageDomain = OuterRequest.GetRefererHost();
var commandKeys = QueryHelpers.ParseNullableQuery(FinalCommandString)?.Keys;

GlobalPerf.Singleton.JobComplete(new ImageJobInstrumentation(jobResult)
{
    FinalCommandKeys = commandKeys,
    ImageDomain = imageDomain,
    PageDomain = pageDomain
});
```

Add usings:
```csharp
using Imazen.Common.Instrumentation;
using Imageflow.Server;
```

**Existing helpers reused (no changes needed):**
- `GetServerHost()` at `HttpRequestStreamAdapterExtensions.cs:58`
- `GetRefererHost()` at `HttpRequestStreamAdapterExtensions.cs:63`
- `QueryHelpers.ParseNullableQuery()` at `Imazen.Abstractions/HttpStrings/QueryHelpers.cs:148`
- `GlobalPerf.JobComplete()` at `GlobalPerf.cs:101`
- `ImageJobInstrumentation` constructor at `ImageJobInstrumentation.cs:8`

**Visibility:** `GlobalPerf` (internal, Imazen.Common) is visible to `Imazen.Routing` via `InternalsVisibleTo` in `Imazen.Common/VisibleToTests.cs:5`. `ImageJobInstrumentation` is in the `Imazen.Routing` assembly itself (just uses `Imageflow.Server` namespace).

### Change 3: Add process uptime tracking

**File:** `src/Imazen.Common/Instrumentation/BasicProcessInfo.cs`

Add static start timestamp:
```csharp
private static readonly long StartTimestamp = System.Diagnostics.Stopwatch.GetTimestamp();
```

Add uptime reporting in `Add()`:
```csharp
var uptimeSeconds = (System.Diagnostics.Stopwatch.GetTimestamp() - StartTimestamp)
    / System.Diagnostics.Stopwatch.Frequency;
q.Add("uptime_s", uptimeSeconds);
```

Reports as `proc_uptime_s=NNN` in telemetry (~20 chars, within 7000-char budget).

---

## What does NOT need client changes

- **Monthly archival** — requires s3-log-combiner changes (persistent storage of past summaries)
- **7-day window expansion** — s3-log-combiner could process more days of logs
- **Domain classification** — heuristic logic in s3-log-combiner, not a client-side concern
- **Per-domain job attribution** — fundamental limitation of how telemetry works (flat domain list per machine)

---

## Server-side changes needed in s3-log-combiner (separate work)

1. Parse `proc_uptime_s` from reports to detect counter resets
2. Implement monthly summary archival (TODO at summarize.rs:585)
3. Add domain classification heuristics (production vs staging vs client)
4. Fix the "90d" labeling to be honest about the 7-day data window
5. Handle `ProductType::Unknown` for empty version strings more gracefully
