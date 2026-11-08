# Summary Data Enhancements Spec

**Requested by:** CRM discount audit workflow
**Date:** 2025-12-30
**Purpose:** Enable better decision-making about customer outreach by providing time-based usage data

---

## Current Limitations

The current `.summary.txt` files provide aggregate totals but lack:

- Time-based breakdowns needed to assess recent activity
- Easy-to-parse structured output
- Trend analysis data

---

## Requested Enhancements

### 1. Recent Usage Window (Critical)

**Need:** Know if a customer is actively using the product before outreach.

```
recent_usage:
  last_90_days:
    jobs_completed: 12345
    first_activity: 2025-10-15T...
    last_activity: 2025-12-28T...
  last_30_days:
    jobs_completed: 4500
    first_activity: 2025-12-01T...
    last_activity: 2025-12-28T...
```

### 2. Monthly Job Counts (High Priority)

**Need:** Identify usage trends (growing, stable, declining, churned).

```
monthly_jobs:
  2025-12: 4500
  2025-11: 4200
  2025-10: 3800
  2025-09: 4100
  2025-08: 3900
  # ... last 12 months
```

### 3. Activity Timeline (High Priority)

**Need:** Quick assessment of account health.

```
activity:
  first_seen: 2023-05-14T...
  last_seen: 2025-12-28T14:32:00Z
  days_since_last_activity: 2
  total_active_months: 28
  status: "active" | "dormant" | "churned"  # churned = no activity 90+ days
```

### 4. Domain Classification (Medium Priority)

**Need:** Distinguish between customer's own domains vs client domains (reseller detection).

```
domain_analysis:
  total_unique_domains: 193
  likely_production_domains: 45  # excludes localhost, staging, episerver.net, etc.
  likely_client_domains: 38      # domains not matching primary company domain
  primary_domain_pattern: "epinova.no"
  sample_client_domains:
    - "abelia.no"
    - "advokatforeningen.no"
    - "dnv.com"
```

**Heuristics for classification:**

- Exclude: localhost, *.azurewebsites.net, *.episerver.net, *.dxcloud.*, staging.*, test.*, inte.*, prep.*
- Identify primary domain from license registration or most common pattern
- Flag domains that don't match primary pattern as potential client domains

### 5. Infrastructure Scale (Medium Priority)

**Need:** Assess operation size beyond job counts.

```
infrastructure:
  unique_ips: 117
  unique_machines: 45
  peak_concurrent_machines: 12
  total_cores_seen: 180
```

### 6. Usage Intensity Metrics (Low Priority)

```
intensity:
  avg_jobs_per_day_last_90: 137
  avg_jobs_per_day_last_30: 150
  peak_daily_jobs: 890
  peak_date: 2025-11-15
```

---

## Output Format Request

### Option A: Enhanced summary.txt (backward compatible)

Add new sections to existing format:

```
Summary {
    // ... existing fields ...

    // NEW FIELDS
    recent_90_day_jobs: 12345,
    recent_30_day_jobs: 4500,
    last_activity: Some(2025-12-28T14:32:00Z),
    first_activity: Some(2023-05-14T10:00:00Z),
    days_since_activity: 2,

    monthly_jobs: {
        "2025-12": 4500,
        "2025-11": 4200,
        // ...
    },

    production_domain_count: 45,
    likely_client_domain_count: 38,
}
```

### Option B: Separate JSON file (preferred)

Create `{license_id}.analytics.json` alongside `.summary.txt`:

```json
{
  "license_id": "639078372",
  "generated_at": "2025-12-30T01:30:00Z",
  "activity": {
    "first_seen": "2023-05-14T10:00:00Z",
    "last_seen": "2025-12-28T14:32:00Z",
    "days_since_last": 2,
    "status": "active"
  },
  "usage": {
    "all_time": {
      "jobs": 1370969,
      "encoded_pixels": 123456789,
      "decoded_pixels": 987654321
    },
    "last_90_days": {
      "jobs": 125000,
      "first_activity": "2025-10-02",
      "last_activity": "2025-12-28"
    },
    "last_30_days": {
      "jobs": 45000
    },
    "monthly": {
      "2025-12": 45000,
      "2025-11": 42000,
      "2025-10": 38000
    }
  },
  "domains": {
    "total": 193,
    "production": 156,
    "staging_test": 37,
    "likely_client": 120,
    "top_10": [
      "dnv.com",
      "postnord.com",
      "kiwa.com",
      "..."
    ]
  },
  "infrastructure": {
    "unique_ips": 117,
    "unique_machines": 45,
    "total_cores": 180
  },
  "trend": {
    "direction": "stable",
    // "growing" | "stable" | "declining" | "churned"
    "mom_change_percent": 7.1
  }
}
```

---

## Use Cases This Enables

1. **Pre-outreach filtering:** Don't contact customers who haven't used the product in 90 days (separate churn recovery
   workflow)

2. **Reseller detection:** High `likely_client_domain_count` indicates agency/reseller usage

3. **Usage-based prioritization:** Focus on high-usage customers first

4. **Trend analysis:** Identify at-risk accounts (declining usage) vs growth accounts

5. **Audit timeline:** Know how long improper discounts have been in effect

---

## Implementation Notes

- Monthly aggregation can use existing `{license}/{year}/{month}/summary.txt` structure
- Last activity date: use `last_full_report_from` field or latest file timestamp
- Domain classification: regex patterns for staging/test environments
- Consider caching/incremental updates for performance

---

## Priority Order

1. **last_activity date** - Critical for outreach decisions
2. **last_90_days.jobs** - Critical for outreach decisions
3. **monthly_jobs** - High for trend analysis
4. **likely_client_domain_count** - High for reseller detection
5. **JSON output format**make - Medium, improves parsing
6. **trend direction** - Low, nice to have
