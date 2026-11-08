# Customer outreach reporting pipeline

Joins license telemetry (from the S3 access logs) with Chargebee billing data and
produces a single self-contained HTML page for collections / renewal triage.

## The pipeline

```
S3 logs ──combine──> logs.licenses.imazen.net/*.zst
                              │
                              ├─summarize──> parsed_120d/report.json
                              │
Chargebee MCP ─fetch_chargebee_mcp.py──> chargebee_customers.json
                              │
                              └─build_customer_report.py──> customer_report.html
```

### 1. Pull the latest logs

Resumes automatically from the newest file already in the output directory.

```bash
export AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=...
./target/release/s3-log-combiner combine \
    --from-bucket logs.licenses.imazen.net \
    --output-directory logs.licenses.imazen.net
```

### 2. Summarize into report.json

`--days` controls how far back log lines are read. It defaults to **7**, which is
fine for a quick health check but far too short to tell an active customer from a
churned one. Pass a large value for outreach work.

> **The S3 bucket expires log objects after roughly 45 days.** Asking for 120 days
> does not fail, it just returns everything that still exists — currently a ~46-day
> window. To build real long-term history, run `combine` on a schedule (monthly or
> better) so logs land on disk before S3 drops them. The gaps in
> `logs.licenses.imazen.net/` (nothing between 2025-12 and 2026-07) are exactly the
> periods when nobody ran it.

```bash
./target/release/s3-log-combiner summarize \
    --input logs.licenses.imazen.net \
    --output-directory parsed_120d \
    --days 120
```

### 3. Pull Chargebee data

Credentials live in `.env` at the repo root (gitignored):

```
CHARGEBEE_MCP_URL=https://imazen.mcp.chargebee.com/data_lookup_agent
CHARGEBEE_MCP_KEY=live_ai_...
```

```bash
python tools/fetch_chargebee_mcp.py
```

The script probes `tools/list` and fetches whatever the server exposes. It prints
which toolsets are missing and keeps working without them.

> **To get true unpaid-invoice status**, an admin must enable the
> **Invoice Lookup**, **Subscription Lookup** and **Transaction Lookup** toolsets
> under *Settings → Configure Chargebee → Agentic AI → MCP Servers → Chargebee
> Data Lookup MCP Server → Toolsets*. Until then only the Customer Lookup toolset
> is on, and the report falls back to card status as a payment-health proxy.

### 4. Build the HTML report

```bash
python tools/build_customer_report.py \
    --report parsed_120d/report.json \
    --chargebee chargebee_customers.json \
    --out customer_report.html \
    --recent-days 14
```

`--recent-days` sets the "still live" threshold. Keep it well inside the data
window — with a ~46-day window, 14 gives useful separation (silent = not seen in
28 days); 30 would leave almost nothing classifiable as silent.

## How customers are matched

Licenses carry no Chargebee customer ID — only an `Owner` string and a `Product`
plan name. The join normalizes both sides (HTML-unescape, lowercase, strip
punctuation and corporate suffixes like Inc/Ltd/GmbH/AB/BV) and matches on the
result. Unmatched licenses are shown explicitly as "No CB match" rather than
silently dropped, and paying Chargebee customers with no telemetry get their own
table at the bottom.

## What each expanded row shows

Clicking a row opens the full record for that customer, in eight sections:

| Section | Contents |
|---|---|
| License & entitlement | ID, owner, plan, kind, issued/expiry dates, core and server caps, licensed domains, features, **contractual restrictions**, revocation message |
| Company | Company name, **reported employee count** (`cf_company_size`), contact name, phone, postal address, country |
| Contacts | Every email on the account — primary, billing, Chargebee `contacts`, `cf_backup_contacts`, `cf_administrators` — each labelled with its role |
| Websites | Declared site (`cf_website`), the email domain (consumer providers filtered out), and the domains actually observed serving images |
| Billing | Chargebee ID, MRR, currency, payment state, card status, auto-collect, net terms, unbilled charges, credits, deep link into Chargebee |
| Usage | Peak machines, machines seen, IPs, lifetime jobs, Imageflow vs ImageResizer split, last seen |
| Other Chargebee fields | Everything else on the customer record that is not shown above |
| Domains | Every reported domain, linked, with job counts and which product served them |

**Card and payment-instrument details are excluded** — no card numbers, last-4,
expiry, gateway references or payment-source IDs reach the page. `card_status`
(`valid` / `expired` / `no_card`) is kept because it is a state flag that drives
segmentation, not a card detail.

### Compliance flags

Many licenses are capped by headcount ("Only valid for organizations with less
than 30 employees"), and the Chargebee record carries a self-reported employee
count. The report compares the two and flags each license green (within cap),
red (over cap) or amber (cap exists but headcount not reported). Non-profit-only,
AGPLv3-only, no-resale and revenue-cap clauses are surfaced as amber, since none
of those can be verified from billing data alone.

Both sides are self-reported, so treat a red flag as *worth a conversation*, not
as proof of a breach.

## Outreach segments

The report's whole point is that these groups need different messages:

| Segment | Meaning | Tone |
|---|---|---|
| Revoked but still running | License revoked for non-payment, still serving jobs | Highest priority — they are using unlicensed software |
| Expired but still running | Term lapsed, install still working | Renewal conversation |
| Payment broken, actively using | Card dead, product in daily use | Warn before service breaks — they have real exposure |
| Paying, not using | Billing fine, product idle | Churn risk — re-engage before renewal |
| Payment broken, not using | Card dead, already gone | Low-touch collections or write-off |
| Healthy | Active, paid | No action |

## Activity classification

Measured against the **end of the log window**, not today, because log delivery
lags. With `--recent-days 14`:

- **Active** — machines seen in the last 14 days with work on record
- **Fading** — jobs earlier in the window, none recently
- **Idle** — still checking in, but zero jobs (installed, not serving traffic)
- **Silent** — no check-in in 2x that window

## Files

| File | Purpose |
|---|---|
| `mcp_client.py` | Minimal MCP-over-HTTP JSON-RPC client (no dependencies) |
| `fetch_chargebee_mcp.py` | Pulls Chargebee data via the MCP server |
| `fetch_chargebee.py` | Alternative puller for the Chargebee **REST** API, if a standard API key is available |
| `build_customer_report.py` | Joins both sources and renders the HTML |

The CSV export carries the same data flattened for mail merge, including
all-emails, all-websites, employee count, restrictions and the compliance verdict.
