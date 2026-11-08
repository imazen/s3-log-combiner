#!/usr/bin/env python3
"""
Join license telemetry (report.json) with Chargebee billing data
(chargebee_customers.json) into a single self-contained HTML page for
collections / outreach triage.

Usage:
    python tools/build_customer_report.py \
        --report parsed_test/report.json \
        --chargebee chargebee_customers.json \
        --out customer_report.html
"""
import argparse
import datetime as dt
import html
import json
import os
import re
import sys
import unicodedata

# ---------------------------------------------------------------- name matching

CORP_SUFFIXES = r"""\b(inc|llc|l\.l\.c|ltd|limited|gmbh|mbh|ab|a/s|as|aps|oy|bv|b\.v|nv|n\.v|
sa|s\.a|srl|s\.r\.l|sl|slu|sas|spa|plc|corp|corporation|co|company|group|holding|holdings|
pty|ehf|kg|ag|se|llp|lp|gbr|ug|sarl|sp|zoo|doo|dd|kft|ou|sia|uab|the)\b"""


MIN_KEY_LEN = 4


def norm_company(s, strip_suffixes=True):
    """Normalize a company name for joining: unescape, lowercase, fold accents,
    drop punctuation and (optionally) common corporate suffixes.

    Stripping suffixes is what lets "Acme Ltd" match "Acme, Inc." -- but on a name
    that is mostly suffix ("M+ Group BV") it can eat almost everything and leave a
    stub like "m" that would cross-match unrelated companies. Callers use
    `company_keys()` rather than this directly so that over-stripped stubs are
    discarded instead of matched.
    """
    if not s:
        return ""
    s = html.unescape(str(s))
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = s.lower()
    s = re.sub(r"[^a-z0-9]+", " ", s)
    if strip_suffixes:
        s = re.sub(CORP_SUFFIXES, " ", s, flags=re.VERBOSE)
    return re.sub(r"\s+", " ", s).strip()


def company_keys(name):
    """Join keys for a company name, most-specific first.

    Always includes the full normalized name; includes the suffix-stripped form
    only when it is still substantial enough to identify a company on its own.
    """
    keys = []
    full = norm_company(name, strip_suffixes=False)
    if full:
        keys.append(full)
    stripped = norm_company(name, strip_suffixes=True)
    if stripped and stripped != full and len(stripped) >= MIN_KEY_LEN:
        keys.append(stripped)
    return keys


def clean(s):
    """Un-escape stray HTML entities that leaked into license blobs."""
    if s is None:
        return None
    return html.unescape(str(s)).strip()


# ---------------------------------------------------------------- date helpers

def parse_iso(s):
    if not s:
        return None
    try:
        s = s.replace("Z", "+00:00")
        d = dt.datetime.fromisoformat(s)
        return d if d.tzinfo else d.replace(tzinfo=dt.timezone.utc)
    except (ValueError, TypeError):
        return None


def days_between(a, b):
    if not a or not b:
        return None
    return (b - a).days


def iso_week_start(week_key):
    """'2026-W07' -> date of that ISO week's Monday."""
    m = re.match(r"(\d{4})-W(\d{1,2})$", week_key or "")
    if not m:
        return None
    try:
        return dt.date.fromisocalendar(int(m.group(1)), int(m.group(2)), 1)
    except ValueError:
        return None


# ---------------------------------------------------------------- customer detail

# Card/payment-instrument details are deliberately excluded from the report.
# `card_status` survives because it is a state flag (valid/expired/no_card) that
# drives segmentation, not a card detail.
CARD_FIELDS = {
    "_card", "card", "payment_method", "primary_payment_source_id",
    "backup_payment_source_id", "payment_sources",
}
# Pure API plumbing that carries no business meaning.
PLUMBING_FIELDS = {
    "object", "resource_version", "deleted", "pii_cleared",
    "use_default_hierarchy_settings", "card_status",
    "mrr", "mrr_formatted", "email", "phone", "first_name", "last_name",
    "company", "id", "app_url", "billing_address", "contacts",
    "cf_backup_contacts", "cf_administrators", "cf_website", "cf_company_size",
    "auto_collection", "net_term_days", "preferred_currency_code",
}

FIELD_LABELS = {
    "cf_tax_id": "Tax ID",
    "cf_company_size": "Reported employees",
    "cf_website": "Website",
    "created_at_formatted": "Created",
    "updated_at_formatted": "Updated",
    "created_from_ip": "Signup IP",
    "taxability": "Taxability",
    "allow_direct_debit": "Direct debit",
    "channel": "Channel",
    "locale": "Locale",
    "fraud_flag": "Fraud flag",
    "invoice_notes": "Invoice notes",
    "excess_payments_formatted": "Excess payments",
    "promotional_credits_formatted": "Promotional credits",
    "refundable_credits_formatted": "Refundable credits",
    "unbilled_charges_formatted": "Unbilled charges",
    "vat_number": "VAT number",
    "entity_code": "Entity code",
    "business_entity_id": "Business entity",
    "billing_date": "Billing date",
    "balances": "Balances",
    "meta_data": "Metadata",
}

SPLIT_EMAILS = re.compile(r"[,;\n]+")


def emails_from(text):
    """Pull addresses out of a free-text contact field like
    'baruch@x.com (manager)  yael@x.com (Accounts)'."""
    if not text:
        return []
    found = re.findall(r"[\w.+-]+@[\w-]+\.[\w.-]+", str(text))
    return [e.strip(" .,;") for e in found]


def extra_contacts(cust):
    """Every additional address on the account, deduped, with where it came from."""
    if not cust:
        return []
    out = []
    seen = set()

    def add(email, role, extra=None):
        key = (email or "").lower()
        if not key or key in seen:
            return
        seen.add(key)
        out.append({"email": email, "role": role, "note": extra or ""})

    add((cust.get("email") or "").strip(), "Primary")
    ba = cust.get("billing_address") or {}
    add((ba.get("email") or "").strip(), "Billing")

    for c in cust.get("contacts") or []:
        flags = []
        if c.get("send_account_email"):
            flags.append("account email")
        if c.get("send_billing_email"):
            flags.append("billing email")
        if c.get("enabled") is False:
            flags.append("disabled")
        add((c.get("email") or "").strip(), "Contact", ", ".join(flags))

    for e in emails_from(cust.get("cf_backup_contacts")):
        add(e, "Backup")
    for e in emails_from(cust.get("cf_administrators")):
        add(e, "Administrator")
    return out


def websites_for(cust, domains, product_domains=None):
    """Best-effort website list: the declared site, the email domain, and the
    busiest domains actually seen serving images."""
    out = []
    seen = set()

    def add(url, source):
        host = re.sub(r"^https?://", "", (url or "").strip()).strip("/").lower()
        if not host or host in seen or "@" in host:
            return
        seen.add(host)
        out.append({"host": host, "source": source})

    if cust:
        add(cust.get("cf_website"), "declared")
        email = cust.get("email") or ""
        if "@" in email:
            dom = email.split("@")[-1].strip().lower()
            # Skip consumer mail providers -- they say nothing about the company.
            if dom not in FREEMAIL:
                add(dom, "email domain")
    for d in (domains or [])[:12]:
        add(d.get("d"), "serving images")
    return out


FREEMAIL = {
    "gmail.com", "googlemail.com", "yahoo.com", "hotmail.com", "outlook.com",
    "live.com", "icloud.com", "me.com", "aol.com", "protonmail.com", "gmx.de",
    "gmx.net", "web.de", "mail.ru", "yandex.ru", "qq.com", "163.com",
}

# Employee caps are written both ways round:
#   "less than 30 employees"      -> number before the noun
#   "employee count cannot exceed 4" -> number after it
EMPLOYEE_LIMIT_BEFORE = re.compile(
    r"(?:less than|fewer than|cannot exceed|no more than|not exceed|under|up to)\s+"
    r"(\d[\d,]*)\s*(?:\w+\s+){0,3}?employee", re.I)
EMPLOYEE_LIMIT_AFTER = re.compile(
    r"employee\s*(?:count|headcount|s)?\s*"
    r"(?:cannot|can not|must not|may not|shall not)?\s*"
    r"(?:exceed|be (?:more|greater) than|be over|is limited to|limited to)\s+(\d[\d,]*)",
    re.I)
REVENUE_LIMIT = re.compile(
    r"revenue\s+(?:cannot|can not|must not|may not)?\s*exceed\s+(\$?[\d.,]+\s*[A-Za-z$]*)",
    re.I)


def _employee_limit(text):
    for rx in (EMPLOYEE_LIMIT_BEFORE, EMPLOYEE_LIMIT_AFTER):
        m = rx.search(text)
        if m:
            try:
                return int(m.group(1).replace(",", ""))
            except ValueError:
                continue
    return None


def compliance_flags(restrictions, company_size):
    """Compare the license's contractual limits against the reported headcount.

    Returns a list of {level, text}. Only flags what the data actually supports --
    a missing headcount is reported as unknown, not as a violation.
    """
    flags = []
    if not restrictions:
        return flags
    limit = _employee_limit(restrictions)
    if limit is not None:
        if company_size is None:
            flags.append({"level": "unknown",
                          "text": "Capped at %d employees; headcount not reported" % limit})
        elif company_size > limit:
            flags.append({"level": "critical",
                          "text": "Reported %d employees exceeds the %d-employee cap"
                                  % (company_size, limit)})
        else:
            flags.append({"level": "good",
                          "text": "Reported %d employees is within the %d-employee cap"
                                  % (company_size, limit)})
    rev = REVENUE_LIMIT.search(restrictions)
    if rev:
        flags.append({"level": "unknown",
                      "text": "Revenue capped at %s - not verifiable from billing data"
                              % rev.group(1).strip()})
    if re.search(r"non-?profit", restrictions, re.I):
        flags.append({"level": "unknown", "text": "Non-profit only - verify eligibility"})
    if re.search(r"agplv3", restrictions, re.I):
        flags.append({"level": "unknown", "text": "AGPLv3 only - no commercial grant"})
    if re.search(r"not for usage resale|resale", restrictions, re.I):
        flags.append({"level": "unknown", "text": "No usage resale"})
    return flags


def format_address(cust):
    """Single-line postal address from the Chargebee billing address."""
    if not cust:
        return ""
    a = cust.get("billing_address") or {}
    parts = [a.get("line1"), a.get("line2"), a.get("city"),
             a.get("state") or a.get("state_code"), a.get("zip"), a.get("country")]
    return ", ".join(p for p in parts if p)

def sanitize_customer(cust):
    """Everything on the Chargebee record except card/payment-instrument details
    and API plumbing, as ordered (label, value) pairs ready to render."""
    if not cust:
        return []
    rows = []
    for key in sorted(cust.keys()):
        if key in CARD_FIELDS or key in PLUMBING_FIELDS:
            continue
        val = cust[key]
        if val in (None, "", [], {}):
            continue
        # Chargebee ships raw + "_formatted" twins (0 / "USD 0.00"). Show the
        # readable one only.
        if key + "_formatted" in cust:
            continue
        if key.endswith("_at") and isinstance(val, int):
            continue  # the *_formatted twin is shown instead
        label = FIELD_LABELS.get(key, key.replace("cf_", "").replace("_", " ").capitalize())
        if isinstance(val, bool):
            val = "yes" if val else "no"
        elif isinstance(val, (dict, list)):
            val = json.dumps(val, ensure_ascii=False)
        rows.append({"k": label, "v": str(val)})
    return rows


# ---------------------------------------------------------------- classification

def classify_activity(lic, window_end, recent_days=30):
    """How alive is this install?

    active   - completed jobs recently
    idle     - phoning home recently, but no jobs (installed, not serving traffic)
    fading   - jobs in the window, but nothing recent
    silent   - hasn't checked in at all recently
    """
    last = parse_iso(lic.get("last_activity"))
    since = days_between(last, window_end)
    peak_machines = lic.get("peak_machines", 0)
    live_jobs = lic.get("live_jobs", 0)
    total_jobs = lic.get("total_jobs", 0) or 0

    if since is None:
        return "silent", None
    if since > recent_days * 2:
        return "silent", since
    if peak_machines > 0 and live_jobs > 0:
        return "active", since
    if total_jobs > 0:
        return "fading", since
    return "idle", since


def classify_payment(cust):
    """Payment health from the billing signals the MCP endpoint exposes.

    NOTE: this is a proxy. `data_lookup_agent` has no invoice tools, so we cannot
    see true invoice status. Card state + MRR is the strongest available stand-in.
    """
    if not cust:
        return "unmatched", "No Chargebee customer matched"
    status = (cust.get("card_status") or "").lower()
    mrr = cust.get("mrr", 0) or 0
    auto = (cust.get("auto_collection") or "").lower()

    if status == "expired":
        return "critical", "Card expired"
    if status == "no_card":
        if auto == "off":
            return "manual", "No card on file, invoiced manually"
        return "critical", "No card on file, auto-collect on"
    if status == "expiring":
        return "warning", "Card expiring soon"
    if status == "valid":
        return "good", "Card valid"
    return "unknown", "Card status: %s" % (status or "unknown")


def outreach_segment(activity, payment, license_status, mrr):
    """The cross-product that decides how you talk to them."""
    paying = (mrr or 0) > 0
    broken = payment in ("critical", "warning")
    live = activity == "active"

    if license_status in ("revoked",) and live:
        return "using_after_revoke"
    if license_status in ("if_expired", "r_expired") and live:
        return "using_after_expiry"
    if broken and live:
        return "at_risk_active"
    if broken and not live:
        return "at_risk_inactive"
    if paying and activity in ("silent", "idle"):
        return "paying_unused"
    if live and payment == "good":
        return "healthy"
    return "other"


SEGMENTS = [
    ("using_after_revoke", "Revoked but still running",
     "License revoked for non-payment, yet actively serving jobs. Highest-priority contact.",
     "critical"),
    ("using_after_expiry", "Expired but still running",
     "License term lapsed while the install keeps working. Renewal conversation.",
     "serious"),
    ("at_risk_active", "Payment broken, actively using",
     "They depend on the product daily and their card will fail. Warn before service is disrupted.",
     "serious"),
    ("paying_unused", "Paying, not using",
     "Billing fine, product idle. Churn risk - re-engage or they cancel at renewal.",
     "warning"),
    ("at_risk_inactive", "Payment broken, not using",
     "Likely already gone. Low-touch collections or write-off.",
     "warning"),
    ("healthy", "Healthy",
     "Active usage, payment in good standing. No action needed.",
     "good"),
    ("other", "Everything else",
     "No MRR, no usage, or unmatched - review manually.",
     "neutral"),
]

ACTIVITY_LABELS = {
    "active": "Active",
    "fading": "Fading",
    "idle": "Idle",
    "silent": "Silent",
}

PAYMENT_LABELS = {
    "good": "Card valid",
    "warning": "Card expiring",
    "critical": "Payment broken",
    "manual": "Manual invoice",
    "unknown": "Unknown",
    "unmatched": "No CB match",
}


# ---------------------------------------------------------------- data assembly

def build_chargebee_index(cb_customers):
    """Map company-name keys -> customer. Where several customers share a key,
    keep the one with the highest MRR (the live account, not a stale duplicate)."""
    index = {}
    for c in cb_customers:
        names = {c.get("company"), (c.get("billing_address") or {}).get("company")}
        for name in names:
            for key in company_keys(name):
                prev = index.get(key)
                if prev is None or (c.get("mrr", 0) or 0) > (prev.get("mrr", 0) or 0):
                    index[key] = c
    return index


def lookup_customer(index, owner):
    """Try the exact normalized name first, then the suffix-stripped form."""
    for key in company_keys(owner):
        hit = index.get(key)
        if hit:
            return hit
    return None


def live_counts(lic, window_end, days):
    """Peak concurrent machines seen within `days` of the window end, and the
    lifetime job total attributable to machines last seen in that period.

    IMPORTANT: the telemetry records only each machine's LAST report, and
    `jobs_completed_total` is that machine's lifetime counter. So the daily and
    weekly buckets are a "when was this machine last seen" histogram, not a
    time series of work done. Summing a bucket therefore yields lifetime jobs
    for the machines last seen in it -- which is a fair measure of "how much
    work is being done by installs that are still running", but is NOT
    "jobs completed during that period". Labels in the report say so.
    """
    cutoff = (window_end - dt.timedelta(days=days)).date()
    peak = 0
    jobs = 0
    for d in lic.get("daily_machine_counts", []) or []:
        try:
            day = dt.date.fromisoformat(d.get("date", ""))
        except (ValueError, TypeError):
            continue
        if day >= cutoff:
            # Machine counts are per-day SETS, so a machine recurs across days --
            # summing them would count machine-days. Peak concurrency is the only
            # aggregate that stays a machine count.
            peak = max(peak, d.get("concurrent_machines", 0) or 0)
            # Job counters are contributed once per reporting process, on the day
            # of its last report, so these ARE additive.
            jobs += d.get("jobs", 0) or 0
    return peak, jobs


def weekly_series(lic):
    """Weekly job counts sorted chronologically, for the sparkline."""
    rows = []
    for w in lic.get("weekly_data", []) or []:
        start = iso_week_start(w.get("week"))
        if start:
            rows.append((start, w.get("jobs_completed", 0) or 0,
                         w.get("unique_machines", 0) or 0))
    rows.sort(key=lambda r: r[0])
    return rows


def assemble(report, cb_customers, recent_days):
    index = build_chargebee_index(cb_customers)
    licenses = report.get("licenses", [])

    # The data window ends at the latest observed activity, not "now" -- logs lag.
    ends = [parse_iso(l.get("last_activity")) for l in licenses]
    ends = [e for e in ends if e]
    window_end = max(ends) if ends else dt.datetime.now(dt.timezone.utc)
    starts = [parse_iso(l.get("first_activity")) for l in licenses]
    starts = [s for s in starts if s]
    window_start = min(starts) if starts else None

    matched_ids = set()
    rows = []
    for lic in licenses:
        owner = clean(lic.get("owner")) or "(unknown owner)"
        cust = lookup_customer(index, owner)
        if cust:
            matched_ids.add(cust.get("id"))

        lic = dict(lic)
        lic["peak_machines"], lic["live_jobs"] = live_counts(lic, window_end, recent_days)
        activity, days_since = classify_activity(lic, window_end, recent_days)
        payment, payment_note = classify_payment(cust)
        mrr = (cust.get("mrr", 0) or 0) if cust else 0
        status = lic.get("license_status") or "unknown"
        segment = outreach_segment(activity, payment, status, mrr)

        weeks = weekly_series(lic)
        domains = sorted(
            lic.get("domains", []) or [],
            key=lambda d: d.get("job_count", 0) or 0,
            reverse=True,
        )

        rows.append({
            "license_id": lic.get("license_id"),
            "owner": owner,
            "product": clean(lic.get("product")) or "",
            "kind": clean(lic.get("kind")) or "",
            "features": lic.get("features") or [],
            "license_status": status,
            "status_message": clean(lic.get("status_message")) or "",
            "restrictions": clean(lic.get("restrictions")) or "",
            "total_cores": clean(lic.get("total_cores")) or "",
            "max_servers": clean(lic.get("max_servers")) or "",
            "is_public": clean(lic.get("is_public")) or "",
            "licensed_domains": clean(lic.get("licensed_domains")) or "",
            "expires": lic.get("expires"),
            "imageflow_expires": lic.get("imageflow_expires"),
            "issued": lic.get("issued"),
            "total_jobs": lic.get("total_jobs", 0) or 0,
            "peak_machines": lic["peak_machines"],
            "live_jobs": lic["live_jobs"],
            "machines": lic.get("unique_machines", 0) or 0,
            "ips": lic.get("unique_ips", 0) or 0,
            "imageflow_jobs": (lic.get("imageflow") or {}).get("jobs", 0) or 0,
            "imageresizer_jobs": (lic.get("imageresizer") or {}).get("jobs", 0) or 0,
            "job_trend": lic.get("job_trend") or "",
            "last_activity": lic.get("last_activity"),
            "days_since": days_since,
            "activity": activity,
            "payment": payment,
            "payment_note": payment_note,
            "segment": segment,
            "spark": [w[2] for w in weeks],
            "spark_weeks": [w[0].isoformat() for w in weeks],
            "domains": [
                {"d": d.get("domain"), "j": d.get("job_count", 0) or 0,
                 "if": bool(d.get("uses_imageflow")), "ir": bool(d.get("uses_imageresizer"))}
                for d in domains[:40]
            ],
            "domain_total": len(domains),
            # Chargebee
            "cb_id": cust.get("id") if cust else None,
            "cb_company": clean(cust.get("company")) if cust else None,
            "cb_email": (cust.get("email") if cust else None) or "",
            "cb_name": (" ".join(filter(None, [
                cust.get("first_name"), cust.get("last_name")])) if cust else ""),
            "cb_phone": (cust.get("phone") if cust else "") or "",
            "cb_url": cust.get("app_url") if cust else None,
            "employees": (cust.get("cf_company_size") if cust else None),
            "address": format_address(cust),
            "country": ((cust.get("billing_address") or {}).get("country")
                        if cust else "") or "",
            "contacts": extra_contacts(cust),
            "websites": websites_for(cust, [
                {"d": d.get("domain")} for d in domains]),
            "cb_fields": sanitize_customer(cust),
            "compliance": compliance_flags(
                clean(lic.get("restrictions")),
                cust.get("cf_company_size") if cust else None),
            "mrr": mrr,
            "currency": (cust.get("preferred_currency_code") if cust else "USD") or "USD",
            "card_status": (cust.get("card_status") if cust else "") or "",
            "auto_collection": (cust.get("auto_collection") if cust else "") or "",
            "net_term_days": (cust.get("net_term_days") if cust else 0) or 0,
            "unbilled": (cust.get("unbilled_charges") if cust else 0) or 0,
            "credits": (cust.get("promotional_credits") if cust else 0) or 0,
        })

    # Paying Chargebee customers with no license telemetry at all -- they are
    # invisible in the logs, which is itself a churn signal worth surfacing.
    orphans = []
    for c in cb_customers:
        if c.get("id") in matched_ids:
            continue
        if (c.get("mrr", 0) or 0) <= 0:
            continue
        payment, payment_note = classify_payment(c)
        orphans.append({
            "cb_id": c.get("id"),
            "owner": clean(c.get("company")) or clean(c.get("email")) or c.get("id"),
            "cb_email": c.get("email") or "",
            "cb_name": " ".join(filter(None, [c.get("first_name"), c.get("last_name")])),
            "cb_phone": c.get("phone") or "",
            "cb_url": c.get("app_url"),
            "mrr": c.get("mrr", 0) or 0,
            "currency": c.get("preferred_currency_code") or "USD",
            "card_status": c.get("card_status") or "",
            "payment": payment,
            "payment_note": payment_note,
            "employees": c.get("cf_company_size"),
            "address": format_address(c),
            "country": (c.get("billing_address") or {}).get("country") or "",
            "contacts": extra_contacts(c),
            "websites": websites_for(c, []),
        })
    orphans.sort(key=lambda r: -r["mrr"])

    return {
        "rows": rows,
        "orphans": orphans,
        "window_start": window_start.isoformat() if window_start else None,
        "window_end": window_end.isoformat() if window_end else None,
        "recent_days": recent_days,
    }


# ---------------------------------------------------------------- HTML render

CSS = """
*,*::before,*::after{box-sizing:border-box}
:root{
  color-scheme:light;
  --plane:#f9f9f7; --surface:#fcfcfb; --surface-2:#f2f1ed;
  --ink:#0b0b0b; --ink-2:#52514e; --muted:#898781;
  --grid:#e1e0d9; --axis:#c3c2b7; --border:rgba(11,11,11,0.10);
  --good:#0ca30c; --warning:#fab219; --serious:#ec835a; --critical:#d03b3b;
  --s1:#2a78d6; --s2:#eb6834; --s3:#1baf7a; --link:#256abf;
  --good-ink:#006300; --critical-ink:#a92c2c; --serious-ink:#a4522c; --warning-ink:#8a6000;
  --shadow:0 1px 2px rgba(11,11,11,.05),0 4px 12px rgba(11,11,11,.04);
}
@media (prefers-color-scheme:dark){
  :root:not([data-theme="light"]){
    color-scheme:dark;
    --plane:#0d0d0d; --surface:#1a1a19; --surface-2:#232322;
    --ink:#fff; --ink-2:#c3c2b7; --muted:#898781;
    --grid:#2c2c2a; --axis:#383835; --border:rgba(255,255,255,0.10);
    --s1:#3987e5; --s2:#d95926; --s3:#199e70; --link:#3987e5;
    --good-ink:#0ca30c; --critical-ink:#e88080; --serious-ink:#ec835a; --warning-ink:#fab219;
    --shadow:0 1px 2px rgba(0,0,0,.4),0 4px 12px rgba(0,0,0,.3);
  }
}
:root[data-theme="dark"]{
  color-scheme:dark;
  --plane:#0d0d0d; --surface:#1a1a19; --surface-2:#232322;
  --ink:#fff; --ink-2:#c3c2b7; --muted:#898781;
  --grid:#2c2c2a; --axis:#383835; --border:rgba(255,255,255,0.10);
  --s1:#3987e5; --s2:#d95926; --s3:#199e70; --link:#3987e5;
  --good-ink:#0ca30c; --critical-ink:#e88080; --serious-ink:#ec835a; --warning-ink:#fab219;
  --shadow:0 1px 2px rgba(0,0,0,.4),0 4px 12px rgba(0,0,0,.3);
}
body{margin:0;background:var(--plane);color:var(--ink);
  font:14px/1.5 system-ui,-apple-system,"Segoe UI",sans-serif;
  -webkit-font-smoothing:antialiased}
.wrap{max-width:1580px;margin:0 auto;padding:28px 22px 80px}
h1{font-size:22px;font-weight:650;margin:0 0 4px;letter-spacing:-.01em}
h2{font-size:15px;font-weight:650;margin:34px 0 12px;letter-spacing:-.005em}
.sub{color:var(--ink-2);font-size:13px;margin:0}
.sub b{color:var(--ink);font-weight:600}
header.top{display:flex;justify-content:space-between;align-items:flex-start;gap:20px;flex-wrap:wrap}
.themebtn{background:var(--surface);border:1px solid var(--border);color:var(--ink-2);
  border-radius:8px;padding:7px 12px;font:inherit;font-size:12.5px;cursor:pointer}
.themebtn:hover{color:var(--ink);border-color:var(--axis)}

.tiles{display:grid;grid-template-columns:repeat(auto-fit,minmax(178px,1fr));gap:12px;margin-top:20px}
.tile{background:var(--surface);border:1px solid var(--border);border-radius:11px;
  padding:14px 16px;box-shadow:var(--shadow)}
.tile .lab{font-size:11.5px;color:var(--muted);text-transform:uppercase;
  letter-spacing:.055em;font-weight:600}
.tile .val{font-size:26px;font-weight:650;margin-top:5px;letter-spacing:-.02em}
.tile .note2{font-size:11.5px;color:var(--ink-2);margin-top:3px}
.tile.crit .val{color:var(--critical-ink)}
.tile.ser .val{color:var(--serious-ink)}
.tile.warn .val{color:var(--warning-ink)}
.tile.good .val{color:var(--good-ink)}

.segs{display:grid;grid-template-columns:repeat(auto-fit,minmax(252px,1fr));gap:12px}
.seg{background:var(--surface);border:1px solid var(--border);border-left:4px solid var(--axis);
  border-radius:11px;padding:13px 15px;cursor:pointer;text-align:left;font:inherit;
  color:var(--ink);box-shadow:var(--shadow);transition:transform .09s}
.seg:hover{transform:translateY(-1px)}
.seg[aria-pressed="true"]{outline:2px solid var(--link);outline-offset:1px}
.seg.critical{border-left-color:var(--critical)}
.seg.serious{border-left-color:var(--serious)}
.seg.warning{border-left-color:var(--warning)}
.seg.good{border-left-color:var(--good)}
.seg.neutral{border-left-color:var(--axis)}
.seg .h{display:flex;justify-content:space-between;align-items:baseline;gap:10px}
.seg .n{font-weight:650;font-size:13.5px}
.seg .c{font-size:20px;font-weight:650;letter-spacing:-.02em;font-variant-numeric:tabular-nums}
.seg .d{font-size:11.5px;color:var(--ink-2);margin-top:5px;line-height:1.42}
.seg .m{font-size:11.5px;color:var(--muted);margin-top:6px;font-variant-numeric:tabular-nums}

.bar{display:flex;gap:9px;flex-wrap:wrap;align-items:center;margin:14px 0 10px;
  padding:12px 14px;background:var(--surface);border:1px solid var(--border);
  border-radius:11px;box-shadow:var(--shadow)}
.bar input[type=search],.bar select{background:var(--plane);color:var(--ink);
  border:1px solid var(--border);border-radius:8px;padding:7px 10px;font:inherit;font-size:13px}
.bar input[type=search]{min-width:250px;flex:1}
.bar label{font-size:11.5px;color:var(--muted);font-weight:600;
  text-transform:uppercase;letter-spacing:.05em}
.bar .grp{display:flex;align-items:center;gap:6px}
.bar button{background:var(--plane);border:1px solid var(--border);color:var(--ink-2);
  border-radius:8px;padding:7px 12px;font:inherit;font-size:12.5px;cursor:pointer}
.bar button:hover{color:var(--ink);border-color:var(--axis)}
.count{margin-left:auto;font-size:12.5px;color:var(--ink-2);font-variant-numeric:tabular-nums}

.tblwrap{overflow-x:auto;background:var(--surface);border:1px solid var(--border);
  border-radius:11px;box-shadow:var(--shadow)}
table{border-collapse:collapse;width:100%;font-size:13px}
th{text-align:left;font-size:11px;text-transform:uppercase;letter-spacing:.055em;
  color:var(--muted);font-weight:650;padding:10px 11px;border-bottom:1px solid var(--grid);
  white-space:nowrap;cursor:pointer;user-select:none;position:sticky;top:0;
  background:var(--surface);z-index:2}
th:hover{color:var(--ink-2)}
th.num,td.num{text-align:right;font-variant-numeric:tabular-nums}
th .ar{opacity:.45;font-size:9px;margin-left:3px}
td{padding:9px 11px;border-bottom:1px solid var(--grid);vertical-align:middle}
tr.row{cursor:pointer}
tr.row:hover>td{background:var(--surface-2)}
.owner{font-weight:600;max-width:230px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.lid{font-size:11.5px;color:var(--muted);font-variant-numeric:tabular-nums;margin-top:1px}
.plan{max-width:215px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;
  color:var(--ink-2);font-size:12.5px}
.pill{display:inline-flex;align-items:center;gap:4px;padding:2px 8px;border-radius:99px;
  font-size:11.5px;font-weight:600;white-space:nowrap;border:1px solid transparent}
.pill .dot{width:6px;height:6px;border-radius:50%;flex:none}
.p-good{color:var(--good-ink);background:color-mix(in srgb,var(--good) 11%,transparent);
  border-color:color-mix(in srgb,var(--good) 30%,transparent)}
.p-good .dot{background:var(--good)}
.p-warning{color:var(--warning-ink);background:color-mix(in srgb,var(--warning) 15%,transparent);
  border-color:color-mix(in srgb,var(--warning) 38%,transparent)}
.p-warning .dot{background:var(--warning)}
.p-serious{color:var(--serious-ink);background:color-mix(in srgb,var(--serious) 14%,transparent);
  border-color:color-mix(in srgb,var(--serious) 35%,transparent)}
.p-serious .dot{background:var(--serious)}
.p-critical{color:var(--critical-ink);background:color-mix(in srgb,var(--critical) 12%,transparent);
  border-color:color-mix(in srgb,var(--critical) 32%,transparent)}
.p-critical .dot{background:var(--critical)}
.p-neutral{color:var(--ink-2);background:var(--surface-2);border-color:var(--border)}
.p-neutral .dot{background:var(--muted)}
svg.spark{display:block}
.detail>td{background:var(--surface-2);padding:0}
.dinner{padding:16px 18px 6px;display:grid;
  grid-template-columns:repeat(auto-fit,minmax(272px,1fr));gap:20px 26px}
.dsec h4{margin:0 0 8px;font-size:11px;text-transform:uppercase;letter-spacing:.055em;
  color:var(--muted);font-weight:650}
.kv{display:grid;grid-template-columns:auto 1fr;gap:4px 12px;font-size:12.5px;margin:0}
.kv dt{color:var(--muted)}
.kv dd{margin:0;color:var(--ink)}
.dsec.wide{grid-column:1/-1}
.none{color:var(--muted);margin:0;font-size:12.5px}
table.mini{border-collapse:collapse;width:100%;font-size:12.5px}
table.mini td{padding:2.5px 0;border-bottom:1px solid var(--grid);vertical-align:top}
table.mini td:last-child{text-align:right;white-space:nowrap;padding-left:10px}
.role{display:inline-block;font-size:10.5px;text-transform:uppercase;
  letter-spacing:.05em;color:var(--muted);font-weight:650}
.restrict{font-size:12.5px;color:var(--ink-2);margin:10px 0 0;padding:9px 11px;
  background:var(--plane);border:1px solid var(--border);border-radius:8px;line-height:1.5}
.restrict b{color:var(--ink)}
.cflag{margin-top:7px}
.cardnote{margin:0;padding:0 18px 15px;font-size:11.5px;color:var(--muted)}
.kv dd{overflow-wrap:anywhere}
.dom{font-size:12.5px;max-height:210px;overflow-y:auto}
.dom div{display:flex;justify-content:space-between;gap:12px;padding:2.5px 0;
  border-bottom:1px solid var(--grid)}
.dom span:last-child{color:var(--ink-2);font-variant-numeric:tabular-nums;flex:none}
a{color:var(--link)}
a:hover{text-decoration:underline}
.tag{display:inline-block;padding:1.5px 7px;border-radius:5px;background:var(--surface);
  border:1px solid var(--border);font-size:11.5px;margin:0 4px 4px 0;color:var(--ink-2)}
.empty{padding:40px;text-align:center;color:var(--muted)}
.callout{font-size:12.5px;color:var(--ink-2);background:var(--surface);padding:13px 15px;
  border:1px solid var(--border);border-left:3px solid var(--warning);
  border-radius:9px;margin:12px 0 0;line-height:1.55}
.callout b{color:var(--ink)}
footer{margin-top:34px;font-size:12px;color:var(--muted);line-height:1.6}
"""


JS = r"""
const $ = s => document.querySelector(s);
const fmt = n => (n||0).toLocaleString();
const esc = s => String(s==null?'':s).replace(/[&<>"']/g, c =>
  ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));

function money(cents, cur){
  if(!cents) return '—';
  const v = cents/100;
  try { return new Intl.NumberFormat(undefined,
    {style:'currency',currency:cur||'USD',maximumFractionDigits:0}).format(v); }
  catch(e){ return (cur||'USD')+' '+v.toFixed(0); }
}

function ago(iso){
  if(!iso) return '—';
  const d = new Date(iso);
  if(isNaN(d)) return '—';
  const days = Math.floor((WINDOW_END - d)/86400000);
  if(days <= 0) return 'today';
  if(days === 1) return '1 day';
  if(days < 60) return days + ' days';
  const m = Math.round(days/30.44);
  return m < 24 ? m + ' mo' : (days/365.25).toFixed(1) + ' yr';
}

/* Sparkline: weekly jobs. Single series, so no legend -- the column head names it.
   Area under a 2px line; the last point gets a marker so the end reads clearly. */
function spark(vals, tone){
  const W=76, H=22, P=2;
  if(!vals || vals.length===0) return '<svg class="spark" width="'+W+'" height="'+H+'"></svg>';
  if(vals.length===1) vals=[0].concat(vals);
  const max = Math.max.apply(null, vals) || 1;
  const dx = (W-P*2)/(vals.length-1);
  const pts = vals.map((v,i)=>[P+i*dx, H-P-(v/max)*(H-P*2)]);
  const line = pts.map((p,i)=>(i?'L':'M')+p[0].toFixed(1)+' '+p[1].toFixed(1)).join(' ');
  const area = line+' L'+(W-P).toFixed(1)+' '+(H-P)+' L'+P+' '+(H-P)+' Z';
  const c = tone==='dead' ? 'var(--muted)' : 'var(--s1)';
  const last = pts[pts.length-1];
  return '<svg class="spark" width="'+W+'" height="'+H+'" viewBox="0 0 '+W+' '+H+
    '" aria-hidden="true">'+
    '<path d="'+area+'" fill="'+c+'" opacity=".13"/>'+
    '<path d="'+line+'" fill="none" stroke="'+c+'" stroke-width="2" '+
    'stroke-linejoin="round" stroke-linecap="round"/>'+
    '<circle cx="'+last[0].toFixed(1)+'" cy="'+last[1].toFixed(1)+'" r="2.2" fill="'+c+'"/>'+
    '</svg>';
}

const ACT_TONE = {active:'good', fading:'warning', idle:'neutral', silent:'neutral'};
const PAY_TONE = {good:'good', warning:'warning', critical:'critical',
                  manual:'neutral', unknown:'neutral', unmatched:'neutral'};
const LIC_TONE = {active:'good', if_expired:'serious', r_expired:'serious',
                  revoked:'critical', corrupted:'critical', unknown:'neutral'};
const LIC_LABEL = {active:'Active', if_expired:'Imageflow expired',
                   r_expired:'Resizer expired', revoked:'Revoked',
                   corrupted:'Corrupted', unknown:'Unknown'};

const pill = (tone,label) =>
  '<span class="pill p-'+tone+'"><span class="dot"></span>'+esc(label)+'</span>';

/* ------------------------------------------------------------------ state */
let sortKey='mrr', sortDir=-1, expanded=new Set();
const state = {q:'', segment:'', activity:'', payment:'', license:''};

function passes(r){
  if(state.segment && r.segment!==state.segment) return false;
  if(state.activity && r.activity!==state.activity) return false;
  if(state.payment && r.payment!==state.payment) return false;
  if(state.license && r.license_status!==state.license) return false;
  if(state.q){
    const q=state.q.toLowerCase();
    const hay=[r.owner,r.license_id,r.product,r.cb_email,r.cb_company,r.cb_id,r.cb_name]
      .concat((r.domains||[]).map(d=>d.d)).join(' ').toLowerCase();
    if(hay.indexOf(q)<0) return false;
  }
  return true;
}

function sorted(rows){
  const k=sortKey;
  return rows.slice().sort((a,b)=>{
    let x=a[k], y=b[k];
    if(k==='last_activity'){ x=x?Date.parse(x):0; y=y?Date.parse(y):0; }
    if(typeof x==='string'||typeof y==='string'){
      x=(x||'').toString().toLowerCase(); y=(y||'').toString().toLowerCase();
      return x<y?-sortDir:x>y?sortDir:0;
    }
    return ((x||0)-(y||0))*sortDir;
  });
}

function render(){
  const rows = sorted(DATA.filter(passes));
  const tb = $('#tb');
  if(!rows.length){
    tb.innerHTML='<tr><td colspan="12" class="empty">No licenses match these filters.</td></tr>';
  } else {
    tb.innerHTML = rows.map(r=>{
      const open = expanded.has(r.license_id);
      const tone = (r.activity==='silent'||r.activity==='idle')?'dead':'live';
      return '<tr class="row" data-id="'+esc(r.license_id)+'">'+
        '<td><div class="owner" title="'+esc(r.owner)+'">'+esc(r.owner)+'</div>'+
          '<div class="lid">'+esc(r.license_id)+'</div></td>'+
        '<td><div class="plan" title="'+esc(r.product||'')+'">'+
          esc(r.product||'—')+'</div></td>'+
        '<td>'+pill(LIC_TONE[r.license_status]||'neutral',
          LIC_LABEL[r.license_status]||r.license_status)+'</td>'+
        '<td>'+pill(ACT_TONE[r.activity]||'neutral', ACT_LABELS[r.activity])+'</td>'+
        '<td class="num">'+ago(r.last_activity)+'</td>'+
        '<td class="num">'+fmt(r.peak_machines)+'</td>'+
        '<td class="num">'+fmt(r.machines)+'</td>'+
        '<td class="num">'+fmt(r.total_jobs)+'</td>'+
        '<td>'+spark(r.spark, tone)+'</td>'+
        '<td class="num"><span class="mrr'+(r.mrr?'':' zero')+'">'+
          money(r.mrr,r.currency)+'</span></td>'+
        '<td>'+pill(PAY_TONE[r.payment]||'neutral', PAY_LABELS[r.payment])+'</td>'+
        '<td style="color:var(--muted)">'+(open?'▾':'▸')+'</td>'+
      '</tr>'+ (open ? detailRow(r) : '');
    }).join('');
  }
  $('#count').textContent = rows.length+' of '+DATA.length+' licenses · '+
    money(rows.reduce((s,r)=>s+(r.mrr||0),0),'USD')+' MRR shown';
  document.querySelectorAll('th[data-k]').forEach(th=>{
    const a=th.querySelector('.ar');
    if(a) a.textContent = th.dataset.k===sortKey ? (sortDir>0?'▲':'▼') : '';
  });
  document.querySelectorAll('.seg').forEach(b=>
    b.setAttribute('aria-pressed', String(b.dataset.seg===state.segment)));
}

function kv(pairs){
  const rows = pairs.filter(p => p[1] !== undefined && p[1] !== null && p[1] !== '')
    .map(p => '<dt>'+p[0]+'</dt><dd>'+p[1]+'</dd>').join('');
  return rows ? '<dl class="kv">'+rows+'</dl>'
              : '<p class="none">Nothing recorded</p>';
}
const mailto = e => '<a href="mailto:'+esc(e)+'">'+esc(e)+'</a>';
const tel = p => '<a href="tel:'+esc(String(p).replace(/[^\d+]/g,''))+'">'+esc(p)+'</a>';
const dateOnly = v => v ? esc(String(v).slice(0,10)) : '';

const COMPLIANCE_TONE = {critical:'critical', good:'good', unknown:'warning'};

function detailRow(r){
  /* --- contacts ------------------------------------------------------- */
  const contacts = (r.contacts||[]).length
    ? '<table class="mini"><tbody>'+(r.contacts||[]).map(c =>
        '<tr><td>'+mailto(c.email)+'</td><td class="role">'+esc(c.role)+
        (c.note?' <span class="none">('+esc(c.note)+')</span>':'')+'</td></tr>').join('')+
      '</tbody></table>'
    : '<p class="none">No email addresses on the account</p>';

  /* --- websites ------------------------------------------------------- */
  const sites = (r.websites||[]).length
    ? '<table class="mini"><tbody>'+(r.websites||[]).map(w =>
        '<tr><td><a href="https://'+esc(w.host)+'" target="_blank" rel="noopener">'+
        esc(w.host)+'</a></td><td class="role">'+esc(w.source)+'</td></tr>').join('')+
      '</tbody></table>'
    : '<p class="none">No website known</p>';

  /* --- compliance ----------------------------------------------------- */
  const comp = (r.compliance||[]).length
    ? (r.compliance||[]).map(f =>
        '<div class="cflag">'+pill(COMPLIANCE_TONE[f.level]||'neutral', f.text)+'</div>').join('')
    : '';

  /* --- license -------------------------------------------------------- */
  const feats = (r.features||[]).map(f=>'<span class="tag">'+esc(f)+'</span>').join('')
    || '<span class="none">none</span>';
  const license = kv([
    ['License ID', esc(r.license_id)],
    ['Owner', esc(r.owner)],
    ['Plan', esc(r.product||'')],
    ['Kind', esc(r.kind||'')],
    ['Issued', dateOnly(r.issued)],
    ['Expires', dateOnly(r.expires)],
    ['Imageflow expires', dateOnly(r.imageflow_expires)],
    ['Total cores', esc(r.total_cores||'')],
    ['Max servers', esc(r.max_servers||'')],
    ['Licensed domains', esc(r.licensed_domains||'')],
    ['Public', esc(r.is_public||'')],
  ]) + '<div style="margin-top:9px">'+feats+'</div>' +
  (r.restrictions ? '<p class="restrict"><b>Restrictions.</b> '+esc(r.restrictions)+'</p>' : '') +
  comp +
  (r.status_message ? '<p class="callout" style="margin-top:10px">'+esc(r.status_message)+'</p>' : '');

  /* --- company / billing ---------------------------------------------- */
  const company = kv([
    ['Company', esc(r.cb_company||r.owner)],
    ['Reported employees', r.employees === null || r.employees === undefined
        ? '<span class="none">not reported</span>' : fmt(r.employees)],
    ['Contact', esc(r.cb_name||'')],
    ['Phone', r.cb_phone ? tel(r.cb_phone) : ''],
    ['Address', esc(r.address||'')],
    ['Country', esc(r.country||'')],
  ]);

  const billing = r.cb_id ? kv([
    ['Chargebee ID', esc(r.cb_id)],
    ['MRR', money(r.mrr, r.currency)],
    ['Currency', esc(r.currency||'')],
    ['Payment state', esc(PAY_LABELS[r.payment]||'')+
       ' <span class="none">('+esc(r.payment_note||'')+')</span>'],
    ['Card status', esc(r.card_status||'')],
    ['Auto-collect', esc(r.auto_collection||'')],
    ['Net terms', (r.net_term_days||0)+' days'],
    ['Unbilled', r.unbilled ? money(r.unbilled, r.currency) : ''],
    ['Credits', r.credits ? money(r.credits, r.currency) : ''],
  ]) + (r.cb_url ? '<p style="margin:10px 0 0"><a href="'+esc(r.cb_url)+
      '" target="_blank" rel="noopener">Open in Chargebee &rarr;</a></p>' : '')
    : '<p class="none">No Chargebee customer matched &ldquo;'+esc(r.owner)+
      '&rdquo;. Search Chargebee by that name or by one of the domains below.</p>';

  /* --- remaining Chargebee fields ------------------------------------- */
  const rest = (r.cb_fields||[]).length
    ? kv((r.cb_fields||[]).map(f=>[esc(f.k), esc(f.v)]))
    : '<p class="none">Nothing further on the account</p>';

  /* --- usage & domains ------------------------------------------------ */
  const usage = kv([
    ['Peak machines', fmt(r.peak_machines)+
      ' <span class="none">(concurrent, last '+RECENT_DAYS+'d)</span>'],
    ['Machines seen', fmt(r.machines)],
    ['IPs', fmt(r.ips)],
    ['Jobs, lifetime', fmt(r.total_jobs)],
    ['&hellip; on live machines', fmt(r.live_jobs)],
    ['Imageflow', fmt(r.imageflow_jobs)],
    ['ImageResizer', fmt(r.imageresizer_jobs)],
    ['Last seen', dateOnly(r.last_activity)+' ('+ago(r.last_activity)+' ago)'],
  ]);

  const dom = (r.domains||[]).length
    ? (r.domains||[]).map(d=>{
        const h = String(d.d||'').trim();
        const name = h ? '<a href="https://'+esc(h)+'" target="_blank" rel="noopener">'+
              esc(h)+'</a>' : '<span class="none">(not reported)</span>';
        const which = [d['if']?'IF':'', d.ir?'IR':''].filter(Boolean).join('+');
        return '<div><span>'+name+'</span><span>'+(which?'<span class="role">'+which+
          '</span> ':'')+fmt(d.j)+'</span></div>';
      }).join('')
    : '<div class="none" style="border:0">No domains reported</div>';
  const more = r.domain_total > (r.domains||[]).length
    ? '<div class="none" style="border:0">+'+(r.domain_total-(r.domains||[]).length)+
      ' more</div>' : '';

  const sec = (title, body, wide) =>
    '<div class="dsec'+(wide?' wide':'')+'"><h4>'+title+'</h4>'+body+'</div>';

  return '<tr class="detail"><td colspan="12"><div class="dinner">'+
    sec('License &amp; entitlement', license) +
    sec('Company', company) +
    sec('Contacts ('+(r.contacts||[]).length+')', contacts) +
    sec('Websites', sites) +
    sec('Billing', billing) +
    sec('Usage', usage) +
    sec('Other Chargebee fields', rest) +
    sec('Domains ('+r.domain_total+')', '<div class="dom">'+dom+more+'</div>') +
    '</div><p class="cardnote">Card and payment-instrument details are '+
    'deliberately excluded from this report.</p></td></tr>';
}

/* ------------------------------------------------------------------ CSV */
const CSV_COLS = [
  ['owner','Owner'],['license_id','License ID'],['product','Plan'],
  ['license_status','License status'],['activity','Activity'],
  ['segment','Segment'],['payment','Payment state'],['payment_note','Payment note'],
  ['last_activity','Last activity'],['peak_machines','Peak concurrent machines'],
  ['machines','Machines seen'],['total_jobs','Jobs lifetime'],
  ['live_jobs','Jobs on live machines'],
  ['mrr','MRR cents'],['currency','Currency'],['card_status','Card status'],
  ['auto_collection','Auto collect'],['net_term_days','Net terms'],
  ['cb_id','Chargebee ID'],['cb_company','CB company'],['cb_name','CB contact'],
  ['cb_email','Primary email'],['@emails','All emails'],['cb_phone','Phone'],
  ['employees','Reported employees'],['@websites','Websites'],
  ['address','Address'],['country','Country'],
  ['restrictions','License restrictions'],['@compliance','Compliance check'],
  ['total_cores','Total cores'],['max_servers','Max servers'],
  ['licensed_domains','Licensed domains'],['@domains','Top domains'],
  ['cb_url','CB link'],
];
/* Columns prefixed with @ are derived from arrays rather than read straight off
   the row, so the CSV stays flat and mail-merge friendly. */
function csvValue(r, key){
  switch(key){
    case '@emails':     return (r.contacts||[]).map(c=>c.email).join('; ');
    case '@websites':   return (r.websites||[]).map(w=>w.host).join('; ');
    case '@compliance': return (r.compliance||[]).map(f=>f.text).join('; ');
    case '@domains':    return (r.domains||[]).slice(0,10).map(d=>d.d)
                                 .filter(Boolean).join('; ');
    default: return r[key];
  }
}
function exportCsv(){
  const rows = sorted(DATA.filter(passes));
  const q = v => '"'+String(v==null?'':v).replace(/"/g,'""')+'"';
  const body = rows.map(r=>CSV_COLS.map(c=>q(csvValue(r,c[0]))).join(',')).join('\r\n');
  const csv = CSV_COLS.map(c=>q(c[1])).join(',')+'\r\n'+body;
  const url = URL.createObjectURL(new Blob([csv],{type:'text/csv;charset=utf-8'}));
  const a = document.createElement('a');
  a.href=url; a.download='customer_outreach.csv'; a.click();
  setTimeout(()=>URL.revokeObjectURL(url),2000);
}

/* ------------------------------------------------------------------ wiring */
document.addEventListener('click', e=>{
  const seg = e.target.closest('.seg');
  if(seg){ state.segment = state.segment===seg.dataset.seg ? '' : seg.dataset.seg;
           render(); return; }
  const th = e.target.closest('th[data-k]');
  if(th){ const k=th.dataset.k;
          if(k===sortKey) sortDir=-sortDir; else {sortKey=k; sortDir=-1;}
          render(); return; }
  const row = e.target.closest('tr.row');
  if(row){ const id=row.dataset.id;
           expanded.has(id)?expanded.delete(id):expanded.add(id);
           render(); return; }
});
$('#q').addEventListener('input', e=>{ state.q=e.target.value.trim(); render(); });
['activity','payment','license'].forEach(k=>
  $('#f-'+k).addEventListener('change', e=>{ state[k]=e.target.value; render(); }));
$('#clear').addEventListener('click', ()=>{
  state.q=state.segment=state.activity=state.payment=state.license='';
  $('#q').value=''; ['activity','payment','license'].forEach(k=>$('#f-'+k).value='');
  expanded.clear(); render();
});
$('#csv').addEventListener('click', exportCsv);
$('#theme').addEventListener('click', ()=>{
  const cur = document.documentElement.getAttribute('data-theme');
  const next = cur==='dark' ? 'light' : cur==='light' ? 'dark'
    : (matchMedia('(prefers-color-scheme: dark)').matches ? 'light' : 'dark');
  document.documentElement.setAttribute('data-theme', next);
  try{ localStorage.setItem('cr-theme', next); }catch(e){}
});
try{ const t=localStorage.getItem('cr-theme');
     if(t) document.documentElement.setAttribute('data-theme',t); }catch(e){}
render();
"""


TILE = ('<div class="tile {cls}"><div class="lab">{lab}</div>'
        '<div class="val">{val}</div><div class="note2">{note}</div></div>')


def money(cents, cur="USD"):
    if not cents:
        return "$0"
    return "%s%s" % ("$" if cur == "USD" else cur + " ", "{:,.0f}".format(cents / 100.0))


def render_html(bundle, report, cb_meta):
    rows = bundle["rows"]
    orphans = bundle["orphans"]
    by_seg = {}
    for r in rows:
        by_seg.setdefault(r["segment"], []).append(r)

    total_mrr = sum(r["mrr"] for r in rows)
    at_risk = [r for r in rows if r["payment"] in ("critical", "warning")]
    at_risk_mrr = sum(r["mrr"] for r in at_risk)
    active = [r for r in rows if r["activity"] == "active"]
    unmatched = [r for r in rows if not r["cb_id"]]
    violating = by_seg.get("using_after_revoke", []) + by_seg.get("using_after_expiry", [])
    over_cap = [r for r in rows
                if any(f["level"] == "critical" for f in (r.get("compliance") or []))]
    capped = [r for r in rows if r.get("compliance")]

    win_s = (bundle["window_start"] or "")[:10]
    win_e = (bundle["window_end"] or "")[:10]
    recent_days = bundle["recent_days"]

    tiles = "".join([
        TILE.format(cls="", lab="Licenses seen", val="{:,}".format(len(rows)),
                    note="%d matched to Chargebee" % (len(rows) - len(unmatched))),
        TILE.format(cls="good", lab="Actively serving", val="{:,}".format(len(active)),
                    note="machines seen within %dd" % recent_days),
        TILE.format(cls="", lab="MRR represented", val=money(total_mrr),
                    note="across matched customers"),
        TILE.format(cls="crit", lab="MRR at risk", val=money(at_risk_mrr),
                    note="%d customers, card expired or expiring" % len(at_risk)),
        TILE.format(cls="ser", lab="Using past entitlement",
                    val="{:,}".format(len(violating)),
                    note="revoked or expired, still running"),
        TILE.format(cls="warn", lab="Paying, not using",
                    val="{:,}".format(len(by_seg.get("paying_unused", []))),
                    note="churn risk at renewal"),
        TILE.format(cls="ser", lab="Over employee cap",
                    val="{:,}".format(len(over_cap)),
                    note="of %d licenses with a restriction" % len(capped)),
    ])

    segs = []
    for key, name, desc, tone in SEGMENTS:
        group = by_seg.get(key, [])
        if not group:
            continue
        mrr = sum(r["mrr"] for r in group)
        segs.append(
            '<button class="seg {tone}" data-seg="{k}" aria-pressed="false">'
            '<div class="h"><span class="n">{n}</span><span class="c">{c}</span></div>'
            '<div class="d">{d}</div><div class="m">{m} MRR</div></button>'.format(
                tone=tone, k=key, n=html.escape(name), c=len(group),
                d=html.escape(desc), m=money(mrr)))

    def opts(pairs, present):
        out = ['<option value="">All</option>']
        for value, label in pairs:
            if value in present:
                out.append('<option value="%s">%s</option>' % (value, html.escape(label)))
        return "".join(out)

    act_opts = opts(list(ACTIVITY_LABELS.items()), {r["activity"] for r in rows})
    pay_opts = opts(list(PAYMENT_LABELS.items()), {r["payment"] for r in rows})
    lic_present = {r["license_status"] for r in rows}
    lic_opts = opts([("active", "Active"), ("if_expired", "Imageflow expired"),
                     ("r_expired", "Resizer expired"), ("revoked", "Revoked"),
                     ("corrupted", "Corrupted")], lic_present)

    cols = [
        ("owner", "Customer / license", ""),
        ("product", "Plan", ""),
        ("license_status", "License", ""),
        ("activity", "Activity", ""),
        ("last_activity", "Last seen", "num"),
        ("peak_machines", "Peak machines", "num"),
        ("machines", "Machines seen", "num"),
        ("total_jobs", "Jobs (lifetime)", "num"),
        ("live_jobs", "Machine last-seen", ""),
        ("mrr", "MRR", "num"),
        ("payment", "Payment", ""),
    ]
    thead = "".join(
        '<th data-k="%s" class="%s">%s<span class="ar"></span></th>' % (k, c, html.escape(l))
        for k, l, c in cols) + "<th></th>"

    orphan_html = ""
    if orphans:
        def orphan_row(o):
            emails = "<br>".join(
                '<a href="mailto:%s">%s</a>' % (html.escape(c["email"]), html.escape(c["email"]))
                for c in (o["contacts"] or [])[:4]) or "—"
            sites = "<br>".join(
                '<a href="https://%s" target="_blank" rel="noopener">%s</a>'
                % (html.escape(w["host"]), html.escape(w["host"]))
                for w in (o["websites"] or [])[:3]) or "—"
            return (
                '<tr><td><div class="owner">%s</div><div class="lid">%s</div></td>'
                '<td>%s</td><td>%s</td><td>%s</td><td class="num">%s</td>'
                '<td class="num">%s</td><td>%s</td><td>%s</td></tr>' % (
                    html.escape(o["owner"]), html.escape(o["cb_id"] or ""),
                    html.escape(o["cb_name"] or "—"),
                    emails,
                    ('<a href="tel:%s">%s</a>' % (
                        html.escape("".join(ch for ch in o["cb_phone"] if ch.isdigit() or ch == "+")),
                        html.escape(o["cb_phone"]))) if o["cb_phone"] else "—",
                    "—" if o["employees"] is None else "{:,}".format(o["employees"]),
                    money(o["mrr"], o["currency"]),
                    sites,
                    ('<a href="%s" target="_blank" rel="noopener">Chargebee →</a>'
                     % html.escape(o["cb_url"])) if o["cb_url"] else "—"))

        items = "".join(orphan_row(o) for o in orphans[:150])
        orphan_html = (
            '<h2>Paying customers with no telemetry <span style="color:var(--muted);'
            'font-weight:400">— %d with MRR and no matching license activity</span></h2>'
            '<p class="sub">Either they are not running the software at all, or their '
            'license owner name differs from their Chargebee company name. Both are '
            'worth a look.</p>'
            '<div class="tblwrap" style="margin-top:12px"><table><thead><tr>'
            '<th>Customer</th><th>Contact</th><th>Emails</th><th>Phone</th>'
            '<th class="num">Staff</th><th class="num">MRR</th><th>Websites</th>'
            '<th></th></tr></thead><tbody>%s</tbody></table></div>'
            % (len(orphans), items))

    payload = json.dumps(rows, separators=(",", ":")).replace("</", "<\\/")
    act_labels = json.dumps(ACTIVITY_LABELS).replace("</", "<\\/")
    pay_labels = json.dumps(PAYMENT_LABELS).replace("</", "<\\/")

    invoice_note = (
        '<p class="callout"><b>Payment state is inferred from card status, not from '
        'invoices.</b> The Chargebee <code>data_lookup_agent</code> MCP server currently '
        'has only the Customer Lookup toolset enabled, so invoice and transaction data '
        'is not reachable. To get true unpaid-invoice status here, enable '
        '<b>Invoice Lookup</b>, <b>Subscription Lookup</b> and <b>Transaction Lookup</b> '
        'under Settings → Configure Chargebee → Agentic AI → MCP Servers → Chargebee Data '
        'Lookup MCP Server → Toolsets, then re-run the fetch. Until then, "Payment broken" '
        'means an expired or missing card on an auto-collect account — a strong predictor '
        'of a failed invoice, but not a confirmed one.</p>')

    return """<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Customer outreach — licenses &amp; billing</title>
<style>%(css)s</style></head><body><div class="wrap">
<header class="top">
  <div>
    <h1>Customer outreach report</h1>
    <p class="sub">License telemetry <b>%(win_s)s → %(win_e)s</b> joined to Chargebee
      billing · %(nrows)d licenses · %(ncust)d Chargebee customers ·
      generated %(gen)s</p>
  </div>
  <button class="themebtn" id="theme">Toggle theme</button>
</header>

<div class="tiles">%(tiles)s</div>

%(invoice_note)s

<h2>Who to contact, and how</h2>
<div class="segs">%(segs)s</div>

<h2>All licenses</h2>
<div class="bar">
  <input type="search" id="q" placeholder="Search customer, license ID, plan, email, domain…">
  <div class="grp"><label for="f-activity">Activity</label>
    <select id="f-activity">%(act_opts)s</select></div>
  <div class="grp"><label for="f-payment">Payment</label>
    <select id="f-payment">%(pay_opts)s</select></div>
  <div class="grp"><label for="f-license">License</label>
    <select id="f-license">%(lic_opts)s</select></div>
  <button id="clear">Clear</button>
  <button id="csv">Export CSV</button>
  <span class="count" id="count"></span>
</div>
<div class="tblwrap"><table>
  <thead><tr>%(thead)s</tr></thead><tbody id="tb"></tbody>
</table></div>
<p class="sub" style="margin-top:9px;color:var(--muted)">
  Click any row for domains, features, contact details and a Chargebee link.
  Click a column header to sort, or a card above to filter.
  The <b>Machine last-seen</b> spark shows how many machines were last heard from in each
  week of the window — bars to the right mean the install is still live.</p>

%(orphans)s

<footer>
  <b>How the usage numbers work.</b> Each reporting process sends a <i>lifetime</i> job
  counter, and the telemetry retains only that process’s most recent report. So
  &ldquo;Jobs (lifetime)&rdquo; is the sum of those counters across every process seen in the
  window — <b>not</b> jobs completed during the window — and the sparkline shows
  <b>how many distinct machines were last heard from in each week</b>, not work over time.
  A fleet still reporting reads as marks on the right; one that drifted away months ago reads
  as marks on the left. &ldquo;Peak machines&rdquo; is the highest single-day concurrent count
  in the recent window, since per-day machine counts are sets and cannot be summed.<br>
  <b>Activity</b> is measured against the end of the log window (%(win_e)s), not today, because
  log delivery lags. Active = machines checked in within %(rd)d days and have done work;
  Fading = work on record but no recent check-in; Idle = checking in with a zero job counter;
  Silent = no check-in in %(rd2)d days.<br>
  <b>Compliance flags</b> compare the license’s contractual limits against the headcount
  the customer reported at signup. Both sides are self-reported and the headcount field is
  never re-validated — some values are obviously stale or mistyped. Treat a red flag as
  worth a conversation, not as proof of a breach. Non-profit, AGPLv3, no-resale and
  revenue-cap clauses show amber because billing data cannot confirm them either way.<br>
  <b>Card details are excluded</b> from this report by design — no numbers, last-4, expiry,
  gateway references or payment-source IDs. Card <i>status</i> is kept because it is the
  payment-health signal the segments are built on.<br>
  <b>Matching</b> joins the license <code>Owner</code> field to the Chargebee company name
  after normalizing case, punctuation and corporate suffixes. %(unmatched)d licenses did not
  match and are shown with “No CB match”.<br>
  <b>Window.</b> The S3 access-log bucket appears to expire objects after roughly 45 days,
  so %(win_s)s is as far back as the source data goes — not a choice made here. Longer
  history requires pulling <code>combine</code> on a schedule so logs are archived locally
  before S3 drops them.<br>
  Sources: <code>%(src_report)s</code> and <code>%(src_cb)s</code> (fetched %(cb_at)s).
</footer>
</div>
<script>
const DATA=%(payload)s;
const ACT_LABELS=%(act_labels)s;
const PAY_LABELS=%(pay_labels)s;
const RECENT_DAYS=%(rd)d;
const WINDOW_END=Date.parse(%(win_end_js)s);
%(js)s
</script></body></html>""" % {
        "css": CSS,
        "js": JS,
        "tiles": tiles,
        "segs": "".join(segs),
        "invoice_note": invoice_note,
        "thead": thead,
        "act_opts": act_opts,
        "pay_opts": pay_opts,
        "lic_opts": lic_opts,
        "orphans": orphan_html,
        "payload": payload,
        "act_labels": act_labels,
        "pay_labels": pay_labels,
        "win_s": win_s,
        "win_e": win_e,
        "win_end_js": json.dumps(bundle["window_end"] or ""),
        "rd": recent_days,
        "rd2": recent_days * 2,
        "nrows": len(rows),
        "ncust": cb_meta.get("customer_count", 0),
        "gen": dt.datetime.now().strftime("%Y-%m-%d %H:%M"),
        "unmatched": len(unmatched),
        "src_report": html.escape(cb_meta.get("_report_path", "report.json")),
        "src_cb": html.escape(cb_meta.get("_cb_path", "chargebee_customers.json")),
        "cb_at": html.escape(cb_meta.get("fetched_at", "unknown")),
    }


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--report", default="parsed_test/report.json")
    ap.add_argument("--chargebee", default="chargebee_customers.json")
    ap.add_argument("--out", default="customer_report.html")
    ap.add_argument("--recent-days", type=int, default=30,
                    help="window that counts as 'recently active' (default 30)")
    args = ap.parse_args()

    for path in (args.report, args.chargebee):
        if not os.path.exists(path):
            raise SystemExit("Missing input: %s" % path)

    with open(args.report, "r", encoding="utf-8") as fh:
        report = json.load(fh)
    with open(args.chargebee, "r", encoding="utf-8") as fh:
        cb = json.load(fh)

    customers = cb.get("customers", [])
    bundle = assemble(report, customers, args.recent_days)

    meta = dict(cb)
    meta.pop("customers", None)
    meta["_report_path"] = args.report
    meta["_cb_path"] = args.chargebee

    out = render_html(bundle, report, meta)
    with open(args.out, "w", encoding="utf-8") as fh:
        fh.write(out)

    rows = bundle["rows"]
    seg_counts = {}
    for r in rows:
        seg_counts[r["segment"]] = seg_counts.get(r["segment"], 0) + 1
    sys.stderr.write("Wrote %s (%.1f KB)\n" % (args.out, os.path.getsize(args.out) / 1024.0))
    sys.stderr.write("  %d licenses, %d matched to Chargebee, %d orphan paying customers\n"
                     % (len(rows), sum(1 for r in rows if r["cb_id"]), len(bundle["orphans"])))
    for key, name, _, _ in SEGMENTS:
        if seg_counts.get(key):
            sys.stderr.write("    %-28s %3d\n" % (name, seg_counts[key]))


if __name__ == "__main__":
    main()
