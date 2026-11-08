#!/usr/bin/env python3
"""
Pull customers, subscriptions and invoices from the Chargebee v2 API into a single
JSON cache (chargebee.json) for the license/billing cross-reference report.

Credentials are read from the environment (or a .env file in the repo root):
    CHARGEBEE_SITE      e.g. "imazen"  (the {site} in https://{site}.chargebee.com)
    CHARGEBEE_API_KEY   a read-only API key

Usage:
    python tools/fetch_chargebee.py [--out chargebee.json]

Only GET requests are issued; this script never mutates billing data.
"""
import argparse
import base64
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

API_TIMEOUT = 60
PAGE_LIMIT = 100
MAX_RETRIES = 5


def load_dotenv(path):
    """Minimal .env reader: KEY=VALUE lines, # comments, optional quotes."""
    if not os.path.exists(path):
        return
    with open(path, "r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, value = line.partition("=")
            key = key.strip()
            value = value.strip().strip('"').strip("'")
            # Real environment wins over .env
            os.environ.setdefault(key, value)


class Chargebee:
    def __init__(self, site, api_key):
        self.base = "https://%s.chargebee.com/api/v2" % site
        token = base64.b64encode(("%s:" % api_key).encode("utf-8")).decode("ascii")
        self.auth = "Basic %s" % token

    def get(self, path, params):
        url = "%s/%s" % (self.base, path.lstrip("/"))
        if params:
            url += "?" + urllib.parse.urlencode(params, doseq=True)
        req = urllib.request.Request(url, method="GET")
        req.add_header("Authorization", self.auth)
        req.add_header("Accept", "application/json")

        for attempt in range(MAX_RETRIES):
            try:
                with urllib.request.urlopen(req, timeout=API_TIMEOUT) as resp:
                    return json.loads(resp.read().decode("utf-8"))
            except urllib.error.HTTPError as e:
                body = e.read().decode("utf-8", "replace")[:400]
                # 429 = rate limited, 5xx = transient. Back off and retry.
                if e.code in (429, 500, 502, 503, 504) and attempt < MAX_RETRIES - 1:
                    wait = 2 ** attempt
                    sys.stderr.write("  %s -> HTTP %d, retrying in %ds\n" % (path, e.code, wait))
                    time.sleep(wait)
                    continue
                raise SystemExit(
                    "Chargebee API error on %s: HTTP %d\n%s" % (url, e.code, body)
                )
            except urllib.error.URLError as e:
                if attempt < MAX_RETRIES - 1:
                    time.sleep(2 ** attempt)
                    continue
                raise SystemExit("Could not reach Chargebee at %s: %s" % (url, e))
        raise SystemExit("Exhausted retries for %s" % url)

    def list_all(self, path, params=None, label=None):
        """Walk Chargebee's offset-based pagination and return every entry."""
        params = dict(params or {})
        params["limit"] = PAGE_LIMIT
        out = []
        offset = None
        page = 0
        while True:
            if offset:
                params["offset"] = offset
            elif "offset" in params:
                del params["offset"]
            data = self.get(path, params)
            rows = data.get("list", [])
            out.extend(rows)
            page += 1
            sys.stderr.write(
                "  %s: page %d (+%d, total %d)\n" % (label or path, page, len(rows), len(out))
            )
            offset = data.get("next_offset")
            if not offset:
                break
        return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="chargebee.json")
    ap.add_argument("--site", default=None, help="overrides CHARGEBEE_SITE")
    args = ap.parse_args()

    repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    load_dotenv(os.path.join(repo_root, ".env"))

    site = args.site or os.environ.get("CHARGEBEE_SITE")
    api_key = os.environ.get("CHARGEBEE_API_KEY")
    if not site or not api_key:
        raise SystemExit(
            "Set CHARGEBEE_SITE and CHARGEBEE_API_KEY (env vars or .env in the repo root).\n"
            "Example .env:\n  CHARGEBEE_SITE=imazen\n  CHARGEBEE_API_KEY=live_xxxxxxxx"
        )

    cb = Chargebee(site, api_key)

    sys.stderr.write("Fetching customers...\n")
    customers = cb.list_all("customers", label="customers")

    sys.stderr.write("Fetching subscriptions...\n")
    subscriptions = cb.list_all("subscriptions", label="subscriptions")

    # Every non-settled invoice. `posted` and `payment_due` are finalized-but-unpaid;
    # `not_paid` is a failed collection. `pending` is not yet finalized.
    sys.stderr.write("Fetching unpaid invoices...\n")
    unpaid = cb.list_all(
        "invoices",
        {"status[in]": '["posted","payment_due","not_paid"]'},
        label="unpaid invoices",
    )

    # Recent paid invoices give us "last payment" context for healthy accounts.
    sys.stderr.write("Fetching paid invoices...\n")
    paid = cb.list_all(
        "invoices",
        {"status[is]": "paid", "sort_by[desc]": "date"},
        label="paid invoices",
    )

    payload = {
        "site": site,
        "fetched_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "customers": [c.get("customer", c) for c in customers],
        "subscriptions": [s.get("subscription", s) for s in subscriptions],
        "unpaid_invoices": [i.get("invoice", i) for i in unpaid],
        "paid_invoices": [i.get("invoice", i) for i in paid],
    }

    with open(args.out, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, indent=1)

    sys.stderr.write(
        "\nWrote %s\n  %d customers, %d subscriptions, %d unpaid invoices, %d paid invoices\n"
        % (
            args.out,
            len(payload["customers"]),
            len(payload["subscriptions"]),
            len(payload["unpaid_invoices"]),
            len(payload["paid_invoices"]),
        )
    )


if __name__ == "__main__":
    main()
