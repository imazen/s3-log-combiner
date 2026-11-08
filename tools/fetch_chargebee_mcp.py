#!/usr/bin/env python3
"""
Pull Chargebee billing data through the data-lookup MCP server into
chargebee_customers.json, for joining against license telemetry.

Customers are always fetched. Invoices, subscriptions and transactions are
fetched *if* the corresponding toolsets are enabled on the MCP server -- this
script probes tools/list and adapts, so no code change is needed once an admin
turns them on under:

    Settings > Configure Chargebee > Agentic AI > MCP Servers
      > Chargebee Data Lookup MCP Server > Toolsets

Enable "Invoice Lookup", "Subscription Lookup" and "Transaction Lookup" to get
true unpaid-invoice status into the report.

Usage:  python tools/fetch_chargebee_mcp.py [--out chargebee_customers.json]
"""
import argparse
import json
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from mcp_client import connect_chargebee, text_of  # noqa: E402

PAGE_LIMIT = 100
MAX_PAGES = 400

# Tool-name candidates per resource, most-preferred first. The MCP server has
# used both bare and toolset-prefixed names, so try each.
CANDIDATES = {
    "invoices": ["list_invoices", "invoice_lookup-list_invoices_tool"],
    "subscriptions": ["list_subscriptions", "subscription_lookup-list_subscriptions_tool"],
    "transactions": ["list_transactions", "transaction_lookup-list_transactions_tool"],
}

# Only non-settled invoices matter for collections.
UNPAID_STATUSES = ["posted", "payment_due", "not_paid"]


def paginate(client, tool, extra=None, label=None):
    """Walk a Chargebee list tool's cursor pagination, returning every row."""
    rows = []
    offset = None
    pages = 0
    while pages < MAX_PAGES:
        args = {
            "limit": PAGE_LIMIT,
            "offset": offset,
            "filters_json": json.dumps(extra) if extra else None,
            "include_fields_json": None,
        }
        payload = json.loads(text_of(client.call_tool(tool, args)))
        page = payload.get("list", [])
        rows.extend(page)
        pages += 1
        sys.stderr.write("  %s: page %d (+%d, total %d)\n"
                         % (label or tool, pages, len(page), len(rows)))
        offset = payload.get("next_offset")
        if not offset or not page:
            break
    return rows


def unwrap(rows, key):
    """Chargebee list rows arrive wrapped as {"<key>": {...}}; flatten them,
    keeping any sibling objects under an underscore prefix."""
    out = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        obj = dict(row.get(key, row))
        for k, v in row.items():
            if k != key and isinstance(v, (dict, list)):
                obj["_" + k] = v
        out.append(obj)
    return out


def first_available(available, names):
    for n in names:
        if n in available:
            return n
    return None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="chargebee_customers.json")
    args = ap.parse_args()

    client = connect_chargebee()
    available = {t.get("name") for t in client.list_tools()}
    sys.stderr.write("MCP exposes %d tools\n" % len(available))

    sys.stderr.write("Fetching customers...\n")
    customers = unwrap(paginate(client, "list_customers", label="customers"), "customer")

    out = {
        "source": "chargebee data_lookup_agent MCP",
        "fetched_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "tools_available": sorted(available),
        "customers": customers,
        "customer_count": len(customers),
    }

    # --- optional resources, present only when their toolset is enabled -------
    missing = []

    sub_tool = first_available(available, CANDIDATES["subscriptions"])
    if sub_tool:
        sys.stderr.write("Fetching subscriptions...\n")
        out["subscriptions"] = unwrap(
            paginate(client, sub_tool, label="subscriptions"), "subscription")
    else:
        missing.append("Subscription Lookup")

    inv_tool = first_available(available, CANDIDATES["invoices"])
    if inv_tool:
        sys.stderr.write("Fetching unpaid invoices...\n")
        unpaid = unwrap(
            paginate(client, inv_tool, {"status": {"in": UNPAID_STATUSES}},
                     label="unpaid invoices"), "invoice")
        out["unpaid_invoices"] = unpaid
        sys.stderr.write("Fetching paid invoices...\n")
        out["paid_invoices"] = unwrap(
            paginate(client, inv_tool, {"status": {"is": "paid"}},
                     label="paid invoices"), "invoice")
    else:
        missing.append("Invoice Lookup")

    txn_tool = first_available(available, CANDIDATES["transactions"])
    if txn_tool:
        sys.stderr.write("Fetching transactions...\n")
        out["transactions"] = unwrap(
            paginate(client, txn_tool, label="transactions"), "transaction")
    else:
        missing.append("Transaction Lookup")

    out["missing_toolsets"] = missing

    with open(args.out, "w", encoding="utf-8") as fh:
        json.dump(out, fh, indent=1)

    sys.stderr.write("\nWrote %s\n" % args.out)
    sys.stderr.write("  customers          %d\n" % len(customers))
    for key in ("subscriptions", "unpaid_invoices", "paid_invoices", "transactions"):
        if key in out:
            sys.stderr.write("  %-18s %d\n" % (key, len(out[key])))
    if missing:
        sys.stderr.write(
            "\n  NOT AVAILABLE: %s\n"
            "  Enable these under Settings > Configure Chargebee > Agentic AI >\n"
            "  MCP Servers > Chargebee Data Lookup MCP Server > Toolsets,\n"
            "  then re-run this script to get true invoice payment status.\n"
            % ", ".join(missing))


if __name__ == "__main__":
    main()
