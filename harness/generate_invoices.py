#!/usr/bin/env python3
"""
Synthetic invoice generator for the Blue Ocean reconciliation take-home.

Emits NDJSON (one invoice per line) to stdout or a file. The dataset is
intentionally messy, mirroring real upstream feeds:

  * ~3% exact duplicates (same invoice_id submitted again)            -> tests idempotency
  * ~2% near-duplicates (same content, different invoice_id)          -> tests dedupe by content
  * ~4% malformed records (missing/blank required fields, bad amounts)-> tests validation
  * ~15% that will NOT match the ERP (unknown PO / vendor / amount)   -> tests "unmatched" path
  * the rest are clean and should reconcile against the mock ERP.

The matchable invoices reference purchase orders PO-000001..PO-<N> that the
mock ERP (mock_erp.py) also knows about, using the SAME seed. Run both with the
same --seed so the matchable set lines up.

Usage:
    python generate_invoices.py --count 100000 --seed 42 --out invoices.ndjson
    python generate_invoices.py --count 1000 | head    # quick peek

This script has NO third-party dependencies (stdlib only).
"""
import argparse
import hashlib
import json
import random
import sys
from datetime import datetime, timedelta

VENDORS = [
    ("V-1001", "Atlas Facilities LLC"),
    ("V-1002", "Brightline HVAC"),
    ("V-1003", "Cornerstone Electric"),
    ("V-1004", "Delta Plumbing Co"),
    ("V-1005", "Evergreen Landscaping"),
    ("V-1006", "Ferro Steel Supply"),
    ("V-1007", "Granite Roofing"),
    ("V-1008", "Horizon IT Services"),
    ("V-1009", "Ironwood Carpentry"),
    ("V-1010", "Juniper Cleaning"),
]
CURRENCIES = ["USD", "USD", "USD", "EUR", "GBP"]  # weighted toward USD
SOURCES = ["property-erp", "email-pdf", "bank-feed", "ap-portal"]
COST_CENTERS = [f"CC-{n:03d}" for n in range(1, 41)]


def _po_count(invoice_count: int) -> int:
    # Roughly one PO per ~3 invoices so POs are referenced repeatedly
    # (this is what makes ERP-lookup caching worthwhile).
    return max(100, invoice_count // 3)


def _content_hash(inv: dict) -> str:
    basis = f"{inv['vendor_id']}|{inv['po_number']}|{inv['amount']}|{inv['currency']}|{inv['invoice_number']}"
    return hashlib.sha256(basis.encode()).hexdigest()[:16]


def _make_clean(rng: random.Random, idx: int, po_max: int, base_date: datetime) -> dict:
    vendor_id, vendor_name = rng.choice(VENDORS)
    po_number = f"PO-{rng.randint(1, po_max):06d}"
    amount = round(rng.uniform(125.00, 85000.00), 2)
    issued = base_date - timedelta(days=rng.randint(0, 365), minutes=rng.randint(0, 1440))
    inv = {
        "invoice_id": f"INV-{idx:09d}",
        "invoice_number": f"{vendor_id[-4:]}-{rng.randint(10000, 99999)}",
        "vendor_id": vendor_id,
        "vendor_name": vendor_name,
        "po_number": po_number,
        "amount": amount,
        "currency": rng.choice(CURRENCIES),
        "cost_center": rng.choice(COST_CENTERS),
        "source": rng.choice(SOURCES),
        "issued_at": issued.isoformat() + "Z",
        "submitted_at": (issued + timedelta(days=rng.randint(0, 10))).isoformat() + "Z",
    }
    inv["content_hash"] = _content_hash(inv)
    return inv


def _corrupt(rng: random.Random, inv: dict) -> dict:
    """Return a malformed variant of an invoice."""
    bad = dict(inv)
    mode = rng.choice(["no_amount", "blank_vendor", "neg_amount", "bad_currency", "no_po"])
    if mode == "no_amount":
        bad.pop("amount", None)
    elif mode == "blank_vendor":
        bad["vendor_id"] = ""
    elif mode == "neg_amount":
        bad["amount"] = -abs(inv.get("amount", 100.0))
    elif mode == "bad_currency":
        bad["currency"] = "XYZ"
    elif mode == "no_po":
        bad["po_number"] = ""
    bad["_defect"] = mode  # marker so you can sanity-check; remove on ingest if you like
    return bad


def _wont_match(rng: random.Random, inv: dict) -> dict:
    """Valid-looking invoice that references something the ERP won't confirm."""
    bad = dict(inv)
    mode = rng.choice(["unknown_po", "wrong_amount", "unknown_vendor"])
    if mode == "unknown_po":
        bad["po_number"] = f"PO-{rng.randint(9_000_000, 9_999_999):06d}"
    elif mode == "wrong_amount":
        bad["amount"] = round(inv["amount"] * rng.uniform(1.5, 3.0), 2)
    elif mode == "unknown_vendor":
        bad["vendor_id"] = f"V-{rng.randint(9000, 9999)}"
    bad["content_hash"] = _content_hash(bad)
    return bad


def generate(count: int, seed: int):
    rng = random.Random(seed)
    po_max = _po_count(count)
    base_date = datetime(2026, 1, 1)  # fixed so output is deterministic given seed

    # Build a pool of clean invoices first, then weave in defects/dupes as we stream.
    emitted = 0
    recent_clean = []  # keep a small window to source duplicates from
    target = count

    idx = 0
    while emitted < target:
        idx += 1
        inv = _make_clean(rng, idx, po_max, base_date)
        roll = rng.random()

        if roll < 0.04:  # malformed
            out = _corrupt(rng, inv)
        elif roll < 0.19:  # won't match ERP (~15%)
            out = _wont_match(rng, inv)
        else:
            out = inv
            recent_clean.append(inv)
            if len(recent_clean) > 500:
                recent_clean.pop(0)

        yield out
        emitted += 1

        # Exact duplicate (~3%): re-emit a recent clean invoice verbatim.
        if recent_clean and rng.random() < 0.03 and emitted < target:
            yield dict(rng.choice(recent_clean))
            emitted += 1

        # Near-duplicate (~2%): same content_hash, new invoice_id.
        if recent_clean and rng.random() < 0.02 and emitted < target:
            src = rng.choice(recent_clean)
            near = dict(src)
            near["invoice_id"] = f"INV-{idx:09d}-N"
            yield near
            emitted += 1


def main():
    ap = argparse.ArgumentParser(description="Generate synthetic invoices (NDJSON).")
    ap.add_argument("--count", type=int, default=100_000, help="approx number of records")
    ap.add_argument("--seed", type=int, default=42, help="RNG seed (match mock_erp.py)")
    ap.add_argument("--out", type=str, default="-", help="output file, or '-' for stdout")
    args = ap.parse_args()

    out = sys.stdout if args.out == "-" else open(args.out, "w", encoding="utf-8")
    try:
        n = 0
        for inv in generate(args.count, args.seed):
            out.write(json.dumps(inv) + "\n")
            n += 1
        if out is not sys.stdout:
            print(f"Wrote {n} invoices to {args.out} (seed={args.seed})", file=sys.stderr)
    finally:
        if out is not sys.stdout:
            out.close()


if __name__ == "__main__":
    main()
