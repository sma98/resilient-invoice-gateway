# Harness — Invoice Ingestion & Reconciliation Pipeline

This folder is provided to you so you don't waste time building upstream plumbing. Build
your pipeline **on top of** these. You may read both scripts to understand their contracts;
**do not modify `mock_erp.py` to remove its rate limit or fault injection** — reconciling
against a hostile upstream is the assignment.

Both scripts are deterministic given a `--seed`. **Use the same seed (default `42`) for the
generator and the ERP** so the matchable invoices line up with the ERP's known POs.

---

## 1. `generate_invoices.py` — your ingestion feed

Pure stdlib, no dependencies. Emits NDJSON (one JSON invoice per line).

```bash
# 100k invoices to a file (default count/seed)
python generate_invoices.py --count 100000 --seed 42 --out invoices.ndjson

# quick peek
python generate_invoices.py --count 20 | python -m json.tool --json-lines 2>/dev/null || python generate_invoices.py --count 5
```

The dataset intentionally contains: ~3% exact duplicates, ~2% near-duplicates (same content,
new `invoice_id`), ~4% malformed records, and ~15% that will **not** match the ERP. The rest
should reconcile cleanly.

### Invoice schema (clean record)
```json
{
  "invoice_id": "INV-000000123",
  "invoice_number": "1003-48217",
  "vendor_id": "V-1003",
  "vendor_name": "Cornerstone Electric",
  "po_number": "PO-001234",
  "amount": 4210.55,
  "currency": "USD",
  "cost_center": "CC-014",
  "source": "property-erp",
  "issued_at": "2025-09-14T08:22:00Z",
  "submitted_at": "2025-09-20T08:22:00Z",
  "content_hash": "9f2c1a7b3e8d0f44"
}
```
Malformed records may be missing `amount`/`po_number`, have a blank `vendor_id`, a negative
amount, or a bogus currency, and carry a `_defect` marker. Decide how to validate them.

---

## 2. `mock_erp.py` — the system-of-record ERP (hostile upstream)

```bash
pip install fastapi uvicorn
ERP_SEED=42 ERP_RATE_LIMIT=10 ERP_PO_COUNT=33333 uvicorn mock_erp:app --host 0.0.0.0 --port 9100
```

> **Important:** `ERP_PO_COUNT` must equal `_po_count(count)` from the generator
> (`max(100, count // 3)`). For the default 100k dataset that is **33333**. If you change
> `--count`, set `ERP_PO_COUNT` to match or some "clean" invoices won't find their PO.

### Behavior (do not disable for your final submission)
- **Global** rate limit of `ERP_RATE_LIMIT` req/s across *all* callers → `429` + `Retry-After`.
- 200–900 ms latency per call.
- ~5% transient `503`, ~1% timeout (handler hangs ~6 s → your client should time out & retry).
- Set `ERP_CHAOS=0` only for local debugging; your submission must work with chaos **on**.

### Endpoint
```
GET /erp/purchase-orders/match?po_number=PO-001234&vendor_id=V-1003&amount=4210.55&currency=USD
```
Match rule: PO must exist, `vendor_id` must equal the PO's vendor, and `|amount - po_amount|`
must be within **1%** tolerance. Returns `{"matched": true, ...}` or
`{"matched": false, "reason": "..."}`. See the docstring in `mock_erp.py` for the full
response shapes.

`GET /healthz` returns `{"status":"ok", ...}`.

---

## Suggested docker-compose wiring (you provide your own)

Your `docker-compose.yml` should bring up: **your API**, **your worker(s)**, **Redis**, and
**this mock ERP**. A minimal sketch for the ERP service:

```yaml
  mock-erp:
    build: ./harness          # or mount and run uvicorn
    command: uvicorn mock_erp:app --host 0.0.0.0 --port 9100
    environment:
      ERP_SEED: "42"
      ERP_RATE_LIMIT: "10"
      ERP_PO_COUNT: "33333"
      ERP_CHAOS: "1"
    ports: ["9100:9100"]
```

Point your workers at `http://mock-erp:9100`. Good luck — and remember: **a correct slice
plus a sharp design doc beats a big pile of half-working features.**
