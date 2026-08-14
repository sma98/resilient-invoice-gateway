#!/usr/bin/env python3
"""
Mock ERP "system of record" for the Blue Ocean reconciliation take-home.

This service deliberately behaves like a hostile, slow, rate-limited third-party
ERP (think Oracle Fusion / Epicor / Business Central behind a flaky gateway).
DO NOT modify it to make reconciliation easier — coping with it IS the assignment.

It exposes one matching endpoint and enforces, GLOBALLY (not per-client):

  * a hard rate limit of ERP_RATE_LIMIT requests/second  -> 429 with Retry-After
  * injected latency of 200-900 ms per call
  * ~5% transient 503 errors
  * ~1% timeouts (the handler sleeps long enough to trip your client timeout)

The set of "known" purchase orders is generated from the SAME seed as
generate_invoices.py, so clean invoices in the dataset will match and the
intentionally-broken ones (unknown PO / wrong amount / unknown vendor) will not.

Run:
    pip install fastapi uvicorn
    uvicorn mock_erp:app --host 0.0.0.0 --port 9100
        (env: ERP_SEED=42  ERP_RATE_LIMIT=10  ERP_PO_COUNT=33333  ERP_CHAOS=1)

Or with Flask-free pure stdlib? No — we use FastAPI for clarity. It is provided
in docker-compose by the harness; you should not need to touch it.

Contract
--------
GET /healthz
    -> 200 {"status": "ok"}

GET /erp/purchase-orders/match?po_number=PO-000123&vendor_id=V-1003&amount=4210.55&currency=USD
    -> 200 {"matched": true,  "po_number": "...", "vendor_id": "...",
            "po_amount": 4210.55, "currency": "USD", "status": "OPEN",
            "match_confidence": "exact"}
    -> 200 {"matched": false, "reason": "po_not_found" | "vendor_mismatch" |
            "amount_out_of_tolerance", ...}
    -> 429 {"error": "rate_limited"}        (Retry-After header in seconds)
    -> 503 {"error": "upstream_unavailable"}  (transient; retry it)
    (timeouts: the request just hangs ~6s; your client should time out and retry)

Matching rule the ERP applies:
    * po_number must exist
    * vendor_id must equal the PO's vendor
    * |amount - po_amount| must be within 1% tolerance (amounts can differ slightly)
"""
import asyncio
import os
import random
import threading
import time

from fastapi import FastAPI, Query, Response

SEED = int(os.getenv("ERP_SEED", "42"))
RATE_LIMIT = int(os.getenv("ERP_RATE_LIMIT", "10"))      # global requests/sec
CHAOS = os.getenv("ERP_CHAOS", "1") == "1"               # set 0 to disable faults (debugging only)
# Must match generate_invoices.py's _po_count(count): default count 100k -> 33333.
PO_COUNT = int(os.getenv("ERP_PO_COUNT", "33333"))

VENDORS = [
    "V-1001", "V-1002", "V-1003", "V-1004", "V-1005",
    "V-1006", "V-1007", "V-1008", "V-1009", "V-1010",
]

app = FastAPI(title="Mock ERP (system of record)")


def _build_po_table(seed: int, n: int) -> dict:
    """Deterministically build PO -> {vendor_id, amount, currency, status}."""
    rng = random.Random(seed * 7919 + 13)  # different stream than the generator's invoices
    table = {}
    for i in range(1, n + 1):
        po = f"PO-{i:06d}"
        table[po] = {
            "vendor_id": rng.choice(VENDORS),
            "po_amount": round(rng.uniform(125.00, 85000.00), 2),
            "currency": rng.choice(["USD", "USD", "USD", "EUR", "GBP"]),
            "status": rng.choice(["OPEN", "OPEN", "OPEN", "PARTIALLY_BILLED"]),
        }
    return table


PO_TABLE = _build_po_table(SEED, PO_COUNT)


class GlobalRateLimiter:
    """A simple fixed-window-per-second global limiter, shared across all requests."""

    def __init__(self, limit_per_sec: int):
        self.limit = limit_per_sec
        self.lock = threading.Lock()
        self.window = int(time.time())
        self.count = 0

    def allow(self) -> bool:
        with self.lock:
            now = int(time.time())
            if now != self.window:
                self.window = now
                self.count = 0
            if self.count >= self.limit:
                return False
            self.count += 1
            return True


limiter = GlobalRateLimiter(RATE_LIMIT)
_rng = random.Random(SEED)
_rng_lock = threading.Lock()


def _roll() -> float:
    with _rng_lock:
        return _rng.random()


@app.get("/healthz")
async def healthz():
    return {"status": "ok", "po_count": len(PO_TABLE), "rate_limit_per_sec": RATE_LIMIT}


@app.get("/erp/purchase-orders/match")
async def match(
    response: Response,
    po_number: str = Query(...),
    vendor_id: str = Query(...),
    amount: float = Query(...),
    currency: str = Query("USD"),
):
    # 1) Global rate limit — returns 429 regardless of which worker called.
    if not limiter.allow():
        response.status_code = 429
        response.headers["Retry-After"] = "1"
        return {"error": "rate_limited"}

    # 2) Chaos: transient failures and timeouts.
    if CHAOS:
        r = _roll()
        if r < 0.01:            # ~1% timeout: hang long enough to trip a sane client timeout
            await asyncio.sleep(6.0)
            response.status_code = 504
            return {"error": "gateway_timeout"}
        if r < 0.06:            # ~5% transient 503
            response.status_code = 503
            return {"error": "upstream_unavailable"}

    # 3) Injected latency.
    await asyncio.sleep(_roll() * 0.7 + 0.2)  # 200-900 ms

    # 4) Actual matching logic.
    po = PO_TABLE.get(po_number)
    if po is None:
        return {"matched": False, "reason": "po_not_found", "po_number": po_number}
    if po["vendor_id"] != vendor_id:
        return {
            "matched": False, "reason": "vendor_mismatch",
            "po_number": po_number, "expected_vendor": po["vendor_id"],
        }
    tolerance = po["po_amount"] * 0.01
    if abs(amount - po["po_amount"]) > tolerance:
        return {
            "matched": False, "reason": "amount_out_of_tolerance",
            "po_number": po_number, "po_amount": po["po_amount"],
        }
    return {
        "matched": True,
        "po_number": po_number,
        "vendor_id": vendor_id,
        "po_amount": po["po_amount"],
        "currency": po["currency"],
        "status": po["status"],
        "match_confidence": "exact",
    }
