#!/usr/bin/env python3
"""
Coinbase Advanced Trade "sell winners" bot.
- Scopes to one portfolio (PORTFOLIO_UUID preferred, else PORTFOLIO_NAME="bot").
- Rebuilds moving-average cost basis from fills (paginated).
- Sells 100% of any asset whose gain >= TARGET_PROFIT_PCT vs avg cost.
- Verbose logging/heartbeat for Railway.

Env vars:
  COINBASE_API_KEY, COINBASE_API_SECRET (required by coinbase-advanced-py)
  TARGET_PROFIT_PCT=0.10
  SLEEP_SEC=60
  QUOTE_CURRENCY=USD
  PORTFOLIO_UUID=<uuid>        # preferred
  PORTFOLIO_NAME=bot           # used if UUID not set
  FALLBACK_TO_LAST_BUY=1       # optional, use last BUY price if avg cannot be computed
  DEBUG=1                      # more verbose logs
"""

import os
import re
import time
from decimal import Decimal, ROUND_DOWN, InvalidOperation
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple, Iterable, List

from coinbase.rest import RESTClient  # pip install coinbase-advanced-py


# -----------------------------
# Config / env
# -----------------------------
TARGET_PROFIT_PCT = float(os.getenv("TARGET_PROFIT_PCT", "0.10"))  # 10%
SLEEP_SEC         = int(os.getenv("SLEEP_SEC", "60"))
QUOTE_CURRENCY    = os.getenv("QUOTE_CURRENCY", "USD").upper()
FALLBACK_TO_LAST_BUY = os.getenv("FALLBACK_TO_LAST_BUY", "0") not in ("0", "false", "False", "")
PORTFOLIO_UUID    = os.getenv("PORTFOLIO_UUID", "").strip()
PORTFOLIO_NAME    = os.getenv("PORTFOLIO_NAME", "bot").strip() if not PORTFOLIO_UUID else ""
DEBUG             = os.getenv("DEBUG", "0") not in ("0", "false", "False", "")

client = RESTClient()  # relies on COINBASE_API_KEY/COINBASE_API_SECRET in env


# -----------------------------
# Logging helpers
# -----------------------------
def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%SZ")

def log(msg: str) -> None:
    print(f"[cb-sell-winners] {_now()} | {msg}", flush=True)

def dbg(msg: str) -> None:
    if DEBUG:
        print(f"[cb-sell-winners][debug] {_now()} | {msg}", flush=True)


# -----------------------------
# Utilities
# -----------------------------
def _get(o: Any, k: str, default=None):
    if isinstance(o, dict):
        return o.get(k, default)
    return getattr(o, k, default)

def _to_decimal_maybe(v) -> Optional[Decimal]:
    if v is None:
        return None
    if isinstance(v, (int, float)):
        try:
            return Decimal(str(v))
        except InvalidOperation:
            return None
    if isinstance(v, str):
        s = v.strip()
        if not s:
            return None
        if not re.match(r"^-?\d+(\.\d+)?$", s):
            m = re.search(r"-?\d+(?:\.\d+)?", s)
            if not m:
                return None
            s = m.group(0)
        try:
            return Decimal(s)
        except InvalidOperation:
            return None
    return None

def round_to_inc(value: Decimal, inc: Decimal) -> Decimal:
    if inc <= 0:
        return value
    return (value / inc).to_integral_value(rounding=ROUND_DOWN) * inc


# -----------------------------
# Portfolio scoping
# -----------------------------
def ensure_portfolio_uuid() -> Optional[str]:
    global PORTFOLIO_UUID
    if PORTFOLIO_UUID:
        return PORTFOLIO_UUID
    # Discover by name
    try:
        res = client.get("/api/v3/brokerage/portfolios")
        ports = _get(res, "portfolios") or _get(res, "data") or []
        for p in ports:
            name = str(_get(p, "name") or "").strip().lower()
            if name == PORTFOLIO_NAME.lower():
                PORTFOLIO_UUID = _get(p, "uuid") or _get(p, "portfolio_uuid")
                if PORTFOLIO_UUID:
                    log(f"Using portfolio '{PORTFOLIO_NAME}' ({PORTFOLIO_UUID})")
                    return PORTFOLIO_UUID
        log(f"Portfolio named '{PORTFOLIO_NAME}' not found; defaulting to ALL portfolios.")
        return None
    except Exception as e:
        log(f"Error listing portfolios: {e}")
        return None


# -----------------------------
# Market data
# -----------------------------
def price_for_product(pid: str) -> Optional[Decimal]:
    # Try ticker
    try:
        res = client.get(f"/api/v3/brokerage/products/{pid}/ticker")
        price = _get(res, "price") or _get(_get(res, "ticker", {}), "price")
        px = _to_decimal_maybe(price)
        if px is not None:
            return px
        bid = _to_decimal_maybe(_get(res, "bid"))
        ask = _to_decimal_maybe(_get(res, "ask"))
        if bid is not None and ask is not None:
            return (bid + ask) / 2
    except Exception as e:
        dbg(f"{pid} | /ticker fetch error: {e}")

    # Fallback: product book best bid/ask
    try:
        book = client.get("/api/v3/brokerage/product_book", params={"product_id": pid, "limit": 1})
        # Response shapes vary; support both flat and nested "pricebook"
        raw_bids = _get(book, "bids", []) or _get(_get(book, "pricebook", {}), "bids", [])
        raw_asks = _get(book, "asks", []) or _get(_get(book, "pricebook", {}), "asks", [])
        def first_price(side):
            if not side:
                return None
            head = side[0]
            p = _get(head, "price") if isinstance(head, dict) else (head[0] if isinstance(head, (list, tuple)) and head else None)
            return _to_decimal_maybe(p)
        b = first_price(raw_bids)
        a = first_price(raw_asks)
        if b is not None and a is not None:
            return (b + a) / 2
    except Exception as e:
        dbg(f"{pid} | /product_book fetch error: {e}")
    return None


def get_product_meta(pid: str) -> Dict[str, Decimal]:
    p = client.get_product(product_id=pid)
    return {
        "base_inc":  Decimal(p.base_increment),
        "base_ccy":  p.base_currency_id,
        "quote_ccy": p.quote_currency_id,
    }


# -----------------------------
# Fills and cost basis
# -----------------------------
def fetch_fills_pages(pid: str, portfolio_uuid: Optional[str], max_pages: int = 20) -> Iterable[list]:
    cursor = None
    for _ in range(max_pages):
        params = {"product_id": pid, "limit": 250}
        if cursor:
            params["cursor"] = cursor
        if portfolio_uuid:
            params["portfolio_id"] = portfolio_uuid
        res = client.get("/api/v3/brokerage/orders/historical/fills", params=params)
        fills = _get(res, "fills") or _get(res, "data") or getattr(res, "fills", []) or []
        def f_time(f): return _get(f, "trade_time") or _get(f, "created_at") or _get(f, "time") or ""
        fills_sorted = sorted(fills, key=f_time)
        if not fills_sorted:
            break
        yield fills_sorted
        cursor = _get(res, "cursor") or _get(res, "next_cursor") or _get(res, "next")
        if not cursor:
            break

def compute_avg_cost_for_balance(pid: str, portfolio_uuid: Optional[str]) -> Tuple[Optional[Decimal], int, Optional[Decimal]]:
    """
    Walk paginated fills oldest->newest using moving-average method.
    Returns (avg_cost, fills_consumed, last_buy_price).
    """
    units = Decimal("0")
    cost  = Decimal("0")
    consumed = 0
    last_buy_price: Optional[Decimal] = None

    for page in fetch_fills_pages(pid, portfolio_uuid, max_pages=20):
        for f in page:
            side = str(_get(f, "side", "")).upper()
            qty  = _to_decimal_maybe(_get(f, "size") or _get(f, "base_size") or _get(f, "filled_size"))
            price= _to_decimal_maybe(_get(f, "price"))
            if qty is None or price is None or qty <= 0 or price <= 0:
                continue
            if side == "BUY":
                units += qty
                cost  += qty * price
                last_buy_price = price
            elif side == "SELL" and units > 0:
                avg = cost / units
                consume = qty if qty <= units else units
                cost  -= avg * consume
                units -= consume
            consumed += 1
        if units > 0:
            return (cost / units, consumed, last_buy_price)

    return (None, consumed, last_buy_price)


# -----------------------------
# Trading
# -----------------------------
def place_sell_order(pid: str, size: Decimal, portfolio_uuid: Optional[str]) -> None:
    payload = {
        "product_id": pid,
        "side": "SELL",
        "order_configuration": {"market_market_ioc": {"base_size": f"{size.normalize():f}"}},
    }
    if portfolio_uuid:
        # supported by API; safe to include when present
        payload["portfolio_id"] = portfolio_uuid

    resp = client.post("/api/v3/brokerage/orders", data=payload)
    oid = (_get(resp, "order_id") or _get(resp, "orderId") or _get(_get(resp, "success_response", {}), "order_id"))
    log(f"{pid} | SELL ALL size={size} submitted (order {oid})")


# -----------------------------
# One scan iteration (with lots of logs)
# -----------------------------
def scan_once(portfolio_uuid: Optional[str]) -> None:
    try:
        accs = client.get_accounts()
    except Exception as e:
        log(f"get_accounts error: {e}")
        return

    inspected = 0
    nonzero   = 0

    for a in getattr(accs, "accounts", []):
        # Try to read portfolio on the account object if present
        a_pf = getattr(a, "portfolio_uuid", None) or _get(getattr(a, "hold", {}) if hasattr(a, "hold") else {}, "portfolio_uuid")
        if portfolio_uuid and a_pf != portfolio_uuid:
            continue

        sym = a.currency.upper()
        inspected += 1
        try:
            bal = Decimal(a.available_balance["value"])
        except Exception:
            dbg(f"{sym} | could not parse available_balance; skipping account row")
            continue

        dbg(f"{sym} | balance={bal} (portfolio={a_pf})")

        if sym == QUOTE_CURRENCY:
            dbg(f"{sym} | is quote currency; skip")
            continue

        if bal <= 0:
            dbg(f"{sym} | zero balance; skip")
            continue

        nonzero += 1

        pid = f"{sym}-{QUOTE_CURRENCY}"
        # Check product meta (increments, currency ids)
        try:
            meta = get_product_meta(pid)
        except Exception as e:
            log(f"{sym} | No product {pid} or meta error: {e}; skip.")
            continue

        # Compute average cost
        avg, used, last_buy = compute_avg_cost_for_balance(pid, portfolio_uuid)
        if avg is None:
            if FALLBACK_TO_LAST_BUY and last_buy is not None:
                avg = last_buy
                log(f"{pid} | Using last BUY price fallback avg={avg} after {used} fills.")
            else:
                log(f"{pid} | Unknown avg cost after {used} fills; skip.")
                continue

        # Fetch price
        px = price_for_product(pid)
        if px is None:
            log(f"{pid} | Could not fetch ticker/book price; skip.")
            continue

        gain = (px - avg) / avg
        log(f"{pid} | avg={avg:.8f} price={px:.8f} gain={float(gain)*100:.2f}%")

        if gain >= TARGET_PROFIT_PCT:
            # Re-read balance to respect increments right before order
            try:
                accs2 = client.get_accounts()
                base_bal = Decimal("0")
                for acc in accs2.accounts:
                    acc_pf = getattr(acc, "portfolio_uuid", None) or _get(getattr(acc, "hold", {}) if hasattr(acc, "hold") else {}, "portfolio_uuid")
                    if portfolio_uuid and acc_pf != portfolio_uuid:
                        continue
                    if acc.currency == _get(meta, "base_ccy"):
                        base_bal = Decimal(acc.available_balance["value"])
                        break
                size = round_to_inc(base_bal, _get(meta, "base_inc"))
                if size > 0:
                    place_sell_order(pid, size, portfolio_uuid)
                else:
                    log(f"{pid} | Balance rounds to 0 with increment; nothing to sell.")
            except Exception as e:
                log(f"{pid} | SELL flow error: {type(e).__name__}: {e}")

    log(f"Scan summary: inspected={inspected}, nonzero={nonzero}")


# -----------------------------
# Main loop with heartbeat
# -----------------------------
def main():
    pf = ensure_portfolio_uuid()
    scope_msg = f"portfolio={pf}" if pf else "portfolio=ALL"
    log(f"Started | target_profit={int(TARGET_PROFIT_PCT*100)}% | quote={QUOTE_CURRENCY} | {scope_msg}")

    loop = 0
    while True:
        loop += 1
        try:
            log(f"Heartbeat: loop={loop}, sleep={SLEEP_SEC}s, debug={'on' if DEBUG else 'off'}")
            scan_once(pf)
        except Exception as e:
            log(f"Top-level error: {type(e).__name__}: {e}")
        time.sleep(max(5, SLEEP_SEC))


if __name__ == "__main__":
    main()
