#!/usr/bin/env python3
import os
import time
import re
from decimal import Decimal, ROUND_DOWN, InvalidOperation
from datetime import datetime, timezone
from typing import Dict, Tuple, List, Any

from coinbase.rest import RESTClient

TARGET_PROFIT_PCT = float(os.getenv("TARGET_PROFIT_PCT", "0.10"))  # 10%
SLEEP_SEC         = int(os.getenv("SLEEP_SEC", "60"))
QUOTE_CURRENCY    = os.getenv("QUOTE_CURRENCY", "USD")
FALLBACK_TO_LAST_BUY = os.getenv("FALLBACK_TO_LAST_BUY", "0") not in ("0", "false", "False", "")

client = RESTClient()  # needs COINBASE_API_KEY / SECRET set in env

def log(msg: str):
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%SZ")
    print(f"[cb-sell-winners] {now} | {msg}", flush=True)

def _get(o: Any, k: str, default=None):
    if isinstance(o, dict):
        return o.get(k, default)
    return getattr(o, k, default)

def _to_decimal_maybe(v) -> Decimal | None:
    if v is None:
        return None
    # Accept numbers or numeric strings; strip non-numeric except dot and minus
    if isinstance(v, (int, float)):
        return Decimal(str(v))
    if isinstance(v, str):
        s = v.strip()
        # Some SDKs return nested objects or empty strings; guard.
        if not s:
            return None
        # If string has non-numeric chars, attempt to extract first number
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

def price_for_product(pid: str) -> Decimal | None:
    # Try the ticker endpoint, with multiple shapes supported
    try:
        res = client.get(f"/api/v3/brokerage/products/{pid}/ticker")
        # possible shapes: {'price': '123.45'} or {'ticker': {'price': '...'}} or list form
        price = _get(res, "price")
        if price is None:
            ticker = _get(res, "ticker")
            if ticker:
                price = _get(ticker, "price")
        px = _to_decimal_maybe(price)
        if px is not None:
            return px
        # Try mid of bid/ask if available
        bid = _to_decimal_maybe(_get(res, "bid"))
        ask = _to_decimal_maybe(_get(res, "ask"))
        if bid is not None and ask is not None:
            return (bid + ask) / 2
    except Exception as e:
        log(f"{pid} | ticker error: {e}")
    return None

def round_to_inc(value: Decimal, inc: Decimal) -> Decimal:
    if inc <= 0:
        return value
    return (value / inc).to_integral_value(rounding=ROUND_DOWN) * inc

def get_product_meta(pid: str) -> Dict[str, Decimal]:
    p = client.get_product(product_id=pid)
    return {
        "base_inc":  Decimal(p.base_increment),
        "base_ccy":  p.base_currency_id,
        "quote_ccy": p.quote_currency_id,
    }

def fetch_fills_pages(pid: str, max_pages: int = 20):
    """Yield pages of fills in chronological order (oldest -> newest)."""
    cursor = None
    for _ in range(max_pages):
        params = {"product_id": pid, "limit": 250}
        if cursor:
            params["cursor"] = cursor
        res = client.get("/api/v3/brokerage/orders/historical/fills", params=params)
        fills = res["fills"] if isinstance(res, dict) else getattr(res, "fills", [])
        # Sort within page just in case
        def f_time(f): return _get(f, "trade_time") or _get(f, "created_at") or _get(f, "time") or ""
        fills_sorted = sorted(fills, key=f_time)
        if not fills_sorted:
            break
        yield fills_sorted
        cursor = _get(res, "cursor") or _get(res, "next_cursor") or _get(res, "next")
        if not cursor:
            break

def compute_avg_cost_for_balance(pid: str, target_units: Decimal) -> Tuple[Decimal | None, int]:
    """
    Walk paginated fills oldest->newest using moving-average method until we have
    a nonzero resulting position (units>0). Return (avg_cost, fills_consumed).
    If units end at 0 even after all pages and target_units>0, cost is unknown.
    """
    units = Decimal("0")
    cost  = Decimal("0")
    consumed = 0
    last_buy_price = None

    for page in fetch_fills_pages(pid, max_pages=20):
        for f in page:
            side = str(_get(f, "side", "")).upper()
            qty  = _to_decimal_maybe(_get(f, "size") or _get(f, "base_size") or _get(f, "filled_size"))
            price= _to_decimal_maybe(_get(f, "price"))
            if qty is None or price is None or qty <= 0 or price <= 0:
                continue
            if side == "BUY":
                units += qty; cost += qty * price
                last_buy_price = price
            elif side == "SELL" and units > 0:
                avg = cost / units
                consume = min(qty, units)
                cost -= avg * consume
                units -= consume
            consumed += 1
        # If after processing this page we have some units, we can compute avg
        if units > 0:
            return (cost / units, consumed)

    # If we got here and still no units but we do hold balance, optionally fallback
    if FALLBACK_TO_LAST_BUY and last_buy_price is not None:
        return (last_buy_price, consumed)
    return (None, consumed)

def sell_all(pid: str):
    try:
        meta = get_product_meta(pid)
        base_bal = Decimal("0")
        accs = client.get_accounts()
        for a in accs.accounts:
            if a.currency == meta["base_ccy"]:
                base_bal = Decimal(a.available_balance["value"]); break
        if base_bal <= 0:
            log(f"{pid} | No {meta['base_ccy']} available; skip.")
            return
        size = round_to_inc(base_bal, meta["base_inc"])
        if size <= 0: 
            log(f"{pid} | Rounds to 0; skip.")
            return
        payload = {
            "product_id": pid,
            "side": "SELL",
            "order_configuration": {"market_market_ioc": {"base_size": f"{size.normalize():f}"}}
        }
        resp = client.post("/api/v3/brokerage/orders", data=payload)
        oid = (_get(resp, "order_id") or _get(resp, "orderId") or _get(resp, "success_response", {}).get("order_id"))
        log(f"{pid} | SELL ALL size={size} submitted (order {oid})")
    except Exception as e:
        log(f"{pid} | SELL failed: {type(e).__name__}: {e}")

def main():
    log(f"Started | target_profit={int(TARGET_PROFIT_PCT*100)}% | quote={QUOTE_CURRENCY}")
    while True:
        try:
            accs = client.get_accounts()
            for a in accs.accounts:
                sym = a.currency.upper()
                if sym == QUOTE_CURRENCY.upper():
                    continue
                try:
                    bal = Decimal(a.available_balance["value"])
                except Exception:
                    continue
                if bal <= 0:
                    continue
                pid = f"{sym}-{QUOTE_CURRENCY.upper()}"
                # Ensure product exists
                try:
                    _ = get_product_meta(pid)
                except Exception as e:
                    log(f"{sym} | No {pid} product or meta error: {e}; skip.")
                    continue

                avg, used = compute_avg_cost_for_balance(pid, bal)
                if avg is None:
                    log(f"{pid} | Unknown avg cost after {used} fills; skip.")
                    continue

                px = price_for_product(pid)
                if px is None:
                    log(f"{pid} | Could not fetch ticker price; skip.")
                    continue

                gain = (px - avg) / avg
                log(f"{pid} | avg={avg:.6f} price={px:.6f} gain={float(gain)*100:.2f}%")
                if gain >= TARGET_PROFIT_PCT:
                    sell_all(pid)
        except Exception as e:
            log(f"Top-level error: {type(e).__name__}: {e}")
        time.sleep(max(5, SLEEP_SEC))

if __name__ == "__main__":
    main()
