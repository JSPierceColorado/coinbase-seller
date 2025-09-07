#!/usr/bin/env python3
# Sell winners on Coinbase Advanced Trade every N seconds.
# - Iterates nonzero crypto balances.
# - For each symbol, maps to PRODUCT_ID "<BASE>-USD" (configurable QUOTE via QUOTE_CURRENCY).
# - Computes moving-average cost basis from recent fills (buy/sell) and sells 100% if
#   current price >= avg_cost * (1 + TARGET_PROFIT_PCT).
# - Logs for Railway.

import os
import time
from decimal import Decimal, ROUND_DOWN
from datetime import datetime, timezone
from typing import Dict

from coinbase.rest import RESTClient

TARGET_PROFIT_PCT = float(os.getenv("TARGET_PROFIT_PCT", "0.10"))  # 10%
SLEEP_SEC         = int(os.getenv("SLEEP_SEC", "60"))
QUOTE_CURRENCY    = os.getenv("QUOTE_CURRENCY", "USD")             # pairs like BTC-USD

client = RESTClient()  # requires COINBASE_API_KEY / COINBASE_API_SECRET

def log(msg: str):
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%SZ")
    print(f"[cb-sell-winners] {now} | {msg}", flush=True)

def _get(obj, key, default=None):
    if hasattr(obj, key):
        return getattr(obj, key, default)
    if isinstance(obj, dict):
        return obj.get(key, default)
    return default

def price_for_product(pid: str) -> Decimal | None:
    try:
        # Ticker has "price"
        res = client.get(f"/api/v3/brokerage/products/{pid}/ticker")
        price = _get(res, "price")
        return Decimal(str(price))
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

def fetch_fills_for_product(pid: str, limit: int = 250):
    try:
        res = client.get("/api/v3/brokerage/orders/historical/fills", params={"product_id": pid, "limit": limit})
        fills = res["fills"] if isinstance(res, dict) else getattr(res, "fills", [])
        def f_time(f): return _get(f, "trade_time") or _get(f, "created_at") or _get(f, "time") or ""
        return sorted(fills, key=f_time)
    except Exception as e:
        log(f"{pid} | fills fetch error: {e}")
        return []

def compute_avg_cost_from_fills(fills) -> Decimal | None:
    units = Decimal("0")
    cost  = Decimal("0")
    for f in fills:
        side = str(_get(f, "side", "")).upper()
        qty_s  = _get(f, "size") or _get(f, "base_size") or _get(f, "filled_size") or "0"
        price_s= _get(f, "price") or "0"
        try:
            qty = Decimal(str(qty_s))
            price = Decimal(str(price_s))
        except Exception:
            continue
        if qty <= 0 or price <= 0:
            continue
        if side == "BUY":
            units += qty; cost += qty * price
        elif side == "SELL":
            if units <= 0: continue
            avg = cost / units
            consume = min(qty, units)
            cost -= avg * consume
            units -= consume
    if units > 0:
        return cost / units
    return None

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
                if sym == QUOTE_CURRENCY.upper():  # skip quote balance
                    continue
                try:
                    bal = Decimal(a.available_balance["value"])
                except Exception:
                    continue
                if bal <= 0:  # nothing to sell
                    continue
                pid = f"{sym}-{QUOTE_CURRENCY.upper()}"
                # verify product exists
                try:
                    _ = get_product_meta(pid)
                except Exception as e:
                    log(f"{sym} | No {pid} product or meta error: {e}; skip.")
                    continue
                fills = fetch_fills_for_product(pid)
                avg = compute_avg_cost_from_fills(fills)
                if avg is None:
                    log(f"{pid} | Unknown avg cost; skip for safety.")
                    continue
                px = price_for_product(pid)
                if px is None:
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
