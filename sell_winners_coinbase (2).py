#!/usr/bin/env python3
import os
import time
import re
from decimal import Decimal, ROUND_DOWN, InvalidOperation
from datetime import datetime, timezone
from typing import Dict, Tuple, List, Any, Optional

from coinbase.rest import RESTClient

TARGET_PROFIT_PCT = float(os.getenv("TARGET_PROFIT_PCT", "0.10"))
SLEEP_SEC         = int(os.getenv("SLEEP_SEC", "60"))
QUOTE_CURRENCY    = os.getenv("QUOTE_CURRENCY", "USD")
FALLBACK_TO_LAST_BUY = os.getenv("FALLBACK_TO_LAST_BUY", "0") not in ("0", "false", "False", "")
PORTFOLIO_UUID    = os.getenv("PORTFOLIO_UUID", "").strip()
PORTFOLIO_NAME    = os.getenv("PORTFOLIO_NAME", "bot").strip() if not PORTFOLIO_UUID else ""

client = RESTClient()

def log(msg: str):
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%SZ")
    print(f"[cb-sell-winners] {now} | {msg}", flush=True)

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
        log(f"Portfolio named '{PORTFOLIO_NAME}' not found; using default (all).")
        return None
    except Exception as e:
        log(f"Error listing portfolios: {e}")
        return None

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
    except Exception:
        pass
    # Fallback: product book best bid/ask
    try:
        book = client.get("/api/v3/brokerage/product_book", params={"product_id": pid, "limit": 1})
        bids = _get(book, "bids", []) or _get(_get(book, "pricebook", {}), "bids", [])
        asks = _get(book, "asks", []) or _get(_get(book, "pricebook", {}), "asks", [])
        def first_price(side):
            if not side: return None
            p = _get(side[0], "price") if isinstance(side[0], dict) else side[0][0] if isinstance(side[0], (list, tuple)) else None
            return _to_decimal_maybe(p)
        b = first_price(bids); a = first_price(asks)
        if b is not None and a is not None:
            return (b + a) / 2
    except Exception as e:
        log(f"{pid} | price fetch error: {e}")
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

def fetch_fills_pages(pid: str, portfolio_uuid: Optional[str], max_pages: int = 20):
    cursor = None
    for _ in range(max_pages):
        params = {"product_id": pid, "limit": 250}
        if cursor:
            params["cursor"] = cursor
        if portfolio_uuid:
            params["portfolio_id"] = portfolio_uuid
        res = client.get("/api/v3/brokerage/orders/historical/fills", params=params)
        fills = res["fills"] if isinstance(res, dict) else getattr(res, "fills", [])
        def f_time(f): return _get(f, "trade_time") or _get(f, "created_at") or _get(f, "time") or ""
        fills_sorted = sorted(fills, key=f_time)
        if not fills_sorted:
            break
        yield fills_sorted
        cursor = _get(res, "cursor") or _get(res, "next_cursor") or _get(res, "next")
        if not cursor:
            break

def compute_avg_cost_for_balance(pid: str, target_units: Decimal, portfolio_uuid: Optional[str]) -> Tuple[Optional[Decimal], int]:
    units = Decimal("0")
    cost  = Decimal("0")
    consumed = 0
    last_buy_price = None
    for page in fetch_fills_pages(pid, portfolio_uuid, max_pages=20):
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
        if units > 0:
            return (cost / units, consumed)
    if FALLBACK_TO_LAST_BUY and last_buy_price is not None:
        return (last_buy_price, consumed)
    return (None, consumed)

def main():
    pf = ensure_portfolio_uuid()
    scope_msg = f"portfolio={pf}" if pf else "portfolio=ALL"
    log(f"Started | target_profit={int(TARGET_PROFIT_PCT*100)}% | quote={QUOTE_CURRENCY} | {scope_msg}")
    while True:
        try:
            # List accounts and filter by portfolio
            accs = client.get_accounts()
            for a in accs.accounts:
                # Some SDKs expose a.portfolio_uuid; otherwise it may be nested
                a_pf = getattr(a, "portfolio_uuid", None) or _get(getattr(a, "hold", {}) if hasattr(a, "hold") else {}, "portfolio_uuid")
                if pf and a_pf != pf:
                    continue
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
                try:
                    _ = get_product_meta(pid)
                except Exception as e:
                    log(f"{sym} | No {pid} product or meta error: {e}; skip.")
                    continue

                avg, used = compute_avg_cost_for_balance(pid, bal, pf)
                if avg is None:
                    log(f"{pid} | Unknown avg cost after {used} fills (portfolio scope applied); skip.")
                    continue

                px = price_for_product(pid)
                if px is None:
                    log(f"{pid} | Could not fetch ticker price; skip.")
                    continue

                gain = (px - avg) / avg
                log(f"{pid} | avg={avg:.6f} price={px:.6f} gain={float(gain)*100:.2f}%")
                if gain >= TARGET_PROFIT_PCT:
                    # place in the same portfolio by default; advanced trade routes per account/portfolio automatically
                    try:
                        # issue order with portfolio id if API supports; safe to omit if not
                        meta = client.get_product(product_id=pid)
                        # market order
                        # compute base balance again to respect increments
                        # (sell_all() refactor inline to include portfolio_uuid is more verbose; keep simple here)
                        pass
                    except Exception:
                        pass
                    # Reuse sell_all from previous version (without explicit portfolio param).
                    from decimal import Decimal as D
                    base_inc = D(str(meta.base_increment)) if hasattr(meta, "base_increment") else D("0.00000001")
                    # fetch balance again
                    base_bal = Decimal("0")
                    accounts_now = client.get_accounts()
                    for acc in accounts_now.accounts:
                        acc_pf = getattr(acc, "portfolio_uuid", None)
                        if pf and acc_pf != pf:
                            continue
                        if acc.currency == meta.base_currency_id:
                            base_bal = Decimal(acc.available_balance["value"]); break
                    size = round_to_inc(base_bal, base_inc)
                    if size > 0:
                        payload = {
                            "product_id": pid,
                            "side": "SELL",
                            "order_configuration": {"market_market_ioc": {"base_size": f"{size.normalize():f}"}}
                        }
                        if pf:
                            payload["portfolio_id"] = pf
                        resp = client.post("/api/v3/brokerage/orders", data=payload)
                        oid = (_get(resp, "order_id") or _get(resp, "orderId") or _get(resp, "success_response", {}).get("order_id"))
                        log(f"{pid} | SELL ALL size={size} submitted (order {oid})")
        except Exception as e:
            log(f"Top-level error: {type(e).__name__}: {e}")
        time.sleep(max(5, SLEEP_SEC))

def round_to_inc(value: Decimal, inc: Decimal) -> Decimal:
    if inc <= 0:
        return value
    return (value / inc).to_integral_value(rounding=ROUND_DOWN) * inc

if __name__ == "__main__":
    main()
