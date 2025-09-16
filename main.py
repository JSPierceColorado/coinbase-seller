#!/usr/bin/env python3
"""
Coinbase Advanced Trade — "sell winners" bot (fixed cost-basis + sturdier quotes).

What's fixed / improved
- Cost basis now computed from the FULL fill history (all pages), globally
  sorted oldest→newest. No early return after a page.
- Fewer "avg=unknown" skips: we scan everything and only return None when the
  final net units are 0 from the fills we could retrieve.
- Fresh price re-check just before selling to avoid dumping after a quick dip.
- More robust price fetching: tries /ticker, then best bid/ask, then last trade.
- Logging remains concise and backward-compatible.

Behavior
- Optionally scopes to a single portfolio (PORTFOLIO_UUID preferred; else PORTFOLIO_NAME="bot").
- Iterates non-zero balances (excluding quote currency).
- Rebuilds moving-average cost basis from fills (all pages, oldest->newest).
- If (price - avg_cost)/avg_cost >= TARGET_PROFIT_PCT, sells 100% with market IOC.

Logs (concise)
- Per-asset evaluation line:
    <PID> | bal=<base> avg=<avg or unknown> px=<price or unknown> gain=<%> target=<%> -> <action>
- SELL lines:
    <PID> | SELL size=<size> (order <id>)   OR   <PID> | SELL error: <reason>

Env vars:
  COINBASE_API_KEY, COINBASE_API_SECRET
  TARGET_PROFIT_PCT=0.10
  SLEEP_SEC=60
  QUOTE_CURRENCY=USD
  PORTFOLIO_UUID=<uuid>            # preferred
  PORTFOLIO_NAME=bot               # used only if UUID not set
  FALLBACK_TO_LAST_BUY=1           # optional; use last BUY price if avg unknown
  MAX_FILL_PAGES=200               # pagination cap for fills (250 fills/page)
"""

import os
import re
import time
import uuid
from decimal import Decimal, ROUND_DOWN, InvalidOperation
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple

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
MAX_FILL_PAGES    = int(os.getenv("MAX_FILL_PAGES", "200"))

client = RESTClient()  # requires COINBASE_API_KEY / COINBASE_API_SECRET

# -----------------------------
# Minimal logging
# -----------------------------
def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%SZ")

def log(msg: str) -> None:
    print(f"[cb-sell-winners] {_now()} | {msg}", flush=True)

# -----------------------------
# Small utilities
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
    if inc is None or inc <= 0:
        return value
    return (value / inc).to_integral_value(rounding=ROUND_DOWN) * inc

# -----------------------------
# Portfolio scoping
# -----------------------------
def ensure_portfolio_uuid() -> Optional[str]:
    """Return a portfolio UUID. Prefer existing env; otherwise look up by name."""
    global PORTFOLIO_UUID
    if PORTFOLIO_UUID:
        return PORTFOLIO_UUID
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
        log(f"Portfolio named '{PORTFOLIO_NAME}' not found; defaulting to ALL.")
    except Exception as e:
        log(f"Error listing portfolios: {e}")
    return None

# -----------------------------
# Market data
# -----------------------------
def price_for_product(pid: str) -> Optional[Decimal]:
    """Return a best-effort mid/last price."""
    # 1) Try the /ticker endpoint
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

    # 2) Try best bid/ask from product_book
    try:
        book = client.get("/api/v3/brokerage/product_book", params={"product_id": pid, "limit": 1})
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
    except Exception:
        pass

    # 3) Final fallback: last trade
    try:
        trades = client.get(f"/api/v3/brokerage/products/{pid}/trades", params={"limit": 1})
        tlist = _get(trades, "trades") or _get(trades, "data") or []
        if tlist:
            last = tlist[0]
            p = _to_decimal_maybe(_get(last, "price"))
            if p is not None:
                return p
    except Exception:
        pass

    return None

def get_product_meta(pid: str) -> Dict[str, Decimal]:
    p = client.get_product(product_id=pid)
    # Some SDKs expose .base_increment, etc.; normalize to Decimals
    return {
        "base_inc":  Decimal(str(getattr(p, "base_increment", "0.00000001"))),
        "base_ccy":  str(getattr(p, "base_currency_id", pid.split("-")[0])),
        "quote_ccy": str(getattr(p, "quote_currency_id", pid.split("-")[-1])),
    }

# -----------------------------
# Fills & cost basis (FIXED)
# -----------------------------
def _fill_time_key(f) -> str:
    return str(_get(f, "trade_time") or _get(f, "created_at") or _get(f, "time") or "")

def fetch_all_fills(pid: str, portfolio_uuid: Optional[str], max_pages: int = MAX_FILL_PAGES) -> List[dict]:
    cursor = None
    out: List[dict] = []
    for _ in range(max_pages):
        params = {"product_id": pid, "limit": 250}
        if cursor:
            params["cursor"] = cursor
        if portfolio_uuid:
            params["retail_portfolio_id"] = portfolio_uuid
        res = client.get("/api/v3/brokerage/orders/historical/fills", params=params)
        fills = _get(res, "fills") or _get(res, "data") or getattr(res, "fills", []) or []
        if not fills:
            break
        out.extend(fills)
        cursor = _get(res, "cursor") or _get(res, "next_cursor") or _get(res, "next")
        if not cursor:
            break
    out.sort(key=_fill_time_key)  # GLOBAL oldest→newest
    return out

def compute_avg_cost_for_balance(pid: str, portfolio_uuid: Optional[str]) -> Tuple[Optional[Decimal], int, Optional[Decimal]]:
    """
    True moving-average over the FULL fill history (oldest→newest).
    Returns (avg_cost, fills_consumed, last_buy_price).
    """
    fills = fetch_all_fills(pid, portfolio_uuid, max_pages=MAX_FILL_PAGES)

    units = Decimal("0")
    cost  = Decimal("0")
    last_buy_price: Optional[Decimal] = None
    consumed = 0

    for f in fills:
        side  = str(_get(f, "side", "")).upper()
        qty   = _to_decimal_maybe(_get(f, "size") or _get(f, "base_size") or _get(f, "filled_size"))
        price = _to_decimal_maybe(_get(f, "price"))
        if qty is None or price is None or qty <= 0 or price <= 0:
            continue

        if side == "BUY":
            units += qty
            cost  += qty * price
            last_buy_price = price
        elif side == "SELL" and units > 0:
            # Reduce cost using current moving-average
            avg = cost / units
            consume = qty if qty <= units else units
            cost  -= avg * consume
            units -= consume

        consumed += 1

    if units > 0:
        return (cost / units, consumed, last_buy_price)
    else:
        return (None, consumed, last_buy_price)

# -----------------------------
# Accounts listing & normalize
# -----------------------------
def list_accounts(portfolio_uuid: Optional[str]):
    """
    Prefer portfolio-scoped accounts via REST; fallback to unscoped SDK call.
    Returns a list of raw dicts or SDK objects.
    """
    if portfolio_uuid:
        try:
            res = client.get("/api/v3/brokerage/accounts", params={"limit": 250, "retail_portfolio_id": portfolio_uuid})
            accounts = _get(res, "accounts") or getattr(res, "accounts", []) or []
            if accounts:
                return accounts
        except Exception:
            pass
    try:
        accs = client.get_accounts()
        accounts = getattr(accs, "accounts", []) or []
        return accounts
    except Exception as e:
        log(f"get_accounts error: {e}")
        return []

def normalize_account(a: Any) -> dict:
    """
    Return a dict with at least {currency, available_balance, portfolio_uuid}.
    Handles both raw dicts (REST) and SDK objects (get_accounts()).
    """
    if isinstance(a, dict):
        return {
            "currency": str(a.get("currency", "")).upper(),
            "available_balance": a.get("available_balance"),
            "portfolio_uuid": a.get("portfolio_uuid"),
        }
    else:
        return {
            "currency": str(getattr(a, "currency", "")).upper(),
            "available_balance": getattr(a, "available_balance", None),
            "portfolio_uuid": getattr(a, "portfolio_uuid", None),
        }

# -----------------------------
# Order placement
# -----------------------------
def place_sell_order(pid: str, size: Decimal, portfolio_uuid: Optional[str]) -> None:
    # Valid, short client order id
    coid = uuid.uuid4().hex  # 32 lowercase hex chars

    payload = {
        "client_order_id": coid,
        "product_id": pid,
        "side": "SELL",
        "order_configuration": {"market_market_ioc": {"base_size": f"{size.normalize():f}"}},
    }
    # Include only when present; do NOT send empty/None fields.
    if portfolio_uuid:
        payload["retail_portfolio_id"] = portfolio_uuid

    resp = client.post("/api/v3/brokerage/orders", data=payload)

    oid = (_get(resp, "order_id")
           or _get(resp, "orderId")
           or _get(_get(resp, "success_response", {}), "order_id"))
    log(f"{pid} | SELL size={size} (order {oid})")

# -----------------------------
# One scan iteration (concise logs)
# -----------------------------
def scan_once(portfolio_uuid: Optional[str]) -> None:
    accounts = list_accounts(portfolio_uuid)

    for raw in accounts:
        a = normalize_account(raw)

        # Only filter by portfolio if we KNOW the account's portfolio and it doesn't match.
        if portfolio_uuid and a.get("portfolio_uuid") and a["portfolio_uuid"] != portfolio_uuid:
            continue

        sym = a["currency"]
        if sym == QUOTE_CURRENCY:
            continue

        # balance
        bal = None
        bal_val = a["available_balance"]
        try:
            if isinstance(bal_val, dict):
                bal = Decimal(str(bal_val.get("value")))
            elif bal_val is not None:
                bal = Decimal(str(bal_val))
        except Exception:
            continue

        if bal is None or bal <= 0:
            continue

        pid = f"{sym}-{QUOTE_CURRENCY}"

        # Product meta
        try:
            meta = get_product_meta(pid)
        except Exception:
            log(f"{pid} | bal={bal} avg=unknown px=unknown gain=0.00% target={int(TARGET_PROFIT_PCT*100)}% -> skip (no product)")
            continue

        # Average cost over ALL fills
        avg, used, last_buy = compute_avg_cost_for_balance(pid, portfolio_uuid)
        if avg is None:
            if FALLBACK_TO_LAST_BUY and last_buy is not None:
                avg = last_buy
            else:
                log(f"{pid} | bal={bal} avg=unknown px=unknown gain=0.00% target={int(TARGET_PROFIT_PCT*100)}% -> skip")
                continue

        # Price (initial)
        px = price_for_product(pid)
        if px is None or avg <= 0:
            log(f"{pid} | bal={bal} avg={avg if avg is not None else 'unknown'} px=unknown gain=0.00% target={int(TARGET_PROFIT_PCT*100)}% -> skip")
            continue

        gain = (px - avg) / avg
        gain_pct = float(gain) * 100.0
        action = "SELL" if gain >= TARGET_PROFIT_PCT else "skip"
        log(f"{pid} | bal={bal} avg={avg:.8f} px={px:.8f} gain={gain_pct:.2f}% target={int(TARGET_PROFIT_PCT*100)}% -> {action}")

        if gain >= TARGET_PROFIT_PCT:
            try:
                # Fresh read for most recent base balance just before order
                accounts_now = list_accounts(portfolio_uuid)
                base_bal = Decimal("0")
                for r in accounts_now:
                    acc = normalize_account(r)
                    if portfolio_uuid and acc.get("portfolio_uuid") and acc["portfolio_uuid"] != portfolio_uuid:
                        continue
                    if acc["currency"] == _get(meta, "base_ccy"):
                        bv = acc["available_balance"]
                        if isinstance(bv, dict):
                            base_bal = Decimal(str(bv.get("value")))
                        elif bv is not None:
                            base_bal = Decimal(str(bv))
                        break

                size = round_to_inc(base_bal, _get(meta, "base_inc"))

                # Re-check price and threshold right before placing order
                px2 = price_for_product(pid)
                if px2 is None:
                    log(f"{pid} | SELL aborted: no fresh price")
                    continue
                gain2 = (px2 - avg) / avg
                if gain2 < TARGET_PROFIT_PCT:
                    log(f"{pid} | SELL aborted: gain dropped to {float(gain2)*100.0:.2f}%")
                    continue

                if size > 0:
                    place_sell_order(pid, size, portfolio_uuid)
                else:
                    log(f"{pid} | SELL size=0 (rounding) -> skip")
            except Exception as e:
                log(f"{pid} | SELL error: {type(e).__name__}: {e}")

# -----------------------------
# Main loop
# -----------------------------
def main():
    pf = ensure_portfolio_uuid()
    scope_msg = pf if pf else "ALL"
    log(f"Started | target={int(TARGET_PROFIT_PCT*100)}% | quote={QUOTE_CURRENCY} | portfolio={scope_msg}")

    loop = 0
    while True:
        loop += 1
        try:
            scan_once(pf)
        except Exception as e:
            log(f"Top-level error: {type(e).__name__}: {e}")
        time.sleep(max(5, SLEEP_SEC))

if __name__ == "__main__":
    main()
