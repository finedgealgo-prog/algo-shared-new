"""
client.py (Delta Exchange)
─────────────────────────────
Crypto option-chain REST client — ported near-verbatim from the old
system's shared/features/delta_exchange_client.py (list_products/
list_expiries/resolve_delta_expiry/build_chain_from_tickers/
fetch_option_chain_rest), per explicit instruction to use that SAME,
already-working mechanism rather than inventing a new one. Only the
underlying naming is adapted: algo-2_0 uses the full ticker ("BTCUSD"/
"ETHUSD", matching DeltaFeed/ltp_cache token convention) everywhere else,
Delta's own REST API wants the short asset symbol ("BTC"/"ETH") — see
UNDERLYING_TO_ASSET below.

Why this exists (the actual gap it closes): algo-2_0's `instrument_master`
only ever loads NSE data (`active_option_tokens`, broker="dhan") — it has
ZERO crypto strikes/expiries, by design (a Delta option-chain source was
explicitly out of scope when instrument_master was first built). A crypto
leg's strike can therefore never be resolved through instrument_master +
ltp_cache the way an NSE leg's is — this module is the crypto-side
equivalent data source, queried live (no Mongo caching) exactly like the
old system does, not a static prewarmed catalog.

fetch_option_chain_rest's own docstring explains why REST (not WS-subscribe-
and-wait) is the right choice here: "a single, bounded, predictable call
(~100-300ms typically) instead of a variable multi-hop chain — entry price
resolution no longer needs to gamble on whether WS subscription happened to
warm up in time; worst case is always this one call, not an uncertain wait."
Same reasoning applies to algo-2_0's synchronous, immediate
"/strategy/{id}/activate" — a WS subscribe+wait (this file's sibling
problem on the NSE side, see services/strategy_activation.py's
_ensure_live_prices) has no equivalent bounded worst-case; this REST call does.

Public endpoints only — no signature/credentials needed for chain/price data.
"""

from __future__ import annotations

import re
import socket
from datetime import date, datetime, timedelta, timezone
from typing import Any

import requests
import urllib3.util.connection as _urllib3_connection

# Same IPv4-forcing fix as the old client — Delta's API-key IP allowlist
# only ever had the IPv4 address on it; the default dual-stack resolution
# picks IPv6 first on most hosts. These endpoints are public (no signed
# calls here), so this isn't strictly required for THIS module alone, but
# forcing it is harmless and keeps behavior identical to the old, working
# integration rather than leaving it to chance.
_urllib3_connection.allowed_gai_family = lambda: socket.AF_INET

DELTA_REST_BASE = "https://api.india.delta.exchange"
_HTTP_TIMEOUT = 6.0

# Delta quotes option mark_price per 1 unit of the underlying, not per
# actual contract — DELTA_CONTRACT_VALUE scales it down once, here, to the
# real per-contract USD price (0.001 BTC / 0.01 ETH per contract) — same
# verified scaling the old client uses (see that module's own long comment:
# cross-checked against an independent Black-Scholes calc and a third-party
# platform's displayed max-profit for the same position).
DELTA_CONTRACT_VALUE: dict[str, float] = {"BTC": 0.001, "ETH": 0.01}

# Delta India settles/reports P&L at a fixed ₹85 per $1 — same constant the
# algo-admin frontend uses (utils/deltaPnl.ts DELTA_USD_INR_RATE). The risk
# engine needs it so crypto MTM is in the same ₹ unit as every configured
# OverallSL/OverallTgt/broker SL/Target amount.
DELTA_USD_INR_RATE = 85.0

# algo-2_0's own token/underlying convention (DeltaFeed, ltp_cache, LegRuntime.token)
# uses the full ticker; Delta's REST API wants the short asset symbol.
UNDERLYING_TO_ASSET: dict[str, str] = {"BTCUSD": "BTC", "ETHUSD": "ETH"}

# Matches a real Delta OPTION contract symbol (e.g. "C-BTC-82000-180926") —
# same shape as api/live_broadcast.py's _is_delta_token option-half check
# (algo.trade side), kept here too as the canonical version for anything
# on the shared/broker side (gap_recovery.py's Delta historical fetch)
# that needs to tell "is this token a real Delta symbol" apart from an
# NSE security_id, which is always purely numeric.
_OPTION_SYMBOL_RE = re.compile(r"^[CP]-(BTC|ETH)-\d+-\d+$")


def normalize_underlying(ticker: str) -> str:
    """AlgoTest-exported crypto strategies carry Ticker "DELTA_BTCUSD" /
    "DELTA_ETHUSD"; the engine (DeltaFeed, ltp_cache, strike resolution)
    keys crypto underlyings as plain "BTCUSD"/"ETHUSD"."""
    text = str(ticker or "").strip().upper()
    if text.startswith("DELTA_") and text[len("DELTA_"):] in UNDERLYING_TO_ASSET:
        return text[len("DELTA_"):]
    return text


def is_crypto_underlying_token(token: str) -> bool:
    """True for a bare underlying perpetual ("BTCUSD"/"ETHUSD") or a real
    Delta option contract symbol ("C-BTC-82000-180926") — false for
    anything else (an NSE numeric security_id, an empty string, ...)."""
    text = str(token or "").strip().upper()
    return text in UNDERLYING_TO_ASSET or bool(_OPTION_SYMBOL_RE.match(text))


def asset_for_token(token: str) -> str:
    """Short asset ("BTC"/"ETH") for an underlying ticker ("BTCUSD") or a
    Delta option symbol ("C-BTC-82000-180926"); "" for anything else."""
    text = str(token or "").strip().upper()
    if text in UNDERLYING_TO_ASSET:
        return UNDERLYING_TO_ASSET[text]
    match = _OPTION_SYMBOL_RE.match(text)
    return match.group(1) if match else ""


def pnl_multiplier(underlying: str = "", token: str = "") -> float:
    """₹ P&L per 1 point of raw quote per contract — contract_value × USD→INR
    for a Delta instrument, 1.0 for everything else (NSE qty is already
    lots × lot_size in ₹)."""
    asset = asset_for_token(underlying) or asset_for_token(token)
    if not asset:
        return 1.0
    return DELTA_CONTRACT_VALUE[asset] * DELTA_USD_INR_RATE


def _get(path: str, params: dict | None = None) -> dict:
    resp = requests.get(f"{DELTA_REST_BASE}{path}", params=params or {}, timeout=_HTTP_TIMEOUT)
    resp.raise_for_status()
    return resp.json()


def list_products(asset: str) -> list[dict]:
    """Live call/put option products for the given short asset symbol (e.g. 'BTC')."""
    data = _get("/v2/products", {
        "contract_types": "call_options,put_options",
        "underlying_asset_symbols": asset,
        "states": "live",
    })
    return data.get("result", []) if data.get("success") else []


def list_expiries(asset: str) -> list[str]:
    """Sorted expiry dates (DD-MM-YYYY, Delta's own query-param format),
    derived from the live product list's symbol suffix (DDMMYY). Sorted by
    a YYMMDD key, not the raw DDMMYY suffix — lexicographic DDMMYY sort is
    NOT chronological once expiries span more than one month (see the old
    client's own note: "040926" < "220826" lexicographically, but 04-Sep is
    AFTER 22-Aug)."""
    products = list_products(asset)
    seen: dict[str, str] = {}
    for p in products:
        symbol = p.get("symbol", "")
        suffix = symbol.rsplit("-", 1)[-1]  # DDMMYY
        if len(suffix) != 6 or not suffix.isdigit():
            continue
        dd, mm, yy = suffix[0:2], suffix[2:4], suffix[4:6]
        sort_key = f"{yy}{mm}{dd}"  # YYMMDD — sorts chronologically
        seen[sort_key] = f"{dd}-{mm}-20{yy}"
    return [seen[k] for k in sorted(seen.keys())]


def _parse_delta_expiry_date(expiry_str: str) -> date | None:
    try:
        dd, mm, yyyy = str(expiry_str or "").strip().split("-")
        return date(int(yyyy), int(mm), int(dd))
    except Exception:
        return None


# Delta Exchange India options expire/settle at 17:30 IST on their expiry date.
DELTA_EXPIRY_HOUR_IST, DELTA_EXPIRY_MINUTE_IST = 17, 30
_IST = timezone(timedelta(hours=5, minutes=30))


def delta_expiry_at(expiry_str: str) -> datetime | None:
    """Exact expiry instant (17:30 IST) of a DD-MM-YYYY Delta expiry."""
    d = _parse_delta_expiry_date(expiry_str)
    if d is None:
        return None
    return datetime(d.year, d.month, d.day, DELTA_EXPIRY_HOUR_IST, DELTA_EXPIRY_MINUTE_IST, tzinfo=_IST)


def resolve_delta_expiry(trade_date: str, expiry_kind: str, expiries: list[str], now: datetime | None = None) -> str | None:
    """Picks the Delta expiry (DD-MM-YYYY) for a leg's ExpiryKind at the
    moment of entry.

    Only contracts that have NOT yet expired at `now` (default: current IST
    time) are candidates — a contract expires at 17:30 IST on its date, so
    at 15:00 today's contract is still live, at 18:00 it is gone.

      ExpiryType.Today    -> the nearest still-live expiry (today's before
                             17:30 IST, the next day's after it)
      ExpiryType.Tomorrow -> the one after that
      Weekly/NextWeekly/Monthly/NextMonthly -> calendar arithmetic from
                             `trade_date` (YYYY-MM-DD) as before.
    Any unrecognised kind -> nearest still-live expiry."""
    if not expiries:
        return None
    now = now or datetime.now(_IST)
    try:
        trade_dt = datetime.strptime(str(trade_date or "")[:10], "%Y-%m-%d").date()
    except Exception:
        trade_dt = now.astimezone(_IST).date()

    parsed = [(e, _parse_delta_expiry_date(e)) for e in expiries]
    parsed = [pair for pair in parsed if pair[1] is not None]
    if not parsed:
        return expiries[0]
    parsed.sort(key=lambda pair: pair[1])

    live = [pair for pair in parsed if (delta_expiry_at(pair[0]) or now) > now]
    future = [pair for pair in live if pair[1] >= trade_dt] or live
    if not future:
        return parsed[-1][0]

    kind = str(expiry_kind or "").strip()

    if kind == "ExpiryType.Tomorrow":
        return future[1][0] if len(future) > 1 else future[0][0]

    if kind in ("ExpiryType.Weekly", "ExpiryType.NextWeekly"):
        target = trade_dt + timedelta(days=7)
        weekly = [pair for pair in future if pair[1] >= target]
        if kind == "ExpiryType.Weekly":
            return weekly[0][0] if weekly else future[-1][0]
        return weekly[1][0] if len(weekly) > 1 else (weekly[0][0] if weekly else future[-1][0])

    if kind in ("ExpiryType.Monthly", "ExpiryType.NextMonthly"):
        cur_mo = (trade_dt.year, trade_dt.month)
        if kind == "ExpiryType.Monthly":
            this_month = [pair for pair in future if (pair[1].year, pair[1].month) == cur_mo]
            return this_month[-1][0] if this_month else future[-1][0]
        nxt_mo = (trade_dt.year + 1, 1) if trade_dt.month == 12 else (trade_dt.year, trade_dt.month + 1)
        next_month = [pair for pair in future if (pair[1].year, pair[1].month) == nxt_mo]
        return next_month[-1][0] if next_month else future[-1][0]

    # ExpiryType.Today and any unrecognised kind — nearest still-live expiry.
    return future[0][0]


def _leg_row(ticker: dict) -> dict:
    quotes = ticker.get("quotes") or {}
    greeks = ticker.get("greeks") or {}
    return {
        "token": ticker.get("symbol", ""),
        "symbol": ticker.get("symbol", ""),
        "strike": float(ticker.get("strike_price") or 0),
        # RAW per-1-underlying-unit quote, deliberately NOT contract_value-
        # scaled — this is the SAME "mark_raw" convention the old system's
        # own delta_exchange_client.py actually stores into entry_trade.price
        # (see that module's _leg_row: it keeps a separate scaled "ltp" for
        # internal chain-comparison math AND an unscaled "mark_raw" — the
        # UNSCALED one is what resolve_live_pending_entry_snapshot picks for
        # entry_trade.price, per delta_event.py's own comment: "NOT contract_
        # value-scaled: callers that need a real $ figure (MTM, margin) must
        # apply DELTA_CONTRACT_VALUE themselves"). algo-2_0 has only ONE ltp
        # field (no separate raw/scaled split) and it flows straight into
        # entry_price/LTP display, so it must be the raw one — the
        # unmodified algo-admin frontend (strategyLegPnl.ts/deltaPnl.ts)
        # already applies DELTA_CONTRACT_VALUE itself when computing $ P&L
        # from entry_price/ltp; pre-scaling it here as well double-scaled
        # every crypto premium by contract_value (confirmed live: a real
        # BTC leg showed Entry Price/LTP as ~0.52 instead of Delta's real
        # ~532 quote — off by exactly 1/contract_value, matching the
        # reference fast-forward page's own display exactly once removed).
        "ltp": float(ticker.get("mark_price") or 0),
        "delta": float(greeks.get("delta") or 0) if greeks.get("delta") is not None else 0.0,
        "gamma": float(greeks.get("gamma") or 0) if greeks.get("gamma") is not None else 0.0,
        "theta": float(greeks.get("theta") or 0) if greeks.get("theta") is not None else 0.0,
        "vega": float(greeks.get("vega") or 0) if greeks.get("vega") is not None else 0.0,
        "iv": float(quotes.get("mark_iv") or 0) if quotes.get("mark_iv") is not None else 0.0,
    }


def fetch_option_chain_rest(underlying: str, expiry: str, option_type: str) -> list[dict]:
    """Cold REST fetch — pulls the live ticker set (strike/ltp/greeks, one
    row per live-quoted contract) for one (underlying, expiry, option_type)
    in ONE bounded REST round trip. `underlying` is the full ticker
    ("BTCUSD"); `expiry` must already be resolved (DD-MM-YYYY, see
    resolve_delta_expiry) — this never re-resolves it itself (that would be
    a second, redundant REST round trip on every call, exactly what the old
    system's own fetch_option_chain_rest was fixed to stop doing — see this
    module's docstring). Returns [] on any failure or if `underlying` isn't
    a known Delta asset — never raises."""
    asset = UNDERLYING_TO_ASSET.get(underlying.upper())
    if not asset or not expiry:
        return []
    contract_type = "call_options" if option_type.upper() == "CE" else "put_options"
    try:
        data = _get("/v2/tickers", {
            "contract_types": contract_type,
            "underlying_asset_symbols": asset,
            "expiry_date": expiry,
        })
    except Exception:
        return []
    tickers = data.get("result", []) if data.get("success") else []
    rows = [_leg_row(t) for t in tickers if t.get("underlying_asset_symbol") == asset]
    rows.sort(key=lambda r: r["strike"])
    return rows
