"""
historical.py (Delta)
────────────────────────
Crypto counterpart of shared/brokers/dhan/historical.py — used ONLY by
services/gap_recovery.py's startup TSL gap recovery (never the tick path).
Same public contract as the Dhan module (fetch_intraday_candles returning
the SAME [{"ts", "open", "high", "low", "close"}, ...] shape) so gap_
recovery.py can branch NSE-vs-crypto purely on which of these two modules
it calls, not on two different candle shapes.

GET /v2/history/candles works directly against an OPTION CONTRACT's own
symbol (e.g. "C-BTC-82000-180926"), not just Delta's special spot-index
symbols (shared/features/delta_exchange_client.py's fetch_candles only
ever used the latter, for spot/index charting) — confirmed live against
Delta's real public API: a real option symbol returns real per-minute mark-
price OHLC bars (volume mostly 0 — this is Delta's continuously-published
mark price, not sparse real-trade prints, which is exactly the continuous
series TSL gap-replay needs; the old system's own module never needed this
because it never re-priced a specific option contract's own history, only
the underlying's spot/index price). Public endpoint, no signing needed —
same as every other Delta REST call in this codebase.
"""

from __future__ import annotations

from datetime import datetime, timezone

from shared.brokers.delta.client import _get, is_crypto_underlying_token
from shared.logging.logger import get_logger

log = get_logger(__name__)

_RESOLUTION = "1m"


def resolve_instrument_type(token: str) -> tuple[str, str] | None:
    """Dhan-module-shaped signature (gap_recovery.py calls this the same
    way for either broker) — Delta has no separate instrument_type/
    exchange_segment concept, so this just confirms the token IS a real
    Delta symbol (option contract or perpetual) worth fetching history
    for, returning a placeholder pair when it is. Returns None for
    anything else (an NSE token; gap_recovery.py's own NSE path already
    handles those via shared/brokers/dhan/historical.py instead)."""
    if not is_crypto_underlying_token(token):
        return None
    return "DELTA", "DELTA"


def fetch_intraday_candles(
    token: str, exchange_segment: str, instrument_type: str,
    from_dt: datetime, to_dt: datetime,
) -> list[dict]:
    """Same shape/contract as dhan/historical.py's fetch_intraday_candles —
    `exchange_segment`/`instrument_type` accepted (and ignored) only so
    gap_recovery.py can call either module through one uniform signature.
    One bounded REST call, no pacing needed (Delta's public candle endpoint
    has no documented per-second quota the way Dhan's account-wide one
    does — this only ever runs from the same startup-only, one-token-at-a-
    time gap_recovery.py pass regardless). Empty list on any failure."""
    start = int(from_dt.timestamp())
    end = int(to_dt.timestamp())
    if end <= start:
        return []
    try:
        data = _get("/v2/history/candles", {"symbol": token, "resolution": _RESOLUTION, "start": start, "end": end})
    except Exception:
        log.exception("[DeltaHistorical] fetch error for %s", token)
        return []
    if not data.get("success"):
        log.warning("[DeltaHistorical] fetch failed for %s: %s", token, data)
        return []
    rows = data.get("result") or []
    candles = [
        {
            "ts": datetime.fromtimestamp(int(row["time"]), tz=timezone.utc),
            "open": float(row.get("open") or 0.0), "high": float(row.get("high") or 0.0),
            "low": float(row.get("low") or 0.0), "close": float(row.get("close") or 0.0),
        }
        for row in rows if row.get("time") is not None
    ]
    candles.sort(key=lambda c: c["ts"])
    return [c for c in candles if from_dt <= c["ts"] <= to_dt]
