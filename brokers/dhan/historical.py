"""
historical.py (Dhan)
──────────────────────
Thin wrapper over dhanhq's HistoricalData.intraday_minute_data — used ONLY
by services/gap_recovery.py's startup TSL gap recovery (never on the tick
path, never per-tick). Same credentials source as adapter.py
(kite_market_config, broker="dhan").

Response field names (timestamp/open/high/low/close, timestamp as UNIX
epoch seconds) and the instrument_type mapping ("index"->"OPTIDX",
"stock"->"OPTSTK") are copied from the old system's real, working
integration (shared/features/candle_fetch.py's candles_from_dhan_response,
shared/features/mtm_historical_data.py's _DHAN_LEG_INSTRUMENT_MAP) — read
from source, not guessed, per the gap-recovery master prompt's explicit
"do not invent Dhan endpoints/fields" instruction.

Rate limit: ~1 req/sec, ACCOUNT-WIDE (shared with every other Dhan REST
call this account makes — quotes, orders, ...) — the same real, measured
value the old system's broker_gateway.py paces on. That system needed a
Mongo-backed CROSS-PROCESS gate because multiple old-system services shared
one account; algo-2_0 currently runs as a single process, so a plain
in-process pacer here is the equivalent, not a different constraint.
"""

from __future__ import annotations

import time
from datetime import datetime, timezone

from dhanhq import DhanContext, HistoricalData

from shared.brokers.dhan.credentials import get_dhan_credentials
from shared.db.mongo import Mongo
from shared.logging.logger import get_logger

log = get_logger(__name__)

_MIN_CALL_INTERVAL_SECONDS = 1.05
_last_call_at: float = 0.0

_INSTRUMENT_TYPE_MAP = {"index": "OPTIDX", "stock": "OPTSTK"}


def _wait_for_slot() -> None:
    """Blocking pacer — this module's calls only ever happen from
    services/gap_recovery.py's startup-only recovery pass, one token at a
    time inside a bounded worker pool (never the tick path), so a plain
    blocking sleep here costs wall-clock recovery time, never tick latency."""
    global _last_call_at
    remaining = _MIN_CALL_INTERVAL_SECONDS - (time.monotonic() - _last_call_at)
    if remaining > 0:
        time.sleep(remaining)
    _last_call_at = time.monotonic()


def resolve_instrument_type(mongo: Mongo, token: str) -> tuple[str, str] | None:
    """(dhan_instrument_type, exchange_segment) for a token, read straight
    from `active_option_tokens` (same collection instrument_master.py loads
    from) — returns None if the token isn't a real Dhan-fed instrument
    (e.g. a crypto token; gap recovery only applies to NSE/Dhan legs)."""
    doc = mongo.raw["active_option_tokens"].find_one(
        {"token": str(token), "broker": "dhan"},
        {"instrument_type": 1, "ws_segment": 1},
    )
    if not doc:
        return None
    dhan_type = _INSTRUMENT_TYPE_MAP.get(str(doc.get("instrument_type") or "").strip().lower(), "OPTIDX")
    segment = str(doc.get("ws_segment") or "NSE_FNO").strip()
    return dhan_type, segment


def fetch_intraday_candles(
    mongo: Mongo, security_id: str, exchange_segment: str, instrument_type: str,
    from_dt: datetime, to_dt: datetime,
) -> list[dict]:
    """One security_id per call — Dhan's own API has no bulk/multi-token
    endpoint (confirmed against the installed dhanhq SDK's
    intraday_minute_data signature); callers must already have deduplicated
    by token before calling this. Returns candles sorted chronologically
    (oldest first): [{"ts": datetime (UTC), "open":, "high":, "low":,
    "close":}, ...] — empty list on any failure (missing credentials, API
    error) rather than raising, since a gap-recovery pass for one token
    failing must not abort recovery for every other token."""
    creds = get_dhan_credentials(mongo)
    client_id = str(creds.get("user_id") or creds.get("dhan_client_id") or "").strip()
    access_token = str(creds.get("access_token") or "").strip()
    if not client_id or not access_token:
        log.warning("[DhanHistorical] missing credentials — skipping fetch for %s", security_id)
        return []

    _wait_for_slot()
    try:
        hist = HistoricalData(DhanContext(client_id, access_token))
        resp = hist.intraday_minute_data(
            security_id=str(security_id), exchange_segment=exchange_segment, instrument_type=instrument_type,
            from_date=from_dt.strftime("%Y-%m-%d"), to_date=to_dt.strftime("%Y-%m-%d"), interval=1,
        )
    except Exception:
        log.exception("[DhanHistorical] fetch error for %s", security_id)
        return []

    if not isinstance(resp, dict) or resp.get("status") != "success":
        log.warning("[DhanHistorical] fetch failed for %s: %s", security_id, resp)
        return []

    data = resp.get("data") or {}
    timestamps = data.get("timestamp") or []
    opens, highs, lows, closes = data.get("open") or [], data.get("high") or [], data.get("low") or [], data.get("close") or []
    candles = [
        {
            "ts": datetime.fromtimestamp(timestamps[i], tz=timezone.utc),
            "open": float(opens[i]) if i < len(opens) else 0.0,
            "high": float(highs[i]) if i < len(highs) else 0.0,
            "low": float(lows[i]) if i < len(lows) else 0.0,
            "close": float(closes[i]) if i < len(closes) else 0.0,
        }
        for i in range(len(timestamps))
    ]
    candles.sort(key=lambda c: c["ts"])
    # Only candles actually inside the requested window — Dhan's own date
    # granularity is per-day, so a day-boundary request can include extra
    # minutes outside [from_dt, to_dt] that the caller didn't ask for.
    return [c for c in candles if from_dt <= c["ts"] <= to_dt]
