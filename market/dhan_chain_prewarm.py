"""
dhan_chain_prewarm.py
──────────────────────
NSE's equivalent of shared/market/delta_instrument_cache.py's own job for
crypto — brought over in SPIRIT, not code, from the old system's
_prewarm_index_chains (shared/features/dhan_ticker.py): subscribe every
current-cycle-expiry option token for the admin-configured index list
(Mongo `algo_subscribe_index`, same collection/shape/default — NIFTY +
SENSEX — the old system reads) ONCE at startup, so a strategy activating
against one of these underlyings finds its tokens already ticking instead
of paying services/strategy_activation.py's own per-activation subscribe-
and-wait (_ensure_live_prices) cost — which stays in place as the fallback
for any underlying/expiry OUTSIDE this current-cycle window, exactly like
the old system's own "warmed on first use, just not pre-subscribed at
boot" fallback for far expiries.

Reads instrument_master (already prewarmed at startup — see main.py's own
on_startup) for the actual tokens; touches Mongo only for the admin-
configured underlying list itself, never for token metadata (that contract
belongs to instrument_master.load alone, master-doc §6).
"""

from __future__ import annotations

from datetime import date, datetime

import httpx

from shared.config.settings import get_settings
from shared.db.mongo import Mongo
from shared.logging.logger import get_logger
from shared.market.instrument_master import InstrumentMaster

log = get_logger(__name__)

# Same window shared/features/dhan_ticker.py's own PREWARM_CURRENT_CYCLE_DAYS
# uses — verified there against real SENSEX/NIFTY token counts (this week's
# expiry + next week's vs every future expiry the exchange has listed).
PREWARM_CURRENT_CYCLE_DAYS = 14

_INDEX_CONFIG_COLLECTION = "algo_subscribe_index"
_INDEX_CONFIG_DOC_ID = "index_instruments"


def _admin_configured_underlyings(mongo: Mongo) -> list[str]:
    doc = mongo.raw[_INDEX_CONFIG_COLLECTION].find_one({"_id": _INDEX_CONFIG_DOC_ID})
    instruments = (doc or {}).get("instruments") or []
    return [str(u).strip().upper() for u in instruments if str(u).strip()]


def prewarm_index_chains(mongo: Mongo, instrument_master: InstrumentMaster) -> int:
    """Returns the number of tokens subscribed (0 if nothing to do or the
    subscribe request failed) — main.py logs this so a misconfigured/empty
    `algo_subscribe_index` doc is visible at startup instead of silently
    warming nothing."""
    underlyings = _admin_configured_underlyings(mongo)
    if not underlyings:
        return 0

    today = date.today()
    # Grouped by each token's own ws_segment, not one hardcoded "NSE_FNO"
    # for every underlying — SENSEX/BANKEX are BSE-listed (active_option_
    # tokens' own exchange/ws_segment fields already say so: "BSE"/
    # "BSE_FNO"), and subscribing them under NSE_FNO silently asks Dhan for
    # a different (nonexistent, from that segment's point of view)
    # instrument — confirmed live: this exact prewarm, with SENSEX in the
    # default algo_subscribe_index list, left SENSEX's whole current-cycle
    # chain at zero LTP/bid/ask/OI forever despite subscribed_count going
    # up (Dhan accepted the request, just for the wrong segment).
    tokens_by_segment: dict[str, set[str]] = {}
    for underlying in underlyings:
        for option_type in ("CE", "PE"):
            for expiry in instrument_master.expiries_for(underlying, option_type):
                try:
                    expiry_date = datetime.strptime(expiry, "%Y-%m-%d").date()
                except ValueError:
                    continue
                if expiry_date < today or (expiry_date - today).days > PREWARM_CURRENT_CYCLE_DAYS:
                    continue
                for meta in instrument_master.by_underlying_expiry_type(underlying, expiry, option_type):
                    seg = meta.ws_segment or "NSE_FNO"
                    tokens_by_segment.setdefault(seg, set()).add(meta.token)

    if not tokens_by_segment:
        log.warning("[ChainPrewarm] no current-cycle tokens found for %s — instrument_master may be stale/empty", underlyings)
        return 0

    tokens = {t for toks in tokens_by_segment.values() for t in toks}
    try:
        settings = get_settings()
        with httpx.Client(timeout=10.0) as client:
            for seg, seg_tokens in tokens_by_segment.items():
                client.post(f"{settings.websocket_base_url}/ticker/subscribe", json={"tokens": list(seg_tokens), "exchange": seg})
    except Exception:
        log.exception("[ChainPrewarm] ticker/subscribe request failed for %d tokens", len(tokens))
        return 0

    log.info("[ChainPrewarm] subscribed %d current-cycle option tokens for %s", len(tokens), underlyings)
    return len(tokens)
