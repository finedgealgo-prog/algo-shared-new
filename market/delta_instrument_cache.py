"""
delta_instrument_cache.py
────────────────────────────
Crypto (BTC/ETH) equivalent of shared/market/instrument_master.py — same
job (static option-contract metadata, prewarmed in RAM, keyed by (underlying,
expiry, option_type) for strike_resolver.build_chain_rows's chain-row
assembly), same shape (by_underlying_expiry_type), just a different data
source: instrument_master.load() is a one-time Mongo read (NSE's contract
universe barely changes), this instead REFRESHES periodically via Delta's
REST API (shared/brokers/delta/client.py) — Delta's contract list is far
smaller, but strikes/expiries genuinely roll (a new one appears, an old one
expires) more often than a single startup load would ever pick up.

Why this exists: before it, EVERY single crypto strike resolution (every
leg, every re-entry, every activation) made its own synchronous REST call
to Delta (strike_resolver.build_chain_rows's crypto branch called
delta_client.fetch_option_chain_rest directly) — the user's own ask: "ne
indian market epdi pannirukiyo same method la crypto kum pannu, indian
market ku cache option chain iruka" (build the same cached-instrument
architecture for crypto that NSE already has). With this cache warm,
build_chain_rows's crypto branch reads metadata from RAM + a live price
from ltp_cache (tick-fed, see subscribe_all below) — zero REST calls during
strike resolution, exactly matching the NSE branch's own zero-Mongo-query
read.

subscribe_all() is the other half: cached metadata alone isn't enough —
ltp_cache only ever has a price for a token DeltaFeed was explicitly told to
subscribe (see feed/delta_feed.py's own docstring — nothing subscribes a
token just because it's listed). refresh() calling subscribe_all() after
every reload keeps every currently-listed contract's mark_price flowing
continuously, the same way DhanFeed already ticks NSE's whole active
universe — instead of a leg needing its own token individually subscribed
(and waiting) at entry time.
"""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass
from functools import lru_cache

import httpx

from shared.brokers.delta import client as delta_client
from shared.config.settings import get_settings
from shared.logging.logger import get_logger

log = get_logger(__name__)


def subscribe_tokens_now(tokens: list[str]) -> None:
    """Immediate, synchronous DeltaFeed subscribe for a SPECIFIC token (or a
    few) — the entry-time counterpart to DeltaInstrumentCache.subscribe_all's
    periodic whole-universe sweep. A leg's own token almost always ends up
    subscribed via that periodic sweep anyway (it's a real listed contract,
    picked up on the next refresh), but "almost always, eventually" isn't
    "the instant the leg enters" — a strategy activated in the gap between
    two refresh cycles would sit with a blank LTP for however long is left
    of that cycle. Called right at the point of entry (strategy_activation.
    py's per-leg loop, leg_followup.py's re-entry/lazy-arm) so the exact
    token this leg/watcher needs starts ticking immediately, same guarantee
    the NSE side already gets from strategy_activation.py's own
    _ensure_live_prices at activation time. Best-effort, never raises — a
    failed POST here just means this leg's token waits for the next
    periodic sweep instead, same as before this existed."""
    tokens = [t for t in tokens if t]
    if not tokens:
        return
    try:
        settings = get_settings()
        with httpx.Client(timeout=5.0) as client:
            client.post(f"{settings.websocket_base_url}/ticker/subscribe", json={"tokens": tokens, "exchange": "DELTA"})
    except Exception:
        log.exception("[DeltaInstrumentCache] immediate subscribe POST failed for %s", tokens)


def subscribe_tokens_background(tokens: list[str]) -> None:
    """Non-blocking subscribe_tokens_now for callers running ON the event
    loop's tick path (re-entry / scheduled entry listeners) — a synchronous
    HTTP POST there (5s timeout) stalls every other leg's SL/TP check."""
    tokens = [t for t in tokens if t]
    if tokens:
        threading.Thread(target=subscribe_tokens_now, args=(tokens,), name="delta_subscribe", daemon=True).start()


@dataclass(frozen=True)
class DeltaInstrumentMeta:
    token: str
    symbol: str
    strike: float
    expiry: str
    option_type: str
    underlying: str
    # Delta's own real, exchange-computed greeks (same values _leg_row's
    # REST-fetch row carries) — REQUIRED for EntryByDelta/EntryByDeltaRange
    # selection: delta_selector.select_closest_delta/select_delta_range both
    # filter rows through _valid_rows(), which drops any row with delta==0.
    # Without these here, every cache-sourced row had delta defaulting to 0
    # (build_chain_rows's cache branch only ever set strike/token/symbol/
    # ltp), so _valid_rows() emptied the whole candidate list and every
    # EntryByDelta selection silently returned None — confirmed live: a
    # scheduled BTCUSD EntryByDelta entry fired exactly on time (expiry/spot
    # both resolved correctly) and still failed "strike not resolved" only
    # once the cache was warm; the SAME config resolved fine standalone
    # against a cold cache falling back to REST (which correctly includes
    # greeks via _leg_row) — that discrepancy is what pointed here.
    delta: float = 0.0
    gamma: float = 0.0
    theta: float = 0.0
    vega: float = 0.0
    iv: float = 0.0


class DeltaInstrumentCache:
    def __init__(self) -> None:
        self._by_chain_key: dict[tuple[str, str, str], list[DeltaInstrumentMeta]] = {}
        self._by_token: dict[str, DeltaInstrumentMeta] = {}
        self.last_refresh_ts: float = 0.0
        self.last_refresh_error: str = ""

    def refresh(self) -> int:
        """Synchronous — same REST client every other crypto call site
        already uses, called off a periodic background task (main.py), not
        per-request. Rebuilds the whole cache fresh each call (a handful of
        REST round trips, not per-strike) rather than diffing — Delta's
        contract universe is small enough that a full rebuild every cycle
        is simpler and self-healing (an expired contract just isn't in the
        new result, no separate eviction logic needed)."""
        by_chain_key: dict[tuple[str, str, str], list[DeltaInstrumentMeta]] = {}
        by_token: dict[str, DeltaInstrumentMeta] = {}
        loaded = 0
        try:
            for underlying, asset in delta_client.UNDERLYING_TO_ASSET.items():
                expiries = delta_client.list_expiries(asset)
                for expiry in expiries:
                    for option_type in ("CE", "PE"):
                        rows = delta_client.fetch_option_chain_rest(underlying, expiry, option_type)
                        metas = [
                            DeltaInstrumentMeta(
                                token=str(r["token"]), symbol=str(r["symbol"]), strike=float(r["strike"]),
                                expiry=expiry, option_type=option_type, underlying=underlying,
                                delta=float(r.get("delta") or 0.0), gamma=float(r.get("gamma") or 0.0),
                                theta=float(r.get("theta") or 0.0), vega=float(r.get("vega") or 0.0),
                                iv=float(r.get("iv") or 0.0),
                            )
                            for r in rows if r.get("token")
                        ]
                        if not metas:
                            continue
                        by_chain_key[(underlying, expiry, option_type)] = metas
                        for meta in metas:
                            by_token[meta.token] = meta
                        loaded += len(metas)
            self._by_chain_key = by_chain_key
            self._by_token = by_token
            self.last_refresh_ts = time.monotonic()
            self.last_refresh_error = ""
            log.info("[DeltaInstrumentCache] refreshed %d contracts across %d chains", loaded, len(by_chain_key))
        except Exception as exc:
            self.last_refresh_error = str(exc)
            log.exception("[DeltaInstrumentCache] refresh failed")
        return loaded

    def subscribe_all(self) -> None:
        """POSTs every currently-cached token to algo.websocket's DeltaFeed
        (mark_price channel — see feed/delta_feed.py's _channel_for) so
        ltp_cache keeps a live price for the WHOLE listed universe, not just
        tokens some already-open position happens to hold. One bounded HTTP
        call, best-effort (a failed subscribe here just means this cycle's
        newly-listed contracts stay REST-only a bit longer via build_chain_
        rows's per-token fallback — never fatal)."""
        subscribe_tokens_now(list(self._by_token.keys()))

    def by_underlying_expiry_type(self, underlying: str, expiry: str, option_type: str) -> list[DeltaInstrumentMeta]:
        return self._by_chain_key.get((str(underlying or "").upper(), str(expiry or ""), str(option_type or "").upper()), [])

    def expiries_for(self, underlying: str) -> list[str]:
        """Listed expiries (DD-MM-YYYY, chronological) for `underlying`
        from the last refresh — the zero-I/O equivalent of
        delta_client.list_expiries."""
        key = str(underlying or "").upper()
        expiries = {expiry for (u, expiry, _t) in self._by_chain_key if u == key}
        parsed = []
        for expiry in expiries:
            try:
                dd, mm, yyyy = expiry.split("-")
                parsed.append(((int(yyyy), int(mm), int(dd)), expiry))
            except ValueError:
                continue
        return [expiry for _key, expiry in sorted(parsed)]

    def get(self, token: str) -> DeltaInstrumentMeta | None:
        """Static-metadata equivalent of InstrumentMaster.get(token) for the
        NSE side — api/routers/strategy_history.py's analyse-view fallback
        uses this to backfill strike/expiry on an already-open leg whose
        own LegRuntime.strike/expiry are 0/"" because it was entered before
        those fields existed (a real, already-checkpointed position can't
        retroactively gain them any other way short of re-entering it)."""
        return self._by_token.get(str(token))

    def get_status(self) -> dict:
        return {
            "contracts": len(self._by_token), "chains": len(self._by_chain_key),
            "last_refresh_ts": self.last_refresh_ts, "last_refresh_error": self.last_refresh_error,
        }


@lru_cache(maxsize=1)
def get_delta_instrument_cache() -> DeltaInstrumentCache:
    return DeltaInstrumentCache()


def crypto_expiries(underlying: str) -> list[str]:
    """Expiries for a crypto underlying ("BTCUSD") — cache first (no I/O),
    Delta REST only when the cache hasn't loaded that underlying yet (cold
    start before the first periodic refresh)."""
    cached = get_delta_instrument_cache().expiries_for(underlying)
    if cached:
        return cached
    asset = delta_client.UNDERLYING_TO_ASSET.get(str(underlying or "").upper(), "")
    return delta_client.list_expiries(asset) if asset else []
