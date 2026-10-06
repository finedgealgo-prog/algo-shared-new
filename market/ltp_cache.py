"""
ltp_cache.py
────────────
Central, in-process, token-keyed market data cache — the "one LTP copy" the
master-implementation-prompt doc calls for (never one copy per strategy).
Populated exclusively by tick_client.py's WS stream from algo.websocket.

Every field here mirrors shared/features/central_tick_client.py's maps
(ltp_map/spot_map/oi_map/bid_map/ask_map/*_qty_map/ltp_ts_map) so later
phases (risk engine, selection, Dhan quote()'s future WS-fast-path) read the
exact same shape the old system's consumers already expect.
"""

from __future__ import annotations

import threading
from dataclasses import dataclass, field
from datetime import datetime, timezone
from functools import lru_cache


@dataclass
class TickUpdate:
    """One coalesced tick event — only the tokens/fields that actually
    changed on this frame, not a full snapshot. token_router.py (Phase 2)
    dispatches off `changed_ltp` to find affected legs."""

    changed_ltp: dict[str, float] = field(default_factory=dict)
    spot: dict[str, float] = field(default_factory=dict)
    changed_oi: dict[str, float] = field(default_factory=dict)
    changed_bid: dict[str, float] = field(default_factory=dict)
    changed_ask: dict[str, float] = field(default_factory=dict)
    changed_bid_qty: dict[str, int] = field(default_factory=dict)
    changed_ask_qty: dict[str, int] = field(default_factory=dict)
    now_ts: str = ""


class LtpCache:
    def __init__(self) -> None:
        self.ltp_map: dict[str, float] = {}
        self.spot_map: dict[str, float] = {}
        self.oi_map: dict[str, float] = {}
        self.bid_map: dict[str, float] = {}
        self.ask_map: dict[str, float] = {}
        self.bid_qty_map: dict[str, int] = {}
        self.ask_qty_map: dict[str, int] = {}
        self.ltp_ts_map: dict[str, str] = {}
        self.tick_count: int = 0
        self._lock = threading.Lock()

    def apply(self, update: TickUpdate) -> None:
        with self._lock:
            if update.changed_ltp:
                self.ltp_map.update(update.changed_ltp)
                now_ts = update.now_ts or datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
                for tok in update.changed_ltp:
                    self.ltp_ts_map[tok] = now_ts
                self.tick_count += len(update.changed_ltp)
            if update.spot:
                self.spot_map.update(update.spot)
            if update.changed_oi:
                self.oi_map.update(update.changed_oi)
            if update.changed_bid:
                self.bid_map.update(update.changed_bid)
            if update.changed_ask:
                self.ask_map.update(update.changed_ask)
            if update.changed_bid_qty:
                self.bid_qty_map.update(update.changed_bid_qty)
            if update.changed_ask_qty:
                self.ask_qty_map.update(update.changed_ask_qty)

    def get_ltp(self, token: str) -> float | None:
        return self.ltp_map.get(str(token))

    def get_spot(self, underlying: str) -> float | None:
        return self.spot_map.get(str(underlying or "").upper())

    def is_stale(self, token: str, max_age_seconds: float) -> bool:
        """Freshness check per master-doc §83/§84 — before using a cached
        price for a new entry/reference, callers should check this rather
        than trusting an old tick silently."""
        ts = self.ltp_ts_map.get(str(token))
        if not ts:
            return True
        try:
            tick_time = datetime.strptime(ts[:19], "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc)
        except ValueError:
            return True
        return (datetime.now(timezone.utc) - tick_time).total_seconds() > max_age_seconds


@lru_cache(maxsize=1)
def get_ltp_cache() -> LtpCache:
    return LtpCache()
