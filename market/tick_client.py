"""
tick_client.py
────────────────
WS client for algo.websocket's `/ws/internal-ticks` — the ONE hard external
wire-protocol dependency algo-2_0 has on the (untouched, external) algo.websocket
service. Message shape and reconnect behavior are ported from
shared/features/central_tick_client.py's `_ws_loop`/`_on_raw`, since that's the
one contract we can't change from this side.

Unlike the old CentralTickClient (a background OS thread running its own
asyncio.run loop, built for sync/threaded callers), this is a native asyncio
task — algo-2_0's trade service is asyncio-first throughout, so there's no
thread-bridge needed here.
"""

from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone
from typing import Awaitable, Callable

import websockets

from shared.logging.logger import get_logger
from shared.market.ltp_cache import LtpCache, TickUpdate

log = get_logger(__name__)

_CRYPTO_UNDERLYINGS = ("BTCUSD", "ETHUSD")

TickListener = Callable[[TickUpdate], "Awaitable[None] | None"]


class TickClient:
    """
    .status         "stopped" | "connecting" | "running" | "error"
    .tick_count     int (mirrors ltp_cache.tick_count, kept for /live/status parity)
    .started_at     ISO8601 str
    .error_msg      str
    .start()        → spawns the reconnect-loop task
    .stop()         → signals the loop to exit
    .add_listener() → registered callables get every TickUpdate (Phase 2's
                      token_router hooks in here to dispatch only affected legs)
    """

    def __init__(self, ws_base_url: str, ltp_cache: LtpCache) -> None:
        self._ws_url = ws_base_url.rstrip("/").replace("http://", "ws://").replace("https://", "wss://") + "/ws/internal-ticks"
        self._cache = ltp_cache
        self.status: str = "stopped"
        self.started_at: str = ""
        self.error_msg: str = ""
        self._listeners: list[TickListener] = []
        self._running = False
        self._task: asyncio.Task | None = None

    @property
    def tick_count(self) -> int:
        return self._cache.tick_count

    def add_listener(self, listener: TickListener) -> None:
        if listener not in self._listeners:
            self._listeners.append(listener)

    def remove_listener(self, listener: TickListener) -> None:
        self._listeners = [l for l in self._listeners if l is not listener]

    def start(self) -> None:
        if self._running:
            return
        self._running = True
        self.status = "connecting"
        self.started_at = datetime.now(timezone.utc).isoformat()
        self._task = asyncio.create_task(self._run_loop(), name="tick_client_ws")
        log.info("[TickClient] starting → %s", self._ws_url)

    def stop(self) -> None:
        self._running = False
        self.status = "stopped"
        if self._task is not None:
            self._task.cancel()
            self._task = None

    def get_ltp(self, token: str) -> float | None:
        return self._cache.get_ltp(token)

    def get_status(self) -> dict:
        return {
            "status": self.status,
            "tick_count": self.tick_count,
            "ltp_count": len(self._cache.ltp_map),
            "spot_map": dict(self._cache.spot_map),
            "started_at": self.started_at,
            "error": self.error_msg,
            "mode": "central",
            "hub": self._ws_url,
        }

    async def _run_loop(self) -> None:
        while self._running:
            try:
                async with websockets.connect(self._ws_url, ping_interval=20, ping_timeout=10) as ws:
                    self.status = "running"
                    self.error_msg = ""
                    log.info("[TickClient] connected to %s", self._ws_url)
                    async for raw in ws:
                        if not self._running:
                            break
                        await self._on_raw(raw)
            except asyncio.CancelledError:
                break
            except Exception as exc:
                self.status = "error"
                self.error_msg = str(exc)
                log.warning("[TickClient] WS error: %s — retry in 2s", exc)
                if self._running:
                    await asyncio.sleep(2)
        self.status = "stopped"

    async def _on_raw(self, raw: str) -> None:
        try:
            msg = json.loads(raw)
        except Exception:
            return
        if msg.get("type") != "tick":
            return

        data = msg.get("data") or {}
        now_ts = str(data.get("now_ts") or datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"))
        update = TickUpdate(
            changed_ltp=data.get("changed_ltp_map") or {},
            spot=data.get("spot_map") or {},
            changed_oi=data.get("changed_oi_map") or {},
            changed_bid=data.get("changed_bid_map") or {},
            changed_ask=data.get("changed_ask_map") or {},
            changed_bid_qty=data.get("changed_bid_qty_map") or {},
            changed_ask_qty=data.get("changed_ask_qty_map") or {},
            now_ts=now_ts,
        )
        if not update.changed_ltp and not update.spot:
            return
        # algo.websocket publishes the Delta perpetuals (BTCUSD/ETHUSD) as
        # ordinary LTP tokens, never via spot_map — but for a crypto strategy
        # they ARE the underlying. Mirror them into spot so Underlying-based
        # SL/TP, ATM selection and entry_spot see a live value, the same way
        # an NSE index's spot does.
        for underlying in _CRYPTO_UNDERLYINGS:
            price = update.changed_ltp.get(underlying)
            if price:
                update.spot = {**update.spot, underlying: price}

        await self.dispatch(update)

    async def dispatch(self, update: TickUpdate) -> None:
        """Apply one TickUpdate and run every listener — the WS stream above,
        and the REST stale-price refresh in algo.trade's main.py."""
        self._cache.apply(update)

        for listener in list(self._listeners):
            try:
                result = listener(update)
                if asyncio.iscoroutine(result):
                    await result
            except Exception:
                log.exception("[TickClient] listener error")
