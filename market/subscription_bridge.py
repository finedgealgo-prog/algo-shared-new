"""
subscription_bridge.py
────────────────────────
Calls algo.websocket's existing `/ticker/{subscribe,unsubscribe,warm-chain}`
REST endpoints to get tokens streamed over the shared `/ws/internal-ticks`
connection — the second (and last) external contract algo-2_0 depends on
from algo.websocket. Request/response shape ported from
shared/features/central_tick_client.py's `_post_with_retry`/`subscribe_tokens`.

Dedup + retry-with-backoff is preserved: tokens are only marked "subscribed"
locally after a confirmed successful POST, so a transient failure doesn't
silently drop the subscription forever.
"""

from __future__ import annotations

import asyncio

import httpx

from shared.logging.logger import get_logger

log = get_logger(__name__)

_MAX_RETRIES = 5
_BACKOFF_SECONDS = 2.0


class SubscriptionBridge:
    def __init__(self, ws_base_url: str) -> None:
        self._base = ws_base_url.rstrip("/")
        self.subscribed_tokens: set[str] = set()
        self.chain_subscribed_tokens: set[str] = set()

    async def subscribe_tokens(self, token_ids: list[str], exchange: str = "NSE_FNO") -> None:
        new = [str(t) for t in token_ids if str(t).strip() and str(t) not in self.subscribed_tokens]
        if not new:
            return
        asyncio.create_task(self._post_with_retry("/ticker/subscribe", new, exchange, self.subscribed_tokens, "subscribed"))

    async def unsubscribe_tokens(self, token_ids: list[str]) -> None:
        tokens = [str(t) for t in token_ids if str(t).strip()]
        if not tokens:
            return
        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                await client.post(f"{self._base}/ticker/unsubscribe", json={"tokens": tokens})
            self.subscribed_tokens.difference_update(tokens)
            log.info("[SubscriptionBridge] unsubscribed %d tokens", len(tokens))
        except Exception as exc:
            log.warning("[SubscriptionBridge] unsubscribe POST failed: %s", exc)

    async def warm_chain_tokens(self, token_ids: list[str], exchange: str = "NSE_FNO") -> None:
        new = [str(t) for t in token_ids if str(t).strip() and str(t) not in self.chain_subscribed_tokens]
        if not new:
            return
        asyncio.create_task(self._post_with_retry("/ticker/warm-chain", new, exchange, self.chain_subscribed_tokens, "warmed"))

    async def _post_with_retry(
        self, path: str, tokens: list[str], exchange: str, dedup_set: set[str], verb: str, retry: int = 0,
    ) -> None:
        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                await client.post(f"{self._base}{path}", json={"tokens": tokens, "exchange": exchange})
            dedup_set.update(tokens)
            log.info("[SubscriptionBridge] %s %d tokens via hub", verb, len(tokens))
        except Exception as exc:
            if retry < _MAX_RETRIES:
                log.warning("[SubscriptionBridge] %s POST failed (retry %d/%d): %s", path, retry + 1, _MAX_RETRIES, exc)
                await asyncio.sleep(_BACKOFF_SECONDS)
                await self._post_with_retry(path, tokens, exchange, dedup_set, verb, retry + 1)
            else:
                log.error(
                    "[SubscriptionBridge] %s POST failed permanently after %d retries: %s — %d tokens not subscribed",
                    path, _MAX_RETRIES, exc, len(tokens),
                )
