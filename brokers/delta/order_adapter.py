"""
order_adapter.py (Delta Exchange — REAL orders)
──────────────────────────────────────────────────
Signed Delta Exchange India REST client for order placement, ported from the
old system's working shared/features/order_brokers/brokers/delta_exchange.py
(same HMAC signing, product-id resolution, IPv4 forcing, order body shape).
Only the order surface algo-2_0's live executor needs is kept: place, get,
cancel, positions, credential check.

Authentication (https://docs.delta.exchange/#authentication-strategy): every
private request carries api-key / signature / timestamp headers, signature =
HMAC-SHA256(api_secret, method + timestamp + path + query_string + body).
Delta rejects a timestamp older than 5s, so it is computed per request.

Blocking I/O — call only from worker threads (orders/live_executor.py),
never from the event loop.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import socket
import threading
import time
from decimal import ROUND_DOWN, ROUND_UP, Decimal
from typing import Any
from urllib.parse import urlencode

import requests
import urllib3.util.connection as _urllib3_connection

from shared.logging.logger import get_logger

log = get_logger(__name__)

# Delta's API-key IP whitelist holds the server's IPv4 address; dual-stack
# resolution would otherwise send signed calls over IPv6 and every one would
# fail with ip_not_whitelisted_for_api_key (see the old adapter's note).
_urllib3_connection.allowed_gai_family = lambda: socket.AF_INET

DELTA_REST_BASE = "https://api.india.delta.exchange"
_HTTP_TIMEOUT = 10.0

_PRODUCT_CACHE: dict[str, tuple[float, dict]] = {}
_PRODUCT_CACHE_TTL = 3600.0
_product_cache_lock = threading.Lock()

# Delta order state -> engine status.
_STATE_TO_STATUS = {"open": "OPEN", "pending": "OPEN", "closed": "COMPLETE", "cancelled": "CANCELLED"}


class DeltaOrderError(Exception):
    pass


def is_delta_broker_doc(doc: dict) -> bool:
    """Same test as the old _is_delta_doc: a broker_configuration doc whose
    name/broker/icon says Delta."""
    text = " ".join(str(doc.get(k) or "") for k in ("broker", "broker_name", "name", "broker_icon")).lower()
    return "delta" in text


class DeltaOrderAdapter:
    def __init__(self, api_key: str, api_secret: str, session: requests.Session | None = None) -> None:
        self.api_key = api_key
        self.api_secret = api_secret
        self._session = session or requests.Session()

    # ── transport ────────────────────────────────────────────────────────────

    def _public_get(self, path: str, params: dict | None = None) -> dict:
        resp = self._session.get(f"{DELTA_REST_BASE}{path}", params=params or {}, timeout=_HTTP_TIMEOUT)
        resp.raise_for_status()
        return resp.json()

    def _signed_request(self, method: str, path: str, *, params: dict | None = None, body: dict | None = None) -> dict:
        query = ("?" + urlencode(params)) if params else ""
        payload = json.dumps(body) if body is not None else ""
        timestamp = str(int(time.time()))
        signature = hmac.new(self.api_secret.encode(), (method + timestamp + path + query + payload).encode(), hashlib.sha256).hexdigest()
        headers = {
            "api-key": self.api_key, "signature": signature, "timestamp": timestamp,
            "Content-Type": "application/json", "User-Agent": "algo-2_0-live-orders/1.0",
        }
        resp = self._session.request(
            method, f"{DELTA_REST_BASE}{path}", params=params or {}, data=payload or None,
            headers=headers, timeout=_HTTP_TIMEOUT,
        )
        try:
            data = resp.json()
        except ValueError:
            raise DeltaOrderError(f"Delta {method} {path}: HTTP {resp.status_code} non-JSON {resp.text[:200]}")
        if not data.get("success"):
            error = data.get("error") or {}
            code = error.get("code") or error.get("message") if isinstance(error, dict) else error
            context = error.get("context") if isinstance(error, dict) else ""
            raise DeltaOrderError(f"Delta {method} {path}: {code or ('HTTP %s' % resp.status_code)} {context or ''}".strip())
        return data

    # ── products ─────────────────────────────────────────────────────────────

    def get_product(self, symbol: str) -> dict:
        now = time.monotonic()
        with _product_cache_lock:
            cached = _PRODUCT_CACHE.get(symbol)
            if cached and (now - cached[0]) < _PRODUCT_CACHE_TTL:
                return cached[1]
        data = self._public_get(f"/v2/products/{symbol}")
        product = data.get("result") if data.get("success") else None
        if not product:
            raise DeltaOrderError(f"unknown/expired Delta product '{symbol}'")
        with _product_cache_lock:
            _PRODUCT_CACHE[symbol] = (now, product)
        return product

    @staticmethod
    def round_to_tick(price: float, tick_size: Any, side: str) -> str:
        """Marketable limit rounded AWAY from the touch (buy up, sell down) on
        the product's tick grid, as the string Delta expects."""
        tick = Decimal(str(tick_size or "0"))
        value = Decimal(str(price))
        if tick > 0:
            steps = (value / tick).quantize(Decimal(1), rounding=ROUND_UP if side == "buy" else ROUND_DOWN)
            value = steps * tick
            if value <= 0:
                value = tick
        return format(value.normalize(), "f")

    # ── orders ───────────────────────────────────────────────────────────────

    def place_order(
        self, *, symbol: str, side: str, size: int, order_type: str,
        limit_price: float = 0.0, reduce_only: bool = False, client_order_id: str = "",
    ) -> dict:
        """POST /v2/orders. `side` "buy"/"sell", `order_type` "limit_order" /
        "market_order". Returns Delta's order object."""
        product = self.get_product(symbol)
        body: dict[str, Any] = {
            "product_id": product["id"], "product_symbol": product.get("symbol", symbol),
            "side": side, "size": int(size), "order_type": order_type,
            "reduce_only": bool(reduce_only),
        }
        if client_order_id:
            body["client_order_id"] = client_order_id[:32]
        if order_type == "limit_order":
            body["limit_price"] = self.round_to_tick(limit_price, product.get("tick_size"), side)
            body["time_in_force"] = "gtc"
        log.info("[DeltaLive] place_order %s", body)
        return self._signed_request("POST", "/v2/orders", body=body).get("result") or {}

    def get_order(self, order_id: str) -> dict:
        """GET /v2/orders/{id}."""
        return self._signed_request("GET", f"/v2/orders/{order_id}").get("result") or {}

    def cancel_order(self, order_id: str, product_id: int) -> dict:
        """DELETE /v2/orders."""
        return self._signed_request("DELETE", "/v2/orders", body={"id": int(order_id), "product_id": int(product_id)}).get("result") or {}

    def positions(self) -> list[dict]:
        """GET /v2/positions/margined."""
        return self._signed_request("GET", "/v2/positions/margined").get("result") or []

    def verify_credentials(self) -> bool:
        self._signed_request("GET", "/v2/wallet/balances")
        return True

    @staticmethod
    def order_summary(order: dict) -> dict:
        size = int(order.get("size") or 0)
        unfilled = int(order.get("unfilled_size") or 0)
        return {
            "order_id": str(order.get("id") or ""),
            "status": _STATE_TO_STATUS.get(str(order.get("state") or "").lower(), "OPEN"),
            "filled": max(size - unfilled, 0),
            "unfilled": unfilled,
            "avg_price": float(order.get("average_fill_price") or 0),
            "product_id": order.get("product_id"),
            "reason": str(order.get("cancellation_reason") or ""),
        }
