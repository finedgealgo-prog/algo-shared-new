"""
adapter.py (Virtual)
──────────────────────
Simulated broker — no real network call, instant fill. Matches the old
system's `LIVE_ORDER_STATUS=false` behavior exactly (see
shared/features/live_order_manager.py's `_build_simulated_live_order_id` /
`_is_live_order_punch_enabled`): "when off, orders are simulated with
instant COMPLETE status, no broker call at all."

Implements the SAME BrokerAdapter interface real adapters (DhanAdapter) do
— order_engine.py depends only on that interface, so swapping this for
DhanAdapter later (when going live) is a one-line change, not a rewrite.
"""

from __future__ import annotations

import time
import uuid
from typing import Any

from shared.brokers.base import BrokerAdapter


class VirtualBrokerAdapter(BrokerAdapter):
    def __init__(self) -> None:
        self._orders: dict[str, dict[str, Any]] = {}

    def place_order(
        self, *, tradingsymbol: str, exchange: str, transaction_type: str,
        quantity: int, order_type: str, product: str, variety: str = "regular",
        price: float = 0.0, trigger_price: float = 0.0, validity: str = "DAY",
        **kwargs: Any,
    ) -> str:
        order_id = f"VIRT-{uuid.uuid4().hex[:12]}"
        fill_price = price if price > 0 else kwargs.get("fill_price", 0.0)
        self._orders[order_id] = {
            "order_id": order_id, "status": "COMPLETE",
            "average_price": fill_price, "price": price, "trigger_price": trigger_price,
            "filled_quantity": quantity, "quantity": quantity,
            "tradingsymbol": tradingsymbol, "exchange": exchange,
            "transaction_type": transaction_type, "product": product,
            "last_price": fill_price, "order_time": time.strftime("%Y-%m-%d %H:%M:%S"),
        }
        return order_id

    def modify_order(self, *, order_id: str, variety: str = "regular", order_type: str = "LIMIT",
                      quantity: int | None = None, price: float = 0.0, trigger_price: float = 0.0,
                      validity: str = "DAY", **kwargs: Any) -> str:
        order = self._orders.get(order_id)
        if order:
            if quantity is not None:
                order["quantity"] = quantity
            order["price"] = price
            order["trigger_price"] = trigger_price
        return order_id

    def cancel_order(self, *, order_id: str, variety: str = "regular") -> str:
        order = self._orders.get(order_id)
        if order:
            order["status"] = "CANCELLED"
        return order_id

    def orders(self) -> list[dict[str, Any]]:
        return list(self._orders.values())

    def quote(self, symbols: list[str]) -> dict[str, dict[str, Any]]:
        return {}
