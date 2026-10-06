"""
adapter.py (Dhan)
──────────────────
Thin BrokerAdapter-shaped wrapper around the main repo's
features.dhan_broker.DhanAdapter (reached via the algo.trade/features ->
../../shared/features symlink) — same real-money-tested Dhan v2 REST logic
every other service uses, instead of algo-2_0's own independent copy of it.

algo-2_0's copy here used to be a near-exact but separately maintained port
of the same class (same field mappings, same status tables) — kept in sync
by hand. This wrapper removes that second copy: place_order/modify_order/
cancel_order/orders/quote all forward straight through to the real
implementation, only translating the small keyword-only vs
positional/default-order differences between algo-2_0's BrokerAdapter ABC
and the main repo class's original signatures.

quote() still calls the real adapter's REST-only quote() — the WS-first
fast path (reading live bid/ask out of broker_gateway's process-wide tick
maps) needs algo-2_0's own tick pipeline (Phase 1), which doesn't exist yet;
see the old docstring here for that context, unchanged.
"""

from __future__ import annotations

from typing import Any

from features.dhan_broker import DhanAdapter as _RealDhanAdapter

from shared.brokers.base import BrokerAdapter
from shared.db.mongo import Mongo


class DhanAdapter(BrokerAdapter):
    def __init__(self, mongo: Mongo, client_id: str, access_token: str) -> None:
        self.mongo = mongo
        self.client_id = client_id
        self.access_token = access_token
        self._real = _RealDhanAdapter(mongo.mongo_data, client_id, access_token)

    def place_order(
        self,
        *,
        tradingsymbol: str,
        exchange: str,
        transaction_type: str,
        quantity: int,
        order_type: str,
        product: str,
        variety: str = "regular",
        price: float = 0.0,
        trigger_price: float = 0.0,
        validity: str = "DAY",
        security_id: str = "",
        exchange_segment: str = "",
        **kwargs: Any,
    ) -> str:
        # The real adapter's place_order doesn't take **kwargs — anything
        # extra here is silently dropped, same as it would be ignored by the
        # ABC contract's forward-compat **kwargs on the caller side.
        return self._real.place_order(
            tradingsymbol=tradingsymbol,
            exchange=exchange,
            transaction_type=transaction_type,
            quantity=quantity,
            order_type=order_type,
            product=product,
            variety=variety,
            price=price,
            trigger_price=trigger_price,
            validity=validity,
            security_id=security_id,
            exchange_segment=exchange_segment,
        )

    def modify_order(
        self,
        *,
        order_id: str,
        variety: str = "regular",
        order_type: str = "LIMIT",
        quantity: int | None = None,
        price: float = 0.0,
        trigger_price: float = 0.0,
        validity: str = "DAY",
        **kwargs: Any,
    ) -> str:
        return self._real.modify_order(
            order_id=order_id,
            order_type=order_type,
            quantity=quantity,
            price=price,
            trigger_price=trigger_price,
            validity=validity,
        )

    def cancel_order(self, *, order_id: str, variety: str = "regular") -> str:
        return self._real.cancel_order(variety=variety, order_id=order_id)

    def orders(self) -> list[dict[str, Any]]:
        return self._real.orders()

    def quote(self, symbols: list[str]) -> dict[str, dict[str, Any]]:
        return self._real.quote(symbols)
