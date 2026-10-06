"""
base.py
───────
Common interface every broker adapter (Dhan, Kite, FlatTrade) implements.
Mirrors the informal Kite-shaped surface the old system's adapters converged
on (DhanAdapter, FlatTradeAdapter) — kept explicit here as an ABC instead of
duck-typing, since algo-2_0's order engine (Phase 6) will depend on this
contract directly rather than probing for methods at runtime.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any


class BrokerAdapter(ABC):
    """Order placement/management surface. Market data (WS ticker) is a
    separate concern — see shared/market/ — brokers only need to expose the
    ability to place/modify/cancel orders, list the order book, and fetch a
    quote/depth snapshot as a REST fallback."""

    @abstractmethod
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
        **kwargs: Any,
    ) -> str:
        """Returns the broker order_id."""

    @abstractmethod
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
        """Returns the (possibly unchanged) broker order_id."""

    @abstractmethod
    def cancel_order(self, *, order_id: str, variety: str = "regular") -> str:
        """Returns the cancelled order_id."""

    @abstractmethod
    def orders(self) -> list[dict[str, Any]]:
        """Order book, normalized to the shared shape (see docstring in each
        adapter for the exact field list)."""

    @abstractmethod
    def quote(self, symbols: list[str]) -> dict[str, dict[str, Any]]:
        """REST quote/depth snapshot keyed by 'EXCHANGE:TRADINGSYMBOL'."""
