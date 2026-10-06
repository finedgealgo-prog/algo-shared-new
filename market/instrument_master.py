"""
instrument_master.py
──────────────────────
Static option/instrument metadata, keyed by token, prewarmed at startup —
master-doc §6. Reads the existing `active_option_tokens` Mongo collection
(same schema the old shared/features/dhan_token_sync.py writes: broker,
instrument, expiry, strike, option_type, exchange, symbol, token, ws_segment,
lot_size) as DATA — algo-2_0 does not own or run that sync job; it's reused
read-only, consistent with the plan's "Mongo collections reused as data"
contract.
"""

from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache

from shared.db.mongo import Mongo


@dataclass(frozen=True)
class InstrumentMeta:
    token: str
    symbol: str
    instrument: str
    expiry: str
    strike: float
    option_type: str
    exchange: str
    ws_segment: str
    lot_size: int | None


class InstrumentMaster:
    def __init__(self) -> None:
        self._by_token: dict[str, InstrumentMeta] = {}
        # (underlying, expiry, option_type) -> [InstrumentMeta] — for
        # strike_resolver.py's chain-row assembly (never a Mongo query per tick).
        self._by_chain_key: dict[tuple[str, str, str], list[InstrumentMeta]] = {}

    def load(self, mongo: Mongo, broker: str = "dhan") -> int:
        """Bulk load from active_option_tokens — call once at startup, not
        per-tick/per-request (master-doc §6: 'do not query instrument
        metadata repeatedly during market events')."""
        cursor = mongo.raw["active_option_tokens"].find({"broker": broker})
        loaded = 0
        for doc in cursor:
            token = str(doc.get("token") or doc.get("tokens") or "").strip()
            if not token:
                continue
            meta = InstrumentMeta(
                token=token,
                symbol=str(doc.get("symbol") or ""),
                instrument=str(doc.get("instrument") or ""),
                expiry=str(doc.get("expiry") or ""),
                strike=float(doc.get("strike") or 0),
                option_type=str(doc.get("option_type") or ""),
                exchange=str(doc.get("exchange") or ""),
                ws_segment=str(doc.get("ws_segment") or ""),
                lot_size=doc.get("lot_size"),
            )
            self._add(meta)
            loaded += 1
        return loaded

    def _add(self, meta: InstrumentMeta) -> None:
        self._by_token[meta.token] = meta
        key = (meta.instrument.upper(), meta.expiry, meta.option_type.upper())
        self._by_chain_key.setdefault(key, []).append(meta)

    def get(self, token: str) -> InstrumentMeta | None:
        return self._by_token.get(str(token))

    def by_underlying_expiry_type(self, underlying: str, expiry: str, option_type: str) -> list[InstrumentMeta]:
        return self._by_chain_key.get((str(underlying or "").upper(), str(expiry or ""), str(option_type or "").upper()), [])

    def expiries_for(self, underlying: str, option_type: str = "CE") -> list[str]:
        """Distinct expiries available for this underlying, sorted ascending
        — feeds selection/expiry_resolver.py's Weekly/NextWeekly/Monthly/
        NextMonthly resolution (master-doc §6: from prewarmed static data,
        never a Mongo query at entry time)."""
        underlying_u = str(underlying or "").upper()
        option_type_u = str(option_type or "").upper()
        expiries = {
            key[1] for key in self._by_chain_key
            if key[0] == underlying_u and key[2] == option_type_u
        }
        return sorted(expiries)

    def __len__(self) -> int:
        return len(self._by_token)


@lru_cache(maxsize=1)
def get_instrument_master() -> InstrumentMaster:
    return InstrumentMaster()
