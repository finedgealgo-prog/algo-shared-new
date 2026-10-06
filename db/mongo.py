"""
mongo.py
────────
Backed by the main repo's shared/features/mongo_data.py (`MongoData`,
reached via the algo.trade/features -> ../../shared/features symlink) —
the same connection every other service (algo.trade:8000, algo.simulator,
algo.websocket, algo.scanner) uses, instead of algo-2_0's own independent
MongoClient. Same DB in practice either way (both resolved to
localhost:27017/stock_data), but this removes the second, separately
configured connection path entirely.

`.raw` still exposes the underlying pymongo Database, same shape as
before, so existing call sites (`mongo.raw[collection]`) don't change.
`.mongo_data` exposes the MongoData instance itself, for delegating into
ported `features.*` functions that expect a `db` argument shaped like
`db._db[...]` (MongoData has that attribute).
"""

from __future__ import annotations

from functools import lru_cache

from features.mongo_data import MongoData


class Mongo:
    def __init__(self) -> None:
        self._mongo_data = MongoData()

    @property
    def raw(self):
        return self._mongo_data._db

    @property
    def mongo_data(self) -> MongoData:
        return self._mongo_data

    def close(self) -> None:
        # MongoData owns a cached, shared client (per-URI singleton with its
        # own pool) — not this wrapper's to close.
        pass


@lru_cache(maxsize=1)
def get_mongo() -> Mongo:
    return Mongo()
