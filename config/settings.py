"""
settings.py
────────────
Single source of runtime configuration for algo-2_0. Values come from the
process environment (.env, loaded by the entrypoint via python-dotenv) —
unlike the old shared/features/mongo_data.py, no Mongo URI or credential is
ever hardcoded here; a missing required var fails startup loudly instead of
silently falling back to a hardcoded live-DB connection string.
"""

from __future__ import annotations

import os
from functools import lru_cache


def _env_flag(name: str, default: bool = False) -> bool:
    raw = str(os.getenv(name, "")).strip().lower()
    if not raw:
        return default
    return raw in {"1", "true", "yes", "on"}


class Settings:
    def __init__(self) -> None:
        # Mongo connection and JWT secret/algorithm/expiry are no longer read
        # here — shared/db/mongo.py and shared/auth/service.py now delegate
        # to the main repo's features.mongo_data.MongoData and features.auth
        # directly (see those files' docstrings), which read MONGO_URI /
        # JWT_SECRET_KEY from the main repo's own shared/.env. Keeping a
        # second, separately-sourced copy of the same values here would just
        # be one more place for them to silently drift apart again — which
        # is exactly what caused the original login/logout bug this rewire
        # fixes (algo-2_0 minting tokens with its own placeholder secret that
        # algo.simulator/algo.trade/algo.websocket couldn't validate).

        # Enforced by default (unlike the old system's off-by-default kill switch) —
        # algo-2_0 is a fresh build, not a gradual migration, so there's no reason to
        # ship with auth silently disabled. Set false only for local dev convenience.
        self.auth_enforcement_enabled: bool = _env_flag("AUTH_ENFORCEMENT_ENABLED", default=True)

        # algo.websocket is untouched/external for now — algo-2_0 is a client of it.
        self.websocket_base_url: str = os.getenv("ALGO_WEBSOCKET_BASE_URL", "http://localhost:8003")

        self.service_port: int = int(os.getenv("ALGO_TRADE_2_0_PORT", "8010"))

        # REAL broker orders (Delta Exchange crypto legs). Off = every order
        # stays a VirtualBrokerAdapter paper fill. On = crypto legs whose
        # broker_scope_id is a Delta broker_configuration (api_key/api_secret)
        # place real orders on that account. NSE legs stay virtual either way.
        self.live_order_enabled: bool = _env_flag("LIVE_ORDER_ENABLED", default=False)
        # Marketable-limit buffer (% of LTP past the touch) and how long to
        # wait for it to fill before cancelling and sending a market order.
        self.live_limit_buffer_pct: float = float(os.getenv("LIVE_LIMIT_BUFFER_PCT", "2.0"))
        self.live_fill_timeout_seconds: float = float(os.getenv("LIVE_FILL_TIMEOUT_SECONDS", "5"))

        self.log_level: str = os.getenv("LOG_LEVEL", "INFO")
        # Default is OUTSIDE each service's own directory (algo.trade/,
        # algo.websocket/) on purpose: `uvicorn --reload` watches its cwd for
        # file changes, and a log file written INSIDE that same watched dir
        # triggers uvicorn to see its own log write as a "code change" and
        # restart itself — repeatedly, mid-write, causing the exact
        # intermittent "port bound, then refused a moment later" symptom a
        # dependent service (algo.trade's tick_client -> algo.websocket)
        # sees as flaky ECONNREFUSED. "../logs" lands in algo-2_0/logs/,
        # a sibling of both service dirs, outside whichever one is being
        # watched.
        self.log_dir: str = os.getenv("LOG_DIR", "../logs")


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
