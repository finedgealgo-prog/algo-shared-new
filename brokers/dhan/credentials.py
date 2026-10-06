"""
credentials.py (Dhan)
──────────────────────
Reads/writes Dhan credentials from MongoDB `kite_market_config` (broker="dhan")
— the same collection the old system's OAuth-consent flow and cron-driven TOTP
refresh use. Kept separate from adapter.py so totp_login.py can update
credentials without importing the order-placement adapter.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Optional

from shared.brokers.dhan.adapter import DhanAdapter
from shared.db.mongo import Mongo

_COLLECTION = "kite_market_config"


def get_dhan_credentials(mongo: Mongo) -> dict[str, Any]:
    return mongo.raw[_COLLECTION].find_one({"broker": "dhan"}) or {}


def get_dhan_adapter(mongo: Mongo) -> Optional[DhanAdapter]:
    creds = get_dhan_credentials(mongo)
    client_id = str(creds.get("user_id") or creds.get("dhan_client_id") or "").strip()
    access_token = str(creds.get("access_token") or "").strip()
    if not client_id or not access_token:
        return None
    return DhanAdapter(mongo, client_id, access_token)


def save_dhan_access_token(mongo: Mongo, client_id: str, access_token: str, expiry_time: str) -> None:
    mongo.raw[_COLLECTION].update_one(
        {"broker": "dhan"},
        {"$set": {
            "user_id": client_id,
            "access_token": access_token,
            "expiry_time": expiry_time,
            "login_time": datetime.now(timezone.utc).isoformat(),
        }},
        upsert=True,
    )
