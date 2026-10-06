"""
service.py
──────────
Backed by the main repo's shared/features/auth.py (reached via the
algo.trade/features -> ../../shared/features symlink) — same JWT secret,
same `user_details` Mongo collection, same token/session semantics as
algo.trade (8000), algo.simulator (8001), algo.websocket and algo.scanner.

algo-2_0 used to carry its own independent reimplementation of every
function below (same collection/shape, but its own JWT_SECRET_KEY value in
.env) — that mismatch is exactly what broke cross-service auth the moment a
client (e.g. algo-admin) mixed a call to algo-2_0 with a call to one of the
other services: a token minted here couldn't be decoded there, or vice
versa. This file is now a thin re-export/adapter layer over the real
implementation (same function names/signatures as before), so no router
file needed to change.
"""

from __future__ import annotations

from typing import Any, Optional

from bson import ObjectId
from features import auth as _auth

from shared.db.mongo import Mongo

USERS_COLLECTION = _auth.USERS_COLLECTION
DEFAULT_COUNTRY_CODE = _auth.DEFAULT_COUNTRY_CODE

normalize_mobile = _auth.normalize_mobile
normalize_email = _auth.normalize_email
hash_password = _auth.hash_password
verify_password = _auth.verify_password
create_access_token = _auth.create_access_token
decode_access_token = _auth.decode_access_token
public_user = _auth.public_user


def register_user(mongo: Mongo, payload: dict[str, Any]) -> dict[str, Any]:
    return _auth.register_user(mongo.mongo_data, payload)


def authenticate_user(mongo: Mongo, mobile_raw: str, password: str) -> dict[str, Any]:
    return _auth.authenticate_user(mongo.mongo_data, mobile_raw, password)


def get_user_by_id(mongo: Mongo, user_id: str) -> Optional[dict[str, Any]]:
    try:
        return mongo.raw[USERS_COLLECTION].find_one({"_id": ObjectId(user_id)})
    except Exception:
        return None
