"""
dependency.py
──────────────
FastAPI dependency for resolving the current authenticated user from a
Bearer token. Auth is enforced by default (Settings.auth_enforcement_enabled) —
see settings.py for why this differs from the old system's default-off kill switch.
"""

from __future__ import annotations

from typing import Any, Optional

import jwt
from fastapi import Header, HTTPException

from shared.auth.service import decode_access_token, get_user_by_id
from shared.config.settings import get_settings
from shared.db.mongo import get_mongo


def get_current_user(authorization: Optional[str] = Header(None)) -> dict[str, Any]:
    settings = get_settings()
    if not settings.auth_enforcement_enabled:
        # A real browser session still sends its own real JWT (signed by
        # whatever backend issued it, routinely NOT algo-2_0's dev
        # JWT_SECRET_KEY, so decode_access_token's signature check below
        # would reject it) — with enforcement off there's no real security
        # boundary to enforce anyway, so still recover the real `sub`/user
        # unverified rather than always returning `_id: None`. Same fix
        # api/ws_client_hub.py's authenticate() already applies for the
        # WS path (see its own docstring) — this REST dependency never got
        # it, so every strategy activated through the actual UI was
        # persisted with an empty user_id, which silently broke every
        # user_id-scoped read downstream (execute-orders' initial snapshot,
        # broker-settings socket push, list_strategies' own filter).
        if authorization and authorization.lower().startswith("bearer "):
            token = authorization.split(" ", 1)[1].strip()
            try:
                unverified = jwt.decode(token, options={"verify_signature": False})
                sub = str(unverified.get("sub") or "").strip()
                if sub:
                    user = get_user_by_id(get_mongo(), sub)
                    if user:
                        return user
                    return {"_id": sub, "mobile": "", "name": "Auth Disabled", "is_admin": False}
            except Exception:
                pass
        return {"_id": None, "mobile": "", "name": "Auth Disabled", "is_admin": False}

    if not authorization or not authorization.lower().startswith("bearer "):
        raise HTTPException(status_code=401, detail="Missing authentication token")
    token = authorization.split(" ", 1)[1].strip()
    claims = decode_access_token(token)

    user = get_user_by_id(get_mongo(), claims["sub"])
    if not user:
        raise HTTPException(status_code=401, detail="User not found")
    return user
