"""
totp_login.py (Dhan)
──────────────────────
Daily unattended Dhan access_token refresh via Dhan's documented TOTP login
endpoint (auth.dhan.co/app/generateAccessToken). Ported from the old
shared/features/dhan_totp_login.py — same source of credentials
(kite_market_config's dhan doc: user_id/totp/pin), same generate-token call.

Token is valid 24h from generation — meant to run once daily (cron hitting a
future /v1/broker/dhan/totp-login route, or this module directly) to keep
order placement (adapter.py) working without a manual login each morning.

NOT ported yet (deliberately, flagged so it isn't mistaken for an oversight):
  - Mirroring the refreshed token into `broker_configuration` for a separate
    multi-account order-placement path — algo-2_0's DhanAdapter reads
    exclusively from kite_market_config (single source), so there is nothing
    to mirror to yet. Revisit if/when multi-account broker_configuration
    support is built.
  - Telegram notification on refresh success/failure — port once algo-2_0
    has its own notification module; until then, monitor via logs/exit code.
"""

from __future__ import annotations

from datetime import datetime, timezone

import pyotp
import requests

from shared.brokers.dhan.credentials import get_dhan_credentials, save_dhan_access_token
from shared.db.mongo import Mongo
from shared.logging.logger import get_logger

log = get_logger(__name__)

_GENERATE_TOKEN_URL = "https://auth.dhan.co/app/generateAccessToken"


def _decode_access_token_expiry(access_token: str) -> str:
    """Read the `exp` claim out of the Dhan access_token's own JWT payload
    (unverified — received directly from Dhan) as a fallback when Dhan's
    response doesn't include expiryTime."""
    try:
        import base64
        import json

        parts = (access_token or "").split(".")
        if len(parts) != 3:
            return ""
        padded = parts[1] + "=" * (-len(parts[1]) % 4)
        payload = json.loads(base64.urlsafe_b64decode(padded))
        exp = payload.get("exp")
        if not exp:
            return ""
        return datetime.fromtimestamp(int(exp), tz=timezone.utc).isoformat()
    except Exception:
        return ""


def refresh_dhan_token_via_totp(mongo: Mongo) -> dict:
    """Generate a fresh Dhan access_token from PIN+TOTP and persist it to
    kite_market_config (broker=dhan)."""
    creds = get_dhan_credentials(mongo)
    client_id = str(creds.get("user_id") or creds.get("dhan_client_id") or "").strip()
    secret = str(creds.get("totp") or "").strip()
    pin = str(creds.get("pin") or "").strip()

    if not client_id or not secret or not pin:
        message = "kite_market_config (broker=dhan) is missing user_id/totp/pin"
        log.error("[dhan totp login] %s", message)
        return {"ok": False, "message": message}

    totp_code = pyotp.TOTP(secret).now()

    try:
        resp = requests.post(
            _GENERATE_TOKEN_URL,
            params={"dhanClientId": client_id, "pin": pin, "totp": totp_code},
            timeout=10,
        )
        resp.raise_for_status()
        data = resp.json()
    except Exception as exc:
        log.exception("[dhan totp login] generateAccessToken request failed")
        return {"ok": False, "message": f"generateAccessToken error: {exc}"}

    access_token = str(data.get("accessToken") or "").strip()
    if not access_token:
        message = f"No accessToken in response: {data}"
        log.error("[dhan totp login] %s", message)
        return {"ok": False, "message": message}

    expiry_time = str(data.get("expiryTime") or "").strip() or _decode_access_token_expiry(access_token)
    resolved_client_id = str(data.get("dhanClientId") or client_id).strip()

    save_dhan_access_token(mongo, resolved_client_id, access_token, expiry_time)

    log.info(
        "[dhan totp login] token refreshed client_id=%s expiry=%s",
        resolved_client_id, expiry_time,
    )
    return {
        "ok": True,
        "message": f"Dhan access token refreshed via TOTP. Valid until {expiry_time}.",
        "client_id": resolved_client_id,
        "expiry_time": expiry_time,
    }


if __name__ == "__main__":
    import pathlib
    import sys

    from dotenv import load_dotenv

    load_dotenv(pathlib.Path(__file__).resolve().parent.parent.parent.parent / ".env")

    from shared.db.mongo import get_mongo

    result = refresh_dhan_token_via_totp(get_mongo())
    sys.exit(0 if result.get("ok") else 1)
