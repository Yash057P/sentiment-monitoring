"""Simple JWT authentication for the Flask backend.

Seeded accounts:
    admin / admin            -> role "admin"  (the admin dashboard)
    <company handle> / <handle>123  -> role "company"  (one per demo company)

""" 

from __future__ import annotations

import datetime as dt
import logging
import sys
from pathlib import Path

import jwt

import companies

logger = logging.getLogger("sentiment.auth")

BACKEND_DIR = Path(__file__).resolve().parent
ROOT_DIR = BACKEND_DIR.parent
if str(ROOT_DIR / "src") not in sys.path:
    sys.path.insert(0, str(ROOT_DIR / "src"))

SECRET_KEY = "brandscope-demo-secret-change-me"
ALGORITHM = "HS256"
TOKEN_TTL_HOURS = 12

ADMIN_USERNAME = "admin"
ADMIN_PASSWORD = "admin"


def users() -> dict:
    """All valid accounts: {username: {password, role, company}}."""
    result: dict = {
        ADMIN_USERNAME: {
            "password": ADMIN_PASSWORD,
            "role": "admin",
            "company": None,
            "display": "Platform Admin",
        }
    }
    for company in companies.COMPANIES:
        result[company.handle] = {
            "password": company.password,
            "role": "company",
            "company": company.handle,
            "display": company.name,
        }
    return result


def authenticate(username: str, password: str) -> tuple[dict | None, dict | None]:
    """Return (token_payload_data, error) - exactly one is non-None."""
    account = users().get(username)
    if account is None or account["password"] != password:
        return None, {"message": "Invalid username or password."}
    payload = {
        "sub": username,
        "role": account["role"],
        "company": account["company"],
        "display": account["display"],
        "exp": dt.datetime.now(dt.timezone.utc)
        + dt.timedelta(hours=TOKEN_TTL_HOURS),
    }
    token = jwt.encode(payload, SECRET_KEY, algorithm=ALGORITHM)
    return {
        "token": token,
        "username": username,
        "role": account["role"],
        "company": account["company"],
        "display": account["display"],
    }, None


def decode_token(token: str) -> dict | None:
    try:
        return jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
    except Exception:
        return None