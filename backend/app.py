"""Flask REST API + static web server for the BrandScope platform.

Roles:

    admin   -> /api/admin/...        sees all companies, their outlook
                                      (green/red) and every tweet
    company -> /api/company/...      sees ONLY its own dashboard: overall
                                      analysis, favourable tweets, negative
                                      impacts and the prediction/forecast

The API reads the live parquet predictions that Spark Structured Streaming
keeps writing, so the numbers refresh in near real time.

Run (from the project root):
    .venv\\Scripts\\python.exe backend\\app.py
"""

from __future__ import annotations

import functools
import os
import sys
from pathlib import Path

from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS

ROOT_DIR = Path(__file__).resolve().parents[1]
BACKEND_DIR = Path(__file__).resolve().parent
if str(ROOT_DIR / "src") not in sys.path:
    sys.path.insert(0, str(ROOT_DIR / "src"))

import analytics, auth, companies, config  # noqa: E402

PORT = int(os.environ.get("BACKEND_PORT", "8000"))
FRONTEND_BUILD = ROOT_DIR / "frontend" / "dist"

app = Flask(__name__, static_folder=None)
CORS(app)


def _bearer_token() -> str | None:
    header = request.headers.get("Authorization", "")
    if header.lower().startswith("bearer "):
        return header[7:].strip()
    return None


def require_role(role: str):
    """Decorator: require a valid token with the given role."""

    def decorator(fn):
        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            token = _bearer_token()
            payload = auth.decode_token(token) if token else None
            if payload is None:
                return jsonify({"message": "Not authenticated."}), 401
            if payload.get("role") != role:
                return jsonify({"message": "Forbidden."}), 403
            request.identity = payload  # type: ignore[attr-defined]
            return fn(*args, **kwargs)

        return wrapper

    return decorator


# ---------------------------------------------------------------------------
# Auth ------------------------------------------------------------------------
# ---------------------------------------------------------------------------

@app.post("/api/auth/login")
def login():
    body = request.get_json(silent=True) or {}
    username = (body.get("username") or "").strip()
    password = body.get("password") or ""
    data, error = auth.authenticate(username, password)
    if error:
        return jsonify(error), 401
    return jsonify(data)


@app.get("/api/me")
def me():
    token = _bearer_token()
    payload = auth.decode_token(token) if token else None
    if payload is None:
        return jsonify({"message": "Not authenticated."}), 401
    return jsonify(
        {
            "username": payload["sub"],
            "role": payload.get("role"),
            "company": payload.get("company"),
            "display": payload.get("display"),
        }
    )


@app.get("/api/health")
def health():
    return jsonify({"status": "ok", "rows": len(analytics.store.rows())})


@app.get("/api/companies/meta")
def companies_meta():
    """Brand info used on the login screen (public)."""
    return jsonify(
        {
            "companies": [
                {
                    "handle": c.handle,
                    "name": c.name,
                    "sector": c.sector,
                    "logo": c.logo,
                    "color": c.color,
                    "tagline": c.tagline,
                }
                for c in companies.COMPANIES
            ]
        }
    )


# ---------------------------------------------------------------------------
# Admin endpoints -------------------------------------------------------------
# ---------------------------------------------------------------------------

@app.get("/api/admin/overview")
@require_role("admin")
def admin_overview():
    return jsonify(analytics.admin_overview())


@app.get("/api/overall")
def overall():
    """Public platform overview (used by both admin and companies)."""
    return jsonify(analytics.admin_overview())


@app.get("/api/admin/tweets")
@require_role("admin")
def admin_tweets():
    company = request.args.get("company")
    sentiment = request.args.get("sentiment", "")
    limit = min(int(request.args.get("limit", 200)), 500)
    df = analytics.store.companies_df()
    rows = df.to_dict(orient="records") if len(df) else []
    if company:
        rows = [r for r in rows if r.get("company") == company]
    if sentiment in ("positive", "negative"):
        label = "Positive" if sentiment == "positive" else "Negative"
        rows = [r for r in rows if r.get("predicted_sentiment") == label]
    rows.sort(key=lambda r: r.get("ingestion_ts_ms") or 0, reverse=True)
    return jsonify({"tweets": rows[:limit]})


# ---------------------------------------------------------------------------
# Company endpoints --------------------------------------------------------------
# ---------------------------------------------------------------------------

@app.get("/api/company/overview")
@require_role("company")
def company_overview():
    handle = request.identity["company"]
    try:
        return jsonify(analytics.company_overview(handle))
    except KeyError:
        return jsonify({"message": "Unknown company."}), 404


@app.get("/api/company/tweets")
@require_role("company")
def company_tweets():
    handle = request.identity["company"]
    sentiment = request.args.get("sentiment", "")
    limit = min(int(request.args.get("limit", 100)), 300)
    return jsonify({"tweets": analytics.tweet_rows(handle, sentiment, limit)})


@app.post("/api/admin/reset")
@require_role("admin")
def admin_reset():
    """Reset the live data so every counter restarts from 0.

    Body: {"wipe_files": true}  - also delete the parquet files already
    produced (default). Pass false to only clear the in-memory view while
    keeping the files on disk.
    """
    body = request.get_json(silent=True) or {}
    wipe = body.get("wipe_files", True)
    result = analytics.store.reset(wipe_files=bool(wipe))
    return jsonify({"status": "reset", "wiped_files": bool(wipe), **result})


# ---------------------------------------------------------------------------
# Static frontend ---------------------------------------------------------------
# ---------------------------------------------------------------------------

@app.get("/")
@app.get("/<path:path>")
def spa(path="index.html"):
    if FRONTEND_BUILD.joinpath(path).is_file():
        return send_from_directory(FRONTEND_BUILD, path)
    index = FRONTEND_BUILD / "index.html"
    if index.is_file():
        return send_from_directory(FRONTEND_BUILD, "index.html")
    return jsonify(
        {
            "message": "Frontend not built yet. Run:  cd frontend && npm install && npm run build"
        }
    ), 200


if __name__ == "__main__":
    import logging

    config.configure_logging(logging.INFO)
    analytics.store.refresh(force=True)
    print(f"BrandScope API listening on http://localhost:{PORT}")
    app.run(host="0.0.0.0", port=PORT, debug=False, threaded=True)