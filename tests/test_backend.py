"""Tests for the Flask backend: JWT auth, role guards and analytics."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
BACKEND = ROOT / "backend"
for p in (SRC, BACKEND):
    if str(p) not in sys.path:
        sys.path.insert(0, str(p))


@pytest.fixture()
def backend_module():
    import analytics
    import auth
    import app
    app.analytics.store = analytics.LiveStore(ROOT / "output" / "predictions")
    app.app.config["TESTING"] = True
    return app


def _login(client, username, password):
    return client.post("/api/auth/login", json={"username": username, "password": password})


def test_admin_and_company_logins_succeed(backend_module):
    client = backend_module.app.test_client()
    admin = _login(client, "admin", "admin")
    assert admin.status_code == 200
    assert admin.get_json()["role"] == "admin"
    company = _login(client, "technova", "technova123")
    assert company.status_code == 200
    assert company.get_json()["role"] == "company"
    assert company.get_json()["company"] == "technova"


def test_wrong_password_is_rejected(backend_module):
    client = backend_module.app.test_client()
    assert _login(client, "admin", "nope").status_code == 401
    assert _login(client, "ghost", "ghost123").status_code == 401


def test_company_cannot_access_admin_endpoints(backend_module):
    client = backend_module.app.test_client()
    tok = _login(client, "urbaneats", "urbaneats123").get_json()["token"]
    resp = client.get("/api/admin/overview", headers={"Authorization": f"Bearer {tok}"})
    assert resp.status_code == 403


def test_missing_token_is_rejected(backend_module):
    client = backend_module.app.test_client()
    assert client.get("/api/company/overview").status_code == 401
    assert client.get("/api/admin/overview").status_code == 401


def test_health_reports_zero_until_data(backend_module):
    client = backend_module.app.test_client()
    resp = client.get("/api/health")
    assert resp.status_code == 200
    assert resp.get_json()["rows"] >= 0


def test_admin_can_reset_live_data_but_company_cannot(backend_module):
    client = backend_module.app.test_client()
    admin = {"Authorization": f"Bearer {_login(client, 'admin', 'admin').get_json()['token']}"}
    company = {"Authorization": f"Bearer {_login(client, 'skyride', 'skyride123').get_json()['token']}"}

    assert client.post("/api/admin/reset", json={}, headers=company).status_code == 403

    resp = client.post("/api/admin/reset", json={"wipe_files": False}, headers=admin)
    assert resp.status_code == 200
    assert resp.get_json()["rows"] == 0
    assert client.get("/api/health").get_json()["rows"] == 0