"""Tests for /api/control (session + poll) and POST /api/log (phone receiver).

Control now returns a session id immediately and the client polls
``/api/control/poll`` for log lines, so a phone POSTing into an active session
is a plain request/response flow — testable through the TestClient.  The
"real flow" tests still spin up a real uvicorn server (see ``_LiveServer``)
to exercise true cross-request HTTP with a shared queue, which is exactly the
scenario the Redis backend exists for.

Dry-run mode is on (see conftest), so the real MacroDroid webhook is never
called.  Tests that need the real-webhook path monkeypatch
``index.trigger_macrodroid`` and flip ``index._CONTROL_DRY_RUN`` instead.
"""

from __future__ import annotations

import asyncio
import threading
import time

import httpx
import pytest
import uvicorn

from api import index
from api.control import CONTROL_MAP, resolve_action, trigger_url
from api.index import app
from api.logqueue import get_log_store


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _create_session(sid: str) -> None:
    """Create a session directly in the (fresh) log store."""
    asyncio.run(get_log_store().create_session(sid))


def _fetch_lines(sid: str, offset: int = 0) -> tuple[list[str], bool]:
    lines, _, done = asyncio.run(get_log_store().fetch(sid, offset))
    return lines, done


def _poll_to_done(get, sid: str, max_polls: int = 40, interval: float = 0.1):
    """Poll /api/control/poll via ``get(url)`` until the session is done,
    collecting the lines that arrive meanwhile.

    ``get`` is any callable returning a response with ``.status_code`` and
    ``.json()`` — works for both TestClient and a live httpx client.
    """
    lines: list[str] = []
    offset = 0
    done = False
    for _ in range(max_polls):
        r = get(f"/api/control/poll?session={sid}&offset={offset}")
        assert r.status_code == 200, r.text
        data = r.json()
        lines.extend(data["lines"])
        offset = data["offset"]
        done = data["done"]
        if done:
            break
        time.sleep(interval)
    return lines, done


class _LiveServer:
    """Run the app on a real uvicorn server in a background thread.

    This is the only faithful way to test a phone POSTing log lines *while*
    the client stream is still open (the TestClient transport fully consumes
    a streaming response before returning).
    """

    def __init__(self) -> None:
        self._config = uvicorn.Config(app, host="127.0.0.1", port=0, log_level="warning")
        self._server = uvicorn.Server(self._config)
        self._thread = threading.Thread(target=self._server.run, daemon=True)
        self.base = ""

    def __enter__(self) -> "_LiveServer":
        self._thread.start()
        deadline = time.monotonic() + 10
        while not getattr(self._server, "started", False):
            if time.monotonic() > deadline:
                raise RuntimeError("uvicorn failed to start")
            time.sleep(0.01)
        port = self._server.servers[0].sockets[0].getsockname()[1]
        self.base = f"http://127.0.0.1:{port}"
        return self

    def __exit__(self, *exc: object) -> None:
        self._server.should_exit = True
        self._thread.join(timeout=10)


# ---------------------------------------------------------------------------
# Action resolution / URL building
# ---------------------------------------------------------------------------

def test_control_map_contains_all_actions():
    assert set(CONTROL_MAP) == {
        "unlock", "lock", "headlights", "honk", "trunk_open",
        "trunk_close", "windows_close", "engine_off",
    }


@pytest.mark.parametrize("q", ["unlock", "LOCK", " HeadLights "])
def test_resolve_action_case_insensitive(q):
    assert resolve_action(q) is not None


def test_resolve_action_unknown_returns_none():
    assert resolve_action("open_door") is None
    assert resolve_action("") is None
    assert resolve_action(None) is None


def test_trigger_url_appends_control_and_sid():
    url = trigger_url("lock")
    assert url.endswith("?control=lock")
    assert "sid=" not in url

    url = trigger_url("honk", sid="abc123")
    assert url.endswith("&sid=abc123")


# ---------------------------------------------------------------------------
# /api/control — dry-run polling
# ---------------------------------------------------------------------------

def test_dry_run_runs_to_completion_via_poll(client):
    """Control returns a session immediately; the dry-run phone simulation
    writes log lines in the background that polling then delivers."""
    r = client.get("/api/control?q=unlock")
    assert r.status_code == 200
    data = r.json()
    assert data["ok"] is True
    assert data["action"] == "unlock"
    assert data["dry_run"] is True
    assert "session" in data
    assert data["poll_url"].startswith("/api/control/poll?session=")

    lines, done = _poll_to_done(client.get, data["session"])
    assert done is True
    assert "[unlock] sim: received control command" in lines
    assert "[unlock] sim: done" in lines


def test_control_unknown_action_400(client):
    r = client.get("/api/control?q=open_door")
    assert r.status_code == 400
    assert "Unknown or missing control action" in r.json()["detail"]


def test_control_missing_action_400(client):
    r = client.get("/api/control")
    assert r.status_code == 400


# ---------------------------------------------------------------------------
# Auth (CONTROL_API_KEY)
# ---------------------------------------------------------------------------

def test_control_requires_key_when_configured(client, monkeypatch):
    monkeypatch.setattr(index, "_CONTROL_API_KEY", "sekret")

    r = client.get("/api/control?q=lock")
    assert r.status_code == 401

    r = client.get("/api/control?q=lock&key=sekret")
    assert r.status_code == 200

    r = client.get(
        "/api/control?q=lock",
        headers={"Authorization": "Bearer sekret"},
    )
    assert r.status_code == 200


def test_log_requires_key_when_configured(client, monkeypatch):
    monkeypatch.setattr(index, "_CONTROL_API_KEY", "sekret")
    _create_session("abc123")

    r = client.post("/api/log?line=hi")
    assert r.status_code == 401

    r = client.post("/api/log?line=hi&key=sekret")
    assert r.status_code == 200


# ---------------------------------------------------------------------------
# Real-flow: phone POSTs into an active stream (live uvicorn server)
# ---------------------------------------------------------------------------

@pytest.fixture()
def _real_trigger(monkeypatch):
    """Turn off dry-run and stub the MacroDroid webhook with a fast no-op."""
    monkeypatch.setattr(index, "_CONTROL_DRY_RUN", False)

    async def fake_trigger(action, sid=None, timeout=8.0):
        return {"ok": True, "status_code": 200}

    monkeypatch.setattr(index, "trigger_macrodroid", fake_trigger)


def test_phone_posts_log_to_polling_client(_real_trigger):
    """The phone POSTs live status lines to /api/log while the client polls
    /api/control/poll — lines flow through the shared queue to the poller."""
    with _LiveServer() as server:
        with httpx.Client(timeout=httpx.Timeout(30.0, connect=10.0)) as c:
            r = c.get(f"{server.base}/api/control?q=honk")
            assert r.status_code == 200
            sid = r.json()["session"]

            step1 = c.post(
                f"{server.base}/api/log",
                params={"session": sid, "line": "step 1: opening BYD app"},
            )
            assert step1.status_code == 200
            assert step1.json()["ok"] is True

            step2 = c.post(
                f"{server.base}/api/log",
                params={"session": sid, "line": "step 2: pressing honk"},
            )
            assert step2.status_code == 200

            done = c.post(f"{server.base}/api/log", params={"session": sid, "done": "1"})
            assert done.status_code == 200
            assert done.json()["done"] is True

            lines, is_done = _poll_to_done(
                lambda url: c.get(f"{server.base}{url}"), sid
            )

    assert "step 1: opening BYD app" in lines
    assert "step 2: pressing honk" in lines
    assert is_done is True


def test_log_done_marker_line_completes_poll(_real_trigger):
    with _LiveServer() as server:
        with httpx.Client(timeout=httpx.Timeout(30.0, connect=10.0)) as c:
            r = c.get(f"{server.base}/api/control?q=lock")
            sid = r.json()["session"]
            resp = c.post(
                f"{server.base}/api/log",
                params={"session": sid, "line": "__DONE__"},
            )
            assert resp.json()["done"] is True
            lines, is_done = _poll_to_done(
                lambda url: c.get(f"{server.base}{url}"), sid
            )

    assert "__DONE__" in lines
    assert is_done is True


def test_control_macro_failure_502(client, monkeypatch):
    async def fake_trigger(action, sid=None, timeout=8.0):
        return {"ok": False, "status_code": 503, "error": "webhook returned HTTP 503"}

    monkeypatch.setattr(index, "_CONTROL_DRY_RUN", False)
    monkeypatch.setattr(index, "trigger_macrodroid", fake_trigger)

    r = client.get("/api/control?q=lock")
    assert r.status_code == 502
    assert "MacroDroid" in r.json()["detail"]


# ---------------------------------------------------------------------------
# POST /api/log — receiver formats (session pre-created in the store)
# ---------------------------------------------------------------------------

def test_log_query_params(client):
    _create_session("abc123")
    resp = client.post("/api/log?session=abc123&line=query line")
    assert resp.status_code == 200
    assert resp.json()["session"] == "abc123"
    lines, _ = _fetch_lines("abc123")
    assert lines == ["query line"]


def test_log_json_body(client):
    _create_session("abc123")
    resp = client.post("/api/log?session=abc123", json={"line": "json line"})
    assert resp.status_code == 200
    lines, _ = _fetch_lines("abc123")
    assert lines == ["json line"]


def test_log_form_body(client):
    _create_session("abc123")
    resp = client.post("/api/log?session=abc123", data={"line": "form line"})
    assert resp.status_code == 200
    lines, _ = _fetch_lines("abc123")
    assert lines == ["form line"]


def test_log_raw_text_body(client):
    _create_session("abc123")
    resp = client.post(
        "/api/log?session=abc123",
        content="raw body line",
        headers={"content-type": "text/plain"},
    )
    assert resp.status_code == 200
    lines, _ = _fetch_lines("abc123")
    assert lines == ["raw body line"]


def test_log_x_session_header(client):
    _create_session("abc123")
    resp = client.post(
        "/api/log?line=header line",
        headers={"x-session": "abc123"},
    )
    assert resp.status_code == 200
    lines, _ = _fetch_lines("abc123")
    assert lines == ["header line"]


def test_log_done_flag_completes_session(client):
    _create_session("abc123")
    resp = client.post("/api/log?session=abc123&line=bye&done=1")
    assert resp.status_code == 200
    assert resp.json()["done"] is True
    lines, done = _fetch_lines("abc123")
    assert lines == ["bye"]
    assert done is True


def test_log_no_session_routes_to_current(client):
    _create_session("abc123")  # becomes the current session
    resp = client.post("/api/log?line=no session given")
    assert resp.status_code == 200
    assert resp.json()["session"] == "abc123"
    lines, _ = _fetch_lines("abc123")
    assert lines == ["no session given"]


def test_log_404_when_no_active_session(client):
    r = client.post("/api/log?line=orphan")
    assert r.status_code == 404


def test_log_404_when_session_unknown(client):
    r = client.post("/api/log?session=does-not-exist&line=hi")
    assert r.status_code == 404


def test_log_400_when_no_line_or_done(client):
    r = client.post("/api/log")
    assert r.status_code == 400
