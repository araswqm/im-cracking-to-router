"""
MacroDroid vehicle-control bridge.

Turns a ``q`` control parameter into a MacroDroid webhook trigger, and
(simulated or real) performs the action.  The phone runs the MacroDroid
macro, which POSTs live status lines back to ``/api/log`` on this API —
those lines are streamed to the client by ``/api/control``.

Environment Variables:
    MACRODROID_BASE_URL  (optional) — webhook base, default points at the
                         project's own MacroDroid trigger.
    CONTROL_DRY_RUN      (optional) — "1"/"true" → never call the real
                         webhook; push simulated log lines instead.  Great
                         for testing the streaming pipeline without a phone.
"""

from __future__ import annotations

import asyncio
import os
from typing import TYPE_CHECKING

import httpx

if TYPE_CHECKING:  # pragma: no cover
    from logqueue import BaseLogStore

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

# Base URL of the MacroDroid "HTTP Trigger" for this project.  Each action is
# fired by appending ``?control=<action>`` (+ optional ``&sid=<session>``).
DEFAULT_MACRODROID_BASE_URL = (
    "https://trigger.macrodroid.com/0de8afc4-6b8a-441f-b404-12214c310aaa/wh"
)
MACRODROID_BASE_URL = os.environ.get(
    "MACRODROID_BASE_URL", DEFAULT_MACRODROID_BASE_URL
).strip()

# Supported vehicle actions → value sent as the ``control`` query parameter.
# The keys are the ``?q=`` values accepted by /api/control.
CONTROL_MAP: dict[str, str] = {
    "unlock": "unlock",
    "lock": "lock",
    "headlights": "headlights",
    "honk": "honk",
    "trunk_open": "trunk_open",
    "trunk_close": "trunk_close",
    "windows_close": "windows_close",
    "engine_off": "engine_off",
}

# How long the phone (MacroDroid macro) is expected to keep running.  The
# control stream gives up after this many seconds if the phone never sends
# a completion marker.
_DRY_RUN_STEPS: list[str] = [
    "sim: received control command",
    "sim: authenticating with BYD app",
    "sim: action executed successfully",
    "sim: done",
]


# ---------------------------------------------------------------------------
# Action resolution
# ---------------------------------------------------------------------------

def resolve_action(q: str | None) -> str | None:
    """Normalize a ``q`` value into a known control action.

    Returns ``None`` for missing/blank input or unknown actions, so the
    endpoint can return HTTP 400.  Matching is case-insensitive.
    """
    if not q:
        return None
    return CONTROL_MAP.get(q.strip().lower())


def trigger_url(action: str, sid: str | None = None) -> str:
    """Build the full MacroDroid webhook URL for ``action``."""
    base = MACRODROID_BASE_URL.rstrip("/")
    url = f"{base}?control={action}"
    if sid:
        # Optional — lets the phone correlate its own session id so its log
        # POSTs land on the right stream even when the stream isn't the
        # "current" one.
        url += f"&sid={sid}"
    return url


# ---------------------------------------------------------------------------
# Webhook call
# ---------------------------------------------------------------------------

async def trigger_macrodroid(
    action: str, sid: str | None = None, timeout: float = 8.0
) -> dict:
    """Fire the MacroDroid webhook for ``action``.

    Returns ``{"ok": True, "status_code": int}`` on success or
    ``{"ok": False, "status_code": int|None, "error": str}`` on failure.
    A 2xx is considered success — MacroDroid triggers answer quickly and
    don't carry a meaningful body.
    """
    url = trigger_url(action, sid)
    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            resp = await client.get(url)
    except httpx.HTTPError as exc:
        return {"ok": False, "status_code": None, "error": str(exc)}
    ok = 200 <= resp.status_code < 300
    return {
        "ok": ok,
        "status_code": resp.status_code,
        "error": None if ok else f"webhook returned HTTP {resp.status_code}",
    }


# ---------------------------------------------------------------------------
# Dry-run simulator (CONTROL_DRY_RUN=1)
# ---------------------------------------------------------------------------

async def simulate_phone(
    store: "BaseLogStore", sid: str, action: str, delay: float = 0.4
) -> None:
    """Pretend to be the phone: push a few log lines then mark done.

    Used when ``CONTROL_DRY_RUN`` is enabled so the whole stream pipeline
    can be exercised end-to-end without a real MacroDroid trigger.
    """
    for line in _DRY_RUN_STEPS:
        await asyncio.sleep(delay)
        await store.append(sid, f"[{action}] {line}")
    await store.complete(sid)
