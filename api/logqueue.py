"""
Log queue shared between the phone (MacroDroid log POSTs) and the streaming
client (``/api/control``).

Vercel serverless functions are stateless — each request may land on a
different instance, so an in-process variable alone is not reliable for
cross-request state.  Two backends are provided behind one async interface:

* :class:`InMemoryLogStore` — zero dependencies, works for local dev and the
  common "one warm instance" case.
* :class:`RedisLogStore` — production-safe, backed by Upstash Redis REST
  (via the existing ``httpx`` dependency, no extra packages).  Enabled
  automatically when ``UPSTASH_REDIS_REST_URL`` is set.

Session ids are short (12 hex chars) and every key expires after
``KEY_TTL`` seconds, so abandoned streams clean themselves up.
"""

from __future__ import annotations

import os
import re
import urllib.parse
from collections import deque
from typing import Protocol

import httpx

# ---------------------------------------------------------------------------
# Constants / protocol
# ---------------------------------------------------------------------------

KEY_PREFIX = "byd:control"
DONE_MARKER = "__DONE__"
KEY_TTL = 180  # seconds — max age of a session and its log lines

_DONE_TRUE = frozenset({"__DONE__", "DONE"})
_DONE_TRUE_PARAM = frozenset({"1", "true", "yes", "on"})
_PER_CENT_RE = re.compile(r"%(?:[0-9A-Fa-f]{2})")


def is_done_marker(line: str) -> bool:
    """True when ``line`` is the completion sentinel."""
    return line.strip().upper() in _DONE_TRUE


def is_done_param(value: str | None) -> bool:
    """True when a ``done`` query/form value requests completion."""
    return bool(value and value.strip().lower() in _DONE_TRUE_PARAM)


# ---------------------------------------------------------------------------
# Interface
# ---------------------------------------------------------------------------

class BaseLogStore(Protocol):
    """Async interface both backends implement."""

    async def create_session(self, sid: str) -> None: ...
    async def append(self, sid: str | None, line: str) -> bool: ...
    async def fetch(self, sid: str, offset: int) -> tuple[list[str], int, bool]: ...
    async def complete(self, sid: str) -> None: ...
    async def current(self) -> str | None: ...
    async def drop(self, sid: str) -> None: ...


# ---------------------------------------------------------------------------
# In-memory backend (local dev / single warm instance)
# ---------------------------------------------------------------------------

class InMemoryLogStore:
    """Process-local queue.  Shared only across requests that hit the same
    serverless instance."""

    def __init__(self) -> None:
        self._lines: dict[str, deque[str]] = {}
        self._done: set[str] = set()
        self._current: str | None = None

    async def create_session(self, sid: str) -> None:
        self._lines.setdefault(sid, deque())
        self._current = sid

    async def append(self, sid: str | None, line: str) -> bool:
        if sid is None:
            sid = self._current
        if sid is None or sid not in self._lines:
            return False  # unknown session → 404 upstream
        self._lines[sid].append(line)
        return True

    async def fetch(self, sid: str, offset: int) -> tuple[list[str], int, bool]:
        if sid not in self._lines:
            raise KeyError(f"unknown session: {sid}")
        lines = self._lines[sid]
        new_lines = list(lines)[offset:]
        return new_lines, offset + len(new_lines), sid in self._done

    async def complete(self, sid: str) -> None:
        if sid in self._lines:
            self._done.add(sid)

    async def current(self) -> str | None:
        return self._current

    async def drop(self, sid: str) -> None:
        self._lines.pop(sid, None)
        self._done.discard(sid)
        if self._current == sid:
            self._current = None


# ---------------------------------------------------------------------------
# Redis backend (Upstash REST — production-safe)
# ---------------------------------------------------------------------------

class RedisLogStore:
    """Session log backed by Upstash Redis over its REST API.

    Commands used: ``lpush``/``lrange`` for the line list, ``set`` with EX
    for the done flag and current-pointer, ``del``/``expire`` for cleanup.
    Redis list TTLs are refreshed on every append so an active stream never
    dies mid-flight.
    """

    def __init__(self, url: str, token: str) -> None:
        self._url = url.rstrip("/")
        self._token = token
        self._client = httpx.AsyncClient(timeout=5.0)

    # -- low-level --------------------------------------------------------

    async def _req(self, path: str, body: object | None = None) -> object:
        """Send a command against the Upstash REST API."""
        url = f"{self._url}/{path}"
        headers = {"Authorization": f"Bearer {self._token}"}
        resp = await self._client.post(url, headers=headers, json=body)
        resp.raise_for_status()
        return resp.json()

    @staticmethod
    def _decode(value: object) -> str:
        """Upstash returns strings percent-encoded; decode defensively."""
        if not isinstance(value, str):
            return "" if value is None else str(value)
        if _PER_CENT_RE.search(value):
            try:
                return urllib.parse.unquote(value)
            except Exception:
                return value
        return value

    # -- helpers ----------------------------------------------------------

    def _key(self, sid: str, suffix: str = "") -> str:
        return f"{KEY_PREFIX}:{sid}{suffix}"

    async def _touch(self, key: str) -> None:
        await self._req("expire/" + key, body=[KEY_TTL])

    # -- interface --------------------------------------------------------

    async def create_session(self, sid: str) -> None:
        # Seed the list with an empty element so append()/fetch() can tell the
        # session exists (an expired/never-seen session has llen == 0).
        key = self._key(sid)
        await self._req("lpush/" + key, body=[""])
        await self._req("set/" + self._key("", "current"), body=[sid, "EX", KEY_TTL])
        await self._touch(key)

    async def append(self, sid: str | None, line: str) -> bool:
        if sid is None:
            sid = await self.current()
        if sid is None:
            return False
        key = self._key(sid)
        n = await self._req("llen/" + key)
        if n is None or int(n) == 0:
            return False  # unknown/expired session → 404 upstream
        await self._req("lpush/" + key, body=[line])
        await self._touch(key)
        return True

    async def fetch(self, sid: str, offset: int) -> tuple[list[str], int, bool]:
        key = self._key(sid)
        n = await self._req("llen/" + key)
        if n is None or int(n) == 0:
            raise KeyError(f"unknown session: {sid}")
        # lrange returns newest-first (we push with lpush), so reverse into
        # oldest-first and drop the empty seed element.  Sessions are short
        # (≤ a few dozen lines) — re-reading the list each poll is fine.
        raw = await self._req(f"lrange/{key}/0/-1")
        lines = [ln for v in reversed(raw) if (ln := self._decode(v))]
        new_lines = lines[offset:]
        done = await self._req("get/" + self._key(sid, ":done"))
        return new_lines, offset + len(new_lines), done is not None and done != "null"

    async def complete(self, sid: str) -> None:
        done_key = self._key(sid, ":done")
        await self._req("set/" + done_key, body=["1", "EX", KEY_TTL])

    async def current(self) -> str | None:
        raw = await self._req("get/" + self._key("", "current"))
        value = self._decode(raw)
        return value if value and value != "null" else None

    async def drop(self, sid: str) -> None:
        await self._req("del/" + self._key(sid) + "/" + self._key(sid, ":done"))


# ---------------------------------------------------------------------------
# Singleton
# ---------------------------------------------------------------------------

_store: BaseLogStore | None = None


def get_log_store() -> BaseLogStore:
    """Return the process-wide log store.

    Uses Redis when ``UPSTASH_REDIS_REST_URL`` is set, otherwise falls back
    to the in-memory store.  ``tests`` can reset it by assigning
    ``logqueue._store = None``.
    """
    global _store
    if _store is None:
        url = os.environ.get("UPSTASH_REDIS_REST_URL", "").strip()
        token = os.environ.get("UPSTASH_REDIS_REST_TOKEN", "").strip()
        if url and token:
            _store = RedisLogStore(url, token)
        else:
            _store = InMemoryLogStore()
    return _store
