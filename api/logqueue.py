"""
Log queue shared between the phone (MacroDroid log POSTs) and the streaming
client (``/api/control``).

Vercel serverless functions are stateless — each request may land on a
different instance, so an in-process variable alone is not reliable for
cross-request state.  Three backends are provided behind one async interface:

* :class:`InMemoryLogStore` — zero dependencies, works for local dev and the
  common "one warm instance" case.
* :class:`RedisClientStore` — production-safe, backed by any connection-string
  Redis via the official ``redis`` client: Vercel's "Redis" integration
  (``REDIS_URL`` only, no token), Upstash, Redis Cloud, or a local server.
  Enabled automatically when ``REDIS_URL`` starts with ``redis://``/``rediss://``.
* :class:`RedisLogStore` — Upstash Redis over its REST API (uses ``httpx``).
  Enabled automatically when a configured REST pair is set — any of
  ``UPSTASH_REDIS_REST_URL``/``_TOKEN``, Vercel KV's
  ``KV_REST_API_URL``/``KV_REST_API_TOKEN``, or generic ``REDIS_URL``/``REDIS_TOKEN``
  (see ``_REDIS_ENV_PAIRS``).  Kept for accounts that expose a REST endpoint
  instead of a connection string.

A ``REDIS_URL`` that does *not* look like a connection string (e.g. Vercel's
unresolved ``[REDIS_URL]`` reference placeholder) is ignored, so local/dev runs
fall back to memory instead of crashing.

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
import redis.asyncio as aioredis

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
# Redis backend (connection string — Vercel Redis / Upstash / Redis Cloud)
# ---------------------------------------------------------------------------

class RedisClientStore:
    """Session log backed by any Redis reachable via a ``redis://``/``rediss://``
    connection string — Vercel's Redis integration sets only ``REDIS_URL`` with
    no token, which is exactly what this store consumes.

    Uses the official ``redis.asyncio`` client (a connection pool shared across
    requests on the warm instance).  Commands mirror :class:`RedisLogStore`:
    ``lpush``/``lrange`` for the line list, ``set`` with EX for the done flag and
    current-pointer, ``expire``/``del`` for cleanup.  The client is created
    lazily on first use so a store can be instantiated at import time without
    touching the network.
    """

    def __init__(self, url: str) -> None:
        self._url = url
        self._client: aioredis.Redis | None = None

    # -- low-level --------------------------------------------------------

    async def _r(self) -> aioredis.Redis:
        """Return the shared client, creating it on first use.

        ``redis.asyncio.from_url`` is lazy — no connection is opened until the
        first command — and its connection pool is safe for concurrent
        requests on the same event loop, so one client per warm instance is
        enough.
        """
        if self._client is None:
            self._client = aioredis.from_url(
                self._url,
                decode_responses=True,  # keep values as str, not bytes
                socket_connect_timeout=5,
            )
        return self._client

    # -- helpers ----------------------------------------------------------

    @staticmethod
    def _key(sid: str, suffix: str = "") -> str:
        return f"{KEY_PREFIX}:{sid}{suffix}"

    async def _touch(self, key: str) -> None:
        """Refresh a session key's TTL so an active stream never dies."""
        r = await self._r()
        await r.expire(key, KEY_TTL)

    # -- interface --------------------------------------------------------

    async def create_session(self, sid: str) -> None:
        # Seed the list with an empty element so append()/fetch() can tell the
        # session exists (an expired/never-seen session has llen == 0).
        r = await self._r()
        key = self._key(sid)
        await r.lpush(key, "")
        await r.set(self._key("", "current"), sid, ex=KEY_TTL)
        await self._touch(key)

    async def append(self, sid: str | None, line: str) -> bool:
        r = await self._r()
        if sid is None:
            sid = await self.current()
        if sid is None:
            return False
        key = self._key(sid)
        n = await r.llen(key)
        if n == 0:
            return False  # unknown/expired session → 404 upstream
        await r.lpush(key, line)
        await self._touch(key)
        return True

    async def fetch(self, sid: str, offset: int) -> tuple[list[str], int, bool]:
        r = await self._r()
        key = self._key(sid)
        n = await r.llen(key)
        if n == 0:
            raise KeyError(f"unknown session: {sid}")
        # lrange returns newest-first (we push with lpush), so reverse into
        # oldest-first and drop the empty seed element.  Sessions are short
        # (≤ a few dozen lines) — re-reading the list each poll is fine.
        raw = await r.lrange(key, 0, -1)
        lines = [ln for v in reversed(raw) if (ln := v)]
        new_lines = lines[offset:]
        done_raw = await r.get(self._key(sid, ":done"))
        return new_lines, offset + len(new_lines), done_raw is not None

    async def complete(self, sid: str) -> None:
        r = await self._r()
        await r.set(self._key(sid, ":done"), "1", ex=KEY_TTL)

    async def current(self) -> str | None:
        r = await self._r()
        value = await r.get(self._key("", "current"))
        return value or None

    async def drop(self, sid: str) -> None:
        r = await self._r()
        await r.delete(self._key(sid), self._key(sid, ":done"))


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
# Backend selection
# ---------------------------------------------------------------------------

# Env-var pairs accepted for the *REST* Redis-backed queue, in priority order.
# Vercel KV exposes KV_REST_API_URL/KV_REST_API_TOKEN, Upstash Redis uses
# UPSTASH_REDIS_REST_URL/UPSTASH_REDIS_REST_TOKEN, and REDIS_URL/REDIS_TOKEN
# covers generic REST Redis hosts.  A bare REDIS_URL that is a connection
# string (redis:// or rediss://) is handled by the connection-string backend
# instead (Vercel Redis sets REDIS_URL with no token).
_REDIS_ENV_PAIRS: tuple[tuple[str, str], ...] = (
    ("UPSTASH_REDIS_REST_URL", "UPSTASH_REDIS_REST_TOKEN"),
    ("KV_REST_API_URL", "KV_REST_API_TOKEN"),  # Vercel KV
    ("REDIS_URL", "REDIS_TOKEN"),  # generic REST Redis
)


def _connection_string_url() -> str:
    """Return the Redis connection string from ``REDIS_URL``, or ``""``.

    Only ``redis://``/``rediss://`` URLs are accepted.  Anything else — a REST
    https URL (handled by ``_redis_env``), an empty value, or Vercel's
    unresolved ``[REDIS_URL]`` reference placeholder — yields ``""`` so the
    store falls back to memory or a REST backend instead of misbehaving.
    """
    url = os.environ.get("REDIS_URL", "").strip()
    if url.startswith(("redis://", "rediss://")):
        return url
    return ""


def _redis_env() -> tuple[str, str]:
    """Return the (url, token) of the first configured REST pair."""
    for url_var, token_var in _REDIS_ENV_PAIRS:
        url = os.environ.get(url_var, "").strip()
        token = os.environ.get(token_var, "").strip()
        if url and token:
            return url, token
    return "", ""


def uses_shared_store() -> bool:
    """True when a Redis-backed store is configured (vs. in-memory).

    Lets callers explain *why* a session vanished: with the in-memory store
    a session lives only on the one Vercel instance that created it.
    """
    return bool(_connection_string_url() or _redis_env()[0])


_store: BaseLogStore | None = None


def get_log_store() -> BaseLogStore:
    """Return the process-wide log store.

    Selection order:

    1. ``REDIS_URL`` connection string (``redis://``/``rediss://``) →
       :class:`RedisClientStore` (Vercel Redis).
    2. A configured REST pair (see ``_REDIS_ENV_PAIRS``) → :class:`RedisLogStore`.
    3. Otherwise → :class:`InMemoryLogStore`.

    ``tests`` can reset it by assigning ``logqueue._store = None``.
    """
    global _store
    if _store is None:
        conn = _connection_string_url()
        if conn:
            _store = RedisClientStore(conn)
        else:
            url, token = _redis_env()
            if url and token:
                _store = RedisLogStore(url, token)
            else:
                _store = InMemoryLogStore()
    return _store
