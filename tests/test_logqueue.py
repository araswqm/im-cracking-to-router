"""Unit tests for the log-queue backends and protocol helpers.

pytest-asyncio is not installed, so every async code path is driven through
``asyncio.run()``.
"""

from __future__ import annotations

import asyncio

import pytest

from api.logqueue import (
    KEY_PREFIX,
    InMemoryLogStore,
    RedisClientStore,
    RedisLogStore,
    get_log_store,
    is_done_marker,
    is_done_param,
    uses_shared_store,
)
import api.logqueue as logqueue


# ---------------------------------------------------------------------------
# Protocol helpers
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "line",
    ["__DONE__", "done", "DONE", "  __DONE__  "],
)
def test_is_done_marker_true(line):
    assert is_done_marker(line) is True


@pytest.mark.parametrize("line", ["__done", "DONE_", "finished", ""])
def test_is_done_marker_false(line):
    assert is_done_marker(line) is False


@pytest.mark.parametrize("value", ["1", "true", "yes", "on", "TRUE"])
def test_is_done_param_true(value):
    assert is_done_param(value) is True


@pytest.mark.parametrize("value", [None, "", "0", "false", "no"])
def test_is_done_param_false(value):
    assert is_done_param(value) is False


# ---------------------------------------------------------------------------
# InMemoryLogStore
# ---------------------------------------------------------------------------

def test_inmemory_full_flow():
    async def scenario():
        store = InMemoryLogStore()

        await store.create_session("abc")
        assert await store.current() == "abc"

        assert await store.append(None, "first") is True  # routes to current
        assert await store.append("abc", "second") is True

        lines, offset, done = await store.fetch("abc", 0)
        assert lines == ["first", "second"]
        assert offset == 2
        assert done is False

        # Offset-based fetch returns only new lines.
        lines, offset, done = await store.fetch("abc", 2)
        assert lines == []
        assert offset == 2

        await store.append("abc", "third")
        lines, offset, _ = await store.fetch("abc", 2)
        assert lines == ["third"]
        assert offset == 3

    asyncio.run(scenario())


def test_inmemory_done_flag():
    async def scenario():
        store = InMemoryLogStore()
        await store.create_session("abc")

        assert (await store.fetch("abc", 0))[2] is False
        await store.complete("abc")
        assert (await store.fetch("abc", 0))[2] is True

    asyncio.run(scenario())


def test_inmemory_unknown_session():
    async def scenario():
        store = InMemoryLogStore()
        assert await store.append("nope", "x") is False
        with pytest.raises(KeyError):
            await store.fetch("nope", 0)

    asyncio.run(scenario())


def test_inmemory_drop_clears_current():
    async def scenario():
        store = InMemoryLogStore()
        await store.create_session("abc")
        await store.drop("abc")
        assert await store.current() is None
        with pytest.raises(KeyError):
            await store.fetch("abc", 0)

    asyncio.run(scenario())


def test_inmemory_new_session_becomes_current():
    async def scenario():
        store = InMemoryLogStore()
        await store.create_session("one")
        await store.create_session("two")
        assert await store.current() == "two"
        # An explicit sid still appends to its own session.
        await store.append("one", "for one")
        lines, _, _ = await store.fetch("one", 0)
        assert lines == ["for one"]

    asyncio.run(scenario())


# ---------------------------------------------------------------------------
# RedisClientStore — connection-string backend (fake redis.asyncio client)
# ---------------------------------------------------------------------------

class FakeRedis:
    """In-memory stand-in for the ``redis.asyncio`` client RedisClientStore
    talks to.  Implements just the commands the store issues."""

    def __init__(self) -> None:
        self.data: dict[str, object] = {}
        self.ttls: dict[str, int] = {}

    async def lpush(self, key: str, *values: str) -> int:
        bucket = self.data.setdefault(key, [])
        for v in reversed(values):
            bucket.insert(0, v)
        return len(bucket)

    async def llen(self, key: str) -> int:
        return len(self.data.get(key, []))

    async def lrange(self, key: str, start: int, end: int) -> list[str]:
        bucket = self.data.get(key, [])
        return bucket[start : None if end == -1 else end + 1]

    async def set(self, key: str, value: str, ex: int | None = None) -> bool:
        self.data[key] = value
        if ex is not None:
            self.ttls[key] = ex
        return True

    async def expire(self, key: str, ttl: int) -> int:
        if key in self.data:
            self.ttls[key] = ttl
            return 1
        return 0

    async def get(self, key: str) -> str | None:
        return self.data.get(key)

    async def delete(self, *keys: str) -> int:
        removed = 0
        for k in keys:
            if k in self.data:
                del self.data[k]
                removed += 1
        return removed


def _client_store() -> tuple[RedisClientStore, FakeRedis]:
    store = RedisClientStore("rediss://example.upstash.io")
    fake = FakeRedis()
    store._client = fake  # type: ignore[assignment]
    return store, fake


def test_redisclient_full_flow():
    async def scenario():
        store, fake = _client_store()

        await store.create_session("abc")
        assert await store.current() == "abc"
        # create_session: seed list + current pointer with EX TTL
        assert fake.ttls[store._key("abc")] == logqueue.KEY_TTL
        assert fake.ttls[store._key("", "current")] == logqueue.KEY_TTL

        assert await store.append(None, "first") is True  # routes to current
        assert await store.append("abc", "second") is True

        lines, offset, done = await store.fetch("abc", 0)
        assert lines == ["first", "second"]
        assert offset == 2
        assert done is False

        # Offset-based fetch returns only new lines.
        lines, offset, done = await store.fetch("abc", 2)
        assert lines == []
        assert offset == 2

        await store.append("abc", "third")
        lines, offset, _ = await store.fetch("abc", 2)
        assert lines == ["third"]
        assert offset == 3
        # append refreshes the session TTL
        assert fake.ttls[store._key("abc")] == logqueue.KEY_TTL

    asyncio.run(scenario())


def test_redisclient_done_flag_complete_and_drop():
    async def scenario():
        store, _ = _client_store()
        await store.create_session("abc")

        assert (await store.fetch("abc", 0))[2] is False
        await store.complete("abc")
        assert (await store.fetch("abc", 0))[2] is True

        await store.drop("abc")
        with pytest.raises(KeyError):
            await store.fetch("abc", 0)

    asyncio.run(scenario())


def test_redisclient_unknown_session():
    async def scenario():
        store, _ = _client_store()
        # No session yet → append has no current target and unknown sids fail.
        assert await store.append(None, "x") is False
        assert await store.append("nope", "x") is False
        with pytest.raises(KeyError):
            await store.fetch("nope", 0)

    asyncio.run(scenario())


def test_redisclient_client_is_lazy():
    """Constructing the store must not touch the network — only first use
    creates the client (substituted with a fake here)."""
    store = RedisClientStore("rediss://example.upstash.io")
    assert store._client is None  # no connection yet


# ---------------------------------------------------------------------------
# RedisLogStore — command building (no real Redis)
# ---------------------------------------------------------------------------

class FakeRedisClient:
    """Stands in for RedisLogStore._req: records commands, returns canned
    responses per path."""

    def __init__(self, paths_to_results: dict | None = None):
        self.calls: list[tuple[str, object | None]] = []
        self.paths_to_results = paths_to_results or {}

    async def _req(self, path: str, body: object | None = None) -> object:
        self.calls.append((path, body))
        return self.paths_to_results.get(path, [] if path.startswith("lrange") else None)


def test_redis_create_session():
    fake = FakeRedisClient()
    store = RedisLogStore("https://example.upstash.io", "tok")
    store._req = fake._req

    asyncio.run(store.create_session("abc"))

    paths = [p for p, _ in fake.calls]
    assert f"lpush/{KEY_PREFIX}:abc" in paths
    assert f"set/{KEY_PREFIX}:current" in paths
    assert f"expire/{KEY_PREFIX}:abc" in paths
    # create_session body: [value, EX, ttl]
    set_call = next((p, b) for p, b in fake.calls if p.startswith("set/"))
    assert set_call[1] == ["abc", "EX", 180]


def test_redis_append_unknown_session_returns_false():
    fake = FakeRedisClient(paths_to_results={f"llen/{KEY_PREFIX}:abc": 0})
    store = RedisLogStore("https://example.upstash.io", "tok")
    store._req = fake._req

    assert asyncio.run(store.append("abc", "hi")) is False


def test_redis_append_known_session():
    llen_path = f"llen/{KEY_PREFIX}:abc"
    fake = FakeRedisClient(
        paths_to_results={llen_path: 2, f"expire/{KEY_PREFIX}:abc": 1}
    )
    store = RedisLogStore("https://example.upstash.io", "tok")
    store._req = fake._req

    assert asyncio.run(store.append("abc", "hi")) is True

    push = next(p for p, b in fake.calls if p.startswith("lpush/"))
    assert push == f"lpush/{KEY_PREFIX}:abc"
    assert push_calls_body(push, fake.calls) == ["hi"]


def test_redis_fetch_reverses_lpush_order():
    key = f"{KEY_PREFIX}:abc"
    fake = FakeRedisClient(
        paths_to_results={
            f"llen/{key}": 4,
            # lpush order = newest-first: "seed" was pushed first, then
            # "first", "second", "third".  Stored list (head=newest):
            # ["third", "second", "first", ""]
            f"lrange/{key}/0/-1": ["third", "second", "first", ""],
            f"get/{key}:done": None,
        }
    )
    store = RedisLogStore("https://example.upstash.io", "tok")
    store._req = fake._req

    lines, offset, done = asyncio.run(store.fetch("abc", 0))

    assert lines == ["first", "second", "third"]
    assert offset == 3
    assert done is False


def test_redis_fetch_offset_skips_consumed_lines():
    key = f"{KEY_PREFIX}:abc"
    fake = FakeRedisClient(
        paths_to_results={
            f"llen/{key}": 4,
            f"lrange/{key}/0/-1": ["third", "second", "first", ""],
            f"get/{key}:done": "1",
        }
    )
    store = RedisLogStore("https://example.upstash.io", "tok")
    store._req = fake._req

    lines, offset, done = asyncio.run(store.fetch("abc", 2))

    assert lines == ["third"]
    assert offset == 3
    assert done is True  # done flag "1" present


def test_redis_fetch_unknown_session_raises():
    key = f"{KEY_PREFIX}:nope"
    fake = FakeRedisClient(paths_to_results={f"llen/{key}": 0})
    store = RedisLogStore("https://example.upstash.io", "tok")
    store._req = fake._req

    with pytest.raises(KeyError):
        asyncio.run(store.fetch("nope", 0))


def test_redis_decode_percent_encoding():
    store = RedisLogStore("https://example.upstash.io", "tok")
    assert store._decode("Checking%20locks%E2%80%A6") == "Checking locks…"
    assert store._decode("plain text") == "plain text"
    assert store._decode(None) == ""


# ---------------------------------------------------------------------------
# Backend selection — get_log_store() / uses_shared_store()
# ---------------------------------------------------------------------------

_NO_REDIS_VARS = (
    "UPSTASH_REDIS_REST_URL",
    "UPSTASH_REDIS_REST_TOKEN",
    "KV_REST_API_URL",
    "KV_REST_API_TOKEN",
    "REDIS_URL",
    "REDIS_TOKEN",
)


@pytest.mark.parametrize(
    "url_var,token_var",
    [
        ("UPSTASH_REDIS_REST_URL", "UPSTASH_REDIS_REST_TOKEN"),
        ("KV_REST_API_URL", "KV_REST_API_TOKEN"),
        ("REDIS_URL", "REDIS_TOKEN"),
    ],
)
def test_get_log_store_picks_redis_for_any_env_pair(monkeypatch, url_var, token_var):
    for var in _NO_REDIS_VARS:
        monkeypatch.delenv(var, raising=False)
    monkeypatch.setenv(url_var, "https://example.upstash.io")
    monkeypatch.setenv(token_var, "tok")
    logqueue._store = None

    assert isinstance(get_log_store(), RedisLogStore)
    assert uses_shared_store() is True


def test_get_log_store_falls_back_to_memory_when_no_redis_env(monkeypatch):
    for var in _NO_REDIS_VARS:
        monkeypatch.delenv(var, raising=False)
    logqueue._store = None

    assert isinstance(get_log_store(), InMemoryLogStore)
    assert uses_shared_store() is False


@pytest.mark.parametrize("url", ["redis://localhost:6379", "rediss://example.upstash.io"])
def test_get_log_store_connection_string_uses_client_store(monkeypatch, url):
    """Vercel Redis sets REDIS_URL alone (no token) — that must select the
    redis-py connection-string store."""
    for var in _NO_REDIS_VARS:
        monkeypatch.delenv(var, raising=False)
    monkeypatch.setenv("REDIS_URL", url)
    logqueue._store = None

    assert isinstance(get_log_store(), RedisClientStore)
    assert uses_shared_store() is True


def test_get_log_store_ignores_unresolved_placeholder_url(monkeypatch):
    """Vercel stores env references as [REDIS_URL] placeholders until runtime;
    locally (and if the reference ever fails to resolve) that must NOT be
    treated as a usable connection string."""
    for var in _NO_REDIS_VARS:
        monkeypatch.delenv(var, raising=False)
    monkeypatch.setenv("REDIS_URL", "[REDIS_URL]")
    logqueue._store = None

    assert isinstance(get_log_store(), InMemoryLogStore)
    assert uses_shared_store() is False


def test_get_log_store_connection_string_wins_over_rest_pair(monkeypatch):
    for var in _NO_REDIS_VARS:
        monkeypatch.delenv(var, raising=False)
    monkeypatch.setenv("REDIS_URL", "redis://localhost:6379")
    monkeypatch.setenv("UPSTASH_REDIS_REST_URL", "https://x.upstash.io")
    monkeypatch.setenv("UPSTASH_REDIS_REST_TOKEN", "tok")
    logqueue._store = None

    assert isinstance(get_log_store(), RedisClientStore)


def test_get_log_store_redis_requires_both_url_and_token(monkeypatch):
    for var in _NO_REDIS_VARS:
        monkeypatch.delenv(var, raising=False)
    monkeypatch.setenv("KV_REST_API_URL", "https://example.upstash.io")
    logqueue._store = None

    assert isinstance(get_log_store(), InMemoryLogStore)


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def push_calls_body(path: str, calls: list[tuple[str, object | None]]) -> object:
    for p, body in calls:
        if p == path:
            return body
    raise AssertionError(f"no call recorded for {path!r}")
