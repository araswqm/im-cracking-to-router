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
    RedisLogStore,
    is_done_marker,
    is_done_param,
)


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
# helpers
# ---------------------------------------------------------------------------

def push_calls_body(path: str, calls: list[tuple[str, object | None]]) -> object:
    for p, body in calls:
        if p == path:
            return body
    raise AssertionError(f"no call recorded for {path!r}")
