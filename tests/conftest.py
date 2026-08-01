"""
Shared test fixtures.

Loads the pybyd shim into ``sys.modules`` before importing the app, sets the
env vars the app reads at import time (BYD credentials + dry-run mode so no
test ever hits the real MacroDroid webhook), and exposes an isolated
FastAPI TestClient.  Every test gets a fresh log store.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
TESTS_DIR = Path(__file__).resolve().parent

# Make both the repo root (for `api.*`) and this dir (for `shim_pybyd`)
# importable regardless of how pytest is invoked.
for path in (ROOT, TESTS_DIR):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

# Env the app reads at import time.
os.environ.setdefault("BYD_USERNAME", "test@example.com")
os.environ.setdefault("BYD_PASSWORD", "test-secret")
os.environ["CONTROL_DRY_RUN"] = "1"  # never hit the real webhook in tests

# pybyd stand-in — must be in place before `api.index` is imported.
import shim_pybyd  # noqa: E402

sys.modules.setdefault("pybyd", shim_pybyd)
sys.modules.setdefault("pybyd.exceptions", shim_pybyd)

import pytest  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402

from api.index import app  # noqa: E402
import api.logqueue as logqueue  # noqa: E402


@pytest.fixture(autouse=True)
def _fresh_log_store():
    """Reset the module-level log store before and after each test so no
    session or "current" pointer leaks between tests."""
    logqueue._store = None
    yield
    logqueue._store = None


@pytest.fixture()
def client():
    with TestClient(app) as c:
        yield c
