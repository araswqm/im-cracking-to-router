"""
BYD Vehicle Data API — Vercel Serverless Function.

Endpoints:

    GET  /monitor          — clean, deduplicated, human-readable vehicle data
    GET  /monitor/raw      — raw pyBYD vehicle data, untransformed
    GET  /api/control?q=…  — fire a vehicle action via MacroDroid; returns a
                              session id to poll (no long-lived stream)
    GET  /api/control/poll — read new log lines for a running action; the
                              client polls this every ~500ms
    POST /api/log          — log receiver the phone (MacroDroid) POSTs to
    GET  /api/health       — lightweight health check (no BYD auth)

Environment Variables:
    BYD_USERNAME (required)          — BYD account email or phone
    BYD_PASSWORD (required)          — BYD account password
    BYD_BASE_URL  (optional)         — API base URL (default: EU endpoint)
    BYD_COUNTRY_CODE (optional)      — Two-letter country code (default: NL)
    MACRODROID_BASE_URL (optional)   — webhook base for /api/control
    CONTROL_API_KEY (optional)       — if set, /api/control + /api/log require
                                       ``?key=`` or ``Authorization: Bearer``
    CONTROL_STREAM_TIMEOUT (optional)— max seconds the control stream stays
                                       open waiting for the phone (default 45)
    CONTROL_DRY_RUN (optional)       — "1"/"true": simulate the phone instead
                                       of calling the real MacroDroid webhook
    UPSTASH_REDIS_REST_URL (optional)— enables the shared Redis log queue
    UPSTASH_REDIS_REST_TOKEN (optional)
"""

from __future__ import annotations

import asyncio
import os
import urllib.parse
import uuid
from datetime import datetime, timezone

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse

from pybyd import BydClient, BydConfig
from pybyd.exceptions import (
    BydApiError,
    BydAuthenticationError,
    BydDataUnavailableError,
    BydTransportError,
)

try:
    from transform import transform_vehicle  # Vercel prod (api/ on sys.path)
except ImportError:  # local/dev runs
    from api.transform import transform_vehicle

try:
    from control import CONTROL_MAP, resolve_action, simulate_phone, trigger_macrodroid
    from logqueue import get_log_store, is_done_marker, is_done_param
except ImportError:  # local/dev runs
    from api.control import CONTROL_MAP, resolve_action, simulate_phone, trigger_macrodroid
    from api.logqueue import get_log_store, is_done_marker, is_done_param

# ---------------------------------------------------------------------------
# FastAPI application
# ---------------------------------------------------------------------------

app = FastAPI(
    title="BYD Vehicle Data API",
    description=(
        "Fetch all vehicle data from your BYD account via pyBYD and control "
        "the vehicle through MacroDroid webhooks with live streaming logs."
    ),
    version="2.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Dry-run mode simulates the phone (no real MacroDroid webhook call).
_CONTROL_DRY_RUN = os.environ.get("CONTROL_DRY_RUN", "").strip().lower() in (
    "1",
    "true",
    "yes",
)
_CONTROL_API_KEY = os.environ.get("CONTROL_API_KEY", "").strip()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _build_config() -> BydConfig:
    """Read BYD credentials from environment and return a BydConfig.

    Raises HTTPException(500) if required variables are missing.
    """
    username = os.environ.get("BYD_USERNAME", "").strip()
    password = os.environ.get("BYD_PASSWORD", "").strip()

    if not username or not password:
        raise HTTPException(
            status_code=500,
            detail=(
                "Missing required environment variables. "
                "Set BYD_USERNAME and BYD_PASSWORD in your Vercel project settings."
            ),
        )

    return BydConfig(
        username=username,
        password=password,
        base_url=os.environ.get(
            "BYD_BASE_URL", "https://dilinkappoversea-eu.byd.auto"
        ),
        country_code=os.environ.get("BYD_COUNTRY_CODE", "NL"),
    )


def _serialize(value: object) -> object:
    """Convert a pyBYD model or plain value into a JSON-safe dict/value."""
    if value is None:
        return None
    if hasattr(value, "model_dump"):
        return value.model_dump(by_alias=True, mode="json")
    return value


def _serialize_gps(value: object) -> object | None:
    """Serialize GPS data, returning ``None`` when coordinates are invalid.

    Lat 0 / Lon 0 (Null Island) or None values indicate the vehicle has no
    GPS fix (garage, underground parking, etc.) or the last known position
    is unavailable.  Returning ``None`` lets consumers distinguish between
    "no data" and an actual position at (0, 0).
    """
    raw = _serialize(value)
    if not isinstance(raw, dict):
        return None
    lat = raw.get("latitude")
    lon = raw.get("longitude")
    # Both zero (Null Island) or both None → no valid fix
    if (lat is None or lat == 0) and (lon is None or lon == 0):
        return None
    return raw


def _authorized(request: Request) -> bool:
    """True when the request may use the control endpoints.

    With no ``CONTROL_API_KEY`` set, everyone is allowed.  Otherwise the
    caller must send ``?key=`` or ``Authorization: Bearer <key>``.
    """
    if not _CONTROL_API_KEY:
        return True
    if request.query_params.get("key") == _CONTROL_API_KEY:
        return True
    auth = request.headers.get("authorization", "")
    if auth.lower().startswith("bearer ") and auth[7:].strip() == _CONTROL_API_KEY:
        return True
    return False


def _first_form(values: object) -> str | None:
    """First value from ``urllib.parse.parse_qs`` (a list), or ``None``."""
    if isinstance(values, list) and values:
        return values[0]
    return None


# ---------------------------------------------------------------------------
# Browser view for /api/control
# ---------------------------------------------------------------------------

_CONTROL_PAGE = """<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>BYD Control — Live Log</title>
<style>
  :root { color-scheme: dark; }
  * { box-sizing: border-box; }
  body { margin: 0; font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
         background: #0d1117; color: #e6edf3; height: 100vh; display: flex; flex-direction: column; }
  header { padding: 10px 16px; border-bottom: 1px solid #30363d; display: flex; gap: 16px;
           flex-wrap: wrap; align-items: baseline; }
  #title { font-weight: 600; }
  #title .act { color: #58a6ff; }
  #sub { color: #8b949e; font-size: 13px; }
  #status { margin-left: auto; font-size: 13px; color: #8b949e; }
  #log { flex: 1; overflow-y: auto; padding: 12px 16px; white-space: pre-wrap;
         word-break: break-word; font-size: 13px; line-height: 1.6; }
  #log .ts { color: #58a6ff; }
  #log .err { color: #f85149; }
  #log .done { color: #3fb950; }
  .note { color: #d29922; }
  a { color: #58a6ff; }
</style>
</head>
<body>
<header>
  <div id="title">Action: <span class="act" id="action"></span></div>
  <div id="sub">session: <span id="session"></span></div>
  <div id="status">connecting…</div>
</header>
<pre id="log"></pre>
<script>
  const SESSION = "__SESSION__";
  const ACTION = "__ACTION__";
  const KEY_QS = "__KEY_QS__";
  const DRY = __DRY__;

  const logEl = document.getElementById('log');
  const statusEl = document.getElementById('status');
  document.getElementById('action').textContent = ACTION;
  document.getElementById('session').textContent = SESSION;

  const stamp = () => new Date().toLocaleTimeString();
  function append(text, cls) {
    const div = document.createElement('div');
    const t = document.createElement('span');
    t.className = 'ts';
    t.textContent = `[${stamp()}] `;
    div.appendChild(t);
    const body = document.createElement('span');
    body.textContent = text;
    if (cls) body.className = cls;
    div.appendChild(body);
    logEl.appendChild(div);
    logEl.scrollTop = logEl.scrollHeight;
  }

  append(`[byd-control] action=${ACTION} session=${SESSION} connected`);
  if (DRY) append('[byd-control] dry-run mode — simulating phone output', 'done');

  const POLL_MS = 500;   // how often we fetch new lines
  const MAX_MS = 90_000; // safety cap so we never poll forever

  (async () => {
    const started = Date.now();
    let offset = 0;
    let done = false;
    try {
      while (!done && Date.now() - started < MAX_MS) {
        const r = await fetch(
          `/api/control/poll?session=${encodeURIComponent(SESSION)}&offset=${offset}${KEY_QS}`
        );
        if (!r.ok) {
          const detail = await r.text().catch(() => '');
          append(`poll HTTP ${r.status} ${detail}`.trim(), 'err');
          statusEl.textContent = `poll failed (HTTP ${r.status})`;
          break;
        }
        const data = await r.json();
        for (const line of (data.lines || [])) {
          // Don't surface the internal completion sentinel itself.
          if (line && line.trim().toUpperCase() === '__DONE__') continue;
          append(line);
        }
        offset = data.offset != null ? data.offset : offset;
        done = !!data.done;
        if (!done) await new Promise(res => setTimeout(res, POLL_MS));
      }
      statusEl.textContent = done ? 'completed' : 'timed out';
    } catch (err) {
      append(String(err), 'err');
      statusEl.textContent = 'error';
    }
  })();
</script>
</body>
</html>
"""


def _render_control_page(session: str, action: str, dry_run: bool, api_key: str) -> str:
    """Self-contained HTML page showing a control action's live log lines.

    Served instead of JSON when a *browser* navigates straight to
    ``/api/control?q=...`` (the request ``Accept`` header asks for
    ``text/html``).  API clients and the ``control.html`` JavaScript fetch
    with ``Accept: */*`` and keep getting JSON.  The page polls
    ``/api/control/poll`` every 500ms and renders each line as it arrives —
    the same behaviour as ``public/control.html``, but self-starting.
    """
    key_qs = f"&key={api_key}" if api_key else ""
    return (
        _CONTROL_PAGE.replace("__SESSION__", session)
        .replace("__ACTION__", action)
        .replace("__KEY_QS__", key_qs)
        .replace("__DRY__", "true" if dry_run else "false")
    )


# ---------------------------------------------------------------------------
# Endpoints — monitoring
# ---------------------------------------------------------------------------


@app.get("/api/health")
async def health() -> dict:
    """Lightweight health check — does NOT contact BYD servers."""
    return {
        "status": "ok",
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


@app.get("/monitor/raw")
@app.get("/monitor/raw/")
async def get_vehicle_data_raw() -> dict:
    """Raw vehicle data — every pyBYD section, untouched.

    Untransformed raw pyBYD payload for every vehicle section.
    """
    vehicle_results = await _login_and_fetch_vehicles()

    if not vehicle_results:
        return {
            "success": True,
            "vehicle_count": 0,
            "vehicles": [],
            "message": "No vehicles found on this BYD account.",
            "fetched_at": datetime.now(timezone.utc).isoformat(),
        }

    return {
        "success": True,
        "vehicle_count": len(vehicle_results),
        "vehicles": vehicle_results,
        "fetched_at": datetime.now(timezone.utc).isoformat(),
    }


@app.get("/monitor")
@app.get("/monitor/")
async def get_vehicle_data() -> dict:
    """Clean, deduplicated, human-readable vehicle data.

    Uses the exact same BYD data source as ``/monitor/raw`` but transforms
    each vehicle with :func:`transform.transform_vehicle`:

    * every nested ``raw`` BYD payload is removed,
    * values BYD repeats across sections (odometer, SoC, timezone,
      temperatures, seat states, ...) are collapsed to one canonical field,
    * enum integers become ``{"code", "label"}`` objects and binary flags
      become booleans.
    """
    vehicle_results = await _login_and_fetch_vehicles()

    if not vehicle_results:
        return {
            "success": True,
            "vehicle_count": 0,
            "vehicles": [],
            "message": "No vehicles found on this BYD account.",
            "fetched_at": datetime.now(timezone.utc).isoformat(),
        }

    return {
        "success": True,
        "vehicle_count": len(vehicle_results),
        "vehicles": [transform_vehicle(v) for v in vehicle_results],
        "fetched_at": datetime.now(timezone.utc).isoformat(),
    }


# ---------------------------------------------------------------------------
# Endpoints — vehicle control + streaming log
# ---------------------------------------------------------------------------


@app.get("/api/control")
async def control_vehicle(
    request: Request,
    q: str | None = Query(
        default=None,
        description=(
            "Action to perform. One of: unlock, lock, headlights, honk, "
            "trunk_open, trunk_close, windows_close, engine_off."
        ),
    ),
) -> dict:
    """Fire a vehicle action via MacroDroid and return a session to poll.

    Flow: this endpoint triggers the matching MacroDroid webhook → the
    always-on phone performs the action in the BYD app while POSTing status
    updates to ``/api/log`` → the client polls ``/api/control/poll`` every
    ~500ms to read those lines as they arrive.

    Returns JSON immediately instead of a long-lived stream — Vercel's Python
    runtime buffers ``StreamingResponse`` bodies until the generator ends, so
    line-by-line live output is delivered by polling instead.

    A browser navigating here directly (``Accept: text/html``) gets a
    self-refreshing HTML page that polls ``/api/control/poll`` every 500ms
    and renders the live lines; API clients get the JSON below.
    """
    if not _authorized(request):
        raise HTTPException(status_code=401, detail="Invalid or missing control API key.")

    action = resolve_action(q)
    if action is None:
        raise HTTPException(
            status_code=400,
            detail=(
                "Unknown or missing control action. Use one of: "
                + ", ".join(sorted(CONTROL_MAP))
            ),
        )

    store = get_log_store()
    sid = uuid.uuid4().hex[:12]
    await store.create_session(sid)

    # Kick off the phone work — real webhook call or dry-run simulation.
    if _CONTROL_DRY_RUN:
        asyncio.create_task(simulate_phone(store, sid, action))
        dry_run = True
    else:
        trigger = await trigger_macrodroid(action, sid=sid)
        if not trigger.get("ok"):
            await store.drop(sid)
            raise HTTPException(
                status_code=502,
                detail=f"Failed to trigger MacroDroid. {trigger.get('error')}",
            )
        dry_run = False

    # A browser hitting the URL directly wants to *watch* the action run.
    if "text/html" in (request.headers.get("accept") or "").lower():
        return HTMLResponse(
            _render_control_page(
                sid, action, dry_run, request.query_params.get("key") or ""
            )
        )

    return {
        "ok": True,
        "action": action,
        "session": sid,
        "dry_run": dry_run,
        "poll_url": f"/api/control/poll?session={sid}",
    }


@app.get("/api/control/poll")
async def control_poll(
    request: Request,
    session: str | None = Query(default=None),
    offset: int = Query(default=0, ge=0),
) -> dict:
    """Return new log lines for ``session`` since ``offset``.

    The client polls this every ~500ms while a control action runs.  Returns
    the fresh lines, the next ``offset`` to pass back, and whether the phone
    has reported completion (``done``).
    """
    if not _authorized(request):
        raise HTTPException(status_code=401, detail="Invalid or missing control API key.")
    if not session:
        raise HTTPException(
            status_code=400, detail="Missing required query param 'session'."
        )

    store = get_log_store()
    try:
        lines, offset, done = await store.fetch(session, offset)
    except KeyError:
        raise HTTPException(
            status_code=404,
            detail=f"No active control session {session!r}. Start one via /api/control.",
        )

    return {"ok": True, "lines": lines, "offset": offset, "done": done}


@app.post("/api/log")
async def log_receiver(
    request: Request,
    session: str | None = Query(default=None),
    done: str | None = Query(default=None),
    line: str | None = Query(default=None),
) -> dict:
    """Log receiver — the phone (MacroDroid) POSTs its live status lines here.

    Accepted payloads:
      * query params ``?session=&line=&done=``
      * form-urlencoded body with the same fields
      * JSON body ``{"session": …, "line": …, "done": bool}``
      * raw text body (the whole body is one log line)
      * ``X-Session`` header

    ``done`` may be ``1/true/yes/on`` (or the line itself may be
    ``__DONE__``) to mark the session complete and end the client stream.
    When ``session`` is omitted, the line lands on the most recently created
    (current) session.
    """
    if not _authorized(request):
        raise HTTPException(status_code=401, detail="Invalid or missing control API key.")

    store = get_log_store()

    # --- Parse the incoming payload -------------------------------------
    sid = session
    text = line
    done_flag = done

    if request.headers.get("x-session"):
        sid = sid or request.headers["x-session"]

    if request.method == "POST":
        ctype = (request.headers.get("content-type") or "").split(";")[0].strip().lower()
        body = await request.body()
        if ctype == "application/json":
            try:
                data = await request.json()
            except Exception:
                data = {}
            sid = sid or data.get("session") or data.get("sid")
            text = text or data.get("line") or data.get("message")
            if "done" in data:
                done_flag = str(data.get("done", "")).lower()
        elif ctype == "application/x-www-form-urlencoded":
            # Parsed with the stdlib so a simple MacroDroid ``key=value``
            # POST works without pulling in python-multipart.  (multipart
            # bodies fall through to the raw-text branch below.)
            form = urllib.parse.parse_qs(body.decode("utf-8", "replace"))
            sid = sid or _first_form(form.get("session")) or _first_form(form.get("sid"))
            text = text or _first_form(form.get("line")) or _first_form(form.get("message"))
            done_flag = done_flag or _first_form(form.get("done"))
        elif body:
            text = text or body.decode("utf-8", "replace").strip() or None

    if not text and not is_done_param(done_flag):
        raise HTTPException(status_code=400, detail="No log line received.")

    # --- Find the target session -----------------------------------------
    if sid is None:
        sid = await store.current()
    if sid is None:
        raise HTTPException(
            status_code=404, detail="No active control session. Start one via /api/control."
        )

    # --- Mark done before appending so the line + completion arrive
    # together on the next poll.
    complete = is_done_param(done_flag) or (
        text is not None and is_done_marker(text)
    )
    if complete:
        await store.complete(sid)

    if text:
        ok = await store.append(sid, text)
        if not ok:
            raise HTTPException(status_code=404, detail=f"No active session {sid!r}.")

    return {"ok": True, "session": sid, "done": complete}


# ---------------------------------------------------------------------------
# Shared auth / fetch pipeline
# ---------------------------------------------------------------------------


async def _login_and_fetch_vehicles() -> list[dict]:
    """Authenticate, discover vehicles, and fetch the raw v1 payload for
    every vehicle.

    Shared by the ``/monitor`` and ``/monitor/raw`` endpoints.  Raises
    ``HTTPException`` (401/502/504) on any failure; returns a list of
    per-vehicle dicts (empty only when the account has no vehicles).
    """
    config = _build_config()

    # ── Authenticate & discover vehicles ──────────────────────────────
    async with BydClient(config) as client:
        try:
            await client.login()
        except BydAuthenticationError as exc:
            raise HTTPException(
                status_code=401,
                detail=f"BYD authentication failed. Check your credentials. ({exc})",
            ) from exc
        except BydTransportError as exc:
            raise HTTPException(
                status_code=504,
                detail=f"Could not reach BYD servers. ({exc})",
            ) from exc
        except BydApiError as exc:
            raise HTTPException(
                status_code=502,
                detail=f"BYD API error during login. ({exc})",
            ) from exc

        # Fetch vehicle list
        try:
            vehicles = await client.get_vehicles()
        except (BydApiError, BydTransportError) as exc:
            raise HTTPException(
                status_code=502,
                detail=f"Failed to fetch vehicle list. ({exc})",
            ) from exc

        # ── Gather all data for each vehicle ──────────────────────────
        vehicle_results: list[dict] = []

        for vehicle in vehicles:
            v_data = await _fetch_vehicle_data(client, vehicle.vin)
            v_data["vin"] = vehicle.vin
            v_data["info"] = vehicle.model_dump(by_alias=True, mode="json")
            vehicle_results.append(v_data)

        return vehicle_results


# ---------------------------------------------------------------------------
# Per-vehicle data gathering
# ---------------------------------------------------------------------------

_FETCH_ENDPOINTS: dict[str, str] = {
    "realtime": "get_vehicle_realtime",
    "gps": "get_gps_info",
    "hvac": "get_hvac_status",
    "charging": "get_charging_homepage",
    "energy": "get_energy_consumption",
    "config": "get_latest_config",
}


async def _fetch_vehicle_data(client: BydClient, vin: str) -> dict[str, object]:
    """Fetch all data endpoints for a single vehicle in parallel.

    Returns a dict keyed by data category.  On per-endpoint failure the
    value is ``{"error": "<message>"}`` or ``null`` (unavailable data).
    """

    async def _safe_fetch(key: str) -> tuple[str, object | None, str | None]:
        """Run one endpoint and return (key, result, error_message)."""
        coro = getattr(client, _FETCH_ENDPOINTS[key])(vin)
        try:
            result = await coro
            return key, result, None
        except BydDataUnavailableError:
            return key, None, None
        except (BydApiError, BydTransportError) as exc:
            return key, None, str(exc)
        except Exception as exc:
            return key, None, f"Unexpected error: {exc}"

    async def _safe_fetch_gps() -> tuple[str, object | None, str | None]:
        """Fetch GPS with more aggressive polling.

        GPS uses a trigger-and-poll mechanism that can take longer
        than the default 15 s (10 x 1.5 s).  This gives it more time
        and returns a structured response when coordinates aren't
        available instead of silently ``None``.
        """
        try:
            result = await client.get_gps_info(
                vin,
                poll_attempts=30,
                poll_interval=2.0,
                mqtt_timeout=10.0,
            )
            return "gps", result, None
        except BydDataUnavailableError:
            return "gps", None, "No GPS fix (vehicle may be in a garage or underground)"
        except (BydApiError, BydTransportError) as exc:
            return "gps", None, str(exc)
        except Exception as exc:
            return "gps", None, f"Unexpected error: {exc}"

    # Launch all 6 endpoints concurrently -- GPS uses its own call
    tasks = [_safe_fetch(key) for key in _FETCH_ENDPOINTS if key != "gps"]
    tasks.append(_safe_fetch_gps())
    gathered = await asyncio.gather(*tasks)

    data: dict[str, object] = {}
    for key, value, error in gathered:
        if error is not None:
            data[key] = {"error": error}
        elif value is None:
            data[key] = None
        elif isinstance(value, tuple):
            # get_charging_homepage → (ChargingStatus, SmartChargingSchedule)
            data[key] = {
                "status": _serialize(value[0]),
                "schedule": _serialize(value[1]),
            }
        elif key == "gps":
            data[key] = _serialize_gps(value)
        else:
            data[key] = _serialize(value)

    return data


# ---------------------------------------------------------------------------
# Global error handlers
# ---------------------------------------------------------------------------


@app.exception_handler(BydAuthenticationError)
async def _auth_exc_handler(_request: Request, exc: BydAuthenticationError) -> JSONResponse:
    return JSONResponse(
        status_code=401,
        content={"success": False, "error": "authentication_failed", "detail": str(exc)},
    )


@app.exception_handler(BydTransportError)
async def _transport_exc_handler(_request: Request, exc: BydTransportError) -> JSONResponse:
    return JSONResponse(
        status_code=504,
        content={"success": False, "error": "transport_error", "detail": str(exc)},
    )


@app.exception_handler(BydApiError)
async def _api_exc_handler(_request: Request, exc: BydApiError) -> JSONResponse:
    return JSONResponse(
        status_code=502,
        content={"success": False, "error": "byd_api_error", "detail": str(exc)},
    )


@app.exception_handler(Exception)
async def _generic_exc_handler(_request: Request, exc: Exception) -> JSONResponse:
    return JSONResponse(
        status_code=500,
        content={"success": False, "error": "internal_error", "detail": str(exc)},
    )
