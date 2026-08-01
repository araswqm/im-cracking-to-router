# BYD Vehicle Monitor & Control API

Vercel serverless API for a BYD account, backed by [pyBYD](https://github.com/jkaberg/pyBYD).

* Monitor your vehicle: realtime, GPS, HVAC, charging, energy, config.
* Control it through a MacroDroid webhook on an always-on phone, with the
  phone's live progress streamed back to the client as plain text.

## Endpoints

| Method | Path | Description |
|---|---|---|
| `GET` | `/monitor` | Clean, deduplicated, human-readable vehicle data |
| `GET` | `/monitor/raw` | Raw pyBYD payloads, untransformed |
| `GET` | `/api/control?q=<action>` | Fire a vehicle action + stream live phone log |
| `POST` | `/api/log` | Phone log receiver (MacroDroid POSTs here) |
| `GET` | `/api/health` | Health check (no BYD auth) |

## Environment variables

See [`.env.example`](.env.example). Required: `BYD_USERNAME`, `BYD_PASSWORD`.
Optional: `BYD_BASE_URL`, `BYD_COUNTRY_CODE`, `MACRODROID_BASE_URL`,
`CONTROL_API_KEY`, `CONTROL_STREAM_TIMEOUT`, `CONTROL_DRY_RUN`,
`UPSTASH_REDIS_REST_URL`, `UPSTASH_REDIS_REST_TOKEN`.

## Vehicle control flow

```
Client ──GET /api/control?q=lock──▶ Vercel API
                                       │ triggers MacroDroid webhook
                                       ▼
                                   MacroDroid  ──▶  phone performs action
                                       │              in the BYD app
                                       │  POST /api/log (each step)
                                       ▼
                                   Vercel API ── streams lines to client
                                       ▼
                                   Client sees live text/plain output
```

1. `GET /api/control?q=<action>` validates the action and creates a session.
2. The API fires the MacroDroid webhook:
   `{MACRODROID_BASE_URL}?control=<action>&sid=<session>`.
3. The phone's MacroDroid macro performs the action and POSTs each status
   update to `POST /api/log?session=<session>&line=<message>` (or as a
   form/JSON/raw-text body). When finished it sends
   `POST /api/log?...&done=1` — or posts a `__DONE__` line.
4. The `/api/control` response streams each line back live as
   `text/plain; charset=utf-8` (chunked, no HTML), one line per newline,
   ending with `[byd-control] completed`.

Supported `q` values: `unlock`, `lock`, `headlights`, `honk`, `trunk_open`,
`trunk_close`, `windows_close`, `engine_off`.

### MacroDroid setup (phone side)

1. MacroDroid **HTTP Trigger** with URL
   `{MACRODROID_BASE_URL}` (macro starts when this trigger fires).
2. In the macro's settings, capture the incoming query parameters:
   * `control` → which action to perform (choose the BYD app action).
   * `sid` → the session id, to send back on every log POST.
3. For each step, add an **HTTP Request** action that POSTs to
   `https://<your-vercel-deployment>/api/log` — e.g.
   * query string: `?session=[sid]&line=Checking locks…`
   * or `Content-Type: text/plain` body: `Checking locks…`
   * send `X-Session: [sid]` header as an alternative to the query param.
4. On the final step, POST `?session=[sid]&done=1` (or a body of `__DONE__`)
   so the client stream ends cleanly.

> When the phone omits `session`, the line lands on the most recent active
> session automatically.

### Auth

If `CONTROL_API_KEY` is set, `/api/control` and `/api/log` require it via
`?key=<key>` or `Authorization: Bearer <key>`.

### Dry-run

Set `CONTROL_DRY_RUN=1` to simulate the phone: no real MacroDroid call, the
stream emits a handful of fake steps. Good for testing without the phone.

## Streaming on Vercel

* `vercel.json` sets `maxDuration: 60` for the function (Hobby default is
  10 s).
* The stream closes after `CONTROL_STREAM_TIMEOUT` (default 45 s) if the
  phone never reports completion — stay under the function limit.
* Anti-buffering headers (`Cache-Control: no-cache, no-transform`,
  `X-Accel-Buffering: no`) are set on the control response.

## Shared state across instances

Vercel serverless is stateless. Without extra configuration the log queue is
in-process (`api/logqueue.py`), which works when requests keep hitting the
same warm instance. For reliability across instances, set
`UPSTASH_REDIS_REST_URL` + `UPSTASH_REDIS_REST_TOKEN` and the same code
switches to an Upstash Redis-backed queue (no new dependencies).

## Local development

```bash
pip install -r requirements.txt
uvicorn api.index:app --reload
```

## Tests

```bash
pip install pytest
pytest
```

Tests use a pybyd shim, so they run without a real BYD account or Vercel.
